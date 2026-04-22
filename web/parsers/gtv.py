"""
GTV (Q2Pro Global TV) client + live game-state aggregator.

Connects to a live Q2Pro server's MVD streaming port (sv_mvd_enable 2) and
yields raw GTS_STREAM_DATA payloads, then incrementally parses them into a
snapshot dict that mirrors the shape of the .mvd2 replay JSON.

Wire framing (both client→server and server→client):
  [uint16 LE: payload_size+1] [uint8 op] [payload_size bytes]

Handshake:
  → 4 bytes "MVD2" (magic);  ← 4 bytes echo
  → GTC_HELLO  payload: uint16 protocol, int32 flags, int32 reserved,
                        cstr name, cstr password, cstr version
  ← GTS_HELLO  (server info, payload ignored)
  → GTC_STREAM_START payload: int16 maxbuf=0
  ← GTS_STREAM_START
  ← GTS_STREAM_DATA … (one block per game frame; empty payload = paused)

Security:
  - Only ports in GTV_PORT_RANGE are allowed.
  - Loopback addresses are blocked (prevents SSRF against the Flask process).
"""

from __future__ import annotations

import ipaddress
import logging
import socket
import struct
import time
from collections import defaultdict
from typing import Generator, Optional

from .mvd2 import (
    ANGLE_SCALE,
    COORD_SCALE,
    CS_IMAGES,
    CS_MODELS,
    CS_PLAYERSKINS,
    MAX_CLIENTS,
    MAX_CONFIGSTRINGS,
    MVD_CONFIGSTRING,
    MVD_FRAME,
    MVD_MULTICAST_ALL,
    MVD_MULTICAST_PVS_R,
    MVD_NOP,
    MVD_PRINT,
    MVD_SERVERDATA,
    MVD_SOUND,
    MVD_UNICAST,
    MVD_UNICAST_R,
    PRINT_CHAT,
    SVCMD_BITS,
    SVCMD_MASK,
    Msg,
    _ITEM_ICON_STEMS,
    _append_round_event_from_print,
    _build_chat_event,
    _build_weapon_models,
    _is_world_bsp,
    _normalize_round_print_text,
    _parse_frame,
    _parse_multicast,
    _parse_skin_configstring,
    _parse_unicast,
    _team_label_key,
    _extract_layout_scoreboard_rows,
    _extract_layout_team_rows,
    CS_SOUNDS,
    CHAN_WEAPON,
)

_log = logging.getLogger(__name__)

# ── GTV protocol constants ───────────────────────────────────────────────────

GTS_HELLO        = 0
GTS_PONG         = 1
GTS_STREAM_START = 2
GTS_STREAM_STOP  = 3
GTS_STREAM_DATA  = 4
GTS_ERROR        = 5
GTS_BADREQUEST   = 6
GTS_NOACCESS     = 7
GTS_DISCONNECT   = 8
GTS_RECONNECT    = 9

GTC_HELLO        = 0
GTC_PING         = 1
GTC_STREAM_START = 2
GTC_STREAM_STOP  = 3
GTC_STRINGCMD    = 4

GTV_PROTOCOL_VERSION = 0xED04

GTV_PORT_MIN = 27900
GTV_PORT_MAX = 27970

# ── Damage model constants (mirrors web/parsers/mvd2.py _build) ──────────────

MZ_SHOTGUN_WIRE = 2
MZ_HC_WIRE      = 13
MZ_SNIPER_WIRE  = 14

_WEAPON_BASE = {
    'MK23': 90, 'Dual MK23': 90, 'M4': 90, 'MP5': 55,
    'SR': 250, 'Sniper': 250,
    'M3': 17, 'HC': 18,
    'Knife': 50, 'Knife Thrown': 50,
}
_LOC_MULT = {
    'head': 1.8, 'chest': 0.65, 'stomach': 0.40, 'legs': 0.25,
    'unknown': 0.55,
    'kvlr_helmet': 0.5, 'kvlr_vest': 0.1,
}

# ── Security validation ──────────────────────────────────────────────────────

def _validate_host_port(host: str, port: int) -> None:
    """Reject loopback addresses and ports outside the GTV range."""
    if not (GTV_PORT_MIN <= port <= GTV_PORT_MAX):
        raise ValueError(
            f"Port {port} is outside the allowed range "
            f"{GTV_PORT_MIN}-{GTV_PORT_MAX}"
        )
    try:
        addr = ipaddress.ip_address(host)
        if addr.is_loopback:
            raise ValueError("Loopback addresses are not allowed")
    except ValueError as exc:
        if "Loopback" in str(exc):
            raise
    if host.lower() in ('localhost', 'localhost.localdomain'):
        raise ValueError("Loopback addresses are not allowed")


# ── Low-level GTV framing ────────────────────────────────────────────────────

def _pack_string(s: str) -> bytes:
    return s.encode('utf-8', errors='replace') + b'\x00'


def _build_gtv_msg(op: int, payload: bytes) -> bytes:
    length = len(payload) + 1
    return struct.pack('<H', length) + bytes([op]) + payload


# ── GTV TCP client ───────────────────────────────────────────────────────────

class GtvError(Exception):
    pass


class GtvClient:
    """
    Connects to a Q2Pro server's MVD streaming port (GTV protocol) and yields
    raw GTS_STREAM_DATA block payloads.  Each yielded bytes value is the raw
    MVD command stream for one game frame.  An empty bytes value means the
    stream is temporarily suspended (between maps).
    """

    def __init__(
        self,
        host: str,
        port: int,
        password: str = '',
        name: str = 'aq2stats',
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
    ):
        _validate_host_port(host, port)
        self.host            = host
        self.port            = port
        self.password        = password
        self.name            = name
        self.connect_timeout = connect_timeout
        self.read_timeout    = read_timeout
        self._sock: Optional[socket.socket] = None

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise GtvError("Connection closed by server")
            buf.extend(chunk)
        return bytes(buf)

    def _read_message(self) -> tuple:
        header = self._recv_exact(3)
        length = struct.unpack('<H', header[:2])[0]
        op     = header[2]
        payload = self._recv_exact(length - 1) if length > 1 else b''
        return op, payload

    def _handshake(self) -> None:
        # Step 1: send 4-byte MVD magic and read echo
        self._sock.sendall(b'MVD2')
        echo = self._recv_exact(4)
        if echo != b'MVD2':
            raise GtvError(f"Bad magic echo from server: {echo!r}")

        # Step 2: GTC_HELLO
        body = (
            struct.pack('<H', GTV_PROTOCOL_VERSION)
            + struct.pack('<ii', 0, 0)
            + _pack_string(self.name)
            + _pack_string(self.password)
            + _pack_string('aq2stats/1.0')
        )
        self._sock.sendall(_build_gtv_msg(GTC_HELLO, body))

        op, payload = self._read_message()
        if op == GTS_NOACCESS:
            raise GtvError("Access denied — wrong password or IP blacklisted")
        if op == GTS_ERROR:
            raise GtvError(
                f"Server error: {payload.split(b'<chr-0>')[0].decode('latin-1', errors='replace')}"
            )
        if op != GTS_HELLO:
            raise GtvError(f"Expected GTS_HELLO, got opcode {op}")

        # Step 3: GTC_STREAM_START (maxbuf=0 = unlimited)
        self._sock.sendall(_build_gtv_msg(GTC_STREAM_START, struct.pack('<h', 0)))
        op, payload = self._read_message()
        if op == GTS_NOACCESS:
            raise GtvError("Stream start denied — sv_mvd_enable not set on server?")
        if op == GTS_ERROR:
            raise GtvError(
                f"Stream start error: {payload.split(b'<chr-0>')[0].decode('latin-1', errors='replace')}"
            )
        if op != GTS_STREAM_START:
            raise GtvError(f"Expected GTS_STREAM_START, got opcode {op}")

    def connect(self) -> None:
        try:
            self._sock = socket.create_connection(
                (self.host, self.port), timeout=self.connect_timeout
            )
        except ConnectionRefusedError:
            raise GtvError(
                f"Connection refused — {self.host}:{self.port} not accepting GTV. "
                f"Check sv_mvd_enable 2 on the server and that the port is correct."
            )
        except socket.timeout:
            raise GtvError(f"Connection timed out reaching {self.host}:{self.port}")
        except OSError as e:
            raise GtvError(f"Network error connecting to {self.host}:{self.port}: {e}")
        self._sock.settimeout(self.read_timeout)
        self._handshake()

    def iter_blocks(self) -> Generator[bytes, None, None]:
        """Yield GTS_STREAM_DATA payloads. Sends keepalive pings every 20 s."""
        PING_INTERVAL = 20.0
        self._sock.settimeout(5.0)
        last_ping = time.monotonic()
        buf = bytearray()
        try:
            while True:
                now = time.monotonic()
                if now - last_ping >= PING_INTERVAL:
                    self._sock.sendall(_build_gtv_msg(GTC_PING, b''))
                    last_ping = now
                try:
                    chunk = self._sock.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    raise GtvError("Connection closed by server")
                buf.extend(chunk)
                while len(buf) >= 3:
                    length = struct.unpack('<H', buf[:2])[0]
                    total  = 2 + length
                    if len(buf) < total:
                        break
                    op      = buf[2]
                    payload = bytes(buf[3:total])
                    del buf[:total]
                    if op == GTS_STREAM_DATA:
                        yield payload
                    elif op in (GTS_DISCONNECT, GTS_RECONNECT):
                        return
                    elif op == GTS_ERROR:
                        err = payload.split(b'\x00')[0].decode('latin-1', errors='replace')
                        raise GtvError(f"Server error: {err}")
                    # GTS_PONG and other ops silently consumed
        finally:
            if self._sock:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None

    def stream_blocks(self) -> Generator[bytes, None, None]:
        self.connect()
        yield from self.iter_blocks()


# ── Live game-state aggregator ───────────────────────────────────────────────

# Round-state machine for the HLTV ticker:
ROUND_IDLE   = 'idle'    # warmup / between rounds
ROUND_LIGHTS = 'lights'  # LIGHTS centerprint — players spawn, can't shoot
ROUND_CAMERA = 'camera'  # CAMERA  centerprint — countdown
ROUND_ACTION = 'action'  # ACTION! — combat enabled


class LiveGameState:
    """
    Maintain a live snapshot of the game by processing GTS_STREAM_DATA payloads
    one block at a time.  Each `process_block()` call returns either None (no
    new frame yet) or a snapshot dict shaped like the replay JSON:

        {
          'type': 'snapshot',
          'map': 'rantasauna',
          'frame_idx': 1234,
          'duration_s': 123.4,
          'frame': {'t': 1234, 'players': {numStr: {x,y,z,a,w}}},
          'player_names': {numStr: name},
          'player_teams': {name: 1|2},
          'player_teams_num': {numStr: 1|2},
          'ghost_clients': [int, ...],
          'weapon_models': {idxStr: vwep_stem},
          'team_scores':   {'1': int, '2': int},
          'round_wins':    {'1': int, '2': int},
          'round_ties':    int,
          'round_state':   'idle' | 'lights' | 'camera' | 'action',
          'kill_counts': {name: int},
          'death_counts': {name: int},
          'team_kill_counts': {name: int},
          'hit_counts': {name: int},
          'damage_dealt': {name: int},
          'shots_fired': {name: int},
          'accuracy':    {name: float},
          'headshot_kills': {name: int},
          'best_streak': {name: int},
          'weapon_counts':   {weapon: int},
          'kill_loc_counts': {loc: int},
          'award_counts':    {name: {award: int}},
          # Events delta since the last snapshot:
          'kills':  [...kill events...],
          'prints': [...round/score banner texts...],
          'chat':   [...chat events...],
        }
    """

    # ── lifecycle ────────────────────────────────────────────────────────────

    def __init__(self):
        self._reset_match()

    def _reset_match(self) -> None:
        # Configuration / identity
        self.map_name: Optional[str] = None
        self._configstrings: dict     = {}
        self._weapon_models: dict     = {}
        self.frame_idx: int           = 0

        # Player roster
        self.player_names: dict        = {}   # cnum (int) → name
        self.player_teams: dict        = {}   # cnum (int) → 1 | 2
        self.name_history: dict        = {}   # cnum → set[name]
        self.player_team_history: dict = {}   # cnum → [(frame, team)]

        # Per-frame mutable position cache
        self._pos_state: dict = {}            # cnum → {ox, oy, oz, yaw, weapon}
        self._img_to_bit: dict = {}           # CS_IMAGES index → item bitmask

        # Per-player aggregates (keyed by display name).
        # `hit_counts` and `hit_loc_by_player` count ALL hits regardless of
        # round-active state (matches offline _build()) so the displayed
        # totals and damage_dealt include every confirmed hit.
        # `hits_during_round` is round-gated and used only for accuracy
        # (mirrors the game's Stats_AddHit gated on team_round_going).
        # Layout floor: authoritative K/D from end-of-round scoreboards.
        # Delta: kills/deaths counted live since the last layout snapshot.
        # Effective kill_counts = floor + delta. When we join mid-match the
        # first end-of-round layout resets the floor to the server's totals.
        self._layout_kills:    dict = {}
        self._layout_deaths:   dict = {}
        self._delta_kills:     dict = defaultdict(int)
        self._delta_deaths:    dict = defaultdict(int)
        self.team_kill_counts: dict = defaultdict(int)
        self.hit_counts:       dict = defaultdict(int)
        self.hit_loc_by_player: dict = defaultdict(lambda: defaultdict(int))
        self.hits_during_round: dict = defaultdict(int)
        self.shots_fired:      dict = defaultdict(int)
        self.shots_by_mz:      dict = defaultdict(lambda: defaultdict(int))
        self.player_weapon_kills: dict = defaultdict(lambda: defaultdict(int))
        self.headshot_kills:   dict = defaultdict(int)
        self.award_counts:     dict = defaultdict(lambda: defaultdict(int))
        self.best_streak:      dict = defaultdict(int)
        self._cur_streak:      dict = defaultdict(int)

        # Match-wide aggregates
        self.weapon_counts:     dict = defaultdict(int)
        self.kill_loc_counts:   dict = defaultdict(int)
        self.team_scores:       dict = {1: 0, 2: 0}
        self.round_wins:        dict = {1: 0, 2: 0}
        self.round_ties:        int  = 0

        # Server-authoritative scoreboard values from SVC_LAYOUT.
        # These carry real T_Damage() totals + true accuracy, identical to
        # what the stat-collection tool reports.  Preferred over heuristics.
        self._layout_damage:   dict = {}   # name → int
        self._layout_accuracy: dict = {}   # name → float

        # Round state machine. Stats (shots/hits) are gated on `_round_active`,
        # which mirrors the game's `team_round_going` flag: opens at ACTION!,
        # closes at win/tie. We start closed so warmup activity is excluded
        # (matches offline `_build()` semantics).
        self.round_state:       str  = ROUND_IDLE
        self._round_active:     bool = False
        self._has_seen_round:   bool = False   # True once we've seen any round event
        # Deferred round-close: 'win'/'tie' close the gate at end-of-block, not
        # immediately, so events at the same frame as the win broadcast are
        # still counted (offline window is inclusive of the win frame).
        self._block_close_pending: Optional[str] = None
        # Maps team-name label → 1 or 2 (learned from "Current score is..." prints)
        self._score_name_to_team: dict = {}
        # Wins whose team label couldn't be resolved at receive time (typically
        # the very first round, since "Team X won!" arrives before the first
        # "Current score is..." broadcast that establishes label→side mapping).
        # Resolved retroactively when a later score event populates the map.
        self._unresolved_wins_by_label: dict = defaultdict(int)

        # Pending event queues drained on each snapshot
        self._pending_kills:  list = []
        self._pending_prints: list = []
        self._pending_chat:   list = []
        self._round_ended:    bool = False   # cleared after each snapshot
        self._item_state:     dict = {}      # cnum → item bitmask (from SVC_INVENTORY)

        # Dedup key cache so we don't double-count the same kill across all
        # the unicast copies sent to every client.
        self._seen_kill_keys: set = set()
        # Track the most recently emitted snapshot signature to skip re-emission
        # when nothing has changed (rare but possible during pauses).
        self._last_snapshot_frame: int = -1

    # ── helpers ──────────────────────────────────────────────────────────────

    def _team_for_name(self, name: Optional[str]) -> int:
        if not name:
            return 0
        # Direct name → team via current roster
        for cnum, pname in self.player_names.items():
            if pname == name:
                return int(self.player_teams.get(cnum, 0) or 0)
        # Try alias history (player renamed mid-game)
        for cnum, aliases in self.name_history.items():
            if name in aliases:
                return int(self.player_teams.get(cnum, 0) or 0)
        return 0

    def _classify_kill(self, k: dict) -> dict:
        """Annotate one kill with team_kill / suicide-effective flags."""
        if k.get('suicide'):
            k['team_kill'] = False
            k['killer_team'] = self._team_for_name(k.get('killer'))
            k['victim_team'] = k['killer_team']
            # In a round-based game, a suicide outside the active round
            # window is printed but doesn't decrement the score (p_client.c:901).
            if self._has_seen_round and not self._round_active:
                k['no_score'] = True
            return k
        kt = self._team_for_name(k.get('killer'))
        vt = self._team_for_name(k.get('victim'))
        k['killer_team'] = kt
        k['victim_team'] = vt
        k['team_kill'] = (kt != 0 and kt == vt)
        return k

    def _apply_kill(self, k: dict) -> None:
        """Update aggregates for one classified kill event."""
        killer = k.get('killer')
        victim = k.get('victim')
        weapon = k.get('weapon', 'unknown')
        loc    = k.get('location', 'unknown')

        self.weapon_counts[weapon] += 1
        self.kill_loc_counts[loc]  += 1

        if k.get('suicide'):
            if not k.get('no_score'):
                if killer:
                    self._delta_kills[killer] -= 1
                if victim:
                    self._delta_deaths[victim] += 1
            return

        if k.get('team_kill'):
            if killer:
                self.team_kill_counts[killer] += 1
                # Headshot stat counts ALL non-suicide kills to the head,
                # including TKs (mirrors offline _build()).
                if loc == 'head':
                    self.headshot_kills[killer] += 1
            if self._round_active or not self._has_seen_round:
                if killer:
                    self._delta_kills[killer] -= 1
                if victim:
                    self._delta_deaths[victim] += 1
            # streaks: TKs do NOT continue the streak
            if killer:
                self._cur_streak[killer] = 0
            return

        # Regular enemy kill
        if killer:
            self._delta_kills[killer] += 1
            self.player_weapon_kills[killer][weapon] += 1
            # team_scores is mirrored from "Current score" broadcasts in
            # _absorb_round_event (matches offline). Don't tally here — would
            # diverge from broadcast values in Finalstand mode where the
            # broadcast reports round wins, not raw enemy-kill totals.
            if loc == 'head':
                self.headshot_kills[killer] += 1
            self._cur_streak[killer] = self._cur_streak.get(killer, 0) + 1
            if self._cur_streak[killer] > self.best_streak.get(killer, 0):
                self.best_streak[killer] = self._cur_streak[killer]
        if victim:
            self._delta_deaths[victim] += 1
            self._cur_streak[victim] = 0
        # Punish-kill: victim loses a frag too.
        if k.get('victim_frag_penalty') and victim:
            penalty = int(k.get('victim_frag_penalty', 1) or 1)
            self._delta_kills[victim] -= penalty

    def _apply_hit(self, h: dict) -> None:
        """Update hit aggregates for one PRINT_HIGH 'You hit X' event."""
        attacker_cnum = h.get('attacker')
        name = self.player_names.get(attacker_cnum)
        if not name:
            return
        loc = h.get('location', 'unknown')
        # Display + damage tallies count every confirmed hit (matches offline).
        self.hit_counts[name] = self.hit_counts.get(name, 0) + 1
        self.hit_loc_by_player[name][loc] += 1
        # Accuracy uses round-gated hits only (game's Stats_AddHit guard).
        if self._round_active:
            self.hits_during_round[name] = self.hits_during_round.get(name, 0) + 1

    def _apply_muzzle(self, mf: dict) -> None:
        """Update shots aggregates for one SVC_MUZZLEFLASH multicast event."""
        cnum = mf.get('client')
        name = self.player_names.get(cnum)
        if not name:
            return
        if not self._round_active:
            return  # Stats_AddShot is gated on team_round_going=1 && !in_warmup
        self.shots_fired[name] = self.shots_fired.get(name, 0) + 1
        mz = int(mf.get('mz', 0))
        self.shots_by_mz[name][mz] += 1

    def _apply_silencer_shot(self, cnum: int) -> None:
        """Silenced fire: shot count without an SVC_MUZZLEFLASH."""
        name = self.player_names.get(cnum)
        if not name:
            return
        if not self._round_active:
            return
        self.shots_fired[name] = self.shots_fired.get(name, 0) + 1
        # No MZ wire code for silenced fire.

    def _apply_layout(self, text: str) -> None:
        """Extract server-authoritative K/D/Damage/Acc from end-of-round scoreboards.

        Also bootstraps team assignments from the team-column in the scoreboard.
        On mid-match join, the first layout resets the kill/death floor to the
        server's cumulative totals, so the displayed stats are never zero.
        """
        team_rows = _extract_layout_team_rows(text)
        if not team_rows:
            return
        known = set(self.player_names.values())
        for r in team_rows:
            name = r.get('name', '').strip()
            if not name:
                continue
            # Bootstrap team assignment from scoreboard if not yet known.
            team_from_layout = int(r.get('team', 0) or 0)
            if team_from_layout in (1, 2):
                for cnum, pname in self.player_names.items():
                    if pname == name and not self.player_teams.get(cnum):
                        self.player_teams[cnum] = team_from_layout
            if name not in known:
                continue
            lk  = int(r.get('kills',  0) or 0)
            ld  = int(r.get('deaths', 0) or 0)
            ldm = int(r.get('damage', 0) or 0)
            la  = int(r.get('acc',    0) or 0)
            # Set the authoritative kill/death floor; reset live delta so the
            # next events count from this baseline (prevents double-counting).
            self._layout_kills[name]  = lk
            self._layout_deaths[name] = ld
            self._delta_kills[name]   = 0
            self._delta_deaths[name]  = 0
            if ldm > 0:
                self._layout_damage[name] = ldm
            if la > 0:
                self._layout_accuracy[name] = float(la)

    def _apply_award(self, a: dict) -> None:
        name = a.get('player')
        award = a.get('award')
        if not name or not award:
            return
        self.award_counts[name][award] += 1

    def _update_configstring(self, idx: int, value: str) -> None:
        prev_map = self.map_name
        self._configstrings[idx] = value
        if _is_world_bsp(idx, value):
            new_map = value.split('/')[-1][:-4]
            if prev_map and new_map != prev_map:
                # Map changed mid-stream — wipe match state
                self._reset_match()
                self._configstrings[idx] = value
            self.map_name = new_map
        elif CS_PLAYERSKINS <= idx < CS_PLAYERSKINS + MAX_CLIENTS:
            _parse_skin_configstring(
                idx - CS_PLAYERSKINS, value,
                self.player_names, self.player_teams,
                self.name_history, self.player_team_history,
                self.frame_idx,
            )
        if CS_MODELS <= idx < CS_MODELS + 256:
            self._weapon_models = _build_weapon_models(self._configstrings)
        if CS_IMAGES <= idx < CS_IMAGES + 256 and value:
            stem = value.split('/')[-1].lower().split('.')[0]
            bit  = _ITEM_ICON_STEMS.get(stem, 0)
            if bit:
                self._img_to_bit[idx - CS_IMAGES] = bit

    def _absorb_round_event(self, ev: dict) -> None:
        t = ev.get('type')
        frame = int(ev.get('frame', self.frame_idx) or self.frame_idx)
        if t == 'lights':
            # Mid-block LIGHTS shouldn't reopen a closed gate, but it also
            # shouldn't immediately close one if win/tie hasn't fired yet
            # (out-of-order delivery would drop boundary stats). The gate
            # close is deferred and applied at end-of-block.
            self._has_seen_round = True
            self._block_close_pending = ROUND_LIGHTS
            self._pending_prints.append({
                'type': 'lights', 'frame': frame, 'text': 'LIGHTS…',
            })
        elif t == 'camera':
            # CAMERA only ticks; never opens or closes the gate.
            if self._block_close_pending is None and not self._round_active:
                self.round_state = ROUND_CAMERA
            else:
                self._block_close_pending = ROUND_CAMERA
            self._pending_prints.append({
                'type': 'camera', 'frame': frame, 'text': 'CAMERA…',
            })
        elif t == 'action':
            # ACTION opens the gate immediately so subsequent stat events in
            # this same block are counted for the new round.
            self._has_seen_round = True
            self._round_active = True
            self.round_state = ROUND_ACTION
            self._block_close_pending = None
            self._pending_prints.append({
                'type': 'action', 'frame': frame, 'text': 'ACTION!',
            })
        elif t == 'win':
            self._has_seen_round = True
            tname = _normalize_round_print_text(ev.get('team_name', ''))
            tkey = _team_label_key(tname)
            tnum = self._score_name_to_team.get(tkey, 0) or _team_num_from_label(tname)
            if tnum in (1, 2):
                self.round_wins[tnum] = self.round_wins.get(tnum, 0) + 1
            elif tkey:
                # Stash the win until a later "Current score is..." broadcast
                # tells us which side this team label belongs to.
                self._unresolved_wins_by_label[tkey] += 1
            # Defer gate-close to end of block so muzzle/hit events at the
            # win frame are still counted (offline window is inclusive).
            self._block_close_pending = ROUND_IDLE
            self._round_ended = True
            self._pending_prints.append({
                'type': 'win', 'frame': frame,
                'team_name': tname, 'team_num': tnum,
                'text': f'\U0001f3c6 {tname or "Team"} won the round!',
            })
            # End-of-round resets per-player streaks (game also does this)
            for n in list(self._cur_streak.keys()):
                self._cur_streak[n] = 0
        elif t == 'tie':
            self._has_seen_round = True
            self.round_ties += 1
            self._block_close_pending = ROUND_IDLE
            self._round_ended = True
            self._pending_prints.append({
                'type': 'tie', 'frame': frame,
                'text': '\U0001f91d Round ended in a tie!',
            })
            for n in list(self._cur_streak.keys()):
                self._cur_streak[n] = 0
        elif t == 'score':
            t1 = _normalize_round_print_text(ev.get('team1_name', ''))
            t2 = _normalize_round_print_text(ev.get('team2_name', ''))
            s1 = int(ev.get('score1', 0) or 0)
            s2 = int(ev.get('score2', 0) or 0)
            # First score line in the match: memorise label → side ordering.
            # Score broadcasts emit Team1/Team2 in fixed positional order, so
            # position is authoritative for label→team mapping (matches offline).
            if t1 and t2:
                self._score_name_to_team.setdefault(_team_label_key(t1), 1)
                self._score_name_to_team.setdefault(_team_label_key(t2), 2)
            # Authoritative team_scores: mirror the broadcast values. In
            # Finalstand mode the broadcast reports round wins, not raw enemy
            # kill totals — so this is what scoreboards should display.
            tn1 = self._score_name_to_team.get(_team_label_key(t1), 0) or _team_num_from_label(t1)
            tn2 = self._score_name_to_team.get(_team_label_key(t2), 0) or _team_num_from_label(t2)
            if tn1 in (1, 2):
                self.team_scores[tn1] = s1
                # Sync round_wins to match — bootstraps mid-match joins where
                # we missed earlier "Team X won!" broadcasts.
                if s1 > self.round_wins.get(tn1, 0):
                    self.round_wins[tn1] = s1
            if tn2 in (1, 2):
                self.team_scores[tn2] = s2
                if s2 > self.round_wins.get(tn2, 0):
                    self.round_wins[tn2] = s2
            # Apply any wins we couldn't resolve earlier (typically the first
            # win of the match arrives before the first score broadcast).
            if self._unresolved_wins_by_label:
                resolved_keys: list = []
                for tkey, cnt in self._unresolved_wins_by_label.items():
                    side = self._score_name_to_team.get(tkey, 0)
                    if side in (1, 2):
                        self.round_wins[side] = self.round_wins.get(side, 0) + cnt
                        resolved_keys.append(tkey)
                for k in resolved_keys:
                    del self._unresolved_wins_by_label[k]
            self._pending_prints.append({
                'type': 'score', 'frame': frame,
                'team1_name': t1, 'score1': s1,
                'team2_name': t2, 'score2': s2,
                'text': f'Score: {t1} {s1} \u2014 {s2} {t2}',
            })

    def _dedup_kill(self, k: dict) -> bool:
        """Return True if this kill is new (not a duplicate from another unicast)."""
        key = (k.get('killer'), k.get('victim'), k.get('frame'))
        if key in self._seen_kill_keys:
            return False
        self._seen_kill_keys.add(key)
        # Bound the dedup set so it doesn't grow unbounded over hours.
        if len(self._seen_kill_keys) > 2048:
            # Cheap eviction: clear the oldest half by rebuilding from the tail.
            self._seen_kill_keys = set(list(self._seen_kill_keys)[-1024:])
        return True

    # ── snapshot construction ───────────────────────────────────────────────

    def _player_avg_base_dmg(self, name: str) -> tuple[float, bool]:
        """Mirror _build()'s _player_avg_base_dmg to get (avg_base, is_shotgun)."""
        mz_map  = self.shots_by_mz.get(name, {})
        total_mz = sum(mz_map.values()) if mz_map else 0
        sg_mz   = mz_map.get(MZ_SHOTGUN_WIRE, 0) if mz_map else 0
        hc_mz   = mz_map.get(MZ_HC_WIRE, 0) if mz_map else 0
        sn_mz   = mz_map.get(MZ_SNIPER_WIRE, 0) if mz_map else 0
        if total_mz >= 5:
            if sg_mz / total_mz > 0.5: return 17.0, True
            if hc_mz / total_mz > 0.5: return 18.0, True
            if sn_mz / total_mz > 0.5: return 250.0, False
        wkills = self.player_weapon_kills.get(name, {})
        total_k = sum(wkills.values()) if wkills else 0
        if not total_k:
            return 75.0, False
        m3_k = wkills.get('M3', 0)
        hc_k = wkills.get('HC', 0)
        if m3_k / total_k > 0.5: return 17.0, True
        if hc_k / total_k > 0.5: return 18.0, True
        weighted = sum(_WEAPON_BASE.get(w, 75) * c for w, c in wkills.items())
        return weighted / total_k, False

    def _final_damage_dealt(self) -> dict:
        """Heuristic damage, overridden by server scoreboard where available."""
        out = self._compute_damage_dealt()
        for name, val in self._layout_damage.items():
            if val > 0:
                out[name] = val
        return out

    def _final_accuracy(self) -> dict:
        """Heuristic accuracy, overridden by server scoreboard where available."""
        out = self._compute_accuracy()
        for name, val in self._layout_accuracy.items():
            if val > 0:
                out[name] = val
        return out

    def _compute_damage_dealt(self) -> dict:
        out: dict = {}
        for name, locs in self.hit_loc_by_player.items():
            avg, is_shotgun = self._player_avg_base_dmg(name)
            total = 0.0
            for loc, cnt in locs.items():
                if is_shotgun:
                    total += avg * cnt
                else:
                    total += avg * _LOC_MULT.get(loc, 0.55) * cnt
            out[name] = int(round(total))
        return out

    def _compute_accuracy(self) -> dict:
        out: dict = {}
        for name, shots in self.shots_fired.items():
            if not shots:
                continue
            mz_map  = self.shots_by_mz.get(name, {})
            total_mz = sum(mz_map.values()) if mz_map else 0
            sg = (mz_map.get(MZ_SHOTGUN_WIRE, 0) + mz_map.get(MZ_HC_WIRE, 0)) if mz_map else 0
            if total_mz >= 5 and sg / total_mz > 0.5:
                continue  # shotgun primary — server doesn't emit "You hit"
            # Accuracy uses round-gated hits / round-gated shots so it matches
            # the in-game value (game's Stats_AddHit / Stats_AddShot are both
            # guarded on team_round_going).
            hits = self.hits_during_round.get(name, 0)
            if hits > shots:
                continue
            out[name] = round(hits / shots * 100, 1)
        return out

    def _build_frame_dict(self) -> dict:
        """Return the {numStr: {x,y,z,a,w,h,ar,am,cl,it}} dict for the current frame."""
        out: dict = {}
        for cnum, s in self._pos_state.items():
            d: dict = {
                'x': round(s.get('ox', 0) * COORD_SCALE, 1),
                'y': round(s.get('oy', 0) * COORD_SCALE, 1),
                'z': round(s.get('oz', 0) * COORD_SCALE, 1),
                'a': round(s.get('yaw', 0) * ANGLE_SCALE % 360, 1),
                'w': s.get('weapon', 0),
                'h': s.get('health', -1),
                'ar': s.get('armor', -1),
                'it': self._item_state.get(cnum, 0),
            }
            am = s.get('ammo', -1)
            cl = s.get('clip', -1)
            if am >= 0: d['am'] = am
            if cl >= 0: d['cl'] = cl
            out[str(cnum)] = d
        return out

    def _ghost_clients(self) -> list:
        """Cnums present in pos_state that are excluded from the snapshot's player_names.
        Includes: no skin configstring, empty name, and [MVDSPEC] recorder slots."""
        visible = {k for k, v in self.player_names.items() if v and v != '[MVDSPEC]'}
        return sorted(int(c) for c in self._pos_state.keys() if c not in visible)

    def _make_snapshot(self) -> dict:
        kills_out   = self._pending_kills
        prints_out  = self._pending_prints
        chat_out    = self._pending_chat
        round_ended = self._round_ended
        self._pending_kills  = []
        self._pending_prints = []
        self._pending_chat   = []
        self._round_ended    = False

        frame_dict = self._build_frame_dict()
        names_str  = {str(k): v for k, v in self.player_names.items()
                      if v != '[MVDSPEC]'}
        teams_by_name: dict = {}
        teams_by_num:  dict = {}
        for cnum, name in self.player_names.items():
            t = int(self.player_teams.get(cnum, 0) or 0)
            if t:
                teams_by_name[name] = t
                teams_by_num[str(cnum)] = t

        return {
            'type':            'snapshot',
            'map':             self.map_name or 'unknown',
            'frame_idx':       self.frame_idx,
            'duration_s':      round(self.frame_idx * 0.1, 1),
            'frame':           {'t': self.frame_idx, 'players': frame_dict},
            'player_names':    names_str,
            'player_teams':    teams_by_name,
            'player_teams_num': teams_by_num,
            'ghost_clients':   self._ghost_clients(),
            'weapon_models':   {str(k): v for k, v in self._weapon_models.items()},
            'team_scores':     {'1': self.team_scores.get(1, 0),
                                '2': self.team_scores.get(2, 0)},
            'round_wins':      {'1': self.round_wins.get(1, 0),
                                '2': self.round_wins.get(2, 0)},
            'round_ties':      self.round_ties,
            'round_state':     self.round_state,
            'kill_counts':     {n: self._layout_kills.get(n, 0) + self._delta_kills.get(n, 0)
                                for n in (set(self._layout_kills) | set(self._delta_kills))},
            'death_counts':    {n: self._layout_deaths.get(n, 0) + self._delta_deaths.get(n, 0)
                                for n in (set(self._layout_deaths) | set(self._delta_deaths))},
            'team_kill_counts': dict(self.team_kill_counts),
            'hit_counts':      dict(self.hit_counts),
            'damage_dealt':    self._final_damage_dealt(),
            'shots_fired':     dict(self.shots_fired),
            'accuracy':        self._final_accuracy(),
            'headshot_kills':  dict(self.headshot_kills),
            'best_streak':     dict(self.best_streak),
            'weapon_counts':   dict(self.weapon_counts),
            'kill_loc_counts': dict(self.kill_loc_counts),
            'award_counts':    {n: dict(d) for n, d in self.award_counts.items()},
            'kills':           kills_out,
            'prints':          prints_out,
            'chat':            chat_out,
            'round_end':       round_ended,
        }

    # ── public API ──────────────────────────────────────────────────────────

    def process_block(self, block_bytes: bytes) -> Optional[dict]:
        """
        Process one GTS_STREAM_DATA payload.  Returns a snapshot dict on each
        completed MVD_FRAME (or after MVD_SERVERDATA), or None for empty
        blocks / blocks containing only configstring/print updates.
        """
        if not block_bytes:
            return None

        msg = Msg(block_bytes)
        snapshot: Optional[dict] = None

        while msg.remaining > 0:
            try:
                cb    = msg.u8()
                extra = cb >> SVCMD_BITS
                cmd   = cb & SVCMD_MASK

                if cmd == MVD_NOP:
                    continue

                if cmd == MVD_SERVERDATA:
                    msg.u32()                # protocol major (37)
                    msg.u16()                # protocol minor
                    msg.u32()                # spawncount
                    msg.string()             # gamedir
                    msg.i16()                # dummy clientnum
                    # Treat each MVD_SERVERDATA as a fresh map: wipe everything
                    # and re-establish identity from the embedded configstrings.
                    prev_map = self.map_name
                    self._reset_match()
                    while True:
                        idx = msg.u16()
                        if idx >= MAX_CONFIGSTRINGS:
                            break
                        val = msg.string()
                        # Use the helper so map/skin/weapon-model side effects fire,
                        # but call it with the (now zeroed) frame_idx.
                        self._update_configstring(idx, val)
                    # Baseline frame
                    self._pos_state.clear()
                    _parse_frame(msg, self._pos_state, self._item_state, self._img_to_bit)
                    if prev_map and self.map_name and self.map_name != prev_map:
                        self._pending_prints.append({
                            'type': 'map_change', 'frame': 0,
                            'text': f'Map changed: {prev_map} → {self.map_name}',
                        })
                    snapshot = self._make_snapshot()
                    continue

                if cmd == MVD_CONFIGSTRING:
                    idx = msg.u16()
                    if idx < MAX_CONFIGSTRINGS:
                        val = msg.string()
                        self._update_configstring(idx, val)
                    else:
                        msg.string()  # discard out-of-range value
                    continue

                if cmd == MVD_FRAME:
                    _parse_frame(msg, self._pos_state, self._item_state, self._img_to_bit)
                    self.frame_idx += 1
                    snapshot = self._make_snapshot()
                    continue

                if MVD_MULTICAST_ALL <= cmd <= MVD_MULTICAST_PVS_R:
                    award_events:   list = []
                    round_events:   list = []
                    muzzle_flashes: list = []
                    chat_events:    list = []
                    layout_events:  list = []
                    _parse_multicast(
                        msg, cmd, extra, self.frame_idx,
                        award_events, round_events, muzzle_flashes,
                        self.player_names, self.player_teams, self.name_history,
                        chat_events, layout_events,
                    )
                    for r in round_events:   self._absorb_round_event(r)
                    for a in award_events:   self._apply_award(a)
                    for m in muzzle_flashes: self._apply_muzzle(m)
                    for c in chat_events:    self._pending_chat.append(c)
                    for lv in layout_events: self._apply_layout(lv.get('text', ''))
                    continue

                if cmd in (MVD_UNICAST, MVD_UNICAST_R):
                    kills_buf:    list = []
                    hit_events:   list = []
                    award_events: list = []
                    round_events: list = []
                    chat_events:  list = []
                    layout_events: list = []
                    _parse_unicast(
                        msg, extra, self.frame_idx,
                        kills_buf, hit_events, award_events,
                        self.player_names, self.player_teams, self.name_history,
                        round_events, chat_events, layout_events, None,
                        self._item_state,
                    )
                    # Round events first so kill classification sees correct round_active state.
                    for r in round_events:   self._absorb_round_event(r)
                    for a in award_events:   self._apply_award(a)
                    for h in hit_events:     self._apply_hit(h)
                    for c in chat_events:    self._pending_chat.append(c)
                    for lv in layout_events: self._apply_layout(lv.get('text', ''))
                    for k in kills_buf:
                        if not self._dedup_kill(k):
                            continue
                        self._classify_kill(k)
                        self._apply_kill(k)
                        self._pending_kills.append(k)
                    continue

                if cmd == MVD_SOUND:
                    sb       = msg.u8()
                    snd_idx  = msg.u8()
                    if sb & 1:  msg.u8()
                    if sb & 2:  msg.u8()
                    if sb & 16: msg.u8()
                    chan_word = msg.u16()
                    entity_num = chan_word >> 3
                    channel    = chan_word & 7
                    if channel == CHAN_WEAPON:
                        snd_name = self._configstrings.get(CS_SOUNDS + snd_idx, '')
                        if 'silencer' in snd_name:
                            cnum = entity_num - 1
                            if cnum >= 0:
                                self._apply_silencer_shot(cnum)
                    continue

                if cmd == MVD_PRINT:
                    level = msg.u8()
                    txt = msg.string().strip()
                    if level == PRINT_CHAT and txt:
                        ev = _build_chat_event(
                            txt, self.frame_idx,
                            self.player_names, self.player_teams, self.name_history,
                        )
                        if ev:
                            self._pending_chat.append(ev)
                    if level != PRINT_CHAT:
                        rounds_buf: list = []
                        if _append_round_event_from_print(txt, self.frame_idx, rounds_buf):
                            for r in rounds_buf:
                                self._absorb_round_event(r)
                    continue

                # Unknown opcode — abandon this block to stay aligned with the next
                _log.debug('LiveGameState: unknown cmd=0x%02x extra=%d at offset %d/%d',
                           cmd, extra, msg._p - 1, len(block_bytes))
                break

            except Exception:
                _log.exception('LiveGameState: parse error at offset %d/%d',
                               msg._p, len(block_bytes))
                break

        # Apply any deferred round-close from this block. Done here so that
        # muzzle/hit events sharing a frame with the win/tie broadcast were
        # counted while the gate was still open (matches offline's inclusive
        # `_is_active_round(frame)` window semantics).
        if self._block_close_pending is not None:
            self._round_active = False
            self.round_state = self._block_close_pending
            self._block_close_pending = None

        return snapshot


def _team_num_from_label(label: str) -> int:
    """Best-effort: extract a 1/2 from a team name like 'Team 1', '2', 'Red'."""
    if not label:
        return 0
    s = label.strip().lower()
    if s in ('1', 'team 1', 'team1', 'red'):    return 1
    if s in ('2', 'team 2', 'team2', 'blue'):   return 2
    if s.endswith(' 1') or s.endswith('team1'): return 1
    if s.endswith(' 2') or s.endswith('team2'): return 2
    return 0
