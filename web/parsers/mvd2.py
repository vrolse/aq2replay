"""
MVD2 binary format parser (Quake 2 protocol 37 / q2pro).

Extracts per-frame player positions and metadata for the replay viewer.

Wire format overview
────────────────────
File:
  4 bytes  magic = "MVD2"
  Blocks:
    uint16  length  (0 = EOF)
    <length bytes of MVD command stream>

Each block is a series of commands:
  uint8  cmd_byte
    high 3 bits  = extra flags / length extension
    low  5 bits  = mvd_ops_t command

Coordinates are Q2 12.3 fixed-point  →  divide by 8 for world units.
Angles are stored as uint16           →  multiply by (360/65536) for degrees.
"""
import gzip
import re
import struct
from typing import Optional

# ── Constants ──────────────────────────────────────────────────────────────────

MVD_MAGIC           = b'MVD2'
PROTOCOL_VERSION_MVD = 37
MAX_CONFIGSTRINGS   = 2080
MAX_CLIENTS         = 256
MAX_EDICTS          = 1024
MAX_STATS           = 32
CLIENTNUM_NONE      = 255
SVCMD_BITS          = 5
SVCMD_MASK          = (1 << SVCMD_BITS) - 1

# MVD command IDs
MVD_NOP            = 1
MVD_SERVERDATA     = 4
MVD_CONFIGSTRING   = 5
MVD_FRAME          = 6
MVD_UNICAST        = 8
MVD_UNICAST_R      = 9
MVD_MULTICAST_ALL  = 10
MVD_MULTICAST_PHS  = 11
MVD_MULTICAST_PVS  = 12
MVD_MULTICAST_ALL_R = 13
MVD_MULTICAST_PHS_R = 14
MVD_MULTICAST_PVS_R = 15
MVD_SOUND          = 16
MVD_PRINT          = 17

# Player-state delta bits (P_*)
P_TYPE        = 1 << 0
P_ORIGIN      = 1 << 1   # x, y  as int16
P_ORIGIN2     = 1 << 2   # z     as int16
P_VIEWOFFSET  = 1 << 3   # 3×int8
P_VIEWANGLES  = 1 << 4   # yaw, pitch  as int16
P_VIEWANGLE2  = 1 << 5   # roll  as int16
P_KICKANGLES  = 1 << 6   # 3×int8
P_BLEND       = 1 << 7   # 4×uint8
P_FOV         = 1 << 8   # uint8
P_WEAPONINDEX = 1 << 9   # uint8
P_WEAPONFRAME = 1 << 10  # uint8
P_GUNOFFSET   = 1 << 11  # 3×int8
P_GUNANGLES   = 1 << 12  # 3×int8
P_RDFLAGS     = 1 << 13  # uint8
P_STATS       = 1 << 14  # statbits (uint32) + variable int16s
P_REMOVE      = 1 << 15  # player disconnect

# Entity-delta bits (E_*)
E_ORIGIN1  = 1 << 0;  E_ORIGIN2  = 1 << 1;  E_ORIGIN3  = 1 << 9
E_ANGLE1   = 1 << 10; E_ANGLE2   = 1 << 2;  E_ANGLE3   = 1 << 3
E_FRAME8   = 1 << 4;  E_FRAME16  = 1 << 17; E_EVENT    = 1 << 5
E_REMOVE   = 1 << 6;  E_MOREBITS1 = 1 << 7; E_MOREBITS2 = 1 << 15
E_MOREBITS3 = 1 << 23; E_NUMBER16 = 1 << 8
E_MODEL    = 1 << 11; E_MODEL2   = 1 << 20; E_MODEL3   = 1 << 21; E_MODEL4 = 1 << 22
E_SKIN8    = 1 << 16; E_SKIN16   = 1 << 25
E_EFFECTS8 = 1 << 14; E_EFFECTS16 = 1 << 19
E_RENDERFX8 = 1 << 12; E_RENDERFX16 = 1 << 18
E_OLDORIGIN = 1 << 24; E_SOUND = 1 << 26; E_SOLID = 1 << 27
E_FRAME32  = E_FRAME8  | E_FRAME16
E_SKIN32   = E_SKIN8   | E_SKIN16
E_EFFECTS32 = E_EFFECTS8 | E_EFFECTS16
E_RENDERFX32 = E_RENDERFX8 | E_RENDERFX16

# Coordinate / angle conversion
COORD_SCALE = 1.0 / 8.0
ANGLE_SCALE = 360.0 / 65536.0

# Configstring index constants (from q_shared.h)
# Note: q2pro/aqtion uses CS_MODELS=33 for the actual world BSP (not 32).
# We scan a small window around the nominal value to be safe.
CS_MODELS      = 32
CS_MODELS_SCAN = range(32, 40)   # first slot in this range containing maps/*.bsp is the world
CS_PLAYERSKINS = 1312   # CS_ITEMS + MAX_ITEMS

# SVC opcodes (Q2 protocol 34 / 37) — used inside unicast payloads
SVC_MUZZLEFLASH  = 1
SVC_LAYOUT       = 4
SVC_INVENTORY    = 5
SVC_SOUND        = 9
SVC_PRINT        = 10
SVC_STUFFTEXT    = 11
SVC_CONFIGSTRING = 13
SVC_CENTERPRINT  = 15

# Print levels
PRINT_LOW    = 0
PRINT_MEDIUM = 1   # kill announcements
PRINT_HIGH   = 2   # per-player messages ("You hit X in Y")
PRINT_CHAT   = 3

# Hit-location tokens → canonical labels
_LOC_HEAD_KEYS    = ('eyes', 'makeover', 'brains', 'trepanned', 'throat',
                     'hole in his head', 'hole in her head', 'hole in its head',
                     'scope', 'forehead', 'caught a sniper bullet')
_LOC_STOMACH_KEYS = ('stomach', 'upset stomach', 'lunch', 'pepto',
                     'gutted', 'contents of', 'kidneys',
                     'sniped in the stomach')
_LOC_LEGS_KEYS    = ('legs', 'legless', 'shorter',
                     'legs blown off', 'legs cut off',
                     'shot in the legs')
_LOC_CHEST_KEYS   = ('heart burn', 'heart surgery', 'chest pain',
                     'ribs', 'open heart', 'picked off',
                     'chest organ', 'vital organ',
                     'full of buckshot', 'hole-y matrimony',
                     'john woo')

# Regex for "You hit PLAYER in the PART" (level-2 messages to attacker)
_YOU_HIT_RE = re.compile(r'^You hit (.+?) in the (\w+)', re.IGNORECASE)

# Award centerprint patterns (CenterPrintAll broadcasts via SVC_CENTERPRINT)
#   IMPRESSIVE {player}!          – every 5 consecutive kills
#   ACCURACY {player}!            – every 3 consecutive headshots
#   EXCELLENT {player} ({n}x)!    – every 12 consecutive kills
_IMPRESSIVE_RE = re.compile(r'^IMPRESSIVE (.+?)!$')
_ACCURACY_RE   = re.compile(r'^ACCURACY (.+?)!$')
_EXCELLENT_RE  = re.compile(r'^EXCELLENT (.+?) \((\d+)x\)!$')
_ROUND_WIN_RE  = re.compile(r'^(Team \d+) won!', re.IGNORECASE)
_TIE_RE        = re.compile(r'it was a tie', re.IGNORECASE)
_CURR_SCORE_RE = re.compile(r'Current score is (Team \d+): (\d+) to (Team \d+): (\d+)', re.IGNORECASE)

# ── Bit-reader ─────────────────────────────────────────────────────────────────

class Msg:
    """Lightweight cursor-based binary reader for one MVD message block."""

    __slots__ = ('_d', '_p')

    def __init__(self, data: bytes):
        self._d = data
        self._p = 0

    @property
    def remaining(self) -> int:
        return len(self._d) - self._p

    def u8(self) -> int:
        v = self._d[self._p]; self._p += 1; return v

    def i8(self) -> int:
        v = struct.unpack_from('b', self._d, self._p)[0]; self._p += 1; return v

    def u16(self) -> int:
        v = struct.unpack_from('<H', self._d, self._p)[0]; self._p += 2; return v

    def i16(self) -> int:
        v = struct.unpack_from('<h', self._d, self._p)[0]; self._p += 2; return v

    def u32(self) -> int:
        v = struct.unpack_from('<I', self._d, self._p)[0]; self._p += 4; return v

    def skip(self, n: int):
        self._p += n

    def string(self) -> str:
        start = self._p
        d = self._d
        while self._p < len(d) and d[self._p]:
            self._p += 1
        s = d[start:self._p].decode('latin-1', errors='replace')
        self._p += 1      # null terminator
        return s

    def blob(self) -> bytes:
        n = self.u8()
        if n == 0:
            return b''
        b = self._d[self._p:self._p + n]
        self._p += n
        return b

    def read(self, n: int) -> bytes:
        b = self._d[self._p:self._p + n]
        self._p += n
        return b


# ── Parsers ────────────────────────────────────────────────────────────────────

def _read_player(msg: Msg) -> Optional[dict]:
    """Read one player-state delta.  Returns None when CLIENTNUM_NONE is seen."""
    num = msg.u8()
    if num == CLIENTNUM_NONE:
        return None

    bits = msg.u16()
    p: dict = {'n': num, 'bits': bits}

    if bits & P_TYPE:       msg.u8()
    if bits & P_ORIGIN:     p['ox'] = msg.i16(); p['oy'] = msg.i16()
    if bits & P_ORIGIN2:    p['oz'] = msg.i16()
    if bits & P_VIEWOFFSET: msg.skip(3)
    if bits & P_VIEWANGLES:
        msg.skip(2)                    # pitch (viewangles[0])
        p['yaw'] = msg.i16()           # yaw   (viewangles[1])
    if bits & P_VIEWANGLE2: msg.skip(2)
    if bits & P_KICKANGLES: msg.skip(3)
    if bits & P_WEAPONINDEX: p['weapon'] = msg.u8()
    if bits & P_WEAPONFRAME: msg.u8()
    if bits & P_GUNOFFSET:  msg.skip(3)
    if bits & P_GUNANGLES:  msg.skip(3)
    if bits & P_BLEND:      msg.skip(4)
    if bits & P_FOV:        msg.u8()
    if bits & P_RDFLAGS:    msg.u8()
    if bits & P_STATS:
        statbits = msg.u32()
        for i in range(MAX_STATS):
            if statbits & (1 << i):
                msg.skip(2)
    return p


def _skip_entity(msg: Msg) -> bool:
    """Consume one entity delta.  Returns False if entity number was 0 (end-list)."""
    bits = msg.u8()
    if bits & E_MOREBITS1:  bits |= msg.u8() << 8
    if bits & E_MOREBITS2:  bits |= msg.u8() << 16
    if bits & E_MOREBITS3:  bits |= msg.u8() << 24

    num = msg.u16() if (bits & E_NUMBER16) else msg.u8()
    if num == 0:
        return False

    if bits & E_REMOVE:
        return True

    if bits & E_MODEL:  msg.u8()
    if bits & E_MODEL2: msg.u8()
    if bits & E_MODEL3: msg.u8()
    if bits & E_MODEL4: msg.u8()

    fb = bits & E_FRAME32
    if   fb == E_FRAME32: pass           # not supported, but skip nothing extra
    elif fb == E_FRAME16: msg.skip(2)
    elif fb == E_FRAME8:  msg.u8()

    sb = bits & E_SKIN32
    if   sb == E_SKIN32: msg.skip(4)
    elif sb == E_SKIN16: msg.skip(2)
    elif sb == E_SKIN8:  msg.u8()

    eb = bits & E_EFFECTS32
    if   eb == E_EFFECTS32: msg.skip(4)
    elif eb == E_EFFECTS16: msg.skip(2)
    elif eb == E_EFFECTS8:  msg.u8()

    rb = bits & E_RENDERFX32
    if   rb == E_RENDERFX32: msg.skip(4)
    elif rb == E_RENDERFX16: msg.skip(2)
    elif rb == E_RENDERFX8:  msg.u8()

    if bits & E_ORIGIN1:   msg.skip(2)
    if bits & E_ORIGIN2:   msg.skip(2)
    if bits & E_ORIGIN3:   msg.skip(2)
    if bits & E_ANGLE1:    msg.u8()
    if bits & E_ANGLE2:    msg.u8()
    if bits & E_ANGLE3:    msg.u8()
    if bits & E_OLDORIGIN: msg.skip(6)
    if bits & E_SOUND:     msg.u8()
    if bits & E_EVENT:     msg.u8()
    if bits & E_SOLID:     msg.skip(2)
    return True


# ── Event helpers ──────────────────────────────────────────────────────────────

def _infer_location(text: str) -> str:
    t = text.lower()
    if any(k in t for k in _LOC_HEAD_KEYS):    return 'head'
    if any(k in t for k in _LOC_STOMACH_KEYS): return 'stomach'
    if any(k in t for k in _LOC_LEGS_KEYS):    return 'legs'
    if any(k in t for k in _LOC_CHEST_KEYS):   return 'chest'
    return 'unknown'


def _infer_weapon(text: str) -> str:
    # Unambiguous weapon name suffixes
    if 'M4 Assault Rifle'    in text: return 'M4'
    if 'M3 Super 90'         in text: return 'M3'
    if 'hole-y matrimony'    in text: return 'M3'   # M3 random message 1
    t = text.lower()
    # HC must come before generic 'buckshot' — HC double-barrel also uses that word
    if 'handcannon'          in t:    return 'HC'
    if 'sawed'               in t:    return 'HC'   # 'sawed-off' / 'sawed off shotgun'
    if 'minch'               in t:    return 'HC'
    if 'metal detector'      in t:    return 'HC'   # HC single-barrel message
    if 'buckshot'            in t:    return 'M3'   # M3 random message 2 (HC caught above)
    # Sniper — explicit bullet identifier only; heuristics ('picked off', 'shot in
    # the legs') are handled further down, after explicit weapon names are checked
    if 'sniper bullet'       in text: return 'SR'
    # MP5
    if 'MP5'                 in text: return 'MP5'
    # Dual MK23 — check before generic 'Mark 23' AND before sniper heuristics
    # (Dual LOC_LDAM uses 'shot in the legs' which looks like a sniper message)
    if 'akimbo'              in text: return 'Dual MK23'
    if 'trepanned'           in t:    return 'Dual MK23'
    if 'john woo'            in t:    return 'Dual MK23'
    if 'pair of mark 23'     in t:    return 'Dual MK23'
    # MK23
    if 'Mark 23'             in text: return 'MK23'
    if 'pistol round'        in text: return 'MK23'
    # Knife Thrown (flying) before slashing Knife
    if 'flying knife'        in text: return 'Knife Thrown'
    # Knife (slashing) — messages that lack an explicit weapon-name suffix
    if 'Combat Knife'        in text: return 'Knife'
    if 'throat slit'         in t:    return 'Knife'
    if 'gutted'              in t:    return 'Knife'
    if 'open heart surgery'  in t:    return 'Knife'
    if 'stabbed'             in t:    return 'Knife'
    if 'slashed'             in t:    return 'Knife'
    # Grenade
    if 'grenade'             in t:    return 'Grenade'
    # Sniper heuristics — only reached when no explicit weapon suffix was present
    if 'sniped'              in t:    return 'SR'
    if 'picked off'          in t:    return 'SR'
    if 'shot in the legs'    in t:    return 'SR'  # MOD_SNIPER LOC_LDAM (no suffix)
    if 'boot'                in t:    return 'Kick'
    if 'ass kicked'          in t:    return 'Kick'
    if 'bruce lee'           in t:    return 'Kick'
    if 'taught how to fly'   in t:    return 'Kick'  # MOD_FALLING with push attacker
    # Punch (distinct from Kick — MOD_PUNCH = 14)
    if 'facelift'            in t:    return 'Punch'
    if 'knocked out'         in t:    return 'Punch'
    if 'iron fist'           in text: return 'Punch'
    if 'grapple'             in t:    return 'Grapple'
    return 'unknown'


def _parse_kill_message(text: str, player_names: dict) -> Optional[dict]:
    """
    Parse a level-1 (PRINT_MEDIUM) death message.
    Kill message format (from p_client.c):
      sprintf(death_msg, "%s%s %s%s\n", victim, middle, killer, suffix)
    Returns {killer, victim, location, weapon} or None.
    """
    names = list(player_names.values())
    if len(names) < 2:
        return None
    text = text.strip()
    for victim in names:
        if not (text.startswith(victim + ' ') or text.startswith(victim + "'")):
            continue
        rest = text[len(victim):]
        for killer in names:
            if killer == victim:
                continue
            if (f' {killer}' in rest or
                    f' {killer}.' in rest or
                    rest.endswith(f' {killer}')):
                return {
                    'killer':   killer,
                    'victim':   victim,
                    'location': _infer_location(rest),
                    'weapon':   _infer_weapon(text),
                }
    return None


def _parse_svc_stream(payload: bytes, clientnum: int, frame: int,
                      kills: list, hit_events: list, award_events: list,
                      player_names: dict):
    """Walk an SVC byte stream extracted from a unicast payload."""
    d   = payload
    pos = 0

    def read_str(p: int):
        end = d.find(0, p)
        if end == -1:
            end = len(d)
        s = d[p:end].decode('latin-1', errors='replace')
        return s, end + 1

    while pos < len(d):
        try:
            svc = d[pos]; pos += 1

            if svc == SVC_PRINT:
                level    = d[pos]; pos += 1
                msg, pos = read_str(pos)
                if level == PRINT_MEDIUM:
                    evt = _parse_kill_message(msg, player_names)
                    if evt:
                        evt['frame'] = frame
                        kills.append(evt)
                elif level == PRINT_HIGH:
                    m = _YOU_HIT_RE.match(msg)
                    if m:
                        part = m.group(2).lower()
                        if   'leg'     in part: loc = 'legs'
                        elif 'stomach' in part: loc = 'stomach'
                        elif 'body'    in part: loc = 'chest'   # LOC_CDAM fallback
                        elif 'head'    in part: loc = 'head'
                        elif 'chest'   in part: loc = 'chest'
                        else:                   loc = 'unknown'
                        hit_events.append({
                            'frame':    frame,
                            'attacker': clientnum,
                            'victim':   m.group(1),
                            'location': loc,
                        })

            elif svc == SVC_CENTERPRINT:
                msg_text, pos = read_str(pos)
                t = msg_text.strip()
                m = _IMPRESSIVE_RE.match(t)
                if m:
                    award_events.append({'frame': frame, 'player': m.group(1), 'award': 'Impressive'})
                else:
                    m = _ACCURACY_RE.match(t)
                    if m:
                        award_events.append({'frame': frame, 'player': m.group(1), 'award': 'Accuracy'})
                    else:
                        m = _EXCELLENT_RE.match(t)
                        if m:
                            award_events.append({'frame': frame, 'player': m.group(1),
                                                 'award': 'Excellent', 'count': int(m.group(2))})

            elif svc == SVC_LAYOUT:
                _msg, pos = read_str(pos)

            elif svc == SVC_STUFFTEXT:
                _msg, pos = read_str(pos)

            elif svc == SVC_MUZZLEFLASH:
                pos += 3   # int16 entity + uint8 weapon_byte

            elif svc == SVC_CONFIGSTRING:
                pos += 2   # uint16 index
                _v, pos = read_str(pos)

            elif svc == SVC_INVENTORY:
                pos += 512  # 256 × uint16

            else:
                break       # unknown opcode – abandon this block

        except (IndexError, UnicodeDecodeError):
            break


def _parse_frame(msg: Msg, state: dict) -> dict:
    """
    Parse one MVD frame block.

    state  – mutable dict keyed by client_num; each value holds the last
             known {ox, oy, oz, yaw, weapon}.  Delta-updates are applied here.

    Returns snapshot dict  {client_num: {x, y, z, yaw}}.
    """
    msg.blob()   # portalbits (ignored)

    snapshot: dict = {}

    # Players
    while True:
        p = _read_player(msg)
        if p is None:
            break
        n = p['n']
        if p['bits'] & P_REMOVE:
            state.pop(n, None)
            continue
        s = state.setdefault(n, {})
        if 'ox' in p: s['ox'] = p['ox']
        if 'oy' in p: s['oy'] = p['oy']
        if 'oz' in p: s['oz'] = p['oz']
        if 'yaw' in p: s['yaw'] = p['yaw']
        if 'weapon' in p: s['weapon'] = p['weapon']

    for n, s in state.items():
        snapshot[n] = {
            'x': round(s.get('ox', 0) * COORD_SCALE, 1),
            'y': round(s.get('oy', 0) * COORD_SCALE, 1),
            'z': round(s.get('oz', 0) * COORD_SCALE, 1),
            'a': round(s.get('yaw', 0) * ANGLE_SCALE % 360, 1),
        }

    # Entities (skip all)
    try:
        while _skip_entity(msg):
            pass
    except Exception:
        pass

    return snapshot


def _parse_multicast(msg: Msg, cmd: int, extra: int, frame: int,
                     award_events: list, round_events: list,
                     muzzle_flashes: list):
    """Read a multicast block and scan for SVC_CENTERPRINT award announces
    and SVC_PRINT round-win / current-score messages."""
    length = msg.u8() | (extra << 8)
    if (cmd - MVD_MULTICAST_ALL) % 3 != 0:   # PHS / PVS flavours carry a leafnum
        msg.skip(2)
    payload = msg.read(length)
    # Scan for SVC_CENTERPRINT in the payload (awards only, no kills/hits in multicasts)
    d   = payload
    pos = 0

    def read_str(p: int):
        end = d.find(0, p)
        if end == -1:
            end = len(d)
        s = d[p:end].decode('latin-1', errors='replace')
        return s, end + 1

    while pos < len(d):
        try:
            svc = d[pos]; pos += 1
            if svc == SVC_CENTERPRINT:
                t, pos = read_str(pos)
                t = t.strip()
                m = _IMPRESSIVE_RE.match(t)
                if m:
                    award_events.append({'frame': frame, 'player': m.group(1), 'award': 'Impressive'})
                else:
                    m = _ACCURACY_RE.match(t)
                    if m:
                        award_events.append({'frame': frame, 'player': m.group(1), 'award': 'Accuracy'})
                    else:
                        m = _EXCELLENT_RE.match(t)
                        if m:
                            award_events.append({'frame': frame, 'player': m.group(1),
                                                 'award': 'Excellent', 'count': int(m.group(2))})
            elif svc == SVC_PRINT:
                pos += 1          # level byte
                t, pos = read_str(pos)
                t = t.strip()
                mw = _ROUND_WIN_RE.match(t)
                if mw:
                    round_events.append({'type': 'win', 'team_name': mw.group(1), 'frame': frame})
                else:
                    ms = _CURR_SCORE_RE.search(t)
                    if ms:
                        round_events.append({
                            'type':       'score',
                            'team1_name': ms.group(1), 'score1': int(ms.group(2)),
                            'team2_name': ms.group(3), 'score2': int(ms.group(4)),
                            'frame':      frame,
                        })
            elif svc == SVC_STUFFTEXT:
                _, pos = read_str(pos)
            elif svc == SVC_LAYOUT:
                _, pos = read_str(pos)
            elif svc == SVC_MUZZLEFLASH:
                if pos + 3 > len(d):
                    break
                entity_num = d[pos] | (d[pos + 1] << 8); pos += 2
                mz_byte    = d[pos] & 0x7f;               pos += 1  # strip MZ_SILENCED
                client_num = entity_num - 1
                if client_num >= 0:
                    muzzle_flashes.append({'frame': frame, 'client': client_num, 'mz': mz_byte})
            elif svc == SVC_SOUND:
                sb = d[pos]; pos += 1
                pos += 1                      # index
                if sb & 1:  pos += 1          # volume
                if sb & 2:  pos += 1          # attenuation
                if sb & 16: pos += 1          # offset
                pos += 2                      # channel word
            else:
                break   # unknown — stop scanning this multicast
        except (IndexError, UnicodeDecodeError):
            break


def _parse_unicast(msg: Msg, extra: int, frame_idx: int,
                   kills: list, hit_events: list, award_events: list,
                   player_names: dict):
    # The length byte encodes the SVC payload size (msg_write.cursize).
    # clientNum is a separate byte written AFTER the length, NOT counted in it.
    length    = msg.u8() | (extra << 8)
    clientnum = msg.u8()
    payload   = msg.read(length)
    _parse_svc_stream(payload, clientnum, frame_idx, kills, hit_events,
                      award_events, player_names)


def _is_world_bsp(idx: int, val: str) -> bool:
    """True if configstring at idx looks like the world BSP model."""
    return idx in CS_MODELS_SCAN and val.startswith('maps/') and val.endswith('.bsp')


def _parse_skin_configstring(pnum: int, value: str,
                             player_names: dict, player_teams: dict) -> None:
    """Extract name and team from a CS_PLAYERSKINS configstring value.
    Format: name\\model/skin  where skin suffix ctf_r→team1, ctf_b→team2.
    """
    if not value:
        return
    parts = value.split('\\')
    player_names[pnum] = parts[0]
    if len(parts) > 1:
        skin = parts[1].split('/')[-1] if '/' in parts[1] else parts[1]
        skin_lo = skin.lower()
        if 'ctf_r' in skin_lo or skin_lo.endswith('_r'):
            player_teams[pnum] = 1
        elif 'ctf_b' in skin_lo or skin_lo.endswith('_b'):
            player_teams[pnum] = 2


def _read_configstring(msg: Msg, configstrings: dict,
                       player_names: dict, player_teams: dict) -> Optional[str]:
    """Read one configstring and update tracking dicts.  Returns the map name if found."""
    idx = msg.u16()
    if idx >= MAX_CONFIGSTRINGS:
        return None
    value = msg.string()
    configstrings[idx] = value

    if _is_world_bsp(idx, value):
        bsp = value.split('/')[-1]
        return bsp[:-4]
    elif CS_PLAYERSKINS <= idx < CS_PLAYERSKINS + MAX_CLIENTS:
        _parse_skin_configstring(idx - CS_PLAYERSKINS, value, player_names, player_teams)
    return None


# ── Public API ─────────────────────────────────────────────────────────────────

def parse_mvd2(data: bytes, max_frames: int = 0) -> dict:
    """
    Parse a raw (or gzip-compressed) MVD2 bytestring.

    Returns:
      map_name       str
      player_names   {client_num: name}
      frame_interval float   (seconds per frame, nominally 0.1)
      frames         list of {t: frame_index, players: {num: {x,y,z,a}}}
    """
    if data[:2] == b'\x1f\x8b':
        data = gzip.decompress(data)
    if data[:4] != MVD_MAGIC:
        raise ValueError("Not a valid MVD2 file")

    pos           = 4
    configstrings: dict  = {}
    player_names:  dict  = {}    # pnum → name
    player_teams:  dict  = {}    # pnum → 1 or 2
    state:         dict  = {}    # client_num → last known position
    map_name:      Optional[str] = None
    frames:        list  = []
    kills:         list  = []
    hit_events:    list  = []
    award_events:  list  = []
    round_events:  list  = []     # {'type': 'win'/'score', ...}
    round_start_frames: list = []
    muzzle_flashes:      list = []
    frame_idx            = 0

    while pos + 2 <= len(data):
        msglen = struct.unpack_from('<H', data, pos)[0]
        pos += 2
        if msglen == 0:
            break
        if pos + msglen > len(data):
            break

        block = data[pos: pos + msglen]
        pos  += msglen
        msg   = Msg(block)

        while msg.remaining > 0:
            try:
                cb    = msg.u8()
                extra = cb >> SVCMD_BITS
                cmd   = cb & SVCMD_MASK

                if cmd == MVD_NOP:
                    pass

                elif cmd == MVD_SERVERDATA:
                    _major = msg.u32()   # 37
                    _minor = msg.u16()
                    _cnt   = msg.u32()
                    _gdir  = msg.string()
                    _cnum  = msg.i16()
                    # Configstrings until sentinel MAX_CONFIGSTRINGS
                    while True:
                        idx = msg.u16()
                        if idx >= MAX_CONFIGSTRINGS:
                            break
                        val = msg.string()
                        configstrings[idx] = val
                        if _is_world_bsp(idx, val):
                            map_name = val.split('/')[-1][:-4]
                        elif CS_PLAYERSKINS <= idx < CS_PLAYERSKINS + MAX_CLIENTS:
                            _parse_skin_configstring(idx - CS_PLAYERSKINS, val,
                                                     player_names, player_teams)
                    # Base frame
                    snap = _parse_frame(msg, state)
                    frames.append({'t': frame_idx, 'players': snap})
                    frame_idx += 1

                elif cmd == MVD_CONFIGSTRING:
                    mn = _read_configstring(msg, configstrings,
                                            player_names, player_teams)
                    if mn:
                        map_name = mn

                elif cmd == MVD_FRAME:
                    prev_keys = set(state.keys())
                    snap = _parse_frame(msg, state)
                    curr_keys = set(state.keys())
                    # A named player respawning (key returns after P_REMOVE) = new round
                    named_nums = set(player_names.keys())
                    if (curr_keys - prev_keys) & named_nums and frame_idx > 0:
                        round_start_frames.append(frame_idx)
                    frames.append({'t': frame_idx, 'players': snap})
                    frame_idx += 1
                    if max_frames and frame_idx >= max_frames:
                        return _build(map_name, player_names, player_teams, frames,
                                      frame_idx, kills, hit_events, award_events,
                                      round_start_frames, round_events, muzzle_flashes)

                elif MVD_MULTICAST_ALL <= cmd <= MVD_MULTICAST_PVS_R:
                    _parse_multicast(msg, cmd, extra, frame_idx, award_events, round_events, muzzle_flashes)

                elif cmd in (MVD_UNICAST, MVD_UNICAST_R):
                    _parse_unicast(msg, extra, frame_idx,
                                   kills, hit_events, award_events, player_names)

                elif cmd == MVD_SOUND:
                    sb = msg.u8()
                    msg.u8()                     # index
                    if sb & 1:  msg.u8()         # volume
                    if sb & 2:  msg.u8()         # attenuation
                    if sb & 16: msg.u8()         # offset
                    msg.skip(2)                  # channel word

                elif cmd == MVD_PRINT:
                    msg.u8()               # print level (ignored for routing)
                    txt = msg.string().strip()
                    mw = _ROUND_WIN_RE.match(txt)
                    if mw:
                        round_events.append({'type': 'win', 'team_name': mw.group(1),
                                             'frame': frame_idx})
                    elif _TIE_RE.search(txt):
                        round_events.append({'type': 'tie', 'frame': frame_idx})
                    else:
                        ms = _CURR_SCORE_RE.search(txt)
                        if ms:
                            round_events.append({
                                'type':       'score',
                                'team1_name': ms.group(1), 'score1': int(ms.group(2)),
                                'team2_name': ms.group(3), 'score2': int(ms.group(4)),
                                'frame':      frame_idx,
                            })

                else:
                    break   # unknown cmd – abandon this block

            except Exception:
                break   # corrupt / truncated block, move to next

    # Deduplicate kills: the same kill is unicast to all connected clients
    # (victim, killer, spectators) in the same frame, producing identical entries.
    seen: set = set()
    unique_kills: list = []
    for k in kills:
        key = (k['killer'], k['victim'], k['frame'])
        if key not in seen:
            seen.add(key)
            unique_kills.append(k)
    kills = unique_kills

    # Deduplicate awards: CenterPrintAll sends SVC_CENTERPRINT to every client
    # and appears in every unicast block for that frame → N copies per event.
    # Also appears at most once in a multicast block.  Deduplicate by (award, player, frame).
    seen_awards: set = set()
    unique_awards: list = []
    for a in award_events:
        key = (a['award'], a['player'], a['frame'])
        if key not in seen_awards:
            seen_awards.add(key)
            unique_awards.append(a)
    award_events = unique_awards

    return _build(map_name, player_names, player_teams, frames, frame_idx,
                  kills, hit_events, award_events, round_start_frames, round_events,
                  muzzle_flashes)


def _cluster_rounds(raw_frames: list, gap: int = 150, min_cluster: int = 2) -> list:
    """Collapse individual player-respawn frame indices into true round boundaries.

    In pickup/CTF a real round boundary is a *mass* respawn — multiple players
    reappearing within a short window (``gap`` frames ≈ 15 s at 10 fps).
    Single-player respawns (individual deaths) are ignored.

    Returns a sorted list of frame indices, one per detected round start.
    """
    if not raw_frames:
        return []
    # Group consecutive frames that are within `gap` of each other
    clusters: list = []          # list of (first_frame, count)
    cluster_start = raw_frames[0]
    cluster_count = 1
    prev = raw_frames[0]
    for f in raw_frames[1:]:
        if f - prev <= gap:
            cluster_count += 1
        else:
            if cluster_count >= min_cluster:
                clusters.append(cluster_start)
            cluster_start = f
            cluster_count = 1
        prev = f
    if cluster_count >= min_cluster:
        clusters.append(cluster_start)
    return clusters


def _compute_round_wins(kills: list, round_starts: list,
                        total_frames: int,
                        teams_by_name: dict) -> dict:
    """For each round interval, the team that made the *last* kill wins.

    Returns {1: N, 2: M} win counts.
    """
    wins: dict = {1: 0, 2: 0}
    if not kills or not round_starts:
        return wins
    boundaries = round_starts + [total_frames]
    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        end   = boundaries[i + 1]
        round_kills = [k for k in kills if start <= k['frame'] < end]
        if not round_kills:
            continue
        last = max(round_kills, key=lambda k: k['frame'])
        t = teams_by_name.get(last['killer'], 0)
        if t in wins:
            wins[t] += 1
    return wins


def _build(map_name, player_names, player_teams, frames, frame_count,
           kills=None, hit_events=None, award_events=None,
           round_start_frames=None, round_events=None,
           muzzle_flashes=None) -> dict:
    kills              = kills              or []
    hit_events         = hit_events         or []
    award_events       = award_events       or []
    round_start_frames = round_start_frames or []
    round_events       = round_events       or []

    # Use authoritative win/tie event frames as round boundaries when available
    # (finalstand: every round ends with a "Team X won!" or tie broadcast).
    # Fall back to respawn-based clustering for pickup games.
    auth_boundaries = sorted(ev['frame'] for ev in round_events if ev['type'] in ('win', 'tie'))
    if auth_boundaries:
        round_starts = auth_boundaries
    else:
        round_starts = _cluster_rounds(round_start_frames)

    # Strip the demo-recorder pseudo-client — it is never a real player.
    player_names = {k: v for k, v in player_names.items() if v != '[MVDSPEC]'}

    # Ghost client detection: client nums that appear in frames but have no skin configstring
    all_frame_nums: set = set()
    for fr in frames:
        all_frame_nums.update(fr['players'].keys())
    ghost_clients = sorted(all_frame_nums - set(player_names.keys()))

    # Map player_teams from pnum→team to name→team
    teams_by_name = {player_names[n]: t for n, t in player_teams.items() if n in player_names}

    # Team scores: total kills made by each team
    team_scores = {1: 0, 2: 0, 0: 0}
    for k in kills:
        kname = k['killer']
        t = teams_by_name.get(kname, 0)
        team_scores[t] = team_scores.get(t, 0) + 1

    # Round wins: authoritative source = "Team X won!" broadcasts in the file
    _team_num = lambda name: int(re.search(r'(\d+)', name).group(1)) if re.search(r'(\d+)', name) else 0
    round_wins: dict = {1: 0, 2: 0}
    round_ties: int  = 0
    for ev in round_events:
        if ev['type'] == 'win':
            t = _team_num(ev['team_name'])
            if t in round_wins:
                round_wins[t] += 1
        elif ev['type'] == 'tie':
            round_ties += 1

    # If the file contains authoritative "Current score" messages, the last one
    # gives the definitive per-team kill total — use it to override the kill-count.
    score_events = [ev for ev in round_events if ev['type'] == 'score']
    if score_events:
        last_score = max(score_events, key=lambda e: e['frame'])
        t1 = _team_num(last_score['team1_name'])
        t2 = _team_num(last_score['team2_name'])
        if t1 and t2:
            team_scores[t1] = last_score['score1']
            team_scores[t2] = last_score['score2']

    # Tally per-player kills and deaths
    kill_counts  = {n: 0 for n in player_names.values()}
    death_counts = {n: 0 for n in player_names.values()}
    for k in kills:
        kill_counts [k['killer']] = kill_counts .get(k['killer'], 0) + 1
        death_counts[k['victim']] = death_counts.get(k['victim'], 0) + 1

    # Weapon distribution
    weapon_counts: dict = {}
    for k in kills:
        weapon_counts[k['weapon']] = weapon_counts.get(k['weapon'], 0) + 1

    # Hit-location distribution
    loc_counts: dict = {}
    for h in hit_events:
        loc_counts[h['location']] = loc_counts.get(h['location'], 0) + 1

    # Kill-shot location distribution (final hit that got the kill)
    kill_loc_counts: dict = {}
    for k in kills:
        kill_loc_counts[k['location']] = kill_loc_counts.get(k['location'], 0) + 1

    # Per-player award counts  {player: {Impressive: N, Accuracy: N, Excellent: N}}
    award_counts: dict = {}
    for a in award_events:
        p = a['player']
        if p not in award_counts:
            award_counts[p] = {'Impressive': 0, 'Accuracy': 0, 'Excellent': 0}
        award_counts[p][a['award']] = award_counts[p].get(a['award'], 0) + 1

    # Per-player hits landed {player_name: total} and per-location breakdown
    hit_counts: dict = {}
    hit_loc_by_player: dict = {}
    for h in hit_events:
        name = player_names.get(h['attacker'], str(h['attacker']))
        hit_counts[name] = hit_counts.get(name, 0) + 1
        ploc = hit_loc_by_player.setdefault(name, {})
        ploc[h['location']] = ploc.get(h['location'], 0) + 1

    # ── True shots fired (from SVC_MUZZLEFLASH multicast events) ────────────
    # Wire MZ codes: 1=MK23/MP5/M4/Dual  2=M3  13=HC  14=Sniper
    MZ_SHOTGUN_WIRE  = 2
    MZ_HC_WIRE       = 13
    MZ_SNIPER_WIRE   = 14

    shots_fired: dict = {}    # name → total shots
    shots_by_mz: dict = {}    # name → {mz: count}
    name_by_client = {v: k for k, v in player_names.items()}  # unused here but useful
    for mf in (muzzle_flashes or []):
        pname = player_names.get(mf['client'])
        if not pname:
            continue
        shots_fired[pname] = shots_fired.get(pname, 0) + 1
        mz_map = shots_by_mz.setdefault(pname, {})
        mz = mf['mz']
        mz_map[mz] = mz_map.get(mz, 0) + 1

    # Accuracy: hits / shots as a percentage.
    # NOTE: M3 and HC (shotguns) do NOT emit "You hit" messages in T_Damage
    # (MOD_M3/MOD_HC fall into a branch with no gi.cprintf), so hit_counts for
    # shotgun-primary players would always be 0 — exclude them to avoid showing
    # a misleading 0% (or meaningless value).
    accuracy: dict = {}
    for name, shots in shots_fired.items():
        if not shots:
            continue
        mz_map    = shots_by_mz.get(name, {})
        total_mz  = sum(mz_map.values())
        sg_shots  = mz_map.get(MZ_SHOTGUN_WIRE, 0) + mz_map.get(MZ_HC_WIRE, 0)
        if total_mz >= 5 and sg_shots / total_mz > 0.5:
            continue  # shotgun primary — no reliable hit data
        hits = hit_counts.get(name, 0)
        accuracy[name] = min(100.0, round(hits / shots * 100, 1))

    # ── Damage model ─────────────────────────────────────────────────────────
    # Base damage per bullet/pellet (from p_weapon.c):
    #   MK23/M4/Dual=90  MP5=55  Sniper=250  M3=17/pellet  HC=18/pellet(avg)
    # Location multipliers (from g_combat.c T_Damage):
    #   head×1.8  chest×0.65  stomach×0.40  legs×0.25
    # Shotguns (M3/HC) have NO location multiplier — flat per pellet.
    _WEAPON_BASE = {
        'MK23': 90, 'Dual MK23': 90, 'M4': 90, 'MP5': 55,
        'SR': 250, 'Sniper': 250,
        'M3': 17, 'HC': 18,  # HC: avg of single(15) and dual(20)
        'Knife': 50, 'Knife Thrown': 50,
        'Grenade': 0, 'Kick': 0, 'Punch': 0, 'Grapple': 0,
    }
    _LOC_MULT = {'head': 1.8, 'chest': 0.65, 'stomach': 0.40, 'legs': 0.25, 'unknown': 0.55}
    _SHOTGUN_WEAPONS = {'M3', 'HC'}

    # Per-player weapon kill profile  {name: {weapon: count}}
    player_weapon_kills: dict = {}
    for k in kills:
        wd = player_weapon_kills.setdefault(k['killer'], {})
        wd[k['weapon']] = wd.get(k['weapon'], 0) + 1

    def _player_avg_base_dmg(name: str):
        """Return (avg_base_damage, is_shotgun) for a player."""
        # Use MZ-based shot counts if available, else fall back to kills
        mz_map = shots_by_mz.get(name, {})
        total_mz = sum(mz_map.values())

        shotgun_mz = mz_map.get(MZ_SHOTGUN_WIRE, 0)
        hc_mz      = mz_map.get(MZ_HC_WIRE, 0)
        sniper_mz  = mz_map.get(MZ_SNIPER_WIRE, 0)

        # Infer weapon from MZ if dominant shot type is clear
        if total_mz >= 5:
            if shotgun_mz / total_mz > 0.5:
                return 17, True   # M3
            if hc_mz / total_mz > 0.5:
                return 18, True   # HC
            if sniper_mz / total_mz > 0.5:
                return 250, False # Sniper

        # Fall back to kill-weapon distribution
        wkills = player_weapon_kills.get(name, {})
        total_k = sum(wkills.values())
        if not total_k:
            return 75, False  # fallback: typical body shot

        # Shotgun dominant via kills
        m3_k  = wkills.get('M3', 0)
        hc_k  = wkills.get('HC', 0)
        if m3_k / total_k > 0.5:
            return 17, True
        if hc_k / total_k > 0.5:
            return 18, True

        # Weighted average for bullet weapons
        weighted = sum(_WEAPON_BASE.get(w, 75) * c for w, c in wkills.items())
        return weighted / total_k, False

    damage_dealt: dict = {}
    for name, locs in hit_loc_by_player.items():
        avg_base, is_shotgun = _player_avg_base_dmg(name)
        total_dmg = 0
        for loc, cnt in locs.items():
            if is_shotgun:
                total_dmg += avg_base * cnt
            else:
                total_dmg += avg_base * _LOC_MULT.get(loc, 0.55) * cnt
        damage_dealt[name] = round(total_dmg)

    # Headshot kills per player (killing shots to the head)
    headshot_kills: dict = {}
    for k in kills:
        if k.get('location') == 'head':
            headshot_kills[k['killer']] = headshot_kills.get(k['killer'], 0) + 1

    return {
        'map':            map_name or 'unknown',
        'player_names':   player_names,
        'frame_count':    frame_count,
        'frame_interval': 0.1,
        'duration':       round(frame_count * 0.1, 1),
        'frames':         frames,
        # ── Event data ──────────────────────────────────────────
        'kills':          kills,
        'hit_events':     hit_events,
        'award_events':   award_events,
        'kill_counts':    kill_counts,
        'death_counts':   death_counts,
        'weapon_counts':  weapon_counts,
        'loc_counts':     loc_counts,
        'kill_loc_counts': kill_loc_counts,
        'award_counts':   award_counts,
        'hit_counts':         hit_counts,
        'hit_loc_by_player':  hit_loc_by_player,
        'shots_fired':        shots_fired,
        'accuracy':           accuracy,
        'damage_dealt':       damage_dealt,
        'headshot_kills':     headshot_kills,
        'player_teams':       teams_by_name,   # {name: 1 or 2}
        'team_scores':        {str(k): v for k, v in team_scores.items() if k != 0},
        'round_wins':         {str(k): v for k, v in round_wins.items()},
        'round_ties':         round_ties,
        'ghost_clients':      ghost_clients,
        'round_start_frames': round_starts,
    }


def load_mvd2(path: str, max_frames: int = 0) -> dict:
    if path.endswith('.gz'):
        # Read in chunks so a truncated archive yields whatever data was
        # successfully decompressed rather than raising EOFError.
        chunks: list = []
        try:
            with gzip.open(path, 'rb') as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
        except EOFError:
            pass   # truncated file — use whatever we got
        data = b''.join(chunks)
        if not data:
            raise ValueError(f'Gzip file is empty or fully corrupt: {path}')
    else:
        with open(path, 'rb') as f:
            data = f.read()
    return parse_mvd2(data, max_frames)
