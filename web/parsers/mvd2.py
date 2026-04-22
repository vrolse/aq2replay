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
from bisect import bisect_right
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
CS_SOUNDS      = CS_MODELS + 256  # = 288; configstring[CS_SOUNDS + sound_index] = name
CS_IMAGES      = CS_SOUNDS + 256  # = 544; configstring[CS_IMAGES + image_index] = name
CS_PLAYERSKINS = 1312   # CS_ITEMS + MAX_ITEMS

# Item icon stem names (from Q2Pro pics/ registry) → item bitmask.
# STAT_ITEMS_ICON (stat 19) sends the CS_IMAGES index for the currently displayed item icon.
# Collecting these across frames gives us the set of items a player holds since last spawn.
_ITEM_ICON_STEMS: dict = {
    'i_jacketarmor': 1,   # kevlar vest
    'p_rebreather':  2,   # kevlar helmet
    'p_silencer':    4,   # silencer
    'p_bandolier':   8,   # bandolier
    'p_laser':       16,  # laser sight
}

# Sound-packet flags (SND_*) shared by SVC_SOUND and MVD_SOUND
SND_ENTITY  = 1 << 3   # channel word (entity*8 + channel) follows instead of xyz
# Quake 2 CHAN_* values (encoded in the low 3 bits of the channel word)
CHAN_WEAPON = 1

# Mapping from AQ2-TNG view-model directory name fragment → vwep model filename stem.
# Configstring[CS_MODELS + ps.gunindex] contains the view-model path; this table maps
# the directory component to the matching vwep model (players/male/<vwep>.md2).
_VIEW_TO_VWEP: dict = {
    'v_blast':  'w_mk23',
    'v_machn':  'w_mp5',
    'v_m4':     'w_m4',
    'v_shotg':  'w_super90',
    'v_cannon': 'w_cannon',
    'v_sniper': 'w_sniper',
    'v_dual':   'w_akimbo',
    'v_knife':  'w_knife',
    'v_handgr': 'a_m61frag',
}

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
# Kevlar armor messages sent to attacker when target has Kevlar Vest/Helmet
# e.g. "X has a Kevlar Vest - AIM FOR THE HEAD!" / "X has a Kevlar Helmet, too bad..."
_KVLR_RE    = re.compile(r'^(.+?) has a Kevlar (Vest|Helmet)', re.IGNORECASE)

# Award centerprint patterns (CenterPrintAll broadcasts via SVC_CENTERPRINT)
# Source strings (source/src/action/p_client.c::Announce_Reward):
#   IMPRESSIVE {player}! | IMPRESSIVE {player} ({n}x)!
#   ACCURACY {player}!   | ACCURACY {player} ({n}x)!
#   EXCELLENT {player} ({n}x)!
#   {player} IS DOMINATING!
#   {player} IS UNSTOPPABLE!
_IMPRESSIVE_RE  = re.compile(r'^IMPRESSIVE (.+?)(?: \((\d+)x\))?!$')
_ACCURACY_RE    = re.compile(r'^ACCURACY (.+?)(?: \((\d+)x\))?!$')
_EXCELLENT_RE   = re.compile(r'^EXCELLENT (.+?)(?: \((\d+)x\))?!$')
_DOMINATING_RE  = re.compile(r'^(.+?) IS DOMINATING!$', re.IGNORECASE)
_UNSTOPPABLE_RE = re.compile(r'^(.+?) IS UNSTOPPABLE!$', re.IGNORECASE)
_ROUND_WIN_RE  = re.compile(r'^(.+?)\s+won\s*!*\s*$', re.IGNORECASE)
_ROUND_TEAM_WINS_RE = re.compile(r'^(.+?)\s+team\s+wins\s*!*\s*$', re.IGNORECASE)
_TIE_RE        = re.compile(r'it was a tie', re.IGNORECASE)
_CURR_SCORE_RE = re.compile(
    r'Current score is\s+(.+?):\s*(\d+)\s+to\s+(.+?):\s*(\d+)(?:\s+to\s+(.+?):\s*(\d+))?',
    re.IGNORECASE,
)
_Q2_COLOR_TOKEN_RE = re.compile(r'\^(?:x[0-9a-fA-F]{3}|\d)')
_NON_PRINTABLE_RE = re.compile(r'[\x00-\x1f\x7f-\x9f]')
# Team chat wraps the sender name in parentheses: "(name): msg" or "[DEAD] (name): msg"
_TEAM_SAY_RE   = re.compile(r'^(\[DEAD\]\s*)?\(')
_TEAM_CHAT_PARSE_RE = re.compile(r'^\s*(\[DEAD\]\s*)?\((.+?)\)\s*:\s*(.*)$')
_PUBLIC_CHAT_PARSE_RE = re.compile(r'^\s*(\[DEAD\]\s*)?(.+?)\s*:\s*(.*)$')
_ROUND_LIGHTS_RE = re.compile(r'^LIGHTS[\s.!]*$', re.IGNORECASE)
_ROUND_CAMERA_RE = re.compile(r'^CAMERA[\s.!]*$', re.IGNORECASE)
_ROUND_ACTION_RE = re.compile(r'^ACTION[\s.!]*$', re.IGNORECASE)
_LAYOUT_ROW_RE = re.compile(
    r'^\s*(.*?)\s+(-?\d+)\s+(-?\d+)\s+(-?\d+)\s+(-?\d+)\s+(-?\d+)\s+(-?\d+)\s*$'
)
_TEAM_COL_TOKEN_RE = re.compile(r'^\d+[A-Z]?$')


def _normalize_round_print_text(text: str) -> str:
    """Normalize round/score print text across old AQ2 demo variants.

    Older demos can include Q2 color tokens/control chars and inconsistent
    spacing/punctuation around win/score broadcasts.
    """
    s = (text or '').replace('\r', ' ').replace('\n', ' ')
    s = _Q2_COLOR_TOKEN_RE.sub('', s)
    s = _NON_PRINTABLE_RE.sub('', s)
    s = ' '.join(s.split())
    return s.strip()


def _team_label_key(name: str) -> str:
    """Canonical key used for score/win team-name matching."""
    return _normalize_round_print_text(name).casefold()

# Suicide/world-death obituary suffixes from AQ2 TNG and base Q2 gamecode:
# - source/src/action/p_client.c (ClientObituary)
# - source/src/game/p_client.c   (ClientObituary)
_SUICIDE_MSG_EQ = {
    'tried to put the pin back in',
    "didn't throw his grenade far enough",
    "didn't throw her grenade far enough",
    "didn't throw its grenade far enough",
    'tripped on his own grenade',
    'tripped on her own grenade',
    'tripped on its own grenade',
    'blew himself up',
    'blew herself up',
    'blew itself up',
    'should have used a smaller gun',
    'killed himself',
    'killed herself',
    'killed itself',
    'is done with the world',
    'suicides',
    'plummets to his death',
    'plummets to her death',
    'plummets to its death',
    'cratered',
    'was flattened',
    'was squished',
    'sank like a rock',
    'melted',
    'does a back flip into the lava',
    'blew up',
    'found a way out',
    'saw the light',
    'got blasted',
    'was in the wrong place',
    'ate too much glass',
    'died',
}

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
                val = msg.i16()
                if   i == 1:  p['health']      = val   # STAT_HEALTH
                elif i == 3:  p['ammo']        = val   # STAT_AMMO
                elif i == 5:  p['armor']        = val  # STAT_ARMOR
                elif i == 8:  p['pickup_str']  = val   # STAT_PICKUP_STRING
                elif i == 14: p['frags']        = val  # STAT_FRAGS
                elif i == 17: p['clip']         = val  # STAT_CLIP
                elif i == 19: p['items_icon']  = val   # STAT_ITEMS_ICON (cycles through held items)
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
    # AQ2 world/special obituary phrases (source/src/action/p_client.c, a_team.c)
    if ' ph34rs ' in t and ' committed suicide' in t: return 'Suicide Punish'
    if 'was shoved off the edge by' in t or 'was in the wrong place' in t:
        return 'Trigger Hurt'
    if 'was thrown through a window by' in t or 'ate too much glass' in t:
        return 'Glass'
    if 'was taught how to fly by' in t:
        return 'Falling'
    if ('plummets to his death' in t or 'plummets to her death' in t or
            'plummets to its death' in t or 'cratered' in t or
            'was flattened' in t or 'was squished' in t or
            'sank like a rock' in t):
        return 'Falling'
    if 'melted' in t: return 'Slime'
    if 'lava' in t: return 'Lava'
    if ('killed himself' in t or 'killed herself' in t or 'killed itself' in t or
            'is done with the world' in t or 'suicides' in t):
        return 'Suicide'
    if 'found a way out' in t or 'saw the light' in t or 'got blasted' in t:
        return 'World'
    # Base Q2 obituary phrases (source/src/game/p_client.c)
    if ' was blasted by ' in t: return 'Blaster'
    if 'gunned down by' in t or 'super shotgun' in t or 'blown away by' in t:
        return 'Shotgun'
    if 'machinegunned by' in t: return 'Machinegun'
    if 'chaingun' in t or 'cut in half by' in t: return 'Chaingun'
    if "'s rocket" in t: return 'Rocket'
    if 'was railed by' in t: return 'Railgun'
    if 'bfg' in t: return 'BFG'
    if 'hyperblaster' in t: return 'Hyperblaster'
    if 'sniper rifle' in t: return 'SR'
    if 'tried to invade' in t or 'personal space' in t: return 'Telefrag'
    if 'shrapnel' in t: return 'Grenade'
    if "feels" in t and "'s pain" in t: return 'Grenade'
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
    # Keep leading spaces: some players intentionally use whitespace-prefixed
    # names (e.g. "              ."), and stripping would break victim matching.
    text = text.rstrip('\r\n')
    for victim in names:
        if not (text.startswith(victim + ' ') or text.startswith(victim + "'")):
            continue
        rest = text[len(victim):]
        best_killer = None
        best_score = None
        for killer in names:
            if killer == victim:
                continue
            # Use word-boundary matching so a short name like 'M' does not
            # false-match inside weapon names such as 'M4', 'MK23', 'MP5'.
            pat = r'(?<!\w)' + re.escape(killer) + r'(?!\w)'
            m = re.search(pat, rest)
            if not m:
                continue
            # Prefer the most specific candidate: longest name first.
            # Tie-break by latest position, which tends to be closer to the
            # killer slot in AQ2 death strings (victim + middle + killer + suffix).
            score = (len(killer), m.start())
            if best_score is None or score > best_score:
                best_score = score
                best_killer = killer
        if best_killer is not None:
            evt = {
                'killer':   best_killer,
                'victim':   victim,
                'location': _infer_location(rest),
                'weapon':   _infer_weapon(text),
            }
            # AQ2 punish-kill line from killPlayer():
            #   "<victim> ph34rs <killer> so much <he/she> committed suicide! :)"
            # This awards +1 frag to killer and subtracts 1 frag from victim.
            _rt = rest.lower()
            if ' ph34rs ' in _rt and ' committed suicide' in _rt:
                evt['victim_frag_penalty'] = 1
            return evt
        if _is_suicide_obituary(rest):
            return {
                'killer':   victim,
                'victim':   victim,
                'location': _infer_location(rest),
                'weapon':   _infer_weapon(text),
                'suicide':  True,
            }
    return None


def _extract_mode_signal(line: str) -> Optional[str]:
    """Extract mode label from MOTD lines like "Matchmode: ..." or "Game Type: ..."."""
    t = (line or '').strip()
    for prefix in ('Matchmode:', 'Game Type:'):
        if t.startswith(prefix):
            return t.split(':', 1)[1].strip()
    return None


def _is_suicide_obituary(rest: str) -> bool:
    """Return True when obituary suffix matches a suicide/world-death line."""
    t = (rest or '').strip().lower().rstrip('.!')
    return t in _SUICIDE_MSG_EQ


def _team_num_for_name(name: Optional[str], player_names: dict,
                       player_teams: dict, name_history: Optional[dict] = None) -> int:
    """Resolve a player name to current team number (1/2), if known."""
    if not name:
        return 0
    for pnum, pname in player_names.items():
        if pname == name:
            return int(player_teams.get(pnum, 0) or 0)
    if name_history:
        for pnum, aliases in name_history.items():
            if name in aliases:
                return int(player_teams.get(pnum, 0) or 0)
    return 0


def _parse_chat_sender(text: str) -> tuple[Optional[str], bool]:
    """Extract sender name from chat text. Returns (sender, is_dead_chat)."""
    t = (text or '').strip()
    m = _TEAM_CHAT_PARSE_RE.match(t)
    if m:
        return m.group(2).strip(), bool(m.group(1))
    m = _PUBLIC_CHAT_PARSE_RE.match(t)
    if m:
        return m.group(2).strip(), bool(m.group(1))
    return None, False


def _build_chat_event(text: str, frame: int, player_names: dict, player_teams: dict,
                      name_history: Optional[dict] = None) -> Optional[dict]:
    """Build one normalized chat event with optional sender/team metadata."""
    t = (text or '').strip()
    if not t:
        return None
    sender, is_dead = _parse_chat_sender(t)
    is_team = bool(_TEAM_SAY_RE.match(t))
    ev = {'frame': frame, 'text': t, 'team': is_team}
    if sender:
        ev['sender'] = sender
        team_num = _team_num_for_name(sender, player_names, player_teams, name_history)
        if team_num:
            ev['team_num'] = team_num
    if is_dead:
        ev['dead'] = True
    return ev


def _dedupe_round_events(round_events: list) -> list:
    """Deduplicate round events coming from repeated unicast/multicast streams."""
    seen: set = set()
    unique: list = []
    for ev in round_events:
        key = tuple(sorted(ev.items()))
        if key not in seen:
            seen.add(key)
            unique.append(ev)
    return unique


def _dedupe_chat_events(chat_events: list) -> list:
    """Deduplicate chat events while preserving sender/team metadata."""
    seen: set = set()
    unique: list = []
    for c in chat_events:
        key = (
            c.get('frame'),
            c.get('text'),
            bool(c.get('team')),
            c.get('sender', ''),
            int(c.get('team_num', 0) or 0),
            bool(c.get('dead')),
        )
        if key not in seen:
            seen.add(key)
            unique.append(c)
    return unique


def _dedupe_layout_events(layout_events: list) -> list:
    """Deduplicate layout events repeated in multi-client streams."""
    seen: set = set()
    unique: list = []
    for e in layout_events:
        key = (e.get('frame'), e.get('text', ''))
        if key not in seen:
            seen.add(key)
            unique.append(e)
    return unique


def _extract_layout_team_rows(layout_text: str) -> list:
    """Parse AQ2 layout scoreboard rows including team token and captain marker.

    Returns list items shaped as:
      {'team', 'captain', 'name', 'kills', 'deaths', 'damage', 'acc'}
    """
    if not layout_text:
        return []
    if 'Team Player' not in layout_text or 'Damage Acc' not in layout_text:
        return []

    out: list = []
    for line in re.findall(r'"([^\"]*)"', layout_text):
        t = (line or '').strip()
        if not t or 'Team Player' in t:
            continue

        m = _LAYOUT_ROW_RE.match(t)
        if not m:
            continue

        prefix = (m.group(1) or '').strip()
        parts = prefix.split()
        if len(parts) < 2:
            continue

        team_tok = parts[0]
        if not _TEAM_COL_TOKEN_RE.match(team_tok):
            continue

        mt = re.match(r'^(\d+)([A-Z]?)$', team_tok)
        if not mt:
            continue
        team_num = int(mt.group(1))
        marker = (mt.group(2) or '').upper()

        name = ' '.join(parts[1:]).strip()
        if not name:
            continue
        # Aggregate pseudo-row, not a real player.
        if re.fullmatch(r'Team\s+\d+', name, flags=re.IGNORECASE):
            continue

        out.append({
            'team': team_num,
            'captain': marker == 'C',
            'name': name,
            'kills': int(m.group(4)),
            'deaths': int(m.group(5)),
            'damage': int(m.group(6)),
            'acc': int(m.group(7)),
        })

    return out


def _extract_layout_scoreboard_rows(layout_text: str) -> dict:
    """Parse AQ2 layout scoreboard rows with Team/Player/Kills/Deaths/Damage/Acc columns.

    Expected header: "Team Player ... Kills Deaths Damage Acc"
    Returns mapping: {name: {'kills', 'deaths', 'damage', 'acc'}}
    """
    rows: dict = {}
    for r in _extract_layout_team_rows(layout_text):
        rows[r['name']] = {
            'kills': int(r['kills']),
            'deaths': int(r['deaths']),
            'damage': int(r['damage']),
            'acc': int(r['acc']),
        }

    return rows


def _append_award_from_centerprint(text: str, frame: int, award_events: list) -> bool:
    """Parse one centerprint line as an award event. Returns True on match."""
    t = (text or '').strip()

    m = _IMPRESSIVE_RE.match(t)
    if m:
        ev = {'frame': frame, 'player': m.group(1), 'award': 'Impressive'}
        if m.group(2):
            ev['count'] = int(m.group(2))
        award_events.append(ev)
        return True

    m = _ACCURACY_RE.match(t)
    if m:
        ev = {'frame': frame, 'player': m.group(1), 'award': 'Accuracy'}
        if m.group(2):
            ev['count'] = int(m.group(2))
        award_events.append(ev)
        return True

    m = _EXCELLENT_RE.match(t)
    if m:
        ev = {'frame': frame, 'player': m.group(1), 'award': 'Excellent'}
        if m.group(2):
            ev['count'] = int(m.group(2))
        award_events.append(ev)
        return True

    m = _DOMINATING_RE.match(t)
    if m:
        award_events.append({'frame': frame, 'player': m.group(1), 'award': 'Dominating'})
        return True

    m = _UNSTOPPABLE_RE.match(t)
    if m:
        award_events.append({'frame': frame, 'player': m.group(1), 'award': 'Unstoppable'})
        return True

    return False


def _append_round_event_from_print(text: str, frame: int, round_events: list) -> bool:
    """Parse one PRINT text line as a round event. Returns True on match."""
    t = _normalize_round_print_text(text)

    if _ROUND_LIGHTS_RE.match(t):
        round_events.append({'type': 'lights', 'frame': frame})
        return True

    if _ROUND_CAMERA_RE.match(t):
        round_events.append({'type': 'camera', 'frame': frame})
        return True

    if _ROUND_ACTION_RE.match(t):
        round_events.append({'type': 'action', 'frame': frame})
        return True

    mw = _ROUND_WIN_RE.match(t)
    if mw:
        round_events.append({
            'type': 'win',
            'team_name': _normalize_round_print_text(mw.group(1)),
            'frame': frame,
        })
        return True

    mw = _ROUND_TEAM_WINS_RE.match(t)
    if mw:
        round_events.append({
            'type': 'win',
            'team_name': _normalize_round_print_text(mw.group(1)),
            'frame': frame,
        })
        return True

    if _TIE_RE.search(t):
        round_events.append({'type': 'tie', 'frame': frame})
        return True

    ms = _CURR_SCORE_RE.search(t)
    if ms:
        ev = {
            'type':       'score',
            'team1_name': _normalize_round_print_text(ms.group(1)), 'score1': int(ms.group(2)),
            'team2_name': _normalize_round_print_text(ms.group(3)), 'score2': int(ms.group(4)),
            'frame':      frame,
        }
        if ms.group(5) is not None and ms.group(6) is not None:
            ev['team3_name'] = _normalize_round_print_text(ms.group(5))
            ev['score3'] = int(ms.group(6))
        round_events.append(ev)
        return True

    return False


def _classify_game_mode(configstrings: dict, motd_signals: list,
                        round_wins: dict) -> tuple[str, str]:
    """Classify game mode as (canonical_bucket, detailed_mode)."""
    has_ctf = any('models/flags/flag1.md2' in v
                  for v in configstrings.values() if isinstance(v, str))
    if has_ctf:
        return 'ctf', 'ctf'

    detail: Optional[str] = None
    if motd_signals:
        mm = ' '.join((s or '').strip().lower() for s in motd_signals if s).strip()
        if 'capture the flag' in mm or re.search(r'\bctf\b', mm):
            return 'ctf', 'ctf'
        if 'domination' in mm:
            detail = 'domination'
        elif 'espionage' in mm:
            detail = 'espionage'
        elif 'team deathmatch' in mm or re.search(r'\btdm\b', mm):
            detail = 'team_deathmatch'
        elif 'teamplay' in mm or re.search(r'\btp\b', mm):
            detail = 'teamplay'
        elif 'tourney' in mm:
            detail = 'tourney'
        elif 'jumpmod' in mm:
            detail = 'jumpmod'
        elif 'deathmatch' in mm or re.search(r'\bdm\b', mm):
            detail = 'deathmatch'

    if detail in ('deathmatch', 'jumpmod'):
        return 'dm', detail
    if detail:
        return 'tdm', detail

    rw = round_wins or {}
    if any(v > 0 for v in rw.values()):
        return 'tdm', 'teamplay'
    return 'dm', 'deathmatch'


# AQ2 TNG item inventory indices (from g_local.h / g_items.c) and their bitmask positions.
_ITEM_INV_BITS = {
    13: 1,   # KEV_NUM  → kevlar vest
    15: 2,   # HELM_NUM → kevlar helmet
    10: 4,   # SIL_NUM  → silencer
    12: 8,   # BAND_NUM → bandolier
    14: 16,  # LASER_NUM→ laser sight
}

# Mapping from STAT_PICKUP_STRING configstring index → item bitmask.
# CS_ITEMS = CS_LIGHTS_OLD + MAX_LIGHTSTYLES = 800 + 256 = 1056  (old protocol)
# CS_ITEMS + typeNum = configstring index sent in stat[8] on item pickup.
_CS_ITEMS = CS_PLAYERSKINS - 256  # = 1056
_ITEM_CS_TO_BIT: dict = {
    _CS_ITEMS + 10: 4,   # SIL_NUM  → silencer
    _CS_ITEMS + 12: 8,   # BAND_NUM → bandolier
    _CS_ITEMS + 13: 1,   # KEV_NUM  → kevlar vest
    _CS_ITEMS + 14: 16,  # LASER_NUM → laser sight
    _CS_ITEMS + 15: 2,   # HELM_NUM  → kevlar helmet
}


def _parse_svc_stream(payload: bytes, clientnum: int, frame: int,
                      kills: list, hit_events: list, award_events: list,
                      player_names: dict, player_teams: dict, name_history: dict,
                      round_events: Optional[list] = None,
                      chat_events: Optional[list] = None,
                      layout_events: Optional[list] = None,
                      motd_signals: Optional[list] = None,
                      item_state: Optional[dict] = None):
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
                if level == PRINT_CHAT:
                    if chat_events is not None:
                        _ev = _build_chat_event(msg, frame, player_names,
                                                player_teams, name_history)
                        if _ev:
                            chat_events.append(_ev)
                elif level == PRINT_LOW:
                    if motd_signals is not None and not motd_signals:
                        for _line in msg.split('\n'):
                            _mode = _extract_mode_signal(_line)
                            if _mode:
                                motd_signals.append(_mode)
                                break
                elif level == PRINT_MEDIUM:
                    evt = _parse_kill_message(msg, player_names)
                    if evt:
                        killer_slots = [int(pnum) for pnum, pname in player_names.items()
                                        if pname == evt.get('killer')]
                        victim_slots = [int(pnum) for pnum, pname in player_names.items()
                                        if pname == evt.get('victim')]
                        if killer_slots:
                            evt['killer_slots'] = killer_slots
                            if len(killer_slots) == 1:
                                evt['killer_slot'] = killer_slots[0]
                        if victim_slots:
                            evt['victim_slots'] = victim_slots
                            if len(victim_slots) == 1:
                                evt['victim_slot'] = victim_slots[0]
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
                    else:
                        m = _KVLR_RE.match(msg)
                        if m:
                            # Kevlar Helmet = head hit absorbed; Kevlar Vest = chest hit absorbed
                            loc = 'kvlr_helmet' if 'helmet' in m.group(2).lower() else 'kvlr_vest'
                            hit_events.append({
                                'frame':    frame,
                                'attacker': clientnum,
                                'victim':   m.group(1),
                                'location': loc,
                            })

            elif svc == SVC_CENTERPRINT:
                msg_text, pos = read_str(pos)
                t = msg_text.strip()
                if not _append_award_from_centerprint(t, frame, award_events):
                    _is_round = bool(round_events is not None and
                                     _append_round_event_from_print(t, frame, round_events))
                    if (not _is_round) and motd_signals is not None and not motd_signals:
                        # Old-format demos send the MOTD as SVC_CENTERPRINT in a unicast.
                        for _ln in t.split('\n'):
                            _mode = _extract_mode_signal(_ln)
                            if _mode:
                                motd_signals.append(_mode)
                                break

            elif svc == SVC_LAYOUT:
                _msg, pos = read_str(pos)
                if layout_events is not None and _msg:
                    layout_events.append({'frame': frame, 'text': _msg})

            elif svc == SVC_STUFFTEXT:
                _msg, pos = read_str(pos)

            elif svc == SVC_MUZZLEFLASH:
                pos += 3   # int16 entity + uint8 weapon_byte

            elif svc == SVC_CONFIGSTRING:
                pos += 2   # uint16 index
                _v, pos = read_str(pos)

            elif svc == SVC_INVENTORY:
                if item_state is not None and pos + 512 <= len(d):
                    bits = 0
                    for inv_idx, flag in _ITEM_INV_BITS.items():
                        count_lo = d[pos + inv_idx * 2]
                        count_hi = d[pos + inv_idx * 2 + 1]
                        if count_lo | count_hi:
                            bits |= flag
                    item_state[clientnum] = bits
                pos += 512  # 256 × uint16

            else:
                break       # unknown opcode – abandon this block

        except (IndexError, UnicodeDecodeError):
            break


def _parse_frame(msg: Msg, state: dict, item_state: Optional[dict] = None,
                 img_to_bit: Optional[dict] = None) -> dict:
    """
    Parse one MVD frame block.

    state      – mutable dict keyed by client_num; each value holds the last
                 known {ox, oy, oz, yaw, weapon, health, armor, ...}.
    item_state – optional dict keyed by client_num → item bitmask (from SVC_INVENTORY).
    img_to_bit – optional dict keyed by CS_IMAGES index → item bitmask
                 (built from configstrings at load time for STAT_ITEMS_ICON tracking).

    Returns snapshot dict  {client_num: {x, y, z, a, w, h, ar, am, cl, it}}.
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
            if item_state is not None:
                item_state.pop(n, None)
            continue
        s = state.setdefault(n, {})
        if 'ox' in p: s['ox'] = p['ox']
        if 'oy' in p: s['oy'] = p['oy']
        if 'oz' in p: s['oz'] = p['oz']
        if 'yaw' in p: s['yaw'] = p['yaw']
        if 'weapon' in p: s['weapon'] = p['weapon']
        if 'health' in p: s['health'] = p['health']
        if 'armor'  in p: s['armor']  = p['armor']
        if 'ammo'   in p: s['ammo']   = p['ammo']
        if 'frags'  in p: s['frags']  = p['frags']
        if 'clip'   in p: s['clip']   = p['clip']
        if item_state is not None:
            if 'pickup_str' in p:
                bit = _ITEM_CS_TO_BIT.get(p['pickup_str'], 0)
                if bit:
                    item_state[n] = (item_state.get(n, 0) | bit)
            if 'items_icon' in p and img_to_bit is not None:
                bit = img_to_bit.get(p['items_icon'], 0)
                if bit:
                    item_state[n] = (item_state.get(n, 0) | bit)

    for n, s in state.items():
        am = s.get('ammo', -1)
        cl = s.get('clip', -1)
        snapshot[n] = {
            'x': round(s.get('ox', 0) * COORD_SCALE, 1),
            'y': round(s.get('oy', 0) * COORD_SCALE, 1),
            'z': round(s.get('oz', 0) * COORD_SCALE, 1),
            'a': round(s.get('yaw', 0) * ANGLE_SCALE % 360, 1),
            'w': s.get('weapon', 0),
            'h': s.get('health', -1),
            'ar': s.get('armor', -1),
            'it': item_state.get(n, 0) if item_state else 0,
        }
        if am >= 0: snapshot[n]['am'] = am
        if cl >= 0: snapshot[n]['cl'] = cl

    # Entities (skip all)
    try:
        while _skip_entity(msg):
            pass
    except Exception:
        pass

    return snapshot


def _parse_multicast(msg: Msg, cmd: int, extra: int, frame: int,
                     award_events: list, round_events: list,
                     muzzle_flashes: list, player_names: dict,
                     player_teams: dict, name_history: dict,
                     chat_events: Optional[list] = None,
                     layout_events: Optional[list] = None):
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
                if not _append_award_from_centerprint(t, frame, award_events):
                    _append_round_event_from_print(t, frame, round_events)
            elif svc == SVC_PRINT:
                level = d[pos]; pos += 1
                t, pos = read_str(pos)
                t = t.strip()
                if level == PRINT_CHAT:
                    if chat_events is not None:
                        _ev = _build_chat_event(t, frame, player_names,
                                                player_teams, name_history)
                        if _ev:
                            chat_events.append(_ev)
                else:
                    _append_round_event_from_print(t, frame, round_events)
            elif svc == SVC_STUFFTEXT:
                _, pos = read_str(pos)
            elif svc == SVC_LAYOUT:
                _layout, pos = read_str(pos)
                if layout_events is not None and _layout:
                    layout_events.append({'frame': frame, 'text': _layout})
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
                   player_names: dict, player_teams: dict, name_history: dict,
                   round_events: Optional[list] = None,
                   chat_events: Optional[list] = None,
                   layout_events: Optional[list] = None,
                   motd_signals: Optional[list] = None,
                   item_state: Optional[dict] = None):
    # The length byte encodes the SVC payload size (msg_write.cursize).
    # clientNum is a separate byte written AFTER the length, NOT counted in it.
    length    = msg.u8() | (extra << 8)
    clientnum = msg.u8()
    payload   = msg.read(length)
    _parse_svc_stream(payload, clientnum, frame_idx, kills, hit_events,
                      award_events, player_names, player_teams, name_history,
                      round_events, chat_events, layout_events, motd_signals,
                      item_state)


def _is_world_bsp(idx: int, val: str) -> bool:
    """True if configstring at idx looks like the world BSP model."""
    return idx in CS_MODELS_SCAN and val.startswith('maps/') and val.endswith('.bsp')


def _parse_skin_configstring(pnum: int, value: str,
                             player_names: dict, player_teams: dict,
                             name_history: dict, player_team_history: dict,
                             frame_idx: int) -> None:
    """Extract name and team from a CS_PLAYERSKINS configstring value.
    Format: name\\model/skin  where skin suffix ctf_r→team1, ctf_b→team2.
    name_history accumulates every name a pnum has ever used so kills/deaths
    recorded under old names can be remapped to the final name at build time.
    player_team_history records per-slot team transitions as (frame, team).
    """
    def _record_team_state(team_num: int) -> None:
        hist = player_team_history.setdefault(pnum, [])
        if hist and int(hist[-1][0]) == int(frame_idx):
            hist[-1] = (int(frame_idx), int(team_num))
            return
        if not hist or int(hist[-1][1]) != int(team_num):
            hist.append((int(frame_idx), int(team_num)))

    if not value:
        # Empty playerskin configstrings are emitted on disconnect; clear slot
        # state so a later client reusing the same pnum doesn't inherit aliases.
        _record_team_state(0)
        player_names.pop(pnum, None)
        player_teams.pop(pnum, None)
        name_history.pop(pnum, None)
        return
    parts = value.split('\\')
    name = parts[0]
    player_names[pnum] = name
    name_history.setdefault(pnum, set()).add(name)
    if len(parts) > 1:
        skin = parts[1].split('/')[-1] if '/' in parts[1] else parts[1]
        skin_lo = skin.lower()
        if 'ctf_r' in skin_lo or skin_lo.endswith('_r'):
            player_teams[pnum] = 1
            _record_team_state(1)
        elif 'ctf_b' in skin_lo or skin_lo.endswith('_b'):
            player_teams[pnum] = 2
            _record_team_state(2)


def _read_configstring(msg: Msg, configstrings: dict,
                       player_names: dict, player_teams: dict,
                       name_history: dict, player_team_history: dict,
                       frame_idx: int) -> Optional[str]:
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
        _parse_skin_configstring(idx - CS_PLAYERSKINS, value,
                                 player_names, player_teams, name_history,
                                 player_team_history, frame_idx)
    return None


# ── Public API ─────────────────────────────────────────────────────────────────

def parse_mvd2(data: bytes, max_frames: int = 0, stats_only: bool = False) -> dict:
    """
    Parse a raw (or gzip-compressed) MVD2 bytestring.

    Returns:
      map_name       str
      player_names   {client_num: name}
      frame_interval float   (seconds per frame, nominally 0.1)
      frames         list of {t: frame_index, players: {num: {x,y,z,a}}}
                     (always empty when stats_only=True)

    stats_only=True skips building the frames position list (~87% of parse
    time) and is used by the DB indexer which never needs position data.
    """
    if data[:2] == b'\x1f\x8b':
        data = gzip.decompress(data)
    if data[:4] != MVD_MAGIC:
        raise ValueError("Not a valid MVD2 file")

    pos           = 4
    configstrings: dict  = {}
    player_names:  dict  = {}    # pnum → current name
    player_teams:  dict  = {}    # pnum → 1 or 2
    name_history:  dict  = {}    # pnum → set of all names ever used
    player_team_history: dict = {}  # pnum -> [(frame, team)]
    state:         dict  = {}    # client_num → last known position
    map_name:      Optional[str] = None
    frames:        list  = []
    kills:         list  = []
    hit_events:    list  = []
    award_events:  list  = []
    round_events:  list  = []     # {'type': 'win'/'score', ...}
    round_start_frames: list = []
    muzzle_flashes:      list = []
    silencer_shots:      list = []   # {'frame', 'client'} — silenced-weapon shots
    chat_events:         list = []   # {'frame', 'text', 'team'}
    layout_events:       list = []   # {'frame', 'text'} extracted from SVC_LAYOUT
    motd_signals:        list = []   # matchmode strings extracted from server MOTD
    item_state:          dict = {}   # client_num → item bitmask (from SVC_INVENTORY)
    img_to_bit:          dict = {}   # CS_IMAGES index → item bitmask (from STAT_ITEMS_ICON)
    frame_idx            = 0
    stats_frame_stride   = 20

    def _update_img_to_bit(idx: int, val: str) -> None:
        """If idx is a CS_IMAGES-range configstring, map its relative image index to an item bit.
        STAT_ITEMS_ICON sends the relative image index (returned by gi.imageindex()), not the
        absolute configstring index, so we key by (idx - CS_IMAGES)."""
        if CS_IMAGES <= idx < CS_IMAGES + 256 and val:
            stem = val.split('/')[-1].lower().split('.')[0]
            bit  = _ITEM_ICON_STEMS.get(stem, 0)
            if bit:
                img_to_bit[idx - CS_IMAGES] = bit

    def _store_frame_snapshot(snapshot: dict) -> None:
        if not snapshot:
            return
        if not stats_only:
            frames.append({'t': frame_idx, 'players': snapshot})
            return
        # Keep sparse snapshots in stats_only mode for heatmap indexing.
        if frame_idx % stats_frame_stride != 0:
            return
        if player_names:
            slim = {n: p for n, p in snapshot.items() if n in player_names}
        else:
            slim = snapshot
        if slim:
            frames.append({'t': frame_idx, 'players': slim})

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
                        _update_img_to_bit(idx, val)
                        if _is_world_bsp(idx, val):
                            map_name = val.split('/')[-1][:-4]
                        elif CS_PLAYERSKINS <= idx < CS_PLAYERSKINS + MAX_CLIENTS:
                            _parse_skin_configstring(idx - CS_PLAYERSKINS, val,
                                                     player_names, player_teams,
                                                     name_history, player_team_history,
                                                     frame_idx)
                    # Base frame
                    snap = _parse_frame(msg, state, item_state, img_to_bit)
                    _store_frame_snapshot(snap)
                    frame_idx += 1

                elif cmd == MVD_CONFIGSTRING:
                    mn = _read_configstring(msg, configstrings,
                                            player_names, player_teams,
                                            name_history, player_team_history,
                                            frame_idx)
                    if mn:
                        map_name = mn

                elif cmd == MVD_FRAME:
                    prev_keys = set(state.keys())
                    snap = _parse_frame(msg, state, item_state, img_to_bit)
                    curr_keys = set(state.keys())
                    # A named player respawning (key returns after P_REMOVE) = new round
                    named_nums = set(player_names.keys())
                    if (curr_keys - prev_keys) & named_nums and frame_idx > 0:
                        round_start_frames.append(frame_idx)
                    _store_frame_snapshot(snap)
                    frame_idx += 1
                    if max_frames and frame_idx >= max_frames:
                        _uc = _dedupe_chat_events(chat_events)
                        _ur = _dedupe_round_events(round_events)
                        _ul = _dedupe_layout_events(layout_events)
                        result = _build(map_name, player_names, player_teams, frames,
                                        frame_idx, kills, hit_events, award_events,
                                        round_start_frames, _ur, muzzle_flashes,
                                        name_history, player_team_history,
                                        silencer_shots, _ul)
                        result['chat'] = _uc
                        result['round_events'] = _ur
                        result['weapon_models'] = _build_weapon_models(configstrings)
                        mode_bucket, mode_detail = _classify_game_mode(
                            configstrings,
                            motd_signals,
                            result.get('round_wins', {}),
                        )
                        result['game_mode'] = mode_bucket
                        result['game_mode_detail'] = mode_detail
                        return result

                elif MVD_MULTICAST_ALL <= cmd <= MVD_MULTICAST_PVS_R:
                    _parse_multicast(msg, cmd, extra, frame_idx, award_events,
                                     round_events, muzzle_flashes, player_names,
                                     player_teams, name_history, chat_events,
                                     layout_events)

                elif cmd in (MVD_UNICAST, MVD_UNICAST_R):
                    _parse_unicast(msg, extra, frame_idx,
                                   kills, hit_events, award_events, player_names,
                                   player_teams, name_history, round_events,
                                   chat_events, layout_events, motd_signals,
                                   item_state)

                elif cmd == MVD_SOUND:
                    sb      = msg.u8()
                    snd_idx = msg.u8()                   # index into CS_SOUNDS
                    if sb & 1:  msg.u8()                 # volume
                    if sb & 2:  msg.u8()                 # attenuation
                    if sb & 16: msg.u8()                 # offset
                    chan_word  = msg.u16()                # entity*8 + channel (always present)
                    entity_num = chan_word >> 3
                    channel    = chan_word & 7
                    # Silencer fires gi.sound(ent, CHAN_WEAPON, snd_silencer, ...) with no
                    # SVC_MUZZLEFLASH → detect "misc/silencer.wav" on CHAN_WEAPON as a shot.
                    if channel == CHAN_WEAPON:
                        snd_name = configstrings.get(CS_SOUNDS + snd_idx, '')
                        if 'silencer' in snd_name:
                            client_num = entity_num - 1  # entity 1 = client 0
                            silencer_shots.append({'frame': frame_idx, 'client': client_num})

                elif cmd == MVD_PRINT:
                    _print_level = msg.u8()
                    txt = msg.string().strip()
                    if _print_level == PRINT_CHAT and txt:
                        _ev = _build_chat_event(txt, frame_idx, player_names,
                                                player_teams, name_history)
                        if _ev:
                            chat_events.append(_ev)
                    elif _print_level == PRINT_LOW and not motd_signals:
                        for _ln in txt.split('\n'):
                            _mode = _extract_mode_signal(_ln)
                            if _mode:
                                motd_signals.append(_mode)
                                break
                    if _print_level != PRINT_CHAT:
                        _append_round_event_from_print(txt, frame_idx, round_events)

                else:
                    break   # unknown cmd – abandon this block

            except Exception:
                break   # corrupt / truncated block, move to next

    # Deduplicate kills: the same kill is unicast to all connected clients
    # (victim, killer, spectators) in the same frame, producing identical entries.
    seen: set = set()
    unique_kills: list = []
    for k in kills:
        key = (
            k['killer'],
            k['victim'],
            k['frame'],
            tuple(sorted(k.get('killer_slots') or ())),
            tuple(sorted(k.get('victim_slots') or ())),
        )
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

    unique_chat = _dedupe_chat_events(chat_events)
    unique_round_events = _dedupe_round_events(round_events)
    unique_layout_events = _dedupe_layout_events(layout_events)

    result = _build(map_name, player_names, player_teams, frames, frame_idx,
                    kills, hit_events, award_events, round_start_frames, unique_round_events,
                    muzzle_flashes, name_history, player_team_history,
                    silencer_shots, unique_layout_events)
    result['chat'] = unique_chat
    result['round_events'] = unique_round_events
    result['weapon_models'] = _build_weapon_models(configstrings)
    mode_bucket, mode_detail = _classify_game_mode(
        configstrings,
        motd_signals,
        result.get('round_wins', {}),
    )
    result['game_mode'] = mode_bucket
    result['game_mode_detail'] = mode_detail
    return result


def _build_weapon_models(configstrings: dict) -> dict:
    """Build a mapping of ps.gunindex → vwep model filename stem.

    ps.gunindex equals the model configstring index (CS_MODELS + N where N is the
    return value of gi.modelindex on the server).  The view-model path stored in
    the configstring is matched against _VIEW_TO_VWEP to find the vwep MD2 name.
    Returns {int_index: 'w_mk23', ...}  (string keys after JSON serialisation).
    """
    result: dict = {}
    for i in range(1, 256):
        path = configstrings.get(CS_MODELS + i, '')
        if not path:
            continue
        for frag, vwep in _VIEW_TO_VWEP.items():
            if frag in path:
                result[i] = vwep
                break
    return result


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
           muzzle_flashes=None, name_history=None,
           player_team_history=None, silencer_shots=None,
           layout_events=None) -> dict:
    kills              = kills              or []
    hit_events         = hit_events         or []
    award_events       = award_events       or []
    round_start_frames = round_start_frames or []
    round_events       = round_events       or []
    layout_events      = layout_events      or []

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

    # Players that appeared in at least one position frame (had a real game presence).
    # A named player with a team skin configstring but zero position frames was
    # never truly in-game (connected briefly, spectated, or joined and disconnected
    # before the parser captured a movement frame for them).
    # When stats_only=True, frames is empty so all_frame_nums is empty.  Fall
    # back to every named player that has any recorded stat activity — this is
    # the best available signal when position data was not collected.
    if all_frame_nums:
        players_with_frames: set = {
            player_names[n] for n in player_names if n in all_frame_nums
        }
    else:
        # stats_only mode: no position frames available.  Derive activity from
        # events: kills, hits, and muzzle flashes (shots fired).
        active_names = (set(k['killer'] for k in kills) |
                        set(k['victim'] for k in kills) |
                        set(player_names.get(h['attacker'], '') for h in hit_events))
        active_cnums = set(mf['client'] for mf in (muzzle_flashes or []))
        active_names |= {player_names[n] for n in active_cnums if n in player_names}
        active_names.discard('')
        players_with_frames = {
            player_names[n] for n in player_names
            if player_names[n] in active_names
        } or set(player_names.values())  # last resort: treat all named players as present

    # Name-change remapping: a player may rename mid-game. Kill messages record
    # the name active at the time; player_names only holds the final name.
    # Build alias→final_name so every historical name resolves to current name.
    name_history = name_history or {}
    alias_to_final: dict = {}

    # Safety against cross-player merges:
    # If multiple client slots end on the same final name in one replay,
    # historical aliases cannot be remapped reliably to that shared name.
    final_name_counts: dict = {}
    for _pnum, _final_name in player_names.items():
        final_name_counts[_final_name] = final_name_counts.get(_final_name, 0) + 1

    alias_owners: dict = {}
    for _pnum, _names_set in name_history.items():
        for _alias in _names_set:
            owners = alias_owners.setdefault(_alias, set())
            owners.add(_pnum)

    for pnum, names_set in name_history.items():
        final = player_names.get(pnum)
        if not final:
            continue
        if final_name_counts.get(final, 0) != 1:
            # Ambiguous final name shared by multiple slots.
            continue
        for old_name in names_set:
            if old_name == final:
                continue
            if alias_owners.get(old_name, set()) != {pnum}:
                # Alias appears under multiple slots in one replay.
                continue
            if final_name_counts.get(old_name, 0) > 0:
                # Alias is another player's final current name.
                continue
            alias_to_final[old_name] = final

    def _canon(name: str) -> str:
        """Resolve a possibly-old player name to their current final name."""
        return alias_to_final.get(name, name)

    # Apply name normalisation to all event lists.
    # hit_events['attacker'] is a pnum integer resolved later via player_names,
    # so it doesn't need remapping; only string name fields are affected.
    for k in kills:
        k['killer'] = _canon(k['killer'])
        k['victim'] = _canon(k['victim'])
    for h in hit_events:
        h['victim'] = _canon(h['victim'])
    for a in award_events:
        a['player'] = _canon(a['player'])

    # Old demos may not carry per-hit events (PRINT_HIGH "You hit ...") but
    # still include scoreboard layout rows with K/D/Damage/Acc columns.
    # Keep the latest layout snapshot per player as a fallback stats source.
    layout_scoreboard_by_name: dict = {}
    captain_counts_by_team: dict = {1: {}, 2: {}}
    captain_changes_by_team: dict = {1: 0, 2: 0}
    captain_seen_by_team: dict = {}
    captain_first_frame_by_team: dict = {}
    captain_last_frame_by_team: dict = {}
    captain_events: list = []
    for ev in sorted(layout_events, key=lambda e: e.get('frame', 0)):
        frame = int(ev.get('frame', 0) or 0)
        rows = _extract_layout_scoreboard_rows(ev.get('text', ''))
        for raw_name, stats in rows.items():
            cname = _canon(raw_name)
            layout_scoreboard_by_name[cname] = stats

        for row in _extract_layout_team_rows(ev.get('text', '')):
            team_num = int(row.get('team', 0) or 0)
            if team_num not in (1, 2) or not row.get('captain'):
                continue
            raw_captain = str(row.get('name', '') or '').strip()
            if not raw_captain:
                continue
            captain_name = _canon(raw_captain)

            captain_team_counts = captain_counts_by_team.setdefault(team_num, {})
            captain_team_counts[captain_name] = captain_team_counts.get(captain_name, 0) + 1

            if team_num not in captain_first_frame_by_team:
                captain_first_frame_by_team[team_num] = frame
            captain_last_frame_by_team[team_num] = frame

            prev = captain_seen_by_team.get(team_num)
            if prev != captain_name:
                if prev is not None:
                    captain_changes_by_team[team_num] = int(captain_changes_by_team.get(team_num, 0) or 0) + 1
                captain_seen_by_team[team_num] = captain_name
                captain_events.append({
                    'frame': frame,
                    'team': team_num,
                    'captain': captain_name,
                    'event': 'set' if prev is None else 'switch',
                })

    captains_by_team: dict = {}
    captain_stats_by_team: dict = {}
    for team_num in (1, 2):
        counts = captain_counts_by_team.get(team_num, {})
        if not counts:
            continue
        ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0].lower()))
        primary = ordered[0][0]
        key = str(team_num)
        captains_by_team[key] = primary
        captain_stats_by_team[key] = {
            'primary': primary,
            'sightings': int(counts.get(primary, 0) or 0),
            'switches': int(captain_changes_by_team.get(team_num, 0) or 0),
            'first_frame': int(captain_first_frame_by_team.get(team_num, 0) or 0),
            'last_frame': int(captain_last_frame_by_team.get(team_num, 0) or 0),
        }

    # Compute dominant team per pnum from player_team_history (most frames spent on).
    # This handles players who switch teams mid-match: we use the team they played
    # the majority of the game on, rather than just their final state.
    _pth = player_team_history or {}
    dominant_team_by_pnum: dict = {}
    for _pnum, _entries in _pth.items():
        if not _entries:
            continue
        _sorted = sorted(_entries, key=lambda x: x[0])
        _team_frames: dict = {}
        for _i, (_f, _t) in enumerate(_sorted):
            _end = _sorted[_i + 1][0] if _i + 1 < len(_sorted) else frame_count
            if _t:
                _team_frames[_t] = _team_frames.get(_t, 0) + max(0, _end - _f)
        if _team_frames:
            dominant_team_by_pnum[int(_pnum)] = max(_team_frames, key=lambda t: _team_frames[t])

    # Map player_teams from pnum→team to name→team.
    # Use the dominant team (most frames) so a player who switches teams near the
    # end of a match is classified on their primary team.
    # If two DIFFERENT pnums share the same name on different dominant teams,
    # the assignment is genuinely ambiguous → use 0 to avoid team-kill misclassification.
    name_to_pnums: dict = {}   # nm → list of pnums
    for pnum, _t in player_teams.items():
        if pnum not in player_names:
            continue
        nm = player_names[pnum]
        name_to_pnums.setdefault(nm, []).append(int(pnum))

    teams_by_name: dict = {}
    for nm, pnums in name_to_pnums.items():
        team_set = set()
        for pn in pnums:
            # Prefer dominant-team; fall back to current player_teams value.
            t = dominant_team_by_pnum.get(pn) or int(player_teams.get(pn, 0) or 0)
            if t:
                team_set.add(t)
        if len(team_set) == 1:
            teams_by_name[nm] = next(iter(team_set))
        elif len(team_set) > 1:
            teams_by_name[nm] = 0   # genuinely ambiguous (two slots, different teams)

    teams_by_slot: dict = {}
    for pnum, t in player_teams.items():
        if pnum not in player_names:
            continue
        teams_by_slot[int(pnum)] = int(t or 0)

    # Build slot-team timelines from CS_PLAYERSKINS updates so team lookup can
    # be resolved at the event frame instead of using only final slot state.
    player_team_history = player_team_history or {}
    team_timeline_by_slot: dict = {}
    for _pnum, _entries in player_team_history.items():
        if not _entries:
            continue
        _timeline: list = []
        for _frame, _team in _entries:
            try:
                _f = int(_frame)
                _t = int(_team or 0)
            except Exception:
                continue
            if _timeline and _timeline[-1][0] == _f:
                _timeline[-1] = (_f, _t)
            elif _timeline and _timeline[-1][1] == _t:
                continue
            else:
                _timeline.append((_f, _t))
        if _timeline:
            team_timeline_by_slot[int(_pnum)] = (
                [x[0] for x in _timeline],
                [x[1] for x in _timeline],
            )

    def _team_for_slot_at_frame(slot: int, frame: int) -> int:
        try:
            s = int(slot)
            f = int(frame)
        except Exception:
            return 0
        timeline = team_timeline_by_slot.get(s)
        if timeline:
            frames_for_slot, teams_for_slot = timeline
            idx = bisect_right(frames_for_slot, f) - 1
            if idx >= 0:
                return int(teams_for_slot[idx] or 0)
        return int(teams_by_slot.get(s, 0) or 0)

    def _candidate_teams_for_side(ev: dict, side: str) -> set:
        fr = int(ev.get('frame', frame_count) or frame_count)
        vals: set = set()
        for s in (ev.get(side + '_slots') or []):
            try:
                t = _team_for_slot_at_frame(int(s), fr)
            except Exception:
                t = 0
            if t:
                vals.add(t)
        return vals

    def _team_for_side(ev: dict, side: str) -> int:
        fr = int(ev.get('frame', frame_count) or frame_count)
        slot_key = side + '_slot'
        slots_key = side + '_slots'
        slot = ev.get(slot_key)
        if slot is not None:
            try:
                t = _team_for_slot_at_frame(int(slot), fr)
            except Exception:
                t = 0
            if t:
                return t

        slots = ev.get(slots_key) or []
        if slots:
            cand = _candidate_teams_for_side(ev, side)
            if len(cand) == 1:
                return next(iter(cand))

        return int(teams_by_name.get(ev.get(side), 0) or 0)

    # Tag team kills (killer and victim on same known team).
    # In AQ2 TNG, during-round TKs penalise the killer (Subtract_Frag) and
    # after-round TKs carry no score change at all — neither case is a +1 frag.
    for k in kills:
        if k.get('suicide'):
            # Mark suicides as non-enemy events for aggregate consumers that
            # filter by team_kill=0 (weapon stats, rivals, etc.).
            k['team_kill'] = True
            k['killer_team'] = int(_team_for_side(k, 'killer') or 0)
            k['victim_team'] = int(_team_for_side(k, 'victim') or 0)
            continue
        kt = _team_for_side(k, 'killer')
        vt = _team_for_side(k, 'victim')

        # If one side is still unresolved but all slot candidates for that
        # side agree on one team, use that team.
        if kt == 0:
            cand = _candidate_teams_for_side(k, 'killer')
            if len(cand) == 1:
                kt = next(iter(cand))
        if vt == 0:
            cand = _candidate_teams_for_side(k, 'victim')
            if len(cand) == 1:
                vt = next(iter(cand))

        k['killer_team'] = int(kt or 0)
        k['victim_team'] = int(vt or 0)
        k['team_kill'] = (kt != 0 and kt == vt)

    # Split same-visible-name players into slot identities so downstream stats
    # and DB rows don't collapse distinct players into one name bucket.
    #
    # For duplicate final names, prefer a slot-specific alias that appears in
    # kill/death events for that slot (e.g. pre-troll name) so shots/hits and
    # frags/deaths stay merged under one identity.
    base_name_counts: dict = {}
    base_name_slots: dict = {}
    for _pnum, _nm in player_names.items():
        _slot = int(_pnum)
        base_name_counts[_nm] = base_name_counts.get(_nm, 0) + 1
        base_name_slots.setdefault(_nm, []).append(_slot)
    duplicate_names = {nm for nm, cnt in base_name_counts.items() if cnt > 1}

    duplicate_slots = {
        int(_pnum) for _pnum, _nm in player_names.items()
        if _nm in duplicate_names
    }

    slot_name_votes: dict = {}

    def _vote_slot_name(slot, nm):
        if slot is None or not nm:
            return
        try:
            ss = int(slot)
        except Exception:
            return
        d = slot_name_votes.setdefault(ss, {})
        d[nm] = d.get(nm, 0) + 1

    for _k in kills:
        _ks = _k.get('killer_slot')
        if _ks is None:
            _cands = _k.get('killer_slots') or []
            if len(_cands) == 1:
                _ks = _cands[0]
        _vote_slot_name(_ks, _k.get('killer'))

        _vs = _k.get('victim_slot')
        if _vs is None:
            _cands = _k.get('victim_slots') or []
            if len(_cands) == 1:
                _vs = _cands[0]
        _vote_slot_name(_vs, _k.get('victim'))

    slot_preferred_alias: dict = {}
    for _slot in duplicate_slots:
        _votes = slot_name_votes.get(_slot, {})
        if not _votes:
            continue
        _cands = []
        for _nm, _cnt in _votes.items():
            if not _nm:
                continue
            if _nm in duplicate_names:
                continue
            if _nm == '[MVDSPEC]':
                continue
            if alias_owners.get(_nm, {_slot}) != {_slot}:
                continue
            _cands.append((_cnt, _nm))
        if _cands:
            _cands.sort(key=lambda x: (-x[0], x[1].lower()))
            slot_preferred_alias[_slot] = _cands[0][1]

    slot_identity_names: dict = {}
    for _pnum, _nm in player_names.items():
        _slot = int(_pnum)
        if _nm in duplicate_names:
            _preferred = slot_preferred_alias.get(_slot)
            if _preferred:
                slot_identity_names[_slot] = _preferred
            else:
                slot_identity_names[_slot] = f'{_nm} #{_slot}'
        else:
            slot_identity_names[_slot] = _nm

    # Ensure labels are unique even if preferred aliases collide.
    identity_counts: dict = {}
    for _label in slot_identity_names.values():
        identity_counts[_label] = identity_counts.get(_label, 0) + 1
    for _slot, _label in list(slot_identity_names.items()):
        if identity_counts.get(_label, 0) > 1 and not _label.endswith(f' #{_slot}'):
            slot_identity_names[_slot] = f'{_label} #{_slot}'

    # Slot activity is used as a late tie-breaker for ambiguous obituary names.
    slot_activity: dict = {}
    for _h in hit_events:
        _s = _h.get('attacker')
        if _s is None:
            continue
        try:
            _slot = int(_s)
        except Exception:
            continue
        slot_activity[_slot] = slot_activity.get(_slot, 0) + 1
    for _mf in (muzzle_flashes or []):
        try:
            _slot = int(_mf.get('client'))
        except Exception:
            continue
        slot_activity[_slot] = slot_activity.get(_slot, 0) + 1
    for _ss in (silencer_shots or []):
        try:
            _slot = int(_ss.get('client'))
        except Exception:
            continue
        slot_activity[_slot] = slot_activity.get(_slot, 0) + 1

    def _identity_for_event_side(ev: dict, side: str) -> str:
        nm = ev.get(side)
        if not nm:
            return nm

        slot = ev.get(side + '_slot')
        if slot is not None:
            try:
                ss = int(slot)
            except Exception:
                ss = None
            if ss is not None and ss in duplicate_slots:
                ident = slot_identity_names.get(ss)
                if ident:
                    return ident

        candidate_slots: list = []
        for s in (ev.get(side + '_slots') or []):
            try:
                ss = int(s)
            except Exception:
                continue
            candidate_slots.append(ss)

        if not candidate_slots and nm in duplicate_names:
            candidate_slots = list(base_name_slots.get(nm, []))

        # If side-team is known, use team-at-frame to narrow to one slot.
        side_team = int(ev.get(side + '_team') or 0)
        if side_team:
            fr = int(ev.get('frame', frame_count) or frame_count)
            by_team = [
                s for s in candidate_slots
                if int(_team_for_slot_at_frame(s, fr) or 0) == side_team
            ]
            if len(by_team) == 1:
                ident = slot_identity_names.get(by_team[0])
                if ident:
                    return ident
            if by_team:
                candidate_slots = by_team

        # Last-resort tie-break: one candidate has clear activity and others
        # have none in this replay.
        if len(candidate_slots) > 1:
            weights = [(slot_activity.get(s, 0), s) for s in candidate_slots]
            top_weight = max((w for w, _ in weights), default=0)
            top_slots = [s for w, s in weights if w == top_weight]
            if top_weight > 0 and len(top_slots) == 1:
                ident = slot_identity_names.get(top_slots[0])
                if ident:
                    return ident

        cand: list = []
        for ss in candidate_slots:
            ident = slot_identity_names.get(ss)
            if ident:
                cand.append(ident)
        uniq = set(cand)
        if len(uniq) == 1:
            return next(iter(uniq))
        return nm

    for k in kills:
        k['killer'] = _identity_for_event_side(k, 'killer')
        k['victim'] = _identity_for_event_side(k, 'victim')

    # Keep parser output player_names slot-keyed but expose identity labels for
    # duplicate-name slots so UI/API/DB all reference the same identity string.
    player_names = {pnum: slot_identity_names.get(int(pnum), nm)
                    for pnum, nm in player_names.items()}

    # Recompute players_with_frames using identity labels after any slot split.
    if all_frame_nums:
        players_with_frames = {
            player_names[n] for n in player_names if n in all_frame_nums
        }
    else:
        active_names = (
            set(k['killer'] for k in kills) |
            set(k['victim'] for k in kills) |
            set(player_names.get(h['attacker'], '') for h in hit_events)
        )
        active_cnums = set(mf['client'] for mf in (muzzle_flashes or []))
        active_names |= {player_names[n] for n in active_cnums if n in player_names}
        active_names.discard('')
        players_with_frames = {
            player_names[n] for n in player_names
            if player_names[n] in active_names
        } or set(player_names.values())

    # Seed identity teams from slot teams so non-kill participants still get a
    # stable team value where available.
    for _slot, _ident in slot_identity_names.items():
        _t = dominant_team_by_pnum.get(int(_slot)) or _team_for_slot_at_frame(_slot, frame_count)
        if _t:
            teams_by_name[_ident] = int(_t)

    # Prefer event-resolved team identity for per-name team assignment. This
    # avoids leaving a player at team 0 when slot history clearly resolves all
    # observed events to one side.
    event_teams_by_name: dict = {}
    for k in kills:
        kt = int(k.get('killer_team') or 0)
        vt = int(k.get('victim_team') or 0)
        if kt:
            event_teams_by_name.setdefault(k.get('killer', ''), set()).add(kt)
        if vt:
            event_teams_by_name.setdefault(k.get('victim', ''), set()).add(vt)

    # Build a reverse map: identity name → slot number for dominant-team lookup.
    _name_to_slot: dict = {v: int(k) for k, v in slot_identity_names.items()}

    resolved_teams_by_name: dict = dict(teams_by_name)
    for nm, tset in event_teams_by_name.items():
        if not nm:
            continue
        if len(tset) == 1:
            resolved_teams_by_name[nm] = next(iter(tset))
        elif len(tset) > 1:
            # Conflict (player switched teams mid-match). Prefer the dominant
            # team (most frames spent on) seeded above rather than zeroing out.
            _slot_n = _name_to_slot.get(nm)
            _dom = dominant_team_by_pnum.get(_slot_n) if _slot_n is not None else None
            if _dom and _dom in tset:
                resolved_teams_by_name[nm] = _dom
            else:
                resolved_teams_by_name[nm] = 0
    teams_by_name = resolved_teams_by_name

    # ── Spatial samples for analytics ───────────────────────────────────────
    # frames contains full snapshots in viewer mode and sparse snapshots in
    # stats_only mode. frame_lookup/sampled_frames are built here because they
    # are consumed by _coord_for_slot_at_frame below. The actual position_samples
    # list is populated later (after active_round_windows is known) so warmup,
    # pre-ACTION countdown and between-round movement are excluded from
    # distance-based analytics like aggression_index.
    position_samples: list = []
    frame_lookup: dict = {}
    sampled_frames: list = []
    for _fr in frames:
        _fidx = int(_fr.get('t', 0) or 0)
        _players = _fr.get('players') or {}
        frame_lookup[_fidx] = _players
        sampled_frames.append(_fidx)

    sampled_frames = sorted(set(sampled_frames))

    def _resolve_event_slot(ev: dict, side: str) -> Optional[int]:
        _slot = ev.get(side + '_slot')
        if _slot is not None:
            try:
                return int(_slot)
            except Exception:
                return None
        _slots = ev.get(side + '_slots') or []
        if len(_slots) == 1:
            try:
                return int(_slots[0])
            except Exception:
                return None
        return None

    def _coord_for_slot_at_frame(slot: Optional[int], frame: int):
        if slot is None or not sampled_frames:
            return None, None, None
        try:
            _slot = int(slot)
            _frame = int(frame)
        except Exception:
            return None, None, None

        # Use nearest sampled frame (prefer <= frame for causality).
        idx = bisect_right(sampled_frames, _frame) - 1
        cand_frames = []
        if idx >= 0:
            cand_frames.append(sampled_frames[idx])
        if idx + 1 < len(sampled_frames):
            cand_frames.append(sampled_frames[idx + 1])
        if not cand_frames:
            return None, None, None

        best = min(cand_frames, key=lambda f: abs(f - _frame))
        pdata = (frame_lookup.get(best) or {}).get(_slot)
        if not pdata:
            return None, None, None
        return (
            float(pdata.get('x', 0.0) or 0.0),
            float(pdata.get('y', 0.0) or 0.0),
            float(pdata.get('z', 0.0) or 0.0),
        )

    kill_points: list = []
    for _k in kills:
        _kf = int(_k.get('frame', 0) or 0)
        _ks = _resolve_event_slot(_k, 'killer')
        _vs = _resolve_event_slot(_k, 'victim')
        kx, ky, kz = _coord_for_slot_at_frame(_ks, _kf)
        vx, vy, vz = _coord_for_slot_at_frame(_vs, _kf)
        kill_points.append({
            'frame': _kf,
            'killer': _k.get('killer', ''),
            'victim': _k.get('victim', ''),
            'weapon': _k.get('weapon', 'unknown'),
            'location': _k.get('location', 'unknown'),
            'team_kill': bool(_k.get('team_kill')),
            'killer_x': kx,
            'killer_y': ky,
            'killer_z': kz,
            'victim_x': vx,
            'victim_y': vy,
            'victim_z': vz,
        })

    def _is_no_score_telefrag(k: dict) -> bool:
        # AQ2 source (ClientObituary): in teamplay, MOD_TELEFRAG does not call
        # Add_Frag/Add_Death (no score/death change), though obituary is printed.
        kt = int(k.get('killer_team') or teams_by_name.get(k.get('killer'), 0) or 0)
        return (
            not k.get('suicide')
            and not k.get('team_kill')
            and k.get('weapon') == 'Telefrag'
            and kt != 0
        )

    # Team scores: enemy kills only (TKs excluded)
    team_scores = {1: 0, 2: 0, 0: 0}
    for k in kills:
        if not k['team_kill'] and not k.get('suicide') and not _is_no_score_telefrag(k):
            t = int(k.get('killer_team') or teams_by_name.get(k.get('killer'), 0) or 0)
            team_scores[t] = team_scores.get(t, 0) + 1

    # Round wins and score lines may use explicit numeric team labels
    # ("Team 1", "team2", or just "2") or named labels ("Blue").
    # Do not treat arbitrary digits inside custom names (e.g. "fs plus6")
    # as team numbers.
    def _team_num(name: str) -> int:
        s = _normalize_round_print_text(name)
        if not s:
            return 0
        low = re.sub(r'[\s_-]+', ' ', s).strip().lower()

        # Older logs can wrap team labels in brackets/parentheses, e.g. "[3]".
        # Unwrap once (or twice for nested wrappers) before numeric matching.
        for _ in range(2):
            m_wrap = re.match(r'^[\[(\{]\s*(.*?)\s*[\])\}]$', low)
            if not m_wrap:
                break
            low = (m_wrap.group(1) or '').strip().lower()

        # Explicit numeric forms seen in older demos/logs.
        m = re.match(r'^(?:team\s*)?(\d+)$', low, re.IGNORECASE)
        if m:
            # Only 1/2 are canonical team IDs in our data model.
            # Legacy demos sometimes print teams as 3/4; treat those as
            # unresolved here so score-position fallback can map them to sides.
            n = int(m.group(1))
            return n if n in (1, 2) else 0

        # Common textual labels for legacy team broadcasts.
        if low in ('one', 'team one', 'red', 'team red', 'alpha'):
            return 1
        if low in ('two', 'team two', 'blue', 'team blue', 'beta'):
            return 2
        return 0

    score_events = [ev for ev in round_events if ev['type'] == 'score']
    score_name_to_team: dict = {}
    for ev in score_events:
        for _key, _fallback in (('team1_name', 1), ('team2_name', 2), ('team3_name', 3)):
            _nm = _normalize_round_print_text(ev.get(_key) or '')
            if not _nm:
                continue
            _nk = _team_label_key(_nm)
            # Score lines are emitted as TeamName(TEAM1/TEAM2/TEAM3), so side
            # position is authoritative for mapping labels to team slots.
            _cand = _fallback
            _prev = score_name_to_team.get(_nk)
            if _prev is None:
                score_name_to_team[_nk] = _cand
            elif _prev != _cand:
                # Same visible label used for multiple score sides in one file.
                # Mark as ambiguous so win mapping can fall back to kill evidence.
                score_name_to_team[_nk] = 0

    scoring_kill_frames: list[int] = []
    scoring_kill_teams: list[int] = []
    for _k in kills:
        if _k.get('team_kill') or _k.get('suicide') or _is_no_score_telefrag(_k):
            continue
        _kt = int(_k.get('killer_team') or teams_by_name.get(_k.get('killer'), 0) or 0)
        if _kt in (1, 2):
            scoring_kill_frames.append(int(_k.get('frame', 0) or 0))
            scoring_kill_teams.append(_kt)

    round_wins: dict = {1: 0, 2: 0}
    round_ties: int  = 0
    _unknown_win_count = 0
    _last_round_boundary = -1
    for ev in round_events:
        _etype = ev.get('type')
        _ev_frame = int(ev.get('frame', 0) or 0)
        if _etype == 'win':
            _name = _normalize_round_print_text(ev.get('team_name', ''))
            _k = _team_label_key(_name)
            _mapped = score_name_to_team.get(_k, None)
            if _mapped == 0:
                t = 0
            elif _mapped in (1, 2):
                t = _mapped
            else:
                t = _team_num(_name)
            if t not in round_wins:
                _unknown_win_count += 1

            # Legacy logs may emit winner labels that don't map to canonical
            # team ids. Recover winner from the last scoring kill in this
            # round window (between previous boundary and this win event).
            if t not in round_wins and scoring_kill_frames:
                _idx = bisect_right(scoring_kill_frames, _ev_frame) - 1
                while _idx >= 0 and scoring_kill_frames[_idx] > _last_round_boundary:
                    _cand = scoring_kill_teams[_idx]
                    if _cand in round_wins:
                        t = _cand
                        break
                    _idx -= 1

            if t in round_wins:
                round_wins[t] += 1
            _last_round_boundary = _ev_frame
        elif _etype == 'tie':
            round_ties += 1
            _last_round_boundary = _ev_frame

    def _resolve_score_side_teams(score_ev: dict) -> tuple[int, int]:
        _n1 = _normalize_round_print_text(score_ev.get('team1_name') or '')
        _n2 = _normalize_round_print_text(score_ev.get('team2_name') or '')
        _t1 = score_name_to_team.get(_team_label_key(_n1), 0) or _team_num(_n1)
        _t2 = score_name_to_team.get(_team_label_key(_n2), 0) or _team_num(_n2)
        if _t1 in (1, 2) and _t2 in (1, 2) and _t1 != _t2:
            return _t1, _t2

        # Ambiguous labels (duplicate/legacy aliases): infer orientation by
        # whichever side best matches known round-win totals.
        _s1 = int(score_ev.get('score1', 0) or 0)
        _s2 = int(score_ev.get('score2', 0) or 0)
        _r1 = int(round_wins.get(1, 0) or 0)
        _r2 = int(round_wins.get(2, 0) or 0)
        if (_r1 + _r2) > 0:
            _direct = abs(_s1 - _r1) + abs(_s2 - _r2)
            _swap = abs(_s1 - _r2) + abs(_s2 - _r1)
            if _direct < _swap:
                return 1, 2
            if _swap < _direct:
                return 2, 1

        # Final deterministic fallback: map score side order to team ids.
        return 1, 2

    # If the file contains authoritative "Current score" messages, the last one
    # gives the definitive per-team kill total — use it to override the kill-count.
    if score_events:
        last_score = max(score_events, key=lambda e: e['frame'])
        t1, t2 = _resolve_score_side_teams(last_score)
        team_scores[t1] = last_score['score1']
        team_scores[t2] = last_score['score2']

        # When winner labels are ambiguous but score lines track round totals,
        # trust the final score split for round_wins side distribution.
        if _unknown_win_count > 0:
            _win_events = sum(1 for _ev in round_events if _ev.get('type') == 'win')
            _score_total = int(last_score.get('score1', 0) or 0) + int(last_score.get('score2', 0) or 0)
            if _win_events > 0 and abs(_score_total - _win_events) <= max(1, round_ties):
                round_wins[t1] = int(last_score.get('score1', 0) or 0)
                round_wins[t2] = int(last_score.get('score2', 0) or 0)

    # ── Active-round windows ─────────────────────────────────────────────────
    # The game only tracks shots, hits, and deaths during live rounds
    # (team_round_going=1, !in_warmup). We use "ACTION!" centerprints as the
    # authoritative round-start signal (team_round_going=1 fires ~20 ms after
    # ACTION!, always within the same 100 ms demo frame). Falls back to the
    # first enemy kill after each boundary for demos without ACTION! frames.
    action_frames      = sorted(set(e['frame'] for e in round_events
                                   if e.get('type') == 'action'))
    lights_frames      = sorted(set(e['frame'] for e in round_events
                                   if e.get('type') == 'lights'))
    enemy_kill_frames  = sorted(
        k['frame'] for k in kills
        if not k['team_kill'] and not k.get('suicide') and not _is_no_score_telefrag(k)
    )

    def _first_frame_in(source: list, after_frame: int, before_frame: int) -> Optional[int]:
        for f in source:
            if after_frame < f < before_frame:
                return f
        return None

    def _first_enemy_kill_after(after_frame: int) -> Optional[int]:
        for f in enemy_kill_frames:
            if f > after_frame:
                return f
        return None

    window_bounds = [0] + list(round_starts) + [frame_count]
    # Combat window: starts at ACTION! (players can shoot). Used by shot/hit/kill stats.
    active_round_windows: list = []
    # Movement window: starts at LIGHTS (players spawn and can move during the
    # lights/camera/action countdown, but cannot shoot). Used by position-sample
    # based distance metrics. Falls back to ACTION! if LIGHTS was not captured.
    movement_round_windows: list = []
    for _i in range(len(window_bounds) - 1):
        _bnd_start = window_bounds[_i]
        _bnd_end   = window_bounds[_i + 1]
        _action = _first_frame_in(action_frames, _bnd_start, _bnd_end)
        _first = _action
        if _first is None:
            _first = _first_enemy_kill_after(_bnd_start)
        if _first is None and _i == 0:
            _first = 0
        if _first is not None and _first < _bnd_end:
            active_round_windows.append((_first, _bnd_end))

        _lights = _first_frame_in(lights_frames, _bnd_start, _bnd_end)
        _move_start = _lights if _lights is not None else _first
        if _move_start is not None and _move_start < _bnd_end:
            movement_round_windows.append((_move_start, _bnd_end))

    def _is_active_round(frame: int) -> bool:
        return any(s <= frame <= e for s, e in active_round_windows)

    def _is_movement_round(frame: int) -> bool:
        return any(s <= frame <= e for s, e in movement_round_windows)

    # Emit position samples only for frames inside the movement window
    # (LIGHTS → round end). Players spawn at LIGHTS and can move during the
    # lights/camera/action countdown; that movement is part of the round
    # even though shooting isn't enabled yet. Warmup roaming and post-round
    # scoreboard movement stay excluded.
    for _fr in frames:
        _fidx = int(_fr.get('t', 0) or 0)
        if not _is_movement_round(_fidx):
            continue
        _players = _fr.get('players') or {}
        for _slot, _pos in _players.items():
            try:
                _s = int(_slot)
            except Exception:
                continue
            _name = player_names.get(_s)
            if not _name:
                continue
            _team = int(_team_for_slot_at_frame(_s, _fidx) or teams_by_name.get(_name, 0) or 0)
            position_samples.append({
                'frame': _fidx,
                'name': _name,
                'team': _team,
                'x': float(_pos.get('x', 0.0) or 0.0),
                'y': float(_pos.get('y', 0.0) or 0.0),
                'z': float(_pos.get('z', 0.0) or 0.0),
            })

    # Tally per-player frags/deaths.
    # AQ2 TNG behavior:
    # - Enemy kill: Add_Frag(killer) + Add_Death(victim, true)
    # - Suicide/world: Subtract_Frag(self) + Add_Death(self, true)
    # - Team kill: during active round -> Subtract_Frag(killer) + Add_Death(victim, false)
    #              after round (ff_afterround=1 typical) -> no score/death change
    kill_counts       = {n: 0 for n in player_names.values()}
    death_counts      = {n: 0 for n in player_names.values()}
    team_kill_counts  = {}   # {name: count} — TKs only
    # AQ2 ClientObituary only calls Subtract_Frag(self) + Add_Death(self) when
    # team_round_going is true (or the game is non-team / ff_afterround is off).
    # Per p_client.c:901:
    #   if (!teamplay->value || team_round_going || !ff_afterround->value) {
    #       Subtract_Frag(self); Add_Death(self, true);
    #   }
    # Post-round plummets/lava/etc print the obituary but never touch the
    # scoreboard. In DM (no rounds) all suicides still count. Tag the events
    # so downstream consumers (db.py per-round aggregation) can skip them too.
    for k in kills:
        if k.get('suicide') and round_starts and not _is_active_round(k['frame']):
            k['no_score'] = True

    for k in kills:
        if k.get('suicide'):
            if k.get('no_score'):
                continue
            kill_counts[k['killer']] = kill_counts.get(k['killer'], 0) - 1
            death_counts[k['victim']] = death_counts.get(k['victim'], 0) + 1
        elif k['team_kill']:
            team_kill_counts[k['killer']] = team_kill_counts.get(k['killer'], 0) + 1
            if _is_active_round(k['frame']):
                # During-round TK penalties (ff_afterround behavior is handled
                # by active-round windowing): killer loses one frag, victim dies.
                kill_counts[k['killer']] = kill_counts.get(k['killer'], 0) - 1
                death_counts[k['victim']] = death_counts.get(k['victim'], 0) + 1
        else:
            if _is_no_score_telefrag(k):
                continue
            kill_counts[k['killer']] = kill_counts.get(k['killer'], 0) + 1
            death_counts[k['victim']] = death_counts.get(k['victim'], 0) + 1
        # Source killPlayer() punish logic subtracts one frag from the victim.
        if k.get('victim_frag_penalty') and not k.get('suicide') and not k['team_kill']:
            penalty = int(k.get('victim_frag_penalty', 1) or 1)
            kill_counts[k['victim']] = kill_counts.get(k['victim'], 0) - penalty

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

    # shots_fired / shots_by_mz: only count shots fired DURING active rounds.
    # The game's Stats_AddShot is gated on team_round_going=1 && !in_warmup,
    # so after-round spray does NOT contribute to the server's shot counter.
    shots_fired: dict = {}    # name → during-round shots
    shots_by_mz: dict = {}    # name → {mz: count}  (during-round only)
    for mf in (muzzle_flashes or []):
        pname = player_names.get(mf['client'])
        if not pname or not _is_active_round(mf['frame']):
            continue
        shots_fired[pname] = shots_fired.get(pname, 0) + 1
        mz_map = shots_by_mz.setdefault(pname, {})
        mz = mf['mz']
        mz_map[mz] = mz_map.get(mz, 0) + 1

    # Silenced-weapon shots: the silencer suppresses SVC_MUZZLEFLASH but the
    # server still plays gi.sound(ent, CHAN_WEAPON, snd_silencer, ...) which is
    # captured as MVD_SOUND.  Count these as shots so accuracy stays accurate.
    # (Stats_AddShot in p_weapon.c is called before the silencer check.)
    for ss in (silencer_shots or []):
        pname = player_names.get(ss['client'])
        if not pname or not _is_active_round(ss['frame']):
            continue
        shots_fired[pname] = shots_fired.get(pname, 0) + 1
        # Silenced shots don't carry an MZ wire code; leave shots_by_mz unchanged.

    # Hits during active round: the game's Stats_AddHit has the same guard as
    # Stats_AddShot (team_round_going && !in_warmup).  Use this for accuracy.
    hit_counts_during_round: dict = {}
    for h in hit_events:
        if not _is_active_round(h['frame']):
            continue
        hname = player_names.get(h['attacker'], str(h['attacker']))
        hit_counts_during_round[hname] = hit_counts_during_round.get(hname, 0) + 1

    # Accuracy: during-round hits / during-round shots as a percentage.
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
        hits = hit_counts_during_round.get(name, 0)
        # Silenced shots are now tracked via MVD_SOUND, so hits > shots should
        # only occur in edge cases (PHS-filtered MZ, incomplete demo, etc.).
        # Still suppress rather than clamp if it happens.
        if hits > shots:
            continue
        accuracy[name] = round(hits / shots * 100, 1)

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
        'Falling': 0, 'Trigger Hurt': 0, 'Slime': 0, 'Lava': 0,
        'Suicide': 0, 'Suicide Punish': 0, 'World': 0, 'Glass': 0,
        'Blaster': 0, 'Hyperblaster': 0, 'Shotgun': 0, 'Machinegun': 0, 'Chaingun': 0,
        'Rocket': 0, 'Railgun': 0, 'BFG': 0, 'Telefrag': 0,
    }
    _LOC_MULT = {'head': 1.8, 'chest': 0.65, 'stomach': 0.40, 'legs': 0.25, 'unknown': 0.55,
                  'kvlr_helmet': 0.5, 'kvlr_vest': 0.1}  # armored hits: helmet=dmg/2, vest=dmg/10
    _SHOTGUN_WEAPONS = {'M3', 'HC'}

    # Per-player weapon kill profile  {name: {weapon: count}}
    player_weapon_kills: dict = {}
    for k in kills:
        if k.get('suicide'):
            continue
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

    # The end-of-match SVC_LAYOUT scoreboard carries server-computed Damage and
    # Acc values — identical to what the stat-collection tool reports to aq2.rocks.
    # These are ground truth: the game calculates actual T_Damage() totals
    # including HP-cap on killing shots, exact pellet damage, and real armor
    # reduction.  Prefer them over our heuristic inference wherever present.
    # For old demos without hit events the scoreboard also provides K/D fallbacks.
    if layout_scoreboard_by_name:
        known_players = set(player_names.values())
        for name, st in layout_scoreboard_by_name.items():
            if name not in known_players:
                continue

            lk  = int(st.get('kills',  0) or 0)
            ld  = int(st.get('deaths', 0) or 0)
            ldm = int(st.get('damage', 0) or 0)
            la  = int(st.get('acc',    0) or 0)

            # K/D: only backfill when our event-based counts are zero
            # (event stream is always more granular than scoreboard integers).
            if kill_counts.get(name, 0) == 0 and lk > 0:
                kill_counts[name] = lk
            if death_counts.get(name, 0) == 0 and ld > 0:
                death_counts[name] = ld

            # Damage: server value is ground truth — always prefer it.
            if ldm > 0:
                damage_dealt[name] = ldm

            # Accuracy: server value is ground truth — always prefer it.
            if la > 0:
                accuracy[name] = float(la)

            # Hit-count estimate from shots × layout accuracy (backfill only).
            if hit_counts.get(name, 0) == 0 and shots_fired.get(name, 0) > 0 and la > 0:
                est_hits = int(round(shots_fired[name] * (la / 100.0)))
                if est_hits > 0:
                    hit_counts[name] = est_hits

    # Headshot kills per player (killing shots to the head)
    headshot_kills: dict = {}
    for k in kills:
        if not k.get('suicide') and k.get('location') == 'head':
            headshot_kills[k['killer']] = headshot_kills.get(k['killer'], 0) + 1

    # Best kill streak per player: max consecutive enemy kills between own deaths.
    best_kill_streak: dict = {}
    _cur_streak: dict      = {}

    def _counts_as_death_event(k: dict) -> bool:
        if k.get('suicide'):
            # Post-round plummets/lava do not call Add_Death (p_client.c:901).
            return not k.get('no_score')
        if k.get('team_kill'):
            return _is_active_round(k['frame'])
        if _is_no_score_telefrag(k):
            return False
        return True

    for k in sorted(kills, key=lambda x: x['frame']):
        if not k['team_kill'] and not k.get('suicide') and not _is_no_score_telefrag(k):
            killer = k['killer']
            _cur_streak[killer] = _cur_streak.get(killer, 0) + 1
            if _cur_streak[killer] > best_kill_streak.get(killer, 0):
                best_kill_streak[killer] = _cur_streak[killer]
        # Only deaths that count in AQ2 stats reset streaks.
        if _counts_as_death_event(k):
            victim = k['victim']
            _cur_streak[victim] = 0

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
        'kill_counts':       kill_counts,
        'death_counts':      death_counts,
        'team_kill_counts':  team_kill_counts,
        'weapon_counts':  weapon_counts,
        'loc_counts':     loc_counts,
        'kill_loc_counts': kill_loc_counts,
        'award_counts':   award_counts,
        'hit_counts':         hit_counts,
        'hit_loc_by_player':  hit_loc_by_player,
        'shots_fired':        shots_fired,
        'shots_list':         [[mf['frame'], mf['client']]
                               for mf in (muzzle_flashes or [])
                               if player_names.get(mf['client']) and _is_active_round(mf['frame'])],
        'accuracy':           accuracy,
        'damage_dealt':       damage_dealt,
        'headshot_kills':     headshot_kills,
        'best_kill_streak':   best_kill_streak,
        'player_teams':       teams_by_name,   # {name: 1 or 2}
        'team_scores':        {str(k): v for k, v in team_scores.items() if k != 0},
        'round_wins':         {str(k): v for k, v in round_wins.items()},
        'round_ties':         round_ties,
        'ghost_clients':      ghost_clients,
        'round_start_frames': round_starts,
        'players_with_frames': sorted(players_with_frames),
        'position_samples': position_samples,
        'kill_points': kill_points,
        'captains_by_team': captains_by_team,
        'captain_stats_by_team': captain_stats_by_team,
        'captain_events': captain_events,
    }


def load_mvd2(path: str, max_frames: int = 0, stats_only: bool = False) -> dict:
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
    return parse_mvd2(data, max_frames, stats_only=stats_only)
