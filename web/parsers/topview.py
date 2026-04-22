"""
Top-down SVG map renderer for Q2 BSP files.

Reads the Quake 2 palette from colormap.pcx, decodes WAL/PNG/JPG textures,
then outputs an SVG of all upward-facing (floor/roof) faces coloured with
the average texture colour, height-shaded for depth perception.

The SVG viewBox uses world-space coordinates (with a Y-flip transform) so
the client can align it on the canvas using the world bounds from topview.json.
Out-of-bounds sky/terrain geometry is excluded automatically by cropping the
viewBox to the playable area derived from player spawn points.
"""
import os
import struct
from functools import lru_cache

from PIL import Image

# ── Q2 palette ─────────────────────────────────────────────────────────────────

def load_q2_palette(pcx_path: str) -> list:
    """
    Extract the 256-colour RGB palette from a Quake 2 colormap.pcx.
    The palette sits in the last 769 bytes:  0x0C  +  256×(R,G,B).
    Returns a list of 256 (r, g, b) tuples.
    """
    with open(pcx_path, 'rb') as f:
        f.seek(-769, 2)
        marker = f.read(1)
        if marker != b'\x0c':
            raise ValueError(f"Bad PCX palette marker {marker!r} in {pcx_path}")
        raw = f.read(768)
    return [(raw[i*3], raw[i*3+1], raw[i*3+2]) for i in range(256)]


# ── WAL decoder ────────────────────────────────────────────────────────────────

WAL_HEADER_FMT = '<32sII4I32siii'   # name,w,h,offsets[4],animname,flags,contents,value
WAL_HEADER_SIZE = struct.calcsize(WAL_HEADER_FMT)  # 100 bytes

def decode_wal(data: bytes, palette: list) -> Image.Image:
    """Decode a WAL texture into an RGB PIL image using the Q2 palette."""
    hdr = struct.unpack_from(WAL_HEADER_FMT, data, 0)
    w, h = hdr[1], hdr[2]
    offset = hdr[3]              # offsets[0] = mip level 0
    pixels = data[offset: offset + w * h]
    img = Image.new('RGB', (w, h))
    img.putdata([palette[p] for p in pixels])
    return img


# ── Texture colour cache ───────────────────────────────────────────────────────

def _avg_color(img: Image.Image) -> tuple:
    """Compute average RGB, applying Q2's 2× overbright factor to match in-game brightness."""
    img = img.convert('RGBA').resize((64, 64), Image.BOX)
    pixels = list(img.getdata())
    rs = gs = bs = n = 0
    for r, g, b, a in pixels:
        if a > 32:
            rs += r; gs += g; bs += b; n += 1
    if n == 0:
        return (80, 80, 80)
    # Q2 WAL textures are stored at ~50% display brightness; the engine applies a 2× overbright.
    return (min(255, (rs // n) * 2), min(255, (gs // n) * 2), min(255, (bs // n) * 2))


@lru_cache(maxsize=512)
def texture_color(tex_name: str, tex_root: str, palette_path: str,
                  tex_root2: str = '') -> tuple:
    """
    Return average RGB for a texture name (Q2 path without extension).
    Searches: .png, .jpg, .wal under tex_root (and tex_root2 if given) before
    falling back to name heuristics.
    """
    roots = [r for r in (tex_root, tex_root2) if r]
    for root in roots:
        for ext in ('.png', '.jpg', '.jpeg', '.wal'):
            path = os.path.join(root, tex_name.replace('/', os.sep) + ext)
            if os.path.exists(path):
                try:
                    if ext == '.wal':
                        with open(path, 'rb') as f:
                            raw = f.read()
                        pal = load_q2_palette(palette_path)
                        img = decode_wal(raw, pal)
                    else:
                        img = Image.open(path)
                    return _avg_color(img)
                except Exception:
                    pass
    return _name_color(tex_name)


def list_bsp_textures(bsp_data: bytes) -> list:
    """
    Return a sorted list of unique texture names referenced by a BSP file.
    Useful for pre-fetching textures before rendering the topview SVG.
    """
    lumps    = _read_lumps(bsp_data)
    texinfos = _read_texinfo(bsp_data, lumps)
    names = set()
    for ti in texinfos:
        n = ti['name']
        if n and not n.startswith('__') and n.lower() not in ('trigger', 'clip', 'hint', 'origin'):
            names.add(n)
    return sorted(names)


# ── Name-based colour fallback ─────────────────────────────────────────────────

def _name_color(tex_name: str) -> tuple:
    """
    Infer a plausible colour from the texture path when no texture file exists.
    Covers the flunx/ custom AQ2 set and standard Q2 episode directories.
    """
    n = tex_name.lower()
    leaf = n.rsplit('/', 1)[-1]   # last path component

    # ── flunx/ custom AQ2 urban textures ──────────────────────────────────────
    if 'flunx' in n:
        if 'dirt'     in leaf:              return (90,  72,  50)   # earthy ground
        if 'rooftile' in leaf:              return (138, 105, 72)   # terracotta tile
        if leaf.startswith('f_tile'):       return (152, 142, 125)  # stone tile
        if 'gray'     in leaf:              return (118, 113, 108)  # concrete
        if 'rustmetal'in leaf:              return (118,  75,  45)  # rust
        if 'wood'     in leaf:              return (115,  82,  52)  # weathered wood
        if 'trim'     in leaf:              return ( 92,  80,  68)  # dark trim
        if 'tapet'    in leaf:              return (162, 148, 125)  # wallpaper/plaster
        if 'crate'    in leaf:              return (118,  90,  55)  # wooden crate
        if 'dumpster' in leaf:              return ( 80,  85,  80)  # metal dumpster
        if 'wire'     in leaf:              return ( 75,  78,  82)  # wire mesh
        if 'much'     in leaf:              return (142, 122,  92)  # café interior
        if 'parasol'  in leaf:              return (172, 138,  95)  # canvas awning
        if 'wall'     in leaf:              return (158, 148, 130)  # plastered wall
        return (145, 132, 112)  # generic flunx fallback (warm beige)

    # ── osiris/ props ──────────────────────────────────────────────────────────
    if 'osiris' in n:
        if 'barrel' in leaf:                return (100,  82,  65)
        if 'box'    in leaf or 'crate' in leaf: return (120, 90, 58)
        if 'desk'   in leaf:                return (118,  85,  52)
        if 'door'   in leaf:                return (108,  82,  58)
        return (110, 92, 72)

    # ── kaq2/ AQ2 base textures ────────────────────────────────────────────────
    if 'kaq2' in n:
        if 'door' in leaf:                  return (108,  80,  55)
        return (125, 110, 90)

    # ── wetwired/ textures ────────────────────────────────────────────────────
    if 'wetwired' in n:
        if 'tile' in leaf:                  return (148, 142, 135)
        return (130, 120, 108)

    # ── city/ textures ────────────────────────────────────────────────────────
    if n.startswith('city/'):
        if 'fence' in leaf:                 return ( 80,  80,  80)
        return (135, 120, 100)

    # ── community/ textures ───────────────────────────────────────────────────
    if 'community' in n:                    return (140, 125, 105)

    # ── sandstorm/ — sandy desert/urban outdoor textures ─────────────────────
    if 'sandstorm' in n:
        if 'wall'   in leaf:                return (175, 148, 108)
        if 'floor'  in leaf or 'ground' in leaf: return (168, 140,  95)
        if 'rock'   in leaf or 'stone'  in leaf: return (155, 130,  92)
        if 'wood'   in leaf:                return (140, 105,  65)
        if 'door'   in leaf:                return (120,  88,  55)
        return (165, 138, 100)  # generic sandy fallback

    # ── nemesis/ — Q2-style tech/industrial base ──────────────────────────────
    if 'nemesis' in n:
        if 'concrete' in leaf or 'floor'  in leaf: return (110, 105, 100)
        if 'metal'    in leaf or 'plate'  in leaf: return ( 85,  90,  95)
        if 'brick'    in leaf:                     return (138, 102,  72)
        if 'wood'     in leaf:                     return (118,  85,  55)
        return (100,  98,  95)  # dark industrial fallback

    # ── makkon_* — high-quality modern Q2 replacement texture sets ────────────
    if n.startswith('makkon') or n.startswith('mak_'):
        if 'conc'   in n or 'concrete' in n:
            if 'red'   in n:                return (145,  88,  72)
            if 'grn'   in n:                return ( 90, 110,  85)
            if 'wht'   in n:                return (200, 198, 195)
            return (118, 112, 105)                            # grey concrete
        if 'ind'    in n or 'industrial' in n: return ( 90,  92,  95)
        if 'metal'  in n:                   return ( 85,  90,  95)
        if 'marble' in n:                   return (185, 175, 162)
        if 'stone'  in n:                   return (138, 122, 105)
        if 'nature' in n or 'natu' in n:    return ( 85, 112,  72)
        if 'urban'  in n:                   return (145, 135, 120)
        if 'tech'   in n:                   return ( 88,  95, 100)
        if 'build'  in n:                   return (155, 142, 122)
        return (130, 122, 112)

    # ── Standard Q2 episode directories ──────────────────────────────────────
    if n.startswith('e3'):                  return (155, 130,  88)  # desert/outer base
    if n.startswith('e2'):                  return ( 82,  90,  72)  # sewage/organic
    if n.startswith('e1') or n.startswith('e1u'): return ( 88,  92,  98)  # Q2 base crates/metal

    # ── karr/ — suburban/city outdoor ────────────────────────────────────────
    if n.startswith('karr'):
        if 'grass' in leaf:                 return ( 72, 115,  58)
        if 'wall'  in leaf:                 return (148, 138, 122)
        return (128, 118, 100)

    # ── Generic keyword matching for unknown texture sets ─────────────────────
    for keywords, color in (
        (['sand', 'dust', 'desert', 'dirt', 'mud', 'earth'],   (160, 132,  90)),
        (['stone', 'rock', 'granite', 'cobble'],                (138, 122, 105)),
        (['brick', 'masonry'],                                  (148, 108,  78)),
        (['concrete', 'plaster', 'stucco', 'cement'],           (118, 112, 105)),
        (['wood', 'plank', 'board', 'timber'],                  (118,  85,  55)),
        (['metal', 'steel', 'iron', 'grate', 'plate'],          ( 85,  90,  95)),
        (['tile', 'floor', 'flr'],                              (148, 140, 128)),
        (['grass', 'lawn', 'nature', 'jungle', 'foliage'],      ( 72, 115,  58)),
        (['water', 'pool', 'slime', 'lava'],                    ( 48,  82, 128)),
        (['snow', 'ice'],                                       (208, 215, 222)),
        (['fence', 'wire', 'mesh', 'grill'],                    ( 80,  82,  85)),
        (['door'],                                              (108,  80,  55)),
        (['window', 'glass'],                                   ( 82, 102, 130)),
        (['roof', 'ceil'],                                      (110,  95,  82)),
        (['wall'],                                              (145, 135, 120)),
    ):
        if any(k in n for k in keywords):
            return color

    # Final fallback — warm neutral instead of cold grey
    return (115, 108, 98)


# ── BSP face parsing ───────────────────────────────────────────────────────────

# Lump indices
LUMP_PLANES    = 1
LUMP_VERTICES  = 2
LUMP_TEXINFO   = 5
LUMP_FACES     = 6
LUMP_EDGES     = 11
LUMP_SURFEDGES = 12

def _read_lumps(data: bytes) -> list:
    lumps = []
    for i in range(19):
        off, length = struct.unpack_from('<ii', data, 8 + i * 8)
        lumps.append((off, length))
    return lumps


def _read_planes(data: bytes, lumps: list) -> list:
    """Returns list of (nx, ny, nz, dist) tuples."""
    off, length = lumps[LUMP_PLANES]
    n = length // 20
    planes = []
    for i in range(n):
        nx, ny, nz, dist, _ = struct.unpack_from('<4fi', data, off + i * 20)
        planes.append((nx, ny, nz, dist))
    return planes


def _read_vertices(data: bytes, lumps: list) -> list:
    off, length = lumps[LUMP_VERTICES]
    n = length // 12
    verts = list(struct.unpack_from(f'<{n*3}f', data, off))
    return [(verts[i*3], verts[i*3+1], verts[i*3+2]) for i in range(n)]


def _read_edges(data: bytes, lumps: list) -> list:
    """Returns list of (v0, v1) uint16 pairs."""
    off, length = lumps[LUMP_EDGES]
    n = length // 4
    return [struct.unpack_from('<HH', data, off + i*4) for i in range(n)]


def _read_surfedges(data: bytes, lumps: list) -> list:
    off, length = lumps[LUMP_SURFEDGES]
    n = length // 4
    return list(struct.unpack_from(f'<{n}i', data, off))


def _read_texinfo(data: bytes, lumps: list) -> list:
    """Returns list of dicts with s_vec, t_vec, flags, texture_name."""
    off, length = lumps[LUMP_TEXINFO]
    n = length // 76
    texinfos = []
    for i in range(n):
        base = off + i * 76
        sx, sy, sz, so = struct.unpack_from('<4f', data, base)
        tx, ty, tz, to = struct.unpack_from('<4f', data, base + 16)
        flags  = struct.unpack_from('<I', data, base + 32)[0]
        # texture name at offset 40, 32 bytes
        raw_name = data[base + 40: base + 72]
        name = raw_name.rstrip(b'\x00').decode('latin-1', errors='replace')
        texinfos.append({
            's_vec': (sx, sy, sz), 's_off': so,
            't_vec': (tx, ty, tz), 't_off': to,
            'flags': flags,
            'name':  name,
        })
    return texinfos


# Face struct: plane_num(u16), side(u16), first_edge(i32), num_edges(u16),
#              texinfo(u16), styles(4×u8), lightofs(i32)  = 20 bytes
FACE_FMT  = '<HHiHH4bi'
FACE_SIZE = struct.calcsize(FACE_FMT)   # 20 bytes

def _read_faces(data: bytes, lumps: list) -> list:
    off, length = lumps[LUMP_FACES]
    n = length // FACE_SIZE
    faces = []
    for i in range(n):
        fields = struct.unpack_from(FACE_FMT, data, off + i * FACE_SIZE)
        plane_num, side, first_edge, num_edges, texinfo_id = fields[:5]
        faces.append({
            'plane':      plane_num,
            'side':       side,
            'first_edge': first_edge,
            'num_edges':  num_edges,
            'texinfo':    texinfo_id,
        })
    return faces


def _face_polygon(face: dict, surfedges: list, edges: list, verts: list) -> list:
    """Gather the ordered vertex list for a face."""
    polygon = []
    for k in range(face['num_edges']):
        se = surfedges[face['first_edge'] + k]
        if se >= 0:
            v = edges[se][0]
        else:
            v = edges[-se][1]
        polygon.append(verts[v])
    return polygon


# ── Surface flags — skip nodraw / sky / trigger ────────────────────────────────

SURF_NODRAW  = 0x0080
SURF_SKY     = 0x0004
SURF_SKIP    = 0x0200
SURF_HINT    = 0x0100
_SKIP_FLAGS  = SURF_NODRAW | SURF_SKY | SURF_SKIP | SURF_HINT

# Lump 0 is entities
LUMP_ENTITIES = 0


# ── Entity / spawn parsing ─────────────────────────────────────────────────────

def _entity_spawns(entity_string: str) -> list:
    """
    Extract player spawn origins from a BSP entity string.
    Returns list of {'x', 'y', 'z'} dicts.
    """
    _SPAWN_CLASSES = frozenset({
        'info_player_start', 'info_player_deathmatch',
        'info_player_team1', 'info_player_team2',
    })
    spawns = []
    current: dict = {}
    for line in entity_string.splitlines():
        line = line.strip()
        if line == '{':
            current = {}
        elif line == '}':
            if current.get('classname', '') in _SPAWN_CLASSES:
                try:
                    ox, oy, oz = current['origin'].split()
                    spawns.append({'x': float(ox), 'y': float(oy), 'z': float(oz)})
                except (KeyError, ValueError):
                    pass
            current = {}
        elif line.startswith('"'):
            parts = line.split('"')
            if len(parts) >= 4:
                current[parts[1]] = parts[3]
    return spawns


# ── Playable bounds ────────────────────────────────────────────────────────────

def _playable_bounds(spawns: list, vert_bounds: dict) -> dict:
    """
    Compute a viewable world-space bounding box based on spawn points.

    Uses the spawn centroid ± generous margin so that sky boxes and distant
    out-of-level geometry don't blow out the SVG viewBox.  If no spawns are
    found, falls back to the full vertex bounds.
    """
    if not spawns:
        return {k: vert_bounds[k] for k in ('min_x', 'max_x', 'min_y', 'max_y')}

    sxs = [s['x'] for s in spawns]
    sys_ = [s['y'] for s in spawns]
    sp_min_x, sp_max_x = min(sxs), max(sxs)
    sp_min_y, sp_max_y = min(sys_), max(sys_)

    # Margin = 80 % of spawn span each side, minimum 1 200 units
    mx = max((sp_max_x - sp_min_x) * 0.8, 1200)
    my = max((sp_max_y - sp_min_y) * 0.8, 1200)

    return {
        'min_x': max(sp_min_x - mx, vert_bounds['min_x']),
        'max_x': min(sp_max_x + mx, vert_bounds['max_x']),
        'min_y': max(sp_min_y - my, vert_bounds['min_y']),
        'max_y': min(sp_max_y + my, vert_bounds['max_y']),
    }


# ── Colour helpers ─────────────────────────────────────────────────────────────

def _shade_z(rgb: tuple, z_norm: float) -> tuple:
    """
    Apply height-based brightness to an RGB tuple using a sqrt curve for perceptual contrast.
    z_norm=0 (ground) → 55% brightness.  z_norm=1 (top) → 135%.
    The sqrt curve gives good local contrast for boxes/stairs even on large-Z-range maps.
    """
    import math
    b = 0.55 + math.sqrt(z_norm) * 0.80
    return (
        min(255, int(rgb[0] * b)),
        min(255, int(rgb[1] * b)),
        min(255, int(rgb[2] * b)),
    )


def _darken(rgb: tuple, factor: float = 0.55) -> tuple:
    return (int(rgb[0] * factor), int(rgb[1] * factor), int(rgb[2] * factor))


# ── SVG renderer ───────────────────────────────────────────────────────────────

def render_topview_svg(bsp_data: bytes, tex_root: str, palette_path: str,
                       floor_dot_threshold: float = 0.65,
                       tex_root2: str = '') -> tuple:
    """
    Render a top-down SVG of a Q2 BSP map.

    Only upward-facing faces (floor / roof, normal.z > threshold) are drawn so
    that interior ceilings don't obscure floors.  Faces are sorted by average Z
    ascending so that higher floors paint over lower ones.

    Returns:
        (svg_text: str, bounds: dict)
        bounds = {min_x, max_x, min_y, max_y}  — the playable world-space window
        used as the SVG viewBox; pass this to topview.json for the client.
    """
    lumps     = _read_lumps(bsp_data)
    planes    = _read_planes(bsp_data, lumps)
    verts     = _read_vertices(bsp_data, lumps)
    edges     = _read_edges(bsp_data, lumps)
    surfedges = _read_surfedges(bsp_data, lumps)
    texinfos  = _read_texinfo(bsp_data, lumps)
    faces     = _read_faces(bsp_data, lumps)

    # Entity string → spawn points
    ent_off, ent_len = lumps[LUMP_ENTITIES]
    entity_string = bsp_data[ent_off:ent_off + ent_len].decode('latin-1', errors='replace')
    spawns = _entity_spawns(entity_string)

    # Vertex bounds (used as fallback + clamp)
    xs = [v[0] for v in verts]
    ys = [v[1] for v in verts]
    zs = [v[2] for v in verts]
    vert_bounds = {
        'min_x': min(xs), 'max_x': max(xs),
        'min_y': min(ys), 'max_y': max(ys),
    }
    min_z, max_z = min(zs), max(zs)
    z_range = max(max_z - min_z, 1.0)

    # Playable bounds → SVG viewBox
    bounds = _playable_bounds(spawns, vert_bounds)
    bx0, bx1 = bounds['min_x'], bounds['max_x']
    by0, by1 = bounds['min_y'], bounds['max_y']
    vb_w = bx1 - bx0
    vb_h = by1 - by0

    max_span = max(vb_w, vb_h)
    # Wall lines: thick enough to be a clear dark band between rooms
    wall_stroke_w   = max(5.0, max_span / 220.0)
    # Faces whose centroid falls more than this far outside the playable bounds are culled.
    # 300 units gives outdoor maps more room to include ground-level street/courtyard faces.
    MARGIN = 300.0

    # SVG uses Y-down; Q2 uses Y-up.  We flip in Python: svg_y = -q2_y.
    # viewBox top = -by1, bottom = -by0
    vb_top = -by1

    # ── Collect floor faces ────────────────────────────────────────────────────
    # Only upward-facing faces (nz >= threshold) are drawn as colored polygons.
    # Wall lines are derived afterwards from the floor polygon perimeter edges
    # (see below) — this avoids any noise from terrain or sloped geometry.
    floor_faces = []

    for face in faces:
        if face['texinfo'] >= len(texinfos):
            continue
        ti = texinfos[face['texinfo']]
        if ti['flags'] & _SKIP_FLAGS:
            continue
        name = ti['name']
        if not name or name.startswith('__') or \
                name.lower() in ('trigger', 'clip', 'hint', 'origin'):
            continue
        if face['plane'] >= len(planes):
            continue
        nx, ny, nz, _ = planes[face['plane']]
        if face['side']:
            nz = -nz

        if nz < floor_dot_threshold:
            continue

        poly = _face_polygon(face, surfedges, edges, verts)
        if len(poly) < 3:
            continue

        cx = sum(v[0] for v in poly) / len(poly)
        cy = sum(v[1] for v in poly) / len(poly)
        if (cx < bx0 - MARGIN or cx > bx1 + MARGIN or
                cy < by0 - MARGIN or cy > by1 + MARGIN):
            continue
        avg_z = sum(v[2] for v in poly) / len(poly)
        floor_faces.append((avg_z, name, poly))

    # Lower floors first; elevated floors / roofs (stairs, crates, rooftops) paint on top
    floor_faces.sort(key=lambda f: f[0])

    # ── Z range for height shading: use floor face distribution ───────────────
    # Full vertex Z range includes skybox/terrain extremes and makes small
    # box/stair differences invisible.  Use the 10th–90th percentile of actual
    # floor face Z values so the full brightness curve maps to playable levels.
    if len(floor_faces) >= 10:
        _fz          = sorted(f[0] for f in floor_faces)
        _n           = len(_fz)
        z_lo         = _fz[_n // 10]
        z_hi         = _fz[min(_n - 1, 9 * _n // 10)]
        z_range_eff  = max(z_hi - z_lo, 80.0)
        # "Base floor" = 20th percentile.  Faces clearly above this level
        # (box tops, stair landings, mezzanines) get a thin dark outline edge.
        z_base = _fz[_n // 5]
    else:
        z_lo = z_base = min_z
        z_range_eff   = z_range

    ELEV_THRESH   = 32.0                          # units above z_base = "elevated"
    elev_stroke_w = max(3.0, max_span / 600.0)   # thin outline on elevated faces

    # ── Wall lines: strictly vertical BSP faces ────────────────────────────────
    # Only faces with |nz| < 0.15 (~81° from horizontal) qualify.
    # This captures room walls, box/crate sides and stair risers while reliably
    # excluding all terrain polys (sloped surfaces fail the threshold).
    wall_segs: dict = {}   # rounded-endpoint key → (p1_xy, p2_xy)

    for face in faces:
        if face['texinfo'] >= len(texinfos):
            continue
        ti = texinfos[face['texinfo']]
        if ti['flags'] & _SKIP_FLAGS:
            continue
        name = ti['name']
        if not name or name.startswith('__') or \
                name.lower() in ('trigger', 'clip', 'hint', 'origin'):
            continue
        if face['plane'] >= len(planes):
            continue
        nx, ny, nz, _ = planes[face['plane']]
        if face['side']:
            nz = -nz
        if abs(nz) >= 0.15:        # not vertical enough → skip
            continue

        poly = _face_polygon(face, surfedges, edges, verts)
        if len(poly) < 2:
            continue

        seen_xy: dict = {}
        pts_xy = []
        for v in poly:
            key = (round(v[0] / 4) * 4, round(v[1] / 4) * 4)
            if key not in seen_xy:
                seen_xy[key] = True
                pts_xy.append((v[0], v[1]))
        if len(pts_xy) < 2:
            continue
        if not any(bx0 - MARGIN <= p[0] <= bx1 + MARGIN and
                   by0 - MARGIN <= p[1] <= by1 + MARGIN for p in pts_xy):
            continue

        for i in range(len(pts_xy)):
            p1 = pts_xy[i]
            p2 = pts_xy[(i + 1) % len(pts_xy)]
            r1 = (round(p1[0] / 4) * 4, round(p1[1] / 4) * 4)
            r2 = (round(p2[0] / 4) * 4, round(p2[1] / 4) * 4)
            if r1 == r2:
                continue
            seg_len = ((p2[0] - p1[0]) ** 2 + (p2[1] - p1[1]) ** 2) ** 0.5
            if seg_len < 28.0:
                continue
            seg_key = (min(r1, r2), max(r1, r2))
            if seg_key not in wall_segs:
                wall_segs[seg_key] = (p1, p2)

    # ── Tighten viewport to actual rendered face extents ───────────────────────
    # Prevents void space when spawn points are unevenly distributed (e.g. dusty).
    # The spawn bounds are still used above to filter out-of-level geometry;
    # here we just shrink the viewBox to fit only what actually rendered.
    if floor_faces:
        fvx = [v[0] for _, _, poly in floor_faces for v in poly]
        fvy = [v[1] for _, _, poly in floor_faces for v in poly]
        PAD = max(vb_w * 0.02, 80.0)
        bx0 = max(bx0, min(fvx) - PAD)
        bx1 = min(bx1, max(fvx) + PAD)
        by0 = max(by0, min(fvy) - PAD)
        by1 = min(by1, max(fvy) + PAD)
        bounds = {'min_x': bx0, 'max_x': bx1, 'min_y': by0, 'max_y': by1}
        vb_w = bx1 - bx0
        vb_h = by1 - by0
        max_span = max(vb_w, vb_h)
        wall_stroke_w  = max(5.0, max_span / 220.0)
        elev_stroke_w  = max(3.0, max_span / 600.0)
        vb_top = -by1  # re-derive after tightening

    # ── Build SVG ─────────────────────────────────────────────────────────────
    # Canonical pixel size so browsers can rasterize when loaded as <img>.
    # ctx.drawImage uses the destination rect, so the exact size doesn't matter
    # as long as it's > 0 and preserves the aspect ratio.
    px_w = 1024
    px_h = max(1, round(px_w * vb_h / vb_w))
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg"'
        f' width="{px_w}" height="{px_h}"'
        f' viewBox="{bx0:.1f} {vb_top:.1f} {vb_w:.1f} {vb_h:.1f}">',
        '<defs>',
        # clipPath guarantees nothing renders outside the playable bounds,
        # even when the SVG is loaded as an <img> where viewBox alone won't clip.
        f'<clipPath id="pb">'
        f'<rect x="{bx0:.1f}" y="{vb_top:.1f}" width="{vb_w:.1f}" height="{vb_h:.1f}"/>'
        f'</clipPath>',
        '</defs>',
        # Dark void background
        f'<rect x="{bx0:.1f}" y="{vb_top:.1f}" width="{vb_w:.1f}" height="{vb_h:.1f}" fill="#0d1018"/>',
        '<g clip-path="url(#pb)">',
    ]

    # Pass 1: floor / roof faces (colored, height-shaded)
    # Elevated faces (box tops, stair landings, mezzanines) get a thin dark
    # outline stroke to define their edges against the floor level below.
    for avg_z, tex_name, poly in floor_faces:
        base = texture_color(tex_name, tex_root, palette_path, tex_root2)
        z_n  = max(0.0, min(1.0, (avg_z - z_lo) / z_range_eff))
        fill = _shade_z(base, z_n)
        fc   = '#{:02x}{:02x}{:02x}'.format(*fill)
        pts  = ' '.join(f'{v[0]:.1f},{-v[1]:.1f}' for v in poly)
        if avg_z > z_base + ELEV_THRESH:
            # Thin dark edge outlines the top of boxes, stairs, mezzanines
            ec = '#{:02x}{:02x}{:02x}'.format(
                max(0, fill[0] - 45), max(0, fill[1] - 45), max(0, fill[2] - 45))
            lines.append(
                f'<polygon points="{pts}" fill="{fc}"'
                f' stroke="{ec}" stroke-width="{elev_stroke_w:.1f}" stroke-linejoin="round"/>'
            )
        else:
            lines.append(f'<polygon points="{pts}" fill="{fc}" stroke="none"/>')

    # Pass 2: wall segments — thick dark lines on top of floors
    for (p1, p2) in wall_segs.values():
        lines.append(
            f'<line x1="{p1[0]:.1f}" y1="{-p1[1]:.1f}"'
            f' x2="{p2[0]:.1f}" y2="{-p2[1]:.1f}"'
            f' stroke="#0d1018" stroke-width="{wall_stroke_w:.1f}" stroke-linecap="round"/>'
        )

    lines += ['</g>', '</svg>']
    return '\n'.join(lines), bounds
