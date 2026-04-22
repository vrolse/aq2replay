"""
Q2 BSP mesh exporter for the 3-D viewer.

Exports a compact JSON mesh with:
  - triangulated geometry grouped by texture
  - UV coordinates derived from BSP texinfo S/T vectors
  - world-space bounds + playable bounds
  - spawn point list

The client (Three.js) loads this once and renders it with actual WAL textures
served by /api/map/<name>/texture/<texname>.
"""
import os
import struct
import math
from functools import lru_cache

# ─── BSP lump indices (Q2 version 38) ─────────────────────────────────────────
LUMP_ENTITIES   = 0
LUMP_PLANES     = 1
LUMP_VERTICES   = 2
LUMP_TEXINFO    = 5
LUMP_FACES      = 6
LUMP_EDGES      = 11
LUMP_SURFEDGES  = 12

SURF_NODRAW = 0x0080
SURF_SKY    = 0x0004
SURF_SKIP   = 0x0200
SURF_HINT   = 0x0100
SURF_TRANS33 = 0x0010
SURF_TRANS66 = 0x0020
_SKIP_FLAGS = SURF_NODRAW | SURF_SKY | SURF_SKIP | SURF_HINT

# ─── Low-level lump readers ───────────────────────────────────────────────────

def _lumps(data: bytes) -> list:
    return [struct.unpack_from('<ii', data, 8 + i * 8) for i in range(19)]


def _planes(data: bytes, lumps: list) -> list:
    off, length = lumps[LUMP_PLANES]
    n = length // 20
    out = []
    for i in range(n):
        nx, ny, nz, dist, _ = struct.unpack_from('<4fi', data, off + i * 20)
        out.append((nx, ny, nz, dist))
    return out


def _verts(data: bytes, lumps: list) -> list:
    off, length = lumps[LUMP_VERTICES]
    n = length // 12
    flat = struct.unpack_from(f'<{n*3}f', data, off)
    return [(flat[i*3], flat[i*3+1], flat[i*3+2]) for i in range(n)]


def _edges(data: bytes, lumps: list) -> list:
    off, length = lumps[LUMP_EDGES]
    n = length // 4
    return [struct.unpack_from('<HH', data, off + i * 4) for i in range(n)]


def _surfedges(data: bytes, lumps: list) -> list:
    off, length = lumps[LUMP_SURFEDGES]
    n = length // 4
    return list(struct.unpack_from(f'<{n}i', data, off))


def _texinfos(data: bytes, lumps: list) -> list:
    off, length = lumps[LUMP_TEXINFO]
    n = length // 76
    out = []
    for i in range(n):
        base = off + i * 76
        sx, sy, sz, so = struct.unpack_from('<4f', data, base)
        tx, ty, tz, to = struct.unpack_from('<4f', data, base + 16)
        flags = struct.unpack_from('<I', data, base + 32)[0]
        raw = data[base + 40: base + 72].rstrip(b'\x00')
        name = raw.decode('latin-1', errors='replace')
        out.append({
            's': (sx, sy, sz), 'so': so,
            't': (tx, ty, tz), 'to': to,
            'flags': flags,
            'name': name,
        })
    return out


FACE_FMT  = '<HHiHH4bi'
FACE_SIZE = struct.calcsize(FACE_FMT)   # 20 bytes

def _faces(data: bytes, lumps: list) -> list:
    off, length = lumps[LUMP_FACES]
    n = length // FACE_SIZE
    out = []
    for i in range(n):
        f = struct.unpack_from(FACE_FMT, data, off + i * FACE_SIZE)
        out.append({
            'plane': f[0], 'side': f[1],
            'first_edge': f[2], 'num_edges': f[3],
            'texinfo': f[4],
        })
    return out


def _face_verts(face: dict, surfedges: list, edges: list, verts: list) -> list:
    poly = []
    for k in range(face['num_edges']):
        se = surfedges[face['first_edge'] + k]
        v = edges[se][0] if se >= 0 else edges[-se][1]
        poly.append(verts[v])
    return poly


def _entity_string(data: bytes, lumps: list) -> str:
    off, length = lumps[LUMP_ENTITIES]
    return data[off: off + length].decode('latin-1', errors='replace').rstrip('\x00')


# ─── UV calculation ───────────────────────────────────────────────────────────

def _uv(v: tuple, ti: dict, tw: int, th: int) -> tuple:
    """Compute (u, v) in [0,1] for vertex v using texinfo S/T vectors.

    Q2 BSP uses a top-left texture origin (T=0 is the top of the image).
    Three.js TextureLoader loads PNGs with flipY=true, which maps V=0 to the
    bottom of the original image.  We must flip V so that Q2 T=0 (top) maps
    to Three.js V=1 (top after flipY), otherwise all textures appear upside down.
    """
    sx, sy, sz = ti['s']
    tx_, ty, tz = ti['t']
    s = v[0]*sx + v[1]*sy + v[2]*sz + ti['so']
    t = v[0]*tx_ + v[1]*ty + v[2]*tz + ti['to']
    if tw > 0 and th > 0:
        return (s / tw, 1.0 - t / th)
    return (s / 64.0, 1.0 - t / 64.0)


# ─── Fan triangulation ────────────────────────────────────────────────────────

def _triangulate(poly: list) -> list:
    """Triangle fan from vertex 0."""
    tris = []
    for i in range(1, len(poly) - 1):
        tris.append((poly[0], poly[i], poly[i + 1]))
    return tris


# ─── Entity / spawn parsing ───────────────────────────────────────────────────

def _spawns(ent_str: str) -> list:
    _SPAWN = frozenset({
        'info_player_start', 'info_player_deathmatch',
        'info_player_team1', 'info_player_team2',
    })
    spawns, cur = [], {}
    for line in ent_str.splitlines():
        line = line.strip()
        if line == '{':
            cur = {}
        elif line == '}':
            if cur.get('classname', '') in _SPAWN:
                try:
                    ox, oy, oz = cur['origin'].split()
                    spawns.append({'x': float(ox), 'y': float(oy), 'z': float(oz)})
                except (KeyError, ValueError):
                    pass
            cur = {}
        elif line.startswith('"'):
            parts = line.split('"')
            if len(parts) >= 4:
                cur[parts[1]] = parts[3]
    return spawns


# ─── WAL header reader ────────────────────────────────────────────────────────

def _wal_size(data: bytes) -> tuple:
    """Return (width, height) from a WAL header; (64, 64) if unreadable."""
    try:
        w, h = struct.unpack_from('<2I', data, 32)
        return int(w), int(h)
    except Exception:
        return 64, 64


def _tex_size(name: str, tex_root: str) -> tuple:
    """Return pixel (w, h) for texture name; fall back to 64×64."""
    for ext in ('.wal', '.png', '.jpg', '.jpeg'):
        path = os.path.join(tex_root, name.replace('/', os.sep) + ext)
        if os.path.exists(path):
            try:
                if ext == '.wal':
                    with open(path, 'rb') as f:
                        return _wal_size(f.read(40))
                from PIL import Image
                with Image.open(path) as im:
                    return im.size
            except Exception:
                pass
    return 64, 64


# ─── Playable bounds ──────────────────────────────────────────────────────────

def _playable_bounds(spawns: list, vert_bounds: dict) -> dict:
    if not spawns:
        return vert_bounds.copy()
    sxs = [s['x'] for s in spawns]
    sys_ = [s['y'] for s in spawns]
    mx = max((max(sxs) - min(sxs)) * 0.8, 1200)
    my = max((max(sys_) - min(sys_)) * 0.8, 1200)
    return {
        'min_x': max(min(sxs) - mx, vert_bounds['min_x']),
        'max_x': min(max(sxs) + mx, vert_bounds['max_x']),
        'min_y': max(min(sys_) - my, vert_bounds['min_y']),
        'max_y': min(max(sys_) + my, vert_bounds['max_y']),
        'min_z': vert_bounds['min_z'],
        'max_z': vert_bounds['max_z'],
    }


# ─── Main export function ─────────────────────────────────────────────────────

def build_mesh(bsp_data: bytes, tex_root: str) -> dict:
    """
    Build a Three.js-ready mesh dict from BSP data.

    Returns:
    {
      bounds: {min_x,max_x,min_y,max_y,min_z,max_z},
      playable: {same plus min_z,max_z},
      spawns: [{x,y,z},...],
      textures: ['wall/brickface', ...],   # texture names (no extension)
      groups: [
        {
          tex_index: int,          # index into textures[]
          opacity: float,          # 1.0 or 0.33/0.66 for transparent surfaces
          positions: [x,y,z, ...], # flat float32 array (3 per vertex)
          uvs:       [u,v, ...],   # flat float32 array (2 per vertex)
          normals:   [nx,ny,nz,..],# flat float32 array (3 per vertex)
        },
        ...
      ]
    }

    Three.js usage:
      const geo = new THREE.BufferGeometry();
      geo.setAttribute('position', new THREE.Float32BufferAttribute(g.positions, 3));
      geo.setAttribute('uv',       new THREE.Float32BufferAttribute(g.uvs, 2));
      geo.setAttribute('normal',   new THREE.Float32BufferAttribute(g.normals, 3));
    """
    lumps_     = _lumps(bsp_data)
    planes_    = _planes(bsp_data, lumps_)
    verts_     = _verts(bsp_data, lumps_)
    edges_     = _edges(bsp_data, lumps_)
    surfe_     = _surfedges(bsp_data, lumps_)
    texinfos_  = _texinfos(bsp_data, lumps_)
    faces_     = _faces(bsp_data, lumps_)
    ent_str    = _entity_string(bsp_data, lumps_)

    xs = [v[0] for v in verts_]
    ys = [v[1] for v in verts_]
    zs = [v[2] for v in verts_]
    vert_bounds = {
        'min_x': min(xs), 'max_x': max(xs),
        'min_y': min(ys), 'max_y': max(ys),
        'min_z': min(zs), 'max_z': max(zs),
    }
    spawn_list = _spawns(ent_str)
    play       = _playable_bounds(spawn_list, vert_bounds)

    # Playable XY filter — skip faces far outside the playable window
    MARGIN = 500.0
    bx0, bx1 = play['min_x'] - MARGIN, play['max_x'] + MARGIN
    by0, by1 = play['min_y'] - MARGIN, play['max_y'] + MARGIN

    # Collect faces grouped by texture name
    # groups_by_tex: tex_name → {opacity: float, tris: [(v0,v1,v2), ...]}
    groups_by_tex: dict = {}
    tex_sizes: dict = {}   # tex_name → (w, h) — cached

    for face in faces_:
        ti_idx = face['texinfo']
        if ti_idx >= len(texinfos_):
            continue
        ti = texinfos_[ti_idx]
        if ti['flags'] & _SKIP_FLAGS:
            continue
        name = ti['name']
        if not name or name.startswith('__') or \
                name.lower() in ('trigger', 'clip', 'hint', 'origin'):
            continue

        poly = _face_verts(face, surfe_, edges_, verts_)
        if len(poly) < 3:
            continue

        # Centroid filter
        cx = sum(v[0] for v in poly) / len(poly)
        cy = sum(v[1] for v in poly) / len(poly)
        if cx < bx0 or cx > bx1 or cy < by0 or cy > by1:
            continue

        # Plane normal (for Three.js normals)
        if face['plane'] >= len(planes_):
            continue
        nx, ny, nz, _ = planes_[face['plane']]
        if face['side']:
            nx, ny, nz = -nx, -ny, -nz

        # Texture size for UV normalisation
        if name not in tex_sizes:
            tex_sizes[name] = _tex_size(name, tex_root)
        tw, th = tex_sizes[name]

        # Compute UVs per vertex
        uvs_poly = [_uv(v, ti, tw, th) for v in poly]

        # Fan triangulate
        poly_with_uv = list(zip(poly, uvs_poly))
        tris = _triangulate(poly_with_uv)

        # Transparency flag
        opacity = 1.0
        if ti['flags'] & SURF_TRANS33:
            opacity = 0.33
        elif ti['flags'] & SURF_TRANS66:
            opacity = 0.66

        key = (name, opacity)
        if key not in groups_by_tex:
            groups_by_tex[key] = {'opacity': opacity, 'tris': []}
        for tri in tris:
            for (vpos, vuv) in tri:
                groups_by_tex[key]['tris'].append((vpos, vuv, (nx, ny, nz)))

    # Build flat arrays per texture group
    # De-duplicate textures into a name list
    tex_list = []
    tex_index = {}
    for (tname, _opacity) in groups_by_tex:
        if tname not in tex_index:
            tex_index[tname] = len(tex_list)
            tex_list.append(tname)

    groups_out = []
    for (tname, opacity), gdata in groups_by_tex.items():
        positions = []
        uvs       = []
        normals   = []
        for (vpos, vuv, norm) in gdata['tris']:
            positions += [vpos[0], vpos[1], vpos[2]]
            uvs       += [vuv[0],  vuv[1]]
            normals   += [norm[0], norm[1], norm[2]]
        groups_out.append({
            'tex_index': tex_index[tname],
            'opacity':   opacity,
            'positions': positions,
            'uvs':       uvs,
            'normals':   normals,
        })

    return {
        'bounds':   vert_bounds,
        'playable': play,
        'spawns':   spawn_list,
        'textures': tex_list,
        'groups':   groups_out,
    }
