"""
Top-down textured map renderer for Q2 BSP files.

Reads the Quake 2 palette from colormap.pcx, decodes WAL/PNG/JPG textures,
then paints all horizontal (floor/ceiling) faces from the BSP as filled
polygons coloured with the average texture colour.

The output is a PNG image whose world-space bounds match the BSP bounds exactly,
so the client can overlay it on the canvas using the same projection math.
"""
import os
import struct
import math
from functools import lru_cache
from typing import Optional

from PIL import Image, ImageDraw

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
    """Compute average RGB of an image, excluding fully transparent pixels."""
    img = img.convert('RGBA').resize((64, 64), Image.BOX)
    pixels = list(img.getdata())
    rs = gs = bs = n = 0
    for r, g, b, a in pixels:
        if a > 32:
            rs += r; gs += g; bs += b; n += 1
    if n == 0:
        return (80, 80, 80)
    return (rs // n, gs // n, bs // n)


@lru_cache(maxsize=512)
def texture_color(tex_name: str, tex_root: str, palette_path: str) -> tuple:
    """
    Return average RGB for a texture name (Q2 path without extension).
    Searches: .png, .jpg, .wal under tex_root.
    Falls back to neutral grey if not found.
    """
    for ext in ('.png', '.jpg', '.jpeg', '.wal'):
        path = os.path.join(tex_root, tex_name.replace('/', os.sep) + ext)
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
    # Fallback: neutral dark grey
    return (60, 65, 75)


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


# ── Main renderer ──────────────────────────────────────────────────────────────

# Surface flags — skip nodraw / sky / trigger surfaces
SURF_NODRAW  = 0x0080
SURF_SKY     = 0x0004
SURF_SKIP    = 0x0200
SURF_HINT    = 0x0100
_SKIP_FLAGS  = SURF_NODRAW | SURF_SKY | SURF_SKIP | SURF_HINT


def render_topview(bsp_data: bytes, tex_root: str, palette_path: str,
                   img_size: int = 1024,
                   floor_dot_threshold: float = 0.7) -> Image.Image:
    """
    Render a textured top-down view of a Q2 BSP.

    Args:
        bsp_data:             raw BSP bytes
        tex_root:             path to textures directory
        palette_path:         path to colormap.pcx
        img_size:             output image size (square)
        floor_dot_threshold:  |normal.z| threshold to classify as floor/ceiling

    Returns:
        PIL Image (RGB) sized img_size×img_size aligned to BSP world bounds.
        The image also carries attributes  .world_bounds = {min_x,max_x,min_y,max_y}.
    """
    lumps    = _read_lumps(bsp_data)
    planes   = _read_planes(bsp_data, lumps)
    verts    = _read_vertices(bsp_data, lumps)
    edges    = _read_edges(bsp_data, lumps)
    surfedges = _read_surfedges(bsp_data, lumps)
    texinfos = _read_texinfo(bsp_data, lumps)
    faces    = _read_faces(bsp_data, lumps)

    # World bounds from vertices
    xs = [v[0] for v in verts]
    ys = [v[1] for v in verts]
    zs = [v[2] for v in verts]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)

    # Projection: same centre-preserving logic as makeProjection() in viewer.js
    pad = 0   # server PNG uses no padding; client handles it
    range_x = max_x - min_x or 1
    range_y = max_y - min_y or 1
    sx = img_size / range_x
    sy = img_size / range_y
    scale = min(sx, sy)
    off_x = ((img_size) - range_x * scale) / 2
    off_y = ((img_size) - range_y * scale) / 2

    def to_px(wx, wy):
        return (
            off_x + (wx - min_x) * scale,
            off_y + (max_y - wy)  * scale,   # Y flip
        )

    img  = Image.new('RGB', (img_size, img_size), (11, 14, 22))
    draw = ImageDraw.Draw(img)

    # Collect floor/ceiling faces with their average Z for depth sorting
    face_list = []
    for face in faces:
        if face['texinfo'] >= len(texinfos):
            continue
        ti = texinfos[face['texinfo']]
        if ti['flags'] & _SKIP_FLAGS:
            continue
        if not ti['name'] or ti['name'].startswith('__') or ti['name'] == 'trigger':
            continue

        # Check plane normal
        if face['plane'] >= len(planes):
            continue
        nx, ny, nz, _ = planes[face['plane']]
        # Side=1 means face normal is flipped from the plane normal
        if face['side']:
            nz = -nz

        if abs(nz) < floor_dot_threshold:
            continue         # wall — skip

        poly = _face_polygon(face, surfedges, edges, verts)
        if len(poly) < 3:
            continue

        avg_z = sum(v[2] for v in poly) / len(poly)
        face_list.append((avg_z, nz, ti['name'], poly))

    # Sort: lower Z first so higher floors paint over lower ceilings
    face_list.sort(key=lambda f: f[0])

    for avg_z, nz, tex_name, poly in face_list:
        color = texture_color(tex_name, tex_root, palette_path)

        # Slightly darken ceiling faces to distinguish from floors
        if nz < 0:
            color = tuple(max(0, c - 30) for c in color)

        pts = [to_px(v[0], v[1]) for v in poly]
        try:
            draw.polygon(pts, fill=color)
        except Exception:
            pass

    # Draw wall outlines on top for readability
    draw.rectangle([0, 0, img_size-1, img_size-1], outline=(11, 14, 22))

    # Store world bounds as image attributes for the Flask route
    img.world_bounds = {
        'min_x': min_x, 'max_x': max_x,
        'min_y': min_y, 'max_y': max_y,
        'off_x': off_x, 'off_y': off_y,
        'scale': scale,
        'img_size': img_size,
    }
    return img
