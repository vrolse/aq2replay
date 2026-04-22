"""
MD2 player model parser for Action Quake 2.

Exports animation frames as Three.js-ready JSON:
  {
    ntri:   int,
    uvs:    [float...],       # ntri*3*2 — constant across all frames
    frames: [[float...]...],  # each: ntri*3*3, already in Three.js coords
    anims:  {name: [start, end]}   # inclusive indices into frames[]
  }

Coordinate transform applied here (Q2 → Three.js):
    three.x =  q2.x
    three.y =  q2.z      (up axis swap)
    three.z = -q2.y
"""

import struct

# Frame ranges from q2pro-aqtion/src/action/m_player.h
# Format: (first_frame_inclusive, one_past_last_frame) — half-open range
_ANIMS = {
    'stand':   (0,   40),   # 0–39    40 frames  idle/breathing
    'run':     (40,  46),   # 40–45    6 frames  running
    'attack':  (46,  54),   # 46–53    8 frames  firing
    'pain1':   (54,  58),   # 54–57    4 frames
    'pain2':   (58,  62),   # 58–61    4 frames
    'pain3':   (62,  66),   # 62–65    4 frames
    'jump':    (66,  72),   # 66–71    6 frames
    'crstnd':  (135, 154),  # 135–153 19 frames  crouch-stand
    'crwalk':  (154, 160),  # 154–159  6 frames  crouch-walk
    'crdeath': (173, 178),  # 173–177  5 frames  crouch-death
    'death1':  (178, 184),  # 178–183  6 frames
    'death2':  (184, 190),  # 184–189  6 frames
    'death3':  (190, 198),  # 190–197  8 frames
}

_MD2_IDENT   = 844121161   # b'IDP2' as little-endian int
_MD2_VERSION = 8


def parse_md2_json(path: str, anims: dict | None = None) -> dict:
    """Parse an MD2 file and return a Three.js-ready geometry dict."""
    if anims is None:
        anims = _ANIMS

    with open(path, 'rb') as fh:
        data = fh.read()

    # ── Header (17 × int32) ──────────────────────────────────────────────────
    (ident, ver, sw, sh, frame_size,
     ns, nxyz, nst, ntri, nglcmd, nf,
     ofs_skins, ofs_st, ofs_tri, ofs_frames, ofs_glcmds, ofs_end
     ) = struct.unpack_from('<17i', data)

    if ident != _MD2_IDENT:
        raise ValueError(f'Not an MD2 file (magic={ident:#x})')
    if ver != _MD2_VERSION:
        raise ValueError(f'Unsupported MD2 version {ver}')

    # ── Embedded skin path (first skin only) ─────────────────────────────────
    skin_path = ''
    if ns > 0:
        raw = data[ofs_skins : ofs_skins + 64].rstrip(b'\x00')
        skin_path = raw.decode('ascii', 'replace').replace('.pcx', '.png').rstrip('\x00 ')

    # ── ST texture coords → normalised UV ───────────────────────────────────
    st: list[tuple[float, float]] = []
    for i in range(nst):
        s, t = struct.unpack_from('<hh', data, ofs_st + i * 4)
        st.append((s / sw, 1.0 - t / sh))   # flip T so origin is bottom-left

    # ── Triangles (vertex-index triple + UV-index triple) ────────────────────
    tris: list[tuple] = []
    for i in range(ntri):
        ixyz = struct.unpack_from('<3H', data, ofs_tri + i * 12)
        ist  = struct.unpack_from('<3H', data, ofs_tri + i * 12 + 6)
        tris.append((ixyz, ist))

    # ── UV array: ntri × 3 corners × 2 floats (same for every frame) ────────
    uvs: list[float] = []
    for _, ist in tris:
        for ui in ist:
            u, v = st[ui]
            uvs.append(round(u, 5))
            uvs.append(round(v, 5))

    # ── Export selected animation frames ─────────────────────────────────────
    out_frames: list[list[float]] = []
    anim_ranges: dict = {}

    for anim_name, (anim_start, anim_end) in anims.items():
        range_start = len(out_frames)
        for fi in range(anim_start, anim_end):
            if fi >= nf:
                break

            base = ofs_frames + fi * frame_size
            sx, sy, sz = struct.unpack_from('<3f', data, base)
            tx, ty, tz = struct.unpack_from('<3f', data, base + 12)

            # Decompress all num_xyz vertices for this frame
            vpos: list[tuple[float, float, float]] = []
            for vi in range(nxyz):
                bx, by, bz, _nl = struct.unpack_from('<4B', data, base + 40 + vi * 4)
                q2x = sx * bx + tx
                q2y = sy * by + ty
                q2z = sz * bz + tz
                # Q2 → Three.js coordinate system
                vpos.append((q2x, q2z, -q2y))

            # Expand to triangle soup: ntri × 3 verts × 3 coords
            pos: list[float] = []
            for ixyz, _ in tris:
                for vi in ixyz:
                    vx, vy, vz = vpos[vi]
                    pos.append(round(vx, 1))
                    pos.append(round(vy, 1))
                    pos.append(round(vz, 1))

            out_frames.append(pos)

        range_end = len(out_frames) - 1
        if range_end >= range_start:
            anim_ranges[anim_name] = [range_start, range_end]

    return {
        'ntri':      ntri,
        'uvs':       uvs,
        'frames':    out_frames,
        'anims':     anim_ranges,
        'skin_path': skin_path,
    }
