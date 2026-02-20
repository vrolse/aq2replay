"""
Q2 BSP version 38 parser.
Extracts 2D projected geometry (top-down XY view) for the map viewer.
"""
import struct

BSP_MAGIC = b'IBSP'
BSP_VERSION = 38

# Lump indices
LUMP_ENTITIES    = 0
LUMP_PLANES      = 1
LUMP_VERTICES    = 2
LUMP_VISIBILITY  = 3
LUMP_NODES       = 4
LUMP_TEXINFO     = 5
LUMP_FACES       = 6
LUMP_LIGHTING    = 7
LUMP_LEAVES      = 8
LUMP_LEAF_FACES  = 9
LUMP_LEAF_BRUSHES = 10
LUMP_EDGES       = 11
LUMP_SURFEDGES   = 12
LUMP_MODELS      = 13
NUM_LUMPS        = 19


def load_bsp(path: str) -> dict:
    with open(path, 'rb') as f:
        data = f.read()
    return parse_bsp(data)


def parse_bsp(data: bytes) -> dict:
    """
    Parse a Q2 BSP file.

    Returns a dict:
      bounds  - {min_x, max_x, min_y, max_y, min_z, max_z}
      edges   - list of [x1, y1, x2, y2]  (XY projection, deduplicated)
      entity_string - raw entity lump text (for spawn-point extraction)
    """
    if data[:4] != BSP_MAGIC:
        raise ValueError(f"Not a Q2 BSP file (got {data[:4]!r})")
    version = struct.unpack_from('<i', data, 4)[0]
    if version != BSP_VERSION:
        raise ValueError(f"Unsupported BSP version {version}, expected {BSP_VERSION}")

    # Read lump directory (19 lumps × 8 bytes each, starting at offset 8)
    lumps = []
    for i in range(NUM_LUMPS):
        off, length = struct.unpack_from('<ii', data, 8 + i * 8)
        lumps.append((off, length))

    # --- Vertices (lump 2): 3 × float32 per vertex = 12 bytes ---
    v_off, v_len = lumps[LUMP_VERTICES]
    num_verts = v_len // 12
    vx = struct.unpack_from(f'<{num_verts * 3}f', data, v_off)
    vertices = [(vx[i*3], vx[i*3+1], vx[i*3+2]) for i in range(num_verts)]

    # --- Edges (lump 11): 2 × uint16 per edge = 4 bytes ---
    e_off, e_len = lumps[LUMP_EDGES]
    num_edges = e_len // 4

    seen: set = set()
    edges_out = []

    for i in range(1, num_edges):          # edge 0 is a dummy in Q2 BSP
        v0, v1 = struct.unpack_from('<HH', data, e_off + i * 4)
        if v0 >= num_verts or v1 >= num_verts:
            continue
        key = (min(v0, v1), max(v0, v1))
        if key in seen:
            continue
        seen.add(key)

        x1, y1, _ = vertices[v0]
        x2, y2, _ = vertices[v1]
        if x1 == x2 and y1 == y2:      # zero-length projected edge
            continue
        edges_out.append([round(x1, 1), round(y1, 1), round(x2, 1), round(y2, 1)])

    # Compute 3-D bounding box from all vertices
    xs = [v[0] for v in vertices]
    ys = [v[1] for v in vertices]
    zs = [v[2] for v in vertices]

    # --- Entity string (lump 0) ---
    ent_off, ent_len = lumps[LUMP_ENTITIES]
    entity_string = data[ent_off:ent_off + ent_len].decode('latin-1', errors='replace').rstrip('\x00')

    # Extract info_player_start and info_player_deathmatch spawn points
    spawns = _parse_spawn_points(entity_string)

    return {
        'bounds': {
            'min_x': min(xs), 'max_x': max(xs),
            'min_y': min(ys), 'max_y': max(ys),
            'min_z': min(zs), 'max_z': max(zs),
        },
        'edges': edges_out,
        'spawns': spawns,
    }


def _parse_spawn_points(entity_string: str) -> list:
    """Extract spawn point positions from the entity string."""
    spawns = []
    current: dict = {}
    for line in entity_string.splitlines():
        line = line.strip()
        if line == '{':
            current = {}
        elif line == '}':
            classname = current.get('classname', '')
            if classname in ('info_player_start', 'info_player_deathmatch',
                             'info_player_team1', 'info_player_team2'):
                origin = current.get('origin', '')
                try:
                    parts = origin.split()
                    x, y, z = float(parts[0]), float(parts[1]), float(parts[2])
                    spawns.append({'x': x, 'y': y, 'z': z, 'class': classname})
                except (ValueError, IndexError):
                    pass
            current = {}
        elif line.startswith('"'):
            # "key" "value"
            parts = line.split('"')
            if len(parts) >= 4:
                current[parts[1]] = parts[3]
    return spawns
