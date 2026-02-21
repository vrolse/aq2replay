import os
import re
import time
from functools import lru_cache
from typing import Optional
from flask import Flask, render_template, jsonify, abort, request, send_file

# Allowlist for map names — only alphanumerics, underscores, hyphens and dots
_SAFE_NAME_RE = re.compile(r'^[a-zA-Z0-9_\-\.]+$')

from parsers.bsp import load_bsp
from parsers.mvd2 import load_mvd2
from parsers.topview import render_topview

# ── Paths ──────────────────────────────────────────────────────────────────────

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MVD2_DIR     = os.path.join(BASE_DIR, 'mvd2')
BSP_DIR      = os.path.join(BASE_DIR, 'bsp')
TEX_DIR      = os.path.join(BASE_DIR, 'textures')
PALETTE_PATH = os.path.join(BASE_DIR, 'colormap.pcx')
TOPVIEW_CACHE_DIR = os.path.join(BASE_DIR, 'cache', 'topview')
os.makedirs(TOPVIEW_CACHE_DIR, exist_ok=True)

# ── App ────────────────────────────────────────────────────────────────────────

app = Flask(__name__, template_folder='templates', static_folder='static')

# ── Data helpers ───────────────────────────────────────────────────────────────

_geo_cache: dict = {}   # only caches successful loads; misses are retried each request

def _map_geo(mapname: str):
    """Load BSP geometry for mapname. Caches hits; always retries misses (new BSP files)."""
    if mapname in _geo_cache:
        return _geo_cache[mapname]
    path = os.path.join(BSP_DIR, f'{mapname}.bsp')
    if not os.path.exists(path):
        # Case-insensitive scan
        for entry in os.scandir(BSP_DIR):
            if entry.name.lower() == f'{mapname.lower()}.bsp':
                path = entry.path
                break
        else:
            return None   # not cached — will retry next request
    try:
        geo = load_bsp(path)
        _geo_cache[mapname] = geo
        return geo
    except Exception as e:
        app.logger.warning('BSP load failed for %s: %s', mapname, e)
        return None


_REPLAY_CACHE: dict = {'data': None, 'expires': 0.0}
_REPLAY_CACHE_TTL = 60  # seconds — re-scan disk at most once per minute


def _get_all_replays() -> list:
    """Return full sorted replay list, refreshed at most every TTL seconds."""
    now = time.time()
    if _REPLAY_CACHE['data'] is None or now > _REPLAY_CACHE['expires']:
        raw_relpaths: set = set()
        candidates: list = []
        for dirpath, _dirs, files in os.walk(MVD2_DIR):
            for fname in files:
                if not (fname.endswith('.mvd2') or fname.endswith('.mvd2.gz')):
                    continue
                abspath = os.path.join(dirpath, fname)
                relpath = os.path.relpath(abspath, MVD2_DIR).replace(os.sep, '/')
                candidates.append({
                    'filename': relpath,
                    'size':     os.path.getsize(abspath),
                    'mtime':    os.path.getmtime(abspath),
                })
                if fname.endswith('.mvd2'):
                    raw_relpaths.add(relpath)
        result = [
            r for r in candidates
            if not r['filename'].endswith('.gz')
            or r['filename'][:-3] not in raw_relpaths
        ]
        result.sort(key=lambda r: r['mtime'], reverse=True)
        _REPLAY_CACHE['data'] = result
        _REPLAY_CACHE['expires'] = now + _REPLAY_CACHE_TTL
    return _REPLAY_CACHE['data']


def _list_replays():
    """Backwards-compatible wrapper."""
    return _get_all_replays()


# ── Page routes ────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    # Data is fetched client-side via /api/dirs + /api/replays
    return render_template('index.html')


@app.route('/replay/<path:filename>')
def replay_page(filename: str):
    # Resolve against MVD2_DIR and ensure we stay inside it
    candidate = os.path.abspath(os.path.join(MVD2_DIR, filename.replace('/', os.sep)))
    if not candidate.startswith(os.path.abspath(MVD2_DIR) + os.sep):
        abort(400)
    if not os.path.exists(candidate) and not os.path.exists(candidate + '.gz'):
        abort(404)
    return render_template('replay.html', filename=filename)


# ── API routes ─────────────────────────────────────────────────────────────────

@app.route('/api/map/<mapname>/geo')
def api_map_geo(mapname: str):
    if not _SAFE_NAME_RE.match(mapname):
        abort(400)
    geo = _map_geo(mapname)
    if geo is None:
        abort(404)
    return jsonify(geo)


@app.route('/api/replays')
def api_replays():
    """Paginated, filterable replay list.

    Query params:
      dir      — only return files directly inside this subdir (prefix match)
      q        — case-insensitive substring filter on filename
      page     — 1-based page number (default 1)
      per_page — items per page (default 100, max 500)
    """
    all_r = _get_all_replays()

    dir_filter = request.args.get('dir', '').strip('/')
    q          = request.args.get('q', '').lower().strip()
    try:
        page     = max(1, int(request.args.get('page', 1)))
        per_page = min(500, max(1, int(request.args.get('per_page', 100))))
    except ValueError:
        page, per_page = 1, 100

    filtered = all_r
    if dir_filter:
        prefix = dir_filter + '/'
        filtered = [r for r in filtered if r['filename'].startswith(prefix)]
    if q:
        filtered = [r for r in filtered if q in r['filename'].lower()]

    total = len(filtered)
    start = (page - 1) * per_page
    items = filtered[start:start + per_page]

    return jsonify({
        'total':    total,
        'page':     page,
        'per_page': per_page,
        'pages':    max(1, (total + per_page - 1) // per_page),
        'items':    items,
    })


@app.route('/api/dirs')
def api_dirs():
    """Return sorted list of all subdirectory paths that contain replays."""
    all_r = _get_all_replays()
    dirs: set = set()
    for r in all_r:
        parts = r['filename'].split('/')
        # Add every ancestor path (so nested folders are all listed)
        for i in range(1, len(parts)):
            dirs.add('/'.join(parts[:i]))
    return jsonify(sorted(dirs))


@lru_cache(maxsize=8)
def _load_mvd2_cached(path: str):
    """Parse and cache full MVD2 replay (max_frames=0)."""
    return load_mvd2(path, max_frames=0)


@app.route('/api/replay/<path:filename>')
def api_replay(filename: str):
    max_frames = request.args.get('max_frames', 0, type=int)
    path = os.path.join(MVD2_DIR, filename)
    # Security: stay within MVD2_DIR
    if not os.path.abspath(path).startswith(os.path.abspath(MVD2_DIR)):
        abort(400)
    if not os.path.exists(path):
        abort(404)
    try:
        if max_frames:
            data = load_mvd2(path, max_frames=max_frames)
        else:
            data = _load_mvd2_cached(path)
    except Exception as e:
        app.logger.exception('Failed to parse %s', filename)
        return jsonify({'error': 'Failed to parse replay', 'filename': filename}), 500
    return jsonify(data)


@app.route('/download/<path:filename>')
def download_replay(filename: str):
    """Serve the raw .mvd2 / .mvd2.gz file as a download attachment."""
    path = os.path.join(MVD2_DIR, filename)
    # Security: stay within MVD2_DIR
    if not os.path.abspath(path).startswith(os.path.abspath(MVD2_DIR)):
        abort(400)
    if not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=os.path.basename(filename))


# ── Topview routes ────────────────────────────────────────────────────────────

TOPVIEW_SIZE = 1024


def _topview_path(mapname: str) -> str:
    return os.path.join(TOPVIEW_CACHE_DIR, f'{mapname}.png')


def _ensure_topview(mapname: str) -> Optional[str]:
    """Render and cache the topview PNG; return its path or None on failure."""
    out = _topview_path(mapname)
    if os.path.exists(out):
        return out
    bsp_path = os.path.join(BSP_DIR, f'{mapname}.bsp')
    if not os.path.exists(bsp_path):
        for entry in os.scandir(BSP_DIR):
            if entry.name.lower() == f'{mapname.lower()}.bsp':
                bsp_path = entry.path
                break
        else:
            return None
    try:
        with open(bsp_path, 'rb') as f:
            bsp_data = f.read()
        img = render_topview(bsp_data, TEX_DIR, PALETTE_PATH, img_size=TOPVIEW_SIZE)
        img.save(out)
        return out
    except Exception as e:
        app.logger.warning('topview render failed for %s: %s', mapname, e)
        return None


@app.route('/api/map/<mapname>/topview.png')
def api_map_topview(mapname: str):
    if not _SAFE_NAME_RE.match(mapname):
        abort(400)
    path = _ensure_topview(mapname)
    if path is None:
        abort(404)
    return send_file(path, mimetype='image/png')


@app.route('/api/map/<mapname>/topview.json')
def api_map_topview_json(mapname: str):
    """Return projection params so the client can align the PNG on the canvas."""
    if not _SAFE_NAME_RE.match(mapname):
        abort(400)
    geo = _map_geo(mapname)
    if geo is None:
        abort(404)
    b = geo['bounds']
    range_x = b['max_x'] - b['min_x'] or 1
    range_y = b['max_y'] - b['min_y'] or 1
    scale = min(TOPVIEW_SIZE / range_x, TOPVIEW_SIZE / range_y)
    off_x = (TOPVIEW_SIZE - range_x * scale) / 2
    off_y = (TOPVIEW_SIZE - range_y * scale) / 2
    return jsonify({
        'img_size': TOPVIEW_SIZE,
        'scale':    scale,
        'off_x':    off_x,
        'off_y':    off_y,
    })


# ── Dev server ─────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    debug = os.environ.get('FLASK_DEBUG', 'true').lower() == 'true'
    app.run(debug=debug, host='0.0.0.0', port=5000)

