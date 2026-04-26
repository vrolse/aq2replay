import os
import re
import json
import time
import hmac
import sqlite3
import threading
import collections
import urllib.error as _urllib_error
import urllib.request as _urllib_request
from functools import lru_cache
from typing import Optional
import queue as _queue
from flask import Flask, render_template, jsonify, abort, request, send_file, Response, stream_with_context
from werkzeug.middleware.proxy_fix import ProxyFix

# Allowlist for map names — only alphanumerics, underscores, hyphens and dots
_SAFE_NAME_RE = re.compile(r'^[a-zA-Z0-9_\-\.]+$')

from parsers.bsp import load_bsp
from parsers.mvd2 import load_mvd2
from parsers.topview import render_topview_svg, list_bsp_textures
from parsers.mesh import build_mesh
from parsers.gtv import GtvClient, GtvError, LiveGameState, GTV_PORT_MIN, GTV_PORT_MAX
import db as _db

# ── Paths ──────────────────────────────────────────────────────────────────────

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MVD2_DIR     = os.path.join(BASE_DIR, 'mvd2')
BSP_DIR      = os.path.join(BASE_DIR, 'bsp')
TEX_DIR      = os.path.join(BASE_DIR, 'textures')
PALETTE_PATH = os.path.join(BASE_DIR, 'colormap.pcx')
TOPVIEW_CACHE_DIR = os.path.join(BASE_DIR, 'cache', 'topview')
MESH_CACHE_DIR    = os.path.join(BASE_DIR, 'cache', 'mesh')
PLAYERS_DIR       = os.path.join(BASE_DIR, 'players')
MODELS_DIR        = os.path.join(BASE_DIR, 'models')
_MD2_CACHE_DIR    = os.path.join(BASE_DIR, 'cache', 'md2')
os.makedirs(TOPVIEW_CACHE_DIR, exist_ok=True)
os.makedirs(MESH_CACHE_DIR, exist_ok=True)
os.makedirs(_MD2_CACHE_DIR, exist_ok=True)
_db_env_path = (os.environ.get('STATS_DB_PATH', '') or '').strip()
if _db_env_path:
    _DB_PATH = _db_env_path if os.path.isabs(_db_env_path) else os.path.join(BASE_DIR, _db_env_path)
else:
    _DB_PATH = os.path.join(BASE_DIR, 'cache', 'stats.db')

# ── Remote asset download server ───────────────────────────────────────────────
# Files are fetched on demand and cached locally under cache/remote/.
# The download server mirrors the standard AQ2 directory layout under its base URL.
ASSET_BASE_URL   = os.environ.get('ASSET_BASE_URL',
                       'http://dlserver.aq2world.com/action/action').rstrip('/')
REMOTE_ASSET_DIR = os.path.join(BASE_DIR, 'cache', 'remote')
os.makedirs(REMOTE_ASSET_DIR, exist_ok=True)


def _download_asset(rel_path: str) -> 'Optional[str]':
    """Download *rel_path* from ASSET_BASE_URL into the local remote-asset cache.

    Returns the local filesystem path on success, or None on any failure.
    Downloads are idempotent: an already-cached file is returned immediately.
    rel_path must use forward slashes and must not contain '..'.
    """
    if not ASSET_BASE_URL:
        return None
    if '..' in rel_path or rel_path.startswith('/'):
        return None
    local = os.path.join(REMOTE_ASSET_DIR, rel_path.replace('/', os.sep))
    if os.path.exists(local):
        return local
    url = f'{ASSET_BASE_URL}/{rel_path}'
    tmp = local + '.tmp'
    try:
        os.makedirs(os.path.dirname(local), exist_ok=True)
        req = _urllib_request.Request(url, headers={'User-Agent': 'aq2replay/1.0'})
        with _urllib_request.urlopen(req, timeout=15) as resp:
            data = resp.read(16 * 1024 * 1024)   # hard cap: 16 MB per asset
        with open(tmp, 'wb') as fh:
            fh.write(data)
        os.replace(tmp, local)
        return local
    except Exception as exc:
        app.logger.debug('Asset download failed %s: %s', url, exc)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return None

# ── App ────────────────────────────────────────────────────────────────────────

app = Flask(__name__, template_folder='templates', static_folder='static')

# Trust one reverse proxy hop by default (Nginx Proxy Manager).
try:
    _proxy_fix_x_for = max(0, int(os.environ.get('PROXY_FIX_X_FOR', '1')))
except ValueError:
    _proxy_fix_x_for = 1
if os.environ.get('PROXY_FIX_ENABLED', 'true').lower() == 'true':
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=_proxy_fix_x_for, x_proto=1, x_host=1, x_port=1)

_enable_hsts = os.environ.get('ENABLE_HSTS', 'false').lower() == 'true'
_api_bearer_auth_enabled = os.environ.get('API_BEARER_AUTH_ENABLED', 'false').lower() == 'true'
_api_bearer_token = os.environ.get('API_BEARER_TOKEN', '').strip()
_api_bearer_header = os.environ.get('API_BEARER_HEADER', 'X-Api-Bearer').strip() or 'X-Api-Bearer'
_api_bearer_scope = os.environ.get('API_BEARER_SCOPE', 'admin').strip().lower()
if _api_bearer_scope not in ('admin', 'all'):
    _api_bearer_scope = 'admin'
_api_rate_limit_enabled = os.environ.get('API_RATE_LIMIT_ENABLED', 'true').lower() == 'true'
try:
    _api_rate_limit_per_min = max(30, int(os.environ.get('API_RATE_LIMIT_PER_MIN', '240')))
except ValueError:
    _api_rate_limit_per_min = 240
_api_rate_state: dict = {}
_api_rate_lock = threading.Lock()

_db.init_db(MVD2_DIR, _DB_PATH)
# Only start the background indexer in the actual server process.
# With debug=True, Werkzeug's reloader spawns two processes (parent monitor +
# child server). Both import this module, so without this guard two indexers
# can fight over the same SQLite write lock.
_debug_mode = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
_reindex_api_enabled = os.environ.get('STATS_REINDEX_API_ENABLED', 'false').lower() == 'true'
_reindex_api_token = os.environ.get('STATS_REINDEX_API_TOKEN', '').strip()
_is_reloader_child = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
if (not _debug_mode) or _is_reloader_child:
    _db.trigger_reindex()
else:
    app.logger.info('stats: skip indexer in debug reloader parent process')
if _reindex_api_enabled and (not _reindex_api_token) and (not _debug_mode):
    app.logger.warning('stats: STATS_REINDEX_API_ENABLED=true without token; manual reindex endpoint will refuse requests')
if _api_bearer_auth_enabled and (not _api_bearer_token):
    app.logger.warning('stats: API_BEARER_AUTH_ENABLED=true without API_BEARER_TOKEN; all /api requests will be refused')


def _requires_api_bearer(path: str) -> bool:
    """Return whether a path should enforce global bearer auth."""
    if not _api_bearer_auth_enabled or not path.startswith('/api/'):
        return False
    if _api_bearer_scope == 'all':
        return True
    # admin scope: protect only explicitly sensitive endpoints.
    return path == '/api/stats/reindex' or path.startswith('/api/admin/')

@app.after_request
def no_cache_api(response):
    """Prevent Cloudflare (and any other proxy) from caching /api/ responses.
    Without this, Cloudflare caches URLs ending in .svg/.json as static assets,
    serving stale or wrong-sized SVGs to clients."""
    if request.path.startswith('/api/'):
        response.headers['Cache-Control'] = 'no-store'
    # Baseline security headers that are safe for this app's current templates.
    response.headers.setdefault('X-Content-Type-Options', 'nosniff')
    response.headers.setdefault('X-Frame-Options', 'SAMEORIGIN')
    response.headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
    response.headers.setdefault('Permissions-Policy', 'geolocation=(), microphone=(), camera=()')
    if _enable_hsts:
        response.headers.setdefault('Strict-Transport-Security', 'max-age=31536000; includeSubDomains')
    return response


@app.before_request
def api_request_guards():
    """Optional API auth + in-process per-IP limiter for abuse protection."""
    if not request.path.startswith('/api/'):
        return

    if _requires_api_bearer(request.path):
        if not _api_bearer_token:
            abort(503)
        provided = request.headers.get(_api_bearer_header, '').strip()
        if not provided:
            auth = request.headers.get('Authorization', '').strip()
            if auth.lower().startswith('bearer '):
                provided = auth[7:].strip()
        if not provided or not hmac.compare_digest(provided, _api_bearer_token):
            abort(401)

    if not _api_rate_limit_enabled:
        return

    ip = request.remote_addr or 'unknown'
    now = time.monotonic()
    with _api_rate_lock:
        win_start, count = _api_rate_state.get(ip, (now, 0))
        if now - win_start >= 60:
            win_start, count = now, 0
        if count >= _api_rate_limit_per_min:
            abort(429)
        _api_rate_state[ip] = (win_start, count + 1)


@app.route('/healthz')
def healthz():
    """Lightweight liveness endpoint for container health checks."""
    return jsonify({'status': 'ok'}), 200


def _show_reindex_button() -> bool:
    """Only expose manual reindex UI in local debug without token auth."""
    return _reindex_api_enabled and _debug_mode and (not _reindex_api_token)


def _extract_reindex_token() -> str:
    token = request.headers.get('X-Reindex-Token', '').strip()
    if token:
        return token
    auth = request.headers.get('Authorization', '').strip()
    if auth.lower().startswith('bearer '):
        return auth[7:].strip()
    return ''


def _require_reindex_access() -> None:
    """Gate manual reindex endpoint behind explicit env flag and token."""
    if not _reindex_api_enabled:
        abort(404)
    if _reindex_api_token:
        provided = _extract_reindex_token()
        if not provided or not hmac.compare_digest(provided, _reindex_api_token):
            abort(403)
        return
    # Tokenless mode is only allowed in local debug sessions.
    if not _debug_mode:
        abort(503)


@app.errorhandler(sqlite3.DatabaseError)
def handle_sqlite_database_error(exc):
    """Handle SQLite failures gracefully; auto-recover on corruption."""
    if _db.is_db_corruption_error(exc):
        app.logger.error('stats: sqlite corruption detected: %s', exc)
        recovered = _db.recover_corrupted_db(str(exc))
        message = 'Stats database corruption detected. Rebuilding from replay files; retry shortly.'
        status = 503 if recovered else 500
        if request.path.startswith('/api/'):
            return jsonify({'error': message, 'rebuilding': bool(recovered)}), status
        return message, status

    app.logger.exception('stats: sqlite database error: %s', exc)
    if request.path.startswith('/api/'):
        return jsonify({'error': 'Database error'}), 500
    return 'Database error', 500

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


_REPLAY_CACHE: dict = {'data': None, 'mtime': 0.0}
_REPLAY_CACHE_TTL = 300          # seconds — stale-while-revalidate window
_REPLAY_CACHE_LOCK = threading.Lock()
_REPLAY_CACHE_REFRESHING = False
_REPLAY_CACHE_FILE = os.path.join(BASE_DIR, 'cache', 'replay_list.json')


def _scan_replays_disk() -> list:
    """Walk MVD2_DIR with os.scandir, using DirEntry.stat() to avoid extra
    syscalls (getsize + getmtime → one stat call). Significantly faster than
    os.walk on slow mounts (Docker Desktop bind mounts, network shares, etc.)."""
    raw_relpaths: set = set()
    candidates: list = []
    root_abs = os.path.abspath(MVD2_DIR)

    def walk(path: str) -> None:
        try:
            it = os.scandir(path)
        except OSError:
            return
        with it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        walk(entry.path)
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                except OSError:
                    continue
                name = entry.name
                if not (name.endswith('.mvd2') or name.endswith('.mvd2.gz')):
                    continue
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                relpath = os.path.relpath(entry.path, root_abs).replace(os.sep, '/')
                candidates.append({
                    'filename': relpath,
                    'size':     st.st_size,
                    'mtime':    st.st_mtime,
                })
                if name.endswith('.mvd2'):
                    raw_relpaths.add(relpath)

    walk(root_abs)

    result = [
        r for r in candidates
        if not r['filename'].endswith('.gz')
        or r['filename'][:-3] not in raw_relpaths
    ]
    result.sort(key=lambda r: r['mtime'], reverse=True)
    return result


def _save_replay_cache_to_disk(data: list) -> None:
    try:
        os.makedirs(os.path.dirname(_REPLAY_CACHE_FILE), exist_ok=True)
        tmp = _REPLAY_CACHE_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as fh:
            json.dump(data, fh, separators=(',', ':'))
        os.replace(tmp, _REPLAY_CACHE_FILE)
    except Exception as e:
        app.logger.warning('Failed to persist replay cache: %s', e)


def _load_replay_cache_from_disk() -> Optional[list]:
    try:
        with open(_REPLAY_CACHE_FILE, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
        if isinstance(data, list):
            return data
    except (FileNotFoundError, ValueError, OSError):
        return None
    except Exception as e:
        app.logger.warning('Failed to load replay cache: %s', e)
    return None


def _refresh_replay_cache_sync() -> list:
    data = _scan_replays_disk()
    _REPLAY_CACHE['data'] = data
    _REPLAY_CACHE['mtime'] = time.time()
    _save_replay_cache_to_disk(data)
    return data


def _refresh_replay_cache_background() -> None:
    global _REPLAY_CACHE_REFRESHING
    with _REPLAY_CACHE_LOCK:
        if _REPLAY_CACHE_REFRESHING:
            return
        _REPLAY_CACHE_REFRESHING = True
    try:
        _refresh_replay_cache_sync()
    except Exception as e:
        app.logger.warning('Background replay scan failed: %s', e)
    finally:
        with _REPLAY_CACHE_LOCK:
            _REPLAY_CACHE_REFRESHING = False


def _get_all_replays() -> list:
    """Return the full replay list from cache. First request after a cold start
    loads a persisted JSON snapshot (instant) and kicks off a background refresh.
    Subsequent stale requests also refresh in the background — the caller always
    gets current in-memory data without blocking."""
    data = _REPLAY_CACHE['data']
    now = time.time()

    if data is None:
        disk = _load_replay_cache_from_disk()
        if disk is not None:
            _REPLAY_CACHE['data'] = disk
            _REPLAY_CACHE['mtime'] = 0.0  # force background refresh on first access
            data = disk
            threading.Thread(target=_refresh_replay_cache_background, daemon=True).start()
        else:
            return _refresh_replay_cache_sync()

    if (now - _REPLAY_CACHE['mtime']) > _REPLAY_CACHE_TTL:
        threading.Thread(target=_refresh_replay_cache_background, daemon=True).start()

    return data


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


@app.route('/stats')
def stats_page():
    return render_template('stats.html', show_reindex_button=_show_reindex_button())


@app.route('/stats/players')
def stats_players_page():
    return render_template('stats_players.html')


@app.route('/stats/maps')
def stats_maps_page():
    return render_template('stats_maps.html')


@app.route('/stats/activity')
def stats_activity_page():
    return render_template('stats_activity.html')


@app.route('/stats/matches')
def stats_matches_page():
    return render_template('stats_matches.html')


@app.route('/stats/weapons')
def stats_weapons_page():
    return render_template('stats_weapons.html')


@app.route('/stats/analytics')
def stats_analytics_page():
    return render_template('stats_analytics.html')


@app.route('/stats/advanced')
def stats_advanced_page():
    return render_template('stats_advanced.html', show_reindex_button=_show_reindex_button())


@app.route('/rankings')
def rankings_page():
    return render_template('rankings.html')


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
        per_page = min(100000, max(1, int(request.args.get('per_page', 100))))
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


@app.route('/api/replay/<path:filename>/insights')
def api_replay_insights(filename: str):
    return jsonify(_db.get_replay_ai_summary(filename))


@app.route('/api/replay/<path:filename>/highlights')
def api_replay_highlights(filename: str):
    limit = request.args.get('limit', 20, type=int)
    return jsonify(_db.get_replay_highlights(filename, limit=min(max(limit, 1), 100)))


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

def _topview_svg_path(mapname: str) -> str:
    return os.path.join(TOPVIEW_CACHE_DIR, f'{mapname}.svg')


def _topview_bounds_path(mapname: str) -> str:
    return os.path.join(TOPVIEW_CACHE_DIR, f'{mapname}.bounds.json')


def _ensure_topview(mapname: str) -> Optional[str]:
    """Render and cache the topview SVG; return its path or None on failure."""
    import json as _json
    out = _topview_svg_path(mapname)
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
        # Pre-fetch textures from the remote asset server into cache/remote/textures/
        # so the SVG renderer gets real colours instead of name-based heuristics.
        # Try .wal first (native Q2 format), then PNG/JPG; stop on first hit.
        remote_tex_dir = os.path.join(REMOTE_ASSET_DIR, 'textures')
        for tex_name in list_bsp_textures(bsp_data):
            tex_lower = tex_name.lower()
            for ext in ('.wal', '.png', '.jpg'):
                if _download_asset(f'textures/{tex_lower}{ext}'):
                    break
        svg_text, bounds = render_topview_svg(
            bsp_data, TEX_DIR, PALETTE_PATH, tex_root2=remote_tex_dir)
        with open(out, 'w', encoding='utf-8') as f:
            f.write(svg_text)
        with open(_topview_bounds_path(mapname), 'w') as f:
            _json.dump(bounds, f)
        return out
    except Exception as e:
        app.logger.warning('topview render failed for %s: %s', mapname, e)
        return None


@app.route('/api/map/<mapname>/topview.svg')
def api_map_topview(mapname: str):
    if not _SAFE_NAME_RE.match(mapname):
        abort(400)
    path = _ensure_topview(mapname)
    if path is None:
        abort(404)
    return send_file(path, mimetype='image/svg+xml')


@app.route('/api/map/<mapname>/topview.json')
def api_map_topview_json(mapname: str):
    """Return world-space bounds for the SVG so the client can place it on canvas."""
    import json as _json
    if not _SAFE_NAME_RE.match(mapname):
        abort(400)
    # Ensure the SVG (and its bounds sidecar) exist
    if _ensure_topview(mapname) is None:
        abort(404)
    bounds_path = _topview_bounds_path(mapname)
    try:
        with open(bounds_path) as f:
            bounds = _json.load(f)
    except Exception:
        abort(404)
    return jsonify({'format': 'svg', **bounds})


# ── 3-D mesh route ────────────────────────────────────────────────────────────

def _mesh_cache_path(mapname: str) -> str:
    return os.path.join(MESH_CACHE_DIR, f'{mapname}.mesh.json')


@app.route('/api/map/<mapname>/mesh')
def api_map_mesh(mapname: str):
    """Return a JSON triangle mesh with UV coordinates for Three.js."""
    import json as _json
    if not _SAFE_NAME_RE.match(mapname):
        abort(400)

    cache_path = _mesh_cache_path(mapname)
    if os.path.exists(cache_path):
        return send_file(cache_path, mimetype='application/json')

    bsp_path = os.path.join(BSP_DIR, f'{mapname}.bsp')
    if not os.path.exists(bsp_path):
        for entry in os.scandir(BSP_DIR):
            if entry.name.lower() == f'{mapname.lower()}.bsp':
                bsp_path = entry.path
                break
        else:
            abort(404)
    try:
        with open(bsp_path, 'rb') as f:
            bsp_data = f.read()
        mesh = build_mesh(bsp_data, TEX_DIR)
        with open(cache_path, 'w', encoding='utf-8') as f:
            _json.dump(mesh, f, separators=(',', ':'))
        return send_file(cache_path, mimetype='application/json')
    except Exception as e:
        app.logger.exception('mesh build failed for %s: %s', mapname, e)
        abort(500)


# ── Texture PNG route ─────────────────────────────────────────────────────────
# Serves WAL textures converted to PNG so Three.js TextureLoader can use them.

_TEX_CACHE_DIR = os.path.join(BASE_DIR, 'cache', 'textures')
os.makedirs(_TEX_CACHE_DIR, exist_ok=True)


@app.route('/api/map/<mapname>/texture/<path:texname>')
def api_texture(mapname: str, texname: str):
    """
    Convert a WAL/PNG/JPG texture to PNG and serve it.
    mapname is unused here (all textures are global) but kept for REST clarity.
    texname is the texture path without extension, e.g. 'flunx/dirt'.
    """
    import json as _json
    # Security: texname must be safe characters, no path traversal
    if not re.match(r'^[a-zA-Z0-9_\-\./]+$', texname) or '..' in texname:
        abort(400)
    if not _SAFE_NAME_RE.match(mapname):
        abort(400)

    # Cached PNG? (only real textures are cached; magenta fallbacks are never written)
    safe_key = texname.lower().replace('/', '__')
    cached = os.path.join(_TEX_CACHE_DIR, safe_key + '.png')
    if os.path.exists(cached):
        return send_file(cached, mimetype='image/png')

    # Find source file — try original case then lowercase (BSP names are often uppercase
    # but local files and the download server use lowercase)
    src_path = None
    for name_variant in dict.fromkeys([texname, texname.lower()]):
        for ext in ('.wal', '.png', '.jpg', '.jpeg'):
            candidate = os.path.join(TEX_DIR, name_variant.replace('/', os.sep) + ext)
            if os.path.exists(candidate):
                src_path = candidate
                break
        if src_path:
            break

    if src_path is None:
        # Try to download texture from the remote asset server (always lowercase)
        if ASSET_BASE_URL:
            tex_lower = texname.lower()
            for ext in ('.wal', '.png', '.jpg', '.jpeg'):
                downloaded = _download_asset(f'textures/{tex_lower}{ext}')
                if downloaded:
                    src_path = downloaded
                    break

    if src_path is None:
        # Return a 4×4 magenta texture so Three.js doesn't hang — NOT cached to disk
        # so the next request will retry the download (asset may appear later).
        import io as _io
        from PIL import Image as _Image
        buf = _io.BytesIO()
        _Image.new('RGB', (4, 4), (180, 0, 180)).save(buf, 'PNG')
        buf.seek(0)
        return send_file(buf, mimetype='image/png')

    try:
        from PIL import Image as _Image
        if src_path.endswith('.wal'):
            from parsers.topview import load_q2_palette, decode_wal
            pal = load_q2_palette(PALETTE_PATH)
            with open(src_path, 'rb') as f:
                raw = f.read()
            img = decode_wal(raw, pal).convert('RGBA')
        else:
            img = _Image.open(src_path).convert('RGBA')
        img.save(cached, 'PNG')
        return send_file(cached, mimetype='image/png')
    except Exception as e:
        app.logger.warning('texture convert failed for %s: %s', texname, e)
        abort(404)


# ── Player MD2 model route ────────────────────────────────────────────────────

@app.route('/api/player/<model>/md2')
def api_player_md2(model: str):
    """Return parsed MD2 player model as Three.js-ready JSON (cached to disk)."""
    import json as _json
    if not re.match(r'^[a-zA-Z0-9_]+$', model):
        abort(404)

    cache_path = os.path.join(_MD2_CACHE_DIR, f'{model}.md2.json')
    if os.path.exists(cache_path):
        return send_file(cache_path, mimetype='application/json')

    md2_path = os.path.join(PLAYERS_DIR, model, 'tris.md2')
    if not os.path.exists(md2_path):
        md2_path = _download_asset(f'players/{model}/tris.md2')
        if not md2_path:
            abort(404)

    try:
        from parsers.md2 import parse_md2_json
        result = parse_md2_json(md2_path)
        with open(cache_path, 'w', encoding='utf-8') as f:
            _json.dump(result, f, separators=(',', ':'))
        return send_file(cache_path, mimetype='application/json')
    except Exception as e:
        app.logger.exception('MD2 parse failed for %s: %s', model, e)
        abort(500)


@app.route('/api/player/<model>/weapon/<weapname>/md2')
def api_weapon_md2(model: str, weapname: str):
    """Return parsed MD2 weapon model as Three.js-ready JSON (full animations, cached)."""
    import json as _json
    if not re.match(r'^[a-zA-Z0-9_]+$', model):
        abort(404)
    if not re.match(r'^[a-zA-Z0-9_]+$', weapname):
        abort(404)

    # v2 cache: full animations + skin_url (replaces old single-frame v1 cache)
    cache_path = os.path.join(_MD2_CACHE_DIR, f'{model}.weapon.{weapname}.v2.md2.json')
    if os.path.exists(cache_path):
        return send_file(cache_path, mimetype='application/json')

    md2_path = os.path.join(PLAYERS_DIR, model, f'{weapname}.md2')
    if not os.path.exists(md2_path):
        md2_path = _download_asset(f'players/{model}/{weapname}.md2')
    if not md2_path or not os.path.exists(md2_path):
        abort(404)

    try:
        from parsers.md2 import parse_md2_json
        # Full animations — weapon frames match body frames 1:1
        result = parse_md2_json(md2_path)
        # Convert embedded skin_path → public skin_url (served via /models/ route)
        skin_path = result.pop('skin_path', '')
        if skin_path:
            result['skin_url'] = '/' + skin_path.replace('\\', '/')
        with open(cache_path, 'w', encoding='utf-8') as f:
            _json.dump(result, f, separators=(',', ':'))
        return send_file(cache_path, mimetype='application/json')
    except Exception as e:
        app.logger.exception('Weapon MD2 parse failed for %s/%s: %s', model, weapname, e)
        abort(500)


@app.route('/models/<path:filename>')
def serve_model_asset(filename: str):
    """Serve model asset files (e.g. weapon skin PNGs) from the models/ directory.
    Falls back to PCX→PNG conversion when a .png is requested but only .pcx exists.
    Falls back to downloading from the remote asset server when not present locally."""
    if '..' in filename or not re.match(r'^[a-zA-Z0-9_/\-\.]+$', filename):
        abort(404)
    import io as _io
    safe = os.path.normpath(os.path.join(MODELS_DIR, filename.replace('/', os.sep)))
    if not safe.startswith(os.path.normpath(MODELS_DIR) + os.sep):
        abort(403)
    if os.path.isfile(safe):
        return send_file(safe)
    # PNG requested but not present locally — try PCX conversion via Pillow
    if safe.endswith('.png'):
        pcx_path = safe[:-4] + '.pcx'
        if not os.path.isfile(pcx_path):
            pcx_path = None
        if pcx_path is None and ASSET_BASE_URL:
            # Try to download the PNG directly, then fall back to PCX
            remote_png = _download_asset(f'models/{filename}')
            if remote_png:
                return send_file(remote_png)
            pcx_rel = filename[:-4] + '.pcx'
            pcx_path = _download_asset(f'models/{pcx_rel}')
        if pcx_path and os.path.isfile(pcx_path):
            from PIL import Image
            img = Image.open(pcx_path).convert('RGBA')
            buf = _io.BytesIO()
            img.save(buf, 'PNG')
            buf.seek(0)
            return send_file(buf, mimetype='image/png')
    elif ASSET_BASE_URL:
        remote = _download_asset(f'models/{filename}')
        if remote:
            return send_file(remote)
    abort(404)


@app.route('/api/pics/<name>')
def api_pics(name: str):
    """Serve a HUD icon as PNG, fetching/converting the source PCX from the
    asset server on demand. AQ2 ships its weapon/item icons as 8-bit PCX files
    under pics/ (e.g. pics/w_mk23.pcx). We cache the converted PNG locally."""
    # Allow either bare name or .png/.pcx suffix; normalize to a stem.
    if not re.match(r'^[a-zA-Z0-9_\-]+(\.(?:png|pcx))?$', name):
        abort(404)
    stem = re.sub(r'\.(png|pcx)$', '', name)
    png_cache = os.path.join(REMOTE_ASSET_DIR, 'pics', stem + '.png')
    if os.path.exists(png_cache):
        return send_file(png_cache, mimetype='image/png')
    pcx_path = _download_asset(f'pics/{stem}.pcx')
    if not pcx_path:
        abort(404)
    try:
        from PIL import Image
        os.makedirs(os.path.dirname(png_cache), exist_ok=True)
        img = Image.open(pcx_path).convert('RGBA')
        img.save(png_cache, 'PNG')
        return send_file(png_cache, mimetype='image/png')
    except Exception as exc:
        app.logger.warning('pics convert failed for %s: %s', stem, exc)
        abort(404)


@app.route('/players/<path:skinfile>')
def player_skin(skinfile: str):
    """Serve player skin images (PNG/PCX) from the players/ directory.
    Falls back to downloading from the remote asset server when not present locally."""
    if not re.match(r'^[a-zA-Z0-9_/\-\.]+$', skinfile) or '..' in skinfile:
        abort(404)
    path = os.path.abspath(os.path.join(PLAYERS_DIR, skinfile.replace('/', os.sep)))
    if not path.startswith(os.path.normpath(PLAYERS_DIR) + os.sep):
        abort(403)
    if not os.path.isfile(path):
        remote = _download_asset(f'players/{skinfile}')
        if not remote:
            abort(404)
        path = remote
    return send_file(path)


# ── 3-D viewer page ───────────────────────────────────────────────────────────

@app.route('/view3d/<path:filename>')
def view3d_page(filename: str):
    """Serve the Three.js 3-D replay viewer."""
    candidate = os.path.abspath(os.path.join(MVD2_DIR, filename.replace('/', os.sep)))
    if not candidate.startswith(os.path.abspath(MVD2_DIR) + os.sep):
        abort(400)
    if not os.path.exists(candidate) and not os.path.exists(candidate + '.gz'):
        abort(404)
    return render_template('viewer3d.html', filename=filename)


@app.route('/md2test')
def md2test_page():
    """Standalone MD2 model + animation test page."""
    return render_template('md2test.html')


# ── Stats API ──────────────────────────────────────────────────────────────────

def _stats_mode_arg() -> str:
    """Read requested stats mode filter; db layer handles normalization."""
    return (request.args.get('mode', 'teamplay', type=str) or 'teamplay').strip()


def _stats_period_arg() -> str:
    """Read requested period filter; db layer handles normalization/defaults."""
    return (request.args.get('period', 'this_year', type=str) or 'this_year').strip()


def _stats_week_arg() -> str:
    """Read optional week filter in YYYY-WW format; db layer handles normalization."""
    return (request.args.get('week', '', type=str) or '').strip()


@app.route('/api/stats/summary')
def api_stats_summary():
    return jsonify(_db.get_summary(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/stats/leaderboard')
def api_stats_leaderboard():
    min_games = request.args.get('min_games', 1, type=int)
    return jsonify(_db.get_leaderboard(
        min_games=min_games,
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/stats/maps')
def api_stats_maps():
    return jsonify(_db.get_map_stats(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/stats/recent')
def api_stats_recent():
    limit = request.args.get('limit', 20, type=int)
    return jsonify(_db.get_recent_matches(
        limit=min(limit, 100),
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/stats/reindex', methods=['POST'])
def api_stats_reindex():
    _require_reindex_access()
    force = request.args.get('force', '0') in ('1', 'true', 'yes')
    _db.trigger_reindex(force=force)
    return jsonify(_db.get_index_status())


@app.route('/api/stats/status')
def api_stats_status():
    return jsonify(_db.get_index_status())


@app.route('/api/stats/records')
def api_stats_records():
    return jsonify(_db.get_records(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/stats/activity')
def api_stats_activity():
    return jsonify(_db.get_activity_by_week(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/stats/weapons')
def api_stats_weapons():
    return jsonify(_db.get_weapon_stats(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/stats/map-win-rates')
def api_stats_map_win_rates():
    return jsonify(_db.get_map_win_rates(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/stats/first-kill')
def api_stats_first_kill():
    limit = request.args.get('limit', 15, type=int)
    return jsonify(_db.get_first_kill_stats(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        limit=min(max(limit, 1), 100),
    ))


@app.route('/api/stats/team-analytics')
def api_stats_team_analytics():
    limit = request.args.get('limit', 20, type=int)
    return jsonify(_db.get_team_analytics(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        limit=min(max(limit, 1), 100),
    ))


@app.route('/api/stats/round-analytics')
def api_stats_round_analytics():
    limit = request.args.get('limit', 25, type=int)
    return jsonify(_db.get_round_analytics(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        limit=min(max(limit, 1), 200),
    ))


@app.route('/api/stats/match-analytics')
def api_stats_match_analytics():
    limit = request.args.get('limit', 20, type=int)
    return jsonify(_db.get_match_analytics(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        limit=min(max(limit, 1), 100),
    ))


@app.route('/api/stats/weapon-analytics')
def api_stats_weapon_analytics():
    limit = request.args.get('limit', 20, type=int)
    return jsonify(_db.get_weapon_analytics(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        limit=min(max(limit, 1), 200),
    ))


@app.route('/api/stats/playstyles')
def api_stats_playstyles():
    limit = request.args.get('limit', 100, type=int)
    min_games = request.args.get('min_games', 3, type=int)
    return jsonify(_db.get_behavior_analytics(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        min_games=min_games,
        limit=min(max(limit, 1), 500),
    ))


@app.route('/api/stats/ratings')
def api_stats_ratings():
    limit = request.args.get('limit', 100, type=int)
    min_games = request.args.get('min_games', 5, type=int)
    return jsonify(_db.get_rating_rankings(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        min_games=min_games,
        limit=min(max(limit, 1), 500),
    ))


@app.route('/api/stats/insights')
def api_stats_insights():
    limit = request.args.get('limit', 8, type=int)
    return jsonify(_db.get_ai_meta_insights(
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        limit=min(max(limit, 1), 20),
    ))


# ── Weapon detail ───────────────────────────────────────────────────────────────

@app.route('/weapon/<path:weapon>')
def weapon_page(weapon: str):
    return render_template('weapon.html', weapon_name=weapon)


@app.route('/api/weapon/<path:weapon>/summary')
def api_weapon_summary(weapon: str):
    return jsonify(_db.get_weapon_detail(weapon, period=_stats_period_arg()))


@app.route('/api/weapon/<path:weapon>/top-killers')
def api_weapon_top_killers(weapon: str):
    min_games = request.args.get('min_games', 1, type=int)
    return jsonify(_db.get_weapon_top_killers(
        weapon,
        min_games=min_games,
        period=_stats_period_arg(),
    ))


@app.route('/api/weapon/<path:weapon>/victims')
def api_weapon_victims(weapon: str):
    return jsonify(_db.get_weapon_victims(weapon, period=_stats_period_arg()))


@app.route('/api/weapon/<path:weapon>/map-effectiveness')
def api_weapon_map_effectiveness(weapon: str):
    limit = request.args.get('limit', 20, type=int)
    return jsonify(_db.get_weapon_map_effectiveness(
        weapon,
        period=_stats_period_arg(),
        limit=min(max(limit, 1), 100),
    ))


# ── Map detail ─────────────────────────────────────────────────────────────────

@app.route('/map/<path:map_name>')
def map_page(map_name: str):
    return render_template('map_detail.html', map_name=map_name)


@app.route('/api/map/<path:map_name>/detail')
def api_map_detail(map_name: str):
    return jsonify(_db.get_map_detail(map_name, period=_stats_period_arg()))


@app.route('/api/map/<path:map_name>/leaderboard')
def api_map_leaderboard(map_name: str):
    min_games = request.args.get('min_games', 1, type=int)
    return jsonify(_db.get_map_leaderboard(
        map_name,
        min_games=min_games,
        period=_stats_period_arg(),
    ))


@app.route('/api/map/<path:map_name>/recent')
def api_map_recent(map_name: str):
    limit = request.args.get('limit', 20, type=int)
    return jsonify(_db.get_map_recent_matches(
        map_name,
        limit=min(limit, 100),
        period=_stats_period_arg(),
    ))


@app.route('/api/map/<path:map_name>/round-analytics')
def api_map_round_analytics(map_name: str):
    limit = request.args.get('limit', 25, type=int)
    return jsonify(_db.get_round_analytics(
        mode='team_modes',
        period=_stats_period_arg(),
        map_name=map_name,
        limit=min(max(limit, 1), 200),
    ))


@app.route('/api/map/<path:map_name>/heatmap')
def api_map_heatmap(map_name: str):
    kind = request.args.get('kind', 'kills', type=str)
    cell_size = request.args.get('cell_size', 128.0, type=float)
    limit = request.args.get('limit', 2000, type=int)
    player = request.args.get('player', '', type=str)
    weapon = request.args.get('weapon', '', type=str)
    return jsonify(_db.get_map_heatmap(
        map_name,
        kind=kind,
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        cell_size=cell_size,
        player=player,
        weapon=weapon,
        limit=limit,
    ))


@app.route('/api/map/<path:map_name>/movement')
def api_map_movement(map_name: str):
    cell_size = request.args.get('cell_size', 128.0, type=float)
    route_cell_size = request.args.get('route_cell_size', 192.0, type=float)
    sample_limit = request.args.get('sample_limit', 200000, type=int)
    route_limit = request.args.get('route_limit', 25, type=int)
    max_frame_gap = request.args.get('max_frame_gap', 40, type=int)
    player = request.args.get('player', '', type=str)
    return jsonify(_db.get_map_movement_analytics(
        map_name,
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        player=player,
        cell_size=cell_size,
        route_cell_size=route_cell_size,
        sample_limit=sample_limit,
        route_limit=route_limit,
        max_frame_gap=max_frame_gap,
    ))


@app.route('/api/map/<path:map_name>/fk-routes')
def api_map_fk_routes(map_name: str):
    cell_size = request.args.get('cell_size', 128.0, type=float)
    route_cell_size = request.args.get('route_cell_size', 192.0, type=float)
    lookback_frames = request.args.get('lookback_frames', 80, type=int)
    route_limit = request.args.get('route_limit', 25, type=int)
    player = request.args.get('player', '', type=str)
    return jsonify(_db.get_map_fk_routes(
        map_name,
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        player=player,
        cell_size=cell_size,
        route_cell_size=route_cell_size,
        lookback_frames=lookback_frames,
        route_limit=route_limit,
    ))


@app.route('/api/map/<path:map_name>/weapons')
def api_map_weapons(map_name: str):
    with _db._connect() as conn:
        rows = conn.execute(
            '''SELECT DISTINCT ke.weapon
               FROM kill_events ke
               JOIN matches m ON m.id = ke.match_id
               WHERE m.map = ? AND ke.team_kill = 0 AND ke.weapon != '' AND ke.weapon != 'unknown'
               ORDER BY ke.weapon ASC''',
            (map_name,),
        ).fetchall()
    return jsonify([r[0] for r in rows])


@app.route('/api/map/<path:map_name>/zone-risk')
def api_map_zone_risk(map_name: str):
    cell_size = request.args.get('cell_size', 160.0, type=float)
    min_events = request.args.get('min_events', 2, type=int)
    limit = request.args.get('limit', 120, type=int)
    player = request.args.get('player', '', type=str)
    weapon = request.args.get('weapon', '', type=str)
    return jsonify(_db.get_map_zone_risk(
        map_name,
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        cell_size=cell_size,
        min_events=min_events,
        limit=limit,
        player=player,
        weapon=weapon,
    ))


@app.route('/api/map/<path:map_name>/spawn-analytics')
def api_map_spawn_analytics(map_name: str):
    cell_size = request.args.get('cell_size', 192.0, type=float)
    limit = request.args.get('limit', 30, type=int)
    return jsonify(_db.get_map_spawn_analytics(
        map_name,
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        cell_size=cell_size,
        limit=min(max(limit, 1), 200),
    ))


# ── Player profile ──────────────────────────────────────────────────────────────

@app.route('/player/<path:name>')
def player_page(name: str):
    return render_template('player.html', player_name=name)


@app.route('/api/player/<path:name>/summary')
def api_player_summary(name: str):
    return jsonify(_db.get_player_summary(
        name,
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/player/<path:name>/round-metrics')
def api_player_round_metrics(name: str):
    return jsonify(_db.get_player_round_metrics(
        name,
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/player/<path:name>/multikill-stats')
def api_player_multikill_stats(name: str):
    return jsonify(_db.get_player_multikill_stats(
        name,
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/player/<path:name>/playstyle')
def api_player_playstyle(name: str):
    return jsonify(_db.get_player_behavior_analytics(
        name,
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/player/<path:name>/rating-history')
def api_player_rating_history(name: str):
    limit = request.args.get('limit', 80, type=int)
    return jsonify(_db.get_player_rating_history(
        name,
        mode=_stats_mode_arg(),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
        limit=min(max(limit, 1), 500),
    ))


@app.route('/api/player/<path:name>/matches')
def api_player_matches(name: str):
    limit  = request.args.get('limit',  50, type=int)
    offset = request.args.get('offset',  0, type=int)
    return jsonify(_db.get_player_match_history(
        name,
        limit=min(limit, 200),
        offset=max(0, offset),
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/player/<path:name>/maps')
def api_player_maps(name: str):
    return jsonify(_db.get_player_map_breakdown(
        name,
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/player/<path:name>/weapons')
def api_player_weapons(name: str):
    return jsonify(_db.get_player_weapon_stats(
        name,
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/player/<path:name>/rivals')
def api_player_rivals(name: str):
    return jsonify(_db.get_player_rivals(
        name,
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


@app.route('/api/player/<path:name>/activity')
def api_player_activity(name: str):
    return jsonify(_db.get_player_activity_by_week(
        name,
        period=_stats_period_arg(),
        week=_stats_week_arg(),
    ))


# ── Player name search ──────────────────────────────────────────────────────────

@app.route('/api/players/search')
def api_players_search():
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify([])
    limit = min(request.args.get('limit', 20, type=int), 50)
    return jsonify(_db.search_players(q, limit=limit))


# ── Head-to-head ───────────────────────────────────────────────────────────────

@app.route('/h2h')
def h2h_page():
    p1 = request.args.get('p1', '').strip()
    p2 = request.args.get('p2', '').strip()
    return render_template('h2h.html', p1=p1, p2=p2)


@app.route('/api/h2h')
def api_h2h():
    p1 = request.args.get('p1', '').strip()
    p2 = request.args.get('p2', '').strip()
    period = request.args.get('period', '').strip()
    if not p1 or not p2:
        abort(400)
    return jsonify(_db.get_h2h(p1, p2, period=period))


@app.route('/api/h2h/rivalries')
def api_h2h_rivalries():
    limit = request.args.get('limit', 20, type=int)
    period = request.args.get('period', '').strip()
    return jsonify(_db.get_h2h_rivalries(
        limit=min(max(limit, 1), 100),
        period=period,
    ))


# ── Live (GTV) viewer ──────────────────────────────────────────────────────────
#
# A single TCP connection per (host, port, password) is shared across all
# browser subscribers. A background daemon thread reads GTS_STREAM_DATA blocks,
# feeds them into LiveGameState, and fans out each snapshot to every subscriber
# via per-subscriber bounded queues. SSE consumers drop frames if they fall
# behind (queue full → discard oldest) so a slow client never stalls the
# feeder thread.

_LIVE_PRESET_HOST       = os.environ.get('LIVE_PRESET_HOST', 'aq2.vrol.se').strip()
_LIVE_PRESET_PASSWORD   = os.environ.get('LIVE_PRESET_PASSWORD', 'aq2world').strip()
try:
    _LIVE_PRESET_PORT_MIN = int(os.environ.get('LIVE_PRESET_PORT_MIN', '27920'))
    _LIVE_PRESET_PORT_MAX = int(os.environ.get('LIVE_PRESET_PORT_MAX', '27960'))
except ValueError:
    _LIVE_PRESET_PORT_MIN, _LIVE_PRESET_PORT_MAX = 27920, 27960
_LIVE_PRESET_PORT_MIN = max(GTV_PORT_MIN, min(_LIVE_PRESET_PORT_MIN, GTV_PORT_MAX))
_LIVE_PRESET_PORT_MAX = max(GTV_PORT_MIN, min(_LIVE_PRESET_PORT_MAX, GTV_PORT_MAX))

# Ports shown in the server status widget on the index page.
# Defaults to 27930,27940,27950,27960; override via LIVE_STATUS_PORTS env var.
_LIVE_STATUS_PORTS: list = []
for _sp in os.environ.get('LIVE_STATUS_PORTS', '27930,27940,27950,27960').split(','):
    try:
        _spp = int(_sp.strip())
        if GTV_PORT_MIN <= _spp <= GTV_PORT_MAX:
            _LIVE_STATUS_PORTS.append(_spp)
    except ValueError:
        pass
# Optional human-readable labels per port, e.g. "27930=Main,27940=Pickup"
_LIVE_STATUS_LABELS: dict = {}
for _sl in os.environ.get('LIVE_STATUS_LABELS', '').split(','):
    if '=' in _sl:
        _slk, _slv = _sl.split('=', 1)
        try:
            _LIVE_STATUS_LABELS[int(_slk.strip())] = _slv.strip()
        except ValueError:
            pass

_LIVE_SUB_QUEUE_SIZE   = 16   # per-subscriber backlog before old frames drop
_LIVE_IDLE_GRACE_SEC   = 90   # close upstream after this long with no subscribers
_LIVE_MAX_SESSIONS     = 24
try:
    _LIVE_STREAM_DELAY_SEC = float(os.environ.get('LIVE_STREAM_DELAY_SEC', '60'))
except ValueError:
    _LIVE_STREAM_DELAY_SEC = 60.0


class _LiveSession:
    """One upstream GTV TCP connection shared by 0..N browser subscribers."""

    __slots__ = (
        'host', 'port', 'password', 'state', 'subscribers',
        '_lock', '_thread', '_stop', '_last_snapshot', '_last_realtime_snapshot',
        '_last_active', '_started_at', '_status', '_error',
    )

    def __init__(self, host: str, port: int, password: str):
        self.host        = host
        self.port        = port
        self.password    = password
        self.state       = LiveGameState()
        self.subscribers: list = []   # list of queue.Queue
        self._lock       = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop       = threading.Event()
        self._last_snapshot: Optional[dict] = None
        self._last_realtime_snapshot: Optional[dict] = None
        self._last_active: float = time.monotonic()
        self._started_at: float  = time.time()
        self._status     = 'idle'      # idle | connecting | streaming | closed | error
        self._error: Optional[str] = None

    # ---- subscriber bookkeeping -------------------------------------------------

    def add_subscriber(self) -> '_queue.Queue':
        q: '_queue.Queue' = _queue.Queue(maxsize=_LIVE_SUB_QUEUE_SIZE)
        with self._lock:
            self.subscribers.append(q)
            self._last_active = time.monotonic()
            # Warm new subscriber with the most recent snapshot if we have one.
            if self._last_snapshot is not None:
                try:
                    q.put_nowait(self._last_snapshot)
                except _queue.Full:
                    pass
        return q

    def remove_subscriber(self, q: '_queue.Queue') -> None:
        with self._lock:
            try:
                self.subscribers.remove(q)
            except ValueError:
                pass
            if not self.subscribers:
                self._last_active = time.monotonic()

    def _fanout(self, snap: dict) -> None:
        with self._lock:
            self._last_snapshot = snap
            for q in list(self.subscribers):
                try:
                    q.put_nowait(snap)
                except _queue.Full:
                    # Subscriber is behind: drop the oldest, then push the new one.
                    try:
                        q.get_nowait()
                    except _queue.Empty:
                        pass
                    try:
                        q.put_nowait(snap)
                    except _queue.Full:
                        pass

    # ---- feeder thread ----------------------------------------------------------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        t = threading.Thread(target=self._run, name=f'live-{self.host}:{self.port}', daemon=True)
        self._thread = t
        t.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        self._status = 'connecting'
        client = GtvClient(self.host, self.port, password=self.password,
                           name='aq2stats-live', read_timeout=30.0)
        try:
            client.connect()
        except GtvError as exc:
            self._status, self._error = 'error', str(exc)
            self._fanout({'type': 'error', 'error': str(exc)})
            self._fanout({'type': 'disconnect'})
            return
        except Exception as exc:                                   # noqa: BLE001
            self._status, self._error = 'error', f'connect: {exc!r}'
            self._fanout({'type': 'error', 'error': f'connect: {exc!r}'})
            self._fanout({'type': 'disconnect'})
            return

        self._status = 'streaming'
        # Delay buffer: deque of (release_time, snap) tuples protected by its
        # own lock so the drain thread and the GTV reader thread don't race.
        delay_buf: collections.deque = collections.deque()
        delay_buf_lock = threading.Lock()
        delay = _LIVE_STREAM_DELAY_SEC
        drain_stop = threading.Event()

        def _drain_loop() -> None:
            # Runs on its own thread at a fixed ~50 ms tick so release timing
            # is independent of GTV block arrival jitter.  Before the drain
            # thread existed, draining only happened when a new GTV block
            # arrived — current network irregularities then caused stuttering
            # in playback of old (already-smooth) buffered data.
            while not drain_stop.is_set():
                now = time.monotonic()
                to_release: list = []
                with delay_buf_lock:
                    while delay_buf and delay_buf[0][0] <= now:
                        _, snap = delay_buf.popleft()
                        to_release.append(snap)
                for snap in to_release:
                    self._fanout(snap)
                drain_stop.wait(0.05)

        drain_thread = threading.Thread(target=_drain_loop, daemon=True,
                                        name=f'gtv-drain-{self.port}')
        drain_thread.start()

        try:
            for block in client.iter_blocks():
                if self._stop.is_set():
                    break
                with self._lock:
                    has_subs = bool(self.subscribers)
                    idle_for = time.monotonic() - self._last_active
                if not has_subs and idle_for > _LIVE_IDLE_GRACE_SEC:
                    break
                snap = self.state.process_block(block)
                if snap is not None:
                    if snap.get('type') == 'snapshot':
                        with self._lock:
                            self._last_realtime_snapshot = snap
                    if delay > 0 and snap.get('type') == 'snapshot':
                        with delay_buf_lock:
                            delay_buf.append((time.monotonic() + delay, snap))
                    else:
                        self._fanout(snap)
        except GtvError as exc:
            self._status, self._error = 'error', str(exc)
            self._fanout({'type': 'error', 'error': str(exc)})
        except Exception as exc:                                   # noqa: BLE001
            self._status, self._error = 'error', f'stream: {exc!r}'
            self._fanout({'type': 'error', 'error': f'stream: {exc!r}'})
        finally:
            drain_stop.set()
            drain_thread.join(timeout=2.0)
            self._status = 'closed'
            self._fanout({'type': 'disconnect'})

    # ---- introspection ----------------------------------------------------------

    def info(self) -> dict:
        with self._lock:
            return {
                'host':        self.host,
                'port':        self.port,
                'subscribers': len(self.subscribers),
                'status':      self._status,
                'error':       self._error,
                'started_at':  self._started_at,
                'idle_for':    round(time.monotonic() - self._last_active, 1),
                'has_snapshot': self._last_snapshot is not None,
            }


_live_sessions: dict = {}    # (host, port, password) → _LiveSession
_live_sessions_lock = threading.Lock()


def _get_or_create_live_session(host: str, port: int, password: str) -> _LiveSession:
    key = (host, port, password)
    with _live_sessions_lock:
        sess = _live_sessions.get(key)
        if sess is not None and sess._thread and sess._thread.is_alive():
            return sess
        # Reap dead/closed sessions if we've hit the cap.
        for k, s in list(_live_sessions.items()):
            if not (s._thread and s._thread.is_alive()):
                _live_sessions.pop(k, None)
        if len(_live_sessions) >= _LIVE_MAX_SESSIONS:
            abort(503)
        sess = _LiveSession(host, port, password)
        _live_sessions[key] = sess
        sess.start()
        return sess


def _live_query_args() -> tuple:
    """Validate and extract (host, port, password) from request args."""
    host = (request.args.get('host', '') or '').strip()
    port_s = (request.args.get('port', '') or '').strip()
    password = request.args.get('password', '') or ''
    if not host or not port_s:
        abort(400, 'host and port are required')
    if len(host) > 253 or not re.match(r'^[A-Za-z0-9_.\-]+$', host):
        abort(400, 'invalid host')
    if len(password) > 64:
        abort(400, 'password too long')
    try:
        port = int(port_s)
    except ValueError:
        abort(400, 'invalid port')
    if not (GTV_PORT_MIN <= port <= GTV_PORT_MAX):
        abort(400, f'port must be {GTV_PORT_MIN}-{GTV_PORT_MAX}')
    return host, port, password


def _format_sse(payload: dict) -> bytes:
    """Encode a JSON payload as one SSE 'data:' frame."""
    return b'data: ' + json.dumps(payload, separators=(',', ':')).encode('utf-8') + b'\n\n'


@app.route('/live')
def live_page():
    """HLTV-style live 2D viewer page."""
    preset_ports = list(range(_LIVE_PRESET_PORT_MIN, _LIVE_PRESET_PORT_MAX + 1))
    return render_template(
        'live.html',
        preset_host=_LIVE_PRESET_HOST,
        preset_password=_LIVE_PRESET_PASSWORD,
        preset_ports=preset_ports,
        gtv_port_min=GTV_PORT_MIN,
        gtv_port_max=GTV_PORT_MAX,
    )


@app.route('/live3d')
def live3d_page():
    """Standalone Three.js 3D live viewer (also embedded as iframe in /live)."""
    return render_template('live3d.html')


@app.route('/api/live/stream')
def api_live_stream():
    """Server-sent events stream of LiveGameState snapshots."""
    host, port, password = _live_query_args()
    sess = _get_or_create_live_session(host, port, password)
    sub_q = sess.add_subscriber()

    @stream_with_context
    def gen():
        # Open the stream immediately so the client doesn't sit on an empty body.
        yield b': hello\n\n'
        # Tell the client about the delay so it can show a countdown overlay.
        if _LIVE_STREAM_DELAY_SEC > 0:
            yield _format_sse({'type': 'waiting', 'delay_sec': _LIVE_STREAM_DELAY_SEC})
        last_keepalive = time.monotonic()
        try:
            while True:
                try:
                    snap = sub_q.get(timeout=15.0)
                except _queue.Empty:
                    snap = None
                if snap is not None:
                    yield _format_sse(snap)
                    if isinstance(snap, dict) and snap.get('type') == 'disconnect':
                        return
                else:
                    # SSE comment as keepalive (does not raise client-side onmessage).
                    yield b': keepalive\n\n'
                if time.monotonic() - last_keepalive > 25.0:
                    yield b': keepalive\n\n'
                    last_keepalive = time.monotonic()
        except GeneratorExit:
            pass
        finally:
            sess.remove_subscriber(sub_q)

    headers = {
        'Content-Type':      'text/event-stream',
        'Cache-Control':     'no-cache, no-store, no-transform',
        'X-Accel-Buffering': 'no',                         # disable proxy buffering
        'Connection':        'keep-alive',
    }
    return Response(gen(), headers=headers)


@app.route('/api/live/status')
def api_live_status():
    """Debug endpoint: list active GTV sessions and subscriber counts."""
    with _live_sessions_lock:
        sessions = [s.info() for s in _live_sessions.values()]
    return jsonify({
        'count':    len(sessions),
        'sessions': sessions,
    })


# ── Server status widget ───────────────────────────────────────────────────────

_server_status_cache: dict = {}   # (host, port) → {'data': dict, 'expires': float}
_server_status_lock = threading.Lock()
_server_probe_threads: dict = {}  # port → thread (to avoid double-probing)
_SERVER_STATUS_TTL = 20.0         # seconds before a cached result is considered stale


def _probe_server_status_bg(host: str, port: int, password: str) -> None:
    """Background thread: probe one GTV server, store result in cache.

    Reads _last_realtime_snapshot directly (bypasses the stream delay) so that
    the server status cards on the live page are always up to date.
    """
    result: dict = {'host': host, 'port': port, 'status': 'offline',
                    'map': None, 'team_scores': None, 'round_wins': None,
                    'player_count': 0, 'player_names': []}
    try:
        sess = _get_or_create_live_session(host, port, password)
        # Poll the realtime snapshot (not the delayed queue) for up to 8 s.
        deadline = time.monotonic() + 8.0
        snap = None
        while time.monotonic() < deadline:
            with sess._lock:
                snap = sess._last_realtime_snapshot
            if snap is not None:
                break
            time.sleep(0.15)
        if snap is not None:
            names = snap.get('player_names') or {}
            result.update({
                'status':       'online',
                'map':          snap.get('map'),
                'team_scores':  snap.get('team_scores'),
                'round_wins':   snap.get('round_wins'),
                'player_count': len(names),
                'player_names': list(names.values()),
            })
    except Exception:   # noqa: BLE001
        pass
    with _server_status_lock:
        _server_status_cache[(host, port)] = {
            'data':    result,
            'expires': time.monotonic() + _SERVER_STATUS_TTL,
        }
        _server_probe_threads.pop(port, None)


@app.route('/api/live/servers')
def api_live_servers():
    """Return live status for all configured GTV servers (used by server status widget)."""
    now = time.monotonic()
    results = []
    for port in _LIVE_STATUS_PORTS:
        host = _LIVE_PRESET_HOST
        pw   = _LIVE_PRESET_PASSWORD
        ck   = (host, port)
        label = _LIVE_STATUS_LABELS.get(port, f':{port}')

        with _server_status_lock:
            cached     = _server_status_cache.get(ck)
            is_probing = port in _server_probe_threads

        if cached:
            entry = dict(cached['data'])
            entry['label'] = label
            results.append(entry)
            # Kick off background refresh if stale
            if cached['expires'] < now and not is_probing:
                t = threading.Thread(target=_probe_server_status_bg,
                                     args=(host, port, pw), daemon=True,
                                     name=f'srv-probe-{port}')
                with _server_status_lock:
                    _server_probe_threads[port] = t
                t.start()
        else:
            results.append({'host': host, 'port': port, 'label': label, 'status': 'probing'})
            if not is_probing:
                t = threading.Thread(target=_probe_server_status_bg,
                                     args=(host, port, pw), daemon=True,
                                     name=f'srv-probe-{port}')
                with _server_status_lock:
                    _server_probe_threads[port] = t
                t.start()

    return jsonify(results)


# ── Dev server ─────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(debug=debug, host='0.0.0.0', port=5000)
