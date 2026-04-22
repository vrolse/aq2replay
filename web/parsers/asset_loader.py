"""
S3/HTTP asset loader for AQ2 replay viewer.

Fetches BSP map files and textures directly from the MinIO S3 bucket at
dlserver.aq2world.com. No disk caching of raw assets — only the rendered
topview PNGs are cached (handled by app.py / topview.py).

Usage::

    from parsers.asset_loader import fetch_bsp_bytes, fetch_texture_bytes

    data        = fetch_bsp_bytes('urban')
    data, ext   = fetch_texture_bytes('e1u1/bwall1')
"""
import urllib.request
import urllib.error
import logging

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

S3_BASE_URL   = 'http://dlserver.aq2world.com/action/action'
BSP_URL_TMPL  = S3_BASE_URL + '/maps/{name}.bsp'
TEX_URL_BASE  = S3_BASE_URL + '/textures'

# Extension priority when probing for textures on S3
_TEX_EXTS = ('.wal', '.png', '.jpg', '.jpeg')

_FETCH_TIMEOUT = 30    # seconds — per HTTP request
_RETRY_LIMIT   = 1     # one retry on transient failures


def _http_get(url: str) -> bytes | None:
    """Fetch *url* with one retry. Returns bytes on HTTP 200, None otherwise."""
    for attempt in range(_RETRY_LIMIT + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'aq2replay/1.0'})
            with urllib.request.urlopen(req, timeout=_FETCH_TIMEOUT) as resp:
                if resp.status != 200:
                    logger.warning('asset_loader: HTTP %d for %s', resp.status, url)
                    return None
                return resp.read()
        except urllib.error.HTTPError as e:
            logger.info('asset_loader: HTTP %d fetching %s', e.code, url)
            return None
        except (urllib.error.URLError, OSError) as e:
            if attempt < _RETRY_LIMIT:
                logger.info('asset_loader: transient error for %s (%s), retrying', url, e)
            else:
                logger.warning('asset_loader: failed to fetch %s: %s', url, e)
                return None
    return None


def fetch_bsp_bytes(name: str) -> bytes | None:
    """
    Fetch raw BSP bytes for *name* (map name without extension) from S3.
    Returns bytes on success, None on failure.
    """
    if not name or not all(c.isalnum() or c in '_-.' for c in name):
        logger.warning('asset_loader: invalid map name %r', name)
        return None

    url  = BSP_URL_TMPL.format(name=name)
    data = _http_get(url)
    if data is None:
        return None

    if len(data) < 8 or data[:4] != b'IBSP':
        logger.warning('asset_loader: unexpected BSP magic from %s', url)
        return None

    logger.info('asset_loader: fetched %s (%d bytes)', name, len(data))
    return data


def fetch_texture_bytes(tex_name: str) -> tuple:
    """
    Fetch raw texture bytes for a Q2 texture name (e.g. ``'e1u1/bwall1'``).

    Texture names come directly from the BSP texinfo lump — relative paths
    without the ``textures/`` prefix and without any file extension.

    Tries extensions in order: .wal, .png, .jpg, .jpeg.

    Returns ``(data: bytes, ext: str)`` on success, ``(None, None)`` on failure.
    """
    if not tex_name or '..' in tex_name or tex_name.startswith('/'):
        return None, None
    if not all(c.isalnum() or c in '_-./\\' for c in tex_name):
        return None, None
    tex_name = tex_name.replace('\\', '/')

    for ext in _TEX_EXTS:
        url  = f'{TEX_URL_BASE}/{tex_name}{ext}'
        data = _http_get(url)
        if data:
            return data, ext

    return None, None


# Allowed top-level directories for the generic asset proxy
_ASSET_DIRS = frozenset({'players', 'models', 'sprites', 'pics', 'sound'})


def fetch_asset_bytes(asset_path: str) -> bytes | None:
    """
    Fetch a generic game asset from S3 by its in-game relative path
    (e.g. ``'players/keel/keel.md2'`` or ``'models/weapons/v_m4a1/tris.md2'``).

    Only paths whose first component is in ``_ASSET_DIRS`` are allowed.
    Returns bytes on success, None on failure.
    """
    if not asset_path or '..' in asset_path:
        return None
    asset_path = asset_path.replace('\\', '/').lstrip('/')
    top = asset_path.split('/')[0].lower()
    if top not in _ASSET_DIRS:
        return None
    url = f'{S3_BASE_URL}/{asset_path}'
    return _http_get(url)
