"""
Persistent SQLite stats database for cross-match aggregates.

The indexer scans the MVD2 directory in a background daemon thread, parses
any demo files not yet in the DB (or whose mtime changed), and writes one
row per real player per match.  All query functions return plain dicts/lists
and are safe to call from Flask request handlers concurrently (WAL mode).

Public API
----------
init_db(mvd2_dir, db_path)   — create schema + wire directories
trigger_reindex()             — start background indexer if not already running
get_leaderboard(min_games, limit, mode) — sorted player ranking list
get_map_stats(mode)           — map popularity list
get_summary(mode)             — headline totals dict
get_index_status()            — {total, indexed, running}
"""

import logging
import math
import os
import re
import sqlite3
import threading
import time as _time
import json
import urllib.error as _urllib_error
import urllib.parse as _urllib_parse
import urllib.request as _urllib_request
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger(__name__)

# Match YYYY-MM-DD_HHMMSS or YYYY-MM-DD_HHMM embedded in a filename
_DATE_RE = re.compile(r'(\d{4}-\d{2}-\d{2})[_T](\d{4,6})')
_WEEK_FILTER_RE = re.compile(r'^(\d{4})[-_/]?w?(\d{1,2})$', re.IGNORECASE)
_SLOT_IDENTITY_RE = re.compile(r'^.+ #\d+$')
_SLOT_SUFFIX_RE = re.compile(r' #\d+$')

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS matches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT    UNIQUE NOT NULL,
    map         TEXT    NOT NULL DEFAULT '',
    played_at   INTEGER NOT NULL DEFAULT 0,
    duration    REAL    NOT NULL DEFAULT 0,
    total_kills INTEGER NOT NULL DEFAULT 0,
    t1_rounds   INTEGER NOT NULL DEFAULT 0,
    t2_rounds   INTEGER NOT NULL DEFAULT 0,
    file_mtime  REAL    NOT NULL DEFAULT 0,
    -- Canonical bucket: Teamplay/TeamDM/Domination/Espionage/Tourney -> 'tdm'.
    game_mode   TEXT    NOT NULL DEFAULT 'tdm',
    -- Specific mode label preserved for filtering (teamplay, team_deathmatch, ...).
    game_mode_detail TEXT NOT NULL DEFAULT 'teamplay'
);

CREATE TABLE IF NOT EXISTS player_match_stats (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id    INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    name        TEXT    NOT NULL,
    team        INTEGER NOT NULL DEFAULT 0,
    kills       INTEGER NOT NULL DEFAULT 0,
    deaths      INTEGER NOT NULL DEFAULT 0,
    team_kills  INTEGER NOT NULL DEFAULT 0,
    hits        INTEGER NOT NULL DEFAULT 0,
    shots       INTEGER NOT NULL DEFAULT 0,
    damage      INTEGER NOT NULL DEFAULT 0,
    accuracy    REAL,
    hs_kills    INTEGER NOT NULL DEFAULT 0,
    hits_head   INTEGER NOT NULL DEFAULT 0,
    hits_stomach INTEGER NOT NULL DEFAULT 0,
    hits_chest  INTEGER NOT NULL DEFAULT 0,
    hits_legs   INTEGER NOT NULL DEFAULT 0,
    awards_impressive INTEGER NOT NULL DEFAULT 0,
    awards_accuracy   INTEGER NOT NULL DEFAULT 0,
    awards_excellent  INTEGER NOT NULL DEFAULT 0,
    best_kill_streak  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS kill_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id   INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    killer     TEXT    NOT NULL,
    victim     TEXT    NOT NULL,
    weapon     TEXT    NOT NULL DEFAULT 'unknown',
    location   TEXT    NOT NULL DEFAULT 'unknown',
    team_kill  INTEGER NOT NULL DEFAULT 0,
    frame_idx  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rounds (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id           INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    round_index        INTEGER NOT NULL,
    start_frame        INTEGER NOT NULL DEFAULT 0,
    end_frame          INTEGER NOT NULL DEFAULT 0,
    duration_frames    INTEGER NOT NULL DEFAULT 0,
    duration_seconds   REAL    NOT NULL DEFAULT 0,
    winner_team        INTEGER NOT NULL DEFAULT 0,
    is_tie             INTEGER NOT NULL DEFAULT 0,
    win_condition      TEXT    NOT NULL DEFAULT 'unknown',
    first_kill_frame   INTEGER,
    first_kill_player  TEXT,
    first_kill_team    INTEGER,
    first_kill_weapon  TEXT,
    first_kill_location TEXT,
    first_death_player TEXT,
    first_death_team   INTEGER,
    UNIQUE(match_id, round_index)
);

CREATE TABLE IF NOT EXISTS match_captains (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id     INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    team         INTEGER NOT NULL DEFAULT 0,
    captain_name TEXT    NOT NULL,
    sightings    INTEGER NOT NULL DEFAULT 0,
    switches     INTEGER NOT NULL DEFAULT 0,
    first_frame  INTEGER NOT NULL DEFAULT 0,
    last_frame   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(match_id, team)
);

CREATE TABLE IF NOT EXISTS round_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id     INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    round_id     INTEGER REFERENCES rounds(id) ON DELETE CASCADE,
    frame_idx    INTEGER NOT NULL DEFAULT 0,
    event_type   TEXT    NOT NULL DEFAULT '',
    event_payload TEXT   NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS position_samples (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id   INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    round_id   INTEGER REFERENCES rounds(id) ON DELETE CASCADE,
    frame_idx  INTEGER NOT NULL DEFAULT 0,
    name       TEXT    NOT NULL,
    team       INTEGER NOT NULL DEFAULT 0,
    x          REAL    NOT NULL DEFAULT 0,
    y          REAL    NOT NULL DEFAULT 0,
    z          REAL    NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS kill_positions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id   INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    round_id   INTEGER REFERENCES rounds(id) ON DELETE CASCADE,
    frame_idx  INTEGER NOT NULL DEFAULT 0,
    killer     TEXT    NOT NULL,
    victim     TEXT    NOT NULL,
    weapon     TEXT    NOT NULL DEFAULT 'unknown',
    location   TEXT    NOT NULL DEFAULT 'unknown',
    team_kill  INTEGER NOT NULL DEFAULT 0,
    killer_x   REAL,
    killer_y   REAL,
    killer_z   REAL,
    victim_x   REAL,
    victim_y   REAL,
    victim_z   REAL
);

CREATE TABLE IF NOT EXISTS parse_errors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT    NOT NULL,
    file_mtime  REAL    NOT NULL DEFAULT 0,
    stage       TEXT    NOT NULL DEFAULT 'parse',
    error_text  TEXT    NOT NULL,
    occurred_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS parse_file_status (
    path          TEXT PRIMARY KEY,
    file_mtime    REAL    NOT NULL DEFAULT 0,
    last_status   TEXT    NOT NULL DEFAULT 'unknown',
    last_error    TEXT,
    updated_at    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS player_round_stats (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id          INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    round_id          INTEGER NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
    name              TEXT    NOT NULL,
    team              INTEGER NOT NULL DEFAULT 0,
    kills             INTEGER NOT NULL DEFAULT 0,
    deaths            INTEGER NOT NULL DEFAULT 0,
    assists_verified  INTEGER NOT NULL DEFAULT 0,
    assists_estimated INTEGER NOT NULL DEFAULT 0,
    damage            INTEGER NOT NULL DEFAULT 0,
    first_kill        INTEGER NOT NULL DEFAULT 0,
    first_death       INTEGER NOT NULL DEFAULT 0,
    survived          INTEGER NOT NULL DEFAULT 0,
    UNIQUE(round_id, name)
);

CREATE TABLE IF NOT EXISTS weapon_stats_agg (
    weapon      TEXT PRIMARY KEY,
    kills       INTEGER NOT NULL DEFAULT 0,
    hs          INTEGER NOT NULL DEFAULT 0,
    hs_pct      REAL    NOT NULL DEFAULT 0,
    top_killer  TEXT
);

CREATE TABLE IF NOT EXISTS leaderboard_agg (
    name               TEXT PRIMARY KEY,
    games              INTEGER NOT NULL DEFAULT 0,
    kills              INTEGER NOT NULL DEFAULT 0,
    deaths             INTEGER NOT NULL DEFAULT 0,
    team_kills         INTEGER NOT NULL DEFAULT 0,
    hits               INTEGER NOT NULL DEFAULT 0,
    shots              INTEGER NOT NULL DEFAULT 0,
    damage             INTEGER NOT NULL DEFAULT 0,
    hs_kills           INTEGER NOT NULL DEFAULT 0,
    best_game          INTEGER NOT NULL DEFAULT 0,
    kd                 REAL    NOT NULL DEFAULT 0,
    avg_kills          REAL    NOT NULL DEFAULT 0,
    avg_acc            REAL,
    h_head             INTEGER NOT NULL DEFAULT 0,
    h_stomach          INTEGER NOT NULL DEFAULT 0,
    h_chest            INTEGER NOT NULL DEFAULT 0,
    h_legs             INTEGER NOT NULL DEFAULT 0,
    awards_impressive  INTEGER NOT NULL DEFAULT 0,
    awards_accuracy    INTEGER NOT NULL DEFAULT 0,
    awards_excellent   INTEGER NOT NULL DEFAULT 0,
    rounds_played      INTEGER NOT NULL DEFAULT 0,
    damage_per_round   REAL    NOT NULL DEFAULT 0,
    impact_score       REAL    NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS map_stats_agg (
    map               TEXT PRIMARY KEY,
    matches           INTEGER NOT NULL DEFAULT 0,
    kills             INTEGER NOT NULL DEFAULT 0,
    avg_kills         REAL    NOT NULL DEFAULT 0,
    players           INTEGER NOT NULL DEFAULT 0,
    avg_duration_min  REAL    NOT NULL DEFAULT 0,
    top_killer        TEXT
);

CREATE TABLE IF NOT EXISTS summary_agg (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    total_matches    INTEGER NOT NULL DEFAULT 0,
    total_players    INTEGER NOT NULL DEFAULT 0,
    total_kills      INTEGER NOT NULL DEFAULT 0,
    total_damage     INTEGER NOT NULL DEFAULT 0,
    best_game_kills  INTEGER NOT NULL DEFAULT 0,
    total_maps       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS records_agg (
    key      TEXT PRIMARY KEY,
    payload  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS activity_week_agg (
    week     TEXT PRIMARY KEY,
    matches  INTEGER NOT NULL DEFAULT 0,
    kills    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS recent_matches_agg (
    path         TEXT PRIMARY KEY,
    map          TEXT    NOT NULL DEFAULT '',
    played_at    INTEGER NOT NULL DEFAULT 0,
    duration     REAL    NOT NULL DEFAULT 0,
    total_kills  INTEGER NOT NULL DEFAULT 0,
    t1_rounds    INTEGER NOT NULL DEFAULT 0,
    t2_rounds    INTEGER NOT NULL DEFAULT 0,
    game_mode    TEXT    NOT NULL DEFAULT 'tdm',
    game_mode_detail TEXT NOT NULL DEFAULT 'teamplay',
    player_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS map_win_rates_agg (
    map      TEXT PRIMARY KEY,
    matches  INTEGER NOT NULL DEFAULT 0,
    t1_wins  INTEGER NOT NULL DEFAULT 0,
    t2_wins  INTEGER NOT NULL DEFAULT 0,
    draws    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pms_name     ON player_match_stats(name);
CREATE INDEX IF NOT EXISTS idx_pms_match    ON player_match_stats(match_id);
CREATE INDEX IF NOT EXISTS idx_pms_match_name_team ON player_match_stats(match_id, name, team);
CREATE INDEX IF NOT EXISTS idx_matches_map  ON matches(map);
CREATE INDEX IF NOT EXISTS idx_matches_time ON matches(played_at);
CREATE INDEX IF NOT EXISTS idx_matches_mode_detail ON matches(game_mode_detail);
CREATE INDEX IF NOT EXISTS idx_ke_killer    ON kill_events(killer);
CREATE INDEX IF NOT EXISTS idx_ke_victim    ON kill_events(victim);
CREATE INDEX IF NOT EXISTS idx_ke_weapon    ON kill_events(weapon);
CREATE INDEX IF NOT EXISTS idx_ke_match     ON kill_events(match_id);
CREATE INDEX IF NOT EXISTS idx_ke_victim_tk    ON kill_events(victim, team_kill);
CREATE INDEX IF NOT EXISTS idx_ke_killer_tk    ON kill_events(killer, team_kill);
CREATE INDEX IF NOT EXISTS idx_ke_weapon_cover ON kill_events(team_kill, weapon, killer, location);
CREATE INDEX IF NOT EXISTS idx_ke_weapon_killer_match ON kill_events(team_kill, weapon, killer, match_id, location);
CREATE INDEX IF NOT EXISTS idx_ke_weapon_victim_match ON kill_events(team_kill, weapon, victim, match_id, location);
CREATE INDEX IF NOT EXISTS idx_rounds_match     ON rounds(match_id);
CREATE INDEX IF NOT EXISTS idx_rounds_winner    ON rounds(winner_team);
CREATE INDEX IF NOT EXISTS idx_match_captains_match ON match_captains(match_id);
CREATE INDEX IF NOT EXISTS idx_match_captains_team  ON match_captains(team);
CREATE INDEX IF NOT EXISTS idx_match_captains_name  ON match_captains(captain_name);
CREATE INDEX IF NOT EXISTS idx_round_events_match_frame ON round_events(match_id, frame_idx);
CREATE INDEX IF NOT EXISTS idx_round_events_round       ON round_events(round_id);
CREATE INDEX IF NOT EXISTS idx_pos_samples_match_frame  ON position_samples(match_id, frame_idx);
CREATE INDEX IF NOT EXISTS idx_pos_samples_round        ON position_samples(round_id);
CREATE INDEX IF NOT EXISTS idx_pos_samples_name         ON position_samples(name);
CREATE INDEX IF NOT EXISTS idx_kill_pos_match_frame     ON kill_positions(match_id, frame_idx);
CREATE INDEX IF NOT EXISTS idx_kill_pos_round           ON kill_positions(round_id);
CREATE INDEX IF NOT EXISTS idx_kill_pos_killer          ON kill_positions(killer);
CREATE INDEX IF NOT EXISTS idx_prs_match        ON player_round_stats(match_id);
CREATE INDEX IF NOT EXISTS idx_prs_name         ON player_round_stats(name);
CREATE INDEX IF NOT EXISTS idx_prs_round        ON player_round_stats(round_id);
CREATE INDEX IF NOT EXISTS idx_parse_errors_path_time   ON parse_errors(path, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_parse_status_state       ON parse_file_status(last_status);
CREATE INDEX IF NOT EXISTS idx_lb_kills        ON leaderboard_agg(kills DESC);
CREATE INDEX IF NOT EXISTS idx_lb_games        ON leaderboard_agg(games);
CREATE INDEX IF NOT EXISTS idx_mapagg_matches  ON map_stats_agg(matches DESC);
CREATE INDEX IF NOT EXISTS idx_recent_played_at ON recent_matches_agg(played_at DESC);
CREATE INDEX IF NOT EXISTS idx_mapwin_matches   ON map_win_rates_agg(matches DESC);
"""

_db_path:  Optional[str] = None
_mvd2_dir: Optional[str] = None
_lock    = threading.Lock()
_running = False
_thread_local = threading.local()
_repair_lock = threading.Lock()
_CORRUPTION_PATTERNS = (
    'database disk image is malformed',
    'file is not a database',
    'malformed database schema',
    'database corrupt',
)

# ── In-memory result cache ─────────────────────────────────────────────────────
# Avoids re-running heavy aggregation queries on every page load.
# Invalidated at the end of each indexer run; also expires after _CACHE_TTL seconds.
_CACHE_TTL   = 300.0  # seconds
_result_cache: dict = {}
_cache_lock  = threading.Lock()
_INITIAL_WAL_SOFT_LIMIT_BYTES = 512 * 1024 * 1024
_INITIAL_WAL_CHECK_EVERY_BATCHES = 4
_DEFAULT_STATS_MODE = 'teamplay'
_MODE_DETAIL_TO_GROUP = {
    'teamplay': 'tdm',
    'team_deathmatch': 'tdm',
    'domination': 'tdm',
    'espionage': 'tdm',
    'tourney': 'tdm',
    'deathmatch': 'dm',
    'jumpmod': 'dm',
    'ctf': 'ctf',
}
_MODE_GROUP_DEFAULT_DETAIL = {
    'tdm': 'teamplay',
    'dm': 'deathmatch',
    'ctf': 'ctf',
}
_MODE_FILTER_ALIASES = {
    'tdm': 'team_deathmatch',
    'dm': 'deathmatch',
    'tp': 'teamplay',
    'team_play': 'teamplay',
    'team': 'teamplay',
    'teamdeathmatch': 'team_deathmatch',
    'team_dm': 'team_deathmatch',
    'team_death_match': 'team_deathmatch',
    'death_match': 'deathmatch',
    'ffa': 'deathmatch',
    'dom': 'domination',
    'esp': 'espionage',
    'tourny': 'tourney',
    'jm': 'jumpmod',
    'all_team_modes': 'team_modes',
    'all_dm_modes': 'dm_modes',
}
_DEFAULT_H2H_PERIOD = 'this_year'
_H2H_PERIOD_ALIASES = {
    'all': 'all',
    'all_time': 'all',
    'alltime': 'all',
    'week': 'this_week',
    'thisweek': 'this_week',
    'this_week': 'this_week',
    'lastweek': 'last_week',
    'last_week': 'last_week',
    'month': 'this_month',
    'thismonth': 'this_month',
    'this_month': 'this_month',
    'lastmonth': 'last_month',
    'last_month': 'last_month',
    'year': 'this_year',
    'thisyear': 'this_year',
    'this_year': 'this_year',
    'lastyear': 'last_year',
    'last_year': 'last_year',
    'previous_year': 'last_year',
}
_H2H_PERIOD_VALUES = {
    'all',
    'this_week',
    'last_week',
    'this_month',
    'last_month',
    'this_year',
    'last_year',
}


def _env_int(name: str, default: int, min_value: int, max_value: int) -> int:
    raw = (os.environ.get(name, '') or '').strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(min_value, min(max_value, value))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name, '') or '').strip().lower()
    if not raw:
        return default
    return raw in ('1', 'true', 'yes', 'on')


_SQLITE_READ_CACHE_KB = _env_int('STATS_SQLITE_READ_CACHE_KB', default=32000, min_value=4096, max_value=1048576)
_SQLITE_WRITE_CACHE_KB = _env_int('STATS_SQLITE_WRITE_CACHE_KB', default=16000, min_value=4096, max_value=1048576)
_SQLITE_MMAP_MB = _env_int('STATS_SQLITE_MMAP_MB', default=0, min_value=0, max_value=8192)
_SQLITE_MMAP_BYTES = int(_SQLITE_MMAP_MB) * 1024 * 1024
_SQLITE_BUSY_TIMEOUT_MS = _env_int('STATS_SQLITE_BUSY_TIMEOUT_MS', default=60000, min_value=1000, max_value=300000)
_SQLITE_TEMP_STORE = (os.environ.get('STATS_SQLITE_TEMP_STORE', 'MEMORY') or 'MEMORY').strip().upper()
if _SQLITE_TEMP_STORE not in ('DEFAULT', 'FILE', 'MEMORY'):
    _SQLITE_TEMP_STORE = 'MEMORY'


_PREWARM_SCOPES = {'off', 'standard', 'extended', 'full'}
_PREWARM_SCOPE = (os.environ.get('STATS_PREWARM_SCOPE', 'standard') or 'standard').strip().lower()
if _PREWARM_SCOPE not in _PREWARM_SCOPES:
    _PREWARM_SCOPE = 'standard'

_PREWARM_TOP_WEAPONS = _env_int('STATS_PREWARM_TOP_WEAPONS', default=16, min_value=1, max_value=200)
_PREWARM_TOP_PLAYERS = _env_int('STATS_PREWARM_TOP_PLAYERS', default=16, min_value=1, max_value=200)
_PREWARM_TOP_MAPS = _env_int('STATS_PREWARM_TOP_MAPS', default=12, min_value=1, max_value=200)
_PREWARM_TOP_H2H = _env_int('STATS_PREWARM_TOP_H2H', default=10, min_value=1, max_value=100)
_PREWARM_INCLUDE_GEMINI = _env_bool('STATS_PREWARM_INCLUDE_GEMINI', default=False)

_PREWARM_MODES_EXTENDED = (
    'teamplay',
    'team_modes',
    'dm_modes',
    'all',
)
_PREWARM_PERIODS_EXTENDED = (
    'this_year',
    'all',
)
_PREWARM_MODES_FULL = (
    'teamplay',
    'team_deathmatch',
    'domination',
    'espionage',
    'tourney',
    'deathmatch',
    'jumpmod',
    'ctf',
    'team_modes',
    'dm_modes',
    'all',
)
_PREWARM_PERIODS_FULL = (
    'this_week',
    'last_week',
    'this_month',
    'last_month',
    'this_year',
    'last_year',
    'all',
)

_prewarm_lock = threading.Lock()
_prewarm_running = False


def _apply_sqlite_session_pragmas(conn: sqlite3.Connection, writer: bool) -> None:
    """Apply per-connection SQLite tuning PRAGMAs (env-configurable)."""
    conn.execute('PRAGMA foreign_keys=ON')
    cache_kb = _SQLITE_WRITE_CACHE_KB if writer else _SQLITE_READ_CACHE_KB
    conn.execute(f'PRAGMA cache_size={-int(cache_kb)}')
    conn.execute(f'PRAGMA busy_timeout={int(_SQLITE_BUSY_TIMEOUT_MS)}')
    conn.execute(f'PRAGMA temp_store={_SQLITE_TEMP_STORE}')
    if _SQLITE_MMAP_BYTES > 0:
        try:
            conn.execute(f'PRAGMA mmap_size={_SQLITE_MMAP_BYTES}')
        except sqlite3.DatabaseError:
            # Some filesystems/builds may ignore or reject mmap tuning.
            pass
    if writer:
        conn.execute('PRAGMA synchronous=NORMAL')

def _cache_get(key: str):
    with _cache_lock:
        entry = _result_cache.get(key)
    if entry and (_time.monotonic() - entry[1]) < entry[2]:
        return entry[0]
    return None

def _cache_set(key: str, value, ttl: float = _CACHE_TTL) -> None:
    with _cache_lock:
        _result_cache[key] = (value, _time.monotonic(), ttl)

def _cache_clear() -> None:
    with _cache_lock:
        _result_cache.clear()


def _canonical_mode_detail(detail: Optional[str], fallback_group: str = 'tdm') -> str:
    """Normalize one parsed mode value into a supported detail key."""
    raw = (detail or '').strip().lower().replace('-', '_').replace(' ', '_')
    raw = _MODE_FILTER_ALIASES.get(raw, raw)
    if raw in _MODE_DETAIL_TO_GROUP:
        return raw
    return _MODE_GROUP_DEFAULT_DETAIL.get(
        (fallback_group or 'tdm').strip().lower(),
        _DEFAULT_STATS_MODE,
    )


def _normalize_mode_filter(mode: Optional[str]) -> str:
    """Normalize API mode filters; defaults to teamplay when unset/invalid."""
    raw = (mode or '').strip().lower().replace('-', '_').replace(' ', '_')
    if not raw:
        return _DEFAULT_STATS_MODE
    raw = _MODE_FILTER_ALIASES.get(raw, raw)
    if raw == 'all':
        return 'all'
    if raw in ('team_modes', 'dm_modes'):
        return raw
    if raw in _MODE_DETAIL_TO_GROUP:
        return raw
    return _DEFAULT_STATS_MODE


def _mode_filter_clause(mode: Optional[str],
                        detail_col: str = 'm.game_mode_detail',
                        bucket_col: str = 'm.game_mode') -> tuple[str, tuple]:
    """Return SQL WHERE snippet + params for requested mode filter."""
    normalized = _normalize_mode_filter(mode)
    if normalized == 'all':
        return '1=1', ()
    if normalized == 'team_modes':
        return f'{bucket_col} = ?', ('tdm',)
    if normalized == 'dm_modes':
        return f'{bucket_col} = ?', ('dm',)
    return f'{detail_col} = ?', (normalized,)


def _normalize_h2h_period(period: Optional[str]) -> str:
    """Normalize H2H time-range filters; defaults to this_year."""
    raw = (period or '').strip().lower().replace('-', '_').replace(' ', '_')
    if not raw:
        return _DEFAULT_H2H_PERIOD
    normalized = _H2H_PERIOD_ALIASES.get(raw, raw)
    if normalized in _H2H_PERIOD_VALUES:
        return normalized
    return _DEFAULT_H2H_PERIOD


def _h2h_period_filter_clause(period: Optional[str],
                              played_at_col: str = 'm.played_at') -> tuple[str, tuple, str]:
    """Return SQL filter clause + params + normalized period for H2H queries."""
    normalized = _normalize_h2h_period(period)
    if normalized == 'all':
        return '1=1', (), normalized

    now = datetime.now().astimezone()

    if normalized == 'this_year':
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(year=start.year + 1)
    elif normalized == 'last_year':
        end = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        start = end.replace(year=end.year - 1)
    elif normalized == 'this_month':
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
    elif normalized == 'last_month':
        end = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if end.month == 1:
            start = end.replace(year=end.year - 1, month=12)
        else:
            start = end.replace(month=end.month - 1)
    elif normalized == 'this_week':
        start = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end = start + timedelta(days=7)
    elif normalized == 'last_week':
        end = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start = end - timedelta(days=7)
    else:
        # Fallback safety (should be unreachable due to normalization).
        normalized = _DEFAULT_H2H_PERIOD
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(year=start.year + 1)

    return f'({played_at_col} >= ? AND {played_at_col} < ?)', (int(start.timestamp()), int(end.timestamp())), normalized


def _normalize_week_filter(week: Optional[str]) -> Optional[str]:
    """Normalize optional week filters to YYYY-WW or return None when invalid."""
    raw = (week or '').strip()
    if not raw:
        return None
    m = _WEEK_FILTER_RE.match(raw)
    if not m:
        return None
    year = int(m.group(1))
    week_num = int(m.group(2))
    if week_num < 0 or week_num > 53:
        return None
    return f'{year:04d}-{week_num:02d}'


def _week_filter_clause(week: Optional[str],
                        played_at_col: str = 'm.played_at') -> tuple[str, tuple, Optional[str]]:
    """Return SQL filter clause + params + normalized week for week-specific queries."""
    normalized = _normalize_week_filter(week)
    if not normalized:
        return '1=1', (), None
    return f"strftime('%Y-%W', datetime({played_at_col}, 'unixepoch')) = ?", (normalized,), normalized


def _sql_like_escape(value: str) -> str:
    return (value or '').replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')


def _resolve_identity_variants(conn: sqlite3.Connection, name: str) -> list:
    """Resolve a query name to DB identity variants.

    - Explicit slot identity labels ("name #3") are treated as exact.
    - Base names match exact + slot identities with suffix " #<n>".
    """
    nm = (name or '').strip()
    if not nm:
        return []
    if _SLOT_IDENTITY_RE.match(nm):
        return [nm]

    like_pat = _sql_like_escape(nm) + ' #%'
    rows = conn.execute('''
        SELECT name AS n
        FROM player_match_stats
        WHERE name = ? OR name LIKE ? ESCAPE '\\'
        UNION
        SELECT killer AS n
        FROM kill_events
        WHERE killer = ? OR killer LIKE ? ESCAPE '\\'
        UNION
        SELECT victim AS n
        FROM kill_events
        WHERE victim = ? OR victim LIKE ? ESCAPE '\\'
    ''', (nm, like_pat, nm, like_pat, nm, like_pat)).fetchall()
    vals = sorted({r['n'] for r in rows if r['n']})
    return vals or [nm]


def _in_clause(column: str, values: list) -> tuple[str, tuple]:
    if not values:
        return '1=0', ()
    ph = ','.join('?' for _ in values)
    return f'{column} IN ({ph})', tuple(values)


def _close_conn(conn: Optional[sqlite3.Connection]) -> None:
    """Close a connection and clear thread-local reference if it points to it."""
    if conn is None:
        return
    if getattr(_thread_local, 'read_conn', None) is conn:
        _thread_local.read_conn = None
    try:
        conn.close()
    except Exception:
        pass


def _safe_rollback(conn: Optional[sqlite3.Connection]) -> None:
    if conn is None:
        return
    try:
        conn.rollback()
    except Exception:
        pass


def _is_locked_error(exc: Exception) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and 'locked' in str(exc).lower()


def _is_corruption_error(exc: Exception) -> bool:
    if not isinstance(exc, sqlite3.DatabaseError):
        return False
    msg = str(exc or '').lower()
    return any(pat in msg for pat in _CORRUPTION_PATTERNS)


def _recover_malformed_db(reason: str = '') -> bool:
    """Quarantine a corrupted DB file, recreate schema, and clear caches."""
    if not _db_path:
        return False

    with _repair_lock:
        db_path = os.path.abspath(_db_path)
        stamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        quarantine_prefix = f'{db_path}.corrupt.{stamp}'

        # Best-effort close of current thread-local read handle.
        _close_conn(getattr(_thread_local, 'read_conn', None))

        moved_main = False
        if os.path.exists(db_path):
            try:
                os.replace(db_path, quarantine_prefix)
                moved_main = True
            except OSError as exc:
                log.error('stats: failed to quarantine corrupted db %s: %s', db_path, exc)

        # Move sidecars if present so recreated DB starts clean.
        for suffix in ('-wal', '-shm'):
            src = db_path + suffix
            if not os.path.exists(src):
                continue
            dst = (quarantine_prefix if moved_main else f'{db_path}.corrupt.{stamp}') + suffix
            try:
                os.replace(src, dst)
            except OSError:
                pass

        os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)
        conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA journal_mode=WAL')
            conn.executescript(_SCHEMA)
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        _cache_clear()
        log.error(
            'stats: recovered malformed sqlite db (%s); quarantined files under %s',
            reason or 'unknown reason',
            quarantine_prefix,
        )
        return True


def is_db_corruption_error(exc: Exception) -> bool:
    """Public helper for app-level error handlers."""
    return _is_corruption_error(exc)


def recover_corrupted_db(reason: str = '') -> bool:
    """Public helper to recover from corruption and trigger a background rebuild."""
    rebuilt = _recover_malformed_db(reason)
    if rebuilt:
        try:
            trigger_reindex()
        except Exception as exc:
            log.warning('stats: failed to trigger reindex after corruption recovery: %s', exc)
    return rebuilt


def _maybe_passive_checkpoint(conn: sqlite3.Connection, min_wal_bytes: int = 0) -> None:
    """Run a non-blocking WAL checkpoint when WAL grows beyond threshold."""
    if not _db_path:
        return
    wal_path = _db_path + '-wal'
    try:
        wal_size = os.path.getsize(wal_path)
    except OSError:
        return
    if wal_size < min_wal_bytes:
        return
    try:
        row = conn.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
        if row:
            busy, log_frames, ckpt_frames = int(row[0]), int(row[1]), int(row[2])
            log.info(
                'stats: wal checkpoint passive (wal=%.1fMB busy=%d frames=%d ckpt=%d)',
                wal_size / (1024 * 1024), busy, log_frames, ckpt_frames
            )
        else:
            log.info('stats: wal checkpoint passive (wal=%.1fMB)', wal_size / (1024 * 1024))
    except Exception as exc:
        log.debug('stats: wal checkpoint passive skipped: %s', exc)


def _open_sqlite_connection(path: str, timeout: int = 30) -> sqlite3.Connection:
    """Open SQLite connection with path-recovery for containerized deployments.

    Some deployments can temporarily lose access to a bind-mounted DB path
    (e.g. permissions/volume issues), which surfaces as
    'unable to open database file'. We first ensure the parent exists and
    retry once, then optionally fall back to STATS_DB_FALLBACK_PATH.
    """
    try:
        return sqlite3.connect(path, timeout=timeout, check_same_thread=False)
    except sqlite3.OperationalError as exc:
        if 'unable to open database file' not in str(exc).lower():
            raise

    parent = os.path.dirname(path) or '.'
    try:
        os.makedirs(parent, exist_ok=True)
    except OSError:
        pass

    try:
        return sqlite3.connect(path, timeout=timeout, check_same_thread=False)
    except sqlite3.OperationalError as retry_exc:
        if 'unable to open database file' not in str(retry_exc).lower():
            raise

        fallback = (os.environ.get('STATS_DB_FALLBACK_PATH', '/tmp/aq2stats/stats.db') or '').strip()
        if not fallback:
            raise
        if not os.path.isabs(fallback):
            fallback = os.path.abspath(fallback)
        if os.path.abspath(fallback) == os.path.abspath(path):
            raise

        fb_parent = os.path.dirname(fallback) or '.'
        os.makedirs(fb_parent, exist_ok=True)
        conn = sqlite3.connect(fallback, timeout=timeout, check_same_thread=False)

        global _db_path
        prev = _db_path
        _db_path = fallback
        log.error('stats: unable to open database file at %s; using fallback %s', prev, fallback)
        return conn


# ── Connection helper ──────────────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    """Persistent per-thread read connection for Flask request handlers.

    Returns the same connection on every call within the same thread, so the
    per-thread page cache stays warm across requests and PRAGMA overhead is paid
    once per thread lifetime instead of once per query.
    """
    if not _db_path:
        raise RuntimeError('db.init_db() has not been called')
    conn = getattr(_thread_local, 'read_conn', None)
    if conn is not None:
        # The indexer thread may have closed this handle; recreate if stale.
        try:
            conn.execute('SELECT 1')
        except sqlite3.DatabaseError as exc:
            _close_conn(conn)
            conn = None
            if _is_corruption_error(exc):
                _recover_malformed_db('read health-check')
        except sqlite3.Error:
            _close_conn(conn)
            conn = None
    if conn is None:
        last_exc: Optional[Exception] = None
        for attempt in range(2):
            conn = _open_sqlite_connection(_db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            try:
                _apply_sqlite_session_pragmas(conn, writer=False)
                _thread_local.read_conn = conn
                return conn
            except sqlite3.DatabaseError as exc:
                last_exc = exc
                _close_conn(conn)
                if attempt == 0 and _is_corruption_error(exc) and _recover_malformed_db('read connect'):
                    continue
                raise
        if last_exc:
            raise last_exc
    return conn


def _connect_writer() -> sqlite3.Connection:
    """Write-optimised connection for the background indexer.

    Always creates a fresh connection — never reuses the thread-local read
    connection — so writer PRAGMAs don't bleed into reader sessions.
    """
    if not _db_path:
        raise RuntimeError('db.init_db() has not been called')
    last_exc: Optional[Exception] = None
    for attempt in range(2):
        conn = _open_sqlite_connection(_db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            _apply_sqlite_session_pragmas(conn, writer=True)
            return conn
        except sqlite3.DatabaseError as exc:
            last_exc = exc
            _close_conn(conn)
            if attempt == 0 and _is_corruption_error(exc) and _recover_malformed_db('writer connect'):
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError('failed to open writer connection')


# ── Init ───────────────────────────────────────────────────────────────────────

def init_db(mvd2_dir: str, db_path: str) -> None:
    """Create the schema and register directories.  Safe to call multiple times."""
    global _db_path, _mvd2_dir
    _db_path  = os.path.abspath(db_path)
    _mvd2_dir = mvd2_dir
    os.makedirs(os.path.dirname(_db_path) or '.', exist_ok=True)
    try:
        with _connect() as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.executescript(_SCHEMA)
    except sqlite3.DatabaseError as exc:
        if not _is_corruption_error(exc):
            raise
        log.error('stats: corruption detected during init: %s', exc)
        if not _recover_malformed_db('init_db'):
            raise
        with _connect() as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.executescript(_SCHEMA)
    threading.Thread(target=_watchdog, daemon=True, name='stats-watchdog').start()
    # Pre-warm cache on startup in the background so the first web request
    # is served from cache, not a cold query.
    threading.Thread(target=_prewarm_cache, daemon=True, name='stats-prewarm').start()


def _prewarm_call(label: str, fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        log.warning('stats: pre-warm %s failed: %s', label, exc)
        return None


def _prewarm_global_queries(mode: str, period: str, include_ai: bool) -> None:
    _prewarm_call(f'get_weapon_stats[{mode},{period}]', get_weapon_stats, mode=mode, period=period)
    _prewarm_call(f'get_leaderboard[{mode},{period}]', get_leaderboard, min_games=1, limit=300, mode=mode, period=period)
    _prewarm_call(f'get_map_stats[{mode},{period}]', get_map_stats, mode=mode, period=period)
    _prewarm_call(f'get_summary[{mode},{period}]', get_summary, mode=mode, period=period)
    _prewarm_call(f'get_records[{mode},{period}]', get_records, mode=mode, period=period)
    _prewarm_call(f'get_activity_by_week[{mode},{period}]', get_activity_by_week, mode=mode, period=period)
    _prewarm_call(f'get_map_win_rates[{mode},{period}]', get_map_win_rates, mode=mode, period=period)
    _prewarm_call(f'get_recent_matches[{mode},{period}]', get_recent_matches, limit=20, mode=mode, period=period)
    _prewarm_call(f'get_first_kill_stats[{mode},{period}]', get_first_kill_stats, mode=mode, period=period, limit=15)
    _prewarm_call(f'get_team_analytics[{mode},{period}]', get_team_analytics, mode=mode, period=period, limit=20)
    _prewarm_call(f'get_round_analytics[{mode},{period}]', get_round_analytics, mode=mode, period=period, limit=20)
    _prewarm_call(f'get_match_analytics[{mode},{period}]', get_match_analytics, mode=mode, period=period, limit=20)
    _prewarm_call(f'get_weapon_analytics[{mode},{period}]', get_weapon_analytics, mode=mode, period=period, limit=20)
    _prewarm_call(f'get_behavior_analytics[{mode},{period}]', get_behavior_analytics, mode=mode, period=period, min_games=3, limit=20)
    _prewarm_call(f'get_rating_rankings[{mode},{period}]', get_rating_rankings, mode=mode, period=period, min_games=5, limit=100)
    _prewarm_call(f'get_h2h_rivalries[{period}]', get_h2h_rivalries, limit=_PREWARM_TOP_H2H, period=period)
    if include_ai:
        _prewarm_call(f'get_ai_meta_insights[{mode},{period}]', get_ai_meta_insights, mode=mode, period=period, limit=8)


def _prewarm_hot_entities(mode: str, period: str) -> None:
    weapon_rows = _prewarm_call(
        f'get_weapon_stats[hot:{mode},{period}]',
        get_weapon_stats,
        mode=mode,
        period=period,
    ) or []
    for row in weapon_rows[:_PREWARM_TOP_WEAPONS]:
        weapon = str((row or {}).get('weapon') or '').strip()
        if not weapon:
            continue
        _prewarm_call(f'get_weapon_detail[{weapon},{period}]', get_weapon_detail, weapon, period=period)
        _prewarm_call(f'get_weapon_top_killers[{weapon},{period}]', get_weapon_top_killers, weapon, min_games=1, period=period)
        _prewarm_call(f'get_weapon_victims[{weapon},{period}]', get_weapon_victims, weapon, period=period)
        _prewarm_call(f'get_weapon_map_effectiveness[{weapon},{period}]', get_weapon_map_effectiveness, weapon, period=period, limit=30)

    map_rows = _prewarm_call(
        f'get_map_stats[hot:{mode},{period}]',
        get_map_stats,
        mode=mode,
        period=period,
    ) or []
    for row in map_rows[:_PREWARM_TOP_MAPS]:
        map_name = str((row or {}).get('map') or '').strip()
        if not map_name:
            continue
        _prewarm_call(f'get_map_detail[{map_name},{period}]', get_map_detail, map_name, period=period)
        _prewarm_call(f'get_map_leaderboard[{map_name},{period}]', get_map_leaderboard, map_name, min_games=1, period=period)
        _prewarm_call(f'get_map_recent_matches[{map_name},{period}]', get_map_recent_matches, map_name, limit=20, period=period)

    player_rows = _prewarm_call(
        f'get_leaderboard[hot:{mode},{period}]',
        get_leaderboard,
        min_games=1,
        limit=_PREWARM_TOP_PLAYERS,
        mode=mode,
        period=period,
    ) or []
    for row in player_rows[:_PREWARM_TOP_PLAYERS]:
        name = str((row or {}).get('name') or '').strip()
        if not name:
            continue
        _prewarm_call(f'get_player_summary[{name},{period}]', get_player_summary, name, period=period)
        _prewarm_call(f'get_player_round_metrics[{name},{period}]', get_player_round_metrics, name, period=period)
        _prewarm_call(f'get_player_match_history[{name},{period}]', get_player_match_history, name, limit=50, period=period)
        _prewarm_call(f'get_player_map_breakdown[{name},{period}]', get_player_map_breakdown, name, period=period)
        _prewarm_call(f'get_player_weapon_stats[{name},{period}]', get_player_weapon_stats, name, period=period)
        _prewarm_call(f'get_player_rivals[{name},{period}]', get_player_rivals, name, limit=10, period=period)
        _prewarm_call(f'get_player_behavior_analytics[{name},{mode},{period}]', get_player_behavior_analytics, name, mode=mode, period=period)
        _prewarm_call(f'get_player_rating_history[{name},{mode},{period}]', get_player_rating_history, name, mode=mode, period=period, limit=60)
        _prewarm_call(f'get_player_activity_by_week[{name},{period}]', get_player_activity_by_week, name, period=period)

    rivalry_rows = _prewarm_call(
        f'get_h2h_rivalries[hot:{period}]',
        get_h2h_rivalries,
        limit=_PREWARM_TOP_H2H,
        period=period,
    ) or []
    for row in rivalry_rows[:_PREWARM_TOP_H2H]:
        p1 = str((row or {}).get('p1') or '').strip()
        p2 = str((row or {}).get('p2') or '').strip()
        if not p1 or not p2:
            continue
        _prewarm_call(f'get_h2h[{p1},{p2},{period}]', get_h2h, p1, p2, period=period)


def _prewarm_cache(force: bool = False) -> None:
    """Populate in-memory cache for heavy queries.  Safe to call any time."""
    global _prewarm_running
    if _running and not force:
        log.info('stats: skip pre-warm while indexer is running')
        return
    if _PREWARM_SCOPE == 'off':
        log.info('stats: pre-warm disabled (scope=off)')
        return

    with _prewarm_lock:
        if _prewarm_running:
            log.info('stats: pre-warm already running; skip duplicate request')
            return
        _prewarm_running = True

    started = _time.monotonic()
    try:
        provider, _, model, _ = _get_ai_provider_config()
        include_ai = _PREWARM_INCLUDE_GEMINI or provider != 'gemini'
        if provider == 'gemini' and not _PREWARM_INCLUDE_GEMINI:
            log.info('stats: pre-warm skipping Gemini-backed insights to avoid API overhead (set STATS_PREWARM_INCLUDE_GEMINI=true to include)')

        warmed_pairs: set[tuple[str, str]] = set()

        def warm_pair(mode: str, period: str, with_entities: bool = False) -> None:
            key = (mode, period)
            if key in warmed_pairs:
                return
            warmed_pairs.add(key)
            _prewarm_global_queries(mode, period, include_ai=include_ai)
            if with_entities:
                _prewarm_hot_entities(mode, period)

        # Always warm the default browsing path first.
        warm_pair(_DEFAULT_STATS_MODE, _DEFAULT_H2H_PERIOD,
                  with_entities=_PREWARM_SCOPE in ('extended', 'full'))

        if _PREWARM_SCOPE in ('extended', 'full'):
            for mode in _PREWARM_MODES_EXTENDED:
                for period in _PREWARM_PERIODS_EXTENDED:
                    warm_pair(mode, period, with_entities=False)
            # Also warm hot entities for all-time Teamplay lookups.
            warm_pair(_DEFAULT_STATS_MODE, 'all', with_entities=True)

        if _PREWARM_SCOPE == 'full':
            for mode in _PREWARM_MODES_FULL:
                for period in _PREWARM_PERIODS_FULL:
                    warm_pair(mode, period, with_entities=False)
            # In full mode, warm entity detail caches for all extended mode/period pairs.
            for mode in _PREWARM_MODES_EXTENDED:
                for period in _PREWARM_PERIODS_EXTENDED:
                    _prewarm_hot_entities(mode, period)

        elapsed = _time.monotonic() - started
        log.info('stats: pre-warm finished (scope=%s, pairs=%d, %.1fs)', _PREWARM_SCOPE, len(warmed_pairs), elapsed)
    finally:
        with _prewarm_lock:
            _prewarm_running = False


# ── Background indexer ─────────────────────────────────────────────────────────

def trigger_reindex(force: bool = False) -> None:
    """Start the background indexer thread if not already running.

    When *force* is True, all stored file_mtime values are reset to -1 so
    every MVD2 file is re-parsed regardless of whether it changed on disk.
    """
    global _running
    if force:
        try:
            with _connect() as conn:
                conn.execute('UPDATE matches SET file_mtime = -1')
                conn.commit()
            log.info('stats: forced reindex — all mtimes reset')
        except Exception as exc:
            log.warning('stats: failed to reset mtimes: %s', exc)
    with _lock:
        if _running:
            return
        _running = True
    threading.Thread(target=_run_indexer, daemon=True, name='stats-indexer').start()


def _rebuild_weapon_stats_agg(conn: sqlite3.Connection) -> None:
    """Recompute materialized global weapon stats from kill_events."""
    conn.execute('DELETE FROM weapon_stats_agg')
    conn.execute('''
        WITH weapon_totals AS (
            SELECT k.weapon,
                   COUNT(*) AS kills,
                   SUM(CASE WHEN k.location = 'head' THEN 1 ELSE 0 END) AS hs
            FROM kill_events k
            JOIN matches m ON m.id = k.match_id
            WHERE k.team_kill = 0 AND m.game_mode_detail = ?
            GROUP BY k.weapon
        ),
        weapon_top AS (
            SELECT k.weapon,
                   k.killer,
                   ROW_NUMBER() OVER (
                       PARTITION BY k.weapon ORDER BY COUNT(*) DESC
                   ) AS rn
            FROM kill_events k
            JOIN matches m ON m.id = k.match_id
            WHERE k.team_kill = 0 AND m.game_mode_detail = ?
            GROUP BY k.weapon, k.killer
        )
        INSERT INTO weapon_stats_agg (weapon, kills, hs, hs_pct, top_killer)
        SELECT wt.weapon,
               wt.kills,
               wt.hs,
               CASE WHEN wt.kills > 0
                    THEN ROUND(100.0 * wt.hs / wt.kills, 1)
                    ELSE 0 END AS hs_pct,
               wtop.killer
        FROM weapon_totals wt
        LEFT JOIN weapon_top wtop
               ON wtop.weapon = wt.weapon AND wtop.rn = 1
        ORDER BY wt.kills DESC
    ''', (_DEFAULT_STATS_MODE, _DEFAULT_STATS_MODE))


def _query_weapon_stats_live(conn: sqlite3.Connection,
                             mode: str = _DEFAULT_STATS_MODE,
                             period: str = _DEFAULT_H2H_PERIOD,
                             week: Optional[str] = None):
    """Read weapon stats directly from kill_events without writing aggregates."""
    mode = _normalize_mode_filter(mode)
    mode_clause, mode_params = _mode_filter_clause(mode)
    period_clause, period_params, _ = _h2h_period_filter_clause(period)
    week_clause, week_params, _ = _week_filter_clause(week)
    sql = f'''
        WITH weapon_totals AS (
            SELECT k.weapon,
                   COUNT(*) AS kills,
                   SUM(CASE WHEN k.location = 'head' THEN 1 ELSE 0 END) AS hs
            FROM kill_events k
            JOIN matches m ON m.id = k.match_id
            WHERE k.team_kill = 0 AND {mode_clause} AND {period_clause} AND {week_clause}
            GROUP BY k.weapon
        ),
        weapon_top AS (
            SELECT k.weapon,
                   k.killer,
                   ROW_NUMBER() OVER (
                       PARTITION BY k.weapon ORDER BY COUNT(*) DESC
                   ) AS rn
            FROM kill_events k
            JOIN matches m ON m.id = k.match_id
            WHERE k.team_kill = 0 AND {mode_clause} AND {period_clause} AND {week_clause}
            GROUP BY k.weapon, k.killer
        )
        SELECT wt.weapon,
               wt.kills,
               wt.hs,
               CASE WHEN wt.kills > 0
                    THEN ROUND(100.0 * wt.hs / wt.kills, 1)
                    ELSE 0 END AS hs_pct,
               wtop.killer AS top_killer
        FROM weapon_totals wt
        LEFT JOIN weapon_top wtop
               ON wtop.weapon = wt.weapon AND wtop.rn = 1
        ORDER BY wt.kills DESC
    '''
    return conn.execute(
        sql,
        mode_params + period_params + week_params + mode_params + period_params + week_params,
    ).fetchall()


def _rebuild_leaderboard_agg(conn: sqlite3.Connection) -> None:
    """Recompute materialized leaderboard rows from player_match_stats."""
    conn.execute('DELETE FROM leaderboard_agg')
    conn.execute('''
        INSERT INTO leaderboard_agg (
            name, games, kills, deaths, team_kills, hits, shots, damage,
            hs_kills, best_game, kd, avg_kills, avg_acc,
            h_head, h_stomach, h_chest, h_legs,
            awards_impressive, awards_accuracy, awards_excellent,
            rounds_played, damage_per_round, impact_score
        )
        SELECT
            p.name,
            COUNT(DISTINCT p.match_id)                                    AS games,
            SUM(p.kills)                                                  AS kills,
            SUM(p.deaths)                                                 AS deaths,
            SUM(p.team_kills)                                             AS team_kills,
            SUM(p.hits)                                                   AS hits,
            SUM(p.shots)                                                  AS shots,
            SUM(p.damage)                                                 AS damage,
            SUM(p.hs_kills)                                               AS hs_kills,
            MAX(p.kills)                                                  AS best_game,
            ROUND(CAST(SUM(p.kills) AS REAL) / MAX(SUM(p.deaths),1), 2)  AS kd,
            ROUND(CAST(SUM(p.kills) AS REAL) / COUNT(DISTINCT p.match_id), 1) AS avg_kills,
            ROUND(AVG(CASE WHEN p.accuracy IS NOT NULL THEN p.accuracy END), 1) AS avg_acc,
            SUM(p.hits_head)                                              AS h_head,
            SUM(p.hits_stomach)                                           AS h_stomach,
            SUM(p.hits_chest)                                             AS h_chest,
            SUM(p.hits_legs)                                              AS h_legs,
            SUM(p.awards_impressive)                                      AS awards_impressive,
            SUM(p.awards_accuracy)                                        AS awards_accuracy,
            SUM(p.awards_excellent)                                       AS awards_excellent,
            COALESCE(rp.rounds_played, 0)                                 AS rounds_played,
            CASE WHEN COALESCE(rp.rounds_played, 0) > 0
                 THEN ROUND(CAST(rp.dmg_sum AS REAL) / rp.rounds_played, 2)
                 ELSE 0 END                                               AS damage_per_round,
            CASE WHEN COALESCE(rp.rounds_played, 0) > 0
                 THEN ROUND(
                     (
                         (rp.kill_sum * 100.0) +
                         (rp.assist_v * 60.0) +
                         (rp.assist_e * 30.0) +
                         (rp.dmg_sum * 0.20) +
                         (rp.fk_sum * 40.0) +
                         (rp.surv_sum * 20.0) -
                         (rp.death_sum * 35.0)
                     ) / rp.rounds_played,
                     2
                 )
                 ELSE 0 END                                               AS impact_score
        FROM player_match_stats p
        JOIN matches m ON m.id = p.match_id
        LEFT JOIN (
            SELECT prs.name,
                   COUNT(*)                         AS rounds_played,
                   SUM(prs.kills)                   AS kill_sum,
                   SUM(prs.deaths)                  AS death_sum,
                   SUM(prs.assists_verified)        AS assist_v,
                   SUM(prs.assists_estimated)       AS assist_e,
                   SUM(prs.damage)                  AS dmg_sum,
                   SUM(prs.first_kill)              AS fk_sum,
                   SUM(prs.survived)                AS surv_sum
            FROM player_round_stats prs
            JOIN matches mm ON mm.id = prs.match_id
            WHERE mm.game_mode = 'tdm'
            GROUP BY prs.name
        ) rp ON rp.name = p.name
        WHERE (p.kills > 0 OR p.deaths > 0) AND m.game_mode_detail = ?
        GROUP BY p.name
    ''', (_DEFAULT_STATS_MODE,))


def _rebuild_map_stats_agg(conn: sqlite3.Connection) -> None:
    """Recompute materialized map stats rows."""
    conn.execute('DELETE FROM map_stats_agg')
    conn.execute('''
        WITH map_top AS (
            SELECT m2.map, p2.name,
                   ROW_NUMBER() OVER (
                       PARTITION BY m2.map ORDER BY SUM(p2.kills) DESC
                   ) AS rn
            FROM player_match_stats p2
            JOIN matches m2 ON m2.id = p2.match_id
            WHERE m2.game_mode_detail = ?
            GROUP BY m2.map, p2.name
        )
        INSERT INTO map_stats_agg (
            map, matches, kills, avg_kills, players, avg_duration_min, top_killer
        )
        SELECT
            m.map,
            COUNT(DISTINCT m.id)        AS matches,
            SUM(m.total_kills)          AS kills,
            ROUND(CAST(SUM(m.total_kills) AS REAL) / COUNT(DISTINCT m.id), 1) AS avg_kills,
            COUNT(DISTINCT p.name)      AS players,
            ROUND(AVG(m.duration)/60,1) AS avg_duration_min,
            mt.name                     AS top_killer
        FROM matches m
        LEFT JOIN player_match_stats p ON p.match_id = m.id
        LEFT JOIN map_top mt ON mt.map = m.map AND mt.rn = 1
        WHERE m.map != '' AND m.game_mode_detail = ?
        GROUP BY m.map
    ''', (_DEFAULT_STATS_MODE, _DEFAULT_STATS_MODE))


def _rebuild_summary_agg(conn: sqlite3.Connection) -> None:
    """Recompute one-row summary aggregate."""
    conn.execute('DELETE FROM summary_agg')
    conn.execute('''
        INSERT INTO summary_agg (
            id, total_matches, total_players, total_kills,
            total_damage, best_game_kills, total_maps
        )
        SELECT
            1 AS id,
            COUNT(DISTINCT m.id)    AS total_matches,
            COUNT(DISTINCT p.name)  AS total_players,
            SUM(m.total_kills)      AS total_kills,
            SUM(p.damage)           AS total_damage,
            MAX(p.kills)            AS best_game_kills,
            COUNT(DISTINCT m.map)   AS total_maps
        FROM matches m
        LEFT JOIN player_match_stats p ON p.match_id = m.id
        WHERE m.game_mode_detail = ?
    ''', (_DEFAULT_STATS_MODE,))


def _rebuild_records_agg(conn: sqlite3.Connection) -> None:
    """Recompute serialized records payloads for instant get_records()."""
    def _one(sql: str, params=()):
        r = conn.execute(sql, params).fetchone()
        return dict(r) if r else None

    records = {
        'most_kills': _one('''
            SELECT p.name, p.kills AS value, m.path, m.map,
                   m.played_at, p.match_id
            FROM player_match_stats p JOIN matches m ON m.id = p.match_id
            WHERE m.game_mode_detail = ?
            ORDER BY p.kills DESC LIMIT 1
        ''', (_DEFAULT_STATS_MODE,)),
        'best_kd': _one('''
            SELECT p.name,
                   ROUND(CAST(p.kills AS REAL) / MAX(p.deaths,1), 2) AS value,
                   p.kills, p.deaths, m.path, m.map, m.played_at, p.match_id
            FROM player_match_stats p JOIN matches m ON m.id = p.match_id
            WHERE p.kills >= 3 AND m.game_mode_detail = ?
            ORDER BY CAST(p.kills AS REAL) / MAX(p.deaths,1) DESC LIMIT 1
        ''', (_DEFAULT_STATS_MODE,)),
        'best_acc': _one('''
            SELECT p.name, p.accuracy AS value, m.path, m.map,
                   m.played_at, p.match_id
            FROM player_match_stats p JOIN matches m ON m.id = p.match_id
            WHERE p.accuracy IS NOT NULL AND p.shots >= 5 AND m.game_mode_detail = ?
            ORDER BY p.accuracy DESC LIMIT 1
        ''', (_DEFAULT_STATS_MODE,)),
        'most_damage': _one('''
            SELECT p.name, p.damage AS value, m.path, m.map,
                   m.played_at, p.match_id
            FROM player_match_stats p JOIN matches m ON m.id = p.match_id
            WHERE m.game_mode_detail = ?
            ORDER BY p.damage DESC LIMIT 1
        ''', (_DEFAULT_STATS_MODE,)),
        'most_hs': _one('''
            SELECT p.name, p.hs_kills AS value, m.path, m.map,
                   m.played_at, p.match_id
            FROM player_match_stats p JOIN matches m ON m.id = p.match_id
            WHERE m.game_mode_detail = ?
            ORDER BY p.hs_kills DESC LIMIT 1
        ''', (_DEFAULT_STATS_MODE,)),
        'longest_match': _one('''
            SELECT m.map, m.path, m.played_at,
                   ROUND(m.duration / 60.0, 1) AS value
            FROM matches m
            WHERE m.game_mode_detail = ?
            ORDER BY m.duration DESC LIMIT 1
        ''', (_DEFAULT_STATS_MODE,)),
    }

    conn.execute('DELETE FROM records_agg')
    conn.executemany(
        'INSERT INTO records_agg (key, payload) VALUES (?, ?)',
        [(k, json.dumps(v)) for k, v in records.items()]
    )


def _rebuild_activity_week_agg(conn: sqlite3.Connection) -> None:
    """Recompute weekly activity timeline aggregate."""
    conn.execute('DELETE FROM activity_week_agg')
    conn.execute('''
        INSERT INTO activity_week_agg (week, matches, kills)
        SELECT
            strftime('%Y-%W', datetime(played_at, 'unixepoch')) AS week,
            COUNT(*) AS matches,
            SUM(total_kills) AS kills
        FROM matches
        WHERE game_mode_detail = ?
        GROUP BY week
        ORDER BY week ASC
    ''', (_DEFAULT_STATS_MODE,))


def _rebuild_recent_matches_agg(conn: sqlite3.Connection) -> None:
    """Recompute recent match list with player counts."""
    conn.execute('DELETE FROM recent_matches_agg')
    conn.execute('''
        INSERT INTO recent_matches_agg (
            path, map, played_at, duration,
            total_kills, t1_rounds, t2_rounds, game_mode, game_mode_detail, player_count
        )
        SELECT
            m.path,
            m.map,
            m.played_at,
            m.duration,
            m.total_kills,
            m.t1_rounds,
            m.t2_rounds,
            m.game_mode,
            m.game_mode_detail,
            COUNT(p.id) AS player_count
        FROM matches m
        LEFT JOIN player_match_stats p ON p.match_id = m.id
        GROUP BY m.id
    ''')


def _rebuild_map_win_rates_agg(conn: sqlite3.Connection) -> None:
    """Recompute map win-rate aggregate."""
    conn.execute('DELETE FROM map_win_rates_agg')
    conn.execute('''
        INSERT INTO map_win_rates_agg (map, matches, t1_wins, t2_wins, draws)
        SELECT map,
               COUNT(*) AS matches,
               SUM(CASE WHEN t1_rounds > t2_rounds THEN 1 ELSE 0 END) AS t1_wins,
               SUM(CASE WHEN t2_rounds > t1_rounds THEN 1 ELSE 0 END) AS t2_wins,
               SUM(CASE WHEN t1_rounds = t2_rounds THEN 1 ELSE 0 END) AS draws
        FROM matches
        WHERE map != '' AND (t1_rounds + t2_rounds) > 0 AND game_mode_detail = ?
        GROUP BY map
    ''', (_DEFAULT_STATS_MODE,))


def _rebuild_materialized_aggregates(conn: sqlite3.Connection) -> None:
    """Recompute all heavy materialized aggregates in one transaction."""
    _rebuild_weapon_stats_agg(conn)
    _rebuild_leaderboard_agg(conn)
    _rebuild_map_stats_agg(conn)
    _rebuild_summary_agg(conn)
    _rebuild_records_agg(conn)
    _rebuild_activity_week_agg(conn)
    _rebuild_recent_matches_agg(conn)
    _rebuild_map_win_rates_agg(conn)


def _watchdog() -> None:
    """Trigger a daily reindex at the configured hour (local container time)."""
    import time
    from datetime import datetime, timedelta
    TARGET_HOUR = int(os.environ.get('STATS_REINDEX_HOUR', '10'))
    while True:
        now = datetime.now().astimezone()
        next_run = now.replace(hour=TARGET_HOUR, minute=0, second=0, microsecond=0)
        if now >= next_run:
            next_run += timedelta(days=1)
        sleep_secs = (next_run - now).total_seconds()
        log.info('stats: watchdog sleeping %.0f s until %s', sleep_secs, next_run.strftime('%Y-%m-%d %H:%M %Z'))
        time.sleep(sleep_secs)
        trigger_reindex()


def _parse_date_from_name(relpath: str) -> int:
    """Extract Unix timestamp from a filename like YYYY-MM-DD_HHMMSS or YYYY-MM-DD_HHMM."""
    m = _DATE_RE.search(relpath)
    if m:
        from datetime import datetime
        try:
            time_part = m.group(2)
            fmt = '%Y-%m-%d%H%M%S' if len(time_part) == 6 else '%Y-%m-%d%H%M'
            return int(datetime.strptime(m.group(1) + time_part, fmt).timestamp())
        except ValueError:
            pass
    return 0


def _is_spectator(name: str, data: dict) -> bool:
    """Mirror the JS isSpectator logic from replay.html."""
    kills  = (data.get('kill_counts')  or {}).get(name, 0)
    shots  = (data.get('shots_fired')  or {}).get(name, 0)
    if kills or shots:
        return False
    deaths = (data.get('death_counts') or {}).get(name, 0)
    hits   = (data.get('hit_counts')   or {}).get(name, 0)
    damage = (data.get('damage_dealt') or {}).get(name, 0)
    if not (deaths or hits or damage):
        return True
    return name not in set(data.get('players_with_frames') or [])


def _coerce_frame_count(data: dict, raw_kills: list) -> int:
    """Pick a robust frame_count even when partial parses miss some metadata."""
    frame_count = int(data.get('frame_count') or 0)
    if frame_count <= 0:
        duration = float(data.get('duration', 0) or 0)
        if duration > 0:
            frame_count = max(frame_count, int(round(duration / 0.1)))
    if raw_kills:
        max_kill_frame = max(int(k.get('frame', 0) or 0) for k in raw_kills)
        frame_count = max(frame_count, max_kill_frame + 1)
    return max(frame_count, 1)


def _is_no_score_telefrag(kill_event: dict) -> bool:
    """AQ2 team-mode telefrags can print obituaries without score/death updates."""
    weapon = str(kill_event.get('weapon') or '')
    killer_team = int(kill_event.get('killer_team', 0) or 0)
    return weapon == 'Telefrag' and killer_team != 0


def _is_scoring_enemy_kill(kill_event: dict) -> bool:
    if kill_event.get('suicide'):
        return False
    if int(kill_event.get('team_kill', 0) or 0):
        return False
    if _is_no_score_telefrag(kill_event):
        return False
    return True


def _build_round_intervals(data: dict, frame_count: int) -> list:
    """Return deterministic [(round_index, start_frame, end_frame), ...]."""
    starts = data.get('round_start_frames') or []
    clean = sorted({
        int(v) for v in starts
        if isinstance(v, int) or (isinstance(v, str) and str(v).isdigit())
    })
    boundaries = [0]
    boundaries.extend(v for v in clean if 0 < v < frame_count)
    boundaries.append(frame_count)
    # Deduplicate while preserving order.
    uniq = []
    for v in boundaries:
        if not uniq or uniq[-1] != v:
            uniq.append(v)
    intervals = []
    for idx in range(len(uniq) - 1):
        start_f = int(uniq[idx])
        end_f = int(uniq[idx + 1])
        if end_f <= start_f:
            continue
        intervals.append((len(intervals) + 1, start_f, end_f))
    if not intervals:
        intervals.append((1, 0, frame_count))
    return intervals


_ASSIST_WINDOW_FRAMES = 50  # 5 seconds at 10 fps
_HIT_LOC_WEIGHTS = {
    'head': 1.8,
    'chest': 0.65,
    'stomach': 0.40,
    'legs': 0.25,
    'unknown': 0.55,
    'kvlr_helmet': 0.50,
    'kvlr_vest': 0.10,
}


def _base_identity_name(name: Optional[str]) -> str:
    if not name:
        return ''
    return _SLOT_SUFFIX_RE.sub('', str(name).strip())


def _allocate_integer_proportional(total: int, weights: list[float]) -> list[int]:
    """Allocate integer totals across weighted buckets while preserving exact sum."""
    if total <= 0 or not weights:
        return [0 for _ in weights]

    clean = [max(0.0, float(w or 0.0)) for w in weights]
    wsum = sum(clean)
    if wsum <= 0:
        clean = [1.0 for _ in weights]
        wsum = float(len(clean))

    raw = [(total * w) / wsum for w in clean]
    ints = [int(v) for v in raw]
    remainder = total - sum(ints)
    if remainder > 0:
        order = sorted(
            range(len(clean)),
            key=lambda i: ((raw[i] - ints[i]), clean[i]),
            reverse=True,
        )
        for idx in order[:remainder]:
            ints[idx] += 1
    return ints


def _write_parsed(conn: sqlite3.Connection, relpath: str, mtime: float, data: dict,
                  is_update: bool = False) -> None:
    """Write pre-parsed MVD2 data to the DB.  Called from the main indexer thread."""
    if is_update:
        # Delete old entry (cascade removes player rows) only when replacing
        # an existing row.  Skipping this for new files saves N useless scans.
        conn.execute('DELETE FROM matches WHERE path=?', (relpath,))

    raw_kills = data.get('kills') or []
    raw_round_events = data.get('round_events') or []
    raw_hit_events = data.get('hit_events') or []
    frame_count = _coerce_frame_count(data, raw_kills)

    rw  = data.get('round_wins')  or {}
    ts  = data.get('team_scores') or {}
    t1r = int(rw.get('1', ts.get('1', 0)) or 0)
    t2r = int(rw.get('2', ts.get('2', 0)) or 0)

    played_at = _parse_date_from_name(relpath) or int(mtime)

    game_mode = (data.get('game_mode') or 'tdm').strip().lower()
    game_mode_detail = _canonical_mode_detail(data.get('game_mode_detail'), game_mode)

    try:
        cur = conn.execute(
            'INSERT INTO matches (path, map, played_at, duration, total_kills,'
              ' t1_rounds, t2_rounds, file_mtime, game_mode, game_mode_detail) VALUES (?,?,?,?,?,?,?,?,?,?)',
            (relpath,
             data.get('map', ''),
             played_at,
             float(data.get('duration', 0) or 0),
                         len(raw_kills),
             t1r, t2r,
             mtime,
               game_mode,
               game_mode_detail)
        )
    except sqlite3.IntegrityError:
        # Another process (e.g. Werkzeug reloader's second process) beat us to it.
        log.debug('stats: %s already inserted by concurrent indexer, skipping', relpath)
        return
    match_id = cur.lastrowid

    names       = data.get('player_names')       or {}
    teams       = data.get('player_teams')        or {}
    kill_c      = data.get('kill_counts')         or {}
    death_c     = data.get('death_counts')        or {}
    tk_c        = data.get('team_kill_counts')    or {}
    hit_c       = data.get('hit_counts')          or {}
    shots_c     = data.get('shots_fired')         or {}
    dmg_c       = data.get('damage_dealt')        or {}
    acc_c       = data.get('accuracy')            or {}
    hs_c        = data.get('headshot_kills')      or {}
    loc_c       = data.get('hit_loc_by_player')   or {}   # {name: {loc: count}}
    award_c     = data.get('award_counts')        or {}   # {name: {Impressive, Accuracy, Excellent}}
    streak_c    = data.get('best_kill_streak')    or {}   # {name: int}
    captains_by_team = data.get('captains_by_team') or {}
    captain_stats_by_team = data.get('captain_stats_by_team') or {}

    seen: set = set()
    all_names = set(names.values()) | set(kill_c) | set(death_c)
    player_rows = []
    for name in all_names:
        if not name or name == '[MVDSPEC]' or name in seen:
            continue
        seen.add(name)
        if _is_spectator(name, data):
            continue
        ploc = loc_c.get(name) or {}
        pawd = award_c.get(name) or {}
        player_rows.append(
            (match_id, name,
             int(teams.get(name, 0)),
             int(kill_c.get(name, 0)),
             int(death_c.get(name, 0)),
             int(tk_c.get(name, 0)),
             int(hit_c.get(name, 0)),
             int(shots_c.get(name, 0)),
             int(dmg_c.get(name, 0)),
             acc_c.get(name),
             int(hs_c.get(name, 0)),
             int(ploc.get('head', 0)),
             int(ploc.get('stomach', 0)),
             int(ploc.get('chest', 0)),
             int(ploc.get('legs', 0)),
             int(pawd.get('Impressive', 0)),
             int(pawd.get('Accuracy', 0)),
             int(pawd.get('Excellent', 0)),
             int(streak_c.get(name, 0)))
        )
    conn.executemany(
        'INSERT INTO player_match_stats'
        ' (match_id, name, team, kills, deaths, team_kills, hits, shots,'
        '  damage, accuracy, hs_kills,'
        '  hits_head, hits_stomach, hits_chest, hits_legs,'
        '  awards_impressive, awards_accuracy, awards_excellent, best_kill_streak)'
        ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        player_rows
    )

    captain_rows = []
    for team_num in (1, 2):
        t_key = str(team_num)
        captain_name = str(captains_by_team.get(t_key) or captains_by_team.get(team_num) or '').strip()
        if not captain_name:
            continue
        cmeta = captain_stats_by_team.get(t_key) or captain_stats_by_team.get(team_num) or {}
        captain_rows.append((
            match_id,
            team_num,
            captain_name,
            int(cmeta.get('sightings', 0) or 0),
            int(cmeta.get('switches', 0) or 0),
            int(cmeta.get('first_frame', 0) or 0),
            int(cmeta.get('last_frame', 0) or 0),
        ))
    if captain_rows:
        conn.executemany(
            'INSERT INTO match_captains (match_id, team, captain_name, sightings, switches, first_frame, last_frame)'
            ' VALUES (?,?,?,?,?,?,?)',
            captain_rows,
        )

    # Bulk-insert individual kill events (used for weapon stats, nemesis/prey).
    conn.executemany(
        'INSERT INTO kill_events (match_id, killer, victim, weapon, location, team_kill, frame_idx)'
        ' VALUES (?,?,?,?,?,?,?)',
        [(match_id,
          k.get('killer', ''),
          k.get('victim', ''),
          k.get('weapon', 'unknown'),
          k.get('location', 'unknown'),
          int(k.get('team_kill', False)),
          int(k.get('frame', 0)))
         for k in raw_kills
         if k.get('killer') and k.get('victim')]
    )

    # Persist round-level facts for analytics v2.
    intervals = _build_round_intervals(data, frame_count)
    player_team_by_name = {r[1]: int(r[2]) for r in player_rows}
    round_ranges: list = []  # (round_id, start_frame, end_frame)

    # Normalize hit events into identity names for round damage/assist attribution.
    normalized_hits = []
    for h in raw_hit_events:
        attacker_slot = h.get('attacker')
        attacker_name = names.get(attacker_slot)
        if attacker_name is None:
            try:
                attacker_name = names.get(int(attacker_slot))
            except Exception:
                attacker_name = None
        if not attacker_name:
            continue
        victim_name = str(h.get('victim', '') or '').strip()
        if not victim_name:
            continue
        normalized_hits.append({
            'frame': int(h.get('frame', 0) or 0),
            'attacker': attacker_name,
            'victim': victim_name,
            'victim_base': _base_identity_name(victim_name),
            'location': str(h.get('location', 'unknown') or 'unknown'),
        })

    round_payloads: list = []
    for round_index, start_f, end_f in intervals:
        round_kills = [
            k for k in raw_kills
            if start_f <= int(k.get('frame', 0) or 0) < end_f
        ]
        round_kills.sort(key=lambda ev: int(ev.get('frame', 0) or 0))

        round_hits = [
            h for h in normalized_hits
            if start_f <= int(h.get('frame', 0) or 0) < end_f
        ]

        enemy_kills = [k for k in round_kills if _is_scoring_enemy_kill(k)]
        first_enemy_kill = enemy_kills[0] if enemy_kills else None
        last_enemy_kill = enemy_kills[-1] if enemy_kills else None

        winner_team = int(last_enemy_kill.get('killer_team', 0) or 0) if last_enemy_kill else 0
        is_tie = 1 if winner_team == 0 else 0
        win_condition = 'last_kill' if winner_team else 'timeout_or_tie'

        first_kill_player = first_enemy_kill.get('killer') if first_enemy_kill else None
        first_kill_team = 0
        first_kill_weapon = None
        first_kill_location = None
        first_kill_frame = None
        first_death_player = None
        first_death_team = 0
        if first_enemy_kill:
            first_kill_team = int(first_enemy_kill.get('killer_team', 0) or 0)
            first_kill_weapon = first_enemy_kill.get('weapon', 'unknown')
            first_kill_location = first_enemy_kill.get('location', 'unknown')
            first_kill_frame = int(first_enemy_kill.get('frame', 0) or 0)
            first_death_player = first_enemy_kill.get('victim')
            first_death_team = int(first_enemy_kill.get('victim_team', 0) or 0)

        kills_delta = {name: 0 for name in player_team_by_name.keys()}
        deaths_delta = {name: 0 for name in player_team_by_name.keys()}
        assists_verified = {name: 0 for name in player_team_by_name.keys()}
        assists_estimated = {name: 0 for name in player_team_by_name.keys()}
        hit_weights = {name: 0.0 for name in player_team_by_name.keys()}

        # Round-local hit weights used for damage attribution.
        for hh in round_hits:
            attacker_name = hh['attacker']
            if attacker_name not in hit_weights:
                continue
            loc = str(hh.get('location', 'unknown') or 'unknown')
            hit_weights[attacker_name] += float(_HIT_LOC_WEIGHTS.get(loc, _HIT_LOC_WEIGHTS['unknown']))

        # Kills/deaths and victim-penalty handling.
        for ev in round_kills:
            killer = ev.get('killer')
            victim = ev.get('victim')
            if not killer or not victim:
                continue

            if ev.get('suicide'):
                # Post-round plummets/lava print the obituary but do not call
                # Subtract_Frag / Add_Death (p_client.c:901). Parser tags those
                # events with no_score=True so we skip them here.
                if ev.get('no_score'):
                    continue
                if killer in kills_delta:
                    kills_delta[killer] -= 1
                if victim in deaths_delta:
                    deaths_delta[victim] += 1
            elif int(ev.get('team_kill', 0) or 0):
                if killer in kills_delta:
                    kills_delta[killer] -= 1
                if victim in deaths_delta:
                    deaths_delta[victim] += 1
            elif _is_no_score_telefrag(ev):
                pass
            else:
                if killer in kills_delta:
                    kills_delta[killer] += 1
                if victim in deaths_delta:
                    deaths_delta[victim] += 1

            if ev.get('victim_frag_penalty') and not ev.get('suicide') and not int(ev.get('team_kill', 0) or 0):
                penalty = int(ev.get('victim_frag_penalty', 1) or 1)
                if victim in kills_delta:
                    kills_delta[victim] -= penalty

        # Assist model (confidence tiers).
        for ev in enemy_kills:
            killer = ev.get('killer')
            victim = ev.get('victim')
            if not killer or not victim:
                continue

            kf = int(ev.get('frame', 0) or 0)
            killer_team = int(ev.get('killer_team', 0) or player_team_by_name.get(killer, 0) or 0)
            victim_team = int(ev.get('victim_team', 0) or player_team_by_name.get(victim, 0) or 0)
            victim_base = _base_identity_name(victim)

            in_window = [
                h for h in round_hits
                if (kf - _ASSIST_WINDOW_FRAMES) <= int(h['frame']) < kf
            ]

            verified_set = set()
            estimated_set = set()
            for hh in in_window:
                assister = hh['attacker']
                if assister == killer:
                    continue
                if assister not in player_team_by_name:
                    continue
                ateam = int(player_team_by_name.get(assister, 0) or 0)
                if killer_team and ateam and ateam != killer_team:
                    continue
                if victim_team and ateam and ateam == victim_team:
                    continue

                if hh['victim'] == victim:
                    verified_set.add(assister)
                elif hh['victim_base'] == victim_base:
                    estimated_set.add(assister)

            estimated_set -= verified_set
            for nm in verified_set:
                assists_verified[nm] = int(assists_verified.get(nm, 0) or 0) + 1
            for nm in estimated_set:
                assists_estimated[nm] = int(assists_estimated.get(nm, 0) or 0) + 1

        round_payloads.append({
            'round_index': int(round_index),
            'start_frame': int(start_f),
            'end_frame': int(end_f),
            'winner_team': int(winner_team),
            'is_tie': int(is_tie),
            'win_condition': win_condition,
            'first_kill_player': first_kill_player,
            'first_kill_team': int(first_kill_team),
            'first_kill_weapon': first_kill_weapon,
            'first_kill_location': first_kill_location,
            'first_kill_frame': first_kill_frame,
            'first_death_player': first_death_player,
            'first_death_team': int(first_death_team),
            'kills_delta': kills_delta,
            'deaths_delta': deaths_delta,
            'assists_verified': assists_verified,
            'assists_estimated': assists_estimated,
            'hit_weights': hit_weights,
            'damage_by_name': {},
        })

    # Damage-per-round attribution: preserve per-match totals and split by hit density.
    for name in player_team_by_name.keys():
        total_damage = int(dmg_c.get(name, 0) or 0)
        if total_damage <= 0:
            continue

        hit_weights = [float(p['hit_weights'].get(name, 0.0) or 0.0) for p in round_payloads]
        if sum(hit_weights) <= 0:
            fallback_activity = [
                float(max(int(p['kills_delta'].get(name, 0) or 0), 0) + int(p['deaths_delta'].get(name, 0) or 0))
                for p in round_payloads
            ]
            hit_weights = fallback_activity

        alloc = _allocate_integer_proportional(total_damage, hit_weights)
        for idx, dmg_value in enumerate(alloc):
            round_payloads[idx]['damage_by_name'][name] = int(dmg_value)

    for payload in round_payloads:
        round_cur = conn.execute(
            'INSERT INTO rounds ('
            ' match_id, round_index, start_frame, end_frame, duration_frames, duration_seconds,'
            ' winner_team, is_tie, win_condition,'
            ' first_kill_frame, first_kill_player, first_kill_team, first_kill_weapon, first_kill_location,'
            ' first_death_player, first_death_team'
            ' ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (
                match_id,
                int(payload['round_index']),
                int(payload['start_frame']),
                int(payload['end_frame']),
                int(payload['end_frame'] - payload['start_frame']),
                float((payload['end_frame'] - payload['start_frame']) * 0.1),
                int(payload['winner_team']),
                int(payload['is_tie']),
                str(payload['win_condition']),
                payload['first_kill_frame'],
                payload['first_kill_player'],
                int(payload['first_kill_team']),
                payload['first_kill_weapon'],
                payload['first_kill_location'],
                payload['first_death_player'],
                int(payload['first_death_team']),
            ),
        )
        round_id = int(round_cur.lastrowid)
        round_ranges.append((round_id, int(payload['start_frame']), int(payload['end_frame'])))

        player_round_rows = []
        for name, team_num in player_team_by_name.items():
            rk = int(payload['kills_delta'].get(name, 0) or 0)
            rd = int(payload['deaths_delta'].get(name, 0) or 0)
            player_round_rows.append((
                match_id,
                round_id,
                name,
                int(team_num),
                rk,
                rd,
                int(payload['assists_verified'].get(name, 0) or 0),
                int(payload['assists_estimated'].get(name, 0) or 0),
                int(payload['damage_by_name'].get(name, 0) or 0),
                1 if payload['first_kill_player'] and name == payload['first_kill_player'] else 0,
                1 if payload['first_death_player'] and name == payload['first_death_player'] else 0,
                1 if rd == 0 else 0,
            ))

        conn.executemany(
            'INSERT INTO player_round_stats ('
            ' match_id, round_id, name, team, kills, deaths, assists_verified, assists_estimated, damage,'
            ' first_kill, first_death, survived'
            ' ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            player_round_rows,
        )

    def _round_id_for_frame(frame_idx: int) -> Optional[int]:
        if not round_ranges:
            return None
        f = int(frame_idx)
        for rid, start_f, end_f in round_ranges:
            if start_f <= f < end_f:
                return rid
        if f < round_ranges[0][1]:
            return round_ranges[0][0]
        return round_ranges[-1][0]

    if raw_round_events:
        round_event_rows = []
        for ev in raw_round_events:
            frame_idx = int(ev.get('frame', 0) or 0)
            event_type = str(ev.get('type', '') or '')
            payload = {k: v for k, v in ev.items() if k not in ('frame', 'type')}
            round_event_rows.append((
                match_id,
                _round_id_for_frame(frame_idx),
                frame_idx,
                event_type,
                json.dumps(payload, separators=(',', ':')),
            ))
        conn.executemany(
            'INSERT INTO round_events (match_id, round_id, frame_idx, event_type, event_payload)'
            ' VALUES (?,?,?,?,?)',
            round_event_rows,
        )

    raw_position_samples = data.get('position_samples') or []
    if raw_position_samples:
        pos_rows = []
        for s in raw_position_samples:
            frame_idx = int(s.get('frame', 0) or 0)
            pos_rows.append((
                match_id,
                _round_id_for_frame(frame_idx),
                frame_idx,
                str(s.get('name', '') or ''),
                int(s.get('team', 0) or 0),
                float(s.get('x', 0.0) or 0.0),
                float(s.get('y', 0.0) or 0.0),
                float(s.get('z', 0.0) or 0.0),
            ))
        conn.executemany(
            'INSERT INTO position_samples (match_id, round_id, frame_idx, name, team, x, y, z)'
            ' VALUES (?,?,?,?,?,?,?,?)',
            pos_rows,
        )

    raw_kill_points = data.get('kill_points') or []
    if raw_kill_points:
        kill_pos_rows = []
        for kp in raw_kill_points:
            frame_idx = int(kp.get('frame', 0) or 0)
            kill_pos_rows.append((
                match_id,
                _round_id_for_frame(frame_idx),
                frame_idx,
                str(kp.get('killer', '') or ''),
                str(kp.get('victim', '') or ''),
                str(kp.get('weapon', 'unknown') or 'unknown'),
                str(kp.get('location', 'unknown') or 'unknown'),
                1 if kp.get('team_kill') else 0,
                kp.get('killer_x'),
                kp.get('killer_y'),
                kp.get('killer_z'),
                kp.get('victim_x'),
                kp.get('victim_y'),
                kp.get('victim_z'),
            ))
        conn.executemany(
            'INSERT INTO kill_positions ('
            ' match_id, round_id, frame_idx, killer, victim, weapon, location, team_kill,'
            ' killer_x, killer_y, killer_z, victim_x, victim_y, victim_z'
            ' ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            kill_pos_rows,
        )
    # NOTE: caller is responsible for committing.


def _set_parse_status(conn: sqlite3.Connection,
                      path: str,
                      file_mtime: float,
                      status: str,
                      error_text: Optional[str] = None) -> None:
    conn.execute(
        'INSERT INTO parse_file_status (path, file_mtime, last_status, last_error, updated_at)'
        ' VALUES (?,?,?,?,?)'
        ' ON CONFLICT(path) DO UPDATE SET'
        ' file_mtime=excluded.file_mtime,'
        ' last_status=excluded.last_status,'
        ' last_error=excluded.last_error,'
        ' updated_at=excluded.updated_at',
        (
            path,
            float(file_mtime or 0.0),
            str(status or 'unknown'),
            (str(error_text)[:1000] if error_text else None),
            int(_time.time()),
        ),
    )


def _record_parse_error(conn: sqlite3.Connection,
                        path: str,
                        file_mtime: float,
                        error_text: str,
                        stage: str = 'parse') -> None:
    conn.execute(
        'INSERT INTO parse_errors (path, file_mtime, stage, error_text, occurred_at)'
        ' VALUES (?,?,?,?,?)',
        (
            path,
            float(file_mtime or 0.0),
            str(stage or 'parse'),
            str(error_text or 'unknown error')[:2000],
            int(_time.time()),
        ),
    )


def _index_file(conn: sqlite3.Connection, relpath: str, fullpath: str) -> None:
    """Parse one MVD2 file and upsert its stats.  No-op if mtime unchanged."""
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from parsers.mvd2 import load_mvd2

    mtime = os.path.getmtime(fullpath)
    row = conn.execute(
        'SELECT id, file_mtime FROM matches WHERE path=?', (relpath,)
    ).fetchone()
    if row and abs(row['file_mtime'] - mtime) < 0.01:
        return  # already up to date

    try:
        data = load_mvd2(fullpath, stats_only=True)
    except Exception as exc:
        log.debug('stats: skip %s: %s', relpath, exc)
        _record_parse_error(conn, relpath, mtime, str(exc), stage='parse')
        _set_parse_status(conn, relpath, mtime, 'error', str(exc))
        conn.commit()
        return

    _write_parsed(conn, relpath, mtime, data, is_update=bool(row))
    _set_parse_status(conn, relpath, mtime, 'ok', None)
    conn.commit()


# Keys from the parsed data dict that _write_parsed actually consumes.
# Everything else (hit_events, award_events, frames, chat, weapon_models,
# round_start_frames, etc.) is intermediate build data that would only bloat
# the pickle payload sent back from the worker process.
_WRITE_KEYS = frozenset({
    'map', 'duration', 'round_wins', 'team_scores', 'kills',
    'frame_count', 'round_start_frames', 'round_events',
    'position_samples', 'kill_points',
    'hit_events',
    'player_names', 'player_teams', 'kill_counts', 'death_counts',
    'team_kill_counts', 'hit_counts', 'shots_fired', 'damage_dealt',
    'accuracy', 'headshot_kills', 'hit_loc_by_player', 'award_counts',
    'best_kill_streak', 'players_with_frames', 'game_mode', 'game_mode_detail',
    'captains_by_team', 'captain_stats_by_team',
})


def _parse_file_worker(args: tuple) -> 'tuple[str, float, dict | None, Optional[str]]':
    """Top-level worker function for ProcessPoolExecutor.

    Runs in a separate process: parses one MVD2 file with stats_only=True and
    returns (relpath, mtime, data_dict, error_text).  Returns
    (relpath, mtime, None, error_text) on
    any parse error so the main process can log and skip it.
    Must be a module-level function so it is picklable.
    """
    import sys as _sys
    relpath, fullpath, web_dir = args
    _sys.path.insert(0, web_dir)
    from parsers.mvd2 import load_mvd2 as _load
    try:
        mtime = os.path.getmtime(fullpath)
        data  = _load(fullpath, stats_only=True)
        # Strip fields not needed by _write_parsed to reduce IPC pickle size.
        data = {k: v for k, v in data.items() if k in _WRITE_KEYS}
        return relpath, mtime, data, None
    except Exception as exc:
        try:
            mtime = os.path.getmtime(fullpath)
        except OSError:
            mtime = 0.0
        return relpath, mtime, None, str(exc)


def _run_indexer() -> None:
    import concurrent.futures
    global _running
    log.info('stats: indexer started')
    try:
        conn = _connect()
        # Snapshot current state to avoid re-querying every iteration
        mtimes = {r['path']: r['file_mtime']
                  for r in conn.execute('SELECT path, file_mtime FROM matches').fetchall()}

        # Collect files that need (re-)indexing
        to_index: list = []
        for dirpath, _dirs, files in os.walk(_mvd2_dir):
            for fname in sorted(files):
                if not (fname.endswith('.mvd2') or fname.endswith('.mvd2.gz')):
                    continue
                fullpath = os.path.join(dirpath, fname)
                relpath  = os.path.relpath(fullpath, _mvd2_dir).replace(os.sep, '/')
                try:
                    mtime = os.path.getmtime(fullpath)
                except OSError:
                    continue
                if abs(mtimes.get(relpath, -1) - mtime) < 0.01:
                    continue   # already indexed, same mtime → skip
                to_index.append((relpath, fullpath))

        if not to_index:
            # Fresh installs with an existing DB can have empty materialized
            # aggregate tables. Build them once even on no-op reindex.
            agg_counts = conn.execute('''
                SELECT
                    (SELECT COUNT(*) FROM weapon_stats_agg) AS weapon_rows,
                    (SELECT COUNT(*) FROM leaderboard_agg)  AS leaderboard_rows,
                    (SELECT COUNT(*) FROM map_stats_agg)    AS map_rows,
                    (SELECT COUNT(*) FROM summary_agg)      AS summary_rows,
                    (SELECT COUNT(*) FROM records_agg)      AS records_rows,
                    (SELECT COUNT(*) FROM activity_week_agg) AS activity_rows,
                    (SELECT COUNT(*) FROM recent_matches_agg) AS recent_rows,
                    (SELECT COUNT(*) FROM map_win_rates_agg)  AS mapwin_rows
            ''').fetchone()
            need_agg = (
                int(agg_counts['weapon_rows']) == 0 or
                int(agg_counts['leaderboard_rows']) == 0 or
                int(agg_counts['map_rows']) == 0 or
                int(agg_counts['summary_rows']) == 0 or
                int(agg_counts['records_rows']) == 0 or
                int(agg_counts['activity_rows']) == 0 or
                int(agg_counts['recent_rows']) == 0 or
                int(agg_counts['mapwin_rows']) == 0
            )
            _close_conn(conn)
            if need_agg:
                wconn = _connect_writer()
                _rebuild_materialized_aggregates(wconn)
                wconn.commit()
                _close_conn(wconn)
                _cache_clear()
                log.info('stats: rebuilt materialized aggregates (initial)')
            log.info('stats: indexer finished (nothing to do)')
            _prewarm_cache(force=True)
            return

        log.info('stats: %d files to index', len(to_index))
        web_dir = os.path.dirname(os.path.abspath(__file__))

        # Parse files in parallel processes (bypasses the GIL for CPU-bound work).
        # DB writes stay on this thread since SQLite only supports one writer.
        worker_count = min(os.cpu_count() or 2, 8)
        work_args = [(rel, full, web_dir) for rel, full in to_index]

        # Close the mtime-snapshot connection and open a write-optimised one.
        _close_conn(conn)
        conn = _connect_writer()

        # For bulk initial inserts, drop indexes first and recreate after.
        # SQLite must update every index on every INSERT; deferring them to a
        # single bulk build at the end is dramatically faster (~3-5x for
        # kill_events which has 4 indexes and many rows per file).
        is_initial = not bool(mtimes)   # True when DB was empty before this run
        if is_initial:
            # For one-time bootstrap loads, disable auto-checkpointing and
            # defer index maintenance until after inserts are done. WAL size is
            # still bounded by periodic PASSIVE checkpoints below.
            conn.execute('PRAGMA wal_autocheckpoint=0')
            conn.executescript("""
                DROP INDEX IF EXISTS idx_pms_name;
                DROP INDEX IF EXISTS idx_pms_match;
                DROP INDEX IF EXISTS idx_pms_match_name_team;
                DROP INDEX IF EXISTS idx_matches_map;
                DROP INDEX IF EXISTS idx_matches_time;
                DROP INDEX IF EXISTS idx_matches_mode;
                DROP INDEX IF EXISTS idx_matches_mode_detail;
                DROP INDEX IF EXISTS idx_ke_killer;
                DROP INDEX IF EXISTS idx_ke_victim;
                DROP INDEX IF EXISTS idx_ke_weapon;
                DROP INDEX IF EXISTS idx_ke_match;
                DROP INDEX IF EXISTS idx_ke_victim_tk;
                DROP INDEX IF EXISTS idx_ke_killer_tk;
                DROP INDEX IF EXISTS idx_ke_weapon_cover;
                DROP INDEX IF EXISTS idx_ke_weapon_killer_match;
                DROP INDEX IF EXISTS idx_ke_weapon_victim_match;
                DROP INDEX IF EXISTS idx_rounds_match;
                DROP INDEX IF EXISTS idx_rounds_winner;
                DROP INDEX IF EXISTS idx_round_events_match_frame;
                DROP INDEX IF EXISTS idx_round_events_round;
                DROP INDEX IF EXISTS idx_pos_samples_match_frame;
                DROP INDEX IF EXISTS idx_pos_samples_round;
                DROP INDEX IF EXISTS idx_pos_samples_name;
                DROP INDEX IF EXISTS idx_kill_pos_match_frame;
                DROP INDEX IF EXISTS idx_kill_pos_round;
                DROP INDEX IF EXISTS idx_kill_pos_killer;
                DROP INDEX IF EXISTS idx_prs_match;
                DROP INDEX IF EXISTS idx_prs_name;
                DROP INDEX IF EXISTS idx_prs_round;
                DROP INDEX IF EXISTS idx_parse_errors_path_time;
                DROP INDEX IF EXISTS idx_parse_status_state;
                DROP INDEX IF EXISTS idx_lb_kills;
                DROP INDEX IF EXISTS idx_lb_games;
                DROP INDEX IF EXISTS idx_mapagg_matches;
                DROP INDEX IF EXISTS idx_recent_played_at;
                DROP INDEX IF EXISTS idx_mapwin_matches;
            """)
            log.info('stats: bulk mode — indexes dropped, will recreate after indexing')

        # Commit every BATCH files, then close + reopen the connection.
        # Closing guarantees all locks are fully released, giving Flask
        # readers a clean window.  PASSIVE checkpoint between batches keeps
        # WAL size bounded without requiring exclusive access (TRUNCATE
        # blocks on readers and caused "database is locked" errors).
        BATCH = 1000 if is_initial else 200
        do_batch_checkpoint = not is_initial
        indexed = 0
        pending = 0
        batch_commits = 0
        failed: list = []   # (relpath, mtime, data, error_text) for retry
        chunk_size = 64 if is_initial else 16
        with concurrent.futures.ProcessPoolExecutor(max_workers=worker_count) as pool:
            for relpath, mtime, data, parse_error in pool.map(_parse_file_worker, work_args, chunksize=chunk_size):
                if data is None:
                    msg = str(parse_error or 'parse error')
                    log.debug('stats: skip %s (%s)', relpath, msg)
                    _record_parse_error(conn, relpath, mtime, msg, stage='parse')
                    _set_parse_status(conn, relpath, mtime, 'error', msg)
                    conn.commit()
                    continue
                try:
                    is_update = relpath in mtimes
                    _write_parsed(conn, relpath, mtime, data, is_update=is_update)
                    _set_parse_status(conn, relpath, mtime, 'ok', None)
                    indexed += 1
                    pending += 1
                    if pending >= BATCH:
                        conn.commit()
                        batch_commits += 1
                        if do_batch_checkpoint:
                            conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
                        elif (batch_commits % _INITIAL_WAL_CHECK_EVERY_BATCHES) == 0:
                            _maybe_passive_checkpoint(
                                conn,
                                min_wal_bytes=_INITIAL_WAL_SOFT_LIMIT_BYTES
                            )
                        pending = 0
                        log.info('stats: indexed %d / %d', indexed, len(to_index))
                except Exception as exc:
                    _safe_rollback(conn)
                    if _is_locked_error(exc):
                        log.info('stats: write lock %s: queued for retry', relpath)
                    else:
                        log.warning('stats: write error %s: %s', relpath, exc)
                    failed.append((relpath, mtime, data, str(exc)))

        if pending:
            conn.commit()
            if not do_batch_checkpoint:
                _maybe_passive_checkpoint(
                    conn,
                    min_wal_bytes=_INITIAL_WAL_SOFT_LIMIT_BYTES
                )

        # Retry any files that failed (e.g. transient lock contention).
        # Fresh connection after the pool is shut down — less contention.
        if failed:
            _close_conn(conn)
            conn = _connect_writer()
            log.info('stats: retrying %d failed files', len(failed))
            retried = 0
            for relpath, mtime, data, first_error in failed:
                is_update = relpath in mtimes
                for attempt in range(1, 4):
                    try:
                        _write_parsed(conn, relpath, mtime, data, is_update=is_update)
                        _set_parse_status(conn, relpath, mtime, 'ok', None)
                        retried += 1
                        break
                    except Exception as exc:
                        _safe_rollback(conn)
                        if _is_locked_error(exc) and attempt < 3:
                            _time.sleep(0.15 * attempt)
                            continue
                        log.warning('stats: retry failed %s: %s', relpath, exc)
                        final_error = str(exc or first_error or 'write error')
                        _record_parse_error(conn, relpath, mtime, final_error, stage='write')
                        _set_parse_status(conn, relpath, mtime, 'error', final_error)
                        conn.commit()
                        break
            if retried:
                conn.commit()
                if not do_batch_checkpoint:
                    _maybe_passive_checkpoint(
                        conn,
                        min_wal_bytes=_INITIAL_WAL_SOFT_LIMIT_BYTES
                    )
                log.info('stats: retry recovered %d / %d files', retried, len(failed))

        if is_initial:
            log.info('stats: recreating base indexes…')
            conn.executescript("""
                CREATE INDEX IF NOT EXISTS idx_pms_name     ON player_match_stats(name);
                CREATE INDEX IF NOT EXISTS idx_pms_match    ON player_match_stats(match_id);
                CREATE INDEX IF NOT EXISTS idx_pms_match_name_team ON player_match_stats(match_id, name, team);
                CREATE INDEX IF NOT EXISTS idx_matches_map  ON matches(map);
                CREATE INDEX IF NOT EXISTS idx_matches_time ON matches(played_at);
                CREATE INDEX IF NOT EXISTS idx_matches_mode ON matches(game_mode);
                CREATE INDEX IF NOT EXISTS idx_matches_mode_detail ON matches(game_mode_detail);
                CREATE INDEX IF NOT EXISTS idx_ke_killer    ON kill_events(killer);
                CREATE INDEX IF NOT EXISTS idx_ke_victim    ON kill_events(victim);
                CREATE INDEX IF NOT EXISTS idx_ke_weapon    ON kill_events(weapon);
                CREATE INDEX IF NOT EXISTS idx_ke_match     ON kill_events(match_id);
                CREATE INDEX IF NOT EXISTS idx_ke_victim_tk    ON kill_events(victim, team_kill);
                CREATE INDEX IF NOT EXISTS idx_ke_killer_tk    ON kill_events(killer, team_kill);
                CREATE INDEX IF NOT EXISTS idx_ke_weapon_cover ON kill_events(team_kill, weapon, killer, location);
                CREATE INDEX IF NOT EXISTS idx_ke_weapon_killer_match ON kill_events(team_kill, weapon, killer, match_id, location);
                CREATE INDEX IF NOT EXISTS idx_ke_weapon_victim_match ON kill_events(team_kill, weapon, victim, match_id, location);
                CREATE INDEX IF NOT EXISTS idx_rounds_match     ON rounds(match_id);
                CREATE INDEX IF NOT EXISTS idx_rounds_winner    ON rounds(winner_team);
                CREATE INDEX IF NOT EXISTS idx_round_events_match_frame ON round_events(match_id, frame_idx);
                CREATE INDEX IF NOT EXISTS idx_round_events_round       ON round_events(round_id);
                CREATE INDEX IF NOT EXISTS idx_pos_samples_match_frame  ON position_samples(match_id, frame_idx);
                CREATE INDEX IF NOT EXISTS idx_pos_samples_round        ON position_samples(round_id);
                CREATE INDEX IF NOT EXISTS idx_pos_samples_name         ON position_samples(name);
                CREATE INDEX IF NOT EXISTS idx_kill_pos_match_frame     ON kill_positions(match_id, frame_idx);
                CREATE INDEX IF NOT EXISTS idx_kill_pos_round           ON kill_positions(round_id);
                CREATE INDEX IF NOT EXISTS idx_kill_pos_killer          ON kill_positions(killer);
                CREATE INDEX IF NOT EXISTS idx_prs_match        ON player_round_stats(match_id);
                CREATE INDEX IF NOT EXISTS idx_prs_name         ON player_round_stats(name);
                CREATE INDEX IF NOT EXISTS idx_prs_round        ON player_round_stats(round_id);
                CREATE INDEX IF NOT EXISTS idx_parse_errors_path_time   ON parse_errors(path, occurred_at DESC);
                CREATE INDEX IF NOT EXISTS idx_parse_status_state       ON parse_file_status(last_status);
            """)
            log.info('stats: base indexes recreated')

        # Keep request path fast: precompute heavy materialized aggregates.
        _rebuild_materialized_aggregates(conn)
        conn.commit()

        if is_initial:
            log.info('stats: recreating aggregate indexes…')
            conn.executescript("""
                CREATE INDEX IF NOT EXISTS idx_lb_kills        ON leaderboard_agg(kills DESC);
                CREATE INDEX IF NOT EXISTS idx_lb_games        ON leaderboard_agg(games);
                CREATE INDEX IF NOT EXISTS idx_mapagg_matches  ON map_stats_agg(matches DESC);
                CREATE INDEX IF NOT EXISTS idx_recent_played_at ON recent_matches_agg(played_at DESC);
                CREATE INDEX IF NOT EXISTS idx_mapwin_matches   ON map_win_rates_agg(matches DESC);
            """)
            conn.commit()
            log.info('stats: aggregate indexes recreated')

        # Final checkpoint: PASSIVE moves what it can without blocking,
        # then attempt TRUNCATE to reset WAL file size.
        try:
            conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        except Exception:
            pass  # TRUNCATE failed (reader open) — WAL stays larger, harmless
        _close_conn(conn)
        if indexed > 0:
            _cache_clear()
        log.info('stats: indexer finished (%d files indexed)', indexed)
        _prewarm_cache(force=True)
    except Exception as exc:
        log.error('stats: indexer crashed: %s', exc)
    finally:
        with _lock:
            _running = False


# ── Query API ──────────────────────────────────────────────────────────────────

def get_leaderboard(min_games: int = 1, limit: int = 300,
                    mode: str = _DEFAULT_STATS_MODE,
                    period: str = _DEFAULT_H2H_PERIOD,
                    week: Optional[str] = None) -> list:
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    week_clause, week_params, normalized_week = _week_filter_clause(week)
    cache_key = f'leaderboard:{mode}:{normalized_period}:{normalized_week or "all_weeks"}:{min_games}:{limit}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        if mode == _DEFAULT_STATS_MODE and normalized_period == 'all' and normalized_week is None:
            rows = conn.execute('''
                SELECT name, games, kills, deaths, team_kills, hits, shots, damage,
                       hs_kills, best_game, kd, avg_kills, avg_acc,
                       h_head, h_stomach, h_chest, h_legs,
                       awards_impressive, awards_accuracy, awards_excellent,
                       rounds_played, damage_per_round, impact_score
                FROM leaderboard_agg
                WHERE games >= ?
                ORDER BY kills DESC
                LIMIT ?
            ''', (min_games, limit)).fetchall()
        else:
            mode_clause, mode_params = _mode_filter_clause(mode)
            period_clause_mm, period_params_mm, _ = _h2h_period_filter_clause(
                normalized_period, played_at_col='mm.played_at')
            week_clause_mm, week_params_mm, _ = _week_filter_clause(
                normalized_week, played_at_col='mm.played_at')
            rows = conn.execute(f'''
                SELECT
                    p.name,
                    COUNT(DISTINCT p.match_id)                                    AS games,
                    SUM(p.kills)                                                  AS kills,
                    SUM(p.deaths)                                                 AS deaths,
                    SUM(p.team_kills)                                             AS team_kills,
                    SUM(p.hits)                                                   AS hits,
                    SUM(p.shots)                                                  AS shots,
                    SUM(p.damage)                                                 AS damage,
                    SUM(p.hs_kills)                                               AS hs_kills,
                    MAX(p.kills)                                                  AS best_game,
                    ROUND(CAST(SUM(p.kills) AS REAL) / MAX(SUM(p.deaths),1), 2)  AS kd,
                    ROUND(CAST(SUM(p.kills) AS REAL) / COUNT(DISTINCT p.match_id), 1) AS avg_kills,
                    ROUND(AVG(CASE WHEN p.accuracy IS NOT NULL THEN p.accuracy END), 1) AS avg_acc,
                    SUM(p.hits_head)                                              AS h_head,
                    SUM(p.hits_stomach)                                           AS h_stomach,
                    SUM(p.hits_chest)                                             AS h_chest,
                    SUM(p.hits_legs)                                              AS h_legs,
                    SUM(p.awards_impressive)                                      AS awards_impressive,
                    SUM(p.awards_accuracy)                                        AS awards_accuracy,
                    SUM(p.awards_excellent)                                       AS awards_excellent,
                    COALESCE(rp.rounds_played, 0)                                 AS rounds_played,
                    CASE WHEN COALESCE(rp.rounds_played, 0) > 0
                         THEN ROUND(CAST(rp.dmg_sum AS REAL) / rp.rounds_played, 2)
                         ELSE 0 END                                               AS damage_per_round,
                    CASE WHEN COALESCE(rp.rounds_played, 0) > 0
                         THEN ROUND(
                             (
                                 (rp.kill_sum * 100.0) +
                                 (rp.assist_v * 60.0) +
                                 (rp.assist_e * 30.0) +
                                 (rp.dmg_sum * 0.20) +
                                 (rp.fk_sum * 40.0) +
                                 (rp.surv_sum * 20.0) -
                                 (rp.death_sum * 35.0)
                             ) / rp.rounds_played,
                             2
                         )
                         ELSE 0 END                                               AS impact_score
                FROM player_match_stats p
                JOIN matches m ON m.id = p.match_id
                LEFT JOIN (
                    SELECT prs.name,
                           COUNT(*)                   AS rounds_played,
                           SUM(prs.kills)             AS kill_sum,
                           SUM(prs.deaths)            AS death_sum,
                           SUM(prs.assists_verified)  AS assist_v,
                           SUM(prs.assists_estimated) AS assist_e,
                           SUM(prs.damage)            AS dmg_sum,
                           SUM(prs.first_kill)        AS fk_sum,
                           SUM(prs.survived)          AS surv_sum
                    FROM player_round_stats prs
                    JOIN matches mm ON mm.id = prs.match_id
                    WHERE mm.game_mode = 'tdm' AND {period_clause_mm} AND {week_clause_mm}
                    GROUP BY prs.name
                ) rp ON rp.name = p.name
                WHERE (p.kills > 0 OR p.deaths > 0) AND {mode_clause} AND {period_clause} AND {week_clause}
                GROUP BY p.name
                HAVING games >= ?
                ORDER BY kills DESC
                LIMIT ?
            ''', period_params_mm + week_params_mm + mode_params + period_params + week_params + (min_games, limit)).fetchall()
    result = [dict(r) for r in rows]
    _cache_set(cache_key, result)
    return result


def get_map_stats(mode: str = _DEFAULT_STATS_MODE,
                  period: str = _DEFAULT_H2H_PERIOD,
                  week: Optional[str] = None) -> list:
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    week_clause, week_params, normalized_week = _week_filter_clause(week)
    cache_key = f'map_stats:{mode}:{normalized_period}:{normalized_week or "all_weeks"}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        if mode == _DEFAULT_STATS_MODE and normalized_period == 'all' and normalized_week is None:
            rows = conn.execute('''
                WITH round_stats AS (
                    SELECT mr.map,
                           ROUND(AVG(r.duration_seconds), 1) AS avg_round_seconds,
                           ROUND(100.0 * SUM(CASE WHEN r.first_kill_team > 0
                                                   AND r.first_kill_team = r.winner_team
                                                   AND r.is_tie = 0 THEN 1 ELSE 0 END)
                                       / MAX(SUM(CASE WHEN r.first_kill_team > 0
                                                       AND r.winner_team > 0
                                                       AND r.is_tie = 0 THEN 1 ELSE 0 END), 1),
                                 1) AS fk_win_rate
                    FROM rounds r
                    JOIN matches mr ON mr.id = r.match_id
                    WHERE mr.game_mode_detail = 'teamplay'
                    GROUP BY mr.map
                )
                SELECT a.map, a.matches, a.kills, a.avg_kills, a.players,
                       a.avg_duration_min, a.top_killer,
                       rs.avg_round_seconds, rs.fk_win_rate
                FROM map_stats_agg a
                LEFT JOIN round_stats rs ON rs.map = a.map
                ORDER BY a.matches DESC
            ''').fetchall()
        else:
            mode_clause, mode_params = _mode_filter_clause(mode)
            period_clause_m2, period_params_m2, _ = _h2h_period_filter_clause(
                normalized_period,
                played_at_col='m2.played_at',
            )
            week_clause_m2, week_params_m2, _ = _week_filter_clause(
                normalized_week,
                played_at_col='m2.played_at',
            )
            mode_clause_mr, mode_params_mr = _mode_filter_clause(
                mode,
                detail_col='mr.game_mode_detail',
                bucket_col='mr.game_mode',
            )
            period_clause_mr, period_params_mr, _ = _h2h_period_filter_clause(
                normalized_period,
                played_at_col='mr.played_at',
            )
            week_clause_mr, week_params_mr, _ = _week_filter_clause(
                normalized_week,
                played_at_col='mr.played_at',
            )
            rows = conn.execute(f'''
                WITH map_top AS (
                    SELECT m2.map, p2.name,
                           ROW_NUMBER() OVER (
                               PARTITION BY m2.map ORDER BY SUM(p2.kills) DESC
                           ) AS rn
                    FROM player_match_stats p2
                    JOIN matches m2 ON m2.id = p2.match_id
                    WHERE {mode_clause.replace('m.', 'm2.')} AND {period_clause_m2} AND {week_clause_m2}
                    GROUP BY m2.map, p2.name
                ),
                round_stats AS (
                    SELECT mr.map,
                           ROUND(AVG(r.duration_seconds), 1) AS avg_round_seconds,
                           ROUND(100.0 * SUM(CASE WHEN r.first_kill_team > 0
                                                   AND r.first_kill_team = r.winner_team
                                                   AND r.is_tie = 0 THEN 1 ELSE 0 END)
                                       / MAX(SUM(CASE WHEN r.first_kill_team > 0
                                                       AND r.winner_team > 0
                                                       AND r.is_tie = 0 THEN 1 ELSE 0 END), 1),
                                 1) AS fk_win_rate
                    FROM rounds r
                    JOIN matches mr ON mr.id = r.match_id
                    WHERE {mode_clause_mr} AND {period_clause_mr} AND {week_clause_mr}
                    GROUP BY mr.map
                )
                SELECT
                    m.map,
                    COUNT(DISTINCT m.id)        AS matches,
                    SUM(m.total_kills)          AS kills,
                    ROUND(CAST(SUM(m.total_kills) AS REAL) / COUNT(DISTINCT m.id), 1) AS avg_kills,
                    COUNT(DISTINCT p.name)      AS players,
                    ROUND(AVG(m.duration)/60,1) AS avg_duration_min,
                    mt.name                     AS top_killer,
                    rs.avg_round_seconds,
                    rs.fk_win_rate
                FROM matches m
                LEFT JOIN player_match_stats p ON p.match_id = m.id
                LEFT JOIN map_top mt ON mt.map = m.map AND mt.rn = 1
                LEFT JOIN round_stats rs ON rs.map = m.map
                WHERE m.map != '' AND {mode_clause} AND {period_clause} AND {week_clause}
                GROUP BY m.map
                ORDER BY matches DESC
            ''', mode_params + period_params_m2 + week_params_m2
                + mode_params_mr + period_params_mr + week_params_mr
                + mode_params + period_params + week_params).fetchall()
    result = [dict(r) for r in rows]
    _cache_set(cache_key, result)
    return result


def get_summary(mode: str = _DEFAULT_STATS_MODE,
                period: str = _DEFAULT_H2H_PERIOD,
                week: Optional[str] = None) -> dict:
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    week_clause, week_params, normalized_week = _week_filter_clause(week)
    cache_key = f'summary:{mode}:{normalized_period}:{normalized_week or "all_weeks"}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        if mode == _DEFAULT_STATS_MODE and normalized_period == 'all' and normalized_week is None:
            r = conn.execute('''
                SELECT
                    total_matches,
                    total_players,
                    total_kills,
                    total_damage,
                    best_game_kills,
                    total_maps
                FROM summary_agg
                WHERE id = 1
            ''').fetchone()
        else:
            mode_clause, mode_params = _mode_filter_clause(mode)
            r = conn.execute(f'''
                SELECT
                    COUNT(DISTINCT m.id)    AS total_matches,
                    COUNT(DISTINCT p.name)  AS total_players,
                    SUM(m.total_kills)      AS total_kills,
                    SUM(p.damage)           AS total_damage,
                    MAX(p.kills)            AS best_game_kills,
                    COUNT(DISTINCT m.map)   AS total_maps
                FROM matches m
                LEFT JOIN player_match_stats p ON p.match_id = m.id
                WHERE {mode_clause} AND {period_clause} AND {week_clause}
            ''', mode_params + period_params + week_params).fetchone()
    result = dict(r) if r else {}
    _cache_set(cache_key, result)
    return result


def get_recent_matches(limit: int = 20,
                       mode: str = _DEFAULT_STATS_MODE,
                       period: str = _DEFAULT_H2H_PERIOD,
                       week: Optional[str] = None) -> list:
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    week_clause, week_params, normalized_week = _week_filter_clause(week)
    cache_key = f'recent_matches:{mode}:{normalized_period}:{normalized_week or "all_weeks"}:{limit}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        if mode == _DEFAULT_STATS_MODE and normalized_period == 'all' and normalized_week is None:
            rows = conn.execute('''
                SELECT
                    path, map, played_at, duration,
                    total_kills, t1_rounds, t2_rounds, game_mode, game_mode_detail, player_count
                FROM recent_matches_agg
                WHERE game_mode_detail = ?
                ORDER BY played_at DESC
                LIMIT ?
            ''', (_DEFAULT_STATS_MODE, limit)).fetchall()
        else:
            mode_clause, mode_params = _mode_filter_clause(mode)
            rows = conn.execute(f'''
                SELECT
                    m.path, m.map, m.played_at, m.duration,
                    m.total_kills, m.t1_rounds, m.t2_rounds, m.game_mode, m.game_mode_detail,
                    COUNT(p.id) AS player_count
                FROM matches m
                LEFT JOIN player_match_stats p ON p.match_id = m.id
                WHERE {mode_clause} AND {period_clause} AND {week_clause}
                GROUP BY m.id
                ORDER BY m.played_at DESC
                LIMIT ?
            ''', mode_params + period_params + week_params + (limit,)).fetchall()
    result = [dict(r) for r in rows]
    _cache_set(cache_key, result)
    return result


def get_index_status() -> dict:
    cached_total = _cache_get('_file_total')
    if cached_total is None:
        total = 0
        if _mvd2_dir and os.path.isdir(_mvd2_dir):
            for _dp, _dd, files in os.walk(_mvd2_dir):
                for f in files:
                    if f.endswith('.mvd2') or f.endswith('.mvd2.gz'):
                        total += 1
        _cache_set('_file_total', total)
        cached_total = total
    with _connect() as conn:
        indexed = conn.execute('SELECT COUNT(*) FROM matches').fetchone()[0]
        parse_failed_files = conn.execute(
            "SELECT COUNT(*) FROM parse_file_status WHERE last_status = 'error'"
        ).fetchone()[0]
        parse_errors_total = conn.execute('SELECT COUNT(*) FROM parse_errors').fetchone()[0]
    return {
        'total': cached_total,
        'indexed': indexed,
        'running': _running,
        'parse_failed_files': int(parse_failed_files or 0),
        'parse_errors_total': int(parse_errors_total or 0),
    }


# ── Records ────────────────────────────────────────────────────────────────────

def get_records(mode: str = _DEFAULT_STATS_MODE,
                period: str = _DEFAULT_H2H_PERIOD,
                week: Optional[str] = None) -> dict:
    """Single-game record holders."""
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    week_clause, week_params, normalized_week = _week_filter_clause(week)
    cache_key = f'records:{mode}:{normalized_period}:{normalized_week or "all_weeks"}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        if mode == _DEFAULT_STATS_MODE and normalized_period == 'all' and normalized_week is None:
            rows = conn.execute('SELECT key, payload FROM records_agg').fetchall()
            result = {r['key']: json.loads(r['payload']) for r in rows}
            _cache_set(cache_key, result)
            return result

        mode_clause, mode_params = _mode_filter_clause(mode)

        def _one(sql, params=()):
            r = conn.execute(
                sql.format(mode_clause=mode_clause, period_clause=period_clause, week_clause=week_clause),
                params + mode_params + period_params + week_params,
            ).fetchone()
            return dict(r) if r else None

        most_kills = _one('''
            SELECT p.name, p.kills AS value, m.path, m.map,
                   m.played_at, p.match_id
            FROM player_match_stats p JOIN matches m ON m.id = p.match_id
                 WHERE {mode_clause} AND {period_clause} AND {week_clause}
            ORDER BY p.kills DESC LIMIT 1
        ''')
        best_kd = _one('''
            SELECT p.name,
                   ROUND(CAST(p.kills AS REAL) / MAX(p.deaths,1), 2) AS value,
                   p.kills, p.deaths, m.path, m.map, m.played_at, p.match_id
            FROM player_match_stats p JOIN matches m ON m.id = p.match_id
                 WHERE p.kills >= 3 AND {mode_clause} AND {period_clause} AND {week_clause}
            ORDER BY CAST(p.kills AS REAL) / MAX(p.deaths,1) DESC LIMIT 1
        ''')
        best_acc = _one('''
            SELECT p.name, p.accuracy AS value, m.path, m.map,
                   m.played_at, p.match_id
            FROM player_match_stats p JOIN matches m ON m.id = p.match_id
                 WHERE p.accuracy IS NOT NULL AND p.shots >= 5 AND {mode_clause} AND {period_clause} AND {week_clause}
            ORDER BY p.accuracy DESC LIMIT 1
        ''')
        most_damage = _one('''
            SELECT p.name, p.damage AS value, m.path, m.map,
                   m.played_at, p.match_id
            FROM player_match_stats p JOIN matches m ON m.id = p.match_id
                 WHERE {mode_clause} AND {period_clause} AND {week_clause}
            ORDER BY p.damage DESC LIMIT 1
        ''')
        most_hs = _one('''
            SELECT p.name, p.hs_kills AS value, m.path, m.map,
                   m.played_at, p.match_id
            FROM player_match_stats p JOIN matches m ON m.id = p.match_id
                 WHERE {mode_clause} AND {period_clause} AND {week_clause}
            ORDER BY p.hs_kills DESC LIMIT 1
        ''')
        longest_match = _one('''
            SELECT m.map, m.path, m.played_at,
                   ROUND(m.duration / 60.0, 1) AS value
            FROM matches m
                 WHERE {mode_clause} AND {period_clause} AND {week_clause}
            ORDER BY m.duration DESC LIMIT 1
        ''')
    result = {
        'most_kills':    most_kills,
        'best_kd':       best_kd,
        'best_acc':      best_acc,
        'most_damage':   most_damage,
        'most_hs':       most_hs,
        'longest_match': longest_match,
    }
    _cache_set(cache_key, result)
    return result


# ── Activity timeline ──────────────────────────────────────────────────────────

def get_activity_by_week(mode: str = _DEFAULT_STATS_MODE,
                         period: str = _DEFAULT_H2H_PERIOD,
                         week: Optional[str] = None) -> list:
    """Matches played per calendar week (ISO YYYY-WW), last 52 weeks."""
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='played_at',
    )
    cache_key = f'activity_week:{mode}:{normalized_period}:{normalized_week or "all_weeks"}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        if mode == _DEFAULT_STATS_MODE and normalized_period == 'all' and normalized_week is None:
            rows = conn.execute('''
                SELECT week, matches, kills
                FROM activity_week_agg
                ORDER BY week ASC
            ''').fetchall()
        else:
            mode_clause, mode_params = _mode_filter_clause(mode, detail_col='game_mode_detail', bucket_col='game_mode')
            rows = conn.execute(f'''
                SELECT strftime('%Y-%W', datetime(played_at, 'unixepoch')) AS week,
                       COUNT(*) AS matches,
                       SUM(total_kills) AS kills
                FROM matches
                WHERE {mode_clause} AND {period_clause} AND {week_clause}
                GROUP BY week
                ORDER BY week ASC
            ''', mode_params + period_params + week_params).fetchall()
    result = [dict(r) for r in rows]
    _cache_set(cache_key, result)
    return result


# ── Player profile ─────────────────────────────────────────────────────────────

def get_player_summary(name: str,
                       period: str = _DEFAULT_H2H_PERIOD,
                       week: Optional[str] = None) -> dict:
    """Aggregated career stats for a single player."""
    period_clause, period_params, _ = _h2h_period_filter_clause(period)
    week_clause, week_params, _ = _week_filter_clause(week)
    with _connect() as conn:
        identities = _resolve_identity_variants(conn, name)
        name_clause, name_params = _in_clause('p.name', identities)
        r = conn.execute(f'''
            SELECT
                COUNT(DISTINCT p.match_id)                                     AS games,
                SUM(p.kills)                                                   AS kills,
                SUM(p.deaths)                                                  AS deaths,
                SUM(p.team_kills)                                              AS team_kills,
                SUM(p.damage)                                                  AS damage,
                SUM(p.hs_kills)                                                AS hs_kills,
                MAX(p.kills)                                                   AS best_game,
                ROUND(CAST(SUM(p.kills) AS REAL) / MAX(SUM(p.deaths),1), 2)   AS kd,
                ROUND(AVG(CASE WHEN p.accuracy IS NOT NULL THEN p.accuracy END), 1) AS avg_acc,
                SUM(p.hits_head)                                               AS h_head,
                SUM(p.hits_stomach)                                            AS h_stomach,
                SUM(p.hits_chest)                                              AS h_chest,
                SUM(p.hits_legs)                                               AS h_legs,
                MIN(m.played_at)                                               AS first_seen,
                MAX(m.played_at)                                               AS last_seen,
                COUNT(DISTINCT m.map)                                          AS unique_maps,
                SUM(m.duration)                                                AS total_playtime,
                ROUND(CAST(SUM(p.damage) AS REAL) / MAX(COUNT(DISTINCT p.match_id),1), 0) AS avg_damage,
                CASE WHEN SUM(m.duration) > 0
                     THEN ROUND(CAST(SUM(p.kills) AS REAL) / (SUM(m.duration) / 60.0), 2)
                     ELSE NULL END                                             AS frags_per_min,
                SUM(p.awards_impressive)                                       AS awards_impressive,
                SUM(p.awards_accuracy)                                         AS awards_accuracy,
                SUM(p.awards_excellent)                                        AS awards_excellent,
                MAX(p.best_kill_streak)                                        AS best_kill_streak,
                SUM(CASE WHEN p.team>0 AND (m.t1_rounds+m.t2_rounds)>0
                          AND ((p.team=1 AND m.t1_rounds>m.t2_rounds) OR
                               (p.team=2 AND m.t2_rounds>m.t1_rounds))
                         THEN 1 ELSE 0 END)                                    AS wins,
                SUM(CASE WHEN p.team>0 AND (m.t1_rounds+m.t2_rounds)>0
                          AND m.t1_rounds=m.t2_rounds
                         THEN 1 ELSE 0 END)                                    AS draws,
                SUM(CASE WHEN p.team>0 AND (m.t1_rounds+m.t2_rounds)>0
                         THEN 1 ELSE 0 END)                                    AS tracked_games
            FROM player_match_stats p
            JOIN matches m ON m.id = p.match_id
            WHERE {name_clause} AND m.game_mode = 'tdm' AND {period_clause} AND {week_clause}
        ''', name_params + period_params + week_params).fetchone()
    return dict(r) if r else {}


def get_player_round_metrics(name: str,
                             period: str = _DEFAULT_H2H_PERIOD,
                             week: Optional[str] = None) -> dict:
    """Round-normalized analytics for a single player."""
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    week_clause, week_params, normalized_week = _week_filter_clause(week)
    cache_key = f'player_round_metrics:{name}:{normalized_period}:{normalized_week or "all_weeks"}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        identities = _resolve_identity_variants(conn, name)
        name_clause, name_params = _in_clause('prs.name', identities)
        r = conn.execute(f'''
            SELECT
                COUNT(DISTINCT prs.match_id) AS matches,
                COUNT(*) AS rounds,
                COALESCE(SUM(prs.kills), 0) AS kills,
                COALESCE(SUM(prs.deaths), 0) AS deaths,
                COALESCE(SUM(prs.assists_verified), 0) AS assists_verified,
                COALESCE(SUM(prs.assists_estimated), 0) AS assists_estimated,
                COALESCE(SUM(prs.damage), 0) AS damage,
                COALESCE(SUM(prs.survived), 0) AS rounds_survived,
                COALESCE(SUM(prs.first_kill), 0) AS first_kills,
                COALESCE(SUM(prs.first_death), 0) AS first_deaths,
                COALESCE(SUM(
                    CASE
                        WHEN prs.team > 0 AND r.winner_team > 0 AND prs.team = r.winner_team
                        THEN 1 ELSE 0
                    END
                ), 0) AS rounds_won,
                COALESCE(SUM(CASE WHEN prs.team > 0 AND r.is_tie = 1 THEN 1 ELSE 0 END), 0) AS rounds_tied,
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(CAST(SUM(prs.kills) AS REAL) / COUNT(*), 2)
                     ELSE 0 END AS kills_per_round,
                 CASE WHEN COUNT(*) > 0
                     THEN ROUND(CAST(SUM(prs.assists_verified + prs.assists_estimated) AS REAL) / COUNT(*), 2)
                     ELSE 0 END AS assists_per_round,
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(CAST(SUM(prs.damage) AS REAL) / COUNT(*), 2)
                     ELSE 0 END AS avg_damage_per_round,
                 CASE WHEN COUNT(DISTINCT prs.match_id) > 0
                     THEN ROUND(CAST(SUM(prs.kills) AS REAL) / COUNT(DISTINCT prs.match_id), 2)
                     ELSE 0 END AS kills_per_match,
                 CASE WHEN COUNT(DISTINCT prs.match_id) > 0
                     THEN ROUND(CAST(SUM(prs.deaths) AS REAL) / COUNT(DISTINCT prs.match_id), 2)
                     ELSE 0 END AS deaths_per_match,
                 CASE WHEN COUNT(DISTINCT prs.match_id) > 0
                     THEN ROUND(CAST(SUM(prs.assists_verified + prs.assists_estimated) AS REAL) / COUNT(DISTINCT prs.match_id), 2)
                     ELSE 0 END AS assists_per_match,
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(100.0 * SUM(prs.survived) / COUNT(*), 1)
                     ELSE 0 END AS survival_rate,
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(100.0 * SUM(prs.first_kill) / COUNT(*), 1)
                     ELSE 0 END AS first_kill_rate,
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(100.0 * SUM(prs.first_death) / COUNT(*), 1)
                     ELSE 0 END AS first_death_rate,
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(
                         100.0 * SUM(
                             CASE
                                 WHEN prs.team > 0 AND r.winner_team > 0 AND prs.team = r.winner_team
                                 THEN 1 ELSE 0
                             END
                         ) / COUNT(*), 1
                     )
                     ELSE 0 END AS round_win_rate,
                CASE
                    WHEN SUM(
                        CASE
                            WHEN prs.first_kill = 1 AND prs.team > 0 AND r.winner_team > 0
                            THEN 1 ELSE 0
                        END
                    ) > 0
                    THEN ROUND(
                        100.0 * SUM(
                            CASE
                                WHEN prs.first_kill = 1
                                     AND prs.team > 0
                                     AND r.winner_team > 0
                                     AND prs.team = r.winner_team
                                THEN 1 ELSE 0
                            END
                        ) / SUM(
                            CASE
                                WHEN prs.first_kill = 1 AND prs.team > 0 AND r.winner_team > 0
                                THEN 1 ELSE 0
                            END
                        ),
                        1
                    )
                    ELSE 0
                END AS first_kill_conversion_rate,
                COALESCE(SUM(
                    CASE
                        WHEN prs.team > 0
                             AND r.winner_team > 0
                             AND prs.team = r.winner_team
                             AND (
                                 SELECT COUNT(*) FROM player_round_stats prs2
                                 WHERE prs2.round_id = prs.round_id AND prs2.team = prs.team AND prs2.survived = 1
                             ) < (
                                 SELECT COUNT(*) FROM player_round_stats prs3
                                 WHERE prs3.round_id = prs.round_id AND prs3.team != prs.team AND prs3.team > 0 AND prs3.survived = 1
                             )
                        THEN 1 ELSE 0
                    END
                ), 0) AS clutch_wins,
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(
                         (
                             (SUM(prs.kills) * 100.0) +
                             (SUM(prs.assists_verified) * 60.0) +
                             (SUM(prs.assists_estimated) * 30.0) +
                             (SUM(prs.damage) * 0.20) +
                             (SUM(prs.first_kill) * 40.0) +
                             (SUM(prs.survived) * 20.0) -
                             (SUM(prs.deaths) * 35.0)
                         ) / COUNT(*),
                         2
                     )
                     ELSE 0
                END AS impact_score
            FROM player_round_stats prs
            JOIN rounds r ON r.id = prs.round_id
            JOIN matches m ON m.id = prs.match_id
            WHERE {name_clause} AND m.game_mode = 'tdm' AND {period_clause} AND {week_clause}
        ''', name_params + period_params + week_params).fetchone()
    result = dict(r) if r else {}
    if result:
        result['assists_total'] = int(result.get('assists_verified') or 0) + int(result.get('assists_estimated') or 0)
        rounds_won = int(result.get('rounds_won') or 0)
        result['clutch_rate'] = round(100.0 * int(result.get('clutch_wins') or 0) / rounds_won, 1) if rounds_won else 0
    _cache_set(cache_key, result)
    return result


def get_player_match_history(name: str,
                             limit: int = 50,
                             offset: int = 0,
                             period: str = _DEFAULT_H2H_PERIOD,
                             week: Optional[str] = None) -> list:
    """Per-match stats for a player, newest first."""
    period_clause, period_params, _ = _h2h_period_filter_clause(period)
    week_clause, week_params, _ = _week_filter_clause(week)
    with _connect() as conn:
        identities = _resolve_identity_variants(conn, name)
        name_clause, name_params = _in_clause('p.name', identities)
        rows = conn.execute(f'''
            SELECT p.kills, p.deaths, p.team_kills, p.damage, p.accuracy,
                   p.hs_kills, p.hits_head, p.hits_stomach, p.hits_chest, p.hits_legs,
                   p.awards_impressive, p.awards_accuracy, p.awards_excellent,
                   p.best_kill_streak, p.team,
                   m.path, m.map, m.played_at, m.t1_rounds, m.t2_rounds, m.total_kills,
                   m.duration, m.game_mode
            FROM player_match_stats p
            JOIN matches m ON m.id = p.match_id
            WHERE {name_clause} AND {period_clause} AND {week_clause}
            ORDER BY m.played_at DESC
            LIMIT ? OFFSET ?
        ''', name_params + period_params + week_params + (limit, offset)).fetchall()
    return [dict(r) for r in rows]


def get_player_map_breakdown(name: str,
                             period: str = _DEFAULT_H2H_PERIOD,
                             week: Optional[str] = None) -> list:
    """Per-map aggregated stats for a player."""
    period_clause, period_params, _ = _h2h_period_filter_clause(period)
    week_clause, week_params, _ = _week_filter_clause(week)
    with _connect() as conn:
        identities = _resolve_identity_variants(conn, name)
        name_clause, name_params = _in_clause('p.name', identities)
        rows = conn.execute(f'''
            SELECT m.map,
                   COUNT(DISTINCT p.match_id) AS games,
                   SUM(p.kills)               AS kills,
                   SUM(p.deaths)              AS deaths,
                   SUM(p.damage)              AS damage,
                   ROUND(CAST(SUM(p.kills) AS REAL) / MAX(SUM(p.deaths),1), 2) AS kd,
                   ROUND(CAST(SUM(p.kills) AS REAL) / MAX(COUNT(DISTINCT p.match_id),1), 1) AS avg_kills,
                   ROUND(CAST(SUM(p.damage) AS REAL) / MAX(COUNT(DISTINCT p.match_id),1), 0) AS avg_damage
            FROM player_match_stats p
            JOIN matches m ON m.id = p.match_id
            WHERE {name_clause} AND m.game_mode = 'tdm' AND {period_clause} AND {week_clause}
            GROUP BY m.map
            ORDER BY games DESC
        ''', name_params + period_params + week_params).fetchall()
    return [dict(r) for r in rows]


def get_player_weapon_stats(name: str,
                            period: str = _DEFAULT_H2H_PERIOD,
                            week: Optional[str] = None) -> list:
    """Kill counts grouped by weapon for a player (enemy kills only)."""
    period_clause, period_params, _ = _h2h_period_filter_clause(period)
    week_clause, week_params, _ = _week_filter_clause(week)
    with _connect() as conn:
        identities = _resolve_identity_variants(conn, name)
        killer_clause, killer_params = _in_clause('k.killer', identities)
        rows = conn.execute(f'''
            SELECT k.weapon,
                   COUNT(*) AS kills,
                   SUM(CASE WHEN k.location = 'head' THEN 1 ELSE 0 END) AS hs
            FROM kill_events k
            JOIN matches m ON m.id = k.match_id
            WHERE {killer_clause} AND k.team_kill = 0 AND m.game_mode = 'tdm' AND {period_clause} AND {week_clause}
            GROUP BY k.weapon
            ORDER BY kills DESC
        ''', killer_params + period_params + week_params).fetchall()
    return [dict(r) for r in rows]


def get_player_rivals(name: str,
                      limit: int = 10,
                      period: str = _DEFAULT_H2H_PERIOD,
                      week: Optional[str] = None) -> dict:
    """Nemesis (who kills you most) and prey (who you kill most), with mutual counts."""
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    week_clause, week_params, normalized_week = _week_filter_clause(week)
    cache_key = f'player_rivals:{name}:{limit}:{normalized_period}:{normalized_week or "all_weeks"}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        identities = _resolve_identity_variants(conn, name)
        victim_clause, victim_params = _in_clause('k.victim', identities)
        killer_clause, killer_params = _in_clause('k.killer', identities)
        nemesis = conn.execute(f'''
            WITH as_victim AS (
                SELECT k.killer AS other, COUNT(*) AS cnt
                FROM kill_events k
                JOIN matches m ON m.id = k.match_id
                WHERE {victim_clause} AND k.team_kill = 0 AND m.game_mode = 'tdm' AND {period_clause} AND {week_clause}
                GROUP BY k.killer
            ),
            as_killer AS (
                SELECT k.victim AS other, COUNT(*) AS cnt
                FROM kill_events k
                JOIN matches m ON m.id = k.match_id
                WHERE {killer_clause} AND k.team_kill = 0 AND m.game_mode = 'tdm' AND {period_clause} AND {week_clause}
                GROUP BY k.victim
            )
            SELECT av.other AS player, av.cnt AS count,
                   COALESCE(ak.cnt, 0) AS my_kills
            FROM as_victim av
            LEFT JOIN as_killer ak ON ak.other = av.other
            ORDER BY count DESC
            LIMIT ?
        ''', victim_params + period_params + week_params + killer_params + period_params + week_params + (limit,)).fetchall()
        prey = conn.execute(f'''
            WITH as_killer AS (
                SELECT k.victim AS other, COUNT(*) AS cnt
                FROM kill_events k
                JOIN matches m ON m.id = k.match_id
                WHERE {killer_clause} AND k.team_kill = 0 AND m.game_mode = 'tdm' AND {period_clause} AND {week_clause}
                GROUP BY k.victim
            ),
            as_victim AS (
                SELECT k.killer AS other, COUNT(*) AS cnt
                FROM kill_events k
                JOIN matches m ON m.id = k.match_id
                WHERE {victim_clause} AND k.team_kill = 0 AND m.game_mode = 'tdm' AND {period_clause} AND {week_clause}
                GROUP BY k.killer
            )
            SELECT ak.other AS player, ak.cnt AS count,
                   COALESCE(av.cnt, 0) AS their_kills
            FROM as_killer ak
            LEFT JOIN as_victim av ON av.other = ak.other
            ORDER BY count DESC
            LIMIT ?
        ''', killer_params + period_params + week_params + victim_params + period_params + week_params + (limit,)).fetchall()
    result = {
        'nemesis': [dict(r) for r in nemesis],
        'prey':    [dict(r) for r in prey],
    }
    _cache_set(cache_key, result)
    return result


def get_player_multikill_stats(name: str,
                               period: str = _DEFAULT_H2H_PERIOD,
                               week: Optional[str] = None) -> dict:
    """Per-round multi-kill breakdown (2K, 3K, 4K+) for a single player."""
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    week_clause, week_params, normalized_week = _week_filter_clause(week)
    cache_key = f'player_multikill:{name}:{normalized_period}:{normalized_week or "all_weeks"}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        identities = _resolve_identity_variants(conn, name)
        name_clause, name_params = _in_clause('prs.name', identities)
        r = conn.execute(f'''
            SELECT
                COUNT(*) AS total_rounds,
                SUM(CASE WHEN prs.kills >= 1 THEN 1 ELSE 0 END) AS rounds_with_kill,
                SUM(CASE WHEN prs.kills = 2 THEN 1 ELSE 0 END) AS double_kills,
                SUM(CASE WHEN prs.kills = 3 THEN 1 ELSE 0 END) AS triple_kills,
                SUM(CASE WHEN prs.kills >= 4 THEN 1 ELSE 0 END) AS quad_plus_kills
            FROM player_round_stats prs
            JOIN matches m ON m.id = prs.match_id
            WHERE {name_clause} AND m.game_mode = 'tdm' AND {period_clause} AND {week_clause}
        ''', name_params + period_params + week_params).fetchone()
    result = dict(r) if r else {}
    if result:
        total = int(result.get('total_rounds') or 0)
        result['double_kill_rate'] = round(100.0 * int(result.get('double_kills') or 0) / total, 1) if total else 0
        result['triple_kill_rate'] = round(100.0 * int(result.get('triple_kills') or 0) / total, 1) if total else 0
        result['quad_plus_kill_rate'] = round(100.0 * int(result.get('quad_plus_kills') or 0) / total, 1) if total else 0
    _cache_set(cache_key, result)
    return result


# ── Global weapon stats ────────────────────────────────────────────────────────

def get_weapon_stats(mode: str = _DEFAULT_STATS_MODE,
                     period: str = _DEFAULT_H2H_PERIOD,
                     week: Optional[str] = None) -> list:
    """Global kill counts per weapon across all matches."""
    mode = _normalize_mode_filter(mode)
    _, _, normalized_period = _h2h_period_filter_clause(period)
    _, _, normalized_week = _week_filter_clause(week)
    cache_key = f'weapon_stats:{mode}:{normalized_period}:{normalized_week or "all_weeks"}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    conn = _connect()
    if mode == _DEFAULT_STATS_MODE and normalized_period == 'all' and normalized_week is None:
        rows = conn.execute('''
            SELECT weapon, kills, hs, hs_pct, top_killer
            FROM weapon_stats_agg
            ORDER BY kills DESC
        ''').fetchall()
    else:
        rows = _query_weapon_stats_live(conn, mode=mode, period=normalized_period, week=normalized_week)
    result = [dict(r) for r in rows]
    _cache_set(cache_key, result)
    return result


def get_map_win_rates(mode: str = _DEFAULT_STATS_MODE,
                      period: str = _DEFAULT_H2H_PERIOD,
                      week: Optional[str] = None) -> list:
    """T1/T2 win rates per map."""
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='played_at',
    )
    cache_key = f'map_win_rates:{mode}:{normalized_period}:{normalized_week or "all_weeks"}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        if mode == _DEFAULT_STATS_MODE and normalized_period == 'all' and normalized_week is None:
            rows = conn.execute('''
                SELECT map, matches, t1_wins, t2_wins, draws
                FROM map_win_rates_agg
                ORDER BY matches DESC
            ''').fetchall()
        else:
            mode_clause, mode_params = _mode_filter_clause(mode, detail_col='game_mode_detail', bucket_col='game_mode')
            rows = conn.execute(f'''
                SELECT map,
                       COUNT(*) AS matches,
                       SUM(CASE WHEN t1_rounds > t2_rounds THEN 1 ELSE 0 END) AS t1_wins,
                       SUM(CASE WHEN t2_rounds > t1_rounds THEN 1 ELSE 0 END) AS t2_wins,
                       SUM(CASE WHEN t1_rounds = t2_rounds THEN 1 ELSE 0 END) AS draws
                FROM matches
                WHERE map != '' AND (t1_rounds + t2_rounds) > 0 AND {mode_clause} AND {period_clause} AND {week_clause}
                GROUP BY map
                ORDER BY matches DESC
            ''', mode_params + period_params + week_params).fetchall()
    result = [dict(r) for r in rows]
    _cache_set(cache_key, result)
    return result


def get_first_kill_stats(mode: str = _DEFAULT_STATS_MODE,
                         period: str = _DEFAULT_H2H_PERIOD,
                         week: Optional[str] = None,
                         limit: int = 15) -> dict:
    """Global first-kill analytics with conversion, players, weapons, and locations."""
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )
    normalized_limit = max(1, min(int(limit), 100))
    cache_key = (
        f'first_kill_stats:{mode}:{normalized_period}:'
        f'{normalized_week or "all_weeks"}:{normalized_limit}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )
        summary = conn.execute(f'''
            SELECT
                COUNT(*) AS total_rounds,
                SUM(CASE WHEN r.first_kill_player IS NOT NULL AND r.first_kill_player != '' THEN 1 ELSE 0 END) AS rounds_with_first_kill,
                SUM(CASE WHEN r.first_kill_team = 1 THEN 1 ELSE 0 END) AS team1_first_kills,
                SUM(CASE WHEN r.first_kill_team = 2 THEN 1 ELSE 0 END) AS team2_first_kills,
                SUM(CASE WHEN r.first_kill_team IN (1,2) AND r.winner_team IN (1,2) THEN 1 ELSE 0 END) AS decided_rounds,
                SUM(CASE WHEN r.first_kill_team IN (1,2) AND r.winner_team IN (1,2) AND r.first_kill_team = r.winner_team THEN 1 ELSE 0 END) AS first_kill_to_win_rounds,
                SUM(CASE WHEN r.first_kill_team = 1 AND r.winner_team IN (1,2) THEN 1 ELSE 0 END) AS team1_decided_rounds,
                SUM(CASE WHEN r.first_kill_team = 1 AND r.winner_team = 1 THEN 1 ELSE 0 END) AS team1_converted_rounds,
                SUM(CASE WHEN r.first_kill_team = 2 AND r.winner_team IN (1,2) THEN 1 ELSE 0 END) AS team2_decided_rounds,
                SUM(CASE WHEN r.first_kill_team = 2 AND r.winner_team = 2 THEN 1 ELSE 0 END) AS team2_converted_rounds
            FROM rounds r
            JOIN matches m ON m.id = r.match_id
            WHERE {mode_clause} AND {period_clause} AND {week_clause}
        ''', mode_params + period_params + week_params).fetchone()

        top_players = conn.execute(f'''
            SELECT
                r.first_kill_player AS name,
                COUNT(*) AS first_kills,
                SUM(CASE WHEN r.first_kill_team IN (1,2)
                          AND r.winner_team IN (1,2)
                          AND r.first_kill_team = r.winner_team
                         THEN 1 ELSE 0 END) AS converted_rounds
            FROM rounds r
            JOIN matches m ON m.id = r.match_id
            WHERE r.first_kill_player IS NOT NULL
              AND r.first_kill_player != ''
              AND {mode_clause} AND {period_clause} AND {week_clause}
            GROUP BY r.first_kill_player
            ORDER BY first_kills DESC, name ASC
            LIMIT ?
        ''', mode_params + period_params + week_params + (normalized_limit,)).fetchall()

        top_weapons = conn.execute(f'''
            SELECT
                r.first_kill_weapon AS weapon,
                COUNT(*) AS first_kills
            FROM rounds r
            JOIN matches m ON m.id = r.match_id
            WHERE r.first_kill_weapon IS NOT NULL
              AND r.first_kill_weapon != ''
              AND {mode_clause} AND {period_clause} AND {week_clause}
            GROUP BY r.first_kill_weapon
            ORDER BY first_kills DESC, weapon ASC
            LIMIT ?
        ''', mode_params + period_params + week_params + (normalized_limit,)).fetchall()

        top_locations = conn.execute(f'''
            SELECT
                r.first_kill_location AS location,
                COUNT(*) AS first_kills
            FROM rounds r
            JOIN matches m ON m.id = r.match_id
            WHERE r.first_kill_location IS NOT NULL
              AND r.first_kill_location != ''
              AND {mode_clause} AND {period_clause} AND {week_clause}
            GROUP BY r.first_kill_location
            ORDER BY first_kills DESC, location ASC
            LIMIT ?
        ''', mode_params + period_params + week_params + (normalized_limit,)).fetchall()

        map_breakdown = conn.execute(f'''
            SELECT
                m.map,
                COUNT(*) AS rounds,
                SUM(CASE WHEN r.first_kill_team = 1 THEN 1 ELSE 0 END) AS team1_first_kills,
                SUM(CASE WHEN r.first_kill_team = 2 THEN 1 ELSE 0 END) AS team2_first_kills,
                SUM(CASE WHEN r.first_kill_team IN (1,2)
                          AND r.winner_team IN (1,2)
                          AND r.first_kill_team = r.winner_team
                         THEN 1 ELSE 0 END) AS first_kill_converted
            FROM rounds r
            JOIN matches m ON m.id = r.match_id
            WHERE m.map != '' AND {mode_clause} AND {period_clause} AND {week_clause}
            GROUP BY m.map
            ORDER BY rounds DESC, m.map ASC
            LIMIT ?
        ''', mode_params + period_params + week_params + (normalized_limit,)).fetchall()

        map_hotspots = conn.execute(f'''
            SELECT
                m.map,
                r.first_kill_weapon AS weapon,
                r.first_kill_location AS location,
                COUNT(*) AS first_kills
            FROM rounds r
            JOIN matches m ON m.id = r.match_id
            WHERE m.map != ''
              AND r.first_kill_player IS NOT NULL
              AND r.first_kill_player != ''
              AND r.first_kill_weapon IS NOT NULL
              AND r.first_kill_weapon != ''
              AND r.first_kill_location IS NOT NULL
              AND r.first_kill_location != ''
              AND {mode_clause} AND {period_clause} AND {week_clause}
            GROUP BY m.map, r.first_kill_weapon, r.first_kill_location
            ORDER BY first_kills DESC, m.map ASC
            LIMIT ?
        ''', mode_params + period_params + week_params + (normalized_limit,)).fetchall()

    summary_dict = dict(summary) if summary else {}
    total_rounds = int(summary_dict.get('total_rounds') or 0)
    rounds_with_first = int(summary_dict.get('rounds_with_first_kill') or 0)
    decided_rounds = int(summary_dict.get('decided_rounds') or 0)
    first_to_win = int(summary_dict.get('first_kill_to_win_rounds') or 0)
    team1_decided = int(summary_dict.get('team1_decided_rounds') or 0)
    team1_converted = int(summary_dict.get('team1_converted_rounds') or 0)
    team2_decided = int(summary_dict.get('team2_decided_rounds') or 0)
    team2_converted = int(summary_dict.get('team2_converted_rounds') or 0)

    summary_dict['first_kill_presence_rate'] = (
        round((100.0 * rounds_with_first) / total_rounds, 1) if total_rounds else 0
    )
    summary_dict['first_kill_conversion_rate'] = (
        round((100.0 * first_to_win) / decided_rounds, 1) if decided_rounds else 0
    )
    summary_dict['team1_first_kill_conversion_rate'] = (
        round((100.0 * team1_converted) / team1_decided, 1) if team1_decided else 0
    )
    summary_dict['team2_first_kill_conversion_rate'] = (
        round((100.0 * team2_converted) / team2_decided, 1) if team2_decided else 0
    )

    result = {
        'summary': summary_dict,
        'top_players': [dict(r) for r in top_players],
        'top_weapons': [dict(r) for r in top_weapons],
        'top_locations': [dict(r) for r in top_locations],
        'map_breakdown': [dict(r) for r in map_breakdown],
        'map_hotspots': [dict(r) for r in map_hotspots],
    }
    _cache_set(cache_key, result)
    return result


def get_team_analytics(mode: str = _DEFAULT_STATS_MODE,
                       period: str = _DEFAULT_H2H_PERIOD,
                       week: Optional[str] = None,
                       limit: int = 20) -> dict:
    """Team analytics pack: win rates, first-kill conversion, survival, clutch, per-map."""
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )
    normalized_limit = max(1, min(int(limit), 100))
    cache_key = (
        f'team_analytics:{mode}:{normalized_period}:'
        f'{normalized_week or "all_weeks"}:{normalized_limit}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )
        base_params = mode_params + period_params + week_params

        summary = conn.execute(f'''
            SELECT
                COUNT(*) AS total_rounds,
                SUM(CASE WHEN r.winner_team = 1 THEN 1 ELSE 0 END) AS team1_round_wins,
                SUM(CASE WHEN r.winner_team = 2 THEN 1 ELSE 0 END) AS team2_round_wins,
                SUM(CASE WHEN r.is_tie = 1 THEN 1 ELSE 0 END) AS tied_rounds,
                SUM(CASE WHEN r.first_kill_team = 1 THEN 1 ELSE 0 END) AS team1_first_kills,
                SUM(CASE WHEN r.first_kill_team = 2 THEN 1 ELSE 0 END) AS team2_first_kills,
                SUM(CASE WHEN r.first_kill_team = 1 AND r.winner_team = 1 THEN 1 ELSE 0 END) AS team1_first_kill_converted,
                SUM(CASE WHEN r.first_kill_team = 2 AND r.winner_team = 2 THEN 1 ELSE 0 END) AS team2_first_kill_converted
            FROM rounds r
            JOIN matches m ON m.id = r.match_id
            WHERE {mode_clause} AND {period_clause} AND {week_clause}
        ''', base_params).fetchone()

        teams = conn.execute(f'''
            WITH filtered_rounds AS (
                SELECT r.id AS round_id,
                       r.winner_team,
                       r.first_kill_team,
                       r.is_tie,
                       m.map
                FROM rounds r
                JOIN matches m ON m.id = r.match_id
                WHERE {mode_clause} AND {period_clause} AND {week_clause}
            ),
            team_rows AS (
                SELECT fr.round_id,
                       fr.map,
                       fr.winner_team,
                       fr.first_kill_team,
                       fr.is_tie,
                       prs.team,
                       COUNT(*) AS players,
                       SUM(prs.survived) AS survivors,
                       SUM(prs.kills) AS kills,
                       SUM(prs.deaths) AS deaths
                FROM filtered_rounds fr
                JOIN player_round_stats prs ON prs.round_id = fr.round_id
                WHERE prs.team IN (1,2)
                GROUP BY fr.round_id, fr.map, fr.winner_team, fr.first_kill_team, fr.is_tie, prs.team
            ),
            team_with_opp AS (
                SELECT t.*,
                       o.survivors AS opp_survivors
                FROM team_rows t
                LEFT JOIN team_rows o
                  ON o.round_id = t.round_id AND o.team != t.team
            )
            SELECT
                team,
                COUNT(*) AS rounds,
                SUM(CASE WHEN winner_team = team THEN 1 ELSE 0 END) AS rounds_won,
                SUM(CASE WHEN is_tie = 1 THEN 1 ELSE 0 END) AS rounds_tied,
                SUM(players) AS players_total,
                SUM(survivors) AS survivors_total,
                SUM(kills) AS kills,
                SUM(deaths) AS deaths,
                SUM(CASE WHEN first_kill_team = team THEN 1 ELSE 0 END) AS first_kills,
                SUM(CASE WHEN first_kill_team = team AND winner_team = team THEN 1 ELSE 0 END) AS first_kill_converted,
                SUM(CASE WHEN winner_team = team
                          AND opp_survivors IS NOT NULL
                          AND survivors < opp_survivors
                         THEN 1 ELSE 0 END) AS clutch_wins
            FROM team_with_opp
            GROUP BY team
            ORDER BY team ASC
        ''', base_params).fetchall()

        map_breakdown = conn.execute(f'''
            WITH filtered_rounds AS (
                SELECT r.id AS round_id,
                       r.winner_team,
                       r.first_kill_team,
                       r.is_tie,
                       m.map
                FROM rounds r
                JOIN matches m ON m.id = r.match_id
                WHERE {mode_clause} AND {period_clause} AND {week_clause}
            ),
            team_rows AS (
                SELECT fr.round_id,
                       fr.map,
                       fr.winner_team,
                       fr.first_kill_team,
                       fr.is_tie,
                       prs.team,
                       COUNT(*) AS players,
                       SUM(prs.survived) AS survivors,
                       SUM(prs.kills) AS kills,
                       SUM(prs.deaths) AS deaths
                FROM filtered_rounds fr
                JOIN player_round_stats prs ON prs.round_id = fr.round_id
                WHERE prs.team IN (1,2)
                GROUP BY fr.round_id, fr.map, fr.winner_team, fr.first_kill_team, fr.is_tie, prs.team
            ),
            team_with_opp AS (
                SELECT t.*,
                       o.survivors AS opp_survivors
                FROM team_rows t
                LEFT JOIN team_rows o
                  ON o.round_id = t.round_id AND o.team != t.team
            )
            SELECT
                map,
                team,
                COUNT(*) AS rounds,
                SUM(CASE WHEN winner_team = team THEN 1 ELSE 0 END) AS rounds_won,
                SUM(CASE WHEN is_tie = 1 THEN 1 ELSE 0 END) AS rounds_tied,
                SUM(players) AS players_total,
                SUM(survivors) AS survivors_total,
                SUM(kills) AS kills,
                SUM(deaths) AS deaths,
                SUM(CASE WHEN first_kill_team = team THEN 1 ELSE 0 END) AS first_kills,
                SUM(CASE WHEN first_kill_team = team AND winner_team = team THEN 1 ELSE 0 END) AS first_kill_converted,
                SUM(CASE WHEN winner_team = team
                          AND opp_survivors IS NOT NULL
                          AND survivors < opp_survivors
                         THEN 1 ELSE 0 END) AS clutch_wins
            FROM team_with_opp
            GROUP BY map, team
            ORDER BY rounds DESC, map ASC, team ASC
            LIMIT ?
        ''', base_params + (normalized_limit,)).fetchall()

        captain_summary = conn.execute(f'''
            SELECT
                COUNT(DISTINCT mc.match_id) AS matches_with_captains,
                COUNT(DISTINCT mc.captain_name) AS distinct_captains,
                SUM(mc.switches) AS total_switches
            FROM match_captains mc
            JOIN matches m ON m.id = mc.match_id
            WHERE mc.team IN (1,2)
              AND (m.t1_rounds + m.t2_rounds) > 0
              AND {mode_clause} AND {period_clause} AND {week_clause}
        ''', base_params).fetchone()

        captains = conn.execute(f'''
            SELECT
                mc.team,
                mc.captain_name,
                COUNT(*) AS matches,
                SUM(CASE WHEN mc.team = 1 THEN m.t1_rounds ELSE m.t2_rounds END) AS rounds_won,
                SUM(CASE WHEN mc.team = 1 THEN m.t2_rounds ELSE m.t1_rounds END) AS rounds_lost,
                SUM(CASE
                        WHEN mc.team = 1 AND m.t1_rounds > m.t2_rounds THEN 1
                        WHEN mc.team = 2 AND m.t2_rounds > m.t1_rounds THEN 1
                        ELSE 0
                    END) AS match_wins,
                SUM(CASE
                        WHEN mc.team = 1 AND m.t2_rounds > m.t1_rounds THEN 1
                        WHEN mc.team = 2 AND m.t1_rounds > m.t2_rounds THEN 1
                        ELSE 0
                    END) AS match_losses,
                SUM(mc.switches) AS captain_switches
            FROM match_captains mc
            JOIN matches m ON m.id = mc.match_id
            WHERE mc.team IN (1,2)
              AND (m.t1_rounds + m.t2_rounds) > 0
              AND {mode_clause} AND {period_clause} AND {week_clause}
            GROUP BY mc.team, mc.captain_name
            ORDER BY matches DESC, mc.captain_name ASC
            LIMIT ?
        ''', base_params + (normalized_limit,)).fetchall()

    summary_dict = dict(summary) if summary else {}
    team1_first = int(summary_dict.get('team1_first_kills') or 0)
    team2_first = int(summary_dict.get('team2_first_kills') or 0)
    team1_first_conv = int(summary_dict.get('team1_first_kill_converted') or 0)
    team2_first_conv = int(summary_dict.get('team2_first_kill_converted') or 0)
    total_rounds = int(summary_dict.get('total_rounds') or 0)

    summary_dict['team1_round_win_rate'] = (
        round((100.0 * int(summary_dict.get('team1_round_wins') or 0)) / total_rounds, 1)
        if total_rounds else 0
    )
    summary_dict['team2_round_win_rate'] = (
        round((100.0 * int(summary_dict.get('team2_round_wins') or 0)) / total_rounds, 1)
        if total_rounds else 0
    )
    summary_dict['team1_first_kill_win_rate'] = (
        round((100.0 * team1_first_conv) / team1_first, 1)
        if team1_first else 0
    )
    summary_dict['team2_first_kill_win_rate'] = (
        round((100.0 * team2_first_conv) / team2_first, 1)
        if team2_first else 0
    )
    captain_summary_dict = dict(captain_summary) if captain_summary else {}
    matches_with_captains = int(captain_summary_dict.get('matches_with_captains') or 0)
    total_switches = int(captain_summary_dict.get('total_switches') or 0)
    summary_dict['matches_with_captains'] = matches_with_captains
    summary_dict['distinct_captains'] = int(captain_summary_dict.get('distinct_captains') or 0)
    summary_dict['captain_switches_total'] = total_switches
    summary_dict['captain_switches_per_match'] = (
        round(float(total_switches) / matches_with_captains, 2)
        if matches_with_captains else 0
    )

    teams_rows = []
    for row in teams:
        d = dict(row)
        rounds = int(d.get('rounds') or 0)
        rounds_won = int(d.get('rounds_won') or 0)
        players_total = int(d.get('players_total') or 0)
        first_kills = int(d.get('first_kills') or 0)
        kills = int(d.get('kills') or 0)
        deaths = int(d.get('deaths') or 0)
        d['team_label'] = f"Team {int(d.get('team') or 0)}"
        d['round_win_rate'] = round((100.0 * rounds_won) / rounds, 1) if rounds else 0
        d['survival_rate'] = round((100.0 * int(d.get('survivors_total') or 0)) / players_total, 1) if players_total else 0
        d['first_kill_win_rate'] = round((100.0 * int(d.get('first_kill_converted') or 0)) / first_kills, 1) if first_kills else 0
        d['clutch_rate'] = round((100.0 * int(d.get('clutch_wins') or 0)) / rounds_won, 1) if rounds_won else 0
        d['kd'] = round(float(kills) / max(deaths, 1), 2)
        teams_rows.append(d)

    map_rows = []
    for row in map_breakdown:
        d = dict(row)
        rounds = int(d.get('rounds') or 0)
        rounds_won = int(d.get('rounds_won') or 0)
        players_total = int(d.get('players_total') or 0)
        first_kills = int(d.get('first_kills') or 0)
        kills = int(d.get('kills') or 0)
        deaths = int(d.get('deaths') or 0)
        d['team_label'] = f"Team {int(d.get('team') or 0)}"
        d['round_win_rate'] = round((100.0 * rounds_won) / rounds, 1) if rounds else 0
        d['survival_rate'] = round((100.0 * int(d.get('survivors_total') or 0)) / players_total, 1) if players_total else 0
        d['first_kill_win_rate'] = round((100.0 * int(d.get('first_kill_converted') or 0)) / first_kills, 1) if first_kills else 0
        d['clutch_rate'] = round((100.0 * int(d.get('clutch_wins') or 0)) / rounds_won, 1) if rounds_won else 0
        d['kd'] = round(float(kills) / max(deaths, 1), 2)
        map_rows.append(d)

    captain_rows = []
    for row in captains:
        d = dict(row)
        matches = int(d.get('matches') or 0)
        rounds_won = int(d.get('rounds_won') or 0)
        rounds_lost = int(d.get('rounds_lost') or 0)
        total_rounds_for_captain = rounds_won + rounds_lost
        d['team_label'] = f"Team {int(d.get('team') or 0)}"
        d['match_win_rate'] = (
            round((100.0 * int(d.get('match_wins') or 0)) / matches, 1)
            if matches else 0
        )
        d['round_win_rate'] = (
            round((100.0 * rounds_won) / total_rounds_for_captain, 1)
            if total_rounds_for_captain else 0
        )
        d['avg_switches_per_match'] = (
            round(float(int(d.get('captain_switches') or 0)) / matches, 2)
            if matches else 0
        )
        captain_rows.append(d)

    result = {
        'summary': summary_dict,
        'teams': teams_rows,
        'map_breakdown': map_rows,
        'captains': captain_rows,
    }
    _cache_set(cache_key, result)
    return result


def get_round_analytics(mode: str = _DEFAULT_STATS_MODE,
                        period: str = _DEFAULT_H2H_PERIOD,
                        week: Optional[str] = None,
                        map_name: Optional[str] = None,
                        limit: int = 25) -> dict:
    """Round analytics pack: duration, pacing, win conditions, and first-kill outcomes."""
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )
    map_filter = (map_name or '').strip()
    normalized_limit = max(1, min(int(limit), 200))
    cache_key = (
        f'round_analytics:{mode}:{normalized_period}:{normalized_week or "all_weeks"}:'
        f'{map_filter or "all_maps"}:{normalized_limit}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )
        map_clause = ' AND m.map = ?' if map_filter else ''
        base_params = mode_params + period_params + week_params + ((map_filter,) if map_filter else ())
        cte_sql = f'''
            WITH filtered_rounds AS (
                SELECT r.id AS round_id,
                       r.match_id,
                       m.map,
                       r.winner_team,
                       r.first_kill_team,
                       r.win_condition,
                       r.duration_seconds,
                       r.duration_frames,
                       r.start_frame,
                       r.end_frame
                FROM rounds r
                JOIN matches m ON m.id = r.match_id
                WHERE {mode_clause} AND {period_clause} AND {week_clause}{map_clause}
            ),
            round_kills AS (
                SELECT fr.round_id,
                       COUNT(k.id) AS kills,
                       SUM(CASE WHEN fr.duration_frames > 0
                                 AND (k.frame_idx - fr.start_frame) < (fr.duration_frames / 3.0)
                                THEN 1 ELSE 0 END) AS early_kills,
                       SUM(CASE WHEN fr.duration_frames > 0
                                 AND (k.frame_idx - fr.start_frame) >= (fr.duration_frames / 3.0)
                                 AND (k.frame_idx - fr.start_frame) < ((fr.duration_frames * 2.0) / 3.0)
                                THEN 1 ELSE 0 END) AS mid_kills,
                       SUM(CASE WHEN fr.duration_frames > 0
                                 AND (k.frame_idx - fr.start_frame) >= ((fr.duration_frames * 2.0) / 3.0)
                                THEN 1 ELSE 0 END) AS late_kills
                FROM filtered_rounds fr
                LEFT JOIN kill_events k
                  ON k.match_id = fr.match_id
                 AND k.team_kill = 0
                 AND k.frame_idx >= fr.start_frame
                 AND k.frame_idx < fr.end_frame
                GROUP BY fr.round_id
            )
        '''

        summary = conn.execute(
            cte_sql + '''
            SELECT
                COUNT(*) AS total_rounds,
                ROUND(AVG(fr.duration_seconds), 2) AS avg_round_seconds,
                MIN(fr.duration_seconds) AS shortest_round_seconds,
                MAX(fr.duration_seconds) AS longest_round_seconds,
                ROUND(AVG(COALESCE(rk.kills, 0)), 2) AS avg_kills_per_round,
                SUM(COALESCE(rk.kills, 0)) AS total_enemy_kills,
                SUM(COALESCE(rk.early_kills, 0)) AS early_kills,
                SUM(COALESCE(rk.mid_kills, 0)) AS mid_kills,
                SUM(COALESCE(rk.late_kills, 0)) AS late_kills,
                SUM(CASE WHEN fr.first_kill_team IN (1,2) THEN 1 ELSE 0 END) AS first_kill_rounds,
                SUM(CASE WHEN fr.first_kill_team IN (1,2) AND fr.winner_team = fr.first_kill_team THEN 1 ELSE 0 END) AS first_kill_converted
            FROM filtered_rounds fr
            LEFT JOIN round_kills rk ON rk.round_id = fr.round_id
            ''',
            base_params,
        ).fetchone()

        win_conditions = conn.execute(
            cte_sql + '''
            SELECT
                fr.win_condition,
                COUNT(*) AS rounds,
                ROUND(AVG(fr.duration_seconds), 2) AS avg_round_seconds
            FROM filtered_rounds fr
            GROUP BY fr.win_condition
            ORDER BY rounds DESC, fr.win_condition ASC
            ''',
            base_params,
        ).fetchall()

        first_kill_outcomes = conn.execute(
            cte_sql + '''
            SELECT
                CASE
                    WHEN fr.first_kill_team = 1 THEN 'team1'
                    WHEN fr.first_kill_team = 2 THEN 'team2'
                    ELSE 'unknown'
                END AS first_kill_side,
                COUNT(*) AS rounds,
                SUM(CASE WHEN fr.first_kill_team IN (1,2) AND fr.winner_team = fr.first_kill_team THEN 1 ELSE 0 END) AS converted_rounds
            FROM filtered_rounds fr
            GROUP BY first_kill_side
            ORDER BY
                CASE first_kill_side
                    WHEN 'team1' THEN 1
                    WHEN 'team2' THEN 2
                    ELSE 3
                END
            ''',
            base_params,
        ).fetchall()

        pacing = conn.execute(
            cte_sql + '''
            SELECT
                SUM(CASE WHEN COALESCE(rk.kills, 0) <= 2 THEN 1 ELSE 0 END) AS low_pace_rounds,
                SUM(CASE WHEN COALESCE(rk.kills, 0) BETWEEN 3 AND 5 THEN 1 ELSE 0 END) AS mid_pace_rounds,
                SUM(CASE WHEN COALESCE(rk.kills, 0) >= 6 THEN 1 ELSE 0 END) AS high_pace_rounds
            FROM filtered_rounds fr
            LEFT JOIN round_kills rk ON rk.round_id = fr.round_id
            ''',
            base_params,
        ).fetchone()

        map_breakdown = conn.execute(
            cte_sql + '''
            SELECT
                fr.map,
                COUNT(*) AS rounds,
                ROUND(AVG(fr.duration_seconds), 2) AS avg_round_seconds,
                MIN(fr.duration_seconds) AS shortest_round_seconds,
                MAX(fr.duration_seconds) AS longest_round_seconds,
                ROUND(AVG(COALESCE(rk.kills, 0)), 2) AS avg_kills_per_round,
                SUM(CASE WHEN fr.first_kill_team IN (1,2) THEN 1 ELSE 0 END) AS first_kill_rounds,
                SUM(CASE WHEN fr.first_kill_team IN (1,2) AND fr.winner_team = fr.first_kill_team THEN 1 ELSE 0 END) AS first_kill_converted
            FROM filtered_rounds fr
            LEFT JOIN round_kills rk ON rk.round_id = fr.round_id
            GROUP BY fr.map
            ORDER BY rounds DESC, fr.map ASC
            LIMIT ?
            ''',
            base_params + (normalized_limit,),
        ).fetchall()

    summary_dict = dict(summary) if summary else {}
    total_kills = int(summary_dict.get('total_enemy_kills') or 0)
    first_kill_rounds = int(summary_dict.get('first_kill_rounds') or 0)
    summary_dict['early_kill_pct'] = (
        round((100.0 * int(summary_dict.get('early_kills') or 0)) / total_kills, 1)
        if total_kills else 0
    )
    summary_dict['mid_kill_pct'] = (
        round((100.0 * int(summary_dict.get('mid_kills') or 0)) / total_kills, 1)
        if total_kills else 0
    )
    summary_dict['late_kill_pct'] = (
        round((100.0 * int(summary_dict.get('late_kills') or 0)) / total_kills, 1)
        if total_kills else 0
    )
    summary_dict['first_kill_conversion_rate'] = (
        round((100.0 * int(summary_dict.get('first_kill_converted') or 0)) / first_kill_rounds, 1)
        if first_kill_rounds else 0
    )

    pacing_dict = dict(pacing) if pacing else {}
    total_rounds = int(summary_dict.get('total_rounds') or 0)
    pacing_dict['low_pace_pct'] = (
        round((100.0 * int(pacing_dict.get('low_pace_rounds') or 0)) / total_rounds, 1)
        if total_rounds else 0
    )
    pacing_dict['mid_pace_pct'] = (
        round((100.0 * int(pacing_dict.get('mid_pace_rounds') or 0)) / total_rounds, 1)
        if total_rounds else 0
    )
    pacing_dict['high_pace_pct'] = (
        round((100.0 * int(pacing_dict.get('high_pace_rounds') or 0)) / total_rounds, 1)
        if total_rounds else 0
    )

    first_kill_rows = []
    for row in first_kill_outcomes:
        d = dict(row)
        rounds = int(d.get('rounds') or 0)
        d['conversion_rate'] = (
            round((100.0 * int(d.get('converted_rounds') or 0)) / rounds, 1)
            if rounds else 0
        )
        first_kill_rows.append(d)

    map_rows = []
    for row in map_breakdown:
        d = dict(row)
        fk_rounds = int(d.get('first_kill_rounds') or 0)
        d['first_kill_conversion_rate'] = (
            round((100.0 * int(d.get('first_kill_converted') or 0)) / fk_rounds, 1)
            if fk_rounds else 0
        )
        map_rows.append(d)

    result = {
        'summary': summary_dict,
        'win_conditions': [dict(r) for r in win_conditions],
        'first_kill_outcomes': first_kill_rows,
        'pacing': pacing_dict,
        'map_breakdown': map_rows,
    }
    _cache_set(cache_key, result)
    return result


def get_match_analytics(mode: str = _DEFAULT_STATS_MODE,
                        period: str = _DEFAULT_H2H_PERIOD,
                        week: Optional[str] = None,
                        limit: int = 20) -> dict:
    """Match analytics pack: score distribution, comebacks, dominant wins, momentum."""
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )
    normalized_limit = max(1, min(int(limit), 100))
    cache_key = (
        f'match_analytics:{mode}:{normalized_period}:'
        f'{normalized_week or "all_weeks"}:{normalized_limit}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )
        base_params = mode_params + period_params + week_params
        cte_sql = f'''
            WITH filtered_matches AS (
                SELECT m.id,
                       m.map,
                       m.path,
                       m.played_at,
                       m.duration,
                       m.t1_rounds,
                       m.t2_rounds,
                       ABS(m.t1_rounds - m.t2_rounds) AS round_diff,
                       CASE
                           WHEN m.t1_rounds > m.t2_rounds THEN 1
                           WHEN m.t2_rounds > m.t1_rounds THEN 2
                           ELSE 0
                       END AS winner_team
                FROM matches m
                WHERE (m.t1_rounds + m.t2_rounds) > 0
                  AND {mode_clause} AND {period_clause} AND {week_clause}
            ),
            round_prog AS (
                SELECT r.match_id,
                       r.round_index,
                       r.winner_team,
                       fm.winner_team AS final_winner,
                       SUM(CASE WHEN r.winner_team = 1 THEN 1 ELSE 0 END)
                         OVER (PARTITION BY r.match_id ORDER BY r.round_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS t1_sofar,
                       SUM(CASE WHEN r.winner_team = 2 THEN 1 ELSE 0 END)
                         OVER (PARTITION BY r.match_id ORDER BY r.round_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS t2_sofar,
                       CASE
                           WHEN LAG(r.winner_team) OVER (PARTITION BY r.match_id ORDER BY r.round_index) = r.winner_team
                           THEN 0 ELSE 1
                       END AS change_flag
                FROM rounds r
                JOIN filtered_matches fm ON fm.id = r.match_id
                WHERE r.winner_team IN (1,2)
            ),
            streak_groups AS (
                SELECT match_id,
                       winner_team,
                       SUM(change_flag)
                         OVER (PARTITION BY match_id ORDER BY round_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
                FROM round_prog
            ),
            streak_sizes AS (
                SELECT match_id,
                       winner_team,
                       grp,
                       COUNT(*) AS streak_len
                FROM streak_groups
                GROUP BY match_id, winner_team, grp
            ),
            match_streaks AS (
                SELECT fm.id AS match_id,
                       COALESCE(MAX(ss.streak_len), 0) AS max_streak
                FROM filtered_matches fm
                LEFT JOIN streak_sizes ss ON ss.match_id = fm.id
                GROUP BY fm.id
            ),
            comebacks AS (
                SELECT rp.match_id,
                       MAX(
                           CASE
                               WHEN rp.final_winner = 1 AND (rp.t1_sofar - rp.t2_sofar) <= -2 THEN 1
                               WHEN rp.final_winner = 2 AND (rp.t2_sofar - rp.t1_sofar) <= -2 THEN 1
                               ELSE 0
                           END
                       ) AS is_comeback
                FROM round_prog rp
                WHERE rp.final_winner IN (1,2)
                GROUP BY rp.match_id
            )
        '''

        summary = conn.execute(
            cte_sql + '''
            SELECT
                COUNT(*) AS matches,
                ROUND(AVG(fm.round_diff), 2) AS avg_round_diff,
                SUM(CASE WHEN fm.round_diff <= 2 THEN 1 ELSE 0 END) AS close_matches,
                SUM(CASE WHEN fm.round_diff >= 6 THEN 1 ELSE 0 END) AS dominant_matches,
                SUM(CASE WHEN fm.winner_team = 0 THEN 1 ELSE 0 END) AS draws,
                SUM(COALESCE(cb.is_comeback, 0)) AS comeback_wins,
                ROUND(AVG(CASE WHEN (fm.t1_rounds + fm.t2_rounds) > 0
                               THEN (100.0 * ms.max_streak) / (fm.t1_rounds + fm.t2_rounds)
                               ELSE 0 END), 1) AS momentum_index
            FROM filtered_matches fm
            LEFT JOIN match_streaks ms ON ms.match_id = fm.id
            LEFT JOIN comebacks cb ON cb.match_id = fm.id
            ''',
            base_params,
        ).fetchone()

        score_distribution = conn.execute(
            cte_sql + '''
            SELECT fm.round_diff,
                   COUNT(*) AS matches
            FROM filtered_matches fm
            GROUP BY fm.round_diff
            ORDER BY fm.round_diff ASC
            ''',
            base_params,
        ).fetchall()

        map_breakdown = conn.execute(
            cte_sql + '''
            SELECT
                fm.map,
                COUNT(*) AS matches,
                ROUND(AVG(fm.round_diff), 2) AS avg_round_diff,
                SUM(CASE WHEN fm.round_diff <= 2 THEN 1 ELSE 0 END) AS close_matches,
                SUM(CASE WHEN fm.round_diff >= 6 THEN 1 ELSE 0 END) AS dominant_matches,
                SUM(COALESCE(cb.is_comeback, 0)) AS comeback_wins
            FROM filtered_matches fm
            LEFT JOIN comebacks cb ON cb.match_id = fm.id
            GROUP BY fm.map
            ORDER BY matches DESC, fm.map ASC
            LIMIT ?
            ''',
            base_params + (normalized_limit,),
        ).fetchall()

        top_momentum_matches = conn.execute(
            cte_sql + '''
            SELECT
                fm.path,
                fm.map,
                fm.played_at,
                fm.t1_rounds,
                fm.t2_rounds,
                ms.max_streak,
                ROUND(CASE WHEN (fm.t1_rounds + fm.t2_rounds) > 0
                           THEN (100.0 * ms.max_streak) / (fm.t1_rounds + fm.t2_rounds)
                           ELSE 0 END, 1) AS momentum_score
            FROM filtered_matches fm
            LEFT JOIN match_streaks ms ON ms.match_id = fm.id
            ORDER BY momentum_score DESC, fm.played_at DESC
            LIMIT ?
            ''',
            base_params + (normalized_limit,),
        ).fetchall()

    summary_dict = dict(summary) if summary else {}
    matches = int(summary_dict.get('matches') or 0)
    summary_dict['close_match_rate'] = (
        round((100.0 * int(summary_dict.get('close_matches') or 0)) / matches, 1)
        if matches else 0
    )
    summary_dict['dominant_match_rate'] = (
        round((100.0 * int(summary_dict.get('dominant_matches') or 0)) / matches, 1)
        if matches else 0
    )
    summary_dict['comeback_rate'] = (
        round((100.0 * int(summary_dict.get('comeback_wins') or 0)) / matches, 1)
        if matches else 0
    )

    result = {
        'summary': summary_dict,
        'score_distribution': [dict(r) for r in score_distribution],
        'map_breakdown': [dict(r) for r in map_breakdown],
        'top_momentum_matches': [dict(r) for r in top_momentum_matches],
    }
    _cache_set(cache_key, result)
    return result


def get_weapon_analytics(mode: str = _DEFAULT_STATS_MODE,
                         period: str = _DEFAULT_H2H_PERIOD,
                         week: Optional[str] = None,
                         limit: int = 20) -> dict:
    """Weapon analytics expansion: win-rate proxy and effective weapons per map."""
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )
    normalized_limit = max(1, min(int(limit), 200))
    cache_key = (
        f'weapon_analytics:{mode}:{normalized_period}:'
        f'{normalized_week or "all_weeks"}:{normalized_limit}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )
        base_params = mode_params + period_params + week_params
        cte_sql = f'''
            WITH filtered_kills AS (
                SELECT k.weapon,
                       k.location,
                       k.match_id,
                       m.map,
                       p.team,
                       CASE
                           WHEN p.team IN (1,2) AND (m.t1_rounds + m.t2_rounds) > 0 THEN 1
                           ELSE 0
                       END AS decided_kill,
                       CASE
                           WHEN p.team = 1 AND m.t1_rounds > m.t2_rounds THEN 1
                           WHEN p.team = 2 AND m.t2_rounds > m.t1_rounds THEN 1
                           ELSE 0
                       END AS winning_kill
                FROM kill_events k
                JOIN matches m ON m.id = k.match_id
                LEFT JOIN player_match_stats p
                       ON p.match_id = k.match_id
                      AND p.name = k.killer
                WHERE k.team_kill = 0
                  AND {mode_clause} AND {period_clause} AND {week_clause}
            )
        '''

        summary = conn.execute(
            cte_sql + '''
            SELECT
                COUNT(*) AS total_enemy_kills,
                COUNT(DISTINCT weapon) AS weapon_count,
                COUNT(DISTINCT map) AS map_count,
                SUM(winning_kill) AS winning_kills,
                SUM(decided_kill) AS decided_kills
            FROM filtered_kills
            ''',
            base_params,
        ).fetchone()

        top_weapons = conn.execute(
            cte_sql + '''
            SELECT
                weapon,
                COUNT(*) AS kills,
                SUM(CASE WHEN location = 'head' THEN 1 ELSE 0 END) AS hs,
                SUM(winning_kill) AS winning_kills,
                SUM(decided_kill) AS decided_kills,
                COUNT(DISTINCT match_id) AS matches
            FROM filtered_kills
            GROUP BY weapon
            ORDER BY kills DESC, weapon ASC
            LIMIT ?
            ''',
            base_params + (normalized_limit,),
        ).fetchall()

        map_leaders = conn.execute(
            cte_sql + '''
            , weapon_map AS (
                SELECT
                    map,
                    weapon,
                    COUNT(*) AS kills,
                    SUM(CASE WHEN location = 'head' THEN 1 ELSE 0 END) AS hs,
                    SUM(winning_kill) AS winning_kills,
                    SUM(decided_kill) AS decided_kills
                FROM filtered_kills
                GROUP BY map, weapon
            ),
            ranked AS (
                SELECT wm.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY wm.map
                           ORDER BY wm.kills DESC, wm.hs DESC, wm.weapon ASC
                       ) AS rn
                FROM weapon_map wm
            )
            SELECT
                map,
                weapon,
                kills,
                hs,
                winning_kills,
                decided_kills
            FROM ranked
            WHERE rn = 1
            ORDER BY kills DESC, map ASC
            LIMIT ?
            ''',
            base_params + (normalized_limit,),
        ).fetchall()

    summary_dict = dict(summary) if summary else {}
    decided_kills = int(summary_dict.get('decided_kills') or 0)
    summary_dict['global_weapon_win_rate'] = (
        round((100.0 * int(summary_dict.get('winning_kills') or 0)) / decided_kills, 1)
        if decided_kills else 0
    )

    weapon_rows = []
    for row in top_weapons:
        d = dict(row)
        kills = int(d.get('kills') or 0)
        matches = int(d.get('matches') or 0)
        decided = int(d.get('decided_kills') or 0)
        d['hs_pct'] = round((100.0 * int(d.get('hs') or 0)) / kills, 1) if kills else 0
        d['win_rate'] = round((100.0 * int(d.get('winning_kills') or 0)) / decided, 1) if decided else 0
        d['avg_kills_per_match'] = round(float(kills) / max(matches, 1), 2)
        weapon_rows.append(d)

    map_rows = []
    for row in map_leaders:
        d = dict(row)
        kills = int(d.get('kills') or 0)
        decided = int(d.get('decided_kills') or 0)
        d['hs_pct'] = round((100.0 * int(d.get('hs') or 0)) / kills, 1) if kills else 0
        d['win_rate'] = round((100.0 * int(d.get('winning_kills') or 0)) / decided, 1) if decided else 0
        map_rows.append(d)

    result = {
        'summary': summary_dict,
        'top_weapons': weapon_rows,
        'map_leaders': map_rows,
    }
    _cache_set(cache_key, result)
    return result


# ── Weapon detail page ─────────────────────────────────────────────────────────

def get_weapon_detail(weapon: str,
                      period: str = _DEFAULT_H2H_PERIOD) -> dict:
    """Summary stats for a single weapon."""
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    weapon_key = (weapon or '').strip()
    cache_key = f'weapon_detail:{weapon_key}:{normalized_period}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        r = conn.execute(f'''
            SELECT
                COUNT(*)                                                           AS kills,
                SUM(CASE WHEN k.location = 'head'    THEN 1 ELSE 0 END)             AS loc_head,
                SUM(CASE WHEN k.location = 'stomach' THEN 1 ELSE 0 END)             AS loc_stomach,
                SUM(CASE WHEN k.location = 'chest'   THEN 1 ELSE 0 END)             AS loc_chest,
                SUM(CASE WHEN k.location = 'legs'    THEN 1 ELSE 0 END)             AS loc_legs,
                SUM(CASE WHEN k.team_kill = 1        THEN 1 ELSE 0 END)             AS team_kills,
                SUM(CASE WHEN k.team_kill = 0        THEN 1 ELSE 0 END)             AS enemy_kills,
                COUNT(DISTINCT k.killer)                                             AS unique_users,
                COUNT(DISTINCT k.match_id)                                           AS matches_used,
                ROUND(100.0 * SUM(CASE WHEN k.location = 'head' THEN 1 ELSE 0 END)
                      / MAX(SUM(CASE WHEN k.team_kill=0 THEN 1 ELSE 0 END), 1), 1)  AS hs_pct,
                SUM(CASE WHEN k.team_kill = 0
                           AND p.team IN (1,2)
                           AND ((p.team = 1 AND m.t1_rounds > m.t2_rounds) OR
                                (p.team = 2 AND m.t2_rounds > m.t1_rounds))
                         THEN 1 ELSE 0 END)                                          AS winning_kills,
                SUM(CASE WHEN k.team_kill = 0
                           AND p.team IN (1,2)
                           AND (m.t1_rounds + m.t2_rounds) > 0
                         THEN 1 ELSE 0 END)                                          AS decided_kills
            FROM kill_events k
            JOIN matches m ON m.id = k.match_id
            LEFT JOIN player_match_stats p
                   ON p.match_id = k.match_id
                  AND p.name = k.killer
            WHERE k.weapon = ? AND {period_clause}
        ''', (weapon_key,) + period_params).fetchone()
    result = dict(r) if r else {}
    if result:
        enemy_kills = int(result.get('enemy_kills') or 0)
        matches_used = int(result.get('matches_used') or 0)
        decided_kills = int(result.get('decided_kills') or 0)
        result['kill_win_rate'] = (
            round((100.0 * int(result.get('winning_kills') or 0)) / decided_kills, 1)
            if decided_kills else 0
        )
        result['kills_per_match'] = round(float(enemy_kills) / max(matches_used, 1), 2)
        # Proxy metric until per-weapon shots/hits telemetry is persisted.
        result['accuracy_proxy'] = float(result.get('hs_pct') or 0)
    _cache_set(cache_key, result)
    return result


def get_weapon_top_killers(weapon: str,
                           min_games: int = 1,
                           period: str = _DEFAULT_H2H_PERIOD) -> list:
    """Per-player kill stats with this weapon."""
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    weapon_key = (weapon or '').strip()
    normalized_min_games = max(1, int(min_games or 1))
    cache_key = f'weapon_top_killers:{weapon_key}:{normalized_period}:{normalized_min_games}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        rows = conn.execute(f'''
            SELECT
                k.killer                                                        AS name,
                COUNT(DISTINCT k.match_id)                                      AS games,
                COUNT(*)                                                        AS kills,
                SUM(CASE WHEN k.location = 'head' THEN 1 ELSE 0 END)           AS hs,
                ROUND(100.0 * SUM(CASE WHEN k.location = 'head' THEN 1 ELSE 0 END)
                      / MAX(COUNT(*), 1), 1)                                    AS hs_pct,
                ROUND(CAST(COUNT(*) AS REAL) / COUNT(DISTINCT k.match_id), 1)   AS avg_kills
            FROM kill_events k
            JOIN matches m ON m.id = k.match_id
            WHERE k.weapon = ? AND k.team_kill = 0 AND {period_clause}
            GROUP BY k.killer
            HAVING games >= ?
            ORDER BY kills DESC
        ''', (weapon_key,) + period_params + (normalized_min_games,)).fetchall()
    result = [dict(r) for r in rows]
    _cache_set(cache_key, result)
    return result


def get_weapon_victims(weapon: str,
                       period: str = _DEFAULT_H2H_PERIOD) -> list:
    """Top victims of this weapon (enemy kills only)."""
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    weapon_key = (weapon or '').strip()
    cache_key = f'weapon_victims:{weapon_key}:{normalized_period}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        rows = conn.execute(f'''
            SELECT
                k.victim                                                        AS name,
                COUNT(*)                                                        AS times_killed,
                SUM(CASE WHEN k.location = 'head' THEN 1 ELSE 0 END)           AS hs,
                ROUND(100.0 * SUM(CASE WHEN k.location = 'head' THEN 1 ELSE 0 END)
                      / MAX(COUNT(*), 1), 1)                                    AS hs_pct
            FROM kill_events k
            JOIN matches m ON m.id = k.match_id
            WHERE k.weapon = ? AND k.team_kill = 0 AND {period_clause}
            GROUP BY k.victim
            ORDER BY times_killed DESC
            LIMIT 30
        ''', (weapon_key,) + period_params).fetchall()
    result = [dict(r) for r in rows]
    _cache_set(cache_key, result)
    return result


def get_weapon_map_effectiveness(weapon: str,
                                 period: str = _DEFAULT_H2H_PERIOD,
                                 limit: int = 20) -> list:
    """Per-map effectiveness for a weapon, including kill-win proxy and HS profile."""
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    weapon_key = (weapon or '').strip()
    normalized_limit = max(1, min(int(limit), 100))
    cache_key = f'weapon_map_effectiveness:{weapon_key}:{normalized_period}:{normalized_limit}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        rows = conn.execute(f'''
            SELECT
                m.map,
                COUNT(*) AS kills,
                SUM(CASE WHEN k.location = 'head' THEN 1 ELSE 0 END) AS hs,
                COUNT(DISTINCT k.match_id) AS matches,
                SUM(CASE WHEN p.team IN (1,2)
                           AND ((p.team = 1 AND m.t1_rounds > m.t2_rounds) OR
                                (p.team = 2 AND m.t2_rounds > m.t1_rounds))
                         THEN 1 ELSE 0 END) AS winning_kills,
                SUM(CASE WHEN p.team IN (1,2)
                           AND (m.t1_rounds + m.t2_rounds) > 0
                         THEN 1 ELSE 0 END) AS decided_kills
            FROM kill_events k
            JOIN matches m ON m.id = k.match_id
            LEFT JOIN player_match_stats p
                   ON p.match_id = k.match_id
                  AND p.name = k.killer
            WHERE k.weapon = ?
              AND k.team_kill = 0
              AND m.map != ''
              AND {period_clause}
            GROUP BY m.map
            ORDER BY kills DESC, m.map ASC
            LIMIT ?
                ''', (weapon_key,) + period_params + (normalized_limit,)).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        kills = int(d.get('kills') or 0)
        matches = int(d.get('matches') or 0)
        decided = int(d.get('decided_kills') or 0)
        d['hs_pct'] = round((100.0 * int(d.get('hs') or 0)) / kills, 1) if kills else 0
        d['win_rate'] = round((100.0 * int(d.get('winning_kills') or 0)) / decided, 1) if decided else 0
        d['avg_kills_per_match'] = round(float(kills) / max(matches, 1), 2)
        result.append(d)
    _cache_set(cache_key, result)
    return result


# ── Map detail page ────────────────────────────────────────────────────────────

def get_map_detail(map_name: str,
                   period: str = _DEFAULT_H2H_PERIOD) -> dict:
    """Summary + win-rate stats for a single map."""
    period_clause, period_params, _ = _h2h_period_filter_clause(period)
    with _connect() as conn:
        r = conn.execute(f'''
            SELECT
                COUNT(DISTINCT m.id)                                              AS matches,
                SUM(m.total_kills)                                                AS total_kills,
                ROUND(CAST(SUM(m.total_kills) AS REAL) / COUNT(DISTINCT m.id), 1) AS avg_kills,
                COUNT(DISTINCT p.name)                                            AS unique_players,
                ROUND(AVG(m.duration) / 60.0, 1)                                  AS avg_duration_min,
                SUM(CASE WHEN m.t1_rounds > m.t2_rounds THEN 1 ELSE 0 END)        AS t1_wins,
                SUM(CASE WHEN m.t2_rounds > m.t1_rounds THEN 1 ELSE 0 END)        AS t2_wins,
                SUM(CASE WHEN m.t1_rounds = m.t2_rounds
                          AND (m.t1_rounds + m.t2_rounds) > 0 THEN 1 ELSE 0 END)  AS draws
            FROM matches m
            LEFT JOIN player_match_stats p ON p.match_id = m.id
            WHERE m.map = ? AND m.game_mode = 'tdm' AND {period_clause}
        ''', (map_name,) + period_params).fetchone()
    return dict(r) if r else {}


def get_map_leaderboard(map_name: str,
                        min_games: int = 1,
                        period: str = _DEFAULT_H2H_PERIOD) -> list:
    """Per-player aggregated stats on a single map."""
    period_clause, period_params, _ = _h2h_period_filter_clause(period)
    with _connect() as conn:
        rows = conn.execute(f'''
            SELECT
                p.name,
                COUNT(DISTINCT p.match_id)                                        AS games,
                SUM(p.kills)                                                      AS kills,
                SUM(p.deaths)                                                     AS deaths,
                SUM(p.team_kills)                                                 AS team_kills,
                SUM(p.damage)                                                     AS damage,
                SUM(p.hs_kills)                                                   AS hs_kills,
                MAX(p.kills)                                                      AS best_game,
                ROUND(CAST(SUM(p.kills) AS REAL) / MAX(SUM(p.deaths), 1), 2)     AS kd,
                ROUND(CAST(SUM(p.kills) AS REAL) / COUNT(DISTINCT p.match_id), 1) AS avg_kills,
                ROUND(AVG(CASE WHEN p.accuracy IS NOT NULL THEN p.accuracy END), 1) AS avg_acc
            FROM player_match_stats p
            JOIN matches m ON m.id = p.match_id
            WHERE m.map = ? AND m.game_mode = 'tdm' AND {period_clause}
              AND (p.kills > 0 OR p.deaths > 0)
            GROUP BY p.name
            HAVING games >= ?
            ORDER BY kills DESC
        ''', (map_name,) + period_params + (min_games,)).fetchall()
    return [dict(r) for r in rows]


def get_map_recent_matches(map_name: str,
                           limit: int = 20,
                           period: str = _DEFAULT_H2H_PERIOD) -> list:
    """Most recent matches on a single map."""
    period_clause, period_params, _ = _h2h_period_filter_clause(period)
    with _connect() as conn:
        rows = conn.execute(f'''
            SELECT m.path, m.map, m.played_at, m.duration,
                   m.total_kills, m.t1_rounds, m.t2_rounds, m.game_mode,
                   COUNT(p.id) AS player_count
            FROM matches m
            LEFT JOIN player_match_stats p ON p.match_id = m.id
            WHERE m.map = ? AND m.game_mode = 'tdm' AND {period_clause}
            GROUP BY m.id
            ORDER BY m.played_at DESC
            LIMIT ?
        ''', (map_name,) + period_params + (limit,)).fetchall()
    return [dict(r) for r in rows]


def _spatial_bucket(x: float, y: float, cell_size: float) -> tuple[int, int]:
    return int(math.floor(float(x) / cell_size)), int(math.floor(float(y) / cell_size))


def _build_spatial_cells(points: list,
                         cell_size: float,
                         limit: int) -> dict:
    buckets: dict = {}
    min_x = float('inf')
    max_x = float('-inf')
    min_y = float('inf')
    max_y = float('-inf')

    for x, y in points:
        fx = float(x)
        fy = float(y)
        if fx < min_x:
            min_x = fx
        if fx > max_x:
            max_x = fx
        if fy < min_y:
            min_y = fy
        if fy > max_y:
            max_y = fy
        b = _spatial_bucket(fx, fy, cell_size)
        buckets[b] = int(buckets.get(b, 0)) + 1

    cells = []
    for (ix, iy), count in buckets.items():
        cells.append({
            'x': round((ix + 0.5) * cell_size, 1),
            'y': round((iy + 0.5) * cell_size, 1),
            'count': int(count),
        })

    cells.sort(key=lambda c: (-int(c.get('count') or 0), float(c.get('y') or 0.0), float(c.get('x') or 0.0)))
    max_count = int(cells[0]['count']) if cells else 0
    for c in cells:
        c['intensity'] = round((float(c['count']) / max_count), 4) if max_count else 0.0

    clipped_limit = max(1, min(int(limit), 10000))
    cells = cells[:clipped_limit]

    if points:
        bounds = {
            'min_x': round(min_x, 1),
            'max_x': round(max_x, 1),
            'min_y': round(min_y, 1),
            'max_y': round(max_y, 1),
        }
    else:
        bounds = {'min_x': 0.0, 'max_x': 0.0, 'min_y': 0.0, 'max_y': 0.0}

    return {
        'cells': cells,
        'max_count': max_count,
        'total_points': len(points),
        'bounds': bounds,
    }


def get_map_heatmap(map_name: str,
                    kind: str = 'kills',
                    mode: str = _DEFAULT_STATS_MODE,
                    period: str = _DEFAULT_H2H_PERIOD,
                    week: Optional[str] = None,
                    cell_size: float = 128.0,
                    player: Optional[str] = None,
                    weapon: Optional[str] = None,
                    limit: int = 2000) -> dict:
    """Spatial kill/death/first-kill heatmap cells for a map."""
    normalized_kind = str(kind or 'kills').strip().lower().replace('-', '_')
    if normalized_kind not in ('kills', 'deaths', 'first_kills'):
        normalized_kind = 'kills'
    normalized_cell = max(24.0, min(float(cell_size or 128.0), 1024.0))
    normalized_limit = max(1, min(int(limit), 10000))
    player_filter = (player or '').strip()
    weapon_filter = (weapon or '').strip()
    mode = _normalize_mode_filter(mode)

    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )

    cache_key = (
        f'map_heat:{map_name.lower()}:{normalized_kind}:{mode}:{normalized_period}:'
        f'{normalized_week or "all_weeks"}:{normalized_cell:.1f}:{player_filter or "all"}:'
        f'{weapon_filter or "all"}:{normalized_limit}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )

        if normalized_kind == 'deaths':
            x_col = 'kp.victim_x'
            y_col = 'kp.victim_y'
            player_col = 'kp.victim'
            extra_join = ''
            first_kill_clause = ''
        else:
            x_col = 'kp.killer_x'
            y_col = 'kp.killer_y'
            player_col = 'kp.killer'
            if normalized_kind == 'first_kills':
                extra_join = 'JOIN rounds r ON r.id = kp.round_id'
                first_kill_clause = (
                    ' AND r.first_kill_player = kp.killer'
                    ' AND r.first_kill_frame = kp.frame_idx'
                )
            else:
                extra_join = ''
                first_kill_clause = ''

        extra_filters = []
        extra_params: tuple = ()
        if player_filter:
            extra_filters.append(f'{player_col} = ?')
            extra_params += (player_filter,)
        if weapon_filter:
            extra_filters.append('kp.weapon = ?')
            extra_params += (weapon_filter,)
        extra_filter_sql = ''
        if extra_filters:
            extra_filter_sql = ' AND ' + ' AND '.join(extra_filters)

        rows = conn.execute(f'''
            SELECT {x_col} AS x,
                   {y_col} AS y
            FROM kill_positions kp
            JOIN matches m ON m.id = kp.match_id
            {extra_join}
            WHERE m.map = ?
              AND kp.team_kill = 0
              AND {x_col} IS NOT NULL
              AND {y_col} IS NOT NULL
              AND {mode_clause}
              AND {period_clause}
              AND {week_clause}
              {first_kill_clause}
              {extra_filter_sql}
            LIMIT 250000
        ''', (map_name,) + mode_params + period_params + week_params + extra_params).fetchall()

    points = [(float(r['x']), float(r['y'])) for r in rows if r['x'] is not None and r['y'] is not None]
    packed = _build_spatial_cells(points, normalized_cell, normalized_limit)

    result = {
        'map': map_name,
        'kind': normalized_kind,
        'mode': mode,
        'period': normalized_period,
        'week': normalized_week,
        'cell_size': normalized_cell,
        'player': player_filter or None,
        'weapon': weapon_filter or None,
        **packed,
    }
    _cache_set(cache_key, result)
    return result


def get_map_movement_analytics(map_name: str,
                               mode: str = _DEFAULT_STATS_MODE,
                               period: str = _DEFAULT_H2H_PERIOD,
                               week: Optional[str] = None,
                               player: Optional[str] = None,
                               cell_size: float = 128.0,
                               route_cell_size: float = 192.0,
                               sample_limit: int = 200000,
                               route_limit: int = 25,
                               max_frame_gap: int = 40) -> dict:
    """Movement density heatmap and common route extraction for a map."""
    normalized_cell = max(24.0, min(float(cell_size or 128.0), 1024.0))
    normalized_route_cell = max(24.0, min(float(route_cell_size or 192.0), 1024.0))
    normalized_sample_limit = max(1000, min(int(sample_limit), 500000))
    normalized_route_limit = max(1, min(int(route_limit), 200))
    normalized_max_gap = max(1, min(int(max_frame_gap), 300))
    player_filter = (player or '').strip()
    mode = _normalize_mode_filter(mode)

    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )

    cache_key = (
        f'map_movement:{map_name.lower()}:{mode}:{normalized_period}:{normalized_week or "all_weeks"}:'
        f'{player_filter or "all"}:{normalized_cell:.1f}:{normalized_route_cell:.1f}:'
        f'{normalized_sample_limit}:{normalized_route_limit}:{normalized_max_gap}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )

        player_sql = ''
        player_params: tuple = ()
        if player_filter:
            player_sql = ' AND ps.name = ?'
            player_params = (player_filter,)

        rows = conn.execute(f'''
            SELECT
                ps.match_id,
                ps.name,
                ps.frame_idx,
                ps.x,
                ps.y
            FROM position_samples ps
            JOIN matches m ON m.id = ps.match_id
            WHERE m.map = ?
              AND {mode_clause}
              AND {period_clause}
              AND {week_clause}
              {player_sql}
            ORDER BY ps.match_id ASC, ps.name ASC, ps.frame_idx ASC
            LIMIT ?
        ''', (map_name,) + mode_params + period_params + week_params + player_params + (normalized_sample_limit,)).fetchall()

    points = [(float(r['x']), float(r['y'])) for r in rows]
    packed = _build_spatial_cells(points, normalized_cell, limit=5000)

    route_buckets: dict = {}
    prev_key = None
    prev_frame = 0
    prev_x = 0.0
    prev_y = 0.0

    for r in rows:
        cur_key = (int(r['match_id']), str(r['name']))
        cur_frame = int(r['frame_idx'] or 0)
        cur_x = float(r['x'] or 0.0)
        cur_y = float(r['y'] or 0.0)

        if prev_key == cur_key:
            frame_gap = cur_frame - prev_frame
            if 0 < frame_gap <= normalized_max_gap:
                from_bucket = _spatial_bucket(prev_x, prev_y, normalized_route_cell)
                to_bucket = _spatial_bucket(cur_x, cur_y, normalized_route_cell)
                if from_bucket != to_bucket:
                    rk = (from_bucket, to_bucket)
                    bucket = route_buckets.get(rk)
                    if bucket is None:
                        route_buckets[rk] = {'count': 1, 'total_gap': frame_gap}
                    else:
                        bucket['count'] = int(bucket.get('count', 0)) + 1
                        bucket['total_gap'] = int(bucket.get('total_gap', 0)) + frame_gap

        prev_key = cur_key
        prev_frame = cur_frame
        prev_x = cur_x
        prev_y = cur_y

    routes = []
    for (from_bucket, to_bucket), stats in route_buckets.items():
        count = int(stats.get('count') or 0)
        total_gap = int(stats.get('total_gap') or 0)
        routes.append({
            'from_x': round((from_bucket[0] + 0.5) * normalized_route_cell, 1),
            'from_y': round((from_bucket[1] + 0.5) * normalized_route_cell, 1),
            'to_x': round((to_bucket[0] + 0.5) * normalized_route_cell, 1),
            'to_y': round((to_bucket[1] + 0.5) * normalized_route_cell, 1),
            'count': count,
            'avg_frame_gap': round(float(total_gap) / count, 1) if count else 0.0,
        })

    routes.sort(key=lambda r: (-int(r.get('count') or 0), r['from_y'], r['from_x'], r['to_y'], r['to_x']))
    max_route_count = int(routes[0]['count']) if routes else 0
    for r in routes:
        r['intensity'] = round((float(r['count']) / max_route_count), 4) if max_route_count else 0.0
    routes = routes[:normalized_route_limit]

    result = {
        'map': map_name,
        'mode': mode,
        'period': normalized_period,
        'week': normalized_week,
        'player': player_filter or None,
        'cell_size': normalized_cell,
        'route_cell_size': normalized_route_cell,
        'sample_count': len(rows),
        'heatmap': packed.get('cells', []),
        'heatmap_max_count': packed.get('max_count', 0),
        'heatmap_total_points': packed.get('total_points', 0),
        'bounds': packed.get('bounds', {'min_x': 0.0, 'max_x': 0.0, 'min_y': 0.0, 'max_y': 0.0}),
        'routes': routes,
    }
    _cache_set(cache_key, result)
    return result


def get_map_fk_routes(map_name: str,
                      mode: str = _DEFAULT_STATS_MODE,
                      period: str = _DEFAULT_H2H_PERIOD,
                      week: Optional[str] = None,
                      player: Optional[str] = None,
                      cell_size: float = 128.0,
                      route_cell_size: float = 192.0,
                      lookback_frames: int = 80,
                      route_limit: int = 25) -> dict:
    """Approach routes taken by players leading up to their first kill in a round."""
    normalized_cell = max(24.0, min(float(cell_size or 128.0), 1024.0))
    normalized_route_cell = max(24.0, min(float(route_cell_size or 192.0), 1024.0))
    normalized_lookback = max(10, min(int(lookback_frames), 300))
    normalized_limit = max(1, min(int(route_limit), 200))
    player_filter = (player or '').strip()
    mode = _normalize_mode_filter(mode)

    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )

    cache_key = (
        f'map_fk_routes:{map_name.lower()}:{mode}:{normalized_period}:{normalized_week or "all_weeks"}:'
        f'{player_filter or "all"}:{normalized_cell:.1f}:{normalized_route_cell:.1f}:'
        f'{normalized_lookback}:{normalized_limit}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )

        player_sql = ''
        player_params: tuple = ()
        if player_filter:
            player_sql = ' AND ps.name = ?'
            player_params = (player_filter,)

        rows = conn.execute(f'''
            SELECT ps.round_id,
                   ps.frame_idx,
                   ps.x,
                   ps.y
            FROM position_samples ps
            JOIN rounds r ON r.id = ps.round_id
            JOIN matches m ON m.id = ps.match_id
            WHERE m.map = ?
              AND r.first_kill_player IS NOT NULL
              AND r.first_kill_player != ''
              AND r.first_kill_player = ps.name
              AND r.first_kill_frame IS NOT NULL
              AND ps.frame_idx <= r.first_kill_frame
              AND ps.frame_idx >= r.first_kill_frame - ?
              AND {mode_clause}
              AND {period_clause}
              AND {week_clause}
              {player_sql}
            ORDER BY ps.round_id ASC, ps.frame_idx ASC
            LIMIT 100000
        ''', (map_name, normalized_lookback) + mode_params + period_params + week_params + player_params).fetchall()

    points = [(float(r['x']), float(r['y'])) for r in rows]
    packed = _build_spatial_cells(points, normalized_cell, limit=2000)

    route_buckets: dict = {}
    prev_round_id: Optional[int] = None
    prev_frame = 0
    prev_x = 0.0
    prev_y = 0.0

    for r in rows:
        cur_round_id = int(r['round_id'] or 0)
        cur_frame = int(r['frame_idx'] or 0)
        cur_x = float(r['x'] or 0.0)
        cur_y = float(r['y'] or 0.0)

        if prev_round_id == cur_round_id:
            frame_gap = cur_frame - prev_frame
            if 0 < frame_gap <= 40:
                from_bucket = _spatial_bucket(prev_x, prev_y, normalized_route_cell)
                to_bucket = _spatial_bucket(cur_x, cur_y, normalized_route_cell)
                if from_bucket != to_bucket:
                    rk = (from_bucket, to_bucket)
                    bucket = route_buckets.get(rk)
                    if bucket is None:
                        route_buckets[rk] = {'count': 1}
                    else:
                        bucket['count'] = int(bucket.get('count', 0)) + 1

        prev_round_id = cur_round_id
        prev_frame = cur_frame
        prev_x = cur_x
        prev_y = cur_y

    routes = []
    for (from_bucket, to_bucket), stats in route_buckets.items():
        count = int(stats.get('count') or 0)
        routes.append({
            'from_x': round((from_bucket[0] + 0.5) * normalized_route_cell, 1),
            'from_y': round((from_bucket[1] + 0.5) * normalized_route_cell, 1),
            'to_x': round((to_bucket[0] + 0.5) * normalized_route_cell, 1),
            'to_y': round((to_bucket[1] + 0.5) * normalized_route_cell, 1),
            'count': count,
        })

    routes.sort(key=lambda r: -int(r.get('count') or 0))
    max_route_count = int(routes[0]['count']) if routes else 0
    for r in routes:
        r['intensity'] = round(float(r['count']) / max_route_count, 4) if max_route_count else 0.0
    routes = routes[:normalized_limit]

    total_rounds = len(set(int(r['round_id'] or 0) for r in rows)) if rows else 0

    result = {
        'map': map_name,
        'mode': mode,
        'period': normalized_period,
        'week': normalized_week,
        'player': player_filter or None,
        'cell_size': normalized_cell,
        'route_cell_size': normalized_route_cell,
        'cells': packed.get('cells', []),
        'max_count': packed.get('max_count', 0),
        'total_points': packed.get('total_points', 0),
        'bounds': packed.get('bounds', {'min_x': 0.0, 'max_x': 0.0, 'min_y': 0.0, 'max_y': 0.0}),
        'routes': routes,
        'total_rounds': total_rounds,
    }
    _cache_set(cache_key, result)
    return result


def get_map_zone_risk(map_name: str,
                      mode: str = _DEFAULT_STATS_MODE,
                      period: str = _DEFAULT_H2H_PERIOD,
                      week: Optional[str] = None,
                      cell_size: float = 160.0,
                      min_events: int = 2,
                      limit: int = 120,
                      player: Optional[str] = None,
                      weapon: Optional[str] = None) -> dict:
    """Danger/safe zone scoring from kill-vs-death cell ratios."""
    normalized_cell = max(24.0, min(float(cell_size or 160.0), 1024.0))
    normalized_limit = max(1, min(int(limit), 5000))
    normalized_min_events = max(1, min(int(min_events), 1000))
    player_filter = (player or '').strip()
    weapon_filter = (weapon or '').strip()
    mode = _normalize_mode_filter(mode)

    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )
    cache_key = (
        f'map_zone_risk:{map_name.lower()}:{mode}:{normalized_period}:{normalized_week or "all_weeks"}:'
        f'{normalized_cell:.1f}:{normalized_min_events}:{normalized_limit}:'
        f'{player_filter or "all"}:{weapon_filter or "all"}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )

        extra_filters = []
        extra_params: tuple = ()
        if player_filter:
            extra_filters.append('(kp.killer = ? OR kp.victim = ?)')
            extra_params += (player_filter, player_filter)
        if weapon_filter:
            extra_filters.append('kp.weapon = ?')
            extra_params += (weapon_filter,)
        extra_filter_sql = ''
        if extra_filters:
            extra_filter_sql = ' AND ' + ' AND '.join(extra_filters)

        rows = conn.execute(f'''
            SELECT
                kp.killer_x,
                kp.killer_y,
                kp.victim_x,
                kp.victim_y
            FROM kill_positions kp
            JOIN matches m ON m.id = kp.match_id
            WHERE m.map = ?
              AND kp.team_kill = 0
              AND {mode_clause}
              AND {period_clause}
              AND {week_clause}
              {extra_filter_sql}
            LIMIT 300000
        ''', (map_name,) + mode_params + period_params + week_params + extra_params).fetchall()

    kills_by_cell: dict = {}
    deaths_by_cell: dict = {}

    for r in rows:
        if r['killer_x'] is not None and r['killer_y'] is not None:
            kbucket = _spatial_bucket(float(r['killer_x']), float(r['killer_y']), normalized_cell)
            kills_by_cell[kbucket] = int(kills_by_cell.get(kbucket, 0)) + 1
        if r['victim_x'] is not None and r['victim_y'] is not None:
            dbucket = _spatial_bucket(float(r['victim_x']), float(r['victim_y']), normalized_cell)
            deaths_by_cell[dbucket] = int(deaths_by_cell.get(dbucket, 0)) + 1

    all_cells = set(kills_by_cell.keys()) | set(deaths_by_cell.keys())
    enriched = []
    raw_scores = []
    for bucket in all_cells:
        kills = int(kills_by_cell.get(bucket, 0))
        deaths = int(deaths_by_cell.get(bucket, 0))
        total = kills + deaths
        if total < normalized_min_events:
            continue
        risk_ratio = (float(deaths) + 1.0) / (float(kills) + 1.0)
        activity_weight = 1.0 + (math.log1p(total) / 3.0)
        raw_score = risk_ratio * activity_weight
        raw_scores.append(raw_score)
        enriched.append({
            'x': round((bucket[0] + 0.5) * normalized_cell, 1),
            'y': round((bucket[1] + 0.5) * normalized_cell, 1),
            'kills': kills,
            'deaths': deaths,
            'total': total,
            'risk_ratio': round(risk_ratio, 3),
            '_raw': raw_score,
        })

    if raw_scores:
        min_raw = min(raw_scores)
        max_raw = max(raw_scores)
    else:
        min_raw = 0.0
        max_raw = 0.0

    for item in enriched:
        if max_raw > min_raw:
            danger = 100.0 * (float(item['_raw']) - min_raw) / (max_raw - min_raw)
        else:
            danger = 50.0 if item['total'] else 0.0
        item['danger_score'] = round(danger, 1)
        item['safe_score'] = round(100.0 - item['danger_score'], 1)
        item.pop('_raw', None)

    enriched.sort(key=lambda c: (-int(c.get('total') or 0), -float(c.get('danger_score') or 0.0), c['y'], c['x']))
    cells = enriched[:normalized_limit]

    dangerous = sorted(
        cells,
        key=lambda c: (-float(c.get('danger_score') or 0.0), -int(c.get('total') or 0), c['y'], c['x'])
    )[:10]
    safe = sorted(
        cells,
        key=lambda c: (-float(c.get('safe_score') or 0.0), -int(c.get('total') or 0), c['y'], c['x'])
    )[:10]

    result = {
        'map': map_name,
        'mode': mode,
        'period': normalized_period,
        'week': normalized_week,
        'cell_size': normalized_cell,
        'min_events': normalized_min_events,
        'player': player_filter or None,
        'weapon': weapon_filter or None,
        'cells': cells,
        'dangerous': dangerous,
        'safe': safe,
    }
    _cache_set(cache_key, result)
    return result


def _stddev(values: list) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return 0.0
    mean = sum(values) / float(len(values))
    var = sum((v - mean) * (v - mean) for v in values) / float(len(values))
    return math.sqrt(max(var, 0.0))


def get_map_spawn_analytics(map_name: str,
                            mode: str = _DEFAULT_STATS_MODE,
                            period: str = _DEFAULT_H2H_PERIOD,
                            week: Optional[str] = None,
                            cell_size: float = 192.0,
                            limit: int = 30) -> dict:
    """Spawn pairing and early-round outcome analytics for a map."""
    normalized_cell = max(24.0, min(float(cell_size or 192.0), 1024.0))
    normalized_limit = max(1, min(int(limit), 200))
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )
    cache_key = (
        f'map_spawn:{map_name.lower()}:{mode}:{normalized_period}:{normalized_week or "all_weeks"}:'
        f'{normalized_cell:.1f}:{normalized_limit}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )
        base_params = (map_name,) + mode_params + period_params + week_params

        pos_rows = conn.execute(f'''
            SELECT
                ps.round_id,
                ps.name,
                ps.team,
                ps.frame_idx,
                ps.x,
                ps.y
            FROM position_samples ps
            JOIN matches m ON m.id = ps.match_id
            WHERE m.map = ?
              AND ps.round_id IS NOT NULL
              AND ps.team IN (1,2)
              AND {mode_clause}
              AND {period_clause}
              AND {week_clause}
            ORDER BY ps.round_id ASC, ps.name ASC, ps.frame_idx ASC
            LIMIT 400000
        ''', base_params).fetchall()

        round_rows = conn.execute(f'''
            SELECT
                r.id AS round_id,
                r.winner_team,
                r.first_kill_team
            FROM rounds r
            JOIN matches m ON m.id = r.match_id
            WHERE m.map = ?
              AND {mode_clause}
              AND {period_clause}
              AND {week_clause}
        ''', base_params).fetchall()

    # Earliest sample per player in each round approximates spawn position.
    first_sample_seen = set()
    round_samples: dict = {}
    for row in pos_rows:
        rid = int(row['round_id'])
        name = str(row['name'])
        rteam = int(row['team'] or 0)
        if rteam not in (1, 2):
            continue
        skey = (rid, name)
        if skey in first_sample_seen:
            continue
        first_sample_seen.add(skey)
        slot = round_samples.get(rid)
        if slot is None:
            slot = {'t1': [], 't2': []}
            round_samples[rid] = slot
        slot['t1' if rteam == 1 else 't2'].append((float(row['x']), float(row['y'])))

    round_outcome = {
        int(r['round_id']): {
            'winner_team': int(r['winner_team'] or 0),
            'first_kill_team': int(r['first_kill_team'] or 0),
        }
        for r in round_rows
    }

    pair_stats: dict = {}
    for rid, samples in round_samples.items():
        if rid not in round_outcome:
            continue
        t1 = samples.get('t1') or []
        t2 = samples.get('t2') or []
        if not t1 or not t2:
            continue

        t1x = sum(x for x, _ in t1) / float(len(t1))
        t1y = sum(y for _, y in t1) / float(len(t1))
        t2x = sum(x for x, _ in t2) / float(len(t2))
        t2y = sum(y for _, y in t2) / float(len(t2))

        b1 = _spatial_bucket(t1x, t1y, normalized_cell)
        b2 = _spatial_bucket(t2x, t2y, normalized_cell)
        pkey = (b1, b2)
        bucket = pair_stats.get(pkey)
        if bucket is None:
            bucket = {
                't1_bucket': b1,
                't2_bucket': b2,
                'rounds': 0,
                'team1_wins': 0,
                'team2_wins': 0,
                'team1_first_kills': 0,
                'team2_first_kills': 0,
            }
            pair_stats[pkey] = bucket

        bucket['rounds'] += 1
        winner = int(round_outcome[rid]['winner_team'] or 0)
        first_team = int(round_outcome[rid]['first_kill_team'] or 0)
        if winner == 1:
            bucket['team1_wins'] += 1
        elif winner == 2:
            bucket['team2_wins'] += 1
        if first_team == 1:
            bucket['team1_first_kills'] += 1
        elif first_team == 2:
            bucket['team2_first_kills'] += 1

    pair_rows = []
    for bucket in pair_stats.values():
        rounds = int(bucket['rounds'])
        if rounds <= 0:
            continue
        t1_win_rate = round((100.0 * int(bucket['team1_wins'])) / rounds, 1)
        t2_win_rate = round((100.0 * int(bucket['team2_wins'])) / rounds, 1)
        pair_rows.append({
            'team1_spawn_x': round((bucket['t1_bucket'][0] + 0.5) * normalized_cell, 1),
            'team1_spawn_y': round((bucket['t1_bucket'][1] + 0.5) * normalized_cell, 1),
            'team2_spawn_x': round((bucket['t2_bucket'][0] + 0.5) * normalized_cell, 1),
            'team2_spawn_y': round((bucket['t2_bucket'][1] + 0.5) * normalized_cell, 1),
            'rounds': rounds,
            'team1_win_rate': t1_win_rate,
            'team2_win_rate': t2_win_rate,
            'team1_first_kill_rate': round((100.0 * int(bucket['team1_first_kills'])) / rounds, 1),
            'team2_first_kill_rate': round((100.0 * int(bucket['team2_first_kills'])) / rounds, 1),
            'spawn_advantage': round(abs(t1_win_rate - t2_win_rate), 1),
        })

    pair_rows.sort(key=lambda r: (-int(r.get('rounds') or 0), -float(r.get('spawn_advantage') or 0.0)))
    pair_rows = pair_rows[:normalized_limit]

    result = {
        'map': map_name,
        'mode': mode,
        'period': normalized_period,
        'week': normalized_week,
        'cell_size': normalized_cell,
        'pairings': pair_rows,
        'summary': {
            'pairs': len(pair_rows),
            'rounds': sum(int(r.get('rounds') or 0) for r in pair_rows),
            'max_spawn_advantage': max((float(r.get('spawn_advantage') or 0.0) for r in pair_rows), default=0.0),
        },
    }
    _cache_set(cache_key, result)
    return result


def get_behavior_analytics(mode: str = _DEFAULT_STATS_MODE,
                           period: str = _DEFAULT_H2H_PERIOD,
                           week: Optional[str] = None,
                           min_games: int = 3,
                           limit: int = 100) -> dict:
    """Classify player behavior profiles with movement and round-impact signals."""
    mode = _normalize_mode_filter(mode)
    normalized_min_games = max(1, min(int(min_games), 200))
    normalized_limit = max(1, min(int(limit), 500))
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )
    cache_key = (
        f'behavior:{mode}:{normalized_period}:{normalized_week or "all_weeks"}:'
        f'{normalized_min_games}:{normalized_limit}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )
        base_params = mode_params + period_params + week_params

        round_rows = conn.execute(f'''
            SELECT
                prs.name,
                COUNT(*) AS rounds,
                COUNT(DISTINCT prs.match_id) AS games,
                SUM(prs.kills) AS kills,
                SUM(prs.deaths) AS deaths,
                SUM(prs.damage) AS damage,
                SUM(prs.first_kill) AS first_kills,
                SUM(prs.first_death) AS first_deaths,
                SUM(prs.survived) AS survived
            FROM player_round_stats prs
            JOIN matches m ON m.id = prs.match_id
            WHERE {mode_clause} AND {period_clause} AND {week_clause}
            GROUP BY prs.name
            HAVING games >= ?
            ORDER BY rounds DESC
            LIMIT ?
        ''', base_params + (normalized_min_games, max(normalized_limit * 4, normalized_limit))).fetchall()

        names = [str(r['name']) for r in round_rows]
        if not names:
            result = {
                'summary': {'players': 0},
                'players': [],
                'mode': mode,
                'period': normalized_period,
                'week': normalized_week,
            }
            _cache_set(cache_key, result)
            return result

        name_clause, name_params = _in_clause('ps.name', names)
        # round_kills lets us gate distance_per_kill to only rounds where the
        # player actually got a kill (per community proposal): a round where
        # you never killed anyone is not "distance spent hunting". prs is
        # keyed on UNIQUE(round_id, name), so the join hits that index.
        pos_rows = conn.execute(f'''
            SELECT
                ps.match_id,
                ps.name,
                ps.frame_idx,
                ps.x,
                ps.y,
                COALESCE(prs.kills, 0) AS round_kills
            FROM position_samples ps
            JOIN matches m ON m.id = ps.match_id
            LEFT JOIN player_round_stats prs
              ON prs.round_id = ps.round_id
             AND prs.name     = ps.name
            WHERE {name_clause}
              AND {mode_clause}
              AND {period_clause}
              AND {week_clause}
            ORDER BY ps.match_id ASC, ps.name ASC, ps.frame_idx ASC
            LIMIT 500000
        ''', name_params + base_params).fetchall()

        pms_name_clause, pms_name_params = _in_clause('p.name', names)
        perf_rows = conn.execute(f'''
            SELECT
                p.name,
                p.match_id,
                p.kills,
                p.deaths,
                p.damage
            FROM player_match_stats p
            JOIN matches m ON m.id = p.match_id
            WHERE {pms_name_clause}
              AND {mode_clause}
              AND {period_clause}
              AND {week_clause}
            ORDER BY p.name ASC, p.match_id ASC
        ''', pms_name_params + base_params).fetchall()

    travel_by_name: dict = defaultdict(float)
    # Distance collected only from rounds where the player had >=1 kill.
    # Used for distance_per_kill so wasted/dead rounds don't inflate the metric.
    travel_kill_rounds: dict = defaultdict(float)
    prev_sample: dict = {}
    for row in pos_rows:
        match_id = int(row['match_id'])
        name = str(row['name'])
        frame = int(row['frame_idx'] or 0)
        x = float(row['x'] or 0.0)
        y = float(row['y'] or 0.0)
        round_kills = int(row['round_kills'] or 0)
        key = (match_id, name)
        prev = prev_sample.get(key)
        if prev is not None:
            frame_gap = frame - int(prev['frame'])
            if 0 < frame_gap <= 80:
                dist = math.hypot(x - float(prev['x']), y - float(prev['y']))
                travel_by_name[name] += dist
                if round_kills > 0:
                    travel_kill_rounds[name] += dist
        prev_sample[key] = {'frame': frame, 'x': x, 'y': y}

    perf_by_name: dict = defaultdict(list)
    for row in perf_rows:
        name = str(row['name'])
        kills = int(row['kills'] or 0)
        deaths = int(row['deaths'] or 0)
        damage = int(row['damage'] or 0)
        perf_by_name[name].append(float(kills - deaths) + (float(damage) / 500.0))

    players = []
    profile_counts: dict = defaultdict(int)
    for row in round_rows:
        name = str(row['name'])
        games = int(row['games'] or 0)
        rounds = int(row['rounds'] or 0)
        kills = int(row['kills'] or 0)
        deaths = int(row['deaths'] or 0)
        first_kills = int(row['first_kills'] or 0)
        first_deaths = int(row['first_deaths'] or 0)
        survived = int(row['survived'] or 0)
        damage = int(row['damage'] or 0)

        kills_per_round = float(kills) / max(rounds, 1)
        first_kill_rate = (100.0 * float(first_kills)) / max(rounds, 1)
        first_death_rate = (100.0 * float(first_deaths)) / max(rounds, 1)
        survival_rate = (100.0 * float(survived)) / max(rounds, 1)
        distance_total = float(travel_by_name.get(name, 0.0))
        distance_per_round = distance_total / max(rounds, 1)
        # distance_per_kill: distance travelled in rounds where the player got
        # at least one kill, divided by kills. Rounds with 0 kills (early
        # deaths, whiffs, warming spots) are excluded so the ratio reflects
        # "travel to secure a kill" rather than "total travel" including
        # dead-end rounds. Undefined for zero-kill players.
        distance_kill_rounds = float(travel_kill_rounds.get(name, 0.0))
        distance_per_kill = (distance_kill_rounds / kills) if kills > 0 else None
        damage_per_round = float(damage) / max(rounds, 1)

        # Aggression index: 0-100 composite with per-term caps so no single
        # signal can dominate the score. Kills/round is the primary driver;
        # first-kill rate rewards aggressive opening engagements; active-round
        # distance is capped because it's a secondary indicator; dying first
        # applies a bounded penalty.
        agg_kills = min(45.0, kills_per_round * 20.0)      # 2.25 kpR → +45
        agg_first = min(20.0, first_kill_rate * 0.5)        # 40% FK rate → +20
        agg_dist  = min(25.0, distance_per_round / 8.0)     # 200 u/round → +25
        agg_fd    = min(15.0, first_death_rate * 0.25)      # 60% FD rate → -15
        aggression = agg_kills + agg_first + agg_dist - agg_fd
        aggression = max(0.0, min(100.0, aggression))

        perf_list = perf_by_name.get(name, [])
        perf_avg = (sum(perf_list) / len(perf_list)) if perf_list else 0.0
        perf_std = _stddev(perf_list)
        consistency = 100.0 / (1.0 + (perf_std / max(abs(perf_avg), 1.0))) if perf_list else 0.0

        # Playstyle thresholds calibrated for active-round-filtered distance
        # samples (see mvd2.py position_samples filter). Typical in-round
        # travel is 80–180 u/round depending on map size and player habits.
        if aggression >= 65.0 and distance_per_round >= 120.0:
            profile = 'aggressive_roamer'
        elif aggression >= 58.0:
            profile = 'aggressive_anchor'
        elif survival_rate >= 70.0 and aggression <= 45.0:
            profile = 'passive_survivor'
        elif distance_per_round >= 140.0 and kills_per_round < 0.8:
            profile = 'support_rotator'
        else:
            profile = 'balanced'

        confidence = min(100.0, (games * 4.0) + (rounds * 0.5))
        profile_counts[profile] += 1
        players.append({
            'name': name,
            'games': games,
            'rounds': rounds,
            'kills': kills,
            'deaths': deaths,
            'kd': round(float(kills) / max(deaths, 1), 2),
            'kills_per_round': round(kills_per_round, 3),
            'damage_per_round': round(damage_per_round, 1),
            'distance_per_round': round(distance_per_round, 1),
            'distance_per_kill': round(distance_per_kill, 1) if distance_per_kill is not None else None,
            'first_kill_rate': round(first_kill_rate, 1),
            'first_death_rate': round(first_death_rate, 1),
            'survival_rate': round(survival_rate, 1),
            'aggression_index': round(aggression, 1),
            'consistency_score': round(consistency, 1),
            'skill_variance': round(perf_std, 2),
            'profile': profile,
            'profile_confidence': round(confidence, 1),
        })

    players.sort(key=lambda p: (-float(p.get('aggression_index') or 0.0), -int(p.get('games') or 0), p['name']))
    players = players[:normalized_limit]

    result = {
        'mode': mode,
        'period': normalized_period,
        'week': normalized_week,
        'summary': {
            'players': len(players),
            'avg_aggression': round((sum(float(p.get('aggression_index') or 0.0) for p in players) / len(players)), 1) if players else 0,
            'profiles': dict(profile_counts),
        },
        'players': players,
    }
    _cache_set(cache_key, result)
    return result


def get_player_behavior_analytics(name: str,
                                  mode: str = _DEFAULT_STATS_MODE,
                                  period: str = _DEFAULT_H2H_PERIOD,
                                  week: Optional[str] = None) -> dict:
    """Behavior profile for one player (with identity alias support)."""
    with _connect() as conn:
        variants = _resolve_identity_variants(conn, name)
    pool = get_behavior_analytics(mode=mode, period=period, week=week, min_games=1, limit=500)
    rows = pool.get('players') or []
    by_name = {str(r.get('name')): r for r in rows}

    for variant in variants:
        if variant in by_name:
            return {
                'player': variant,
                'aliases': variants,
                'profile': by_name[variant],
                'mode': pool.get('mode'),
                'period': pool.get('period'),
                'week': pool.get('week'),
            }

    return {
        'player': name,
        'aliases': variants,
        'profile': None,
        'mode': pool.get('mode'),
        'period': pool.get('period'),
        'week': pool.get('week'),
    }


def _compute_rating_state(mode: str,
                          period: str,
                          week: Optional[str]) -> dict:
    mode = _normalize_mode_filter(mode)
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(
        period,
        played_at_col='m.played_at',
    )
    week_clause, week_params, normalized_week = _week_filter_clause(
        week,
        played_at_col='m.played_at',
    )
    cache_key = f'rating_state:{mode}:{normalized_period}:{normalized_week or "all_weeks"}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        mode_clause, mode_params = _mode_filter_clause(
            mode,
            detail_col='m.game_mode_detail',
            bucket_col='m.game_mode',
        )
        params = mode_params + period_params + week_params
        rows = conn.execute(f'''
            SELECT
                m.id AS match_id,
                m.path,
                m.map,
                m.played_at,
                m.t1_rounds,
                m.t2_rounds,
                p.name,
                p.team,
                p.kills,
                p.deaths,
                p.damage
            FROM matches m
            JOIN player_match_stats p ON p.match_id = m.id
            WHERE p.team IN (1,2)
              AND {mode_clause}
              AND {period_clause}
              AND {week_clause}
            ORDER BY m.played_at ASC, m.id ASC, p.team ASC, p.name ASC
        ''', params).fetchall()

    matches: dict = {}
    order = []
    for row in rows:
        match_id = int(row['match_id'])
        slot = matches.get(match_id)
        if slot is None:
            slot = {
                'id': match_id,
                'path': str(row['path'] or ''),
                'map': str(row['map'] or ''),
                'played_at': int(row['played_at'] or 0),
                't1_rounds': int(row['t1_rounds'] or 0),
                't2_rounds': int(row['t2_rounds'] or 0),
                'team1': [],
                'team2': [],
            }
            matches[match_id] = slot
            order.append(match_id)
        row_data = {
            'name': str(row['name']),
            'kills': int(row['kills'] or 0),
            'deaths': int(row['deaths'] or 0),
            'damage': int(row['damage'] or 0),
        }
        if int(row['team'] or 0) == 1:
            slot['team1'].append(row_data)
        else:
            slot['team2'].append(row_data)

    ratings: dict = {}
    history_by_name: dict = defaultdict(list)
    perf_by_name: dict = defaultdict(list)

    def _state(name: str) -> dict:
        st = ratings.get(name)
        if st is None:
            st = {
                'name': name,
                'rating': 1500.0,
                'rd': 350.0,
                'games': 0,
                'wins': 0,
                'draws': 0,
                'losses': 0,
                'kills': 0,
                'deaths': 0,
                'last_played': None,
                'deltas': [],
                'high_skill_perf': [],
            }
            ratings[name] = st
        return st

    for mid in order:
        m = matches[mid]
        t1 = m['team1']
        t2 = m['team2']
        if not t1 or not t2:
            continue

        played_at = int(m['played_at'] or 0)
        t1_score = 1.0 if m['t1_rounds'] > m['t2_rounds'] else (0.5 if m['t1_rounds'] == m['t2_rounds'] else 0.0)
        t2_score = 1.0 - t1_score if t1_score in (0.0, 1.0) else 0.5
        round_diff = abs(int(m['t1_rounds'] or 0) - int(m['t2_rounds'] or 0))

        # Inactivity decay before the match update.
        for player in t1 + t2:
            st = _state(player['name'])
            lp = st['last_played']
            if lp is not None and played_at > lp:
                days = float(played_at - lp) / 86400.0
                if days > 14.0:
                    regress = min(0.30, (days - 14.0) / 365.0)
                    st['rating'] = st['rating'] + ((1500.0 - st['rating']) * regress)
                    st['rd'] = min(350.0, st['rd'] + ((days - 14.0) * 0.45))

        t1_avg = sum(_state(p['name'])['rating'] for p in t1) / float(len(t1))
        t2_avg = sum(_state(p['name'])['rating'] for p in t2) / float(len(t2))
        exp1 = 1.0 / (1.0 + math.pow(10.0, (t2_avg - t1_avg) / 400.0))
        exp2 = 1.0 - exp1
        k_base = 20.0 + min(12.0, float(round_diff) * 2.0)

        for team_players, team_score, exp_score, opp_avg in ((t1, t1_score, exp1, t2_avg), (t2, t2_score, exp2, t1_avg)):
            for p in team_players:
                st = _state(p['name'])
                perf = float(p['kills'] - p['deaths']) + (float(p['damage']) / 500.0)
                perf_by_name[p['name']].append(perf)
                perf_adj = max(0.85, min(1.15, 1.0 + (perf / 50.0)))
                delta = k_base * perf_adj * (team_score - exp_score)

                st['rating'] += delta
                st['rd'] = max(55.0, st['rd'] * 0.985)
                st['games'] += 1
                st['kills'] += int(p['kills'])
                st['deaths'] += int(p['deaths'])
                if team_score > 0.5:
                    st['wins'] += 1
                elif team_score < 0.5:
                    st['losses'] += 1
                else:
                    st['draws'] += 1
                st['last_played'] = played_at
                st['deltas'].append(delta)
                if opp_avg >= 1550.0:
                    st['high_skill_perf'].append(perf)

                history_by_name[p['name']].append({
                    'played_at': played_at,
                    'rating': round(st['rating'], 2),
                    'uncertainty': round(st['rd'], 2),
                    'delta': round(delta, 2),
                    'path': m['path'],
                    'map': m['map'],
                })

    leaderboard = []
    for st in ratings.values():
        perf_list = perf_by_name.get(st['name'], [])
        perf_std = _stddev(perf_list)
        perf_avg = (sum(perf_list) / len(perf_list)) if perf_list else 0.0
        consistency = 100.0 / (1.0 + (perf_std / max(abs(perf_avg), 1.0))) if perf_list else 0.0
        trend_window = st['deltas'][-5:]
        trend = (sum(trend_window) / len(trend_window)) if trend_window else 0.0

        leaderboard.append({
            'name': st['name'],
            'rating': round(st['rating'], 1),
            'uncertainty': round(st['rd'], 1),
            'games': int(st['games']),
            'wins': int(st['wins']),
            'draws': int(st['draws']),
            'losses': int(st['losses']),
            'win_rate': round((100.0 * float(st['wins'])) / max(st['games'], 1), 1),
            'kills': int(st['kills']),
            'deaths': int(st['deaths']),
            'kd': round(float(st['kills']) / max(int(st['deaths']), 1), 2),
            'consistency_score': round(consistency, 1),
            'skill_variance': round(perf_std, 2),
            'high_skill_matches': len(st['high_skill_perf']),
            'high_skill_performance': round(
                (sum(st['high_skill_perf']) / len(st['high_skill_perf'])) if st['high_skill_perf'] else 0.0,
                2,
            ),
            'trend': round(trend, 2),
            'last_played': int(st['last_played'] or 0),
        })

    leaderboard.sort(key=lambda r: (-float(r.get('rating') or 0.0), -int(r.get('games') or 0), r['name']))
    for idx, row in enumerate(leaderboard, start=1):
        row['rank'] = idx

    result = {
        'mode': mode,
        'period': normalized_period,
        'week': normalized_week,
        'leaderboard': leaderboard,
        'history': dict(history_by_name),
    }
    _cache_set(cache_key, result)
    return result


def get_rating_rankings(mode: str = _DEFAULT_STATS_MODE,
                        period: str = _DEFAULT_H2H_PERIOD,
                        week: Optional[str] = None,
                        min_games: int = 5,
                        limit: int = 100) -> dict:
    """Rating leaderboard with uncertainty, trend, and consistency metrics."""
    normalized_limit = max(1, min(int(limit), 500))
    normalized_min_games = max(1, min(int(min_games), 200))
    state = _compute_rating_state(mode=mode, period=period, week=week)
    rows = [r for r in state.get('leaderboard', []) if int(r.get('games') or 0) >= normalized_min_games]
    rows = rows[:normalized_limit]

    summary = {
        'players': len(rows),
        'avg_rating': round((sum(float(r.get('rating') or 0.0) for r in rows) / len(rows)), 1) if rows else 0,
        'avg_uncertainty': round((sum(float(r.get('uncertainty') or 0.0) for r in rows) / len(rows)), 1) if rows else 0,
        'mode': state.get('mode'),
        'period': state.get('period'),
        'week': state.get('week'),
    }
    return {'summary': summary, 'leaderboard': rows}


def get_player_rating_history(name: str,
                              mode: str = _DEFAULT_STATS_MODE,
                              period: str = _DEFAULT_H2H_PERIOD,
                              week: Optional[str] = None,
                              limit: int = 80) -> dict:
    """Rating history for one player with alias resolution."""
    normalized_limit = max(1, min(int(limit), 500))
    state = _compute_rating_state(mode=mode, period=period, week=week)
    with _connect() as conn:
        variants = _resolve_identity_variants(conn, name)

    leaderboard = {str(r.get('name')): r for r in state.get('leaderboard', [])}
    selected = None
    best_games = -1
    for variant in variants:
        row = leaderboard.get(variant)
        if row and int(row.get('games') or 0) > best_games:
            selected = variant
            best_games = int(row.get('games') or 0)
    if selected is None:
        selected = name

    hist = list((state.get('history') or {}).get(selected, []))
    hist = hist[-normalized_limit:]
    return {
        'player': selected,
        'aliases': variants,
        'current': leaderboard.get(selected),
        'history': hist,
        'mode': state.get('mode'),
        'period': state.get('period'),
        'week': state.get('week'),
    }


_GEMINI_DEFAULT_MODEL = 'gemini-3.1-flash-lite-preview'
_GEMINI_MISSING_KEY_WARNED = False


def _cache_safe_token(value: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_.-]+', '_', str(value or '').strip().lower())


def _normalize_ai_provider(value: Optional[str]) -> str:
    raw = (value or '').strip().lower()
    if raw in ('gemini', 'hybrid'):
        return 'gemini'
    return 'heuristic'


def _get_ai_provider_config() -> tuple[str, str, str, int]:
    """Resolve AI provider runtime config from environment variables."""
    provider = _normalize_ai_provider(os.environ.get('AI_INSIGHTS_PROVIDER', 'heuristic'))
    api_key = (os.environ.get('GEMINI_API_KEY', '') or '').strip()
    model = (os.environ.get('GEMINI_MODEL', _GEMINI_DEFAULT_MODEL) or _GEMINI_DEFAULT_MODEL).strip()
    if not model:
        model = _GEMINI_DEFAULT_MODEL
    try:
        timeout_seconds = int(os.environ.get('GEMINI_TIMEOUT_SECONDS', '12'))
    except ValueError:
        timeout_seconds = 12
    timeout_seconds = max(3, min(timeout_seconds, 60))

    if provider == 'gemini' and not api_key:
        global _GEMINI_MISSING_KEY_WARNED
        if not _GEMINI_MISSING_KEY_WARNED:
            log.warning('stats: AI_INSIGHTS_PROVIDER=gemini but GEMINI_API_KEY is missing; using heuristic insights')
            _GEMINI_MISSING_KEY_WARNED = True
        provider = 'heuristic'
    return provider, api_key, model, timeout_seconds


def _extract_json_object_from_text(text: str) -> Optional[dict]:
    """Extract one JSON object from model output that may include markdown fences."""
    raw = (text or '').strip()
    if not raw:
        return None

    fenced = re.search(r'```(?:json)?\s*(\{.*\})\s*```', raw, re.IGNORECASE | re.DOTALL)
    if fenced:
        raw = fenced.group(1).strip()

    try:
        decoded = json.loads(raw)
        return decoded if isinstance(decoded, dict) else None
    except Exception:
        pass

    start = raw.find('{')
    end = raw.rfind('}')
    if start < 0 or end <= start:
        return None

    try:
        decoded = json.loads(raw[start:end + 1])
        return decoded if isinstance(decoded, dict) else None
    except Exception:
        return None


def _gemini_generate_json(prompt: str,
                          max_output_tokens: int = 700) -> tuple[Optional[dict], Optional[str]]:
    """Call Gemini API and parse a JSON object response."""
    provider, api_key, model, timeout_seconds = _get_ai_provider_config()
    if provider != 'gemini':
        return None, None

    endpoint_model = _urllib_parse.quote(model, safe='')
    endpoint = (
        f'https://generativelanguage.googleapis.com/v1beta/models/{endpoint_model}:generateContent'
        f'?key={api_key}'
    )
    payload = {
        'contents': [{'parts': [{'text': prompt}]}],
        'generationConfig': {
            'temperature': 0.2,
            'topP': 0.9,
            'maxOutputTokens': int(max(128, min(max_output_tokens, 2048))),
        },
    }
    req = _urllib_request.Request(
        endpoint,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST',
    )

    try:
        with _urllib_request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read(1024 * 1024)
        parsed = json.loads(body.decode('utf-8', errors='replace'))
    except _urllib_error.HTTPError as exc:
        try:
            detail = exc.read().decode('utf-8', errors='replace')
        except Exception:
            detail = ''
        log.warning('stats: Gemini request failed (HTTP %s): %s', getattr(exc, 'code', 'n/a'), detail[:240])
        return None, model
    except Exception as exc:
        log.warning('stats: Gemini request failed: %s', exc)
        return None, model

    candidates = parsed.get('candidates') or []
    if not candidates:
        return None, model
    first = candidates[0] if isinstance(candidates[0], dict) else {}
    content = first.get('content') if isinstance(first.get('content'), dict) else {}
    parts = content.get('parts') if isinstance(content.get('parts'), list) else []
    text = ''.join(str(p.get('text', '')) for p in parts if isinstance(p, dict)).strip()
    return _extract_json_object_from_text(text), model


def _sanitize_ai_insight_rows(rows,
                              limit: int) -> list:
    """Normalize LLM insight rows to the frontend schema."""
    cleaned = []
    if not isinstance(rows, list):
        return cleaned
    for item in rows:
        if not isinstance(item, dict):
            continue
        title = str(item.get('title', '') or '').strip()
        detail = str(item.get('detail', '') or '').strip()
        if not title or not detail:
            continue
        category = str(item.get('category', 'meta') or 'meta').strip().lower().replace(' ', '_')
        try:
            confidence = int(float(item.get('confidence', 70)))
        except Exception:
            confidence = 70
        cleaned.append({
            'category': category[:40],
            'title': title[:140],
            'detail': detail[:340],
            'confidence': int(min(99, max(50, confidence))),
        })
        if len(cleaned) >= limit:
            break
    return cleaned


def _sanitize_key_points(values,
                         limit: int = 6) -> list:
    if not isinstance(values, list):
        return []
    out = []
    for item in values:
        text = str(item or '').strip()
        if not text:
            continue
        out.append(text[:220])
        if len(out) >= limit:
            break
    return out


def _apply_gemini_meta_insights(base_result: dict,
                                context_payload: dict,
                                limit: int) -> dict:
    prompt = (
        'You are an analytics assistant for Action Quake 2 stats. '\
        'Use only the provided context values and avoid inventing numbers. '\
        'Return ONLY valid JSON with this exact schema: '\
        '{"insights":[{"category":"string","title":"string","detail":"string","confidence":0}]} '\
        f'Generate at most {int(limit)} insights. '\
        'Each detail must include concrete stats from context. '\
        f'Context JSON: {json.dumps(context_payload, ensure_ascii=True, separators=(",", ":"))}'
    )
    parsed, model = _gemini_generate_json(prompt, max_output_tokens=900)
    if not parsed:
        return base_result

    rows = _sanitize_ai_insight_rows(parsed.get('insights'), limit)
    if not rows:
        return base_result

    merged = dict(base_result)
    merged['insights'] = rows
    merged['generator'] = 'gemini'
    merged['model'] = model or _GEMINI_DEFAULT_MODEL
    return merged


def _apply_gemini_replay_summary(base_result: dict) -> dict:
    context_payload = {
        'path': base_result.get('path'),
        'map': base_result.get('map'),
        'played_at': base_result.get('played_at'),
        'lead_swings': base_result.get('lead_swings'),
        'headline': base_result.get('headline'),
        'summary': base_result.get('summary'),
        'top_performers': (base_result.get('top_performers') or [])[:3],
        'key_points': (base_result.get('key_points') or [])[:4],
    }
    prompt = (
        'You write concise match narratives for Action Quake 2. '\
        'Use only facts present in the supplied context. '\
        'Return ONLY valid JSON with schema: '\
        '{"headline":"string","summary":"string","key_points":["string"]}. '\
        'Keep summary under 3 sentences and key_points length between 2 and 6. '\
        f'Context JSON: {json.dumps(context_payload, ensure_ascii=True, separators=(",", ":"))}'
    )
    parsed, model = _gemini_generate_json(prompt, max_output_tokens=650)
    if not parsed:
        return base_result

    headline = str(parsed.get('headline', '') or '').strip()
    summary = str(parsed.get('summary', '') or '').strip()
    key_points = _sanitize_key_points(parsed.get('key_points'), limit=6)
    if not (headline or summary or key_points):
        return base_result

    merged = dict(base_result)
    if headline:
        merged['headline'] = headline[:160]
    if summary:
        merged['summary'] = summary[:500]
    if key_points:
        merged['key_points'] = key_points
    merged['generator'] = 'gemini'
    merged['model'] = model or _GEMINI_DEFAULT_MODEL
    return merged


def get_ai_meta_insights(mode: str = _DEFAULT_STATS_MODE,
                         period: str = _DEFAULT_H2H_PERIOD,
                         week: Optional[str] = None,
                         limit: int = 8) -> dict:
    """Strategy/meta insights with heuristic default and optional Gemini generation."""
    normalized_limit = max(1, min(int(limit), 20))
    mode = _normalize_mode_filter(mode)
    period_clause, _, normalized_period = _h2h_period_filter_clause(period)
    week_clause, _, normalized_week = _week_filter_clause(week)
    _ = period_clause, week_clause  # keep normalized values explicit

    provider, _, provider_model, _ = _get_ai_provider_config()
    provider_token = provider
    if provider == 'gemini':
        provider_token = f'gemini_{_cache_safe_token(provider_model)}'

    cache_key = (
        f'ai_meta:{provider_token}:{mode}:{normalized_period}:'
        f'{normalized_week or "all_weeks"}:{normalized_limit}'
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    first_kill = get_first_kill_stats(mode=mode, period=period, week=week, limit=10)
    rounds = get_round_analytics(mode=mode, period=period, week=week, limit=20)
    match = get_match_analytics(mode=mode, period=period, week=week, limit=10)
    weapons = get_weapon_analytics(mode=mode, period=period, week=week, limit=10)
    behavior = get_behavior_analytics(mode=mode, period=period, week=week, min_games=3, limit=20)
    ratings = get_rating_rankings(mode=mode, period=period, week=week, min_games=3, limit=20)

    insights = []

    top_weapon = (weapons.get('top_weapons') or [{}])[0]
    if top_weapon:
        insights.append({
            'category': 'weapon_meta',
            'title': f"{top_weapon.get('weapon', 'Unknown')} is defining engagements",
            'detail': (
                f"Top weapon win-rate proxy {top_weapon.get('win_rate', 0)}% across "
                f"{top_weapon.get('kills', 0)} enemy kills."
            ),
            'confidence': 82,
        })

    fk_hotspot = (first_kill.get('map_hotspots') or [{}])[0]
    if fk_hotspot:
        insights.append({
            'category': 'opening_control',
            'title': f"Openings on {fk_hotspot.get('map', 'this map')} skew toward {fk_hotspot.get('weapon', 'unknown')}",
            'detail': (
                f"Most common first-kill hotspot uses {fk_hotspot.get('weapon', 'unknown')} "
                f"to {fk_hotspot.get('location', 'unknown')} ({fk_hotspot.get('first_kills', 0)} rounds)."
            ),
            'confidence': 76,
        })

    rs = rounds.get('summary') or {}
    insights.append({
        'category': 'tempo',
        'title': 'Round tempo profile',
        'detail': (
            f"Early kills: {rs.get('early_kill_pct', 0)}%, late kills: {rs.get('late_kill_pct', 0)}%, "
            f"FK conversion: {rs.get('first_kill_conversion_rate', 0)}%."
        ),
        'confidence': 74,
    })

    ms = match.get('summary') or {}
    insights.append({
        'category': 'match_flow',
        'title': 'Match volatility signal',
        'detail': (
            f"Close matches {ms.get('close_match_rate', 0)}%, dominant matches {ms.get('dominant_match_rate', 0)}%, "
            f"comebacks {ms.get('comeback_rate', 0)}%."
        ),
        'confidence': 71,
    })

    profile_row = (behavior.get('players') or [{}])[0]
    if profile_row:
        insights.append({
            'category': 'playstyle',
            'title': f"{profile_row.get('name', 'Top player')} sets the pace as {profile_row.get('profile', 'balanced')}",
            'detail': (
                f"Aggression index {profile_row.get('aggression_index', 0)}, "
                f"distance/round {profile_row.get('distance_per_round', 0)}, "
                f"consistency {profile_row.get('consistency_score', 0)}."
            ),
            'confidence': int(min(95, max(60, profile_row.get('profile_confidence', 60)))),
        })

    top_rating = (ratings.get('leaderboard') or [{}])[0]
    if top_rating:
        insights.append({
            'category': 'ranking',
            'title': f"{top_rating.get('name', 'Leader')} leads the rating ladder",
            'detail': (
                f"Rating {top_rating.get('rating', 0)} ± {top_rating.get('uncertainty', 0)}, "
                f"win rate {top_rating.get('win_rate', 0)}%, trend {top_rating.get('trend', 0)}."
            ),
            'confidence': 80,
        })

    result = {
        'mode': mode,
        'period': normalized_period,
        'week': normalized_week,
        'insights': insights[:normalized_limit],
        'generator': 'heuristic',
    }

    if provider == 'gemini':
        context_payload = {
            'mode': mode,
            'period': normalized_period,
            'week': normalized_week,
            'first_kill_summary': first_kill.get('summary') or {},
            'first_kill_hotspots': (first_kill.get('map_hotspots') or [])[:4],
            'round_summary': rounds.get('summary') or {},
            'match_summary': match.get('summary') or {},
            'top_weapons': (weapons.get('top_weapons') or [])[:4],
            'playstyles': (behavior.get('players') or [])[:4],
            'rating_leaders': (ratings.get('leaderboard') or [])[:4],
            'heuristic_insights': result.get('insights') or [],
        }
        result = _apply_gemini_meta_insights(result, context_payload, normalized_limit)

    # Keep insights cached until the next indexer run clears the cache.
    # Default 5-min TTL is too short — each miss triggers heavy queries + Gemini.
    _cache_set(cache_key, result, ttl=86400.0)
    return result


def get_replay_ai_summary(path: str) -> dict:
    """AI-style narrative summary for one replay path using indexed match data."""
    provider, _, provider_model, _ = _get_ai_provider_config()
    provider_token = provider
    if provider == 'gemini':
        provider_token = f'gemini_{_cache_safe_token(provider_model)}'
    cache_key = f'replay_ai_summary:{provider_token}:{path}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _connect() as conn:
        match = conn.execute('''
            SELECT id, path, map, played_at, t1_rounds, t2_rounds, total_kills, duration
            FROM matches
            WHERE path = ?
            LIMIT 1
        ''', (path,)).fetchone()
        if not match:
            return {}

        top_players = conn.execute('''
            SELECT name, team, kills, deaths, damage
            FROM player_match_stats
            WHERE match_id = ?
            ORDER BY kills DESC, damage DESC
            LIMIT 5
        ''', (int(match['id']),)).fetchall()

        rounds = conn.execute('''
            SELECT round_index, winner_team, first_kill_team, first_kill_player
            FROM rounds
            WHERE match_id = ?
            ORDER BY round_index ASC
        ''', (int(match['id']),)).fetchall()

        duel = conn.execute('''
            SELECT killer, victim, COUNT(*) AS kills
            FROM kill_events
            WHERE match_id = ? AND team_kill = 0
            GROUP BY killer, victim
            ORDER BY kills DESC
            LIMIT 1
        ''', (int(match['id']),)).fetchone()

    t1_rounds = int(match['t1_rounds'] or 0)
    t2_rounds = int(match['t2_rounds'] or 0)
    if t1_rounds > t2_rounds:
        winner = 'Team 1'
    elif t2_rounds > t1_rounds:
        winner = 'Team 2'
    else:
        winner = 'Neither team (draw)'

    score_t1 = 0
    score_t2 = 0
    lead_swings = 0
    prev_leader = 0
    for r in rounds:
        w = int(r['winner_team'] or 0)
        if w == 1:
            score_t1 += 1
        elif w == 2:
            score_t2 += 1
        leader = 1 if score_t1 > score_t2 else (2 if score_t2 > score_t1 else 0)
        if leader and prev_leader and leader != prev_leader:
            lead_swings += 1
        if leader:
            prev_leader = leader

    top = [dict(r) for r in top_players]
    top_name = top[0]['name'] if top else 'Unknown'
    top_kills = int(top[0]['kills'] or 0) if top else 0

    headline = f"{winner} on {match['map']} ({t1_rounds}-{t2_rounds})"
    summary_text = (
        f"{winner} finished the match {t1_rounds}-{t2_rounds} on {match['map']}. "
        f"{top_name} led frag output with {top_kills} kills. "
        f"Momentum shifted {lead_swings} time(s) across the round flow."
    )

    key_points = []
    if duel:
        key_points.append(
            f"Key duel: {duel['killer']} over {duel['victim']} ({int(duel['kills'] or 0)} kills)."
        )
    fk_team1 = sum(1 for r in rounds if int(r['first_kill_team'] or 0) == 1)
    fk_team2 = sum(1 for r in rounds if int(r['first_kill_team'] or 0) == 2)
    key_points.append(f"Opening picks split: Team 1 {fk_team1}, Team 2 {fk_team2}.")

    result = {
        'path': str(match['path']),
        'map': str(match['map']),
        'played_at': int(match['played_at'] or 0),
        'headline': headline,
        'summary': summary_text,
        'key_points': key_points,
        'top_performers': top,
        'lead_swings': lead_swings,
        'generator': 'heuristic',
    }

    if provider == 'gemini':
        result = _apply_gemini_replay_summary(result)

    _cache_set(cache_key, result, ttl=86400.0)
    return result


def get_replay_highlights(path: str,
                          limit: int = 20) -> dict:
    """Auto-highlight extraction from indexed kill + round data for one replay."""
    normalized_limit = max(1, min(int(limit), 100))
    with _connect() as conn:
        match = conn.execute('SELECT id, path, map FROM matches WHERE path = ? LIMIT 1', (path,)).fetchone()
        if not match:
            return {'path': path, 'highlights': []}
        match_id = int(match['id'])

        rounds = conn.execute('''
            SELECT id, round_index, first_kill_frame, first_kill_player, winner_team, first_kill_team
            FROM rounds
            WHERE match_id = ?
            ORDER BY round_index ASC
        ''', (match_id,)).fetchall()
        kills = conn.execute('''
            SELECT frame_idx, killer, victim, weapon, location, team_kill
            FROM kill_events
            WHERE match_id = ?
            ORDER BY frame_idx ASC
        ''', (match_id,)).fetchall()
        surv = conn.execute('''
            SELECT round_id, team, SUM(survived) AS survivors
            FROM player_round_stats
            WHERE match_id = ? AND team IN (1,2)
            GROUP BY round_id, team
        ''', (match_id,)).fetchall()

    survivors_by_round: dict = defaultdict(dict)
    for row in surv:
        survivors_by_round[int(row['round_id'])][int(row['team'])] = int(row['survivors'] or 0)

    highlights = []
    for r in rounds:
        fframe = r['first_kill_frame']
        fplayer = str(r['first_kill_player'] or '')
        if fframe is not None and fplayer:
            highlights.append({
                'type': 'first_blood',
                'frame': int(fframe),
                'time_seconds': round(float(fframe) * 0.1, 1),
                'label': f"Round {int(r['round_index']) + 1}: first blood by {fplayer}",
                'player': fplayer,
            })

        rid = int(r['id'])
        teams = survivors_by_round.get(rid, {})
        winner = int(r['winner_team'] or 0)
        if winner in (1, 2):
            win_surv = int(teams.get(winner, 0))
            lose_surv = int(teams.get(1 if winner == 2 else 2, 0))
            if win_surv < lose_surv:
                frame = int(fframe or 0)
                highlights.append({
                    'type': 'clutch_round',
                    'frame': frame,
                    'time_seconds': round(float(frame) * 0.1, 1),
                    'label': f"Round {int(r['round_index']) + 1}: Team {winner} clutch ({win_surv}v{lose_surv})",
                    'player': None,
                })

    streaks: dict = defaultdict(int)
    best_streak = 0
    best_streak_player = None
    best_streak_frame = 0
    for k in kills:
        if int(k['team_kill'] or 0):
            continue
        killer = str(k['killer'])
        victim = str(k['victim'])
        frame = int(k['frame_idx'] or 0)
        streaks[killer] += 1
        streaks[victim] = 0
        if streaks[killer] > best_streak:
            best_streak = streaks[killer]
            best_streak_player = killer
            best_streak_frame = frame
        if streaks[killer] == 3:
            highlights.append({
                'type': 'streak',
                'frame': frame,
                'time_seconds': round(float(frame) * 0.1, 1),
                'label': f"{killer} started a 3-kill streak",
                'player': killer,
            })

    if best_streak_player and best_streak >= 4:
        highlights.append({
            'type': 'best_streak',
            'frame': best_streak_frame,
            'time_seconds': round(float(best_streak_frame) * 0.1, 1),
            'label': f"Best streak: {best_streak_player} x{best_streak}",
            'player': best_streak_player,
        })

    highlights.sort(key=lambda h: (int(h.get('frame') or 0), str(h.get('type') or '')))
    highlights = highlights[:normalized_limit]
    return {
        'path': str(match['path']),
        'map': str(match['map']),
        'highlights': highlights,
    }


# ── Player activity timeline ───────────────────────────────────────────────────

def get_player_activity_by_week(name: str,
                                period: str = _DEFAULT_H2H_PERIOD,
                                week: Optional[str] = None) -> list:
    """Games played per calendar week for a single player."""
    period_clause, period_params, _ = _h2h_period_filter_clause(period)
    week_clause, week_params, _ = _week_filter_clause(week)
    with _connect() as conn:
        identities = _resolve_identity_variants(conn, name)
        name_clause, name_params = _in_clause('p.name', identities)
        rows = conn.execute(f'''
            SELECT strftime('%Y-%W', datetime(m.played_at, 'unixepoch')) AS week,
                   COUNT(*) AS matches,
                   SUM(p.kills) AS kills
            FROM player_match_stats p
            JOIN matches m ON m.id = p.match_id
            WHERE {name_clause} AND {period_clause} AND {week_clause}
            GROUP BY week
            ORDER BY week ASC
        ''', name_params + period_params + week_params).fetchall()
    return [dict(r) for r in rows]


# ── Head-to-head ───────────────────────────────────────────────────────────────

def get_h2h_rivalries(limit: int = 20,
                      period: str = _DEFAULT_H2H_PERIOD) -> list:
    """Frequent encounter leaderboard with rivalry index."""
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    normalized_limit = max(1, min(int(limit), 100))
    cache_key = f'h2h_rivalries:{normalized_period}:{normalized_limit}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        rows = conn.execute(f'''
            WITH duels AS (
                SELECT k.killer,
                       k.victim
                FROM kill_events k
                JOIN matches m ON m.id = k.match_id
                WHERE k.team_kill = 0
                  AND m.game_mode = 'tdm'
                  AND {period_clause}
            ),
            pairs AS (
                SELECT
                    CASE WHEN killer < victim THEN killer ELSE victim END AS p1,
                    CASE WHEN killer < victim THEN victim ELSE killer END AS p2,
                    SUM(CASE WHEN killer < victim THEN 1 ELSE 0 END) AS p1_kills,
                    SUM(CASE WHEN killer > victim THEN 1 ELSE 0 END) AS p2_kills,
                    COUNT(*) AS encounters
                FROM duels
                GROUP BY p1, p2
                HAVING COUNT(*) >= 3
            )
            SELECT
                p1,
                p2,
                p1_kills,
                p2_kills,
                encounters
            FROM pairs
            ORDER BY encounters DESC, ABS(p1_kills - p2_kills) ASC, p1 ASC, p2 ASC
            LIMIT ?
        ''', period_params + (normalized_limit,)).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        encounters = int(d.get('encounters') or 0)
        diff = abs(int(d.get('p1_kills') or 0) - int(d.get('p2_kills') or 0))
        d['balance_score'] = round((100.0 * (1.0 - (diff / max(encounters, 1)))), 1)
        d['rivalry_index'] = round(min(100.0, (encounters * 2.0) + (d['balance_score'] * 0.6)), 1)
        result.append(d)
    _cache_set(cache_key, result)
    return result

def get_h2h(p1: str, p2: str, period: str = _DEFAULT_H2H_PERIOD) -> dict:
    """Head-to-head stats between two players."""
    period_clause, period_params, normalized_period = _h2h_period_filter_clause(period)
    p1_key = (p1 or '').strip()
    p2_key = (p2 or '').strip()
    cache_key = f'h2h:{normalized_period}:{p1_key}:{p2_key}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _connect() as conn:
        p1_names = _resolve_identity_variants(conn, p1)
        p2_names = _resolve_identity_variants(conn, p2)

        p1_killer_clause, p1_killer_params = _in_clause('k.killer', p1_names)
        p1_victim_clause, p1_victim_params = _in_clause('k.victim', p1_names)
        p2_killer_clause, p2_killer_params = _in_clause('k.killer', p2_names)
        p2_victim_clause, p2_victim_params = _in_clause('k.victim', p2_names)

        p1_stats_clause, p1_stats_params = _in_clause('p.name', p1_names)
        p2_stats_clause, p2_stats_params = _in_clause('p.name', p2_names)

        p1_over_p2_clause = f'({p1_killer_clause} AND {p2_victim_clause})'
        p2_over_p1_clause = f'({p2_killer_clause} AND {p1_victim_clause})'

        p1_kills = conn.execute(
            f'SELECT COUNT(*) FROM kill_events k JOIN matches m ON m.id=k.match_id'
            f' WHERE {p1_killer_clause} AND {p2_victim_clause} AND k.team_kill=0 AND m.game_mode=\'tdm\''
            f' AND {period_clause}',
            p1_killer_params + p2_victim_params + period_params).fetchone()[0]
        p2_kills = conn.execute(
            f'SELECT COUNT(*) FROM kill_events k JOIN matches m ON m.id=k.match_id'
            f' WHERE {p2_killer_clause} AND {p1_victim_clause} AND k.team_kill=0 AND m.game_mode=\'tdm\''
            f' AND {period_clause}',
            p2_killer_params + p1_victim_params + period_params).fetchone()[0]
        shared = conn.execute(f'''
            WITH p1m AS (
                SELECT p.match_id,
                       SUM(p.kills) AS kills,
                       SUM(p.deaths) AS deaths,
                       SUM(p.damage) AS damage,
                       ROUND(AVG(CASE WHEN p.accuracy IS NOT NULL THEN p.accuracy END), 1) AS acc,
                       CASE WHEN MIN(p.team) = MAX(p.team) THEN MIN(p.team) ELSE 0 END AS team
                FROM player_match_stats p
                WHERE {p1_stats_clause}
                GROUP BY p.match_id
            ),
            p2m AS (
                SELECT p.match_id,
                       SUM(p.kills) AS kills,
                       SUM(p.deaths) AS deaths,
                       SUM(p.damage) AS damage,
                       ROUND(AVG(CASE WHEN p.accuracy IS NOT NULL THEN p.accuracy END), 1) AS acc,
                       CASE WHEN MIN(p.team) = MAX(p.team) THEN MIN(p.team) ELSE 0 END AS team
                FROM player_match_stats p
                WHERE {p2_stats_clause}
                GROUP BY p.match_id
            )
            SELECT m.id, m.map, m.path, m.played_at, m.duration, m.t1_rounds, m.t2_rounds, m.game_mode,
                   p1m.kills AS p1_kills, p1m.deaths AS p1_deaths, p1m.damage AS p1_damage,
                   p1m.acc AS p1_acc, p1m.team AS p1_team,
                   p2m.kills AS p2_kills, p2m.deaths AS p2_deaths, p2m.damage AS p2_damage,
                   p2m.acc AS p2_acc, p2m.team AS p2_team
            FROM matches m
            JOIN p1m ON p1m.match_id = m.id
            JOIN p2m ON p2m.match_id = m.id
            WHERE m.game_mode = 'tdm' AND {period_clause}
            ORDER BY m.played_at DESC
            LIMIT 200
        ''', p1_stats_params + p2_stats_params + period_params).fetchall()
        maps = conn.execute(f'''
            WITH p1m AS (
                SELECT p.match_id,
                       SUM(p.kills) AS kills,
                       SUM(p.deaths) AS deaths
                FROM player_match_stats p
                WHERE {p1_stats_clause}
                GROUP BY p.match_id
            ),
            p2m AS (
                SELECT p.match_id,
                       SUM(p.kills) AS kills,
                       SUM(p.deaths) AS deaths
                FROM player_match_stats p
                WHERE {p2_stats_clause}
                GROUP BY p.match_id
            )
            SELECT m.map,
                   COUNT(DISTINCT m.id) AS games,
                   SUM(p1m.kills) AS p1_kills, SUM(p1m.deaths) AS p1_deaths,
                   SUM(p2m.kills) AS p2_kills, SUM(p2m.deaths) AS p2_deaths
            FROM matches m
            JOIN p1m ON p1m.match_id = m.id
            JOIN p2m ON p2m.match_id = m.id
            WHERE m.game_mode = 'tdm' AND {period_clause}
            GROUP BY m.map
            ORDER BY games DESC
        ''', p1_stats_params + p2_stats_params + period_params).fetchall()
        duel_weapons = conn.execute(f'''
            WITH duel AS (
                SELECT k.weapon,
                       CASE
                           WHEN {p1_over_p2_clause} AND k.team_kill=0 THEN 1
                           WHEN {p2_over_p1_clause} AND k.team_kill=0 THEN 2
                           ELSE 0
                       END AS side
                FROM kill_events k
                JOIN matches m ON m.id = k.match_id
                WHERE m.game_mode = 'tdm'
                  AND ({p1_over_p2_clause} OR {p2_over_p1_clause})
                  AND {period_clause}
            )
            SELECT k.weapon,
                   SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) AS p1_kills,
                   SUM(CASE WHEN side=2 THEN 1 ELSE 0 END) AS p2_kills
            FROM duel k
            GROUP BY weapon
            HAVING (
                SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) +
                SUM(CASE WHEN side=2 THEN 1 ELSE 0 END)
            ) > 0
            ORDER BY (p1_kills + p2_kills) DESC, weapon ASC
            LIMIT 12
        ''',
            (p1_killer_params + p2_victim_params + p2_killer_params + p1_victim_params) +
            (p1_killer_params + p2_victim_params + p2_killer_params + p1_victim_params) +
            period_params
        ).fetchall()
        duel_locations = conn.execute(f'''
            WITH duel AS (
                SELECT k.location,
                       CASE
                           WHEN {p1_over_p2_clause} AND k.team_kill=0 THEN 1
                           WHEN {p2_over_p1_clause} AND k.team_kill=0 THEN 2
                           ELSE 0
                       END AS side
                FROM kill_events k
                JOIN matches m ON m.id = k.match_id
                WHERE m.game_mode = 'tdm'
                  AND ({p1_over_p2_clause} OR {p2_over_p1_clause})
                  AND {period_clause}
            )
            SELECT k.location,
                   SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) AS p1_kills,
                   SUM(CASE WHEN side=2 THEN 1 ELSE 0 END) AS p2_kills
            FROM duel k
            GROUP BY location
            HAVING (
                SUM(CASE WHEN side=1 THEN 1 ELSE 0 END) +
                SUM(CASE WHEN side=2 THEN 1 ELSE 0 END)
            ) > 0
            ORDER BY (p1_kills + p2_kills) DESC, location ASC
            LIMIT 12
        ''',
            (p1_killer_params + p2_victim_params + p2_killer_params + p1_victim_params) +
            (p1_killer_params + p2_victim_params + p2_killer_params + p1_victim_params) +
            period_params
        ).fetchall()

        duel_total = int(p1_kills) + int(p2_kills)
        rivalry_rank = None
        if duel_total > 0:
            rivalry_rank_row = conn.execute(f'''
                WITH pair_counts AS (
                    SELECT
                        CASE WHEN k.killer < k.victim THEN k.killer ELSE k.victim END AS a,
                        CASE WHEN k.killer < k.victim THEN k.victim ELSE k.killer END AS b,
                        COUNT(*) AS encounters
                    FROM kill_events k
                    JOIN matches m ON m.id = k.match_id
                    WHERE k.team_kill = 0
                      AND m.game_mode = 'tdm'
                      AND {period_clause}
                    GROUP BY a, b
                )
                SELECT 1 + COUNT(*) AS rank
                FROM pair_counts
                WHERE encounters > ?
            ''', period_params + (duel_total,)).fetchone()
            if rivalry_rank_row:
                rivalry_rank = int(rivalry_rank_row['rank'])

    shared_rows = [dict(r) for r in shared]
    p1_match_wins = 0
    p2_match_wins = 0
    match_ties = 0
    for row in shared_rows:
        p1_mk = int(row.get('p1_kills') or 0)
        p2_mk = int(row.get('p2_kills') or 0)
        if p1_mk > p2_mk:
            p1_match_wins += 1
        elif p2_mk > p1_mk:
            p2_match_wins += 1
        else:
            match_ties += 1

    shared_games = len(shared_rows)
    duel_total = int(p1_kills) + int(p2_kills)
    kill_diff = int(p1_kills) - int(p2_kills)
    balance = 1.0 - (abs(kill_diff) / max(duel_total, 1)) if duel_total else 0.0
    volume_factor = min(duel_total, 120) / 120.0
    shared_factor = min(shared_games, 50) / 50.0
    rivalry_index = round((balance * 45.0) + (volume_factor * 35.0) + (shared_factor * 20.0), 1)
    dominance_score = round((100.0 * kill_diff) / max(duel_total, 1), 1) if duel_total else 0
    result = {
        'p1': p1, 'p2': p2,
        'period': normalized_period,
        'p1_kills': p1_kills, 'p2_kills': p2_kills,
        'shared_games': shared_games,
        'p1_match_wins': p1_match_wins,
        'p2_match_wins': p2_match_wins,
        'match_ties': match_ties,
        'p1_match_win_rate': round((100.0 * p1_match_wins) / shared_games, 1) if shared_games else 0,
        'p2_match_win_rate': round((100.0 * p2_match_wins) / shared_games, 1) if shared_games else 0,
        'duel_total_kills': duel_total,
        'dominance_score': dominance_score,
        'encounter_intensity': round(float(duel_total) / max(shared_games, 1), 2) if shared_games else 0,
        'rivalry_index': rivalry_index,
        'frequent_encounter_rank': rivalry_rank,
        'matches': shared_rows,
        'maps': [dict(r) for r in maps],
        'duel_weapons': [dict(r) for r in duel_weapons],
        'duel_locations': [dict(r) for r in duel_locations],
    }
    _cache_set(cache_key, result)
    return result


# ── Player name search ─────────────────────────────────────────────────────────

def search_players(q: str, limit: int = 20) -> list:
    """Case-insensitive contains-match on player names."""
    with _connect() as conn:
        rows = conn.execute('''
            SELECT p.name, COUNT(DISTINCT p.match_id) AS games, SUM(p.kills) AS kills
            FROM player_match_stats p
            WHERE LOWER(p.name) LIKE ?
            GROUP BY p.name
            ORDER BY games DESC
            LIMIT ?
        ''', (f'%{q.lower()}%', limit)).fetchall()
    return [dict(r) for r in rows]
