(function () {
  'use strict';

  const MODE_ALIASES = {
    tdm: 'team_deathmatch',
    dm: 'deathmatch',
    all_team_modes: 'team_modes',
    all_dm_modes: 'dm_modes',
  };

  const MODE_ALLOWED = new Set([
    'teamplay', 'team_deathmatch', 'domination', 'espionage', 'tourney',
    'deathmatch', 'jumpmod', 'ctf', 'team_modes', 'dm_modes', 'all'
  ]);

  const PERIOD_ALIASES = {
    alltime: 'all',
    all_time: 'all',
    thisweek: 'this_week',
    lastweek: 'last_week',
    thismonth: 'this_month',
    lastmonth: 'last_month',
    thisyear: 'this_year',
    lastyear: 'last_year',
    previous_year: 'last_year',
  };

  const PERIOD_ALLOWED = new Set([
    'this_week', 'last_week', 'this_month', 'last_month',
    'this_year', 'last_year', 'all'
  ]);

  function escHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function normalizeMode(raw) {
    const key = String(raw || 'teamplay').trim().toLowerCase();
    const resolved = MODE_ALIASES[key] || key;
    return MODE_ALLOWED.has(resolved) ? resolved : 'teamplay';
  }

  function normalizePeriod(raw) {
    const key = String(raw || 'this_year').trim().toLowerCase();
    const resolved = PERIOD_ALIASES[key] || key;
    return PERIOD_ALLOWED.has(resolved) ? resolved : 'this_year';
  }

  function normalizeWeek(raw) {
    const value = String(raw || '').trim();
    const m = value.match(/^(\d{4})[-_]?w?(\d{1,2})$/i);
    if (!m) return '';
    const weekNum = parseInt(m[2], 10);
    if (!Number.isFinite(weekNum) || weekNum < 0 || weekNum > 53) return '';
    return m[1] + '-' + String(weekNum).padStart(2, '0');
  }

  const PERIOD_LS_KEY = 'aq2stats.period';
  const MODE_LS_KEY = 'aq2stats.mode';

  function _readLS(key) {
    try { return window.localStorage ? window.localStorage.getItem(key) : null; }
    catch (_e) { return null; }
  }
  function _writeLS(key, value) {
    try {
      if (!window.localStorage) return;
      if (value == null) window.localStorage.removeItem(key);
      else window.localStorage.setItem(key, value);
    } catch (_e) { /* ignore quota / privacy */ }
  }

  function getFilters() {
    const params = new URLSearchParams(window.location.search);
    const rawPeriod = params.get('period');
    const rawMode = params.get('mode');
    const periodSource = rawPeriod != null ? rawPeriod : _readLS(PERIOD_LS_KEY);
    const modeSource = rawMode != null ? rawMode : _readLS(MODE_LS_KEY);
    return {
      mode: normalizeMode(modeSource),
      period: normalizePeriod(periodSource),
      week: normalizeWeek(params.get('week')),
    };
  }

  function _reloadWithFilters(mode, period, week) {
    const params = new URLSearchParams(window.location.search);
    if (mode && mode !== 'teamplay') params.set('mode', mode);
    else params.delete('mode');

    if (period && period !== 'this_year') params.set('period', period);
    else params.delete('period');

    if (week) params.set('week', week);
    else params.delete('week');

    _writeLS(PERIOD_LS_KEY, period || null);
    _writeLS(MODE_LS_KEY, mode || null);

    const qs = params.toString();
    window.location.search = qs ? ('?' + qs) : '';
  }

  function bindFilters(options) {
    const opts = options || {};
    const modeId = opts.modeId || 'modeFilter';
    const periodId = opts.periodId || 'periodFilter';
    const filters = getFilters();

    const modeEl = document.getElementById(modeId);
    if (modeEl) {
      modeEl.value = filters.mode;
      modeEl.addEventListener('change', function () {
        const selected = normalizeMode(modeEl.value);
        _reloadWithFilters(selected, filters.period, '');
      });
    }

    const periodEl = document.getElementById(periodId);
    if (periodEl) {
      periodEl.value = filters.period;
      periodEl.addEventListener('change', function () {
        const selected = normalizePeriod(periodEl.value);
        _reloadWithFilters(filters.mode, selected, '');
      });
    }

    applyFiltersToSubnav(filters);

    return filters;
  }

  function applyFiltersToSubnav(filters) {
    const navLinks = document.querySelectorAll('.stats-subnav a');
    if (!navLinks || !navLinks.length) return;
    const f = filters || getFilters();
    navLinks.forEach(function (a) {
      try {
        const url = new URL(a.getAttribute('href'), window.location.origin);
        if (f.mode && f.mode !== 'teamplay') url.searchParams.set('mode', f.mode);
        else url.searchParams.delete('mode');
        if (f.period && f.period !== 'this_year') url.searchParams.set('period', f.period);
        else url.searchParams.delete('period');
        if (f.week) url.searchParams.set('week', f.week);
        else url.searchParams.delete('week');
        a.setAttribute('href', url.pathname + (url.search || ''));
      } catch (_err) {
      }
    });
  }

  function withFilters(path, filters) {
    const f = filters || getFilters();
    const sep = path.includes('?') ? '&' : '?';
    let url = path + sep +
      'mode=' + encodeURIComponent(f.mode) +
      '&period=' + encodeURIComponent(f.period);
    if (f.week) {
      url += '&week=' + encodeURIComponent(f.week);
    }
    return url;
  }

  async function fetchJson(path, filters) {
    const response = await fetch(withFilters(path, filters));
    if (!response.ok) {
      throw new Error('Request failed: ' + response.status);
    }
    return response.json();
  }

  function fmtNum(value, decimals) {
    if (value == null || value === '') return '—';
    const n = Number(value);
    if (!Number.isFinite(n)) return '—';
    if (typeof decimals === 'number') return n.toFixed(decimals);
    return n.toLocaleString();
  }

  function fmtPct(value, decimals) {
    if (value == null || value === '') return '—';
    const n = Number(value);
    if (!Number.isFinite(n)) return '—';
    const d = typeof decimals === 'number' ? decimals : 1;
    return n.toFixed(d) + '%';
  }

  function fmtDate(unixSeconds) {
    const n = Number(unixSeconds || 0);
    if (!Number.isFinite(n) || n <= 0) return '—';
    return new Date(n * 1000).toLocaleDateString(undefined, {
      year: 'numeric', month: 'short', day: 'numeric'
    });
  }

  function fmtDateTime(unixSeconds) {
    const n = Number(unixSeconds || 0);
    if (!Number.isFinite(n) || n <= 0) return '—';
    return new Date(n * 1000).toLocaleString(undefined, {
      year: 'numeric', month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit'
    });
  }

  function fmtDuration(seconds) {
    const n = Number(seconds || 0);
    if (!Number.isFinite(n) || n < 0) return '—';
    const m = Math.floor(n / 60);
    const s = Math.floor(n % 60);
    return m + 'm ' + String(s).padStart(2, '0') + 's';
  }

  function modeBadge(modeDetail, modeGroup) {
    const style = 'font-size:.68rem;color:#fff;border-radius:3px;padding:1px 4px;vertical-align:middle;';
    const map = {
      teamplay: ['TP', 'var(--accent)'],
      team_deathmatch: ['TDM', '#3f8cff'],
      domination: ['DOM', '#0aa58f'],
      espionage: ['ESP', '#6b62d6'],
      tourney: ['TOUR', '#9a7b2f'],
      deathmatch: ['DM', '#666'],
      jumpmod: ['JMP', '#888'],
      ctf: ['CTF', 'var(--accent2)'],
    };
    const raw = String(modeDetail || '').toLowerCase();
    const fallback = modeGroup === 'ctf'
      ? 'ctf'
      : (modeGroup === 'dm' ? 'deathmatch' : 'teamplay');
    const key = map[raw] ? raw : fallback;
    const def = map[key];
    return '<span style="' + style + 'background:' + def[1] + ';">' + def[0] + '</span>';
  }

  function initStatusBadge(options) {
    const opts = options || {};
    const badge = document.getElementById(opts.badgeId || 'statusBadge');
    const rebuildBtn = document.getElementById(opts.rebuildBtnId || 'rebuildBtn');
    if (!badge) return;

    const IDLE_MS = 60000;
    const RUNNING_MS = 10000;
    const HIDDEN_MS = 30000;

    let timer = null;
    let inFlight = false;

    function schedule(ms) {
      if (timer) clearTimeout(timer);
      const jitter = Math.floor(Math.random() * 1000);
      timer = setTimeout(load, ms + jitter);
    }

    function load() {
      if (inFlight) return;
      if (document.hidden) {
        schedule(HIDDEN_MS);
        return;
      }
      inFlight = true;
      fetch('/api/stats/status')
        .then(function (r) { return r.json(); })
        .then(function (d) {
          if (d.running) {
            badge.textContent = 'Indexing... ' + d.indexed + '/' + d.total;
            badge.className = 'badge badge-t1';
            schedule(RUNNING_MS);
          } else {
            badge.textContent = d.indexed + ' / ' + d.total + ' indexed';
            badge.className = 'badge badge-spec';
            schedule(IDLE_MS);
          }
        })
        .catch(function () {
          schedule(HIDDEN_MS);
        })
        .finally(function () {
          inFlight = false;
        });
    }

    document.addEventListener('visibilitychange', function () {
      if (!document.hidden) load();
    });

    if (rebuildBtn) {
      rebuildBtn.addEventListener('click', function () {
        rebuildBtn.disabled = true;
        fetch('/api/stats/reindex', { method: 'POST' })
          .then(function () {
            load();
            setTimeout(function () { rebuildBtn.disabled = false; }, 2500);
          })
          .catch(function () {
            rebuildBtn.disabled = false;
          });
      });
    }

    load();
  }

  // ── Player autocomplete ──────────────────────────────────────────────────────
  // initPlayerAutocomplete(inputEl, opts)
  //   opts.onSelect(name)  — called when a name is chosen (default: fills input)
  //   opts.align           — 'left'|'right' (default 'left')
  function initPlayerAutocomplete(inputEl, opts) {
    if (!inputEl) return;
    opts = opts || {};

    var drop = document.createElement('div');
    drop.style.cssText = 'display:none;position:absolute;top:calc(100% + 3px);left:0;' +
      'background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);' +
      'min-width:220px;max-height:260px;overflow-y:auto;z-index:200;' +
      'box-shadow:0 4px 16px rgba(0,0,0,.5);font-size:.85rem;';
    if (opts.align === 'right') { drop.style.left = 'auto'; drop.style.right = '0'; }

    var wrap = inputEl.parentNode;
    var savedPosition = getComputedStyle(wrap).position;
    if (savedPosition === 'static') wrap.style.position = 'relative';
    wrap.appendChild(drop);

    var timer = null;
    var activeIdx = -1;

    function items() { return drop.querySelectorAll('[data-ac-item]'); }

    function setActive(idx) {
      var list = items();
      list.forEach(function (el, i) {
        el.style.background = i === idx ? 'var(--surface)' : '';
      });
      activeIdx = idx;
    }

    function hideDrop() {
      drop.style.display = 'none';
      drop.innerHTML = '';
      activeIdx = -1;
    }

    function choose(name) {
      hideDrop();
      if (opts.onSelect) {
        opts.onSelect(name);
      } else {
        inputEl.value = name;
      }
    }

    inputEl.setAttribute('autocomplete', 'off');

    inputEl.addEventListener('input', function () {
      clearTimeout(timer);
      var q = this.value.trim();
      if (q.length < 2) { hideDrop(); return; }
      timer = setTimeout(function () {
        fetch('/api/players/search?q=' + encodeURIComponent(q))
          .then(function (r) { return r.json(); })
          .then(function (rows) {
            if (!rows || !rows.length) { hideDrop(); return; }
            drop.innerHTML = rows.map(function (r) {
              var safeName = escHtml(r.name || '');
              return '<div data-ac-item data-name="' + safeName + '"' +
                ' style="padding:.38rem .75rem;cursor:pointer;border-bottom:1px solid var(--border);' +
                'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' +
                safeName +
                '<span style="float:right;color:var(--muted);font-size:.75rem;margin-left:.5rem;">' +
                escHtml(String(r.games || '')) + ' games</span>' +
                '</div>';
            }).join('');
            drop.querySelectorAll('[data-ac-item]').forEach(function (el) {
              el.addEventListener('mousedown', function (e) {
                e.preventDefault();
                choose(this.dataset.name);
              });
              el.addEventListener('mouseenter', function () {
                var list = items();
                list.forEach(function (x, i) { if (x === el) setActive(i); });
              });
            });
            activeIdx = -1;
            drop.style.display = 'block';
          })
          .catch(function () { hideDrop(); });
      }, 220);
    });

    inputEl.addEventListener('keydown', function (e) {
      var list = items();
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setActive(Math.min(activeIdx + 1, list.length - 1));
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        setActive(Math.max(activeIdx - 1, 0));
      } else if (e.key === 'Enter') {
        if (activeIdx >= 0 && list[activeIdx]) {
          e.preventDefault();
          choose(list[activeIdx].dataset.name);
        }
      } else if (e.key === 'Escape') {
        hideDrop();
      }
    });

    document.addEventListener('click', function (e) {
      if (!inputEl.contains(e.target) && !drop.contains(e.target)) hideDrop();
    });
  }

  window.AQ2Stats = {
    bindFilters: bindFilters,
    getFilters: getFilters,
    withFilters: withFilters,
    fetchJson: fetchJson,
    fmtNum: fmtNum,
    fmtPct: fmtPct,
    fmtDate: fmtDate,
    fmtDateTime: fmtDateTime,
    fmtDuration: fmtDuration,
    escHtml: escHtml,
    modeBadge: modeBadge,
    initStatusBadge: initStatusBadge,
    initPlayerAutocomplete: initPlayerAutocomplete,
  };
})();
