/**
 * viewer.js — shared canvas renderer for map geometry, kill heatmaps, and replay visualization.
 *
 * Coordinate system:
 *   Q2 world coords: X = east, Y = north (up-on-screen when we flip Y).
 *   Canvas: origin top-left, Y increases downward.
 *   Mapping: canvas_x = (world_x - min_x) * scale + pad
 *            canvas_y = (max_y - world_y) * scale + pad   ← Y flip
 */

// ── Palette ──────────────────────────────────────────────────────────────────

const PLAYER_COLORS = [
  '#5b8dee','#e85d5d','#4ecb71','#e8b64d',
  '#c45be8','#5de8d1','#e88c5b','#8de85b',
  '#ee5ba3','#b8ee5b','#5beea3','#ee5b8c',
];

const TEAM_COLORS = { 1: '#e85d5d', 2: '#5b8dee' };  // 1=red 2=blue

function playerColor(num) {
  return PLAYER_COLORS[num % PLAYER_COLORS.length];
}

function playerColorByTeam(numStr, playerNames, playerTeams) {
  const name = playerNames && playerNames[numStr];
  if (name && playerTeams) {
    const t = playerTeams[name];
    if (t) return TEAM_COLORS[t];
  }
  return playerColor(parseInt(numStr));
}

// ── Projection helpers ───────────────────────────────────────────────────────

function makeProjection(geo, canvasW, canvasH, pad) {
  pad = pad || 30;
  const b = geo.bounds;
  const rangeX = b.max_x - b.min_x || 1;
  const rangeY = b.max_y - b.min_y || 1;
  const scaleX = (canvasW - pad * 2) / rangeX;
  const scaleY = (canvasH - pad * 2) / rangeY;
  const scale  = Math.min(scaleX, scaleY);
  // Centre the map
  const offX = pad + ((canvasW - pad * 2) - rangeX * scale) / 2;
  const offY = pad + ((canvasH - pad * 2) - rangeY * scale) / 2;

  return {
    toCanvas(wx, wy) {
      return [
        offX + (wx - b.min_x) * scale,
        offY + (b.max_y - wy) * scale,
      ];
    },
    scale,
    offX,
    offY,
  };
}

// ── Map geometry rendering ───────────────────────────────────────────────────

// ── Textured top-view overlay ───────────────────────────────────────────────

/**
 * Draw the server-rendered textured PNG aligned to the current canvas projection.
 * tvParams = {img_size, scale, off_x, off_y}  (from /api/map/X/topview.json)
 */
function drawTopview(ctx, topviewImg, tvParams, proj) {
  const ratio = proj.scale / tvParams.scale;
  const dx = proj.offX - tvParams.off_x * ratio;
  const dy = proj.offY - tvParams.off_y * ratio;
  const dw = tvParams.img_size * ratio;
  ctx.save();
  ctx.globalAlpha = 0.88;
  ctx.drawImage(topviewImg, dx, dy, dw, dw);
  ctx.restore();
}

function drawEdges(ctx, geo, proj, subtle) {
  ctx.save();
  ctx.strokeStyle = subtle ? 'rgba(255,255,255,0.18)' : '#2a3050';
  ctx.lineWidth   = subtle ? 0.6 : 0.8;
  ctx.beginPath();
  for (const [x1, y1, x2, y2] of geo.edges) {
    const [cx1, cy1] = proj.toCanvas(x1, y1);
    const [cx2, cy2] = proj.toCanvas(x2, y2);
    ctx.moveTo(cx1, cy1);
    ctx.lineTo(cx2, cy2);
  }
  ctx.stroke();
  ctx.restore();
}

function drawSpawns(ctx, geo, proj) {
  if (!geo.spawns || !geo.spawns.length) return;
  ctx.save();
  for (const sp of geo.spawns) {
    const [cx, cy] = proj.toCanvas(sp.x, sp.y);
    const color = sp.type === 'team1' ? 'rgba(91,141,238,.6)'
                : sp.type === 'team2' ? 'rgba(232,93,93,.6)'
                : 'rgba(122,127,154,.4)';
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.arc(cx, cy, 4, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.restore();
}

// ── Heatmap ──────────────────────────────────────────────────────────────────

function buildHeatmap(points, proj, canvasW, canvasH, radius) {
  radius = radius || 24;
  const offscreen = document.createElement('canvas');
  offscreen.width  = canvasW;
  offscreen.height = canvasH;
  const octx = offscreen.getContext('2d');

  for (const [wx, wy] of points) {
    const [cx, cy] = proj.toCanvas(wx, wy);
    const g = octx.createRadialGradient(cx, cy, 0, cx, cy, radius);
    g.addColorStop(0, 'rgba(255,255,255,0.35)');
    g.addColorStop(1, 'rgba(255,255,255,0)');
    octx.fillStyle = g;
    octx.beginPath();
    octx.arc(cx, cy, radius, 0, Math.PI * 2);
    octx.fill();
  }

  // Colour-map the alpha channel
  const result = document.createElement('canvas');
  result.width  = canvasW;
  result.height = canvasH;
  const rctx = result.getContext('2d');

  const imgData = octx.getImageData(0, 0, canvasW, canvasH);
  const d = imgData.data;
  const out = rctx.createImageData(canvasW, canvasH);
  const od  = out.data;

  for (let i = 0; i < d.length; i += 4) {
    const v = d[i + 3] / 255;  // alpha = density
    if (v < 0.01) continue;
    // hot → cold: yellow → orange → red
    const r = Math.min(255, Math.floor(v > 0.5 ? 255 : v * 2 * 255));
    const g = Math.min(255, Math.floor(v > 0.5 ? (1 - (v - 0.5) * 2) * 255 : 255));
    const b = 0;
    od[i]   = r;
    od[i+1] = g;
    od[i+2] = b;
    od[i+3] = Math.floor(v * 200);
  }
  rctx.putImageData(out, 0, 0);
  return result;
}

// ── Match map (stats) ─────────────────────────────────────────────────────────

function drawMap(canvas, geo, matchData, layerMode, topview) {
  const W = canvas.offsetWidth  || 800;
  const H = canvas.offsetHeight || 600;
  if (canvas.width !== W)  canvas.width  = W;
  if (canvas.height !== H) canvas.height = H;

  const ctx  = canvas.getContext('2d');
  const proj = makeProjection(geo, W, H, 32);

  ctx.fillStyle = '#0b0e16';
  ctx.fillRect(0, 0, W, H);

  if (topview && topview.img && topview.params) {
    drawTopview(ctx, topview.img, topview.params, proj);
    drawEdges(ctx, geo, proj, true);
  } else {
    drawEdges(ctx, geo, proj, false);
  }
  drawSpawns(ctx, geo, proj);

  if (layerMode !== 'none' && matchData && matchData.frags) {
    const points = [];
    for (const f of matchData.frags) {
      const loc = layerMode === 'kills' ? f.killer_loc : f.victim_loc;
      if (loc && loc.length >= 2) {
        points.push([loc[0], loc[1]]);
      }
    }
    if (points.length) {
      const hm = buildHeatmap(points, proj, W, H, Math.max(16, proj.scale * 60));
      ctx.drawImage(hm, 0, 0);
    }
  }
}

// ── Kill skull overlay ──────────────────────────────────────────────────────

/**
 * Draw ☠ at each kill location for kills up to frameIdx.
 * Skulls reset at each round boundary so only the current round's kills show.
 * killPositions = [{x, y, frame}] pre-computed by computeKillPositions().
 * roundStartFrames = [frame_idx, ...] sorted list of round-start frames.
 */
function drawKillSkulls(ctx, killPositions, frameIdx, proj, roundStartFrames) {
  if (!killPositions || !killPositions.length) return;
  // Find the last round-start frame ≤ current frame
  let roundStart = 0;
  if (roundStartFrames) {
    for (const rsf of roundStartFrames) {
      if (rsf <= frameIdx) roundStart = rsf;
      else break;
    }
  }
  ctx.save();
  ctx.font = '13px sans-serif';
  ctx.textAlign    = 'center';
  ctx.textBaseline = 'middle';
  for (const kp of killPositions) {
    if (kp.frame < roundStart || kp.frame > frameIdx) continue;
    const [cx, cy] = proj.toCanvas(kp.x, kp.y);
    ctx.fillStyle = 'rgba(0,0,0,0.45)';
    ctx.beginPath();
    ctx.arc(cx, cy, 7, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = 'rgba(255,255,255,0.88)';
    ctx.fillText('\u2620', cx, cy + 0.5);
  }
  ctx.restore();
}

// ── Replay renderer ─────────────────────────────────────────────────────────

function drawReplay(canvas, geo, frame, replayData, hiddenPlayers, topview, killPositions, showWireframe) {
  if (!replayData) return;
  const W = canvas.offsetWidth  || 800;
  const H = canvas.offsetHeight || 600;
  if (canvas.width !== W)  canvas.width  = W;
  if (canvas.height !== H) canvas.height = H;

  const ctx = canvas.getContext('2d');

  ctx.fillStyle = '#0b0e16';
  ctx.fillRect(0, 0, W, H);

  const proj = geo ? makeProjection(geo, W, H, 32) : makeFallbackProjection(replayData, W, H);

  if (geo) {
    const hasTopview = topview && topview.img && topview.params;
    if (hasTopview) {
      drawTopview(ctx, topview.img, topview.params, proj);
      if (showWireframe !== false) drawEdges(ctx, geo, proj, true);
    } else {
      if (showWireframe !== false) drawEdges(ctx, geo, proj, false);
    }
    drawSpawns(ctx, geo, proj);
  } else {
    const b = getReplayBounds(replayData);
    drawGrid(ctx, proj, b, W, H);
  }

  const names = replayData.player_names || {};

  // Kill skulls below player dots
  if (geo) drawKillSkulls(ctx, killPositions, frame.t != null ? frame.t : 0, proj,
                           replayData.round_start_frames);

  if (!frame || !frame.players) return;

  const playerTeams = replayData.player_teams || {};

  for (const [numStr, ps] of Object.entries(frame.players)) {
    const num = parseInt(numStr);
    if (hiddenPlayers && hiddenPlayers.has(num)) continue;

    const [cx, cy] = proj.toCanvas(ps.x, ps.y);
    // Use ghost_clients list to skip the recorder
    const isGhost = replayData.ghost_clients && replayData.ghost_clients.includes(num);
    if (isGhost) continue;  // skip MVD recorder

    const color = playerColorByTeam(numStr, names, playerTeams);
    const name  = names[numStr] || names[num] || `P${num}`;

    // Direction indicator
    if (ps.a != null) {
      const angle = -ps.a * Math.PI / 180;
      const len   = 12;
      ctx.save();
      ctx.strokeStyle = color;
      ctx.lineWidth   = 2;
      ctx.beginPath();
      ctx.moveTo(cx, cy);
      ctx.lineTo(cx + Math.cos(angle) * len, cy + Math.sin(angle) * len);
      ctx.stroke();
      ctx.restore();
    }

    // Body circle
    ctx.save();
    ctx.fillStyle   = color;
    ctx.strokeStyle = '#0b0e16';
    ctx.lineWidth   = 1.5;
    ctx.beginPath();
    ctx.arc(cx, cy, 6, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
    ctx.restore();

    // Name label
    ctx.save();
    ctx.font         = '11px sans-serif';
    ctx.fillStyle    = color;
    ctx.textAlign    = 'center';
    ctx.textBaseline = 'bottom';
    ctx.fillText(name, cx, cy - 8);
    ctx.restore();
  }
}

// ── Full-replay bounds (built once from all frames when no BSP) ─────────────
const _replayBoundsCache = new WeakMap();

function getReplayBounds(replayData) {
  if (_replayBoundsCache.has(replayData)) return _replayBoundsCache.get(replayData);
  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  let found = false;
  for (const frame of (replayData.frames || [])) {
    for (const p of Object.values(frame.players || {})) {
      if (p.x < minX) minX = p.x;
      if (p.x > maxX) maxX = p.x;
      if (p.y < minY) minY = p.y;
      if (p.y > maxY) maxY = p.y;
      found = true;
    }
  }
  const bounds = found
    ? { min_x: minX, max_x: maxX, min_y: minY, max_y: maxY }
    : { min_x: -500, max_x: 500, min_y: -500, max_y: 500 };
  _replayBoundsCache.set(replayData, bounds);
  return bounds;
}

// Fallback when no BSP: use full-replay bounds so players are always visible
function makeFallbackProjection(replayData, W, H) {
  const pad = 48;
  const b = getReplayBounds(replayData);
  const margin = Math.max((b.max_x - b.min_x) * 0.08, (b.max_y - b.min_y) * 0.08, 64);
  const fakeGeo = { bounds: {
    min_x: b.min_x - margin, max_x: b.max_x + margin,
    min_y: b.min_y - margin, max_y: b.max_y + margin,
  }};
  return makeProjection(fakeGeo, W, H, pad);
}

// Draw a soft grid when there's no map geometry
function drawGrid(ctx, proj, bounds, W, H) {
  ctx.save();
  ctx.strokeStyle = 'rgba(42,48,72,0.6)';
  ctx.lineWidth   = 0.5;
  const rangeX = bounds.max_x - bounds.min_x;
  const rangeY = bounds.max_y - bounds.min_y;
  const gridStep = Math.pow(10, Math.ceil(Math.log10(Math.max(rangeX, rangeY) / 8)));
  const startX = Math.floor(bounds.min_x / gridStep) * gridStep;
  const startY = Math.floor(bounds.min_y / gridStep) * gridStep;
  for (let wx = startX; wx <= bounds.max_x + gridStep; wx += gridStep) {
    const [cx] = proj.toCanvas(wx, bounds.min_y);
    ctx.beginPath(); ctx.moveTo(cx, 0); ctx.lineTo(cx, H); ctx.stroke();
  }
  for (let wy = startY; wy <= bounds.max_y + gridStep; wy += gridStep) {
    const [, cy] = proj.toCanvas(bounds.min_x, wy);
    ctx.beginPath(); ctx.moveTo(0, cy); ctx.lineTo(W, cy); ctx.stroke();
  }
  ctx.restore();
}
