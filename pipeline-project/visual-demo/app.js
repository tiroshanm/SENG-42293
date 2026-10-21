/* ReviewFlow control room - interactive visual demo of the travel-review pipeline.
   Everything shown comes from data.js (built from ../output and ../data/raw by build_data.py);
   the "run" is an animated replay of the five pipeline scripts, not a live Spark job. */
(() => {
'use strict';
const D = window.PIPELINE_DATA;
if (!D) {
  document.body.innerHTML = '<p style="padding:2rem;font-family:system-ui">data.js not found - run <code>python visual-demo/build_data.py</code> first.</p>';
  return;
}

/* ============================== helpers ============================== */
const $ = (s, r = document) => r.querySelector(s);
const fmt = n => Number(n).toLocaleString('en-US');
const pct = (a, b) => (100 * a / b).toFixed(1) + '%';
const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const ease = t => t < .5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;

const I = {
  search: '<circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/>',
  bell: '<path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9"/><path d="M10 21a2 2 0 0 0 4 0"/>',
  box: '<path d="M21 8 12 3 3 8v8l9 5 9-5z"/><path d="m3 8 9 5 9-5M12 13v8"/>',
  truck: '<path d="M2 6h12v10H2zM14 10h4l4 3v3h-8z"/><circle cx="6" cy="17" r="2"/><circle cx="17" cy="17" r="2"/>',
  star: '<path d="m12 3 2.8 5.7 6.2.9-4.5 4.4 1 6.2L12 17.3 6.5 20.2l1-6.2L3 9.6l6.2-.9z"/>',
  pulse: '<path d="M3 12h4l3-8 4 16 3-8h4"/>',
  plus: '<path d="M12 5v14M5 12h14"/>', minus: '<path d="M5 12h14"/>',
  fit: '<path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/>',
  tag: '<path d="M3 12V3h9l9 9-9 9z"/><circle cx="7.5" cy="7.5" r="1.5"/>',
  play: '<path d="M7 4v16l13-8z"/>', stop: '<rect x="6" y="6" width="12" height="12" rx="2"/>',
  chart: '<path d="M4 20V10M10 20V4M16 20v-8M22 20H2"/>',
  layers: '<path d="m12 3 9 5-9 5-9-5z"/><path d="m3 13 9 5 9-5"/>',
  cpu: '<rect x="6" y="6" width="12" height="12" rx="2"/><path d="M9 2v4M15 2v4M9 18v4M15 18v4M2 9h4M2 15h4M18 9h4M18 15h4"/>',
  x: '<path d="M6 6l12 12M18 6 6 18"/>',
  alert: '<path d="M12 3 2 20h20z"/><path d="M12 10v4M12 17h.01"/>',
  check: '<path d="m5 12 5 5 9-10"/>',
  file: '<path d="M14 3H6v18h12V7z"/><path d="M14 3v4h4"/>',
  route: '<circle cx="6" cy="19" r="2"/><circle cx="18" cy="5" r="2"/><path d="M8 19h8a3 3 0 0 0 0-6H8a3 3 0 0 1 0-6h8"/>',
  locate: '<circle cx="12" cy="12" r="3"/><path d="M12 2v4M12 18v4M2 12h4M18 12h4"/>',
  db: '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
  filter: '<path d="M3 4h18l-7 9v6l-4 2v-8z"/>',
  sparkle: '<path d="M12 3v4M12 17v4M3 12h4M17 12h4M6 6l2.5 2.5M15.5 15.5 18 18M6 18l2.5-2.5M15.5 8.5 18 6"/>',
  ban: '<circle cx="12" cy="12" r="9"/><path d="m5.6 5.6 12.8 12.8"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  open: '<path d="M14 4h6v6M20 4l-9 9M18 14v6H4V6h6"/>',
  target: '<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1"/>',
  net: '<circle cx="5" cy="6" r="2.5"/><circle cx="19" cy="6" r="2.5"/><circle cx="12" cy="18" r="2.5"/><path d="M7.5 6h9M6.3 8.2l4.4 7.6M17.7 8.2l-4.4 7.6"/>',
  click: '<path d="M9 9l11 4-5 2-2 5z"/><path d="M5 3v3M3 5h3M8 2.5 7 4.5M2.5 8l2-1"/>',
  map: '<path d="M3 6l6-3 6 3 6-3v15l-6 3-6-3-6 3z"/><path d="M9 3v15M15 6v15"/>',
};
const icon = (n, cls = '') => `<svg class="ic ${cls}" viewBox="0 0 24 24" aria-hidden="true">${I[n]}</svg>`;

/* ============================== data ============================== */
const ING = D.dq_ingest, CLN = D.dq_clean, SUM = D.summary, RAW = D.raw;
const goodLines = ING.lines_read - ING.lines_quarantined;
const dupes = CLN.reviews_flattened - CLN.after_dedup;
const SENT = Object.fromEntries(D.sentiment.map(r => [r.label, r]));
const rawKB = RAW.files.reduce((a, f) => a + f.kb, 0);
const months = D.monthly_volume.slice(0, -1);          // final month is partial (same rule as 05_dashboard.py)
const cityByName = Object.fromEntries(D.city_summary.map(c => [c.city, c]));

// lexicon copied from pipeline/03_features.py so the tracer and "try it" box score text exactly like Spark does
const POS = ['great', 'friendly', 'clean', 'excellent', 'peaceful', 'comfortable', 'amazing', 'lovely', 'good', 'recommended'];
const NEG = ['noisy', 'dirty', 'rude', 'slow', 'overpriced', 'uncomfortable', 'poor', 'cold', 'disappointing', 'not'];
function sentiment(text) {
  const words = String(text).trim().toLowerCase().replace(/[^a-z\s]/g, '').split(/\s+/);
  const p = words.filter(w => POS.includes(w)), n = words.filter(w => NEG.includes(w));
  const s = (p.length - n.length) / Math.max(words.length, 1);
  return { s, label: s > 0 ? 'positive' : s < 0 ? 'negative' : 'neutral', p, n, words: words.length };
}

const ISSUES = [
  ['missing_key', 'Rating key missing', 'rating: <absent>'],
  ['null', 'Rating is null', 'rating: null'],
  ['date_ddmmyyyy', 'Date as dd/MM/yyyy', '03/05/2024'],
  ['string_number', 'Rating stored as text', '"4.5"'],
  ['na_text', 'Rating = "N/A"', '"N/A"'],
  ['duplicate', 'Exact duplicate review', 'same review_id'],
  ['out_of_range', 'Rating out of range', '0, 11, -1'],
].filter(([k]) => RAW.issues[k]);

/* ============================== pipeline model ============================== */
const STAGES = {
  raw:        { name: 'Raw JSON Yard', code: 'SRC', color: '#6b7a90', icon: 'file' },
  bronze:     { name: 'Bronze Layer', code: 'L1', color: '#c27c45', icon: 'layers' },
  quarantine: { name: 'Quarantine Bay', code: 'Q', color: '#d14343', icon: 'ban' },
  silver:     { name: 'Silver Layer', code: 'L2', color: '#8692a8', icon: 'filter' },
  gold:       { name: 'Gold Layer', code: 'L3', color: '#d29d18', icon: 'sparkle' },
  dash:       { name: 'Insights Hub', code: 'OUT', color: '#2f66f6', icon: 'chart' },
  spark:      { name: 'Spark Cluster', code: 'SPK', color: '#22304f', icon: 'cpu' },
  predict:    { name: 'Prediction Lab', code: 'L06', color: '#7c5cff', icon: 'pulse', lab: 6, label: 'Predict' },
  optimise:   { name: 'Decision Lab', code: 'L07', color: '#0f9d8a', icon: 'target', lab: 7, label: 'Optimise' },
  survival:   { name: 'Retention Lab', code: 'L08', color: '#d6457a', icon: 'clock', lab: 8, label: 'Survival', source: ['member_tenure.csv', '4,000 loyalty members'] },
  network:    { name: 'Network Lab', code: 'L10', color: '#e09a1f', icon: 'net', lab: 10, label: 'Network', source: ['follows.csv', '3,167 follow edges'] },
  web:        { name: 'Web Analytics Lab', code: 'L11', color: '#1f9bd6', icon: 'click', lab: 11, label: 'Web', source: ['clickstream.csv', '44,276 click events'] },
};
const ROAD_ORDER = ['raw', 'bronze', 'silver', 'gold', 'dash'];
const LABS = Object.keys(STAGES).filter(k => STAGES[k].lab);
const STEPS = [
  { id: 'ingest', label: 'Ingest', script: '01_ingest.py', from: 0, to: 1, stage: 'bronze', count: `${fmt(goodLines)} hotels`,
    payloadIn: `${ING.lines_read} lines`, payloadOut: `${goodLines} hotels`, task: 'read.json · PERMISSIVE parse' },
  { id: 'clean', label: 'Clean & flatten', script: '02_clean_flatten.py', from: 1, to: 2, stage: 'silver', count: `${fmt(CLN.after_dedup)} reviews`,
    payloadIn: `${goodLines} hotels`, payloadOut: `${fmt(CLN.after_dedup)} reviews`, task: 'explode · dropDuplicates · cast' },
  { id: 'features', label: 'Features', script: '03_features.py', from: 2, to: 3, stage: 'gold', count: '3 gold tables',
    payloadIn: `${fmt(CLN.after_dedup)} reviews`, payloadOut: '3 gold tables', task: 'tokenise · lexicon score · groupBy' },
  { id: 'analytics', label: 'Analytics', script: '04_analytics.py', from: 3, to: 4, stage: 'dash', count: `${fmt(SUM.n_rated_reviews)} rated`,
    payloadIn: '3 gold tables', payloadOut: '7 summaries', task: 'approxQuantile · corr · groupBy' },
  { id: 'serve', label: 'Serve', script: '05_dashboard.py', from: 4, to: 5, stage: 'dash', count: '5 charts',
    payloadIn: '7 summaries', payloadOut: '5 PNG charts', task: null },
];

const state = {
  selected: 'bronze', running: false, runToken: 0, speed: 1, follow: true, labels: true,
  steps: STEPS.map(() => 'done'), stepTimes: STEPS.map(() => null), current: -1,
  stage: { raw: 'done', bronze: 'done', quarantine: 'done', silver: 'done', gold: 'done', dash: 'done', spark: 'idle' },
  tab: 'exec', log: [], traceKey: ISSUES.find(i => i[0] === 'na_text') ? 'na_text' : ISSUES[0][0],
};

/* ============================== isometric engine ============================== */
const S = 16, CS = Math.cos(Math.PI / 6), SN = 0.5;
const P = (x, y, z = 0) => [(x - y) * CS * S, (x + y) * SN * S - z * S];
const pstr = a => a.map(p => P(...p).map(v => v.toFixed(1)).join(',')).join(' ');
const poly = (a, fill, attr = '') => `<polygon points="${pstr(a)}" fill="${fill}" ${attr}/>`;
function shade(hex, f) {
  const n = parseInt(hex.slice(1), 16), t = f < 0 ? 0 : 255, p = Math.abs(f);
  const c = [n >> 16, (n >> 8) & 255, n & 255].map(v => Math.round(v + (t - v) * p));
  return `rgb(${c.join(',')})`;
}
const faces = c => ({ top: shade(c, .28), left: c, right: shade(c, -.16) });
function box(x, y, z, w, d, h, col, attr = '') {
  const f = typeof col === 'string' ? faces(col) : col;
  return `<g ${attr}>` +
    poly([[x, y, z + h], [x + w, y, z + h], [x + w, y + d, z + h], [x, y + d, z + h]], f.top) +
    poly([[x, y + d, z], [x + w, y + d, z], [x + w, y + d, z + h], [x, y + d, z + h]], f.left) +
    poly([[x + w, y, z], [x + w, y + d, z], [x + w, y + d, z + h], [x + w, y, z + h]], f.right) + '</g>';
}
// transforms that let us draw flat 2D content onto a wall: local px, +x along the wall, +y downwards
const onFaceY = (x, y, z) => { const [a, b] = P(x, y, z); return `matrix(${CS},${SN},0,1,${a.toFixed(1)},${b.toFixed(1)})`; };
const onFaceX = (x, y, z) => { const [a, b] = P(x, y, z); return `matrix(${CS},${-SN},0,1,${a.toFixed(1)},${b.toFixed(1)})`; };

// layout (ground units): data stores sit ON the pipe at each stop, and one processing unit per
// script sits on the pipe between two stores, so data visibly changes shape as it passes a unit
const POS_ = { raw: [6, 46], bronze: [16, 36], silver: [26, 26], gold: [36, 16], dash: [46, 6] };
const PIPE_Z = .9;
const ROAD = (() => {
  const pts = [], stops = [];
  ROAD_ORDER.forEach((id, k) => {
    const [cx, cy] = POS_[id];
    stops.push(pts.length); pts.push([cx, cy + 5]);
    if (k < ROAD_ORDER.length - 1) pts.push([cx + 5, cy + 5], [cx + 5, cy - 5]);
  });
  const [xe, ye] = POS_.dash; pts.push([xe + 5, ye + 5]); stops.push(pts.length - 1);   // the pipe ends at the display
  const cum = [0];
  for (let i = 1; i < pts.length; i++) cum.push(cum[i - 1] + Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]));
  const units = [0, 1, 2, 3].map(k => (cum[stops[k] + 1] + cum[stops[k] + 2]) / 2);   // midway along the leg after each store
  units.push(cum[stops[4]] + 2.6);                                                   // 05 sits between output/ and the display
  return { pts, stops: stops.map(i => cum[i]), cum, units };
})();
function roadAt(dist) {
  const { pts, cum } = ROAD;
  let i = 1; while (i < cum.length - 1 && cum[i] < dist) i++;
  const t = clamp((dist - cum[i - 1]) / ((cum[i] - cum[i - 1]) || 1), 0, 1);
  const [ax, ay] = pts[i - 1], [bx, by] = pts[i];
  return { x: ax + (bx - ax) * t, y: ay + (by - ay) * t, dir: Math.abs(bx - ax) > Math.abs(by - ay) ? 'x' : 'y' };
}
const TANK = {
  bronze: { at: [16, 41], r: 1.9, h: 3.2, c: '#c27c45', label: 'BRONZE' },
  silver: { at: [26, 31], r: 2, h: 3.4, c: '#9aa6ba', label: 'SILVER' },
  gold:   { at: [36, 21], r: 2, h: 3.6, c: '#d9a520', label: 'GOLD' },
  dash:   { at: [46, 11], r: 1.5, h: 2.6, c: '#2f66f6', label: 'OUTPUT' },
};
const QBIN = [14.4, 46];
const BLD = {
  raw:    { x: 1.5, y: 42.6, w: 9, d: 7, top: 4.2 },
  bronze: { x: 14.1, y: 39.1, w: 3.8, d: 3.8, top: 4.4 },
  silver: { x: 24, y: 29, w: 4, d: 4, top: 4.6 },
  gold:   { x: 34, y: 19, w: 4, d: 4, top: 4.8 },
  dash:   { x: 44.5, y: 9.5, w: 11, d: 3, top: 5.8 },
  quarantine: { x: 13.25, y: 44.85, w: 2.3, d: 2.3, top: 2.8 },
  spark:  { x: 10.8, y: 11.8, w: 6.8, d: 3.6, top: 5.4 },
  // lab district behind the main line (x + y = 26), one small tank per lab lecture
  predict:  { x: 17.4, y: 5.4, w: 3.2, d: 3.2, top: 4 },
  optimise: { x: 21.4, y: 1.4, w: 3.2, d: 3.2, top: 4 },
  survival: { x: 25.4, y: -2.6, w: 3.2, d: 3.2, top: 4 },
  network:  { x: 29.4, y: -6.6, w: 3.2, d: 3.2, top: 4 },
  web:      { x: 33.4, y: -10.6, w: 3.2, d: 3.2, top: 4 },
};
const center = id => { const b = BLD[id]; return [b.x + b.w / 2, b.y + b.d / 2]; };
// stretches of pipe hidden inside a store or a unit: no blocks or glass are drawn there
const ZONES = [[-1, .9], ...['bronze', 'silver', 'gold', 'dash'].map((id, k) => [ROAD.stops[k + 1] - TANK[id].r - .1, ROAD.stops[k + 1] + TANK[id].r + .1]),
  ...ROAD.units.map((u, i) => [u - (i === 4 ? .85 : 1.05), u + (i === 4 ? .85 : 1.05)]), [ROAD.cum.at(-1) - .15, Infinity]];
const hidden = d => ZONES.some(([a, b]) => d > a && d < b);

// deterministic pseudo-random so the background city is identical on every load
let seed = 7; const rnd = () => (seed = (seed * 16807) % 2147483647) / 2147483647;

/* ---------- cylinders (stores) ---------- */
const GRAD = new Map(); let gradDefs = '';
function grad(c) {
  if (!GRAD.has(c)) {
    const id = 'cyl' + GRAD.size; GRAD.set(c, id);
    gradDefs += `<linearGradient id="${id}" x1="0" x2="1"><stop offset="0" stop-color="${shade(c, .12)}"/><stop offset=".33" stop-color="${shade(c, .36)}"/><stop offset="1" stop-color="${shade(c, -.3)}"/></linearGradient>`;
  }
  return GRAD.get(c);
}
const rxOf = r => r * Math.SQRT2 * CS * S, ryOf = r => r * Math.SQRT2 * SN * S;
function cyl(x, y, z, r, h, c) {
  const [cx, yb] = P(x, y, z), yt = yb - h * S, rx = rxOf(r), ry = ryOf(r);
  return `<path d="M${cx - rx},${yt}V${yb}A${rx},${ry} 0 0 0 ${cx + rx},${yb}V${yt}Z" fill="url(#${grad(c)})"/><ellipse cx="${cx}" cy="${yt}" rx="${rx}" ry="${ry}" fill="${shade(c, .3)}"/>`;
}
const groundShadow = (x, y, r) => { const [cx, cy] = P(x, y, 0); return `<ellipse cx="${cx + 7}" cy="${cy + 3}" rx="${rxOf(r) + 6}" ry="${ryOf(r) + 3}" fill="rgba(40,60,100,.12)"/>`; };
function tank(t) {
  const [x, y] = t.at, { r, h, c } = t, [cx, yb] = P(x, y, 0), rx = rxOf(r), ry = ryOf(r), yt = yb - h * S;
  let s = groundShadow(x, y, r) + cyl(x, y, 0, r + .15, .25, '#cfd8e5') + cyl(x, y, .25, r, h - .25, c);
  [.32, .74].forEach(f => { const yy = yb - f * h * S; s += `<path d="M${cx - rx},${yy}A${rx},${ry} 0 0 0 ${cx + rx},${yy}" fill="none" stroke="${shade(c, -.32)}" stroke-width="1.2" opacity=".55"/>`; });
  // level gauge: fills when this layer has been materialised
  const gx = cx + rx * .5, gh = (h - 1.1) * S, gy = yt + ry + 7;
  s += `<rect x="${gx - 3.5}" y="${gy}" width="7" height="${gh}" rx="3.5" fill="#152033" opacity=".55"/><rect class="fill" x="${gx - 2}" y="${gy + 1.5}" width="4" height="${gh - 3}" rx="2" fill="#7ef0b0"/>`;
  s += `<text x="${cx - rx * .2}" y="${yb - h * S * .45}" text-anchor="middle" font-size="9.5" font-weight="800" letter-spacing="1.3" fill="#fff" stroke="${shade(c, -.38)}" stroke-width="2.2" paint-order="stroke" font-family="Inter,system-ui">${t.label}</text>`;
  s += `<ellipse cx="${cx}" cy="${yt}" rx="${rx * .33}" ry="${ry * .33}" fill="${shade(c, .12)}" stroke="${shade(c, -.2)}"/>`;
  return s;
}

/* ---------- processing units (one per script) ---------- */
function unitSVG(i) {
  const p = roadAt(ROAD.units[i]), sz = i === 4 ? 1.5 : 1.9, h = i === 4 ? 1.5 : 1.8, x = p.x - sz / 2, y = p.y - sz / 2;
  let s = box(x - .2, y - .2, 0, sz + .4, sz + .4, .15, { top: '#e6ebf3', left: '#cfd8e5', right: '#c0cbdb' });
  s += box(x, y, .15, sz, sz, h, { top: '#3a4b72', left: '#22304f', right: '#1a2540' });
  s += `<g transform="${onFaceY(x, y + sz, h + .15)}"><text x="${sz * S / 2}" y="11" text-anchor="middle" font-size="9" font-weight="800" fill="#9fb6ff" font-family="Inter,system-ui">0${i + 1}</text>${[0, 1, 2].map(k => `<rect x="5" y="${16 + k * 4.5}" width="${sz * S - 10}" height="2" rx="1" fill="#3a4b72"/>`).join('')}</g>`;
  const [gx, gy] = P(p.x, p.y, h + .15);
  s += `<g transform="translate(${gx.toFixed(1)},${(gy - 2).toFixed(1)})"><g class="gear"><circle r="7.5" fill="none" stroke="#8fb4ff" stroke-width="3.6" stroke-dasharray="2.9 2.99"/><circle r="5" fill="#8fb4ff"/><circle r="1.9" fill="#22304f"/></g></g>`;
  return `<g class="unit" id="unit-${i}" data-unit="${i}" tabindex="0" role="button" aria-label="Look inside ${STEPS[i].script}">${s}</g>`;
}

function rawYard(b) {
  let s = poly([[b.x, b.y, 0], [b.x + b.w, b.y, 0], [b.x + b.w, b.y + b.d, 0], [b.x, b.y + b.d, 0]], '#e2e8f1');
  const cols = ['#2f66f6', '#2a9d8f', '#e07a3f', '#34477a', '#7f9bd6'];
  const slots = [[0, .4, 0], [1, 2.4, 0], [3, 2.4, 1.9], [2, 4.4, 0], [4, 4.4, 1.9]];   // [file idx, y offset, z]
  for (const [fi, oy, z] of slots) {
    const f = RAW.files[fi]; if (!f) continue;
    const x = b.x + 1.2, y = b.y + oy, w = 5.4, d = 1.8, h = 1.85, c = cols[fi];
    s += `<g><title>${f.file} · ${f.lines} lines · ${f.kb} KB${f.bad ? ` · ${f.bad} corrupt` : ''}</title>`;
    s += box(x, y, z, w, d, h, c);
    let ribs = '';
    for (let r = 4; r < w * S; r += 5) ribs += `<line x1="${r}" x2="${r}" y1="2" y2="${h * S - 2}" stroke="rgba(0,0,0,.13)" stroke-width="1"/>`;
    s += `<g transform="${onFaceY(x, y + d, z + h)}">${ribs}<rect x="${w * S / 2 - 34}" y="${h * S / 2 - 7}" width="68" height="13" rx="3" fill="rgba(255,255,255,.92)"/><text x="${w * S / 2}" y="${h * S / 2 + 3}" text-anchor="middle" font-size="8.5" font-weight="700" fill="#152033" font-family="Inter,system-ui">${f.file}</text></g>`;
    if (f.bad) {
      const [px, py] = P(x + w - .4, y + .9, z + h);
      s += `<g transform="translate(${px},${py - 4})"><circle r="7.5" fill="#d14343" stroke="#fff" stroke-width="1.5"/><text y="3.2" text-anchor="middle" font-size="9" font-weight="800" fill="#fff" font-family="Inter,system-ui">${f.bad}</text></g>`;
    }
    s += '</g>';
  }
  // loading crane over the stack, and the hopper that feeds the pipe
  s += box(b.x + 7.4, b.y + .3, 0, .35, .35, 5, '#f0b429') + box(b.x + 7.4, b.y + 6.3, 0, .35, .35, 5, '#f0b429');
  s += box(b.x + 7.4, b.y + .3, 5, .35, 6.35, .35, '#f0b429');
  const [hx, hy] = ROAD.pts[0];
  s += groundShadow(hx, hy, .9) + cyl(hx, hy, 0, .95, 1.5, '#6b7a90');
  return s;
}

function quarantineBin() {
  const [x, y] = QBIN, r = 1.15, h = 1.6, [cx, yb] = P(x, y, 0), yt = yb - h * S;
  let s = groundShadow(x, y, r) + cyl(x, y, 0, r, h, '#d14343');
  s += `<ellipse cx="${cx}" cy="${yt}" rx="${rxOf(r) * .82}" ry="${ryOf(r) * .82}" fill="#7a1f1f"/>`;
  for (let k = 0; k < 2; k++) { const yy = yb - (.3 + k * .45) * h * S; s += `<path d="M${cx - rxOf(r)},${yy}A${rxOf(r)},${ryOf(r)} 0 0 0 ${cx + rxOf(r)},${yy}" fill="none" stroke="#fff" stroke-width="2.4" stroke-dasharray="5 5" opacity=".85"/>`; }
  const n = Math.min(ING.lines_quarantined, 6);
  for (let k = 0; k < n; k++) {         // one red block per rejected line, piled in the open bin
    const a = k / n * Math.PI * 2, bx = x + Math.cos(a) * .42 - .2, by = y + Math.sin(a) * .42 - .2;
    s += `<g class="q-crate">${box(bx, by, h - .25, .4, .4, .4, '#e05a4f')}</g>`;
  }
  return s;
}

function sparkCluster(b) {
  let s = box(b.x, b.y, 0, b.w, b.d, .3, { top: '#dbe3ee', left: '#c5d0df', right: '#b5c2d4' });
  [['M', 11.4, 4.6], ['W1', 13.6, 3.9], ['W2', 15.8, 3.9]].forEach(([lab, x, h]) => {
    const y = 12.5, w = 1.5, d = 2;
    s += box(x, y, .3, w, d, h, { top: '#3a4b72', left: '#22304f', right: '#1a2540' });
    let leds = '';
    for (let r = 0; r < Math.floor((h - 1) / .45); r++) for (let c = 0; c < 3; c++)
      leds += `<rect class="led" x="${3 + c * 7}" y="${16 + r * 7}" width="4.5" height="2.5" rx="1"/>`;
    s += `<g transform="${onFaceY(x, y + d, h + .3)}"><text x="${w * S / 2}" y="11" text-anchor="middle" font-size="8.5" font-weight="800" fill="#9fb6ff" font-family="Inter,system-ui">${lab}</text>${leds}</g>`;
  });
  return s;
}

function display() {     // end of the pipe: the charts written by 05_dashboard.py
  const x = 51.3, y = 10.4, SW = 4 * S, SH = 2.8 * S, maxC = Math.max(...D.rating_dist.map(r => r.count));
  let s = box(x, y, 0, 4.6, 1.2, 1, { top: '#e9eef6', left: '#d4dce8', right: '#c3cddc' });
  s += box(x + .1, y + .3, 1, 4.4, .35, 3.4, { top: '#3a4b72', left: '#22304f', right: '#1a2540' });
  let bars = '';
  D.rating_dist.forEach((r, i) => {
    const bw = (SW - 16) / D.rating_dist.length, bh = (SH - 22) * r.count / maxC;
    bars += `<rect x="${8 + i * bw + 1}" y="${SH - 5 - bh}" width="${bw - 2}" height="${bh}" rx="1" fill="${i === 5 ? '#8fb4ff' : '#4d7cf7'}"/>`;
  });
  s += `<g transform="${onFaceY(x + .3, y + .65, 4.2)}"><rect width="${SW}" height="${SH}" rx="3" fill="url(#g-screen)"/>
    <g class="screen-on"><text x="6" y="11" font-size="7.5" font-weight="700" fill="#cfe0ff" font-family="Inter,system-ui">★ ${SUM.mean_rating.toFixed(2)} · r=${SUM.corr_rating_sentiment}</text>${bars}</g></g>`;
  return s;
}

function tree(x, y, sc = 1, v = 0) {
  const [sx, sy] = P(x, y, 0), [cx, cy] = P(x, y, 1.5 * sc);
  return `<ellipse cx="${sx + 6}" cy="${sy + 2}" rx="${13 * sc}" ry="${6 * sc}" fill="rgba(45,70,110,.10)"/>` +
    box(x - .1, y - .1, 0, .2, .2, 1.1 * sc, '#9a7652') +
    `<circle cx="${cx}" cy="${cy}" r="${13 * sc}" fill="url(#${v ? 'g-canopy2' : 'g-canopy'})"/>` +
    `<circle cx="${cx - 4 * sc}" cy="${cy - 5 * sc}" r="${4.5 * sc}" fill="rgba(255,255,255,.22)"/>`;
}

function pillLabel(id, num, text, x, y, z, attr = `data-stage="${id}"`, cls = '') {
  const [px, py] = P(x, y, z), nw = String(num).length * 6.4 + 10, w = text.length * 5.9 + nw + 20;
  return `<g class="pill-label ${cls}" ${attr} transform="translate(${px.toFixed(1)},${py.toFixed(1)})" style="cursor:pointer">
    <rect x="${-w / 2}" y="-11" width="${w}" height="22" rx="11"/><text class="pn" x="${-w / 2 + 10}" y="3.5">${num}</text>
    <text x="${-w / 2 + 10 + nw}" y="3.5">${esc(text)}</text></g>`;
}

function labTank(id) {
  const [x, y] = center(id), c = STAGES[id].color, r = 1.25, h = 2.5, [cx, yb] = P(x, y, 0);
  let s = groundShadow(x, y, r) + cyl(x, y, 0, r + .15, .2, '#dfe6f0') + cyl(x, y, .2, r, h, c);
  const [dx, dy] = P(x, y, h + .2), rx = rxOf(r);
  s += `<path d="M${dx - rx},${dy}A${rx},${rx * .75} 0 0 1 ${dx + rx},${dy}A${rx},${ryOf(r)} 0 0 1 ${dx - rx},${dy}Z" fill="${shade(c, .4)}" opacity=".9"/>`;
  s += `<text x="${cx}" y="${yb - h * S * .42}" text-anchor="middle" font-size="10" font-weight="800" fill="#fff" stroke="${shade(c, -.35)}" stroke-width="2" paint-order="stroke" font-family="Inter,system-ui">${STAGES[id].code}</text>`;
  if (STAGES[id].source) {        // labs that bring their own dataset get a small container beside them
    const [f, n] = STAGES[id].source;
    s += `<g><title>notebooks/data/${f} · ${n}</title>${box(x - 3.9, y + .4, 0, 1.6, .9, .85, shade(c, -.05))}</g>`;
  }
  return s;
}

const svg = $('#scene'), world = $('#world');
const ptsAt = (pts, z) => pts.map(p => P(p[0], p[1], z).map(v => v.toFixed(1)).join(',')).join(' ');
function subPts(d0, d1) {          // the pipe's centre line between two distances, as ground points
  const out = [roadAt(d0)];
  ROAD.cum.forEach((c, i) => { if (c > d0 && c < d1) out.push({ x: ROAD.pts[i][0], y: ROAD.pts[i][1] }); });
  out.push(roadAt(d1));
  return out.map(p => [p.x, p.y]);
}
function freeStretches() {
  const z = [...ZONES].sort((a, b) => a[0] - b[0]), out = [];
  let d = 0;
  z.forEach(([a, b]) => { if (a > d) out.push([d, a]); d = Math.max(d, b); });
  return out.filter(([a, b]) => b - a > .05 && a < ROAD.cum.at(-1)).map(([a, b]) => [a, Math.min(b, ROAD.cum.at(-1))]);
}
const PW = 1.25 * CS * S;          // pipe diameter in px
const pipeBack = pts => `<polyline points="${ptsAt(pts, 0)}" fill="none" stroke="rgba(40,60,100,.08)" stroke-width="${PW}" stroke-linejoin="round" transform="translate(5,3)"/>
  <polyline points="${ptsAt(pts, PIPE_Z)}" fill="none" stroke="#8ea2bf" stroke-width="${PW + 3}" stroke-linejoin="round" stroke-linecap="round"/>
  <polyline points="${ptsAt(pts, PIPE_Z)}" fill="none" stroke="#e3eaf4" stroke-width="${PW}" stroke-linejoin="round" stroke-linecap="round"/>
  <polyline points="${ptsAt(pts, PIPE_Z)}" fill="none" stroke="#cdd8e8" stroke-width="${PW * .45}" stroke-linejoin="round" transform="translate(0,${(PW * .24).toFixed(1)})"/>`;
const pipeGlass = pts => `<polyline points="${ptsAt(pts, PIPE_Z)}" fill="none" stroke="rgba(255,255,255,.28)" stroke-width="${PW}" stroke-linejoin="round"/>
  <polyline points="${ptsAt(pts, PIPE_Z)}" fill="none" stroke="#fff" stroke-width="1.6" stroke-linejoin="round" opacity=".85" transform="translate(0,${(-PW * .3).toFixed(1)})"/>`;
function buildScene() {
  let g = '';
  // faint isometric grid on the ground band that the scene occupies
  g += `<clipPath id="band"><polygon points="${pstr([[-30, 30, 0], [30, -30, 0], [64, 4, 0], [4, 64, 0]])}"/></clipPath><g clip-path="url(#band)" opacity=".55">`;
  for (let i = -30; i <= 64; i += 2) {
    const [a, b] = P(i, -30), [c, e] = P(i, 70), [f, h] = P(-30, i), [k, l] = P(70, i);
    g += `<line x1="${a}" y1="${b}" x2="${c}" y2="${e}" stroke="#dbe3ee" stroke-width=".6"/><line x1="${f}" y1="${h}" x2="${k}" y2="${l}" stroke="#dbe3ee" stroke-width=".6"/>`;
  }
  g += '</g>';
  // lawns
  [[12.5, 32.5, 8, 6], [32.5, 22.5, 8, 7.5], [43, 13, 9, 7], [-1, 53, 13, 5], [7.5, 25, 8, 7], [27, 5, 9, 7], [28, 33.5, 5, 6], [42, 2.5, 8, 6]].forEach(([x, y, w, d]) =>
    g += poly([[x, y, 0], [x + w, y, 0], [x + w, y + d, 0], [x, y + d, 0]], '#dcecd8'));
  // pipe supports, then the pipe itself (blocks are drawn inside it every frame, then the glass on top)
  for (let d = 1.6; d < ROAD.cum.at(-1); d += 3.2) {
    if (hidden(d)) continue;
    const p = roadAt(d); g += box(p.x - .09, p.y - .09, 0, .18, .18, PIPE_Z - .4, '#aab6c8');
  }
  const branch = [[QBIN[0] - 3.4, QBIN[1]], QBIN];
  g += pipeBack(ROAD.pts) + pipeBack(branch);

  // painter's algorithm: everything that stands up is sorted back-to-front by x+y
  const items = [];
  const add = (k, svgStr) => items.push({ k, svg: svgStr });
  for (let t = -42; t <= 50; t += 7.5) {            // background city blocks
    const bx = (16 + t) / 2 - 1.5, by = (16 - t) / 2 - 1.5, h = 1.2 + rnd() * 2.6;
    if (Math.abs(t) < 46) add(bx + by, box(bx, by, 0, 3, 3, h, { top: '#f2f5fa', left: '#e7edf5', right: '#dce4ee' }, 'opacity=".8"'));
    const fx = (72 + t) / 2 - 1.5, fy = (72 - t) / 2 - 1.5;
    add(fx + fy, box(fx, fy, 0, 3, 3, .5 + rnd() * .9, { top: '#f2f5fa', left: '#e7edf5', right: '#dce4ee' }));
  }
  [[1, 37], [3.5, 39.5], [8.5, 37.5], [9, 31], [11.5, 28.5], [6.5, 28], [22, 17], [25, 14], [28.5, 9.5], [31, 7], [38.5, 5.5],
   [21.5, 46], [1.5, 55], [5.5, 56], [10, 55], [35, 27.5], [38, 28], [39.5, 25.5], [45.5, 17.5], [49, 18], [51.5, 15.5],
   [30, 38.5], [32.5, 39], [52, 2], [40, 1], [14.5, 34.5], [18, 33.5], [16.2, 37.4], [44, 5], [47.5, 6.5], [23.5, 24.2], [28, 23.8], [33.8, 14.8], [38.2, 14.2]]
    .forEach(([x, y], i) => add(x + y, tree(x, y, .85 + (i % 3) * .12, i % 2)));
  const grp = (id, inner) => `<g class="bldg" data-stage="${id}" tabindex="0" role="button" aria-label="${STAGES[id].name}">${inner}</g>`;
  add(48, grp('raw', rawYard(BLD.raw)));
  add(60.4, `<g id="q-group">${grp('quarantine', quarantineBin())}</g>`);
  ['bronze', 'silver', 'gold'].forEach(id => add(TANK[id].at[0] + TANK[id].at[1] + .5, grp(id, tank(TANK[id]))));
  D.city_summary.forEach((c, i) => {                // silver is partitioned by city: one small can per partition
    const n = D.replay?.silver_by_city?.[c.city] || c.n, x = 22.6 + i * 1.3, y = 28;
    add(x + y, grp('silver', `<g><title>lake/silver/city=${esc(c.city)}/ · ${fmt(n)} rows</title>${cyl(x, y, 0, .5, .7 + n / 3700, '#b9c2d1')}</g>`));
  });
  [['hotel_stats', 33.4, 18.4, D.replay?.n_hotel_stats], ['city_month', 38.6, 18.4, D.replay?.n_city_month]].forEach(([t, x, y, n]) =>
    add(x + y, grp('gold', `<g><title>lake/gold/${t}/${n ? ` · ${fmt(n)} rows` : ''}</title>${groundShadow(x, y, .8)}${cyl(x, y, 0, .8, 1.9, '#e2b84a')}</g>`)));
  add(57.5, grp('dash', tank(TANK.dash)));
  add(62.5, `<g id="dash-group">${grp('dash', display())}</g>`);
  ROAD.units.forEach((u, i) => { const p = roadAt(u); add(p.x + p.y + .2, unitSVG(i)); });
  add(27, `<g id="cluster">${grp('spark', sparkCluster(BLD.spark))}</g>`);
  LABS.forEach(id => add(26, `<g class="lab">${grp(id, labTank(id))}</g>`));
  items.sort((a, b) => a.k - b.k).forEach(o => g += o.svg);

  // compute links: Spark master -> the unit where each Spark script runs
  const [mx, my] = P(12.15, 13.5, 5.1);
  g += '<g id="links">';
  ['bronze', 'silver', 'gold', 'dash'].forEach((id, i) => {
    const p = roadAt(ROAD.units[i]), [tx, ty] = P(p.x, p.y, 2.3);
    g += `<path class="link" data-to="${id}" d="M${mx},${my} Q${(mx + tx) / 2},${Math.min(my, ty) - 70} ${tx},${ty}"/>`;
  });
  // gold feeds the two labs that read the gold tables
  const [gx, gy] = P(36, 21, TANK.gold.h);
  ['predict', 'optimise'].forEach(id => {
    const [cx, cy] = center(id), [tx, ty] = P(cx, cy, 3);
    g += `<path class="feed" data-to="${id}" d="M${gx},${gy} Q${(gx + tx) / 2},${Math.min(gy, ty) - 50} ${tx},${ty}"/>`;
  });
  g += `</g><g id="blocks"></g><g id="glass">${freeStretches().map(([a, b]) => pipeGlass(subPts(a, b))).join('')}${pipeGlass([[QBIN[0] - 2.4, QBIN[1]], [QBIN[0] - 1.2, QBIN[1]]])}</g><g id="batch"></g><g id="labels">`;
  const labels = [['raw', '0', 'data/raw · 5 files'], ['bronze', 'L1', 'Bronze · lake/bronze'], ['quarantine', '!', `Quarantine · ${ING.lines_quarantined}`],
    ['silver', 'L2', 'Silver · lake/silver'], ['gold', 'L3', 'Gold · lake/gold'], ['dash', 'OUT', 'Insights · output/'], ['spark', '⚡', 'Spark master + 2 workers']];
  LABS.forEach(id => labels.push([id, STAGES[id].code, STAGES[id].label]));
  labels.forEach(([id, n, t]) => { const [cx, cy] = id === 'dash' ? [48.5, 11] : center(id); g += pillLabel(id, n, t, cx, cy, BLD[id].top + .9); });
  ROAD.units.forEach((u, i) => { const p = roadAt(u); g += pillLabel(null, `0${i + 1}`, STEPS[i].script.replace(/^\d+_/, ''), p.x + (i === 4 ? 1.2 : 0), p.y + (i === 4 ? 1.2 : 0), i === 4 ? 2.3 : 3.5, `data-unit="${i}"`, 'unit-pill'); });
  g += '</g><g id="pin"></g><g id="ticker"></g>';
  world.innerHTML = `<defs>${gradDefs}</defs>` + g;
}

/* ============================== data blocks flowing through the pipe ============================== */
// what the data looks like on each stretch between two processing units
const FLOW = [
  { c: '#8e9ab0', s: .62, h: .62, sp: 2.1 },               // raw JSON lines (red = corrupt line)
  { c: '#c98a55', s: .62, h: .62, sp: 2.1 },               // bronze: one block per hotel, still nested
  { c: '#b4bdcc', s: .36, h: .36, sp: .9 },                // silver: exploded into many small review rows
  { c: '#e5b53a', s: .36, h: .36, sp: .9, dot: true },     // gold: each row now carries a sentiment label
  { c: '#2f66f6', s: .66, h: .2, sp: 2.8 },                // output/: a few small summary tables
  { c: '#16935c', s: .5, h: .5, sp: 1.5 },                 // charts on their way to the display
];
const REGIONS = FLOW.map((_, r) => [r ? ROAD.units[r - 1] : 0, r < ROAD.units.length ? ROAD.units[r] : ROAD.cum.at(-1)]);
const SENT_DOT = ['#2f66f6', '#2f66f6', '#2f66f6', '#98a3b5', '#e0703f', '#2f66f6', '#e0703f', '#2f66f6', '#98a3b5', '#2f66f6'];   // ≈ 60 / 20 / 20 mix
const regionOn = r => r === 0 || state.steps[r - 1] === 'done';    // a stretch only carries data once the script before it has produced some
function blockSVG(x, y, st, color, k) {
  const z = PIPE_Z - st.h / 2;
  let s = box(x - st.s / 2, y - st.s / 2, z, st.s, st.s, st.h, color);
  if (st.dot) { const [dx, dy] = P(x, y, z + st.h); s += `<circle cx="${dx.toFixed(1)}" cy="${dy.toFixed(1)}" r="1.9" fill="${SENT_DOT[k % 10]}"/>`; }
  return s;
}
let flowT = 0, flowLast = 0;
const calm = matchMedia('(prefers-reduced-motion: reduce)');
function flowFrame(now) {
  const dt = Math.min(.1, ((now - flowLast) || 16) / 1000); flowLast = now;
  if (!calm.matches && !document.hidden) flowT += dt * 1.3 * state.speed;
  let s = '';
  REGIONS.forEach(([a, b], r) => {
    if (!regionOn(r)) return;
    const st = FLOW[r], n = Math.ceil((b - a) / st.sp) + 1, loop = n * st.sp;
    for (let k = 0; k < n; k++) {
      const d = a + (flowT + k * st.sp) % loop;
      if (d >= b || hidden(d)) continue;
      const p = roadAt(d);
      s += blockSVG(p.x, p.y, st, r === 0 && k % 9 === 4 ? '#d14343' : st.c, k);
    }
  });
  if (regionOn(1)) for (let k = 0; k < 3; k++) {      // rejected lines branch off into the quarantine bin
    const d = (flowT * .5 + k * 1.3) % 3.9;
    if (d > 1 && d < 2.25) s += blockSVG(QBIN[0] - 3.4 + d, QBIN[1], FLOW[0], '#d14343', k);
  }
  $('#blocks').innerHTML = s;
  requestAnimationFrame(flowFrame);
}

/* ============================== the tracked batch ============================== */
const batch = { dist: ROAD.stops[0], payload: `${ING.lines_read} lines`, visible: true };
function batchSVG(gx, gy, label, snap) {
  const c = SNAPS[snap].color, sz = 1.05, z = PIPE_Z - sz / 2, [rx, ry] = P(gx, gy, PIPE_Z);
  let s = `<ellipse class="batch-ring" cx="${rx}" cy="${ry}" rx="20" ry="13" fill="none" stroke="${c}" stroke-width="2"/>`;
  s += box(gx - sz / 2, gy - sz / 2, z, sz, sz, sz, c);
  const [px, py] = P(gx, gy, 2.9), w = label.length * 6 + 16;
  s += `<g class="payload" transform="translate(${px.toFixed(1)},${py.toFixed(1)})"><rect x="${-w / 2}" y="-10" width="${w}" height="19" rx="9.5"/><text text-anchor="middle" y="3.4">${esc(label)}</text></g>`;
  return s;
}
function drawBatch() {
  const p = roadAt(batch.dist);
  $('#batch').innerHTML = batch.visible ? `<g data-cargo="1" style="cursor:pointer">${batchSVG(p.x, p.y, batch.payload, cargo.i)}</g>` : '';
  linkCargo();
}
function syncStores() {           // empty the level gauges of layers that have not been written yet
  world.querySelectorAll('.bldg[data-stage]').forEach(b => b.classList.toggle('empty', state.stage[b.dataset.stage] === 'queued'));
}

/* ============================== cargo: what the dataset looks like between stages ============================== */
const DM = D.replay?.demo, demoRev = id => DM?.reviews.find(r => r.review_id === id) || {};
const col = (n, t, from) => ({ n, t, from });
const SILVER_COLS = [col('hotel_id', 'string'), col('hotel_name', 'string', 'name'), col('stars', 'int'), col('city', 'string · partition', 'location'),
  col('review_id', 'string', 'reviews'), col('user', 'string', 'reviews'), col('rating', 'double', 'reviews.rating'), col('rating_raw', 'string', 'reviews.rating'),
  col('text', 'string', 'reviews'), col('tags', 'array<string>', 'reviews'), col('helpful_votes', 'int', 'reviews'), col('review_date', 'date', 'reviews.date'), col('has_tags', 'boolean', 'reviews')];
const ratingProblems = ['missing_key', 'null', 'na_text', 'out_of_range'].reduce((a, k) => a + (RAW.issues[k] || 0), 0);
const sampleIds = ['H-1001-R039', 'H-1001-R012', 'H-1001-R043'];
const SNAPS = [
  { title: 'Raw JSON lines', stop: 0, color: '#6b7a90', icon: 'file', format: 'JSON Lines (text, no schema)', where: 'data/raw/*.jsonl', rows: [ING.lines_read, 'lines'], grain: '1 line = 1 hotel, reviews nested inside', step: [0, 0],
    cols: [col('hotel_id', 'text'), col('name', 'text'), col('location', 'object'), col('stars', 'number'), col('reviews', 'array of objects'), col('reviews.rating', 'number | text | null'), col('reviews.date', 'text, 2 formats')],
    sample: () => `<div class="code xs">${sampleIds.map(id => { const r = demoRev(id); return esc(JSON.stringify({ review_id: r.review_id, rating: 'rating' in r ? r.rating : undefined, date: r.date })); }).join('\n')}</div>` },
  { title: 'Bronze Parquet (nested)', stop: 1, color: '#c27c45', icon: 'layers', format: 'Parquet · columnar, typed', where: 'lake/bronze/', rows: [goodLines, 'hotel rows'], grain: '1 row = 1 hotel (still nested)', step: [0, 5],
    cols: [col('hotel_id', 'string'), col('name', 'string'), col('location', 'struct'), col('stars', 'int'), col('reviews', 'array<struct>'), col('reviews.rating', 'string'), col('reviews.date', 'string'), col('_ingested_at', 'timestamp'), col('_source_file', 'string')],
    sample: () => miniTbl(['hotel_id', 'name', 'reviews', '_source_file'], [[DM?.hotel.hotel_id, esc(DM?.hotel.name || ''), `array[${DM?.n_reviews}]`, `…/hotels_${String(+(DM?.hotel.hotel_id || 'H-0').slice(2) % 5).padStart(2, '0')}.jsonl`]]) },
  { title: 'Silver Parquet (flat, clean)', stop: 2, color: '#8692a8', icon: 'filter', format: 'Parquet · partitionBy(city)', where: 'lake/silver/city=*/', rows: [CLN.silver_rows, 'review rows'], grain: '1 row = 1 review', step: [1, 0],
    cols: SILVER_COLS,
    sample: () => miniTbl(['review_id', 'rating_raw', 'rating', 'review_date'], sampleIds.map(id => { const r = demoRev(id), raw = 'rating' in r && r.rating !== null ? String(r.rating) : null, v = raw != null && !isNaN(+raw) && +raw >= 1 && +raw <= 5 ? (+raw).toFixed(1) : null;
      const m = /^(\d\d)\/(\d\d)\/(\d{4})$/.exec(r.date); return [r.review_id, raw == null ? 'null' : `"${esc(raw)}"`, v == null ? '<b class="bad">null</b>' : v, m ? `<b class="chg">${m[3]}-${m[2]}-${m[1]}</b>` : r.date]; })) },
  { title: 'Gold tables (features + aggregates)', stop: 3, color: '#d29d18', icon: 'sparkle', format: 'Parquet · 3 tables', where: 'lake/gold/', rows: [CLN.silver_rows, 'review rows + aggregates'], grain: 'reviews: 1 row = 1 review', step: [2, 0],
    extra: `+ hotel_stats (${fmt(D.replay?.n_hotel_stats ?? 0)} rows, 1 per hotel) · city_month (${fmt(D.replay?.n_city_month ?? 0)} rows, 1 per city × month)`,
    cols: [...SILVER_COLS.map(c => col(c.n, c.t)), col('n_words', 'int'), col('pos_hits', 'int'), col('neg_hits', 'int'), col('sentiment', 'double'), col('label', 'string'), col('year', 'int'), col('month', 'string'), col('weekday', 'string')],
    sample: () => miniTbl(['review_id', 'sentiment', 'label', 'month'], sampleIds.map(id => { const r = demoRev(id), f = sentiment(r.text || ''); return [r.review_id, (f.s >= 0 ? '+' : '') + f.s.toFixed(3), f.label, (/^\d{4}-\d\d/.exec(r.date) || [(r.date || '').split('/').reverse().slice(0, 2).join('-')])[0]]; })) },
  { title: 'Summary tables (small CSV / JSON)', stop: 4, color: '#2f66f6', icon: 'chart', format: 'CSV + JSON on the driver', where: 'output/', rows: [SUM.n_rated_reviews, 'rated reviews → 7 small files'], grain: '1 row = 1 group (city, rating, month…)', step: [3, 0],
    files: [['summary.json', 1], ['city_summary.csv', D.city_summary.length], ['rating_dist.csv', D.rating_dist.length], ['sentiment_vs_rating.csv', D.sentiment.length], ['monthly_volume.csv', D.monthly_volume.length], ['top_hotels.csv', D.top_hotels.length], ['dq_*.json', 2]],
    sample: () => miniTbl(['city', 'n', 'avg_rating'], D.city_summary.slice(0, 3).map(c => [esc(c.city), fmt(c.n), c.avg_rating])) },
  { title: 'Charts (PNG)', stop: 5, color: '#16935c', icon: 'chart', format: 'PNG images (matplotlib)', where: 'output/chart_*.png', rows: [5, 'charts'], grain: '1 file = 1 chart', step: [4, 0],
    files: ['dq_funnel', 'city_rating', 'monthly_volume', 'rating_dist', 'sentiment'].map(n => [`chart_${n}.png`, '']),
    sample: () => `<div class="cg-pngs">${['rating_dist', 'sentiment'].map(n => `<img src="../output/chart_${n}.png" alt="chart_${n}.png" onerror="this.remove()">`).join('')}</div>` },
];
// data-quality problems injected in the source, and their status at each snapshot
const DQ_TRACK = [
  ['Corrupt JSON lines', [['open', ING.lines_quarantined], ['fixed', 'quarantined'], ['fixed', 'quarantined']]],
  ['Duplicate reviews', [['open', RAW.issues.duplicate || 0], ['open', RAW.issues.duplicate || 0], ['fixed', 'dropped']]],
  ['Missing / "N/A" / out-of-range rating', [['open', ratingProblems], ['open', ratingProblems], ['flag', `${fmt(CLN.rating_missing_or_invalid)} → null, text kept`]]],
  ['Rating stored as text', [['open', RAW.issues.string_number || 0], ['hold', 'whole column STRING'], ['fixed', 'cast to double']]],
  ['dd/MM/yyyy dates', [['open', RAW.issues.date_ddmmyyyy || 0], ['open', RAW.issues.date_ddmmyyyy || 0], ['fixed', 'parsed to date']]],
];
const dqAt = (row, i) => i < 3 ? row[1][i] : i === 3 ? row[1][2] : ['fixed', i === 4 && row[0].startsWith('Missing') ? 'rated reviews only' : row[1][2][1]];
function miniTbl(cols, rows) {
  return `<table class="cg-tbl"><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr>${rows.map(r => `<tr>${r.map(v => `<td>${v ?? ''}</td>`).join('')}</tr>`).join('')}</table>`;
}
function diffCols(prev, cur) {
  const pm = new Map(prev.map(c => [c.n, c])), used = new Set(cur.map(c => c.from).filter(Boolean));
  const out = cur.map(c => { const p = pm.get(c.n); return p ? { ...c, s: p.t === c.t ? 'same' : 'chg', was: p.t } : { ...c, s: 'new' }; });
  prev.forEach(p => { if (!cur.some(c => c.n === p.n)) out.push({ ...p, s: used.has(p.n) ? 'flat' : 'gone' }); });
  return out;
}
const cargo = { i: 0, mode: 'idle', open: true, prevShown: -1 };
function renderCargo() {
  const el = $('#cargo'); if (!el) return;
  el.hidden = !cargo.open;
  $('#map-tools [data-z="cargo"]')?.classList.toggle('on', cargo.open);
  if (!cargo.open) { linkCargo(); return; }
  const sn = SNAPS[cargo.i], prev = SNAPS[cargo.i - 1], st = STEPS[Math.min(cargo.i, STEPS.length - 1)];
  const status = cargo.mode === 'transit' ? `<span class="chip run">In transit → ${STAGES[STEPS[cargo.i].stage].name}</span>`
    : cargo.mode === 'processing' ? `<span class="chip run">Being transformed by ${STEPS[cargo.i - 1].script}</span>`
    : cargo.mode === 'stored' ? `<span class="chip good">Stored at ${sn.where}</span>` : `<span class="chip">Snapshot ${cargo.i + 1} of ${SNAPS.length}</span>`;
  let schema = '';
  if (sn.cols) {
    const d = prev?.cols ? diffCols(prev.cols, sn.cols) : sn.cols.map(c => ({ ...c, s: 'same' }));
    const same = d.filter(c => c.s === 'same'), changed = d.filter(c => c.s !== 'same');
    const lab = { new: '+', chg: '~', flat: '⇢', gone: '−' };
    schema = `<div class="cg-sec"><b>Columns</b><span>${sn.cols.length} columns${prev?.cols ? ' · changes vs previous stage' : ''}</span></div><div class="cg-cols">${
      changed.map((c, k) => `<span class="cc ${c.s}" style="animation-delay:${k * 60}ms" title="${esc(c.n)}: ${esc(c.t)}${c.was && c.s === 'chg' ? ` (was ${esc(c.was)})` : ''}${c.from ? ` · from ${esc(c.from)}` : ''}">${lab[c.s]} ${esc(c.n)}<small>${c.s === 'chg' ? `${esc(c.was)}→${esc(c.t)}` : c.s === 'flat' ? 'flattened' : c.s === 'gone' ? 'dropped' : esc(c.t)}</small></span>`).join('')}${
      same.length ? `<span class="cc same" title="${esc(same.map(c => c.n).join(', '))}">${prev?.cols ? `${same.length} unchanged` : same.map(c => `${esc(c.n)} <small>${esc(c.t)}</small>`).join('</span><span class="cc same">')}</span>` : ''}</div>`;
  } else {
    schema = `<div class="cg-sec"><b>Files</b><span>${sn.files.length} files</span></div><div class="cg-cols">${sn.files.map(([f, n]) => `<span class="cc new">${esc(f)}${n !== '' ? `<small>${n} rows</small>` : ''}</span>`).join('')}</div>`;
  }
  const dq = DQ_TRACK.map(row => { const [s, v] = dqAt(row, cargo.i); return `<div class="dq ${s}"><i></i><span>${row[0]}</span><b>${typeof v === 'number' ? fmt(v) : esc(v)}</b></div>`; }).join('');
  el.innerHTML = `<div class="cg-head"><div class="ph-icon" style="--c:${sn.color}">${icon(sn.icon, 'sm')}</div><div><div class="eyebrow">Tracked data batch</div><h4>${sn.title}</h4></div>
      <button class="icon-btn" data-cg="close" aria-label="Hide data card">${icon('x', 'sm')}</button></div>
    <div class="cg-status">${status}</div>
    <div class="cg-meta"><div><label>Rows</label><b class="num">${fmt(sn.rows[0])}</b><span>${sn.rows[1]}</span></div><div><label>Format</label><b>${sn.format}</b><span>${sn.where}</span></div></div>
    <div class="cg-grain">${icon('box', 'sm')} ${sn.grain}</div>${sn.extra ? `<div class="cg-extra">${sn.extra}</div>` : ''}
    <div class="cg-sec"><b>Data-quality problems</b><span>${DQ_TRACK.filter(r => dqAt(r, cargo.i)[0] === 'open').length} still open</span></div><div class="cg-dq">${dq}</div>
    ${schema}
    <div class="cg-sec"><b>Sample from ${esc(DM?.hotel.name || 'the data')}</b><span></span></div>${sn.sample()}
    <div class="cg-foot">${state.running ? '' : `<button class="btn" data-cg="prev" ${cargo.i ? '' : 'disabled'}>←</button>
      <div class="cg-dots">${SNAPS.map((s, k) => `<button data-cg="go" data-k="${k}" class="${k === cargo.i ? 'on' : ''}" title="${s.title}" style="--c:${s.color}"></button>`).join('')}</div>
      <button class="btn" data-cg="next" ${cargo.i < SNAPS.length - 1 ? '' : 'disabled'}>→</button>`}
      <button class="btn" data-cg="inside">${icon('search', 'sm')} How it's made</button></div>`;
  if (cargo.prevShown !== cargo.i) { el.animate([{ opacity: .4, transform: 'translateY(4px)' }, { opacity: 1, transform: 'none' }], { duration: 350 }); cargo.prevShown = cargo.i; }
  linkCargo();
}
function setCargo(i, mode) { cargo.i = i; cargo.mode = mode; renderCargo(); }
// dashed line from the card to the tracked batch so it is obvious which data the card describes
function linkCargo() {
  const ln = $('#cargo-link'), el = $('#cargo');
  if (!ln || !el) return;
  if (el.hidden || !batch.visible || getComputedStyle(el).position !== 'absolute') { ln.innerHTML = ''; return; }
  const p = roadAt(batch.dist), [px, py] = P(p.x, p.y, 1.2), wrap = $('#scene-wrap').getBoundingClientRect(), c = el.getBoundingClientRect();
  const tx = (px - cam.x) * cam.k, ty = (py - cam.y) * cam.k, cx = c.right - wrap.left, cy = clamp(ty, c.top - wrap.top + 24, c.bottom - wrap.top - 24);
  ln.innerHTML = tx > cx + 8 ? `<path d="M${cx},${cy} C${cx + 60},${cy} ${tx - 60},${ty} ${tx},${ty}"/><circle cx="${tx}" cy="${ty}" r="5"/><circle cx="${cx}" cy="${cy}" r="3.5"/>` : '';
}
let cargoAnim = 0;
function browseCargo(i) {
  if (state.running) return;
  i = clamp(i, 0, SNAPS.length - 1);
  setCargo(i, 'idle');
  const to = ROAD.stops[SNAPS[i].stop], from = batch.dist, t0c = performance.now(), id = ++cargoAnim;
  batch.payload = `${fmt(SNAPS[i].rows[0])} ${SNAPS[i].rows[1].split(' ')[0]}`;
  const tick = now => {
    if (id !== cargoAnim || state.running) return;
    const t = clamp((now - t0c) / Math.max(250, Math.abs(to - from) * 28), 0, 1);
    batch.dist = from + (to - from) * ease(t); drawBatch();
    if (t < 1) requestAnimationFrame(tick);
  };
  requestAnimationFrame(tick);
}
$('#cargo').addEventListener('click', e => {
  const b = e.target.closest('[data-cg]'); if (!b) return;
  const a = b.dataset.cg;
  if (a === 'close') { cargo.open = false; renderCargo(); fit(); }
  if (a === 'prev') browseCargo(cargo.i - 1);
  if (a === 'next') browseCargo(cargo.i + 1);
  if (a === 'go') browseCargo(+b.dataset.k);
  if (a === 'inside') window.RF.openExplorer?.(...SNAPS[cargo.i].step);
});

/* ============================== camera ============================== */
const cam = { x: 0, y: 0, k: 1 };
function applyCam() {
  const r = svg.getBoundingClientRect();
  svg.setAttribute('viewBox', `${cam.x} ${cam.y} ${r.width / cam.k} ${r.height / cam.k}`);
  linkCargo();
}
function availRect() {
  const r = svg.getBoundingClientRect(), floating = getComputedStyle($('#panel')).position === 'absolute';
  if (!floating) return { l: 8, t: 8, r: r.width - 52, b: r.height - 8 };
  const cg = $('#cargo');
  return { l: cg && !cg.hidden ? cg.getBoundingClientRect().right + 16 : 16, t: $('#kpis').getBoundingClientRect().bottom + 10, r: $('#panel').getBoundingClientRect().left - 60,
           b: Math.min($('#tracking').getBoundingClientRect().top, $('#ops').getBoundingClientRect().top) - 10 };
}
function coreBox() {        // fit to the pipeline itself, not the decorative city around it
  const pts = [P(1.5, 49.6), P(1.5, 42.6, 6), P(10.8, 11.8, 7), P(17.4, 5.4, 6.5), P(36.6, -10.6, 6.5), P(56, 10.4, 5.5), P(56, 11.6), P(ROAD.pts[0][0], ROAD.pts[0][1]), P(...ROAD.pts.at(-1))];
  const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]);
  return { x: Math.min(...xs), y: Math.min(...ys), width: Math.max(...xs) - Math.min(...xs), height: Math.max(...ys) - Math.min(...ys) };
}
function fit(animate = true) {
  const b = coreBox(), a = availRect();
  const k = clamp(Math.min((a.r - a.l) / b.width, (a.b - a.t) / b.height), .35, 3);
  const cx = b.x + b.width / 2, cy = b.y + b.height / 2;
  tweenCam({ k, x: cx - ((a.l + a.r) / 2) / k, y: cy - ((a.t + a.b) / 2) / k }, animate ? 600 : 0);
}
function focusOn(id, animate = true) {
  if (!BLD[id]) return;
  const a = availRect(), [cx, cy] = center(id), [px, py] = P(cx, cy, 2), k = Math.max(cam.k, 1.15);
  tweenCam({ k, x: px - ((a.l + a.r) / 2) / k, y: py - ((a.t + a.b) / 2) / k }, animate ? 650 : 0);
}
let camAnim = 0;
function tweenCam(to, ms) {
  const from = { ...cam }, t0 = performance.now(), id = ++camAnim;
  if (!ms) { Object.assign(cam, to); applyCam(); return; }
  const tick = now => {
    if (id !== camAnim) return;
    const t = ease(clamp((now - t0) / ms, 0, 1));
    for (const key of ['x', 'y', 'k']) cam[key] = from[key] + (to[key] - from[key]) * t;
    applyCam(); if (t < 1) requestAnimationFrame(tick);
  };
  requestAnimationFrame(tick);
}
function zoomAt(f, sx, sy) {
  camAnim++;
  const k = clamp(cam.k * f, .3, 4), wx = cam.x + sx / cam.k, wy = cam.y + sy / cam.k;
  cam.x = wx - sx / k; cam.y = wy - sy / k; cam.k = k; applyCam();
}
svg.addEventListener('wheel', e => {
  e.preventDefault();
  const r = svg.getBoundingClientRect();
  zoomAt(Math.exp(-e.deltaY * .0015), e.clientX - r.left, e.clientY - r.top);
}, { passive: false });
let drag = null;
svg.addEventListener('pointerdown', e => { drag = { x: e.clientX, y: e.clientY, cx: cam.x, cy: cam.y, moved: false }; });
window.addEventListener('pointermove', e => {
  if (!drag) return;
  const dx = e.clientX - drag.x, dy = e.clientY - drag.y;
  if (!drag.moved && Math.hypot(dx, dy) > 4) { drag.moved = true; svg.classList.add('dragging'); svg.setPointerCapture?.(e.pointerId); camAnim++; }
  if (drag.moved) { cam.x = drag.cx - dx / cam.k; cam.y = drag.cy - dy / cam.k; applyCam(); }
});
window.addEventListener('pointerup', e => {
  if (drag && !drag.moved && e.target.closest?.('[data-cargo]')) { cargo.open = true; renderCargo(); fit(); drag = null; return; }
  const unit = drag && !drag.moved && e.target.closest?.('[data-unit]');
  if (unit && svg.contains(unit)) { const i = +unit.dataset.unit; select(STEPS[i].stage); window.RF.openExplorer?.(i); drag = null; return; }
  if (drag && !drag.moved) {
    const hit = e.target.closest?.('[data-stage]');
    if (hit && svg.contains(hit)) select(hit.dataset.stage, true);
  }
  drag = null; svg.classList.remove('dragging');
});
svg.addEventListener('keydown', e => {
  const unit = e.target.closest?.('[data-unit]');
  if (unit && (e.key === 'Enter' || e.key === ' ')) { e.preventDefault(); select(STEPS[+unit.dataset.unit].stage); window.RF.openExplorer?.(+unit.dataset.unit); return; }
  const hit = e.target.closest?.('[data-stage]');
  if (hit && (e.key === 'Enter' || e.key === ' ')) { e.preventDefault(); select(hit.dataset.stage, true); }
});

/* ============================== selection + side panel ============================== */
function select(id, focus = false) {
  state.selected = id;
  const lit = BLD[id] ? null : window.RF?.litFor?.(id) || null;   // lectures and milestones highlight several places
  svg.classList.toggle('focus-mode', !!lit);
  world.querySelectorAll('.bldg').forEach(b => { b.classList.toggle('sel', b.dataset.stage === id); b.classList.toggle('lit', !!lit && lit.includes(b.dataset.stage)); });
  world.querySelectorAll('.pill-label').forEach(b => b.classList.toggle('lit', !!lit && lit.includes(b.dataset.stage)));
  const at = BLD[id] ? id : lit?.find(k => BLD[k]);
  if (at) {
    const [cx, cy] = center(at), [px, py] = P(cx, cy, BLD[at].top + 2.3);
    $('#pin').innerHTML = `<g transform="translate(${px},${py})"><g class="pin"><path d="M0 0C-8-9-12-14-12-21a12 12 0 0 1 24 0c0 7-4 12-12 21z" fill="#2f66f6" stroke="#fff" stroke-width="2"/><circle cy="-21" r="4.5" fill="#fff"/></g></g>`;
  } else $('#pin').innerHTML = '';
  renderPanel();
  if (focus) BLD[id] ? focusOn(id) : fit();
}

function statusChip(id) {
  const st = state.stage[id];
  if (id === 'spark') return st === 'busy' ? '<span class="chip run">Executing</span>' : '<span class="chip good">Ready</span>';
  return st === 'running' ? '<span class="chip run">Processing</span>' : st === 'queued' ? '<span class="chip">Waiting</span>'
    : `<span class="chip good">${id === 'raw' ? 'Available' : 'Materialised'}</span>`;
}
const tile = (label, value, sub = '', meter = null, color = 'var(--good)') =>
  `<div class="tile"><label>${label}</label><b>${value}</b>${sub ? `<div class="tsub">${sub}</div>` : ''}${meter != null ? `<div class="meter"><i style="width:${meter}%;background:${color}"></i></div>` : ''}</div>`;
const head = (id, eyebrow, sub) => `<div class="ph"><div class="ph-icon" style="--c:${STAGES[id].color}">${icon(STAGES[id].icon)}</div>
  <div><div class="eyebrow">${eyebrow}</div><h2>${STAGES[id].name}</h2><div class="sub">${sub}</div></div>
  <button class="icon-btn" data-act="focus" title="Zoom to ${STAGES[id].name}" aria-label="Zoom to stage">${icon('locate', 'sm')}</button></div>`;
const sec = (title, right, body) => `<div class="section"><div class="sec-head"><span>${title}</span><span>${right}</span></div>${body}</div>`;
const fileRows = key => `<div class="rows">${RAW.files.map(f => `<div class="row"><div class="ri">${icon('file', 'sm')}</div>
  <div class="rt"><b>${f.file}</b><span>${f.kb} KB · ${f.lines} lines</span></div>
  ${key === 'bad' ? `<span class="rv">${f.bad}</span>` : `<span class="rv">${f.good}</span>`}
  ${f.bad ? `<span class="chip ${key === 'bad' ? 'bad' : 'warn'}">${f.bad} corrupt</span>` : '<span class="chip good">Clean</span>'}</div>`).join('')}</div>`;

const PANELS = {
  raw: () => head('raw', 'Source · data/raw/*.jsonl', 'Seeded generator · nested hotel → reviews[]') +
    `<div class="chips">${statusChip('raw')}<span class="chip">${RAW.files.length} files</span><span class="chip">${fmt(rawKB)} KB</span><span class="chip">nested JSON lines</span></div>
    <div class="tiles">${tile('Lines (1 hotel each)', ING.lines_read, `${RAW.files.length} JSONL files`)}${tile('Reviews incl. dups', fmt(CLN.reviews_flattened), 'inside parseable lines')}
    ${tile('Parseable lines', goodLines, pct(goodLines, ING.lines_read), 100 * goodLines / ING.lines_read)}${tile('Corrupt lines', ING.lines_quarantined, 'truncated JSON → quarantine')}</div>` +
    sec('Injected quality problems', 'click to trace', ISSUES.map(([k, lab, ex]) => {
      const max = Math.max(...ISSUES.map(i => RAW.issues[i[0]]));
      return `<button class="hbar" data-trace="${k}"><span>${lab} <span class="muted mono">${esc(ex)}</span></span><b>${fmt(RAW.issues[k])}</b><i style="--w:${100 * RAW.issues[k] / max}%;--c:${k === 'duplicate' || k === 'date_ddmmyyyy' ? 'var(--warn)' : 'var(--bad)'}"></i></button>`;
    }).join('')) + sec('Inventory', 'parseable lines', fileRows('good')),

  bronze: () => head('bronze', 'Stage 1 · pipeline/01_ingest.py', 'lake/bronze · Parquet + _ingested_at, _source_file') +
    `<div class="chips">${statusChip('bronze')}<span class="chip">schema-on-read</span><span class="chip">mode = PERMISSIVE</span></div>
    <div class="tiles">${tile('Lines read', ING.lines_read)}${tile('Good → bronze', goodLines, pct(goodLines, ING.lines_read), 100 * goodLines / ING.lines_read)}
    ${tile('Quarantined', ING.lines_quarantined, 'kept in lake/quarantine')}${tile('Audit columns', 2, '_ingested_at · _source_file')}</div>` +
    sec('What happens here', '', `<ol class="steps-list"><li>Explicit schema; <code>rating</code> read as STRING because the source mixes 4.5, "4.5", "N/A" and null</li>
      <li>Unparseable lines land in <code>_corrupt</code> instead of failing the job</li><li><code>.cache()</code>, then split good vs. bad</li>
      <li>Good rows → bronze Parquet, bad rows → quarantine JSON</li></ol>`) +
    sec('Code', '01_ingest.py', `<div class="code"><span class="k">raw</span> = (spark.read.schema(schema)
  .option(<span class="s">"mode"</span>, <span class="s">"PERMISSIVE"</span>)
  .option(<span class="s">"columnNameOfCorruptRecord"</span>, <span class="s">"_corrupt"</span>)
  .json(RAW).cache())
<span class="k">bad</span>  = raw.filter(F.col(<span class="s">"_corrupt"</span>).isNotNull())</div>`) +
    sec('Inventory', 'good lines per file', fileRows('good')),

  quarantine: () => head('quarantine', 'Stage 1 side output · lake/quarantine', 'Rejected lines, kept for inspection - never silently dropped') +
    `<div class="chips">${statusChip('quarantine')}<span class="chip bad">${ING.lines_quarantined} rejected</span></div>
    <div class="tiles">${tile('Rejected lines', ING.lines_quarantined)}${tile('Reject rate', pct(ING.lines_quarantined, ING.lines_read))}
    ${tile('Files affected', RAW.files.filter(f => f.bad).length, `of ${RAW.files.length}`)}${tile('Cause', 'Truncated', 'line cut mid-object')}</div>` +
    (RAW.corrupt_sample ? sec('Tail of a rejected line', '_corrupt column', `<div class="code">…${esc(RAW.corrupt_sample.slice(-120))}<span class="hl"> ⟵ EOF</span></div>`) : '') +
    sec('Inventory', 'rejected per file', fileRows('bad')),

  silver: () => head('silver', 'Stage 2 · pipeline/02_clean_flatten.py', 'lake/silver · one row per review · partitionBy(city)') +
    `<div class="chips">${statusChip('silver')}<span class="chip">${D.city_summary.length} partitions</span><span class="chip warn">−${dupes} duplicates</span></div>
    <div class="tiles">${tile('Flattened reviews', fmt(CLN.reviews_flattened), 'explode(reviews)')}${tile('After de-dup', fmt(CLN.after_dedup), `−${dupes} on review_id`, 100 * CLN.after_dedup / CLN.reviews_flattened)}
    ${tile('Rating → null', fmt(CLN.rating_missing_or_invalid), 'missing, "N/A", out of range', 100 * CLN.rating_missing_or_invalid / CLN.after_dedup, 'var(--warn)')}${tile('Unparsed dates', CLN.date_unparsed, `${RAW.issues.date_ddmmyyyy || 0} dd/MM/yyyy repaired`)}</div>` +
    sec('Transformations', '', `<ol class="steps-list"><li><code>explode("reviews")</code> → lift nested fields to columns</li><li><code>dropDuplicates(["review_id"])</code></li>
      <li><code>cast("double")</code> then keep only 1–5</li><li><code>coalesce(to_date(yyyy-MM-dd), to_date(dd/MM/yyyy))</code></li><li><code>trim(text)</code>, flag <code>has_tags</code></li></ol>`) +
    sec('Inventory', 'city partitions', `<div class="rows">${D.city_summary.map(c => `<div class="row"><div class="ri">${icon('box', 'sm')}</div>
      <div class="rt"><b>city=${esc(c.city)}</b><span>${RAW.hotels_by_city[c.city] || '–'} hotels · ${fmt(c.n)} rated reviews</span></div>
      <span class="chip ${c.avg_rating >= SUM.mean_rating ? 'good' : 'warn'}">★ ${c.avg_rating.toFixed(2)}</span></div>`).join('')}</div>`),

  gold: () => {
    const tot = D.sentiment.reduce((a, r) => a + r.n, 0), ord = ['positive', 'neutral', 'negative'], col = { positive: 'var(--brand)', neutral: 'var(--neu)', negative: 'var(--neg)' };
    return head('gold', 'Stage 3 · pipeline/03_features.py', 'lake/gold · reviews, hotel_stats, city_month') +
    `<div class="chips">${statusChip('gold')}<span class="chip">3 tables</span><span class="chip">lexicon sentiment</span></div>
    <div class="tiles">${ord.map(l => SENT[l] ? tile(l[0].toUpperCase() + l.slice(1) + ' reviews', fmt(SENT[l].n), `avg rating ${SENT[l].avg_rating.toFixed(2)}`) : '').join('')}
    ${tile('Lexicon size', POS.length + NEG.length, `${POS.length} positive · ${NEG.length} negative`)}</div>` +
    sec('Sentiment mix (rated reviews)', fmt(tot), `<div class="stack">${ord.map(l => `<i style="width:${100 * SENT[l].n / tot}%;background:${col[l]}" title="${l}: ${fmt(SENT[l].n)}"></i>`).join('')}</div>
      <div class="legend">${ord.map(l => `<span><i style="background:${col[l]}"></i>${l} ${pct(SENT[l].n, tot)}</span>`).join('')}</div>`) +
    sec('Try the feature step', 'same lexicon as Spark', `<div class="try"><input id="try-text" value="Friendly staff but noisy at night and slow service." aria-label="Review text to score"><div class="try-out" id="try-out"></div></div>`) +
    sec('Lexicon', '', `<div class="lex">${POS.map(w => `<span class="p">${w}</span>`).join('')}${NEG.map(w => `<span class="n">${w}</span>`).join('')}</div>`) +
    sec('Gold tables', '', `<div class="rows">${[['reviews', 'n_words, pos_hits, neg_hits, sentiment, label, month, weekday'], ['hotel_stats', 'n_reviews, avg_rating, avg_sentiment, pct_positive'], ['city_month', 'n_reviews, avg_rating per city × month']]
      .map(([t, c]) => `<div class="row"><div class="ri">${icon('db', 'sm')}</div><div class="rt"><b>gold/${t}</b><span>${c}</span></div></div>`).join('')}</div>`);
  },

  dash: () => head('dash', 'Stage 4–5 · 04_analytics.py → 05_dashboard.py', 'output/ · CSV summaries + PNG charts') +
    `<div class="chips">${statusChip('dash')}<span class="chip">${fmt(SUM.n_rated_reviews)} rated reviews</span></div>
    <div class="tiles">${tile('Mean rating', SUM.mean_rating.toFixed(3), '★ out of 5')}${tile('Median', SUM.median.toFixed(1), `IQR ${SUM.q1}–${SUM.q3}`)}
    ${tile('Rating ↔ sentiment', 'r ' + SUM.corr_rating_sentiment, 'Pearson, gold/reviews', SUM.corr_rating_sentiment * 100, 'var(--brand)')}${tile('Top city', [...D.city_summary].sort((a, b) => b.avg_rating - a.avg_rating)[0].city, 'highest avg rating')}</div>
    <button class="btn primary block" data-act="insights">${icon('chart', 'sm')} Open insights dashboard</button>` +
    sec('Top hotels', '≥ 30 rated reviews', `<div class="rows">${D.top_hotels.slice(0, 5).map((h, i) => `<div class="row"><div class="ri"><b>${i + 1}</b></div>
      <div class="rt"><b>${esc(h.hotel_name)}</b><span>${esc(h.city)} · ${'★'.repeat(h.stars)} · ${h.n_reviews} reviews</span></div><span class="chip good">★ ${h.avg_rating.toFixed(2)}</span></div>`).join('')}</div>`),

  spark: () => head('spark', 'Compute · common.get_spark()', 'SPARK_MASTER = local[*] or spark://spark-master:7077') +
    `<div class="chips">${statusChip('spark')}<span class="chip">Spark 3.5.3</span><span class="chip">Java 17</span></div>
    <div class="tiles">${tile('Workers', 2, '2 cores · 2 GB each')}${tile('Shuffle partitions', 8, 'SHUFFLE_PARTITIONS')}
    ${tile('Spark jobs', 4, 'steps 01–04')}${tile('Master UI', ':8080', 'docker compose')}</div>` +
    sec('How to run', '', `<div class="code"><span class="c"># laptop, local[*]</span>
./run_all.sh
<span class="c"># 1 master + 2 workers in Docker</span>
./run_cluster.sh   <span class="c"># then open localhost:8080</span></div>`) +
    sec('Stages it executes', '', `<div class="rows">${STEPS.slice(0, 4).map((s, i) => `<button class="row" data-stage-link="${s.stage}"><div class="ri">${i + 1}</div>
      <div class="rt"><b>${s.script}</b><span>${s.task}</span></div>${stepChip(i)}</button>`).join('')}</div>`),
};
function stepChip(i) {
  const st = state.steps[i];
  return st === 'running' ? '<span class="chip run">Running</span>' : st === 'done' ? '<span class="chip good">Done</span>' : '<span class="chip">Queued</span>';
}
const STAGE_STEPS = { raw: [[0, 0]], bronze: [[0, 0]], quarantine: [[0, 3]], silver: [[1, 0]], gold: [[2, 0]], dash: [[3, 0], [4, 0]], spark: [[1, 3]] };
function insideButtons(id) {
  return (STAGE_STEPS[id] || []).map(([si, oi], k) => {
    const n = window.RF?.OPS?.[si]?.length;
    return `<button class="btn ${k ? '' : 'primary'} block inside-btn" data-act="inside" data-si="${si}" data-oi="${oi}">${icon('search', 'sm')} Look inside ${STEPS[si].script}${n ? ` · ${n} operations` : ''}</button>`;
  }).join('');
}
function renderPanel() {
  const p = $('#panel'), top = p.scrollTop;
  p.innerHTML = (PANELS[state.selected] || PANELS.bronze)();
  p.querySelector('.tiles')?.insertAdjacentHTML('afterend', insideButtons(state.selected));
  p.scrollTop = top;
  const t = $('#try-text');
  if (t) { const upd = () => { const r = sentiment(t.value); $('#try-out').innerHTML = `score <b>${r.s >= 0 ? '+' : ''}${r.s.toFixed(3)}</b> → <span class="chip ${r.label === 'positive' ? 'brand' : r.label === 'negative' ? 'bad' : ''}">${r.label}</span> <span class="muted">+${r.p.length} / −${r.n.length} of ${r.words} words</span>`; }; t.addEventListener('input', upd); upd(); }
}
$('#panel').addEventListener('click', e => {
  const a = e.target.closest('[data-act],[data-trace],[data-stage-link]'); if (!a) return;
  if (a.dataset.act === 'focus') focusOn(state.selected);
  if (a.dataset.act === 'insights') openInsights();
  if (a.dataset.act === 'inside') window.RF.openExplorer?.(+a.dataset.si, +a.dataset.oi);
  if (a.dataset.trace) { state.traceKey = a.dataset.trace; renderTracer(); flash($('#tracer')); }
  if (a.dataset.stageLink) select(a.dataset.stageLink, true);
});
const flash = el => el.animate([{ boxShadow: '0 0 0 3px rgba(47,102,246,.5)' }, { boxShadow: '0 0 0 0 rgba(47,102,246,0)' }], { duration: 900 });

/* ============================== KPIs ============================== */
const KPI = [
  { icon: 'file', label: 'Raw lines ingested', val: () => fmt(ING.lines_read), unit: 'lines', chip: `<span class="chip bad">${ING.lines_quarantined} quarantined</span>`, sub: `${RAW.files.length} JSONL files`, after: 0 },
  { icon: 'layers', label: 'Clean reviews (silver)', val: () => fmt(CLN.after_dedup), unit: 'rows', chip: `<span class="chip warn">−${dupes} dups</span>`, sub: `from ${fmt(CLN.reviews_flattened)} flattened`, after: 1 },
  { icon: 'star', label: 'Mean rating', val: () => SUM.mean_rating.toFixed(2), unit: '/ 5', chip: `<span class="chip">median ${SUM.median}</span>`, sub: `${fmt(SUM.n_rated_reviews)} rated reviews`, after: 3 },
  { icon: 'pulse', label: 'Rating ↔ sentiment', val: () => 'r ' + SUM.corr_rating_sentiment.toFixed(2), unit: '', chip: '<span class="chip good">strong</span>', sub: 'lexicon model validated', after: 3 },
];
function renderKPIs() {
  $('#kpis').classList.toggle('road', !!state.roadmap);
  if (state.roadmap && window.RF.renderRoadmap) { $('#kpis').innerHTML = window.RF.renderRoadmap(); return; }
  $('#kpis').innerHTML = KPI.map(k => {
    const ready = state.steps[k.after] === 'done';
    return `<div class="kpi card"><div class="kicon">${icon(k.icon)}</div><div><label>${k.label}</label>
      <div class="kval"><b class="num">${ready ? k.val() : '—'}</b>${ready && k.unit ? `<small>${k.unit}</small>` : ''}</div>
      <div class="ksub">${ready ? k.chip + ' ' + k.sub : '<span class="muted">waiting for ' + STEPS[k.after].script + '</span>'}</div></div></div>`;
  }).join('');
}

/* ============================== batch tracking + record tracer ============================== */
function renderStepper() {
  $('#stepper').innerHTML = STEPS.map((s, i) => {
    const st = state.steps[i], t = state.stepTimes[i];
    return `<div class="step ${st}" data-i="${i}" role="button" tabindex="0" title="Look inside ${s.script}">
      <div class="sc">${st === 'done' ? icon('check', 'sm') : st === 'running' ? icon('box', 'sm') : `<b>${i + 1}</b>`}</div>
      <b>${s.label}</b><em>${st === 'queued' ? '—' : st === 'running' ? 'processing…' : s.count}</em><span>${t != null ? 'T+' + t.toFixed(1) + 's' : st === 'done' ? 'last run' : ''}</span></div>`;
  }).join('');
}
$('#stepper').addEventListener('click', e => { const s = e.target.closest('.step'); if (s) { select(STEPS[+s.dataset.i].stage); window.RF.openExplorer?.(+s.dataset.i); } });

function traceRecord(key) {
  const r = RAW.samples[key]; if (!r) return null;
  const has = 'rating' in r, v = r.rating;
  const rawShow = !has ? 'rating: <absent>' : v === null ? 'rating: null' : typeof v === 'string' ? `rating: "${v}"` : `rating: ${v}`;
  const bronze = !has || v === null ? 'rating = null (STRING)' : `rating = "${v}" (STRING)`;
  let silver, sClass = '';
  if (key === 'duplicate') { silver = 'dropped by dropDuplicates'; sClass = 'bad'; }
  else {
    const num = has && v !== null && String(v).trim() !== '' ? Number(v) : NaN;
    const ok = Number.isFinite(num) && num >= 1 && num <= 5;
    silver = ok ? `rating = ${num.toFixed(1)}` : Number.isFinite(num) ? `rating = null (${num} out of 1–5)` : 'rating = null (cast fails)';
    sClass = ok ? 'good' : 'bad';
    if (r.date.includes('/')) { const [dd, mm, yy] = r.date.split('/'); silver = `date ${r.date} → ${yy}-${mm}-${dd}`; sClass = 'good'; }
  }
  const sen = sentiment(r.text);
  const gold = key === 'duplicate' ? 'counted once (kept copy)' : `${sen.label} (${sen.s >= 0 ? '+' : ''}${sen.s.toFixed(2)})${sClass === 'bad' && !r.date.includes('/') ? ' · no rating' : ''}`;
  return { r, rows: [['Raw', rawShow + (key === 'date_ddmmyyyy' ? ` · ${r.date}` : ''), 'bad'], ['Bronze', bronze, ''], ['Silver', silver, sClass], ['Gold', gold, key === 'duplicate' ? '' : sen.label === 'negative' ? 'bad' : 'good']] };
}
function renderTracer() {
  const t = traceRecord(state.traceKey); if (!t) return;
  const cur = state.running ? Math.min(state.current + 1, 3) : -1;   // raw=0, ingest->bronze=1, clean->silver=2, features+->gold=3
  $('#tracer').innerHTML = `<div class="tracer-top">${icon('route', 'sm')}<b class="mono">#${esc(t.r.review_id)}</b>
    <select id="trace-sel" aria-label="Choose a record to trace">${ISSUES.map(([k, l]) => `<option value="${k}" ${k === state.traceKey ? 'selected' : ''}>${l}</option>`).join('')}</select></div>
    <div class="muted" style="font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(t.r.hotel)} · “${esc(t.r.text)}”</div>
    <dl class="trace-rows">${t.rows.map(([k, v, c], i) => `<dt>${k}</dt><dd class="${c} ${i === cur ? 'cur' : ''}" title="${esc(v)}">${esc(v)}</dd>`).join('')}</dl>`;
  $('#trace-sel').addEventListener('change', e => { state.traceKey = e.target.value; renderTracer(); });
}

/* ============================== ops card: executors / jobs / log ============================== */
const execState = [{ name: 'worker-1', task: 'idle', p: 0 }, { name: 'worker-2', task: 'idle', p: 0 }];
function renderTabs() {
  const done = state.steps.filter(s => s === 'done').length;
  const tabs = [['exec', 'Executors', `${state.running ? 2 : 0}/2`], ['jobs', 'Jobs', `${done}/${STEPS.length}`], ['log', 'Driver log', '']];
  $('#ops-tabs').innerHTML = tabs.map(([k, l, n]) => `<button role="tab" data-tab="${k}" class="${state.tab === k ? 'on' : ''}">${l}${n ? `<small>${n}</small>` : ''}</button>`).join('');
}
function renderOps() {
  renderTabs();
  const b = $('#ops-body');
  if (state.tab === 'exec') {
    b.innerHTML = execState.map((e, i) => `<div class="exec"><div class="ri">${icon('cpu', 'sm')}</div><b>${e.name} <span class="muted">· 2 cores · 2 GB</span></b>
      <span class="chip ${e.task === 'idle' ? '' : 'run'}">${e.task === 'idle' ? 'Idle' : 'Busy'}</span><span id="ex-t-${i}">${esc(e.task)}</span><span></span>
      <div class="bar"><i id="ex-b-${i}" style="width:${e.p}%"></i></div></div>`).join('') +
      `<div class="exec"><div class="ri">${icon('route', 'sm')}</div><b>driver <span class="muted">· host</span></b><span class="chip ${state.current === 4 && state.running ? 'run' : ''}">${state.current === 4 && state.running ? 'Plotting' : 'Coordinating'}</span>
      <span>${state.current === 4 && state.running ? 'pandas + matplotlib → output/*.png' : 'spark-submit · collects small results'}</span></div>`;
  } else if (state.tab === 'jobs') {
    b.innerHTML = `<div class="rows">${STEPS.map((s, i) => `<button class="row" data-stage-link="${s.stage}"><div class="ri">${i + 1}</div>
      <div class="rt"><b>${s.script}</b><span>${s.task || 'host-side charts'} </span></div>${stepChip(i)}</button>`).join('')}</div>`;
  } else {
    b.innerHTML = `<div class="log">${state.log.length ? state.log.map(l => `<div><span class="t">${l.t}</span> <span class="${l.c || ''}">${esc(l.m)}</span></div>`).join('') : '<span class="t">No run in this session yet - press Run pipeline.</span>'}</div>`;
    b.scrollTop = b.scrollHeight;
  }
}
$('#ops-tabs').addEventListener('click', e => { const t = e.target.closest('[data-tab]'); if (t) { state.tab = t.dataset.tab; renderOps(); } });
$('#ops-body').addEventListener('click', e => { const r = e.target.closest('[data-stage-link]'); if (r) select(r.dataset.stageLink, true); });
let t0 = 0;
function log(m, c) { state.log.push({ t: ((performance.now() - t0) / 1000).toFixed(1).padStart(5, ' ') + 's', m, c }); if (state.tab === 'log') renderOps(); }

/* ============================== the run (animated replay) ============================== */
class Cancelled extends Error {}
const sleep = (ms, tok) => new Promise((res, rej) => setTimeout(() => tok === state.runToken ? res() : rej(new Cancelled()), ms / state.speed));
function driveTo(dist, tok) {
  return new Promise((res, rej) => {
    const from = batch.dist, ms = Math.abs(dist - from) * 70, start = performance.now();
    if (ms === 0) return res();
    const tick = now => {
      if (tok !== state.runToken) return rej(new Cancelled());
      const t = clamp((now - start) * state.speed / ms, 0, 1);
      batch.dist = from + (dist - from) * ease(t); drawBatch();
      if (state.follow && cam.k > 1.05) { const p = roadAt(batch.dist), [px, py] = P(p.x, p.y), a = availRect(); camAnim++; cam.x += ((px - (a.l + a.r) / 2 / cam.k) - cam.x) * .06; cam.y += ((py - (a.t + a.b) / 2 / cam.k) - cam.y) * .06; applyCam(); }
      t < 1 ? requestAnimationFrame(tick) : res();
    };
    requestAnimationFrame(tick);
  });
}
function showTicker(i, text) {
  if (!text) { $('#ticker').innerHTML = ''; return; }
  const p = roadAt(ROAD.units[i]), [px, py] = P(p.x, p.y, 5.2), w = text.length * 5.8 + 22;
  $('#ticker').innerHTML = `<g class="ticker" transform="translate(${px},${py})"><rect x="${-w / 2}" y="-11" width="${w}" height="21" rx="10.5"/><text text-anchor="middle" y="3.5">${esc(text)}</text></g>`;
}
function process(i, ms, tok) {
  const step = STEPS[i], start = performance.now(), ops = window.RF?.OPS?.[i] || [];
  let shown = -1;
  $(`#unit-${i}`)?.classList.add('busy');
  return new Promise((res, rej) => {
    const tick = now => {
      if (tok !== state.runToken) { showTicker(); $(`#unit-${i}`)?.classList.remove('busy'); return rej(new Cancelled()); }
      const t = clamp((now - start) * state.speed / ms, 0, 1);
      const oi = Math.min(ops.length - 1, Math.floor(t * ops.length));
      if (ops.length && oi !== shown) { shown = oi; showTicker(i, `${oi + 1}/${ops.length} · ${ops[oi].t}`); log(`  · ${ops[oi].t}`, 't'); }
      if (t >= 1) { showTicker(); $(`#unit-${i}`)?.classList.remove('busy'); }
      execState.forEach((e, w) => {
        e.p = step.task ? clamp(t * 100 * (w ? .94 : 1) + (w ? 0 : 3), 0, 100) : 0;
        const task = Math.min(4, Math.floor(e.p / 25) + 1);
        e.task = step.task ? `${step.task} · task ${w * 4 + task}/8` : 'idle';
        const tb = $(`#ex-b-${w}`), tt = $(`#ex-t-${w}`);
        if (tb) tb.style.width = e.p + '%'; if (tt) tt.textContent = e.task;
      });
      t < 1 ? requestAnimationFrame(tick) : res();
    };
    requestAnimationFrame(tick);
  });
}
function setStage(id, st) { state.stage[id] = st; syncStores(); if (state.selected === id || state.selected === 'spark') renderPanel(); }
function setSpark(busy, to) {
  $('#cluster').classList.toggle('busy', busy);
  world.querySelectorAll('.link').forEach(l => l.classList.toggle('active', busy && l.dataset.to === to));
  state.stage.spark = busy ? 'busy' : 'idle';
}
function refreshAll() { syncStores(); renderStepper(); renderKPIs(); renderOps(); renderTracer(); renderRunBtn(); renderLive(); renderCargo(); }

async function run() {
  const tok = ++state.runToken;
  Object.assign(state, { running: true, current: -1, steps: STEPS.map(() => 'queued'), stepTimes: STEPS.map(() => null), log: [] });
  Object.keys(state.stage).forEach(k => state.stage[k] = k === 'raw' ? 'done' : k === 'spark' ? 'idle' : 'queued');
  $('#q-group').classList.add('q-hidden'); $('#dash-group').classList.add('screen-off');
  cargoAnim++; batch.dist = ROAD.stops[0]; batch.payload = STEPS[0].payloadIn; batch.visible = true;
  setCargo(0, 'transit'); drawBatch();
  t0 = performance.now(); runNo++;
  $('#run-meta').textContent = `RUN-${String(runNo).padStart(3, '0')} · ${state.speed}× replay`;
  select('raw', state.follow); refreshAll();
  log(`spark-submit ${state.speed > 1 ? '(fast replay) ' : ''}--master local[*]`, 't');
  try {
    for (let i = 0; i < STEPS.length; i++) {
      const s = STEPS[i];
      state.current = i; state.steps[i] = 'running'; refreshAll();
      log(`=== ${s.script.replace('.py', '')} ===`);
      batch.payload = s.payloadIn; if (s.from !== s.to) setCargo(i, 'transit'); drawBatch();
      await driveTo(ROAD.stops[s.to], tok);
      setCargo(i + 1, 'processing');
      setStage(s.stage, 'running'); if (state.follow) select(s.stage);
      if (s.task) setSpark(true, s.stage);
      renderOps();
      const guided = state.teach && window.RF.playStep ? window.RF.playStep(i) : null;
      await process(i, state.teach ? 1800 : 2200 + 450 * ((window.RF.OPS?.[i] || []).length), tok);
      if (guided) { await guided; if (tok !== state.runToken) throw new Cancelled(); }
      setSpark(false); execState.forEach(e => { e.task = 'idle'; e.p = 0; });
      if (s.id === 'ingest') {
        $('#q-group').classList.remove('q-hidden'); setStage('quarantine', 'done');
        log(`lines read=${ING.lines_read}  good=${goodLines}  quarantined=${ING.lines_quarantined}`, 'w');
      } else if (s.id === 'clean') {
        Object.entries(CLN).forEach(([k, v]) => log(`  "${k}": ${v}`));
      } else if (s.id === 'features') {
        log(`gold tables written: ${CLN.silver_rows} review rows; hotel_stats; city_month`);
      } else if (s.id === 'analytics') {
        log(JSON.stringify(SUM)); log('CSV outputs written to output/');
      } else {
        $('#dash-group').classList.remove('screen-off'); log('charts written', 'ok');
      }
      batch.payload = s.payloadOut; setCargo(i + 1, 'stored'); drawBatch();
      state.steps[i] = 'done'; state.stepTimes[i] = (performance.now() - t0) / 1000;
      setStage(s.stage, 'done'); refreshAll();
      await sleep(350, tok);
    }
    state.running = false; state.current = -1;
    execState.forEach(e => { e.task = 'idle'; e.p = 0; });
    log(`pipeline finished in ${((performance.now() - t0) / 1000).toFixed(1)}s (replay)`, 'ok');
    refreshAll(); renderPanel();
    log(`quality gates: ${GATES.filter(g => g.pass).length}/${GATES.length} passed`, GATES.every(g => g.pass) ? 'ok' : 'w');
    toast(`${icon('check', 'sm')} Pipeline complete · quality gates ${GATES.filter(g => g.pass).length}/${GATES.length} passed`, 'Open insights', openInsights);
  } catch (e) {
    if (!(e instanceof Cancelled)) throw e;
  }
}
let runNo = 0;
function stopRun() {
  state.runToken++; showTicker(); window.RF.closeExplorer?.(); state.running = false; state.current = -1;
  setSpark(false); execState.forEach(e => { e.task = 'idle'; e.p = 0; });
  STEPS.forEach((s, i) => { if (state.steps[i] === 'running') state.steps[i] = 'queued'; });
  log('run cancelled by user', 'w'); refreshAll(); renderPanel(); setCargo(cargo.i, 'idle');
}
function renderRunBtn() {
  $('#run-btn').innerHTML = state.running ? `${icon('stop', 'sm')} Stop` : `${icon('play', 'sm')} Run pipeline`;
}
let liveTimer = 0;
function renderLive() {
  const el = $('#live');
  el.classList.toggle('on', state.running);
  clearInterval(liveTimer);
  const upd = () => { el.querySelector('span').textContent = state.running ? `Running ${((performance.now() - t0) / 1000).toFixed(1)}s` : runNo ? 'Run complete' : 'Last run'; };
  upd(); if (state.running) liveTimer = setInterval(upd, 100);
}
$('#run-btn').addEventListener('click', () => state.running ? stopRun() : run());
$('#teach').addEventListener('change', e => { state.teach = e.target.checked; });
$('#speed').addEventListener('click', e => {
  const b = e.target.closest('button'); if (!b) return;
  state.speed = +b.dataset.s; $('#speed').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
});

/* ============================== toast ============================== */
let toastTimer = 0;
function toast(html, action, fn) {
  const t = $('#toast');
  t.innerHTML = `<span>${html}</span>${action ? `<button class="btn">${action}</button>` : ''}`;
  if (action) t.querySelector('button').onclick = () => { t.classList.remove('show'); fn(); };
  t.classList.add('show'); clearTimeout(toastTimer); toastTimer = setTimeout(() => t.classList.remove('show'), 6000);
}

/* ============================== insights (charts) ============================== */
const tip = $('#tip');
function showTip(html, x, y) { tip.innerHTML = html; tip.classList.add('show'); const w = tip.offsetWidth; tip.style.left = clamp(x + 14, 8, innerWidth - w - 8) + 'px'; tip.style.top = (y - 12 - tip.offsetHeight) + 'px'; }
function hideTip() { tip.classList.remove('show'); }
function wireHover(el) {
  el.querySelectorAll('svg').forEach(s => {
    s.addEventListener('pointermove', e => {
      const m = e.target.closest('[data-tip]');
      s.querySelectorAll('.mark.on').forEach(x => x.classList.remove('on'));
      if (!m) { s.classList.remove('hovering'); return hideTip(); }
      s.classList.add('hovering');
      s.querySelectorAll(`.mark[data-g="${m.dataset.g}"]`).forEach(x => x.classList.add('on'));
      showTip(m.dataset.tip, e.clientX, e.clientY);
    });
    s.addEventListener('pointerleave', () => { s.classList.remove('hovering'); s.querySelectorAll('.mark.on').forEach(x => x.classList.remove('on')); hideTip(); });
  });
}
const BRAND = '#2f66f6';
function colChart(rows, { W = 360, H = 210, key, val, color = () => BRAND, yMax, ticks, tipFn, xLab = key, labelIdx = [] }) {
  const m = { l: 40, r: 8, t: 16, b: 26 }, iw = W - m.l - m.r, ih = H - m.t - m.b, bw = iw / rows.length;
  const y = v => m.t + ih - ih * v / yMax;
  let s = ticks.map(t => `<line class="grid-l" x1="${m.l}" x2="${W - m.r}" y1="${y(t)}" y2="${y(t)}"/><text x="${m.l - 6}" y="${y(t) + 3.5}" text-anchor="end">${fmt(t)}</text>`).join('');
  rows.forEach((r, i) => {
    const x = m.l + i * bw + 3, w = bw - 6, v = r[val], top = y(v), h = m.t + ih - top, rr = Math.min(4, w / 2, h);
    s += `<path class="mark" data-g="${i}" data-tip="${esc(tipFn(r))}" fill="${color(r, i)}" d="M${x},${m.t + ih}V${top + rr}Q${x},${top} ${x + rr},${top}H${x + w - rr}Q${x + w},${top} ${x + w},${top + rr}V${m.t + ih}Z"/>`;
    s += `<rect class="hit" data-g="${i}" data-tip="${esc(tipFn(r))}" x="${m.l + i * bw}" y="${m.t}" width="${bw}" height="${ih}"/>`;
    s += `<text x="${x + w / 2}" y="${H - 8}" text-anchor="middle">${esc(xLab(r))}</text>`;
    if (labelIdx.includes(i)) s += `<text class="lbl" x="${x + w / 2}" y="${top - 5}" text-anchor="middle">${fmt(v)}</text>`;
  });
  return `<svg viewBox="0 0 ${W} ${H}">${s}</svg>`;
}
function lineChart() {
  const W = 760, H = 230, m = { l: 40, r: 14, t: 14, b: 28 }, iw = W - m.l - m.r, ih = H - m.t - m.b, n = months.length;
  const yMax = Math.ceil(Math.max(...months.map(r => r.n_reviews)) / 200) * 200;
  const x = i => m.l + iw * i / (n - 1), y = v => m.t + ih - ih * v / yMax;
  const label = mo => { const [yy, mm] = mo.split('-'); return new Date(+yy, +mm - 1, 1).toLocaleString('en-US', { month: 'short' }) + ' ’' + yy.slice(2); };
  let s = [0, yMax / 3, 2 * yMax / 3, yMax].map(t => `<line class="grid-l" x1="${m.l}" x2="${W - m.r}" y1="${y(t)}" y2="${y(t)}"/><text x="${m.l - 6}" y="${y(t) + 3.5}" text-anchor="end">${fmt(Math.round(t))}</text>`).join('');
  const line = months.map((r, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(r.n_reviews).toFixed(1)}`).join('');
  s += `<path d="${line}L${x(n - 1)},${y(0)}L${x(0)},${y(0)}Z" fill="${BRAND}" opacity=".08"/><path d="${line}" fill="none" stroke="${BRAND}" stroke-width="2" stroke-linejoin="round"/>`;
  months.forEach((r, i) => { if (i % 6 === 0) s += `<text x="${x(i)}" y="${H - 8}" text-anchor="middle">${label(r.month)}</text>`; });
  s += `<line id="xh" y1="${m.t}" y2="${m.t + ih}" stroke="#152033" stroke-width="1" opacity="0"/><circle id="xd" r="5" fill="${BRAND}" stroke="#fff" stroke-width="2" opacity="0"/>`;
  s += `<rect class="hit" id="line-hit" x="${m.l}" y="${m.t}" width="${iw}" height="${ih}"/>`;
  return { svg: `<svg viewBox="0 0 ${W} ${H}" id="line-svg">${s}</svg>`, wire: root => {
    const sv = $('#line-svg', root), hit = $('#line-hit', root), xh = $('#xh', root), xd = $('#xd', root);
    hit.addEventListener('pointermove', e => {
      const r = sv.getBoundingClientRect(), px = (e.clientX - r.left) * W / r.width, i = clamp(Math.round((px - m.l) / iw * (n - 1)), 0, n - 1);
      xh.setAttribute('x1', x(i)); xh.setAttribute('x2', x(i)); xh.setAttribute('opacity', .25);
      xd.setAttribute('cx', x(i)); xd.setAttribute('cy', y(months[i].n_reviews)); xd.setAttribute('opacity', 1);
      showTip(`<b>${label(months[i].month)}</b><br>${fmt(months[i].n_reviews)} reviews`, e.clientX, e.clientY);
    });
    hit.addEventListener('pointerleave', () => { xh.setAttribute('opacity', 0); xd.setAttribute('opacity', 0); hideTip(); });
  } };
}
function dotPlot() {
  const rows = [...D.city_summary].sort((a, b) => b.avg_rating - a.avg_rating);
  const W = 360, rowH = 28, H = rows.length * rowH + 40, m = { l: 92, r: 40, t: 10 };
  const lo = Math.floor(Math.min(...rows.map(r => r.avg_rating)) * 10) / 10 - .05, hi = Math.ceil(Math.max(...rows.map(r => r.avg_rating)) * 10) / 10 + .05;
  const x = v => m.l + (W - m.l - m.r) * (v - lo) / (hi - lo);
  let s = '';
  for (let t = Math.ceil(lo * 10) / 10; t <= hi; t += .1) s += `<line class="grid-l" x1="${x(t)}" x2="${x(t)}" y1="${m.t}" y2="${m.t + rows.length * rowH}"/><text x="${x(t)}" y="${H - 12}" text-anchor="middle">${t.toFixed(1)}</text>`;
  s += `<line x1="${x(SUM.mean_rating)}" x2="${x(SUM.mean_rating)}" y1="${m.t - 4}" y2="${m.t + rows.length * rowH}" stroke="#152033" stroke-dasharray="3 3" opacity=".5"/><text x="${x(SUM.mean_rating) + 4}" y="${m.t + 2}" class="lbl" font-size="10">overall ${SUM.mean_rating.toFixed(2)}</text>`;
  rows.forEach((r, i) => {
    const cy = m.t + i * rowH + rowH / 2 + 6, t = `<b>${esc(r.city)}</b><br>avg ★ ${r.avg_rating.toFixed(3)} · ${fmt(r.n)} rated reviews<br>avg sentiment ${r.avg_sentiment.toFixed(3)} · ${RAW.hotels_by_city[r.city] || '–'} hotels`;
    s += `<text x="${m.l - 10}" y="${cy + 3.5}" text-anchor="end" class="lbl">${esc(r.city)}</text><line class="grid-l" x1="${x(lo)}" x2="${x(r.avg_rating)}" y1="${cy}" y2="${cy}" stroke-width="2"/>`;
    s += `<circle class="mark" data-g="${i}" data-tip="${esc(t)}" cx="${x(r.avg_rating)}" cy="${cy}" r="6" fill="${BRAND}" stroke="#fff" stroke-width="2"/><text x="${x(r.avg_rating) + 11}" y="${cy + 3.5}">${r.avg_rating.toFixed(2)}</text>`;
    s += `<rect class="hit" data-g="${i}" data-tip="${esc(t)}" x="0" y="${cy - rowH / 2}" width="${W}" height="${rowH}"/>`;
  });
  return `<svg viewBox="0 0 ${W} ${H}">${s}</svg>`;
}
function funnel() {
  const steps = [['Flattened reviews', CLN.reviews_flattened, ''], ['After de-dup', CLN.after_dedup, `−${fmt(dupes)} duplicate review_ids`], ['Valid rating', SUM.n_rated_reviews, `−${fmt(CLN.rating_missing_or_invalid)} missing / invalid ratings`]];
  const W = 360, H = 210, m = { l: 112, r: 50 }, max = steps[0][1], bh = 26;
  let s = '';
  steps.forEach(([l, v, note], i) => {
    const y = 16 + i * 62, w = (W - m.l - m.r) * v / max, t = `<b>${l}</b><br>${fmt(v)} rows · ${pct(v, max)} of flattened`;
    s += `<text x="${m.l - 10}" y="${y + bh / 2 + 4}" text-anchor="end" class="lbl">${l}</text>`;
    s += `<rect class="mark" data-g="${i}" data-tip="${esc(t)}" x="${m.l}" y="${y}" width="${w}" height="${bh}" rx="4" fill="${i === 2 ? BRAND : '#9db8fb'}"/>`;
    s += `<text x="${m.l + w + 6}" y="${y + bh / 2 + 4}" class="lbl">${fmt(v)}</text>`;
    if (note) s += `<text x="${m.l}" y="${y - 6}" fill="#b0501f" style="fill:#b0501f">${note}</text>`;
    s += `<rect class="hit" data-g="${i}" data-tip="${esc(t)}" x="0" y="${y - 4}" width="${W}" height="${bh + 8}"/>`;
  });
  return `<svg viewBox="0 0 ${W} ${H}">${s}</svg>`;
}
const GATES = [
  ['Corrupt-line rate below 5%', ING.lines_quarantined / ING.lines_read, v => v < .05, v => v.toFixed(4)],
  ['Duplicate rate below 5%', dupes / CLN.reviews_flattened, v => v < .05, v => v.toFixed(4)],
  ['Missing/invalid rating below 20%', CLN.rating_missing_or_invalid / CLN.after_dedup, v => v < .2, v => v.toFixed(4)],
  ['Unparsed dates equal 0', CLN.date_unparsed, v => v === 0, v => v],
  ['Mean rating within 1–5', SUM.mean_rating, v => v >= 1 && v <= 5, v => v],
  ['Quartiles ordered Q1 ≤ median ≤ Q3', [SUM.q1, SUM.median, SUM.q3], v => v[0] <= v[1] && v[1] <= v[2], v => `(${v.join(', ')})`],
  ['Rating–sentiment correlation positive', SUM.corr_rating_sentiment, v => v > 0, v => v],
].map(([name, v, ok, show]) => ({ name, value: show(v), pass: ok(v) }));
const gatesHTML = () => `<table class="tbl"><thead><tr><th>Gate (Week 12)</th><th class="r">Value</th><th>Result</th></tr></thead><tbody>${GATES.map(g =>
  `<tr><td>${g.name}</td><td class="r mono">${g.value}</td><td><span class="chip ${g.pass ? 'good' : 'bad'}">${g.pass ? '✓ PASS' : '✗ FAIL'}</span></td></tr>`).join('')}</tbody></table>`;
function openInsights() {
  const tot = D.rating_dist.reduce((a, r) => a + r.count, 0), modeI = D.rating_dist.reduce((b, r, i, a) => r.count > a[b].count ? i : b, 0);
  const ord = ['negative', 'neutral', 'positive'].filter(l => SENT[l]), sc = { negative: '#e0703f', neutral: '#98a3b5', positive: BRAND };
  const lc = lineChart();
  $('#ins-body').innerHTML = `
  <div class="hero-row">
    <div class="hero card"><label>Rated reviews</label><b>${fmt(SUM.n_rated_reviews)}</b><span>of ${fmt(CLN.after_dedup)} clean rows</span></div>
    <div class="hero card"><label>Mean rating</label><b>${SUM.mean_rating.toFixed(2)}</b><span>★ out of 5</span></div>
    <div class="hero card"><label>Median · IQR</label><b>${SUM.median}</b><span>Q1 ${SUM.q1} · Q3 ${SUM.q3}</span></div>
    <div class="hero card"><label>Rating ↔ sentiment</label><b>r ${SUM.corr_rating_sentiment}</b><span>Pearson correlation</span></div>
    <div class="hero card"><label>Coverage</label><b>${D.city_summary.length} cities</b><span>${goodLines} hotels · ${months.length} full months</span></div>
  </div>
  <div class="grid" style="margin-top:14px">
    <div class="card chart-card span2"><h3>Reviews per month</h3><p class="cap">gold/city_month summed · final partial month (${D.monthly_volume.at(-1).month}) excluded, as in 05_dashboard.py</p>${lc.svg}</div>
    <div class="card chart-card"><h3>Data-quality funnel</h3><p class="cap">Rows surviving each cleaning rule in silver</p>${funnel()}</div>
    <div class="card chart-card"><h3>Rating distribution</h3><p class="cap">${fmt(tot)} rated reviews · half-star buckets</p>
      ${colChart(D.rating_dist, { key: 'rating', val: 'count', yMax: Math.ceil(Math.max(...D.rating_dist.map(r => r.count)) / 1000) * 1000, ticks: [0, 1000, 2000, 3000],
        xLab: r => r.rating.toFixed(1), labelIdx: [modeI], color: (r, i) => i === modeI ? BRAND : '#7fa1f9', tipFn: r => `<b>★ ${r.rating.toFixed(1)}</b><br>${fmt(r.count)} reviews · ${pct(r.count, tot)}` })}</div>
    <div class="card chart-card"><h3>Average rating by city</h3><p class="cap">Dot plot - differences are small, so the axis is zoomed</p>${dotPlot()}</div>
    <div class="card chart-card"><h3>Mean rating by sentiment label</h3><p class="cap">Lexicon labels agree with star ratings</p>
      ${colChart(ord.map(l => SENT[l]), { key: 'label', val: 'avg_rating', yMax: 5, ticks: [0, 1, 2, 3, 4, 5], labelIdx: [0, 1, 2], xLab: r => `${r.label} (${fmt(r.n)})`,
        color: r => sc[r.label], tipFn: r => `<b>${r.label}</b><br>avg ★ ${r.avg_rating.toFixed(3)}<br>${fmt(r.n)} reviews` })}</div>
    <div class="card chart-card span3"><h3>Quality gates · ${GATES.filter(g => g.pass).length}/${GATES.length} passed</h3><p class="cap">The automated checks from the Week 12 notebook, computed here from this pipeline's output/</p>${gatesHTML()}</div>
    <div class="card chart-card span3"><h3>Top hotels</h3><p class="cap">output/top_hotels.csv · hotels with ≥ 30 rated reviews, ranked by average rating</p>
      <table class="tbl"><thead><tr><th>#</th><th>Hotel</th><th>City</th><th>Stars</th><th class="r">Reviews</th><th class="r">Avg ★</th><th class="r">Avg sentiment</th><th>% positive</th><th>Last review</th></tr></thead><tbody>
      ${D.top_hotels.map((h, i) => `<tr><td>${i + 1}</td><td><b>${esc(h.hotel_name)}</b> <span class="muted mono">${h.hotel_id}</span></td><td>${esc(h.city)}</td><td>${'★'.repeat(h.stars)}</td>
        <td class="r">${h.n_reviews}</td><td class="r"><b>${h.avg_rating.toFixed(2)}</b></td><td class="r">${h.avg_sentiment.toFixed(3)}</td>
        <td><span class="pbar"><i style="width:${h.pct_positive * 100}%"></i></span>${(h.pct_positive * 100).toFixed(0)}%</td><td class="muted">${h.last_review}</td></tr>`).join('')}</tbody></table></div>
    <div class="card chart-card span3"><h3>Static charts from 05_dashboard.py</h3><p class="cap">The PNGs the pipeline itself writes to output/ - click to open full size</p>
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:10px">${['dq_funnel', 'city_rating', 'monthly_volume', 'rating_dist', 'sentiment'].map(n =>
        `<a href="../output/chart_${n}.png" target="_blank" rel="noopener" style="display:block;border:1px solid var(--line);border-radius:10px;overflow:hidden;background:#fff"><img src="../output/chart_${n}.png" alt="chart_${n}.png" style="width:100%;display:block" loading="lazy" onerror="this.parentElement.style.display='none'"></a>`).join('')}</div></div>
  </div>`;
  lc.wire($('#ins-body')); wireHover($('#ins-body'));
  $('#insights').hidden = false; $('#ins-close').focus();
}
function closeInsights() { $('#insights').hidden = true; hideTip(); }
$('#ins-close').addEventListener('click', closeInsights);
$('#insights').addEventListener('click', e => { if (e.target.id === 'insights') closeInsights(); });

/* ============================== search + alerts ============================== */
const INDEX = [
  ...Object.entries(STAGES).map(([id, s]) => ({ label: s.name, kind: 'stage', icon: s.icon, go: () => select(id, true) })),
  ...STEPS.map(s => ({ label: s.script, kind: 'script', icon: 'file', go: () => select(s.stage, true) })),
  ...D.city_summary.map(c => ({ label: `${c.city}`, kind: `silver partition · ★ ${c.avg_rating.toFixed(2)}`, icon: 'box', go: () => select('silver', true) })),
  ...D.top_hotels.map(h => ({ label: `${h.hotel_name} (${h.hotel_id})`, kind: `top hotel · ★ ${h.avg_rating.toFixed(2)}`, icon: 'star', go: () => { select('dash', true); openInsights(); } })),
  ...ISSUES.map(([k, l]) => ({ label: `${RAW.samples[k].review_id} - ${l}`, kind: 'trace record', icon: 'route', go: () => { state.traceKey = k; renderTracer(); flash($('#tracer')); } })),
  { label: 'Open insights dashboard', kind: 'action', icon: 'chart', go: openInsights },
  { label: 'Run pipeline', kind: 'action', icon: 'play', go: () => state.running || run() },
];
const sInput = $('#search'), sRes = $('#search-results');
let sHits = [], sIdx = 0;
function renderSearch() {
  const q = sInput.value.trim().toLowerCase();
  sHits = q ? INDEX.filter(i => (i.label + ' ' + i.kind).toLowerCase().includes(q)).slice(0, 9) : [];
  sIdx = 0;
  sRes.innerHTML = sHits.length ? sHits.map((h, i) => `<button class="sr-item ${i === sIdx ? 'active' : ''}" data-i="${i}" role="option">${icon(h.icon, 'sm')}<span>${esc(h.label)}</span><span class="kind">${esc(h.kind)}</span></button>`).join('')
    : q ? '<div class="sr-item muted">No matches</div>' : '';
  sRes.classList.toggle('open', !!q);
}
function pick(i) { const h = sHits[i]; if (!h) return; sInput.value = ''; sRes.classList.remove('open'); sInput.blur(); h.go(); }
sInput.addEventListener('input', renderSearch);
sInput.addEventListener('keydown', e => {
  if (e.key === 'ArrowDown' || e.key === 'ArrowUp') { e.preventDefault(); sIdx = clamp(sIdx + (e.key === 'ArrowDown' ? 1 : -1), 0, sHits.length - 1); sRes.querySelectorAll('.sr-item').forEach((b, i) => b.classList.toggle('active', i === sIdx)); }
  if (e.key === 'Enter') pick(sIdx);
  if (e.key === 'Escape') { sInput.value = ''; renderSearch(); sInput.blur(); }
});
sRes.addEventListener('click', e => { const b = e.target.closest('[data-i]'); if (b) pick(+b.dataset.i); });
document.addEventListener('keydown', e => {
  if (e.key === '/' && document.activeElement !== sInput && !/input|select|textarea/i.test(document.activeElement.tagName)) { e.preventDefault(); sInput.focus(); }
  if (e.key === 'Escape' && !$('#insights').hidden) closeInsights();
});

const ALERTS = [
  { c: 'bad', i: 'ban', t: `${ING.lines_quarantined} corrupt JSON lines quarantined`, d: `${RAW.files.filter(f => f.bad).map(f => f.file).join(', ')}`, go: 'quarantine' },
  { c: 'warn', i: 'alert', t: `${fmt(CLN.rating_missing_or_invalid)} ratings set to null`, d: 'missing key, null, "N/A" or outside 1–5', go: 'silver' },
  { c: 'warn', i: 'layers', t: `${dupes} duplicate reviews removed`, d: 'dropDuplicates on review_id', go: 'silver' },
  { c: 'good', i: 'clock', t: `${RAW.issues.date_ddmmyyyy || 0} dd/MM/yyyy dates repaired`, d: `${CLN.date_unparsed} dates left unparsed`, go: 'silver' },
];
$('#bell').innerHTML = icon('bell') + `<span class="dot">${ALERTS.length}</span>`;
$('#notif').innerHTML = `<h4>Data-quality alerts · last run</h4>` + ALERTS.map((a, i) => `<button class="notif-item" data-i="${i}"><span class="ib chip ${a.c}">${icon(a.i, 'sm')}</span><div><b>${a.t}</b><span>${esc(a.d)}</span></div></button>`).join('');
$('#bell').addEventListener('click', e => { e.stopPropagation(); $('#notif').classList.toggle('open'); });
$('#notif').addEventListener('click', e => { const b = e.target.closest('[data-i]'); if (b) { $('#notif').classList.remove('open'); select(ALERTS[+b.dataset.i].go, true); } });
document.addEventListener('click', e => {
  if (!e.target.closest('.notif') && !e.target.closest('#bell')) $('#notif').classList.remove('open');
  if (!e.target.closest('.search')) sRes.classList.remove('open');
});

/* ============================== map tools + init ============================== */
$('#map-tools').innerHTML = `<button class="icon-btn" data-z="in" title="Zoom in" aria-label="Zoom in">${icon('plus')}</button>
  <button class="icon-btn" data-z="out" title="Zoom out" aria-label="Zoom out">${icon('minus')}</button>
  <button class="icon-btn" data-z="fit" title="Fit whole pipeline" aria-label="Fit">${icon('fit')}</button><hr>
  <button class="icon-btn on" data-z="follow" title="Camera follows the data batch" aria-label="Follow run">${icon('route')}</button>
  <button class="icon-btn on" data-z="labels" title="Toggle labels" aria-label="Toggle labels">${icon('tag')}</button>
  <button class="icon-btn on" data-z="cargo" title="Show the tracked data batch" aria-label="Toggle data card">${icon('db')}</button>`;
$('#map-tools').addEventListener('click', e => {
  const b = e.target.closest('[data-z]'); if (!b) return;
  const r = svg.getBoundingClientRect(), z = b.dataset.z;
  if (z === 'in' || z === 'out') { const k0 = cam.k, k = clamp(k0 * (z === 'in' ? 1.3 : 1 / 1.3), .3, 4); tweenCam({ k, x: cam.x + r.width / 2 / k0 - r.width / 2 / k, y: cam.y + r.height / 2 / k0 - r.height / 2 / k }, 250); }
  if (z === 'fit') fit();
  if (z === 'follow') { state.follow = !state.follow; b.classList.toggle('on', state.follow); }
  if (z === 'cargo') { cargo.open = !cargo.open; renderCargo(); fit(); }
  if (z === 'labels') { state.labels = !state.labels; b.classList.toggle('on', state.labels); svg.classList.toggle('hide-labels', !state.labels); }
});
$('#cluster-pill').addEventListener('click', () => select('spark', true));
$('#roadmap-btn').addEventListener('click', () => { state.roadmap = !state.roadmap; $('#roadmap-btn').classList.toggle('on', state.roadmap); renderKPIs(); requestAnimationFrame(() => fit()); });
$('#roadmap-btn').innerHTML = `${icon('map', 'sm')} 12-week roadmap`;
$('#brand-mark').innerHTML = icon('layers');
$('#search-ic').outerHTML = icon('search');
$('#track-ic').outerHTML = `<span class="ph-icon" style="width:28px;height:28px;border-radius:8px">${icon('route', 'sm')}</span>`;
$('#ins-icon').innerHTML = icon('chart');
$('#ins-close').innerHTML = icon('x');
window.RF = { D, icon, fmt, esc, pct, sentiment, POS, NEG, STEPS, STAGES, PANELS, INDEX, GATES, gatesHTML, state, select, renderKPIs, fit, openInsights };   // shared with explorer.js and roadmap.js

buildScene();
renderCargo();
drawBatch();
requestAnimationFrame(flowFrame);
select(state.selected);
refreshAll();
requestAnimationFrame(() => {
  fit(false);
  if (location.hash === '#run') run();               // index.html#run starts the replay on load (handy for a projector)
  if (location.hash === '#insights') openInsights();
});
let rz = 0; addEventListener('resize', () => { clearTimeout(rz); rz = setTimeout(() => fit(false), 120); });
})();
