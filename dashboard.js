/* ================================================================
   Rock Capture Dashboard — main script
   Uses DuckDB-WASM to query parquet files with SQL in the browser
   and Plotly.js for interactive charts.
   ================================================================ */
import * as duckdb from '@duckdb/duckdb-wasm';

// Base URL of your Cloudflare R2 bucket (public access or custom domain)
const R2_BASE_URL = 'https://pub-f15ce98d2f554311b0543bcb6562b082.r2.dev';

// Nested directory structure with dates, userID, and sessionID
// Example: compositions/2026/04/09/<userID>/<sessionID>/compositions_batch001.parquet
const R2_COLLECTION = 'compositions';

// If you create a manifest.json file listing all available parquets, reference it here
// Otherwise leave as null and use PARQUET_FILES below or loadParquetsFromPattern()
const R2_MANIFEST_URL = `${R2_BASE_URL}/manifest.json`;

// Static parquet files to load (if your data doesn't have dynamic paths)
const PARQUET_FILES = {};

// ── R2 Path Helpers ──────────────────────────────────────────────
/**
 * Build a full R2 URL for a parquet file using the nested structure
 * Example: r2Url('2026', '04', '09', 'user-123', 'session-456', 'compositions_batch001')
 */
function r2Url(year, month, day, userId, sessionId, fileName) {
  return `${R2_BASE_URL}/${R2_COLLECTION}/${year}/${month}/${day}/${userId}/${sessionId}/${fileName}.parquet`;
}

/**
 * Register a single parquet file with DuckDB
 */
async function registerParquetFile(url, viewName) {
  try {
    console.log(`[DuckDB] registering ${viewName} →`, url);
    await db.registerFileURL(viewName + '.parquet', url, duckdb.DuckDBDataProtocol.HTTP, false);
    await conn.query(`CREATE VIEW ${viewName} AS SELECT * FROM read_parquet('${viewName}.parquet')`);
    console.log(`[DuckDB] ✓ ${viewName} registered`);
  } catch (e) {
    console.error(`[DuckDB] ✗ Failed to register ${viewName}:`, e.message);
  }
}

/**
 * Load all parquets from a manifest.json file
 * Manifest format:
 * {
 *   "version": "1.0",
 *   "generatedAt": "2026-04-09T00:00:00Z",
 *   "fileCount": 42,
 *   "files": [
 *     {
 *       "key": "type/year/month/day/userId/sessionId/filename.parquet",
 *       "type": "scans|compositions",
 *       "userId": "...",
 *       "sessionId": "...",
 *       "batchNumber": 1,
 *       "uploadedAt": "2026-04-09T..."
 *     },
 *     ...
 *   ]
 * }
 */
async function loadParquetsFromManifest() {
  if (!R2_MANIFEST_URL) {
    console.warn('[DuckDB] No manifest URL configured');
    return;
  }
  try {
    const res = await fetch(R2_MANIFEST_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const manifest = await res.json();
    const fileCount = manifest.files?.length || 0;
    console.log(`[DuckDB] Loaded manifest with ${fileCount} parquets (generated ${manifest.generatedAt})`);
    
    for (const file of manifest.files || []) {
      const fullUrl = `${R2_BASE_URL}/${file.key}`;
      // Generate a unique view name: type_userId_sessionId_batchN
      const viewName = `${file.type}_${file.userId?.slice(0, 8)}_${file.sessionId?.slice(0, 8)}_batch${file.batchNumber}`;
      await registerParquetFile(fullUrl, viewName);
    }
  } catch (e) {
    console.error('[DuckDB] Failed to load manifest:', e.message);
  }
}

/**
 * Load parquets matching a specific date/user/session pattern
 * Example:
 *   loadParquetsFromPattern('2026/04/09') — load all from today
 *   loadParquetsFromPattern('2026/04/09', 'user-123') — load specific user
 *   loadParquetsFromPattern(null, null, 'session-456') — load specific session
 */
async function loadParquetsFromPattern(datePattern = null, userId = null, sessionId = null) {
  console.warn('[DuckDB] loadParquetsFromPattern() requires manual URL list or a manifest.');
  console.warn('Use loadParquetsFromManifest(), registerParquetFile(), or R2 S3 list API');
}

// ── Globals ──────────────────────────────────────────────────────
let db = null;   // DuckDB instance
let conn = null; // DuckDB connection

// ── Filter state ─────────────────────────────────────────────────
let filters = { location: [], material: [] };
let autocompleteData = { location: [], material: [] };

// ── DuckDB bootstrap ─────────────────────────────────────────────
async function initDuckDB() {
  const DIST = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev20.0/dist/';
  const bundles = {
    mvp: {
      mainModule: DIST + 'duckdb-mvp.wasm',
      mainWorker: DIST + 'duckdb-browser-mvp.worker.js',
    },
    eh: {
      mainModule: DIST + 'duckdb-eh.wasm',
      mainWorker: DIST + 'duckdb-browser-eh.worker.js',
    },
  };

  const bundle = await duckdb.selectBundle(bundles);
  console.log('[DuckDB] bundle mainWorker:', bundle.mainWorker);

  // Inline worker via blob — no separate worker file needed in the project root
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );
  const worker = new Worker(workerUrl);
  db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  await db.instantiate(bundle.mainModule);
  URL.revokeObjectURL(workerUrl);
  conn = await db.connect();

  // Load parquets from manifest or add them manually via registerParquetFile()
  if (R2_MANIFEST_URL) {
    await loadParquetsFromManifest();
  } else if (Object.keys(PARQUET_FILES).length > 0) {
    // Fallback to static PARQUET_FILES if defined
    for (const [name, url] of Object.entries(PARQUET_FILES)) {
      await registerParquetFile(url, name);
    }
  } else {
    console.warn('[DuckDB] No parquets configured. Add a manifest or call registerParquetFile() manually.');
  }
}

// ── Helper: run SQL, return JS array of row objects ──────────────
async function query(sql) {
  const result = await conn.query(sql);
  const cols = result.schema.fields.map(f => f.name);
  const rows = [];
  for (let i = 0; i < result.numRows; i++) {
    const row = {};
    for (const col of cols) {
      const vec = result.getChild(col);
      let val = vec.get(i);
      // Arrow returns BigInt for INT64/BIGINT columns — coerce to Number
      if (typeof val === 'bigint') val = Number(val);
      row[col] = val;
    }
    rows.push(row);
  }
  return rows;
}

// ── Helper: Plotly wrapper exposed to user JS snippets ───────────
function plot(divId, data, layout = {}, config = {}) {
  const defaults = {
    paper_bgcolor: 'transparent',
    plot_bgcolor:  'transparent',
    font: { color: '#e6edf3' },
    margin: { t: 30, r: 20, b: 50, l: 60 },
  };
  Plotly.newPlot(divId, data, { ...defaults, ...layout }, { responsive: true, ...config });
}

// ── Render helpers ───────────────────────────────────────────────
function chip(label, value) {
  return `<div class="stat-chip"><div class="label">${label}</div><div class="value">${value}</div></div>`;
}

function renderTable(rows, targetId) {
  if (!rows || rows.length === 0) {
    document.getElementById(targetId).innerHTML = '<p style="color:var(--muted)">No rows returned.</p>';
    return;
  }
  const cols = Object.keys(rows[0]);
  let html = '<div class="table-scroll"><table class="result-table"><thead><tr>';
  for (const c of cols) html += `<th>${esc(c)}</th>`;
  html += '</tr></thead><tbody>';
  for (const r of rows) {
    html += '<tr>';
    for (const c of cols) html += `<td>${esc(String(r[c] ?? ''))}</td>`;
    html += '</tr>';
  }
  html += '</tbody></table></div>';
  document.getElementById(targetId).innerHTML = html;
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

// ── Dashboard Config ─────────────────────────────────────────────
const CONFIG_KEY = 'rcd_dashboard_v1';

// Populated at startup from default-config.json
let DEFAULT_CONFIG = { version: 1, panels: [] };
let CONFIG = null;

async function loadDefaultConfig() {
  try {
    const res = await fetch('default-config.json');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    DEFAULT_CONFIG = await res.json();
    console.log('[Config] Loaded default-config.json');
  } catch (e) {
    console.warn('[Config] Could not load default-config.json, using empty defaults:', e.message);
  }
}

function loadConfig() {
  try {
    const raw = localStorage.getItem(CONFIG_KEY);
    if (raw) {
      const cfg = JSON.parse(raw);
      if (cfg.version === DEFAULT_CONFIG.version) return cfg;
    }
  } catch (_) {}
  return JSON.parse(JSON.stringify(DEFAULT_CONFIG));
}

function saveConfig() {
  localStorage.setItem(CONFIG_KEY, JSON.stringify(CONFIG));
}

// ── toNum helper ─────────────────────────────────────────────────
const toNum = v => (v === null || v === undefined) ? null : (typeof v === 'bigint' ? Number(v) : v);

// ── Filter helpers ───────────────────────────────────────────────
function parseFilterInput(value) {
  return value.split(',').map(v => v.trim()).filter(v => v.length > 0);
}

function escapeSQLVal(v) {
  return "'" + v.replace(/'/g, "''") + "'";
}

function hasActiveFilters() {
  return filters.location.length > 0 || filters.material.length > 0;
}

function buildFilterWhere() {
  const parts = [];
  if (filters.location.length > 0) {
    const vals = filters.location.map(escapeSQLVal).join(',');
    parts.push(`(s.gravity_well IN (${vals}) OR s.system IN (${vals}))`);
  }
  if (filters.material.length > 0) {
    const vals = filters.material.map(escapeSQLVal).join(',');
    parts.push(`c.type IN (${vals})`);
  }
  return parts.length > 0 ? 'AND ' + parts.join(' AND ') : '';
}

function getDefaultSQL(key) {
  const fw = buildFilterWhere();
  switch (key) {
    case 'quality_vs_counts':
      return `SELECT CAST(FLOOR(c.quality / 10) * 10 AS INT) AS quality_bin,
                     COUNT(*) AS count
              FROM compositions c
              JOIN scans s ON s.capture_id = c.capture_id
              WHERE c.type NOT IN ('inert_materials','none','Inert Materials')
              ${fw}
              GROUP BY quality_bin
              ORDER BY quality_bin`;
    case 'material_vs_volume':
      return `SELECT c.type AS material_type,
                     ROUND(SUM(s.volume * c.amount / 100), 2) AS material_volume
              FROM scans s
              JOIN compositions c ON s.capture_id = c.capture_id
              WHERE c.type NOT IN ('inert_materials','none','Inert Materials')
              ${fw}
              GROUP BY c.type
              ORDER BY material_volume DESC`;
    case 'deposit_pie':
      return `SELECT s.deposit,
                     COUNT(*) AS count
              FROM scans s
              JOIN compositions c ON s.capture_id = c.capture_id
              WHERE c.type NOT IN ('inert_materials','none','Inert Materials')
              ${fw}
              GROUP BY s.deposit
              ORDER BY count DESC`;
    default:
      return null;
  }
}

async function renderFilterablePanels() {
  for (const def of CONFIG.panels) {
    if (def.filterable) await renderPanel(def);
  }
}

// ── Panel DOM builder ─────────────────────────────────────────────
function makePanel(def) {
  const sec = document.createElement('section');
  sec.className = 'panel';
  sec.id = def.id;
  if (def.size) {
    sec.style.width  = def.size.w + 'px';
    sec.style.height = def.size.h + 'px';
    sec.style.flex   = '0 0 auto';
  }
  const cid = def.type === 'summary' ? (def.id + '-body') : (def.id + '-chart');
  const inner = def.type === 'summary'
    ? `<div id="${cid}" class="panel-body">Loading…</div>`
    : `<div id="${cid}" class="chart-container"><span class="panel-placeholder">Loading…</span></div>`;
  sec.innerHTML = `
    <div class="panel-header">
      <h2>${esc(def.title)}</h2>
      <button class="panel-cog" title="Edit panel">&#9881;</button>
    </div>
    <div class="panel-content">${inner}</div>
    <div class="resize-handle" title="Drag to resize">&#8690;</div>
  `;
  sec.querySelector('.panel-cog').addEventListener('click', () => openPanelEdit(def.id));
  initResize(sec, def.id);
  return sec;
}

// ── Render a single panel's data ──────────────────────────────────
async function renderPanel(def) {
  if (def.type === 'summary') {
    const el = document.getElementById(def.id + '-body');
    if (!el) return;
    el.textContent = 'Loading…';
    try {
      let scSQL, ccSQL, dpSQL, usSQL;
      if (def.filterable && hasActiveFilters()) {
        const fw = buildFilterWhere();
        scSQL = `SELECT COUNT(DISTINCT s.capture_id) AS n FROM scans s JOIN compositions c ON s.capture_id = c.capture_id WHERE 1=1 ${fw}`;
        ccSQL = `SELECT COUNT(*) AS n FROM compositions c JOIN scans s ON s.capture_id = c.capture_id WHERE 1=1 ${fw}`;
        dpSQL = `SELECT COUNT(DISTINCT s.deposit) AS n FROM scans s JOIN compositions c ON s.capture_id = c.capture_id WHERE 1=1 ${fw}`;
        usSQL = `SELECT COUNT(DISTINCT s.user) AS n FROM scans s JOIN compositions c ON s.capture_id = c.capture_id WHERE 1=1 ${fw}`;
      } else {
        scSQL = 'SELECT COUNT(*) AS n FROM scans';
        ccSQL = 'SELECT COUNT(*) AS n FROM compositions';
        dpSQL = 'SELECT COUNT(DISTINCT deposit) AS n FROM scans';
        usSQL = 'SELECT COUNT(DISTINCT "user") AS n FROM scans';
      }
      const [sc] = await query(scSQL);
      const [cc] = await query(ccSQL);
      const [dp] = await query(dpSQL);
      const [us] = await query(usSQL);
      el.innerHTML = '<div class="stat-grid">'
        + chip('Scans', sc.n) + chip('Compositions', cc.n)
        + chip('Deposit types', dp.n) + chip('Users', us.n)
        + '</div>';
    } catch (e) { el.innerHTML = `<span style="color:var(--danger)">${esc(e.message)}</span>`; }
    return;
  }

  const chartEl = document.getElementById(def.id + '-chart');
  if (!chartEl) return;
  const sql = def.defaultKey ? getDefaultSQL(def.defaultKey) : def.sql;
  if (!sql) {
    chartEl.innerHTML = '<span class="panel-placeholder">No query — click ⚙ to configure</span>';
    return;
  }
  try {
    const rows = await query(sql);
    const c = def.chart;
    if (!c || !c.x || !c.y) { chartEl.innerHTML = '<span class="panel-placeholder">Map axes via ⚙</span>'; return; }

    if (c.type === 'pie') {
      plot(def.id + '-chart', [{
        type: 'pie', labels: rows.map(r => r[c.x]),
        values: rows.map(r => toNum(r[c.y])),
        hole: 0.4, textinfo: 'label+percent',
      }], {});
    } else if (c.group) {
      const groups = {};
      for (const r of rows) {
        const key = String(r[c.group]);
        if (!groups[key]) groups[key] = [];
        groups[key].push(r);
      }
      const traces = Object.entries(groups).map(([name, gRows]) => {
        const t = {
          type: c.type,
          name,
          x: gRows.map(r => toNum(r[c.x])),
          y: gRows.map(r => toNum(r[c.y])),
          mode: c.type === 'scatter' ? 'markers' : undefined,
        };
        if (c.text) {
          t.text = gRows.map(r => String(r[c.text]));
          t.hovertemplate = `x: %{x}<br>y: %{y}<br>${esc(c.text)}: %{text}<extra></extra>`;
        }
        return t;
      });
      plot(def.id + '-chart', traces, { xaxis: { title: c.x }, yaxis: { title: c.y } });
    } else {
      const trace = {
        type: c.type,
        x: rows.map(r => toNum(r[c.x])),
        y: rows.map(r => toNum(r[c.y])),
        mode: c.type === 'scatter' ? 'markers' : undefined,
      };
      if (c.color) {
        const colors = rows.map(r => Number(r[c.color]));
        trace.marker = { color: colors, colorscale: 'Viridis', showscale: true,
          colorbar: { title: { text: c.color }, thickness: 14 } };
      }
      if (c.size && c.type === 'scatter') {
        const raw = rows.map(r => Number(r[c.size]));
        const lo = Math.min(...raw), hi = Math.max(...raw), span = hi - lo || 1;
        trace.marker = trace.marker ?? {};
        trace.marker.size = raw.map(v => 5 + ((v - lo) / span) * 23);
      }
      if (c.text) {
        trace.text = rows.map(r => String(r[c.text]));
        trace.hovertemplate = `x: %{x}<br>y: %{y}<br>${esc(c.text)}: %{text}<extra></extra>`;
      }
      plot(def.id + '-chart', [trace], { xaxis: { title: c.x }, yaxis: { title: c.y } });
    }
  } catch (e) {
    chartEl.innerHTML = `<pre style="color:var(--danger);white-space:pre-wrap;padding:.5rem">${esc(e.message)}</pre>`;
  }
}

// ── Build panel DOM shells (no data) ─────────────────────────────
function buildPanelShells() {
  const dash = document.getElementById('dashboard');
  dash.innerHTML = '';
  for (const def of CONFIG.panels) dash.appendChild(makePanel(def));
}

// ── Resize drag handle ───────────────────────────────────────────
function initResize(panelEl, panelId) {
  const handle = panelEl.querySelector('.resize-handle');
  if (!handle) return;
  let sx, sy, sw, sh;
  handle.addEventListener('mousedown', e => {
    e.preventDefault();
    sx = e.clientX; sy = e.clientY;
    sw = panelEl.offsetWidth; sh = panelEl.offsetHeight;
    const onMove = e => {
      const w = Math.max(280, sw + (e.clientX - sx));
      const h = Math.max(180, sh + (e.clientY - sy));
      panelEl.style.width  = w + 'px';
      panelEl.style.height = h + 'px';
      panelEl.style.flex   = '0 0 auto';
    };
    const onUp = () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      const def = CONFIG.panels.find(p => p.id === panelId);
      if (def) { def.size = { w: panelEl.offsetWidth, h: panelEl.offsetHeight }; saveConfig(); }
      // Tell Plotly to relayout inside this panel
      const chartDiv = panelEl.querySelector('.chart-container');
      if (chartDiv) Plotly.Plots.resize(chartDiv);
    };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  });
}

// ── Panel Edit Modal ──────────────────────────────────────────────
let _pemId = null, _pemRows = [];

function fillPemSelects(cols) {
  for (const id of ['pem-x', 'pem-y', 'pem-color', 'pem-size', 'pem-text', 'pem-group']) {
    const sel = document.getElementById(id);
    const none = ['pem-color', 'pem-size', 'pem-text', 'pem-group'].includes(id);
    sel.innerHTML = none ? '<option value="">— none —</option>' : '';
    for (const col of cols) {
      const o = document.createElement('option');
      o.value = col; o.textContent = col; sel.appendChild(o);
    }
  }
}

function openPanelEdit(id) {
  _pemId = id; _pemRows = [];
  const def = CONFIG.panels.find(p => p.id === id);
  if (!def) return;
  document.getElementById('pem-title').value = def.title;
  document.getElementById('pem-sql').value   = (def.defaultKey ? getDefaultSQL(def.defaultKey) : def.sql) || '';
  document.getElementById('pem-mapping').classList.add('hidden');
  document.getElementById('pem-fetch-status').textContent = '';
  document.getElementById('pem-status').textContent = '';
  const isSummary = def.type === 'summary';
  ['pem-sql-row', 'pem-fetch-row', 'pem-mapping-row'].forEach(rid => {
    document.getElementById(rid).style.display = isSummary ? 'none' : '';
  });
  if (def.chart) {
    document.getElementById('pem-type').value  = def.chart.type  || 'bar';
    document.getElementById('pem-color').value = def.chart.color || '';
    document.getElementById('pem-size').value  = def.chart.size  || '';
    document.getElementById('pem-text').value  = def.chart.text  || '';
    document.getElementById('pem-group').value = def.chart.group || '';
  }
  document.getElementById('panel-edit-modal').classList.remove('hidden');
}

function setupPanelEditModal() {
  const modal = document.getElementById('panel-edit-modal');
  document.getElementById('pem-close').addEventListener('click', () => modal.classList.add('hidden'));
  modal.addEventListener('click', e => { if (e.target === modal) modal.classList.add('hidden'); });

  document.getElementById('pem-sql').addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); document.getElementById('pem-fetch').click(); }
    if (e.key === 'Tab') {
      e.preventDefault();
      const el = e.target, s = el.selectionStart;
      el.value = el.value.substring(0, s) + '  ' + el.value.substring(el.selectionEnd);
      el.selectionStart = el.selectionEnd = s + 2;
    }
  });

  document.getElementById('pem-fetch').addEventListener('click', async () => {
    const sql = document.getElementById('pem-sql').value.trim();
    const st  = document.getElementById('pem-fetch-status');
    if (!sql) return;
    st.textContent = 'Running…'; st.style.color = 'var(--accent)';
    try {
      _pemRows = await query(sql);
      if (!_pemRows.length) { st.textContent = 'No rows.'; return; }
      const cols = Object.keys(_pemRows[0]);
      fillPemSelects(cols);
      const def = CONFIG.panels.find(p => p.id === _pemId);
      if (def?.chart) {
        document.getElementById('pem-x').value     = cols.includes(def.chart.x)     ? def.chart.x     : cols[0];
        document.getElementById('pem-y').value     = cols.includes(def.chart.y)     ? def.chart.y     : cols[1] || cols[0];
        document.getElementById('pem-color').value = cols.includes(def.chart.color) ? def.chart.color : '';
        document.getElementById('pem-size').value  = cols.includes(def.chart.size)  ? def.chart.size  : '';
        document.getElementById('pem-text').value  = cols.includes(def.chart.text)  ? def.chart.text  : '';
        document.getElementById('pem-group').value = cols.includes(def.chart.group) ? def.chart.group : '';
      }
      document.getElementById('pem-mapping').classList.remove('hidden');
      st.textContent = `${_pemRows.length} rows — map axes below.`;
      st.style.color = 'var(--accent2)';
    } catch(e) { st.textContent = 'Error: ' + e.message; st.style.color = 'var(--danger)'; }
  });

  document.getElementById('pem-save').addEventListener('click', () => {
    const def = CONFIG.panels.find(p => p.id === _pemId);
    if (!def) return;
    def.title = document.getElementById('pem-title').value.trim() || def.title;
    if (def.type === 'chart') {
      def.sql   = document.getElementById('pem-sql').value.trim();
      def.chart = {
        type:  document.getElementById('pem-type').value,
        x:     document.getElementById('pem-x').value,
        y:     document.getElementById('pem-y').value,
        color: document.getElementById('pem-color').value,
        size:  document.getElementById('pem-size').value,
        text:  document.getElementById('pem-text').value,
        group: document.getElementById('pem-group').value,
      };
    }
    saveConfig();
    const panelEl = document.getElementById(_pemId);
    if (panelEl) panelEl.querySelector('h2').textContent = def.title;
    modal.classList.add('hidden');
    renderPanel(def);
  });

  document.getElementById('pem-delete').addEventListener('click', () => {
    if (!_pemId) return;
    const st = document.getElementById('pem-status');
    if (_pemId === 'p-summary') {
      st.textContent = 'Cannot delete the summary panel.';
      st.style.color = 'var(--danger)'; return;
    }
    CONFIG.panels = CONFIG.panels.filter(p => p.id !== _pemId);
    saveConfig();
    const panelEl = document.getElementById(_pemId);
    if (panelEl) panelEl.remove();
    modal.classList.add('hidden');
  });

  document.getElementById('pem-reset').addEventListener('click', () => {
    if (!confirm('Reset dashboard to defaults? This removes all custom panels.')) return;
    CONFIG = JSON.parse(JSON.stringify(DEFAULT_CONFIG));
    saveConfig();
    buildPanelShells();
    if (conn) for (const def of CONFIG.panels) renderPanel(def);
    document.getElementById('panel-edit-modal').classList.add('hidden');
  });
}

// ── Config export / import / add-panel ───────────────────────────
function setupConfigIO() {
  document.getElementById('export-config').addEventListener('click', () => {
    const blob = new Blob([JSON.stringify(CONFIG, null, 2)], { type: 'application/json' });
    const url  = URL.createObjectURL(blob);
    Object.assign(document.createElement('a'), { href: url, download: 'dashboard-config.json' }).click();
    URL.revokeObjectURL(url);
  });

  document.getElementById('import-config-input').addEventListener('change', e => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = ev => {
      try {
        const cfg = JSON.parse(ev.target.result);
        if (!cfg.panels || !Array.isArray(cfg.panels)) throw new Error('Invalid config: missing panels array');
        CONFIG = cfg; CONFIG.version = DEFAULT_CONFIG.version;
        saveConfig();
        buildPanelShells();
        if (conn) for (const def of CONFIG.panels) renderPanel(def);
      } catch(err) { alert('Import failed: ' + err.message); }
    };
    reader.readAsText(file);
    e.target.value = '';
  });

  document.getElementById('add-panel').addEventListener('click', () => {
    // Switch overlay to Chart Builder tab
    document.querySelectorAll('.overlay-tabs .tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.querySelector('.tab[data-tab="chart"]').classList.add('active');
    document.getElementById('tab-chart').classList.add('active');
    document.getElementById('overlay').classList.remove('hidden');
  });
}

// ── Chart Builder ────────────────────────────────────────────────
let _cbRows = [];

function populateColumnSelects(cols) {
  const withNone = ['cb-color', 'cb-size', 'cb-text', 'cb-group'];
  for (const id of ['cb-x', 'cb-y', ...withNone]) {
    const sel = document.getElementById(id);
    sel.innerHTML = withNone.includes(id) ? '<option value="">— none —</option>' : '';
    for (const col of cols) {
      const opt = document.createElement('option');
      opt.value = col;
      opt.textContent = col;
      sel.appendChild(opt);
    }
  }
  // Sensible defaults: first two numeric-looking columns for x/y
  const numeric = cols.filter(c => {
    const sample = _cbRows[0]?.[c];
    return sample !== null && sample !== undefined && !isNaN(Number(sample));
  });
  const textCols = cols.filter(c => !numeric.includes(c));

  document.getElementById('cb-x').value = numeric[0] ?? cols[0] ?? '';
  document.getElementById('cb-y').value = numeric[1] ?? cols[1] ?? '';
  if (numeric[2]) document.getElementById('cb-color').value = numeric[2];
  if (textCols[0]) document.getElementById('cb-text').value = textCols[0];
}

function _cbPreviewPanel() {
  let panel = document.getElementById('panel-user');
  if (!panel) {
    panel = document.createElement('section');
    panel.id = 'panel-user';
    panel.className = 'panel full-width';
    panel.innerHTML = `
      <div class="panel-header"><h2>Query Result</h2></div>
      <div class="panel-content">
        <div id="user-table" class="panel-body"></div>
        <div id="user-chart" class="chart-container"></div>
      </div>
      <div class="resize-handle" title="Drag to resize">&#8690;</div>
    `;
    initResize(panel, 'panel-user');
    document.getElementById('dashboard').appendChild(panel);
  } else {
    panel.style.display = 'block';
  }
  return panel;
}

function _cbBuildTrace(type, xCol, yCol, colorCol, sizeCol, textCol, groupCol) {
  if (groupCol) {
    const groups = {};
    for (const r of _cbRows) {
      const key = String(r[groupCol]);
      if (!groups[key]) groups[key] = [];
      groups[key].push(r);
    }
    return Object.entries(groups).map(([name, gRows]) => {
      const t = { type, name, x: gRows.map(r => toNum(r[xCol])), y: gRows.map(r => toNum(r[yCol])),
        mode: type === 'scatter' ? 'markers' : undefined };
      if (textCol) {
        t.text = gRows.map(r => String(r[textCol]));
        t.hovertemplate = `x: %{x}<br>y: %{y}<br>${esc(textCol)}: %{text}<extra></extra>`;
      }
      return t;
    });
  }
  const xs = _cbRows.map(r => toNum(r[xCol]));
  const ys = _cbRows.map(r => toNum(r[yCol]));
  const trace = { type, x: xs, y: ys, mode: type === 'scatter' ? 'markers' : undefined };
  if (colorCol) {
    trace.marker = { color: _cbRows.map(r => Number(r[colorCol])), colorscale: 'Viridis', showscale: true,
      colorbar: { title: { text: colorCol }, thickness: 14 } };
  }
  if (sizeCol && type === 'scatter') {
    const raw = _cbRows.map(r => Number(r[sizeCol]));
    const lo = Math.min(...raw), hi = Math.max(...raw), span = hi - lo || 1;
    trace.marker = trace.marker ?? {};
    trace.marker.size = raw.map(v => 5 + ((v - lo) / span) * 23);
  }
  if (textCol) {
    trace.text = _cbRows.map(r => String(r[textCol]));
    trace.hovertemplate = `x: %{x}<br>y: %{y}<br>${esc(textCol)}: %{text}<extra></extra>`;
  }
  return [trace];
}

function setupChartBuilder() {
  document.getElementById('cb-fetch').addEventListener('click', async () => {
    const sql    = document.getElementById('chart-sql').value.trim();
    const status = document.getElementById('cb-fetch-status');
    if (!sql) return;
    status.textContent = 'Running…';
    status.style.color = 'var(--accent)';
    try {
      _cbRows = await query(sql);
      if (_cbRows.length === 0) { status.textContent = 'No rows returned.'; return; }
      populateColumnSelects(Object.keys(_cbRows[0]));
      document.getElementById('cb-mapping').classList.remove('hidden');
      status.textContent = `${_cbRows.length} rows fetched — map columns below.`;
      status.style.color = 'var(--accent2)';
    } catch(e) {
      status.textContent = 'Error: ' + e.message;
      status.style.color = 'var(--danger)';
    }
  });

  document.getElementById('cb-plot').addEventListener('click', () => {
    if (!_cbRows.length) return;
    const status   = document.getElementById('cb-plot-status');
    const type     = document.getElementById('cb-type').value;
    const xCol     = document.getElementById('cb-x').value;
    const yCol     = document.getElementById('cb-y').value;
    const colorCol = document.getElementById('cb-color').value;
    const sizeCol  = document.getElementById('cb-size').value;
    const textCol  = document.getElementById('cb-text').value;
    const groupCol = document.getElementById('cb-group').value;
    if (!xCol || !yCol) { status.textContent = 'Pick X and Y columns.'; return; }

    const panel = _cbPreviewPanel();
    document.getElementById('user-table').innerHTML = '';
    const traces = _cbBuildTrace(type, xCol, yCol, colorCol, sizeCol, textCol, groupCol);
    plot('user-chart', traces, { xaxis: { title: xCol }, yaxis: { title: yCol } });
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
    status.textContent = `Plotted ${_cbRows.length} points.`;
    status.style.color = 'var(--accent2)';
  });

  document.getElementById('cb-save').addEventListener('click', () => {
    if (!_cbRows.length) { document.getElementById('cb-plot-status').textContent = 'Fetch data first.'; return; }
    const status   = document.getElementById('cb-plot-status');
    const type     = document.getElementById('cb-type').value;
    const xCol     = document.getElementById('cb-x').value;
    const yCol     = document.getElementById('cb-y').value;
    const colorCol = document.getElementById('cb-color').value;
    const sizeCol  = document.getElementById('cb-size').value;
    const textCol  = document.getElementById('cb-text').value;
    const groupCol = document.getElementById('cb-group').value;
    const sql      = document.getElementById('chart-sql').value.trim();
    if (!xCol || !yCol) { status.textContent = 'Pick X and Y columns.'; return; }

    const id  = 'p-custom-' + Date.now();
    const def = {
      id, type: 'chart',
      title: `${type.charAt(0).toUpperCase() + type.slice(1)}: ${xCol} vs ${yCol}`,
      sql, chart: { type, x: xCol, y: yCol, color: colorCol, size: sizeCol, text: textCol, group: groupCol },
      size: null,
    };
    CONFIG.panels.push(def);
    saveConfig();
    const panelEl = makePanel(def);
    const dashboard = document.getElementById('dashboard');
    // Insert before panel-user if it exists
    const userPanel = document.getElementById('panel-user');
    dashboard.insertBefore(panelEl, userPanel || null);
    renderPanel(def);
    document.getElementById('overlay').classList.add('hidden');
    panelEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
    status.textContent = 'Panel saved to dashboard!';
    status.style.color = 'var(--accent2)';
  });

  // Ctrl+Enter in chart-sql textarea also fetches
  document.getElementById('chart-sql').addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      document.getElementById('cb-fetch').click();
    }
    if (e.key === 'Tab') {
      e.preventDefault();
      const el = e.target, s = el.selectionStart;
      el.value = el.value.substring(0, s) + '  ' + el.value.substring(el.selectionEnd);
      el.selectionStart = el.selectionEnd = s + 2;
    }
  });
}

// ── Overlay toggle ───────────────────────────────────────────────
function setupOverlay() {
  const overlay = document.getElementById('overlay');
  document.getElementById('toggle-overlay').addEventListener('click', () => overlay.classList.remove('hidden'));
  document.getElementById('close-overlay').addEventListener('click', () => overlay.classList.add('hidden'));
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.classList.add('hidden'); });

  // Tabs
  document.querySelectorAll('.overlay-tabs .tab').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
    });
  });

  // Run button
  document.getElementById('run-query').addEventListener('click', runUserQuery);

  // Ctrl+Enter to run
  document.querySelectorAll('#sql-input, #js-input').forEach(el => {
    el.addEventListener('keydown', e => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runUserQuery(); }
      // Tab key inserts spaces
      if (e.key === 'Tab') {
        e.preventDefault();
        const start = el.selectionStart;
        el.value = el.value.substring(0, start) + '  ' + el.value.substring(el.selectionEnd);
        el.selectionStart = el.selectionEnd = start + 2;
      }
    });
  });
}

// ── Run user query from overlay ──────────────────────────────────
async function runUserQuery() {
  const status = document.getElementById('query-status');
  const isSql  = document.querySelector('.tab.active').dataset.tab === 'sql';

  const panel = _cbPreviewPanel();
  status.textContent = 'Running…';
  status.style.color = 'var(--accent)';
  document.getElementById('user-chart').innerHTML = '';
  document.getElementById('user-table').innerHTML = '';

  const t0 = performance.now();
  try {
    let rows;
    if (isSql) {
      const sql = document.getElementById('sql-input').value.trim();
      if (!sql) { status.textContent = 'Empty query'; return; }
      rows = await query(sql);
    } else {
      const code = document.getElementById('js-input').value.trim();
      if (!code) { status.textContent = 'Empty script'; return; }
      const fn = new Function('query', 'plot', `return (async () => { ${code} })();`);
      rows = await fn(query, plot);
    }
    if (Array.isArray(rows) && rows.length > 0) renderTable(rows, 'user-table');
    const ms = (performance.now() - t0).toFixed(0);
    status.textContent = `Done in ${ms}ms` + (Array.isArray(rows) ? ` — ${rows.length} rows` : '');
    status.style.color = 'var(--accent2)';
  } catch (err) {
    status.textContent = 'Error: ' + err.message;
    status.style.color = 'var(--danger)';
    document.getElementById('user-table').innerHTML =
      `<pre style="color:var(--danger);white-space:pre-wrap">${esc(err.message)}</pre>`;
  }
  panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ── Sidebar & Filters ────────────────────────────────────────────
function setupSidebar() {
  const sidebar = document.getElementById('sidebar');
  const toggle  = document.getElementById('sidebar-toggle');

  toggle.addEventListener('click', () => {
    sidebar.classList.toggle('collapsed');
    toggle.innerHTML = sidebar.classList.contains('collapsed') ? '&#9654;' : '&#9664;';
  });

  setupAutocomplete('filter-location', 'autocomplete-location', 'location');
  setupAutocomplete('filter-material', 'autocomplete-material', 'material');

  document.getElementById('apply-filters').addEventListener('click', applyFilters);
  document.getElementById('clear-filters').addEventListener('click', () => {
    document.getElementById('filter-location').value = '';
    document.getElementById('filter-material').value = '';
    filters = { location: [], material: [] };
    renderFilterablePanels();
  });

  document.querySelectorAll('.filter-input').forEach(el => {
    el.addEventListener('keydown', e => {
      if (e.key === 'Enter') applyFilters();
    });
  });
}

function applyFilters() {
  filters.location = parseFilterInput(document.getElementById('filter-location').value);
  filters.material = parseFilterInput(document.getElementById('filter-material').value);
  renderFilterablePanels();
}

function setupAutocomplete(inputId, listId, dataKey) {
  const input = document.getElementById(inputId);
  const list  = document.getElementById(listId);

  input.addEventListener('input', () => {
    const parts   = input.value.split(',');
    const current = parts[parts.length - 1].trim().toLowerCase();
    if (!current) { list.classList.add('hidden'); return; }

    const existing = parts.slice(0, -1).map(p => p.trim().toLowerCase());
    const matches  = autocompleteData[dataKey]
      .filter(v => v.toLowerCase().includes(current) && !existing.includes(v.toLowerCase()))
      .slice(0, 10);

    if (matches.length === 0) { list.classList.add('hidden'); return; }

    list.innerHTML = matches.map(v => `<div class="autocomplete-item">${esc(v)}</div>`).join('');
    list.classList.remove('hidden');

    list.querySelectorAll('.autocomplete-item').forEach(item => {
      item.addEventListener('mousedown', e => {
        e.preventDefault();
        parts[parts.length - 1] = ' ' + item.textContent;
        input.value = parts.join(',').replace(/^,\s*/, '') + ', ';
        list.classList.add('hidden');
        input.focus();
      });
    });
  });

  input.addEventListener('blur', () => {
    setTimeout(() => list.classList.add('hidden'), 150);
  });
}

async function populateAutocomplete() {
  try {
    const wells   = await query("SELECT DISTINCT gravity_well AS val FROM scans WHERE gravity_well IS NOT NULL ORDER BY val");
    const systems = await query("SELECT DISTINCT system AS val FROM scans WHERE system IS NOT NULL ORDER BY val");
    autocompleteData.location = [...new Set([...wells.map(r => r.val), ...systems.map(r => r.val)])].sort();

    const mats = await query("SELECT DISTINCT type AS val FROM compositions WHERE type NOT IN ('inert_materials','none','Inert Materials') ORDER BY val");
    autocompleteData.material = mats.map(r => r.val);
  } catch (e) {
    console.warn('[Autocomplete] Failed to populate:', e.message);
  }
}

// ── Boot ─────────────────────────────────────────────────────────
(async function main() {
  setupOverlay();
  setupChartBuilder();
  setupPanelEditModal();
  setupConfigIO();
  setupSidebar();

  // Load defaults from JSON file before building the dashboard
  await loadDefaultConfig();
  CONFIG = loadConfig();

  buildPanelShells();
  try {
    const summaryEl = document.getElementById('p-summary-body');
    if (summaryEl) summaryEl.textContent = 'Initializing DuckDB…';
    await initDuckDB();
    await populateAutocomplete();
    for (const def of CONFIG.panels) renderPanel(def);
  } catch (err) {
    const summaryEl = document.getElementById('p-summary-body');
    if (summaryEl) summaryEl.innerHTML =
      `<pre style="color:var(--danger)">Failed to load: ${esc(err.message)}</pre>`;
    console.error(err);
  }
})();
