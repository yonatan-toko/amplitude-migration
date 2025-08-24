// Helpers
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
const fetchJSON = async (url, opt) => {
  const res = await fetch(url, opt);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
};
const fmtUSD = (v) => (v == null || v === '-' ? '-' : `$${Number(v).toFixed(2)}`);
const fmtTimeAny = (t) => (t == null ? '-' : (typeof t === 'number' ? new Date(t * 1000).toLocaleString() : new Date(t).toLocaleString()));

// ---- Reports dir header refresh ----
async function refreshReportsDir() {
  try {
    const res = await fetch('/api/migration/reports-dir');
    if (!res.ok) throw new Error();
    const data = await res.json();
    const el = document.getElementById('reportsDir');
    if (el) el.textContent = data.reports_dir || '(not set)';
  } catch {
    const el = document.getElementById('reportsDir');
    if (el) el.textContent = '(unable to detect)';
  }
}

// ---- Live JSON validation for textareas ----
const JSON_FIELDS = [
  'EVENT_PROPERTY_KEEP',
  'EVENT_RENAME_MAP',
  'EVENT_PROP_RENAME_MAP'
];

function markJsonInvalid(el, msg = 'Invalid JSON') {
  el.style.borderColor = '#ef4444'; // red-500
  el.title = msg;
  el.dataset.invalid = '1';
}

function clearJsonInvalid(el) {
  el.style.borderColor = '';
  el.title = '';
  delete el.dataset.invalid;
}

function validateJSONField(el, fallback = '{}') {
  try {
    JSON.parse(el.value && el.value.trim() ? el.value : fallback);
    clearJsonInvalid(el);
    return true;
  } catch (e) {
    markJsonInvalid(el, e.message || 'Invalid JSON');
    return false;
  }
}

function updateSaveEnabled() {
  const form = document.getElementById('settings-form');
  const btn = form ? form.querySelector('button[type="submit"]') : null;
  if (!btn) return;
  const anyInvalid = JSON_FIELDS.some(name => {
    const el = form.elements.namedItem(name);
    return el && el.dataset.invalid === '1';
  });
  btn.disabled = anyInvalid;
  btn.style.opacity = anyInvalid ? 0.6 : 1;
  btn.style.cursor = anyInvalid ? 'not-allowed' : 'pointer';
}

// Tabs
$$('nav button').forEach(b => b.addEventListener('click', () => {
  $$('.tab').forEach(el => el.classList.remove('active'));
  $$('nav button').forEach(x => x.classList.remove('active'));
  $(`#tab-${b.dataset.tab}`).classList.add('active');
  b.classList.add('active');
}));

// ---------- Settings (reads/writes config.py via API) ----------
const STRATS = [
  "client",
  "server_received",
  "server_upload",
  "prefer_client_fallback_server_received",
  "prefer_client_fallback_server_upload"
];

// populate time strategy
(function fillStrats(){
  const sel = $('#time-strategy');
  sel.innerHTML = '';
  STRATS.forEach(s => {
    const o = document.createElement('option');
    o.value = s; o.textContent = s;
    sel.appendChild(o);
  });
})();

async function loadSettings() {
  const s = await fetchJSON('/api/settings');
  const form = $('#settings-form');
  for (const [k,v] of Object.entries(s)) {
    const el = form.elements.namedItem(k);
    if (!el) continue;
    if (el.type === 'checkbox') el.checked = !!v;
    else if (el.tagName === 'TEXTAREA') el.value = typeof v === 'string' ? v : JSON.stringify(v, null, 2);
    else if (k === 'EVENT_ALLOWLIST' && Array.isArray(v)) el.value = v.join(', ');
    else el.value = v ?? '';
  }
  // attach validators to JSON fields (allow empty => '{}')
  JSON_FIELDS.forEach(name => {
    const el = form.elements.namedItem(name);
    if (!el) return;
    validateJSONField(el, '{}');
    el.addEventListener('input', () => {
      validateJSONField(el, '{}');
      updateSaveEnabled();
    });
  });
  updateSaveEnabled();
}
loadSettings().catch(console.error);
refreshReportsDir().catch(() => {});

// Save settings => writes config.py
$('#settings-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  $('#save-status').textContent = 'Saving…';
  const form = e.currentTarget;
  const payload = {};
  for (const el of form.elements) {
    if (!el.name) continue;
    if (el.type === 'checkbox') payload[el.name] = el.checked;
    else if (el.tagName === 'TEXTAREA') {
      try { payload[el.name] = JSON.parse(el.value || '{}'); }
      catch { alert(`Invalid JSON in ${el.name}`); $('#save-status').textContent = ''; return; }
    } else if (el.name === 'EVENT_ALLOWLIST') {
      payload[el.name] = (el.value || '').split(',').map(s => s.trim()).filter(Boolean);
    } else if (el.type === 'number') {
      payload[el.name] = Number(el.value);
    } else {
      payload[el.name] = el.value;
    }
  }
  try {
    const r = await fetchJSON('/api/settings/save', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    $('#save-status').textContent = 'Saved ✓';
    // update Reports dir display
    const el = $('#reportsDir');
    if (el && r.reports_dir) el.textContent = r.reports_dir;
    // refresh header path from server (in case)
    refreshReportsDir().catch(() => {});
    setTimeout(() => $('#save-status').textContent = '', 1200);
  } catch (err) {
    console.error(err);
    $('#save-status').textContent = 'Failed';
  }
});

// Upload ID map CSV → auto-updates USER_ID_REMAP_PATH in config.py
$('#btn-upload').addEventListener('click', async () => {
  const file = $('#idmap-file').files?.[0];
  if (!file) { alert('Choose a CSV first.'); return; }
  const fd = new FormData();
  fd.append('file', file, file.name);
  $('#upload-result').textContent = 'Uploading…';
  try {
    const r = await fetchJSON('/api/upload/id-map', { method: 'POST', body: fd });
    $('#upload-result').textContent = `Uploaded: ${r.path}`;
    const input = $('#settings-form').elements.namedItem('USER_ID_REMAP_PATH');
    if (input) input.value = r.path;
  } catch (e) {
    console.error(e);
    $('#upload-result').textContent = 'Failed';
  }
});

// ---------- Run ----------
async function triggerRun(dry) {
  $('#run-status').textContent = dry ? 'Dry run…' : 'Real run…';
  $('#run-output').textContent = '';
  try {
    const data = await fetchJSON('/api/run', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ dry_run: dry })
    });
    $('#run-status').textContent = data.ok ? 'Done ✓' : 'Failed';
    $('#run-output').textContent = JSON.stringify(data.summary, null, 2);
    await fetchRuns(); // refresh list
    // switch to reports tab
    $$('nav button').find(b => b.dataset.tab === 'reports')?.click();
  } catch (e) {
    console.error(e);
    $('#run-status').textContent = 'Run failed';
  }
}
$('#btn-dry-run').addEventListener('click', () => triggerRun(true));
$('#btn-real-run').addEventListener('click', () => {
  const ok = window.confirm('This will SEND events to your DESTINATION project.\nAre you sure you want to proceed?');
  if (ok) triggerRun(false);
});

// ---------- Reports ----------
const runsBody = $('#runsBody');
const detailJson = $('#detailJson');
const refreshBtn = $('#refreshBtn');

function renderRows(rows) {
  if (!rows || rows.length === 0) {
    runsBody.innerHTML = `<tr><td colspan="10" class="muted">No reports yet. Run a migration and refresh.</td></tr>`;
    return;
  }
  const html = rows.map(r => {
    const name = r.id || r.name || r;
    const started = fmtTimeAny(r.started_at);
    const duration = r.duration_s ?? '-';
    const read = r.events_read ?? '-';
    const kept = r.events_kept ?? '-';
    const sent = r.events_sent ?? '-';
    const mtu = r.mtu_estimate ?? '-';
    const cost = r.estimated_cost_usd != null ? r.estimated_cost_usd
               : (r.mtu && r.mtu.estimated_cost_usd != null ? r.mtu.estimated_cost_usd : '-');
    const mode = r.dry_run === true ? 'Dry run' : (r.dry_run === false ? 'Real' : '-');
    return `
      <tr>
        <td class="mono">${name}</td>
        <td>${started}</td>
        <td>${duration}</td>
        <td>${read}</td>
        <td>${kept}</td>
        <td>${sent}</td>
        <td>${mtu}</td>
        <td>${fmtUSD(cost)}</td>
        <td>${mode}</td>
        <td><button class="btn ghost" data-name="${name}">View</button></td>
      </tr>`;
  }).join('');
  runsBody.innerHTML = html;
}

async function fetchRuns() {
  runsBody.innerHTML = `<tr><td colspan="10" class="muted">Loading…</td></tr>`;
  try {
    const raw = await fetchJSON('/api/migration/runs');
    const runs = Array.isArray(raw) ? raw : (raw && raw.runs ? raw.runs : []);
    runs.reverse(); // newest first
    renderRows(runs);
  } catch (e) {
    console.error(e);
    runsBody.innerHTML = `<tr><td colspan="10" class="muted">Failed to load runs.</td></tr>`;
  }
}
fetchRuns().catch(console.error);

runsBody.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-name]');
  if (!btn) return;
  const name = btn.getAttribute('data-name');
  detailJson.textContent = 'Loading…';
  try {
    // Accept either route shape
    let data;
    try { data = await fetchJSON(`/api/migration/run/${encodeURIComponent(name)}`); }
    catch { data = await fetchJSON(`/api/migration/runs/${encodeURIComponent(name)}`); }

    const lines = [];
    lines.push(`Started: ${fmtTimeAny(data.started_at)} | Ended: ${fmtTimeAny(data.ended_at)} | Duration(s): ${data.duration_s ?? '-'}`);
    const c = data.counters || {};
    lines.push(`Counters → read=${c.events_read ?? 0}, kept=${c.events_kept ?? 0}, sent=${c.events_sent ?? 0}`);
    const mtu = data.mtu || {};
    const estCost = mtu.estimated_cost_usd != null ? `$${Number(mtu.estimated_cost_usd).toFixed(2)}` : '-';
    lines.push(`MTU → users=${mtu.unique_user_ids ?? 0}, devices=${mtu.unique_device_ids ?? 0}, estimate=${mtu.estimate ?? 0}, cost=${estCost}`);
    const rem = data.id_remap || {};
    if (rem && (rem.enabled || rem.scope || rem.user_map_path || rem.device_map_path)) {
      lines.push(`ID Remap → enabled=${rem.enabled ? 'yes' : 'no'} scope=${rem.scope ?? '-'} user_map=${rem.user_map_path ?? '-'} device_map=${rem.device_map_path ?? '-'}`);
    }
    lines.push('');
    const samples = (data.samples && Array.isArray(data.samples.events)) ? data.samples.events : [];
    lines.push(`Samples → Events (count=${samples.length})`);
    if (samples.length === 0) {
      lines.push('(no samples captured — increase REPORT_SAMPLE_LIMIT and rerun)');
    } else {
      const cap = Math.min(samples.length, 20);
      for (let i = 0; i < cap; i++) {
        lines.push(`\n— Sample #${i + 1} —`);
        lines.push(JSON.stringify(samples[i], null, 2));
      }
      if (samples.length > cap) lines.push(`\n… ${samples.length - cap} more not shown`);
    }
    lines.push('\nFull run JSON →');
    lines.push(JSON.stringify(data, null, 2));
    detailJson.textContent = lines.join('\n');
    detailJson.classList.remove('muted');
  } catch (err) {
    console.error(err);
    detailJson.textContent = 'Failed to load run details.';
  }
});

// Refresh button
$('#refreshBtn').addEventListener('click', fetchRuns);

// Ensure initial save state
updateSaveEnabled();