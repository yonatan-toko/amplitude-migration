// --- helpers ---
const $ = (s, r = document) => r.querySelector(s);

const fmtUSD = (v) => (v == null || v === '-' ? '-' : `$${Number(v).toFixed(2)}`);
function fmtTimeAny(t) {
  if (t == null) return '-';
  try {
    if (typeof t === 'number') return new Date(t * 1000).toLocaleString();
    return new Date(t).toLocaleString();
  } catch { return String(t); }
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function updateReportsDir() {
  try {
    const d = await fetchJSON('/api/migration/reports-dir');
    const el = $('#reportsDir');
    if (el) el.textContent = d.reports_dir || '(not set)';
  } catch {
    const el = $('#reportsDir');
    if (el) el.textContent = '(unable to detect)';
  }
}

// --- runs table ---
const runsBody = document.getElementById('runsBody');
const detailTitle = document.getElementById('detailTitle');
const detailJson = document.getElementById('detailJson');
const refreshBtn = document.getElementById('refreshBtn');
const closeBtn = document.getElementById('closeDetailBtn');

function renderRows(rows) {
  if (!rows || rows.length === 0) {
    runsBody.innerHTML = `<tr><td colspan="9" class="muted">No reports yet. Run a migration and refresh.</td></tr>`;
    return;
  }
  const html = rows.map(r => {
    const name = r.name || r.id || r; // support simple string items
    const started = fmtTimeAny(r.started_at);
    const duration = r.duration_s ?? '-';
    const read = r.events_read ?? '-';
    const kept = r.events_kept ?? '-';
    const sent = r.events_sent ?? '-';
    const mtu = r.mtu_estimate ?? '-';
    // Allow either top-level estimated_cost_usd or under mtu
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
        <td><button class="btn btn-small" data-name="${name}">View</button></td>
      </tr>`;
  }).join('');
  runsBody.innerHTML = html;
}

async function fetchRuns() {
  runsBody.innerHTML = `<tr><td colspan="9" class="muted">Loading…</td></tr>`;
  try {
    await updateReportsDir();

    const raw = await fetchJSON('/api/migration/runs');
    // Accept both array and {runs: [...]}
    let runs = Array.isArray(raw) ? raw : (raw && raw.runs ? raw.runs : []);
    // Normalize: convert array of names into objects
    runs = runs.map(item => (typeof item === 'string' ? { name: item } : item));
    // Newest first (if name has timestamp-like)
    runs.reverse();
    renderRows(runs);
  } catch (e) {
    console.error(e);
    runsBody.innerHTML = `<tr><td colspan="9" class="muted">Failed to load runs.</td></tr>`;
  }
}

async function openDetail(name) {
  try {
    detailTitle.textContent = `Report: ${name}`;
    detailJson.textContent = 'Loading…';
    // Support either endpoint shape: /api/migration/run/{name} or /api/migration/runs/{name}
    let data, ok = false, lastErr = null;
    for (const path of [`/api/migration/run/${encodeURIComponent(name)}`,
                        `/api/migration/runs/${encodeURIComponent(name)}`]) {
      try {
        data = await fetchJSON(path);
        ok = true;
        break;
      } catch (err) {
        lastErr = err;
      }
    }
    if (!ok) throw lastErr || new Error('No run endpoint available');

    // Summary + samples + raw
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
      lines.push('(no samples captured — set REPORT_SAMPLE_LIMIT in settings/config and rerun)');
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
  } catch (e) {
    console.error(e);
    detailJson.textContent = 'Failed to load run details.';
  }
}

// --- events ---
runsBody.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-name]');
  if (!btn) return;
  openDetail(btn.getAttribute('data-name'));
});

refreshBtn.addEventListener('click', fetchRuns);
closeBtn.addEventListener('click', () => {
  detailTitle.textContent = 'Details';
  detailJson.textContent = 'Select a run to view details';
  detailJson.classList.add('muted');
});

// initial load
fetchRuns();