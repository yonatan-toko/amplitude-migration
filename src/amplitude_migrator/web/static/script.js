const runsBody = document.getElementById('runsBody');
const detailTitle = document.getElementById('detailTitle');
const detailJson = document.getElementById('detailJson');
const refreshBtn = document.getElementById('refreshBtn');
const closeBtn = document.getElementById('closeDetailBtn');

const fmtUSD = (v) => (v == null ? '-' : `$${Number(v).toFixed(2)}`);

function fmtTimeAny(t) {
  // Accept ISO string or unix seconds
  if (!t && t !== 0) return '-';
  try {
    if (typeof t === 'number') return new Date(t * 1000).toLocaleString();
    return new Date(t).toLocaleString();
  } catch {
    return String(t);
  }
}

async function fetchRuns() {
  runsBody.innerHTML = `<tr><td colspan="9" class="muted">Loading…</td></tr>`;
  try {
    const res = await fetch('/api/migration/runs');
    const data = await res.json();
    const runs = data.runs || [];
    if (runs.length === 0) {
      runsBody.innerHTML = `<tr><td colspan="9" class="muted">No reports yet. Run a migration and refresh.</td></tr>`;
      return;
    }
    // newest last in API; show newest first
    const rows = runs.slice().reverse().map(r => {
      const id = r.name || r.id || '(unknown)';
      const started = fmtTimeAny(r.started_at);
      const duration = r.duration_s ?? '-';
      const read = r.events_read ?? '-';
      const kept = r.events_kept ?? '-';
      const sent = r.events_sent ?? '-';
      const mtu = r.mtu_estimate ?? '-';
      const cost = r.estimated_cost_usd != null ? fmtUSD(r.estimated_cost_usd) : '-';
      const mode = r.dry_run ? 'Dry run' : 'Real';
      return `
        <tr>
          <td class="mono">${id}</td>
          <td>${started}</td>
          <td>${duration}</td>
          <td>${read}</td>
          <td>${kept}</td>
          <td>${sent}</td>
          <td>${mtu}</td>
          <td>${cost}</td>
          <td>${mode}</td>
          <td><button class="btn btn-small" data-name="${id}">View</button></td>
        </tr>`;
    }).join('');
    runsBody.innerHTML = rows;
  } catch (e) {
    runsBody.innerHTML = `<tr><td colspan="9" class="error">Failed to load runs.</td></tr>`;
    console.error(e);
  }
}

function renderDetailText(name, run) {
  const lines = [];
  lines.push(`Report: ${name}`);
  lines.push(`Started: ${fmtTimeAny(run.started_at)} | Ended: ${fmtTimeAny(run.ended_at)} | Duration(s): ${run.duration_s ?? '-'}`);
  const c = run.counters || {};
  lines.push(`Counters → read=${c.events_read ?? 0}, kept=${c.events_kept ?? 0}, sent=${c.events_sent ?? 0}`);
  const mtu = run.mtu || {};
  lines.push(`MTU → users=${mtu.unique_user_ids ?? 0}, devices=${mtu.unique_device_ids ?? 0}, estimate=${mtu.estimate ?? 0}, cost=${mtu.estimated_cost_usd != null ? fmtUSD(mtu.estimated_cost_usd) : '-'}`);
  const rem = run.id_remap || {};
  if (rem && rem.enabled) {
    lines.push(`ID Remap → scope=${rem.scope} policy=${rem.unmapped_policy} user_map=${rem.user_map_path ?? '-'} device_map=${rem.device_map_path ?? '-'}`);
  }
  lines.push('');
  // Samples (actual events with properties)
  const samples = (run.samples && Array.isArray(run.samples.events)) ? run.samples.events : [];
  lines.push(`Samples → Events (count=${samples.length})`);
  if (samples.length === 0) {
    lines.push('(no samples captured — set REPORT_SAMPLE_LIMIT in config.py and rerun)');
  } else {
    const cap = Math.min(samples.length, 20);
    for (let i = 0; i < cap; i++) {
      lines.push(`\n— Sample #${i + 1} —`);
      lines.push(JSON.stringify(samples[i], null, 2));
    }
    if (samples.length > cap) {
      lines.push(`\n… ${samples.length - cap} more not shown`);
    }
  }
  lines.push('\nFull run JSON →');
  lines.push(JSON.stringify(run, null, 2));
  return lines.join('\n');
}

async function openDetail(name) {
  try {
    const res = await fetch(`/api/migration/run/${encodeURIComponent(name)}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    detailTitle.textContent = `Report: ${name}`;
    detailJson.textContent = renderDetailText(name, data);
    // No detailCard container in current HTML; we simply fill the pre block.
  } catch (e) {
    alert('Failed to load run details.');
    console.error(e);
  }
}

runsBody.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-name]');
  if (btn) openDetail(btn.getAttribute('data-name'));
});
refreshBtn.addEventListener('click', fetchRuns);
closeBtn.addEventListener('click', () => {
  detailTitle.textContent = 'Details';
  detailJson.textContent = 'Select a run to view details';
});

// initial load
fetchRuns();