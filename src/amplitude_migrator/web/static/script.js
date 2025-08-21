const runsBody = document.getElementById('runsBody');
const detailCard = document.getElementById('detailCard');
const detailTitle = document.getElementById('detailTitle');
const detailJson = document.getElementById('detailJson');
const refreshBtn = document.getElementById('refreshBtn');
const closeBtn = document.getElementById('closeDetailBtn');

const fmtUSD = (v) => (v == null ? '-' : `$${Number(v).toFixed(2)}`);
const fmtTime = (t) => (t ? new Date(t * 1000).toLocaleString() : '-');

async function fetchRuns() {
  runsBody.innerHTML = `<tr><td colspan="10" class="muted">Loading…</td></tr>`;
  try {
    const res = await fetch('/api/migration/runs');
    const data = await res.json();
    const runs = data.runs || [];
    if (runs.length === 0) {
      runsBody.innerHTML = `<tr><td colspan="10" class="muted">No reports yet. Run a migration and refresh.</td></tr>`;
      return;
    }
    runsBody.innerHTML = runs.map(r => `
      <tr>
        <td class="mono">${r.id}</td>
        <td>${fmtTime(r.started_at)}</td>
        <td>${r.duration_s ?? '-'}</td>
        <td>${r.events_read ?? '-'}</td>
        <td>${r.events_kept ?? '-'}</td>
        <td>${r.events_sent ?? '-'}</td>
        <td>${r.mtu_estimate ?? '-'}</td>
        <td>${fmtUSD(r.estimated_cost_usd)}</td>
        <td>${r.dry_run ? 'Dry run' : 'Real'}</td>
        <td><button class="btn btn-small" data-id="${r.id}">View</button></td>
      </tr>
    `).join('');
  } catch (e) {
    runsBody.innerHTML = `<tr><td colspan="10" class="error">Failed to load runs.</td></tr>`;
    console.error(e);
  }
}

async function openDetail(id) {
  try {
    const res = await fetch(`/api/migration/runs/${id}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    detailTitle.textContent = `Report: ${id}`;
    detailJson.textContent = JSON.stringify(data, null, 2);
    detailCard.classList.remove('hidden');
    detailCard.scrollIntoView({ behavior: 'smooth' });
  } catch (e) {
    alert('Failed to load run details.');
  }
}

runsBody.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-id]');
  if (btn) openDetail(btn.getAttribute('data-id'));
});
refreshBtn.addEventListener('click', fetchRuns);
closeBtn.addEventListener('click', () => detailCard.classList.add('hidden'));

// initial load
fetchRuns();