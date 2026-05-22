'use strict';

const API = '';  // same origin
const LIMIT = 50;

let state = {
  batchId: '',
  status: 'all',
  q: '',
  sort: 'stage_b_asc',   // default to Stage B ranking so likely positives surface first
  page: 1,
  total: 0,
  pages: 1,
  items: [],
  selectedIdx: -1,
};

// ── DOM refs ──────────────────────────────────────────────────────────────
const tbody       = document.getElementById('tbody');
const emptyEl     = document.getElementById('empty');
const resultCount = document.getElementById('result-count');
const pgInfo      = document.getElementById('pg-info');
const pgPrev      = document.getElementById('pg-prev');
const pgNext      = document.getElementById('pg-next');
const batchSel    = document.getElementById('batch-select');
const sortSel     = document.getElementById('sort-select');
const searchEl    = document.getElementById('search');
const toast       = document.getElementById('toast');
const statTotal   = document.getElementById('stat-total');
const statUnrev   = document.getElementById('stat-unreviewed');
const statPri     = document.getElementById('stat-prioritized');
const statExcl    = document.getElementById('stat-excluded');

// ── API ───────────────────────────────────────────────────────────────────
async function fetchCandidates() {
  const params = new URLSearchParams({
    status: state.status,
    page: state.page,
    limit: LIMIT,
    sort: state.sort,
  });
  if (state.batchId) params.set('batch_id', state.batchId);
  if (state.q)       params.set('q', state.q);
  const res = await fetch(`${API}/api/candidates?${params}`);
  return res.json();
}

async function fetchStats() {
  const res = await fetch(`${API}/api/stats`);
  return res.json();
}

async function postTriage(candidateId, action) {
  const res = await fetch(`${API}/api/triage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ candidate_id: candidateId, action }),
  });
  if (!res.ok) throw new Error(`triage failed: ${res.status}`);
  return res.json();
}

// ── render ────────────────────────────────────────────────────────────────
function renderStats(s) {
  statTotal.textContent  = s.total;
  statUnrev.textContent  = s.unreviewed;
  statPri.textContent    = s.prioritized;
  statExcl.textContent   = s.excluded;
}

function renderBatchOptions(batchIds) {
  // preserve current selection
  const cur = batchSel.value;
  while (batchSel.options.length > 1) batchSel.remove(1);
  for (const bid of batchIds) {
    const opt = document.createElement('option');
    opt.value = bid;
    opt.textContent = bid.slice(0, 8) + '…';
    opt.title = bid;
    batchSel.appendChild(opt);
  }
  if (cur) batchSel.value = cur;
}

function triagebadge(triage) {
  if (triage === 'excluded')    return `<span class="triage-badge excluded">● הוחרג</span>`;
  if (triage === 'prioritized') return `<span class="triage-badge prioritized">● עדיפות</span>`;
  return `<span class="triage-badge unreviewed">○ לא סוקר</span>`;
}

function formatDate(iso) {
  if (!iso) return '';
  try { return iso.slice(0, 10); } catch { return iso; }
}

function stageBChip(score) {
  if (score == null) return '<td></td>';
  const pct = score.toFixed(4);
  const cls = score < 0.95  ? 'tier1'
            : score < 0.99  ? 'tier2'
            : score < 0.9957 ? 'tier3'
            : 'tier4';
  return `<td><span class="sb-score ${cls}" title="Stage B p_negative: ${score.toFixed(6)}">${pct}</span></td>`;
}

function renderRows() {
  const { items, selectedIdx } = state;
  if (!items.length) {
    tbody.innerHTML = '';
    emptyEl.style.display = '';
    return;
  }
  emptyEl.style.display = 'none';

  const rows = items.map((c, i) => {
    const sel = i === selectedIdx ? 'selected' : '';
    const triageCls = c.triage ? c.triage : '';
    const badge = triagebadge(c.triage);
    const shortUrl = c.url.replace(/^https?:\/\//, '').slice(0, 60);
    const title = escHtml(c.title || c.url);
    const snippet = escHtml(c.snippet || '');
    const actionBtns = c.triage === 'excluded'
      ? `<button class="btn btn-prioritize" data-id="${c.candidate_id}" data-action="prioritize">עדיפות</button>
         <button class="btn btn-reset"      data-id="${c.candidate_id}" data-action="reset">אפס</button>`
      : c.triage === 'prioritized'
      ? `<button class="btn btn-exclude" data-id="${c.candidate_id}" data-action="exclude">החרג</button>
         <button class="btn btn-reset"   data-id="${c.candidate_id}" data-action="reset">אפס</button>`
      : `<button class="btn btn-exclude"    data-id="${c.candidate_id}" data-action="exclude">החרג</button>
         <button class="btn btn-prioritize" data-id="${c.candidate_id}" data-action="prioritize">עדיפות</button>`;

    return `<tr class="candidate ${triageCls} ${sel}" data-idx="${i}" data-id="${c.candidate_id}">
      <td>
        <div class="domain">${escHtml(c.domain)}</div>
        <a class="url-link" href="${escHtml(c.url)}" target="_blank" rel="noopener">${escHtml(shortUrl)}</a>
      </td>
      <td><div class="title">${title}</div></td>
      <td><div class="snippet">${snippet}</div></td>
      <td><div class="date">${formatDate(c.first_seen_at)}</div></td>
      ${stageBChip(c.stage_b_score)}
      <td class="actions" style="text-align:left">
        ${badge}
        <div style="display:flex;gap:6px;margin-top:4px">${actionBtns}</div>
      </td>
    </tr>`;
  });

  tbody.innerHTML = rows.join('');
}

function renderPagination() {
  pgPrev.disabled = state.page <= 1;
  pgNext.disabled = state.page >= state.pages;
  pgInfo.textContent = `עמוד ${state.page} מתוך ${state.pages}`;
  resultCount.textContent = `${state.total} תוצאות`;
}

function escHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── load ──────────────────────────────────────────────────────────────────
async function load(resetPage = false) {
  if (resetPage) { state.page = 1; state.selectedIdx = -1; }
  const [data, stats] = await Promise.all([fetchCandidates(), fetchStats()]);
  state.total = data.total;
  state.pages = data.pages;
  state.items = data.items;
  renderStats(stats);
  renderBatchOptions(stats.batch_ids);
  renderRows();
  renderPagination();
}

// ── triage action ─────────────────────────────────────────────────────────
async function triage(candidateId, action) {
  let result;
  try {
    result = await postTriage(candidateId, action);
  } catch (err) {
    showToast(`שגיאה: ${err.message}`);
    return;
  }
  if (result.stats) renderStats(result.stats);

  // update local item only after server confirmed the decision
  const item = state.items.find(c => c.candidate_id === candidateId);
  if (item) {
    if (action === 'exclude')    { item.triage = 'excluded';    item.candidate_status = 'suppressed'; }
    if (action === 'prioritize') { item.triage = 'prioritized'; item.retry_priority = 100; }
    if (action === 'reset')      { item.triage = '';            item.candidate_status = 'new'; item.retry_priority = 0; }
  }
  renderRows();

  const labels = { exclude: 'הוחרג', prioritize: 'עדיפות', reset: 'אופס' };
  showToast(labels[action] || action);
}

// ── toast ─────────────────────────────────────────────────────────────────
let _toastTimer;
function showToast(msg) {
  toast.textContent = msg;
  toast.classList.add('show');
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => toast.classList.remove('show'), 1200);
}

// ── selection ─────────────────────────────────────────────────────────────
function selectRow(idx) {
  if (idx < 0 || idx >= state.items.length) return;
  state.selectedIdx = idx;
  renderRows();
  const row = tbody.querySelector(`tr[data-idx="${idx}"]`);
  if (row) row.scrollIntoView({ block: 'nearest' });
}

// ── keyboard ──────────────────────────────────────────────────────────────
document.addEventListener('keydown', async e => {
  // don't intercept when typing in inputs
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT') {
    if (e.key === 'Escape') { e.target.blur(); searchEl.value = ''; state.q = ''; load(true); }
    return;
  }

  const { selectedIdx, items } = state;
  const sel = items[selectedIdx];

  switch (e.key) {
    case 'j': case 'ArrowDown':
      e.preventDefault(); selectRow(Math.min(selectedIdx + 1, items.length - 1)); break;
    case 'k': case 'ArrowUp':
      e.preventDefault(); selectRow(Math.max(selectedIdx - 1, 0)); break;
    case 'e': if (sel) { e.preventDefault(); await triage(sel.candidate_id, 'exclude'); } break;
    case 'p': if (sel) { e.preventDefault(); await triage(sel.candidate_id, 'prioritize'); } break;
    case 'r': if (sel) { e.preventDefault(); await triage(sel.candidate_id, 'reset'); } break;
    case '/': e.preventDefault(); searchEl.focus(); break;
    case 'ArrowLeft':  e.preventDefault(); if (state.page < state.pages)  { state.page++; load(); } break;
    case 'ArrowRight': e.preventDefault(); if (state.page > 1) { state.page--; load(); } break;
  }
});

// ── event wiring ──────────────────────────────────────────────────────────
tbody.addEventListener('click', async e => {
  const btn = e.target.closest('[data-action]');
  if (btn) {
    e.stopPropagation();
    await triage(btn.dataset.id, btn.dataset.action);
    return;
  }
  const row = e.target.closest('tr[data-idx]');
  if (row) selectRow(Number(row.dataset.idx));
});

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    state.status = tab.dataset.status;
    load(true);
  });
});

batchSel.addEventListener('change', () => {
  state.batchId = batchSel.value;
  load(true);
});

sortSel.addEventListener('change', () => {
  state.sort = sortSel.value;
  load(true);
});

let _searchTimer;
searchEl.addEventListener('input', () => {
  clearTimeout(_searchTimer);
  _searchTimer = setTimeout(() => { state.q = searchEl.value.trim(); load(true); }, 250);
});

pgPrev.addEventListener('click', () => { if (state.page > 1) { state.page--; load(); } });
pgNext.addEventListener('click', () => { if (state.page < state.pages) { state.page++; load(); } });

// ── init ──────────────────────────────────────────────────────────────────
(async () => {
  // Sync dropdown to match the default state.sort value
  sortSel.value = state.sort;
  await load(true);
  // Default: select new-batch filter if a batch exists and has candidates
  const stats = await fetchStats();
  if (stats.batch_ids.length > 0) {
    // pre-select the most recent batch (last in list = most recently added)
    const latestBatch = stats.batch_ids[stats.batch_ids.length - 1];
    batchSel.value = latestBatch;
    state.batchId = latestBatch;
    await load(true);
  }
  if (state.items.length > 0) selectRow(0);
})();
