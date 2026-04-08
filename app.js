const tableBody = document.getElementById('tableBody');
const countLabel = document.getElementById('countLabel');
const searchInput = document.getElementById('searchInput');
const statusFilter = document.getElementById('statusFilter');
const resetBtn = document.getElementById('resetBtn');

const REFRESH_INTERVAL_MS = 15000;
const WS_RECONNECT_MS = 5000;

let rows = [];
let lastFingerprint = '';
let lastSyncAt = null;
let ws = null;
let wsActive = false;

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function render(data) {
  const synced = lastSyncAt
    ? ` · обновлено ${lastSyncAt.toLocaleTimeString('ru-RU')}`
    : '';
  countLabel.textContent = `Показано: ${data.length} из ${rows.length}${synced}`;

  if (!data.length) {
    tableBody.innerHTML = '<tr><td colspan="4">Нет данных по текущему фильтру.</td></tr>';
    return;
  }

  tableBody.innerHTML = data.map((r) => `
    <tr>
      <td>${escapeHtml(r.number || '')}</td>
      <td>${escapeHtml(r.createdAt || '')}</td>
      <td><span class="tag">${escapeHtml(r.status || '')}</span></td>
      <td>${escapeHtml(r.additionalInfoFirstLine || '')}</td>
    </tr>
  `).join('');
}

function applyFilters() {
  const q = searchInput.value.trim().toLowerCase();
  const status = statusFilter.value;

  const filtered = rows.filter((r) => {
    const byStatus = !status || (r.status || '') === status;
    const text = `${r.number || ''} ${r.status || ''} ${r.additionalInfoFirstLine || ''}`.toLowerCase();
    const byText = !q || text.includes(q);
    return byStatus && byText;
  });

  render(filtered);
}

function fillStatuses(data) {
  const selectedStatus = statusFilter.value;
  statusFilter.innerHTML = '<option value="">Все</option>';
  const statuses = [...new Set(data.map((r) => r.status).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'ru'));
  for (const s of statuses) {
    const option = document.createElement('option');
    option.value = s;
    option.textContent = s;
    statusFilter.appendChild(option);
  }

  if ([...statusFilter.options].some((o) => o.value === selectedStatus)) {
    statusFilter.value = selectedStatus;
  }
}

async function loadRows() {
  const response = await fetch('kp_2026_march_april.json', { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  const data = await response.json();
  data.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  return data;
}

function fingerprint(data) {
  return JSON.stringify(data);
}

function setRows(nextRows, syncedAt = null) {
  const nextFingerprint = fingerprint(nextRows);
  if (nextFingerprint !== lastFingerprint) {
    rows = nextRows;
    lastFingerprint = nextFingerprint;
    fillStatuses(rows);
  }

  lastSyncAt = syncedAt || new Date();
  applyFilters();
}

async function refreshData(initial = false) {
  try {
    const nextRows = await loadRows();
    setRows(nextRows, new Date());
  } catch (err) {
    if (initial) {
      countLabel.textContent = 'Ошибка загрузки данных';
      tableBody.innerHTML = `<tr><td colspan="4">Не удалось загрузить данные: ${escapeHtml(err.message)}</td></tr>`;
    }
  }
}

function connectWebSocket() {
  if (!window.location.origin.startsWith('http')) {
    return;
  }

  const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
  const url = `${scheme}://${window.location.host}/ws/kp`;

  ws = new WebSocket(url);

  ws.onopen = () => {
    wsActive = true;
  };

  ws.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);
      if (payload.type === 'rows' && Array.isArray(payload.rows)) {
        const sorted = payload.rows.slice().sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
        setRows(sorted, payload.updatedAt ? new Date(payload.updatedAt) : new Date());
      }
    } catch {
      // Ignore malformed WS frames.
    }
  };

  ws.onclose = () => {
    wsActive = false;
    setTimeout(connectWebSocket, WS_RECONNECT_MS);
  };

  ws.onerror = () => {
    wsActive = false;
  };
}

async function init() {
  await refreshData(true);
  connectWebSocket();
  setInterval(() => {
    if (!wsActive) {
      refreshData(false);
    }
  }, REFRESH_INTERVAL_MS);
}

searchInput.addEventListener('input', applyFilters);
statusFilter.addEventListener('change', applyFilters);
resetBtn.addEventListener('click', () => {
  searchInput.value = '';
  statusFilter.value = '';
  applyFilters();
});

init();
