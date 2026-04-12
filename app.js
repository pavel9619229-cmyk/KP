const tableBody = document.getElementById('tableBody');
const countLabel = document.getElementById('countLabel');
const searchInput = document.getElementById('searchInput');
const statusFilter = document.getElementById('statusFilter');
const resetBtn = document.getElementById('resetBtn');
const darkBtn = document.getElementById('darkBtn');

(function initDark() {
  if (localStorage.getItem('darkMode') === '1') {
    document.body.classList.add('dark');
    darkBtn.textContent = '☀️ Светлый фон';
  }
})();

darkBtn.addEventListener('click', () => {
  const isDark = document.body.classList.toggle('dark');
  darkBtn.textContent = isDark ? '☀️ Светлый фон' : '🌙 Тёмный фон';
  localStorage.setItem('darkMode', isDark ? '1' : '0');
});

const REFRESH_INTERVAL_MS = 15000;
const WS_RECONNECT_MS = 5000;

let rows = [];
let lastFingerprint = '';
let lastSyncAt = null;
let ws = null;
let wsActive = false;
const TABLE_COLUMN_COUNT = 15;

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function normalizeFlag(value) {
  if (value === true || value === false) {
    return value;
  }

  if (typeof value === 'number') {
    if (value === 1) return true;
    if (value === 0) return false;
    return null;
  }

  if (typeof value === 'string') {
    const v = value.trim().toLowerCase();
    if (!v) return null;
    if (['true', '1', 'yes', 'y', 'да', 'заполнен'].includes(v)) return true;
    if (['false', '0', 'no', 'n', 'нет', 'не заполнен'].includes(v)) return false;
  }

  return null;
}

function getFlag(row, keys, fallback = null) {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(row, key)) {
      const flag = normalizeFlag(row[key]);
      if (flag !== null) {
        return flag;
      }
    }
  }

  if (typeof fallback === 'function') {
    return fallback(row);
  }

  return null;
}

function formatFlag(flag) {
  if (flag === true) return 'Да';
  if (flag === false) return 'Нет';
  return '—';
}

function render(data) {
  const synced = lastSyncAt
    ? ` · обновлено ${lastSyncAt.toLocaleTimeString('ru-RU')}`
    : '';
  countLabel.textContent = `Показано: ${data.length} из ${rows.length}${synced}`;

  if (!data.length) {
    tableBody.innerHTML = `<tr><td colspan="${TABLE_COLUMN_COUNT}">Нет данных по текущему фильтру.</td></tr>`;
    return;
  }

  tableBody.innerHTML = data.map((r) => `
    <tr>
      <td>${escapeHtml(r.number || '')}</td>
      <td>${escapeHtml(r.createdAt || '')}</td>
      <td>${escapeHtml(r.customerName || '')}</td>
      <td>${formatFlag(getFlag(r, ['clientFilled', 'isClientFilled', 'клиентЗаполнен'], (row) => {
        const name = String(row.customerName || '').trim();
        if (!name) return false;
        const normalized = name.toLowerCase().replaceAll('ё', 'е');
        return normalized !== 'не определен' && normalized !== 'неопределен';
      }))}</td>
      <td>${formatFlag(getFlag(r, ['managerFilled', 'isManagerFilled', 'менеджерЗаполнен']))}</td>
      <td>${formatFlag(getFlag(r, ['productSpecified', 'isProductSpecified', 'товарУказан']))}</td>
      <td>${formatFlag(getFlag(r, ['kpSent', 'isKpSent', 'кпОтправлено']))}</td>
      <td>${formatFlag(getFlag(r, ['receiptConfirmed', 'isReceiptConfirmed', 'получениеПодтверждено']))}</td>
      <td>${formatFlag(getFlag(r, ['paymentReceived', 'isPaymentReceived', 'оплатаПолучена']))}</td>
      <td>${formatFlag(getFlag(r, ['invoiceCreated', 'isInvoiceCreated', 'накладнаяСоздана']))}</td>
      <td>${formatFlag(getFlag(r, ['edoSent', 'isEdoSent', 'вЭдоОтправлено']))}</td>
      <td>${formatFlag(getFlag(r, ['rejected', 'isRejected', 'отказ']))}</td>
      <td>${formatFlag(getFlag(r, ['problem', 'hasProblem', 'проблема']))}</td>
      <td><span class="tag">${escapeHtml(r.statusKp || '')}</span></td>
      <td>${escapeHtml(r.additionalInfoFirstLine || '')}</td>
    </tr>
  `).join('');
}

function applyFilters() {
  const q = searchInput.value.trim().toLowerCase();
  const status = statusFilter.value;

  const filtered = rows.filter((r) => {
    const byStatus = !status || (r.status || '') === status;
    const text = `${r.number || ''} ${r.customerName || ''} ${r.status || ''} ${r.statusKp || ''} ${r.additionalInfoFirstLine || ''}`.toLowerCase();
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
  const sources = [
    '/api/kp/all',
    'https://onec-kp-realtime.onrender.com/api/kp/all',
    'kp_2026_march_april.json',
  ];

  let response = null;
  let lastError = null;
  for (const src of sources) {
    try {
      const r = await fetch(src, { cache: 'no-store' });
      if (!r.ok) {
        throw new Error(`HTTP ${r.status}`);
      }
      response = r;
      break;
    } catch (e) {
      lastError = e;
    }
  }

  if (!response) {
    throw lastError || new Error('Нет доступного источника данных');
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
      tableBody.innerHTML = `<tr><td colspan="${TABLE_COLUMN_COUNT}">Не удалось загрузить данные: ${escapeHtml(err.message)}</td></tr>`;
    }
  }
}

function connectWebSocket() {
  if (!window.location.origin.startsWith('http')) {
    return;
  }

  const isLocalStatic = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
  const url = isLocalStatic
    ? 'wss://onec-kp-realtime.onrender.com/ws/kp'
    : `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/kp`;

  ws = new WebSocket(url);

  ws.onopen = () => {
    wsActive = true;
  };

  ws.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);
      if (payload.type === 'rows' && Array.isArray(payload.rows)) {
        const sorted = payload.rows.slice().sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
        setRows(sorted, new Date());
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
