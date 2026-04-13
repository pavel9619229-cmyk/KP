const searchInput = document.getElementById('searchInput');
const clearSearchBtn = document.getElementById('clearSearchBtn');
const newRequestBtn = document.getElementById('newRequestBtn');
const newRequestPanel = document.getElementById('newRequestPanel');
const requestTextInput = document.getElementById('requestTextInput');
const submitRequestBtn = document.getElementById('submitRequestBtn');
const requestStatusMsg = document.getElementById('requestStatusMsg');
const themeBtn = document.getElementById('themeBtn');
const statusTabs = document.getElementById('statusTabs');
const updatedAtLabel = document.getElementById('updatedAtLabel');
const boardContent = document.getElementById('boardContent');

const REFRESH_INTERVAL_MS = 15000;
const WS_RECONNECT_MS = 5000;
const THEME_STORAGE_KEY = 'kpDashboardThemeV1';
const ALL_TAB_KEY = '__all__';
const STATUS_ORDER = [
  'ПРОБЛЕМА',
  'ОТКАЗ',
  'ЖДЕМ ОПЛАТУ',
  'ОТПРАВИТЬ В ЭДО',
  'ОТГРУЗИТЬ',
  'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП',
  'КЛИЕНТ ДУМАЕТ',
  'ОТПРАВИТЬ КЛИЕНТУ',
  'ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО',
  'ОБРАБОТАТЬ',
];

const STATUS_LABELS_COMPACT = {
  '__all__': 'Все',
  'ПРОБЛЕМА': 'Пробл.',
  'ОТКАЗ': 'Отказ',
  'ЖДЕМ ОПЛАТУ': 'Оплата',
  'ОТПРАВИТЬ В ЭДО': 'ЭДО',
  'ОТГРУЗИТЬ': 'Отгр.',
  'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП': 'Проверка',
  'КЛИЕНТ ДУМАЕТ': 'Думает',
  'ОТПРАВИТЬ КЛИЕНТУ': 'Клиенту',
  'ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО': 'Готово',
  'ОБРАБОТАТЬ': 'В работу',
};

let rows = [];
let ws = null;
let wsActive = false;
let activeTab = ALL_TAB_KEY;
let lastFingerprint = '';
let lastSyncAt = null;

initTheme();

themeBtn.addEventListener('click', () => {
  const isLight = document.body.classList.toggle('light');
  localStorage.setItem(THEME_STORAGE_KEY, isLight ? 'light' : 'dark');
  themeBtn.textContent = isLight ? 'Тёмная тема' : 'Светлая тема';
});

searchInput.addEventListener('input', () => {
  updateClearSearchButton();
  renderBoard();
});

clearSearchBtn.addEventListener('click', () => {
  searchInput.value = '';
  updateClearSearchButton();
  renderBoard();
  searchInput.focus();
});
newRequestBtn.addEventListener('click', () => {
  const isHidden = newRequestPanel.hidden;
  newRequestPanel.hidden = !isHidden;
  if (isHidden) {
    requestTextInput.focus();
    newRequestBtn.textContent = 'СКРЫТЬ ФОРМУ';
  } else {
    newRequestBtn.textContent = 'НОВЫЙ ЗАПРОС';
  }
});

submitRequestBtn.addEventListener('click', async () => {
  const requestText = String(requestTextInput.value || '').trim();
  if (!requestText) {
    requestStatusMsg.className = 'request-panel__status is-error';
    requestStatusMsg.textContent = 'Введите текст запроса.';
    return;
  }

  submitRequestBtn.disabled = true;
  submitRequestBtn.textContent = 'ОТПРАВКА...';
  requestStatusMsg.className = 'request-panel__status';
  requestStatusMsg.textContent = 'Создаю КП в 1С...';

  try {
    const response = await fetch('/api/kp/new-request', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ requestText }),
    });

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const details = payload?.detail || payload?.error || `HTTP ${response.status}`;
      throw new Error(String(details));
    }

    const number = payload?.number ? ` № ${payload.number}` : '';
    const customer = payload?.resolvedCustomerName ? ` · Клиент: ${payload.resolvedCustomerName}` : '';
    const statusHint = payload?.statusMarkedInComment === true
      ? ' · Статус запроса аварийно отмечен в комментарии'
      : '';
    requestStatusMsg.className = 'request-panel__status is-success';
    requestStatusMsg.textContent = `КП успешно создано${number}${customer}${statusHint}.`;
    requestTextInput.value = '';
    await refreshData(false);
  } catch (error) {
    requestStatusMsg.className = 'request-panel__status is-error';
    requestStatusMsg.textContent = `Ошибка создания КП: ${error.message}`;
  } finally {
    submitRequestBtn.disabled = false;
    submitRequestBtn.textContent = 'ОТПРАВИТЬ';
  }
});

statusTabs.addEventListener('click', (event) => {
  const button = event.target.closest('[data-status-key]');
  if (!(button instanceof HTMLButtonElement)) {
    return;
  }
  activeTab = button.dataset.statusKey || ALL_TAB_KEY;
  renderBoard();
});

function initTheme() {
  const savedTheme = localStorage.getItem(THEME_STORAGE_KEY);
  const isLight = savedTheme === 'light';
  document.body.classList.toggle('light', isLight);
  themeBtn.textContent = isLight ? 'Тёмная тема' : 'Светлая тема';
}

function updateClearSearchButton() {
  clearSearchBtn.hidden = !String(searchInput.value || '').trim();
}

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function parseKpNumber(value) {
  const raw = String(value || '').trim();
  if (!raw) return Number.NEGATIVE_INFINITY;
  const digits = raw.replace(/\D+/g, '');
  if (!digits) return Number.NEGATIVE_INFINITY;
  const parsed = Number.parseInt(digits, 10);
  return Number.isFinite(parsed) ? parsed : Number.NEGATIVE_INFINITY;
}

function sortRowsByKpNumberDesc(data) {
  return data.sort((a, b) => {
    const byNumber = parseKpNumber(b?.number) - parseKpNumber(a?.number);
    if (byNumber !== 0) return byNumber;
    return new Date(b?.createdAt || 0) - new Date(a?.createdAt || 0);
  });
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
    const normalized = value.trim().toLowerCase();
    if (!normalized) return null;
    if (['true', '1', 'yes', 'y', 'да', 'заполнен'].includes(normalized)) return true;
    if (['false', '0', 'no', 'n', 'нет', 'не заполнен'].includes(normalized)) return false;
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

function hasRejectInComment(row) {
  const commentText = [
    row?.additionalInfoFirstLine,
    row?.comment,
    row?.Комментарий,
  ]
    .filter(Boolean)
    .map((value) => String(value).toUpperCase())
    .join(' ');
  return commentText.includes('ОТКАЗ');
}

function computeKpStatus(row) {
  const problem = getFlag(row, ['problem', 'hasProblem', 'проблема']);
  if (problem === true) return 'ПРОБЛЕМА';

  if (hasRejectInComment(row)) return 'ОТКАЗ';

  const rejected = getFlag(row, ['rejected', 'isRejected', 'отказ']);
  if (rejected === true) return 'ОТКАЗ';

  const invoiceCreated = getFlag(row, ['invoiceCreated', 'isInvoiceCreated', 'накладнаяСоздана']);
  const paymentReceived = getFlag(row, ['paymentReceived', 'isPaymentReceived', 'оплатаПолучена']);
  const edoSent = getFlag(row, ['edoSent', 'isEdoSent', 'вЭдоОтправлено']);
  if (invoiceCreated === true && paymentReceived === true && edoSent === true) return 'ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО';
  if (invoiceCreated === true && edoSent === true && paymentReceived !== true) return 'ЖДЕМ ОПЛАТУ';
  if (invoiceCreated === true && edoSent !== true) return 'ОТПРАВИТЬ В ЭДО';

  const shipmentPending = getFlag(row, ['shipmentPending', 'isShipmentPending', 'отгрузить']);
  if (shipmentPending === true) return 'ОТГРУЗИТЬ';

  const receiptConfirmed = getFlag(row, ['receiptConfirmed', 'isReceiptConfirmed', 'получениеПодтверждено']);
  if (receiptConfirmed === true) return 'КЛИЕНТ ДУМАЕТ';

  const kpSent = getFlag(row, ['kpSent', 'isKpSent', 'кпОтправлено']);
  if (kpSent === true) return 'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП';

  const clientFilled = getFlag(row, ['clientFilled', 'isClientFilled', 'клиентЗаполнен'], (currentRow) => {
    const name = String(currentRow.customerName || '').trim();
    if (!name) return false;
    const normalized = name.toLowerCase().replaceAll('ё', 'е');
    return normalized !== 'не определен' && normalized !== 'неопределен';
  });
  const managerFilled = getFlag(row, ['managerFilled', 'isManagerFilled', 'менеджерЗаполнен'], (currentRow) => {
    const manager = String(currentRow.managerName || currentRow.manager || currentRow['Менеджер'] || '').trim();
    if (!manager) return null;
    const normalized = manager.toLowerCase().replaceAll('ё', 'е');
    return normalized !== 'не определен' && normalized !== 'неопределен';
  });
  const productSpecified = getFlag(row, ['productSpecified', 'isProductSpecified', 'товарУказан']);

  if (clientFilled === true && managerFilled === true && productSpecified === true) return 'ОТПРАВИТЬ КЛИЕНТУ';
  return 'ОБРАБОТАТЬ';
}

function getStatusCounts(data) {
  const counts = new Map();
  for (const row of data) {
    const status = computeKpStatus(row);
    counts.set(status, (counts.get(status) || 0) + 1);
  }
  return counts;
}

function getOrderedStatuses(counts) {
  const dynamicStatuses = [...counts.keys()].filter((status) => !STATUS_ORDER.includes(status)).sort((a, b) => a.localeCompare(b, 'ru'));
  return STATUS_ORDER.filter((status) => counts.has(status)).concat(dynamicStatuses);
}

function getTabLabel(statusKey, fallbackLabel) {
  const isCompactViewport = window.matchMedia('(max-width: 720px)').matches;
  if (!isCompactViewport) {
    return fallbackLabel;
  }
  return STATUS_LABELS_COMPACT[statusKey] ? STATUS_LABELS_COMPACT[statusKey] : fallbackLabel;
}

function formatUpdatedAt(value) {
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
    return 'Нет данных';
  }
  return value.toLocaleString('ru-RU', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function buildMetaChips(row) {
  const chips = [];

  chips.push({ label: `Клиент: ${getFlag(row, ['clientFilled']) === true ? 'да' : 'нет'}`, state: getFlag(row, ['clientFilled']) === true ? 'is-true' : 'is-false' });
  chips.push({ label: `Менеджер: ${getFlag(row, ['managerFilled']) === true ? 'да' : 'нет'}`, state: getFlag(row, ['managerFilled']) === true ? 'is-true' : 'is-false' });
  chips.push({ label: `Товар: ${getFlag(row, ['productSpecified']) === true ? 'указан' : 'не указан'}`, state: getFlag(row, ['productSpecified']) === true ? 'is-true' : 'is-false' });

  if (getFlag(row, ['kpSent']) === true) {
    chips.push({ label: 'КП отправлено', state: 'is-true' });
  }
  if (getFlag(row, ['receiptConfirmed']) === true) {
    chips.push({ label: 'Получение подтверждено', state: 'is-true' });
  }
  if (getFlag(row, ['invoiceCreated']) === true) {
    chips.push({ label: 'Накладная создана', state: 'is-true' });
  }
  if (getFlag(row, ['paymentReceived']) === true) {
    chips.push({ label: 'Оплата получена', state: 'is-true' });
  }
  if (getFlag(row, ['edoSent']) === true) {
    chips.push({ label: 'Отправлено в ЭДО', state: 'is-true' });
  }
  if (getFlag(row, ['problem']) === true) {
    chips.push({ label: 'Есть проблема', state: 'is-alert' });
  }
  if (getFlag(row, ['rejected']) === true) {
    chips.push({ label: 'Отказ', state: 'is-alert' });
  }

  return chips;
}

function renderTabs(counts) {
  const orderedStatuses = getOrderedStatuses(counts);
  const pinnedOrder = ['ОТКАЗ', 'ОБРАБОТАТЬ', 'ОТПРАВИТЬ КЛИЕНТУ', 'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП'];

  const tabs = [{ key: ALL_TAB_KEY, label: 'ALL', count: rows.length }];
  for (const status of pinnedOrder) {
    if (counts.has(status)) {
      tabs.push({ key: status, label: status, count: counts.get(status) || 0 });
    }
  }

  for (const status of orderedStatuses) {
    if (!pinnedOrder.includes(status)) {
      tabs.push({ key: status, label: status, count: counts.get(status) || 0 });
    }
  }

  statusTabs.innerHTML = tabs.map((tab) => `
    <button class="status-tab ${getTabRowClass(tab.key)} ${tab.key === activeTab ? 'is-active' : ''}" data-status-key="${escapeHtml(tab.key)}" type="button">
      <span class="status-tab__label">${escapeHtml(tab.label)}</span>
      <span class="status-tab__count">${tab.count}</span>
    </button>
  `).join('');
}

function getTabRowClass(tabKey) {
  if (tabKey === ALL_TAB_KEY || tabKey === 'ОТКАЗ') {
    return 'status-tab--top-pair';
  }
  if (tabKey === 'ОТПРАВИТЬ КЛИЕНТУ' || tabKey === 'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП') {
    return 'status-tab--second-pair';
  }
  return 'status-tab--full';
}

function renderBoard() {
  const counts = getStatusCounts(rows);
  const query = searchInput.value.trim().toLowerCase();
  const filtered = rows.filter((row) => {
    const status = computeKpStatus(row);
    const matchesTab = activeTab === ALL_TAB_KEY || status === activeTab;
    if (!matchesTab) {
      return false;
    }

    const haystack = `${row.number || ''} ${row.customerName || ''} ${row.additionalInfoFirstLine || ''} ${status}`.toLowerCase();
    return !query || haystack.includes(query);
  });

  if (activeTab !== ALL_TAB_KEY && !counts.has(activeTab)) {
    activeTab = ALL_TAB_KEY;
  }

  renderTabs(counts);

  updatedAtLabel.textContent = formatUpdatedAt(lastSyncAt);

  if (!filtered.length) {
    boardContent.innerHTML = '<div class="board-empty">По текущему фильтру подходящих КП нет.</div>';
    return;
  }

  boardContent.innerHTML = filtered.map((row) => {
    const status = computeKpStatus(row);
    return `
      <article class="kp-card">
        <div class="kp-card__row">
          <div class="kp-cell kp-cell--number">
            <span class="kp-cell__value kp-card__number">${escapeHtml(row.number || '—')}</span>
          </div>
          <div class="kp-cell kp-cell--date">
            <span class="kp-cell__value kp-card__date">${escapeHtml(row.createdAt || '')}</span>
          </div>
          <div class="kp-cell kp-cell--customer">
            <span class="kp-cell__value kp-card__customer">${escapeHtml(row.customerName || 'Клиент не указан')}</span>
          </div>
          <div class="kp-cell kp-cell--status">
            <span class="kp-cell__value kp-card__status">${escapeHtml(status)}</span>
          </div>
          <div class="kp-cell kp-cell--note">
            <span class="kp-cell__value kp-card__note">${escapeHtml(row.additionalInfoFirstLine || 'Без дополнительной информации')}</span>
          </div>
        </div>
      </article>
    `;
  }).join('');
}

async function loadRows() {
  const sources = [
    '/api/kp/all',
    'https://onec-kp-realtime.onrender.com/api/kp/all',
  ];

  let response = null;
  let lastError = null;
  for (const src of sources) {
    try {
      const nextResponse = await fetch(src, { cache: 'no-store' });
      if (!nextResponse.ok) {
        throw new Error(`HTTP ${nextResponse.status}`);
      }
      response = nextResponse;
      break;
    } catch (error) {
      lastError = error;
    }
  }

  if (!response) {
    throw lastError || new Error('Нет доступного источника данных');
  }

  const data = await response.json();
  return sortRowsByKpNumberDesc(data);
}

function fingerprint(data) {
  return JSON.stringify(data);
}

function setRows(nextRows, syncedAt = null) {
  const nextFingerprint = fingerprint(nextRows);
  if (nextFingerprint === lastFingerprint) {
    lastSyncAt = syncedAt || new Date();
    updatedAtLabel.textContent = formatUpdatedAt(lastSyncAt);
    return;
  }

  rows = nextRows;
  lastFingerprint = nextFingerprint;
  lastSyncAt = syncedAt || new Date();
  renderBoard();
}

async function refreshData(initial = false) {
  try {
    const nextRows = await loadRows();
    setRows(nextRows, new Date());
  } catch (error) {
    if (initial) {
      boardContent.innerHTML = `<div class="board-empty">Не удалось загрузить данные: ${escapeHtml(error.message)}</div>`;
      updatedAtLabel.textContent = 'Ошибка';
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
        const sorted = sortRowsByKpNumberDesc(payload.rows.slice());
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
  updateClearSearchButton();
  await refreshData(true);
  connectWebSocket();
  setInterval(() => {
    if (!wsActive) {
      refreshData(false);
    }
  }, REFRESH_INTERVAL_MS);
}

init();
