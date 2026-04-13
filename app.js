const tableBody = document.getElementById('tableBody');
const countLabel = document.getElementById('countLabel');
const searchInput = document.getElementById('searchInput');
const statusFilter = document.getElementById('statusFilter');
const resetBtn = document.getElementById('resetBtn');
const darkBtn = document.getElementById('darkBtn');
const rulesBtn = document.getElementById('rulesBtn');
const rulesPanel = document.getElementById('rulesPanel');
const closeRulesBtn = document.getElementById('closeRulesBtn');
const rulesList = document.getElementById('rulesList');
const addRuleBtn = document.getElementById('addRuleBtn');
const resetRulesBtn = document.getElementById('resetRulesBtn');

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
const STATUS_RULES_STORAGE_KEY = 'kpStatusRulesV1';
const DEFAULT_FALLBACK_STATUS = 'ОБРАБОТАТЬ';

const RULE_FIELDS = [
  { value: 'problem', label: 'Проблема' },
  { value: 'rejected', label: 'Отказ' },
  { value: 'invoiceCreated', label: 'Накладная создана' },
  { value: 'paymentReceived', label: 'Оплата получена' },
  { value: 'edoSent', label: 'В ЭДО отправлено' },
  { value: 'shipmentPending', label: 'Нужно отгрузить' },
  { value: 'receiptConfirmed', label: 'Получение подтверждено' },
  { value: 'kpSent', label: 'КП отправлено' },
  { value: 'clientFilled', label: 'Клиент заполнен' },
  { value: 'managerFilled', label: 'Менеджер заполнен' },
  { value: 'productSpecified', label: 'Товар указан' },
];

const RULE_OPERATORS = [
  { value: 'is_true', label: 'равно Да' },
  { value: 'is_false', label: 'равно Нет' },
  { value: 'is_not_true', label: 'не равно Да' },
  { value: 'is_not_false', label: 'не равно Нет' },
];

let rows = [];
let lastFingerprint = '';
let lastSyncAt = null;
let ws = null;
let wsActive = false;
const TABLE_COLUMN_COUNT = 15;
let statusRules = loadStatusRules();

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

function createRuleId() {
  return `rule-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
}

function createDefaultStatusRules() {
  return [
    { id: createRuleId(), label: 'ПРОБЛЕМА', conditions: [{ field: 'problem', operator: 'is_true' }] },
    { id: createRuleId(), label: 'ОТКАЗ', conditions: [{ field: 'rejected', operator: 'is_true' }] },
    {
      id: createRuleId(),
      label: 'ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО',
      conditions: [
        { field: 'invoiceCreated', operator: 'is_true' },
        { field: 'paymentReceived', operator: 'is_true' },
        { field: 'edoSent', operator: 'is_true' },
      ],
    },
    {
      id: createRuleId(),
      label: 'ЖДЕМ ОПЛАТУ',
      conditions: [
        { field: 'invoiceCreated', operator: 'is_true' },
        { field: 'edoSent', operator: 'is_true' },
        { field: 'paymentReceived', operator: 'is_not_true' },
      ],
    },
    {
      id: createRuleId(),
      label: 'ОТПРАВИТЬ В ЭДО',
      conditions: [
        { field: 'invoiceCreated', operator: 'is_true' },
        { field: 'edoSent', operator: 'is_not_true' },
      ],
    },
    { id: createRuleId(), label: 'ОТГРУЗИТЬ', conditions: [{ field: 'shipmentPending', operator: 'is_true' }] },
    { id: createRuleId(), label: 'КЛИЕНТ ДУМАЕТ', conditions: [{ field: 'receiptConfirmed', operator: 'is_true' }] },
    { id: createRuleId(), label: 'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП', conditions: [{ field: 'kpSent', operator: 'is_true' }] },
    {
      id: createRuleId(),
      label: 'ОТПРАВИТЬ КЛИЕНТУ',
      conditions: [
        { field: 'clientFilled', operator: 'is_true' },
        { field: 'managerFilled', operator: 'is_true' },
        { field: 'productSpecified', operator: 'is_true' },
      ],
    },
  ];
}

function normalizeStoredCondition(condition) {
  const field = RULE_FIELDS.some((item) => item.value === condition?.field)
    ? condition.field
    : 'problem';
  const operator = RULE_OPERATORS.some((item) => item.value === condition?.operator)
    ? condition.operator
    : 'is_true';
  return { field, operator };
}

function normalizeStoredRule(rule, index) {
  const conditions = Array.isArray(rule?.conditions) && rule.conditions.length
    ? rule.conditions.map(normalizeStoredCondition)
    : [{ field: 'problem', operator: 'is_true' }];

  return {
    id: typeof rule?.id === 'string' && rule.id ? rule.id : `rule-restored-${index}`,
    label: String(rule?.label || '').trim() || `Правило ${index + 1}`,
    conditions,
  };
}

function loadStatusRules() {
  try {
    const raw = localStorage.getItem(STATUS_RULES_STORAGE_KEY);
    if (!raw) {
      return createDefaultStatusRules();
    }

    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) || !parsed.length) {
      return createDefaultStatusRules();
    }

    return parsed.map(normalizeStoredRule);
  } catch {
    return createDefaultStatusRules();
  }
}

function persistStatusRules() {
  localStorage.setItem(STATUS_RULES_STORAGE_KEY, JSON.stringify(statusRules));
}

function deriveStatusFacts(row) {
  const commentText = [
    row?.additionalInfoFirstLine,
    row?.comment,
    row?.Комментарий,
  ]
    .filter(Boolean)
    .map((value) => String(value).toUpperCase())
    .join(' ');
  const rejectByComment = commentText.includes('ОТКАЗ');

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

  return {
    problem: getFlag(row, ['problem', 'hasProblem', 'проблема']),
    rejected: rejectByComment || getFlag(row, ['rejected', 'isRejected', 'отказ']),
    invoiceCreated: getFlag(row, ['invoiceCreated', 'isInvoiceCreated', 'накладнаяСоздана']),
    paymentReceived: getFlag(row, ['paymentReceived', 'isPaymentReceived', 'оплатаПолучена']),
    edoSent: getFlag(row, ['edoSent', 'isEdoSent', 'вЭдоОтправлено']),
    shipmentPending: getFlag(row, ['shipmentPending', 'isShipmentPending', 'отгрузить']),
    receiptConfirmed: getFlag(row, ['receiptConfirmed', 'isReceiptConfirmed', 'получениеПодтверждено']),
    kpSent: getFlag(row, ['kpSent', 'isKpSent', 'кпОтправлено']),
    clientFilled,
    managerFilled,
    productSpecified,
  };
}

function matchesRuleCondition(facts, condition) {
  const value = facts[condition.field];
  switch (condition.operator) {
    case 'is_true':
      return value === true;
    case 'is_false':
      return value === false;
    case 'is_not_true':
      return value !== true;
    case 'is_not_false':
      return value !== false;
    default:
      return false;
  }
}

function computeKpStatus(row) {
  const facts = deriveStatusFacts(row);
  for (const rule of statusRules) {
    if (rule.conditions.every((condition) => matchesRuleCondition(facts, condition))) {
      return rule.label;
    }
  }
  return DEFAULT_FALLBACK_STATUS;
}

function updateRulesAndRefresh() {
  persistStatusRules();
  renderRulesEditor();
  fillStatuses(rows);
  applyFilters();
}

function renderRulesEditor() {
  if (!statusRules.length) {
    rulesList.innerHTML = '<div class="rule-empty">Правил пока нет.</div>';
    return;
  }

  rulesList.innerHTML = statusRules.map((rule, index) => `
    <article class="rule-card" data-rule-id="${escapeHtml(rule.id)}">
      <div class="rule-card__header">
        <div>
          <div class="rule-card__priority">Приоритет ${index + 1}</div>
          <input data-action="label" data-rule-index="${index}" type="text" value="${escapeHtml(rule.label)}" aria-label="Название статуса">
        </div>
        <div class="rule-card__actions">
          <button data-action="move-up" data-rule-index="${index}" type="button" ${index === 0 ? 'disabled' : ''}>Выше</button>
          <button data-action="move-down" data-rule-index="${index}" type="button" ${index === statusRules.length - 1 ? 'disabled' : ''}>Ниже</button>
        </div>
      </div>
      <div class="rule-card__conditions">
        ${rule.conditions.map((condition, conditionIndex) => `
          <div class="rule-condition">
            <select data-action="field" data-rule-index="${index}" data-condition-index="${conditionIndex}" aria-label="Поле условия">
              ${RULE_FIELDS.map((field) => `<option value="${field.value}" ${field.value === condition.field ? 'selected' : ''}>${field.label}</option>`).join('')}
            </select>
            <select data-action="operator" data-rule-index="${index}" data-condition-index="${conditionIndex}" aria-label="Оператор условия">
              ${RULE_OPERATORS.map((operator) => `<option value="${operator.value}" ${operator.value === condition.operator ? 'selected' : ''}>${operator.label}</option>`).join('')}
            </select>
            <button data-action="remove-condition" data-rule-index="${index}" data-condition-index="${conditionIndex}" type="button" ${rule.conditions.length === 1 ? 'disabled' : ''}>Удалить условие</button>
          </div>
        `).join('')}
      </div>
      <div class="rule-card__footer">
        <button data-action="add-condition" data-rule-index="${index}" type="button">Добавить условие</button>
        <button data-action="remove-rule" data-rule-index="${index}" type="button" ${statusRules.length === 1 ? 'disabled' : ''}>Удалить правило</button>
      </div>
    </article>
  `).join('');
}

function openRulesPanel() {
  rulesPanel.hidden = false;
  rulesBtn.textContent = 'Скрыть правила';
  renderRulesEditor();
}

function closeRulesPanel() {
  rulesPanel.hidden = true;
  rulesBtn.textContent = 'Правила статусов';
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
      <td>${formatFlag(getFlag(r, ['managerFilled', 'isManagerFilled', 'менеджерЗаполнен'], (row) => {
        const manager = String(row.managerName || row.manager || row['Менеджер'] || '').trim();
        if (!manager) return null;
        const normalized = manager.toLowerCase().replaceAll('ё', 'е');
        return normalized !== 'не определен' && normalized !== 'неопределен';
      }))}</td>
      <td>${formatFlag(getFlag(r, ['productSpecified', 'isProductSpecified', 'товарУказан']))}</td>
      <td>${formatFlag(getFlag(r, ['kpSent', 'isKpSent', 'кпОтправлено']))}</td>
      <td>${formatFlag(getFlag(r, ['receiptConfirmed', 'isReceiptConfirmed', 'получениеПодтверждено']))}</td>
      <td>${formatFlag(getFlag(r, ['paymentReceived', 'isPaymentReceived', 'оплатаПолучена']))}</td>
      <td>${formatFlag(getFlag(r, ['invoiceCreated', 'isInvoiceCreated', 'накладнаяСоздана']))}</td>
      <td>${formatFlag(getFlag(r, ['edoSent', 'isEdoSent', 'вЭдоОтправлено']))}</td>
      <td>${formatFlag(getFlag(r, ['rejected', 'isRejected', 'отказ']))}</td>
      <td>${formatFlag(getFlag(r, ['problem', 'hasProblem', 'проблема']))}</td>
      <td><span class="tag">${escapeHtml(computeKpStatus(r))}</span></td>
      <td>${escapeHtml(r.additionalInfoFirstLine || '')}</td>
    </tr>
  `).join('');
}

function applyFilters() {
  const q = searchInput.value.trim().toLowerCase();
  const status = statusFilter.value;

  const filtered = rows.filter((r) => {
    const byStatus = !status || computeKpStatus(r) === status;
    const text = `${r.number || ''} ${r.customerName || ''} ${computeKpStatus(r)} ${r.additionalInfoFirstLine || ''}`.toLowerCase();
    const byText = !q || text.includes(q);
    return byStatus && byText;
  });

  render(filtered);
}

function fillStatuses(data) {
  const selectedStatus = statusFilter.value;
  statusFilter.innerHTML = '<option value="">Все</option>';
  const statuses = [...new Set(data.map((r) => computeKpStatus(r)))].sort((a, b) => a.localeCompare(b, 'ru'));
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
  return sortRowsByKpNumberDesc(data);
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
  await refreshData(true);
  renderRulesEditor();
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

rulesBtn.addEventListener('click', () => {
  if (rulesPanel.hidden) {
    openRulesPanel();
  } else {
    closeRulesPanel();
  }
});

closeRulesBtn.addEventListener('click', closeRulesPanel);

addRuleBtn.addEventListener('click', () => {
  statusRules.push({
    id: createRuleId(),
    label: `Новое правило ${statusRules.length + 1}`,
    conditions: [{ field: 'problem', operator: 'is_true' }],
  });
  updateRulesAndRefresh();
});

resetRulesBtn.addEventListener('click', () => {
  statusRules = createDefaultStatusRules();
  updateRulesAndRefresh();
});

rulesList.addEventListener('change', (event) => {
  const target = event.target;
  if (!(target instanceof HTMLInputElement) && !(target instanceof HTMLSelectElement)) {
    return;
  }

  const ruleIndex = Number(target.dataset.ruleIndex);
  const rule = statusRules[ruleIndex];
  if (!rule) {
    return;
  }

  if (target instanceof HTMLInputElement && target.dataset.action === 'label') {
    rule.label = target.value.trim() || `Правило ${ruleIndex + 1}`;
    updateRulesAndRefresh();
    return;
  }

  const conditionIndex = Number(target.dataset.conditionIndex);
  const condition = rule.conditions?.[conditionIndex];
  if (!condition) {
    return;
  }

  if (target.dataset.action === 'field') {
    condition.field = target.value;
  }
  if (target.dataset.action === 'operator') {
    condition.operator = target.value;
  }
  updateRulesAndRefresh();
});

rulesList.addEventListener('click', (event) => {
  const target = event.target;
  if (!(target instanceof HTMLButtonElement)) {
    return;
  }

  const action = target.dataset.action;
  const ruleIndex = Number(target.dataset.ruleIndex);
  const conditionIndex = Number(target.dataset.conditionIndex);

  if (!Number.isInteger(ruleIndex) || !statusRules[ruleIndex]) {
    return;
  }

  if (action === 'move-up' && ruleIndex > 0) {
    [statusRules[ruleIndex - 1], statusRules[ruleIndex]] = [statusRules[ruleIndex], statusRules[ruleIndex - 1]];
    updateRulesAndRefresh();
    return;
  }

  if (action === 'move-down' && ruleIndex < statusRules.length - 1) {
    [statusRules[ruleIndex + 1], statusRules[ruleIndex]] = [statusRules[ruleIndex], statusRules[ruleIndex + 1]];
    updateRulesAndRefresh();
    return;
  }

  if (action === 'add-condition') {
    statusRules[ruleIndex].conditions.push({ field: 'problem', operator: 'is_true' });
    updateRulesAndRefresh();
    return;
  }

  if (action === 'remove-condition') {
    if (statusRules[ruleIndex].conditions.length > 1 && Number.isInteger(conditionIndex)) {
      statusRules[ruleIndex].conditions.splice(conditionIndex, 1);
      updateRulesAndRefresh();
    }
    return;
  }

  if (action === 'remove-rule' && statusRules.length > 1) {
    statusRules.splice(ruleIndex, 1);
    updateRulesAndRefresh();
  }
});

init();
