const searchInput = document.getElementById('searchInput');
const clearSearchBtn = document.getElementById('clearSearchBtn');
const managerFilter = document.getElementById('managerFilter');
const newRequestBtn = document.getElementById('newRequestBtn');
const newRequestPanel = document.getElementById('newRequestPanel');
const closeRequestPanelBtn = document.getElementById('closeRequestPanelBtn');
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
const DEFAULT_FALLBACK_STATUS = 'ОБРАБОТАТЬ';
const STATUS_RULES_SOURCES = ['/api/status-rules', 'https://onec-kp-realtime.onrender.com/api/status-rules'];
const RULE_FIELDS = new Set([
  'problem',
  'rejected',
  'invoiceCreated',
  'paymentReceived',
  'edoSent',
  'shipmentPending',
  'receiptConfirmed',
  'kpSent',
  'clientFilled',
  'managerFilled',
  'productSpecified',
  'priceFilled',
]);
const RULE_FIELD_ALIASES = new Map([
  ['problem', 'problem'],
  ['проблема', 'problem'],
  ['rejected', 'rejected'],
  ['отказ', 'rejected'],
  ['invoicecreated', 'invoiceCreated'],
  ['накладнаясоздана', 'invoiceCreated'],
  ['paymentreceived', 'paymentReceived'],
  ['оплатаполучена', 'paymentReceived'],
  ['edosent', 'edoSent'],
  ['вэдоотправлено', 'edoSent'],
  ['shipmentpending', 'shipmentPending'],
  ['отгрузить', 'shipmentPending'],
  ['требуетсяотгрузка', 'shipmentPending'],
  ['receiptconfirmed', 'receiptConfirmed'],
  ['клиенткувидел', 'receiptConfirmed'],
  ['клиенткпувидел', 'receiptConfirmed'],
  ['получениекпподтверждено', 'receiptConfirmed'],
  ['kpsent', 'kpSent'],
  ['кпотправлено', 'kpSent'],
  ['clientfilled', 'clientFilled'],
  ['клиентзаполнен', 'clientFilled'],
  ['managerfilled', 'managerFilled'],
  ['менеджерзаполнен', 'managerFilled'],
  ['productspecified', 'productSpecified'],
  ['товаруказан', 'productSpecified'],
  ['pricefilled', 'priceFilled'],
  ['ценауказана', 'priceFilled'],
  ['ценавпервойстрокетоварауказана', 'priceFilled'],
]);
const STATUS_ORDER = [
  'ПРОБЛЕМА',
  'ОТКАЗ',
  'ЖДЕМ ОПЛАТУ',
  'КЛИЕНТ ДУМАЕТ',
  'ОТПРАВИТЬ В ЭДО',
  'ОТГРУЗИТЬ',
  'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП',
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
let statusRules = createDefaultStatusRules();

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

managerFilter.addEventListener('change', () => {
  renderBoard();
});
function closeRequestPanel() {
  newRequestPanel.hidden = true;
  newRequestBtn.textContent = 'НОВЫЙ ЗАПРОС';
}

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

closeRequestPanelBtn.addEventListener('click', closeRequestPanel);

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

function getManagerName(row) {
  const manager = String(row?.managerName || row?.manager || row?.['Менеджер'] || '').trim();
  return manager || 'НЕ ОПРЕДЕЛЕН';
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

function createDefaultStatusRules() {
  return [
    { label: 'ПРОБЛЕМА', conditions: [{ field: 'problem', operator: 'is_true' }] },
    { label: 'ОТКАЗ', conditions: [{ field: 'rejected', operator: 'is_true' }] },
    {
      label: 'ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО',
      conditions: [
        { field: 'invoiceCreated', operator: 'is_true' },
        { field: 'paymentReceived', operator: 'is_true' },
        { field: 'edoSent', operator: 'is_true' },
      ],
    },
    {
      label: 'ЖДЕМ ОПЛАТУ',
      conditions: [
        { field: 'invoiceCreated', operator: 'is_true' },
        { field: 'edoSent', operator: 'is_true' },
        { field: 'paymentReceived', operator: 'is_not_true' },
      ],
    },
    {
      label: 'ОТПРАВИТЬ В ЭДО',
      conditions: [
        { field: 'invoiceCreated', operator: 'is_true' },
        { field: 'edoSent', operator: 'is_not_true' },
      ],
    },
    { label: 'ОТГРУЗИТЬ', conditions: [{ field: 'shipmentPending', operator: 'is_true' }] },
    { label: 'КЛИЕНТ ДУМАЕТ', conditions: [{ field: 'receiptConfirmed', operator: 'is_true' }] },
    { label: 'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП', conditions: [{ field: 'kpSent', operator: 'is_true' }] },
    {
      label: 'ОТПРАВИТЬ КЛИЕНТУ',
      conditions: [
        { field: 'clientFilled', operator: 'is_true' },
        { field: 'managerFilled', operator: 'is_true' },
        { field: 'productSpecified', operator: 'is_true' },
      ],
    },
  ];
}

function parseBooleanToken(value) {
  const v = String(value || '').trim().toLowerCase();
  if (['true', '1', 'yes', 'y', 'да'].includes(v)) return true;
  if (['false', '0', 'no', 'n', 'нет'].includes(v)) return false;
  return null;
}

function normalizeRuleFieldName(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replaceAll('ё', 'е')
    .replace(/[^a-zа-я0-9]/gi, '');
}

function resolveRuleField(fieldRaw) {
  const field = String(fieldRaw || '').trim();
  if (RULE_FIELDS.has(field)) return field;
  const alias = RULE_FIELD_ALIASES.get(normalizeRuleFieldName(field));
  return alias || '';
}

function parseConditionToken(token) {
  const raw = String(token || '').trim();

  const technicalMatch = raw.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=)\s*(.+)$/);
  if (technicalMatch) {
    const field = resolveRuleField(technicalMatch[1]);
    if (!field) return { error: `Неизвестное поле: ${technicalMatch[1]}` };

    const boolValue = parseBooleanToken(technicalMatch[3]);
    if (boolValue === null) return { error: `Значение должно быть true/false (или да/нет): ${technicalMatch[3]}` };

    let operator = 'is_true';
    if (technicalMatch[2] === '=' && boolValue === true) operator = 'is_true';
    if (technicalMatch[2] === '=' && boolValue === false) operator = 'is_false';
    if (technicalMatch[2] === '!=' && boolValue === true) operator = 'is_not_true';
    if (technicalMatch[2] === '!=' && boolValue === false) operator = 'is_not_false';
    return { condition: { field, operator } };
  }

  const humanMatch = raw.match(/^(.+?)\s*[-:=]\s*(.+)$/);
  if (!humanMatch) return { error: `Некорректное условие: ${token}` };

  const field = resolveRuleField(humanMatch[1]);
  if (!field) return { error: `Неизвестное поле: ${humanMatch[1]}` };

  const boolValue = parseBooleanToken(humanMatch[2]);
  if (boolValue === null) return { error: `Значение должно быть да/нет (или true/false): ${humanMatch[2]}` };

  return { condition: { field, operator: boolValue ? 'is_true' : 'is_false' } };
}

function parseHumanRuleLine(line) {
  const match = String(line || '').trim().match(/^статус\s+(.+?)\s+устанавливается,\s*если\s+(.+)$/i);
  if (!match) return null;

  const label = String(match[1] || '').trim();
  const left = String(match[2] || '').trim();
  if (!label || !left) return { error: 'пустой статус или условие' };

  let matchMode = 'all';
  let conditionsExpr = left;
  const anyOfMatch = left.match(/^(?:(?:выполнено|выполняется)\s+)?хотя\s*бы\s*одно\s+из\s+условий\s*(?::|-)?\s*(.+)$/i);
  if (anyOfMatch) {
    matchMode = 'any';
    conditionsExpr = String(anyOfMatch[1] || '').trim();
  }

  const conditionTokens = conditionsExpr
    .split(matchMode === 'any' ? /\s*,\s*|\s+(?:OR|ИЛИ|AND|И)\s+/i : /\s*,\s*|\s+(?:AND|И)\s+/i)
    .map((x) => x.trim())
    .filter(Boolean);
  if (!conditionTokens.length) return { error: 'нет условий после слова "если"' };

  const conditions = [];
  for (const token of conditionTokens) {
    const parsed = parseConditionToken(token);
    if (parsed.error) return { error: parsed.error };
    conditions.push(parsed.condition);
  }

  return { rule: { label, conditions, matchMode } };
}

function parseRulesText(text) {
  const lines = String(text || '').replace(/\r\n/g, '\n').split('\n');
  const parsedRules = [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line || line.startsWith('#')) continue;

    const humanRule = parseHumanRuleLine(line);
    if (humanRule && humanRule.rule) {
      parsedRules.push(humanRule.rule);
      continue;
    }

    const parts = line.split('->');
    if (parts.length < 2) continue;

    const left = parts[0].trim();
    const label = parts.slice(1).join('->').trim();
    if (!left || !label) continue;

    const conditionTokens = left.split(/\s+(?:AND|И)\s+/i).map((x) => x.trim()).filter(Boolean);
    if (!conditionTokens.length) continue;

    const conditions = [];
    let valid = true;
    for (const token of conditionTokens) {
      const parsed = parseConditionToken(token);
      if (parsed.error) {
        valid = false;
        break;
      }
      conditions.push(parsed.condition);
    }

    if (valid && conditions.length) {
      parsedRules.push({ label, conditions });
    }
  }

  return parsedRules.length ? parsedRules : createDefaultStatusRules();
}

async function loadStatusRulesFromServer() {
  let loaded = false;
  for (const src of STATUS_RULES_SOURCES) {
    try {
      const response = await fetch(src, { cache: 'no-store' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      const rulesText = String(payload?.rulesText || '').trim();
      statusRules = parseRulesText(rulesText);
      loaded = true;
      break;
    } catch {
      // Try next source.
    }
  }

  if (!loaded) {
    statusRules = createDefaultStatusRules();
  }
}

function deriveStatusFacts(row) {
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

  return {
    problem: getFlag(row, ['problem', 'hasProblem', 'проблема']),
    rejected: hasRejectInComment(row) || getFlag(row, ['rejected', 'isRejected', 'отказ']),
    invoiceCreated: getFlag(row, ['invoiceCreated', 'isInvoiceCreated', 'накладнаяСоздана']),
    paymentReceived: getFlag(row, ['paymentReceived', 'isPaymentReceived', 'оплатаПолучена']),
    edoSent: getFlag(row, ['edoSent', 'isEdoSent', 'вЭдоОтправлено']),
    shipmentPending: getFlag(row, ['shipmentPending', 'isShipmentPending', 'отгрузить']),
    receiptConfirmed: getFlag(row, ['receiptConfirmed', 'isReceiptConfirmed', 'получениеПодтверждено']),
    kpSent: getFlag(row, ['kpSent', 'isKpSent', 'кпОтправлено']),
    clientFilled,
    managerFilled,
    productSpecified: getFlag(row, ['productSpecified', 'isProductSpecified', 'товарУказан']),
    priceFilled: getFlag(row, ['priceFilled']),
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
  const serverComputed = String(row?.statusKpComputed || '').trim();
  if (serverComputed) return serverComputed;
  const statusKp = String(row?.statusKp || '').trim();
  if (statusKp) return statusKp;
  return DEFAULT_FALLBACK_STATUS;
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
  const primaryTabs = [
    'ОТКАЗ',
    'ОБРАБОТАТЬ',
    'ОТПРАВИТЬ КЛИЕНТУ',
    'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП',
    'КЛИЕНТ ДУМАЕТ',
    'ОТГРУЗИТЬ И ОТПРАВИТЬ В ЭДО',
    'ЖДЕМ ОПЛАТУ',
  ];
  const forcedTailTabs = [
    'ПРОБЛЕМА',
    'ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО',
  ];

  const tabs = [{ key: ALL_TAB_KEY, label: 'ALL', count: rows.length }];
  for (const status of primaryTabs) {
    tabs.push({ key: status, label: status, count: counts.get(status) || 0 });
  }

  for (const status of orderedStatuses) {
    if (!primaryTabs.includes(status) && !forcedTailTabs.includes(status)) {
      tabs.push({ key: status, label: status, count: counts.get(status) || 0 });
    }
  }

  for (const status of forcedTailTabs) {
    tabs.push({ key: status, label: status, count: counts.get(status) || 0 });
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
  if (
    tabKey === 'ПРОБЛЕМА' || tabKey === 'ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО' ||
    tabKey === 'ОБРАБОТАТЬ' || tabKey === 'ОТПРАВИТЬ КЛИЕНТУ' ||
    tabKey === 'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП' || tabKey === 'КЛИЕНТ ДУМАЕТ' ||
    tabKey === 'ОТГРУЗИТЬ И ОТПРАВИТЬ В ЭДО' || tabKey === 'ЖДЕМ ОПЛАТУ'
  ) {
    return 'status-tab--second-pair';
  }
  return 'status-tab--full';
}

function renderBoard() {
  const counts = getStatusCounts(rows);
  const query = searchInput.value.trim().toLowerCase();
  const manager = managerFilter.value;
  const filtered = rows.filter((row) => {
    const status = computeKpStatus(row);
    const rowManager = getManagerName(row);
    const matchesTab = activeTab === ALL_TAB_KEY || status === activeTab;
    if (!matchesTab) {
      return false;
    }

    const byManager = !manager || rowManager === manager;
    const haystack = `${row.number || ''} ${row.customerName || ''} ${rowManager} ${row.additionalInfoFirstLine || ''} ${status}`.toLowerCase();
    return byManager && (!query || haystack.includes(query));
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

function fillManagers(data) {
  const selectedManager = managerFilter.value;
  managerFilter.innerHTML = '<option value="">Все менеджеры</option>';

  const managers = [...new Set((data || []).map((row) => getManagerName(row)))].sort((a, b) => a.localeCompare(b, 'ru'));
  for (const manager of managers) {
    const option = document.createElement('option');
    option.value = manager;
    option.textContent = manager;
    managerFilter.appendChild(option);
  }

  if ([...managerFilter.options].some((o) => o.value === selectedManager)) {
    managerFilter.value = selectedManager;
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
  if (nextFingerprint !== lastFingerprint) {
    rows = nextRows;
    lastFingerprint = nextFingerprint;
    fillManagers(rows);
  }
  lastSyncAt = syncedAt || new Date();
  renderBoard();
}

async function refreshData(initial = false) {
  try {
    await loadStatusRulesFromServer();
    const nextRows = await loadRows();
    setRows(nextRows, new Date());
    // Clear any previous error state once data loads successfully
    if (boardContent.querySelector('.board-empty')) {
      renderBoard();
    }
  } catch (error) {
    if (initial) {
      boardContent.innerHTML = `<div class="board-empty">Не удалось загрузить данные: ${escapeHtml(error.message)}<br><small>Повторная попытка через несколько секунд…</small></div>`;
      updatedAtLabel.textContent = 'Ошибка';
      // Retry initial load after a delay (Render free tier may be waking up)
      setTimeout(() => refreshData(true), 5000);
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
  await loadStatusRulesFromServer();
  await refreshData(true);
  connectWebSocket();
  setInterval(() => {
    if (!wsActive) {
      refreshData(false);
    }
  }, REFRESH_INTERVAL_MS);
}

init();
