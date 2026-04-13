const tableBody = document.getElementById('tableBody');
const countLabel = document.getElementById('countLabel');
const searchInput = document.getElementById('searchInput');
const statusFilter = document.getElementById('statusFilter');
const resetBtn = document.getElementById('resetBtn');
const darkBtn = document.getElementById('darkBtn');
const rulesBtn = document.getElementById('rulesBtn');
const rulesPanel = document.getElementById('rulesPanel');
const closeRulesBtn = document.getElementById('closeRulesBtn');
const rulesTextInput = document.getElementById('rulesTextInput');
const saveRulesBtn = document.getElementById('saveRulesBtn');
const rulesSaveMsg = document.getElementById('rulesSaveMsg');

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
]);
const DEFAULT_STATUS_RULES_TEXT = [
  '# Формат: условие AND условие -> СТАТУС',
  '# Поля: problem, rejected, invoiceCreated, paymentReceived, edoSent, shipmentPending, receiptConfirmed, kpSent, clientFilled, managerFilled, productSpecified',
  '# Операторы: = true|false или != true|false',
  '',
  'problem = true -> ПРОБЛЕМА',
  'rejected = true -> ОТКАЗ',
  'invoiceCreated = true AND paymentReceived = true AND edoSent = true -> ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО',
  'invoiceCreated = true AND edoSent = true AND paymentReceived != true -> ЖДЕМ ОПЛАТУ',
  'invoiceCreated = true AND edoSent != true -> ОТПРАВИТЬ В ЭДО',
  'shipmentPending = true -> ОТГРУЗИТЬ',
  'receiptConfirmed = true -> КЛИЕНТ ДУМАЕТ',
  'kpSent = true -> ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП',
  'clientFilled = true AND managerFilled = true AND productSpecified = true -> ОТПРАВИТЬ КЛИЕНТУ',
].join('\n');

let rows = [];
let lastFingerprint = '';
let lastSyncAt = null;
let ws = null;
let wsActive = false;
const TABLE_COLUMN_COUNT = 15;
let statusRules = createDefaultStatusRules();

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
  if (value === true || value === false) return value;
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
      if (flag !== null) return flag;
    }
  }
  if (typeof fallback === 'function') return fallback(row);
  return null;
}

function formatFlag(flag) {
  if (flag === true) return 'Да';
  if (flag === false) return 'Нет';
  return '—';
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

function setRulesMessage(text, type = '') {
  if (!rulesSaveMsg) return;
  rulesSaveMsg.textContent = text || '';
  rulesSaveMsg.classList.remove('is-ok', 'is-error');
  if (type === 'ok') rulesSaveMsg.classList.add('is-ok');
  if (type === 'error') rulesSaveMsg.classList.add('is-error');
}

function parseBooleanToken(value) {
  const v = String(value || '').trim().toLowerCase();
  if (['true', '1', 'yes', 'y', 'да'].includes(v)) return true;
  if (['false', '0', 'no', 'n', 'нет'].includes(v)) return false;
  return null;
}

function parseConditionToken(token) {
  const match = String(token || '').trim().match(/^([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=)\s*(.+)$/);
  if (!match) return { error: `Некорректное условие: ${token}` };

  const field = String(match[1] || '').trim();
  if (!RULE_FIELDS.has(field)) return { error: `Неизвестное поле: ${field}` };

  const boolValue = parseBooleanToken(match[3]);
  if (boolValue === null) return { error: `Значение должно быть true/false (или да/нет): ${match[3]}` };

  let operator = 'is_true';
  if (match[2] === '=' && boolValue === true) operator = 'is_true';
  if (match[2] === '=' && boolValue === false) operator = 'is_false';
  if (match[2] === '!=' && boolValue === true) operator = 'is_not_true';
  if (match[2] === '!=' && boolValue === false) operator = 'is_not_false';

  return { condition: { field, operator } };
}

function parseRulesText(text) {
  const rawText = String(text || '').replace(/\r\n/g, '\n');
  const lines = rawText.split('\n');
  const rules = [];
  const errors = [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line || line.startsWith('#')) continue;

    const parts = line.split('->');
    if (parts.length < 2) {
      errors.push(`Строка ${i + 1}: нет разделителя ->`);
      continue;
    }

    const left = parts[0].trim();
    const label = parts.slice(1).join('->').trim();
    if (!label) {
      errors.push(`Строка ${i + 1}: пустой статус справа от ->`);
      continue;
    }

    const conditionTokens = left.split(/\s+(?:AND|И)\s+/i).map((x) => x.trim()).filter(Boolean);
    if (!conditionTokens.length) {
      errors.push(`Строка ${i + 1}: нет условий слева от ->`);
      continue;
    }

    const conditions = [];
    let hasError = false;
    for (const token of conditionTokens) {
      const parsed = parseConditionToken(token);
      if (parsed.error) {
        errors.push(`Строка ${i + 1}: ${parsed.error}`);
        hasError = true;
      } else {
        conditions.push(parsed.condition);
      }
    }

    if (!hasError && conditions.length) {
      rules.push({ label, conditions });
    }
  }

  if (!rules.length) {
    return { rules: createDefaultStatusRules(), errors: errors.length ? errors : ['Не найдено ни одного корректного правила'] };
  }

  return { rules, errors };
}

function deriveStatusFacts(row) {
  const commentText = [row?.additionalInfoFirstLine, row?.comment, row?.Комментарий]
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
    productSpecified: getFlag(row, ['productSpecified', 'isProductSpecified', 'товарУказан']),
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

function renderRulesEditor(text) {
  if (!rulesTextInput) return;
  rulesTextInput.value = String(text || DEFAULT_STATUS_RULES_TEXT);
}

async function loadStatusRulesFromServer() {
  let lastError = null;
  for (const src of STATUS_RULES_SOURCES) {
    try {
      const response = await fetch(src, { cache: 'no-store' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      const rulesText = String(payload?.rulesText || '').trim() || DEFAULT_STATUS_RULES_TEXT;
      const parsed = parseRulesText(rulesText);
      statusRules = parsed.rules;
      renderRulesEditor(rulesText);
      if (parsed.errors.length) {
        setRulesMessage(`Загружено с предупреждениями: ${parsed.errors[0]}`, 'error');
      } else {
        setRulesMessage('Правила загружены с сервера.', 'ok');
      }
      return;
    } catch (err) {
      lastError = err;
    }
  }

  statusRules = createDefaultStatusRules();
  renderRulesEditor(DEFAULT_STATUS_RULES_TEXT);
  setRulesMessage(`Серверные правила не загружены: ${lastError?.message || 'неизвестная ошибка'}`, 'error');
}

async function saveStatusRulesToServer(rulesText) {
  let lastError = null;
  for (const src of STATUS_RULES_SOURCES) {
    try {
      const response = await fetch(src, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rulesText }),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return;
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError || new Error('Не удалось сохранить правила');
}

async function onSaveRulesClick() {
  const rulesText = String(rulesTextInput?.value || '').trim();
  if (!rulesText) {
    setRulesMessage('Введите текст правил перед сохранением.', 'error');
    return;
  }

  const parsed = parseRulesText(rulesText);
  if (parsed.errors.length) {
    setRulesMessage(`Ошибка в правилах: ${parsed.errors[0]}`, 'error');
    return;
  }

  try {
    await saveStatusRulesToServer(rulesText);
    statusRules = parsed.rules;
    fillStatuses(rows);
    applyFilters();
    setRulesMessage('Правила сохранены на сервере.', 'ok');
  } catch (err) {
    setRulesMessage(`Сохранение не удалось: ${err.message}`, 'error');
  }
}

function openRulesPanel() {
  rulesPanel.hidden = false;
  rulesBtn.textContent = 'Скрыть правила';
}

function closeRulesPanel() {
  rulesPanel.hidden = true;
  rulesBtn.textContent = 'Правила статусов';
}

function render(data) {
  const synced = lastSyncAt ? ` · обновлено ${lastSyncAt.toLocaleTimeString('ru-RU')}` : '';
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
    const rowStatus = computeKpStatus(r);
    const byStatus = !status || rowStatus === status;
    const text = `${r.number || ''} ${r.customerName || ''} ${rowStatus} ${r.additionalInfoFirstLine || ''}`.toLowerCase();
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
  const sources = ['/api/kp/all', 'https://onec-kp-realtime.onrender.com/api/kp/all'];

  let response = null;
  let lastError = null;
  for (const src of sources) {
    try {
      const r = await fetch(src, { cache: 'no-store' });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      response = r;
      break;
    } catch (e) {
      lastError = e;
    }
  }

  if (!response) throw lastError || new Error('Нет доступного источника данных');

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
  if (!window.location.origin.startsWith('http')) return;

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
  await loadStatusRulesFromServer();
  await refreshData(true);
  connectWebSocket();
  setInterval(() => {
    if (!wsActive) refreshData(false);
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
  if (rulesPanel.hidden) openRulesPanel();
  else closeRulesPanel();
});

closeRulesBtn.addEventListener('click', closeRulesPanel);
saveRulesBtn.addEventListener('click', onSaveRulesClick);

init();
