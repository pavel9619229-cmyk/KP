const tableBody = document.getElementById('tableBody');
const countLabel = document.getElementById('countLabel');
const searchInput = document.getElementById('searchInput');
const managerFilter = document.getElementById('managerFilter');
const statusFilter = document.getElementById('statusFilter');
const resetBtn = document.getElementById('resetBtn');
const darkBtn = document.getElementById('darkBtn');
const rulesBtn = document.getElementById('rulesBtn');
const loadingRulesBtn = document.getElementById('loadingRulesBtn');
const copilotRulesBtn = document.getElementById('copilotRulesBtn');
const requestTemplateBtn = document.getElementById('requestTemplateBtn');
const versionNumbersBtn = document.getElementById('versionNumbersBtn');
const rulesStorageBtn = document.getElementById('rulesStorageBtn');
const payMatchBtn = document.getElementById('payMatchBtn');
const payMatchPanel = document.getElementById('payMatchPanel');
const closePayMatchBtn = document.getElementById('closePayMatchBtn');
const loadPayMatchBtn = document.getElementById('loadPayMatchBtn');
const payMatchStatus = document.getElementById('payMatchStatus');
const payMatchBlock1Body = document.getElementById('payMatchBlock1Body');
const payMatchBlock2Body = document.getElementById('payMatchBlock2Body');
const payMatchBlock3Body = document.getElementById('payMatchBlock3Body');
const rulesPanel = document.getElementById('rulesPanel');
const loadingRulesPanel = document.getElementById('loadingRulesPanel');
const copilotRulesPanel = document.getElementById('copilotRulesPanel');
const requestTemplatePanel = document.getElementById('requestTemplatePanel');
const versionNumbersPanel = document.getElementById('versionNumbersPanel');
const rulesStoragePanel = document.getElementById('rulesStoragePanel');
const closeRulesBtn = document.getElementById('closeRulesBtn');
const rulesTextInput = document.getElementById('rulesTextInput');
const versionNumbersInput = document.getElementById('versionNumbersInput');
const rulesStorageLocationsInput = document.getElementById('rulesStorageLocationsInput');
const enrichStatusLabel = document.getElementById('enrichStatusLabel');
const saveRulesBtn = document.getElementById('saveRulesBtn');
const rulesSaveMsg = document.getElementById('rulesSaveMsg');
const lastRefreshLabel = document.getElementById('lastRefreshLabel');

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
const RULES_STATUS_REPO_FILE_URL = 'https://github.com/pavel9619229-cmyk/KP/blob/main/data/status_rules.json';
const ENRICHMENT_FLAG_KEYS = [
  'managerFilled',
  'productSpecified',
  'kpSent',
  'receiptConfirmed',
  'edoSent',
  'rejected',
  'problem',
  'shipmentPending',
  'invoiceCreated',
  'paymentReceived',
];
const PRESERVE_ON_NULL_KEYS = [...ENRICHMENT_FLAG_KEYS, 'customerName', 'additionalInfoFirstLine'];
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

function isCurrentOriginSource(url) {
  try {
    return new URL(url, window.location.origin).origin === window.location.origin;
  } catch {
    return false;
  }
}
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
const HUMAN_FIELD_LABELS = {
  problem: 'Проблема',
  rejected: 'Отказ',
  invoiceCreated: 'Накладная создана',
  paymentReceived: 'Оплата получена',
  edoSent: 'В ЭДО отправлено',
  shipmentPending: 'Отгрузить',
  receiptConfirmed: 'Клиент КП увидел',
  kpSent: 'КП отправлено',
  clientFilled: 'Клиент заполнен',
  managerFilled: 'Менеджер заполнен',
  productSpecified: 'Товар указан',
  priceFilled: 'Цена в первой строке товара указана',
};
const DEFAULT_STATUS_RULES_TEXT = [
  '# Формат 1 (простой): статус СТАТУС устанавливается, если Поле - ДА, Поле - НЕТ',
  '# Формат 2 (технический, тоже поддерживается): condition AND condition -> STATUS',
  '# Поля: Проблема, Отказ, Накладная создана, Оплата получена, В ЭДО отправлено, Отгрузить, Клиент КП увидел, КП отправлено, Клиент заполнен, Менеджер заполнен, Товар указан',
  '',
  'статус ПРОБЛЕМА устанавливается, если Проблема - ДА',
  'статус ОТКАЗ устанавливается, если Отказ - ДА',
  'статус ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО устанавливается, если Накладная создана - ДА, Оплата получена - ДА, В ЭДО отправлено - ДА',
  'статус ЖДЕМ ОПЛАТУ устанавливается, если Накладная создана - ДА, В ЭДО отправлено - ДА, Оплата получена - НЕТ',
  'статус ОТПРАВИТЬ В ЭДО устанавливается, если Накладная создана - ДА, В ЭДО отправлено - НЕТ',
  'статус ОТГРУЗИТЬ устанавливается, если Отгрузить - ДА',
  'статус КЛИЕНТ ДУМАЕТ устанавливается, если Клиент КП увидел - ДА',
  'статус ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП устанавливается, если КП отправлено - ДА',
  'статус ОТПРАВИТЬ КЛИЕНТУ устанавливается, если Клиент заполнен - ДА, Менеджер заполнен - ДА, Товар указан - ДА',
].join('\n');

let rows = [];
let lastFingerprint = '';
let lastSyncAt = null;
let frontendLoadedVersion = null;
let ws = null;
let wsActive = false;
const TABLE_COLUMN_COUNT = 17;
let statusRules = createDefaultStatusRules();

function hasMeaningfulValue(value) {
  if (value === null || value === undefined) return false;
  if (typeof value === 'string') return value.trim().length > 0;
  return true;
}

function mergeRowsPreservingKnown(nextRows, prevRows) {
  if (!Array.isArray(nextRows) || !nextRows.length || !Array.isArray(prevRows) || !prevRows.length) {
    return nextRows;
  }

  const prevByNumber = new Map(prevRows.map((row) => [String(row?.number || ''), row]));
  return nextRows.map((row) => {
    const key = String(row?.number || '');
    const prev = prevByNumber.get(key);
    if (!prev) return row;

    const merged = { ...row };
    for (const field of PRESERVE_ON_NULL_KEYS) {
      if (!hasMeaningfulValue(merged[field]) && hasMeaningfulValue(prev[field])) {
        merged[field] = prev[field];
      }
    }
    return merged;
  });
}

function getEnrichmentStats(data) {
  const rowsList = Array.isArray(data) ? data : [];
  if (!rowsList.length) {
    return { percent: 100, readyRows: 0, totalRows: 0, unknownFlags: 0 };
  }

  let readyRows = 0;
  let knownFlags = 0;
  let unknownFlags = 0;

  for (const row of rowsList) {
    let rowReady = true;
    for (const field of ENRICHMENT_FLAG_KEYS) {
      const value = row?.[field];
      if (value === true || value === false) {
        knownFlags += 1;
      } else {
        unknownFlags += 1;
        rowReady = false;
      }
    }
    if (rowReady) readyRows += 1;
  }

  const totalFlags = knownFlags + unknownFlags;
  const percent = totalFlags > 0 ? Math.round((knownFlags / totalFlags) * 100) : 100;
  return { percent, readyRows, totalRows: rowsList.length, unknownFlags };
}

function renderEnrichmentStatus(data) {
  if (!enrichStatusLabel) return;

  const stats = getEnrichmentStats(data);
  enrichStatusLabel.classList.remove('is-busy', 'is-ready');

  if (!stats.totalRows) {
    enrichStatusLabel.textContent = 'Ожидание данных...';
    return;
  }

  if (stats.unknownFlags > 0) {
    enrichStatusLabel.classList.add('is-busy');
    enrichStatusLabel.textContent = `Дообогащение признаков: ${stats.percent}% (${stats.readyRows}/${stats.totalRows} строк готовы)`;
    return;
  }

  enrichStatusLabel.classList.add('is-ready');
  enrichStatusLabel.textContent = `Дообогащение завершено: ${stats.percent}% (${stats.readyRows}/${stats.totalRows} строк)`;
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

function getManagerName(row) {
  const manager = String(row?.managerName || row?.manager || row?.['Менеджер'] || '').trim();
  return manager || 'НЕ ОПРЕДЕЛЕН';
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

function isHumanReadableRulesText(text) {
  const lines = String(text || '').split(/\r?\n/);
  return lines.some((line) => /^\s*статус\s+.+\s+устанавливается,\s*если\s+/i.test(line));
}

function conditionToHumanText(condition) {
  const fieldLabel = HUMAN_FIELD_LABELS[condition.field] || condition.field;
  const value = condition.operator === 'is_false' || condition.operator === 'is_not_true' ? 'НЕТ' : 'ДА';
  return `${fieldLabel} - ${value}`;
}

function rulesToHumanText(rules) {
  const header = [
    '# Формат: статус СТАТУС устанавливается, если Поле - ДА/НЕТ, Поле - ДА/НЕТ',
    '# Редактируйте строки ниже по одной на правило',
    '',
  ];
  const lines = rules.map((rule) => {
    const conditions = (rule.conditions || []).map(conditionToHumanText).join(', ');
    return `статус ${rule.label} устанавливается, если ${conditions}`;
  });
  return [...header, ...lines].join('\n');
}

function setRulesMessage(text, type = '') {
  if (!rulesSaveMsg) return;
  rulesSaveMsg.textContent = text || '';
  rulesSaveMsg.classList.remove('is-ok', 'is-error');
  if (type === 'ok') rulesSaveMsg.classList.add('is-ok');
  if (type === 'error') rulesSaveMsg.classList.add('is-error');
}

function renderRulesStorageLocations() {
  if (!rulesStorageLocationsInput) return;

  const origin = window.location.origin;
  const lines = [
    '1) После нажатия "Сохранить" актуальная версия записывается сервером в файл:',
    '   STATUS_RULES_FILE (по умолчанию: data/status_rules.json).',
    '',
    '2) API-адрес чтения/проверки актуальной версии после сохранения:',
    `   ${origin}/api/status-rules`,
    '   (резервный источник: https://onec-kp-realtime.onrender.com/api/status-rules).',
    '',
    '3) Адрес файла в GitHub (становится актуальным после commit + push):',
    `   ${RULES_STATUS_REPO_FILE_URL}`,
  ];

  rulesStorageLocationsInput.value = lines.join('\n');
}

function formatVersionMoment(value) {
  const raw = String(value || '').trim();
  if (!raw) return 'n/a';
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return 'n/a';
  return date.toLocaleString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatVersionValue(value, moment) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return 'n/a';
  const formattedMoment = formatVersionMoment(moment);
  return formattedMoment === 'n/a' ? String(parsed) : `${parsed} | ${formattedMoment}`;
}

function renderVersionNumbers(info = {}) {
  if (!versionNumbersInput) return;

  const lines = [
    {
      label: 'frontendLoadedVersion:',
      version: formatVersionValue(frontendLoadedVersion, info.runtimeGeneratedAt),
      desc: 'Версия, которую фронтенд реально сейчас показывает пользователю.',
      extraSpaces: 3,
    },
    {
      label: 'last1cLoadedVersion:',
      version: formatVersionValue(info.last1cLoadedVersion, info.last1cLoadedAt),
      desc: 'Последняя успешная версия, полученная напрямую из 1С.',
      extraSpaces: 5,
    },
    {
      label: 'lastGithubBackupVersion:',
      version: formatVersionValue(info.lastGithubBackupVersion, info.githubGeneratedAt),
      desc: 'Последняя версия, доступная в GitHub как резерв для восстановления.',
      extraSpaces: 1,
    },
  ];

  // Find max version string width for right-alignment
  const maxVersionWidth = Math.max(...lines.map(l => l.version.length));

  versionNumbersInput.value = lines
    .map(l => `${l.label}${' '.repeat(1 + (l.extraSpaces || 0))}${l.version.padStart(maxVersionWidth, ' ')} — ${l.desc}`)
    .join('\n');
}

async function loadVersionInfo() {
  const sources = ['/api/kp/version-info', 'https://onec-kp-realtime.onrender.com/api/kp/version-info'];

  let response = null;
  let lastError = null;
  for (const src of sources) {
    try {
      const r = await fetch(src, { cache: 'no-store', credentials: 'include' });
      if (r.status === 401 && isCurrentOriginSource(src)) {
        window.location.href = '/login';
        return null;
      }
      if (r.status === 401) throw new Error('Unauthorized fallback source');
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      response = r;
      break;
    } catch (e) {
      lastError = e;
    }
  }

  if (!response) throw lastError || new Error('Нет доступа к версии загрузки');
  return response.json();
}

async function refreshVersionNumbers(syncFrontendVersion = false) {
  try {
    const info = await loadVersionInfo();
    if (!info) return;
    // Always sync frontendLoadedVersion from API
    const nextVersion = Number(info.frontendRecommendedVersion);
    frontendLoadedVersion = Number.isFinite(nextVersion) && nextVersion > 0 ? nextVersion : null;
    renderVersionNumbers(info);
  } catch (err) {
    if (versionNumbersInput) {
      versionNumbersInput.value = `Не удалось загрузить версии: ${err.message}`;
    }
  }
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
    if (boolValue === null) {
      return { error: `Значение должно быть true/false (или да/нет): ${technicalMatch[3]}` };
    }

    let operator = 'is_true';
    if (technicalMatch[2] === '=' && boolValue === true) operator = 'is_true';
    if (technicalMatch[2] === '=' && boolValue === false) operator = 'is_false';
    if (technicalMatch[2] === '!=' && boolValue === true) operator = 'is_not_true';
    if (technicalMatch[2] === '!=' && boolValue === false) operator = 'is_not_false';

    return { condition: { field, operator } };
  }

  const humanMatch = raw.match(/^(.+?)\s*[-:=]\s*(.+)$/);
  if (!humanMatch) {
    return { error: `Некорректное условие: ${token}` };
  }

  const field = resolveRuleField(humanMatch[1]);
  if (!field) return { error: `Неизвестное поле: ${humanMatch[1]}` };

  const boolValue = parseBooleanToken(humanMatch[2]);
  if (boolValue === null) {
    return { error: `Значение должно быть да/нет (или true/false): ${humanMatch[2]}` };
  }

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
  const rawText = String(text || '').replace(/\r\n/g, '\n');
  const lines = rawText.split('\n');
  const rules = [];
  const errors = [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line || line.startsWith('#')) continue;

    const humanRule = parseHumanRuleLine(line);
    if (humanRule) {
      if (humanRule.error) {
        errors.push(`Строка ${i + 1}: ${humanRule.error}`);
      } else {
        rules.push(humanRule.rule);
      }
      continue;
    }

    const parts = line.split('->');
    if (parts.length < 2) {
      errors.push(`Строка ${i + 1}: нет формата правила (используйте "статус ... устанавливается, если ..." или "... -> ...")`);
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
  const facts = deriveStatusFacts(row);
  for (const rule of statusRules) {
    const matchMode = rule.matchMode === 'any' ? 'any' : 'all';
    const isRuleMatched = matchMode === 'any'
      ? rule.conditions.some((condition) => matchesRuleCondition(facts, condition))
      : rule.conditions.every((condition) => matchesRuleCondition(facts, condition));
    if (isRuleMatched) {
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
      const displayText = isHumanReadableRulesText(rulesText) ? rulesText : rulesToHumanText(parsed.rules);
      renderRulesEditor(displayText);
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
    renderRulesStorageLocations();
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

function openRulesStoragePanel() {
  if (!rulesStoragePanel || !rulesStorageBtn) return;
  rulesStoragePanel.hidden = false;
  rulesStorageBtn.textContent = 'СКРЫТЬ МЕСТА ХРАНЕНИЯ ПРАВИЛ СТАТУСОВ';
}

function closeRulesStoragePanel() {
  if (!rulesStoragePanel || !rulesStorageBtn) return;
  rulesStoragePanel.hidden = true;
  rulesStorageBtn.textContent = 'МЕСТА ХРАНЕНИЯ ПРАВИЛ СТАТУСОВ';
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
      <td>${escapeHtml(getManagerName(r))}</td>
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
      <td>${formatFlag(getFlag(r, ['priceFilled']))}</td>
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
  const manager = managerFilter.value;
  const status = statusFilter.value;

  const filtered = rows.filter((r) => {
    const rowStatus = computeKpStatus(r);
    const rowManager = getManagerName(r);
    const byStatus = !status || rowStatus === status;
    const byManager = !manager || rowManager === manager;
    const text = `${r.number || ''} ${r.customerName || ''} ${rowManager} ${rowStatus} ${r.additionalInfoFirstLine || ''}`.toLowerCase();
    const byText = !q || text.includes(q);
    return byStatus && byManager && byText;
  });

  render(filtered);
}

function fillManagers(data) {
  const selectedManager = managerFilter.value;
  managerFilter.innerHTML = '<option value="">Все менеджеры</option>';

  const managers = [...new Set(data.map((r) => getManagerName(r)))].sort((a, b) => a.localeCompare(b, 'ru'));
  for (const m of managers) {
    const option = document.createElement('option');
    option.value = m;
    option.textContent = m;
    managerFilter.appendChild(option);
  }

  if ([...managerFilter.options].some((o) => o.value === selectedManager)) {
    managerFilter.value = selectedManager;
  }
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
      const r = await fetch(src, { cache: 'no-store', credentials: 'include' });
      if (r.status === 401 && isCurrentOriginSource(src)) {
        window.location.href = '/login';
        return [];
      }
      if (r.status === 401) throw new Error('Unauthorized fallback source');
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
  const mergedRows = mergeRowsPreservingKnown(nextRows, rows);
  const nextFingerprint = fingerprint(mergedRows);
  if (nextFingerprint !== lastFingerprint) {
    rows = mergedRows;
    lastFingerprint = nextFingerprint;
    fillManagers(rows);
    fillStatuses(rows);
  }

  lastSyncAt = syncedAt || new Date();
  renderEnrichmentStatus(rows);
  applyFilters();
}

async function refreshData(initial = false) {
  try {
    const nextRows = await loadRows();
    if (!Array.isArray(nextRows)) return;
    setRows(nextRows, new Date());
    if (!versionNumbersPanel?.hidden) {
      await refreshVersionNumbers(true);
    }
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
        if (!versionNumbersPanel?.hidden) {
          refreshVersionNumbers(true);
        }
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

async function pollLastRefresh() {
  const urls = ['/healthz', 'https://onec-kp-realtime.onrender.com/healthz'];
  for (const url of urls) {
    try {
      const r = await fetch(url, { cache: 'no-store' });
      if (!r.ok) continue;
      const d = await r.json();
      if (d.lastRefresh && lastRefreshLabel) {
        const ts = d.lastRefresh.replace('T', ' ').slice(0, 19);
        lastRefreshLabel.textContent = `Обновление из 1С: ${ts} MSK`;
      }
      return;
    } catch { /* next source */ }
  }
}

async function init() {
  renderRulesStorageLocations();
  await loadStatusRulesFromServer();
  await refreshData(true);
  connectWebSocket();
  setInterval(() => {
    if (!wsActive) refreshData(false);
  }, REFRESH_INTERVAL_MS);
  pollLastRefresh();
  setInterval(pollLastRefresh, 30000);
}

searchInput.addEventListener('input', applyFilters);
managerFilter.addEventListener('change', applyFilters);
statusFilter.addEventListener('change', applyFilters);
resetBtn.addEventListener('click', () => {
  searchInput.value = '';
  managerFilter.value = '';
  statusFilter.value = '';
  applyFilters();
});

rulesBtn.addEventListener('click', () => {
  if (rulesPanel.hidden) openRulesPanel();
  else closeRulesPanel();
});

loadingRulesBtn?.addEventListener('click', () => {
  if (loadingRulesPanel?.hidden) {
    loadingRulesPanel.hidden = false;
  } else {
    loadingRulesPanel.hidden = true;
  }
});

copilotRulesBtn?.addEventListener('click', () => {
  if (copilotRulesPanel?.hidden) {
    copilotRulesPanel.hidden = false;
    copilotRulesBtn.textContent = 'СКРЫТЬ ПРАВИЛА ОГРАНИЧЕНИЯ ДЛЯ COPILOT';
  } else {
    copilotRulesPanel.hidden = true;
    copilotRulesBtn.textContent = 'ПРАВИЛА ОГРАНИЧЕНИЯ ДЛЯ COPILOT';
  }
});

requestTemplateBtn?.addEventListener('click', () => {
  if (requestTemplatePanel?.hidden) {
    requestTemplatePanel.hidden = false;
    requestTemplateBtn.textContent = 'СКРЫТЬ ШАБЛОН ДЛЯ КАЖДОГО ЗАПРОСА';
  } else {
    requestTemplatePanel.hidden = true;
    requestTemplateBtn.textContent = 'ШАБЛОН ДЛЯ КАЖДОГО ЗАПРОСА';
  }
});

versionNumbersBtn?.addEventListener('click', async () => {
  if (versionNumbersPanel?.hidden) {
    versionNumbersPanel.hidden = false;
    await refreshVersionNumbers(false);
  } else {
    versionNumbersPanel.hidden = true;
  }
});

rulesStorageBtn?.addEventListener('click', () => {
  if (rulesStoragePanel?.hidden) openRulesStoragePanel();
  else closeRulesStoragePanel();
});

// ── Payment match table ──────────────────────────────────────────────────────

function renderPayMatchTable(data) {
  if (!payMatchBlock1Body || !payMatchBlock2Body || !payMatchBlock3Body) return;
  payMatchBlock1Body.innerHTML = '';
  payMatchBlock2Body.innerHTML = '';
  payMatchBlock3Body.innerHTML = '';

  const rows = data.rows || [];

  const toNumDesc = (value) => {
    const n = Number.parseInt(String(value || '').replace(/\D+/g, ''), 10);
    return Number.isFinite(n) ? n : -1;
  };

  // Block 1: unique pairs (KP, order), sorted by KP desc then order desc.
  const block1Map = new Map();
  rows.forEach((r) => {
    const kp = String(r.kpNum || '').trim();
    const order = String(r.orderNum || '').trim();
    if (!kp && !order) return;
    const key = `${kp}|${order}`;
    if (!block1Map.has(key)) block1Map.set(key, { kp, order });
  });
  const block1Rows = Array.from(block1Map.values()).sort((a, b) => {
    const kpCmp = toNumDesc(b.kp) - toNumDesc(a.kp);
    if (kpCmp !== 0) return kpCmp;
    return toNumDesc(b.order) - toNumDesc(a.order);
  });

  block1Rows.forEach((r) => {
    const tr = document.createElement('tr');
    const tdKp = document.createElement('td');
    const tdOrder = document.createElement('td');
    tdKp.textContent = r.kp || 'не определено';
    tdOrder.textContent = r.order || 'не определено';
    if (!r.kp) tdKp.classList.add('pm-empty');
    if (!r.order) tdOrder.classList.add('pm-empty');
    tr.append(tdKp, tdOrder);
    payMatchBlock1Body.appendChild(tr);
  });

  // Block 2: unique pairs (pay, purpose), sorted by pay desc.
  const block2Map = new Map();
  rows.forEach((r) => {
    const pay = String(r.payNum || '').trim();
    const purpose = String(r.purposeNum || '').trim();
    if (!pay && !purpose) return;
    const key = `${pay}|${purpose}`;
    if (!block2Map.has(key)) block2Map.set(key, { pay, purpose });
  });
  const block2Rows = Array.from(block2Map.values()).sort((a, b) => toNumDesc(b.pay) - toNumDesc(a.pay));

  block2Rows.forEach((r) => {
    const tr = document.createElement('tr');
    const tdPay = document.createElement('td');
    const tdPurpose = document.createElement('td');
    tdPay.textContent = r.pay || 'не определено';
    tdPurpose.textContent = r.purpose || 'не определено';
    if (!r.pay) tdPay.classList.add('pm-empty');
    if (!r.purpose) tdPurpose.classList.add('pm-empty');
    tr.append(tdPay, tdPurpose);
    payMatchBlock2Body.appendChild(tr);
  });

  // Block 3: KP numbers (descending) whose linked order number appears in block2 purposeNum list.
  // purposeNum in block2 is extracted from НазначениеПлатежа text like "УТ-199" → "199" = order number.
  // purposeNum may be comma-separated ("198, 199") — split before building the set.
  const purposeNumSet = new Set();
  block2Map.forEach((r) => {
    if (r.purpose) r.purpose.split(',').forEach((s) => { const t = s.trim(); if (t) purposeNumSet.add(t); });
  });
  // For each KP in block1, check if any of its order numbers appears in purposeNumSet.
  const block3KpSet = new Set();
  block1Map.forEach(({ kp, order }) => {
    if (kp && order && purposeNumSet.has(order)) block3KpSet.add(kp);
  });
  const block3Rows = Array.from(block3KpSet).sort((a, b) => toNumDesc(b) - toNumDesc(a));

  if (block3Rows.length) {
    block3Rows.forEach((kp) => {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.textContent = kp;
      tr.appendChild(td);
      payMatchBlock3Body.appendChild(tr);
    });
  } else {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.textContent = 'нет совпадений';
    td.classList.add('pm-empty');
    tr.appendChild(td);
    payMatchBlock3Body.appendChild(tr);
  }

  if (!block1Rows.length) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = 2;
    td.textContent = 'нет данных';
    td.classList.add('pm-empty');
    tr.appendChild(td);
    payMatchBlock1Body.appendChild(tr);
  }

  if (!block2Rows.length) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = 2;
    td.textContent = 'нет данных';
    td.classList.add('pm-empty');
    tr.appendChild(td);
    payMatchBlock2Body.appendChild(tr);
  }

  if (payMatchStatus) {
    payMatchStatus.textContent =
      `Блок 1: ${block1Rows.length} строк | Блок 2: ${block2Rows.length} строк | ` +
      `Заказы: ${data.ordersScanComplete ? 'полный скан' : 'неполный скан'}, ` +
      `Платежи: ${data.paymentsScanComplete ? 'полный скан' : 'неполный скан'}`;
  }
}

async function loadPayMatchTable() {
  if (!loadPayMatchBtn) return;
  loadPayMatchBtn.disabled = true;
  loadPayMatchBtn.textContent = 'Загружаю...';
  if (payMatchStatus) payMatchStatus.textContent = 'Сканирование 1С — подождите 1–2 минуты...';
  if (payMatchBlock1Body) payMatchBlock1Body.innerHTML = '';
  if (payMatchBlock2Body) payMatchBlock2Body.innerHTML = '';
  if (payMatchBlock3Body) payMatchBlock3Body.innerHTML = '';
  try {
    const resp = await fetch('/api/admin/payment-match-table', { credentials: 'include' });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    renderPayMatchTable(data);
  } catch (err) {
    if (payMatchStatus) payMatchStatus.textContent = `Ошибка: ${err.message}`;
  } finally {
    loadPayMatchBtn.disabled = false;
    loadPayMatchBtn.textContent = 'Загрузить / обновить';
  }
}

payMatchBtn?.addEventListener('click', () => {
  if (payMatchPanel?.hidden) {
    payMatchPanel.hidden = false;
    payMatchBtn.textContent = 'СКРЫТЬ СОПОСТАВЛЕНИЕ';
  } else {
    payMatchPanel.hidden = true;
    payMatchBtn.textContent = 'СОПОСТАВЛЕНИЕ ПЛАТЕЖЕЙ';
  }
});

closePayMatchBtn?.addEventListener('click', () => {
  if (payMatchPanel) payMatchPanel.hidden = true;
  if (payMatchBtn) payMatchBtn.textContent = 'СОПОСТАВЛЕНИЕ ПЛАТЕЖЕЙ';
});

loadPayMatchBtn?.addEventListener('click', loadPayMatchTable);

// ────────────────────────────────────────────────────────────────────────────

closeRulesBtn.addEventListener('click', closeRulesPanel);
saveRulesBtn.addEventListener('click', onSaveRulesClick);

init();
