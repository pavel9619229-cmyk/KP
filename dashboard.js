const searchInput = document.getElementById('searchInput');
const clearSearchBtn = document.getElementById('clearSearchBtn');
const managerFilter = document.getElementById('managerFilter');
const refreshBtn = document.getElementById('refreshBtn');
const newRequestBtn = document.getElementById('newRequestBtn');
const newRequestPanel = document.getElementById('newRequestPanel');
const closeRequestPanelBtn = document.getElementById('closeRequestPanelBtn');
const requestTextInput = document.getElementById('requestTextInput');
const submitRequestBtn = document.getElementById('submitRequestBtn');
const requestStatusMsg = document.getElementById('requestStatusMsg');
const themeBtn = document.getElementById('themeBtn');
const statusTabs = document.getElementById('statusTabs');
const updatedAtLabel = document.getElementById('updatedAtLabel');
const processClientStatusBtn = document.getElementById('processClientStatusBtn');
const processReceiptStatusBtn = document.getElementById('processReceiptStatusBtn');
const processThinkStatusBtn = document.getElementById('processThinkStatusBtn');
const stage1of4Btn = document.getElementById('stage1of4Btn');
const stage1of4TimeLabel = document.getElementById('stage1of4TimeLabel');
const stage4of4Btn = document.getElementById('stage4of4Btn');
const stage4of4TimeLabel = document.getElementById('stage4of4TimeLabel');

const boardContent = document.getElementById('boardContent');

let lastRefreshDurationSec = null;
const STATUS_PROCESSING_ALLOWED_LOGIN = 'info@10-16-5.ru';
let currentUsername = '';
let currentAllowedManagers = [];

function setUpdatedAtText(text) {
  if (!updatedAtLabel) return;
  updatedAtLabel.textContent = String(text || '');
}

function isInfoLogin() {
  return String(currentUsername || '').trim().toLowerCase() === STATUS_PROCESSING_ALLOWED_LOGIN;
}

function updateStatusProcessingButtonsVisibility() {
  const isAllowed = isInfoLogin();
  const targets = [processClientStatusBtn, processReceiptStatusBtn, processThinkStatusBtn];
  for (const btn of targets) {
    if (!btn) continue;
    btn.hidden = !isAllowed;
    btn.disabled = !isAllowed;
  }
}

function updateRefreshButtonVisibility() {
  if (!refreshBtn) return;
  // Кнопка ОБНОВИТЬ больше не показывается ни для одного логина, включая
  // info@10-16-5.ru — панель полностью заменена кнопками 1/4 и 4/4.
  refreshBtn.hidden = true;
  refreshBtn.disabled = true;
}

function updateStageButtonsVisibility() {
  const isAllowed = isInfoLogin();
  const targets = [stage1of4Btn, stage1of4TimeLabel, stage4of4Btn, stage4of4TimeLabel];
  for (const el of targets) {
    if (!el) continue;
    el.hidden = !isAllowed;
  }
  if (stage1of4Btn) stage1of4Btn.disabled = !isAllowed;
  if (stage4of4Btn) stage4of4Btn.disabled = !isAllowed;
}

function formatStageTimestamp(value) {
  if (!value) return '';
  const parsed = new Date(String(value).replace(' ', 'T'));
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function renderStageStatusLabel(labelEl, state) {
  if (!labelEl || !state) return;
  if (state.running) {
    const startedText = formatStageTimestamp(state.startedAt || state.requestedAt);
    labelEl.textContent = startedText ? `Выполняется... (с ${startedText})` : 'Выполняется...';
    return;
  }
  if (!state.finishedAt) {
    labelEl.textContent = 'Ещё не запускался';
    return;
  }
  const finishedText = formatStageTimestamp(state.finishedAt);
  if (state.lastOk === false) {
    labelEl.textContent = `Последний запуск: ${finishedText} — ошибка`;
  } else if (state.lastOk === true) {
    labelEl.textContent = `Последний запуск: ${finishedText} — успешно`;
  } else {
    labelEl.textContent = `Последний запуск: ${finishedText}`;
  }
}

// Исходный текст кнопок ("1/4", "4/4") — чтобы возвращать его, когда процесс не выполняется.
const STAGE_DEFAULT_TEXT = new Map();
if (stage1of4Btn) STAGE_DEFAULT_TEXT.set(stage1of4Btn, stage1of4Btn.textContent);
if (stage4of4Btn) STAGE_DEFAULT_TEXT.set(stage4of4Btn, stage4of4Btn.textContent);

// Кнопки, которыми в данный момент управляет активный runStageRefresh в ЭТОЙ вкладке —
// фоновый опрос ниже их текст/disabled не трогает (сам runStageRefresh уже это делает точнее).
const stageManualControl = new Set();

function applyStageState(state, btn, labelEl) {
  if (labelEl) renderStageStatusLabel(labelEl, state);
  if (!btn || stageManualControl.has(btn)) return;
  if (state?.running) {
    btn.textContent = 'ВЫПОЛНЯЕТСЯ...';
    btn.disabled = true;
  } else {
    btn.textContent = STAGE_DEFAULT_TEXT.get(btn) || btn.textContent;
    btn.disabled = !isInfoLogin();
  }
}

async function pollStageStatus(url, labelEl, btn) {
  try {
    const response = await fetch(url, { method: 'GET', credentials: 'include', cache: 'no-store' });
    if (!response.ok) return;
    const state = await response.json().catch(() => null);
    applyStageState(state, btn, labelEl);
  } catch {
    // ignore — next poll will retry
  }
}

const STAGE_STATUS_POLL_MS = 15000;
function startStageStatusPolling() {
  const poll = () => {
    if (!isInfoLogin()) return;
    pollStageStatus('/api/kp/refresh/stage1_4/status', stage1of4TimeLabel, stage1of4Btn);
    pollStageStatus('/api/kp/refresh/stage4_4/status', stage4of4TimeLabel, stage4of4Btn);
  };
  poll();
  setInterval(poll, STAGE_STATUS_POLL_MS);
}

// Запускает процесс на backend (POST startUrl), опрашивает statusUrl до завершения
// и, при успехе, перезагружает данные дашборда без смены активной вкладки/поиска.
async function runStageRefresh(startUrl, statusUrl, btn, labelEl) {
  if (!btn) return;
  const pollIntervalMs = 3000;
  // Небольшой запас сверх серверного hard deadline для этой стадии (~1260с у stage1/4).
  const maxWaitMs = 1400000;
  const maxAttempts = Math.ceil(maxWaitMs / pollIntervalMs);
  const originalText = btn.textContent;
  const startedAt = Date.now();
  let tickTimerId = null;

  const formatElapsed = (seconds) => {
    const mm = String(Math.floor(seconds / 60)).padStart(2, '0');
    const ss = String(seconds % 60).padStart(2, '0');
    return `${mm}:${ss}`;
  };

  const setRunningLabel = () => {
    if (!labelEl) return;
    const elapsedSec = Math.max(0, Math.floor((Date.now() - startedAt) / 1000));
    labelEl.textContent = `Выполняется... ${formatElapsed(elapsedSec)}`;
  };

  stageManualControl.add(btn);
  btn.disabled = true;
  btn.textContent = 'ВЫПОЛНЯЕТСЯ...';
  setRunningLabel();
  tickTimerId = setInterval(setRunningLabel, 1000);

  try {
    const startResponse = await fetch(startUrl, {
      method: 'POST',
      credentials: 'include',
      cache: 'no-store',
    });
    if (startResponse.status === 401) {
      window.location.href = '/login';
      return;
    }
    const startPayload = await startResponse.json().catch(() => ({}));
    if (!startResponse.ok || startPayload?.ok === false) {
      const details = startPayload?.detail || startPayload?.error || `HTTP ${startResponse.status}`;
      throw new Error(String(details));
    }

    let done = false;
    let lastState = startPayload;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      let statePayload;
      try {
        const stateResponse = await fetch(statusUrl, { method: 'GET', credentials: 'include', cache: 'no-store' });
        if (stateResponse.status === 401) {
          window.location.href = '/login';
          return;
        }
        if (!stateResponse.ok) {
          await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
          continue;
        }
        statePayload = await stateResponse.json().catch(() => ({}));
      } catch {
        await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
        continue;
      }

      lastState = statePayload;

      if (statePayload?.running) {
        setRunningLabel();
        await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
        continue;
      }

      if (statePayload?.finishedAt) {
        done = true;
        break;
      }

      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }

    if (tickTimerId) {
      clearInterval(tickTimerId);
      tickTimerId = null;
    }

    if (!done) {
      throw new Error(String(lastState?.lastError || 'Превышено время ожидания'));
    }

    renderStageStatusLabel(labelEl, lastState);

    if (lastState?.lastOk === false) {
      // Ошибка уже отражена в labelEl через renderStageStatusLabel — данные не перезагружаем.
      return;
    }

    const _savedTab = activeTab;
    const _savedSearch = searchInput.value;
    await refreshData(false);
    activeTab = _savedTab;
    searchInput.value = _savedSearch;
    updateClearSearchButton();
    renderBoard();
  } catch (error) {
    if (tickTimerId) {
      clearInterval(tickTimerId);
      tickTimerId = null;
    }
    if (labelEl) labelEl.textContent = `Ошибка: ${error.message}`;
  } finally {
    if (tickTimerId) clearInterval(tickTimerId);
    stageManualControl.delete(btn);
    btn.disabled = !isInfoLogin();
    btn.textContent = originalText;
  }
}

function updateLastDurationBtn() {}

const WS_RECONNECT_MS = 5000;
const THEME_STORAGE_KEY = 'kpDashboardThemeV1';
const EMBEDDED_SCALE_MAX_WIDTH = 420;
const EMBEDDED_FIXED_SCALE = 0.7;
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

function isCurrentOriginSource(url) {
  try {
    return new URL(url, window.location.origin).origin === window.location.origin;
  } catch {
    return false;
  }
}

let rows = [];
let ws = null;
let wsActive = false;
let activeTab = ALL_TAB_KEY;
let lastFingerprint = '';
let lastSyncAt = null;
let statusRules = createDefaultStatusRules();
let currentUserRole = 'manager';

updateRefreshButtonVisibility();

function applyEmbeddedViewportScale() {
  if (window.innerWidth <= EMBEDDED_SCALE_MAX_WIDTH) {
    document.documentElement.style.zoom = String(EMBEDDED_FIXED_SCALE);
    return;
  }

  if (document.documentElement.style.zoom === String(EMBEDDED_FIXED_SCALE)) {
    document.documentElement.style.zoom = '';
  }
}

applyEmbeddedViewportScale();

initTheme();

window.addEventListener('resize', applyEmbeddedViewportScale);

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

// NOTE: click handlers intentionally do not trigger any refresh process yet —
// only re-check status. Wiring the actual 1/4 and 4/4 processes to these
// buttons is a separate, explicitly-approved next step.
if (stage1of4Btn) {
  stage1of4Btn.addEventListener('click', () => {
    runStageRefresh('/api/kp/refresh/stage1_4', '/api/kp/refresh/stage1_4/status', stage1of4Btn, stage1of4TimeLabel);
  });
}
if (stage4of4Btn) {
  stage4of4Btn.addEventListener('click', () => {
    runStageRefresh('/api/kp/refresh/stage4_4', '/api/kp/refresh/stage4_4/status', stage4of4Btn, stage4of4TimeLabel);
  });
}

if (refreshBtn) {
  refreshBtn.addEventListener('click', async () => {
  const defaultLabel = 'ОБНОВИТЬ';
  const pollIntervalMs = 2000;
  // Backend manual refresh timeout is now 900s (15 min) to handle slow 1C API read timeouts.
  const maxWaitMs = 900000;
  const maxAttempts = Math.ceil(maxWaitMs / pollIntervalMs);
  const isAdmin = String(currentUserRole || '').toLowerCase() === 'admin';
  const startedAt = Date.now();
  let refreshTimerId = null;

  const formatElapsed = (seconds) => {
    const mm = String(Math.floor(seconds / 60)).padStart(2, '0');
    const ss = String(seconds % 60).padStart(2, '0');
    return `${mm}:${ss}`;
  };

  const setRefreshingLabel = () => {
    if (!isAdmin) {
      setUpdatedAtText('ИДЕТ ОБНОВЛЕНИЕ');
      return;
    }
    const elapsedSec = Math.max(0, Math.floor((Date.now() - startedAt) / 1000));
    setUpdatedAtText(formatElapsed(elapsedSec));
  };

  const setRefreshButtonText = (value) => {
    const text = String(value || '');
    // Hard guard: never show infra wake/restart wording in UI.
    if (/сервер\s+(просыпается|перезапускается)/i.test(text)) {
      refreshBtn.textContent = 'ОБНОВЛЕНИЕ...';
      return;
    }
    refreshBtn.textContent = text;
  };

  refreshBtn.disabled = true;
  setRefreshButtonText('ОБНОВЛЕНИЕ...');
  setRefreshingLabel();
  refreshTimerId = setInterval(setRefreshingLabel, 1000);

  try {
    // If server is sleeping (Render cold start), wait up to 90s for it to wake before starting
    let startResponse;
    for (let wake = 0; wake < 45; wake += 1) {
      startResponse = await fetch('/api/kp/refresh', {
        method: 'POST',
        credentials: 'include',
        cache: 'no-store',
      });
      if (startResponse.status !== 503 && startResponse.status !== 502 && startResponse.status !== 504) break;
      // Hide infra-level wake/restart wording in UI; keep a stable refresh state instead.
      setRefreshButtonText('ОБНОВЛЕНИЕ...');
      setRefreshingLabel();
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }

    if (startResponse.status === 401) {
      window.location.href = '/login';
      return;
    }

    const startPayload = await startResponse.json().catch(() => ({}));
    if (!startResponse.ok || startPayload?.ok === false) {
      const details = startPayload?.detail || startPayload?.error || `HTTP ${startResponse.status}`;
      throw new Error(String(details));
    }

    let done = false;
    let lastState = null;
    let consecutiveStatusErrors = 0;
    let restartRetries = 0;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      let stateResponse;
      let statePayload;
      try {
        stateResponse = await fetch('/api/kp/refresh/status', {
          method: 'GET',
          credentials: 'include',
          cache: 'no-store',
        });
        if (stateResponse.status === 401) {
          window.location.href = '/login';
          return;
        }
        if (stateResponse.status === 502 || stateResponse.status === 503 || stateResponse.status === 504) {
          // Keep waiting on transient gateway errors without alarming UI text.
          consecutiveStatusErrors += 1;
          setRefreshButtonText('ОБНОВЛЕНИЕ...');
          setRefreshingLabel();
          await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
          continue;
        }
        if (!stateResponse.ok) {
          throw new Error(`HTTP ${stateResponse.status}`);
        }
        statePayload = await stateResponse.json().catch(() => ({}));
        lastState = statePayload;
        consecutiveStatusErrors = 0;
        setRefreshingLabel();
      } catch (statusError) {
        consecutiveStatusErrors += 1;
        setRefreshButtonText('ОБНОВЛЕНИЕ...');
        setRefreshingLabel();
        await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
        continue;
      }

      if (statePayload?.running) {
        setRefreshingLabel();
        await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
        continue;
      }

      // Server restarted mid-refresh: it has no memory of our request.
      // Detect by: not running, no finishedAt, no lastOk, no lastError, no requestedAt.
      if (!statePayload.running && !statePayload.finishedAt && statePayload.lastOk == null && !statePayload.lastError && !statePayload.requestedAt) {
        if (restartRetries < 2) {
          restartRetries += 1;
          setRefreshButtonText('ОБНОВЛЕНИЕ...');
          setRefreshingLabel();
          await new Promise((resolve) => setTimeout(resolve, 3000));
          try {
            const retryResp = await fetch('/api/kp/refresh', { method: 'POST', credentials: 'include', cache: 'no-store' });
            if (retryResp.status === 401) { window.location.href = '/login'; return; }
          } catch { /* ignore, keep polling */ }
          await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
          continue;
        }
        // Gave up retrying after restarts — load whatever data is on server.
        done = true;
        break;
      }

      if (statePayload?.lastOk === true || (statePayload?.lastRefresh && !statePayload?.lastRefreshError)) {
        done = true;
        break;
      }

      if (statePayload?.finishedAt && !statePayload?.lastError && !statePayload?.lastRefreshError) {
        done = true;
        break;
      }

      if (statePayload?.lastError || statePayload?.lastRefreshError) {
        throw new Error(String(statePayload.lastError || statePayload.lastRefreshError));
      }

      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }

    if (!done) {
      // Final safety check: if backend is still running, has just completed,
      // or lost volatile status after restart, do not show a false timeout.
      try {
        const finalResp = await fetch('/api/kp/refresh/status', {
          method: 'GET',
          credentials: 'include',
          cache: 'no-store',
        });
        if (finalResp.ok) {
          const finalState = await finalResp.json().catch(() => ({}));
          if (finalState?.running) {
            const _savedTab = activeTab;
            const _savedSearch = searchInput.value;
            await refreshData(false);
            activeTab = _savedTab;
            searchInput.value = _savedSearch;
            updateClearSearchButton();
            renderBoard();
            done = true;
          } else if (
            finalState?.lastOk === true
            || (finalState?.lastRefresh && !finalState?.lastRefreshError)
            || (finalState?.finishedAt && !finalState?.lastError && !finalState?.lastRefreshError)
          ) {
            done = true;
          } else if (
            !finalState?.running
            && !finalState?.requestedAt
            && !finalState?.startedAt
            && !finalState?.finishedAt
            && finalState?.lastOk == null
            && !finalState?.lastError
            && !finalState?.lastRefreshError
          ) {
            // Render restart can wipe in-memory refresh status even if data is already updated.
            const _savedTab = activeTab;
            const _savedSearch = searchInput.value;
            await refreshData(false);
            activeTab = _savedTab;
            searchInput.value = _savedSearch;
            updateClearSearchButton();
            renderBoard();
            done = true;
          }
        }
      } catch {
        // Keep original behavior below if final status check fails.
      }
    }

    if (!done) {
      const details = lastState?.lastError || lastState?.lastRefreshError || 'Превышено время ожидания обновления';
      throw new Error(String(details));
    }

    lastRefreshDurationSec = Math.max(0, Math.floor((Date.now() - startedAt) / 1000));
    // После обновления длительности сразу обновляем отдельную кнопку
    updateLastDurationBtn();
    const _savedTab = activeTab;
    const _savedSearch = searchInput.value;
    await refreshData(false);
    // Восстанавливаем состояние фильтра и поиска после обновления
    activeTab = _savedTab;
    searchInput.value = _savedSearch;
    updateClearSearchButton();
    renderBoard();
  } catch (error) {
    setUpdatedAtText(`Ошибка обновления: ${error.message}`);
  } finally {
    if (refreshTimerId) {
      clearInterval(refreshTimerId);
      refreshTimerId = null;
    }
    refreshBtn.disabled = false;
    setRefreshButtonText(defaultLabel);
  }
  });
}

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

if (processClientStatusBtn) {
  processClientStatusBtn.addEventListener('click', async () => {
    const defaultLabel = 'Обработать статусы Отправить клиенту';
    const transientStatuses = new Set([502, 503, 504]);
    const maxAttempts = 3;
    processClientStatusBtn.disabled = true;
    processClientStatusBtn.textContent = 'ОБРАБОТКА...';

    try {
      let payload = {};
      let response = null;

      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        response = await fetch('/api/kp/process/send-to-client', {
          method: 'POST',
          credentials: 'include',
          cache: 'no-store',
        });

        if (response.status === 401) {
          window.location.href = '/login';
          return;
        }

        const rawBody = await response.text().catch(() => '');
        payload = {};
        if (rawBody) {
          try {
            payload = JSON.parse(rawBody);
          } catch {
            payload = { error: rawBody };
          }
        }

        if (response.ok) {
          break;
        }

        const details = payload?.detail || payload?.error || rawBody || `HTTP ${response.status}`;
        const canRetry = transientStatuses.has(response.status) && attempt < maxAttempts;
        if (!canRetry) {
          throw new Error(String(details));
        }

        setUpdatedAtText(`Временная ошибка ${response.status}, повтор ${attempt + 1}/${maxAttempts}...`);
        await new Promise((resolve) => setTimeout(resolve, 1500 * attempt));
      }

      if (!response || !response.ok) {
        throw new Error('Не удалось обработать статусы после повторов');
      }

      const processed = Number(payload?.processed || 0);
      const updated = Number(payload?.updated || 0);
      const skipped = Number(payload?.skipped || 0);
      const failed = Number(payload?.failed || 0);

      await refreshData(false);
      setUpdatedAtText(`Обработано: ${processed}; обновлено: ${updated}; пропущено: ${skipped}; ошибок: ${failed}`);
    } catch (error) {
      setUpdatedAtText(`Ошибка обработки статуса: ${error.message}`);
    } finally {
      processClientStatusBtn.disabled = false;
      processClientStatusBtn.textContent = defaultLabel;
    }
  });
}

if (processThinkStatusBtn) {
  processThinkStatusBtn.addEventListener('click', async () => {
    const defaultLabel = 'Обработать статусы Клиент думает';
    const transientStatuses = new Set([502, 503, 504]);
    const maxAttempts = 3;
    processThinkStatusBtn.disabled = true;
    processThinkStatusBtn.textContent = 'ОБРАБОТКА...';

    try {
      let payload = {};
      let response = null;

      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        response = await fetch('/api/kp/process/client-thinking-reminder', {
          method: 'POST',
          credentials: 'include',
          cache: 'no-store',
        });

        if (response.status === 401) {
          window.location.href = '/login';
          return;
        }

        const rawBody = await response.text().catch(() => '');
        payload = {};
        if (rawBody) {
          try {
            payload = JSON.parse(rawBody);
          } catch {
            payload = { error: rawBody };
          }
        }

        if (response.ok) {
          break;
        }

        const details = payload?.detail || payload?.error || rawBody || `HTTP ${response.status}`;
        const canRetry = transientStatuses.has(response.status) && attempt < maxAttempts;
        if (!canRetry) {
          throw new Error(String(details));
        }

        setUpdatedAtText(`Временная ошибка ${response.status}, повтор ${attempt + 1}/${maxAttempts}...`);
        await new Promise((resolve) => setTimeout(resolve, 1500 * attempt));
      }

      if (!response || !response.ok) {
        throw new Error('Не удалось обработать статусы после повторов');
      }

      const matched = Number(payload?.matched || 0);
      const sent = Number(payload?.sent || 0);
      const skipped = Number(payload?.skipped || 0);
      const failed = Number(payload?.failed || 0);

      setUpdatedAtText(`Найдено: ${matched}; отправлено: ${sent}; пропущено: ${skipped}; ошибок: ${failed}`);
    } catch (error) {
      setUpdatedAtText(`Ошибка обработки статуса: ${error.message}`);
    } finally {
      processThinkStatusBtn.disabled = false;
      processThinkStatusBtn.textContent = defaultLabel;
    }
  });
}

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

function linkifyPhones(text) {
  const escaped = escapeHtml(text);
  return escaped.replace(
    /(\+7|8)[\s\-.]?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{2}[\s\-.]?\d{2}/g,
    (match) => {
      const digits = match.replace(/\D/g, '');
      const normalized = digits.startsWith('8') ? '+7' + digits.slice(1) : '+' + digits;
      return `<a href="tel:${normalized}" class="phone-link">${match}</a>`;
    }
  );
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

const LABEL_LINE_GROUPS = {
  'ВСЕ КП': ['ВСЕ КП'],
  'ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП': ['ПРОВЕРИТЬ', 'ПОЛУЧЕНИЕ КП'],
  'ОТГРУЗИТЬ И ОТПРАВИТЬ В ЭДО': ['ОТГРУЗИТЬ И', 'ОТПРАВИТЬ', 'В ЭДО'],
  'ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО': ['ОТГРУЖЕНО,', 'ОФОРМЛЕНО', 'И ОПЛАЧЕНО'],
};

function renderStackedLabel(label) {
  const normalizedLabel = String(label || '').trim();
  const groupedLines = LABEL_LINE_GROUPS[normalizedLabel];
  const lines = Array.isArray(groupedLines)
    ? groupedLines
    : normalizedLabel.split(/\s+/).filter(Boolean);
  if (!lines.length) {
    return '';
  }
  return lines.map((line) => `<span class="status-tab__word">${escapeHtml(line)}</span>`).join('');
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

function formatUpdatedAtForRole(value) {
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
    return 'Нет данных';
  }
  const durationSuffix = lastRefreshDurationSec !== null ? ` (${formatElapsedStatic(lastRefreshDurationSec)})` : '';
  if (String(currentUserRole || '').toLowerCase() === 'admin') {
    return formatUpdatedAt(value) + durationSuffix;
  }
  return 'Обновлено' + durationSuffix;
}

function formatElapsedStatic(seconds) {
  const mm = Math.floor(seconds / 60);
  const ss = seconds % 60;
  if (mm === 0) return `${ss}с`;
  return `${mm}м ${ss}с`;
}

async function loadCurrentUserRole() {
  try {
    const response = await fetch('/api/auth/session', {
      method: 'GET',
      credentials: 'include',
      cache: 'no-store',
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      currentUserRole = 'manager';
      currentUsername = '';
      currentAllowedManagers = [];
      updateStatusProcessingButtonsVisibility();
      updateRefreshButtonVisibility();
      updateStageButtonsVisibility();
      updateLastDurationBtn();
      return;
    }
    const payload = await response.json().catch(() => ({}));
    const role = String(payload?.user?.role || '').trim().toLowerCase();
    currentUsername = String(payload?.user?.username || '').trim().toLowerCase();
    const allowedManagers = payload?.user?.allowedManagers;
    if (Array.isArray(allowedManagers)) {
      currentAllowedManagers = allowedManagers.map((value) => String(value || '').trim()).filter(Boolean);
    } else {
      currentAllowedManagers = [];
    }
    currentUserRole = role || 'manager';
    updateStatusProcessingButtonsVisibility();
    updateRefreshButtonVisibility();
    updateStageButtonsVisibility();
    updateLastDurationBtn();
  } catch {
    currentUserRole = 'manager';
    currentUsername = '';
    currentAllowedManagers = [];
    updateStatusProcessingButtonsVisibility();
    updateRefreshButtonVisibility();
    updateStageButtonsVisibility();
    updateLastDurationBtn();
  }
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
  if (hasRejectInComment(row) || getFlag(row, ['rejected']) === true) {
    chips.push({ label: 'Отказ', state: 'is-alert' });
  }

  return chips;
}

function renderTabs(counts, totalCount) {
  const orderedStatuses = getOrderedStatuses(counts);
  const primaryTabs = [
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
    'ОТКАЗ',
  ];

  const tabs = [{ key: ALL_TAB_KEY, label: 'ВСЕ КП', count: totalCount }];
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
      <span class="status-tab__label">${renderStackedLabel(tab.label)}</span>
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
  const query = searchInput.value.trim().toLowerCase();
  const manager = managerFilter.value;
  const rowsForCounts = rows.filter((row) => {
    if (!manager) return true;
    return getManagerName(row) === manager;
  });
  const counts = getStatusCounts(rowsForCounts);
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

  renderTabs(counts, rowsForCounts.length);

  setUpdatedAtText(formatUpdatedAtForRole(lastSyncAt));
  updateLastDurationBtn();

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
            <span class="kp-cell__value kp-card__note">${linkifyPhones(row.additionalInfoFirstLine || 'Без дополнительной информации')}</span>
          </div>
        </div>
      </article>
    `;
  }).join('');
}

function fillManagers(data) {
  const selectedManager = managerFilter.value;
  const managers = [...new Set((data || []).map((row) => getManagerName(row)))].sort((a, b) => a.localeCompare(b, 'ru'));
  let defaultLabel = 'ВСЕ';
  if (!isInfoLogin()) {
    if (currentAllowedManagers.length > 0) {
      defaultLabel = currentAllowedManagers[0];
    } else if (managers.length > 0) {
      defaultLabel = managers[0];
    } else {
      defaultLabel = 'Менеджер';
    }
  }
  managerFilter.innerHTML = `<option value="">${escapeHtml(defaultLabel)}</option>`;

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
      const nextResponse = await fetch(src, { cache: 'no-store', credentials: 'include' });
      if (nextResponse.status === 401 && isCurrentOriginSource(src)) {
        window.location.href = '/login';
        return [];
      }
      if (nextResponse.status === 401) {
        throw new Error('Unauthorized fallback source');
      }
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
    if (!Array.isArray(nextRows)) return;
    setRows(nextRows, new Date());
    // Clear any previous error state once data loads successfully
    if (boardContent.querySelector('.board-empty')) {
      renderBoard();
    }
  } catch (error) {
    if (initial) {
      boardContent.innerHTML = `<div class="board-empty">Не удалось загрузить данные: ${escapeHtml(error.message)}<br><small>Повторная попытка через несколько секунд…</small></div>`;
      setUpdatedAtText('Ошибка');
      // Keep status tags visible even when the first data request fails.
      renderTabs(getStatusCounts(rows), rows.length);
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

// Periodic HTTP ping to prevent Render free tier from sleeping the server.
// Render sleeps after 15 min of no HTTP requests; WS connections don't count.
const KEEP_ALIVE_MS = 10 * 60 * 1000; // 10 minutes
function startKeepAlive() {
  setInterval(async () => {
    try {
      await fetch('/api/kp/refresh/status', { method: 'GET', credentials: 'include', cache: 'no-store' });
    } catch {
      // ignore — next ping will retry
    }
  }, KEEP_ALIVE_MS);
}

async function init() {
  updateClearSearchButton();
  renderTabs(getStatusCounts(rows), rows.length);
  await loadCurrentUserRole();
  await loadStatusRulesFromServer();
  await refreshData(true);
  connectWebSocket();
  startKeepAlive();
  startStageStatusPolling();

  // При инициализации обновить кнопку длительности
  updateLastDurationBtn();
}

init();
