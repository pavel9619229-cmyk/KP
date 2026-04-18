const loginCard = document.getElementById('loginCard');
const editorCard = document.getElementById('editorCard');
const usernameInput = document.getElementById('username');
const passwordInput = document.getElementById('password');
const togglePasswordBtn = document.getElementById('togglePasswordBtn');
const loginBtn = document.getElementById('loginBtn');
const loginMsg = document.getElementById('loginMsg');
const rightsEditor = document.getElementById('rightsEditor');
const reloadBtn = document.getElementById('reloadBtn');
const saveBtn = document.getElementById('saveBtn');
const logoutBtn = document.getElementById('logoutBtn');
const editorMsg = document.getElementById('editorMsg');

function setMsg(node, text, isError = false) {
  node.textContent = text || '';
  node.className = `msg ${text ? (isError ? 'err' : 'ok') : ''}`;
}

function setLoggedIn(isLoggedIn) {
  loginCard.classList.toggle('hidden', isLoggedIn);
  editorCard.classList.toggle('hidden', !isLoggedIn);
}

function setPasswordVisibility(isVisible) {
  passwordInput.type = isVisible ? 'text' : 'password';
  if (togglePasswordBtn) {
    togglePasswordBtn.textContent = isVisible ? 'Скрыть' : 'Показать';
    togglePasswordBtn.setAttribute('aria-pressed', isVisible ? 'true' : 'false');
    togglePasswordBtn.setAttribute('aria-label', isVisible ? 'Скрыть пароль' : 'Показать пароль');
  }
}

async function api(url, options = {}) {
  const resp = await fetch(url, {
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    ...options,
  });
  const payload = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(String(payload?.detail || payload?.error || `HTTP ${resp.status}`));
  }
  return payload;
}

async function loadRights() {
  const data = await api('/api/admin/rights');
  rightsEditor.value = JSON.stringify({ users: data.users || [] }, null, 2);
  setMsg(editorMsg, `Загружено. updatedAt: ${data.updatedAt || 'n/a'}`);
}

async function checkSession() {
  try {
    const session = await api('/api/admin/session', { method: 'GET' });
    if (session.ok) {
      setLoggedIn(true);
      await loadRights();
      return;
    }
  } catch {
    // ignore
  }
  setLoggedIn(false);
}

loginBtn.addEventListener('click', async () => {
  try {
    loginBtn.disabled = true;
    setMsg(loginMsg, 'Проверка...');
    await api('/api/admin/login', {
      method: 'POST',
      body: JSON.stringify({
        username: String(usernameInput.value || '').trim(),
        password: String(passwordInput.value || ''),
      }),
    });
    passwordInput.value = '';
    setLoggedIn(true);
    setMsg(loginMsg, '');
    await loadRights();
  } catch (err) {
    setMsg(loginMsg, `Ошибка входа: ${err.message}`, true);
  } finally {
    loginBtn.disabled = false;
  }
});

reloadBtn.addEventListener('click', async () => {
  try {
    setMsg(editorMsg, 'Читаю...');
    await loadRights();
  } catch (err) {
    setMsg(editorMsg, `Ошибка чтения: ${err.message}`, true);
  }
});

saveBtn.addEventListener('click', async () => {
  try {
    saveBtn.disabled = true;
    setMsg(editorMsg, 'Сохраняю...');
    const parsed = JSON.parse(String(rightsEditor.value || '{}'));
    const users = Array.isArray(parsed.users) ? parsed.users : [];
    const result = await api('/api/admin/rights', {
      method: 'PUT',
      body: JSON.stringify({ users }),
    });
    rightsEditor.value = JSON.stringify({ users: result.users || [] }, null, 2);
    setMsg(editorMsg, `Сохранено. updatedAt: ${result.updatedAt || 'n/a'}`);
  } catch (err) {
    setMsg(editorMsg, `Ошибка сохранения: ${err.message}`, true);
  } finally {
    saveBtn.disabled = false;
  }
});

logoutBtn.addEventListener('click', async () => {
  try {
    await api('/api/admin/logout', { method: 'POST' });
  } catch {
    // ignore
  }
  setLoggedIn(false);
  setMsg(editorMsg, '');
  setMsg(loginMsg, 'Вы вышли.');
});

checkSession();

if (togglePasswordBtn) {
  togglePasswordBtn.addEventListener('click', () => {
    const isVisible = passwordInput.type !== 'password';
    setPasswordVisibility(!isVisible);
  });
  setPasswordVisibility(false);
}
