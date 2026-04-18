const usernameInput = document.getElementById('username');
const passwordInput = document.getElementById('password');
const loginBtn = document.getElementById('loginBtn');
const msg = document.getElementById('msg');

function dashboardUrlForRole(role) {
  return String(role || '').toLowerCase() === 'admin' ? '/admin/dashboard' : '/dashboard';
}

function setMsg(text) {
  msg.textContent = String(text || '');
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

async function checkSession() {
  try {
    const session = await api('/api/auth/session', { method: 'GET' });
    if (session?.ok) {
      window.location.href = dashboardUrlForRole(session?.user?.role);
    }
  } catch {
    // keep login page
  }
}

loginBtn.addEventListener('click', async () => {
  const username = String(usernameInput.value || '').trim();
  const password = String(passwordInput.value || '');
  if (!username || !password) {
    setMsg('Введите логин и пароль.');
    return;
  }

  try {
    loginBtn.disabled = true;
    setMsg('Проверка...');
    const result = await api('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    window.location.href = dashboardUrlForRole(result?.user?.role);
  } catch (err) {
    setMsg(`Ошибка входа: ${err.message}`);
  } finally {
    loginBtn.disabled = false;
  }
});

checkSession();
