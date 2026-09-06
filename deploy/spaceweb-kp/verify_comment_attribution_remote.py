import os
import sys
from pathlib import Path

sys.path.insert(0, '/opt/kp-api')
import kp_max_runtime as runtime
import spaceweb_app as app
import kp_max_counterparties as cp

TEST_DB = Path('/tmp/kp_max_runtime_attribution_test.sqlite')
for suffix in ('', '-wal', '-shm'):
    try: Path(str(TEST_DB) + suffix).unlink()
    except FileNotFoundError: pass
runtime.DB_PATH = TEST_DB
runtime._INITIALIZED_PATH = ''
runtime.remember_user('attr-user', 'Иван', 'Петров')
marked = runtime.attributed_text('attr-user', 'Новый текст', 1788691860)
assert marked.startswith('[') and 'Иван Петров] Новый текст' in marked
print('ATTRIBUTION_FORMAT_OK=true')
state = {'value': 'СТАРЫЙ КОММЕНТАРИЙ'}
orig_fetch = app._fetch_comment_raw_by_ref
orig_patch = app.requests.patch
orig_cache = app._update_comment_memory_cache
orig_audit = app._audit_comment_edit
class Resp:
    status_code = 204
    text = ''
def fake_fetch(ref):
    return state['value']
def fake_patch(url, *args, **kwargs):
    state['value'] = str((kwargs.get('json') or {}).get('Комментарий') or '')
    return Resp()
app._fetch_comment_raw_by_ref = fake_fetch
app.requests.patch = fake_patch
app._update_comment_memory_cache = lambda *a, **k: None
app._audit_comment_edit = lambda *a, **k: None
with app._EDIT_LOCK:
    app._EDIT_SESSIONS['attr-user'] = {'number':'588','refKey':'mock-kp','stage':'confirm','newText':'НОВАЯ СТРОКА','expiresAt':9999999999}
try:
    saved = app._commit_comment_edit('attr-user', 'user')
    assert state['value'].startswith('[')
    assert 'Иван Петров] НОВАЯ СТРОКА' in state['value']
    assert state['value'].endswith('СТАРЫЙ КОММЕНТАРИЙ')
    assert saved['oldChars'] == len('СТАРЫЙ КОММЕНТАРИЙ')
finally:
    app._fetch_comment_raw_by_ref = orig_fetch
    app.requests.patch = orig_patch
    app._update_comment_memory_cache = orig_cache
    app._audit_comment_edit = orig_audit
print('KP_COMMENT_ATTRIBUTION_OK=true')
print('KP_OLD_COMMENT_PRESERVED=true')
runtime.remember_user('attr-user', 'Иван', 'Петров')
cp_state = {'value': 'СТАРАЯ ПРОЧАЯ ИНФОРМАЦИЯ'}
orig_cp_fetch = cp._fetch_one
orig_cp_patch = cp.requests.patch
def fake_cp_fetch(entity, ref_key, timeout=30):
    if entity == 'Catalog_Партнеры':
        return {'ДополнительнаяИнформация': cp_state['value']}
    return {'Description':'mock','Партнер_Key':'mock-partner'}
def fake_cp_patch(url, *args, **kwargs):
    cp_state['value'] = str((kwargs.get('json') or {}).get('ДополнительнаяИнформация') or '')
    return Resp()
cp._fetch_one = fake_cp_fetch
cp.requests.patch = fake_cp_patch
with cp._LOCK:
    cp._SESSIONS['attr-user'] = {'stage':'comment_confirm','refKey':'mock-cp','partnerKey':'mock-partner','proposedComment':'НОВАЯ ЗАПИСЬ','expiresAt':9999999999}
try:
    menu, saved_cp = cp.commit_comment('attr-user', 'user')
    assert cp_state['value'].startswith('[')
    assert 'Иван Петров] НОВАЯ ЗАПИСЬ' in cp_state['value']
    assert cp_state['value'].endswith('СТАРАЯ ПРОЧАЯ ИНФОРМАЦИЯ')
    assert saved_cp['oldChars'] == len('СТАРАЯ ПРОЧАЯ ИНФОРМАЦИЯ')
finally:
    cp._fetch_one = orig_cp_fetch
    cp.requests.patch = orig_cp_patch
    cp.clear('attr-user')
print('CLIENT_CARD_ATTRIBUTION_OK=true')
print('CLIENT_OLD_TEXT_PRESERVED=true')
print('NO_1C_WRITES=true')
for suffix in ('', '-wal', '-shm'):
    try: Path(str(TEST_DB) + suffix).unlink()
    except FileNotFoundError: pass
