import json, os, sys
from pathlib import Path
for raw in Path('/etc/kp-api/kp-api.env').read_text(encoding='utf-8').splitlines():
    if '=' in raw and not raw.lstrip().startswith('#'):
        k,v=raw.split('=',1); os.environ[k.strip()]=v.strip().strip('"').strip("'")
sys.path.insert(0,'/opt/kp-api')
import api_proxy as core
import kp_max_navigation as nav
import spaceweb_app as app
core._cached_rows=json.loads(Path('/opt/kp-api/data/kp_runtime_cache.json').read_text(encoding='utf-8'))
row=nav.recent_rows()[0]
number=core._normalize_kp_number(row.get('number') or '')
_,ref_key=app._find_kp_target(number)
raw=app._fetch_comment_raw_by_ref(ref_key)
text=app._comment_display(raw)
menu=nav.comment_menu(number,0,0,text,overflow=len(text)>3000)
buttons=menu['attachments'][0]['payload']['buttons']
flat=[b for r in buttons for b in r]
assert any(b.get('text')=='ДОБАВИТЬ ТЕКСТ В КОММЕНТАРИЙ' and str(b.get('payload') or '').startswith('nav:ce:') for b in flat)
assert not any(b.get('text')=='РЕДАКТИРОВАТЬ' for b in flat)
started=nav.comment_edit_started_menu(number)
assert 'СВЕРХУ' in started['text'] and 'ОЧИСТИТЬ' not in started['text']
user='verify-kp-comment-prepend'
app._start_comment_edit(user,number)
try:
    app._set_comment_proposal(user,'ОЧИСТИТЬ')
    raise AssertionError('OCHISTIT must be disabled')
except ValueError:
    pass
app._set_comment_proposal(user,'MOCK NEW TOP')
state={'value':'EXISTING OLD TEXT','payload':{}}
orig_fetch=app._fetch_comment_raw_by_ref; orig_patch=app.requests.patch
orig_cache=app._update_comment_memory_cache; orig_audit=app._audit_comment_edit
class Resp: status_code=204
def fake_fetch(ref): return state['value']
def fake_patch(*a,**kw):
    state['payload']=kw.get('json') or {}; state['value']=str(state['payload'].get('Комментарий') or ''); return Resp()
app._fetch_comment_raw_by_ref=fake_fetch; app.requests.patch=fake_patch
app._update_comment_memory_cache=lambda *a,**k:None; app._audit_comment_edit=lambda *a,**k:None
try:
    saved=app._commit_comment_edit(user,'user')
    assert state['value']=='MOCK NEW TOP\nEXISTING OLD TEXT'
    assert state['value'].endswith('EXISTING OLD TEXT')
    assert saved['chars']==len('MOCK NEW TOP') and saved['oldChars']==len('EXISTING OLD TEXT')
finally:
    app._fetch_comment_raw_by_ref=orig_fetch; app.requests.patch=orig_patch
    app._update_comment_memory_cache=orig_cache; app._audit_comment_edit=orig_audit; app._edit_session_clear(user)
print('KP_COMMENT_PREPEND_OK=true')
print('KP_OLD_COMMENT_PRESERVED=true')
print('KP_COMMENT_CLEAR_DISABLED=true')
print('NO_REAL_PATCH=true')