import json, os, sys
from pathlib import Path
for raw in Path('/etc/kp-api/kp-api.env').read_text(encoding='utf-8').splitlines():
    if '=' in raw and not raw.lstrip().startswith('#'):
        k,v=raw.split('=',1); os.environ[k.strip()]=v.strip().strip('"').strip("'")
sys.path.insert(0,'/opt/kp-api')
import api_proxy as core
import kp_max_create as create
core._cached_rows=json.loads(Path('/opt/kp-api/data/kp_runtime_cache.json').read_text(encoding='utf-8'))

class Resp:
    status_code=201
    content=b'1'
    text=''
    def json(self):
        return {'Ref_Key':'11111111-2222-3333-4444-555555555555','Number':'ПСУТ-009999','Date':'2026-09-02T20:30:00','Статус':'','Клиент_Key':create.ZERO_GUID,'Контрагент_Key':create.ZERO_GUID}

state={'post':0,'payload':None}
orig_post=create.requests.post
orig_audit=create._audit
create.requests.post=lambda *a,**k:(state.update(post=state['post']+1,payload=k.get('json')) or Resp())
create._audit=lambda *a,**k:None
try:
    row=create.create_empty('verify-user','admin')
    assert state['post']==1
    assert state['payload']['Товары']==[] and state['payload']['Комментарий']==''
    assert state['payload']['Клиент_Key']==create.ZERO_GUID
    assert state['payload']['СрокДействия'].endswith('T00:00:00')
    assert state['payload']['СуммаДокумента']==0
    row2=create.create_empty('verify-user','admin')
    assert state['post']==1 and row2['number']=='9999'
    print('CREATE_EMPTY_PAYLOAD_OK')
    print('CREATE_DOUBLE_CLICK_GUARD_OK')
    print('NO_REAL_CREATE=true')
finally:
    create.requests.post=orig_post; create._audit=orig_audit
    core._cached_rows=[r for r in core._cached_rows if str(r.get('number') or '')!='9999']
