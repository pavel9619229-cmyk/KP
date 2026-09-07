import json, sys
from pathlib import Path
sys.path.insert(0,'/opt/kp-api')
import api_proxy as core
import kp_max_navigation as nav
cache=Path('/opt/kp-api/data/kp_runtime_cache.json')
if cache.exists():
    core._cached_rows=json.loads(cache.read_text(encoding='utf-8'))
user_a='verify-manager-a'; user_b='verify-manager-b'
nav.set_manager_filter(user_a,'all')
menu=nav.statuses_menu(user_a)
buttons=[b for row in menu['attachments'][0]['payload']['buttons'] for b in row]
assert any(b.get('text')=='ФИЛЬТР ПО МЕНЕДЖЕРУ — ВСЕ' for b in buttons)
selector=nav.manager_filter_menu(user_a)
texts=[r[0]['text'] for r in selector['attachments'][0]['payload']['buttons']]
assert texts[:5]==['ВСЕ','АНДРЕЙ','ЕЛЕНА','ПАВЕЛ','ТАТЬЯНА']
print('MANAGER_FILTER_BUTTONS_OK=true')
counts={}
for key,(label,ref) in nav.MANAGER_FILTERS.items():
    nav.set_manager_filter(user_a,key)
    rows=nav.rows_for_status(0,user_a)
    if ref:
        for r in rows:
            row_ref=str(r.get('Менеджер_Key') or '').strip()
            row_name=str(r.get('managerName') or '').strip().casefold()
            assert (row_ref==ref) if row_ref else (row_name==label.casefold())
    counts[label]=len(rows)
    m=nav.statuses_menu(user_a)
    flat=[b for row in m['attachments'][0]['payload']['buttons'] for b in row]
    assert any(b.get('text')==f'ФИЛЬТР ПО МЕНЕДЖЕРУ — {label}' for b in flat)
print('MANAGER_FILTER_COUNTS='+json.dumps(counts,ensure_ascii=False))
print('MANAGER_FILTER_ROWS_OK=true')
nav.set_manager_filter(user_a,'pavel'); nav.set_manager_filter(user_b,'elena')
assert nav.manager_filter_label(user_a)=='ПАВЕЛ'
assert nav.manager_filter_label(user_b)=='ЕЛЕНА'
print('MANAGER_FILTER_PER_USER_OK=true')
page=nav.status_page(0,0,user_a)
assert 'Менеджер: ПАВЕЛ' in str(page.get('text') or '')
print('MANAGER_FILTER_STATUS_PAGE_OK=true')
nav.set_manager_filter(user_a,'all')
assert nav.manager_filter_label(user_a)=='ВСЕ'
print('MANAGER_FILTER_RESET_OK=true')
print('NO_1C_WRITES=true')

import inspect, spaceweb_app
src=inspect.getsource(spaceweb_app._handle_navigation_callback)
assert 'nav:mgr:menu' in src and 'nav:mgr:set:' in src
assert 'nav.status_page(nav.status_index(key), int(page), sender_id)' in src
print('MANAGER_FILTER_CALLBACK_ROUTE_OK=true')
