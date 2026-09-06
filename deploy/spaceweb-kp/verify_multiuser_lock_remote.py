import json
import os
import sqlite3
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

for raw in Path('/etc/kp-api/kp-api.env').read_text(encoding='utf-8').splitlines():
    if '=' in raw and not raw.lstrip().startswith('#'):
        k,v=raw.split('=',1); os.environ[k.strip()]=v.strip().strip(chr(34)).strip(chr(39))
sys.path.insert(0,'/opt/kp-api')
import api_proxy as core
import kp_max_runtime as runtime
import kp_max_navigation as nav
import kp_max_customer as customer
import spaceweb_app as app

fd, db_name = tempfile.mkstemp(prefix='kp-lock-verify-', suffix='.sqlite')
os.close(fd); Path(db_name).unlink(missing_ok=True)
runtime.DB_PATH = Path(db_name); runtime._INITIALIZED_PATH = ''

def cleanup():
    for suffix in ('', '-wal', '-shm'):
        Path(db_name + suffix).unlink(missing_ok=True)
try:
    runtime.remember_from_payload({'message':{'sender':{'user_id':'1001','first_name':'Alice','last_name':'Admin'}}})
    runtime.remember_from_payload({'callback':{'user':{'user_id':'1002','first_name':'Bob','last_name':'Builder'}}})
    assert runtime.user_label('1001') == 'Alice Admin'
    assert runtime.user_label('1002') == 'Bob Builder'
    print('MAX_USER_IDENTITY_OK=true')

    ref='11111111-1111-1111-1111-111111111111'
    a=runtime.acquire(ref,'832','1001',120)
    assert a['ok'] is True
    b=runtime.acquire(ref,'832','1002',120)
    assert b['ok'] is False and b['ownerId']=='1001' and b['ownerLabel']=='Alice Admin'
    a2=runtime.acquire(ref,'832','1001',120)
    assert a2['ok'] is True
    print('KP_EXCLUSIVE_LOCK_OK=true')
    print('LOCK_OWNER_NAME_OK=true')

    runtime.release_user('1001')
    b2=runtime.acquire(ref,'832','1002',120)
    assert b2['ok'] is True
    assert runtime.current_lock(ref)['ownerId']=='1002'
    print('LOCK_RELEASE_REACQUIRE_OK=true')

    conn=sqlite3.connect(db_name)
    conn.execute('UPDATE kp_locks SET expires_at=0 WHERE kp_ref=?',(ref,)); conn.commit(); conn.close()
    a3=runtime.acquire(ref,'832','1001',120)
    assert a3['ok'] is True
    print('LOCK_EXPIRY_OK=true')
    runtime.release_user('1001'); runtime.release_user('1002')
    race_ref='22222222-2222-2222-2222-222222222222'
    barrier=threading.Barrier(2)
    def race(uid):
        barrier.wait()
        return runtime.acquire(race_ref,'900',uid,120)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(race,['1001','1002']))
    assert sum(1 for x in results if x['ok']) == 1
    assert sum(1 for x in results if not x['ok']) == 1
    print('ATOMIC_TWO_USER_RACE_OK=true')

    core._cached_rows=json.loads(Path('/opt/kp-api/data/kp_runtime_cache.json').read_text(encoding='utf-8'))
    row=nav.recent_rows()[0]
    number=core._normalize_kp_number(row.get('number') or '')
    assert number
    runtime.release_user('1001'); runtime.release_user('1002')
    ok1, menu1=app._acquire_kp_lock(number,'1001')
    assert ok1 and menu1 is None
    ok2, menu2=app._acquire_kp_lock(number,'1002')
    assert not ok2 and 'Alice Admin' in str(menu2.get('text') or '') and 'Доступен только просмотр' in str(menu2.get('text') or '')
    print('BOT_LOCK_MESSAGE_OK=true')

    customer._SESSIONS['1002']={'number':number,'stage':'await_query','expiresAt':9999999999}
    denied=app._ensure_active_kp_lock('1002')
    assert denied and customer.session_get('1002') is None
    current=runtime.current_lock(str(row.get('refKey') or row.get('Ref_Key') or ''))
    assert current and current['ownerId']=='1001'
    print('STALE_EDITOR_SESSION_STOPPED_OK=true')
    runtime.release_user('1001'); runtime.release_user('1002')
    ok3,_=app._acquire_kp_lock(number,'1001')
    assert ok3
    customer._SESSIONS['1001']={'number':number,'stage':'await_query','expiresAt':9999999999}
    customer.clear('1001')
    ref_key=str(row.get('refKey') or row.get('Ref_Key') or '')
    assert runtime.current_lock(ref_key) is None
    print('SESSION_CLEAR_RELEASES_LOCK_OK=true')

    st=runtime.stats()
    assert st['locks'] == 0 and st['users'] >= 2
    print('SQLITE_RUNTIME_STATS_OK=true')
    print('NO_1C_WRITES=true')
finally:
    customer._SESSIONS.pop('1001',None); customer._SESSIONS.pop('1002',None)
    app._EDIT_SESSIONS.pop('1001',None); app._EDIT_SESSIONS.pop('1002',None)
    cleanup()
