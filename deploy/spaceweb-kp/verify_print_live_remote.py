import json, os, sys
from pathlib import Path
for raw in Path('/etc/kp-api/kp-api.env').read_text(encoding='utf-8').splitlines():
    if '=' in raw and not raw.lstrip().startswith('#'):
        k,v=raw.split('=',1); os.environ[k.strip()]=v.strip().strip(chr(34)).strip(chr(39))
sys.path.insert(0,'/opt/kp-api')
import requests
import api_proxy as core
core._cached_rows=json.loads(Path('/opt/kp-api/data/kp_runtime_cache.json').read_text(encoding='utf-8'))
import kp_max_documents as docs
import kp_max_print as prn
import spaceweb_app as app
import reportlab
print('REPORTLAB_OK=true')
kp_row,kp_ref=docs._find_kp('824')
kp=docs._fetch_kp(kp_ref)
order=docs._build_order_payload(kp_ref,kp)
order.update({'Ref_Key':'11111111-1111-1111-1111-111111111111','Number':'TEST-824','ДополнительнаяИнформация':'old extra','НазначениеПлатежа':'old purpose','Комментарий':'old comment','БанковскийСчет_Key':''})
state=dict(order)
orig_fetch,orig_patch,orig_audit=prn._fetch_order,prn._patch_order,prn._audit
prn._fetch_order=lambda ref: dict(state)
def fake_patch(ref,patch): state.update(patch)
prn._patch_order=fake_patch; prn._audit=lambda *a,**k:None
u='verify-print-edit'; prn.clear(u)
try:
    prn.start_edit(u,'extra','824',order['Ref_Key'],0,0)
    prn.set_text(u,'new extra')
    menu,saved=prn.commit(u,'admin')
    assert state['ДополнительнаяИнформация']=='new extra'
    assert saved['field']=='ДополнительнаяИнформация'
    print('PRINT_EDIT_MOCK_OK=true')
finally:
    prn._fetch_order,prn._patch_order,prn._audit=orig_fetch,orig_patch,orig_audit; prn.clear(u)
orig_fetch,orig_bank=prn._fetch_order,prn._resolve_bank
prn._fetch_order=lambda ref: dict(state)
prn._resolve_bank=lambda order:{'НомерСчета':'40700000000000000000','НаименованиеБанка':'ТЕСТ БАНК','БИКБанка':'000000000','КоррСчетБанка':'30100000000000000000'}
try:
    pdf_path,pdf_order=prn.generate_pdf(order['Ref_Key'])
    raw=pdf_path.read_bytes()
    assert raw.startswith(b'%PDF') and len(raw)>1000
    print('PRINT_PDF_OK=true')
    print('PRINT_PDF_BYTES='+str(len(raw)))
finally:
    prn._fetch_order,prn._resolve_bank=orig_fetch,orig_bank

init=requests.post('https://platform-api2.max.ru/uploads',params={'type':'file'},headers={'Authorization':app.KP_MAX_BOT_TOKEN,'Accept':'application/json'},timeout=25,verify=app._max_verify())
idata=init.json() if init.content else {}
assert init.status_code==200 and idata.get('url')
with pdf_path.open('rb') as f:
    up=requests.post(str(idata['url']),files={'data':(pdf_path.name,f,'application/pdf')},timeout=90,verify=app._max_verify())
udata=up.json() if up.content else {}
assert 200 <= up.status_code < 300 and (udata.get('token') or idata.get('token'))
print('MAX_FILE_UPLOAD_OK=true')
print('NO_MAX_MESSAGE_SENT=true')
pdf_path.unlink(missing_ok=True)
orig_fetch,orig_patch,orig_banks,orig_cat,orig_audit=prn._fetch_order,prn._patch_order,prn._bank_accounts,prn._catalog_one,prn._audit
prn._fetch_order=lambda ref: dict(state); prn._patch_order=fake_patch; prn._audit=lambda *a,**k:None
bank={'Ref_Key':'22222222-2222-2222-2222-222222222222','НомерСчета':'407TEST','Description':'Основной','DeletionMark':False,'Закрыт':False}
prn._bank_accounts=lambda org:[dict(bank)]
prn._catalog_one=lambda entity,key: dict(bank) if entity=='Catalog_БанковскиеСчетаОрганизаций' and key==bank['Ref_Key'] else orig_cat(entity,key)
u='verify-print-bank'; prn.clear(u)
try:
    bm=prn.bank_menu(u,'824',order['Ref_Key'],0,0)
    assert any('407TEST' in r[0]['text'] for r in bm['attachments'][0]['payload']['buttons'])
    prn.pick_bank(u,bank['Ref_Key']); prn.commit(u,'admin')
    assert state['БанковскийСчет_Key']==bank['Ref_Key']
    print('PRINT_BANK_EDIT_MOCK_OK=true')
finally:
    prn._fetch_order,prn._patch_order,prn._bank_accounts,prn._catalog_one,prn._audit=orig_fetch,orig_patch,orig_banks,orig_cat,orig_audit; prn.clear(u)
