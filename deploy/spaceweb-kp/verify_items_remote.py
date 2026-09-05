import json, os, sys
from pathlib import Path
for raw in Path('/etc/kp-api/kp-api.env').read_text(encoding='utf-8').splitlines():
    if '=' in raw and not raw.lstrip().startswith('#'):
        k,v=raw.split('=',1); os.environ[k.strip()]=v.strip().strip('"').strip("'")
sys.path.insert(0,'/opt/kp-api')
import api_proxy as core
import kp_max_items as items
import kp_max_navigation as nav
core._cached_rows=json.loads(Path('/opt/kp-api/data/kp_runtime_cache.json').read_text(encoding='utf-8'))
menu=items.list_menu('695',0,0,0)
buttons=menu['attachments'][0]['payload']['buttons']
item_buttons=[r[0] for r in buttons if r and str(r[0].get('payload') or '').startswith('itm:open:')]
print('ITEM_LIST_COUNT='+str(len(item_buttons)))
assert len(item_buttons)>=1
assert all('🟢 ←' not in b['text'] for b in item_buttons)
print('ITEM_LIST_OK')
line_menu=items.item_menu('695',1,0,0,0)
texts=[r[0]['text'] for r in line_menu['attachments'][0]['payload']['buttons']]
for wanted in ('ПОСТАВЩИК И ЦЕНА','КОММЕНТАРИЙ','ТОВАР','ЦЕНА','КОЛИЧЕСТВО'):
    assert wanted in texts
print('ITEM_FIELDS_OK')

real_row=items._fetch_row(items._find_kp('695')[1],1)
for field in ('internal','buyer','price','qty','product'):
    user='verify-'+field
    items.clear(user)
    start=items.start_edit(user,field,'695',1,0,0,0)
    s=items.session_get(user); assert s
    if field=='internal':
        confirm=items.set_value(user,'TEST INTERNAL')
    elif field=='buyer':
        confirm=items.set_value(user,'TEST BUYER')
    elif field=='price':
        confirm=items.set_value(user,str(float(real_row.get('Цена') or 0)+1))
    elif field=='qty':
        confirm=items.set_value(user,str(float(real_row.get('Количество') or 0)+1))
    else:
        key=str(real_row.get('Номенклатура_Key') or '')
        confirm=items.pick_product(user,key)
    assert items.session_get(user).get('stage')=='confirm'
    assert any(r[0]['text']=='СОХРАНИТЬ' for r in confirm['attachments'][0]['payload']['buttons'])
    items.clear(user)
print('ITEM_EDIT_FLOWS_OK')

# Verify parent-document tabular-section PATCH shape without touching 1C.
user='verify-patch'; items.clear(user)
items.start_edit(user,'internal','695',1,0,0,0); items.set_value(user,'MOCK VALUE')
ref695=items.session_get(user)['refKey']; base_rows=items._fetch_items(ref695)
state={'payload':None}
orig_fetch_items=items._fetch_items; orig_patch=items.requests.patch; orig_audit=items._audit; orig_field_menu=items.field_menu
def fake_fetch_items(ref_key):
    return state['payload']['Товары'] if state['payload'] else [dict(x) for x in base_rows]
class Resp: status_code=204; text=''
def fake_patch(*args,**kwargs):
    state['payload']=kwargs.get('json'); return Resp()
items._fetch_items=fake_fetch_items; items.requests.patch=fake_patch; items._audit=lambda *a,**k:None
items.field_menu=lambda *a,**k:{'text':'mock','attachments':[]}
try:
    menu,saved=items.commit(user,'admin')
    assert state['payload'] and set(state['payload'])=={'Товары','СуммаДокумента'}
    sent=state['payload']['Товары']
    assert abs(float(state['payload']['СуммаДокумента'])-items._document_total(sent))<1e-9
    target=next(x for x in sent if int(x.get('LineNumber') or 0)==1)
    assert target['КомментарийВнутренний']=='MOCK VALUE'
    assert saved['field']=='internal'
finally:
    items._fetch_items=orig_fetch_items; items.requests.patch=orig_patch; items._audit=orig_audit; items.field_menu=orig_field_menu; items.clear(user)
print('ITEM_PARENT_SECTION_PATCH_OK')
print('NO_REAL_PATCH=true')

# Verify navigation button order and ADD ROW button.
assert buttons[0][0]['text']=='🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ'
assert buttons[0][0]['payload']=='nav:root'
assert 'ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ' in buttons[1][0]['text']
assert buttons[2][0]['text']=='ДОБАВИТЬ СТРОКУ'
assert buttons[2][0]['payload'].startswith('itm:addrow:')
assert line_menu['attachments'][0]['payload']['buttons'][0][0]['text']=='🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ'
print('ITEM_NAV_ADD_BUTTONS_OK')

# Verify add-row wizard and full-array PATCH shape without touching 1C.
user='verify-add'; items.clear(user)
start=items.start_add(user,'695',0,0,0)
ref=items.session_get(user)['refKey']; base_rows=items._fetch_items(ref)
product_key=str(base_rows[0].get('Номенклатура_Key') or '')
items.pick_add_product(user,product_key)
items.set_add_value(user,'3')
confirm=items.set_add_value(user,'12345')
assert items.session_get(user).get('stage')=='add_confirm'
assert any(r[0]['text']=='СОХРАНИТЬ' for r in confirm['attachments'][0]['payload']['buttons'])
state={'payload':None}
orig_fetch_items=items._fetch_items; orig_patch=items.requests.patch; orig_audit=items._audit; orig_item_menu=items.item_menu
class AddResp: status_code=204; text=''
def fake_fetch_items(ref_key):
    return state['payload']['Товары'] if state['payload'] else [dict(x) for x in base_rows]
def fake_add_patch(*args,**kwargs):
    state['payload']=kwargs.get('json'); return AddResp()
items._fetch_items=fake_fetch_items; items.requests.patch=fake_add_patch; items._audit=lambda *a,**k:None
items.item_menu=lambda number,line,status_idx,status_page,item_page:{'text':'mock','attachments':[]}
try:
    menu,saved=items.commit_add(user,'admin')
    assert state['payload'] and set(state['payload'])=={'Товары','СуммаДокумента'}
    sent=state['payload']['Товары']; assert sent[:-1]==base_rows
    assert abs(float(state['payload']['СуммаДокумента'])-items._document_total(sent))<1e-9
    new=sent[-1]
    assert new['Номенклатура_Key']==product_key and float(new['Количество'])==3 and float(new['Цена'])==12345
    assert new['КомментарийВнутренний']=='' and new['КомментарийДляПокупателя']==''
    assert float(new['Сумма'])==float(new['СуммаСНДС'])
    assert abs(float(new['СуммаНДС'])-round(float(new['Сумма'])*22/122,2))<1e-9
    assert new['СрокПоставки']=='0'
    assert new['СтавкаНДС']==items._vat22_key()
    assert new['СтавкаНДС_Type']=='StandardODATA.Catalog_СтавкиНДС'
    assert saved['field']=='add'
finally:
    items._fetch_items=orig_fetch_items; items.requests.patch=orig_patch; items._audit=orig_audit; items.item_menu=orig_item_menu; items.clear(user)
print('ITEM_ADD_FLOW_MOCK_OK')
print('NO_REAL_ADD_PATCH=true')
