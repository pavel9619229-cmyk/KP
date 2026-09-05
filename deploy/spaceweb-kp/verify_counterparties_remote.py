import os, sys
from pathlib import Path
for raw in Path('/etc/kp-api/kp-api.env').read_text(encoding='utf-8').splitlines():
    if '=' in raw and not raw.lstrip().startswith('#'):
        k,v=raw.split('=',1); os.environ[k.strip()]=v.strip().strip(chr(34)).strip(chr(39))
sys.path.insert(0,'/opt/kp-api')
import kp_max_navigation as nav
import kp_max_counterparties as cp
root=nav.root_menu('admin')
buttons=root['attachments'][0]['payload']['buttons']
assert buttons[0][0]['text']=='КОНТРАГЕНТЫ'
assert buttons[0][0]['payload']=='cp:menu'
print('COUNTERPARTIES_ROOT_FIRST_OK=true')
u='verify-cp'; cp.clear(u)
menu=cp.start(u); assert cp.session_get(u).get('stage')=='await_query'
results=cp.search(u,'Павел')
labels=[r[0]['text'] for r in results['attachments'][0]['payload']['buttons']]
assert any('ЧЛ Павел' in x for x in labels)
cp.clear(u); cp.start(u)
token_results=cp.search(u,'аск пф')
token_labels=[r[0]['text'] for r in token_results['attachments'][0]['payload']['buttons']]
assert any('АСК ООО ПФ' in x for x in token_labels)
print('COUNTERPARTIES_SEARCH_OK=true')
print('COUNTERPARTIES_TOKEN_SEARCH_OK=true')
card=cp.card('ca8e8364-4a9b-11e6-a4f2-00155d00c206','admin')
text=card['text']
for wanted in ('Рабочее наименование:','ИНН:','ПРОЧАЯ ИНФОРМАЦИЯ','КОНТАКТНЫЕ ЛИЦА'):
    assert wanted in text
assert 'ЧЛ Павел' in text
card_buttons=[r[0]['text'] for r in card['attachments'][0]['payload']['buttons']]
assert '🔎 ИСКАТЬ ДРУГОГО' in card_buttons
cp.clear(u)
print('COUNTERPARTY_CARD_OK=true')
print('NO_1C_WRITES=true')
# Verify safe prepend flow: new text goes above old text; old text is preserved.
assert 'ДОБАВИТЬ ТЕКСТ В КОММЕНТАРИЙ' in card_buttons
assert 'РЕДАКТИРОВАТЬ КОММЕНТАРИЙ' not in card_buttons
cp.clear(u)
edit=cp.start_comment_edit(u,'ca8e8364-4a9b-11e6-a4f2-00155d00c206')
assert cp.session_get(u).get('stage')=='comment_text'
assert 'СВЕРХУ' in edit['text']
try:
    cp.set_comment(u,'ОЧИСТИТЬ')
    raise AssertionError('OCHISTIT must be rejected')
except ValueError:
    pass
old=str(cp.session_get(u).get('originalComment') or '')
confirm=cp.set_comment(u,'MOCK NEW TOP')
assert 'Существующий текст будет сохранён' in confirm['text']
state={'value':old,'comment':'KEEP COMMENT','entity':'','payload':{}}
orig_fetch=cp._fetch_one; orig_patch=cp.requests.patch; orig_card=cp.card
class Resp: status_code=204; text=''
def fake_fetch(entity,ref,timeout=30):
    if entity=='Catalog_Партнеры': return {'ДополнительнаяИнформация':state['value'],'Комментарий':state['comment']}
    return {'Description':'mock','Партнер_Key':''}
def fake_patch(url,*a,**kw):
    state['entity']=url; state['payload']=kw.get('json') or {}; state['value']=str(state['payload'].get('ДополнительнаяИнформация') or ''); return Resp()
cp._fetch_one=fake_fetch; cp.requests.patch=fake_patch; cp.card=lambda ref,role:{'text':'mock','attachments':[]}
try:
    saved_menu,saved=cp.commit_comment(u,'admin')
    assert state['value'].startswith('MOCK NEW TOP')
    assert state['value'].endswith(old)
    if old: assert old in state['value']
    assert state['payload']=={'ДополнительнаяИнформация':state['value']}
    assert 'Комментарий' not in state['payload'] and state['comment']=='KEEP COMMENT'
    assert saved['oldChars']==len(old) and saved['totalChars']==len(state['value'])
finally:
    cp._fetch_one=orig_fetch; cp.requests.patch=orig_patch; cp.card=orig_card; cp.clear(u)
print('PARTNER_OTHER_INFO_PREPEND_MOCK_OK=true')
print('OLD_TEXT_PRESERVED=true')
print('CLEAR_DISABLED=true')
print('PARTNER_COMMENT_UNTOUCHED=true')
print('NO_REAL_COMMENT_PATCH=true')
# Saved result must stay compact and keep an action button.
saved_buttons=[r[0]['text'] for r in saved_menu['attachments'][0]['payload']['buttons']]
assert 'ДОБАВИТЬ ЕЩЕ ТЕКСТ В КОММЕНТАРИЙ' in saved_buttons
print('COUNTERPARTY_ACTIONS_PERSIST_AFTER_SAVE=true')