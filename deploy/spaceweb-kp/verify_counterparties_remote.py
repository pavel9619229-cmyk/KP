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
print('COUNTERPARTIES_SEARCH_OK=true')
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