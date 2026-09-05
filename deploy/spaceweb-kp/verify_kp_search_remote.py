import json
import sys
from pathlib import Path

sys.path.insert(0, '/opt/kp-api')
import api_proxy as core
import kp_max_navigation as nav
import kp_max_search as search

core._cached_rows = json.loads(Path('/opt/kp-api/data/kp_runtime_cache.json').read_text(encoding='utf-8'))
root = nav.root_menu('admin')
root_buttons = [b['text'] for row in root['attachments'][0]['payload']['buttons'] for b in row]
assert '🔎 ПОИСК КП' in root_buttons
print('SEARCH_ROOT_BUTTON_OK')

user = 'verify-search-number'
search.start(user, 'number')
menu = search.submit(user, '695')
buttons = [b for row in menu['attachments'][0]['payload']['buttons'] for b in row]
result_buttons = [b for b in buttons if str(b.get('payload') or '').startswith('find:open:')]
assert result_buttons and result_buttons[0]['payload'] == 'find:open:695'
print('SEARCH_NUMBER_EXACT_OK')
row695 = nav.find_row('695')
assert row695
client_name = str(row695.get('customerName') or '').strip()
assert client_name
user2 = 'verify-search-client'
search.start(user2, 'client')
menu2 = search.submit(user2, client_name)
buttons2 = [b for row in menu2['attachments'][0]['payload']['buttons'] for b in row]
assert any(b.get('payload') == 'find:open:695' for b in buttons2)
print('SEARCH_CLIENT_OK')
rows_live=search.live_rows.load()
candidate=next(r for r in rows_live if len([x for x in search.re.findall(r'[0-9a-zа-я]+',search._norm(r.get('customerName') or '')) if len(x)>=2])>=3)
ct=[x for x in search.re.findall(r'[0-9a-zа-я]+',search._norm(candidate.get('customerName') or '')) if len(x)>=2]
token_query=ct[0]+' '+ct[-1]
token_hits=search._search_client(token_query)
assert any(search._number_value(r)==search._number_value(candidate) for r in token_hits)
print('SEARCH_CLIENT_TOKENIZED_OK')

opened = search.open_result(user2, '695')
assert str(opened.get('text') or '').startswith('КП 695')
assert search.session_get(user2) is None
print('SEARCH_OPEN_LEVEL3_OK')

user3 = 'verify-search-partial'
search.start(user3, 'number')
menu3 = search.submit(user3, '69')
buttons3 = [b for row in menu3['attachments'][0]['payload']['buttons'] for b in row]
assert any(b.get('payload') == 'find:open:695' for b in buttons3)
print('SEARCH_NUMBER_PARTIAL_OK')
search.clear(user3)
