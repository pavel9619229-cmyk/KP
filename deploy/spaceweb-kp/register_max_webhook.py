from pathlib import Path
import json
import requests

ENV_PATH = Path('/etc/kp-api/kp-api.env')
API = 'https://platform-api2.max.ru'
TARGET = 'https://89.111.131.83/api/max/kp-bot/webhook'

def load_env(path: Path):
    data = {}
    for raw in path.read_text(encoding='utf-8').splitlines():
        if not raw or raw.lstrip().startswith('#') or '=' not in raw:
            continue
        k, v = raw.split('=', 1)
        data[k.strip()] = v.strip()
    return data

env = load_env(ENV_PATH)
token = env.get('KP_MAX_BOT_TOKEN', '')
secret = env.get('KP_MAX_WEBHOOK_SECRET', '')
ca = env.get('KP_MAX_CA_BUNDLE', '') or True
if not token or not secret:
    raise SystemExit('MAX_ENV_INCOMPLETE')
headers = {'Authorization': token, 'Content-Type': 'application/json'}
me = requests.get(API + '/me', headers={'Authorization': token}, verify=ca, timeout=20)
print('ME_HTTP=' + str(me.status_code))
if me.status_code != 200:
    raise SystemExit('MAX_TOKEN_INVALID')
try:
    me_data = me.json()
except Exception:
    raise SystemExit('MAX_ME_PARSE_ERROR')
print('ME_IS_BOT=' + str(bool(me_data.get('is_bot'))).lower())

subs = requests.get(API + '/subscriptions', headers={'Authorization': token}, verify=ca, timeout=20)
print('SUBS_HTTP=' + str(subs.status_code))
if subs.status_code != 200:
    raise SystemExit('SUBS_GET_FAILED')
try:
    subs_data = subs.json()
except Exception:
    raise SystemExit('SUBS_PARSE_ERROR')
items = subs_data.get('subscriptions', []) if isinstance(subs_data, dict) else subs_data
if not isinstance(items, list):
    items = []
print('SUBS_BEFORE=' + str(len(items)))
for item in items:
    url = item.get('url') if isinstance(item, dict) else None
    if isinstance(url, str) and url:
        r = requests.delete(API + '/subscriptions', params={'url': url}, headers={'Authorization': token}, verify=ca, timeout=20)
        print('DELETE_HTTP=' + str(r.status_code))

payload = {'url': TARGET, 'update_types': ['message_created', 'message_callback'], 'secret': secret}
post = requests.post(API + '/subscriptions', headers=headers, json=payload, verify=ca, timeout=20)
print('POST_HTTP=' + str(post.status_code))
try:
    post_data = post.json()
except Exception:
    post_data = {}
if post.status_code != 200 or not bool(post_data.get('success', True)):
    raise SystemExit('SUBS_POST_FAILED')

verify_resp = requests.get(API + '/subscriptions', headers={'Authorization': token}, verify=ca, timeout=20)
if verify_resp.status_code != 200:
    raise SystemExit('SUBS_VERIFY_FAILED')
verify_data = verify_resp.json()
verify_items = verify_data.get('subscriptions', []) if isinstance(verify_data, dict) else verify_data
if not isinstance(verify_items, list):
    verify_items = []
found = any(isinstance(x, dict) and x.get('url') == TARGET and {'message_created','message_callback'}.issubset(set(x.get('update_types') or [])) for x in verify_items)
print('SUBS_AFTER=' + str(len(verify_items)))
print('WEBHOOK_REGISTERED=' + str(found).lower())
if not found:
    raise SystemExit('WEBHOOK_NOT_FOUND')
