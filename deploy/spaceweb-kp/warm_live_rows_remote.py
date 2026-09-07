import os,sys
from pathlib import Path
for raw in Path('/etc/kp-api/kp-api.env').read_text(encoding='utf-8').splitlines():
    if '=' in raw and not raw.lstrip().startswith('#'):
        k,v=raw.split('=',1); os.environ[k.strip()]=v.strip().strip(chr(34)).strip(chr(39))
sys.path.insert(0,'/opt/kp-api')
import kp_max_live_rows as live
rows=live.load(force=True)
assert len(rows)==live.MAX_ROWS, len(rows)
assert live.LIVE_CACHE_PATH.is_file()
print('LIVE_ROWS_WARMED='+str(len(rows)))
print('LIVE_CACHE_PATH='+str(live.LIVE_CACHE_PATH))
