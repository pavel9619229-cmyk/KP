import os,sys,time
from pathlib import Path
for raw in Path('/etc/kp-api/kp-api.env').read_text(encoding='utf-8').splitlines():
    if '=' in raw and not raw.lstrip().startswith('#'):
        k,v=raw.split('=',1); os.environ[k.strip()]=v.strip().strip(chr(34)).strip(chr(39))
sys.path.insert(0,'/opt/kp-api')
import kp_max_full_search as full
started=time.perf_counter()
count=full.rebuild()
print('FULL_SEARCH_INDEX_ROWS='+str(count))
print('FULL_SEARCH_INDEX_SECONDS='+f'{time.perf_counter()-started:.3f}')
assert count > 10000
print('FULL_SEARCH_INDEX_OK=true')
