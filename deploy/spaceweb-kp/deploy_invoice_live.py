from pathlib import Path
import paramiko, time
root=Path(r'C:\Users\Server\Documents\API')
key=paramiko.Ed25519Key.from_private_key_file(str(root/'deploy'/'spaceweb-kp'/'private'/'kp_max_ed25519'))
c=paramiko.SSHClient(); c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('89.111.131.83',username='root',pkey=key,timeout=10)
sftp=c.open_sftp()
files=[
 ('spaceweb_app.py','/opt/kp-api/spaceweb_app.py.new'),
 ('kp_max_live_rows.py','/opt/kp-api/kp_max_live_rows.py.new'),
 ('kp_max_navigation.py','/opt/kp-api/kp_max_navigation.py.new'),
 ('kp_max_customer.py','/opt/kp-api/kp_max_customer.py.new'),
 ('kp_max_items.py','/opt/kp-api/kp_max_items.py.new'),
 ('kp_max_create.py','/opt/kp-api/kp_max_create.py.new'),
 ('kp_max_documents.py','/opt/kp-api/kp_max_documents.py.new'),
 ('kp_max_print.py','/opt/kp-api/kp_max_print.py.new'),
]
for name,remote in files: sftp.put(str(root/name),remote)
sftp.put(str(root/'deploy'/'spaceweb-kp'/'verify_invoice_live_remote.py'),'/opt/kp-api/verify_invoice_live_remote.py')
sftp.put(str(root/'deploy'/'spaceweb-kp'/'verify_print_live_remote.py'),'/opt/kp-api/verify_print_live_remote.py')
sftp.close()
def run(cmd):
    _,o,e=c.exec_command(cmd)
    out=o.read().decode('utf-8',errors='replace').strip(); err=e.read().decode('utf-8',errors='replace').strip()
    rc=o.channel.recv_exit_status()
    if rc: raise RuntimeError(err[-1200:])
    return out
print('REMOTE_CHECKPOINT='+run("d=/opt/kp-api/checkpoints/pre-print-$(date +%Y%m%d-%H%M%S); mkdir -p \"$d\"; cp -a /opt/kp-api/spaceweb_app.py /opt/kp-api/kp_max_live_rows.py /opt/kp-api/kp_max_navigation.py /opt/kp-api/kp_max_customer.py /opt/kp-api/kp_max_items.py /opt/kp-api/kp_max_create.py /opt/kp-api/kp_max_documents.py \"$d\"/; test ! -f /opt/kp-api/kp_max_print.py || cp -a /opt/kp-api/kp_max_print.py \"$d\"/; echo \"$d\""))
run("mv /opt/kp-api/spaceweb_app.py.new /opt/kp-api/spaceweb_app.py && mv /opt/kp-api/kp_max_live_rows.py.new /opt/kp-api/kp_max_live_rows.py && mv /opt/kp-api/kp_max_navigation.py.new /opt/kp-api/kp_max_navigation.py && mv /opt/kp-api/kp_max_customer.py.new /opt/kp-api/kp_max_customer.py && mv /opt/kp-api/kp_max_items.py.new /opt/kp-api/kp_max_items.py && mv /opt/kp-api/kp_max_create.py.new /opt/kp-api/kp_max_create.py && mv /opt/kp-api/kp_max_documents.py.new /opt/kp-api/kp_max_documents.py && mv /opt/kp-api/kp_max_print.py.new /opt/kp-api/kp_max_print.py")
run("chown kpapi:kpapi /opt/kp-api/spaceweb_app.py /opt/kp-api/kp_max_live_rows.py /opt/kp-api/kp_max_navigation.py /opt/kp-api/kp_max_customer.py /opt/kp-api/kp_max_items.py /opt/kp-api/kp_max_create.py /opt/kp-api/kp_max_documents.py /opt/kp-api/kp_max_print.py && chmod 644 /opt/kp-api/spaceweb_app.py /opt/kp-api/kp_max_live_rows.py /opt/kp-api/kp_max_navigation.py /opt/kp-api/kp_max_customer.py /opt/kp-api/kp_max_items.py /opt/kp-api/kp_max_create.py /opt/kp-api/kp_max_documents.py /opt/kp-api/kp_max_print.py")
run("install -d -o kpapi -g kpapi -m 750 /opt/kp-api/data/print_forms && chown -R kpapi:kpapi /opt/kp-api/data/print_forms")
run("/opt/kp-api/.venv/bin/python -c 'import reportlab' >/dev/null 2>&1 || /opt/kp-api/.venv/bin/pip install reportlab==4.2.5")
run("/opt/kp-api/.venv/bin/python -m py_compile /opt/kp-api/spaceweb_app.py /opt/kp-api/kp_max_live_rows.py /opt/kp-api/kp_max_navigation.py /opt/kp-api/kp_max_customer.py /opt/kp-api/kp_max_items.py /opt/kp-api/kp_max_create.py /opt/kp-api/kp_max_documents.py /opt/kp-api/kp_max_print.py")
run("systemctl restart kp-api.service")
time.sleep(3)
print('SERVICE='+run('systemctl is-active kp-api.service'))
print(run('cd /opt/kp-api && /opt/kp-api/.venv/bin/python verify_invoice_live_remote.py'))
print(run('cd /opt/kp-api && /opt/kp-api/.venv/bin/python verify_print_live_remote.py'))
run('rm -f /opt/kp-api/verify_invoice_live_remote.py /opt/kp-api/verify_print_live_remote.py')
c.close(); print('INVOICE_LIVE_DEPLOY_OK')
