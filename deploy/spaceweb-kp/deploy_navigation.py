from pathlib import Path
import time
import paramiko

ROOT=Path(r'C:\Users\Server\Documents\API')
KEY=ROOT/'deploy'/'spaceweb-kp'/'private'/'kp_max_ed25519'
FILES={
    ROOT/'spaceweb_app.py':'/opt/kp-api/spaceweb_app.py.new',
    ROOT/'kp_max_navigation.py':'/opt/kp-api/kp_max_navigation.py.new',
    ROOT/'kp_max_counterparties.py':'/opt/kp-api/kp_max_counterparties.py.new',
    ROOT/'kp_max_customer.py':'/opt/kp-api/kp_max_customer.py.new',
    ROOT/'kp_max_search.py':'/opt/kp-api/kp_max_search.py.new',
    ROOT/'deploy'/'spaceweb-kp'/'verify_navigation_remote.py':'/opt/kp-api/verify_navigation_remote.py',
    ROOT/'deploy'/'spaceweb-kp'/'verify_counterparties_remote.py':'/opt/kp-api/verify_counterparties_remote.py',
    ROOT/'deploy'/'spaceweb-kp'/'verify_customer_edit_remote.py':'/opt/kp-api/verify_customer_edit_remote.py',
    ROOT/'deploy'/'spaceweb-kp'/'verify_alpha_part_remote.py':'/opt/kp-api/verify_alpha_part_remote.py',
    ROOT/'deploy'/'spaceweb-kp'/'verify_comment_button_remote.py':'/opt/kp-api/verify_comment_button_remote.py',
    ROOT/'deploy'/'spaceweb-kp'/'verify_kp_search_remote.py':'/opt/kp-api/verify_kp_search_remote.py',
    ROOT/'deploy'/'spaceweb-kp'/'verify_global_home_remote.py':'/opt/kp-api/verify_global_home_remote.py',
    ROOT/'deploy'/'spaceweb-kp'/'register_max_webhook.py':'/opt/kp-api/register_max_webhook.py',
}
key=paramiko.Ed25519Key.from_private_key_file(str(KEY))
client=paramiko.SSHClient(); client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('89.111.131.83',username='root',pkey=key,timeout=10)
sftp=client.open_sftp()
for local,remote in FILES.items(): sftp.put(str(local),remote)
sftp.close()
def run(cmd):
    _,out,err=client.exec_command(cmd)
    stdout=out.read().decode('utf-8',errors='replace').strip()
    stderr=err.read().decode('utf-8',errors='replace').strip()
    rc=out.channel.recv_exit_status()
    if rc!=0: raise RuntimeError(f'RC={rc}: {stderr[-800:]}')
    return stdout

run("mv /opt/kp-api/spaceweb_app.py.new /opt/kp-api/spaceweb_app.py && mv /opt/kp-api/kp_max_navigation.py.new /opt/kp-api/kp_max_navigation.py && mv /opt/kp-api/kp_max_counterparties.py.new /opt/kp-api/kp_max_counterparties.py && mv /opt/kp-api/kp_max_customer.py.new /opt/kp-api/kp_max_customer.py && mv /opt/kp-api/kp_max_search.py.new /opt/kp-api/kp_max_search.py")
run("chown kpapi:kpapi /opt/kp-api/spaceweb_app.py /opt/kp-api/kp_max_navigation.py /opt/kp-api/kp_max_counterparties.py /opt/kp-api/kp_max_customer.py /opt/kp-api/kp_max_search.py && chmod 644 /opt/kp-api/spaceweb_app.py /opt/kp-api/kp_max_navigation.py /opt/kp-api/kp_max_counterparties.py /opt/kp-api/kp_max_customer.py /opt/kp-api/kp_max_search.py")
run("/opt/kp-api/.venv/bin/python -m py_compile /opt/kp-api/spaceweb_app.py /opt/kp-api/kp_max_navigation.py /opt/kp-api/kp_max_counterparties.py /opt/kp-api/kp_max_customer.py /opt/kp-api/kp_max_search.py")
run("systemctl restart kp-api.service")
time.sleep(3)
print('SERVICE=' + run("systemctl is-active kp-api.service"))
print(run("curl -fsS http://127.0.0.1:8088/api/max/kp-bot/status"))
print(run("cd /opt/kp-api && /opt/kp-api/.venv/bin/python verify_navigation_remote.py"))
print(run("cd /opt/kp-api && /opt/kp-api/.venv/bin/python verify_counterparties_remote.py"))
print(run("cd /opt/kp-api && /opt/kp-api/.venv/bin/python verify_customer_edit_remote.py"))
print(run("cd /opt/kp-api && /opt/kp-api/.venv/bin/python verify_alpha_part_remote.py"))
print(run("cd /opt/kp-api && /opt/kp-api/.venv/bin/python verify_comment_button_remote.py"))
print(run("cd /opt/kp-api && /opt/kp-api/.venv/bin/python verify_kp_search_remote.py"))
print(run("cd /opt/kp-api && /opt/kp-api/.venv/bin/python verify_global_home_remote.py"))
print(run("cd /opt/kp-api && /opt/kp-api/.venv/bin/python register_max_webhook.py"))
run("rm -f /opt/kp-api/verify_navigation_remote.py /opt/kp-api/verify_counterparties_remote.py /opt/kp-api/verify_customer_edit_remote.py /opt/kp-api/verify_alpha_part_remote.py /opt/kp-api/verify_comment_button_remote.py /opt/kp-api/verify_kp_search_remote.py /opt/kp-api/verify_global_home_remote.py /opt/kp-api/register_max_webhook.py")
client.close()
print('NAV_DEPLOY_OK')
