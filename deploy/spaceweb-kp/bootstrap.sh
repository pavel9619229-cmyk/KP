#!/usr/bin/env bash
set -euo pipefail
DOMAIN="${1:?usage: bootstrap.sh <domain>}"
LE_EMAIL="${LE_EMAIL:?set LE_EMAIL}"
APP=/opt/kp-api

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y python3 python3-venv nginx certbot python3-certbot-nginx ca-certificates curl
id kpapi >/dev/null 2>&1 || useradd --system --home "$APP" --shell /usr/sbin/nologin kpapi
mkdir -p "$APP/data" /etc/kp-api
python3 -m venv "$APP/.venv"
"$APP/.venv/bin/pip" install --upgrade pip
"$APP/.venv/bin/pip" install -r "$APP/requirements.txt"
cp "$APP/deploy/spaceweb-kp/kp-api.service" /etc/systemd/system/kp-api.service
sed "s/__DOMAIN__/$DOMAIN/g" "$APP/deploy/spaceweb-kp/nginx-kp.conf.template" > /etc/nginx/sites-available/kp-api
ln -sfn /etc/nginx/sites-available/kp-api /etc/nginx/sites-enabled/kp-api
rm -f /etc/nginx/sites-enabled/default
chown -R kpapi:kpapi "$APP"
chmod 750 /etc/kp-api
chmod 600 /etc/kp-api/kp-api.env
systemctl daemon-reload
systemctl enable --now kp-api
nginx -t
systemctl enable --now nginx
certbot --nginx -d "$DOMAIN" --redirect --non-interactive --agree-tos --email "$LE_EMAIL"
systemctl reload nginx
curl --fail --silent "https://$DOMAIN/healthz" >/dev/null
echo SPACEWEB_KP_BASE_READY
