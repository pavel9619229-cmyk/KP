# SpaceWeb KP deployment

Цель: заменить Render российским VPS SpaceWeb для КП/1С и будущего отдельного MAX-бота.

Архитектура:
- nginx: публичные HTTP/HTTPS 80/443;
- Uvicorn/FastAPI: только 127.0.0.1:8088;
- systemd: `kp-api.service`;
- рабочие данные: `/opt/kp-api/data` на VPS;
- секреты: `/etc/kp-api/kp-api.env`, права 600;
- GitHub runtime backup: отключён через `RUNTIME_REMOTE_BACKUP_ENABLED=false` и пустой `GITHUB_REPO`;
- HTTPS: публично доверенный сертификат, автоматическое продление Certbot.

Перед запуском нужны отдельный российский VPS SpaceWeb и DNS-имя, направленное на его IPv4.
MAX требует webhook по HTTPS строго на порту 443; текущий VPS UDU для этого не используется, потому что его 443 занят hbbr.

Порядок:
1. Загрузить приложение в `/opt/kp-api` без `.git`, `private/` и `checkpoints/`.
2. Создать `/etc/kp-api/kp-api.env` из шаблона и заполнить секреты локально на VPS.
3. Указать DNS A-запись на новый VPS.
4. Запустить `LE_EMAIL=... bash deploy/spaceweb-kp/bootstrap.sh <domain>`.
5. Проверить `/healthz`, вход в интерфейс, чтение КП и ручной refresh из 1С.
6. Только после этого включать отдельный MAX webhook на российском VPS.
