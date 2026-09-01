# SpaceWeb KP deployment

Цель: заменить Render российским VPS SpaceWeb для КП/1С и отдельного MAX-бота.

Текущий VPS:
- имя: `KP-MAX`;
- IPv4: `89.111.131.83`;
- Ubuntu 24.04 LTS, Москва;
- nginx: 80/443;
- Uvicorn/FastAPI: `127.0.0.1:8088`;
- systemd: `kp-api.service`;
- приложение VPS запускается как `spaceweb_app:app`, Render — как `api_proxy:app`.

Хранение и безопасность:
- рабочие данные: `/opt/kp-api/data`;
- секреты: `/etc/kp-api/kp-api.env`, mode 600;
- GitHub runtime backup отключён;
- MAX webhook защищён `X-Max-Bot-Api-Secret`;
- CA Минцифры используется только клиентом MAX и не добавлен системно;
- TLS: Let's Encrypt IP certificate для `89.111.131.83`;
- продление TLS: `kp-cert-renew.timer` дважды в сутки.

Проверено:
- `https://89.111.131.83/healthz` -> HTTP 200, локальный cache 300 КП;
- `/api/kp/all` без авторизации -> HTTP 401;
- webhook без секрета -> HTTP 401;
- команда `КП 588` доходит до отправки MAX; пока ожидаемо падает без `KP_MAX_BOT_TOKEN`;
- TLS до `platform-api2.max.ru` с отдельным CA-файлом проходит.
