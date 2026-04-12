# Развертывание и структура

## Корневая структура
- `api_proxy.py` — backend и статика для Render
- `app.js`, `index.html`, `styles.css` — клиент
- `render.yaml`, `requirements.txt`, `.gitignore` — конфигурация
- `data/` — snapshot-данные и runtime seed metadata
- `tools/` — служебные и исследовательские скрипты

## Render
Сервис разворачивается по [render.yaml](render.yaml):
- build command: `pip install -r requirements.txt`
- start command: `python -m uvicorn api_proxy:app --host 0.0.0.0 --port $PORT`
- health check: `/healthz`

Фронтенд обслуживается тем же backend. Отдельный fallback на локальный JSON во фронтенде не используется.

## Данные
- основной snapshot backend: `data/kp_2026_march_april.json`
- runtime metadata для seed: `data/kp_seed_meta.json`
- исторические или ручные выгрузки: `data/`

После первого успешного live-refresh backend больше не возвращается к snapshot-файлу как к источнику отката.

## Служебные скрипты
- `tools/fetch_kp_march.py` — ручная выгрузка snapshot в `data/`
- `tools/auto_update.ps1` — локальный цикл запуска ручной выгрузки
- `tools/explore_1c.py` — исследовательский скрипт для OData

## Локальный запуск
```bash
pip install -r requirements.txt
python api_proxy.py
```

или

```bash
python -m uvicorn api_proxy:app --reload
```
