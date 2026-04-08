# Развертывание на Render.com (бесплатно)

## Шаг 1: Подготовка GitHub репозитория
```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/1c-kp-app.git
git push -u origin main
```

## Шаг 2: На Render.com
1. Перейдите на https://render.com
2. Нажмите "New" → "Web Service"
3. Подключите GitHub репозиторий
4. Заполните:
   - **Name**: `1c-kp-api`
   - **Runtime**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn api_proxy:app --host 0.0.0.0 --port 8000`
5. Нажмите Deploy

Render выдаст публичный URL: `https://1c-kp-api.onrender.com`

## Шаг 3: GitHub Pages для фронтенда (опционально)
1. На GitHub откройте Settings → Pages
2. Выберите branch `main`, папка `/ (root)`
3. GitHub Pages будет доступна по `https://YOUR_USERNAME.github.io/1c-kp-app`

## Шаг 4: Если развертываете только бэкенд на Render
Фронтенд будет доступен автоматически по адресу бэкенда (Render маунтит статику).

URL таблицы: `https://1c-kp-api.onrender.com`

## Локальное тестирование перед деплоем:
```bash
pip install -r requirements.txt
python api_proxy.py
# или
uvicorn api_proxy:app --reload
```

Откройте http://localhost:8000

---

## Примечания:
- **Render**: сервер засыпает через 15 минут неактивности (бесплатный уровень) → первый запрос может быть медленным
- **GitHub Pages**: только для статики (нельзя проксировать к 1C), нужен отдельный бэкенд
- Данные кэшируются в `kp_2026_march_april.json`, обновляется вручную через `fetch_kp_march.py`
