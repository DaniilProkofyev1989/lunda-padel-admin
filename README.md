# Lunda Admin

Админ-панель для просмотра ивентов Lunda Padel: дашборд, список игр с фильтрами, история скрейпов.

## Стек

- **Backend**: FastAPI + Jinja2 + HTMX + Bootstrap 5
- **Database**: Neon PostgreSQL (read-only, данные наполняются скрейпером)
- **Auth**: Neon Auth (email/password)
- **Deploy**: Timeweb VPS, systemd, GitHub Actions

## Быстрый старт

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # заполнить DATABASE_URL, NEON_AUTH_URL, SECRET_KEY
uvicorn admin.app:app --port 8000
```

Открыть http://localhost:8000/login

## Структура

```
├── admin/
│   ├── app.py          # FastAPI: dashboard, events, scrape log
│   ├── auth.py         # Neon Auth + signed session cookies
│   └── templates/      # Jinja2 шаблоны
├── database.py         # PostgreSQL: чтение events + scrape_log
├── config.py           # DATABASE_URL из .env
├── deploy/
│   └── lunda-admin.service   # systemd unit
└── .github/workflows/
    └── deploy.yml      # Push to main → deploy to VPS
```

## Env-переменные

| Переменная | Описание |
|-----------|----------|
| `DATABASE_URL` | PostgreSQL connection string (Neon) |
| `ADMIN_EMAIL` | Email для входа в админку |
| `NEON_AUTH_URL` | Neon Auth endpoint |
| `SECRET_KEY` | Ключ для подписи session cookies |
