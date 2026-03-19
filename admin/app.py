import math
import os
import sys
from datetime import date, timedelta


from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

# Add parent dir to path so we can import database, config etc.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from database import Database
from admin.auth import verify_password, create_session_token, verify_session_token

app = FastAPI(title="Lunda Admin")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Lazy DB connection
_db: Database | None = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
    return _db


# --- Auth middleware ---

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        public_paths = {"/login", "/favicon.ico"}
        if request.url.path in public_paths:
            return await call_next(request)

        token = request.cookies.get("session")
        email = verify_session_token(token) if token else None
        if not email:
            return RedirectResponse("/login", status_code=302)

        request.state.user = email
        return await call_next(request)


app.add_middleware(AuthMiddleware)


# --- Routes ---

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, email: str = Form(), password: str = Form()):
    origin = f"{request.url.scheme}://{request.url.netloc}"
    if await verify_password(email, password, origin=origin):
        token = create_session_token(email)
        response = RedirectResponse("/", status_code=302)
        response.set_cookie("session", token, max_age=86400, httponly=True)
        return response
    return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})


@app.get("/logout")
async def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie("session")
    return response


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    db = get_db()
    stats = db.get_scrape_stats()
    return templates.TemplateResponse("dashboard.html", {"request": request, "stats": stats})


@app.get("/events", response_class=HTMLResponse)
async def events_list(
    request: Request,
    page: int = 1,
    city: str = "",
    status: str = "",
    type: str = "",
    grade: str = "",
):
    db = get_db()
    events, total = db.get_events_paginated(
        page=page,
        per_page=50,
        city=city or None,
        game_status=status or None,
        event_type=type or None,
        min_grade=grade or None,
    )
    filters = db.get_filter_options()
    total_pages = math.ceil(total / 50) if total > 0 else 1

    ctx = {
        "request": request,
        "events": events,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "filters": filters,
        "current_filters": {"city": city, "status": status, "type": type, "grade": grade},
    }

    # HTMX partial render
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("_events_table.html", ctx)

    return templates.TemplateResponse("events.html", ctx)


@app.get("/events/{uid}", response_class=HTMLResponse)
async def event_detail(request: Request, uid: str):
    db = get_db()
    event = db.get_event_by_uid(uid)
    if not event:
        return HTMLResponse("Not found", status_code=404)
    return templates.TemplateResponse("event_detail.html", {"request": request, "event": event})



@app.get("/tournaments", response_class=HTMLResponse)
async def tournaments_list(
    request: Request,
    page: int = 1,
    city: str = "",
    status: str = "",
    grade: str = "",
    club: str = "",
    owner: str = "",
    price_min: str = "",
    price_max: str = "",
    date_from: str = "",
    date_to: str = "",
    sort: str = "planned_date",
    sort_dir: str = "desc",
):
    db = get_db()
    events, total = db.get_tournaments(
        page=page,
        per_page=50,
        city=city or None,
        game_status=status or None,
        min_grade=grade or None,
        club_name=club or None,
        owner_name=owner or None,
        price_min=int(price_min) if price_min else None,
        price_max=int(price_max) if price_max else None,
        date_from=date_from or None,
        date_to=date_to or None,
        sort=sort,
        sort_dir=sort_dir,
    )
    filters = db.get_tournament_filters()
    total_pages = math.ceil(total / 50) if total > 0 else 1

    ctx = {
        "request": request,
        "events": events,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "filters": filters,
        "current_filters": {
            "city": city, "status": status, "grade": grade,
            "club": club, "owner": owner,
            "price_min": price_min, "price_max": price_max,
            "date_from": date_from, "date_to": date_to,
            "sort": sort, "sort_dir": sort_dir,
        },
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("_tournaments_table.html", ctx)

    return templates.TemplateResponse("tournaments.html", ctx)


@app.get("/tournaments/{uid}", response_class=HTMLResponse)
async def tournament_detail(request: Request, uid: str):
    db = get_db()
    event = db.get_event_by_uid(uid)
    if not event:
        return HTMLResponse("Не найден", status_code=404)
    history = db.get_tournament_history(uid)
    return templates.TemplateResponse("tournament_detail.html", {
        "request": request, "event": event, "history": history,
    })


@app.get("/analytics", response_class=HTMLResponse)
async def analytics(
    request: Request,
    date_from: str = "",
    date_to: str = "",
    metric: str = "count",
):
    db = get_db()
    today = date.today()
    d_to = date_to or (today + timedelta(days=14)).isoformat()
    d_from = date_from or (today - timedelta(days=30)).isoformat()

    daily = db.get_analytics_chart_data(d_from, d_to, metric)
    labels = [row["date"].isoformat() for row in daily]
    values = [float(row["value"]) if row["value"] is not None else 0 for row in daily]

    ratings = db.get_analytics_ratings(d_from, d_to)
    heatmap = db.get_heatmap_data(d_from, d_to)

    dow_names = {0: "Вс", 1: "Пн", 2: "Вт", 3: "Ср", 4: "Чт", 5: "Пт", 6: "Сб"}
    for row in ratings["by_dow"]:
        row["name"] = dow_names.get(row["dow"], str(row["dow"]))
        # Monday=1 for sorting starting from Monday (0=Sun becomes 7)
        row["dow_sort"] = row["dow"] if row["dow"] > 0 else 7
    ratings["by_dow"].sort(key=lambda r: r["dow_sort"])
    for row in ratings["by_hour"]:
        row["name"] = "%d:00" % row["hour"]

    metric_labels = {
        "count": "Количество турниров",
        "avg_price": "Средняя цена (₽)",
        "total_players": "Всего игроков",
        "avg_players": "Среднее кол-во игроков",
    }

    ctx = {
        "request": request,
        "labels": labels,
        "values": values,
        "date_from": d_from,
        "date_to": d_to,
        "metric": metric,
        "metric_labels": metric_labels,
        "ratings": ratings,
        "heatmap": heatmap,
        "today": today.isoformat(),
        "dow_names": dow_names,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("_analytics_content.html", ctx)

    return templates.TemplateResponse("analytics.html", ctx)


@app.get("/reports", response_class=HTMLResponse)
async def reports_list(request: Request):
    return templates.TemplateResponse("reports.html", {"request": request})


@app.get("/reports/{report_id}", response_class=HTMLResponse)
async def report_detail(request: Request, report_id: str):
    template_name = f"reports/{report_id}.html"
    try:
        return templates.TemplateResponse(template_name, {"request": request})
    except Exception:
        return HTMLResponse("Отчёт не найден", status_code=404)


@app.get("/scrape-log", response_class=HTMLResponse)
async def scrape_log(request: Request, page: int = 1):
    db = get_db()
    logs, total = db.get_scrape_log_paginated(page=page, per_page=50)
    total_pages = math.ceil(total / 50) if total > 0 else 1
    return templates.TemplateResponse("scrape_log.html", {
        "request": request,
        "logs": logs,
        "total": total,
        "page": page,
        "total_pages": total_pages,
    })
