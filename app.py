"""Серенити Мун — марафон: приём оплаты + табличка для Милы.

Не трогает сам кабинет участниц (это следующий этап) — только:
- приём формы Venmo/Zelle (POST /register)
- приём вебхука Stripe (POST /webhook/stripe)
- админ-табличка со списком и кнопкой "отметить оплаченным" (GET /admin)

Настройки: .env (см. .env.example). Без реальных ключей Stripe/Resend
всё работает в тестовом режиме — только без реальной отправки.
"""
import base64
import csv
import hashlib
import hmac
import html
import io
import json
import os
import re
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg
from aiohttp import web, ClientSession

HERE = Path(__file__).parent
ENV = HERE / ".env"
if ENV.exists():
    for line in ENV.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

SECRET = os.environ.get("PLATFORM_SECRET", "").encode()
HOST = os.environ.get("PLATFORM_HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", os.environ.get("PLATFORM_PORT", "8091")))  # Render задаёт PORT сам
COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "0") == "1"
COOKIE = "mila_admin"
SESSION_DAYS = 14

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")  # обычный текст, задаётся в Render → Environment

STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")  # resend.com
MAIL_FROM = os.environ.get("MAIL_FROM", "")  # напр. marathon@serenitymoonevents.com
MAIL_REPLY_TO = os.environ.get("MAIL_REPLY_TO", "")  # куда попадают ответы участниц
CHANNEL_INVITE_LINK = os.environ.get("CHANNEL_INVITE_LINK", "")  # обычная (не одноразовая) ссылка на телеграм-канал
DATABASE_URL = os.environ.get("DATABASE_URL", "")  # supabase/postgres connection string
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")  # напр. https://xxxx.supabase.co
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")  # публичный (anon/publishable) ключ — НЕ service_role

# пул подключений к базе — создаётся асинхронно при старте (см. on_startup), пока сервер
# не поднялся — DB_POOL == None
DB_POOL: asyncpg.Pool | None = None


async def init_db(app):
    global DB_POOL
    if not DATABASE_URL:
        raise SystemExit("Нет DATABASE_URL в .env — нужна строка подключения к supabase/postgres, см. .env.example")
    DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, statement_cache_size=0)
    await DB_POOL.execute("""CREATE TABLE IF NOT EXISTS signups (
        id SERIAL PRIMARY KEY,
        name TEXT, email TEXT, email_norm TEXT, instagram TEXT, phone TEXT, comment TEXT,
        method TEXT,                 -- stripe | venmo | zelle
        amount REAL,
        status TEXT DEFAULT 'ждёт',  -- ждёт | оплачено
        invite_sent INTEGER DEFAULT 0,
        note TEXT,
        created DOUBLE PRECISION, confirmed DOUBLE PRECISION
    )""")
    await DB_POOL.execute("CREATE INDEX IF NOT EXISTS idx_email_norm ON signups(email_norm)")

    # ---- контент-админка (2026-10-16): папки/темы/материалы/эфиры/мероприятия Милы ----
    await DB_POOL.execute("""CREATE TABLE IF NOT EXISTS folders (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT DEFAULT '',
        sort INTEGER DEFAULT 0, created DOUBLE PRECISION
    )""")
    await DB_POOL.execute("""CREATE TABLE IF NOT EXISTS themes (
        id TEXT PRIMARY KEY, label TEXT NOT NULL, created DOUBLE PRECISION
    )""")
    await DB_POOL.execute("""CREATE TABLE IF NOT EXISTS materials (
        id SERIAL PRIMARY KEY, folder_id TEXT REFERENCES folders(id) ON DELETE CASCADE,
        title TEXT NOT NULL, format TEXT, tags JSONB DEFAULT '[]',
        day INTEGER,
        url TEXT,              -- внешняя ссылка (если формат "ссылка")
        storage_key TEXT,      -- путь в supabase storage (если файл реально загружен)
        calendar_event_id INTEGER, created DOUBLE PRECISION
    )""")
    await DB_POOL.execute("""CREATE TABLE IF NOT EXISTS calendar_events (
        id SERIAL PRIMARY KEY, date DATE NOT NULL, title TEXT NOT NULL,
        type TEXT, time TEXT, link TEXT, created DOUBLE PRECISION
    )""")
    await DB_POOL.execute("""CREATE TABLE IF NOT EXISTS mila_events (
        id SERIAL PRIMARY KEY, title TEXT NOT NULL, date_place TEXT NOT NULL,
        link TEXT, color TEXT DEFAULT 'lav', created DOUBLE PRECISION
    )""")

    # ---- кабинет участниц (2026-10-19): профиль / выбранные привычки / ежедневные отметки.
    # id профиля = id из Supabase Auth (auth.users.id, uuid) — заполняется при реальной
    # регистрации, когда подключим supabase auth (ещё не подключено на момент создания).
    await DB_POOL.execute("""CREATE TABLE IF NOT EXISTS profiles (
        id UUID PRIMARY KEY, email TEXT UNIQUE NOT NULL, email_norm TEXT NOT NULL,
        name TEXT DEFAULT '', age INTEGER, city TEXT DEFAULT '', bio TEXT DEFAULT '',
        photo_url TEXT, created DOUBLE PRECISION
    )""")
    await DB_POOL.execute("CREATE INDEX IF NOT EXISTS idx_profiles_email_norm ON profiles(email_norm)")
    await DB_POOL.execute("""CREATE TABLE IF NOT EXISTS habit_selections (
        id SERIAL PRIMARY KEY, user_id UUID NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
        name TEXT NOT NULL, config JSONB DEFAULT '{}', sort INTEGER DEFAULT 0, created DOUBLE PRECISION
    )""")
    await DB_POOL.execute("CREATE INDEX IF NOT EXISTS idx_habit_sel_user ON habit_selections(user_id)")
    await DB_POOL.execute("""CREATE TABLE IF NOT EXISTS daily_logs (
        id SERIAL PRIMARY KEY, user_id UUID NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
        log_date DATE NOT NULL, habits_done JSONB DEFAULT '[]',
        closed BOOLEAN DEFAULT FALSE, closed_at DOUBLE PRECISION,
        UNIQUE(user_id, log_date)
    )""")
    await DB_POOL.execute("CREATE INDEX IF NOT EXISTS idx_daily_logs_user ON daily_logs(user_id)")

    # ---- глобальные настройки платформы (ключ-значение) — напр. "cabinet_opens_at" (unix-время,
    # разовое открытие доступа всем оплатившим разом, см. 2026-10-13 решение в памяти проекта) ----
    await DB_POOL.execute("""CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY, value TEXT
    )""")
    # поля для карточки автора (topics/photo) — добавлены позже исходной таблицы folders
    await DB_POOL.execute("ALTER TABLE folders ADD COLUMN IF NOT EXISTS topics TEXT DEFAULT ''")
    await DB_POOL.execute("ALTER TABLE folders ADD COLUMN IF NOT EXISTS photo_url TEXT")
    await DB_POOL.execute("ALTER TABLE folders ADD COLUMN IF NOT EXISTS dot_color TEXT DEFAULT ''")

    await seed_materials_once()


async def seed_materials_once():
    """Разовый перенос старых data/materials.json (13 материалов, 4 автора) в
    реальные таблицы — выполняется только если materials пока пустая (т.е. один
    раз, при первом запуске после появления этих таблиц). Дальше источником
    правды становится админка (/api/admin/folders|themes|materials), не файл."""
    if await DB_POOL.fetchval("SELECT COUNT(*) FROM materials"):
        return
    if not MATERIALS_FILE.exists():
        return
    data = json.loads(MATERIALS_FILE.read_text(encoding="utf-8"))
    now = time.time()
    for key, a in data.get("authors", {}).items():
        await DB_POOL.execute(
            "INSERT INTO folders (id, name, description, topics, photo_url, created) "
            "VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO NOTHING",
            key, a.get("name", key), a.get("desc", ""), a.get("topics", ""), a.get("photo"), now,
        )
    seen_tags = set()
    for m in data.get("materials", []):
        for t in m.get("tags", []):
            seen_tags.add(t)
    for t in seen_tags:
        await DB_POOL.execute(
            "INSERT INTO themes (id, label, created) VALUES ($1,$2,$3) ON CONFLICT (id) DO NOTHING",
            t, t, now,
        )
    for m in data.get("materials", []):
        await DB_POOL.execute(
            "INSERT INTO materials (folder_id, title, format, tags, day, url, created) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            m.get("author"), m.get("title", ""), m.get("type", "link"),
            json.dumps(m.get("tags", []), ensure_ascii=False), m.get("day"), m.get("url", "#"), now,
        )
    print(f"[миграция] перенесено {len(data.get('authors', {}))} папок и {len(data.get('materials', []))} материалов из materials.json в supabase")


async def close_db(app):
    if DB_POOL:
        await DB_POOL.close()


def normalize_email(e: str) -> str:
    return (e or "").strip().lower()


# ---------- материалы ----------
MATERIALS_FILE = HERE / "data" / "materials.json"  # больше не источник правды, см. seed_materials_once()
MARATHON_START = "2026-11-09"
MARATHON_TZ = "America/New_York"


def load_materials() -> dict:
    """Старый путь (читает JSON-файл) — больше не вызывается, оставлено как
    запасной вариант. Реальный источник данных теперь load_materials_from_db()."""
    return json.loads(MATERIALS_FILE.read_text(encoding="utf-8"))


def _jsonb(v):
    """asyncpg по умолчанию отдаёт jsonb как голую строку, не питоновский объект."""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (TypeError, ValueError):
            return v
    return v


async def load_materials_from_db() -> dict:
    folders = await DB_POOL.fetch("SELECT * FROM folders ORDER BY sort, name")
    materials = await DB_POOL.fetch("SELECT * FROM materials ORDER BY day NULLS FIRST, id")
    authors = {
        f["id"]: {"name": f["name"], "topics": f["topics"] or "", "desc": f["description"] or "",
                   "photo": f["photo_url"], "color": f["dot_color"] or ""}
        for f in folders
    }
    mats = [
        {"id": str(m["id"]), "author": m["folder_id"], "title": m["title"],
         "type": m["format"] or "link", "tags": _jsonb(m["tags"]) or [], "day": m["day"], "url": m["url"] or "#"}
        for m in materials
    ]
    return {"marathon_start": MARATHON_START, "timezone": MARATHON_TZ, "authors": authors, "materials": mats}


def marathon_day_today(marathon_start: str, tz_name: str) -> int:
    """День марафона "сегодня" (1 = старт). Может быть <1 (до старта) или >21
    (после финиша) — страница сама решает, что в этом случае показать."""
    start = date.fromisoformat(marathon_start)
    now = datetime.now(ZoneInfo(tz_name)).date()
    return (now - start).days + 1



# ---------- почта (не падает без ключей — просто логирует) ----------
QR_PATHS = {"venmo": HERE / "qr-venmo.jpg", "zelle": HERE / "qr-zelle.jpg"}


async def send_payment_qr_email(to_email: str, name: str, method: str):
    """Дублирует QR-код на почту сразу после заявки (venmo/zelle) — на случай если
    человек закроет страницу до оплаты и потеряет код."""
    greet_name = name if name else "дорогая участница"
    method_title = "Venmo" if method == "venmo" else "Zelle"
    body = (f"Привет, {greet_name}! \n\n"
            f"Спасибо за заявку на марафон «Начни сегодня» 💗 \n\n"
            f"Высылаю код оплаты через {method_title}. QR-код во вложении. \n"
            f"После оплаты пришли, пожалуйста, чек в личные сообщения Миле @milaserenity в Телеграм.\n\n"
            f"До скорой встречи! ✨\n"
            f"Мила и команда Serenity Moon")
    if not (RESEND_API_KEY and MAIL_FROM):
        print(f"[DEV] письмо с qr для {to_email} не отправлено (нет RESEND_API_KEY/MAIL_FROM)")
        return
    qr_path = QR_PATHS.get(method)
    attachments = []
    if qr_path and qr_path.exists():
        attachments.append({
            "filename": f"{method}-qr.jpg",
            "content": base64.b64encode(qr_path.read_bytes()).decode(),
        })
    payload = {
        "from": MAIL_FROM,
        "to": [to_email],
        "subject": f"Serenity Moon — QR-код для оплаты ({method_title})",
        "text": body,
        "attachments": attachments,
    }
    if MAIL_REPLY_TO:
        payload["reply_to"] = MAIL_REPLY_TO
    async with ClientSession() as s:
        async with s.post(
            "https://api.resend.com/emails",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {RESEND_API_KEY}"},
            data=json.dumps(payload),
        ) as resp:
            if resp.status >= 300:
                data = await resp.text()
                print(f"[ПРЕДУПРЕЖДЕНИЕ] письмо с qr не ушло ({resp.status}): {data}")


async def send_access_email(to_email: str, name: str):
    invite = CHANNEL_INVITE_LINK or "[DEV] ссылка появится, как только настроим CHANNEL_INVITE_LINK"
    greet_name = name if name else "дорогая участница"
    body = (f"Привет, {greet_name}! \n\n"
            f"Как здорово, что ты решила присоединиться к нашему 21-дневному марафону «Начни сегодня»!\n\n"
            f"🟣 Вход в наш закрытый Telegram-канал, где будет проходить марафон по ссылке:\n\n"
            f"{invite}\n\n"
            f"🌿 Что дальше?\n\n"
            f"Наш марафон стартует 8 ноября!\n\n"
            f"1 ноября ты получишь на эту же почту ссылку для доступа к онлайн-платформе со всеми материалами и практиками. "
            f"Ссылка также будет опубликована в нашем Telegram-канале.\n\n"
            f"Очень рада, что ты с нами! 💗\n\n"
            f"До скорой встречи!\n"
            f"Мила и команда Serenity Moon")
    if not (RESEND_API_KEY and MAIL_FROM):
        print(f"[DEV] письмо для {to_email} не отправлено (нет RESEND_API_KEY/MAIL_FROM):\n{body}")
        return
    payload = {
        "from": MAIL_FROM,
        "to": [to_email],
        "subject": "Serenity Moon — доступ к марафону",
        "text": body,
    }
    if MAIL_REPLY_TO:
        payload["reply_to"] = MAIL_REPLY_TO
    async with ClientSession() as s:
        async with s.post(
            "https://api.resend.com/emails",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {RESEND_API_KEY}"},
            data=json.dumps(payload),
        ) as resp:
            if resp.status >= 300:
                data = await resp.text()
                print(f"[ПРЕДУПРЕЖДЕНИЕ] письмо не ушло ({resp.status}): {data}")


# ---------- сессия админа ----------
def sign(payload: str) -> str:
    sig = hmac.new(SECRET, payload.encode(), hashlib.sha256).hexdigest()[:32]
    return base64.urlsafe_b64encode(f"{payload}.{sig}".encode()).decode()


def check_session(token: str) -> bool:
    try:
        payload, sig = base64.urlsafe_b64decode(token.encode()).decode().rsplit(".", 1)
        good = hmac.new(SECRET, payload.encode(), hashlib.sha256).hexdigest()[:32]
        if not hmac.compare_digest(sig, good):
            return False
        issued = float(payload.split(":")[1])
        return time.time() - issued <= SESSION_DAYS * 86400
    except Exception:
        return False


def authed(request) -> bool:
    return check_session(request.cookies.get(COOKIE, ""))


FAILS = {}
MAX_FAILS, WINDOW = 8, 600


def too_many(ip) -> bool:
    now = time.time()
    FAILS[ip] = [t for t in FAILS.get(ip, []) if now - t < WINDOW]
    return len(FAILS[ip]) >= MAX_FAILS


E = html.escape
HEADERS = {
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "X-Frame-Options": "DENY",
}


def html_response(text, status=200):
    return web.Response(text=text, status=status, content_type="text/html", headers=HEADERS)


# ---------- страницы ----------
def login_page(error: str = "") -> str:
    err = f'<p style="color:#A9752E;font-size:14px;">{E(error)}</p>' if error else ""
    return f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Вход — заявки марафона</title>
<style>
body{{margin:0;font-family:Inter,system-ui,sans-serif;background:#F7F4F1;color:#352F3D;
display:flex;align-items:center;justify-content:center;min-height:100vh}}
.card{{background:#fff;border:1px solid #E7E1DC;border-radius:16px;padding:32px;width:280px}}
input{{width:100%;box-sizing:border-box;padding:10px 12px;border:1px solid #E7E1DC;border-radius:8px;
font-size:15px;margin-top:6px}}
button{{margin-top:16px;width:100%;background:#7C6A9C;color:#fff;border:none;border-radius:8px;
padding:11px;font-size:14px;cursor:pointer}}
</style></head><body>
<div class="card">
<h2 style="margin:0 0 4px;font-size:18px;">Заявки марафона</h2>
<p style="margin:0;font-size:13px;color:#8A8390;">только для Милы и Ники</p>
<form method="post" action="/admin/login">
<label style="font-size:13px;">Пароль</label>
<input type="password" name="password" required autofocus>
{err}
<button type="submit">Войти</button>
</form>
</div></body></html>"""


def admin_page(rows) -> str:
    def row_html(r):
        paid = r["status"] == "оплачено"
        badge = ('<span style="background:#EAF1E8;color:#4F7249;padding:4px 10px;border-radius:999px;'
                  'font-size:12px;font-weight:600;">оплачено</span>') if paid else \
                ('<span style="background:#FBF0DE;color:#A9752E;padding:4px 10px;border-radius:999px;'
                  'font-size:12px;font-weight:600;">ждёт</span>')
        confirm_btn = "" if paid or r["method"] == "stripe" else \
            f'<form method="post" action="/admin/confirm/{r["id"]}" style="display:inline;">' \
            f'<button style="background:#7C6A9C;color:#fff;border:none;border-radius:8px;' \
            f'padding:7px 12px;font-size:12px;cursor:pointer;">Отметить оплаченным</button></form>'
        delete_btn = (f'<form method="post" action="/admin/delete/{r["id"]}" style="display:inline;" '
                      f'onsubmit="return confirm(\'Удалить эту заявку?\');">'
                      f'<button style="background:none;color:#B3415A;border:1px solid #EAD7DB;border-radius:8px;'
                      f'padding:7px 10px;font-size:12px;cursor:pointer;">удалить</button></form>')
        dt = time.strftime("%d.%m %H:%M", time.localtime(r["confirmed"] or r["created"]))
        note_val = E(r["note"] or "")
        note_form = (f'<form method="post" action="/admin/note/{r["id"]}" style="display:flex;gap:6px;">'
                     f'<input name="note" value="{note_val}" placeholder="заметка…" '
                     f'style="flex:1;min-width:0;border:1px solid #E7E1DC;border-radius:8px;padding:6px 8px;font-size:12.5px;font-family:inherit;">'
                     f'<button style="background:#F2EEF7;color:#5A5266;border:none;border-radius:8px;'
                     f'padding:6px 10px;font-size:12px;cursor:pointer;">✓</button></form>')
        return (f'<tr style="border-bottom:1px solid #F0EBE6;">'
                f'<td style="padding:12px 16px;">{E(r["name"] or "—")}</td>'
                f'<td style="padding:12px 16px;color:#6B6470;">{E(r["email"])}</td>'
                f'<td style="padding:12px 16px;color:#6B6470;">{E(r["phone"] or "—")}</td>'
                f'<td style="padding:12px 16px;color:#6B6470;">{E(r["method"])}</td>'
                f'<td style="padding:12px 16px;">{badge}</td>'
                f'<td style="padding:12px 16px;color:#6B6470;">{dt}</td>'
                f'<td style="padding:12px 16px;min-width:160px;">{note_form}</td>'
                f'<td style="padding:12px 16px;text-align:right;white-space:nowrap;">{confirm_btn} {delete_btn}</td></tr>')

    total = len(rows)
    paid_n = sum(1 for r in rows if r["status"] == "оплачено")
    waiting_n = total - paid_n
    rows_html = "".join(row_html(r) for r in rows) or \
        '<tr><td colspan="8" style="padding:24px;text-align:center;color:#8A8390;">пока пусто</td></tr>'

    return f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Заявки и оплаты</title>
<style>
body{{margin:0;font-family:Inter,system-ui,sans-serif;background:#F7F4F1;color:#352F3D;padding:32px 20px}}
.wrap{{max-width:1100px;margin:0 auto}}
.stat{{background:#fff;border:1px solid #E7E1DC;border-radius:12px;padding:14px 20px;min-width:130px}}
table{{width:100%;border-collapse:collapse;background:#fff;border:1px solid #E7E1DC;border-radius:14px;overflow:hidden}}
th{{text-align:left;padding:12px 16px;background:#F2EEF7;font-size:13px}}
input,select{{font:inherit}}
details.addform summary{{cursor:pointer;font-size:13.5px;color:#5A5266;font-weight:600;padding:10px 0}}
.addform-body{{background:#fff;border:1px solid #E7E1DC;border-radius:12px;padding:16px;display:flex;flex-wrap:wrap;gap:10px;align-items:end}}
.addform-body label{{display:flex;flex-direction:column;gap:4px;font-size:12px;color:#8A8390}}
.addform-body input,.addform-body select{{border:1px solid #E7E1DC;border-radius:8px;padding:8px 10px;font-size:13.5px}}
</style></head><body>
<div class="wrap">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">
<h1 style="font-size:24px;margin:0;">Заявки марафона</h1>
<div style="display:flex;gap:16px;align-items:center;">
<a href="/admin/export.csv" style="font-size:13px;color:#7C6A9C;font-weight:600;">скачать CSV</a>
<a href="/admin/logout" style="font-size:13px;color:#8A8390;">выйти</a>
</div>
</div>
<div style="display:flex;gap:14px;margin-bottom:20px;">
<div class="stat"><div style="font-size:12px;color:#8A8390;">всего</div><div style="font-size:22px;font-weight:600;">{total}</div></div>
<div class="stat"><div style="font-size:12px;color:#8A8390;">оплачено</div><div style="font-size:22px;font-weight:600;color:#6F8F6B;">{paid_n}</div></div>
<div class="stat"><div style="font-size:12px;color:#8A8390;">ждут</div><div style="font-size:22px;font-weight:600;color:#C08A3E;">{waiting_n}</div></div>
</div>
<details class="addform" style="margin-bottom:16px;">
<summary>+ добавить вручную</summary>
<form class="addform-body" method="post" action="/admin/add">
<label>Имя<input name="name"></label>
<label>Email <span style="color:#B3415A">*</span><input name="email" type="email" required></label>
<label>Телефон<input name="phone"></label>
<label>Способ<select name="method"><option value="venmo">venmo</option><option value="zelle">zelle</option><option value="stripe">stripe</option></select></label>
<label>Статус<select name="status"><option value="ждёт">ждёт</option><option value="оплачено">оплачено</option></select></label>
<label>Заметка<input name="note"></label>
<button style="background:#7C6A9C;color:#fff;border:none;border-radius:8px;padding:9px 16px;font-size:13.5px;cursor:pointer;">Добавить</button>
</form>
</details>
<table><thead><tr><th>Имя</th><th>Почта</th><th>Телефон</th><th>Способ</th><th>Статус</th><th>Дата</th><th>Заметка</th><th></th></tr></thead>
<tbody>{rows_html}</tbody></table>
</div></body></html>"""


# Полная админка (Материалы/Календарь/Мероприятия — визуальный прототип без сохранения,
# CRM — реальные данные через /api/admin/*) — одна статичная страница, отдаётся прямо
# с сервера (не claude.ai-артефакт), под той же cookie-сессией, что и остальной /admin.
# Материалы/Календарь/Мероприятия пока только в браузере участницы (сбрасываются при
# обновлении), CRM работает по-настоящему.
CABINET_APP_HTML = """<!doctype html><html><head><meta charset=utf8><meta name=viewport content="width=device-width,initial-scale=1,viewport-fit=cover"><style>:root{color-scheme:light;box-sizing:border-box;padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}html{scroll-padding-top:env(safe-area-inset-top,0px)}body{margin:0;padding:0;font:14px -apple-system,BlinkMacSystemFont,sans-serif;background:#faf9f5;color:#141413}img{max-width:100%}[hidden]:not([hidden=until-found i]){display:none!important}</style></head><body>
<title>Serenity Moon</title>
<style>
:root{
  --lav:#AA9DB8; --lav-ink:#6B5A7C; --sage:#6A9993; --sage-ink:#3F6660;
  --sand:#CBBAA8; --sand-ink:#8A6F55; --ink:#332E3D; --ink-soft:#6B6475;
  --bg:#FAF6F1; --surface:#FFFFFF; --surface-2:#F2ECE6; --line:#E7DED6;
  --shadow:0 10px 30px -14px rgba(51,46,61,.22); color-scheme:light;
  --h0:#ECEFEC; --h0-text:#6B6475; --h30:#B6CFC6; --h30-text:#3F6660;
  --h60:#7FA89C; --h60-text:#FFFFFF; --h100:#4C7D73; --h100-text:#FFFFFF; --h-future:#FFFFFF;
  --sys: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,system-ui,"Helvetica Neue",Arial,sans-serif;
}
*{box-sizing:border-box}
.btn:active,.editlink:active,.close-x:active,.chip:active,.habchip:active,.hab-edit:active,
.evt-row:active,.hday:active,.mcard:active,.evcard a:active{transform:scale(.96)}
.btn,.editlink,.close-x,.chip,.habchip{transition:transform .1s}
html,body{height:100%}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sys)}
h1,h2,h3{font-family:var(--sys);font-weight:600;letter-spacing:-.01em;margin:0;text-wrap:balance}
.eyebrow{font-family:var(--sys);font-style:italic;font-size:13px;color:var(--lav-ink);margin:0 0 4px}

.app{max-width:460px;margin:0 auto;min-height:100%;display:flex;flex-direction:column;position:relative}
.app[hidden]{display:none}
.topbar{padding:10px 18px 6px;padding-top:calc(10px + env(safe-area-inset-top,0px));display:flex;align-items:center;gap:10px}
.topbar .mark{width:26px;height:26px;object-fit:contain}
.topbar .name{font-family:var(--sys);font-weight:600;font-size:14px;letter-spacing:.02em}
.name small{display:block;font-weight:400;font-size:10px;color:var(--ink-soft);letter-spacing:.02em;margin-top:1px}
.marathon-eyebrow{font-family:var(--sys);font-size:13px;font-weight:700;text-transform:uppercase;
  letter-spacing:.07em;color:var(--sage-ink);text-align:center;margin:0 0 12px}

main{flex:1;padding:0 18px calc(88px + env(safe-area-inset-bottom,0px))}
.screen[hidden]{display:none}

.bottomnav{
  position:fixed; left:0; right:0; bottom:0; display:flex; justify-content:center; z-index:30;
  padding-bottom:env(safe-area-inset-bottom,0px);
  background:var(--surface); border-top:1px solid var(--line);
  box-shadow:0 -8px 24px -16px rgba(51,46,61,.25);
}
.bottomnav .inner{
  width:100%; max-width:460px; display:flex;
}
.navbtn{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:4px;
  padding:11px 4px 9px;min-height:48px;background:transparent;border:none;border-radius:12px;
  color:var(--ink-soft);font-family:var(--sys);font-size:10.5px;font-weight:400;cursor:pointer;
  transform:scale(1);transition:background-color .15s ease-in-out,color .15s ease-in-out,transform .15s ease-in-out;
  -webkit-user-select:none;user-select:none;-webkit-tap-highlight-color:transparent}
.navbtn svg{width:21px;height:21px;color:currentColor}
.navbtn:active{transform:scale(.98)}
.navbtn.active{background:rgba(0,0,0,.07);color:var(--ink);font-weight:500}
@media (hover:hover){
  .navbtn:hover{background:rgba(0,0,0,.04);color:var(--ink)}
  .navbtn.active:hover{background:rgba(0,0,0,.08)}
}
.bottomnav .navbtn{min-height:48px}
.bottomnav .navbtn.active{font-weight:500}
.bottomnav .navbtn.active svg{transform:scale(1.05)}
@keyframes navTapBounce{0%{transform:scale(1)}40%{transform:scale(1.1)}70%{transform:scale(.97)}100%{transform:scale(1.05)}}
.bottomnav .navbtn svg.tapbounce{animation:navTapBounce .32s cubic-bezier(.34,1.56,.64,1)}
@media (prefers-reduced-motion: reduce){
  .bottomnav .navbtn svg.tapbounce{animation:none}
  .navbtn, .navbtn svg{transition:none}
}

/* ---------- десктоп: боковое меню ---------- */
.sidebar{display:none}
@media (min-width:900px){
  body{background:var(--bg)}
  .app{max-width:none;margin:0;flex-direction:row;align-items:flex-start;min-height:100vh}
  .topbar{display:none}
  .sidebar{
    display:flex;flex-direction:column;gap:2px;width:240px;flex-shrink:0;
    position:sticky;top:0;align-self:flex-start;height:100vh;padding:28px 14px;
    background:var(--bg);box-sizing:border-box
  }
  .sidebar .brand{display:flex;align-items:center;gap:10px;padding:4px 10px 22px}
  .sidebar .brand .mark{width:28px;height:28px;object-fit:contain}
  .sidebar .brand .name{font-family:var(--sys);font-weight:600;font-size:15px}
  .sidebar .navbtn{
    flex:none;flex-direction:row;justify-content:flex-start;align-items:center;gap:12px;width:100%;
    padding:11px 14px;min-height:auto;font-size:14px;font-weight:400;color:var(--ink-soft);
    text-align:left;margin:0
  }
  .sidebar .navbtn svg{width:18px;height:18px;flex-shrink:0}
  .sidebar .navbtn.active{font-weight:500}
  .sidebar .navbtn.active svg{transform:none}
  .bottomnav{display:none}
  main{
    flex:1;min-width:0;padding:0;background:var(--bg);min-height:100vh;box-sizing:border-box
  }
  .maininner{max-width:960px;margin:0 auto;padding:40px 48px 60px;box-sizing:border-box}
  .mgrid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
}

/* главная */
.dayhero{padding:18px;margin-bottom:10px}
.dh-top{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:10px}
.dh-title{font-size:15px;font-weight:700}
.dh-pct{font-size:12px;color:var(--ink-soft)}
.dh-pill{height:12px;border-radius:999px;background:var(--surface-2);overflow:hidden}
.dh-pill i{display:block;height:100%;border-radius:999px;background:linear-gradient(90deg,var(--lav),var(--sage));transition:width .5s}
.dh-sub{font-size:12.5px;color:var(--ink-soft);line-height:1.4;margin-top:10px}
.streak{display:flex;align-items:center;gap:9px;background:var(--surface);border:1px solid var(--line);
  border-radius:14px;padding:10px 16px;margin-bottom:12px;box-shadow:var(--shadow)}
.streak svg{width:20px;height:20px;color:var(--sand-ink);flex-shrink:0}
.streak .n{font-family:var(--sys);font-weight:600;font-size:15px}
.streak .t{font-size:11px;color:var(--ink-soft)}
.heatcard{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:13px 14px;box-shadow:var(--shadow);margin-bottom:10px}
.heatcard h3{font-size:12.5px;margin-bottom:7px;font-family:var(--sys);font-weight:600}
.weeklbl{font-size:9.5px;font-weight:400;text-transform:uppercase;letter-spacing:.06em;color:var(--ink-soft);opacity:.75;margin:7px 0 4px}
.weeklbl:first-of-type{margin-top:0}
.heatgrid{display:grid;grid-template-columns:repeat(7,1fr);gap:5px;margin-bottom:4px}
.hday{aspect-ratio:1;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:10px;
  font-weight:600;cursor:pointer;border:1.5px solid transparent;position:relative}
.hday-corner{position:absolute;top:2px;right:3px;font-size:6.5px;font-weight:500;opacity:.75}
.hday-main{display:flex;flex-direction:column;align-items:center;line-height:1.1}
.hday-dd{font-size:11px;font-weight:700}
.hday-mm{font-size:6.5px;font-weight:400;opacity:.85}
.hday.d0{background:var(--h0);color:var(--h0-text)}
.hday.d30{background:var(--h30);color:var(--h30-text)}
.hday.d60{background:var(--h60);color:var(--h60-text)}
.hday.d100{background:var(--h100);color:var(--h100-text)}
.hday.future{background:var(--h-future);border:1.5px dashed var(--line);color:var(--ink-soft);font-weight:400;opacity:.65}
.hday.istoday{border-color:#D4A94C;border-width:2.5px;box-shadow:0 0 0 4px rgba(212,169,76,.4),0 0 14px rgba(212,169,76,.5)}
.hint{font-size:11.5px;color:var(--ink-soft);text-align:center;line-height:1.5}

.info-hint{width:20px;height:20px;border-radius:50%;border:1.5px solid var(--line);background:var(--surface-2);
  color:var(--ink-soft);font-size:11.5px;font-weight:700;cursor:pointer;flex-shrink:0;display:inline-flex;
  align-items:center;justify-content:center;line-height:1;padding:0;font-family:var(--sys)}
h3 .info-hint{margin-left:6px;vertical-align:middle}

.statrow{display:grid;grid-template-columns:1fr 1fr 1fr;gap:7px;margin-bottom:10px}
.statrow .heatcard{margin-bottom:0;padding:11px 10px}
.statrow .diamond-icon{font-size:21px}
.statrow .diamond-val{font-size:16px}
.statrow .diamond-val .diamond-max{font-size:11px}
.statrow .diamond-lbl{font-size:9px}
.statrow .starsrow{gap:2px;padding-right:16px}
.statrow .star-icon{font-size:16px}
.statrow .star-day{font-size:7.5px}
.statrow h3{font-size:11px}

.diamond-row{display:flex;flex-direction:column;align-items:flex-start;gap:6px;height:100%}
.diamond-icon{font-size:26px;line-height:1}
.diamond-text{flex:1}
.diamond-val{font-family:var(--sys);font-size:19px;font-weight:700;color:var(--ink)}
.diamond-val .diamond-max{font-size:12.5px;font-weight:500;color:var(--ink-soft);margin-left:2px}
.diamond-lbl{font-size:10px;text-transform:uppercase;letter-spacing:.04em;color:var(--ink-soft);margin-top:2px}
#diamondCard .info-hint,#achCard .info-hint{position:absolute;top:13px;right:13px}
#diamondCard,#achCard{position:relative}

.starsrow{display:flex;justify-content:space-between;gap:4px;padding:2px 24px 0 0}
.star-item{display:flex;flex-direction:column;align-items:center;gap:4px}
.star-icon{font-size:22px;filter:grayscale(1);opacity:.35;line-height:1}
.star-icon.unlocked{filter:none;opacity:1}
.star-day{font-size:9px;color:var(--ink-soft)}

.mm-row{display:flex;align-items:center;gap:8px;padding:10px 0;border-top:1px solid var(--line)}
.mm-row:first-child{border-top:none}
.mm-title{font-size:13px;font-weight:600;flex-shrink:0;width:104px}
.mm-field{flex:1}
.mm-input{width:100%;font-size:13px;padding:8px 10px;border:1px solid var(--line);border-radius:10px;font-family:var(--sys)}
.mm-err{font-size:10.5px;color:#a8443a;margin:3px 0 0}
.mm-btn{font-size:12.5px;font-weight:600;padding:8px 12px;border-radius:10px;border:none;background:var(--lav);color:#fff;cursor:pointer;flex-shrink:0;font-family:var(--sys)}
.mm-done{font-size:12.5px;color:var(--sage-ink);font-weight:600;flex:1}

/* сегодня */
.affirm{background:linear-gradient(135deg,var(--lav),var(--sage) 130%);border-radius:18px;padding:22px 20px;
  color:#fff;text-align:center;margin:10px 0 18px;box-shadow:var(--shadow)}
.affirm .elbl{font-family:var(--sys);font-style:italic;font-size:12px;opacity:.85;margin-bottom:8px}
.affirm .txt{font-family:var(--sys);font-size:18px;font-weight:600;line-height:1.3}
.checklist{background:var(--surface);border:1px solid var(--line);border-radius:18px;padding:10px 12px;box-shadow:var(--shadow);margin-bottom:16px}
.hab{display:flex;align-items:center;gap:12px;padding:11px 10px;border-radius:12px;margin:2px 0;transition:.15s;cursor:pointer;-webkit-user-select:none;user-select:none}
.hab.done{background:var(--surface-2)}
.markvisual{width:24px;height:24px;border-radius:50%;border:2px dashed var(--line);background:var(--surface);
  display:flex;align-items:center;justify-content:center;flex-shrink:0;transition:.15s;overflow:hidden}
.markvisual svg{width:13px;height:13px;opacity:0;transition:.15s;flex-shrink:0}
.markwrap{position:relative;width:40px;height:40px;margin-left:-8px;flex-shrink:0}
.markwrap input{position:absolute;inset:0;opacity:0;margin:0;cursor:pointer;z-index:1}
.markwrap .markvisual{position:absolute;top:50%;left:50%;margin:-12px 0 0 -12px}
.markwrap input:checked + .markvisual, .markvisual.on{background:linear-gradient(135deg,var(--lav),var(--sage));border-color:transparent;
  animation:markpop .32s ease}
.markwrap input:checked + .markvisual svg, .markvisual.on svg{opacity:1}
@keyframes markpop{0%{transform:scale(.7)}55%{transform:scale(1.18)}100%{transform:scale(1)}}
@media (prefers-reduced-motion: reduce){
  .markwrap input:checked + .markvisual, .markvisual.on{animation:none}
}
.hab-text{font-size:14px;flex:1}
.hab.done .hab-text{color:var(--sage-ink);text-decoration:line-through;font-weight:500}
.hab-streak{font-size:11.5px;color:var(--sand-ink);font-weight:600;white-space:nowrap;flex-shrink:0}
.checklist-head{display:flex;justify-content:space-between;align-items:center;margin:4px 0 8px}
.checklist-head h3{font-size:13.5px;font-family:var(--sys);font-weight:600;color:var(--ink-soft)}
.day-status{font-size:13.5px;color:var(--ink-soft);margin:0 0 12px;line-height:1.4}
.day-status b{color:var(--ink);font-weight:700}
.editlink{background:none;border:none;color:var(--lav-ink);font-size:12.5px;font-weight:600;cursor:pointer;font-family:var(--sys)}
.pickcat{margin-bottom:20px}
.pickcat h4{font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:var(--ink-soft);margin-bottom:9px}
.chipgrid{display:flex;flex-wrap:wrap;gap:8px}
.habchip{display:inline-flex;align-items:center;gap:6px;padding:8px 8px 8px 14px;border:1.5px solid var(--line);
  border-radius:999px;cursor:pointer;background:var(--surface);transition:background-color .15s,border-color .15s}
.habchip span.htxt{font-size:13px;color:var(--ink)}
.habchip.checked{background:var(--sage);border-color:var(--sage)}
.habchip.checked span.htxt{color:#fff}
.habchip .hinfo{width:19px;height:19px;border-radius:50%;border:none;background:var(--surface-2);color:var(--ink-soft);
  font-size:11px;font-weight:700;flex-shrink:0;cursor:pointer;display:flex;align-items:center;justify-content:center}
.habchip.checked .hinfo{background:rgba(255,255,255,.3);color:#fff}
.pickintro{background:var(--surface);padding:0 0 16px;font-size:12.5px;color:var(--ink-soft);text-align:center;line-height:1.5}
.pickintro p{margin:0 0 8px}
.pickintro p:last-child{margin-bottom:0;color:var(--sage-ink);font-weight:600}
.custom-row{display:flex;gap:8px;margin-bottom:18px}
.custom-row input{flex:1;font-family:var(--sys);font-size:13.5px;border:1px solid var(--line);
  border-radius:12px;padding:10px 13px;color:var(--ink);background:var(--surface)}
.custom-row button{border:none;background:var(--lav);color:#fff;border-radius:12px;padding:0 16px;font-weight:600;cursor:pointer}
.savepick{width:100%;border:none;background:var(--lav);color:#fff;border-radius:999px;padding:13px;font-weight:600;
  font-size:13.5px;cursor:pointer;position:sticky;bottom:0}
.actions{display:flex;gap:10px;margin-top:16px;margin-bottom:10px}
.btn{flex:1;border:none;border-radius:999px;padding:13px;font-family:var(--sys);font-weight:600;
  font-size:13px;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:7px}
.btn svg{width:15px;height:15px}
.btn.primary{background:var(--lav);color:#fff}
.btn.primary:disabled{background:var(--surface-2);color:var(--ink-soft);cursor:default}
.btn.ghost{background:var(--surface);border:1px solid var(--line);color:var(--ink)}
.share-out{margin-top:14px}
.share-out img{width:100%;border-radius:16px;border:1px solid var(--line);box-shadow:var(--shadow)}
.share-note{font-size:11px;color:var(--ink-soft);margin-top:8px;line-height:1.5;text-align:center}
.evening-rem{display:flex;gap:10px;border:1.5px dashed var(--line);border-radius:14px;padding:13px 15px;margin-top:14px}
.evening-rem svg{width:17px;height:17px;color:var(--sage-ink);flex-shrink:0;margin-top:1px}
.evening-rem .lead{font-size:13px;font-weight:600;color:var(--ink);margin-bottom:4px}
.evening-rem .txt{font-size:12.5px;color:var(--ink-soft);line-height:1.4}

/* материалы */
.evt-next{background:linear-gradient(135deg,var(--lav),var(--sage) 130%);border-radius:14px;padding:13px 15px;color:#fff;margin:4px 0}
.evt-next .lbl{font-size:10px;text-transform:uppercase;letter-spacing:.05em;opacity:.85;margin-bottom:3px}
.evt-next .ttl{font-size:14.5px;font-weight:600;margin-bottom:2px}
.evt-next .meta{font-size:11.5px;opacity:.9}
.evt-row{display:flex;align-items:center;gap:11px;padding:10px 0;border-top:1px solid var(--line)}
.evt-row:first-of-type{border-top:none}
.evt-date{width:36px;flex-shrink:0;text-align:center}
.evt-date .d{font-size:15px;font-weight:700;line-height:1;color:var(--ink)}
.evt-date .l{font-size:8px;text-transform:uppercase;color:var(--ink-soft);margin-top:1px}
.evt-body{flex:1;min-width:0}
.evt-ttl{font-size:13px;font-weight:500}
.evt-meta{font-size:11px;color:var(--ink-soft);margin-top:1px}
.evt-tag{font-size:9.5px;font-weight:600;padding:3px 8px;border-radius:999px;background:var(--surface-2);color:var(--sage-ink);white-space:nowrap}
.evt-row.past{opacity:.5}
.evt-alert{display:flex;align-items:center;gap:9px;background:var(--surface);border:1px solid var(--line);border-radius:12px;
  padding:9px 12px;margin:0 0 14px;box-shadow:var(--shadow)}
.evt-alert-dot{width:7px;height:7px;border-radius:50%;background:var(--sage);flex-shrink:0}
.evt-alert-txt{font-size:12px;flex:1}
.evt-alert-txt b{font-weight:600}
.evt-alert-tag{font-size:11px;color:var(--ink-soft);white-space:nowrap}
.mat-layout{display:block}
.mat-side{margin-top:16px}
@media (min-width:900px){
  .mat-layout{display:flex;gap:20px;align-items:flex-start}
  .mat-main{flex:1;min-width:0}
  .mat-side{width:280px;flex-shrink:0;margin-top:0;position:sticky;top:40px}
  .hday-dd{font-size:14px}
  .hday-mm{font-size:8px}
  .hday-corner{font-size:8px}
}
.search-wrap{position:relative;margin:10px 0 16px}
.search-box{width:100%;font-size:14px;color:var(--ink);background:var(--surface);border:1px solid var(--line);
  border-radius:14px;padding:13px 16px 13px 42px;box-shadow:var(--shadow);font-family:var(--sys)}
.search-icon{position:absolute;left:15px;top:50%;transform:translateY(-50%);width:16px;height:16px;color:var(--ink-soft)}
.search-drop{position:absolute;left:0;right:0;top:calc(100% + 6px);background:var(--surface);border:1px solid var(--line);
  border-radius:14px;box-shadow:var(--shadow);overflow:hidden;z-index:30}
.search-drop[hidden]{display:none}
.sr-item{display:flex;gap:10px;padding:11px 14px;cursor:pointer;font-size:13px;border-top:1px solid var(--line)}
.sr-item:first-child{border-top:none}
.sr-item:hover{background:var(--surface-2)}
.filters{display:flex;flex-direction:column;gap:14px;margin-bottom:18px}
.filter-row{display:flex;flex-direction:column;gap:8px}
.filter-row .chipset{display:flex;flex-wrap:wrap;gap:10px 7px}
.flabel{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--ink-soft);width:100%}
.chip{font-size:12.5px;font-weight:500;color:var(--ink);background:var(--surface);border:1px solid var(--line);
  border-radius:999px;padding:6px 13px;cursor:pointer;font-family:var(--sys)}
.chip.active{background:var(--lav);border-color:var(--lav);color:#fff}
.chip.active.theme{background:var(--sage);border-color:var(--sage)}
.mgrid{display:flex;flex-direction:column;gap:12px}
.mcard{background:var(--surface);border:1px solid var(--line);border-radius:18px;padding:18px;cursor:pointer;
  box-shadow:var(--shadow);display:flex;gap:14px;align-items:flex-start}
.mcard[hidden]{display:none}
.dot{width:36px;height:36px;border-radius:50%;flex-shrink:0;overflow:hidden}
.dot img{width:100%;height:100%;object-fit:cover}
.mcard.mila .dot{background:var(--lav)} .mcard.maria .dot{background:var(--sage)}
.mcard.anna .dot{background:var(--sand)} .mcard.extra .dot{background:linear-gradient(135deg,var(--lav),var(--sage) 55%,var(--sand))}
.mcard .body{flex:1;min-width:0}
.mcard h3{font-size:16.5px;font-weight:600}
.mcard .topics{font-size:12px;color:var(--ink-soft);margin-top:2px}
.mcard .foot{display:flex;justify-content:space-between;font-size:11.5px;color:var(--ink-soft);margin-top:8px}
.mcard .count{font-weight:600;color:var(--ink)}

/* профиль */
.avatar-row{display:flex;flex-direction:column;align-items:center;margin:14px 0 20px}
.avatar{width:84px;height:84px;border-radius:50%;background:linear-gradient(135deg,var(--lav),var(--sand));
  display:flex;align-items:center;justify-content:center;color:#fff;position:relative;cursor:pointer}
.avatar svg{width:28px;height:28px}
.avatar .camera{position:absolute;bottom:-2px;right:-2px;width:26px;height:26px;background:var(--surface);
  border:1px solid var(--line);border-radius:50%;display:flex;align-items:center;justify-content:center;color:var(--ink-soft)}
.avatar .camera svg{width:13px;height:13px}
.avatar img{position:absolute;inset:0;width:100%;height:100%;border-radius:50%;object-fit:cover}
.field{margin-bottom:14px}
.field label{display:block;font-size:11.5px;color:var(--ink-soft);margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em}
.field input, .field textarea{
  width:100%;font-family:var(--sys);font-size:14px;color:var(--ink);background:var(--surface);
  border:1px solid var(--line);border-radius:12px;padding:11px 14px;resize:none
}
.field textarea{min-height:76px}
.optional{font-size:11px;color:var(--ink-soft);font-style:italic;font-family:var(--sys);margin:-10px 0 18px}
.sectitle{font-size:13px;font-weight:600;color:var(--ink-soft);text-transform:uppercase;letter-spacing:.06em;margin:22px 0 12px}
.profcard{background:var(--surface);border:1px solid var(--line);border-radius:18px;padding:20px;box-shadow:var(--shadow);margin-bottom:18px}
.profcard .sectitle{margin-top:0}
.v3{display:flex;align-items:center;gap:13px;cursor:pointer}
.v3-txt{flex:1;font-size:14px;font-weight:600}
.v3-txt small{display:block;font-weight:400;font-size:11.5px;color:var(--ink-soft);margin-top:1px}
.v3 .chev{width:16px;height:16px;color:var(--ink-soft);flex-shrink:0}
.editform .field:last-of-type{margin-bottom:0}
.editform .savebtn{width:100%;border:none;background:var(--lav);color:#fff;border-radius:999px;padding:12px;
  font-weight:600;font-size:13.5px;cursor:pointer;margin-top:4px}
.evcard{background:var(--surface);border:1px solid var(--line);border-radius:16px;overflow:hidden;box-shadow:var(--shadow);margin-bottom:12px}
.evcard .ph{height:92px;background:linear-gradient(135deg,var(--sage),var(--lav))}
.evcard .b{padding:14px 16px}
.evcard h4{font-family:var(--sys);font-size:15.5px;font-weight:600}
.evcard .dt{font-size:12px;color:var(--ink-soft);margin:3px 0 10px}
.evcard a{display:inline-flex;align-items:center;gap:5px;font-size:12.5px;font-weight:600;color:var(--lav-ink);text-decoration:none}

/* overlays — центрированное модальное окно с цветной шапкой */
.overlay{position:fixed;inset:0;background:rgba(26,21,33,.5);display:flex;align-items:center;justify-content:center;
  z-index:40;opacity:0;pointer-events:none;transition:.2s;padding:20px}
.overlay.open{opacity:1;pointer-events:auto}
.panel{position:relative;background:var(--surface);width:100%;max-width:460px;border-radius:24px;overflow:hidden;
  max-height:min(640px,86vh);display:flex;flex-direction:column;transform:scale(.95);transition:.2s;
  box-shadow:0 30px 70px -24px rgba(0,0,0,.45)}
.overlay.open .panel{transform:scale(1)}
.panel-head2{background:linear-gradient(135deg,var(--lav),var(--sage) 130%);padding:20px 50px 18px 22px;color:#fff;flex-shrink:0}
.panel-head2 .eyebrow2{font-family:var(--sys);font-style:italic;font-size:12px;opacity:.85;margin:0 0 4px}
.panel-head2 h2{font-size:19px;color:#fff}
.close-x{position:absolute;top:14px;right:14px;background:rgba(255,255,255,.22);border:none;width:28px;height:28px;
  border-radius:50%;color:#fff;font-size:15px;cursor:pointer;display:flex;align-items:center;justify-content:center}
#overlayBody{padding:18px 22px calc(22px + env(safe-area-inset-bottom,0px));overflow-y:auto}
.info-panel{max-width:400px;max-height:min(480px,80vh)}
.info-close{background:var(--surface-2);border:1px solid var(--line);color:var(--ink-soft)}
#infoOverlayBody{padding:46px 22px calc(20px + env(safe-area-inset-bottom,0px));overflow-y:auto}
#infoOverlayBody .info-title{font-size:18px;font-weight:600;color:var(--ink);margin:0 0 12px}
.info-meta{display:flex;gap:8px;margin-bottom:12px}
.info-meta>div{flex:1;background:var(--surface-2);border-radius:12px;padding:9px 12px}
.info-meta .lbl{font-size:10.5px;text-transform:uppercase;letter-spacing:.05em;color:var(--sage-ink);font-weight:600;margin-bottom:3px}
.info-meta .val{font-size:12.5px;color:var(--ink);line-height:1.3}
#infoOverlayBody .info-desc{font-size:13px;color:var(--ink-soft);line-height:1.55}
.info-why{margin-top:14px;padding-top:14px;border-top:1px solid var(--line)}
.info-why-lbl{font-size:10.5px;text-transform:uppercase;letter-spacing:.05em;color:var(--sage-ink);font-weight:600;margin-bottom:5px}
.info-why-txt{font-size:12.5px;color:var(--ink-soft);line-height:1.55;font-style:italic}
.sub{color:var(--ink-soft);font-size:13px;margin:2px 0 18px}
.mat{display:flex;gap:12px;padding:13px 0;border-top:1px solid var(--line)}
.mat:first-of-type{border-top:none}
.mat.is-locked{opacity:.55}
.mat-icon{width:34px;height:34px;border-radius:10px;background:var(--surface-2);flex-shrink:0;display:flex;align-items:center;justify-content:center}
.mat-icon svg{width:16px;height:16px;flex-shrink:0}
.mat-body{flex:1;min-width:0}
.mat-title{font-size:14px;font-weight:500;line-height:1.35}
.mat-tags{font-size:11.5px;color:var(--ink-soft);margin-top:3px}
.mat-status{font-size:11px;font-weight:600;padding:4px 10px;border-radius:999px;white-space:nowrap;align-self:flex-start;display:inline-flex;align-items:center;gap:4px}
.mat-status svg{width:11px;height:11px;flex-shrink:0}
.mat-status.open{background:var(--surface-2);color:var(--sage-ink)}
.mat-status.locked{background:var(--surface-2);color:var(--ink-soft)}
.mat-new{display:inline-block;margin-left:6px;font-size:10px;font-weight:700;color:#fff;background:var(--lav);
  border-radius:999px;padding:2px 7px;vertical-align:middle}
.note{font-size:11.5px;color:var(--ink-soft);background:var(--surface-2);border-radius:10px;padding:10px 14px;margin-top:4px;line-height:1.5}

/* finish-day modal */
.fd-overlay{position:fixed;inset:0;background:rgba(26,21,33,.5);display:flex;align-items:center;justify-content:center;
  z-index:50;opacity:0;pointer-events:none;transition:.2s;padding:20px}
.fd-overlay.open{opacity:1;pointer-events:auto}
.fd-card{width:100%;max-width:330px;background:linear-gradient(170deg,var(--lav) 0%,var(--sage) 120%);
  border-radius:26px;padding:30px 24px 24px;text-align:center;color:#fff;transform:scale(.96);transition:.22s;
  box-shadow:0 24px 60px -20px rgba(0,0,0,.4)}
.fd-overlay.open .fd-card{transform:scale(1)}
.fd-moon{width:42px;height:42px;margin:0 auto 12px}
.fd-day{font-size:12.5px;opacity:.85;margin-bottom:6px}
.fd-praise{font-family:var(--sys);font-weight:600;font-size:22px;margin-bottom:16px}
.fd-stat{background:rgba(255,255,255,.16);border-radius:14px;padding:13px;margin-bottom:14px}
.fd-stat .num{font-family:var(--sys);font-size:24px;font-weight:600;font-variant-numeric:tabular-nums}
.fd-stat .lbl{font-size:11px;opacity:.85;margin-top:2px}
.fd-quote{font-family:var(--sys);font-style:italic;font-size:13px;line-height:1.5;opacity:.95;margin-bottom:12px}
.fd-honest{font-family:var(--sys);font-size:11px;line-height:1.5;opacity:.8;margin-bottom:20px}
.fd-btn{width:100%;background:rgba(255,255,255,.95);color:var(--lav-ink);border:none;border-radius:999px;
  padding:12px;font-weight:600;font-size:13px;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:7px}
.fd-btn svg{width:14px;height:14px}

/* модалка: получение звезды */
.star-overlay{position:fixed;inset:0;background:rgba(26,21,33,.5);display:flex;align-items:center;justify-content:center;
  z-index:50;opacity:0;pointer-events:none;transition:.2s;padding:20px}
.star-overlay.open{opacity:1;pointer-events:auto}
.star-card{width:100%;max-width:300px;background:var(--surface);border-radius:22px;padding:26px 22px;text-align:center;
  transform:scale(.96);transition:.22s;box-shadow:0 24px 60px -20px rgba(0,0,0,.4)}
.star-overlay.open .star-card{transform:scale(1)}
.star-eyebrow{font-size:11px;color:var(--ink-soft);text-transform:uppercase;letter-spacing:.07em;margin-bottom:10px}
.star-big{font-size:46px;margin-bottom:8px;line-height:1}
.star-card h2{font-size:16px;font-weight:700;margin:0 0 14px;font-family:var(--sys)}
.star-progress{display:flex;justify-content:center;gap:10px;margin-bottom:18px}
.star-progress span{font-size:24px;filter:grayscale(1);opacity:.3}
.star-progress span.on{filter:none;opacity:1}
.star-card button{width:100%;border:none;background:var(--sage);color:#fff;border-radius:999px;padding:13px;
  font-weight:600;font-size:13.5px;cursor:pointer;font-family:var(--sys)}

/* модалка: создать/редактировать привычку */
.hm-backdrop{position:fixed;inset:0;z-index:55;background:rgba(0,0,0,.3);backdrop-filter:blur(4px);-webkit-backdrop-filter:blur(4px);
  display:flex;align-items:center;justify-content:center;padding:16px;opacity:0;pointer-events:none;transition:.2s}
.hm-backdrop.open{opacity:1;pointer-events:auto}
.hm-card{max-width:440px;width:100%;border-radius:24px;padding:24px;background:rgba(255,255,255,.92);
  display:flex;flex-direction:column;gap:20px;transform:scale(.96);transition:.2s;
  box-shadow:0 30px 70px -24px rgba(0,0,0,.35);max-height:88vh;overflow-y:auto;font-family:var(--sys)}
.hm-backdrop.open .hm-card{transform:scale(1)}
.hm-head{display:flex;justify-content:space-between;align-items:center}
.hm-title{font-weight:500;font-size:18px;color:#171717}
.hm-close{background:none;border:none;color:#a3a3a3;font-size:16px;cursor:pointer;line-height:1;padding:4px}
@media (hover:hover){ .hm-close:hover{color:#404040} }
.hm-row1{display:flex;align-items:center;gap:12px}
.hm-nameinput{width:100%;height:48px;border-radius:16px;padding:0 16px;background:rgba(0,0,0,.03);
  border:1px solid rgba(0,0,0,.05);font-weight:500;font-size:14px;color:#171717;font-family:var(--sys)}
.hm-nameinput::placeholder{color:#a3a3a3}
.hm-days-head{font-weight:500;font-size:14px;color:#171717}
.hm-days-sub{font-size:12px;color:#a3a3a3;margin:1px 0 10px}
.hm-daysgrid{display:flex;align-items:center;justify-content:space-between;gap:4px}
.hm-day{width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center;
  font-size:12px;font-weight:500;transition:.15s;border:none;cursor:pointer;background:rgba(0,0,0,.05);color:#737373;font-family:var(--sys)}
.hm-day.active{background:var(--lav);color:#fff}
@media (hover:hover){ .hm-day:not(.active):hover{background:rgba(0,0,0,.1)} }
.hm-qty{background:rgba(0,0,0,.02);border-radius:16px;padding:16px;border:1px solid rgba(0,0,0,.05);display:flex;flex-direction:column;gap:0}
.hm-qty-head{display:flex;justify-content:space-between;align-items:center;gap:10px}
.hm-qty-title{font-weight:500;font-size:14px;color:#171717}
.hm-qty-sub{font-size:12px;color:#a3a3a3;margin-top:2px}
.hm-switch{position:relative;width:42px;height:24px;border-radius:999px;background:rgba(0,0,0,.15);
  border:none;cursor:pointer;flex-shrink:0;transition:.2s;padding:0}
.hm-switch::after{content:"";position:absolute;top:2px;left:2px;width:20px;height:20px;border-radius:50%;
  background:#fff;box-shadow:0 1px 3px rgba(0,0,0,.3);transition:.2s}
.hm-switch.on{background:var(--lav)}
.hm-switch.on::after{transform:translateX(18px)}
.hm-qty-expand{display:flex;flex-direction:column;gap:12px;overflow:hidden;max-height:300px;opacity:1;
  transition:max-height .25s ease,opacity .2s ease,padding-top .25s ease,margin-top .25s ease;
  padding-top:12px;margin-top:12px;border-top:1px solid rgba(0,0,0,.05)}
.hm-qty-expand.collapsed{max-height:0;opacity:0;padding-top:0;margin-top:0;border-top-color:transparent;pointer-events:none}
.hm-qty-line{display:flex;justify-content:space-between;align-items:center}
.hm-qty-line .lbl{font-size:14px;color:#404040}
.hm-stepper{display:inline-flex;align-items:center;gap:12px;background:rgba(0,0,0,.05);border-radius:999px;padding:4px 12px;font-size:14px;font-weight:500;color:#171717}
.hm-stepper button{background:none;border:none;font-size:16px;color:#171717;cursor:pointer;width:18px;height:18px;
  display:flex;align-items:center;justify-content:center;line-height:1;font-family:var(--sys)}
.hm-unit-input{border:none;background:transparent;text-align:right;font-size:14px;font-weight:500;color:#171717;
  width:70px;font-family:var(--sys)}
.hm-unit-input::placeholder{color:#d4d4d4}
.hm-unit-input:focus{outline:none}
.hm-footer{display:flex;flex-direction:column;gap:8px}
.hm-save{width:100%;padding:13.5px;background:var(--lav);color:#fff;border:none;border-radius:16px;font-weight:500;
  font-size:14px;cursor:pointer;transition:transform .1s;font-family:var(--sys)}
.hm-save:active{transform:scale(.98)}
.hm-delete{width:100%;padding:11px;border-radius:12px;font-weight:500;font-size:13px;border:none;cursor:pointer;
  font-family:var(--sys);transition:background-color .15s;background:#fef2f2;color:#ef4444}
@media (hover:hover){ .hm-delete:hover{background:#fee2e2} }
.hab-edit{background:none;border:none;color:var(--ink-soft);opacity:.45;padding:4px;margin-left:0;cursor:pointer;
  flex-shrink:0;display:flex;align-items:center;border-radius:8px}
.hab-edit svg{width:14px;height:14px}
@media (hover:hover){ .hab-edit:hover{opacity:1;background:rgba(0,0,0,.05)} }
.statgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:10px}
.statcard{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:12px 10px;box-shadow:var(--shadow);text-align:center}
.statcard .slbl{font-size:9.5px;text-transform:uppercase;letter-spacing:.04em;color:var(--ink-soft);margin-bottom:5px}
.statcard .sval{font-family:var(--sys);font-size:19px;font-weight:700;color:var(--ink)}
.statcard .sval small{font-size:12px;font-weight:500;color:var(--ink-soft);margin-left:2px}
.byhab-row{display:flex;align-items:center;gap:10px;padding:10px 0;border-top:1px solid var(--line)}
.byhab-row:first-of-type{border-top:none}
.byhab-ring{position:relative;width:30px;height:30px;flex-shrink:0}
.byhab-ring svg{width:30px;height:30px;transform:rotate(-90deg)}
.byhab-body{flex:1;min-width:0}
.byhab-name{font-size:13.5px;font-weight:500;color:var(--ink)}
.byhab-sub{font-size:11px;color:var(--ink-soft);margin-top:1px}
.byhab-right{text-align:right;flex-shrink:0}
.byhab-pct{font-size:13.5px;font-weight:600;color:var(--sage-ink)}
.byhab-fr{font-size:10.5px;color:var(--ink-soft)}
.past-row{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:9px 0;border-top:1px solid var(--line);opacity:.7}
.past-row:first-of-type{border-top:none}
.past-name{font-size:13.5px;color:var(--ink)}
.past-sub{font-size:11px;color:var(--ink-soft)}

.authscreen{max-width:380px;margin:0 auto;min-height:100%;display:flex;flex-direction:column;
  align-items:center;justify-content:center;padding:32px 20px;box-sizing:border-box}
.authscreen[hidden]{display:none}
.authscreen .mark{width:48px;height:48px;object-fit:contain;margin-bottom:14px}
.authscreen h1{font-size:19px;margin-bottom:4px;text-align:center}
.authscreen .sub{font-size:12.5px;color:var(--ink-soft);text-align:center;margin-bottom:22px;line-height:1.5}
.authcard{background:var(--surface);border:1px solid var(--line);border-radius:18px;padding:22px;
  box-shadow:var(--shadow);width:100%}
.authcard .field{margin-bottom:12px}
.authcard .field label{display:block;font-size:11px;color:var(--ink-soft);margin-bottom:5px;text-transform:uppercase;letter-spacing:.05em}
.authcard .field input{width:100%;font-family:var(--sys);font-size:14px;color:var(--ink);background:var(--bg);
  border:1px solid var(--line);border-radius:12px;padding:11px 40px 11px 13px}
.pwd-wrap{position:relative}
.pwd-eye{position:absolute;right:4px;top:50%;transform:translateY(-50%);background:none;border:none;
  font-size:15px;cursor:pointer;padding:6px;color:var(--ink-soft);line-height:1;font-family:var(--sys)}
.auth-err{font-size:12px;color:#a8443a;margin:0 0 10px;line-height:1.4}
.auth-err[hidden]{display:none}
.auth-submit{width:100%;border:none;background:var(--lav);color:#fff;border-radius:999px;padding:13px;
  font-weight:600;font-size:13.5px;cursor:pointer;font-family:var(--sys);margin-top:4px}
.auth-submit:disabled{opacity:.6;cursor:default}
.auth-switch{text-align:center;font-size:12.5px;color:var(--ink-soft);margin-top:16px}
.auth-switch button{background:none;border:none;color:var(--lav-ink);font-weight:600;cursor:pointer;font-family:var(--sys);font-size:12.5px;padding:0}
.auth-loading{text-align:center;color:var(--ink-soft);font-size:13px;padding:40px 0}
</style>

<div class="authscreen" id="authScreen" hidden>
  <img class="mark" src="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/logo.png" alt="Serenity Moon">
  <h1 id="authTitle">Вход в кабинет</h1>
  <p class="sub" id="authSub">Марафон «Начни сегодня» · Serenity Moon</p>
  <div class="authcard">
    <p class="auth-err" id="authErr" hidden></p>
    <div class="field"><label>email</label><input type="email" id="authEmail" autocomplete="email"></div>
    <div class="field"><label>пароль</label><div class="pwd-wrap">
      <input type="password" id="authPassword" autocomplete="current-password">
      <button type="button" class="pwd-eye" id="authPwdEye" aria-label="показать пароль">👁</button>
    </div></div>
    <div class="field" id="authPassword2Field" hidden><label>повтори пароль</label><div class="pwd-wrap">
      <input type="password" id="authPassword2" autocomplete="new-password">
      <button type="button" class="pwd-eye" id="authPwdEye2" aria-label="показать пароль">👁</button>
    </div></div>
    <p class="auth-switch" id="authForgotWrap" style="margin:-2px 0 4px"><button id="authForgotBtn" type="button">Забыла пароль?</button></p>
    <button class="auth-submit" id="authSubmit" type="button">Войти</button>
  </div>
  <p class="auth-switch"><span id="authSwitchHint">Первый раз?</span> <button id="authSwitchMode" type="button">Зарегистрироваться</button></p>
</div>

<div class="authscreen" id="recoverScreen" hidden>
  <img class="mark" src="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/logo.png" alt="Serenity Moon">
  <h1>Восстановление пароля</h1>
  <p class="sub">Пришлём ссылку на email, по ней можно будет задать новый пароль</p>
  <div class="authcard">
    <p class="auth-err" id="recoverErr" hidden></p>
    <div class="field"><label>email</label><input type="email" id="recoverEmail" autocomplete="email"></div>
    <button class="auth-submit" id="recoverSubmit" type="button">Отправить ссылку</button>
  </div>
  <p class="auth-switch"><button id="recoverBack" type="button">← назад ко входу</button></p>
</div>

<div class="authscreen" id="newPasswordScreen" hidden>
  <img class="mark" src="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/logo.png" alt="Serenity Moon">
  <h1>Новый пароль</h1>
  <p class="sub">Придумай новый пароль для входа в кабинет</p>
  <div class="authcard">
    <p class="auth-err" id="newPasswordErr" hidden></p>
    <div class="field"><label>новый пароль</label><div class="pwd-wrap">
      <input type="password" id="newPassword1" autocomplete="new-password">
      <button type="button" class="pwd-eye" id="newPwdEye1" aria-label="показать пароль">👁</button>
    </div></div>
    <div class="field"><label>повтори пароль</label><div class="pwd-wrap">
      <input type="password" id="newPassword2" autocomplete="new-password">
      <button type="button" class="pwd-eye" id="newPwdEye2" aria-label="показать пароль">👁</button>
    </div></div>
    <button class="auth-submit" id="newPasswordSubmit" type="button">Сохранить и войти</button>
  </div>
</div>

<div class="authscreen" id="authLoading" hidden><p class="auth-loading">Загружаем кабинет…</p></div>

<div class="app" id="mainApp" hidden>
  <nav class="sidebar">
    <div class="brand">
      <img class="mark" src="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/logo.png" alt="Serenity Moon">
      <span class="name">Serenity Moon</span>
    </div>
    <button class="navbtn active" data-tab="today">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="5"/><path d="M12 2.5v2.5M12 19v2.5M4.5 12H2M22 12h-2.5M5.5 5.5l1.8 1.8M16.7 16.7l1.8 1.8M18.5 5.5l-1.8 1.8M7.3 16.7l-1.8 1.8"/></svg>
      Сегодня</button>
    <button class="navbtn" data-tab="home">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 19V11l4-3 4 7 4-9 4 6v7"/></svg>
      Прогресс</button>
    <button class="navbtn" data-tab="materials">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="4" y="4" width="7" height="7" rx="1.5"/><rect x="13" y="4" width="7" height="7" rx="1.5"/><rect x="4" y="13" width="7" height="7" rx="1.5"/><rect x="13" y="13" width="7" height="7" rx="1.5"/></svg>
      Материалы</button>
    <button class="navbtn" data-tab="profile">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="8.5" r="3.5"/><path d="M4.5 20c1.5-4 4.5-6 7.5-6s6 2 7.5 6"/></svg>
      Профиль</button>
  </nav>

  <div class="topbar">
    <img class="mark" src="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/logo.png" alt="Serenity Moon">
    <span class="name">Serenity Moon</span>
  </div>

  <main><div class="maininner">
    <section class="screen" id="tab-home" hidden>
      <div class="marathon-eyebrow">Марафон «Начни сегодня!»</div>
      <div class="hcard dayhero">
        <div class="dh-top"><span class="dh-title">День <span id="bigDayTxt">8</span> из 21</span><span class="dh-pct" id="dayPct">38%</span></div>
        <div class="dh-pill"><i id="dayPillFill"></i></div>
        <div class="dh-sub" id="daySub"></div>
      </div>
      <div class="statrow">
        <div class="heatcard" id="diamondCard">
          <div class="diamond-row">
            <div class="diamond-icon">💎</div>
            <div class="diamond-text">
              <div class="diamond-val"><span id="diamondVal">0</span><span class="diamond-max">/21</span></div>
              <div class="diamond-lbl">бриллианты</div>
            </div>
            <button class="info-hint" id="diamondInfoBtn" type="button" aria-label="как начисляются бриллианты">?</button>
          </div>
        </div>

        <div class="heatcard" id="achCard">
          <h3>Достижения</h3>
          <button class="info-hint" id="achInfoBtn" type="button" aria-label="как открываются звёзды">?</button>
          <div class="starsrow" id="starsRow"></div>
        </div>

        <div class="heatcard" id="countdownCard">
          <div class="diamond-row">
            <div class="diamond-icon">⏳</div>
            <div class="diamond-text">
              <div class="diamond-val"><span id="countdownVal">0</span></div>
              <div class="diamond-lbl">дней до финала</div>
            </div>
          </div>
        </div>
      </div>

      <div class="heatcard" id="heatmapCard" style="position:relative">
        <h3>Карта 21 дня</h3>
        <button class="info-hint" id="heatmapInfoBtn" type="button" aria-label="что значит цвет" style="position:absolute;top:13px;right:13px">?</button>
        <div class="weeklbl" id="weeklbl1">неделя 1</div>
        <div class="heatgrid" id="heatgrid1"></div>
        <div class="weeklbl" id="weeklbl2">неделя 2</div>
        <div class="heatgrid" id="heatgrid2"></div>
        <div class="weeklbl" id="weeklbl3">неделя 3</div>
        <div class="heatgrid" id="heatgrid3"></div>
      </div>

      <div class="heatcard" id="byHabitCard">
        <h3>По привычкам</h3>
        <div id="byHabitList"></div>
      </div>

      <div class="heatcard" id="pastHabitsCard" hidden>
        <h3>Раньше тоже делала</h3>
        <div id="pastHabitsList"></div>
      </div>
    </section>

    <section class="screen" id="tab-today">
      <div class="marathon-eyebrow">Марафон «Начни сегодня!»</div>
      <div class="affirm"><div class="elbl">Аффирмация дня</div><div class="txt" id="affirmText"></div></div>
      <div class="evt-alert" id="todayEventAlert" hidden>
        <span class="evt-alert-dot"></span>
        <span class="evt-alert-txt" id="todayEventTxt"></span>
        <span class="evt-alert-tag">Zoom</span>
      </div>
      <div class="checklist-head"><h3>Твои привычки</h3><button class="editlink" id="addHabitBtn" type="button">Изменить привычки</button></div>
      <p class="day-status" id="dayStatus"></p>
      <div class="checklist" id="checklist"></div>
      <div class="evening-rem" id="eveningRem" hidden>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>
        <div>
          <div class="lead">вечер — хорошее время заглянуть к себе</div>
          <div class="txt"><span id="socialOpener"></span> <span id="socialCloser"></span></div>
        </div>
      </div>
      <div class="actions">
        <button class="btn primary" id="closeDayBtn"></button>
        <button class="btn ghost" id="shareBtn">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="18" cy="5" r="2.3"/><circle cx="6" cy="12" r="2.3"/><circle cx="18" cy="19" r="2.3"/><path d="M8 10.8 16 6.2M8 13.2l8 4.6"/></svg>
          Поделиться
        </button>
      </div>
      <div class="share-out" id="shareOut" hidden></div>
    </section>

    <section class="screen" id="tab-materials" hidden>
      <div class="marathon-eyebrow">Марафон «Начни сегодня!»</div>
      <div class="mat-layout">
        <div class="mat-main">
          <div class="search-wrap">
            <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/></svg>
            <input class="search-box" id="searchBox" type="text" placeholder="Поиск: массаж лица, питание…" autocomplete="off">
            <div class="search-drop" id="searchDrop" hidden></div>
          </div>
          <div class="filters">
            <div class="filter-row"><span class="flabel">по теме</span><div class="chipset" id="themeChips"></div></div>
          </div>
          <div class="mgrid" id="mgrid"></div>
          <div class="checklist" id="mflat" style="padding:4px 18px" hidden></div>
        </div>
        <div class="mat-side">
          <div class="heatcard">
            <h3>Календарь</h3>
            <div id="nextEventBanner"></div>
            <div id="eventsList"></div>
          </div>
        </div>
      </div>
    </section>

    <section class="screen" id="tab-profile" hidden>
      <input type="file" id="avatarFileInput" accept="image/*" hidden>
      <div class="avatar-row">
        <div class="avatar" id="mainAvatar">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="8.5" r="3.5"/><path d="M4.5 20c1.5-4 4.5-6 7.5-6s6 2 7.5 6"/></svg>
          <div class="camera"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="7" width="18" height="13" rx="2.5"/><circle cx="12" cy="13.5" r="3.5"/><path d="M8 7l1.5-2.5h5L16 7"/></svg></div>
        </div>
      </div>

      <div class="profcard v3" id="v3tile">
        <div class="v3-txt">Ника Кузнецова<small>34 · Durham · редактировать</small></div>
        <svg class="chev" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m9 6 6 6-6 6"/></svg>
      </div>

      <div class="profcard">
        <div class="sectitle">привычки марафона</div>
        <div class="checklist-head" style="margin-top:0">
          <h3 id="profileHabitsCount">Выбрано 5 привычек</h3>
          <button class="editlink" id="editHabitsBtn">Изменить привычки</button>
        </div>
      </div>

      <div class="sectitle">мероприятия</div>
      <div class="evcard">
        <div class="ph"></div>
        <div class="b"><h4>Ретрит выходного дня</h4><div class="dt">14-15 ноября · Durham, NC</div>
          <a href="#">Узнать больше →</a></div>
      </div>
      <div class="evcard">
        <div class="ph" style="background:linear-gradient(135deg,var(--sand),var(--lav))"></div>
        <div class="b"><h4>Онлайн-ретрит «Новолуние»</h4><div class="dt">2 декабря · онлайн</div>
          <a href="#">Узнать больше →</a></div>
      </div>
      <button class="editlink" id="logoutBtn" type="button" style="display:block;margin:18px auto 0">Выйти из кабинета</button>
    </section>
  </div></main>

  <nav class="bottomnav"><div class="inner">
    <button class="navbtn active" data-tab="today">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="5"/><path d="M12 2.5v2.5M12 19v2.5M4.5 12H2M22 12h-2.5M5.5 5.5l1.8 1.8M16.7 16.7l1.8 1.8M18.5 5.5l-1.8 1.8M7.3 16.7l-1.8 1.8"/></svg>
      Сегодня</button>
    <button class="navbtn" data-tab="home">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 19V11l4-3 4 7 4-9 4 6v7"/></svg>
      Прогресс</button>
    <button class="navbtn" data-tab="materials">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="4" y="4" width="7" height="7" rx="1.5"/><rect x="13" y="4" width="7" height="7" rx="1.5"/><rect x="4" y="13" width="7" height="7" rx="1.5"/><rect x="13" y="13" width="7" height="7" rx="1.5"/></svg>
      Материалы</button>
    <button class="navbtn" data-tab="profile">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="8.5" r="3.5"/><path d="M4.5 20c1.5-4 4.5-6 7.5-6s6 2 7.5 6"/></svg>
      Профиль</button>
  </div></nav>
</div>

<div class="overlay" id="overlay"><div class="panel">
  <div class="panel-head2">
    <button class="close-x" id="closePanel">&#10005;</button>
    <p class="eyebrow2" id="overlayEyebrow" hidden></p>
    <h2 id="overlayTitle"></h2>
  </div>
  <div id="overlayBody"></div>
</div></div>

<div class="overlay" id="infoOverlay"><div class="panel info-panel">
  <button class="close-x info-close" id="closeInfoPanel">&#10005;</button>
  <div id="infoOverlayBody"></div>
</div></div>

<div class="fd-overlay" id="fdOverlay"><div class="fd-card">
  <svg class="fd-moon" viewBox="0 0 24 24" fill="none"><path d="M20 14.5A8.5 8.5 0 1 1 9.5 4a7 7 0 0 0 10.5 10.5Z" fill="#fff"/></svg>
  <p class="fd-day">день <span id="fdDay">8</span> завершён</p>
  <h2 class="fd-praise" id="fdPraise">Ты держишь ритм ✨</h2>
  <div class="fd-stat"><div class="num"><span id="fdDone">0</span>/<span id="fdTotal">5</span></div><div class="lbl">привычек выполнено сегодня</div></div>
  <p class="fd-quote">«Маленькие шаги каждый день создают большие результаты»</p>
  <button class="fd-btn" id="fdClose">
    <svg viewBox="0 0 24 24" fill="none"><path d="M20 14.5A8.5 8.5 0 1 1 9.5 4a7 7 0 0 0 10.5 10.5Z" fill="currentColor"/></svg>
    Спокойной ночи</button>
</div></div>

<div class="star-overlay" id="starOverlay"><div class="star-card">
  <div class="star-eyebrow" id="starEyebrow">неделя 1 пройдена</div>
  <div class="star-big">⭐</div>
  <h2 id="starTitle">Поздравляю, ты прошла неделю марафона — и за это получаешь звезду</h2>
  <div class="star-progress" id="starProgress"></div>
  <button id="starClose">Забрать звезду</button>
</div></div>

<div class="hm-backdrop" id="habitModalBackdrop">
  <div class="hm-card">
    <div class="hm-head">
      <span class="hm-title" id="habitModalTitle">редактировать привычку</span>
      <button class="hm-close" id="habitModalClose" type="button">&#10005;</button>
    </div>

    <div class="hm-row1">
      <input class="hm-nameinput" id="habitNameInput" type="text" placeholder="название привычки...">
    </div>

    <div>
      <div class="hm-days-head">дни недели</div>
      <div class="hm-days-sub">отмечать можно только в выбранные</div>
      <div class="hm-daysgrid" id="habitDaysGrid">
        <button class="hm-day" data-d="0" type="button">пн</button>
        <button class="hm-day" data-d="1" type="button">вт</button>
        <button class="hm-day" data-d="2" type="button">ср</button>
        <button class="hm-day" data-d="3" type="button">чт</button>
        <button class="hm-day" data-d="4" type="button">пт</button>
        <button class="hm-day" data-d="5" type="button">сб</button>
        <button class="hm-day" data-d="6" type="button">вс</button>
      </div>
    </div>

    <div class="hm-qty">
      <div class="hm-qty-head">
        <div>
          <div class="hm-qty-title">учитывать количество</div>
          <div class="hm-qty-sub">число за день — стаканы, км, минуты...</div>
        </div>
        <button class="hm-switch" id="habitQtyToggle" type="button"></button>
      </div>
      <div class="hm-qty-expand collapsed" id="habitQtyExpand">
        <div class="hm-qty-line">
          <span class="lbl">цель</span>
          <span class="hm-stepper">
            <button type="button" data-step="-1" data-target="target">–</button>
            <span id="habitTargetVal">8</span>
            <button type="button" data-step="1" data-target="target">+</button>
          </span>
        </div>
        <div class="hm-qty-line">
          <span class="lbl">единица</span>
          <input class="hm-unit-input" id="habitUnitInput" type="text" placeholder="стак.">
        </div>
        <div class="hm-qty-line">
          <span class="lbl">шаг</span>
          <span class="hm-stepper">
            <button type="button" data-step="-1" data-target="step">–</button>
            <span id="habitStepVal">1</span>
            <button type="button" data-step="1" data-target="step">+</button>
          </span>
        </div>
      </div>
    </div>

    <p class="note" id="habitModalWarn" hidden style="margin:0">Уже выбрано максимум 5 привычек — сначала убери одну</p>

    <div class="hm-footer">
      <button class="hm-save" id="habitSaveBtn" type="button">сохранить</button>
      <button class="hm-delete" id="habitDeleteBtn" type="button">удалить</button>
    </div>
  </div>
</div>

<script>
/* ---------- каталог привычек (справочник, не пользовательские данные) ---------- */
const CATALOG = {
  "Тело": [
    {name:"Полноценный сон",when:"Каждую ночь",dur:"7-8 час.",desc:"Ложитесь спать до 10-11 вечера. Это поможет вам лучше спать и повысить уровень энергии по утрам. Поддержание постоянного циркадного ритма чрезвычайно полезно для здоровья.",why:"Во сне тело восстанавливается, а мозг закрепляет всё, что ты узнала и прожила за день — без этого остальные привычки держатся хуже."},
    {name:"Стакан тёплой воды",when:"Утро",dur:"1 мин.",desc:"Натощак выпивайте 1 стакан тёплой воды (можно с лимоном). Запускает организм и помогает ему проснуться.",why:"После ночи организм немного обезвожен — вода мягко запускает обмен веществ и пищеварение."},
    {name:"100 прыжков на месте",when:"Утро",dur:"2 мин.",desc:"Опускайтесь на полную стопу. Отличный вариант запуска организма по утрам.",why:"Короткая физическая встряска утром усиливает кровообращение и помогает проснуться без кофеина."},
    {name:"Пить 1,5-2 литра воды",when:"В течение дня",dur:"1,5-2 л.",desc:"Пей воду маленькими порциями на протяжении дня, не дожидаясь жажды. Поддерживает энергию, кожу и работу всех систем организма.",why:"Даже лёгкое обезвоживание снижает концентрацию и энергию — регулярная вода держит тело в тонусе весь день."},
    {name:"Контрастный/холодный душ",when:"Утро",dur:"5 мин.",desc:"Принятие холодного душа укрепляет иммунитет, стимулирует кровообращение, повышает энергию и настроение, снимает воспаления.",why:"Резкая смена температуры тренирует сосуды и нервную систему — со временем это ощущается как прилив бодрости."},
    {name:"Массаж щёткой",when:"Ежедневно",dur:"3-5 мин.",desc:"Не менее 1 раза, лучше 2 раза утром и вечером. Улучшает лимфоток и кровообращение в тканях.",why:"Лимфоток не имеет собственного «насоса» и двигается только за счёт движения — сухой массаж стимулирует его механически."},
    {name:"10 000 шагов в день",when:"Ежедневно",dur:"В течение дня",desc:"Помогает укрепить мышечный каркас, поддерживать вес и улучшает снабжение тканей кислородом.",why:"Регулярная ходьба — один из самых простых способов поддерживать сердце и обмен веществ без нагрузки на суставы."},
    {name:"Регулярные тренировки",when:"2-3 раза в неделю",dur:"20-40 мин.",desc:"Укрепляют сердце и лёгкие, повышают выносливость и физическую силу, поддерживают здоровый вес и способствуют психологическому благополучию.",why:"Физическая нагрузка не только укрепляет тело, но и снижает уровень стресса через выброс эндорфинов."},
    {name:"Опускание на пятки",when:"Любое время дня",dur:"2-3 мин.",desc:"Приподняться на 2-3 см и резко опускаться на пятки. Выполняется в быстром темпе.",why:"Короткая вибрация через пятки мягко стимулирует кровообращение и лимфоток, не отнимая много времени."},
    {name:"Простукивание тимуса",when:"Любое время дня",dur:"2-3 мин.",desc:"Тимус играет ключевую роль в развитии и функционировании иммунных клеток. Регулярное простукивание улучшает кровообращение и лимфообращение, а также стимулирует его функции.",why:"Простой телесный ритуал, который помогает поддерживать иммунитет без дополнительных затрат времени."},
    {name:"Тряска ног и рук лёжа на спине",when:"Ежедневно",dur:"2-3 мин.",desc:"Помогает улучшить кровообращение, стимулирует лимфатическую систему, способствует снятию напряжения и стресса, повышает энергию и снимает усталость.",why:"Мягкая встряска телу помогает «сбросить» напряжение, накопленное за день, без нагрузки на суставы."},
    {name:"Растирание ушей",when:"Регулярно",dur:"1-2 мин.",desc:"Массаж ушной раковины, где находятся акупунктурные точки. Снимает напряжение, улучшает кровообращение, снижает стресс и головную боль.",why:"На ушах сосредоточено много акупунктурных точек — короткий массаж быстро снимает напряжение."},
    {name:"Стояние на аппликаторе (Кузнецова/Ляпко/Садху)",when:"Утро",dur:"2-10 мин.",desc:"Статичный массаж ступней способствует улучшению кровообращения, снятию напряжения и боли, стимуляции эндорфинов и повышению энергии.",why:"Воздействие на точки стоп с утра бодрит не хуже чашки кофе, без нагрузки на нервную систему."},
    {name:"Массаж живота «Наули»",when:"Регулярно",dur:"5-10 мин.",desc:"Вакуум живота помогает стимуляции пищеварительной системы, улучшению обмена веществ и снятию напряжения.",why:"Мягкая стимуляция живота поддерживает пищеварение и снимает лишнее напряжение в этой зоне."}
  ],
  "Питание": [
    {name:"Сбалансированная еда",when:"Ежедневно",dur:"Всегда",desc:"Полезное питание — ключевой элемент здорового образа жизни. Стремитесь к сбалансированному соотношению углеводов, белков и жиров. Слушайте своё тело.",why:"Стабильное питание убирает резкие скачки энергии и голода, из-за которых труднее держать остальные привычки."},
    {name:"Без сахара",when:"Ежедневно",dur:"Весь день",desc:"Старайся избегать сахара и сладких перекусов в течение дня. Меньше резких скачков энергии — и легче держать остальные привычки марафона.",why:"Резкий подъём сахара в крови даёт короткий всплеск энергии и такой же резкий спад — без него день ровнее."},
    {name:"Меньше кофе",when:"Ежедневно",dur:"Весь день",desc:"Ограничь кофе 1-2 чашками в день, особенно во второй половине дня. Меньше тревожности и лучше сон.",why:"Кофеин во второй половине дня может мешать засыпанию, даже если ты этого не замечаешь."}
  ],
  "Уход и красота": [
    {name:"Ванна с Эпсом солью",when:"1 раз в неделю",dur:"40-60 мин.",desc:"Отличный вариант снять стресс, расслабиться, повысить тонус кожи и улучшить обмен веществ.",why:"Тёплая вода расслабляет мышцы, а сама пауза даёт нервной системе время переключиться с режима «делать» на «отдыхать»."},
    {name:"Полоскание кокосовым маслом",when:"Регулярно",dur:"5-7 мин.",desc:"Держите ложку кокосового масла во рту сколько сможете. Отличный детокс, а ещё помогает отбелить зубы.",why:"Простой ритуал ухода, который легко встроить в уже существующую утреннюю рутину, не добавляя отдельного времени."},
    {name:"Уход за лицом",when:"Ежедневно",dur:"10-15 мин.",desc:"Ежедневный массаж лица и зоны декольте. Прекрасный способ сохранить молодость и красоту.",why:"Уход работает по накоплению — эффект заметен не сразу, а именно от повторения, а не от одного раза."}
  ],
  "Спокойствие": [
    {name:"Йога и медитации",when:"Ежедневно",dur:"30-40 мин.",desc:"Улучшает общее состояние здоровья, включает техники дыхания и медитации, способствующие расслаблению ума и тела.",why:"Осознанное дыхание переключает нервную систему из режима тревоги в режим покоя за считанные минуты."},
    {name:"Дыхательная практика",when:"Любое время дня",dur:"3-5 мин.",desc:"Несколько глубоких осознанных вдохов и выдохов. Быстро снижает напряжение и возвращает внимание в тело.",why:"Медленный выдох напрямую сигналит нервной системе, что можно расслабиться — самый быстрый доступный инструмент против стресса."},
    {name:"Дневник благодарностей",when:"Ежедневно",dur:"5-10 мин.",desc:"Помогает сместить фокус внимания с негативных аспектов жизни на позитивные, замечать и ценить мелочи и радости.",why:"Фокус на хорошем — тренируемый навык: чем чаще его замечаешь, тем легче это даётся со временем."},
    {name:"Улыбайся своему отражению в зеркале",when:"В любое время",dur:"Сколько хотите",desc:"Расслабляет мышцы лица и шеи, помогает на разных уровнях учиться принимать и любить себя.",why:"Маленький жест заботы к себе, который со временем меняет внутренний разговор с собой."},
    {name:"Быть на природе",when:"Как можно чаще",dur:"Сколько хотите",desc:"Ходить босыми ногами по траве, обниматься с деревьями :)",why:"Контакт с природой — один из самых простых и доступных способов снизить уровень стресса."}
  ],
  "Развитие": [
    {name:"Прочитать новое",when:"Регулярно",dur:"30-45 мин.",desc:"Всего 30 минут чтения в день могут развить вашу концентрацию и критическое мышление.",why:"Чтение — небольшая тренировка для концентрации, которая слабеет от бесконечного скролла."},
    {name:"Изучение иностранного языка",when:"Регулярно",dur:"10-20 мин.",desc:"Повышает когнитивные способности, развивает мозг, улучшает память и способствует саморазвитию.",why:"Новый язык — нагрузка для мозга, похожая на тренировку тела, только для памяти и мышления."}
  ],
  "Близкие": [
    {name:"Время с семьёй/детьми",when:"Регулярно",dur:"Сколько хотите",desc:"Это важно! Качественное время с близкими — один из главных приоритетов.",why:"Качественное присутствие рядом с близкими — один из самых стабильных источников ощущения благополучия."}
  ],
  "Дом и порядок": [
    {name:"Заправить кровать",when:"Утро",dur:"1-2 мин.",desc:"Простое действие, с которого начинается ощущение порядка на весь день.",why:"Маленькое законченное действие с утра создаёт ощущение контроля, которое переносится на весь день."},
    {name:"Уборка",when:"Регулярно",dur:"10-15 мин.",desc:"Немного порядка вокруг — немного порядка внутри. Не обязательно генеральная уборка, достаточно малого.",why:"Порядок вокруг снижает визуальный шум — меньше отвлекающих раздражителей для внимания."},
    {name:"Полить растения",when:"Регулярно",dur:"2-5 мин.",desc:"Забота о чём-то живом рядом с тобой — маленький, но приятный ритуал.",why:"Забота о чём-то живом — простой способ почувствовать себя нужной и внимательной, без лишних усилий."},
    {name:"Разобрать вещи",when:"Регулярно",dur:"10-15 мин.",desc:"Убери то, что давно не нужно. Меньше лишнего — больше свободного пространства и внимания.",why:"Лишние вещи вокруг незаметно забирают немного внимания каждый раз, когда попадаются на глаза."},
    {name:"Планирование",when:"Утро/Вечер",dur:"5-10 мин.",desc:"В твоей жизни будет меньше хаоса, если ты будешь планировать.",why:"Несколько минут планирования снимают тревогу неопределённости и экономят силы на решения в течение дня."}
  ],
  "Без вредного": [
    {name:"Не курить",when:"Каждый день",dur:"Весь день",desc:"День без сигарет — вклад в своё здоровье и энергию.",why:"Восстановление лёгких и сосудов начинается уже в первые часы без сигарет."},
    {name:"Без алкоголя",when:"Каждый день",dur:"Весь день",desc:"День без алкоголя — более чистый сон и больше энергии с утра.",why:"Алкоголь ухудшает качество сна, даже если кажется, что помогает быстрее уснуть."},
    {name:"Меньше соцсетей",when:"Ежедневно",dur:"По желанию",desc:"Сократи бесцельный скролл — освободившееся время и внимание можно отдать себе.",why:"Бесцельный скролл истощает внимание почти так же, как многозадачность — пауза в нём освобождает ресурс для себя."}
  ]
};
function findHabitMeta(name){
  for(const items of Object.values(CATALOG)){
    const found = items.find(h=>h.name===name);
    if(found) return found;
  }
  return null;
}
const MAX_HABITS = 5;
const ICONS_CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="3" style="width:13px;height:13px"><path d="M5 12.5 9.5 17 19 7"/></svg>';
const AFFIRMATIONS = ["Ты уже на верном пути","Твоя энергия становится яснее","Маленький шаг сегодня — большая перемена завтра",
  "Ты заботишься о себе, и это важно","Доверяй своему ритму","Ты сияешь изнутри","Сегодня — хороший день для тишины","Ты становишься устойчивее с каждым днём"];
const AFFIRM_SUPPORT = ["Пропустить день — это нормально, ты не начинаешь с нуля","Вчера был непростой день — сегодня можно просто начать заново",
  "Один пропуск ничего не перечёркивает — ты всё ещё в пути","Заботиться о себе — не значит делать всё идеально"];
const AFFIRM_CELEBRATE = ["Ты держишь серию — это чувствуется по тому, как уверенно ты идёшь","Так держать, это уже настоящая привычка",
  "Твоя последовательность вдохновляет — продолжай в том же духе","Серия растёт — ты формируешь что-то настоящее"];
const SOCIAL_OPENERS = [
  "Сегодня уже 15 человек нашли время позаботиться о себе и выполнили хотя бы 2 привычки 💛",
  "У тебя ещё есть время на себя сегодня — 15 человек уже нашли эти пару минут 💛",
  "15 человек сегодня выбрали заботу о себе вместо спешки 💛",
  "Маленький шаг — уже победа. сегодня его сделали 15 человек 💛",
  "Любовь к себе не требует больших шагов — можно начать всего с десяти минут 💛",
  "15 человек сегодня выбрали себя, хотя бы немного 💛",
  "Ты справишься, даже если день был непростым — 15 человек сегодня тоже справились 💛",
  "Ты не обязана успевать всё — сегодня 15 человек позаботились о себе хотя бы немного 💛",
  "Забота о себе может быть совсем маленькой — сегодня это доказали 15 человек 💛",
  "15 человек сегодня нашли время для себя — ты тоже умеешь это находить 💛"
];
const SOCIAL_CLOSERS = [
  "присоединяйся, когда будешь готова",
  "сделай свой шаг, когда будет удобно",
  "начни с того, что по силам сегодня",
  "просто попробуй — дальше будет легче",
  "просто вернись к себе, когда будет время"
];
function hashStr(s){ let h=0; for(let i=0;i<s.length;i++){ h=(h*31+s.charCodeAt(i))|0; } return Math.abs(h); }
function todayCount(){ return 9 + (hashStr("day"+TODAY) % 17); }

const STAR_DAYS = [7, 14, 21];
function renderStars(){
  document.getElementById("starsRow").innerHTML = STAR_DAYS.map(d=>{
    const unlocked = TODAY >= d;
    return `<div class="star-item"><div class="star-icon ${unlocked?'unlocked':''}">⭐</div><div class="star-day">${d} день</div></div>`;
  }).join("");
}
function mastermindBonus(){ return Object.keys(STATE.mastermind||{}).filter(k=>STATE.mastermind[k]).length * 5; }
function mmCodeBoxHtml(mmId){
  if(STATE.mastermind && STATE.mastermind[String(mmId)]) return `<div class="mm-row"><span class="mm-done">✓ бриллиант получен (+5 💎)</span></div>`;
  return `<div class="mm-row">
    <div class="mm-field"><input class="mm-input" type="text" placeholder="кодовое слово из эфира" id="mmInput${mmId}">
    <p class="mm-err" id="mmErr${mmId}" hidden>неверное слово</p></div>
    <button class="mm-btn" type="button" data-mmid="${mmId}">Получить 💎</button></div>`;
}
function wireMmCodeBox(mmId){
  const btn = document.querySelector('.mm-btn[data-mmid="'+mmId+'"]');
  if(!btn) return;
  btn.addEventListener("click", async ()=>{
    const input = document.getElementById("mmInput"+mmId);
    const val = (input.value||"").trim().toLowerCase();
    if(!val) return;
    try{
      const resp = await apiFetch("/api/cabinet/mastermind", {method:"POST", body:{mm_id:String(mmId), code: val}});
      const data = await resp.json();
      if(resp.ok && data.ok){
        STATE.mastermind = data.mastermind;
        playPopSound();
        openMaterialDetail("mastermind", mmId-1);
        renderProgress();
      } else {
        document.getElementById("mmErr"+mmId).hidden = false;
        input.style.borderColor = "#a8443a";
      }
    }catch(e){ /* unauthorized уже обработан в apiFetch */ }
  });
}

function showInfoSimple(title, html){
  document.getElementById("infoOverlayBody").innerHTML = `<p class="info-title">${title}</p><p class="info-desc">${html}</p>`;
  document.getElementById("infoOverlay").classList.add("open");
}
document.getElementById("diamondInfoBtn").addEventListener("click", ()=>{
  playPopSound();
  showInfoSimple("Бриллианты 💎", "+1 бриллиант за каждый день, в который закрыты все твои привычки на 100%. Максимум — 21 (по одному за каждый день марафона). Плюс +5 за каждый мастермайнд, если ввести кодовое слово после эфира.<br><br><i>Будь честна с самой собой: цель марафона не в том, чтобы поучаствовать в розыгрыше, а в том, чтобы изменить свою жизнь.</i>");
});
document.getElementById("achInfoBtn").addEventListener("click", ()=>{
  playPopSound();
  showInfoSimple("Звёзды ⭐", "Три звезды открываются по неделям марафона — на 7-й, 14-й и 21-й день. Просто дойди до этого дня, и звезда станет твоей.");
});
document.getElementById("heatmapInfoBtn").addEventListener("click", ()=>{
  playPopSound();
  showInfoSimple("Карта 21 дня", "Чем темнее квадратик — тем больше привычек в этот день закрыто. Серый — день ещё не начат или ничего не отмечено, светлый — закрыто немного, насыщенный бирюзовый — почти всё или всё.");
});

const ICONS = {
  video:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="2.5" y="5.5" width="13" height="13" rx="2.5"/><path d="M18.5 9.5 21.5 7v10l-3-2.5"/></svg>',
  audio:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 10v4h3.5L13 18V6L7.5 10H4Z"/><path d="M16.5 9a4 4 0 0 1 0 6"/></svg>',
  pdf:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M6 2.5h8l4.5 4.5V21a.5.5 0 0 1-.5.5H6a.5.5 0 0 1-.5-.5V3a.5.5 0 0 1 .5-.5Z"/><path d="M14 2.5V7h4.5"/></svg>',
  lock:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="5" y="10.5" width="14" height="9.5" rx="2"/><path d="M8 10.5V8a4 4 0 0 1 8 0v2.5"/></svg>'
};
ICONS.link = ICONS.pdf;
const MASTERMIND_AUTHOR = {mastermind:{name:"Мастермайнды",topics:"записи эфиров с Милой",desc:"Запись мастермайнда — введи кодовое слово, которое Мила называла вживую, и получи бриллиант"}};
const MASTERMIND_MATERIALS = [
  {author:"mastermind",title:"Мастермайнд 1",type:"video",tags:["мастермайнд"],day:3,mmId:"1",url:"#"},
  {author:"mastermind",title:"Мастермайнд 2",type:"video",tags:["мастермайнд"],day:15,mmId:"2",url:"#"}
];

/* ---------- звуковой отклик (Web Audio API, без внешних файлов) ---------- */
let _popCtx = null;
function playPopSound(){
  try{
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    if(!AudioCtx) return;
    if(!_popCtx) _popCtx = new AudioCtx();
    if(_popCtx.state === "suspended") _popCtx.resume();
    const t = _popCtx.currentTime;
    const osc = _popCtx.createOscillator();
    const gain = _popCtx.createGain();
    osc.type = "sine";
    osc.frequency.setValueAtTime(550, t);
    osc.frequency.exponentialRampToValueAtTime(150, t + 0.04);
    gain.gain.setValueAtTime(0.15, t);
    gain.gain.exponentialRampToValueAtTime(0.01, t + 0.04);
    osc.connect(gain); gain.connect(_popCtx.destination);
    osc.start(t); osc.stop(t + 0.05);
  }catch(e){}
}

/* ---------- оверлей: общая шапка ---------- */
function openOverlay(title, eyebrow){
  document.getElementById("overlayTitle").textContent = title;
  const eb = document.getElementById("overlayEyebrow");
  eb.hidden = !eyebrow;
  eb.textContent = eyebrow || "";
  document.getElementById("overlay").classList.add("open");
}

/* ---------- навигация: централизованный state + персистентность ---------- */
const TAB_KEY = "serenityActiveTab";
function setActiveTab(tab, opts){
  opts = opts || {};
  const screen = document.getElementById("tab-"+tab);
  if(!screen) return;
  document.querySelectorAll(".navbtn").forEach(b=>{
    const isActive = b.dataset.tab === tab;
    b.classList.toggle("active", isActive);
    if(isActive) b.setAttribute("aria-current","page"); else b.removeAttribute("aria-current");
  });
  document.querySelectorAll(".screen").forEach(s=>s.hidden = true);
  screen.hidden = false;
  if(opts.persist !== false){
    try{ localStorage.setItem(TAB_KEY, tab); }catch(e){}
    try{
      const url = new URL(location.href);
      url.searchParams.set("tab", tab);
      history.replaceState(null, "", url);
    }catch(e){}
  }
}
document.querySelectorAll(".navbtn").forEach(btn=>{
  btn.addEventListener("click", ()=>{
    setActiveTab(btn.dataset.tab);
    playPopSound();
    if(btn.closest(".bottomnav")){
      const icon = btn.querySelector("svg");
      icon.classList.remove("tapbounce");
      void icon.offsetWidth;
      icon.classList.add("tapbounce");
    }
  });
});
let initialTab = "today";
try{ initialTab = new URLSearchParams(location.search).get("tab") || localStorage.getItem(TAB_KEY) || "home"; }catch(e){}
if(!document.getElementById("tab-"+initialTab)) initialTab = "home";

/* ---------- аутентификация и загрузка реальных данных кабинета ---------- */
const TOKEN_KEY = "serenityAccessToken";
function getToken(){ try{ return localStorage.getItem(TOKEN_KEY); }catch(e){ return null; } }
function setToken(t){ try{ if(t) localStorage.setItem(TOKEN_KEY, t); else localStorage.removeItem(TOKEN_KEY); }catch(e){} }

async function apiFetch(url, opts){
  opts = opts || {};
  const headers = Object.assign({}, opts.headers, {"Authorization": "Bearer " + (getToken()||"")});
  let body = opts.body;
  if(body && typeof body !== "string" && !(body instanceof FormData)){ body = JSON.stringify(body); headers["Content-Type"] = "application/json"; }
  const resp = await fetch(url, Object.assign({}, opts, {headers, body}));
  if(resp.status === 401){
    setToken(null);
    showAuthScreen("сессия истекла — войди снова");
    throw new Error("unauthorized");
  }
  return resp;
}

function showAuthScreen(err){
  document.getElementById("mainApp").hidden = true;
  document.getElementById("authLoading").hidden = true;
  document.getElementById("recoverScreen").hidden = true;
  document.getElementById("newPasswordScreen").hidden = true;
  document.getElementById("authScreen").hidden = false;
  const errEl = document.getElementById("authErr");
  if(err){ errEl.style.color = ""; errEl.textContent = err; errEl.hidden = false; } else { errEl.hidden = true; }
}

let STATE = {marathon_start:"2026-11-09", logsByDate:{}, mastermind:{}, authors:{}, materials:[], events:[]};
let MARATHON_START_ISO = "2026-11-09";
let RU_MONTHS_SHORT = ["янв","фев","мар","апр","мая","июн","июл","авг","сен","окт","ноя","дек"];
let TODAY = 1;
let HABITS = [];
let HABIT_IDS = [];
let habitConfig = {};
let liveChecked = [];
let dayClosed = false;
let AUTHORS = {}, MATERIALS = [], THEMES = [];
const COLOR_CSS = {
  lav: "var(--lav)", sage: "var(--sage)", sand: "var(--sand)",
  grad1: "linear-gradient(135deg,var(--sand),var(--lav))",
  grad2: "linear-gradient(135deg,var(--lav),var(--sage))"
};

function dateForDay(d){
  const [y,m,dd] = MARATHON_START_ISO.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m-1, dd));
  dt.setUTCDate(dt.getUTCDate() + (d-1));
  return dt.toISOString().slice(0,10);
}
function dayToDate(d){
  const [y,m,dd] = MARATHON_START_ISO.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m-1, dd));
  dt.setUTCDate(dt.getUTCDate() + (d-1));
  return dt;
}
function fmtDateRu(dt){ return dt.getUTCDate()+" "+RU_MONTHS_SHORT[dt.getUTCMonth()]; }
function logForDate(dateStr){ return STATE.logsByDate[dateStr] || {habits_done:{}, closed:false}; }
function doneCountForDay(d){
  if(d === TODAY) return liveChecked.filter(Boolean).length;
  const hd = logForDate(dateForDay(d)).habits_done || {};
  return Object.values(hd).filter(Boolean).length;
}
function doneForHabitOnDay(id, d){
  if(d === TODAY){
    const i = HABIT_IDS.indexOf(id);
    return i>=0 ? !!liveChecked[i] : false;
  }
  const hd = logForDate(dateForDay(d)).habits_done || {};
  return !!hd[id];
}
function defaultHabitConfig(){ return {days:[0,1,2,3,4,5,6], qtyOn:false, target:8, unit:"", step:1}; }

function applyState(data){
  STATE = data;
  STATE.logsByDate = {};
  (data.logs||[]).forEach(l=>{ STATE.logsByDate[l.date] = {habits_done: l.habits_done||{}, closed: !!l.closed}; });
  STATE.mastermind = data.mastermind || {};
  MARATHON_START_ISO = data.marathon_start || MARATHON_START_ISO;
  TODAY = Math.min(21, Math.max(1, data.today));
  HABIT_IDS = (data.habits||[]).map(h=>h.id);
  HABITS = (data.habits||[]).map(h=>h.name);
  habitConfig = {};
  (data.habits||[]).forEach(h=>{
    habitConfig[h.name] = (h.config && Object.keys(h.config).length) ? h.config : defaultHabitConfig();
  });
  const todayLog = STATE.logsByDate[dateForDay(TODAY)];
  liveChecked = HABIT_IDS.map(id => todayLog ? !!(todayLog.habits_done||{})[id] : false);
  dayClosed = todayLog ? !!todayLog.closed : false;
}

function buildMaterialsState(){
  AUTHORS = Object.assign({}, STATE.authors||{}, MASTERMIND_AUTHOR);
  MATERIALS = MASTERMIND_MATERIALS.concat((STATE.materials||[]).map(m=>Object.assign({}, m, {tags: m.tags||[]})));
  THEMES = [...new Set(MATERIALS.flatMap(m=>m.tags||[]))];
  document.getElementById("themeChips").innerHTML =
    `<button class="chip theme active" data-t="all">все темы</button>` +
    THEMES.map(t=>`<button class="chip theme" data-t="${t}">${t}</button>`).join("");
  activeTheme = "all"; query = "";
}

function initCabinetUI(){
  buildMaterialsState();
  renderHeat(); renderChecklist(); renderMaterials(); renderProgress(); renderEvents(); renderProfileTile();
  syncHabitsCount();
  setActiveTab(initialTab, {persist:false});
  if(HABITS.length === 0) startOnboarding();
  checkStarCelebration();
}

const STAR_SHOWN_KEY = "serenityStarShown";
const STAR_TEXT = {
  7: "Поздравляю, ты прошла неделю марафона — и за это получаешь звезду",
  14: "Поздравляю, ты прошла две недели марафона — и за это получаешь звезду",
  21: "Поздравляю, ты прошла марафон полностью — и за это получаешь звезду"
};
const STAR_EYEBROW = {7:"неделя 1 пройдена", 14:"неделя 2 пройдена", 21:"марафон завершён"};
function checkStarCelebration(){
  let shown = {};
  try{ shown = JSON.parse(localStorage.getItem(STAR_SHOWN_KEY) || "{}"); }catch(e){}
  const due = STAR_DAYS.find(d => TODAY >= d && !shown[d]);
  if(!due) return;
  document.getElementById("starEyebrow").textContent = STAR_EYEBROW[due];
  document.getElementById("starTitle").textContent = STAR_TEXT[due];
  document.getElementById("starProgress").innerHTML = STAR_DAYS.map(d =>
    `<span class="${TODAY>=d?'on':''}">⭐</span>`
  ).join("");
  document.getElementById("starOverlay").classList.add("open");
  shown[due] = true;
  try{ localStorage.setItem(STAR_SHOWN_KEY, JSON.stringify(shown)); }catch(e){}
}
document.getElementById("starClose").addEventListener("click", ()=>{
  playPopSound();
  document.getElementById("starOverlay").classList.remove("open");
});

async function bootCabinet(){
  document.getElementById("authScreen").hidden = true;
  document.getElementById("authLoading").hidden = false;
  try{
    const resp = await apiFetch("/api/cabinet/bootstrap");
    if(!resp.ok) throw new Error("bootstrap_failed");
    const data = await resp.json();
    if(!data.ok) throw new Error("bootstrap_failed");
    applyState(data);
    document.getElementById("authLoading").hidden = true;
    document.getElementById("mainApp").hidden = false;
    initCabinetUI();
  }catch(e){
    if(e.message !== "unauthorized") showAuthScreen("не удалось загрузить кабинет, попробуй ещё раз");
  }
}

let authMode = "login";
function authErrText(code, mode){
  const map = {
    not_paid: "не нашли оплату на этот email — напиши организатору марафона",
    user_already_exists: "такой email уже зарегистрирован — попробуй войти",
    invalid_login_credentials: "неверный email или пароль",
    bad_request: "проверь email и пароль",
  };
  if(map[code]) return map[code];
  const low = String(code||"").toLowerCase();
  if(low.includes("confirm")) return "подтверди email по ссылке из письма, потом попробуй войти";
  if(low.includes("invalid login") || low.includes("invalid_login")) return "неверный email или пароль";
  if(low.includes("already") && low.includes("regist")) return "такой email уже зарегистрирован — попробуй войти";
  return mode==="login" ? "не получилось войти, попробуй ещё раз" : "не получилось зарегистрироваться, попробуй ещё раз";
}
document.getElementById("authSwitchMode").addEventListener("click", ()=>{
  authMode = authMode === "login" ? "register" : "login";
  document.getElementById("authTitle").textContent = authMode==="login" ? "Вход в кабинет" : "Регистрация";
  document.getElementById("authSubmit").textContent = authMode==="login" ? "Войти" : "Зарегистрироваться";
  document.getElementById("authSwitchHint").textContent = authMode==="login" ? "Первый раз?" : "Уже есть аккаунт?";
  document.getElementById("authSwitchMode").textContent = authMode==="login" ? "Зарегистрироваться" : "Войти";
  document.getElementById("authPassword2Field").hidden = authMode !== "register";
  document.getElementById("authForgotWrap").hidden = authMode !== "login";
  document.getElementById("authErr").hidden = true;
});
document.getElementById("authPwdEye").addEventListener("click", ()=>{
  const inp = document.getElementById("authPassword");
  inp.type = inp.type === "password" ? "text" : "password";
});
document.getElementById("authPwdEye2").addEventListener("click", ()=>{
  const inp = document.getElementById("authPassword2");
  inp.type = inp.type === "password" ? "text" : "password";
});

/* ---------- восстановление пароля ---------- */
document.getElementById("authForgotBtn").addEventListener("click", ()=>{
  document.getElementById("recoverEmail").value = document.getElementById("authEmail").value.trim();
  document.getElementById("recoverErr").hidden = true;
  document.getElementById("authScreen").hidden = true;
  document.getElementById("recoverScreen").hidden = false;
});
document.getElementById("recoverBack").addEventListener("click", ()=>{
  document.getElementById("recoverScreen").hidden = true;
  document.getElementById("authScreen").hidden = false;
});
document.getElementById("recoverSubmit").addEventListener("click", async ()=>{
  const email = document.getElementById("recoverEmail").value.trim();
  const errEl = document.getElementById("recoverErr");
  errEl.style.color = ""; errEl.hidden = true;
  if(!email){ errEl.textContent = "введи email"; errEl.hidden = false; return; }
  const btn = document.getElementById("recoverSubmit");
  btn.disabled = true;
  try{
    await fetch("/api/auth/recover", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({email})});
    errEl.style.color = "var(--sage-ink)";
    errEl.textContent = "если такой email зарегистрирован — письмо со ссылкой уже в пути, проверь почту";
    errEl.hidden = false;
  }catch(e){
    errEl.textContent = "не получилось связаться с сервером, попробуй ещё раз"; errEl.hidden = false;
  }
  btn.disabled = false;
});
document.getElementById("newPwdEye1").addEventListener("click", ()=>{
  const inp = document.getElementById("newPassword1");
  inp.type = inp.type === "password" ? "text" : "password";
});
document.getElementById("newPwdEye2").addEventListener("click", ()=>{
  const inp = document.getElementById("newPassword2");
  inp.type = inp.type === "password" ? "text" : "password";
});
document.getElementById("newPasswordSubmit").addEventListener("click", async ()=>{
  const p1 = document.getElementById("newPassword1").value;
  const p2 = document.getElementById("newPassword2").value;
  const errEl = document.getElementById("newPasswordErr");
  errEl.style.color = ""; errEl.hidden = true;
  if(!p1 || p1.length < 6){ errEl.textContent = "пароль минимум 6 символов"; errEl.hidden = false; return; }
  if(p1 !== p2){ errEl.textContent = "пароли не совпадают"; errEl.hidden = false; return; }
  const btn = document.getElementById("newPasswordSubmit");
  btn.disabled = true;
  try{
    const resp = await apiFetch("/api/auth/set-password", {method:"PUT", body:{password:p1}});
    const data = await resp.json();
    if(!resp.ok || !data.ok){
      errEl.textContent = "не получилось сохранить пароль — ссылка могла устареть, запроси новую"; errEl.hidden = false; btn.disabled = false; return;
    }
    document.getElementById("newPasswordScreen").hidden = true;
    bootCabinet();
  }catch(e){
    btn.disabled = false;
  }
});

async function submitAuth(){
  const email = document.getElementById("authEmail").value.trim();
  const password = document.getElementById("authPassword").value;
  const password2 = document.getElementById("authPassword2").value;
  const errEl = document.getElementById("authErr");
  errEl.style.color = ""; errEl.hidden = true;
  if(!email || !password || password.length < 6){
    errEl.textContent = "заполни email и пароль (минимум 6 символов)"; errEl.hidden = false; return;
  }
  if(authMode === "register" && password !== password2){
    errEl.textContent = "пароли не совпадают"; errEl.hidden = false; return;
  }
  const btn = document.getElementById("authSubmit");
  btn.disabled = true;
  try{
    const url = authMode === "login" ? "/api/auth/login" : "/api/auth/register";
    const resp = await fetch(url, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({email,password})});
    const data = await resp.json();
    if(!resp.ok || !data.ok){
      errEl.textContent = authErrText(data.error, authMode); errEl.hidden = false; btn.disabled = false; return;
    }
    if(authMode === "register" && data.needs_confirmation){
      errEl.style.color = "var(--sage-ink)";
      errEl.textContent = "готово! проверь почту, подтверди email и затем войди";
      errEl.hidden = false;
      authMode = "login";
      document.getElementById("authTitle").textContent = "Вход в кабинет";
      document.getElementById("authSubmit").textContent = "Войти";
      document.getElementById("authSwitchHint").textContent = "Первый раз?";
      document.getElementById("authSwitchMode").textContent = "Зарегистрироваться";
      document.getElementById("authPassword2Field").hidden = true;
      btn.disabled = false; return;
    }
    setToken(data.access_token);
    btn.disabled = false;
    bootCabinet();
  }catch(e){
    errEl.textContent = "не получилось связаться с сервером, попробуй ещё раз"; errEl.hidden = false; btn.disabled = false;
  }
}
document.getElementById("authSubmit").addEventListener("click", submitAuth);
document.getElementById("authPassword").addEventListener("keydown", e=>{ if(e.key === "Enter") submitAuth(); });

(function bootOnLoad(){
  let isRecovery = false;
  if(location.hash && location.hash.indexOf("access_token=") !== -1){
    const params = new URLSearchParams(location.hash.slice(1));
    const tok = params.get("access_token");
    isRecovery = params.get("type") === "recovery";
    if(tok){
      setToken(tok);
      try{ history.replaceState(null, "", location.pathname + location.search); }catch(e){}
    }
  }
  if(isRecovery && getToken()){
    document.getElementById("authLoading").hidden = true;
    document.getElementById("newPasswordScreen").hidden = false;
    return;
  }
  if(getToken()){
    document.getElementById("authLoading").hidden = false;
    bootCabinet();
  } else {
    document.getElementById("authLoading").hidden = true;
    document.getElementById("authScreen").hidden = false;
  }
})();

/* ---------- главная ---------- */
function heatClass(pct){ return pct<=0 ? "d0" : pct<=30 ? "d30" : pct<=60 ? "d60" : "d100"; }
function dayPct(n, total){ return total>0 ? Math.min(100, Math.round(n/total*100)) : 0; }

function renderHeat(){
  ["heatgrid1","heatgrid2","heatgrid3"].forEach(id=>document.getElementById(id).innerHTML = "");
  for(let d=1; d<=21; d++){
    let cls;
    if(d > TODAY) cls = "future";
    else cls = heatClass(dayPct(doneCountForDay(d), HABITS.length));
    const cell = document.createElement("div");
    cell.className = "hday "+cls+(d===TODAY?" istoday":"");
    const dt = dayToDate(d);
    cell.innerHTML = `<span class="hday-corner">${d}</span><span class="hday-main"><span class="hday-dd">${dt.getUTCDate()}</span><span class="hday-mm">${RU_MONTHS_SHORT[dt.getUTCMonth()]}</span></span>`;
    cell.title = "День "+d+" · "+fmtDateRu(dt)+(d===TODAY?" · сегодня":"");
    cell.addEventListener("click", ()=>openDay(d));
    const gridId = "heatgrid"+(Math.ceil(d/7));
    document.getElementById(gridId).appendChild(cell);
  }
  document.getElementById("bigDayTxt").textContent = TODAY;
  const marathonPct = Math.round(TODAY/21*100);
  document.getElementById("dayPct").textContent = marathonPct+"%";
  document.getElementById("dayPillFill").style.width = marathonPct+"%";
  document.getElementById("daySub").textContent = "Ты уже прошла треть пути марафона — продолжай в своём темпе";
}

function openDay(d){
  openOverlay("День "+d+(d===TODAY?" (сегодня)":""), "21-дневный марафон");
  if(d > TODAY){
    document.getElementById("overlayBody").innerHTML =
      `<p class="note" style="margin:0">Этот день ещё не наступил — загляни сюда, когда он начнётся.</p>`;
    return;
  }
  if(HABITS.length === 0){
    document.getElementById("overlayBody").innerHTML =
      `<p class="note" style="margin:0">В этот день привычки ещё не были выбраны.</p>`;
    return;
  }
  const checks = d===TODAY ? liveChecked : HABIT_IDS.map(id=>doneForHabitOnDay(id, d));
  const rows = HABITS.map((h,i)=>{
    const c = checks[i];
    return `<div class="hab ${c?'done':''}">
      <span class="markvisual ${c?'on':''}">${ICONS_CHECK}</span>
      <span class="hab-text">${h}</span></div>`;
  }).join("");
  document.getElementById("overlayBody").innerHTML =
    `<div class="checklist" style="padding:4px 10px;box-shadow:none">${rows}</div>`;
}
document.getElementById("closePanel").onclick=()=>{ playPopSound(); if(onboarding) onboarding = false; document.getElementById("overlay").classList.remove("open"); };
document.getElementById("overlay").addEventListener("click",e=>{if(e.target.id==="overlay"){ if(onboarding) onboarding = false; e.currentTarget.classList.remove("open"); }});

/* ---------- сегодня ---------- */
function computeStreak(){
  let s=0;
  for(let d=TODAY-1; d>=1; d--){
    if(HABITS.length>0 && doneCountForDay(d)===HABITS.length) s++;
    else break;
  }
  return s;
}
function saveTodayLog(closedOverride){
  const dateStr = dateForDay(TODAY);
  const habits_done = {};
  HABIT_IDS.forEach((id,i)=>{ habits_done[id] = !!liveChecked[i]; });
  const prevClosed = (STATE.logsByDate[dateStr] && STATE.logsByDate[dateStr].closed) || false;
  const closed = closedOverride !== undefined ? closedOverride : prevClosed;
  STATE.logsByDate[dateStr] = {habits_done, closed};
  apiFetch("/api/cabinet/logs", {method:"PUT", body:{date:dateStr, habits_done, closed}}).catch(()=>{});
}
function renderChecklist(){
  const streakNow = computeStreak();
  const yestMissed = TODAY > 1 && doneCountForDay(TODAY-1) === 0;
  let affirmPool = AFFIRMATIONS, affirmIdx = TODAY-1;
  if(yestMissed){ affirmPool = AFFIRM_SUPPORT; affirmIdx = TODAY; }
  else if(streakNow >= 3){ affirmPool = AFFIRM_CELEBRATE; affirmIdx = TODAY; }
  document.getElementById("affirmText").textContent = "«"+affirmPool[((affirmIdx%affirmPool.length)+affirmPool.length)%affirmPool.length]+"»";
  const openerText = SOCIAL_OPENERS[(TODAY-1)%SOCIAL_OPENERS.length].replace(/15/, String(todayCount()));
  document.getElementById("socialOpener").textContent = openerText;
  document.getElementById("socialCloser").textContent = SOCIAL_CLOSERS[(TODAY-1+2)%SOCIAL_CLOSERS.length];
  document.getElementById("eveningRem").hidden = new Date().getHours() < 18;
  const doneCount = liveChecked.filter(Boolean).length;
  const remaining = HABITS.length - doneCount;
  const statusEl = document.getElementById("dayStatus");
  if(HABITS.length === 0){
    statusEl.innerHTML = `Пока не выбраны привычки — нажми «Изменить привычки».`;
  } else if(remaining === 0){
    statusEl.innerHTML = "Все привычки на сегодня отмечены ✨";
  } else if(doneCount === 0){
    statusEl.innerHTML = `Начни с одной привычки — дальше будет легче. Осталось <b>${remaining}</b>.`;
  } else {
    statusEl.innerHTML = `Хорошо идёшь: часть привычек уже отмечена 💗 Осталось <b>${remaining}</b>.`;
  }
  const box = document.getElementById("checklist");
  box.innerHTML = HABITS.map((h,i)=>{
    return `<label class="hab ${liveChecked[i]?'done':''}" for="hb${i}">
    <span class="markwrap"><input type="checkbox" id="hb${i}" ${liveChecked[i]?'checked':''} ${dayClosed?'disabled':''}>
    <span class="markvisual">${ICONS_CHECK}</span></span>
    <span class="hab-text">${h}</span>
    </label>`;
  }).join("");
  HABITS.forEach((_,i)=>document.getElementById("hb"+i).addEventListener("change",e=>{
    liveChecked[i]=e.target.checked; playPopSound(); renderChecklist(); renderHeat(); renderProgress();
    saveTodayLog();
  }));
  const btn = document.getElementById("closeDayBtn");
  btn.disabled = dayClosed || HABITS.length === 0;
  btn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12.5 9.5 17 19 7"/></svg> ${dayClosed?'День закрыт':'Закрыть день'}`;
}
document.getElementById("closeDayBtn").addEventListener("click", ()=>{
  if(dayClosed || HABITS.length === 0) return;
  playPopSound();
  dayClosed = true; renderChecklist(); renderHeat();
  const done = liveChecked.filter(Boolean).length;
  document.getElementById("fdDay").textContent = TODAY;
  document.getElementById("fdDone").textContent = done;
  document.getElementById("fdTotal").textContent = HABITS.length;
  document.getElementById("fdPraise").textContent = done===HABITS.length?"Ты держишь ритм ✨":done===0?"Завтра новый день":"Ты в процессе, и этого достаточно";
  document.getElementById("fdOverlay").classList.add("open");
  saveTodayLog(true);
  renderProgress();
});
document.getElementById("fdClose").addEventListener("click",()=>{ playPopSound(); document.getElementById("fdOverlay").classList.remove("open"); });
document.getElementById("fdOverlay").addEventListener("click",e=>{if(e.target.id==="fdOverlay")e.currentTarget.classList.remove("open");});

/* ---------- выбор привычек ---------- */
let pickSelection = [];
let onboarding = false;
function openHabitPicker(){
  pickSelection = [...HABITS];
  openOverlay("Выбери привычки");
  renderPicker();
}
function startOnboarding(){
  onboarding = true;
  pickSelection = [];
  openOverlay("Выбери свои первые привычки", "Добро пожаловать в Serenity Moon 🌙");
  renderPicker();
}
function chipHtml(name){
  const on = pickSelection.includes(name);
  return `<div class="habchip ${on?'checked':''}" data-h="${name}">
    <span class="htxt">${name}</span>
    <button class="hinfo" data-h="${name}" type="button">?</button></div>`;
}
function showChipDetail(name){
  const meta = findHabitMeta(name);
  const body = document.getElementById("infoOverlayBody");
  if(!meta){
    body.innerHTML = `<p class="info-title">${name}</p>
      <p class="info-desc">Своя привычка — добавлено вручную, подробностей нет.</p>`;
  } else {
    body.innerHTML = `<p class="info-title">${meta.name}</p>
      <div class="info-meta">
        <div><div class="lbl">Когда</div><div class="val">${meta.when}</div></div>
        <div><div class="lbl">Сколько</div><div class="val">${meta.dur}</div></div>
      </div>
      <p class="info-desc">${meta.desc}</p>
      ${meta.why ? `<div class="info-why"><div class="info-why-lbl">Почему это работает</div><p class="info-why-txt">${meta.why}</p></div>` : ""}`;
  }
  document.getElementById("infoOverlay").classList.add("open");
}
document.getElementById("closeInfoPanel").addEventListener("click", ()=>{ playPopSound(); document.getElementById("infoOverlay").classList.remove("open"); });
document.getElementById("infoOverlay").addEventListener("click",e=>{if(e.target.id==="infoOverlay")e.currentTarget.classList.remove("open");});
function renderPicker(){
  let html = `<div class="pickintro">
    <p>🌿 Выбери от 1 до ${MAX_HABITS} привычек, которые ты готова выполнять ежедневно в течение 21 дня.</p>
    <p>Перед тем как подтвердить свой выбор, ещё раз просмотри список. Убедись, что количество привычек и время на их выполнение реалистичны для тебя.</p>
    <p>Помни: лучше меньше, но регулярно! 💚</p>
    </div>`;
  Object.entries(CATALOG).forEach(([cat,items])=>{
    html += `<div class="pickcat"><h4>${cat}</h4><div class="chipgrid">${items.map(h=>chipHtml(h.name)).join("")}</div></div>`;
  });
  const customOnes = pickSelection.filter(h=>!findHabitMeta(h));
  html += `<div class="pickcat"><h4>Свои привычки</h4>
    <div class="chipgrid" id="customList">${customOnes.map(chipHtml).join("")}</div>
    <button class="editlink" id="customAddBtn" type="button" style="margin-top:2px">+ добавить свою привычку</button>
    </div>
    <p class="note" id="pickWarn" hidden style="margin:0 0 10px">Выбери хотя бы одну привычку, чтобы сохранить</p>
    <button class="savepick" id="savePick">${onboarding ? "Начать марафон 🌙" : "Сохранить"}</button>`;
  document.getElementById("overlayBody").innerHTML = html;
  document.getElementById("overlayBody").querySelectorAll(".habchip").forEach(chip=>{
    chip.addEventListener("click", (e)=>{
      if(e.target.closest(".hinfo")) return;
      openHabitModal({mode:"picker", name:chip.dataset.h});
    });
  });
  document.getElementById("overlayBody").querySelectorAll(".hinfo").forEach(btn=>{
    btn.addEventListener("click", (e)=>{
      e.stopPropagation();
      showChipDetail(btn.dataset.h);
    });
  });
  document.getElementById("customAddBtn").addEventListener("click", ()=>{
    playPopSound();
    openHabitModal({mode:"picker", name:""});
  });
  document.getElementById("savePick").addEventListener("click", async ()=>{
    if(!pickSelection.length){ document.getElementById("pickWarn").hidden = false; return; }
    playPopSound();
    const payload = pickSelection.map(name => ({name, config: habitConfig[name] || defaultHabitConfig()}));
    let data;
    try{
      const resp = await apiFetch("/api/cabinet/habits", {method:"PUT", body:{habits: payload}});
      data = await resp.json();
      if(!resp.ok || !data.ok) return;
    }catch(e){ return; }
    HABIT_IDS = data.habits.map(h=>h.id);
    HABITS = data.habits.map(h=>h.name);
    data.habits.forEach(h=>{ habitConfig[h.name] = (h.config && Object.keys(h.config).length) ? h.config : defaultHabitConfig(); });
    liveChecked = new Array(HABITS.length).fill(false);
    dayClosed = false;
    renderChecklist(); renderHeat(); renderProgress();
    syncHabitsCount();
    if(onboarding){
      onboarding = false;
      document.getElementById("closePanel").style.display = "";
    }
    document.getElementById("overlay").classList.remove("open");
  });
}
document.getElementById("editHabitsBtn").addEventListener("click", openHabitPicker);

/* ---------- прогресс: статистика + "раньше тоже делала" ---------- */
let pastHabits = [];
function habitScheduleLabel(name){
  const cfg = habitConfig[name];
  if(!cfg || !cfg.days || cfg.days.length === 7) return "каждый день";
  if(!cfg.days.length) return "дни не заданы";
  const names = ["пн","вт","ср","чт","пт","сб","вс"];
  return cfg.days.slice().sort((a,b)=>a-b).map(d=>names[d]).join(", ");
}
function renderProgress(){
  let perfectDays = 0;
  if(HABITS.length){
    for(let d=1; d<TODAY; d++){ if(doneCountForDay(d) === HABITS.length) perfectDays++; }
    if(liveChecked.filter(Boolean).length === HABITS.length) perfectDays++;
  }

  document.getElementById("diamondVal").textContent = perfectDays + mastermindBonus();
  document.getElementById("countdownVal").textContent = Math.max(0, 21 - TODAY);
  renderStars();

  document.getElementById("byHabitList").innerHTML = HABITS.length ? HABITS.map((h,i)=>{
    const id = HABIT_IDS[i];
    let done = 0;
    for(let d=1; d<=TODAY; d++){ if(doneForHabitOnDay(id, d)) done++; }
    const pct = Math.round(done/TODAY*100);
    const circ = 75.4, off = (circ*(1-pct/100)).toFixed(1);
    return `<div class="byhab-row">
      <div class="byhab-ring"><svg viewBox="0 0 30 30"><circle cx="15" cy="15" r="12" fill="none" stroke="var(--line)" stroke-width="4"/><circle cx="15" cy="15" r="12" fill="none" stroke="var(--sage)" stroke-width="4" stroke-linecap="round" stroke-dasharray="${circ}" stroke-dashoffset="${off}"/></svg></div>
      <div class="byhab-body"><div class="byhab-name">${h}</div><div class="byhab-sub">${habitScheduleLabel(h)}</div></div>
      <div class="byhab-right"><div class="byhab-pct">${pct}%</div><div class="byhab-fr">${done} из ${TODAY}</div></div></div>`;
  }).join("") : `<p class="note" style="margin:0">Пока нет активных привычек</p>`;

  const pastCard = document.getElementById("pastHabitsCard");
  pastCard.hidden = pastHabits.length === 0;
  document.getElementById("pastHabitsList").innerHTML = pastHabits.map(h=>
    `<div class="past-row"><span class="past-name">${h}</span><span class="past-sub">была в твоём списке ранее</span></div>`
  ).join("");
}

/* ---------- модалка: создать/редактировать привычку ---------- */
let hmState = null;

function renderHabitModalDays(){
  document.querySelectorAll("#habitDaysGrid .hm-day").forEach(btn=>{
    btn.classList.toggle("active", hmState.days.includes(Number(btn.dataset.d)));
  });
}
function renderHabitModalQty(){
  document.getElementById("habitQtyToggle").classList.toggle("on", hmState.qtyOn);
  document.getElementById("habitQtyExpand").classList.toggle("collapsed", !hmState.qtyOn);
  document.getElementById("habitTargetVal").textContent = hmState.target;
  document.getElementById("habitStepVal").textContent = hmState.step;
}
function openHabitModal(opts){
  hmState = opts || {mode:"checklist"};
  let name = "";
  if(hmState.mode === "checklist" && typeof hmState.index === "number") name = HABITS[hmState.index];
  if(hmState.mode === "picker") name = hmState.name || "";
  const isEdit = !!name;
  hmState.name = name;
  document.getElementById("habitModalTitle").textContent = isEdit ? "редактировать привычку" : "новая привычка";
  document.getElementById("habitNameInput").value = name;
  const cfg = (name && habitConfig[name]) ? habitConfig[name] : defaultHabitConfig();
  hmState.days = [...cfg.days]; hmState.qtyOn = cfg.qtyOn; hmState.target = cfg.target; hmState.step = cfg.step;
  document.getElementById("habitUnitInput").value = cfg.unit || "";
  renderHabitModalDays(); renderHabitModalQty();
  document.getElementById("habitDeleteBtn").style.display = isEdit ? "" : "none";
  document.getElementById("habitModalWarn").hidden = true;
  document.getElementById("habitModalBackdrop").classList.add("open");
  setTimeout(()=>document.getElementById("habitNameInput").focus(), 60);
}
function closeHabitModal(){
  document.getElementById("habitModalBackdrop").classList.remove("open");
}
function syncHabitsCount(){
  document.getElementById("profileHabitsCount").textContent = "Выбрано "+HABITS.length+(HABITS.length===1?" привычка":" привычек");
}
async function persistHabits(){
  const payload = HABITS.map(name => ({name, config: habitConfig[name] || defaultHabitConfig()}));
  try{
    const resp = await apiFetch("/api/cabinet/habits", {method:"PUT", body:{habits: payload}});
    const data = await resp.json();
    if(resp.ok && data.ok){ HABIT_IDS = data.habits.map(h=>h.id); }
  }catch(e){}
}
document.getElementById("habitDaysGrid").addEventListener("click", e=>{
  const btn = e.target.closest(".hm-day"); if(!btn) return;
  const d = Number(btn.dataset.d);
  if(hmState.days.includes(d)) hmState.days = hmState.days.filter(x=>x!==d);
  else hmState.days.push(d);
  renderHabitModalDays(); playPopSound();
});
document.getElementById("habitQtyToggle").addEventListener("click", ()=>{
  hmState.qtyOn = !hmState.qtyOn; renderHabitModalQty(); playPopSound();
});
document.querySelectorAll(".hm-stepper button").forEach(btn=>{
  btn.addEventListener("click", ()=>{
    const d = Number(btn.dataset.step);
    if(btn.dataset.target === "target") hmState.target = Math.max(1, hmState.target + d);
    else hmState.step = Math.max(1, hmState.step + d);
    playPopSound();
    renderHabitModalQty();
  });
});
document.getElementById("addHabitBtn").addEventListener("click", openHabitPicker);
document.getElementById("habitModalClose").addEventListener("click", ()=>{ playPopSound(); closeHabitModal(); });
document.getElementById("habitModalBackdrop").addEventListener("click", e=>{
  if(e.target.id === "habitModalBackdrop") closeHabitModal();
});
document.getElementById("habitSaveBtn").addEventListener("click", ()=>{
  const name = document.getElementById("habitNameInput").value.trim();
  if(!name) return;
  playPopSound();
  const unit = document.getElementById("habitUnitInput").value.trim();
  const newCfg = {days:[...hmState.days], qtyOn:hmState.qtyOn, target:hmState.target, unit, step:hmState.step};
  if(hmState.name && hmState.name !== name) delete habitConfig[hmState.name];
  habitConfig[name] = newCfg;
  if(hmState.mode === "checklist"){
    if(typeof hmState.index === "number"){
      HABITS[hmState.index] = name;
    } else {
      if(HABITS.length >= MAX_HABITS){ document.getElementById("habitModalWarn").hidden = false; return; }
      HABITS.push(name); liveChecked.push(false);
    }
    renderChecklist(); renderHeat(); renderProgress();
    persistHabits();
  } else if(hmState.mode === "picker"){
    if(hmState.name && hmState.name !== name){
      pickSelection = pickSelection.map(x => x === hmState.name ? name : x);
    } else if(!pickSelection.includes(name)){
      if(pickSelection.length >= MAX_HABITS){ document.getElementById("habitModalWarn").hidden = false; return; }
      pickSelection.push(name);
    }
    renderPicker();
  }
  syncHabitsCount();
  closeHabitModal();
});
document.getElementById("habitDeleteBtn").addEventListener("click", ()=>{
  playPopSound();
  let name = null, everDone = false;
  if(hmState.mode === "checklist" && typeof hmState.index === "number"){
    name = HABITS[hmState.index];
    everDone = true;
    HABITS.splice(hmState.index,1); liveChecked.splice(hmState.index,1);
    renderChecklist(); renderHeat();
    persistHabits();
  } else if(hmState.mode === "picker" && hmState.name){
    name = hmState.name;
    pickSelection = pickSelection.filter(x=>x!==name);
    renderPicker();
  }
  if(name && everDone && !pastHabits.includes(name)) pastHabits.push(name);
  renderProgress();
  syncHabitsCount();
  closeHabitModal();
});

document.getElementById("shareBtn").addEventListener("click", ()=>{
  playPopSound();
  const c=document.createElement("canvas"); c.width=640; c.height=640;
  const ctx=c.getContext("2d");
  const grad=ctx.createLinearGradient(0,0,640,640); grad.addColorStop(0,"#AA9DB8"); grad.addColorStop(1,"#6A9993");
  ctx.fillStyle=grad; ctx.fillRect(0,0,640,640);
  ctx.fillStyle="#fff"; ctx.textAlign="center";
  ctx.font="italic 22px -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"; ctx.fillText("serenity moon · день "+TODAY+" из 21", 320, 90);
  ctx.font="700 40px -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif";
  const done=liveChecked.filter(Boolean).length;
  ctx.fillText(done+" из "+HABITS.length+" привычек сегодня", 320, 150);
  let y=230; ctx.font="500 24px -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"; ctx.textAlign="left";
  HABITS.forEach((h,i)=>{ ctx.fillText((liveChecked[i]?"✓":"○")+"  "+h, 90, y); y+=54; });
  ctx.textAlign="center"; ctx.font="italic 18px -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif";
  ctx.fillText("Маленькие шаги каждый день создают большие результаты", 320, 580);
  const out=document.getElementById("shareOut"); out.hidden=false;
  out.innerHTML = `<img src="${c.toDataURL('image/png')}" alt="карточка дня"><p class="share-note">Сохранение/отправка в телеграм заработает на реальном сайте.</p>`;
});

/* ---------- материалы ---------- */
let activeTheme="all", query="";
const PHOTOS = {
  mila: "https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/mila.jpg",
  maria: "https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/maria.jpg",
  anna: "https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/anna.jpg"
};

function matchesQuery(m){ if(!query) return true; return (m.title+" "+(m.tags||[]).join(" ")).toLowerCase().includes(query); }

function matRowHtml(m){
  const unlocked = m.day===null||m.day<=TODAY;
  const status = m.day===null ? `<span class="mat-status open">Открыто</span>`
    : unlocked ? `<span class="mat-status open">День ${m.day}</span>` : `<span class="mat-status locked">${ICONS.lock} День ${m.day}</span>`;
  const newBadge = m.day===TODAY ? `<span class="mat-new">Новое</span>` : "";
  return `<div class="mat ${unlocked?'':'is-locked'}"><div class="mat-icon">${ICONS[m.type]||ICONS.pdf}</div>
    <div class="mat-body"><div class="mat-title">${m.title}${newBadge}</div><div class="mat-tags">${(AUTHORS[m.author]||{}).name||''} · #${(m.tags||[]).join(' #')}</div></div>${status}</div>`;
}

function renderEvents(){
  const events = STATE.events || [];
  const next = events.find(e=>e.day >= TODAY);
  document.getElementById("nextEventBanner").innerHTML = next
    ? `<div class="evt-next"><div class="lbl">Ближайший эфир</div><div class="ttl">${next.title}</div><div class="meta">День ${next.day}${next.time?(' · '+next.time):''}${next.type?(' · '+next.type):''}</div></div>`
    : "";
  document.getElementById("eventsList").innerHTML = events.map(e=>{
    const isPast = e.day < TODAY;
    return `<div class="evt-row ${isPast?'past':''}">
      <div class="evt-date"><div class="d">${e.day}</div><div class="l">день</div></div>
      <div class="evt-body"><div class="evt-ttl">${e.title}</div><div class="evt-meta">${isPast?'прошёл · запись скоро':(e.time||'')}</div></div>
      ${isPast?'':(e.type?`<div class="evt-tag">${e.type}</div>`:'')}
    </div>`;
  }).join("");

  const todayEv = events.find(e=>e.day === TODAY);
  const alertEl = document.getElementById("todayEventAlert");
  alertEl.hidden = !todayEv;
  if(todayEv){
    document.getElementById("todayEventTxt").innerHTML = `сегодня${todayEv.time?(' в '+todayEv.time):''} — <b>${todayEv.title}</b>`;
  }
}

function renderMaterials(){
  const grid = document.getElementById("mgrid");
  const flat = document.getElementById("mflat");
  const flatMode = activeTheme !== "all" || !!query;
  grid.hidden = flatMode;
  flat.hidden = !flatMode;

  if(flatMode){
    const items = MATERIALS.filter(m=>
      (activeTheme==="all"||(m.tags||[]).includes(activeTheme)) &&
      matchesQuery(m)
    );
    flat.innerHTML = items.length ? items.map(matRowHtml).join("") :
      `<p class="note" style="margin:12px 0">Ничего не нашлось</p>`;
    return;
  }

  grid.innerHTML = "";
  Object.entries(AUTHORS).forEach(([key,a])=>{
    const items = MATERIALS.filter(m=>m.author===key);
    if(!items.length && key!=="mastermind") return;
    const card = document.createElement("div");
    card.className = "mcard "+key;
    const photo = a.photo || PHOTOS[key] || "";
    const dotInner = photo ? `<img src="${photo}" alt="${a.name}">` : "";
    const dotCss = COLOR_CSS[a.color] || "";
    const dotStyleAttr = (!photo && dotCss) ? ` style="background:${dotCss}"` : "";
    const hasNew = items.some(m=>m.day===TODAY);
    card.innerHTML = `<div class="dot"${dotStyleAttr}>${dotInner}</div><div class="body"><h3>${a.name}${hasNew?'<span class="mat-new">Новое</span>':''}</h3><div class="topics">${a.topics||''}</div>
      <div class="foot"><span>${a.desc||''}</span></div></div>`;
    card.addEventListener("click", ()=>openMaterial(key));
    grid.appendChild(card);
  });
}
function openMaterial(key){
  const a = AUTHORS[key];
  openOverlay(a.name, a.topics);
  const items = MATERIALS.filter(m=>m.author===key);
  const rows = items.map((m,i)=>{
    const unlocked = m.day===null||m.day<=TODAY;
    const status = m.day===null ? `<span class="mat-status open">Открыто</span>`
      : unlocked ? `<span class="mat-status open">День ${m.day}</span>` : `<span class="mat-status locked">${ICONS.lock} День ${m.day}</span>`;
    const newBadge = m.day===TODAY ? `<span class="mat-new">Новое</span>` : "";
    return `<div class="mat ${unlocked?'':'is-locked'}" ${unlocked?`data-i="${i}" style="cursor:pointer"`:""}><div class="mat-icon">${ICONS[m.type]||ICONS.pdf}</div>
      <div class="mat-body"><div class="mat-title">${m.title}${newBadge}</div><div class="mat-tags">#${(m.tags||[]).join(' #')}</div></div>${status}</div>`;
  }).join("");
  document.getElementById("overlayBody").innerHTML =
    `${rows}<p class="note">Доступ к файлу открывается в день разблокировки.</p>`;
  document.getElementById("overlayBody").querySelectorAll(".mat[data-i]").forEach(row=>{
    row.addEventListener("click", ()=>openMaterialDetail(key, Number(row.dataset.i)));
  });
}
function openMaterialDetail(key, idx){
  const a = AUTHORS[key];
  const items = MATERIALS.filter(m=>m.author===key);
  const m = items[idx];
  openOverlay(m.title, a.name);
  const codeBox = m.mmId ? mmCodeBoxHtml(m.mmId) : "";
  const hasLink = m.url && m.url !== "#";
  const openLink = hasLink ? `<button id="matOpenBtn" class="btn primary" style="margin:4px 0 10px">Открыть материал →</button>` : "";
  document.getElementById("overlayBody").innerHTML =
    `<div class="mat" style="margin-bottom:10px"><div class="mat-icon">${ICONS[m.type]||ICONS.pdf}</div>
      <div class="mat-body"><div class="mat-title">${m.title}</div><div class="mat-tags">#${(m.tags||[]).join(' #')}</div></div></div>
     ${openLink}
     ${codeBox}
     <button class="editlink" id="matBackBtn" type="button" style="margin-top:4px">← к материалам ${a.name}</button>`;
  if(m.mmId) wireMmCodeBox(m.mmId);
  if(hasLink){
    document.getElementById("matOpenBtn").addEventListener("click", async ()=>{
      playPopSound();
      let target = m.url;
      if(m.id){
        try{
          const resp = await apiFetch("/api/cabinet/materials/"+m.id+"/link");
          const data = await resp.json();
          if(resp.ok && data.ok) target = data.url;
        }catch(e){ return; }
      }
      window.open(target, "_blank", "noopener");
    });
  }
  document.getElementById("matBackBtn").addEventListener("click", ()=>openMaterial(key));
}
document.getElementById("themeChips").addEventListener("click",e=>{
  const b=e.target.closest(".chip"); if(!b) return;
  document.querySelectorAll("#themeChips .chip").forEach(c=>c.classList.remove("active"));
  b.classList.add("active"); activeTheme=b.dataset.t; renderMaterials();
});
const searchBox=document.getElementById("searchBox"), searchDrop=document.getElementById("searchDrop");
searchBox.addEventListener("input",()=>{
  query = searchBox.value.trim().toLowerCase();
  if(!query){ searchDrop.hidden=true; renderMaterials(); return; }
  const results = MATERIALS.filter(matchesQuery).slice(0,6);
  searchDrop.innerHTML = results.length ? results.map(m=>
    `<div class="sr-item" data-a="${m.author}"><div class="mat-icon">${ICONS[m.type]||ICONS.pdf}</div><div>${m.title}<div class="mat-tags">${(AUTHORS[m.author]||{}).name||''}</div></div></div>`
  ).join("") : `<div class="sr-item">Ничего не нашлось</div>`;
  searchDrop.hidden = false; renderMaterials();
});
searchDrop.addEventListener("click",e=>{ const it=e.target.closest(".sr-item"); if(it&&it.dataset.a) openMaterial(it.dataset.a); searchDrop.hidden=true; });
document.addEventListener("click",e=>{ if(!e.target.closest(".search-wrap")) searchDrop.hidden=true; });

/* ---------- профиль: отображение + редактирование ---------- */
function renderProfileTile(){
  const p = STATE.profile || {};
  const name = p.name || "Добавь своё имя";
  const bits = [];
  if(p.age) bits.push(p.age);
  if(p.city) bits.push(p.city);
  bits.push("редактировать");
  document.querySelector("#v3tile .v3-txt").innerHTML = name + "<small>" + bits.join(" · ") + "</small>";
  document.querySelectorAll(".avatar img").forEach(img=>img.remove());
  if(p.photo_url){
    document.querySelectorAll(".avatar").forEach(av=>{
      const img = document.createElement("img");
      img.src = p.photo_url;
      av.insertBefore(img, av.firstChild);
    });
  }
}
document.getElementById("avatarFileInput").addEventListener("change", async (e)=>{
  const file = e.target.files[0];
  e.target.value = "";
  if(!file) return;
  const fd = new FormData();
  fd.append("file", file);
  try{
    const resp = await apiFetch("/api/cabinet/avatar", {method:"POST", body: fd});
    const data = await resp.json();
    if(resp.ok && data.ok){
      STATE.profile = Object.assign({}, STATE.profile, {photo_url: data.photo_url});
      renderProfileTile();
      playPopSound();
    }
  }catch(err){}
});
function wireCameraButtons(){
  document.querySelectorAll(".camera").forEach(btn=>{
    btn.onclick = (e)=>{ e.stopPropagation(); document.getElementById("avatarFileInput").click(); };
  });
}
wireCameraButtons();
function openProfileEdit(){
  openOverlay("Редактировать профиль");
  const p = STATE.profile || {};
  const esc = s => String(s==null?"":s).replace(/"/g,"&quot;").replace(/</g,"&lt;");
  document.getElementById("overlayBody").innerHTML = `
    <div class="avatar-row" style="margin-top:0">
      <div class="avatar">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="8.5" r="3.5"/><path d="M4.5 20c1.5-4 4.5-6 7.5-6s6 2 7.5 6"/></svg>
        <div class="camera"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="7" width="18" height="13" rx="2.5"/><circle cx="12" cy="13.5" r="3.5"/><path d="M8 7l1.5-2.5h5L16 7"/></svg></div>
      </div>
    </div>
    <div class="editform">
      <div class="field"><label>имя и фамилия</label><input type="text" id="profName" value="${esc(p.name)}"></div>
      <div class="field"><label>возраст</label><input type="text" id="profAge" value="${esc(p.age)}"></div>
      <div class="field"><label>город</label><input type="text" id="profCity" value="${esc(p.city)}"></div>
      <div class="field"><label>о себе</label><textarea id="profBio">${esc(p.bio)}</textarea></div>
      <button class="savebtn" id="overlaySave">Сохранить</button>
    </div>`;
  renderProfileTile();
  wireCameraButtons();
  document.getElementById("overlaySave").addEventListener("click", async ()=>{
    const name = document.getElementById("profName").value.trim();
    const ageRaw = document.getElementById("profAge").value.trim();
    const age = ageRaw ? parseInt(ageRaw,10) : null;
    const city = document.getElementById("profCity").value.trim();
    const bio = document.getElementById("profBio").value.trim();
    try{
      await apiFetch("/api/cabinet/profile", {method:"PUT", body:{name, age, city, bio}});
      STATE.profile = Object.assign({}, STATE.profile, {name, age, city, bio});
      renderProfileTile();
    }catch(e){}
    playPopSound();
    document.getElementById("overlay").classList.remove("open");
  });
}
document.getElementById("v3tile").addEventListener("click", openProfileEdit);
document.getElementById("logoutBtn").addEventListener("click", ()=>{
  setToken(null);
  location.reload();
});

</script>

</body></html>"""


ADMIN_APP_HTML = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Serenity Moon — админка</title>
<style>
:root{
  --lav:#AA9DB8; --lav-ink:#6B5A7C; --sage:#6A9993; --sage-ink:#3F6660;
  --sand:#CBBAA8; --sand-ink:#8A6F55; --ink:#332E3D; --ink-soft:#6B6475;
  --bg:#FAF6F1; --surface:#FFFFFF; --surface-2:#F2ECE6; --line:#E7DED6;
  --ok:#5C8A5C; --ok-bg:#E7F1E4; --warn:#B98A3E; --warn-bg:#FBF0DE;
  --shadow:0 10px 30px -14px rgba(51,46,61,.22);
  --sys:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,system-ui,"Helvetica Neue",Arial,sans-serif;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sys)}
h1,h2,h3{margin:0;font-weight:600;letter-spacing:-.01em}

.shell{max-width:1180px;margin:0 auto;padding:28px 20px 70px}
.top{display:flex;align-items:center;gap:10px;margin-bottom:4px}
.top .mark{width:28px;height:28px;border-radius:8px;background:var(--lav);color:#fff;flex-shrink:0;
  display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700}
.top .name{font-weight:600;font-size:15px}
.top .logout{margin-left:auto;font-size:12px;color:var(--ink-soft);text-decoration:none}
.sub{font-size:12.5px;color:var(--ink-soft);margin:6px 0 22px}

.tabs{display:flex;gap:6px;background:var(--surface-2);padding:4px;border-radius:14px;margin-bottom:22px}
.tab{flex:1;text-align:center;padding:9px 10px;border-radius:11px;font-size:13px;font-weight:600;color:var(--ink-soft);cursor:pointer;user-select:none}
.tab.active{background:var(--lav);color:#fff;font-weight:700;box-shadow:var(--shadow)}

.card{background:var(--surface);border:1px solid var(--line);border-radius:18px;padding:22px;box-shadow:var(--shadow);margin-bottom:16px}
.card h2{font-size:15.5px;margin-bottom:3px}
.card .hint{font-size:12px;color:var(--ink-soft);margin-bottom:18px}
.field{margin-bottom:16px}
.field:last-of-type{margin-bottom:0}
.field label{display:block;font-size:11.5px;font-weight:600;color:var(--ink-soft);text-transform:uppercase;letter-spacing:.04em;margin-bottom:7px}
.field label .req{color:#c0564f;margin-left:3px}
.field input[type=text],.field input[type=number],.field select,.field textarea{
  width:100%;font-family:var(--sys);font-size:14px;color:var(--ink);background:var(--surface);
  border:1.3px solid var(--sage);border-radius:12px;padding:11px 13px}
.field textarea{resize:vertical;min-height:60px}
.chipgrid{display:flex;flex-wrap:wrap;gap:8px}
.tchip{font-size:12.5px;font-weight:500;color:var(--ink);background:var(--surface-2);border:1.5px solid var(--line);
  border-radius:999px;padding:7px 13px;cursor:pointer;user-select:none}
.tchip.on{background:var(--sage);border-color:var(--sage);color:#fff}
.toggle-row{display:flex;align-items:center;justify-content:space-between;background:var(--surface-2);border-radius:12px;padding:11px 13px}
.toggle-row .l{font-size:13px}
.sw{position:relative;width:40px;height:23px;border-radius:999px;background:var(--line);border:none;cursor:pointer;flex-shrink:0}
.sw::after{content:"";position:absolute;top:2px;left:2px;width:19px;height:19px;border-radius:50%;background:#fff;box-shadow:0 1px 3px rgba(0,0,0,.25);transition:.18s}
.sw.on{background:var(--sage)}
.sw.on::after{transform:translateX(17px)}
.filebox{border:1.5px dashed var(--line);border-radius:12px;padding:18px;text-align:center;font-size:12.5px;color:var(--ink-soft);cursor:pointer}
.filebox b{color:var(--lav-ink)}
.save{width:100%;border:none;background:var(--lav);color:#fff;border-radius:999px;padding:13px;font-weight:600;
  font-size:13.5px;cursor:pointer;margin-top:18px;transition:transform .1s}
.save:active{transform:scale(.98)}
.err{font-size:12px;color:#c0564f;margin-top:6px;display:none}
.toast{position:fixed;left:50%;bottom:26px;transform:translateX(-50%) translateY(20px);background:var(--ink);color:#fff;
  font-size:13px;font-weight:500;padding:11px 20px;border-radius:999px;box-shadow:0 14px 30px -10px rgba(0,0,0,.4);
  opacity:0;pointer-events:none;transition:.25s}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.note{font-size:11.5px;color:var(--ink-soft);background:var(--surface-2);border-radius:10px;padding:10px 13px;line-height:1.5;margin-top:4px}

.evlist{display:flex;flex-direction:column;gap:8px;margin-top:4px}
.evrow{display:flex;align-items:center;gap:12px;background:var(--surface-2);border:1px solid var(--line);border-radius:12px;padding:10px 12px}
.evrow.past{background:var(--ok-bg);border-color:var(--ok)}
.evrow.past .evday{background:var(--ok);border-color:var(--ok);color:#fff}
.evrow.past .evtag{background:var(--ok);color:#fff}
.evrow .evday{width:40px;height:44px;border-radius:10px;background:var(--surface);border:1px solid var(--line);
  display:flex;flex-direction:column;align-items:center;justify-content:center;line-height:1.3;font-size:13px;font-weight:700;color:var(--lav-ink);flex-shrink:0}
.evrow .evbody{flex:1;min-width:0}
.evrow .evttl{font-size:13px;font-weight:600}
.evrow .evmeta{font-size:11.5px;color:var(--ink-soft);margin-top:1px}
.evrow .evtag{font-size:10px;font-weight:600;padding:3px 8px;border-radius:999px;background:var(--surface);color:var(--sage-ink);white-space:nowrap;flex-shrink:0}
.evrow .evbtn{border:none;background:none;color:var(--ink-soft);font-size:15px;cursor:pointer;padding:4px 6px;flex-shrink:0}
.evrow .evbtn:hover{color:var(--ink)}
.hintdot{display:inline-flex;align-items:center;justify-content:center;width:15px;height:15px;border-radius:50%;
  background:var(--surface-2);color:var(--ink-soft);font-size:10px;font-weight:700;cursor:pointer;margin-right:6px;flex-shrink:0}
.hintdot.on{background:var(--lav);color:#fff}
.hintnote{display:none;margin-top:8px}
.hintnote.show{display:block}

.crm-wrap{color:var(--ink)}
.crm-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;gap:10px;flex-wrap:wrap}
.crm-top h1{font-size:19px;margin:0;font-weight:600}
.crm-actions{display:flex;gap:14px;align-items:center}
.crm-actions a{font-size:12.5px;font-weight:600;text-decoration:none;cursor:pointer}
.crm-actions a.primary{color:var(--lav-ink)}
.crm-actions a.secondary{color:var(--ink-soft)}
.crm-stats{display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap}
.crm-stat{background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:12px 18px;min-width:90px;box-shadow:var(--shadow)}
.crm-stat .l{font-size:11.5px;color:var(--ink-soft)}
.crm-stat .v{font-size:20px;font-weight:600;margin-top:2px}
.crm-stat .v.paid{color:var(--ok)}
.crm-stat .v.wait{color:var(--warn)}
.crm-toolbar{display:flex;gap:10px;align-items:center;margin-bottom:14px;flex-wrap:wrap}
.crm-search{flex:1;min-width:200px;position:relative}
.crm-search input{width:100%;font:inherit;font-size:13px;padding:9px 12px 9px 32px;border-radius:10px;border:1.3px solid var(--line);
  background:var(--surface);color:var(--ink)}
.crm-search::before{content:"";position:absolute;left:11px;top:50%;width:12px;height:12px;margin-top:-7px;border-radius:50%;
  border:1.6px solid var(--ink-soft);box-shadow:5px 5px 0 -3px var(--ink-soft);pointer-events:none}
.crm-chips{display:flex;gap:6px;flex-wrap:wrap}
.crm-chip{font-size:11.5px;font-weight:600;padding:7px 12px;border-radius:999px;border:1.3px solid var(--line);
  background:var(--surface);color:var(--ink-soft);cursor:pointer;white-space:nowrap;user-select:none}
.crm-chip.on{background:var(--lav);border-color:var(--lav);color:#fff}
.crm-add-btn{width:34px;height:34px;border-radius:50%;border:1.3px solid var(--lav);background:var(--surface);color:var(--lav-ink);
  font-size:19px;line-height:1;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;box-shadow:var(--shadow)}
.crm-add-btn:hover{background:var(--lav);color:#fff}
.crm-modal-overlay{display:none;position:fixed;inset:0;background:rgba(51,46,61,.45);z-index:50;align-items:center;justify-content:center;padding:20px}
.crm-modal-overlay.open{display:flex}
.crm-modal{background:var(--surface);border-radius:16px;padding:18px;max-width:440px;width:100%;box-shadow:var(--shadow);max-height:90vh;overflow:auto}
.crm-modal-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.crm-modal-head h3{font-size:15px;margin:0;font-weight:650}
.crm-modal-x{border:none;background:none;color:var(--ink-soft);font-size:15px;cursor:pointer;padding:4px}
.crm-addbody{display:flex;flex-wrap:wrap;gap:10px;align-items:end}
.crm-addbody label{display:flex;flex-direction:column;gap:4px;font-size:11.5px;color:var(--ink-soft)}
.crm-addbody input,.crm-addbody select{border:1.3px solid var(--sage);background:var(--surface);border-radius:8px;padding:7px 9px;font-size:13px;font-family:inherit}
.crm-addbody button{background:var(--lav);color:#fff;border:none;border-radius:8px;padding:8px 15px;font-size:13px;cursor:pointer}
.crm-bottom-actions{display:flex;justify-content:flex-end;margin-top:16px}
.crm-bottom-actions a{font-size:12.5px;font-weight:600;text-decoration:none;color:var(--lav-ink);cursor:pointer}
.crm-table{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--line);border-radius:14px;overflow:hidden;font-size:12.8px;box-shadow:var(--shadow)}
.crm-table th{text-align:left;padding:10px 12px;background:var(--surface-2);font-size:11.5px;font-weight:650;color:var(--ink-soft)}
.crm-table td{padding:10px 12px;border-top:1px solid var(--line);color:var(--ink-soft);vertical-align:middle}
.crm-table td.nm{color:var(--ink);font-weight:600}
.crm-table td.nm .em{display:block;font-weight:400;font-size:11.5px;color:var(--ink-soft);margin-top:1px}
.crm-badge{padding:3px 9px;border-radius:999px;font-size:11.5px;font-weight:600;white-space:nowrap}
.crm-badge.paid{background:var(--ok-bg);color:var(--ok)}
.crm-badge.wait{background:var(--warn-bg);color:var(--warn)}
.crm-note,.crm-amt{border:1.3px solid var(--sage);background:var(--surface);border-radius:8px;padding:5px 7px;font-size:12px;font-family:inherit;width:100%;min-width:90px}
.crm-amt{min-width:60px;font-variant-numeric:tabular-nums}
.crm-confirm{background:var(--lav);color:#fff;border:none;border-radius:8px;padding:6px 10px;font-size:11.5px;cursor:pointer;white-space:nowrap}
.crm-del{background:none;color:#c0564f;border:1px solid var(--line);border-radius:8px;padding:6px 9px;font-size:11.5px;cursor:pointer}
.crm-mobile-wrap{overflow-x:auto;border-radius:14px}

.mevrow{display:flex;align-items:center;gap:12px;background:var(--surface-2);border:1px solid var(--line);border-radius:12px;padding:10px 12px}
.mevrow .ph{width:40px;height:40px;border-radius:10px;flex-shrink:0}
.mevrow .b{flex:1;min-width:0}
.mevrow .b h4{font-size:13px;font-weight:600;margin:0}
.mevrow .b .dt{font-size:11.5px;color:var(--ink-soft);margin-top:1px}
.mevrow .evbtn{border:none;background:none;color:var(--ink-soft);font-size:15px;cursor:pointer;padding:4px 6px;flex-shrink:0}

.cchip{display:inline-flex;align-items:center;gap:7px;font-size:12.5px;font-weight:500;color:var(--ink);background:var(--surface-2);
  border:1.5px solid var(--line);border-radius:999px;padding:6px 13px 6px 8px;cursor:pointer;user-select:none}
.cchip.on{border-color:var(--ink)}
.cchip .cswatch{width:16px;height:16px;border-radius:50%;flex-shrink:0;border:1px solid rgba(0,0,0,.08)}

.nf-form{display:flex;flex-direction:column;gap:12px;margin-top:14px;padding:14px;background:var(--surface-2);border-radius:12px}
.nf-photo-row{display:flex;flex-direction:column;gap:5px;font-size:12px;color:var(--ink-soft)}
.nf-photo-row input[type=file]{font-size:12px;color:var(--ink-soft)}
.nf-color-acc{font-size:12.5px}
.nf-color-acc summary{cursor:pointer;color:var(--lav-ink);font-weight:600;padding:4px 0;list-style:none}
.nf-color-acc summary::-webkit-details-marker{display:none}
.nf-color-acc summary::before{content:"▸";display:inline-block;margin-right:5px;transition:transform .15s}
.nf-color-acc[open] summary::before{transform:rotate(90deg)}
.nf-swatches{display:flex;gap:10px;padding-top:8px;flex-wrap:wrap}
.nf-swatch{width:30px;height:30px;border-radius:50%;border:2px solid transparent;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.nf-swatch.on{border-color:var(--ink)}
.nf-swatch.on::after{content:"✓";color:#fff;font-size:13px;font-weight:700;text-shadow:0 0 2px rgba(0,0,0,.5)}
.nf-create-btn{align-self:flex-start;border:none;background:var(--sage);color:#fff;border-radius:10px;padding:9px 18px;font-weight:600;font-size:13px;cursor:pointer}
.nf-form .addtheme{margin-top:0}
.addtheme{display:flex;gap:8px;margin-top:14px}
.addtheme input{flex:1;font-family:inherit;font-size:13.5px;color:var(--ink);background:var(--surface);
  border:1.3px solid var(--sage);border-radius:10px;padding:9px 12px}
.addtheme button{border:none;background:var(--sage);color:#fff;border-radius:10px;padding:0 16px;font-weight:600;font-size:13px;cursor:pointer}

.dpicker{position:relative}
.dpicker-btn{width:100%;text-align:left;font-family:inherit;font-size:14px;color:var(--ink-soft);
  background:var(--surface);border:1.3px solid var(--sage);border-radius:12px;padding:11px 13px;cursor:pointer}
.dpicker-btn.has-val{color:var(--ink);font-weight:500}
.dpicker-btn::after{content:"";display:inline-block;float:right;width:7px;height:7px;border-right:1.5px solid var(--ink-soft);
  border-bottom:1.5px solid var(--ink-soft);transform:rotate(45deg) translateY(-2px)}
.dpicker-pop{position:absolute;z-index:20;top:calc(100% + 6px);left:0;width:260px;background:var(--surface);
  border:1px solid var(--line);border-radius:14px;box-shadow:var(--shadow);padding:14px}
.dpicker-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
.dpicker-label{font-size:13px;font-weight:600;text-transform:capitalize}
.dpicker-nav{border:none;background:var(--surface-2);color:var(--ink);width:26px;height:26px;border-radius:8px;
  font-size:14px;cursor:pointer;line-height:1}
.dpicker-wk{display:grid;grid-template-columns:repeat(7,1fr);gap:2px;margin-bottom:4px}
.dpicker-wk span{font-size:9.5px;text-align:center;color:var(--ink-soft);text-transform:uppercase}
.dpicker-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:3px}
.dp-day{display:flex;align-items:center;justify-content:center;aspect-ratio:1;border-radius:8px;font-size:12.5px;
  cursor:pointer;color:var(--ink)}
.dp-day:empty{cursor:default}
.dp-day:not(:empty):hover{background:var(--surface-2)}
.dp-day.on{background:var(--lav);color:#fff;font-weight:600}
</style></head><body>

<div class="shell" id="shell">
  <div class="top">
    <div class="mark">SM</div>
    <div class="name">Serenity Moon · админка</div>
    <a href="/admin/logout" class="logout">выйти</a>
  </div>
  <div class="tabs">
    <div class="tab" data-tab="t-progress">Участницы</div>
    <div class="tab active" data-tab="t-content">Материалы</div>
    <div class="tab" data-tab="t-calendar">Календарь</div>
    <div class="tab" data-tab="t-events">Мероприятия</div>
    <div class="tab" data-tab="t-participants">CRM</div>
  </div>

  <div id="t-progress" style="display:none">
    <div class="crm-wrap">
      <div class="crm-toolbar">
        <div class="crm-chips" id="progFilterChips">
          <div class="crm-chip on" data-f="all">все</div>
          <div class="crm-chip" data-f="quiet">молчат 3+ дня</div>
        </div>
      </div>
      <div class="crm-mobile-wrap">
        <table class="crm-table">
          <thead><tr><th>#</th><th>Имя</th><th>Email</th><th>Привычек</th><th>% выполнения</th><th>💎</th><th>Мастермайнды</th><th>Была в кабинете</th><th></th></tr></thead>
          <tbody id="progBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <div id="t-content">
    <div class="card">
      <h2>Новый материал</h2>
      <div class="hint">видео, аудио или pdf — папку и тему можно выбрать из уже существующих или создать прямо здесь</div>
      <div class="field">
        <label>Папка <span class="req">*</span></label>
        <div class="chipgrid" id="folderPicker"></div>
        <div class="nf-form" id="newFolderForm" style="display:none">
          <div class="addtheme">
            <input type="text" id="nfName" placeholder="Название папки">
            <input type="text" id="nfDesc" placeholder="Описание (необязательно)">
          </div>
          <div class="nf-photo-row">
            <span>Фото папки (необязательно)</span>
            <input type="file" id="nfPhoto" accept="image/*">
          </div>
          <details class="nf-color-acc">
            <summary>цвет / градиент, если без фото</summary>
            <div class="nf-swatches" id="nfColorPicker"></div>
          </details>
          <button id="nfCreate" class="nf-create-btn">Создать</button>
        </div>
      </div>
      <div class="field">
        <label>Название <span class="req">*</span></label>
        <input type="text" id="mTitle" placeholder="Например: Видео-практика дыхания">
      </div>
      <div class="field">
        <label>Формат <span class="req">*</span></label>
        <div class="chipgrid" id="mFormatPicker"></div>
      </div>
      <div class="field">
        <label>Дата открытия</label>
        <div class="dpicker" id="mDayWrap"></div>
      </div>
      <div class="field">
        <label>Тема <span class="req">*</span></label>
        <div class="chipgrid" id="mThemes"></div>
        <div class="addtheme" id="newThemeForm" style="display:none">
          <input type="text" id="newThemeInput" placeholder="Новая тема, например: йога">
          <button id="addThemeBtn">Добавить</button>
        </div>
        <div class="err" id="themeErr">выбери хотя бы одну тему — без неё материал не найти по фильтру</div>
      </div>
      <div class="field" id="evtLinkField">
        <div class="toggle-row">
          <span class="l"><span class="hintdot" id="evtHintDot">?</span>Привязать к эфиру календаря</span>
          <button class="sw" id="evtLinkToggle"></button>
        </div>
        <div class="note hintnote" id="evtHintNote">Включай, если это запись прошедшего онлайн-эфира (Zoom), а не изначально записанное видео/аудио</div>
        <div id="evtPickWrap" style="display:none;margin-top:10px">
          <select id="evtPick">
            <option>День 3 — Мастермайнд #1</option>
            <option>День 10 — Онлайн-практика дыхания</option>
            <option>День 15 — Мастермайнд #2</option>
          </select>
          <div class="note" style="margin-top:8px">после сохранения статус этого эфира в календаре сам сменится на «запись доступна»</div>
        </div>
      </div>
      <div class="field">
        <label>Файл или ссылка</label>
        <input type="file" id="mFile" hidden>
        <div class="filebox" id="mFileBox">перетащи файл сюда или <b>выбери на компьютере</b><br>либо вставь ссылку ниже</div>
        <input type="text" id="mLink" placeholder="https://..." style="margin-top:8px">
      </div>
      <button class="save" id="saveMaterial">Сохранить материал</button>
    </div>
  </div>

  <div id="t-calendar" style="display:none">
    <div class="card">
      <h2>Текущие эфиры марафона</h2>
      <div class="hint">видны участницам на страничках «Сегодня» и «Материалы»</div>
      <div class="evlist" id="evList"></div>
    </div>
    <div class="card">
      <h2>Добавить эфир</h2>
      <div class="hint">появится в списке выше и напоминанием в день эфира</div>
      <div class="field">
        <label>Дата <span class="req">*</span></label>
        <div class="dpicker" id="eDayWrap"></div>
      </div>
      <div class="field">
        <label>Название <span class="req">*</span></label>
        <input type="text" id="eTitle" placeholder="Например: Мастермайнд #3">
      </div>
      <div class="field">
        <label>Тип</label>
        <div class="chipgrid" id="typePicker"></div>
        <div class="addtheme" id="newTypeForm" style="display:none">
          <input type="text" id="ntName" placeholder="Новый тип, например: Прямой эфир">
          <button id="ntCreate">Добавить</button>
        </div>
      </div>
      <div class="field">
        <label>Время</label>
        <input type="text" id="eTime" placeholder="19:00 мск">
      </div>
      <div class="field">
        <label>Ссылка на Zoom</label>
        <input type="text" id="eLink" placeholder="https://zoom.us/...">
      </div>
      <button class="save" id="saveEvent">Добавить в календарь</button>
    </div>
  </div>

  <div id="t-events" style="display:none">
    <div class="card">
      <h2>Мероприятия Милы</h2>
      <div class="hint">не марафон — её другие ретриты/вебинары, показываются на странице «Профиль» у участниц</div>
      <div class="evlist" id="mevList"></div>
    </div>
    <div class="card">
      <h2>Добавить мероприятие</h2>
      <div class="field">
        <label>Название <span class="req">*</span></label>
        <input type="text" id="mevTitle" placeholder="Например: Ретрит выходного дня">
      </div>
      <div class="field">
        <label>Дата и место <span class="req">*</span></label>
        <input type="text" id="mevDate" placeholder="14-15 ноября · Durham, NC (или «онлайн»)">
      </div>
      <div class="field">
        <label>Ссылка «узнать больше»</label>
        <input type="text" id="mevLink" placeholder="https://...">
      </div>
      <div class="field">
        <label>Цвет плашки</label>
        <div class="chipgrid" id="mevColorPicker"></div>
      </div>
      <button class="save" id="saveEventMila">Добавить мероприятие</button>
    </div>
  </div>

  <div id="t-participants" style="display:none">
    <div class="crm-wrap">
      <div class="crm-top">
        <h1>Заявки марафона</h1>
        <button class="crm-add-btn" id="crmAddOpen" title="добавить вручную">+</button>
      </div>
      <div class="crm-stats">
        <div class="crm-stat"><div class="l">всего</div><div class="v" id="crmTotal">0</div></div>
        <div class="crm-stat"><div class="l">оплачено</div><div class="v paid" id="crmPaid">0</div></div>
        <div class="crm-stat"><div class="l">ждут</div><div class="v wait" id="crmWait">0</div></div>
      </div>
      <div class="crm-toolbar">
        <div class="crm-search"><input type="text" id="crmSearch" placeholder="имя, почта, телефон, телеграм…"></div>
        <div class="crm-chips" id="crmStatusChips">
          <div class="crm-chip on" data-status="all">все</div>
          <div class="crm-chip" data-status="оплачено">оплачено</div>
          <div class="crm-chip" data-status="ждёт">ждут</div>
        </div>
        <div class="crm-chips" id="crmMethodChips">
          <div class="crm-chip on" data-method="all">все способы</div>
          <div class="crm-chip" data-method="venmo">venmo</div>
          <div class="crm-chip" data-method="zelle">zelle</div>
          <div class="crm-chip" data-method="stripe">stripe</div>
        </div>
      </div>
      <div class="crm-mobile-wrap">
        <table class="crm-table">
          <thead><tr><th>#</th><th>Имя</th><th>Телефон</th><th>Телеграм</th><th>Способ</th><th>Сумма</th><th>Статус</th><th>Дата</th><th>Заметка</th><th></th></tr></thead>
          <tbody id="crmBody"></tbody>
        </table>
      </div>
      <div class="crm-bottom-actions">
        <a href="/admin/export.csv" class="primary" target="_blank">скачать CSV</a>
      </div>
    </div>
  </div>
</div>

<div class="crm-modal-overlay" id="crmAddOverlay">
  <div class="crm-modal">
    <div class="crm-modal-head"><h3>Добавить вручную</h3><button class="crm-modal-x" id="crmAddClose">✕</button></div>
    <div class="crm-addbody">
      <label>Имя<input type="text" id="caName"></label>
      <label>Email <span style="color:#B3415A">*</span><input type="text" id="caEmail"></label>
      <label>Телефон<input type="text" id="caPhone"></label>
      <label>Телеграм<input type="text" id="caTg" placeholder="@username"></label>
      <label>Способ<select id="caMethod"><option value="venmo">venmo</option><option value="zelle">zelle</option><option value="stripe">stripe</option></select></label>
      <label>Статус<select id="caStatus"><option value="ждёт">ждёт</option><option value="оплачено">оплачено</option></select></label>
      <label>Сумма<input type="text" id="caAmount" placeholder="33" style="width:70px"></label>
      <label>Заметка<input type="text" id="caNote"></label>
      <button id="caAdd">Добавить</button>
    </div>
  </div>
</div>

<div class="toast" id="toast">Сохранено ✓</div>

<script>
const TABS = ["t-progress","t-content","t-calendar","t-events","t-participants"];
document.querySelectorAll(".tab").forEach(t=>{
  t.addEventListener("click", ()=>{
    document.querySelectorAll(".tab").forEach(x=>x.classList.remove("active"));
    t.classList.add("active");
    TABS.forEach(id=>{ document.getElementById(id).style.display = (id===t.dataset.tab) ? "" : "none"; });
    document.getElementById("shell").classList.toggle("wide", t.dataset.tab==="t-participants" || t.dataset.tab==="t-progress");
    if(t.dataset.tab==="t-participants") loadParticipants();
    if(t.dataset.tab==="t-progress") loadProgress();
  });
});

let PARTICIPANT_PROGRESS = [];
let progressLoaded = false;
let progressFilter = "all";
function fmtLastActive(iso){
  if(!iso) return "ни разу";
  const days = Math.floor((Date.now() - new Date(iso+"T00:00:00Z").getTime()) / 86400000);
  if(days <= 0) return "сегодня";
  if(days === 1) return "вчера";
  return days+" дн. назад";
}
function isQuiet(iso){
  if(!iso) return true;
  const days = Math.floor((Date.now() - new Date(iso+"T00:00:00Z").getTime()) / 86400000);
  return days >= 3;
}
function renderProgress(){
  const quiet = PARTICIPANT_PROGRESS.filter(p=>isQuiet(p.last_active));
  const rows = progressFilter === "quiet" ? quiet : PARTICIPANT_PROGRESS;
  document.getElementById("progBody").innerHTML = rows.length ? rows.map((p,i)=>`<tr>
    <td>${i+1}</td>
    <td>${p.name || "—"}</td>
    <td>${p.email || "—"}</td>
    <td>${p.habit_count}</td>
    <td>${p.avg_pct}%</td>
    <td>${p.perfect_days + p.mastermind_count*5}</td>
    <td>${p.mastermind_count}/2</td>
    <td${isQuiet(p.last_active)?' style="color:#B3415A;font-weight:600"':''}>${fmtLastActive(p.last_active)}</td>
    <td><button class="crm-del" data-pdel="${p.id}" title="удалить участницу">✕</button></td>
  </tr>`).join("") : `<tr><td colspan="9" style="text-align:center;color:#999">пока никто не зарегистрировался в кабинете</td></tr>`;
  document.querySelectorAll("[data-pdel]").forEach(btn=>{
    btn.addEventListener("click", async ()=>{
      if(!confirm("Удалить эту участницу из кабинета? Это сотрёт её привычки и отметки, вернуть будет нельзя.")) return;
      btn.disabled = true;
      try{
        await apiFetch("/api/admin/participants/"+btn.dataset.pdel, {method:"DELETE"});
        PARTICIPANT_PROGRESS = PARTICIPANT_PROGRESS.filter(p=>p.id !== btn.dataset.pdel);
        renderProgress();
        showToast("Участница удалена");
      }catch(e){ btn.disabled = false; }
    });
  });
}
async function loadProgress(){
  try{ PARTICIPANT_PROGRESS = await apiFetch("/api/admin/participants"); }catch(e){ PARTICIPANT_PROGRESS = []; }
  progressLoaded = true;
  renderProgress();
}
document.getElementById("progFilterChips").addEventListener("click", e=>{
  const chip = e.target.closest(".crm-chip"); if(!chip) return;
  document.querySelectorAll("#progFilterChips .crm-chip").forEach(c=>c.classList.remove("on"));
  chip.classList.add("on");
  progressFilter = chip.dataset.f;
  renderProgress();
});

function showToast(msg){
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.classList.add("show");
  setTimeout(()=>t.classList.remove("show"), 2200);
}

let THEMES = []; // [{id,label}] — грузится с сервера, см. loadThemes()
function renderMaterialThemes(){
  const wrap = document.getElementById("mThemes");
  wrap.innerHTML = THEMES.map(t=>`<div class="tchip" data-t="${t.id}">${t.label}</div>`).join("")
    + `<div class="tchip" id="themePlusChip">+ своя тема</div>`;
  wrap.querySelectorAll(".tchip").forEach(c=>{
    if(c.id === "themePlusChip") return;
    c.addEventListener("click", ()=>{ c.classList.toggle("on"); document.getElementById("themeErr").style.display="none"; });
  });
  document.getElementById("themePlusChip").addEventListener("click", ()=>{
    const f = document.getElementById("newThemeForm");
    f.style.display = (f.style.display==="none") ? "flex" : "none";
    if(f.style.display==="flex") document.getElementById("newThemeInput").focus();
  });
}
async function loadThemes(){
  try{ THEMES = await apiFetch("/api/admin/themes"); }catch(e){ THEMES = []; }
  renderMaterialThemes();
}
document.getElementById("addThemeBtn").addEventListener("click", async ()=>{
  const inp = document.getElementById("newThemeInput");
  const v = inp.value.trim().toLowerCase();
  if(!v || THEMES.some(t=>t.id===v)){ inp.focus(); return; }
  try{
    const t = await apiFetch("/api/admin/themes", {method:"POST", body: JSON.stringify({label: v})});
    THEMES.push(t);
    inp.value = "";
    document.getElementById("newThemeForm").style.display = "none";
    renderMaterialThemes();
    document.querySelector(`#mThemes .tchip[data-t="${t.id}"]`).classList.add("on");
    document.getElementById("themeErr").style.display="none";
    showToast("Тема «"+v+"» добавлена ✓");
  }catch(e){}
});
loadThemes();

function buildDatePicker(baseId, startMonth){
  const wrap = document.getElementById(baseId+"Wrap");
  wrap.innerHTML = `<button type="button" class="dpicker-btn" id="${baseId}Btn">Выбрать дату</button>
    <div class="dpicker-pop" id="${baseId}Pop" style="display:none">
      <div class="dpicker-head">
        <button type="button" class="dpicker-nav" data-dir="-1">‹</button>
        <span class="dpicker-label" id="${baseId}Label"></span>
        <button type="button" class="dpicker-nav" data-dir="1">›</button>
      </div>
      <div class="dpicker-wk"><span>пн</span><span>вт</span><span>ср</span><span>чт</span><span>пт</span><span>сб</span><span>вс</span></div>
      <div class="dpicker-grid" id="${baseId}Grid"></div>
    </div>
    <input type="hidden" id="${baseId}">`;
  let view = new Date(startMonth+"T00:00:00");
  let selected = null;
  const btn = document.getElementById(baseId+"Btn");
  const pop = document.getElementById(baseId+"Pop");
  const label = document.getElementById(baseId+"Label");
  const grid = document.getElementById(baseId+"Grid");
  const hidden = document.getElementById(baseId);
  function render(){
    label.textContent = view.toLocaleDateString("ru-RU",{month:"long",year:"numeric"});
    const y = view.getFullYear(), m = view.getMonth();
    let startOffset = (new Date(y,m,1).getDay()+6)%7;
    const daysInMonth = new Date(y,m+1,0).getDate();
    let html = "";
    for(let i=0;i<startOffset;i++) html += `<span class="dp-day"></span>`;
    for(let d=1; d<=daysInMonth; d++){
      const iso = `${y}-${String(m+1).padStart(2,"0")}-${String(d).padStart(2,"0")}`;
      html += `<span class="dp-day${iso===selected?" on":""}" data-iso="${iso}">${d}</span>`;
    }
    grid.innerHTML = html;
    grid.querySelectorAll(".dp-day[data-iso]").forEach(el=>{
      el.addEventListener("click", ()=>{
        selected = el.dataset.iso;
        hidden.value = selected;
        const dt = new Date(selected+"T00:00:00");
        btn.textContent = dt.toLocaleDateString("ru-RU",{day:"numeric",month:"short",year:"numeric"});
        btn.classList.add("has-val");
        pop.style.display = "none";
        render();
      });
    });
  }
  btn.addEventListener("click", (e)=>{
    e.stopPropagation();
    document.querySelectorAll(".dpicker-pop").forEach(p=>{ if(p!==pop) p.style.display="none"; });
    pop.style.display = (pop.style.display==="none") ? "block" : "none";
  });
  pop.querySelectorAll(".dpicker-nav").forEach(b=>{
    b.addEventListener("click", (e)=>{
      e.stopPropagation();
      view.setMonth(view.getMonth() + (+b.dataset.dir));
      render();
    });
  });
  pop.addEventListener("click", (e)=>e.stopPropagation());
  render();
  return { reset(){ selected=null; hidden.value=""; btn.textContent="Выбрать дату"; btn.classList.remove("has-val"); render(); } };
}
document.addEventListener("click", ()=>{ document.querySelectorAll(".dpicker-pop").forEach(p=>p.style.display="none"); });
const mDayPicker = buildDatePicker("mDay", "2026-11-01");
const eDayPicker = buildDatePicker("eDay", "2026-11-01");

const FORMATS = [{v:"video",l:"Видео"},{v:"audio",l:"Аудио"},{v:"pdf",l:"PDF"},{v:"link",l:"Ссылка"},{v:"photo",l:"Фото"}];
let selectedFormat = FORMATS[0].v;
function renderFormatPicker(){
  const wrap = document.getElementById("mFormatPicker");
  wrap.innerHTML = FORMATS.map(f=>`<div class="tchip${f.v===selectedFormat?" on":""}" data-f="${f.v}">${f.l}</div>`).join("");
  wrap.querySelectorAll(".tchip").forEach(c=>{
    c.addEventListener("click", ()=>{ selectedFormat = c.dataset.f; renderFormatPicker(); });
  });
}
renderFormatPicker();

let FOLDERS = []; // [{id,name,description,topics,photo,color}] — грузится с сервера
let selectedFolder = null;
const NF_COLORS = [
  {v:"lav", l:"Лавандовый", css:"var(--lav)"},
  {v:"sage", l:"Шалфейный", css:"var(--sage)"},
  {v:"sand", l:"Песочный", css:"var(--sand)"},
  {v:"grad1", l:"Градиент 1", css:"linear-gradient(135deg,var(--sand),var(--lav))"},
  {v:"grad2", l:"Градиент 2", css:"linear-gradient(135deg,var(--lav),var(--sage))"}
];
let selectedNfColor = NF_COLORS[0].v;
function renderNfColorPicker(){
  const wrap = document.getElementById("nfColorPicker");
  wrap.innerHTML = NF_COLORS.map(c=>`<div class="nf-swatch${c.v===selectedNfColor?" on":""}" data-c="${c.v}" title="${c.l}" style="background:${c.css}"></div>`).join("");
  wrap.querySelectorAll(".nf-swatch").forEach(el=>{
    el.addEventListener("click", ()=>{ selectedNfColor = el.dataset.c; renderNfColorPicker(); });
  });
}
renderNfColorPicker();
function readFileAsDataUrl(file){
  return new Promise((resolve,reject)=>{
    const r = new FileReader();
    r.onload = ()=>resolve(r.result);
    r.onerror = reject;
    r.readAsDataURL(file);
  });
}
function renderFolderPicker(){
  const wrap = document.getElementById("folderPicker");
  const addingNew = document.getElementById("newFolderForm") && document.getElementById("newFolderForm").style.display==="flex";
  wrap.innerHTML = FOLDERS.map(f=>`<div class="tchip${(!addingNew && f.id===selectedFolder)?" on":""}" data-f="${f.id}">${f.name}</div>`).join("")
    + `<div class="tchip${addingNew?" on":""}" id="folderPlusChip">+ новая папка</div>`;
  wrap.querySelectorAll(".tchip[data-f]").forEach(c=>{
    c.addEventListener("click", ()=>{
      selectedFolder = c.dataset.f;
      document.getElementById("newFolderForm").style.display = "none";
      renderFolderPicker();
    });
  });
  document.getElementById("folderPlusChip").addEventListener("click", ()=>{
    const f = document.getElementById("newFolderForm");
    f.style.display = (f.style.display==="none") ? "flex" : "none";
    renderFolderPicker();
    if(f.style.display==="flex") document.getElementById("nfName").focus();
  });
}
async function loadFolders(){
  try{ FOLDERS = await apiFetch("/api/admin/folders"); }catch(e){ FOLDERS = []; }
  renderFolderPicker();
}
document.getElementById("nfCreate").addEventListener("click", async ()=>{
  const name = document.getElementById("nfName").value.trim();
  if(!name || FOLDERS.some(f=>f.name===name)){ document.getElementById("nfName").focus(); return; }
  const photoFile = document.getElementById("nfPhoto").files[0];
  const photo = photoFile ? await readFileAsDataUrl(photoFile) : null;
  try{
    const f = await apiFetch("/api/admin/folders", {method:"POST", body: JSON.stringify({
      name, description: document.getElementById("nfDesc").value.trim(),
      photo, color: photo ? "" : selectedNfColor
    })});
    FOLDERS.push(f);
    selectedFolder = f.id;
    document.getElementById("nfName").value = "";
    document.getElementById("nfDesc").value = "";
    document.getElementById("nfPhoto").value = "";
    selectedNfColor = NF_COLORS[0].v;
    renderNfColorPicker();
    document.getElementById("newFolderForm").style.display = "none";
    renderFolderPicker();
    showToast("Папка «"+name+"» создана ✓");
  }catch(e){}
});
loadFolders();

document.getElementById("evtHintDot").addEventListener("click", ()=>{
  document.getElementById("evtHintDot").classList.toggle("on");
  document.getElementById("evtHintNote").classList.toggle("show");
});
const evtToggle = document.getElementById("evtLinkToggle");
evtToggle.addEventListener("click", ()=>{
  evtToggle.classList.toggle("on");
  document.getElementById("evtPickWrap").style.display = evtToggle.classList.contains("on") ? "" : "none";
});

const MARATHON_START_ISO = "2026-11-09"; // держать в паре с MARATHON_START в app.py
function isoDateToMarathonDay(iso){
  if(!iso) return null;
  const a = new Date(MARATHON_START_ISO+"T00:00:00"), b = new Date(iso+"T00:00:00");
  return Math.round((b-a)/86400000) + 1;
}

async function uploadMaterialFile(file){
  const box = document.getElementById("mFileBox");
  box.textContent = "загружаю «" + file.name + "»…";
  const fd = new FormData();
  fd.append("file", file, file.name);
  try{
    const res = await fetch("/api/admin/materials/upload", {method:"POST", body: fd, credentials:"same-origin"});
    if(!res.ok) throw new Error("http "+res.status);
    const data = await res.json();
    document.getElementById("mLink").value = data.url;
    box.innerHTML = "файл загружен: <b>" + file.name + "</b> (заменить — выбери другой файл)";
    showToast("Файл «"+file.name+"» загружен ✓");
  }catch(e){
    box.innerHTML = "не получилось загрузить файл — перетащи файл сюда или <b>выбери на компьютере</b><br>либо вставь ссылку ниже";
    showToast("ошибка загрузки файла");
  }
}
document.getElementById("mFileBox").addEventListener("click", ()=>document.getElementById("mFile").click());
document.getElementById("mFile").addEventListener("change", e=>{
  if(e.target.files[0]) uploadMaterialFile(e.target.files[0]);
});
document.getElementById("mFileBox").addEventListener("dragover", e=>{ e.preventDefault(); });
document.getElementById("mFileBox").addEventListener("drop", e=>{
  e.preventDefault();
  if(e.dataTransfer.files[0]) uploadMaterialFile(e.dataTransfer.files[0]);
});
document.getElementById("saveMaterial").addEventListener("click", async ()=>{
  const title = document.getElementById("mTitle").value.trim();
  const themeIds = [...document.querySelectorAll("#mThemes .tchip.on")].map(c=>c.dataset.t);
  if(!selectedFolder){ document.getElementById("folderPicker").scrollIntoView({block:"center"}); return; }
  if(!themeIds.length){ document.getElementById("themeErr").style.display = "block"; return; }
  if(!title){ document.getElementById("mTitle").focus(); return; }
  const evOn = evtToggle.classList.contains("on");
  try{
    await apiFetch("/api/admin/materials", {method:"POST", body: JSON.stringify({
      folder_id: selectedFolder, title, format: selectedFormat,
      day: isoDateToMarathonDay(document.getElementById("mDay").value),
      tags: themeIds, url: document.getElementById("mLink").value.trim() || "#",
      calendar_event_id: evOn ? (document.getElementById("evtPick").value || null) : null,
    })});
    showToast("Материал «"+title+"» сохранён ✓");
    document.getElementById("mTitle").value = "";
    document.getElementById("mLink").value = "";
    document.getElementById("mFile").value = "";
    document.getElementById("mFileBox").innerHTML = "перетащи файл сюда или <b>выбери на компьютере</b><br>либо вставь ссылку ниже";
    mDayPicker.reset();
    document.querySelectorAll("#mThemes .tchip.on").forEach(c=>c.classList.remove("on"));
  }catch(e){}
});

let EVENT_TYPES = ["Мастермайнд","Онлайн-практика","Эфир с гостем"];
let selectedType = EVENT_TYPES[0];
function renderTypePicker(){
  const wrap = document.getElementById("typePicker");
  wrap.innerHTML = EVENT_TYPES.map(t=>`<div class="tchip${t===selectedType?" on":""}" data-ty="${t}">${t}</div>`).join("")
    + `<div class="tchip" id="typePlusChip">+ новый тип</div>`;
  wrap.querySelectorAll(".tchip[data-ty]").forEach(c=>{
    c.addEventListener("click", ()=>{
      selectedType = c.dataset.ty;
      document.getElementById("newTypeForm").style.display = "none";
      renderTypePicker();
    });
  });
  document.getElementById("typePlusChip").addEventListener("click", ()=>{
    const f = document.getElementById("newTypeForm");
    f.style.display = (f.style.display==="none") ? "flex" : "none";
    if(f.style.display==="flex") document.getElementById("ntName").focus();
  });
}
renderTypePicker();
document.getElementById("ntCreate").addEventListener("click", ()=>{
  const name = document.getElementById("ntName").value.trim();
  if(!name || EVENT_TYPES.includes(name)){ document.getElementById("ntName").focus(); return; }
  EVENT_TYPES.push(name);
  selectedType = name;
  document.getElementById("ntName").value = "";
  document.getElementById("newTypeForm").style.display = "none";
  renderTypePicker();
  showToast("Тип «"+name+"» добавлен ✓");
});

const MONTHS_SHORT = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"];
let EVENTS = []; // [{id,date,title,type,time,link}] — грузится с сервера
function fmtDate(iso){
  const d = new Date(iso+"T00:00:00");
  return {num:d.getDate(), lbl:MONTHS_SHORT[d.getMonth()]};
}
function renderEvents(){
  const list = document.getElementById("evList");
  const todayStr = new Date().toISOString().slice(0,10);
  list.innerHTML = EVENTS.length ? EVENTS.slice().sort((a,b)=>a.date.localeCompare(b.date)).map((e)=>{
    const d = fmtDate(e.date);
    const past = e.date < todayStr;
    return `
    <div class="evrow${past?' past':''}">
      <div class="evday">${d.num}<br><span style="font-size:8px;font-weight:600;text-transform:uppercase">${d.lbl}</span></div>
      <div class="evbody"><div class="evttl">${e.title}</div><div class="evmeta">${e.time||""}${past?' · прошёл':''}</div></div>
      <div class="evtag">${e.type||""}</div>
      <button class="evbtn" data-del="${e.id}">✕</button>
    </div>`;
  }).join("") : `<div class="hint" style="margin:0">пока нет эфиров</div>`;
  list.querySelectorAll("[data-del]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      try{
        await apiFetch(`/api/admin/calendar-events/${b.dataset.del}`, {method:"DELETE"});
        EVENTS = EVENTS.filter(e=>String(e.id)!==b.dataset.del);
        renderEvents();
        renderEvtPick();
        showToast("Эфир удалён");
      }catch(e){}
    });
  });
}
function renderEvtPick(){
  const sel = document.getElementById("evtPick");
  sel.innerHTML = EVENTS.length
    ? EVENTS.slice().sort((a,b)=>a.date.localeCompare(b.date)).map(e=>{
        const d = fmtDate(e.date);
        return `<option value="${e.id}">${d.num} ${d.lbl} — ${e.title}</option>`;
      }).join("")
    : `<option value="">пока нет эфиров — сначала добавь в "Календарь"</option>`;
}
async function loadCalendarEvents(){
  try{ EVENTS = await apiFetch("/api/admin/calendar-events"); }catch(e){ EVENTS = []; }
  renderEvents();
  renderEvtPick();
}
loadCalendarEvents();
document.getElementById("saveEvent").addEventListener("click", async ()=>{
  const eventDate = document.getElementById("eDay").value;
  const title = document.getElementById("eTitle").value.trim();
  if(!eventDate){ document.getElementById("eDayWrap").scrollIntoView({block:"center"}); return; }
  if(!title){ document.getElementById("eTitle").focus(); return; }
  try{
    await apiFetch("/api/admin/calendar-events", {method:"POST", body: JSON.stringify({
      date: eventDate, title, type: selectedType,
      time: document.getElementById("eTime").value.trim() || "19:00 мск",
      link: document.getElementById("eLink").value.trim(),
    })});
    await loadCalendarEvents();
    showToast("Эфир «"+title+"» добавлен в календарь ✓");
    eDayPicker.reset();
    document.getElementById("eTitle").value = "";
    document.getElementById("eLink").value = "";
  }catch(e){}
});

const MEV_COLORS = [
  {v:"lav", l:"Лавандовый", css:"var(--lav)"},
  {v:"sage", l:"Шалфейный", css:"var(--sage)"},
  {v:"sand", l:"Песочный", css:"var(--sand)"},
  {v:"grad1", l:"Градиент 1", css:"linear-gradient(135deg,var(--sand),var(--lav))"},
  {v:"grad2", l:"Градиент 2", css:"linear-gradient(135deg,var(--lav),var(--sage))"}
];
let selectedMevColor = MEV_COLORS[0].v;
function renderMevColorPicker(){
  const wrap = document.getElementById("mevColorPicker");
  wrap.innerHTML = MEV_COLORS.map(c=>`<div class="cchip${c.v===selectedMevColor?" on":""}" data-c="${c.v}">
    <span class="cswatch" style="background:${c.css}"></span>${c.l}</div>`).join("");
  wrap.querySelectorAll(".cchip").forEach(el=>{
    el.addEventListener("click", ()=>{ selectedMevColor = el.dataset.c; renderMevColorPicker(); });
  });
}
renderMevColorPicker();

let MILA_EVENTS = []; // [{id,title,date_place,link,color}] — грузится с сервера
function renderMevList(){
  const wrap = document.getElementById("mevList");
  wrap.innerHTML = MILA_EVENTS.length ? MILA_EVENTS.map((e)=>{
    const c = MEV_COLORS.find(x=>x.v===e.color) || MEV_COLORS[0];
    return `<div class="mevrow">
      <div class="ph" style="background:${c.css}"></div>
      <div class="b"><h4>${e.title}</h4><div class="dt">${e.date_place}</div></div>
      <button class="evbtn" data-del="${e.id}">✕</button>
    </div>`;
  }).join("") : `<div class="hint" style="margin:0">пока нет мероприятий</div>`;
  wrap.querySelectorAll("[data-del]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      try{
        await apiFetch(`/api/admin/mila-events/${b.dataset.del}`, {method:"DELETE"});
        MILA_EVENTS = MILA_EVENTS.filter(e=>String(e.id)!==b.dataset.del);
        renderMevList();
        showToast("Мероприятие удалено");
      }catch(e){}
    });
  });
}
async function loadMilaEvents(){
  try{ MILA_EVENTS = await apiFetch("/api/admin/mila-events"); }catch(e){ MILA_EVENTS = []; }
  renderMevList();
}
loadMilaEvents();
document.getElementById("saveEventMila").addEventListener("click", async ()=>{
  const title = document.getElementById("mevTitle").value.trim();
  const datePlace = document.getElementById("mevDate").value.trim();
  if(!title){ document.getElementById("mevTitle").focus(); return; }
  if(!datePlace){ document.getElementById("mevDate").focus(); return; }
  try{
    await apiFetch("/api/admin/mila-events", {method:"POST", body: JSON.stringify({
      title, date_place: datePlace,
      link: document.getElementById("mevLink").value.trim() || "#", color: selectedMevColor,
    })});
    await loadMilaEvents();
    showToast("Мероприятие «"+title+"» добавлено ✓");
    document.getElementById("mevTitle").value = "";
    document.getElementById("mevDate").value = "";
    document.getElementById("mevLink").value = "";
  }catch(e){}
});

/* ---- CRM: реальные данные с /api/admin/* на этом же сервере — та же cookie-сессия,
   что открыла эту страницу, никаких токенов/паролей в js не нужно ---- */
let PARTICIPANTS = [];
let crmLoaded = false;

async function apiFetch(path, opts){
  opts = opts || {};
  const res = await fetch(path, Object.assign({}, opts, {
    headers: Object.assign({"Content-Type":"application/json"}, opts.headers || {})
  }));
  if(res.status === 401){
    window.location.href = "/admin/login";
    throw new Error("unauthorized");
  }
  if(!res.ok){
    showToast("ошибка сервера: " + res.status);
    throw new Error("http " + res.status);
  }
  return res.status === 204 ? null : res.json();
}

function fmtDT(ts){
  if(!ts) return "—";
  const d = new Date(ts * 1000);
  return d.toLocaleDateString("ru-RU",{day:"2-digit",month:"2-digit"}) + " " + d.toLocaleTimeString("ru-RU",{hour:"2-digit",minute:"2-digit"});
}

async function loadParticipants(){
  const body = document.getElementById("crmBody");
  body.innerHTML = `<tr><td colspan="9" style="padding:24px;text-align:center;color:var(--ink-soft)">загрузка…</td></tr>`;
  try{
    PARTICIPANTS = await apiFetch("/api/admin/signups");
    crmLoaded = true;
    renderParticipants();
  }catch(e){
    if(crmLoaded === false) body.innerHTML = `<tr><td colspan="9" style="padding:24px;text-align:center;color:var(--ink-soft)">не удалось загрузить</td></tr>`;
  }
}

let crmStatus = "all";
let crmMethod = "all";

function renderParticipants(){
  document.getElementById("crmTotal").textContent = PARTICIPANTS.length;
  document.getElementById("crmPaid").textContent = PARTICIPANTS.filter(p=>p.status==="оплачено").length;
  document.getElementById("crmWait").textContent = PARTICIPANTS.filter(p=>p.status!=="оплачено").length;
  const q = document.getElementById("crmSearch").value.trim().toLowerCase();
  const shown = PARTICIPANTS.filter(p=>{
    if(crmStatus!=="all" && p.status!==crmStatus) return false;
    if(crmMethod!=="all" && p.method!==crmMethod) return false;
    if(!q) return true;
    return [p.name,p.email,p.phone,p.instagram].some(v=>(v||"").toLowerCase().includes(q));
  });
  const body = document.getElementById("crmBody");
  body.innerHTML = shown.length ? shown.map((p,i)=>{
    const paid = p.status === "оплачено";
    const badge = paid ? `<span class="crm-badge paid">оплачено</span>` : `<span class="crm-badge wait">ждёт</span>`;
    const confirmBtn = (paid || p.method==="stripe") ? "" : `<button class="crm-confirm" data-pay="${p.id}">Отметить оплаченным</button>`;
    return `<tr>
      <td>${i+1}</td>
      <td class="nm">${p.name||"—"}<span class="em">${p.email||""}</span></td>
      <td>${p.phone||"—"}</td>
      <td>${p.instagram||"—"}</td>
      <td>${p.method||"—"}</td>
      <td><input class="crm-amt" data-amount="${p.id}" value="${p.amount!=null?p.amount:""}" placeholder="—" inputmode="decimal"></td>
      <td>${badge}</td>
      <td>${fmtDT(p.confirmed || p.created)}</td>
      <td><input class="crm-note" data-note="${p.id}" value="${(p.note||"").replace(/"/g,"&quot;")}" placeholder="заметка…"></td>
      <td style="text-align:right;white-space:nowrap">${confirmBtn} <button class="crm-del" data-del="${p.id}">удалить</button></td>
    </tr>`;
  }).join("") : `<tr><td colspan="10" style="padding:24px;text-align:center;color:var(--ink-soft)">ничего не нашлось</td></tr>`;

  body.querySelectorAll("[data-pay]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      b.disabled = true;
      try{
        const row = await apiFetch(`/api/admin/signups/${b.dataset.pay}/confirm`, {method:"POST"});
        const i = PARTICIPANTS.findIndex(p=>p.id===row.id);
        if(i>-1) PARTICIPANTS[i] = row;
        renderParticipants();
        showToast((row.name||row.email)+" отмечена оплаченной — приглашение в канал отправлено");
      }catch(e){ b.disabled = false; }
    });
  });
  body.querySelectorAll("[data-del]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      if(!confirm("Удалить эту заявку?")) return;
      try{
        await apiFetch(`/api/admin/signups/${b.dataset.del}`, {method:"DELETE"});
        PARTICIPANTS = PARTICIPANTS.filter(p=>String(p.id)!==b.dataset.del);
        renderParticipants();
      }catch(e){}
    });
  });
  body.querySelectorAll("[data-note]").forEach(inp=>{
    inp.addEventListener("change", async ()=>{
      try{ await apiFetch(`/api/admin/signups/${inp.dataset.note}/note`, {method:"POST", body: JSON.stringify({note: inp.value})}); }
      catch(e){}
    });
  });
  body.querySelectorAll("[data-amount]").forEach(inp=>{
    inp.addEventListener("change", async ()=>{
      const v = inp.value.trim().replace(",",".");
      try{
        await apiFetch(`/api/admin/signups/${inp.dataset.amount}/note`, {method:"POST", body: JSON.stringify({amount: v === "" ? null : v})});
        const p = PARTICIPANTS.find(p=>String(p.id)===inp.dataset.amount);
        if(p) p.amount = v === "" ? null : parseFloat(v);
      }catch(e){}
    });
  });
}
document.getElementById("crmSearch").addEventListener("input", renderParticipants);
document.querySelectorAll("#crmStatusChips .crm-chip").forEach(c=>{
  c.addEventListener("click", ()=>{
    document.querySelectorAll("#crmStatusChips .crm-chip").forEach(x=>x.classList.remove("on"));
    c.classList.add("on");
    crmStatus = c.dataset.status;
    renderParticipants();
  });
});
document.querySelectorAll("#crmMethodChips .crm-chip").forEach(c=>{
  c.addEventListener("click", ()=>{
    document.querySelectorAll("#crmMethodChips .crm-chip").forEach(x=>x.classList.remove("on"));
    c.classList.add("on");
    crmMethod = c.dataset.method;
    renderParticipants();
  });
});

document.getElementById("caAdd").addEventListener("click", async ()=>{
  const email = document.getElementById("caEmail").value.trim();
  if(!email){ document.getElementById("caEmail").focus(); return; }
  try{
    const row = await apiFetch("/api/admin/signups", {method:"POST", body: JSON.stringify({
      name: document.getElementById("caName").value.trim(),
      email,
      phone: document.getElementById("caPhone").value.trim(),
      instagram: document.getElementById("caTg").value.trim(),
      method: document.getElementById("caMethod").value,
      status: document.getElementById("caStatus").value,
      amount: document.getElementById("caAmount").value.trim() || null,
      note: document.getElementById("caNote").value.trim()
    })});
    PARTICIPANTS.unshift(row);
    renderParticipants();
    document.getElementById("caName").value = "";
    document.getElementById("caEmail").value = "";
    document.getElementById("caPhone").value = "";
    document.getElementById("caTg").value = "";
    document.getElementById("caAmount").value = "";
    document.getElementById("caNote").value = "";
    document.getElementById("crmAddOverlay").classList.remove("open");
    showToast("Заявка добавлена ✓");
  }catch(e){}
});

document.getElementById("crmAddOpen").addEventListener("click", ()=>{ document.getElementById("crmAddOverlay").classList.add("open"); });
document.getElementById("crmAddClose").addEventListener("click", ()=>{ document.getElementById("crmAddOverlay").classList.remove("open"); });
document.getElementById("crmAddOverlay").addEventListener("click", e=>{ if(e.target.id==="crmAddOverlay") e.currentTarget.classList.remove("open"); });
</script>
</body></html>"""


# ---------- обработчики ----------
async def register(request):
    """Форма Venmo/Zelle с лендинга: name, email, phone, ig, pay, msg (реальные поля сайта)."""
    data = await request.post()
    name = str(data.get("name", "")).strip()
    email = str(data.get("email", "")).strip()
    phone = str(data.get("phone", "")).strip()
    instagram = str(data.get("ig", "")).strip()
    comment = str(data.get("msg", "")).strip()
    method = str(data.get("pay", "")).strip().lower()
    if method not in ("venmo", "zelle") or not email:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    await DB_POOL.execute(
        "INSERT INTO signups (name, email, email_norm, instagram, phone, comment, method, status, created) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        name, email, normalize_email(email), instagram, phone, comment, method, "ждёт", time.time(),
    )
    try:
        await send_payment_qr_email(email, name, method)
    except Exception as e:
        print(f"[ПРЕДУПРЕЖДЕНИЕ] не удалось отправить дубль qr на почту: {e}")
    return web.json_response({"ok": True})


# ---------- auth кабинета участниц (supabase auth, 2026-10-19) ----------
# регистрация разрешена только тем, чья почта уже есть в signups со статусом "оплачено" —
# так в кабинет не попадёт никто, кто не оплатил марафон.
async def api_auth_register(request):
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    email = str(data.get("email", "")).strip()
    password = str(data.get("password", ""))
    if not email or not password or len(password) < 6:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    email_norm = normalize_email(email)

    paid = await DB_POOL.fetchval(
        "SELECT 1 FROM signups WHERE email_norm=$1 AND status='оплачено' LIMIT 1", email_norm
    )
    if not paid:
        return web.json_response({"ok": False, "error": "not_paid"}, status=403)

    already = await DB_POOL.fetchval("SELECT 1 FROM profiles WHERE email_norm=$1 LIMIT 1", email_norm)
    if already:
        return web.json_response({"ok": False, "error": "user_already_exists"}, status=409)

    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return web.json_response({"ok": False, "error": "auth_not_configured"}, status=500)

    async with ClientSession() as session:
        async with session.post(
            f"{SUPABASE_URL}/auth/v1/signup",
            json={"email": email, "password": password},
            headers={"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"},
        ) as resp:
            result = await resp.json()
            if resp.status >= 400:
                msg = result.get("msg") or result.get("error_description") or "signup_failed"
                return web.json_response({"ok": False, "error": msg}, status=resp.status)

    user = result.get("user") or result
    user_id = user.get("id")
    if not user_id:
        return web.json_response({"ok": False, "error": "no_user_id"}, status=500)

    await DB_POOL.execute(
        "INSERT INTO profiles (id, email, email_norm, created) VALUES ($1,$2,$3,$4) "
        "ON CONFLICT (id) DO NOTHING",
        user_id, email, email_norm, time.time(),
    )

    return web.json_response({
        "ok": True,
        "access_token": result.get("access_token"),
        "refresh_token": result.get("refresh_token"),
        "user_id": user_id,
        "needs_confirmation": result.get("access_token") is None,
    })


async def api_auth_login(request):
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    email = str(data.get("email", "")).strip()
    password = str(data.get("password", ""))
    if not email or not password:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return web.json_response({"ok": False, "error": "auth_not_configured"}, status=500)

    async with ClientSession() as session:
        async with session.post(
            f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
            json={"email": email, "password": password},
            headers={"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"},
        ) as resp:
            result = await resp.json()
            if resp.status >= 400:
                msg = result.get("msg") or result.get("error_description") or "login_failed"
                return web.json_response({"ok": False, "error": msg}, status=resp.status)

    return web.json_response({
        "ok": True,
        "access_token": result.get("access_token"),
        "refresh_token": result.get("refresh_token"),
    })


async def api_auth_recover(request):
    """Запрос письма восстановления пароля. Supabase всегда отвечает 200, даже если
    такого email нет — так мы не палим, кто зарегистрирован, кто нет."""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    email = str(data.get("email", "")).strip()
    if not email:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return web.json_response({"ok": False, "error": "auth_not_configured"}, status=500)
    async with ClientSession() as session:
        async with session.post(
            f"{SUPABASE_URL}/auth/v1/recover",
            json={"email": email},
            headers={"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"},
        ):
            pass
    return web.json_response({"ok": True})


async def api_auth_set_password(request):
    """Установка нового пароля — вызывается после перехода по ссылке восстановления,
    токен в Authorization это временный recovery-токен от supabase."""
    auth_header = request.headers.get("Authorization", "")
    token = auth_header[7:] if auth_header.startswith("Bearer ") else ""
    if not token:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    password = str(data.get("password", ""))
    if len(password) < 6:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    async with ClientSession() as session:
        async with session.put(
            f"{SUPABASE_URL}/auth/v1/user",
            json={"password": password},
            headers={"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        ) as resp:
            result = await resp.json()
            if resp.status >= 400:
                msg = result.get("msg") or result.get("error_description") or "set_password_failed"
                return web.json_response({"ok": False, "error": msg}, status=resp.status)
    return web.json_response({"ok": True})


async def _supabase_user_from_token(token: str):
    if not token or not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return None
    async with ClientSession() as session:
        async with session.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {token}"},
        ) as resp:
            if resp.status != 200:
                return None
            return await resp.json()


async def require_cabinet_user(request):
    """Возвращает supabase-пользователя (dict с id/email) по Bearer-токену, либо None."""
    auth_header = request.headers.get("Authorization", "")
    token = auth_header[7:] if auth_header.startswith("Bearer ") else ""
    return await _supabase_user_from_token(token)


async def _ensure_profile_row(user):
    """Подстраховка: у подтверждённого supabase-пользователя обязана быть строка в
    profiles (создаётся при регистрации), но если её почему-то нет — создаём на лету,
    а не роняем вход ошибкой no_profile."""
    row = await DB_POOL.fetchrow("SELECT * FROM profiles WHERE id=$1", user["id"])
    if row:
        return row
    await DB_POOL.execute(
        "INSERT INTO profiles (id, email, email_norm, created) VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO NOTHING",
        user["id"], user.get("email", ""), normalize_email(user.get("email", "")), time.time(),
    )
    return await DB_POOL.fetchrow("SELECT * FROM profiles WHERE id=$1", user["id"])


async def api_auth_me(request):
    user = await require_cabinet_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    row = await _ensure_profile_row(user)
    d = dict(row)
    d["id"] = str(d["id"])
    return web.json_response({"ok": True, "profile": d})


# ---------- кабинет участниц: профиль / привычки / ежедневные отметки (2026-10-19) ----------
async def api_cabinet_profile_update(request):
    user = await require_cabinet_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    name = str(data.get("name", ""))[:200]
    age = data.get("age")
    city = str(data.get("city", ""))[:200]
    bio = str(data.get("bio", ""))[:2000]
    photo_url = data.get("photo_url")
    await DB_POOL.execute(
        "UPDATE profiles SET name=$2, age=$3, city=$4, bio=$5, photo_url=$6 WHERE id=$1",
        user["id"], name, age, city, bio, photo_url,
    )
    return web.json_response({"ok": True})


async def api_cabinet_habits_list(request):
    user = await require_cabinet_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    rows = await DB_POOL.fetch(
        "SELECT id, name, config, sort FROM habit_selections WHERE user_id=$1 ORDER BY sort, id", user["id"]
    )
    habits = [{"id": r["id"], "name": r["name"], "config": _jsonb(r["config"]), "sort": r["sort"]} for r in rows]
    return web.json_response({"ok": True, "habits": habits})


async def api_cabinet_habits_replace(request):
    """Полностью заменяет набор привычек участницы — так же, как работает пикер в кабинете."""
    user = await require_cabinet_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    habits = data.get("habits")
    if not isinstance(habits, list) or not (1 <= len(habits) <= 5):
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    async with DB_POOL.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM habit_selections WHERE user_id=$1", user["id"])
            for i, h in enumerate(habits):
                name = str(h.get("name") if isinstance(h, dict) else h)[:200]
                config = json.dumps(h.get("config", {})) if isinstance(h, dict) else "{}"
                await conn.execute(
                    "INSERT INTO habit_selections (user_id, name, config, sort, created) VALUES ($1,$2,$3,$4,$5)",
                    user["id"], name, config, i, time.time(),
                )
    rows = await DB_POOL.fetch(
        "SELECT id, name, config, sort FROM habit_selections WHERE user_id=$1 ORDER BY sort, id", user["id"]
    )
    new_habits = [{"id": r["id"], "name": r["name"], "config": _jsonb(r["config"]), "sort": r["sort"]} for r in rows]
    return web.json_response({"ok": True, "habits": new_habits})


async def api_cabinet_logs_list(request):
    user = await require_cabinet_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    rows = await DB_POOL.fetch(
        "SELECT log_date, habits_done, closed, closed_at FROM daily_logs WHERE user_id=$1 ORDER BY log_date",
        user["id"],
    )
    logs = [{
        "date": r["log_date"].isoformat(), "habits_done": _jsonb(r["habits_done"]),
        "closed": r["closed"], "closed_at": r["closed_at"],
    } for r in rows]
    return web.json_response({"ok": True, "logs": logs})


async def api_cabinet_log_upsert(request):
    """Сохраняет отметки за один день: {date:'2026-11-09', habits_done:{habitId: true/false}, closed:bool}"""
    user = await require_cabinet_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    log_date = str(data.get("date", ""))
    habits_done = data.get("habits_done", {})
    closed = bool(data.get("closed", False))
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", log_date) or not isinstance(habits_done, dict):
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    closed_at = time.time() if closed else None
    await DB_POOL.execute(
        "INSERT INTO daily_logs (user_id, log_date, habits_done, closed, closed_at) VALUES ($1,$2,$3,$4,$5) "
        "ON CONFLICT (user_id, log_date) DO UPDATE SET habits_done=$3, closed=$4, "
        "closed_at=COALESCE(daily_logs.closed_at, $5)",
        user["id"], datetime.strptime(log_date, "%Y-%m-%d").date(), json.dumps(habits_done), closed, closed_at,
    )
    return web.json_response({"ok": True})


MASTERMIND_CODES = {"1": "звезда", "2": "луна"}


async def api_cabinet_mastermind_redeem(request):
    user = await require_cabinet_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    mm_id = str(data.get("mm_id", ""))
    code = str(data.get("code", "")).strip().lower()
    if mm_id not in MASTERMIND_CODES or code != MASTERMIND_CODES[mm_id]:
        return web.json_response({"ok": False, "error": "bad_code"}, status=400)
    row = await DB_POOL.fetchrow("SELECT mastermind FROM profiles WHERE id=$1", user["id"])
    redeemed = _jsonb(row["mastermind"]) if row and row["mastermind"] else {}
    if not isinstance(redeemed, dict):
        redeemed = {}
    redeemed[mm_id] = True
    await DB_POOL.execute("UPDATE profiles SET mastermind=$2 WHERE id=$1", user["id"], json.dumps(redeemed))
    return web.json_response({"ok": True, "mastermind": redeemed})


async def api_cabinet_bootstrap(request):
    """Один вызов на загрузку кабинета: профиль + привычки + отметки + день марафона + материалы + эфиры."""
    user = await require_cabinet_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    profile_row = await _ensure_profile_row(user)
    profile = dict(profile_row)
    profile["id"] = str(profile["id"])
    mastermind = _jsonb(profile.pop("mastermind", None)) or {}
    if not isinstance(mastermind, dict):
        mastermind = {}

    habit_rows = await DB_POOL.fetch(
        "SELECT id, name, config, sort FROM habit_selections WHERE user_id=$1 ORDER BY sort, id", user["id"]
    )
    habits = [{"id": r["id"], "name": r["name"], "config": _jsonb(r["config"]), "sort": r["sort"]} for r in habit_rows]

    log_rows = await DB_POOL.fetch(
        "SELECT log_date, habits_done, closed FROM daily_logs WHERE user_id=$1 ORDER BY log_date", user["id"]
    )
    logs = [{"date": r["log_date"].isoformat(), "habits_done": _jsonb(r["habits_done"]) or {}, "closed": r["closed"]}
            for r in log_rows]

    today = marathon_day_today(MARATHON_START, MARATHON_TZ)
    materials_data = await load_materials_from_db()

    cal_rows = await DB_POOL.fetch("SELECT * FROM calendar_events ORDER BY date")
    start = date.fromisoformat(MARATHON_START)
    events = [{
        "day": (r["date"] - start).days + 1, "title": r["title"],
        "time": r["time"] or "", "type": r["type"] or "", "link": r["link"] or "",
    } for r in cal_rows]

    return web.json_response({
        "ok": True, "profile": profile, "habits": habits, "logs": logs, "today": today,
        "marathon_start": MARATHON_START, "mastermind": mastermind,
        "authors": materials_data["authors"], "materials": materials_data["materials"], "events": events,
    })


def verify_stripe_signature(payload: str, sig_header: str) -> bool:
    """Проверка, что вебхук реально от Stripe (см. Stripe Webhook signing)."""
    if not STRIPE_WEBHOOK_SECRET:
        return False  # секрет не задан — отклоняем всё, а не пропускаем (fail-closed)
    try:
        parts = dict(p.split("=", 1) for p in sig_header.split(","))
        timestamp, v1 = parts["t"], parts["v1"]
        if abs(time.time() - int(timestamp)) > 300:  # защита от повторной отправки старого запроса
            return False
        signed = f"{timestamp}.{payload}"
        expected = hmac.new(STRIPE_WEBHOOK_SECRET.encode(), signed.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, v1)
    except Exception:
        return False


async def stripe_webhook(request):
    payload = await request.text()
    if not verify_stripe_signature(payload, request.headers.get("Stripe-Signature", "")):
        return web.Response(text="bad signature", status=400)
    event = json.loads(payload)
    if event.get("type") != "checkout.session.completed":
        return web.Response(text="ignored")

    session = event["data"]["object"]
    email = normalize_email(session.get("customer_details", {}).get("email", ""))
    name = session.get("customer_details", {}).get("name", "")
    amount = (session.get("amount_total") or 0) / 100

    row = await DB_POOL.fetchrow("SELECT id, status FROM signups WHERE email_norm=$1 AND method='stripe'", email)
    if row:
        await DB_POOL.execute("UPDATE signups SET status='оплачено', confirmed=$1 WHERE id=$2",
                               time.time(), row["id"])
    else:
        await DB_POOL.execute(
            "INSERT INTO signups (name, email, email_norm, method, amount, status, created, confirmed) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            name, email, email, "stripe", amount, "оплачено", time.time(), time.time(),
        )
    await send_access_email(email, name)
    return web.Response(text="ok")


async def admin_login_page(request):
    if authed(request):
        raise web.HTTPFound("/admin")
    return html_response(login_page())


async def admin_login_post(request):
    ip = request.remote or "?"
    if too_many(ip):
        return html_response(login_page("Слишком много попыток, подождите немного."), 429)
    data = await request.post()
    password = str(data.get("password", ""))
    ok = ADMIN_PASSWORD and hmac.compare_digest(password, ADMIN_PASSWORD)
    if not ok:
        FAILS.setdefault(ip, []).append(time.time())
        return html_response(login_page("Неверный пароль."), 401)
    resp = web.HTTPFound("/admin")
    resp.set_cookie(COOKIE, sign(f"admin:{time.time()}"), max_age=SESSION_DAYS * 86400,
                     httponly=True, samesite="Lax", secure=COOKIE_SECURE, path="/")
    raise resp


async def admin(request):
    if not authed(request):
        raise web.HTTPFound("/admin/login")
    return html_response(ADMIN_APP_HTML)


async def admin_confirm(request):
    if not authed(request):
        raise web.HTTPFound("/admin/login")
    row_id = int(request.match_info["id"])
    row = await DB_POOL.fetchrow("SELECT * FROM signups WHERE id=$1", row_id)
    if row and row["status"] != "оплачено":
        await DB_POOL.execute("UPDATE signups SET status='оплачено', confirmed=$1 WHERE id=$2",
                               time.time(), row_id)
        await send_access_email(row["email"], row["name"])
    raise web.HTTPFound("/admin")


async def admin_note(request):
    if not authed(request):
        raise web.HTTPFound("/admin/login")
    row_id = int(request.match_info["id"])
    data = await request.post()
    await DB_POOL.execute("UPDATE signups SET note=$1 WHERE id=$2", str(data.get("note", "")).strip(), row_id)
    raise web.HTTPFound("/admin")


async def admin_delete(request):
    if not authed(request):
        raise web.HTTPFound("/admin/login")
    row_id = int(request.match_info["id"])
    await DB_POOL.execute("DELETE FROM signups WHERE id=$1", row_id)
    raise web.HTTPFound("/admin")


async def admin_add(request):
    if not authed(request):
        raise web.HTTPFound("/admin/login")
    data = await request.post()
    email = str(data.get("email", "")).strip()
    if not email:
        raise web.HTTPFound("/admin")
    status = str(data.get("status", "ждёт")).strip()
    await DB_POOL.execute(
        "INSERT INTO signups (name, email, email_norm, phone, method, status, note, created, confirmed) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        str(data.get("name", "")).strip(), email, normalize_email(email),
        str(data.get("phone", "")).strip(),
        str(data.get("method", "venmo")).strip(), status, str(data.get("note", "")).strip(),
        time.time(), time.time() if status == "оплачено" else None,
    )
    raise web.HTTPFound("/admin")


async def admin_export(request):
    if not authed(request):
        raise web.HTTPFound("/admin/login")
    rows = await DB_POOL.fetch("SELECT * FROM signups ORDER BY created DESC")
    cols = ["id", "name", "email", "phone", "instagram", "method", "amount", "status", "note", "created", "confirmed"]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(cols)
    for r in rows:
        row = []
        for c in cols:
            v = r[c]
            if c in ("created", "confirmed") and v:
                v = time.strftime("%Y-%m-%d %H:%M", time.localtime(v))
            row.append(v if v is not None else "")
        writer.writerow(row)
    resp = web.Response(text="﻿" + buf.getvalue(), content_type="text/csv", charset="utf-8")
    resp.headers["Content-Disposition"] = 'attachment; filename="serenity-signups.csv"'
    return resp


async def admin_logout(request):
    resp = web.HTTPFound("/admin/login")
    resp.del_cookie(COOKIE, path="/")
    raise resp


def admin_token_ok(request) -> bool:
    """Доступ к JSON-api админки: обычная cookie-сессия /admin (страница теперь
    отдаётся этим же сервером) ИЛИ заголовок X-Admin-Token (на случай стороннего
    клиента/отдельной страницы без cookie)."""
    if authed(request):
        return True
    token = request.headers.get("X-Admin-Token", "")
    return bool(ADMIN_PASSWORD) and hmac.compare_digest(token, ADMIN_PASSWORD)


def _signup_json(row) -> dict:
    d = dict(row)
    return d


async def api_signups_list(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    rows = await DB_POOL.fetch("SELECT * FROM signups ORDER BY created DESC")
    return web.json_response([_signup_json(r) for r in rows])


async def api_admin_participants(request):
    """Личный прогресс каждой зарегистрированной в кабинете участницы — для вкладки
    «Участницы» в админке (кто отстаёт/пропал, кому написать лично)."""
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    profiles = await DB_POOL.fetch("SELECT id, name, email, mastermind FROM profiles ORDER BY created DESC")
    result = []
    for p in profiles:
        uid = p["id"]
        habit_count = await DB_POOL.fetchval("SELECT COUNT(*) FROM habit_selections WHERE user_id=$1", uid) or 0
        logs = await DB_POOL.fetch(
            "SELECT log_date, habits_done FROM daily_logs WHERE user_id=$1 ORDER BY log_date", uid
        )
        perfect_days = 0
        pct_sum = 0.0
        last_date = None
        for l in logs:
            hd = _jsonb(l["habits_done"]) or {}
            done = sum(1 for v in hd.values() if v)
            if habit_count and done >= habit_count:
                perfect_days += 1
            if habit_count:
                pct_sum += min(1.0, done / habit_count)
            last_date = l["log_date"]
        avg_pct = round((pct_sum / len(logs)) * 100) if logs else 0
        mm = _jsonb(p["mastermind"]) or {}
        mm_count = sum(1 for v in mm.values() if v) if isinstance(mm, dict) else 0
        result.append({
            "id": str(uid), "name": p["name"] or "", "email": p["email"] or "",
            "habit_count": habit_count, "avg_pct": avg_pct, "perfect_days": perfect_days,
            "mastermind_count": mm_count, "last_active": last_date.isoformat() if last_date else None,
        })
    return web.json_response(result)


async def api_admin_participant_delete(request):
    """Удаляет профиль участницы из кабинета (вместе с её привычками/отметками —
    ON DELETE CASCADE). Сам логин в supabase auth не трогает — только данные в нашей базе."""
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    await DB_POOL.execute("DELETE FROM profiles WHERE id=$1", request.match_info["id"])
    return web.json_response({"ok": True})


async def api_signup_confirm(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    row_id = int(request.match_info["id"])
    row = await DB_POOL.fetchrow("SELECT * FROM signups WHERE id=$1", row_id)
    if not row:
        return web.json_response({"error": "not found"}, status=404)
    if row["status"] != "оплачено":
        await DB_POOL.execute("UPDATE signups SET status='оплачено', confirmed=$1 WHERE id=$2",
                               time.time(), row_id)
        await send_access_email(row["email"], row["name"])
    row = await DB_POOL.fetchrow("SELECT * FROM signups WHERE id=$1", row_id)
    return web.json_response(_signup_json(row))


async def api_signup_note(request):
    """Правка заметки и/или суммы (venmo/zelle не передают сумму сами —
    миля вписывает её вручную в CRM при подтверждении оплаты)."""
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    row_id = int(request.match_info["id"])
    data = await request.json()
    sets, vals = [], []
    if "note" in data:
        sets.append(f"note=${len(vals) + 1}")
        vals.append(str(data.get("note", "")).strip())
    if "amount" in data:
        raw = data.get("amount")
        try:
            amount = float(raw) if raw not in (None, "") else None
        except (TypeError, ValueError):
            return web.json_response({"error": "bad amount"}, status=400)
        sets.append(f"amount=${len(vals) + 1}")
        vals.append(amount)
    if not sets:
        return web.json_response({"ok": True})
    vals.append(row_id)
    await DB_POOL.execute(f"UPDATE signups SET {', '.join(sets)} WHERE id=${len(vals)}", *vals)
    return web.json_response({"ok": True})


async def api_signup_delete(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    row_id = int(request.match_info["id"])
    await DB_POOL.execute("DELETE FROM signups WHERE id=$1", row_id)
    return web.json_response({"ok": True})


async def api_signup_add(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    data = await request.json()
    email = str(data.get("email", "")).strip()
    if not email:
        return web.json_response({"error": "email required"}, status=400)
    status = str(data.get("status", "ждёт")).strip()
    raw_amount = data.get("amount")
    try:
        amount = float(raw_amount) if raw_amount not in (None, "") else None
    except (TypeError, ValueError):
        amount = None
    await DB_POOL.execute(
        "INSERT INTO signups (name, email, email_norm, phone, instagram, method, status, note, amount, created, confirmed) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        str(data.get("name", "")).strip(), email, normalize_email(email),
        str(data.get("phone", "")).strip(),
        str(data.get("instagram", "")).strip(),
        str(data.get("method", "venmo")).strip(), status, str(data.get("note", "")).strip(), amount,
        time.time(), time.time() if status == "оплачено" else None,
    )
    row = await DB_POOL.fetchrow("SELECT * FROM signups WHERE email_norm=$1 ORDER BY id DESC LIMIT 1",
                                  normalize_email(email))
    return web.json_response(_signup_json(row), status=201)


# ---------- материалы-админка: папки / темы / материалы / календарь / мероприятия Милы ----------
def _folder_json(r) -> dict:
    return {"id": r["id"], "name": r["name"], "description": r["description"] or "",
            "topics": r["topics"] or "", "photo": r["photo_url"], "color": r["dot_color"] or ""}


def _material_json(r) -> dict:
    return {"id": r["id"], "folder_id": r["folder_id"], "title": r["title"], "format": r["format"],
            "tags": _jsonb(r["tags"]) or [], "day": r["day"], "url": r["url"],
            "calendar_event_id": r["calendar_event_id"]}


def _calendar_json(r) -> dict:
    return {"id": r["id"], "date": r["date"].isoformat() if r["date"] else None, "title": r["title"],
            "type": r["type"], "time": r["time"], "link": r["link"]}


def _mila_event_json(r) -> dict:
    return {"id": r["id"], "title": r["title"], "date_place": r["date_place"],
            "link": r["link"], "color": r["color"]}


async def api_folders_list(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    rows = await DB_POOL.fetch("SELECT * FROM folders ORDER BY sort, name")
    return web.json_response([_folder_json(r) for r in rows])


async def api_folders_add(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    data = await request.json()
    name = str(data.get("name", "")).strip()
    if not name:
        return web.json_response({"error": "name required"}, status=400)
    fid = str(data.get("id") or uuid.uuid4().hex[:10])
    next_sort = await DB_POOL.fetchval("SELECT COALESCE(MAX(sort), 0) + 1 FROM folders")
    await DB_POOL.execute(
        "INSERT INTO folders (id, name, description, topics, photo_url, dot_color, created, sort) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) "
        "ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, description=EXCLUDED.description, "
        "topics=EXCLUDED.topics, photo_url=COALESCE(EXCLUDED.photo_url, folders.photo_url), "
        "dot_color=EXCLUDED.dot_color",
        fid, name, str(data.get("description", "")).strip(), str(data.get("topics", "")).strip(),
        data.get("photo") or None, str(data.get("color", "")).strip(), time.time(), next_sort,
    )
    row = await DB_POOL.fetchrow("SELECT * FROM folders WHERE id=$1", fid)
    return web.json_response(_folder_json(row), status=201)


async def api_folders_delete(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    fid = request.match_info["id"]
    await DB_POOL.execute("DELETE FROM folders WHERE id=$1", fid)
    return web.json_response({"ok": True})


async def api_themes_list(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    rows = await DB_POOL.fetch("SELECT * FROM themes ORDER BY label")
    return web.json_response([{"id": r["id"], "label": r["label"]} for r in rows])


async def api_themes_add(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    data = await request.json()
    label = str(data.get("label", "")).strip()
    if not label:
        return web.json_response({"error": "label required"}, status=400)
    tid = label.lower()
    await DB_POOL.execute(
        "INSERT INTO themes (id, label, created) VALUES ($1,$2,$3) ON CONFLICT (id) DO NOTHING",
        tid, label, time.time(),
    )
    return web.json_response({"id": tid, "label": label}, status=201)


async def api_themes_delete(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    await DB_POOL.execute("DELETE FROM themes WHERE id=$1", request.match_info["id"])
    return web.json_response({"ok": True})


async def api_materials_list(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    rows = await DB_POOL.fetch("SELECT * FROM materials ORDER BY day NULLS FIRST, id")
    return web.json_response([_material_json(r) for r in rows])


async def api_materials_add(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    data = await request.json()
    title = str(data.get("title", "")).strip()
    folder_id = str(data.get("folder_id", "")).strip()
    if not title or not folder_id:
        return web.json_response({"error": "title and folder_id required"}, status=400)
    day_raw = data.get("day")
    try:
        day = int(day_raw) if day_raw not in (None, "") else None
    except (TypeError, ValueError):
        day = None
    row_id = await DB_POOL.fetchval(
        "INSERT INTO materials (folder_id, title, format, tags, day, url, calendar_event_id, created) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id",
        folder_id, title, str(data.get("format", "link")).strip(),
        json.dumps(data.get("tags", []), ensure_ascii=False), day,
        str(data.get("url", "")).strip() or "#", data.get("calendar_event_id"), time.time(),
    )
    row = await DB_POOL.fetchrow("SELECT * FROM materials WHERE id=$1", row_id)
    return web.json_response(_material_json(row), status=201)


async def api_materials_delete(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    await DB_POOL.execute("DELETE FROM materials WHERE id=$1", int(request.match_info["id"]))
    return web.json_response({"ok": True})


MATERIALS_UPLOAD_MAX_BYTES = 80 * 1024 * 1024  # 80MB — хватает на видео-уроки и аудио


async def api_materials_upload(request):
    """Загружает файл материала в приватный supabase storage bucket 'materials'.
    Возвращает storage-путь (storage:materials/<path>), который кладётся в поле url
    существующего материала через обычный api_materials_add — участницам файл отдаётся
    только подписанной ссылкой с ограниченным сроком, см. api_cabinet_material_link."""
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return web.json_response({"error": "storage_not_configured"}, status=500)
    reader = await request.multipart()
    field = await reader.next()
    if field is None or field.name != "file":
        return web.json_response({"error": "file field required"}, status=400)
    filename = field.filename or "file"
    ext = Path(filename).suffix or ""
    content_type = field.headers.get("Content-Type", "application/octet-stream")
    storage_path = f"{uuid.uuid4().hex}{ext}"
    chunks = []
    total = 0
    while True:
        chunk = await field.read_chunk(1024 * 256)
        if not chunk:
            break
        total += len(chunk)
        if total > MATERIALS_UPLOAD_MAX_BYTES:
            return web.json_response({"error": "file_too_large"}, status=413)
        chunks.append(chunk)
    body = b"".join(chunks)
    async with ClientSession() as session:
        async with session.post(
            f"{SUPABASE_URL}/storage/v1/object/materials/{storage_path}",
            data=body,
            headers={
                "apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type": content_type,
            },
        ) as resp:
            if resp.status >= 400:
                err = await resp.text()
                return web.json_response({"error": "upload_failed", "detail": err[:300]}, status=502)
    return web.json_response({"ok": True, "url": f"storage:materials/{storage_path}", "filename": filename})


async def api_cabinet_material_link(request):
    """Выдаёт временную подписанную ссылку на файл материала, только если день уже
    разблокирован. Для материалов с обычной внешней ссылкой просто отдаёт её обратно."""
    user = await require_cabinet_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        mat_id = int(request.match_info["id"])
    except ValueError:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    row = await DB_POOL.fetchrow("SELECT * FROM materials WHERE id=$1", mat_id)
    if not row:
        return web.json_response({"ok": False, "error": "not_found"}, status=404)
    today = marathon_day_today(MARATHON_START, MARATHON_TZ)
    if row["day"] is not None and row["day"] > today:
        return web.json_response({"ok": False, "error": "locked"}, status=403)
    url = row["url"] or ""
    if not url.startswith("storage:materials/"):
        return web.json_response({"ok": True, "url": url})
    storage_path = url[len("storage:materials/"):]
    async with ClientSession() as session:
        async with session.post(
            f"{SUPABASE_URL}/storage/v1/object/sign/materials/{storage_path}",
            json={"expiresIn": 300},
            headers={"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
        ) as resp:
            result = await resp.json()
            if resp.status >= 400:
                return web.json_response({"ok": False, "error": "sign_failed"}, status=502)
    signed = result.get("signedURL") or result.get("signedUrl") or ""
    return web.json_response({"ok": True, "url": f"{SUPABASE_URL}/storage/v1{signed}"})


AVATAR_UPLOAD_MAX_BYTES = 5 * 1024 * 1024


async def api_cabinet_avatar_upload(request):
    user = await require_cabinet_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return web.json_response({"ok": False, "error": "storage_not_configured"}, status=500)
    reader = await request.multipart()
    field = await reader.next()
    if field is None or field.name != "file":
        return web.json_response({"ok": False, "error": "file field required"}, status=400)
    ext = Path(field.filename or "").suffix.lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        ext = ".jpg"
    content_type = field.headers.get("Content-Type", "image/jpeg")
    chunks = []
    total = 0
    while True:
        chunk = await field.read_chunk(1024 * 256)
        if not chunk:
            break
        total += len(chunk)
        if total > AVATAR_UPLOAD_MAX_BYTES:
            return web.json_response({"ok": False, "error": "file_too_large"}, status=413)
        chunks.append(chunk)
    body = b"".join(chunks)
    storage_path = f"{user['id']}/avatar{ext}"
    async with ClientSession() as session:
        async with session.post(
            f"{SUPABASE_URL}/storage/v1/object/avatars/{storage_path}",
            data=body,
            headers={
                "apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type": content_type, "x-upsert": "true",
            },
        ) as resp:
            if resp.status >= 400:
                err = await resp.text()
                return web.json_response({"ok": False, "error": "upload_failed", "detail": err[:300]}, status=502)
    photo_url = f"{SUPABASE_URL}/storage/v1/object/public/avatars/{storage_path}?v={int(time.time())}"
    await DB_POOL.execute("UPDATE profiles SET photo_url=$2 WHERE id=$1", user["id"], photo_url)
    return web.json_response({"ok": True, "photo_url": photo_url})


async def api_calendar_list(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    rows = await DB_POOL.fetch("SELECT * FROM calendar_events ORDER BY date")
    return web.json_response([_calendar_json(r) for r in rows])


async def api_calendar_add(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    data = await request.json()
    title = str(data.get("title", "")).strip()
    date_str = str(data.get("date", "")).strip()
    if not title or not date_str:
        return web.json_response({"error": "title and date required"}, status=400)
    try:
        ev_date = date.fromisoformat(date_str)
    except ValueError:
        return web.json_response({"error": "bad date"}, status=400)
    row_id = await DB_POOL.fetchval(
        "INSERT INTO calendar_events (date, title, type, time, link, created) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
        ev_date, title, str(data.get("type", "")).strip(), str(data.get("time", "")).strip(),
        str(data.get("link", "")).strip(), time.time(),
    )
    row = await DB_POOL.fetchrow("SELECT * FROM calendar_events WHERE id=$1", row_id)
    return web.json_response(_calendar_json(row), status=201)


async def api_calendar_delete(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    await DB_POOL.execute("DELETE FROM calendar_events WHERE id=$1", int(request.match_info["id"]))
    return web.json_response({"ok": True})


async def api_mila_events_list(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    rows = await DB_POOL.fetch("SELECT * FROM mila_events ORDER BY id DESC")
    return web.json_response([_mila_event_json(r) for r in rows])


async def api_mila_events_add(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    data = await request.json()
    title = str(data.get("title", "")).strip()
    date_place = str(data.get("date_place", "")).strip()
    if not title or not date_place:
        return web.json_response({"error": "title and date_place required"}, status=400)
    row_id = await DB_POOL.fetchval(
        "INSERT INTO mila_events (title, date_place, link, color, created) VALUES ($1,$2,$3,$4,$5) RETURNING id",
        title, date_place, str(data.get("link", "")).strip(), str(data.get("color", "lav")).strip(), time.time(),
    )
    row = await DB_POOL.fetchrow("SELECT * FROM mila_events WHERE id=$1", row_id)
    return web.json_response(_mila_event_json(row), status=201)


async def api_mila_events_delete(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    await DB_POOL.execute("DELETE FROM mila_events WHERE id=$1", int(request.match_info["id"]))
    return web.json_response({"ok": True})


async def get_setting(key: str, default=None):
    row = await DB_POOL.fetchrow("SELECT value FROM settings WHERE key=$1", key)
    return row["value"] if row else default


async def set_setting(key: str, value: str):
    await DB_POOL.execute(
        "INSERT INTO settings (key, value) VALUES ($1,$2) "
        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
        key, value,
    )


async def cabinet_is_open() -> bool:
    """Разовое открытие доступа всем оплатившим сразу (не по одному при оплате) —
    см. решение 2026-10-13 в памяти проекта. Пока в кабинет ещё никто не стучится
    (шаг 3 не готов), это просто готовый примитив для будущей проверки логина."""
    raw = await get_setting("cabinet_opens_at")
    if not raw:
        return False
    try:
        return time.time() >= float(raw)
    except ValueError:
        return False


async def api_settings_cabinet_opens(request):
    """GET — текущее значение; POST {"opens_at": unix-время или null} — задать/снять."""
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    if request.method == "POST":
        data = await request.json()
        raw = data.get("opens_at")
        if raw in (None, ""):
            await DB_POOL.execute("DELETE FROM settings WHERE key='cabinet_opens_at'")
        else:
            await set_setting("cabinet_opens_at", str(float(raw)))
    opens_at = await get_setting("cabinet_opens_at")
    return web.json_response({
        "opens_at": float(opens_at) if opens_at else None,
        "is_open": await cabinet_is_open(),
    })


async def cabinet(request):
    return html_response(CABINET_APP_HTML)


async def root(request):
    """Один сервер отвечает на нескольких доменах — отличаем по Host, чтобы
    admin.serenitymoonevents.com/ не показывал кабинет участниц."""
    host = (request.host or "").split(":")[0]
    if host.startswith("admin."):
        raise web.HTTPFound("/admin")
    return html_response(CABINET_APP_HTML)


async def health(request):
    await DB_POOL.fetchval("SELECT 1")
    return web.Response(text="ok")


@web.middleware
async def cors_middleware(request, handler):
    """Лендинг (gh-pages / свой домен) и сервер — разные источники для браузера,
    без этих заголовков браузер блокирует ответ на fetch('/register', ...) с
    фронта даже если сам запрос реально дошёл до сервера."""
    if request.method == "OPTIONS":
        resp = web.Response()
    else:
        resp = await handler(request)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    return resp


async def cors_preflight(request):
    return web.Response()


def make_app():
    if not SECRET:
        raise SystemExit("Нет PLATFORM_SECRET в .env — см. .env.example")
    if not ADMIN_PASSWORD:
        print("[ПРЕДУПРЕЖДЕНИЕ] ADMIN_PASSWORD не задан — вход в /admin не сработает, см. .env.example")
    app = web.Application(middlewares=[cors_middleware])
    app.on_startup.append(init_db)
    app.on_cleanup.append(close_db)
    app.add_routes([
        web.options("/register", cors_preflight),
        web.post("/register", register),
        web.post("/webhook/stripe", stripe_webhook),
        web.get("/admin", admin),
        web.get("/admin/login", admin_login_page),
        web.post("/admin/login", admin_login_post),
        web.post("/admin/confirm/{id}", admin_confirm),
        web.post("/admin/note/{id}", admin_note),
        web.post("/admin/delete/{id}", admin_delete),
        web.post("/admin/add", admin_add),
        web.get("/admin/export.csv", admin_export),
        web.get("/admin/logout", admin_logout),
        web.get("/health", health),
        web.post("/api/auth/register", api_auth_register),
        web.post("/api/auth/login", api_auth_login),
        web.post("/api/auth/recover", api_auth_recover),
        web.put("/api/auth/set-password", api_auth_set_password),
        web.get("/api/auth/me", api_auth_me),
        web.put("/api/cabinet/profile", api_cabinet_profile_update),
        web.post("/api/cabinet/avatar", api_cabinet_avatar_upload),
        web.get("/api/cabinet/materials/{id}/link", api_cabinet_material_link),
        web.get("/api/cabinet/habits", api_cabinet_habits_list),
        web.put("/api/cabinet/habits", api_cabinet_habits_replace),
        web.get("/api/cabinet/logs", api_cabinet_logs_list),
        web.put("/api/cabinet/logs", api_cabinet_log_upsert),
        web.post("/api/cabinet/mastermind", api_cabinet_mastermind_redeem),
        web.get("/api/cabinet/bootstrap", api_cabinet_bootstrap),
        web.get("/cabinet", cabinet),
        web.get("/", root),
        # ---- JSON api для CRM-вкладки в админ-прототипе (claude.ai, другой origin — токен в
        # заголовке X-Admin-Token вместо cookie-сессии) ----
        web.get("/api/admin/signups", api_signups_list),
        web.get("/api/admin/participants", api_admin_participants),
        web.delete("/api/admin/participants/{id}", api_admin_participant_delete),
        web.post("/api/admin/signups", api_signup_add),
        web.post("/api/admin/signups/{id}/confirm", api_signup_confirm),
        web.post("/api/admin/signups/{id}/note", api_signup_note),
        web.delete("/api/admin/signups/{id}", api_signup_delete),
        web.get("/api/admin/settings/cabinet-opens", api_settings_cabinet_opens),
        web.post("/api/admin/settings/cabinet-opens", api_settings_cabinet_opens),
        web.get("/api/admin/folders", api_folders_list),
        web.post("/api/admin/folders", api_folders_add),
        web.delete("/api/admin/folders/{id}", api_folders_delete),
        web.get("/api/admin/themes", api_themes_list),
        web.post("/api/admin/themes", api_themes_add),
        web.delete("/api/admin/themes/{id}", api_themes_delete),
        web.get("/api/admin/materials", api_materials_list),
        web.post("/api/admin/materials", api_materials_add),
        web.delete("/api/admin/materials/{id}", api_materials_delete),
        web.post("/api/admin/materials/upload", api_materials_upload),
        web.get("/api/admin/calendar-events", api_calendar_list),
        web.post("/api/admin/calendar-events", api_calendar_add),
        web.delete("/api/admin/calendar-events/{id}", api_calendar_delete),
        web.get("/api/admin/mila-events", api_mila_events_list),
        web.post("/api/admin/mila-events", api_mila_events_add),
        web.delete("/api/admin/mila-events/{id}", api_mila_events_delete),
        web.route("OPTIONS", "/{tail:.*}", cors_preflight),
    ])
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host=HOST, port=PORT)
