"""Серенити Мун – марафон: приём оплаты + табличка для Милы.

Не трогает сам кабинет участниц (это следующий этап) – только:
- приём формы Venmo/Zelle (POST /register)
- приём вебхука Stripe (POST /webhook/stripe)
- админ-табличка со списком и кнопкой "отметить оплаченным" (GET /admin)

Настройки: .env (см. .env.example). Без реальных ключей Stripe/Resend
всё работает в тестовом режиме – только без реальной отправки.
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
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")  # публичный (anon/publishable) ключ – НЕ service_role

# пул подключений к базе — создаётся асинхронно при старте (см. on_startup), пока сервер
# не поднялся — DB_POOL == None
DB_POOL: asyncpg.Pool | None = None


async def init_db(app):
    global DB_POOL
    if not DATABASE_URL:
        raise SystemExit("Нет DATABASE_URL в .env – нужна строка подключения к supabase/postgres, см. .env.example")
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
    # описание мероприятия Милы — добавлено позже исходной таблицы mila_events
    await DB_POOL.execute("ALTER TABLE mila_events ADD COLUMN IF NOT EXISTS description TEXT DEFAULT ''")
    # фото + раздельные дата/место (вместо единой строки date_place) — добавлено позже
    await DB_POOL.execute("ALTER TABLE mila_events ADD COLUMN IF NOT EXISTS photo_url TEXT")
    await DB_POOL.execute("ALTER TABLE mila_events ADD COLUMN IF NOT EXISTS date_label TEXT DEFAULT ''")
    await DB_POOL.execute("ALTER TABLE mila_events ADD COLUMN IF NOT EXISTS place_label TEXT DEFAULT ''")
    await DB_POOL.execute(
        "UPDATE mila_events SET date_label=split_part(date_place,',',1), "
        "place_label=trim(substring(date_place from position(',' in date_place)+1)) "
        "WHERE (date_label IS NULL OR date_label='') AND date_place IS NOT NULL "
        "AND date_place<>'' AND position(',' in date_place)>0"
    )

    await seed_materials_once()


async def seed_materials_once():
    """Разовый перенос старых data/materials.json (13 материалов, 4 автора) в
    реальные таблицы – выполняется только если materials пока пустая (т.е. один
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
MARATHON_START_DEFAULT = "2026-10-19"  # используется, пока в settings нет значения "marathon_start" (задаётся в админке)
MARATHON_TZ = "America/New_York"
INTRO_FOLDER_ID = "662d1a2b85"  # папка "Перед началом клуба" — обычная папка материалов, жёстко привязана к карточке


async def get_marathon_start() -> str:
    return await get_setting("marathon_start", MARATHON_START_DEFAULT)


def load_materials() -> dict:
    """Старый путь (читает JSON-файл) – больше не вызывается, оставлено как
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
    return {"marathon_start": await get_marathon_start(), "timezone": MARATHON_TZ, "authors": authors, "materials": mats}


def marathon_day_today(marathon_start: str, tz_name: str) -> int:
    """День марафона "сегодня" (1 = старт). Может быть <1 (до старта) или >21
    (после финиша) – страница сама решает, что в этом случае показать."""
    start = date.fromisoformat(marathon_start)
    now = datetime.now(ZoneInfo(tz_name)).date()
    return (now - start).days + 1



# ---------- почта (не падает без ключей — просто логирует) ----------
QR_PATHS = {"venmo": HERE / "qr-venmo.jpg", "zelle": HERE / "qr-zelle.jpg"}


async def send_payment_qr_email(to_email: str, name: str, method: str):
    """Дублирует QR-код на почту сразу после заявки (venmo/zelle) – на случай если
    человек закроет страницу до оплаты и потеряет код."""
    greet_name = name if name else "дорогая участница"
    method_title = "Venmo" if method == "venmo" else "Zelle"
    body = (f"Привет, {greet_name}! \n\n"
            f"Спасибо за заявку на участие в клубе «Начни сегодня» 💗 \n\n"
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
        "subject": f"Serenity Moon – QR-код для оплаты ({method_title})",
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
            f"Как здорово, что ты решила присоединиться к нашему клубу «Начни сегодня»!\n\n"
            f"🟣 Вход в наш закрытый Telegram-канал, где будет проходить клуб, по ссылке:\n\n"
            f"{invite}\n\n"
            f"🌿 Что дальше?\n\n"
            f"Наш клуб стартует 8 ноября!\n\n"
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
        "subject": "Serenity Moon – доступ к клубу",
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
<title>Вход – заявки клуба</title>
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
<h2 style="margin:0 0 4px;font-size:18px;">Заявки клуба</h2>
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
                f'<td style="padding:12px 16px;">{E(r["name"] or "–")}</td>'
                f'<td style="padding:12px 16px;color:#6B6470;">{E(r["email"])}</td>'
                f'<td style="padding:12px 16px;color:#6B6470;">{E(r["phone"] or "–")}</td>'
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
<h1 style="font-size:24px;margin:0;">Заявки клуба</h1>
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
CABINET_APP_HTML = """<!doctype html><html lang="ru"><head><meta charset=utf8><meta name=viewport content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
<title>Serenity Moon</title>
<link rel="icon" type="image/jpeg" href="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/app-icon.jpg">
<link rel="apple-touch-icon" href="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/app-icon.jpg">
<meta property="og:title" content="Serenity Moon">
<meta property="og:description" content="Личный кабинет участницы клуба «Начни сегодня!»">
<meta property="og:image" content="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/app-icon.jpg">
<meta property="og:type" content="website">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,500;0,600;1,500;1,600&family=Montserrat:wght@400;500;600&display=swap">
<style>:root{color-scheme:light;box-sizing:border-box;padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}html{scroll-padding-top:env(safe-area-inset-top,0px)}body{margin:0;padding:0;font:14px -apple-system,BlinkMacSystemFont,sans-serif;background:#faf9f5;color:#141413}img{max-width:100%}[hidden]:not([hidden=until-found i]){display:none!important}</style></head><body>
<style>
:root{
  --lav:#AA9DB8; --lav-ink:#6B5A7C; --sage:#6A9993; --sage-ink:#3F6660;
  --sand:#CBBAA8; --sand-ink:#8A6F55; --ink:#332E3D; --ink-soft:#6B6475;
  --bg:#FAF6F1; --surface:#FFFFFF; --surface-2:#F4F5F6; --line:#E6E8E9;
  --shadow:0 10px 30px -14px rgba(51,46,61,.22); color-scheme:light;
  --h0:#ECEFEC; --h0-text:#6B6475; --h30:#B6CFC6; --h30-text:#3F6660;
  --h60:#7FA89C; --h60-text:#FFFFFF; --h100:#6A9993; --h100-text:#FFFFFF; --h-future:#F4F5F6;
  --sys: "Montserrat",-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,system-ui,"Helvetica Neue",Arial,sans-serif;
  --font-head: "Cormorant Garamond",Georgia,"Times New Roman",serif;
}
*{box-sizing:border-box}
.btn:active,.editlink:active,.close-x:active,.chip:active,.habchip:active,.hab-edit:active,
.evt-row:active,.hday:active,.mcard:active,.evcard a:active{transform:scale(.96)}
.btn,.editlink,.close-x,.chip,.habchip{transition:transform .1s}
button,a,.btn{-webkit-user-select:none;user-select:none;-webkit-touch-callout:none;touch-action:manipulation}
html,body{height:100%;touch-action:pan-x pan-y}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sys)}
h1,h2,h3{font-family:var(--font-head);font-weight:600;letter-spacing:-.01em;margin:0;text-wrap:balance}
.eyebrow{font-family:var(--font-head);font-style:italic;font-size:13px;color:var(--lav-ink);margin:0 0 4px}

.app{max-width:460px;margin:0 auto;min-height:100%;display:flex;flex-direction:column;position:relative}
.app[hidden]{display:none}
.topbar{padding:10px 18px 6px;padding-top:calc(10px + env(safe-area-inset-top,0px));display:flex;flex-direction:column;align-items:center;gap:4px;text-align:center}
.topbar .mark{width:30px;height:30px;object-fit:contain}
.topbar .name{font-family:var(--font-head);font-weight:600;font-size:16px;letter-spacing:.02em}
.name small{display:block;font-weight:400;font-size:10px;color:var(--ink-soft);letter-spacing:.02em;margin-top:1px}
.marathon-eyebrow{display:block;text-align:center;margin:0 0 20px}
.marathon-eyebrow span{display:inline-block;padding:7px 18px;border-radius:999px;
  background:linear-gradient(90deg,var(--lav),var(--sage));box-shadow:var(--shadow);
  font-family:var(--sys);font-size:11px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:#fff}
.page-title{font-family:var(--font-head);font-weight:600;font-size:34px;color:var(--sage-ink);
  margin:0 auto 6px;text-wrap:balance;text-align:center;text-transform:uppercase}
.cal-card{padding:16px 14px 14px;max-width:380px;margin:0 auto 10px}
.cal-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.cal-head h3{font-family:var(--font-head);font-weight:600;font-size:19px;color:var(--ink);margin:0}
.cal-nav{display:flex;gap:8px}
.cal-nav button{width:30px;height:30px;border-radius:50%;border:none;background:var(--surface-2);color:var(--ink-soft);
  font-size:16px;line-height:1;cursor:pointer;display:flex;align-items:center;justify-content:center;font-family:var(--sys)}
.cal-nav button:active{transform:scale(.92)}
.cal-weekdays{display:grid;grid-template-columns:repeat(7,1fr);margin-bottom:6px}
.cal-weekdays span{text-align:center;font-size:10.5px;font-weight:600;color:var(--ink-soft);text-transform:uppercase}
.cal-grid{display:grid;grid-template-columns:repeat(7,1fr);row-gap:6px}
.cal-day{height:34px;display:flex;align-items:center;justify-content:center}
.cal-day span{width:30px;height:30px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:13px;color:var(--ink)}
.cal-day.ev span{color:#fff;font-weight:600}
.cal-day.ev.c-sage span{background:var(--sage)}
.cal-day.ev.c-lav span{background:linear-gradient(135deg,var(--lav),#C9A9DE)}
.cal-legend{display:flex;align-items:center;gap:7px;margin-top:12px;padding-top:12px;border-top:1px solid var(--line);
  font-size:12px;color:var(--ink-soft)}
.cal-dot{width:11px;height:11px;border-radius:50%;flex-shrink:0}
.cal-dot.c-sage{background:var(--sage)}
.cal-dot.c-lav{background:linear-gradient(135deg,var(--lav),#C9A9DE)}

main{flex:1;padding:0 18px calc(88px + min(16px, env(safe-area-inset-bottom,0px)))}
.screen[hidden]{display:none}

.bottomnav{
  position:fixed; left:0; right:0; bottom:0; display:flex; justify-content:center; z-index:30;
  padding-bottom:min(16px, env(safe-area-inset-bottom,0px));
  background:var(--surface); border-top:1px solid var(--line);
  box-shadow:0 -8px 24px -16px rgba(51,46,61,.25);
}
.bottomnav .inner{
  width:100%; max-width:460px; display:flex; padding-top:6px;
}
.navbtn{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:4px;
  padding:11px 4px 9px;min-height:48px;background:transparent;border:none;border-radius:12px;
  color:var(--ink-soft);font-family:var(--sys);font-size:11.5px;font-weight:400;cursor:pointer;
  transform:scale(1);transition:background-color .15s ease-in-out,color .15s ease-in-out,transform .15s ease-in-out;
  -webkit-user-select:none;user-select:none;-webkit-tap-highlight-color:transparent}
.navbtn svg{width:22px;height:22px;color:currentColor}
.navbtn:active{transform:scale(.98)}
.navbtn.active{background:rgba(106,153,147,.13);border:1px solid rgba(106,153,147,.45);color:var(--sage-ink);font-weight:600}
@media (hover:hover){
  .navbtn:hover{background:rgba(0,0,0,.04);color:var(--ink)}
  .navbtn.active:hover{background:rgba(106,153,147,.2)}
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
.hday-corner{position:absolute;top:3px;right:4px;font-size:9px;font-weight:600;opacity:.8}
.hday-main{display:flex;flex-direction:column;align-items:center;line-height:1.05}
.hday-dd{font-size:16px;font-weight:700}
.hday-mm{font-size:8.5px;font-weight:500;opacity:.9;margin-top:1px}
.hday.d0{background:var(--h0);color:var(--h0-text)}
.hday.d30{background:var(--h30);color:var(--h30-text)}
.hday.d60{background:var(--h60);color:var(--h60-text)}
.hday.d100{background:var(--h100);color:var(--h100-text)}
.hday.future{background:var(--h-future);border:1px solid var(--line);color:var(--ink-soft);font-weight:400}
.hday.istoday{border-color:#D4A94C;border-width:2.5px;box-shadow:0 0 0 4px rgba(212,169,76,.4),0 0 14px rgba(212,169,76,.5)}
.hday-check{display:flex;align-items:center;justify-content:center}
.hday-check svg{width:17px;height:17px;flex-shrink:0}
.hint{font-size:12.5px;color:var(--ink-soft);text-align:center;line-height:1.5}

.info-hint{width:20px;height:20px;border-radius:50%;border:1.5px solid var(--lav);background:var(--surface);
  color:var(--lav-ink);font-size:11.5px;font-weight:700;cursor:pointer;flex-shrink:0;display:inline-flex;
  align-items:center;justify-content:center;line-height:1;padding:0;font-family:var(--sys)}
h3 .info-hint{margin-left:6px;vertical-align:middle}

.statrow{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-bottom:10px}
.stat-card{position:relative;border-radius:16px;padding:16px;color:#fff;min-height:92px;box-shadow:var(--shadow);
  background-image:linear-gradient(90deg,var(--sage),var(--lav));background-size:210% 100%}
.stat-card.sage-grad{background-position:left center}
.stat-card.lav-grad{background-position:right center}
.stat-top{display:flex;align-items:center;gap:8px;margin-bottom:6px}
.stat-icon{font-size:22px;line-height:1}
.stat-icon svg{width:20px;height:20px}
.stat-val{font-family:var(--sys);font-size:25px;font-weight:700;line-height:1}
.stat-val .stat-max{font-size:15px;font-weight:500;opacity:.85;margin-left:2px}
.stat-lbl{font-size:11px;text-transform:uppercase;letter-spacing:.04em;opacity:.9;margin-top:5px}
.info-hint.light{border-color:rgba(255,255,255,.6);background:rgba(255,255,255,.15);color:#fff}
.stat-card .info-hint{position:absolute;top:13px;right:13px}

.starsrow{display:flex;align-items:flex-start;gap:0;padding:4px 0 0}
.star-item{display:flex;flex-direction:column;align-items:center;gap:4px;flex-shrink:0}
.star-connector{flex:1;height:0;min-width:8px;border-top:1.5px dashed var(--line);margin-top:12px}
.star-icon{width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-size:19px;
  filter:grayscale(1);opacity:.35;line-height:1;flex-shrink:0}
.star-icon.unlocked{filter:none;opacity:1}
.star-day{font-size:8.5px;color:var(--ink-soft);white-space:nowrap}
.ach-mm-check{width:24px;height:24px;border-radius:50%;background:var(--surface-2);border:1.5px solid var(--line);
  display:flex;align-items:center;justify-content:center;color:var(--ink-soft);font-size:11px;cursor:pointer;flex-shrink:0}
.ach-mm-check.active{background:var(--sage);border-color:var(--sage);color:#fff;cursor:default}

.mm-cardrow{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-bottom:10px}
.mm-card{display:flex;flex-direction:column;align-items:flex-start;gap:10px}
.mm-card-top{display:flex;align-items:center;gap:10px}
.mm-card-icon{width:34px;height:34px;border-radius:50%;background:rgba(170,157,184,.3);display:flex;align-items:center;justify-content:center;flex-shrink:0;color:#6E4FA0}
.mm-card-icon svg{width:21px;height:21px}
.mm-card-title{font-family:var(--font-head);font-weight:600;font-size:14.5px;color:var(--ink)}
.mm-card-status{display:inline-flex;align-items:center;gap:5px;font-size:11.5px;font-weight:600;border-radius:999px;padding:4px 10px}
.mm-card-status svg{width:11px;height:11px;flex-shrink:0}
.mm-card-status.locked{color:var(--ink-soft);background:var(--surface-2)}
.mm-card-status.open{color:var(--lav-ink);background:rgba(170,157,184,.16)}
.mm-card-status.done{color:var(--sage-ink);background:rgba(106,153,147,.14)}

.hc-row{display:flex;align-items:center;gap:12px;width:100%;background:var(--surface);border:1px solid var(--line);
  border-radius:16px;padding:14px 16px;margin:4px 0 14px;box-shadow:var(--shadow);cursor:pointer;font-family:var(--sys);text-align:left}
.hc-row-icon{width:38px;height:38px;border-radius:50%;background:var(--surface-2);display:flex;align-items:center;justify-content:center;flex-shrink:0;color:var(--ink-soft)}
.hc-row-icon svg{width:17px;height:17px}
.hc-row-body{flex:1;min-width:0}
.hc-row-title{font-family:var(--font-head);font-weight:600;font-size:15.5px;color:var(--ink)}
.hc-row-sub{font-size:12px;color:var(--ink-soft);margin-top:1px}
.hc-row-chev{width:16px;height:16px;color:var(--ink-soft);flex-shrink:0}
.mm-inline{margin-top:0;padding-top:0;border-top:none;max-height:0;opacity:0;overflow:hidden;transition:max-height .2s ease,opacity .2s ease,margin-top .2s ease,padding-top .2s ease}
.mm-inline.open{margin-top:14px;padding-top:12px;border-top:1px solid var(--line);max-height:140px;opacity:1}
.mm-inline-hint{font-size:11px;color:var(--ink-soft);margin:0 0 8px}

.mm-row{display:flex;align-items:center;gap:8px;padding:10px 0;border-top:1px solid var(--line)}
.mm-row:first-child{border-top:none}
.mm-title{font-size:13px;font-weight:600;flex-shrink:0;width:104px}
.mm-field{flex:1}
.mm-input{width:100%;font-size:13px;padding:8px 10px;border:1px solid var(--line);border-radius:10px;font-family:var(--sys)}
.mm-input:focus{outline:none;border-color:var(--lav);box-shadow:0 0 0 3px rgba(170,157,184,.18)}
.mm-err{font-size:10.5px;color:#a8443a;margin:3px 0 0}
.mm-btn{font-size:12.5px;font-weight:600;padding:8px 12px;border-radius:10px;border:none;background:var(--lav);color:#fff;cursor:pointer;flex-shrink:0;font-family:var(--sys)}
.mm-done{font-size:12.5px;color:var(--sage-ink);font-weight:600;flex:1}

.mm-code-habits-row{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin:4px 0 14px}
.mm-code-habits-row.single{grid-template-columns:1fr}
.mm-half{display:flex;flex-direction:column;gap:8px}
.mm-half-title{font-family:var(--sys);font-weight:700;font-size:11.5px;color:var(--ink-soft);text-transform:uppercase;letter-spacing:.03em;margin:0}
.mm-half .mm-row{display:flex;flex-direction:column;align-items:stretch;gap:8px;padding:0;border-top:none}
.mm-half .mm-field{width:100%}
.mm-half .mm-btn{width:100%;padding:9px 12px;text-align:center}
.mm-half .mm-done{font-size:12px}
.hc-row-compact{position:relative;display:grid;grid-template-columns:auto 1fr auto;
  grid-template-areas:"icon title chev" "icon sub chev";align-items:center;column-gap:12px;row-gap:1px;
  background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:12px 14px;
  box-shadow:var(--shadow);cursor:pointer;font-family:var(--sys);text-align:left;width:100%}
.hc-row-compact .hc-row-icon{grid-area:icon;width:48px;height:48px;margin:0;
  background:rgba(170,157,184,.3);color:#6E4FA0}
.hc-row-compact .hc-row-icon svg{width:25px;height:25px}
.hc-row-compact .hc-row-title{grid-area:title;align-self:end}
.hc-row-compact .hc-row-sub{grid-area:sub;align-self:start}
.hc-row-compact .hc-row-chev{grid-area:chev}

/* сегодня */
.chanchat-row{display:flex;gap:8px;justify-content:center;margin:0 0 14px}
.chanchat-btn{display:flex;align-items:center;gap:6px;font-family:var(--sys);font-size:12.5px;font-weight:600;
  color:var(--lav-ink);background:rgba(170,157,184,.12);border-radius:999px;padding:8px 16px;
  text-decoration:none;flex:1;max-width:160px;justify-content:center}
.chanchat-btn svg{width:14px;height:14px;flex-shrink:0}
.affirm-card{position:relative;display:flex;align-items:center;gap:16px;overflow:hidden;
  background:linear-gradient(90deg,var(--sage),var(--lav));border-radius:18px;padding:20px 22px;
  box-shadow:var(--shadow);color:#fff;margin:10px 0 18px}
.affirm-date{flex:0 0 auto;text-align:center;line-height:1;position:relative;z-index:1}
.affirm-date .num{font-family:var(--font-head);font-size:38px;font-weight:600;line-height:1}
.affirm-date .mon{font-size:10.5px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;opacity:.85;margin-top:3px}
.affirm-divider{flex:0 0 auto;width:1px;align-self:stretch;background:rgba(255,255,255,.4)}
.affirm-body{flex:1;min-width:0;position:relative;z-index:1;text-align:center}
.affirm-body .txt{font-family:var(--font-head);font-style:italic;font-size:19px;font-weight:600;line-height:1.3}
.checklist{background:var(--surface);border:1px solid var(--line);border-radius:18px;padding:10px 12px;box-shadow:var(--shadow);margin-bottom:16px}
.hab{display:flex;align-items:center;gap:12px;padding:11px 10px;border-radius:12px;margin:2px 0;transition:.15s;cursor:pointer;-webkit-user-select:none;user-select:none;background:var(--surface-2)}
.hab.done{background:rgba(170,157,184,.14)}
.habits-card .checklist{background:none;border:none;box-shadow:none;padding:12px 0 0;margin-bottom:0}
.markvisual{width:24px;height:24px;border-radius:50%;border:1.3px solid var(--ink-soft);background:var(--surface);
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
.hab-text{font-size:15px;flex:1}
.hab-mins{font-size:12px;color:var(--ink-soft);font-weight:400}
.hab.done .hab-text{color:var(--lav-ink);text-decoration:line-through;font-weight:500}
.hab-streak{font-size:11.5px;color:var(--sand-ink);font-weight:600;white-space:nowrap;flex-shrink:0}
.checklist-head{display:flex;justify-content:space-between;align-items:center;margin:4px 0 8px}
.checklist-head h3{font-size:13.5px;font-family:var(--sys);font-weight:600;color:var(--ink-soft)}
.day-status{font-size:13.5px;color:var(--ink-soft);margin:0 0 12px;line-height:1.4}
.day-status b{color:var(--ink);font-weight:700}
.habits-card{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:16px;margin:4px 0 14px;box-shadow:var(--shadow)}
.habits-card-top{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin-bottom:10px}
.habits-card-top h3{margin:0;font-family:var(--font-head);font-weight:600;font-size:20px;color:var(--ink);line-height:1.25}
.hc-edit{display:inline-flex;align-items:center;gap:6px;background:var(--surface-2);border:none;border-radius:999px;
  padding:8px 14px;font-size:12.5px;font-weight:600;color:var(--lav-ink);cursor:pointer;font-family:var(--sys);flex-shrink:0}
.hc-edit svg{width:13px;height:13px;flex-shrink:0}
.hc-progress-row{font-size:13px;color:var(--ink-soft);margin-bottom:8px}
.hc-progress-row b{color:var(--sage-ink);font-weight:700}
.hc-bar-row{display:flex;align-items:center;gap:10px;margin-bottom:4px}
.hc-bar{flex:1;height:9px;border-radius:999px;background:var(--surface-2);overflow:hidden}
.hc-bar i{display:block;height:100%;border-radius:999px;background:var(--sage);transition:width .4s}
.hc-bar-pct{font-size:12.5px;font-weight:700;color:var(--ink-soft);flex-shrink:0}
.hc-time-row{display:flex;align-items:center;gap:12px;padding-top:12px;margin-top:8px;border-top:1px solid var(--line)}
.hc-time-icon{width:38px;height:38px;border-radius:50%;background:rgba(106,153,147,.14);display:flex;align-items:center;justify-content:center;flex-shrink:0}
.hc-time-icon svg{width:17px;height:17px;color:var(--sage-ink)}
.hc-time-val{font-family:var(--font-head);font-weight:600;font-size:18px;color:var(--ink);text-transform:uppercase;letter-spacing:.04em}
.hc-time-sub{font-size:11.5px;color:var(--ink-soft);margin-top:1px}
.hc-time-num{min-width:58px;height:54px;padding:0 8px;border-radius:14px;background:var(--sage);color:#fff;display:flex;flex-direction:column;align-items:center;justify-content:center;flex-shrink:0;box-shadow:0 6px 14px -6px rgba(63,102,96,.55)}
.hc-time-num b{font-family:var(--font-head);font-weight:700;font-size:24px;line-height:1}
.hc-time-num b.long{font-size:20px}
.hc-time-num span{font-family:var(--sys);font-size:9.5px;font-weight:600;letter-spacing:.04em;text-transform:uppercase;opacity:.9;margin-top:3px}
.habits-card-status{display:flex;align-items:flex-start;gap:8px;margin-top:10px;padding-top:10px;
  border-top:1px solid var(--line);font-size:13.5px;color:var(--ink-soft);line-height:1.4}
.hab-badge{display:inline-flex;align-items:center;gap:4px;font-size:11.5px;font-weight:600;color:var(--sage-ink);
  background:rgba(106,153,147,.14);border-radius:999px;padding:5px 10px;flex-shrink:0;white-space:nowrap}
.hab-badge svg{width:11px;height:11px;flex-shrink:0}
.hab-badge.sleep{color:var(--lav-ink);background:rgba(170,157,184,.16)}
.hcs-pill{font-size:12px;font-weight:700;color:var(--sage-ink);background:rgba(106,153,147,.13);
  border-radius:999px;padding:3px 10px;flex-shrink:0;margin-top:1px}
.editlink{background:none;border:none;color:var(--lav-ink);font-size:13.5px;font-weight:600;cursor:pointer;font-family:var(--sys)}
.checklist-head .editlink{background:var(--surface);border:1.5px solid var(--line);border-radius:999px;
  padding:7px 14px;box-shadow:0 2px 6px -2px rgba(0,0,0,.08)}
@media (max-width:899px){
  .habits-row{flex-direction:column;align-items:stretch;gap:10px}
}
.pickcat{margin-bottom:20px}
.pickcat h4{font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:var(--ink-soft);margin-bottom:9px}
.chipgrid{display:flex;flex-wrap:wrap;gap:8px}
.habchip{display:inline-flex;align-items:center;gap:6px;padding:8px 8px 8px 14px;border:1.5px solid var(--line);
  border-radius:999px;cursor:pointer;background:var(--surface);transition:background-color .15s,border-color .15s}
.habchip span.htxt{font-size:13px;color:var(--ink)}
.habchip.checked{background:var(--sage);border-color:var(--sage)}
.habchip.checked span.htxt{color:#fff}
.habchip .hinfo{width:19px;height:19px;border-radius:50%;border:1.5px solid var(--lav);background:var(--surface);color:var(--lav-ink);
  font-size:11px;font-weight:700;flex-shrink:0;cursor:pointer;display:flex;align-items:center;justify-content:center}
.habchip.checked .hinfo{background:rgba(255,255,255,.3);color:#fff}
.pickintro{background:var(--surface);padding:0 0 16px;font-size:12.5px;color:var(--ink-soft);text-align:center;line-height:1.5}
.pickintro p{margin:0 0 8px}
.pickintro p:last-child{margin-bottom:0;color:var(--sage-ink);font-weight:600}
.addhabit-btn{display:inline-flex;align-items:center;gap:6px;border:1.5px dashed var(--lav);border-radius:999px;
  background:var(--surface);color:var(--lav-ink);font-size:13px;font-weight:600;padding:9px 16px;
  cursor:pointer;font-family:var(--sys);margin-top:6px;transition:transform .1s}
.addhabit-btn:active{transform:scale(.97)}
.custom-row{display:flex;gap:8px;margin-bottom:18px}
.custom-row input{flex:1;font-family:var(--sys);font-size:13.5px;border:1px solid var(--line);
  border-radius:12px;padding:10px 13px;color:var(--ink);background:var(--surface)}
.custom-row button{border:none;background:var(--lav);color:#fff;border-radius:12px;padding:0 16px;font-weight:600;cursor:pointer}
.pick-total{text-align:center;font-size:12.5px;font-weight:600;color:var(--sage-ink);margin:0 0 10px}
.savepick{width:100%;border:none;background:var(--lav);color:#fff;border-radius:999px;padding:13px;font-weight:600;
  font-size:13.5px;cursor:pointer;position:sticky;bottom:0}
.actions{display:flex;gap:10px;margin-top:16px;margin-bottom:10px}
.btn{flex:1;border:none;border-radius:999px;padding:13px;font-family:var(--sys);font-weight:600;
  font-size:14px;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:7px}
.btn svg{width:15px;height:15px}
.btn.primary{background:var(--lav);color:#fff}
.btn.primary.grad{background:linear-gradient(90deg,var(--lav),var(--sage))}
.btn.primary:disabled{background:var(--surface-2);color:var(--ink-soft);cursor:default}
.btn.primary.is-closed{background:var(--surface-2);color:var(--ink-soft)}
.btn.ghost{background:var(--surface);border:1px solid var(--line);color:var(--ink)}
.share-out{margin-top:14px;padding-bottom:calc(22px + env(safe-area-inset-bottom,0px))}
.share-out img{width:100%;border-radius:16px;border:1px solid var(--line);box-shadow:var(--shadow)}
@media (min-width:900px){
  .share-out img{width:260px;display:block;margin:0 auto}
}
.share-note{font-size:12.5px;font-weight:600;color:var(--lav-ink);margin-top:10px;line-height:1.5;text-align:center}
.share-divider{display:flex;align-items:center;gap:10px;margin:16px 0 12px;color:var(--ink-soft);font-size:10.5px;
  font-weight:600;text-transform:uppercase;letter-spacing:.05em}
.share-divider::before,.share-divider::after{content:"";flex:1;height:1px;background:var(--line)}
.share-links{display:flex;flex-direction:column;gap:8px;max-width:240px;margin:0 auto}
.share-links .btn{font-size:12px;padding:9px 14px}
.share-links .btn svg{width:14px;height:14px}
.evening-rem{display:flex;gap:10px;border:1.5px dashed var(--line);border-radius:14px;padding:13px 15px;margin-top:14px}
.evening-rem svg{width:17px;height:17px;color:var(--sage-ink);flex-shrink:0;margin-top:1px}
.evening-rem .lead{font-size:13px;font-weight:600;color:var(--ink);margin-bottom:4px}
.evening-rem .txt{font-size:12.5px;color:var(--ink-soft);line-height:1.4}

/* материалы */
a.evt-row{display:flex;text-decoration:none;color:inherit;cursor:pointer}
.evt-row{display:flex;align-items:center;gap:10px;padding:13px;background:var(--surface);border:1px solid var(--line);box-shadow:var(--shadow);border-radius:16px;margin:10px 0}
.evt-row:first-of-type{border-top:none}
.evt-date{width:48px;flex-shrink:0;text-align:center;border-radius:12px;padding:8px 4px;color:#fff}
.evt-date .d{font-family:var(--font-head);font-size:23px;font-weight:600;line-height:1;color:#fff}
.evt-date .l{font-size:9px;text-transform:lowercase;color:#fff;opacity:.85;margin-top:2px}
.evt-date.c-sage{background:var(--sage)}
.evt-date.c-lav{background:linear-gradient(135deg,var(--lav),#C9A9DE)}
.evt-icon{width:32px;height:32px;border-radius:50%;background:var(--surface-2);display:flex;align-items:center;justify-content:center;flex-shrink:0}
.evt-icon svg{width:15px;height:15px;color:var(--ink-soft)}
.evt-body{flex:1;min-width:0}
.evt-tag{display:block;font-size:10.5px;color:var(--ink-soft);margin-bottom:1px}
.evt-ttl{font-size:13.5px;font-weight:600}
.evt-meta{font-size:11px;color:var(--ink-soft);margin-top:1px}
.evt-go{color:#fff;font-size:12px;font-weight:600;border-radius:999px;padding:9px 15px;flex-shrink:0;white-space:nowrap}
.evt-go.c-sage{background:var(--sage)}
.evt-go.c-lav{background:linear-gradient(135deg,var(--lav),#C9A9DE)}
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
}
.search-filter-row{display:flex;gap:8px;align-items:flex-start;position:relative;margin:10px 0 16px}
.search-wrap{position:relative;flex:1;margin:0}
.search-box{width:100%;font-size:14px;color:var(--ink);background:var(--surface);border:1px solid var(--line);
  border-radius:14px;padding:13px 16px 13px 42px;box-shadow:var(--shadow);font-family:var(--sys)}
.search-icon{position:absolute;left:15px;top:50%;transform:translateY(-50%);width:16px;height:16px;color:var(--ink-soft)}
.search-drop{position:absolute;left:0;right:0;top:calc(100% + 6px);background:var(--surface);border:1px solid var(--line);
  border-radius:14px;box-shadow:var(--shadow);overflow:hidden;z-index:30}
.search-drop[hidden]{display:none}
.sr-item{display:flex;gap:10px;padding:11px 14px;cursor:pointer;font-size:13px;border-top:1px solid var(--line)}
.sr-item:first-child{border-top:none}
.sr-item:hover{background:var(--surface-2)}
.filters-toggle{display:flex;align-items:center;gap:10px;width:100%;background:var(--surface);border:1px solid var(--line);
  border-radius:14px;padding:13px 16px;font-size:14px;font-weight:600;color:var(--ink);cursor:pointer;
  font-family:var(--sys);box-shadow:var(--shadow);margin:0 0 16px}
.filters-toggle-icon{display:flex;color:var(--sage-ink);flex-shrink:0}
.filters-toggle-icon svg{width:17px;height:17px}
.filters-toggle-label{flex:1;text-align:left}
.filters-toggle-chev{display:flex;color:var(--ink-soft);flex-shrink:0;transition:transform .15s}
.filters-toggle-chev svg{width:16px;height:16px}
.filters-toggle.active{border-color:var(--sage);color:var(--sage-ink)}
.filters-toggle.active .filters-toggle-icon{color:var(--sage-ink)}
.filters-toggle.open .filters-toggle-chev{transform:rotate(180deg)}
.filters-panel{background:var(--surface-2);border:1px solid var(--line);border-radius:16px;padding:14px;margin:-10px 0 16px;
  display:flex;flex-direction:column;gap:12px}
.filters-panel[hidden]{display:none}
.filters-field{display:flex;align-items:center;justify-content:space-between;gap:10px}
.filters-label{font-size:13.5px;color:var(--ink-soft);flex-shrink:0}
.filters-select-wrap{position:relative;flex:1;max-width:200px}
.filters-select{width:100%;appearance:none;-webkit-appearance:none;font-family:var(--sys);font-size:13.5px;color:var(--ink);
  background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:9px 30px 9px 12px;cursor:pointer}
.filters-select-chev{position:absolute;right:10px;top:50%;transform:translateY(-50%);width:14px;height:14px;
  color:var(--ink-soft);pointer-events:none}
.filters-actions{display:flex;align-items:center;justify-content:space-between;gap:10px;padding-top:4px;border-top:1px solid var(--line)}
.filters-reset{background:none;border:none;font-family:var(--sys);font-size:13px;font-weight:600;color:var(--lav-ink);
  cursor:pointer;padding:6px 0}
.filters-apply{background:var(--sage);border:none;border-radius:999px;color:#fff;font-family:var(--sys);font-size:13.5px;
  font-weight:600;padding:10px 22px;cursor:pointer}
.folder-back{display:inline-flex;align-items:center;gap:4px;font-size:12.5px;font-weight:600;color:var(--ink-soft);
  cursor:pointer;margin:2px 0 8px}
.filter-row{display:flex;flex-direction:column;gap:10px;margin:18px 0}
.filter-row .chipset{display:grid;grid-template-columns:repeat(auto-fill,minmax(102px,1fr));gap:10px}
.flabel{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--ink-soft);width:100%}
.chip{font-size:12.5px;font-weight:500;color:var(--ink);background:var(--surface);border:1px solid var(--line);
  border-radius:999px;padding:6px 13px;cursor:pointer;font-family:var(--sys)}
.chip.active{background:var(--lav);border-color:var(--lav);color:#fff}
.chip.active.theme{background:var(--sage);border-color:var(--sage)}
.chip.folder{display:flex;align-items:center;justify-content:center;gap:6px;text-align:center;font-size:13px;
  font-weight:600;border-radius:14px;padding:14px 10px;min-height:46px}
.chip.folder.active{background:linear-gradient(135deg,var(--lav),var(--sage));border-color:transparent;color:#fff}
.chip.folder[data-f="__fav__"] svg{width:15px;height:15px;flex-shrink:0;color:#D9707A}
.chip.folder.active[data-f="__fav__"] svg{color:#fff}
.mgrid{display:flex;flex-direction:column;gap:10px;background:var(--surface);border-radius:18px;padding:12px;box-shadow:var(--shadow)}
.mcard{background:var(--surface);border:1px solid var(--line);box-shadow:0 2px 8px -4px rgba(51,46,61,.18);border-radius:14px;padding:16px;cursor:pointer;
  display:flex;gap:14px;align-items:flex-start}
.mcard[hidden]{display:none}
.dot{width:36px;height:36px;border-radius:50%;flex-shrink:0;overflow:hidden}
.dot img{width:100%;height:100%;object-fit:cover}
.mcard.mila .dot{background:var(--lav)} .mcard.maria .dot{background:var(--sage)}
.mcard.anna .dot{background:var(--sand)} .mcard.extra .dot{background:linear-gradient(135deg,var(--lav),var(--sage) 55%,var(--sand))}
.mcard .body{flex:1;min-width:0}
.mcard h3{font-size:16.5px;font-weight:600;text-transform:uppercase}
.mcard .topics{font-size:12px;color:var(--ink-soft);margin-top:2px}
.mcard .foot{display:flex;justify-content:space-between;font-size:11.5px;color:var(--ink-soft);margin-top:8px}
.mcard .count{font-weight:600;color:var(--ink)}

/* профиль */
.avatar-row{display:flex;flex-direction:column;align-items:center;margin:14px 0 20px}
.avatar{width:84px;height:84px;border-radius:50%;background:linear-gradient(135deg,var(--lav),var(--sand));
  display:flex;align-items:center;justify-content:center;color:#fff;position:relative;cursor:pointer}
.avatar svg{width:28px;height:28px}
.avatar .remove{position:absolute;top:-2px;right:-2px;width:22px;height:22px;border-radius:50%;background:var(--surface);
  border:1.5px solid var(--line);display:flex;align-items:center;justify-content:center;cursor:pointer;color:var(--ink-soft)}
.avatar .remove svg{width:11px;height:11px}
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
.evcard{background:var(--surface);border:1px solid var(--line);border-radius:18px;overflow:hidden;box-shadow:var(--shadow);margin-bottom:12px}
.evcard .ph{height:150px;background:linear-gradient(135deg,var(--sage),var(--lav));background-size:cover;background-position:center}
.evcard .b{display:flex;gap:14px;padding:16px}
.evcard .datebadge{flex-shrink:0;width:64px;min-height:56px;border-radius:14px;background:var(--surface-2);
  display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;padding:6px 4px}
.evcard .datebadge .num{font-family:var(--font-head);font-size:30px;font-weight:600;color:var(--lav-ink);line-height:1}
.evcard .datebadge .mon{font-size:10.5px;color:var(--ink-soft);margin-top:2px}
.evcard .info{flex:1;min-width:0}
.evcard .info h4{font-family:var(--sys);font-size:15.5px;font-weight:600;margin:0 0 4px}
.evcard .place{display:flex;align-items:center;gap:4px;font-size:12.5px;color:var(--ink-soft);margin-bottom:8px}
.evcard .place svg{width:13px;height:13px;flex-shrink:0}
.evcard .desc{font-size:13px;color:var(--ink-soft);line-height:1.5;margin:0 0 8px}
.evcard a{display:inline-flex;align-items:center;gap:5px;font-size:12.5px;font-weight:600;color:var(--lav-ink);text-decoration:none}

/* overlays – центрированное модальное окно с цветной шапкой */
.overlay{position:fixed;inset:0;background:rgba(26,21,33,.5);display:flex;align-items:center;justify-content:center;
  z-index:40;opacity:0;pointer-events:none;transition:.2s;padding:20px}
.overlay.open{opacity:1;pointer-events:auto}
.panel{position:relative;background:var(--surface);width:100%;max-width:460px;border-radius:24px;overflow:hidden;
  max-height:min(640px,86vh);display:flex;flex-direction:column;transform:scale(.95);transition:.2s;
  box-shadow:0 30px 70px -24px rgba(0,0,0,.45)}
.overlay.open .panel{transform:scale(1)}
.panel-head2{position:relative;background:linear-gradient(135deg,var(--lav),var(--sage) 130%);padding:20px 50px 18px 22px;color:#fff;flex-shrink:0}
.panel-head2.has-back{padding-left:64px}
.panel-head2.has-back .close-x{display:none}
.panel-head2 .eyebrow2{font-family:var(--sys);font-style:italic;font-size:12px;opacity:.85;margin:0 0 4px}
.panel-head2 h2{font-size:19px;color:#fff}
.close-x{position:absolute;top:50%;right:10px;transform:translateY(-50%);background:rgba(255,255,255,.22);border:none;width:40px;height:40px;
  border-radius:50%;color:#fff;font-size:15px;cursor:pointer;display:flex;align-items:center;justify-content:center}
.back-arrow{position:absolute;top:50%;left:10px;transform:translateY(-50%);background:rgba(255,255,255,.22);border:none;width:40px;height:40px;
  border-radius:50%;color:#fff;font-size:17px;cursor:pointer;display:flex;align-items:center;justify-content:center}
#overlayBody{padding:18px 22px calc(22px + env(safe-area-inset-bottom,0px));overflow-y:auto}
.info-panel{max-width:400px;max-height:min(480px,80vh)}
.info-close{top:14px;transform:none;background:var(--surface-2);border:1px solid var(--line);color:var(--ink-soft)}
#infoOverlayBody{padding:46px 22px calc(20px + env(safe-area-inset-bottom,0px));overflow-y:auto}
#infoOverlayBody .info-title{font-size:18px;font-weight:600;color:var(--ink);margin:0 0 12px}
.info-meta{display:flex;gap:8px;margin-bottom:12px}
.info-meta>div{flex:1;background:var(--surface-2);border-radius:12px;padding:9px 12px}
.info-meta .lbl{font-size:10.5px;text-transform:uppercase;letter-spacing:.05em;color:var(--sage-ink);font-weight:600;margin-bottom:3px}
.info-meta .val{font-size:12.5px;color:var(--ink);line-height:1.3}
#infoOverlayBody .info-desc{font-size:13px;color:var(--ink-soft);line-height:1.55}
#infoOverlayBody .info-rules{margin-top:10px}
#infoOverlayBody .info-rule{font-size:13px;color:var(--ink);padding:10px 0;border-top:1px solid var(--line)}
#infoOverlayBody .info-rule:first-child{border-top:none;padding-top:4px}
.info-why{margin-top:14px;padding-top:14px;border-top:1px solid var(--line)}
.info-why-lbl{font-size:10.5px;text-transform:uppercase;letter-spacing:.05em;color:var(--sage-ink);font-weight:600;margin-bottom:5px}
.info-why-txt{font-size:12.5px;color:var(--ink-soft);line-height:1.55;font-style:italic}
.sub{color:var(--ink-soft);font-size:13px;margin:2px 0 18px}
.mat{display:flex;align-items:center;gap:12px;padding:13px 14px;background:var(--surface);border:1px solid var(--line);box-shadow:0 2px 8px -4px rgba(51,46,61,.18);border-radius:14px;margin:7px 0}
.mat:first-of-type{border-top:none}
.mat.is-locked{opacity:.55}
.mat-icon{width:34px;height:34px;border-radius:10px;background:var(--surface-2);flex-shrink:0;display:flex;align-items:center;justify-content:center}
.mat-icon svg{width:16px;height:16px;flex-shrink:0}
.mat-body{flex:1;min-width:0}
.mat-title{font-size:14px;font-weight:500;line-height:1.35}
.mat-tags{font-size:12.5px;color:var(--ink-soft);margin-top:3px}
.mat-embed{margin:4px 0 10px;position:relative}
.mat-embed-loading{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;background:#fff;border-radius:12px;z-index:1;transition:opacity .25s ease}
.mat-embed-loading.hide{opacity:0;pointer-events:none}
.mat-spinner{width:26px;height:26px;border-radius:50%;border:3px solid var(--line);border-top-color:var(--sage);animation:matSpin .8s linear infinite}
@keyframes matSpin{to{transform:rotate(360deg)}}
.mat-embed-fallback{display:block;text-align:center;font-size:11.5px;color:var(--ink-soft);margin-top:8px;flex-shrink:0}
.panel.force-tall{height:min(640px,86vh)}
#overlayBody.no-scroll{overflow:hidden;display:flex;flex-direction:column;flex:1;min-height:0}
#overlayBody.no-scroll > *{flex-shrink:0}
#overlayBody.no-scroll .mat-embed.is-frame{flex:1;min-height:0;display:flex;flex-direction:column;margin-bottom:0}
.mat-embed.is-frame iframe{flex:1;width:100%;min-height:0;border:1px solid var(--line);border-radius:12px;display:block}
.mat-status{font-size:12px;font-weight:600;padding:4px 10px;border-radius:999px;white-space:nowrap;align-self:flex-start;display:inline-flex;align-items:center;gap:4px}
.mat-status svg{width:11px;height:11px;flex-shrink:0}
.mat-status.open{background:var(--surface-2);color:var(--sage-ink)}
.mat-status.locked{background:var(--surface-2);color:var(--ink-soft)}
.mat-new{display:inline-block;margin-left:6px;font-size:10px;font-weight:700;color:#fff;background:var(--lav);
  border-radius:999px;padding:2px 7px;vertical-align:middle}
.note{font-size:12.5px;color:var(--ink-soft);background:var(--surface-2);border-radius:10px;padding:10px 14px;margin-top:4px;line-height:1.5}
.mlist-title{display:flex;align-items:center;gap:10px;margin:4px 0 10px}
.mlist-title::before,.mlist-title::after{content:"";flex:1;height:1px;background:var(--line)}
.mlist-title span{font-family:var(--font-head);font-weight:600;font-size:15px;letter-spacing:.02em;text-transform:uppercase;color:var(--ink);white-space:nowrap}
.mat-more-btn{display:block;width:100%;margin:6px 0 4px;padding:11px;border:1.5px solid var(--line);border-radius:999px;
  background:var(--surface);color:var(--lav-ink);font-size:13px;font-weight:600;cursor:pointer;font-family:var(--sys)}
.mat-feature{display:flex;align-items:center;gap:12px;width:100%;text-align:left;padding:14px;background:var(--surface);
  border:1px solid var(--line);box-shadow:var(--shadow);border-radius:16px;margin:12px 0;cursor:pointer;font-family:var(--sys)}
.mat-feature-icon{width:48px;height:48px;border-radius:50%;background:var(--surface-2);display:flex;align-items:center;
  justify-content:center;flex-shrink:0;color:var(--sage-ink)}
.mat-feature-icon svg{width:22px;height:22px}
.mat-feature-body{flex:1;min-width:0}
.mat-feature-title{font-family:var(--font-head);font-weight:600;font-size:15px;color:var(--ink);text-transform:uppercase}
.mat-feature-desc{font-size:12px;color:var(--ink-soft);margin-top:3px;line-height:1.4}
.mat-feature-arrow{width:36px;height:36px;border-radius:50%;background:rgba(170,157,184,.16);display:flex;align-items:center;
  justify-content:center;flex-shrink:0;color:var(--lav-ink)}
.mat-feature-arrow svg{width:16px;height:16px}
.fav-card{display:flex;align-items:center;gap:12px;width:100%;text-align:left;padding:14px;background:var(--surface);
  border:1px solid var(--line);box-shadow:var(--shadow);border-radius:16px;margin:14px 0;cursor:pointer;font-family:var(--sys)}
.fav-card-icon{width:48px;height:48px;border-radius:50%;background:rgba(170,157,184,.16);display:flex;align-items:center;
  justify-content:center;flex-shrink:0;color:var(--lav-ink)}
.fav-card-icon svg{width:22px;height:22px}
.fav-card .fav-card-icon{background:rgba(217,112,122,.13);color:#D9707A}
.fav-card .fav-card-icon svg{fill:rgba(217,112,122,.22)}
.fav-card-title{flex:1;min-width:0;font-family:var(--font-head);font-weight:600;font-size:18px;color:var(--ink);line-height:1.3;text-transform:uppercase;letter-spacing:.04em}
.fav-card-btn{display:flex;align-items:center;gap:4px;flex-shrink:0;background:var(--surface-2);border:none;border-radius:999px;
  padding:9px 16px;font-family:var(--sys);font-size:13px;font-weight:600;color:var(--lav-ink)}
.fav-card-btn svg{width:14px;height:14px}
.mitem{position:relative;display:flex;align-items:center;gap:12px;padding:12px;background:var(--surface);border:1px solid var(--line);box-shadow:var(--shadow);border-radius:16px;margin:10px 0;cursor:pointer}
.mitem.is-locked{opacity:.55;cursor:default}
.mitem-fav{position:absolute;top:8px;right:8px;width:30px;height:30px;border:none;background:transparent;display:flex;align-items:center;justify-content:center;color:var(--ink-soft);cursor:pointer;z-index:1}
.mitem-fav svg{width:18px;height:18px}
.mitem-fav.on{color:#E0525F}
.mitem-thumb{width:58px;height:58px;border-radius:13px;flex-shrink:0;display:flex;align-items:center;justify-content:center;position:relative}
.mitem-play{width:28px;height:28px;border-radius:50%;background:rgba(255,255,255,.92);display:flex;align-items:center;justify-content:center}
.mitem-play svg{width:12px;height:12px;color:var(--ink);margin-left:1px}
.mitem-doc svg{width:20px;height:20px;color:#fff}
.mitem-body{flex:1;min-width:0}
.mitem-title{font-size:14px;font-weight:600;line-height:1.3;padding-right:26px}
.mitem-author{font-size:12px;color:var(--ink-soft);margin-top:2px}
.mitem-meta{display:flex;align-items:center;gap:5px;font-size:11.5px;color:var(--ink-soft);margin-top:4px}
.mitem-meta svg{width:12px;height:12px;flex-shrink:0}

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
.hm-backdrop{position:fixed;inset:0;z-index:55;background:rgba(51,46,61,.35);backdrop-filter:blur(4px);-webkit-backdrop-filter:blur(4px);
  display:flex;align-items:center;justify-content:center;padding:16px;opacity:0;pointer-events:none;transition:.2s}
.hm-backdrop.open{opacity:1;pointer-events:auto}
.hm-card{max-width:440px;width:100%;border-radius:24px;padding:24px;background:var(--surface);
  display:flex;flex-direction:column;gap:20px;transform:scale(.96);transition:.2s;
  box-shadow:0 30px 70px -24px rgba(51,46,61,.4);max-height:88vh;overflow-y:auto;font-family:var(--sys)}
.hm-backdrop.open .hm-card{transform:scale(1)}
.hm-head{display:flex;justify-content:space-between;align-items:center}
.hm-title{font-weight:500;font-size:18px;color:var(--ink)}
.hm-close{background:none;border:none;color:var(--ink-soft);font-size:16px;cursor:pointer;line-height:1;padding:4px}
@media (hover:hover){ .hm-close:hover{color:var(--ink)} }
.hm-row1{display:flex;align-items:center;gap:12px}
.hm-nameinput{width:100%;height:48px;border-radius:16px;padding:0 16px;background:var(--surface-2);
  border:1px solid var(--line);font-weight:500;font-size:14px;color:var(--ink);font-family:var(--sys)}
.hm-nameinput::placeholder{color:var(--ink-soft)}
.hm-days-head{font-weight:500;font-size:14px;color:var(--ink)}
.hm-days-sub{font-size:12px;color:var(--ink-soft);margin:1px 0 10px}
.hm-daysgrid{display:flex;align-items:center;justify-content:space-between;gap:4px}
.hm-day{width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center;
  font-size:12px;font-weight:500;transition:.15s;border:none;cursor:pointer;background:var(--surface-2);color:var(--ink-soft);font-family:var(--sys)}
.hm-day.active{background:var(--lav);color:#fff}
@media (hover:hover){ .hm-day:not(.active):hover{background:var(--line)} }
.hm-qty{background:var(--surface-2);border-radius:16px;padding:16px;border:1px solid var(--line);display:flex;flex-direction:column;gap:0}
.hm-qty-head{display:flex;justify-content:space-between;align-items:center;gap:10px}
.hm-qty-title{font-weight:500;font-size:14px;color:var(--ink)}
.hm-qty-sub{font-size:12px;color:var(--ink-soft);margin-top:2px}
.hm-switch{position:relative;width:42px;height:24px;border-radius:999px;background:var(--line);
  border:none;cursor:pointer;flex-shrink:0;transition:.2s;padding:0}
.hm-switch::after{content:"";position:absolute;top:2px;left:2px;width:20px;height:20px;border-radius:50%;
  background:#fff;box-shadow:0 1px 3px rgba(51,46,61,.3);transition:.2s}
.hm-switch.on{background:var(--lav)}
.hm-switch.on::after{transform:translateX(18px)}
.hm-qty-expand{display:flex;flex-direction:column;gap:12px;overflow:hidden;max-height:300px;opacity:1;
  transition:max-height .25s ease,opacity .2s ease,padding-top .25s ease,margin-top .25s ease;
  padding-top:12px;margin-top:12px;border-top:1px solid var(--line)}
.hm-qty-expand.collapsed{max-height:0;opacity:0;padding-top:0;margin-top:0;border-top-color:transparent;pointer-events:none}
.hm-qty-line{display:flex;justify-content:space-between;align-items:center}
.hm-qty-line .lbl{font-size:14px;color:var(--ink-soft)}
.hm-stepper{display:inline-flex;align-items:center;gap:12px;background:var(--surface-2);border-radius:999px;padding:4px 12px;font-size:14px;font-weight:500;color:var(--ink)}
.hm-stepper button{background:none;border:none;font-size:16px;color:var(--ink);cursor:pointer;width:18px;height:18px;
  display:flex;align-items:center;justify-content:center;line-height:1;font-family:var(--sys)}
.hm-stepper-input{border:none;background:transparent;text-align:center;width:34px;font-size:14px;font-weight:500;
  color:var(--ink);font-family:var(--sys);padding:0;-moz-appearance:textfield}
.hm-stepper-input::-webkit-outer-spin-button,.hm-stepper-input::-webkit-inner-spin-button{-webkit-appearance:none;margin:0}
.hm-unit-input{border:1px solid var(--line);background:var(--surface);border-radius:999px;text-align:right;
  font-size:14px;font-weight:500;color:var(--ink);width:84px;padding:5px 12px;font-family:var(--sys)}
.hm-unit-input::placeholder{color:var(--ink-soft)}
.hm-unit-input:focus{outline:none;border-color:var(--lav)}
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

.authscreen{max-width:400px;margin:0 auto;min-height:100%;display:flex;flex-direction:column;
  align-items:center;justify-content:center;padding:32px 20px;box-sizing:border-box}
.authscreen[hidden]{display:none}
.authscreen .mark{width:140px;object-fit:contain;margin-bottom:16px}
.authscreen h1{font-family:var(--font-head);font-weight:600;font-size:32px;color:var(--sage-ink);margin:0 0 8px;text-align:center}
.authscreen .sub{font-size:14.5px;color:var(--lav-ink);text-align:center;margin-bottom:24px;line-height:1.5}
.authcard{background:var(--surface);border:1px solid var(--line);border-radius:24px;padding:28px;
  box-shadow:0 20px 44px -22px rgba(51,46,61,.22);width:100%}
.authcard .field{margin-bottom:18px}
.authcard .field label{display:block;font-size:15px;font-weight:500;color:var(--ink);margin-bottom:8px}
.authcard .field input{width:100%;font-family:var(--sys);font-size:14.5px;color:var(--ink);background:var(--surface-2);
  border:1px solid var(--line);border-radius:14px;padding:15px 44px 15px 17px}
.authcard .field input::placeholder{color:var(--ink-soft)}
.pwd-wrap{position:relative}
.pwd-eye{position:absolute;right:8px;top:50%;transform:translateY(-50%);background:none;border:none;
  display:flex;align-items:center;justify-content:center;cursor:pointer;padding:8px;color:var(--sage-ink);line-height:1}
.pwd-eye svg{width:20px;height:20px}
.auth-err{font-size:12px;color:#a8443a;margin:0 0 10px;line-height:1.4}
.auth-err[hidden]{display:none}
#authForgotWrap{text-align:right;margin:-8px 0 20px!important}
#authForgotWrap button{text-decoration:underline}
.auth-submit{width:100%;border:none;background:linear-gradient(90deg,var(--sage),var(--lav));color:#fff;border-radius:999px;padding:17px;
  font-weight:600;font-size:16px;cursor:pointer;font-family:var(--sys);margin-top:4px}
.auth-submit:disabled{opacity:.6;cursor:default}
.auth-switch{text-align:center;font-size:14px;color:var(--ink);margin-top:20px}
.auth-switch button{background:none;border:none;color:var(--lav-ink);font-weight:600;cursor:pointer;font-family:var(--sys);font-size:14px;padding:0;text-decoration:underline}
.auth-loading{text-align:center;color:var(--ink-soft);font-size:13px;padding:40px 0}
</style>

<div class="authscreen" id="authScreen" hidden>
  <img class="mark" src="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/logo.png" alt="Serenity Moon">
  <h1 id="authTitle">Вход в кабинет</h1>
  <p class="sub" id="authSub">Клуб «Начни сегодня!» · Serenity Moon</p>
  <form class="authcard" id="authForm">
    <p class="auth-err" id="authErr" hidden></p>
    <div class="field"><label>Email</label><input type="email" id="authEmail" name="email" autocomplete="email" placeholder="Введите email"></div>
    <div class="field"><label>Пароль</label><div class="pwd-wrap">
      <input type="password" id="authPassword" name="password" autocomplete="current-password" placeholder="Введите пароль">
      <button type="button" class="pwd-eye" id="authPwdEye" aria-label="показать пароль"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M2 12s3.8-7 10-7 10 7 10 7-3.8 7-10 7-10-7-10-7Z"/><circle cx="12" cy="12" r="3"/></svg></button>
    </div></div>
    <div class="field" id="authPassword2Field" hidden><label>Повтори пароль</label><div class="pwd-wrap">
      <input type="password" id="authPassword2" name="password2" autocomplete="new-password" placeholder="Повтори пароль">
      <button type="button" class="pwd-eye" id="authPwdEye2" aria-label="показать пароль"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M2 12s3.8-7 10-7 10 7 10 7-3.8 7-10 7-10-7-10-7Z"/><circle cx="12" cy="12" r="3"/></svg></button>
    </div></div>
    <p class="auth-switch" id="authForgotWrap" style="margin:-2px 0 4px"><button id="authForgotBtn" type="button">Забыла пароль?</button></p>
    <button class="auth-submit" id="authSubmit" type="submit">Войти</button>
  </form>
  <p class="auth-switch"><span id="authSwitchHint">Первый раз?</span> <button id="authSwitchMode" type="button">Зарегистрироваться</button></p>
</div>

<div class="authscreen" id="recoverScreen" hidden>
  <img class="mark" src="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/logo.png" alt="Serenity Moon">
  <h1>Восстановление пароля</h1>
  <p class="sub">Пришлём ссылку на email, по ней можно будет задать новый пароль</p>
  <form class="authcard" id="recoverForm">
    <p class="auth-err" id="recoverErr" hidden></p>
    <div class="field"><label>Email</label><input type="email" id="recoverEmail" name="email" autocomplete="email" placeholder="Введите email"></div>
    <button class="auth-submit" id="recoverSubmit" type="submit">Отправить ссылку</button>
  </form>
  <p class="auth-switch"><button id="recoverBack" type="button">← назад ко входу</button></p>
</div>

<div class="authscreen" id="newPasswordScreen" hidden>
  <img class="mark" src="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/logo.png" alt="Serenity Moon">
  <h1>Новый пароль</h1>
  <p class="sub">Придумай новый пароль для входа в кабинет</p>
  <form class="authcard" id="newPasswordForm">
    <p class="auth-err" id="newPasswordErr" hidden></p>
    <div class="field"><label>Новый пароль</label><div class="pwd-wrap">
      <input type="password" id="newPassword1" name="password" autocomplete="new-password" placeholder="Введите пароль">
      <button type="button" class="pwd-eye" id="newPwdEye1" aria-label="показать пароль"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M2 12s3.8-7 10-7 10 7 10 7-3.8 7-10 7-10-7-10-7Z"/><circle cx="12" cy="12" r="3"/></svg></button>
    </div></div>
    <div class="field"><label>Повтори пароль</label><div class="pwd-wrap">
      <input type="password" id="newPassword2" name="password2" autocomplete="new-password" placeholder="Повтори пароль">
      <button type="button" class="pwd-eye" id="newPwdEye2" aria-label="показать пароль"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M2 12s3.8-7 10-7 10 7 10 7-3.8 7-10 7-10-7-10-7Z"/><circle cx="12" cy="12" r="3"/></svg></button>
    </div></div>
    <button class="auth-submit" id="newPasswordSubmit" type="submit">Сохранить и войти</button>
  </form>
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
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 20V14.5M9.5 20V9M15 20V12M20 20V5.5"/></svg>
      Прогресс</button>
    <button class="navbtn" data-tab="materials">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="4" y="4" width="7" height="7" rx="1.5"/><rect x="13" y="4" width="7" height="7" rx="1.5"/><rect x="4" y="13" width="7" height="7" rx="1.5"/><rect x="13" y="13" width="7" height="7" rx="1.5"/></svg>
      Материалы</button>
    <button class="navbtn" data-tab="schedule">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="3.5" y="5" width="17" height="15" rx="2"/><path d="M3.5 9.5h17M8 3v4M16 3v4"/></svg>
      Расписание</button>
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
      <h1 class="page-title">Мой прогресс</h1>
      <div class="marathon-eyebrow"><span>Клуб «Начни сегодня!»</span></div>
      <div class="hcard dayhero heatcard">
        <div class="dh-top"><span class="dh-title">День <span id="bigDayTxt">8</span> из 21</span><span class="dh-pct" id="dayPct">38%</span></div>
        <div class="dh-pill"><i id="dayPillFill"></i></div>
        <div class="dh-sub" id="daySub"></div>
      </div>
      <div class="statrow">
        <div class="stat-card sage-grad" id="diamondCard">
          <button class="info-hint light" id="diamondInfoBtn" type="button" aria-label="как начисляются бриллианты">?</button>
          <div class="stat-top">
            <div class="stat-icon">💎</div>
            <div class="stat-val"><span id="diamondVal">0</span><span class="stat-max">/27</span></div>
          </div>
          <div class="stat-lbl">Кристаллы</div>
        </div>

        <div class="stat-card lav-grad" id="countdownCard">
          <div class="stat-top">
            <div class="stat-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="3.5" y="5" width="17" height="15" rx="2"/><path d="M3.5 9.5h17M8 3v4M16 3v4"/></svg></div>
            <div class="stat-val"><span id="countdownVal">0</span></div>
          </div>
          <div class="stat-lbl">Дней до финала</div>
        </div>
      </div>

      <div class="heatcard" id="heatmapCard" style="position:relative">
        <h3>Твой путь — 21 день</h3>
        <button class="info-hint" id="heatmapInfoBtn" type="button" aria-label="что значит цвет" style="position:absolute;top:13px;right:13px">?</button>
        <div class="weeklbl" id="weeklbl1">неделя 1</div>
        <div class="heatgrid" id="heatgrid1"></div>
        <div class="weeklbl" id="weeklbl2">неделя 2</div>
        <div class="heatgrid" id="heatgrid2"></div>
        <div class="weeklbl" id="weeklbl3">неделя 3</div>
        <div class="heatgrid" id="heatgrid3"></div>
      </div>

      <div class="heatcard" id="achCard">
        <h3>Ближайшие достижения<button class="info-hint" id="achInfoBtn" type="button" aria-label="как открываются звёзды и галочки за мастермайнд">?</button></h3>
        <div class="starsrow" id="starsRow"></div>
      </div>

      <div class="mm-cardrow" id="mmCardRow"></div>

      <div class="mm-code-habits-row" id="mmCodeHabitsRow">
        <div class="heatcard mm-half" id="mmCodeCard" hidden>
          <h4 class="mm-half-title" id="mmCodeTitle">Кодовое слово</h4>
          <div id="mmCodeBody"></div>
        </div>

        <button class="heatcard mm-half hc-row-compact" id="editHabitsBtn" type="button">
          <div class="hc-row-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="4" y="5" width="16" height="15" rx="2.5"/><path d="M8 10h8M8 14h5"/></svg></div>
          <div class="hc-row-title">Мои привычки</div>
          <div class="hc-row-sub" id="profileHabitsCount">Выбрано 5 привычек</div>
          <svg class="hc-row-chev" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m9 6 6 6-6 6"/></svg>
        </button>
      </div>

      <button class="btn primary grad" id="progressContinueBtn" type="button" style="width:100%;margin-bottom:14px;display:flex;align-items:center;justify-content:center;gap:8px">
        Продолжить
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M13 6l6 6-6 6"/></svg>
      </button>

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
      <h1 class="page-title">Сегодня</h1>
      <div class="marathon-eyebrow"><span>Клуб «Начни сегодня!»</span></div>
      <div class="affirm-card">
        <div class="affirm-date"><div class="num" id="affirmDateNum"></div><div class="mon" id="affirmDateMon"></div></div>
        <div class="affirm-divider"></div>
        <div class="affirm-body"><div class="txt" id="affirmText"></div></div>
      </div>
      <div class="chanchat-row">
        <a class="chanchat-btn" id="chanLinkBtn" target="_blank" rel="noopener">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M21.5 4.2 2.9 11.4c-.9.4-.9 1.6.1 1.9l4.6 1.5 1.8 5.4c.3.8 1.3 1 1.9.4l2.6-2.5 4.7 3.5c.7.5 1.7.1 1.9-.7L23 5.6c.2-1-.7-1.8-1.5-1.4Z"/></svg>
          Канал</a>
        <a class="chanchat-btn" id="chatLinkBtn" target="_blank" rel="noopener">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 12c0-4.4 3.8-8 8.5-8s8.5 3.6 8.5 8-3.8 8-8.5 8c-1.1 0-2.2-.2-3.1-.6L4 20l1.1-4.3C4.4 14.6 4 13.3 4 12Z"/></svg>
          Чат</a>
      </div>
      <div class="evt-alert" id="todayEventAlert" hidden>
        <span class="evt-alert-dot"></span>
        <span class="evt-alert-txt" id="todayEventTxt"></span>
        <span class="evt-alert-tag">Zoom</span>
      </div>
      <div class="habits-card">
        <div class="habits-card-top">
          <h3 id="habitsHeading">Мои привычки</h3>
          <button class="hc-edit" id="addHabitBtn" type="button">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M16.5 4.5 19.5 7.5 8 19H5V16Z"/></svg>
            Изменить</button>
        </div>
        <div id="hcSummary" hidden>
          <div class="hc-progress-row"><b id="hcDoneNum">0</b> из <b id="hcTotalNum">0</b> выполнено</div>
          <div class="hc-bar-row"><div class="hc-bar"><i id="hcBarFill" style="width:0%"></i></div><span class="hc-bar-pct" id="hcBarPct">0%</span></div>
          <div class="hc-time-row" id="hcTimeRow" hidden>
            <div class="hc-time-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2"/></svg></div>
            <div style="flex:1;min-width:0"><div class="hc-time-val" id="hcTimeVal">Время для себя любимой</div><div class="hc-time-sub" id="hcTimeSub" hidden>Без учёта сна</div></div>
            <div class="hc-time-num"><b id="hcTimeNum"></b><span id="hcTimeUnit">всего мин</span></div>
          </div>
        </div>
        <div class="habits-card-status"><span class="hcs-pill" id="dayStatusPill" hidden></span><span id="dayStatus"></span></div>
        <div class="checklist" id="checklist"></div>
      </div>
      <div class="evening-rem" id="eveningRem" hidden>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>
        <div>
          <div class="lead">вечер – хорошее время заглянуть к себе</div>
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
      <button class="fav-card" id="favMaterialsBtn" type="button">
        <span class="fav-card-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 20.5s-7.5-4.6-9.8-9.3C.7 7.8 2.4 4.5 5.7 4c2-.3 3.8.7 4.8 2.3C11.5 4.7 13.3 3.7 15.3 4c3.3.5 5 3.8 3.5 7.2C16.5 15.9 12 20.5 12 20.5Z"/></svg></span>
        <span class="fav-card-title">Избранные материалы</span>
        <span class="fav-card-btn">Смотреть <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m9 6 6 6-6 6"/></svg></span>
      </button>
    </section>

    <section class="screen" id="tab-materials" hidden>
      <h1 class="page-title">Материалы</h1>
      <div class="marathon-eyebrow"><span>Клуб «Начни сегодня!»</span></div>
      <div class="mat-layout">
        <div class="mat-main">
          <div class="search-filter-row">
            <div class="search-wrap">
              <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/></svg>
              <input class="search-box" id="searchBox" type="text" placeholder="Поиск материалов" autocomplete="off">
              <div class="search-drop" id="searchDrop" hidden></div>
            </div>
          </div>
          <button class="filters-toggle" id="filtersToggleBtn" type="button">
            <span class="filters-toggle-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 7h16M4 12h16M4 17h16"/><circle cx="8" cy="7" r="2" fill="currentColor" stroke="none"/><circle cx="13" cy="12" r="2" fill="currentColor" stroke="none"/><circle cx="16" cy="17" r="2" fill="currentColor" stroke="none"/></svg></span>
            <span class="filters-toggle-label" id="filtersToggleLabel">Фильтры материалов</span>
            <span class="filters-toggle-chev"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m6 9 6 6 6-6"/></svg></span>
          </button>
          <div class="filters-panel" id="filtersPanel" hidden>
            <div class="filters-field">
              <span class="filters-label">Формат</span>
              <div class="filters-select-wrap">
                <select id="formatSelect" class="filters-select">
                  <option value="all">Любой</option>
                  <option value="video">Видео</option>
                  <option value="audio">Аудио</option>
                  <option value="pdf">PDF</option>
                </select>
                <svg class="filters-select-chev" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m6 9 6 6 6-6"/></svg>
              </div>
            </div>
            <div class="filters-field">
              <span class="filters-label">Категория</span>
              <div class="filters-select-wrap">
                <select id="categorySelect" class="filters-select">
                  <option value="all">Любая</option>
                </select>
                <svg class="filters-select-chev" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m6 9 6 6 6-6"/></svg>
              </div>
            </div>
            <div class="filters-actions">
              <button type="button" class="filters-reset" id="filtersResetBtn">Сбросить</button>
              <button type="button" class="filters-apply" id="filtersApplyBtn">Применить</button>
            </div>
          </div>
          <div class="filter-row"><span class="flabel">по папке</span><div class="chipset" id="folderChips"></div></div>
          <button class="mat-feature" id="matFeatureBtn" type="button">
            <div class="mat-feature-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M12 21V11"/><path d="M12 11c0-3.2 2.2-5.5 5.5-5.5 0 3.2-2.3 5.5-5.5 5.5Z"/><path d="M12 14c0-3-2-5-5-5 0 3 2 5 5 5Z"/></svg></div>
            <div class="mat-feature-body">
              <div class="mat-feature-title">Перед началом клуба</div>
              <div class="mat-feature-desc">Важная информация, как получить максимальную пользу от материалов</div>
            </div>
            <div class="mat-feature-arrow"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M13 6l6 6-6 6"/></svg></div>
          </button>
          <div id="mflat"></div>
        </div>
      </div>
    </section>

    <section class="screen" id="tab-schedule" hidden>
      <h1 class="page-title">Расписание</h1>
      <div class="marathon-eyebrow"><span>Клуб «Начни сегодня!»</span></div>
      <div class="heatcard cal-card">
        <div class="cal-head">
          <h3 id="calMonthLabel"></h3>
          <div class="cal-nav">
            <button type="button" id="calPrevBtn" aria-label="предыдущий месяц">&#8249;</button>
            <button type="button" id="calNextBtn" aria-label="следующий месяц">&#8250;</button>
          </div>
        </div>
        <div class="cal-weekdays"><span>Пн</span><span>Вт</span><span>Ср</span><span>Чт</span><span>Пт</span><span>Сб</span><span>Вс</span></div>
        <div class="cal-grid" id="calGrid"></div>
        <div class="cal-legend" id="calLegend"></div>
      </div>
      <div id="eventsList"></div>
    </section>

    <section class="screen" id="tab-profile" hidden>
      <h1 class="page-title">Профиль</h1>
      <div class="marathon-eyebrow"><span>Клуб «Начни сегодня!»</span></div>
      <input type="file" id="avatarFileInput" accept="image/*" hidden>
      <div class="avatar-row">
        <div class="avatar" id="mainAvatar">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="8.5" r="3.5"/><path d="M4.5 20c1.5-4 4.5-6 7.5-6s6 2 7.5 6"/></svg>
          <div class="remove" hidden><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M6 6l12 12M18 6 6 18"/></svg></div>
          <div class="camera"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="7" width="18" height="13" rx="2.5"/><circle cx="12" cy="13.5" r="3.5"/><path d="M8 7l1.5-2.5h5L16 7"/></svg></div>
        </div>
      </div>

      <div class="profcard v3" id="v3tile">
        <div class="v3-txt">Ника Кузнецова<small>34 · Durham · редактировать</small></div>
        <svg class="chev" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m9 6 6 6-6 6"/></svg>
      </div>

      <div class="sectitle">мероприятия</div>
      <div id="profileMilaEvents"></div>
      <button class="editlink" id="logoutBtn" type="button" style="display:block;margin:18px auto 0">Выйти из кабинета</button>
    </section>
  </div></main>

  <nav class="bottomnav"><div class="inner">
    <button class="navbtn active" data-tab="today">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="5"/><path d="M12 2.5v2.5M12 19v2.5M4.5 12H2M22 12h-2.5M5.5 5.5l1.8 1.8M16.7 16.7l1.8 1.8M18.5 5.5l-1.8 1.8M7.3 16.7l-1.8 1.8"/></svg>
      Сегодня</button>
    <button class="navbtn" data-tab="home">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 20V14.5M9.5 20V9M15 20V12M20 20V5.5"/></svg>
      Прогресс</button>
    <button class="navbtn" data-tab="materials">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="4" y="4" width="7" height="7" rx="1.5"/><rect x="13" y="4" width="7" height="7" rx="1.5"/><rect x="4" y="13" width="7" height="7" rx="1.5"/><rect x="13" y="13" width="7" height="7" rx="1.5"/></svg>
      Материалы</button>
    <button class="navbtn" data-tab="schedule">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="3.5" y="5" width="17" height="15" rx="2"/><path d="M3.5 9.5h17M8 3v4M16 3v4"/></svg>
      Расписание</button>
    <button class="navbtn" data-tab="profile">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="8.5" r="3.5"/><path d="M4.5 20c1.5-4 4.5-6 7.5-6s6 2 7.5 6"/></svg>
      Профиль</button>
  </div></nav>
</div>

<div class="overlay" id="overlay"><div class="panel">
  <div class="panel-head2">
    <button class="back-arrow" id="overlayBackBtn" type="button" aria-label="назад" hidden>&#8592;</button>
    <button class="close-x" id="closePanel" type="button">&#10005;</button>
    <p class="eyebrow2" id="overlayEyebrow" hidden></p>
    <h2 id="overlayTitle"></h2>
  </div>
  <div id="overlayBody"></div>
</div></div>

<div class="overlay" id="infoOverlay"><div class="panel info-panel">
  <button class="close-x info-close" id="closeInfoPanel" type="button">&#10005;</button>
  <div id="infoOverlayBody"></div>
</div></div>

<div class="overlay" id="shareOverlay"><div class="panel info-panel">
  <button class="close-x info-close" id="closeShareOverlay" type="button">&#10005;</button>
  <div id="shareOverlayBody"></div>
</div></div>

<div class="fd-overlay" id="fdOverlay"><div class="fd-card">
  <svg class="fd-moon" viewBox="0 0 24 24" fill="none"><path d="M20 14.5A8.5 8.5 0 1 1 9.5 4a7 7 0 0 0 10.5 10.5Z" fill="#fff"/></svg>
  <p class="fd-day">день <span id="fdDay">8</span> завершён</p>
  <h2 class="fd-praise" id="fdPraise">Ты держишь ритм ✨</h2>
  <div class="fd-stat"><div class="num"><span id="fdDone">0</span>/<span id="fdTotal">5</span></div><div class="lbl">привычек выполнено сегодня</div></div>
  <p class="fd-quote">«Маленькие шаги каждый день создают большие результаты»</p>
  <button class="fd-btn" id="fdClose">
    <svg viewBox="0 0 24 24" fill="none"><path d="M20 14.5A8.5 8.5 0 1 1 9.5 4a7 7 0 0 0 10.5 10.5Z" fill="currentColor"/></svg>
    <span id="fdCloseText">Ура! &#127881; Ты это сделала!</span></button>
</div></div>

<div class="star-overlay" id="starOverlay"><div class="star-card">
  <div class="star-eyebrow" id="starEyebrow">неделя 1 пройдена</div>
  <div class="star-big">⭐</div>
  <h2 id="starTitle">Поздравляю, ты прошла первую неделю в клубе – и за это получаешь звезду</h2>
  <div class="star-progress" id="starProgress"></div>
  <button id="starClose" type="button">Забрать звезду</button>
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
      <div class="hm-qty-line" id="habitMinutesLine">
        <span class="lbl">сколько минут занимает</span>
        <input class="hm-unit-input" id="habitMinutesInput" type="number" inputmode="numeric" min="0" placeholder="мин.">
      </div>
      <div class="hm-qty-head" style="margin-top:14px;padding-top:14px;border-top:1px solid var(--line)">
        <div>
          <div class="hm-qty-title">учитывать количество</div>
          <div class="hm-qty-sub">число за день – стаканы, км, шаги...</div>
        </div>
        <button class="hm-switch" id="habitQtyToggle" type="button"></button>
      </div>
      <div class="hm-qty-expand collapsed" id="habitQtyExpand">
        <div class="hm-qty-line">
          <span class="lbl">количество</span>
          <span class="hm-stepper">
            <button type="button" data-step="-1" data-target="target">–</button>
            <input class="hm-stepper-input" id="habitTargetVal" type="number" inputmode="numeric" min="1" value="8">
            <button type="button" data-step="1" data-target="target">+</button>
          </span>
        </div>
      </div>
    </div>

    <p class="note" id="habitModalWarn" hidden style="margin:0">Уже выбрано максимум 5 привычек – сначала убери одну</p>

    <div class="hm-footer">
      <button class="hm-save" id="habitSaveBtn" type="button">сохранить</button>
      <button class="hm-delete" id="habitDeleteBtn" type="button">удалить</button>
    </div>
  </div>
</div>

<script>
/* ---------- блокировка зума на десктопе (моб. зум убран через viewport) ---------- */
document.addEventListener("wheel", e=>{ if(e.ctrlKey) e.preventDefault(); }, {passive:false});
document.addEventListener("keydown", e=>{
  if((e.ctrlKey||e.metaKey) && ["+","-","=","0"].includes(e.key)) e.preventDefault();
});

/* ---------- каталог привычек (справочник, не пользовательские данные) ---------- */
const CATALOG = {
  "Тело": [
    {name:"Полноценный сон",when:"Каждую ночь",dur:"7-8 час.",noTime:true,desc:"Ложитесь спать до 10-11 вечера. Это поможет вам лучше спать и повысить уровень энергии по утрам. Поддержание постоянного циркадного ритма чрезвычайно полезно для здоровья.",why:"Во сне тело восстанавливается, а мозг закрепляет всё, что ты узнала и прожила за день – без этого остальные привычки держатся хуже."},
    {name:"Стакан тёплой воды",when:"Утро",dur:"1 мин.",desc:"Натощак выпивайте 1 стакан тёплой воды (можно с лимоном). Запускает организм и помогает ему проснуться.",why:"После ночи организм немного обезвожен – вода мягко запускает обмен веществ и пищеварение."},
    {name:"100 прыжков на месте",when:"Утро",dur:"2 мин.",desc:"Опускайтесь на полную стопу. Отличный вариант запуска организма по утрам.",why:"Короткая физическая встряска утром усиливает кровообращение и помогает проснуться без кофеина."},
    {name:"Пить 1,5-2 литра воды",when:"В течение дня",dur:"1,5-2 л.",desc:"Пей воду маленькими порциями на протяжении дня, не дожидаясь жажды. Поддерживает энергию, кожу и работу всех систем организма.",why:"Даже лёгкое обезвоживание снижает концентрацию и энергию – регулярная вода держит тело в тонусе весь день."},
    {name:"Контрастный/холодный душ",when:"Утро",dur:"5 мин.",desc:"Принятие холодного душа укрепляет иммунитет, стимулирует кровообращение, повышает энергию и настроение, снимает воспаления.",why:"Резкая смена температуры тренирует сосуды и нервную систему – со временем это ощущается как прилив бодрости."},
    {name:"Массаж щёткой",when:"Ежедневно",dur:"3-5 мин.",desc:"Не менее 1 раза, лучше 2 раза утром и вечером. Улучшает лимфоток и кровообращение в тканях.",why:"Лимфоток не имеет собственного «насоса» и двигается только за счёт движения – сухой массаж стимулирует его механически."},
    {name:"10 000 шагов в день",when:"Ежедневно",dur:"В течение дня",desc:"Помогает укрепить мышечный каркас, поддерживать вес и улучшает снабжение тканей кислородом.",why:"Регулярная ходьба – один из самых простых способов поддерживать сердце и обмен веществ без нагрузки на суставы."},
    {name:"Регулярные тренировки",when:"2-3 раза в неделю",dur:"20-40 мин.",desc:"Укрепляют сердце и лёгкие, повышают выносливость и физическую силу, поддерживают здоровый вес и способствуют психологическому благополучию.",why:"Физическая нагрузка не только укрепляет тело, но и снижает уровень стресса через выброс эндорфинов."},
    {name:"Опускание на пятки",when:"Любое время дня",dur:"2-3 мин.",desc:"Приподняться на 2-3 см и резко опускаться на пятки. Выполняется в быстром темпе.",why:"Короткая вибрация через пятки мягко стимулирует кровообращение и лимфоток, не отнимая много времени."},
    {name:"Простукивание тимуса",when:"Любое время дня",dur:"2-3 мин.",desc:"Тимус играет ключевую роль в развитии и функционировании иммунных клеток. Регулярное простукивание улучшает кровообращение и лимфообращение, а также стимулирует его функции.",why:"Простой телесный ритуал, который помогает поддерживать иммунитет без дополнительных затрат времени."},
    {name:"Тряска ног и рук лёжа на спине",when:"Ежедневно",dur:"2-3 мин.",desc:"Помогает улучшить кровообращение, стимулирует лимфатическую систему, способствует снятию напряжения и стресса, повышает энергию и снимает усталость.",why:"Мягкая встряска телу помогает «сбросить» напряжение, накопленное за день, без нагрузки на суставы."},
    {name:"Растирание ушей",when:"Регулярно",dur:"1-2 мин.",desc:"Массаж ушной раковины, где находятся акупунктурные точки. Снимает напряжение, улучшает кровообращение, снижает стресс и головную боль.",why:"На ушах сосредоточено много акупунктурных точек – короткий массаж быстро снимает напряжение."},
    {name:"Стояние на аппликаторе (Кузнецова/Ляпко/Садху)",when:"Утро",dur:"2-10 мин.",desc:"Статичный массаж ступней способствует улучшению кровообращения, снятию напряжения и боли, стимуляции эндорфинов и повышению энергии.",why:"Воздействие на точки стоп с утра бодрит не хуже чашки кофе, без нагрузки на нервную систему."},
    {name:"Массаж живота «Наули»",when:"Регулярно",dur:"5-10 мин.",desc:"Вакуум живота помогает стимуляции пищеварительной системы, улучшению обмена веществ и снятию напряжения.",why:"Мягкая стимуляция живота поддерживает пищеварение и снимает лишнее напряжение в этой зоне."}
  ],
  "Питание": [
    {name:"Сбалансированная еда",when:"Ежедневно",dur:"Всегда",desc:"Полезное питание – ключевой элемент здорового образа жизни. Стремитесь к сбалансированному соотношению углеводов, белков и жиров. Слушайте своё тело.",why:"Стабильное питание убирает резкие скачки энергии и голода, из-за которых труднее держать остальные привычки."},
    {name:"Без сахара",when:"Ежедневно",dur:"Весь день",desc:"Старайся избегать сахара и сладких перекусов в течение дня. Меньше резких скачков энергии – и легче держать остальные привычки.",why:"Резкий подъём сахара в крови даёт короткий всплеск энергии и такой же резкий спад – без него день ровнее."},
    {name:"Меньше кофе",when:"Ежедневно",dur:"Весь день",desc:"Ограничь кофе 1-2 чашками в день, особенно во второй половине дня. Меньше тревожности и лучше сон.",why:"Кофеин во второй половине дня может мешать засыпанию, даже если ты этого не замечаешь."}
  ],
  "Уход и красота": [
    {name:"Ванна с Эпсом солью",when:"1 раз в неделю",dur:"40-60 мин.",desc:"Отличный вариант снять стресс, расслабиться, повысить тонус кожи и улучшить обмен веществ.",why:"Тёплая вода расслабляет мышцы, а сама пауза даёт нервной системе время переключиться с режима «делать» на «отдыхать»."},
    {name:"Полоскание кокосовым маслом",when:"Регулярно",dur:"5-7 мин.",desc:"Держите ложку кокосового масла во рту сколько сможете. Отличный детокс, а ещё помогает отбелить зубы.",why:"Простой ритуал ухода, который легко встроить в уже существующую утреннюю рутину, не добавляя отдельного времени."},
    {name:"Уход за лицом",when:"Ежедневно",dur:"10-15 мин.",desc:"Ежедневный массаж лица и зоны декольте. Прекрасный способ сохранить молодость и красоту.",why:"Уход работает по накоплению – эффект заметен не сразу, а именно от повторения, а не от одного раза."}
  ],
  "Спокойствие": [
    {name:"Йога и медитации",when:"Ежедневно",dur:"30-40 мин.",desc:"Улучшает общее состояние здоровья, включает техники дыхания и медитации, способствующие расслаблению ума и тела.",why:"Осознанное дыхание переключает нервную систему из режима тревоги в режим покоя за считанные минуты."},
    {name:"Дыхательная практика",when:"Любое время дня",dur:"3-5 мин.",desc:"Несколько глубоких осознанных вдохов и выдохов. Быстро снижает напряжение и возвращает внимание в тело.",why:"Медленный выдох напрямую сигналит нервной системе, что можно расслабиться – самый быстрый доступный инструмент против стресса."},
    {name:"Дневник благодарностей",when:"Ежедневно",dur:"5-10 мин.",desc:"Помогает сместить фокус внимания с негативных аспектов жизни на позитивные, замечать и ценить мелочи и радости.",why:"Фокус на хорошем – тренируемый навык: чем чаще его замечаешь, тем легче это даётся со временем."},
    {name:"Улыбайся своему отражению в зеркале",when:"В любое время",dur:"Сколько хотите",desc:"Расслабляет мышцы лица и шеи, помогает на разных уровнях учиться принимать и любить себя.",why:"Маленький жест заботы к себе, который со временем меняет внутренний разговор с собой."},
    {name:"Быть на природе",when:"Как можно чаще",dur:"Сколько хотите",desc:"Ходить босыми ногами по траве, обниматься с деревьями :)",why:"Контакт с природой – один из самых простых и доступных способов снизить уровень стресса."}
  ],
  "Развитие": [
    {name:"Прочитать новое",when:"Регулярно",dur:"30-45 мин.",desc:"Всего 30 минут чтения в день могут развить вашу концентрацию и критическое мышление.",why:"Чтение – небольшая тренировка для концентрации, которая слабеет от бесконечного скролла."},
    {name:"Изучение иностранного языка",when:"Регулярно",dur:"10-20 мин.",desc:"Повышает когнитивные способности, развивает мозг, улучшает память и способствует саморазвитию.",why:"Новый язык – нагрузка для мозга, похожая на тренировку тела, только для памяти и мышления."}
  ],
  "Близкие": [
    {name:"Время с семьёй/детьми",when:"Регулярно",dur:"Сколько хотите",desc:"Это важно! Качественное время с близкими – один из главных приоритетов.",why:"Качественное присутствие рядом с близкими – один из самых стабильных источников ощущения благополучия."}
  ],
  "Дом и порядок": [
    {name:"Уборка",when:"Регулярно",dur:"10-15 мин.",desc:"Немного порядка вокруг – немного порядка внутри. Не обязательно генеральная уборка, достаточно малого.",why:"Порядок вокруг снижает визуальный шум – меньше отвлекающих раздражителей для внимания."},
    {name:"Полить растения",when:"Регулярно",dur:"2-5 мин.",desc:"Забота о чём-то живом рядом с тобой – маленький, но приятный ритуал.",why:"Забота о чём-то живом – простой способ почувствовать себя нужной и внимательной, без лишних усилий."},
    {name:"Разобрать вещи",when:"Регулярно",dur:"10-15 мин.",desc:"Убери то, что давно не нужно. Меньше лишнего – больше свободного пространства и внимания.",why:"Лишние вещи вокруг незаметно забирают немного внимания каждый раз, когда попадаются на глаза."},
    {name:"Планирование",when:"Утро/Вечер",dur:"5-10 мин.",desc:"В твоей жизни будет меньше хаоса, если ты будешь планировать.",why:"Несколько минут планирования снимают тревогу неопределённости и экономят силы на решения в течение дня."}
  ],
  "Без вредного": [
    {name:"Меньше соцсетей",when:"Ежедневно",dur:"По желанию",desc:"Сократи бесцельный скролл – освободившееся время и внимание можно отдать себе.",why:"Бесцельный скролл истощает внимание почти так же, как многозадачность – пауза в нём освобождает ресурс для себя."}
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
const AFFIRMATIONS = ["Ты уже на верном пути","Твоя энергия становится яснее","Маленькие шаги ведут к большим переменам",
  "Ты заботишься о себе, и это важно","Доверяй своему ритму","Ты сияешь изнутри","Сегодня – хороший день для тишины","Ты становишься устойчивее с каждым днём","Дисциплина – это проявление любви к себе","Каждый день укрепляет твою решимость. Продолжай!"];
const AFFIRM_SUPPORT = ["Пропустить день – это нормально, ты не начинаешь с нуля","Вчера был непростой день – сегодня можно просто начать заново",
  "Один пропуск ничего не перечёркивает – ты всё ещё в пути","Заботиться о себе – не значит делать всё идеально"];
const AFFIRM_CELEBRATE = ["Так держать! Привычка становится частью твоей жизни.","Так держать, это уже настоящая привычка",
  "Твоя последовательность вдохновляет – продолжай в том же духе","Ты создаёшь привычку, которая работает на тебя.","Здорово! Шаг за шагом ты приближаешься к своей цели! ✨"];
const SOCIAL_OPENERS = [
  "Сегодня уже 15 человек нашли время позаботиться о себе и выполнили хотя бы 2 привычки 💛",
  "У тебя ещё есть время на себя сегодня – 15 человек уже нашли эти пару минут 💛",
  "15 человек сегодня выбрали заботу о себе вместо спешки 💛",
  "Маленький шаг – уже победа. сегодня его сделали 15 человек 💛",
  "Любовь к себе не требует больших шагов – можно начать всего с десяти минут 💛",
  "15 человек сегодня выбрали себя, хотя бы немного 💛",
  "Ты справишься, даже если день был непростым – 15 человек сегодня тоже справились 💛",
  "Ты не обязана успевать всё – сегодня 15 человек позаботились о себе хотя бы немного 💛",
  "Забота о себе может быть совсем маленькой – сегодня это доказали 15 человек 💛",
  "15 человек сегодня нашли время для себя – ты тоже умеешь это находить 💛"
];
const SOCIAL_CLOSERS = [
  "присоединяйся, когда будешь готова",
  "сделай свой шаг, когда будет удобно",
  "начни с того, что по силам сегодня",
  "просто попробуй – дальше будет легче",
  "просто вернись к себе, когда будет время"
];
function hashStr(s){ let h=0; for(let i=0;i<s.length;i++){ h=(h*31+s.charCodeAt(i))|0; } return Math.abs(h); }
function todayCount(){ return 9 + (hashStr("day"+TODAY) % 17); }

const STAR_DAYS = [7, 14, 21];
function renderStars(){
  const starNodes = STAR_DAYS.map(d=>{
    const unlocked = TODAY >= d;
    return `<div class="star-item"><div class="star-icon ${unlocked?'unlocked':''}">⭐</div><div class="star-day">${d} дней</div></div>`;
  });
  const mmNodes = MASTERMIND_MATERIALS.map(m=>{
    const done = !!(STATE.mastermind && STATE.mastermind[m.mmId]);
    return `<div class="star-item"><span class="ach-mm-check ${done?'active':''}">✓</span><div class="star-day">ММ ${m.mmId}</div></div>`;
  });
  document.getElementById("starsRow").innerHTML = [...starNodes, ...mmNodes].join('<div class="star-connector"></div>');
  renderMmCards();
  renderMmCodeCard();
}
const MM_ICON_LOCK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="5" y="10.5" width="14" height="9.5" rx="2"/><path d="M8 10.5V8a4 4 0 0 1 8 0v2.5"/></svg>';
const MM_ICON_CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12.5 9.5 17 19 7"/></svg>';
function mmStatus(m){
  const done = !!(STATE.mastermind && STATE.mastermind[m.mmId]);
  return done ? {cls:"done", icon:MM_ICON_CHECK, text:"Получено"} : {cls:"locked", icon:MM_ICON_LOCK, text:"Впереди"};
}
function renderMmCards(){
  document.getElementById("mmCardRow").innerHTML = MASTERMIND_MATERIALS.map(m=>{
    const s = mmStatus(m);
    return `<div class="heatcard mm-card">
      <div class="mm-card-top">
        <div class="mm-card-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"><path d="M14.6 4.6H5.2a1.7 1.7 0 0 0-1.7 1.7v10.2h16.2v-6.4"/><path d="M5.7 6.8v7.6h11.8v-4"/><path d="M1.8 16.5h19.6v.9a1.6 1.6 0 0 1-1.6 1.6H3.4a1.6 1.6 0 0 1-1.6-1.6z"/><path d="M10 16.6v.6h3.2v-.6"/><rect x="7.5" y="8.8" width="4.8" height="3.7" rx=".8"/><path d="m12.3 10 1.9-1.1v3.5l-1.9-1.1"/><circle cx="18.8" cy="5.2" r="3.7"/><g stroke-width=".85" transform="rotate(-45 18.8 5.2)"><rect x="16.5" y="4.3" width="2.8" height="1.8" rx=".9"/><rect x="18.3" y="4.3" width="2.8" height="1.8" rx=".9"/></g></svg></div>
        <div class="mm-card-title">Мастермайнд ${m.mmId}</div>
      </div>
      <span class="mm-card-status ${s.cls}">${s.icon}${s.text}</span>
    </div>`;
  }).join("");
}
function renderMmCodeCard(){
  const pending = MASTERMIND_MATERIALS.find(m => !(STATE.mastermind && STATE.mastermind[m.mmId]));
  const card = document.getElementById("mmCodeCard");
  const row = document.getElementById("mmCodeHabitsRow");
  card.hidden = !pending;
  if(row) row.classList.toggle("single", !pending);
  if(!pending) return;
  document.getElementById("mmCodeBody").innerHTML = mmCodeBoxHtml(pending.mmId);
  wireMmCodeBox(pending.mmId, ()=>{ renderStars(); renderProgress(); });
}
function mastermindBonus(){ return Object.keys(STATE.mastermind||{}).filter(k=>STATE.mastermind[k]).length * 3; }
function mmCodeBoxHtml(mmId){
  if(STATE.mastermind && STATE.mastermind[String(mmId)]) return `<div class="mm-row"><span class="mm-done">✓ бриллиант получен (+3 💎)</span></div>`;
  return `<div class="mm-row">
    <div class="mm-field"><input class="mm-input" type="text" placeholder="кодовое слово из эфира" id="mmInput${mmId}">
    <p class="mm-err" id="mmErr${mmId}" hidden>неверное слово</p></div>
    <button class="mm-btn" type="button" data-mmid="${mmId}">Получить 💎</button></div>`;
}
function wireMmCodeBox(mmId, onSuccess){
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
        if(onSuccess) onSuccess(); else openMaterialDetail("mastermind", mmId-1);
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
  showInfoSimple("Бриллианты 💎", `Набери <b>21–27 бриллиантов</b> – и участвуешь в розыгрыше подарков. Главное – не пропускать дни, идеальных случаев может и не быть.
    <div class="info-rules">
      <div class="info-rule">+1 за день со 100% привычек · максимум 21</div>
      <div class="info-rule">+3 за мастермайнд по кодовому слову · 2 эфира</div>
    </div>
    <p class="info-desc" style="font-style:italic;margin-top:12px;padding-top:12px;border-top:1px solid var(--line)">Будь честна с собой: цель клуба не в розыгрыше, а в том, чтобы изменить свою жизнь.</p>`);
});
document.getElementById("achInfoBtn").addEventListener("click", ()=>{
  playPopSound();
  showInfoSimple("Достижения", "Три звезды открываются по неделям – на 7-й, 14-й и 21-й день. Просто дойди до этого дня, и звезда станет твоей.<br><br>Две галочки «ММ» – за мастермайнды. Нажми на галочку и впиши кодовое слово, которое Мила назовёт на эфире – получишь +3 бриллианта.");
});
document.getElementById("heatmapInfoBtn").addEventListener("click", ()=>{
  playPopSound();
  showInfoSimple("Карта 21 дня", "Чем темнее квадратик – тем больше привычек в этот день закрыто. Серый – день ещё не начат или ничего не отмечено, светлый – закрыто немного, насыщенный бирюзовый – почти всё или всё.");
});

const ICONS = {
  video:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="2.5" y="5.5" width="13" height="13" rx="2.5"/><path d="M18.5 9.5 21.5 7v10l-3-2.5"/></svg>',
  audio:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 10v4h3.5L13 18V6L7.5 10H4Z"/><path d="M16.5 9a4 4 0 0 1 0 6"/></svg>',
  pdf:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M6 2.5h8l4.5 4.5V21a.5.5 0 0 1-.5.5H6a.5.5 0 0 1-.5-.5V3a.5.5 0 0 1 .5-.5Z"/><path d="M14 2.5V7h4.5"/></svg>',
  lock:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="5" y="10.5" width="14" height="9.5" rx="2"/><path d="M8 10.5V8a4 4 0 0 1 8 0v2.5"/></svg>'
};
ICONS.link = ICONS.pdf;
ICONS.play = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7-11-7Z"/></svg>';
const MASTERMIND_AUTHOR = {mastermind:{name:"Мастермайнды",topics:"записи эфиров с Милой",desc:"Запись мастермайнда – введи кодовое слово, которое Мила называла вживую, и получи бриллиант"}};
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
  document.getElementById("overlayBackBtn").hidden = true;
  document.querySelector(".panel-head2").classList.remove("has-back");
  document.getElementById("overlayBody").classList.remove("no-scroll");
  document.querySelector(".panel").classList.remove("force-tall");
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
document.getElementById("favMaterialsBtn").addEventListener("click", ()=>{
  playPopSound();
  setActiveTab("materials");
  setActiveFolder("__fav__");
});
let initialTab = "today";
try{ initialTab = new URLSearchParams(location.search).get("tab") || localStorage.getItem(TAB_KEY) || "home"; }catch(e){}
if(!document.getElementById("tab-"+initialTab)) initialTab = "home";

/* ---------- аутентификация и загрузка реальных данных кабинета ---------- */
const TOKEN_KEY = "serenityAccessToken";
const REFRESH_KEY = "serenityRefreshToken";
function getToken(){ try{ return localStorage.getItem(TOKEN_KEY); }catch(e){ return null; } }
function setToken(t){ try{ if(t) localStorage.setItem(TOKEN_KEY, t); else localStorage.removeItem(TOKEN_KEY); }catch(e){} }
function getRefreshToken(){ try{ return localStorage.getItem(REFRESH_KEY); }catch(e){ return null; } }
function setRefreshToken(t){ try{ if(t) localStorage.setItem(REFRESH_KEY, t); else localStorage.removeItem(REFRESH_KEY); }catch(e){} }

let refreshInFlight = null;
async function trySilentRefresh(){
  const rt = getRefreshToken();
  if(!rt) return false;
  if(!refreshInFlight){
    refreshInFlight = fetch("/api/auth/refresh", {
      method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({refresh_token: rt})
    }).then(r=>r.json()).catch(()=>({ok:false})).finally(()=>{ refreshInFlight = null; });
  }
  const data = await refreshInFlight;
  if(data && data.ok && data.access_token){
    setToken(data.access_token);
    setRefreshToken(data.refresh_token);
    return true;
  }
  return false;
}

async function apiFetch(url, opts, _retried){
  opts = opts || {};
  const headers = Object.assign({}, opts.headers, {"Authorization": "Bearer " + (getToken()||"")});
  let body = opts.body;
  if(body && typeof body !== "string" && !(body instanceof FormData)){ body = JSON.stringify(body); headers["Content-Type"] = "application/json"; }
  const resp = await fetch(url, Object.assign({}, opts, {headers, body}));
  if(resp.status === 401){
    if(!_retried && await trySilentRefresh()){
      return apiFetch(url, opts, true);
    }
    setToken(null);
    setRefreshToken(null);
    showAuthScreen("сессия истекла – войди снова");
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
let TODAY_DATE_ISO = "2026-11-09";
let RU_MONTHS_SHORT = ["янв","фев","мар","апр","мая","июн","июл","авг","сен","окт","ноя","дек"];
let RU_MONTHS_FULL = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"];
let calViewYear = null, calViewMonth = null;
const TG_CHANNEL_URL = "https://t.me/+CS6fkx9D7ns5Njk0";
const TG_CHAT_URL = "https://t.me/+b1UKYlrCL5o5YWY0";
document.getElementById("chanLinkBtn").href = TG_CHANNEL_URL;
document.getElementById("chatLinkBtn").href = TG_CHAT_URL;
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
function fmtIsoDateRu(iso){
  if(!iso) return "";
  const [y,m,dd] = iso.split("-").map(Number);
  return fmtDateRu(new Date(Date.UTC(y, m-1, dd)));
}
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
function defaultHabitConfig(){ return {days:[0,1,2,3,4,5,6], qtyOn:false, target:8, unit:"", minutes:0}; }
function parseDurToMinutes(dur){
  if(!dur) return 5;
  const s = dur.toLowerCase();
  const nums = s.match(/\d+([.,]\d+)?/g);
  if(!nums) return 5;
  const vals = nums.map(n=>parseFloat(n.replace(",", ".")));
  const avg = vals.reduce((a,b)=>a+b, 0) / vals.length;
  const mins = s.includes("час") ? avg * 60 : avg;
  return Math.max(1, Math.round(mins));
}
function defaultHabitConfigFor(name){
  const cfg = defaultHabitConfig();
  const meta = findHabitMeta(name);
  cfg.minutes = (meta && meta.noTime) ? 0 : (meta ? parseDurToMinutes(meta.dur) : 5);
  return cfg;
}
const MIN_UNITS = ["мин","мин.","минут","минута","минуты","минуту"];
const HOUR_UNITS = ["час","часа","часов","ч","ч."];
function unitMinuteMultiplier(u){
  if(!u) return 0;
  const s = u.trim().toLowerCase();
  if(MIN_UNITS.some(t => s === t || s.startsWith(t))) return 1;
  if(HOUR_UNITS.some(t => s === t || s.startsWith(t))) return 60;
  return 0;
}
function isTimeUnit(u){ return unitMinuteMultiplier(u) > 0; }
function habitMinutes(name){
  const meta = findHabitMeta(name);
  if(meta && meta.noTime) return 0;
  const cfg = habitConfig[name] || defaultHabitConfig();
  const mult = unitMinuteMultiplier(cfg.unit);
  if(cfg.qtyOn && mult > 0) return (Number(cfg.target) || 0) * mult;
  if(Number(cfg.minutes) > 0) return Number(cfg.minutes);
  return meta ? parseDurToMinutes(meta.dur) : 5;
}
function formatMinutesRu(mins){
  if(mins < 60) return `${mins} минут`;
  const h = Math.floor(mins/60), m = mins % 60;
  const hLabel = h===1 ? "час" : (h>=2 && h<=4) ? "часа" : "часов";
  return m===0 ? `${h} ${hLabel}` : `${h} ${hLabel} ${m} минут`;
}
function formatMinutesShort(mins){
  if(mins < 60) return `${mins} мин`;
  const h = Math.floor(mins/60), m = mins % 60;
  return m===0 ? `${h} ч` : `${h} ч ${m} мин`;
}
const ICON_CLOCK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2"/></svg>';
const ICON_MOON = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M20 14.5A8.5 8.5 0 1 1 9.5 4a7 7 0 0 0 10.5 10.5Z"/></svg>';
function habitBadgeHtml(name){
  const mins = habitMinutes(name);
  if(mins > 0) return `<span class="hab-badge">${ICON_CLOCK}${formatMinutesShort(mins)}</span>`;
  const meta = findHabitMeta(name);
  if(meta && meta.noTime) return `<span class="hab-badge sleep">${ICON_MOON}${meta.when||"вечером"}</span>`;
  return "";
}
function totalMinutes(names){
  return names.reduce((sum,n)=>sum+habitMinutes(n), 0);
}

function applyState(data){
  STATE = data;
  STATE.logsByDate = {};
  (data.logs||[]).forEach(l=>{ STATE.logsByDate[l.date] = {habits_done: l.habits_done||{}, closed: !!l.closed}; });
  STATE.mastermind = data.mastermind || {};
  MARATHON_START_ISO = data.marathon_start || MARATHON_START_ISO;
  TODAY_DATE_ISO = data.today_date || TODAY_DATE_ISO;
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
  AUTHORS = Object.assign({}, STATE.authors||{});
  MATERIALS = (STATE.materials||[]).map(m=>Object.assign({}, m, {tags: m.tags||[]}));
  THEMES = [...new Set(MATERIALS.flatMap(m=>m.tags||[]))];
  loadFavorites();
  activeTheme = "all"; activeFolder = "all"; activeType = "all"; query = "";
  renderThemeFilterOptions();
  document.getElementById("formatSelect").value = "all";
  updateFiltersToggleState();
  document.getElementById("folderChips").innerHTML =
    `<button class="chip folder active" data-f="all">все папки</button>` +
    `<button class="chip folder" data-f="__fav__">${BOOKMARK_SVG_FILLED} Избранное</button>` +
    Object.entries(AUTHORS).filter(([key])=>key!==STATE.intro_folder_id && MATERIALS.some(m=>m.author===key))
      .map(([key,a])=>`<button class="chip folder" data-f="${key}">${a.name}</button>`).join("");
}
function renderThemeFilterOptions(){
  const sel = document.getElementById("categorySelect");
  const opts = [{v:"all",label:"Любая"}, ...THEMES.map(t=>({v:t,label:t}))];
  sel.innerHTML = opts.map(o=>`<option value="${o.v}"${activeTheme===o.v?' selected':''}>${o.label}</option>`).join("");
}
function updateFiltersToggleState(){
  const active = activeTheme !== "all" || activeType !== "all";
  document.getElementById("filtersToggleBtn").classList.toggle("active", active);
  const label = document.getElementById("filtersToggleLabel");
  if(!active){ label.textContent = "Фильтры материалов"; return; }
  const typeLabels = {video:"Видео", audio:"Аудио", pdf:"PDF"};
  const parts = [];
  if(activeType !== "all") parts.push(typeLabels[activeType] || activeType);
  if(activeTheme !== "all") parts.push(activeTheme);
  label.textContent = parts.join(", ");
}
function setActiveFolder(f){
  activeFolder = f;
  activeTheme = "all";
  matVisibleCount = MAT_PAGE_SIZE;
  renderThemeFilterOptions();
  updateFiltersToggleState();
  renderMaterials();
}

function renderMilaEvents(){
  const wrap = document.getElementById("profileMilaEvents");
  const events = STATE.mila_events || [];
  wrap.innerHTML = events.length ? events.map(e=>{
    const bg = e.photo ? `background-image:url('${e.photo}')` : `background:${COLOR_CSS[e.color] || COLOR_CSS.lav}`;
    const link = e.link ? `<a href="${e.link}" target="_blank" rel="noopener">Узнать больше →</a>` : "";
    const desc = e.description ? `<p class="desc">${e.description}</p>` : "";
    const dateStr = e.date || e.date_place || "";
    const m = dateStr.match(/^(\d{1,2})\s*(.*)$/);
    const dateBadge = m
      ? `<div class="num">${m[1]}</div><div class="mon">${m[2]}</div>`
      : `<div class="mon">${dateStr}</div>`;
    const place = e.place ? `<div class="place"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="10.5" r="3"/><path d="M12 21s7-6.2 7-11.5A7 7 0 0 0 5 9.5C5 14.8 12 21 12 21Z"/></svg>${e.place}</div>` : "";
    return `<div class="evcard">
      <div class="ph" style="${bg}"></div>
      <div class="b">
        <div class="datebadge">${dateBadge}</div>
        <div class="info">
          <h4>${e.title}</h4>
          ${place}
          ${desc}
          ${link}
        </div>
      </div>
    </div>`;
  }).join("") : `<p class="note" style="margin:8px 0 16px">пока нет мероприятий</p>`;
}
function initCabinetUI(){
  buildMaterialsState();
  renderHeat(); renderChecklist(); renderMaterials(); renderProgress(); renderEvents(); renderProfileTile(); renderMilaEvents();
  syncHabitsCount();
  setActiveTab(initialTab, {persist:false});
  if(HABITS.length === 0) startOnboarding();
  checkStarCelebration();
}

const STAR_SHOWN_KEY = "serenityStarShown";
const STAR_TEXT = {
  7: "Поздравляю, ты прошла первую неделю в клубе – и за это получаешь звезду",
  14: "Поздравляю, ты прошла две недели в клубе – и за это получаешь звезду",
  21: "Поздравляю, ты прошла все 21 день – и за это получаешь звезду"
};
const STAR_EYEBROW = {7:"неделя 1 пройдена", 14:"неделя 2 пройдена", 21:"21 день пройден"};
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
  document.getElementById("starOverlay").classList.remove("open");
  playPopSound();
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
    not_paid: "не нашли оплату на этот email – напиши организатору клуба",
    user_already_exists: "такой email уже зарегистрирован – попробуй войти",
    invalid_login_credentials: "неверный email или пароль",
    bad_request: "проверь email и пароль",
  };
  if(map[code]) return map[code];
  const low = String(code||"").toLowerCase();
  if(low.includes("confirm")) return "подтверди email по ссылке из письма, потом попробуй войти";
  if(low.includes("invalid login") || low.includes("invalid_login")) return "неверный email или пароль";
  if(low.includes("already") && low.includes("regist")) return "такой email уже зарегистрирован – попробуй войти";
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
  document.getElementById("authPassword").autocomplete = authMode==="login" ? "current-password" : "new-password";
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
document.getElementById("recoverForm").addEventListener("submit", async (e)=>{
  e.preventDefault();
  const email = document.getElementById("recoverEmail").value.trim();
  const errEl = document.getElementById("recoverErr");
  errEl.style.color = ""; errEl.hidden = true;
  if(!email){ errEl.textContent = "введи email"; errEl.hidden = false; return; }
  const btn = document.getElementById("recoverSubmit");
  btn.disabled = true;
  try{
    await fetch("/api/auth/recover", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({email})});
    errEl.style.color = "var(--sage-ink)";
    errEl.textContent = "если такой email зарегистрирован – письмо со ссылкой уже в пути, проверь почту";
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
document.getElementById("newPasswordForm").addEventListener("submit", async (e)=>{
  e.preventDefault();
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
      errEl.textContent = "не получилось сохранить пароль – ссылка могла устареть, запроси новую"; errEl.hidden = false; btn.disabled = false; return;
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
    setRefreshToken(data.refresh_token);
    btn.disabled = false;
    bootCabinet();
  }catch(e){
    errEl.textContent = "не получилось связаться с сервером, попробуй ещё раз"; errEl.hidden = false; btn.disabled = false;
  }
}
document.getElementById("authForm").addEventListener("submit", e=>{ e.preventDefault(); submitAuth(); });

(function bootOnLoad(){
  let isRecovery = false;
  if(location.hash && location.hash.indexOf("access_token=") !== -1){
    const params = new URLSearchParams(location.hash.slice(1));
    const tok = params.get("access_token");
    isRecovery = params.get("type") === "recovery";
    if(tok){
      setToken(tok);
      setRefreshToken(params.get("refresh_token"));
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
    const isClosed = d===TODAY ? dayClosed : !!logForDate(dateForDay(d)).closed;
    const cell = document.createElement("div");
    cell.className = "hday "+cls+(d===TODAY?" istoday":"");
    const dt = dayToDate(d);
    cell.innerHTML = (d<=TODAY && isClosed)
      ? `<span class="hday-check"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><path d="M5 12.5 9.5 17 19 7"/></svg></span>`
      : `<span class="hday-main"><span class="hday-dd">${dt.getUTCDate()}</span><span class="hday-mm">${RU_MONTHS_SHORT[dt.getUTCMonth()]}</span></span>`;
    cell.title = "День "+d+" · "+fmtDateRu(dt)+(d===TODAY?" · сегодня":"");
    cell.addEventListener("click", ()=>openDay(d));
    const gridId = "heatgrid"+(Math.ceil(d/7));
    document.getElementById(gridId).appendChild(cell);
  }
  document.getElementById("bigDayTxt").textContent = TODAY;
  const marathonPct = Math.round(TODAY/21*100);
  document.getElementById("dayPct").textContent = marathonPct+"%";
  document.getElementById("dayPillFill").style.width = marathonPct+"%";
  document.getElementById("daySub").textContent =
    TODAY<=1 ? "Только начинаешь путь – вперёд, шаг за шагом" :
    marathonPct<33 ? "Ты в начале пути – продолжай в своём темпе" :
    marathonPct<66 ? "Ты уже прошла треть пути – продолжай в своём темпе" :
    marathonPct<100 ? "Ты уже прошла больше половины пути – продолжай в своём темпе" :
    "Ты прошла все 21 день – поздравляю!";
}

function openDay(d){
  openOverlay("День "+d+(d===TODAY?" (сегодня)":""), "21-дневная программа клуба");
  if(d > TODAY){
    document.getElementById("overlayBody").innerHTML =
      `<p class="note" style="margin:0">Этот день ещё не наступил – загляни сюда, когда он начнётся.</p>`;
    return;
  }
  if(HABITS.length === 0){
    document.getElementById("overlayBody").innerHTML =
      `<p class="note" style="margin:0">В этот день привычки ещё не были выбраны.</p>`;
    return;
  }
  const dateStr = dateForDay(d);
  const log = logForDate(dateStr);
  let checks = d===TODAY ? liveChecked.slice() : HABIT_IDS.map(id => !!(log.habits_done||{})[id]);
  let closed = d===TODAY ? dayClosed : !!log.closed;

  function persist(nowClosed){
    const habits_done = {};
    HABIT_IDS.forEach((id,i)=>{ habits_done[id] = !!checks[i]; });
    if(nowClosed !== undefined) closed = nowClosed;
    STATE.logsByDate[dateStr] = {habits_done, closed};
    if(d===TODAY){ liveChecked = checks.slice(); dayClosed = closed; renderChecklist(); }
    apiFetch("/api/cabinet/logs", {method:"PUT", body:{date:dateStr, habits_done, closed}}).catch(()=>{});
    renderHeat(); renderProgress();
  }

  function draw(){
    const rows = HABITS.map((h,i)=>{
      const c = checks[i];
      return `<label class="hab ${c?'done':''}" for="pd${i}">
        <span class="markwrap"><input type="checkbox" id="pd${i}" ${c?'checked':''} ${closed?'disabled':''}>
        <span class="markvisual">${ICONS_CHECK}</span></span>
        <span class="hab-text">${h}</span></label>`;
    }).join("");
    document.getElementById("overlayBody").innerHTML =
      `<div class="checklist" style="padding:4px 10px;box-shadow:none">${rows}</div>
       <button class="btn primary${closed?' is-closed':''}" id="pdCloseBtn" style="width:100%;margin-top:14px;display:flex;align-items:center;justify-content:center;gap:8px">
         <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12.5 9.5 17 19 7"/></svg>
         ${closed?'День закрыт':'Закрыть день'}
       </button>`;
    HABITS.forEach((_,i)=>{
      document.getElementById("pd"+i).addEventListener("change", e=>{
        checks[i] = e.target.checked; playPopSound(); persist(); draw();
      });
    });
    document.getElementById("pdCloseBtn").addEventListener("click", ()=>{
      playPopSound(); persist(!closed); draw();
    });
  }
  draw();
}
document.getElementById("closePanel").onclick=()=>{ document.getElementById("overlay").classList.remove("open"); if(onboarding) onboarding = false; playPopSound(); };
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
  const affirmDt = dayToDate(TODAY);
  document.getElementById("affirmDateNum").textContent = affirmDt.getUTCDate();
  document.getElementById("affirmDateMon").textContent = RU_MONTHS_SHORT[affirmDt.getUTCMonth()];
  const openerText = SOCIAL_OPENERS[(TODAY-1)%SOCIAL_OPENERS.length].replace(/15/, String(todayCount()));
  document.getElementById("socialOpener").textContent = openerText;
  document.getElementById("socialCloser").textContent = SOCIAL_CLOSERS[(TODAY-1+2)%SOCIAL_CLOSERS.length];
  document.getElementById("eveningRem").hidden = new Date().getHours() < 18;
  const doneCount = liveChecked.filter(Boolean).length;
  const remaining = HABITS.length - doneCount;
  const todayDow = (dayToDate(TODAY).getUTCDay()+6)%7;
  const todayHabits = HABITS.filter(h=>{ const cfg=habitConfig[h]; return !cfg || !cfg.days || cfg.days.includes(todayDow); });
  const todayMins = totalMinutes(todayHabits);
  const hcSummary = document.getElementById("hcSummary");
  hcSummary.hidden = HABITS.length === 0;
  if(HABITS.length > 0){
    const pct = Math.round(doneCount / HABITS.length * 100);
    document.getElementById("hcDoneNum").textContent = doneCount;
    document.getElementById("hcTotalNum").textContent = HABITS.length;
    document.getElementById("hcBarFill").style.width = pct + "%";
    document.getElementById("hcBarPct").textContent = pct + "%";
    const timeRow = document.getElementById("hcTimeRow");
    timeRow.hidden = !(todayHabits.length && todayMins > 0);
    if(!timeRow.hidden){
      const _h = Math.floor(todayMins/60), _mm = todayMins % 60;
      const _big = todayMins < 60 ? String(todayMins) : (_mm ? `${_h}:${String(_mm).padStart(2,"0")}` : String(_h));
      document.getElementById("hcTimeNum").textContent = _big;
      document.getElementById("hcTimeNum").classList.toggle("long", _big.length >= 3);
      document.getElementById("hcTimeUnit").textContent = todayMins < 60 ? "всего мин" : "всего ч";
      const hasSleepHabit = todayHabits.some(h=>{ const meta = findHabitMeta(h); return meta && meta.noTime; });
      document.getElementById("hcTimeSub").hidden = !hasSleepHabit;
    }
  }
  const statusEl = document.getElementById("dayStatus");
  const pillEl = document.getElementById("dayStatusPill");
  if(HABITS.length === 0){
    pillEl.hidden = true;
    statusEl.textContent = "Пока не выбраны привычки – нажми «Изменить».";
  } else if(remaining === 0){
    pillEl.hidden = true;
    statusEl.textContent = "Все привычки на сегодня отмечены ✨";
  } else if(doneCount === 0){
    pillEl.hidden = false; pillEl.textContent = remaining;
    statusEl.textContent = "осталось сегодня – начни с одной, дальше будет легче";
  } else {
    pillEl.hidden = false; pillEl.textContent = remaining;
    statusEl.textContent = "осталось сегодня 💗";
  }
  const box = document.getElementById("checklist");
  box.innerHTML = HABITS.map((h,i)=>{
    return `<label class="hab ${liveChecked[i]?'done':''}" for="hb${i}">
    <span class="markwrap"><input type="checkbox" id="hb${i}" ${liveChecked[i]?'checked':''} ${dayClosed?'disabled':''}>
    <span class="markvisual">${ICONS_CHECK}</span></span>
    <span class="hab-text">${h}</span>
    ${habitBadgeHtml(h)}
    </label>`;
  }).join("");
  HABITS.forEach((_,i)=>document.getElementById("hb"+i).addEventListener("change",e=>{
    liveChecked[i]=e.target.checked; playPopSound(); renderChecklist(); renderHeat(); renderProgress();
    saveTodayLog();
  }));
  const btn = document.getElementById("closeDayBtn");
  btn.disabled = HABITS.length === 0;
  btn.classList.toggle("is-closed", dayClosed);
  btn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12.5 9.5 17 19 7"/></svg> ${dayClosed?'День закрыт':'Закрыть день'}`;
}
document.getElementById("closeDayBtn").addEventListener("click", ()=>{
  if(HABITS.length === 0) return;
  playPopSound();
  if(dayClosed){
    dayClosed = false; renderChecklist(); renderHeat();
    saveTodayLog(false); renderProgress();
    return;
  }
  dayClosed = true; renderChecklist(); renderHeat();
  const done = liveChecked.filter(Boolean).length;
  document.getElementById("fdDay").textContent = TODAY;
  document.getElementById("fdDone").textContent = done;
  document.getElementById("fdTotal").textContent = HABITS.length;
  document.getElementById("fdPraise").textContent = done===HABITS.length?"Ты держишь ритм ✨":done===0?"Ура! Ты справилась! 💗":"Ты в процессе, и этого достаточно";
  document.getElementById("fdCloseText").textContent = done===HABITS.length?"Ура! 🎉 Ты это сделала!":done===0?"Хорошо, до встречи":"Отлично, увидимся завтра";
  document.getElementById("fdOverlay").classList.add("open");
  saveTodayLog(true);
  renderProgress();
});
document.getElementById("fdClose").addEventListener("click",()=>{ document.getElementById("fdOverlay").classList.remove("open"); playPopSound(); });
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
      <p class="info-desc">Своя привычка – добавлено вручную, подробностей нет.</p>`;
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
document.getElementById("closeInfoPanel").addEventListener("click", ()=>{ document.getElementById("infoOverlay").classList.remove("open"); playPopSound(); });
document.getElementById("infoOverlay").addEventListener("click",e=>{if(e.target.id==="infoOverlay")e.currentTarget.classList.remove("open");});
function renderPickTotal(){
  const el = document.getElementById("pickTotal");
  if(!el) return;
  if(!pickSelection.length){ el.textContent = ""; return; }
  const mins = totalMinutes(pickSelection);
  el.textContent = mins > 0 ? `выбрано ${pickSelection.length} · ~${formatMinutesRu(mins)} в день` : `выбрано ${pickSelection.length}`;
}
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
    <button class="addhabit-btn" id="customAddBtn" type="button">+ добавить свою привычку</button>
    </div>
    <p class="note" id="pickWarn" hidden style="margin:0 0 10px">Выбери хотя бы одну привычку, чтобы сохранить</p>
    <p class="pick-total" id="pickTotal"></p>
    <button class="savepick" id="savePick">${onboarding ? "Начать 🌙" : "Сохранить"}</button>`;
  document.getElementById("overlayBody").innerHTML = html;
  renderPickTotal();
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
    const payload = pickSelection.map(name => ({name, config: habitConfig[name] || defaultHabitConfigFor(name)}));
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
document.getElementById("progressContinueBtn").addEventListener("click", ()=>{ playPopSound(); setActiveTab("today"); });

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
  document.getElementById("habitTargetVal").value = hmState.target;
  document.getElementById("habitMinutesInput").value = hmState.minutes || "";
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
  const cfg = (name && habitConfig[name]) ? habitConfig[name] : defaultHabitConfigFor(name);
  hmState.days = [...cfg.days]; hmState.qtyOn = cfg.qtyOn; hmState.target = cfg.target;
  hmState.minutes = cfg.minutes || (name ? habitMinutes(name) : 5);
  hmState.unit = cfg.unit || "";
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
  const payload = HABITS.map(name => ({name, config: habitConfig[name] || defaultHabitConfigFor(name)}));
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
    hmState.target = Math.max(1, hmState.target + d);
    playPopSound();
    renderHabitModalQty();
  });
});
document.getElementById("habitTargetVal").addEventListener("input", e=>{
  const v = Math.max(1, parseInt(e.target.value, 10) || 1);
  hmState.target = v;
});
document.getElementById("habitTargetVal").addEventListener("blur", renderHabitModalQty);
document.getElementById("habitMinutesInput").addEventListener("input", e=>{
  hmState.minutes = Math.max(0, parseInt(e.target.value, 10) || 0);
});
document.getElementById("addHabitBtn").addEventListener("click", openHabitPicker);
document.getElementById("habitModalClose").addEventListener("click", ()=>{ closeHabitModal(); playPopSound(); });
document.getElementById("habitModalBackdrop").addEventListener("click", e=>{
  if(e.target.id === "habitModalBackdrop") closeHabitModal();
});
document.getElementById("habitSaveBtn").addEventListener("click", ()=>{
  const name = document.getElementById("habitNameInput").value.trim();
  if(!name) return;
  playPopSound();
  const unit = hmState.unit || "";
  const newCfg = {days:[...hmState.days], qtyOn:hmState.qtyOn, target:hmState.target, unit, minutes:hmState.minutes || 0};
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

const CG_LNUM_LATIN_B64="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";
const CG_LNUM_CYR_B64="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";
async function drawHeroStat(ctx, text, x, baselineY, fontSize, maxW){
  try{
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${maxW}" height="${Math.round(fontSize*1.5)}">`
      + `<style>@font-face{font-family:'CGLnum';src:url(data:font/woff2;base64,${CG_LNUM_LATIN_B64}) format('woff2');unicode-range:U+0000-00FF,U+2000-206F;}`
      + `@font-face{font-family:'CGLnum';src:url(data:font/woff2;base64,${CG_LNUM_CYR_B64}) format('woff2');unicode-range:U+0400-045F;}`
      + `text{font-family:'CGLnum',serif;font-weight:600;font-feature-settings:'lnum' 1,'onum' 0;font-variant-numeric:lining-nums;}</style>`
      + `<text x="0" y="${Math.round(fontSize*0.92)}" font-size="${fontSize}" fill="#ffffff">${text}</text></svg>`;
    const blob = new Blob([svg], {type:"image/svg+xml;charset=utf-8"});
    const url = URL.createObjectURL(blob);
    const img = new Image();
    await new Promise((resolve,reject)=>{ img.onload=resolve; img.onerror=reject; img.src=url; });
    ctx.drawImage(img, x, baselineY-Math.round(fontSize*0.92), maxW, Math.round(fontSize*1.5));
    URL.revokeObjectURL(url);
    return true;
  }catch(e){ return false; }
}
document.getElementById("shareBtn").addEventListener("click", async ()=>{
  playPopSound();
  const HEAD='"Cormorant Garamond"', SYS='"Montserrat"';
  try{
    await Promise.all([
      document.fonts.load('600 112px '+HEAD), document.fonts.load('600 34px '+HEAD),
      document.fonts.load('600 64px '+HEAD), document.fonts.load('italic 600 24px '+HEAD),
      document.fonts.load('600 18px '+SYS), document.fonts.load('600 19px '+SYS),
      document.fonts.load('600 22px '+SYS), document.fonts.load('500 20px '+SYS),
      document.fonts.load('600 12px '+SYS)
    ]);
  }catch(e){}
  const W=640;
  const measure=document.createElement("canvas").getContext("2d");
  const wrapLines=(ctx,text,maxWidth)=>{
    const words=text.split(" "); const lines=[]; let line="";
    words.forEach(w=>{
      const test=line?line+" "+w:w;
      if(measure.measureText(test).width>maxWidth && line){ lines.push(line); line=w; } else line=test;
    });
    if(line) lines.push(line);
    return lines;
  };

  const cardLeft=28, cardTop=28, cardRight=W-28, cardWidth=cardRight-cardLeft;
  const cardRadius=32;
  const contentX=cardLeft+40;

  const panelPadX=26, iconSize=38, iconGap=18, rowGap=22;
  measure.font="500 20px "+SYS+", sans-serif";
  const habitWrapW = cardWidth-28 - panelPadX*2 - iconSize - iconGap;
  const habitLines=HABITS.map(h=>wrapLines(measure,h,habitWrapW));

  const quoteLines=["Маленькие шаги каждый день —","путь к большим переменам"];

  const ribbonTop=cardTop+40, ribbonH=38;
  const eyebrowY=ribbonTop+ribbonH+34;
  const heroBaseline=eyebrowY+100;
  const subtitleBaseline=heroBaseline+34;
  const gradientCardBottom=subtitleBaseline+60;
  const panelOverlap=46;
  const panelLeft=cardLeft+14, panelRight=cardRight-14, panelWidth=panelRight-panelLeft;
  const panelRadius=28;
  const panelPadTop=32, panelPadBottom=26;
  const panelTop=gradientCardBottom-panelOverlap;
  let rowY=panelTop+panelPadTop+16;
  habitLines.forEach(lines=>{ rowY += Math.max(iconSize, lines.length*26) + rowGap; });
  const panelBottom=rowY - rowGap + panelPadBottom - 16;

  const quoteMarkBaseline=panelBottom+74;
  const quoteTextTop=quoteMarkBaseline+30;
  const quoteLineH=32;
  const quoteBottom=quoteTextTop+(quoteLines.length-1)*quoteLineH;
  const H=quoteBottom+48;

  const c=document.createElement("canvas"); c.width=W; c.height=H;
  const ctx=c.getContext("2d");

  ctx.fillStyle="#fff";
  ctx.beginPath(); ctx.roundRect(0,0,W,H,40); ctx.fill();

  ctx.save();
  ctx.beginPath(); ctx.roundRect(cardLeft,cardTop,cardWidth,gradientCardBottom-cardTop,cardRadius); ctx.clip();
  const base=ctx.createLinearGradient(cardLeft,cardTop,cardRight,gradientCardBottom);
  base.addColorStop(0,"#AA9DB8"); base.addColorStop(0.48,"#8E93A8"); base.addColorStop(1,"#6A9993");
  ctx.fillStyle=base;
  ctx.fillRect(cardLeft,cardTop,cardWidth,gradientCardBottom-cardTop);
  const glow1=ctx.createRadialGradient(cardLeft+cardWidth*0.8,cardTop+40,0,cardLeft+cardWidth*0.8,cardTop+40,220);
  glow1.addColorStop(0,"rgba(217,189,138,.38)"); glow1.addColorStop(1,"rgba(217,189,138,0)");
  ctx.fillStyle=glow1; ctx.fillRect(cardLeft,cardTop,cardWidth,gradientCardBottom-cardTop);
  const glow2=ctx.createRadialGradient(cardLeft+cardWidth*0.1,cardTop+(gradientCardBottom-cardTop)*0.6,0,cardLeft+cardWidth*0.1,cardTop+(gradientCardBottom-cardTop)*0.6,240);
  glow2.addColorStop(0,"rgba(255,255,255,.10)"); glow2.addColorStop(1,"rgba(255,255,255,0)");
  ctx.fillStyle=glow2; ctx.fillRect(cardLeft,cardTop,cardWidth,gradientCardBottom-cardTop);
  ctx.restore();

  const dateText=fmtDateRu(dayToDate(TODAY));
  measure.font="600 19px "+SYS+", sans-serif";
  const dateTextW=measure.measureText(dateText).width;
  const ribbonPadX=18, ribbonW=dateTextW+ribbonPadX*2;
  ctx.save();
  ctx.translate(contentX+ribbonW/2, ribbonTop+ribbonH/2);
  ctx.rotate(-2*Math.PI/180);
  ctx.fillStyle="rgba(255,255,255,.16)";
  ctx.strokeStyle="rgba(255,255,255,.3)"; ctx.lineWidth=1;
  ctx.beginPath(); ctx.roundRect(-ribbonW/2,-ribbonH/2,ribbonW,ribbonH,[13,13,13,4]); ctx.fill(); ctx.stroke();
  ctx.fillStyle="#fff"; ctx.textAlign="center"; ctx.textBaseline="middle";
  ctx.font="600 19px "+SYS+", sans-serif";
  ctx.fillText(dateText,0,1);
  ctx.restore();
  ctx.textBaseline="alphabetic";

  ctx.textAlign="left"; ctx.fillStyle="rgba(255,255,255,.72)";
  ctx.font="600 18px "+SYS+", sans-serif";
  ctx.fillText("МАРАФОН «НАЧНИ СЕГОДНЯ!»", contentX, eyebrowY);

  const done=liveChecked.filter(Boolean).length;
  const heroText=done+" из "+HABITS.length;
  ctx.fillStyle="#fff";
  ctx.font="600 112px "+HEAD+", Georgia, serif";
  const heroOk = await drawHeroStat(ctx, heroText, contentX, heroBaseline, 112, cardWidth-80);
  if(!heroOk) ctx.fillText(heroText, contentX, heroBaseline);

  ctx.fillStyle="rgba(255,255,255,.78)";
  ctx.font="600 22px "+SYS+", sans-serif";
  ctx.fillText("привычек сегодня", contentX, subtitleBaseline);

  const badgeR=52, badgeCx=cardRight-16-badgeR, badgeCy=cardTop+16+badgeR;
  ctx.save();
  ctx.shadowColor="rgba(0,0,0,.35)"; ctx.shadowBlur=26; ctx.shadowOffsetY=12;
  ctx.fillStyle="#fff";
  ctx.beginPath(); ctx.arc(badgeCx,badgeCy,badgeR,0,Math.PI*2); ctx.fill();
  ctx.restore();
  ctx.strokeStyle="rgba(255,255,255,.5)"; ctx.lineWidth=1.5;
  ctx.beginPath(); ctx.arc(badgeCx,badgeCy,badgeR+2,0,Math.PI*2); ctx.stroke();
  ctx.strokeStyle="rgba(255,255,255,.12)"; ctx.lineWidth=7;
  ctx.beginPath(); ctx.arc(badgeCx,badgeCy,badgeR+6,0,Math.PI*2); ctx.stroke();

  ctx.save();
  ctx.fillStyle="#D9BD8A";
  const starCx=badgeCx, starCy=badgeCy-21, starR=9;
  ctx.beginPath();
  for(let i=0;i<5;i++){
    const a=-Math.PI/2+i*(Math.PI*2/5), a2=a+Math.PI/5;
    const x1=starCx+Math.cos(a)*starR, y1=starCy+Math.sin(a)*starR;
    const x2=starCx+Math.cos(a2)*starR*0.42, y2=starCy+Math.sin(a2)*starR*0.42;
    i===0?ctx.moveTo(x1,y1):ctx.lineTo(x1,y1); ctx.lineTo(x2,y2);
  }
  ctx.closePath(); ctx.fill();
  ctx.restore();

  ctx.fillStyle="#3F6660"; ctx.textAlign="center";
  ctx.font="600 34px "+HEAD+", Georgia, serif";
  ctx.fillText(String(TODAY), badgeCx, badgeCy+15);
  ctx.fillStyle="#6B6475";
  ctx.font="600 12px "+SYS+", sans-serif";
  ctx.fillText("ДЕНЬ", badgeCx, badgeCy+29);

  ctx.save();
  ctx.shadowColor="rgba(51,46,61,.32)"; ctx.shadowBlur=40; ctx.shadowOffsetY=18;
  ctx.fillStyle="#fff";
  ctx.beginPath(); ctx.roundRect(panelLeft,panelTop,panelWidth,panelBottom-panelTop,panelRadius); ctx.fill();
  ctx.restore();

  let y=panelTop+panelPadTop;
  ctx.textAlign="left";
  HABITS.forEach((h,i)=>{
    const chipX=panelLeft+panelPadX, chipY=y;
    ctx.fillStyle="rgba(106,153,147,.12)";
    ctx.strokeStyle="rgba(106,153,147,.22)"; ctx.lineWidth=1.5;
    ctx.beginPath(); ctx.roundRect(chipX,chipY,iconSize,iconSize,11); ctx.fill(); ctx.stroke();
    ctx.save();
    ctx.strokeStyle="#3F6660"; ctx.lineWidth=3; ctx.lineCap="round"; ctx.lineJoin="round";
    const cx=chipX+iconSize/2, cy=chipY+iconSize/2;
    ctx.beginPath(); ctx.moveTo(cx-8,cy+1); ctx.lineTo(cx-2,cy+7); ctx.lineTo(cx+9,cy-8); ctx.stroke();
    ctx.restore();
    ctx.fillStyle="#332E3D"; ctx.font="500 20px "+SYS+", sans-serif";
    habitLines[i].forEach((ln,li)=>{ ctx.fillText(ln, chipX+iconSize+iconGap, chipY+18+li*26); });
    y += Math.max(iconSize, habitLines[i].length*26) + rowGap;
  });

  ctx.textAlign="center"; ctx.fillStyle="rgba(63,102,96,.25)";
  ctx.font="600 64px "+HEAD+", Georgia, serif";
  ctx.fillText("“", W/2, quoteMarkBaseline);

  ctx.fillStyle="#3F6660"; ctx.font="italic 600 24px "+HEAD+", Georgia, serif";
  quoteLines.forEach((ln,li)=>{ ctx.fillText(ln, W/2, quoteTextTop+li*quoteLineH); });

  const body=document.getElementById("shareOverlayBody");
  body.innerHTML = `<div class="share-out">
      <img src="${c.toDataURL('image/png')}" alt="карточка дня">
      <button class="btn primary" id="shareToChatBtn" type="button" style="width:auto;flex:none;margin:14px auto 0;padding:10px 22px;font-size:13.5px;display:block">Поделиться в чат</button>
    </div>`;
  document.getElementById("shareOverlay").classList.add("open");

  document.getElementById("shareToChatBtn").addEventListener("click", ()=>{
    playPopSound();
    const chatBtn = document.getElementById("shareToChatBtn");
    c.toBlob(async (blob)=>{
      if(!blob) return;
      const file = new File([blob], "day-"+TODAY+".png", {type:"image/png"});
      if(navigator.canShare && navigator.canShare({files:[file]})){
        try{
          await navigator.share({files:[file], title:"Serenity Moon", text:"Мой день "+TODAY+" в клубе «Начни сегодня!»"});
          return;
        }catch(e){ /* отменено или не удалось — переходим к запасному варианту */ }
      }
      const a=document.createElement("a");
      a.href=c.toDataURL("image/png"); a.download="day-"+TODAY+".png";
      document.body.appendChild(a); a.click(); a.remove();
      chatBtn.textContent="Картинка сохранена, открываю чат…";
      setTimeout(()=>{ window.open(TG_CHAT_URL, "_blank"); chatBtn.textContent="Поделиться в чат"; }, 400);
    }, "image/png");
  });
});
document.getElementById("closeShareOverlay").addEventListener("click", ()=>{ document.getElementById("shareOverlay").classList.remove("open"); playPopSound(); });
document.getElementById("shareOverlay").addEventListener("click",e=>{if(e.target.id==="shareOverlay")e.currentTarget.classList.remove("open");});

/* ---------- материалы ---------- */
let activeTheme="all", activeFolder="all", activeType="all", query="";
const MAT_PAGE_SIZE = 6;
let matVisibleCount = MAT_PAGE_SIZE;
const TYPE_LABEL = {video:"Видео", audio:"Аудио", pdf:"PDF", link:"Материал"};
const PHOTOS = {
  mila: "https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/mila.jpg",
  maria: "https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/maria.jpg",
  anna: "https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/anna.jpg"
};

function matchesQuery(m){ if(!query) return true; return (m.title+" "+(m.tags||[]).join(" ")).toLowerCase().includes(query); }

function matThumbHtml(m){
  const grad = m.type==="video" ? "linear-gradient(135deg,var(--sage),#9FC9BE)"
    : m.type==="audio" ? "linear-gradient(135deg,var(--lav),#CBB3DE)"
    : "linear-gradient(135deg,var(--sand),#E3D2BC)";
  const inner = (m.type==="video"||m.type==="audio")
    ? `<span class="mitem-play">${ICONS.play}</span>`
    : `<span class="mitem-doc">${ICONS[m.type]||ICONS.pdf}</span>`;
  return `<div class="mitem-thumb" style="background:${grad}">${inner}</div>`;
}
const FAV_KEY = "serenityFavMaterials";
let FAVORITES = {};
function loadFavorites(){ try{ FAVORITES = JSON.parse(localStorage.getItem(FAV_KEY)||"{}"); }catch(e){ FAVORITES = {}; } }
function saveFavorites(){ try{ localStorage.setItem(FAV_KEY, JSON.stringify(FAVORITES)); }catch(e){} }
function isFavorite(id){ return !!FAVORITES[String(id)]; }
function toggleFavorite(id){ id=String(id); if(FAVORITES[id]) delete FAVORITES[id]; else FAVORITES[id]=true; saveFavorites(); }
const BOOKMARK_SVG_OUTLINE = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 20.5s-7.5-4.6-9.8-9.3C.7 7.8 2.4 4.5 5.7 4c2-.3 3.8.7 4.8 2.3C11.5 4.7 13.3 3.7 15.3 4c3.3.5 5 3.8 3.5 7.2C16.5 15.9 12 20.5 12 20.5Z"/></svg>`;
const BOOKMARK_SVG_FILLED = `<svg viewBox="0 0 24 24" fill="currentColor" stroke="none"><path d="M12 20.5s-7.5-4.6-9.8-9.3C.7 7.8 2.4 4.5 5.7 4c2-.3 3.8.7 4.8 2.3C11.5 4.7 13.3 3.7 15.3 4c3.3.5 5 3.8 3.5 7.2C16.5 15.9 12 20.5 12 20.5Z"/></svg>`;
function mItemHtml(m,i){
  const unlocked = m.day===null||m.day<=TODAY;
  const newBadge = m.day===TODAY ? `<span class="mat-new">Новое</span>` : "";
  const typeLbl = TYPE_LABEL[m.type]||TYPE_LABEL.link;
  const metaHtml = unlocked
    ? `${ICONS[m.type]||ICONS.pdf}<span>${typeLbl}${m.type==="pdf"?" · скачать":""}</span>`
    : `${ICONS.lock}<span>с ${fmtDateRu(dayToDate(m.day))}</span>`;
  const fav = isFavorite(m.id);
  const favBtn = unlocked ? `<button class="mitem-fav${fav?' on':''}" data-favid="${m.id}" type="button" aria-label="в избранное">${fav?BOOKMARK_SVG_FILLED:BOOKMARK_SVG_OUTLINE}</button>` : "";
  return `<div class="mitem ${unlocked?'':'is-locked'}" ${unlocked?`data-i="${i}" style="cursor:pointer"`:""}>
    ${favBtn}
    ${matThumbHtml(m)}
    <div class="mitem-body">
      <div class="mitem-title">${m.title}${newBadge}</div>
      <div class="mitem-author">${(AUTHORS[m.author]||{}).name||''}</div>
      <div class="mitem-meta">${metaHtml}</div>
    </div>
  </div>`;
}

function evtDateParts(iso){
  const [y,m,dd] = iso.split("-").map(Number);
  return {dom: dd, mon: RU_MONTHS_SHORT[m-1]};
}
function mskToEt(raw){
  if(!raw) return raw;
  const re = /(\d{1,2}):(\d{2})(?:\s*[-–]\s*(\d{1,2}):(\d{2}))?/;
  const m = raw.match(re);
  if(!m) return raw;
  const shift = (h,mi)=>{ let t = h*60+mi - 7*60; t = ((t % 1440)+1440)%1440; return String(Math.floor(t/60)).padStart(2,"0")+":"+String(t%60).padStart(2,"0"); };
  let out = shift(Number(m[1]), Number(m[2]));
  if(m[3]!==undefined) out += "–"+shift(Number(m[3]), Number(m[4]));
  return out;
}
function evtColor(i){ return i%2===0 ? "sage" : "lav"; }
function renderCalendar(){
  const events = STATE.events || [];
  if(calViewYear===null){
    const base = events.length ? events[0].date : TODAY_DATE_ISO;
    const [y,m] = base.split("-").map(Number);
    calViewYear = y; calViewMonth = m-1;
  }
  document.getElementById("calMonthLabel").textContent = RU_MONTHS_FULL[calViewMonth] + " " + calViewYear;
  const evByDate = {};
  events.forEach((e,i)=>{ evByDate[e.date] = evtColor(i); });
  const first = new Date(Date.UTC(calViewYear, calViewMonth, 1));
  const daysInMonth = new Date(Date.UTC(calViewYear, calViewMonth+1, 0)).getUTCDate();
  const leading = (first.getUTCDay()+6)%7;
  let html = "";
  for(let i=0;i<leading;i++) html += `<div class="cal-day"></div>`;
  for(let d=1; d<=daysInMonth; d++){
    const iso = calViewYear+"-"+String(calViewMonth+1).padStart(2,"0")+"-"+String(d).padStart(2,"0");
    const col = evByDate[iso];
    html += `<div class="cal-day${col?' ev c-'+col:''}"><span>${d}</span></div>`;
  }
  document.getElementById("calGrid").innerHTML = html;
  const usedColors = [...new Set(events.map((e,i)=>evtColor(i)))];
  document.getElementById("calLegend").innerHTML = usedColors.length
    ? usedColors.map(c=>`<span class="cal-dot c-${c}"></span>`).join("")+" Дни встреч"
    : "";
}
document.getElementById("calPrevBtn").addEventListener("click", ()=>{
  calViewMonth--; if(calViewMonth<0){ calViewMonth=11; calViewYear--; }
  renderCalendar();
});
document.getElementById("calNextBtn").addEventListener("click", ()=>{
  calViewMonth++; if(calViewMonth>11){ calViewMonth=0; calViewYear++; }
  renderCalendar();
});
function renderEvents(){
  const events = STATE.events || [];
  renderCalendar();
  document.getElementById("eventsList").innerHTML = events.map((e,i)=>{
    const isPast = e.date < TODAY_DATE_ISO;
    const dp = evtDateParts(e.date);
    const clickable = !isPast && e.link;
    const tag = clickable ? "a" : "div";
    const attrs = clickable ? ` href="${e.link}" target="_blank" rel="noopener"` : "";
    const col = evtColor(i);
    return `<${tag} class="evt-row ${isPast?'past':''}"${attrs}>
      <div class="evt-date c-${col}"><div class="d">${dp.dom}</div><div class="l">${dp.mon}</div></div>
      <div class="evt-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="3" y="6" width="13" height="12" rx="2.5"/><path d="m16 10 5-3v10l-5-3"/></svg></div>
      <div class="evt-body"><span class="evt-tag">Zoom-встреча</span><div class="evt-ttl">${e.title}</div><div class="evt-meta">${isPast?'прошёл · запись скоро':mskToEt(e.time||'')}</div></div>
      ${clickable?`<span class="evt-go c-${col}">Открыть Zoom &rsaquo;</span>`:''}
    </${tag}>`;
  }).join("");

  const todayEv = events.find(e=>e.date === TODAY_DATE_ISO);
  const alertEl = document.getElementById("todayEventAlert");
  alertEl.hidden = !todayEv;
  if(todayEv){
    document.getElementById("todayEventTxt").innerHTML = `сегодня${todayEv.time?(' в '+mskToEt(todayEv.time)):''} – <b>${todayEv.title}</b>`;
  }
}

function renderMaterials(){
  const flat = document.getElementById("mflat");
  document.querySelectorAll("#folderChips .chip").forEach(c=>c.classList.toggle("active", c.dataset.f===activeFolder));

  const items = MATERIALS.filter(m=>
    (activeFolder!=="all"||m.author!==STATE.intro_folder_id) &&
    (activeTheme==="all"||(m.tags||[]).includes(activeTheme)) &&
    (activeFolder==="all"||activeFolder==="__fav__"||m.author===activeFolder) &&
    (activeFolder!=="__fav__"||isFavorite(m.id)) &&
    (activeType==="all"||m.type===activeType) &&
    matchesQuery(m)
  );
  let headerHtml = "";
  if(activeFolder === "__fav__"){
    headerHtml = `<div class="folder-back" id="folderBackBtn">&#8592; все папки</div>
    <div class="mlist-title"><span>Избранное</span></div>`;
  } else if(activeFolder !== "all" && AUTHORS[activeFolder]){
    const a = AUTHORS[activeFolder];
    const photo = a.photo || PHOTOS[activeFolder] || "";
    const dotInner = photo ? `<img src="${photo}" alt="${a.name}">` : "";
    const dotCss = COLOR_CSS[a.color] || "";
    const dotStyleAttr = (!photo && dotCss) ? ` style="background:${dotCss}"` : "";
    headerHtml = `<div class="folder-back" id="folderBackBtn">&#8592; все папки</div>
    <div class="mcard ${activeFolder}" style="cursor:default;margin:0 0 10px;background:none;border:none;box-shadow:none;padding:4px 2px">
      <div class="dot"${dotStyleAttr}>${dotInner}</div>
      <div class="body"><h3>${a.name}</h3><div class="topics">${a.topics||''}</div>
      <div class="foot"><span>${a.desc||''}</span></div></div>
    </div>`;
  } else {
    headerHtml = `<div class="mlist-title"><span>Практики и материалы</span></div>`;
  }
  const visibleItems = items.slice(0, matVisibleCount);
  const moreHtml = items.length > matVisibleCount
    ? `<button class="mat-more-btn" id="matMoreBtn" type="button">Показать ещё · осталось ${items.length - matVisibleCount}</button>`
    : "";
  flat.innerHTML = headerHtml + (items.length ? visibleItems.map(mItemHtml).join("") + moreHtml :
    `<p class="note" style="margin:12px 0">Ничего не нашлось</p>`);
  const backBtn = flat.querySelector("#folderBackBtn");
  if(backBtn) backBtn.addEventListener("click", ()=>setActiveFolder("all"));
  const moreBtn = document.getElementById("matMoreBtn");
  if(moreBtn) moreBtn.addEventListener("click", ()=>{ matVisibleCount += MAT_PAGE_SIZE; renderMaterials(); });
  flat.querySelectorAll(".mitem[data-i]").forEach(row=>{
    const m = items[Number(row.dataset.i)];
    row.addEventListener("click", ()=>openMaterialDetailFor(m, (AUTHORS[m.author]||{}).name||"", "← назад", ()=>{
      document.getElementById("overlay").classList.remove("open");
    }));
  });
  flat.querySelectorAll(".mitem-fav").forEach(btn=>{
    btn.addEventListener("click", async e=>{
      e.stopPropagation();
      if(isFavorite(btn.dataset.favid) && !(await askConfirm("Убрать материал из избранного?", "Убрать"))) return;
      playPopSound();
      toggleFavorite(btn.dataset.favid);
      renderMaterials();
    });
  });
}
function openMaterial(key){
  const a = AUTHORS[key];
  openOverlay(a.name, a.topics);
  const items = MATERIALS.filter(m=>m.author===key);
  const rows = items.map((m,i)=>{
    const unlocked = m.day===null||m.day<=TODAY;
    const status = m.day===null ? `<span class="mat-status open">Открыто</span>`
      : unlocked ? `<span class="mat-status open">Открыто ${fmtDateRu(dayToDate(m.day))}</span>` : `<span class="mat-status locked">${ICONS.lock} с ${fmtDateRu(dayToDate(m.day))}</span>`;
    const newBadge = m.day===TODAY ? `<span class="mat-new">Новое</span>` : "";
    return `<div class="mat ${unlocked?'':'is-locked'}" ${unlocked?`data-i="${i}" style="cursor:pointer"`:""}><div class="mat-icon">${ICONS[m.type]||ICONS.pdf}</div>
      <div class="mat-body"><div class="mat-title">${m.title}${newBadge}</div><div class="mat-tags">#${(m.tags||[]).join(' #')}</div></div>${status}</div>`;
  }).join("");
  const anyLocked = items.some(m=>!(m.day===null||m.day<=TODAY));
  document.getElementById("overlayBody").innerHTML =
    `${rows}${anyLocked?'<p class="note">Доступ к файлу открывается в день разблокировки.</p>':''}`;
  document.getElementById("overlayBody").querySelectorAll(".mat[data-i]").forEach(row=>{
    row.addEventListener("click", ()=>openMaterialDetail(key, Number(row.dataset.i)));
  });
}
function openMaterialDetail(key, idx){
  const items = MATERIALS.filter(m=>m.author===key);
  openMaterialDetailFor(items[idx], (AUTHORS[key]||{}).name||"", "← к материалам "+((AUTHORS[key]||{}).name||""), ()=>openMaterial(key));
}
function openMaterialDetailFor(m, eyebrow, backLabel, onBack){
  openOverlay(m.title);
  const backBtn = document.getElementById("overlayBackBtn");
  backBtn.hidden = false;
  document.querySelector(".panel-head2").classList.add("has-back");
  backBtn.onclick = ()=>{ playPopSound(); onBack(); };
  const codeBox = m.mmId ? mmCodeBoxHtml(m.mmId) : "";
  const hasLink = m.url && m.url !== "#";
  const needsSign = hasLink && m.url.indexOf("storage:") === 0;
  const openLink = !hasLink ? "" : needsSign
    ? `<button id="matOpenBtn" class="btn primary" style="margin:4px 0 10px">Открыть материал →</button>`
    : `<a id="matOpenBtn" href="${m.url}" target="_blank" rel="noopener" class="btn primary" style="margin:4px 0 10px;display:inline-block;text-decoration:none">Открыть материал →</a>`;
  document.getElementById("overlayBody").innerHTML =
    `<div class="mat" style="margin-bottom:10px"><div class="mat-icon">${ICONS[m.type]||ICONS.pdf}</div>
      <div class="mat-body"><div class="mat-tags">#${(m.tags||[]).join(' #')}</div></div></div>
     ${openLink}
     <div class="mat-embed" id="matEmbed" hidden></div>
     ${codeBox}`;
  if(m.mmId) wireMmCodeBox(m.mmId);
  if(needsSign){
    document.getElementById("matOpenBtn").addEventListener("click", async ()=>{
      playPopSound();
      const btn = document.getElementById("matOpenBtn");
      btn.textContent = "Загружаю…"; btn.disabled = true;
      try{
        const resp = await apiFetch("/api/cabinet/materials/"+m.id+"/link");
        const data = await resp.json();
        if(resp.ok && data.ok && /^https?:\/\//.test(data.url)){
          renderMaterialEmbed(m.type, data.url);
          btn.hidden = true;
        } else {
          alert("Не получилось открыть файл. Попробуй ещё раз через минуту – если не поможет, напиши нам.");
          btn.textContent = "Открыть материал →"; btn.disabled = false;
        }
      }catch(e){
        alert("Не получилось открыть файл. Попробуй ещё раз через минуту – если не поможет, напиши нам.");
        btn.textContent = "Открыть материал →"; btn.disabled = false;
      }
    });
  }
}
function renderMaterialEmbed(type, url){
  const box = document.getElementById("matEmbed");
  const body = document.getElementById("overlayBody");
  let inner, isFrame = false, loadEvt = "load", tag = "iframe";
  if(type === "video"){
    inner = `<video controls playsinline style="width:100%;border-radius:12px;display:block" src="${url}"></video>`;
    loadEvt = "loadeddata"; tag = "video";
  } else if(type === "audio"){
    inner = `<audio controls style="width:100%;display:block" src="${url}"></audio>`;
    loadEvt = "loadeddata"; tag = "audio";
  } else {
    isFrame = true;
    inner = `<iframe src="${url}" title="материал"></iframe>`;
  }
  box.innerHTML = `<div class="mat-embed-loading" id="matEmbedLoading"><span class="mat-spinner"></span></div>`
    + inner + `<a href="${url}" target="_blank" rel="noopener" class="mat-embed-fallback">открыть в отдельной вкладке</a>`;
  box.hidden = false;
  box.classList.toggle("is-frame", isFrame);
  body.classList.toggle("no-scroll", isFrame);
  document.querySelector(".panel").classList.toggle("force-tall", isFrame);
  const loadingEl = document.getElementById("matEmbedLoading");
  const hideLoading = ()=>{ if(loadingEl) loadingEl.classList.add("hide"); };
  const mediaEl = box.querySelector(tag);
  if(mediaEl) mediaEl.addEventListener(loadEvt, hideLoading, {once:true});
  setTimeout(hideLoading, 8000);
}
document.getElementById("filtersToggleBtn").addEventListener("click", ()=>{
  playPopSound();
  const panel = document.getElementById("filtersPanel");
  panel.hidden = !panel.hidden;
  document.getElementById("filtersToggleBtn").classList.toggle("open", !panel.hidden);
});
document.getElementById("formatSelect").addEventListener("change", e=>{
  activeType = e.target.value;
  matVisibleCount = MAT_PAGE_SIZE;
  updateFiltersToggleState();
  renderMaterials();
});
document.getElementById("categorySelect").addEventListener("change", e=>{
  activeTheme = e.target.value;
  matVisibleCount = MAT_PAGE_SIZE;
  updateFiltersToggleState();
  renderMaterials();
});
document.getElementById("filtersResetBtn").addEventListener("click", ()=>{
  playPopSound();
  activeType = "all"; activeTheme = "all";
  document.getElementById("formatSelect").value = "all";
  renderThemeFilterOptions();
  updateFiltersToggleState();
  matVisibleCount = MAT_PAGE_SIZE;
  renderMaterials();
});
document.getElementById("filtersApplyBtn").addEventListener("click", ()=>{
  playPopSound();
  document.getElementById("filtersPanel").hidden = true;
  document.getElementById("filtersToggleBtn").classList.remove("open");
});
document.getElementById("folderChips").addEventListener("click", e=>{
  const b = e.target.closest(".chip"); if(!b) return;
  setActiveFolder(b.dataset.f);
});
document.getElementById("matFeatureBtn").addEventListener("click", ()=>{
  playPopSound();
  const key = STATE.intro_folder_id;
  if(key && AUTHORS[key]){ openMaterial(key); return; }
  openOverlay("Перед началом клуба");
  document.getElementById("overlayBody").innerHTML = `<p class="note" style="margin:0">Текст скоро добавим.</p>`;
});
const searchBox=document.getElementById("searchBox"), searchDrop=document.getElementById("searchDrop");
searchBox.addEventListener("input",()=>{
  query = searchBox.value.trim().toLowerCase();
  matVisibleCount = MAT_PAGE_SIZE;
  if(!query){ searchDrop.hidden=true; renderMaterials(); return; }
  const results = MATERIALS.filter(m=>m.author!==STATE.intro_folder_id && matchesQuery(m)).slice(0,6);
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
  document.querySelectorAll(".avatar .remove").forEach(btn=>btn.hidden = !p.photo_url);
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
async function removePhoto(){
  if(!(await askConfirm("Удалить фото профиля?", "Удалить"))) return;
  const p = STATE.profile || {};
  try{
    await apiFetch("/api/cabinet/profile", {method:"PUT", body:{
      name:p.name||"", age:p.age||null, city:p.city||"", bio:p.bio||"", photo_url:null
    }});
  }catch(e){}
  STATE.profile = Object.assign({}, p, {photo_url:null});
  renderProfileTile();
  playPopSound();
}
function wireCameraButtons(){
  document.querySelectorAll(".camera").forEach(btn=>{
    btn.onclick = (e)=>{ e.stopPropagation(); document.getElementById("avatarFileInput").click(); };
  });
  document.querySelectorAll(".avatar .remove").forEach(btn=>{
    btn.onclick = (e)=>{ e.stopPropagation(); removePhoto(); };
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
        <div class="remove" hidden><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M6 6l12 12M18 6 6 18"/></svg></div>
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
      const photo_url = (STATE.profile||{}).photo_url || null;
      await apiFetch("/api/cabinet/profile", {method:"PUT", body:{name, age, city, bio, photo_url}});
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
  setRefreshToken(null);
  location.reload();
});

</script>

<div id="askConfirmBox" style="display:none;position:fixed;inset:0;z-index:9999;background:rgba(40,34,48,.45);align-items:center;justify-content:center;padding:20px">
  <div style="background:#fff;border-radius:18px;max-width:340px;width:100%;padding:22px 20px 18px;box-shadow:0 24px 60px -20px rgba(0,0,0,.45);text-align:center">
    <div id="askConfirmMsg" style="font-size:15px;line-height:1.45;color:#3D3646;margin-bottom:18px"></div>
    <div style="display:flex;gap:10px;justify-content:center">
      <button type="button" id="askConfirmNo" style="flex:1;padding:11px 14px;border-radius:999px;border:1px solid #E6DED6;background:#fff;color:#3D3646;font:inherit;font-weight:600;cursor:pointer">Отмена</button>
      <button type="button" id="askConfirmYes" style="flex:1;padding:11px 14px;border-radius:999px;border:none;background:#B3415A;color:#fff;font:inherit;font-weight:600;cursor:pointer">Удалить</button>
    </div>
  </div>
</div>
<script>
function askConfirm(msg, yesLabel){
  return new Promise(resolve=>{
    const box=document.getElementById("askConfirmBox");
    document.getElementById("askConfirmMsg").textContent=msg;
    const yes=document.getElementById("askConfirmYes"), no=document.getElementById("askConfirmNo");
    yes.textContent=yesLabel||"Удалить";
    box.style.display="flex";
    const done=v=>{box.style.display="none";yes.onclick=no.onclick=box.onclick=null;resolve(v);};
    yes.onclick=()=>done(true); no.onclick=()=>done(false);
    box.onclick=e=>{ if(e.target===box) done(false); };
  });
}
</script>
</body></html>"""


ADMIN_APP_HTML = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<meta name="robots" content="noindex, nofollow">
<title>Serenity Moon – админка</title>
<link rel="icon" type="image/jpeg" href="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/app-icon.jpg">
<link rel="apple-touch-icon" href="https://zmwvcprkhudqqgmrduxn.supabase.co/storage/v1/object/public/avatars/brand/app-icon.jpg">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,500;0,600;1,500;1,600&family=Montserrat:wght@400;500;600&display=swap">
<style>
:root{
  --lav:#AA9DB8; --lav-ink:#6B5A7C; --sage:#6A9993; --sage-ink:#3F6660;
  --sand:#CBBAA8; --sand-ink:#8A6F55; --ink:#332E3D; --ink-soft:#6B6475;
  --bg:#FAF6F1; --surface:#FFFFFF; --surface-2:#F4F5F6; --line:#E6E8E9;
  --ok:#5C8A5C; --ok-bg:#E7F1E4; --warn:#B98A3E; --warn-bg:#FBF0DE;
  --shadow:0 10px 30px -14px rgba(51,46,61,.22);
  --sys:"Montserrat",-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,system-ui,"Helvetica Neue",Arial,sans-serif;
  --font-head:"Cormorant Garamond",Georgia,"Times New Roman",serif;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sys);touch-action:pan-x pan-y}
h1,h2,h3{margin:0;font-family:var(--font-head);font-weight:600;letter-spacing:-.01em}

.shell{max-width:1180px;margin:0 auto;padding:28px 20px 70px}
.top{display:flex;align-items:center;gap:10px;margin-bottom:4px}
.top .mark{width:28px;height:28px;border-radius:8px;background:var(--lav);color:#fff;flex-shrink:0;
  display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700}
.top .name{font-family:var(--font-head);font-weight:600;font-size:17px}
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
.editlink{display:block;width:100%;text-align:center;background:none;border:none;color:var(--ink-soft);font-size:12.5px;font-weight:600;cursor:pointer;font-family:var(--sys)}
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
.foldercard{background:var(--surface);border:1px solid var(--line);border-radius:18px;padding:16px 18px;margin-bottom:14px}
.foldercard .fhead{display:flex;align-items:center;gap:12px}
.foldercard .dot{width:42px;height:42px;border-radius:50%;flex-shrink:0;overflow:hidden;display:flex;align-items:center;justify-content:center;background:var(--surface-2)}
.foldercard .dot img{width:100%;height:100%;object-fit:cover}
.foldercard .fbody{flex:1;min-width:0}
.foldercard .fbody h3{font-size:16px;font-weight:600;margin:0}
.foldercard .fbody .topics{font-size:12px;color:var(--ink-soft);margin-top:2px}
.foldercard .fcount{font-size:11.5px;color:var(--ink-soft);flex-shrink:0;white-space:nowrap}
.foldercard .fchev{width:16px;height:16px;flex-shrink:0;color:var(--ink-soft);transition:transform .15s}
.foldercard .fchev.open{transform:rotate(90deg)}
.foldercard .factions{display:flex;gap:2px;flex-shrink:0}
.foldercard .factions .evbtn{border:none;background:none;color:var(--ink-soft);font-size:15px;cursor:pointer;padding:4px 6px}
.foldercard .mlist{margin-top:12px;display:flex;flex-direction:column;gap:6px}

.mstep{border:1px solid var(--line);border-radius:14px;margin-bottom:10px;background:var(--surface)}
.mstep-head{display:flex;align-items:center;gap:10px;padding:13px 16px;cursor:pointer}
.mstep.open .mstep-head{background:var(--surface-2)}
.mstep-num{width:22px;height:22px;border-radius:50%;background:var(--sage);color:#fff;font-size:11px;font-weight:700;
  display:flex;align-items:center;justify-content:center;flex-shrink:0}
.mstep.done .mstep-num{background:var(--lav)}
.mstep-title{font-size:13.5px;font-weight:600;flex:1;min-width:0}
.mstep-val{font-size:11.5px;color:var(--ink-soft);max-width:38%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mstep-chev{width:16px;height:16px;flex-shrink:0;color:var(--ink-soft);transition:transform .15s}
.mstep.open .mstep-chev{transform:rotate(90deg)}
.mstep-body{display:none;padding:4px 16px 16px}
.mstep.open .mstep-body{display:block}

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
          <thead><tr><th>#</th><th>Имя</th><th>Email</th><th>Телеграм</th><th>Привычек</th><th>% выполнения</th><th>💎</th><th>Мастермайнды</th><th>Была в кабинете</th><th></th></tr></thead>
          <tbody id="progBody"></tbody>
        </table>
      </div>
      <div class="crm-bottom-actions">
        <a href="/admin/export-progress.csv" class="primary" target="_blank">скачать CSV</a>
      </div>
    </div>
  </div>

  <div id="t-content">
    <div class="card">
      <h2>Материалы</h2>
      <div id="materialsList"></div>
    </div>
    <div class="card">
      <div style="display:flex;align-items:center;justify-content:space-between;gap:10px">
        <h2 style="margin:0">Новый материал</h2>
        <button class="editlink" id="toggleMaterialForm" type="button" style="width:auto;white-space:nowrap">+ добавить</button>
      </div>
      <div id="materialFormBody" style="display:none;margin-top:14px">
      <div class="hint">видео, аудио или pdf – папку и тему можно выбрать из уже существующих или создать прямо здесь</div>

      <div class="mstep" data-mstep="folder" id="mstepFolder">
        <div class="mstep-head" data-mstephead="folder">
          <span class="mstep-num">1</span>
          <span class="mstep-title">Куда положить</span>
          <span class="mstep-val" id="mstepValFolder"></span>
          <svg class="mstep-chev" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m9 6 6 6-6 6"/></svg>
        </div>
        <div class="mstep-body">
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
        </div>
      </div>

      <div class="mstep" data-mstep="what" id="mstepWhat">
        <div class="mstep-head" data-mstephead="what">
          <span class="mstep-num">2</span>
          <span class="mstep-title">Что загружаешь</span>
          <span class="mstep-val" id="mstepValWhat"></span>
          <svg class="mstep-chev" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m9 6 6 6-6 6"/></svg>
        </div>
        <div class="mstep-body">
          <div class="field">
            <label>Название <span class="req">*</span></label>
            <input type="text" id="mTitle" placeholder="Например: Видео-практика дыхания">
          </div>
          <div class="field">
            <label>Формат <span class="req">*</span></label>
            <div class="chipgrid" id="mFormatPicker"></div>
          </div>
          <div class="field">
            <label>Файл или ссылка</label>
            <input type="file" id="mFile" hidden>
            <div class="filebox" id="mFileBox">перетащи файл сюда или <b>выбери на компьютере</b><br>либо вставь ссылку ниже</div>
            <input type="text" id="mLink" placeholder="https://..." style="margin-top:8px">
          </div>
        </div>
      </div>

      <div class="mstep" data-mstep="when" id="mstepWhen">
        <div class="mstep-head" data-mstephead="when">
          <span class="mstep-num">3</span>
          <span class="mstep-title">Когда и как</span>
          <span class="mstep-val" id="mstepValWhen"></span>
          <svg class="mstep-chev" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m9 6 6 6-6 6"/></svg>
        </div>
        <div class="mstep-body">
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
            <div class="err" id="themeErr">выбери хотя бы одну тему – без неё материал не найти по фильтру</div>
          </div>
          <div class="field" id="evtLinkField">
            <div class="toggle-row">
              <span class="l"><span class="hintdot" id="evtHintDot">?</span>Привязать к эфиру календаря</span>
              <button class="sw" id="evtLinkToggle"></button>
            </div>
            <div class="note hintnote" id="evtHintNote">Включай, если это запись прошедшего онлайн-эфира (Zoom), а не изначально записанное видео/аудио</div>
            <div id="evtPickWrap" style="display:none;margin-top:10px">
              <select id="evtPick">
                <option>День 3 – Мастермайнд #1</option>
                <option>День 10 – Онлайн-практика дыхания</option>
                <option>День 15 – Мастермайнд #2</option>
              </select>
              <div class="note" style="margin-top:8px">после сохранения статус этого эфира в календаре сам сменится на «запись доступна»</div>
            </div>
          </div>
        </div>
      </div>

      <button class="save" id="saveMaterial">Сохранить материал</button>
      <button class="editlink" id="cancelEditMaterial" type="button" hidden style="margin-top:8px">Отменить редактирование</button>
      </div>
    </div>
  </div>

  <div id="t-calendar" style="display:none">
    <div class="card">
      <h2>Дата начала клуба</h2>
      <div class="hint">день 1 отсчитывается от этой даты; меняй только до запуска или по согласованию</div>
      <div class="field">
        <label>Дата начала</label>
        <input type="date" id="marathonStartInput">
      </div>
      <button id="marathonStartSave">Сохранить дату</button>
      <div class="hint" id="marathonStartStatus"></div>
    </div>
    <div class="card">
      <h2>Текущие эфиры клуба</h2>
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
        <input type="text" id="eTime" placeholder="19:00 (Нью-Йорк)">
      </div>
      <div class="field">
        <label>Ссылка на Zoom</label>
        <input type="text" id="eLink" placeholder="https://zoom.us/...">
      </div>
      <button class="save" id="saveEvent">Добавить в календарь</button>
      <button class="editlink" id="cancelEditEvent" type="button" hidden style="margin-top:8px">Отменить редактирование</button>
    </div>
  </div>

  <div id="t-events" style="display:none">
    <div class="card">
      <h2>Мероприятия Милы</h2>
      <div class="evlist" id="mevList"></div>
    </div>
    <div class="card">
      <h2>Добавить мероприятие</h2>
      <div class="field">
        <label>Название <span class="req">*</span></label>
        <input type="text" id="mevTitle" placeholder="Например: Ретрит выходного дня">
      </div>
      <div class="field">
        <label>Фото мероприятия</label>
        <input type="file" id="mevPhoto" accept="image/*">
      </div>
      <div class="field">
        <label>Дата <span class="req">*</span></label>
        <input type="text" id="mevDate" placeholder="25 октября">
      </div>
      <div class="field">
        <label>Место</label>
        <input type="text" id="mevPlace" placeholder="Дома у Милы (или «онлайн»)">
      </div>
      <div class="field">
        <label>Описание</label>
        <textarea id="mevDesc" rows="3" placeholder="Коротко, о чём мероприятие"></textarea>
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
      <button class="editlink" id="cancelEditMev" type="button" hidden style="margin-top:8px">Отменить редактирование</button>
    </div>
  </div>

  <div id="t-participants" style="display:none">
    <div class="crm-wrap">
      <div class="crm-top">
        <h1>Заявки клуба</h1>
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
    <div class="crm-modal-head"><h3>Добавить вручную</h3><button class="crm-modal-x" id="crmAddClose" type="button">✕</button></div>
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
/* ---------- блокировка зума на десктопе (моб. зум убран через viewport) ---------- */
document.addEventListener("wheel", e=>{ if(e.ctrlKey) e.preventDefault(); }, {passive:false});
document.addEventListener("keydown", e=>{
  if((e.ctrlKey||e.metaKey) && ["+","-","=","0"].includes(e.key)) e.preventDefault();
});

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
    <td>${p.name || "–"}</td>
    <td>${p.email || "–"}</td>
    <td>${p.telegram || "–"}</td>
    <td>${p.habit_count}</td>
    <td>${p.avg_pct}%</td>
    <td>${p.perfect_days + p.mastermind_count*3}</td>
    <td>${p.mastermind_count}/2</td>
    <td${isQuiet(p.last_active)?' style="color:#B3415A;font-weight:600"':''}>${fmtLastActive(p.last_active)}</td>
    <td><button class="crm-del" data-pdel="${p.id}" title="удалить участницу">✕</button></td>
  </tr>`).join("") : `<tr><td colspan="9" style="text-align:center;color:#999">пока никто не зарегистрировался в кабинете</td></tr>`;
  document.querySelectorAll("[data-pdel]").forEach(btn=>{
    btn.addEventListener("click", async ()=>{
      if(!(await askConfirm("Удалить эту участницу из кабинета? Это сотрёт её привычки и отметки, вернуть будет нельзя."))) return;
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

let THEMES = []; // [{id,label}] – грузится с сервера, см. loadThemes()
function renderMaterialThemes(){
  const wrap = document.getElementById("mThemes");
  wrap.innerHTML = THEMES.map(t=>`<div class="tchip" data-t="${t.id}">${t.label}</div>`).join("")
    + `<div class="tchip" id="themePlusChip">+ своя тема</div>`;
  wrap.querySelectorAll(".tchip").forEach(c=>{
    if(c.id === "themePlusChip") return;
    c.addEventListener("click", ()=>{ c.classList.toggle("on"); document.getElementById("themeErr").style.display="none"; updateMStepSummaries(); });
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
    updateMStepSummaries();
    showToast("Тема «"+v+"» добавлена ✓");
  }catch(e){}
});
loadThemes();

function buildDatePicker(baseId, startMonth, onChange){
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
        if(onChange) onChange();
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
  return {
    reset(){ selected=null; hidden.value=""; btn.textContent="Выбрать дату"; btn.classList.remove("has-val"); render(); },
    setValue(iso){
      selected = iso; hidden.value = iso;
      const dt = new Date(iso+"T00:00:00");
      view = new Date(iso+"T00:00:00");
      btn.textContent = dt.toLocaleDateString("ru-RU",{day:"numeric",month:"short",year:"numeric"});
      btn.classList.add("has-val");
      render();
    }
  };
}
document.addEventListener("click", ()=>{ document.querySelectorAll(".dpicker-pop").forEach(p=>p.style.display="none"); });
const mDayPicker = buildDatePicker("mDay", "2026-11-01", ()=>updateMStepSummaries());
const eDayPicker = buildDatePicker("eDay", "2026-11-01");

const FORMATS = [{v:"video",l:"Видео"},{v:"audio",l:"Аудио"},{v:"pdf",l:"PDF"},{v:"link",l:"Ссылка"},{v:"photo",l:"Фото"}];
let selectedFormat = FORMATS[0].v;
function renderFormatPicker(){
  const wrap = document.getElementById("mFormatPicker");
  wrap.innerHTML = FORMATS.map(f=>`<div class="tchip${f.v===selectedFormat?" on":""}" data-f="${f.v}">${f.l}</div>`).join("");
  wrap.querySelectorAll(".tchip").forEach(c=>{
    c.addEventListener("click", ()=>{ selectedFormat = c.dataset.f; renderFormatPicker(); updateMStepSummaries(); });
  });
}
renderFormatPicker();

let FOLDERS = []; // [{id,name,description,topics,photo,color}] – грузится с сервера
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
  // photos are shrunk to max 1200px and saved as JPEG so they stay small
  // (big phone photos used to fail with error 413 – request too large)
  return new Promise((resolve,reject)=>{
    const r = new FileReader();
    r.onerror = reject;
    r.onload = ()=>{
      const src = r.result;
      if(!/^image\//.test(file.type) || /svg|gif/.test(file.type)){ resolve(src); return; }
      const img = new Image();
      img.onload = ()=>{
        const MAX = 1200, k = Math.min(1, MAX/Math.max(img.width, img.height));
        const c = document.createElement("canvas");
        c.width = Math.round(img.width*k); c.height = Math.round(img.height*k);
        const ctx = c.getContext("2d");
        ctx.fillStyle = "#fff"; ctx.fillRect(0,0,c.width,c.height);
        ctx.drawImage(img, 0, 0, c.width, c.height);
        resolve(c.toDataURL("image/jpeg", 0.82));
      };
      img.onerror = ()=>resolve(src);
      img.src = src;
    };
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
      updateMStepSummaries();
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
  const dup = FOLDERS.some(f=>f.name===name && f.id!==editingFolderId);
  if(!name || dup){ document.getElementById("nfName").focus(); return; }
  const photoFile = document.getElementById("nfPhoto").files[0];
  const photo = photoFile ? await readFileAsDataUrl(photoFile) : null;
  const wasEditing = editingFolderId;
  try{
    const body = {
      name, description: document.getElementById("nfDesc").value.trim(),
      photo, color: photo ? "" : selectedNfColor
    };
    if(wasEditing) body.id = wasEditing;
    const f = await apiFetch("/api/admin/folders", {method:"POST", body: JSON.stringify(body)});
    if(wasEditing){
      FOLDERS = FOLDERS.map(x=>x.id===f.id?f:x);
      showToast("Папка «"+name+"» обновлена ✓");
    } else {
      FOLDERS.push(f);
      showToast("Папка «"+name+"» создана ✓");
    }
    selectedFolder = f.id;
    editingFolderId = null;
    document.getElementById("nfName").value = "";
    document.getElementById("nfDesc").value = "";
    document.getElementById("nfPhoto").value = "";
    selectedNfColor = NF_COLORS[0].v;
    renderNfColorPicker();
    document.getElementById("nfCreate").textContent = "Создать";
    document.getElementById("newFolderForm").style.display = "none";
    renderFolderPicker();
    renderMaterialsList();
    updateMStepSummaries();
  }catch(e){}
});
let editingFolderId = null;
function startEditFolder(f){
  editingFolderId = f.id;
  openMaterialForm(true);
  document.getElementById("newFolderForm").style.display = "flex";
  renderFolderPicker();
  document.getElementById("nfName").value = f.name || "";
  document.getElementById("nfDesc").value = f.description || "";
  selectedNfColor = f.color || NF_COLORS[0].v;
  renderNfColorPicker();
  document.getElementById("nfCreate").textContent = "Сохранить изменения";
  document.getElementById("newFolderForm").scrollIntoView({block:"center", behavior:"smooth"});
}
function cancelEditFolder(){
  editingFolderId = null;
  document.getElementById("nfName").value = "";
  document.getElementById("nfDesc").value = "";
  document.getElementById("nfPhoto").value = "";
  selectedNfColor = NF_COLORS[0].v;
  renderNfColorPicker();
  document.getElementById("nfCreate").textContent = "Создать";
  document.getElementById("newFolderForm").style.display = "none";
  renderFolderPicker();
}

let MATERIALS = []; // [{id,folder_id,title,format,tags,day,url,calendar_event_id}] – грузится с сервера
let openFolderIds = new Set();
function renderMaterialsList(){
  const wrap = document.getElementById("materialsList");
  if(!FOLDERS.length){ wrap.innerHTML = `<div class="hint" style="margin:0">пока нет папок – создай первую через «+ добавить»</div>`; return; }
  wrap.innerHTML = FOLDERS.map(f=>{
    const mats = MATERIALS.filter(m=>m.folder_id===f.id);
    const colorCss = (NF_COLORS.find(c=>c.v===f.color)||{}).css || "var(--surface-2)";
    const dotInner = f.photo ? `<img src="${f.photo}" alt="${f.name}">` : "";
    const dotStyle = f.photo ? "" : ` style="background:${colorCss}"`;
    const matsHtml = mats.length ? mats.map(m=>{
      const fmt = FORMATS.find(x=>x.v===m.format);
      return `<div class="evrow">
        <div class="evbody"><div class="evttl">${m.title}</div><div class="evmeta">${fmt?fmt.l:(m.format||"")}${m.day?` · день ${m.day}`:''}</div></div>
        <button class="evbtn" data-matedit="${m.id}" aria-label="редактировать">✎</button>
        <button class="evbtn" data-matdel="${m.id}">✕</button>
      </div>`;
    }).join("") : `<div class="hint" style="margin:0">материалов нет</div>`;
    const isOpen = openFolderIds.has(f.id);
    return `<div class="foldercard">
      <div class="fhead" data-foldertoggle="${f.id}" style="cursor:pointer">
        <div class="dot"${dotStyle}>${dotInner}</div>
        <div class="fbody"><h3>${f.name}</h3>${f.description?`<div class="topics">${f.description}</div>`:''}</div>
        <svg class="fchev ${isOpen?'open':''}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m9 6 6 6-6 6"/></svg>
        <div class="factions">
          <button class="evbtn" data-folderedit="${f.id}" aria-label="редактировать папку">✎</button>
          <button class="evbtn" data-folderdel="${f.id}">✕</button>
        </div>
      </div>
      <div class="mlist" ${isOpen?'':'hidden'}>${matsHtml}</div>
    </div>`;
  }).join("");
  wrap.querySelectorAll("[data-foldertoggle]").forEach(head=>{
    head.addEventListener("click", e=>{
      if(e.target.closest(".factions")) return;
      const id = head.dataset.foldertoggle;
      if(openFolderIds.has(id)) openFolderIds.delete(id); else openFolderIds.add(id);
      renderMaterialsList();
    });
  });
  wrap.querySelectorAll("[data-matdel]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      if(!(await askConfirm("Удалить материал?"))) return;
      try{
        await apiFetch(`/api/admin/materials/${b.dataset.matdel}`, {method:"DELETE"});
        MATERIALS = MATERIALS.filter(m=>String(m.id)!==b.dataset.matdel);
        renderMaterialsList();
        showToast("Материал удалён");
        if(String(editingMaterialId)===b.dataset.matdel) cancelEditMaterial();
      }catch(e){}
    });
  });
  wrap.querySelectorAll("[data-matedit]").forEach(b=>{
    b.addEventListener("click", ()=>{
      const m = MATERIALS.find(x=>String(x.id)===b.dataset.matedit);
      if(m) startEditMaterial(m);
    });
  });
  wrap.querySelectorAll("[data-folderdel]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      if(MATERIALS.some(m=>m.folder_id===b.dataset.folderdel)){
        showToast("В этой папке есть материалы – сначала удали или перенеси их");
        return;
      }
      if(!(await askConfirm("Удалить папку?"))) return;
      try{
        await apiFetch(`/api/admin/folders/${b.dataset.folderdel}`, {method:"DELETE"});
        FOLDERS = FOLDERS.filter(f=>f.id!==b.dataset.folderdel);
        renderMaterialsList();
        renderFolderPicker();
        showToast("Папка удалена");
      }catch(e){}
    });
  });
  wrap.querySelectorAll("[data-folderedit]").forEach(b=>{
    b.addEventListener("click", ()=>{
      const f = FOLDERS.find(x=>x.id===b.dataset.folderedit);
      if(f) startEditFolder(f);
    });
  });
}
async function loadMaterials(){
  try{ MATERIALS = await apiFetch("/api/admin/materials"); }catch(e){ MATERIALS = []; }
  renderMaterialsList();
}
let openMStep = "folder";
function setMStepOpen(key){
  openMStep = key;
  document.querySelectorAll(".mstep").forEach(el=>{
    el.classList.toggle("open", el.dataset.mstep===key);
  });
}
document.querySelectorAll("[data-mstephead]").forEach(h=>{
  h.addEventListener("click", ()=>{
    const key = h.dataset.mstephead;
    if(openMStep !== key) setMStepOpen(key);
  });
});
function updateMStepSummaries(){
  const f = FOLDERS.find(x=>x.id===selectedFolder);
  document.getElementById("mstepValFolder").textContent = f ? f.name : "";
  document.getElementById("mstepFolder").classList.toggle("done", !!selectedFolder);

  const title = document.getElementById("mTitle").value.trim();
  const fmt = FORMATS.find(x=>x.v===selectedFormat);
  const whatParts = [];
  if(title) whatParts.push(title);
  if(fmt) whatParts.push(fmt.l);
  document.getElementById("mstepValWhat").textContent = whatParts.join(" · ");
  document.getElementById("mstepWhat").classList.toggle("done", !!title);

  const themeCount = document.querySelectorAll("#mThemes .tchip.on").length;
  const dayVal = document.getElementById("mDay") ? document.getElementById("mDay").value : "";
  const whenParts = [];
  if(dayVal){
    const dt = new Date(dayVal+"T00:00:00");
    whenParts.push(dt.toLocaleDateString("ru-RU",{day:"numeric",month:"short"}));
  }
  if(themeCount) whenParts.push(themeCount+(themeCount===1?" тема":" темы"));
  document.getElementById("mstepValWhen").textContent = whenParts.join(" · ");
  document.getElementById("mstepWhen").classList.toggle("done", themeCount>0);
}
function openMaterialForm(show){
  document.getElementById("materialFormBody").style.display = show ? "" : "none";
  document.getElementById("toggleMaterialForm").textContent = show ? "свернуть" : "+ добавить";
  if(show){ setMStepOpen("folder"); updateMStepSummaries(); }
}
document.getElementById("toggleMaterialForm").addEventListener("click", ()=>{
  const open = document.getElementById("materialFormBody").style.display !== "none";
  if(open && editingMaterialId) cancelEditMaterial();
  if(open && editingFolderId) cancelEditFolder();
  openMaterialForm(!open);
});
loadFolders().then(loadMaterials);

document.getElementById("evtHintDot").addEventListener("click", ()=>{
  document.getElementById("evtHintDot").classList.toggle("on");
  document.getElementById("evtHintNote").classList.toggle("show");
});
const evtToggle = document.getElementById("evtLinkToggle");
evtToggle.addEventListener("click", ()=>{
  evtToggle.classList.toggle("on");
  document.getElementById("evtPickWrap").style.display = evtToggle.classList.contains("on") ? "" : "none";
});

let MARATHON_START_ISO = "2026-11-09"; // подтягивается с сервера, см. loadMarathonStart()
function isoDateToMarathonDay(iso){
  if(!iso) return null;
  const a = new Date(MARATHON_START_ISO+"T00:00:00"), b = new Date(iso+"T00:00:00");
  return Math.round((b-a)/86400000) + 1;
}
function dayToIso(day){
  if(!day) return "";
  const [y,mn,dd] = MARATHON_START_ISO.split("-").map(Number);
  const dt = new Date(Date.UTC(y, mn-1, dd));
  dt.setUTCDate(dt.getUTCDate() + (day-1));
  return dt.toISOString().slice(0,10);
}
async function loadMarathonStart(){
  const r = await fetch("/api/admin/settings/marathon-start");
  const data = await r.json();
  if(data.marathon_start){
    MARATHON_START_ISO = data.marathon_start;
    document.getElementById("marathonStartInput").value = data.marathon_start;
  }
}
document.getElementById("marathonStartSave").addEventListener("click", async ()=>{
  const val = document.getElementById("marathonStartInput").value;
  const statusEl = document.getElementById("marathonStartStatus");
  if(!val){ statusEl.textContent = "выбери дату"; return; }
  statusEl.textContent = "сохраняю…";
  const r = await fetch("/api/admin/settings/marathon-start", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({marathon_start: val})
  });
  const data = await r.json();
  if(r.ok && data.marathon_start){
    MARATHON_START_ISO = data.marathon_start;
    statusEl.textContent = "сохранено: " + data.marathon_start;
  } else {
    statusEl.textContent = "ошибка сохранения";
  }
});
loadMarathonStart();

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
    box.innerHTML = "файл загружен: <b>" + file.name + "</b> (заменить – выбери другой файл)";
    showToast("Файл «"+file.name+"» загружен ✓");
  }catch(e){
    box.innerHTML = "не получилось загрузить файл – перетащи файл сюда или <b>выбери на компьютере</b><br>либо вставь ссылку ниже";
    showToast("ошибка загрузки файла");
  }
}
document.getElementById("mTitle").addEventListener("input", updateMStepSummaries);
document.getElementById("mFileBox").addEventListener("click", ()=>document.getElementById("mFile").click());
document.getElementById("mFile").addEventListener("change", e=>{
  if(e.target.files[0]) uploadMaterialFile(e.target.files[0]);
});
document.getElementById("mFileBox").addEventListener("dragover", e=>{ e.preventDefault(); });
document.getElementById("mFileBox").addEventListener("drop", e=>{
  e.preventDefault();
  if(e.dataTransfer.files[0]) uploadMaterialFile(e.dataTransfer.files[0]);
});
let editingMaterialId = null;
function startEditMaterial(m){
  editingMaterialId = m.id;
  openMaterialForm(true);
  selectedFolder = m.folder_id; renderFolderPicker();
  document.getElementById("mTitle").value = m.title || "";
  selectedFormat = m.format || FORMATS[0].v; renderFormatPicker();
  if(m.day) mDayPicker.setValue(dayToIso(m.day)); else mDayPicker.reset();
  renderMaterialThemes();
  (m.tags||[]).forEach(t=>{
    const chip = document.querySelector(`#mThemes .tchip[data-t="${t}"]`);
    if(chip) chip.classList.add("on");
  });
  document.getElementById("mLink").value = (m.url && m.url!=="#") ? m.url : "";
  document.getElementById("mFileBox").innerHTML = "перетащи файл сюда или <b>выбери на компьютере</b><br>либо вставь ссылку ниже";
  const hasEvent = !!m.calendar_event_id;
  evtToggle.classList.toggle("on", hasEvent);
  document.getElementById("evtPickWrap").style.display = hasEvent ? "" : "none";
  if(hasEvent) document.getElementById("evtPick").value = m.calendar_event_id;
  document.getElementById("saveMaterial").textContent = "Сохранить изменения";
  document.getElementById("cancelEditMaterial").hidden = false;
  updateMStepSummaries();
  document.getElementById("materialFormBody").scrollIntoView({block:"center", behavior:"smooth"});
}
function cancelEditMaterial(){
  editingMaterialId = null;
  document.getElementById("mTitle").value = "";
  document.getElementById("mLink").value = "";
  document.getElementById("mFile").value = "";
  document.getElementById("mFileBox").innerHTML = "перетащи файл сюда или <b>выбери на компьютере</b><br>либо вставь ссылку ниже";
  mDayPicker.reset();
  document.querySelectorAll("#mThemes .tchip.on").forEach(c=>c.classList.remove("on"));
  evtToggle.classList.remove("on");
  document.getElementById("evtPickWrap").style.display = "none";
  document.getElementById("saveMaterial").textContent = "Сохранить материал";
  document.getElementById("cancelEditMaterial").hidden = true;
  setMStepOpen("folder");
  updateMStepSummaries();
}
document.getElementById("cancelEditMaterial").addEventListener("click", cancelEditMaterial);
document.getElementById("saveMaterial").addEventListener("click", async ()=>{
  const title = document.getElementById("mTitle").value.trim();
  const themeIds = [...document.querySelectorAll("#mThemes .tchip.on")].map(c=>c.dataset.t);
  if(!selectedFolder){ setMStepOpen("folder"); document.getElementById("folderPicker").scrollIntoView({block:"center"}); return; }
  if(!title){ setMStepOpen("what"); document.getElementById("mTitle").focus(); return; }
  if(!themeIds.length){ setMStepOpen("when"); document.getElementById("themeErr").style.display = "block"; document.getElementById("themeErr").scrollIntoView({block:"center"}); return; }
  const evOn = evtToggle.classList.contains("on");
  const body = JSON.stringify({
    folder_id: selectedFolder, title, format: selectedFormat,
    day: isoDateToMarathonDay(document.getElementById("mDay").value),
    tags: themeIds, url: document.getElementById("mLink").value.trim() || "#",
    calendar_event_id: evOn ? (document.getElementById("evtPick").value || null) : null,
  });
  try{
    if(editingMaterialId){
      await apiFetch(`/api/admin/materials/${editingMaterialId}`, {method:"PUT", body});
      showToast("Материал «"+title+"» обновлён ✓");
      cancelEditMaterial();
    } else {
      await apiFetch("/api/admin/materials", {method:"POST", body});
      showToast("Материал «"+title+"» сохранён ✓");
      document.getElementById("mTitle").value = "";
      document.getElementById("mLink").value = "";
      document.getElementById("mFile").value = "";
      document.getElementById("mFileBox").innerHTML = "перетащи файл сюда или <b>выбери на компьютере</b><br>либо вставь ссылку ниже";
      mDayPicker.reset();
      document.querySelectorAll("#mThemes .tchip.on").forEach(c=>c.classList.remove("on"));
      setMStepOpen("folder");
      updateMStepSummaries();
    }
    await loadMaterials();
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
let EVENTS = []; // [{id,date,title,type,time,link}] – грузится с сервера
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
      <button class="evbtn" data-edit="${e.id}" aria-label="редактировать">✎</button>
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
        if(String(editingEventId)===b.dataset.del) cancelEditEvent();
      }catch(e){}
    });
  });
  list.querySelectorAll("[data-edit]").forEach(b=>{
    b.addEventListener("click", ()=>{
      const ev = EVENTS.find(e=>String(e.id)===b.dataset.edit);
      if(ev) startEditEvent(ev);
    });
  });
}
let editingEventId = null;
function startEditEvent(ev){
  editingEventId = ev.id;
  eDayPicker.setValue(ev.date);
  document.getElementById("eTitle").value = ev.title || "";
  document.getElementById("eTime").value = ev.time || "";
  document.getElementById("eLink").value = ev.link || "";
  if(ev.type && !EVENT_TYPES.includes(ev.type)) EVENT_TYPES.push(ev.type);
  selectedType = ev.type || EVENT_TYPES[0];
  renderTypePicker();
  const btn = document.getElementById("saveEvent");
  btn.textContent = "Сохранить изменения";
  document.getElementById("cancelEditEvent").hidden = false;
  document.getElementById("eTitle").closest("section, div")?.scrollIntoView({block:"center", behavior:"smooth"});
}
function cancelEditEvent(){
  editingEventId = null;
  eDayPicker.reset();
  document.getElementById("eTitle").value = "";
  document.getElementById("eTime").value = "";
  document.getElementById("eLink").value = "";
  selectedType = EVENT_TYPES[0];
  renderTypePicker();
  document.getElementById("saveEvent").textContent = "Добавить в календарь";
  document.getElementById("cancelEditEvent").hidden = true;
}
function renderEvtPick(){
  const sel = document.getElementById("evtPick");
  sel.innerHTML = EVENTS.length
    ? EVENTS.slice().sort((a,b)=>a.date.localeCompare(b.date)).map(e=>{
        const d = fmtDate(e.date);
        return `<option value="${e.id}">${d.num} ${d.lbl} – ${e.title}</option>`;
      }).join("")
    : `<option value="">пока нет эфиров – сначала добавь в "Календарь"</option>`;
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
  const body = JSON.stringify({
    date: eventDate, title, type: selectedType,
    time: document.getElementById("eTime").value.trim() || "19:00 (Нью-Йорк)",
    link: document.getElementById("eLink").value.trim(),
  });
  try{
    if(editingEventId){
      await apiFetch(`/api/admin/calendar-events/${editingEventId}`, {method:"PUT", body});
      showToast("Эфир «"+title+"» обновлён ✓");
      cancelEditEvent();
    } else {
      await apiFetch("/api/admin/calendar-events", {method:"POST", body});
      showToast("Эфир «"+title+"» добавлен в календарь ✓");
      eDayPicker.reset();
      document.getElementById("eTitle").value = "";
      document.getElementById("eLink").value = "";
    }
    await loadCalendarEvents();
  }catch(e){}
});
document.getElementById("cancelEditEvent").addEventListener("click", cancelEditEvent);

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

let MILA_EVENTS = []; // [{id,title,date_place,link,color}] – грузится с сервера
function renderMevList(){
  const wrap = document.getElementById("mevList");
  wrap.innerHTML = MILA_EVENTS.length ? MILA_EVENTS.map((e)=>{
    const c = MEV_COLORS.find(x=>x.v===e.color) || MEV_COLORS[0];
    return `<div class="mevrow">
      <div class="ph" style="background:${c.css}"></div>
      <div class="b"><h4>${e.title}</h4><div class="dt">${e.date_place}</div></div>
      <button class="evbtn" data-medit="${e.id}" aria-label="редактировать">✎</button>
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
        if(String(editingMevId)===b.dataset.del) cancelEditMev();
      }catch(e){}
    });
  });
  wrap.querySelectorAll("[data-medit]").forEach(b=>{
    b.addEventListener("click", ()=>{
      const ev = MILA_EVENTS.find(e=>String(e.id)===b.dataset.medit);
      if(ev) startEditMev(ev);
    });
  });
}
let editingMevId = null;
function startEditMev(ev){
  editingMevId = ev.id;
  document.getElementById("mevTitle").value = ev.title || "";
  document.getElementById("mevDate").value = ev.date || "";
  document.getElementById("mevPlace").value = ev.place || "";
  document.getElementById("mevDesc").value = ev.description || "";
  document.getElementById("mevLink").value = (ev.link && ev.link!=="#") ? ev.link : "";
  document.getElementById("mevPhoto").value = "";
  selectedMevColor = ev.color || MEV_COLORS[0].v;
  renderMevColorPicker();
  document.getElementById("saveEventMila").textContent = "Сохранить изменения";
  document.getElementById("cancelEditMev").hidden = false;
  document.getElementById("mevTitle").scrollIntoView({block:"center", behavior:"smooth"});
}
function cancelEditMev(){
  editingMevId = null;
  document.getElementById("mevTitle").value = "";
  document.getElementById("mevDate").value = "";
  document.getElementById("mevPlace").value = "";
  document.getElementById("mevDesc").value = "";
  document.getElementById("mevLink").value = "";
  document.getElementById("mevPhoto").value = "";
  selectedMevColor = MEV_COLORS[0].v;
  renderMevColorPicker();
  document.getElementById("saveEventMila").textContent = "Добавить мероприятие";
  document.getElementById("cancelEditMev").hidden = true;
}
async function loadMilaEvents(){
  try{ MILA_EVENTS = await apiFetch("/api/admin/mila-events"); }catch(e){ MILA_EVENTS = []; }
  renderMevList();
}
loadMilaEvents();
document.getElementById("saveEventMila").addEventListener("click", async ()=>{
  const title = document.getElementById("mevTitle").value.trim();
  const dateVal = document.getElementById("mevDate").value.trim();
  if(!title){ document.getElementById("mevTitle").focus(); return; }
  if(!dateVal){ document.getElementById("mevDate").focus(); return; }
  const photoFile = document.getElementById("mevPhoto").files[0];
  const photo = photoFile ? await readFileAsDataUrl(photoFile) : null;
  const body = JSON.stringify({
    title, date: dateVal, place: document.getElementById("mevPlace").value.trim(),
    description: document.getElementById("mevDesc").value.trim(), photo,
    link: document.getElementById("mevLink").value.trim() || "#", color: selectedMevColor,
  });
  try{
    if(editingMevId){
      await apiFetch(`/api/admin/mila-events/${editingMevId}`, {method:"PUT", body});
      showToast("Мероприятие «"+title+"» обновлено ✓");
      cancelEditMev();
    } else {
      await apiFetch("/api/admin/mila-events", {method:"POST", body});
      showToast("Мероприятие «"+title+"» добавлено ✓");
      document.getElementById("mevTitle").value = "";
      document.getElementById("mevDate").value = "";
      document.getElementById("mevPlace").value = "";
      document.getElementById("mevDesc").value = "";
      document.getElementById("mevLink").value = "";
      document.getElementById("mevPhoto").value = "";
    }
    await loadMilaEvents();
  }catch(e){}
});
document.getElementById("cancelEditMev").addEventListener("click", cancelEditMev);

/* ---- CRM: реальные данные с /api/admin/* на этом же сервере – та же cookie-сессия,
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
  if(!ts) return "–";
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
      <td class="nm">${p.name||"–"}<span class="em">${p.email||""}</span></td>
      <td>${p.phone||"–"}</td>
      <td>${p.instagram||"–"}</td>
      <td>${p.method||"–"}</td>
      <td><input class="crm-amt" data-amount="${p.id}" value="${p.amount!=null?p.amount:""}" placeholder="–" inputmode="decimal"></td>
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
        showToast((row.name||row.email)+" отмечена оплаченной – приглашение в канал отправлено");
      }catch(e){ b.disabled = false; }
    });
  });
  body.querySelectorAll("[data-del]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      if(!(await askConfirm("Удалить эту заявку?"))) return;
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
<div id="askConfirmBox" style="display:none;position:fixed;inset:0;z-index:9999;background:rgba(40,34,48,.45);align-items:center;justify-content:center;padding:20px">
  <div style="background:#fff;border-radius:18px;max-width:340px;width:100%;padding:22px 20px 18px;box-shadow:0 24px 60px -20px rgba(0,0,0,.45);font-family:inherit;text-align:center">
    <div id="askConfirmMsg" style="font-size:15px;line-height:1.45;color:#3D3646;margin-bottom:18px"></div>
    <div style="display:flex;gap:10px;justify-content:center">
      <button type="button" id="askConfirmNo" style="flex:1;padding:11px 14px;border-radius:999px;border:1px solid #E6DED6;background:#fff;color:#3D3646;font:inherit;font-weight:600;cursor:pointer">Отмена</button>
      <button type="button" id="askConfirmYes" style="flex:1;padding:11px 14px;border-radius:999px;border:none;background:#B3415A;color:#fff;font:inherit;font-weight:600;cursor:pointer">Удалить</button>
    </div>
  </div>
</div>
<script>
function askConfirm(msg){
  return new Promise(resolve=>{
    const box=document.getElementById("askConfirmBox");
    document.getElementById("askConfirmMsg").textContent=msg;
    box.style.display="flex";
    const yes=document.getElementById("askConfirmYes"), no=document.getElementById("askConfirmNo");
    const done=v=>{box.style.display="none";yes.onclick=no.onclick=box.onclick=null;resolve(v);};
    yes.onclick=()=>done(true); no.onclick=()=>done(false);
    box.onclick=e=>{ if(e.target===box) done(false); };
  });
}
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


async def api_auth_refresh(request):
    """Обменивает refresh_token на новую пару access/refresh – так участница не вылетает
    из кабинета каждый час (время жизни access_token в Supabase по умолчанию – 1 час)."""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    refresh_token = str(data.get("refresh_token", "")).strip()
    if not refresh_token:
        return web.json_response({"ok": False, "error": "bad_request"}, status=400)
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return web.json_response({"ok": False, "error": "auth_not_configured"}, status=500)

    async with ClientSession() as session:
        async with session.post(
            f"{SUPABASE_URL}/auth/v1/token?grant_type=refresh_token",
            json={"refresh_token": refresh_token},
            headers={"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"},
        ) as resp:
            result = await resp.json()
            if resp.status >= 400:
                msg = result.get("msg") or result.get("error_description") or "refresh_failed"
                return web.json_response({"ok": False, "error": msg}, status=resp.status)

    return web.json_response({
        "ok": True,
        "access_token": result.get("access_token"),
        "refresh_token": result.get("refresh_token"),
    })


async def api_auth_recover(request):
    """Запрос письма восстановления пароля. Supabase всегда отвечает 200, даже если
    такого email нет – так мы не палим, кто зарегистрирован, кто нет."""
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
    """Установка нового пароля – вызывается после перехода по ссылке восстановления,
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
    profiles (создаётся при регистрации), но если её почему-то нет – создаём на лету,
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
    """Полностью заменяет набор привычек участницы – так же, как работает пикер в кабинете."""
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

    marathon_start = await get_marathon_start()
    today = marathon_day_today(marathon_start, MARATHON_TZ)
    materials_data = await load_materials_from_db()

    cal_rows = await DB_POOL.fetch("SELECT * FROM calendar_events ORDER BY date")
    start = date.fromisoformat(marathon_start)
    events = [{
        "day": (r["date"] - start).days + 1, "date": r["date"].isoformat(), "title": r["title"],
        "time": r["time"] or "", "type": r["type"] or "", "link": r["link"] or "",
    } for r in cal_rows]

    mila_event_rows = await DB_POOL.fetch("SELECT * FROM mila_events ORDER BY id DESC")
    mila_events = [_mila_event_json(r) for r in mila_event_rows]

    return web.json_response({
        "ok": True, "profile": profile, "habits": habits, "logs": logs, "today": today,
        "today_date": datetime.now(ZoneInfo(MARATHON_TZ)).date().isoformat(),
        "marathon_start": marathon_start, "mastermind": mastermind,
        "authors": materials_data["authors"], "materials": materials_data["materials"], "events": events,
        "mila_events": mila_events,
        "intro_folder_id": INTRO_FOLDER_ID,
    })


def verify_stripe_signature(payload: str, sig_header: str) -> bool:
    """Проверка, что вебхук реально от Stripe (см. Stripe Webhook signing)."""
    if not STRIPE_WEBHOOK_SECRET:
        return False  # секрет не задан – отклоняем всё, а не пропускаем (fail-closed)
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
    headers = ["#", "name", "email", "phone", "телеграм", "method", "amount", "status", "note", "created", "confirmed"]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    for i, r in enumerate(rows, start=1):
        row = []
        for c in cols:
            if c == "id":
                row.append(i)
                continue
            v = r[c]
            if c in ("created", "confirmed") and v:
                v = time.strftime("%Y-%m-%d %H:%M", time.localtime(v))
            row.append(v if v is not None else "")
        writer.writerow(row)
    resp = web.Response(text="﻿" + buf.getvalue(), content_type="text/csv", charset="utf-8")
    resp.headers["Content-Disposition"] = 'attachment; filename="serenity-signups.csv"'
    return resp


async def admin_export_progress(request):
    if not authed(request):
        raise web.HTTPFound("/admin/login")
    profiles = await DB_POOL.fetch("SELECT id, name, email, mastermind FROM profiles ORDER BY created DESC")
    signup_rows = await DB_POOL.fetch("SELECT email_norm, instagram FROM signups WHERE instagram IS NOT NULL AND instagram <> ''")
    telegram_by_email = {r["email_norm"]: r["instagram"] for r in signup_rows}
    headers = ["#", "имя", "email", "телеграм", "привычек", "% выполнения", "бриллиантов", "мастермайндов", "была в кабинете"]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    for i, p in enumerate(profiles, start=1):
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
        telegram = telegram_by_email.get(normalize_email(p["email"] or ""), "")
        diamonds = perfect_days + mm_count * 3
        last_active = last_date.isoformat() if last_date else "ни разу"
        writer.writerow([i, p["name"] or "", p["email"] or "", telegram, habit_count,
                          f"{avg_pct}%", diamonds, f"{mm_count}/2", last_active])
    resp = web.Response(text="﻿" + buf.getvalue(), content_type="text/csv", charset="utf-8")
    resp.headers["Content-Disposition"] = 'attachment; filename="serenity-progress.csv"'
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
    """Личный прогресс каждой зарегистрированной в кабинете участницы – для вкладки
    «Участницы» в админке (кто отстаёт/пропал, кому написать лично)."""
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    profiles = await DB_POOL.fetch("SELECT id, name, email, mastermind FROM profiles ORDER BY created DESC")
    signup_rows = await DB_POOL.fetch("SELECT email_norm, instagram FROM signups WHERE instagram IS NOT NULL AND instagram <> ''")
    telegram_by_email = {r["email_norm"]: r["instagram"] for r in signup_rows}
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
        telegram = telegram_by_email.get(normalize_email(p["email"] or ""), "")
        result.append({
            "id": str(uid), "name": p["name"] or "", "email": p["email"] or "", "telegram": telegram,
            "habit_count": habit_count, "avg_pct": avg_pct, "perfect_days": perfect_days,
            "mastermind_count": mm_count, "last_active": last_date.isoformat() if last_date else None,
        })
    return web.json_response(result)


async def api_admin_participant_delete(request):
    """Удаляет профиль участницы из кабинета (вместе с её привычками/отметками –
    ON DELETE CASCADE). Сам логин в supabase auth не трогает – только данные в нашей базе."""
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
    """Правка заметки и/или суммы (venmo/zelle не передают сумму сами –
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
            "date": r["date_label"] or "", "place": r["place_label"] or "", "photo": r["photo_url"],
            "link": r["link"], "color": r["color"], "description": r["description"] or ""}


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


async def api_materials_update(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    mat_id = int(request.match_info["id"])
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
    await DB_POOL.execute(
        "UPDATE materials SET folder_id=$2, title=$3, format=$4, tags=$5, day=$6, url=$7, calendar_event_id=$8 WHERE id=$1",
        mat_id, folder_id, title, str(data.get("format", "link")).strip(),
        json.dumps(data.get("tags", []), ensure_ascii=False), day,
        str(data.get("url", "")).strip() or "#", data.get("calendar_event_id"),
    )
    row = await DB_POOL.fetchrow("SELECT * FROM materials WHERE id=$1", mat_id)
    if row is None:
        return web.json_response({"error": "not_found"}, status=404)
    return web.json_response(_material_json(row))


async def api_materials_delete(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    await DB_POOL.execute("DELETE FROM materials WHERE id=$1", int(request.match_info["id"]))
    return web.json_response({"ok": True})


MATERIALS_UPLOAD_MAX_BYTES = 80 * 1024 * 1024  # 80MB – хватает на видео-уроки и аудио


async def api_materials_upload(request):
    """Загружает файл материала в приватный supabase storage bucket 'materials'.
    Возвращает storage-путь (storage:materials/<path>), который кладётся в поле url
    существующего материала через обычный api_materials_add – участницам файл отдаётся
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
    today = marathon_day_today(await get_marathon_start(), MARATHON_TZ)
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


async def api_calendar_update(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    ev_id = int(request.match_info["id"])
    data = await request.json()
    title = str(data.get("title", "")).strip()
    date_str = str(data.get("date", "")).strip()
    if not title or not date_str:
        return web.json_response({"error": "title and date required"}, status=400)
    try:
        ev_date = date.fromisoformat(date_str)
    except ValueError:
        return web.json_response({"error": "bad date"}, status=400)
    await DB_POOL.execute(
        "UPDATE calendar_events SET date=$2, title=$3, type=$4, time=$5, link=$6 WHERE id=$1",
        ev_id, ev_date, title, str(data.get("type", "")).strip(), str(data.get("time", "")).strip(),
        str(data.get("link", "")).strip(),
    )
    row = await DB_POOL.fetchrow("SELECT * FROM calendar_events WHERE id=$1", ev_id)
    if row is None:
        return web.json_response({"error": "not_found"}, status=404)
    return web.json_response(_calendar_json(row))


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
    date_label = str(data.get("date", "")).strip()
    place_label = str(data.get("place", "")).strip()
    if not title or not date_label:
        return web.json_response({"error": "title and date required"}, status=400)
    date_place = date_label + (", " + place_label if place_label else "")
    row_id = await DB_POOL.fetchval(
        "INSERT INTO mila_events (title, date_place, date_label, place_label, photo_url, link, color, description, created) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id",
        title, date_place, date_label, place_label, data.get("photo") or None,
        str(data.get("link", "")).strip(), str(data.get("color", "lav")).strip(),
        str(data.get("description", "")).strip(), time.time(),
    )
    row = await DB_POOL.fetchrow("SELECT * FROM mila_events WHERE id=$1", row_id)
    return web.json_response(_mila_event_json(row), status=201)


async def api_mila_events_update(request):
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    ev_id = int(request.match_info["id"])
    data = await request.json()
    title = str(data.get("title", "")).strip()
    date_label = str(data.get("date", "")).strip()
    place_label = str(data.get("place", "")).strip()
    if not title or not date_label:
        return web.json_response({"error": "title and date required"}, status=400)
    date_place = date_label + (", " + place_label if place_label else "")
    await DB_POOL.execute(
        "UPDATE mila_events SET title=$2, date_place=$3, date_label=$4, place_label=$5, "
        "photo_url=COALESCE($6, photo_url), link=$7, color=$8, description=$9 WHERE id=$1",
        ev_id, title, date_place, date_label, place_label, data.get("photo") or None,
        str(data.get("link", "")).strip(), str(data.get("color", "lav")).strip(),
        str(data.get("description", "")).strip(),
    )
    row = await DB_POOL.fetchrow("SELECT * FROM mila_events WHERE id=$1", ev_id)
    if row is None:
        return web.json_response({"error": "not_found"}, status=404)
    return web.json_response(_mila_event_json(row))


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
    """Разовое открытие доступа всем оплатившим сразу (не по одному при оплате) –
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
    """GET – текущее значение; POST {"opens_at": unix-время или null} – задать/снять."""
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


async def api_settings_marathon_start(request):
    """GET – текущая дата старта марафона (YYYY-MM-DD); POST {"marathon_start": "YYYY-MM-DD"} – задать."""
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    if request.method == "POST":
        data = await request.json()
        raw = str(data.get("marathon_start", "")).strip()
        try:
            date.fromisoformat(raw)
        except ValueError:
            return web.json_response({"error": "bad_date"}, status=400)
        await set_setting("marathon_start", raw)
    return web.json_response({"marathon_start": await get_marathon_start()})


async def cabinet(request):
    return html_response(CABINET_APP_HTML)


async def root(request):
    """Один сервер отвечает на нескольких доменах – отличаем по Host, чтобы
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
    """Лендинг (gh-pages / свой домен) и сервер – разные источники для браузера,
    без этих заголовков браузер блокирует ответ на fetch('/register', ...) с
    фронта даже если сам запрос реально дошёл до сервера. Остальные роуты
    (кабинет/админка) вызываются только с того же домена, что их отдаёт –
    им кросс-доменные заголовки не нужны, и лишний раз светить их наружу незачем."""
    if request.method == "OPTIONS":
        resp = web.Response()
    else:
        resp = await handler(request)
    if request.path == "/register":
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "*"
    return resp


async def cors_preflight(request):
    return web.Response()


def make_app():
    if not SECRET:
        raise SystemExit("Нет PLATFORM_SECRET в .env – см. .env.example")
    if not ADMIN_PASSWORD:
        print("[ПРЕДУПРЕЖДЕНИЕ] ADMIN_PASSWORD не задан – вход в /admin не сработает, см. .env.example")
    app = web.Application(middlewares=[cors_middleware], client_max_size=15 * 1024 * 1024)
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
        web.get("/admin/export-progress.csv", admin_export_progress),
        web.get("/admin/logout", admin_logout),
        web.get("/health", health),
        web.post("/api/auth/register", api_auth_register),
        web.post("/api/auth/login", api_auth_login),
        web.post("/api/auth/refresh", api_auth_refresh),
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
        web.get("/api/admin/settings/marathon-start", api_settings_marathon_start),
        web.post("/api/admin/settings/marathon-start", api_settings_marathon_start),
        web.get("/api/admin/folders", api_folders_list),
        web.post("/api/admin/folders", api_folders_add),
        web.delete("/api/admin/folders/{id}", api_folders_delete),
        web.get("/api/admin/themes", api_themes_list),
        web.post("/api/admin/themes", api_themes_add),
        web.delete("/api/admin/themes/{id}", api_themes_delete),
        web.get("/api/admin/materials", api_materials_list),
        web.post("/api/admin/materials", api_materials_add),
        web.put("/api/admin/materials/{id}", api_materials_update),
        web.delete("/api/admin/materials/{id}", api_materials_delete),
        web.post("/api/admin/materials/upload", api_materials_upload),
        web.get("/api/admin/calendar-events", api_calendar_list),
        web.post("/api/admin/calendar-events", api_calendar_add),
        web.put("/api/admin/calendar-events/{id}", api_calendar_update),
        web.delete("/api/admin/calendar-events/{id}", api_calendar_delete),
        web.get("/api/admin/mila-events", api_mila_events_list),
        web.post("/api/admin/mila-events", api_mila_events_add),
        web.put("/api/admin/mila-events/{id}", api_mila_events_update),
        web.delete("/api/admin/mila-events/{id}", api_mila_events_delete),
        web.route("OPTIONS", "/{tail:.*}", cors_preflight),
    ])
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host=HOST, port=PORT)
