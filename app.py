"""Серенити Мун — марафон: приём оплаты + табличка для Милы.

Не трогает сам кабинет участниц (это следующий этап) — только:
- приём формы Venmo/Zelle (POST /register)
- приём вебхука Stripe (POST /webhook/stripe)
- админ-табличка со списком и кнопкой "отметить оплаченным" (GET /admin)

Настройки: .env (см. .env.example). Без реальных ключей Stripe/Resend
всё работает в тестовом режиме — только без реальной отправки.
"""
import base64
import hashlib
import hmac
import html
import json
import os
import re
import sqlite3
import time
from pathlib import Path

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
CHANNEL_INVITE_LINK = os.environ.get("CHANNEL_INVITE_LINK", "")  # обычная (не одноразовая) ссылка на телеграм-канал

(HERE / "data").mkdir(exist_ok=True)
DB = sqlite3.connect(HERE / "data" / "signups.sqlite", check_same_thread=False)
DB.row_factory = sqlite3.Row
DB.execute("""CREATE TABLE IF NOT EXISTS signups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT, email TEXT, email_norm TEXT, instagram TEXT, phone TEXT, comment TEXT,
    method TEXT,                 -- stripe | venmo | zelle
    amount REAL,
    status TEXT DEFAULT 'ждёт',  -- ждёт | оплачено
    invite_sent INTEGER DEFAULT 0,
    created REAL, confirmed REAL
)""")
try:
    DB.execute("ALTER TABLE signups ADD COLUMN phone TEXT")
except sqlite3.OperationalError:
    pass
try:
    DB.execute("ALTER TABLE signups ADD COLUMN comment TEXT")
except sqlite3.OperationalError:
    pass
DB.execute("CREATE INDEX IF NOT EXISTS idx_email_norm ON signups(email_norm)")
DB.commit()


def normalize_email(e: str) -> str:
    return (e or "").strip().lower()


# ---------- почта (не падает без ключей — просто логирует) ----------
async def send_access_email(to_email: str, name: str):
    invite = CHANNEL_INVITE_LINK or "[DEV] ссылка появится, как только настроим CHANNEL_INVITE_LINK"
    body = (f"Здравствуйте{', ' + name if name else ''}!\n\n"
            f"Спасибо за оплату марафона «Начни сегодня».\n"
            f"Ссылка на телеграм-канал марафона:\n{invite}\n\n"
            f"Ближе к старту (9 ноября) на эту же почту придёт доступ к платформе.")
    if not (RESEND_API_KEY and MAIL_FROM):
        print(f"[DEV] письмо для {to_email} не отправлено (нет RESEND_API_KEY/MAIL_FROM):\n{body}")
        return
    async with ClientSession() as s:
        async with s.post(
            "https://api.resend.com/emails",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {RESEND_API_KEY}"},
            data=json.dumps({
                "from": MAIL_FROM,
                "to": [to_email],
                "subject": "Serenity Moon — доступ к марафону",
                "text": body,
            }),
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
        btn = "" if paid or r["method"] == "stripe" else \
            f'<form method="post" action="/admin/confirm/{r["id"]}" style="display:inline;">' \
            f'<button style="background:#7C6A9C;color:#fff;border:none;border-radius:8px;' \
            f'padding:7px 12px;font-size:12px;cursor:pointer;">Отметить оплаченным</button></form>'
        dt = time.strftime("%d.%m %H:%M", time.localtime(r["confirmed"] or r["created"]))
        return (f'<tr style="border-bottom:1px solid #F0EBE6;">'
                f'<td style="padding:12px 16px;">{E(r["name"] or "—")}</td>'
                f'<td style="padding:12px 16px;color:#6B6470;">{E(r["email"])}</td>'
                f'<td style="padding:12px 16px;color:#6B6470;">{E(r["method"])}</td>'
                f'<td style="padding:12px 16px;">{badge}</td>'
                f'<td style="padding:12px 16px;color:#6B6470;">{dt}</td>'
                f'<td style="padding:12px 16px;text-align:right;">{btn}</td></tr>')

    total = len(rows)
    paid_n = sum(1 for r in rows if r["status"] == "оплачено")
    waiting_n = total - paid_n
    rows_html = "".join(row_html(r) for r in rows) or \
        '<tr><td colspan="6" style="padding:24px;text-align:center;color:#8A8390;">пока пусто</td></tr>'

    return f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Заявки и оплаты</title>
<style>
body{{margin:0;font-family:Inter,system-ui,sans-serif;background:#F7F4F1;color:#352F3D;padding:32px 20px}}
.wrap{{max-width:1000px;margin:0 auto}}
.stat{{background:#fff;border:1px solid #E7E1DC;border-radius:12px;padding:14px 20px;min-width:130px}}
table{{width:100%;border-collapse:collapse;background:#fff;border:1px solid #E7E1DC;border-radius:14px;overflow:hidden}}
th{{text-align:left;padding:12px 16px;background:#F2EEF7;font-size:13px}}
</style></head><body>
<div class="wrap">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">
<h1 style="font-size:24px;margin:0;">Заявки марафона</h1>
<a href="/admin/logout" style="font-size:13px;color:#8A8390;">выйти</a>
</div>
<div style="display:flex;gap:14px;margin-bottom:20px;">
<div class="stat"><div style="font-size:12px;color:#8A8390;">всего</div><div style="font-size:22px;font-weight:600;">{total}</div></div>
<div class="stat"><div style="font-size:12px;color:#8A8390;">оплачено</div><div style="font-size:22px;font-weight:600;color:#6F8F6B;">{paid_n}</div></div>
<div class="stat"><div style="font-size:12px;color:#8A8390;">ждут</div><div style="font-size:22px;font-weight:600;color:#C08A3E;">{waiting_n}</div></div>
</div>
<table><thead><tr><th>Имя</th><th>Почта</th><th>Способ</th><th>Статус</th><th>Дата</th><th></th></tr></thead>
<tbody>{rows_html}</tbody></table>
</div></body></html>"""


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
    DB.execute(
        "INSERT INTO signups (name, email, email_norm, instagram, phone, comment, method, status, created) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (name, email, normalize_email(email), instagram, phone, comment, method, "ждёт", time.time()),
    )
    DB.commit()
    return web.json_response({"ok": True})


def verify_stripe_signature(payload: str, sig_header: str) -> bool:
    """Проверка, что вебхук реально от Stripe (см. Stripe Webhook signing)."""
    if not STRIPE_WEBHOOK_SECRET:
        return True  # тестовый режим без секрета — пропускаем без проверки
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

    row = DB.execute("SELECT id, status FROM signups WHERE email_norm=? AND method='stripe'",
                      (email,)).fetchone()
    if row:
        DB.execute("UPDATE signups SET status='оплачено', confirmed=? WHERE id=?",
                   (time.time(), row["id"]))
    else:
        DB.execute(
            "INSERT INTO signups (name, email, email_norm, method, amount, status, created, confirmed) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (name, email, email, "stripe", amount, "оплачено", time.time(), time.time()),
        )
    DB.commit()
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
    rows = DB.execute("SELECT * FROM signups ORDER BY created DESC").fetchall()
    return html_response(admin_page(rows))


async def admin_confirm(request):
    if not authed(request):
        raise web.HTTPFound("/admin/login")
    row_id = int(request.match_info["id"])
    row = DB.execute("SELECT * FROM signups WHERE id=?", (row_id,)).fetchone()
    if row and row["status"] != "оплачено":
        DB.execute("UPDATE signups SET status='оплачено', confirmed=? WHERE id=?",
                   (time.time(), row_id))
        DB.commit()
        await send_access_email(row["email"], row["name"])
    raise web.HTTPFound("/admin")


async def admin_logout(request):
    resp = web.HTTPFound("/admin/login")
    resp.del_cookie(COOKIE, path="/")
    raise resp


async def health(request):
    DB.execute("SELECT 1").fetchone()
    return web.Response(text="ok")


def make_app():
    if not SECRET:
        raise SystemExit("Нет PLATFORM_SECRET в .env — см. .env.example")
    if not ADMIN_PASSWORD:
        print("[ПРЕДУПРЕЖДЕНИЕ] ADMIN_PASSWORD не задан — вход в /admin не сработает, см. .env.example")
    app = web.Application()
    app.add_routes([
        web.post("/register", register),
        web.post("/webhook/stripe", stripe_webhook),
        web.get("/admin", admin),
        web.get("/admin/login", admin_login_page),
        web.post("/admin/login", admin_login_post),
        web.post("/admin/confirm/{id}", admin_confirm),
        web.get("/admin/logout", admin_logout),
        web.get("/health", health),
    ])
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host=HOST, port=PORT)
