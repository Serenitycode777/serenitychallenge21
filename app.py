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


async def close_db(app):
    if DB_POOL:
        await DB_POOL.close()


def normalize_email(e: str) -> str:
    return (e or "").strip().lower()


# ---------- материалы ----------
MATERIALS_FILE = HERE / "data" / "materials.json"


def load_materials() -> dict:
    """Читает список материалов заново при каждом запросе — мила правит
    этот файл сама (через свой клод, коммитом в репозиторий), без нашей
    админки; изменения должны подхватываться без перезапуска сервиса."""
    return json.loads(MATERIALS_FILE.read_text(encoding="utf-8"))


def marathon_day_today(marathon_start: str, tz_name: str) -> int:
    """День марафона "сегодня" (1 = старт). Может быть <1 (до старта) или >21
    (после финиша) — страница сама решает, что в этом случае показать."""
    start = date.fromisoformat(marathon_start)
    now = datetime.now(ZoneInfo(tz_name)).date()
    return (now - start).days + 1


def materials_page(data: dict, today: int) -> str:
    payload = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    return f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Материалы — Serenity Moon</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,wght@0,500;0,600;0,700;1,500&family=Playfair+Display:ital@1&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{{
  --lav:#AA9DB8; --lav-ink:#6B5A7C; --sage:#6A9993; --sage-ink:#3F6660;
  --sand:#CBBAA8; --ink:#332E3D; --ink-soft:#6B6475;
  --bg:#FAF6F1; --surface:#FFFFFF; --surface-2:#F2ECE6; --line:#E7DED6;
  --shadow:0 10px 30px -14px rgba(51,46,61,.22); color-scheme:light;
}}
*{{box-sizing:border-box}}
body{{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,system-ui,sans-serif;padding:28px 20px 60px}}
h1,h2,h3{{font-family:Fraunces,Georgia,serif;margin:0}}
.wrap{{max-width:720px;margin:0 auto}}
.eyebrow{{font-family:"Playfair Display",Georgia,serif;font-style:italic;font-size:14px;color:var(--lav-ink);margin:0 0 6px}}
.head h1{{font-size:28px;font-weight:600}}
.head p{{margin:8px 0 0;color:var(--ink-soft);font-size:14px;line-height:1.5}}
.search-wrap{{position:relative;margin:20px 0 16px}}
.search-box{{width:100%;font-size:14.5px;color:var(--ink);background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:13px 16px 13px 42px;box-shadow:var(--shadow)}}
.search-icon{{position:absolute;left:15px;top:50%;transform:translateY(-50%);width:17px;height:17px;color:var(--ink-soft)}}
.search-drop{{position:absolute;left:0;right:0;top:calc(100% + 6px);background:var(--surface);border:1px solid var(--line);border-radius:14px;box-shadow:var(--shadow);overflow:hidden;z-index:30}}
.search-drop[hidden]{{display:none}}
.sr-item{{display:flex;gap:10px;padding:11px 14px;cursor:pointer;font-size:13.5px;border-top:1px solid var(--line)}}
.sr-item:first-child{{border-top:none}}
.sr-item:hover{{background:var(--surface-2)}}
.filters{{display:flex;flex-direction:column;gap:10px;margin-bottom:22px}}
.filter-row{{display:flex;flex-wrap:wrap;gap:8px}}
.flabel{{font-size:11.5px;text-transform:uppercase;letter-spacing:.07em;color:var(--ink-soft);width:100%}}
.chip{{font-size:13px;font-weight:500;color:var(--ink);background:var(--surface);border:1px solid var(--line);border-radius:999px;padding:7px 14px;cursor:pointer}}
.chip.active{{background:var(--lav);border-color:var(--lav);color:#fff}}
.chip.active.theme{{background:var(--sage);border-color:var(--sage)}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}
@media (max-width:520px){{.grid{{grid-template-columns:1fr}}}}
.card{{background:var(--surface);border:1px solid var(--line);border-radius:18px;padding:20px;cursor:pointer;box-shadow:var(--shadow);display:flex;flex-direction:column;gap:10px}}
.card[hidden]{{display:none}}
.dot{{width:34px;height:34px;border-radius:50%;background-size:cover;background-position:center;flex-shrink:0}}
.card.mila .dot{{background-color:var(--lav)}} .card.maria .dot{{background-color:var(--sage)}}
.card.anna .dot{{background-color:var(--sand)}} .card.extra .dot{{background:linear-gradient(135deg,var(--lav),var(--sage) 55%,var(--sand))}}
.card h3{{font-size:19px;font-weight:600}}
.card .topics{{font-size:12.5px;color:var(--ink-soft)}}
.card .desc{{font-size:13.5px;color:var(--ink-soft);line-height:1.45;flex:1}}
.card .foot{{display:flex;justify-content:space-between;font-size:12px;color:var(--ink-soft)}}
.card .count{{font-weight:600;color:var(--ink)}}
.overlay{{position:fixed;inset:0;background:rgba(26,21,33,.45);display:flex;align-items:flex-end;justify-content:center;z-index:40;opacity:0;pointer-events:none;transition:.2s}}
.overlay.open{{opacity:1;pointer-events:auto}}
.panel{{position:relative;background:var(--surface);width:100%;max-width:560px;border-radius:22px 22px 0 0;padding:24px 22px 26px;max-height:82vh;overflow-y:auto}}
.pin{{width:40px;height:4px;border-radius:2px;background:var(--line);margin:0 auto 16px}}
.panel-head{{display:flex;align-items:center;gap:12px}}
.panel-head .dot{{width:40px;height:40px}}
.close-x{{position:absolute;top:18px;right:20px;background:none;border:none;color:var(--ink-soft);font-size:20px;cursor:pointer}}
.sub{{color:var(--ink-soft);font-size:13px;margin:2px 0 18px}}
.mat{{display:flex;gap:12px;padding:13px 0;border-top:1px solid var(--line)}}
.mat:first-of-type{{border-top:none}}
.mat.is-locked{{opacity:.55}}
.mat-icon{{width:34px;height:34px;border-radius:10px;background:var(--surface-2);flex-shrink:0;display:flex;align-items:center;justify-content:center}}
.mat-body{{flex:1;min-width:0}}
.mat-title{{font-size:14.5px;font-weight:500;line-height:1.35}}
.mat-tags{{font-size:12px;color:var(--ink-soft);margin-top:3px}}
.mat-status{{font-size:11.5px;font-weight:600;padding:4px 10px;border-radius:999px;white-space:nowrap;align-self:flex-start}}
.mat-status.open{{background:var(--surface-2);color:var(--sage-ink)}}
.mat-status.locked{{background:var(--surface-2);color:var(--ink-soft)}}
.note{{font-size:12px;color:var(--ink-soft);background:var(--surface-2);border-radius:10px;padding:10px 14px;margin-top:4px;line-height:1.5}}
</style></head><body>
<div class="wrap">
  <div class="head">
    <p class="eyebrow">Личный кабинет · 21-дневный марафон</p>
    <h1>Материалы</h1>
    <p>Сегодня день {max(today, 0)} из 21. Материалы открываются по датам — новые появляются по ходу марафона.</p>
  </div>
  <div class="search-wrap">
    <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/></svg>
    <input class="search-box" id="searchBox" type="text" placeholder="поиск: массаж лица, питание, дыхание…" autocomplete="off">
    <div class="search-drop" id="searchDrop" hidden></div>
  </div>
  <div class="filters">
    <div class="filter-row"><span class="flabel">по автору</span><div id="authorChips"></div></div>
    <div class="filter-row"><span class="flabel">по теме</span><div id="themeChips"></div></div>
  </div>
  <div class="grid" id="grid"></div>
</div>
<div class="overlay" id="overlay"><div class="panel">
  <button class="close-x" id="closePanel">&#10005;</button>
  <div class="pin"></div>
  <div class="panel-head"><div class="dot" id="panelDot"></div><div><h2 id="panelTitle"></h2><p class="sub" id="panelSub"></p></div></div>
  <div id="panelList"></div>
</div></div>
<script id="materials-data" type="application/json">{payload}</script>
<script>
const DATA = JSON.parse(document.getElementById("materials-data").textContent);
const TODAY = {int(today)};
const ICONS = {{
  video:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="2.5" y="5.5" width="13" height="13" rx="2.5"/><path d="M18.5 9.5 21.5 7v10l-3-2.5"/></svg>',
  audio:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 10v4h3.5L13 18V6L7.5 10H4Z"/><path d="M16.5 9a4 4 0 0 1 0 6"/></svg>',
  pdf:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M6 2.5h8l4.5 4.5V21a.5.5 0 0 1-.5.5H6a.5.5 0 0 1-.5-.5V3a.5.5 0 0 1 .5-.5Z"/><path d="M14 2.5V7h4.5"/></svg>',
  lock:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="5" y="10.5" width="14" height="9.5" rx="2"/><path d="M8 10.5V8a4 4 0 0 1 8 0v2.5"/></svg>'
}};
const THEMES = [...new Set(DATA.materials.flatMap(m=>m.tags))];
let activeAuthor="all", activeTheme="all", query="";

function authorChip(key,label,active){{return `<button class="chip ${{active?'active':''}}" data-a="${{key}}">${{label}}</button>`;}}
document.getElementById("authorChips").innerHTML =
  authorChip("all","все",true) + Object.entries(DATA.authors).map(([k,a])=>authorChip(k,a.name,false)).join("");
document.getElementById("themeChips").innerHTML =
  `<button class="chip theme active" data-t="all">все темы</button>` +
  THEMES.map(t=>`<button class="chip theme" data-t="${{t}}">${{t}}</button>`).join("");

function matchesQuery(m){{
  if(!query) return true;
  return (m.title+" "+m.tags.join(" ")).toLowerCase().includes(query);
}}

function renderGrid(){{
  const grid = document.getElementById("grid");
  grid.innerHTML = "";
  Object.entries(DATA.authors).forEach(([key,a])=>{{
    const items = DATA.materials.filter(m=>m.author===key);
    const card = document.createElement("div");
    card.className = "card "+key;
    const mA = activeAuthor==="all"||activeAuthor===key;
    const mT = activeTheme==="all"||items.some(m=>m.tags.includes(activeTheme));
    const mQ = items.some(matchesQuery);
    if(!mA||!mT||!mQ) card.hidden = true;
    const dotStyle = a.photo ? ` style="background-image:url('${{a.photo}}')"` : "";
    card.innerHTML = `<div class="dot"${{dotStyle}}></div><h3>${{a.name}}</h3><div class="topics">${{a.topics}}</div>
      <div class="desc">${{a.desc}}</div>`;
    card.addEventListener("click", ()=>openPanel(key));
    grid.appendChild(card);
  }});
}}

function openPanel(key){{
  const a = DATA.authors[key];
  document.getElementById("panelTitle").textContent = a.name;
  document.getElementById("panelSub").textContent = a.topics;
  const panelDot = document.getElementById("panelDot");
  panelDot.className = "dot "+key;
  panelDot.style.backgroundImage = a.photo ? `url('${{a.photo}}')` : "";
  const items = DATA.materials.filter(m=>m.author===key);
  document.getElementById("panelList").innerHTML = items.map(m=>{{
    const unlocked = m.day===null || m.day<=TODAY;
    const status = m.day===null ? `<span class="mat-status open">открыто</span>`
      : unlocked ? `<span class="mat-status open">день ${{m.day}}</span>`
      : `<span class="mat-status locked">${{ICONS.lock}} день ${{m.day}}</span>`;
    return `<div class="mat ${{unlocked?'':'is-locked'}}">
      <div class="mat-icon">${{ICONS[m.type]}}</div>
      <div class="mat-body"><div class="mat-title">${{m.title}}</div><div class="mat-tags">#${{m.tags.join(' #')}}</div></div>
      ${{status}}</div>`;
  }}).join("") + `<p class="note">доступ к файлу открывается в день разблокировки.</p>`;
  document.getElementById("overlay").classList.add("open");
}}
document.getElementById("closePanel").onclick = ()=>document.getElementById("overlay").classList.remove("open");
document.getElementById("overlay").addEventListener("click",e=>{{if(e.target.id==="overlay")e.currentTarget.classList.remove("open");}});
document.getElementById("authorChips").addEventListener("click",e=>{{
  const b=e.target.closest(".chip"); if(!b) return;
  document.querySelectorAll("#authorChips .chip").forEach(c=>c.classList.remove("active"));
  b.classList.add("active"); activeAuthor=b.dataset.a; renderGrid();
}});
document.getElementById("themeChips").addEventListener("click",e=>{{
  const b=e.target.closest(".chip"); if(!b) return;
  document.querySelectorAll("#themeChips .chip").forEach(c=>c.classList.remove("active"));
  b.classList.add("active"); activeTheme=b.dataset.t; renderGrid();
}});
const searchBox=document.getElementById("searchBox"), searchDrop=document.getElementById("searchDrop");
searchBox.addEventListener("input",()=>{{
  query = searchBox.value.trim().toLowerCase();
  if(!query){{searchDrop.hidden=true; renderGrid(); return;}}
  const results = DATA.materials.filter(matchesQuery).slice(0,6);
  searchDrop.innerHTML = results.length ? results.map(m=>
    `<div class="sr-item" data-a="${{m.author}}"><div class="mat-icon">${{ICONS[m.type]}}</div><div>${{m.title}}<div class="mat-tags">${{DATA.authors[m.author].name}}</div></div></div>`
  ).join("") : `<div class="sr-item">ничего не нашлось</div>`;
  searchDrop.hidden = false;
  renderGrid();
}});
searchDrop.addEventListener("click",e=>{{const it=e.target.closest(".sr-item"); if(it&&it.dataset.a) openPanel(it.dataset.a); searchDrop.hidden=true;}});
document.addEventListener("click",e=>{{if(!e.target.closest(".search-wrap")) searchDrop.hidden=true;}});
renderGrid();
</script>
</body></html>"""


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

.shell{max-width:760px;margin:0 auto;padding:28px 20px 70px}
.shell.wide{max-width:1180px}
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
details.crm-add summary{cursor:pointer;font-size:13px;color:var(--lav-ink);font-weight:600;padding:8px 0}
.crm-addbody{background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:14px;display:flex;flex-wrap:wrap;gap:10px;align-items:end;margin-bottom:14px;box-shadow:var(--shadow)}
.crm-addbody label{display:flex;flex-direction:column;gap:4px;font-size:11.5px;color:var(--ink-soft)}
.crm-addbody input,.crm-addbody select{border:1.3px solid var(--sage);background:var(--surface);border-radius:8px;padding:7px 9px;font-size:13px;font-family:inherit}
.crm-addbody button{background:var(--lav);color:#fff;border:none;border-radius:8px;padding:8px 15px;font-size:13px;cursor:pointer}
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
.cchip .sw{width:16px;height:16px;border-radius:50%;flex-shrink:0;border:1px solid rgba(0,0,0,.08)}

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
  <div class="sub">CRM — реальные заявки марафона. Материалы/Календарь/Мероприятия — пока визуальный прототип (без сохранения).</div>

  <div class="tabs">
    <div class="tab active" data-tab="t-content">Материалы</div>
    <div class="tab" data-tab="t-calendar">Календарь</div>
    <div class="tab" data-tab="t-events">Мероприятия</div>
    <div class="tab" data-tab="t-participants">CRM</div>
  </div>

  <div id="t-content">
    <div class="card">
      <h2>Новый материал</h2>
      <div class="hint">видео, аудио или pdf — папку и тему можно выбрать из уже существующих или создать прямо здесь</div>
      <div class="field">
        <label>Папка <span class="req">*</span></label>
        <div class="chipgrid" id="folderPicker"></div>
        <div class="addtheme" id="newFolderForm" style="display:none">
          <input type="text" id="nfName" placeholder="Название папки">
          <input type="text" id="nfDesc" placeholder="Описание (необязательно)">
          <button id="nfCreate">Создать</button>
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
        <div class="filebox">перетащи файл сюда или <b>выбери на компьютере</b><br>либо вставь ссылку ниже</div>
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
        <div class="crm-actions">
          <a href="/admin/export.csv" class="primary" target="_blank">скачать CSV</a>
          <a href="#" id="crmRefresh" class="secondary">обновить</a>
        </div>
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
      <details class="crm-add">
        <summary>+ добавить вручную</summary>
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
      </details>
      <div class="crm-mobile-wrap">
        <table class="crm-table">
          <thead><tr><th>Имя</th><th>Телефон</th><th>Телеграм</th><th>Способ</th><th>Сумма</th><th>Статус</th><th>Дата</th><th>Заметка</th><th></th></tr></thead>
          <tbody id="crmBody"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<div class="toast" id="toast">Сохранено ✓</div>

<script>
const TABS = ["t-content","t-calendar","t-events","t-participants"];
document.querySelectorAll(".tab").forEach(t=>{
  t.addEventListener("click", ()=>{
    document.querySelectorAll(".tab").forEach(x=>x.classList.remove("active"));
    t.classList.add("active");
    TABS.forEach(id=>{ document.getElementById(id).style.display = (id===t.dataset.tab) ? "" : "none"; });
    document.getElementById("shell").classList.toggle("wide", t.dataset.tab==="t-participants");
    if(t.dataset.tab==="t-participants" && !crmLoaded) loadParticipants();
  });
});

function showToast(msg){
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.classList.add("show");
  setTimeout(()=>t.classList.remove("show"), 2200);
}

let THEMES = ["дыхание","медитация","аюрведа","растяжка","сила","звукотерапия","массаж","разное"];
function renderMaterialThemes(){
  const wrap = document.getElementById("mThemes");
  wrap.innerHTML = THEMES.map(t=>`<div class="tchip" data-t="${t}">${t}</div>`).join("")
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
document.getElementById("addThemeBtn").addEventListener("click", ()=>{
  const inp = document.getElementById("newThemeInput");
  const v = inp.value.trim().toLowerCase();
  if(!v || THEMES.includes(v)){ inp.focus(); return; }
  THEMES.push(v);
  inp.value = "";
  document.getElementById("newThemeForm").style.display = "none";
  renderMaterialThemes();
  document.querySelector(`#mThemes .tchip[data-t="${v}"]`).classList.add("on");
  document.getElementById("themeErr").style.display="none";
  showToast("Тема «"+v+"» добавлена ✓");
});
renderMaterialThemes();

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

let FOLDERS = ["Мила","Мария","Анна","Доп.папка","Мастермайнды","Эфиры с приглашёнными гостями"];
let selectedFolder = null;
function renderFolderPicker(){
  const wrap = document.getElementById("folderPicker");
  wrap.innerHTML = FOLDERS.map(f=>`<div class="tchip${f===selectedFolder?" on":""}" data-f="${f}">${f}</div>`).join("")
    + `<div class="tchip" id="folderPlusChip">+ новая папка</div>`;
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
    if(f.style.display==="flex") document.getElementById("nfName").focus();
  });
}
renderFolderPicker();
document.getElementById("nfCreate").addEventListener("click", ()=>{
  const name = document.getElementById("nfName").value.trim();
  if(!name || FOLDERS.includes(name)){ document.getElementById("nfName").focus(); return; }
  FOLDERS.push(name);
  selectedFolder = name;
  document.getElementById("nfName").value = "";
  document.getElementById("nfDesc").value = "";
  document.getElementById("newFolderForm").style.display = "none";
  renderFolderPicker();
  showToast("Папка «"+name+"» создана ✓");
});

document.getElementById("evtHintDot").addEventListener("click", ()=>{
  document.getElementById("evtHintDot").classList.toggle("on");
  document.getElementById("evtHintNote").classList.toggle("show");
});
const evtToggle = document.getElementById("evtLinkToggle");
evtToggle.addEventListener("click", ()=>{
  evtToggle.classList.toggle("on");
  document.getElementById("evtPickWrap").style.display = evtToggle.classList.contains("on") ? "" : "none";
});

document.getElementById("saveMaterial").addEventListener("click", ()=>{
  const title = document.getElementById("mTitle").value.trim();
  const themesChosen = document.querySelectorAll("#mThemes .tchip.on").length;
  if(!selectedFolder){ document.getElementById("folderPicker").scrollIntoView({block:"center"}); return; }
  if(!themesChosen){ document.getElementById("themeErr").style.display = "block"; return; }
  if(!title){ document.getElementById("mTitle").focus(); return; }
  showToast("Материал «"+title+"» сохранён ✓");
  document.getElementById("mTitle").value = "";
  document.getElementById("mLink").value = "";
  mDayPicker.reset();
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
let EVENTS = [
  {date:"2026-11-11", title:"Мастермайнд #1", type:"Мастермайнд", time:"19:00 мск"},
  {date:"2026-11-18", title:"Онлайн-практика дыхания", type:"Онлайн-практика", time:"19:00 мск"},
  {date:"2026-11-23", title:"Мастермайнд #2", type:"Мастермайнд", time:"19:00 мск"}
];
function fmtDate(iso){
  const d = new Date(iso+"T00:00:00");
  return {num:d.getDate(), lbl:MONTHS_SHORT[d.getMonth()]};
}
function renderEvents(){
  const list = document.getElementById("evList");
  list.innerHTML = EVENTS.slice().sort((a,b)=>a.date.localeCompare(b.date)).map((e)=>{
    const d = fmtDate(e.date);
    return `
    <div class="evrow">
      <div class="evday">${d.num}<br><span style="font-size:8px;font-weight:600;text-transform:uppercase">${d.lbl}</span></div>
      <div class="evbody"><div class="evttl">${e.title}</div><div class="evmeta">${e.time}</div></div>
      <div class="evtag">${e.type}</div>
      <button class="evbtn" data-del="${EVENTS.indexOf(e)}">✕</button>
    </div>`;
  }).join("");
  list.querySelectorAll("[data-del]").forEach(b=>{
    b.addEventListener("click", ()=>{
      EVENTS.splice(+b.dataset.del, 1);
      renderEvents();
      showToast("Эфир удалён");
    });
  });
}
renderEvents();
document.getElementById("saveEvent").addEventListener("click", ()=>{
  const date = document.getElementById("eDay").value;
  const title = document.getElementById("eTitle").value.trim();
  if(!date){ document.getElementById("eDayWrap").scrollIntoView({block:"center"}); return; }
  if(!title){ document.getElementById("eTitle").focus(); return; }
  EVENTS.push({ date, title, type: selectedType, time: document.getElementById("eTime").value.trim() || "19:00 мск" });
  renderEvents();
  showToast("Эфир «"+title+"» добавлен в календарь ✓");
  eDayPicker.reset();
  document.getElementById("eTitle").value = "";
  document.getElementById("eLink").value = "";
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
    <span class="sw" style="background:${c.css}"></span>${c.l}</div>`).join("");
  wrap.querySelectorAll(".cchip").forEach(el=>{
    el.addEventListener("click", ()=>{ selectedMevColor = el.dataset.c; renderMevColorPicker(); });
  });
}
renderMevColorPicker();

let MILA_EVENTS = [
  {title:"Ретрит выходного дня", date:"14-15 ноября · Durham, NC", link:"#", color:"sand"},
  {title:"Онлайн-ретрит «Новолуние»", date:"2 декабря · онлайн", link:"#", color:"grad1"}
];
function renderMevList(){
  const wrap = document.getElementById("mevList");
  wrap.innerHTML = MILA_EVENTS.length ? MILA_EVENTS.map((e,i)=>{
    const c = MEV_COLORS.find(x=>x.v===e.color) || MEV_COLORS[0];
    return `<div class="mevrow">
      <div class="ph" style="background:${c.css}"></div>
      <div class="b"><h4>${e.title}</h4><div class="dt">${e.date}</div></div>
      <button class="evbtn" data-del="${i}">✕</button>
    </div>`;
  }).join("") : `<div class="hint" style="margin:0">пока нет мероприятий</div>`;
  wrap.querySelectorAll("[data-del]").forEach(b=>{
    b.addEventListener("click", ()=>{
      MILA_EVENTS.splice(+b.dataset.del, 1);
      renderMevList();
      showToast("Мероприятие удалено");
    });
  });
}
renderMevList();
document.getElementById("saveEventMila").addEventListener("click", ()=>{
  const title = document.getElementById("mevTitle").value.trim();
  const date = document.getElementById("mevDate").value.trim();
  if(!title){ document.getElementById("mevTitle").focus(); return; }
  if(!date){ document.getElementById("mevDate").focus(); return; }
  MILA_EVENTS.push({ title, date, link: document.getElementById("mevLink").value.trim() || "#", color: selectedMevColor });
  renderMevList();
  showToast("Мероприятие «"+title+"» добавлено ✓");
  document.getElementById("mevTitle").value = "";
  document.getElementById("mevDate").value = "";
  document.getElementById("mevLink").value = "";
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
  body.innerHTML = shown.length ? shown.map((p)=>{
    const paid = p.status === "оплачено";
    const badge = paid ? `<span class="crm-badge paid">оплачено</span>` : `<span class="crm-badge wait">ждёт</span>`;
    const confirmBtn = (paid || p.method==="stripe") ? "" : `<button class="crm-confirm" data-pay="${p.id}">Отметить оплаченным</button>`;
    return `<tr>
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
  }).join("") : `<tr><td colspan="9" style="padding:24px;text-align:center;color:var(--ink-soft)">ничего не нашлось</td></tr>`;

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
    showToast("Заявка добавлена ✓");
  }catch(e){}
});

document.getElementById("crmRefresh").addEventListener("click", (e)=>{ e.preventDefault(); loadParticipants(); });
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


async def materials(request):
    data = load_materials()
    today = marathon_day_today(data["marathon_start"], data["timezone"])
    return html_response(materials_page(data, today))


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
        web.get("/materials", materials),
        web.get("/health", health),
        # ---- JSON api для CRM-вкладки в админ-прототипе (claude.ai, другой origin — токен в
        # заголовке X-Admin-Token вместо cookie-сессии) ----
        web.get("/api/admin/signups", api_signups_list),
        web.post("/api/admin/signups", api_signup_add),
        web.post("/api/admin/signups/{id}/confirm", api_signup_confirm),
        web.post("/api/admin/signups/{id}/note", api_signup_note),
        web.delete("/api/admin/signups/{id}", api_signup_delete),
        web.route("OPTIONS", "/{tail:.*}", cors_preflight),
    ])
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host=HOST, port=PORT)
