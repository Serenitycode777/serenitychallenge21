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


ADMIN_ARTIFACT_URL = "https://claude.ai/artifact/SHAhm9W5oqJV3fkWChCdWP"


async def admin(request):
    # Старая HTML-табличка заменена новой админкой (claude.ai-артефакт с реальным api) —
    # эта страница больше не используется, см. workspace/clients/mila-serenity/platform/
    # (2026-10-19). Роуты/хендлеры ниже оставлены нетронутыми ради остальных api,
    # но сама табличка теперь редиректит на актуальную админку.
    raise web.HTTPFound(ADMIN_ARTIFACT_URL)


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
    """Токен для JSON-api админки (CRM-вкладка в прототипе на claude.ai —
    другой origin, обычная cookie-сессия /admin туда не дотягивается)."""
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
    if not admin_token_ok(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    row_id = int(request.match_info["id"])
    data = await request.json()
    await DB_POOL.execute("UPDATE signups SET note=$1 WHERE id=$2", str(data.get("note", "")).strip(), row_id)
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
    await DB_POOL.execute(
        "INSERT INTO signups (name, email, email_norm, phone, method, status, note, created, confirmed) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        str(data.get("name", "")).strip(), email, normalize_email(email),
        str(data.get("phone", "")).strip(),
        str(data.get("method", "venmo")).strip(), status, str(data.get("note", "")).strip(),
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
