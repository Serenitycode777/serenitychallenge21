# Serenity Moon — платформа марафона

Один файл (`app.py`, aiohttp) — отдаёт и личный кабинет участниц (`/cabinet`),
и админку (`/admin`): привычки/прогресс, материалы, календарь эфиров, табличка
участниц и оплат (CRM), настройки (дата старта марафона, открытие кабинета,
карточка «Перед началом марафона»). Всё хранится в Postgres (`DB_POOL`) +
файлы материалов — в Supabase Storage.

## Если ты ИИ-агент и правишь этот репозиторий

- Код живёт в одном файле `app.py` — HTML/CSS/JS кабинета и админки встроены как
  Python-строки (`CABINET_APP_HTML`, `ADMIN_APP_HTML` и т.п.), это не SPA со
  сборкой, редактируется как текст.
- **`git push` НЕ деплоит сам по себе.** Render разворачивает новую версию
  только по явному вызову API:
  ```
  curl -X POST https://api.render.com/v1/services/srv-db772ahsrm7s73c7r6n0/deploys \
    -H "Authorization: Bearer $RENDER_API_KEY" -H "Content-Type: application/json" -d '{}'
  ```
  Без этого сайт продолжит отдавать старую версию сколько угодно долго после пуша.
  После деплоя дождись статуса `live` (poll `GET /v1/services/{id}/deploys/{deployId}`),
  потом проверь живой HTML (`curl` по `platform.serenitymoonevents.com`, искать
  маркер только что добавленного кода) — commit sha "новый" не значит, что
  контент реально изменился.
- Перед каждым деплоем проверяй синтаксис:
  - Python: `python3 -c "import ast; ast.parse(open('app.py').read())"`
  - JS: вытащить `<script>...</script>` блоки регуляркой и прогнать через
    пакет `esprima` (`pip install esprima`) — второй/админский блок даёт одну
    заведомо безобидную ошибку на `?.` (optional chaining), это нормально.
- Админские API-эндпоинты (`/api/admin/...`) можно вызывать напрямую curl'ом,
  не только из браузера — это проще, чем редактировать данные через код:
  авторизация через cookie, которую даёт `POST /admin/login` с полем `password`
  (значение — переменная `ADMIN_PASSWORD_REAL`). Пример:
  ```
  curl -c cookies.txt -X POST -d "password=$ADMIN_PASSWORD_REAL" https://platform.serenitymoonevents.com/admin/login
  curl -b cookies.txt https://platform.serenitymoonevents.com/api/admin/folders
  ```
  Названия папок/авторов, темы, материалы, участницы, настройки (дата старта
  марафона и т.д.) — всё это данные в Postgres, а не в коде. Их меняют через
  этот API (или просто руками в самой админке в браузере), а не правкой `app.py`.

## Секреты — три переменные, которые нужны для полного доступа

Хранятся в локальном `.env` (не в гите). Если нужно дать своему агенту полный
доступ (код + деплой + данные), ему нужны:
1. **`GITHUB_TOKEN`** — push-доступ к этому репозиторию.
2. **`RENDER_API_KEY`** — запускать деплой после пуша (см. выше, без этого
   изменения не попадут на реальный сайт).
3. **`ADMIN_PASSWORD_REAL`** — логин в `/admin` и в админские API.

## Деплой с нуля (если поднимаешь сервис заново)

1. New → Web Service на render.com → выбрать этот репозиторий.
2. Build Command: `pip install -r requirements.txt`
3. Start Command: `python app.py`
4. Environment → переменные из `.env.example`.

## Подключение Stripe

Stripe Dashboard → Developers → Webhooks → Add endpoint:
`https://platform.serenitymoonevents.com/webhook/stripe`, событие
`checkout.session.completed`. "Signing secret" → `STRIPE_WEBHOOK_SECRET`.

## Почта — Mandrill (Mailchimp Transactional), не Resend

Домен `serenitymoonevents.com` подтверждается в Mailchimp через DNS-записи
(TXT), не через email-верификацию. API-ключ → `MANDRILL_API_KEY`.

## Телеграм-канал

Обычная ссылка-приглашение (без бота) → переменная `CHANNEL_INVITE_LINK` в
`.env` и/или прямо в коде (`TG_CHAT_URL`/`TG_CHANNEL_URL` в кабинете).

## Форма на лендинге

Venmo/Zelle форма отправляет `POST /register` с полями `name`, `email`,
`instagram`, `method` (`venmo` или `zelle`).
