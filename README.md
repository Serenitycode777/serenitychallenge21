# Serenity Moon — приём оплаты и табличка заявок

Маленький сервис: принимает форму Venmo/Zelle, принимает вебхук Stripe,
показывает защищённую паролем табличку заявок с кнопкой «отметить оплаченным»,
шлёт письмо со ссылкой на телеграм-канал.

## Деплой на Render

1. Залить это содержимое в свой GitHub-репозиторий.
2. На render.com: New → Web Service → выбрать репозиторий.
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `python app.py`
5. Environment → добавить переменные из `.env.example` (без реальных ключей
   сервис всё равно запустится и будет работать в тестовом режиме — письма и
   ссылки будут просто печататься в логи вместо реальной отправки).
6. После деплоя Render даст адрес вида `https://your-service.onrender.com`.

## Подключение Stripe

Stripe Dashboard → Developers → Webhooks → Add endpoint:
`https://your-service.onrender.com/webhook/stripe`, событие `checkout.session.completed`.
Скопировать "Signing secret" в переменную `STRIPE_WEBHOOK_SECRET`.

## Подключение почты (Mailchimp Transactional Email / Mandrill)

1. В Mailchimp → Transactional Email → Try for free.
2. Подтвердить домен — покажет TXT/CNAME записи, добавить их в DNS домена
   (у регистратора, где куплен домен).
3. После проверки домена создать API Key в разделе Transactional Email,
   вписать в `MANDRILL_API_KEY`, а в `MAIL_FROM` — адрес на этом домене.

## Телеграм-канал

Просто обычная ссылка-приглашение канала марафона (Telegram → канал →
Invite Links) → вписать в `CHANNEL_INVITE_LINK`. Одна и та же ссылка для всех,
без бота.

## Форма на лендинге

Форма Venmo/Zelle должна отправлять `POST` на `/register` с полями
`name`, `email`, `instagram`, `method` (`venmo` или `zelle`).
