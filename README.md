# Telegram Premium Bot + Flask Admin API

## Overview
Single Flask web service with Telegram webhook and admin API. DB: Supabase Postgres via psycopg2. No background worker. Cron tasks run via `/api/cron`.

## Stack
- Python 3.11, Flask 3.x, Flask-Cors
- pyTelegramBotAPI (webhook)
- psycopg2-binary, requests, python-dotenv

## Repo
- `backend/app.py` Flask app + webhook + admin API
- `backend/database.py` psycopg2 helpers
- `backend/utils.py` logging, cron, verification
- `supabase_schema.sql` Postgres schema
- `railway.json` Railway deploy descriptor
- `.env.example`, `requirements.txt`

## Local Development (Windows + ngrok)

Follow these steps to run the Flask + Telegram bot locally and expose it to Telegram using ngrok.

1) Prerequisites
- Python 3.11+
- Postgres database (Supabase recommended, or local Postgres)
- ngrok installed and logged in

2) Clone and environment
- Copy env template:
  - PowerShell: `Copy-Item .env.example .env`
  - CMD: `copy .env.example .env`
- Edit `.env` and set:
  - `BOT_TOKEN` = your bot token from BotFather
  - `ADMIN_API_KEY` = any strong string
  - `SECRET_KEY` = any strong string
  - `DATABASE_URL` =
    - Supabase: use the connection string from Supabase (keeps `sslmode=require`)
    - Local Postgres: `postgresql://user:pass@localhost:5432/dbname?sslmode=disable`

3) Install and run
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python backend/app.py
```
The app listens on `http://127.0.0.1:5000` by default (`PORT` env can override).

4) Start ngrok
```powershell
ngrok http 5000
```
Copy the HTTPS URL printed by ngrok, for example `https://abc123.ngrok.app`.

5) Set webhook
- Option A (automatic): put the URL in `.env` and restart the app
  - `WEBHOOK_BASE_URL=https://abc123.ngrok.app`
  - Restart: `python backend/app.py`
- Option B (manual): call Telegram API
```powershell
$env:BOT_TOKEN="<your-bot-token>"
$env:WEBHOOK_BASE_URL="https://abc123.ngrok.app"
curl -s "https://api.telegram.org/bot$env:BOT_TOKEN/setWebhook?url=$env:WEBHOOK_BASE_URL/bot/$env:BOT_TOKEN&drop_pending_updates=true"
```

6) Test
- Health: open `http://127.0.0.1:5000/health`
- Send `/start` to your bot in Telegram. You should get a welcome message.
- Admin panel: `http://127.0.0.1:5000/admin/login` (login with `ADMIN_API_KEY`)

7) Database
- Apply schema once to your Postgres:
```powershell
psql "<DATABASE_URL>" -f supabase_schema.sql
```

Tips
- If ngrok URL changes, update `WEBHOOK_BASE_URL` and set webhook again, or restart the app with the new value.
- If using local Postgres, ensure `sslmode=disable` is present in `DATABASE_URL` to avoid SSL errors.

## Setup
1. Create Supabase Postgres. Run `supabase_schema.sql` in SQL editor.
2. In Railway project settings, add environment variables from `.env.example`:
   - Required: `DATABASE_URL`, `BOT_TOKEN`, `ADMIN_API_KEY`, `SECRET_KEY`, `WEBHOOK_BASE_URL=https://<your-railway-domain>` once deployed.
   - Optional: UPI/USDT identifiers and scanner API keys for auto-verification.
3. Deploy to Railway:
   - Create a new Railway project and select **Deploy from GitHub Repo**.
   - Ensure `railway.json` is detected (Nixpacks builder installs `requirements.txt`).
   - After first deploy, note the public URL (e.g. `https://your-service.up.railway.app`) and update `WEBHOOK_BASE_URL` accordingly, then redeploy.

## Webhook
- URL: `https://<your-railway-domain>/bot/<BOT_TOKEN>`
- Set it after `WEBHOOK_BASE_URL` is configured (the app attempts to set automatically on boot):
```
curl -s "https://api.telegram.org/bot$BOT_TOKEN/setWebhook?url=$WEBHOOK_BASE_URL/bot/$BOT_TOKEN&drop_pending_updates=true"
```

## Cron
- Railway Scheduled Jobs or external monitors (e.g. UptimeRobot) should POST:
  - URL: `https://<your-railway-domain>/api/cron`
  - Header: `x-admin-key: <ADMIN_API_KEY>`
  - Suggested cadence: every 12h.
  - Railway job example command:
    ```
    curl -X POST -H "x-admin-key: $ADMIN_API_KEY" "$WEBHOOK_BASE_URL/api/cron"
    ```

## API
- Health:
  - `GET /health`
  - `GET /health/db`
- Admin (header `x-admin-key: ADMIN_API_KEY`):
  - `GET /api/stats`
  - `GET /api/users?q=...`
  - `POST /api/grant` `{ident, days}`
  - `POST /api/revoke` `{ident}`
  - `POST /api/message` `{ident, text}`
  - `POST /api/broadcast` `{text, premium_only?}`
  - `POST /api/cron` `{}`

## Admin Panel
- Routes:
  - `GET /admin/login` → login with `ADMIN_API_KEY`
  - `GET /admin/` → dashboard (stats, run cron)
  - `GET /admin/users?q=...` → search users, grant/revoke/message
  - `GET/POST /admin/broadcast` → send broadcast (optionally premium-only)
  - `GET /admin/logout`
- Setup:
  - Set `SECRET_KEY` for Flask session signing.
  - Admin login uses the same `ADMIN_API_KEY`.

## Curl examples
```
# Stats
curl -H "x-admin-key: $ADMIN_API_KEY" https://<host>/api/stats

# Search
curl -H "x-admin-key: $ADMIN_API_KEY" "https://<host>/api/users?q=@username"

# Grant 30d
curl -X POST -H "Content-Type: application/json" -H "x-admin-key: $ADMIN_API_KEY" \
  -d '{"ident":"@username","days":30}' https://<host>/api/grant

# Cron
curl -X POST -H "x-admin-key: $ADMIN_API_KEY" https://<host>/api/cron
```

## Telegram usage
- `/start` register/update user
- `/status` premium status
- `/premium` payment info
- `/verify_upi <txn_id>`
- `/verify_usdt <tx_hash>`

## Notes
- DB SSL required via `sslmode=require`.
- CORS allowed via `FRONTEND_ORIGIN`.
- Secrets are not logged.
