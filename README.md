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
