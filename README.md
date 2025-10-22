# QuotexAI Pro — Telegram Bot + Admin Panel

A professional Telegram bot with premium access, USDT (TRON/EVM) + UPI payments, and an admin dashboard.

## Features
- User auth with SIGN UP / LOGIN via bot
- Premium purchase (₹499) via:
  - USDT (TRC20 on TRON and EVM chains: ETH/BSC/Polygon)
  - UPI (India only) with manual verification
- Admin Panel at `/admin` with stats, user management, and broadcast
- SQLite database shared by bot and admin app

## Tech Stack
- Python, Flask, pyTelegramBotAPI, SQLite, python-dotenv, requests
- Bootstrap 5 UI (no heavy JS frameworks)

## File Structure
- `main.py` — Bot and Flask app entrypoint
- `webhook.py` — Telegram webhook handler (no payment gateway)
- `admin.py` — Admin routes and actions
- `database.py` — SQLite schema and helpers
- `templates/` — `admin.html` (legacy `pay.html` unused)
- `static/style.css` — Minimal styling
- `requirements.txt` — Dependencies
- `README.md` — This file

## Environment Variables (.env)
```
BOT_TOKEN=123456:ABC...
ADMIN_ID=123456789            # Numeric Telegram ID or @username for in-chat admin
SECRET_KEY=change_me
DATABASE_PATH=data.db

# Payment receiving addresses
TRON_ADDRESS=T...             # USDT TRC20 address on TRON
EVM_ADDRESS=0x...             # USDT ERC20/BEP20/Polygon address (same 0x on EVM chains)
UPI_ID=yourname@oksbi         # UPI for India

# Explorer API keys (for optional auto-monitor service)
TRONGRID_API_KEY=...
ETHERSCAN_API_KEY=...
BSCSCAN_API_KEY=...
POLYGONSCAN_API_KEY=...

# Webhook
APP_BASE_URL=https://your-app.railway.app
TELEGRAM_WEBHOOK_SECRET=super-secret-header

# Optional
POLL_INTERVAL_SECONDS=30
```

## Getting a BOT_TOKEN
1. Open Telegram and chat with @BotFather
2. `/newbot` → choose name (e.g., "QuotexAI Pro") and username (e.g., `@QuotexAI_Pro_bot`)
3. Copy the token as `BOT_TOKEN`

## Payment Options in the Bot
- Run `/premium` to see payment methods:
  - TRC20 (TRON): shows `TRON_ADDRESS`
  - EVM (ETH/BSC/Polygon): shows `EVM_ADDRESS`
  - UPI (India): shows `UPI_ID`
- After USDT transfer: `/verify <TRANSACTION_ID>`
- After UPI transfer: `/verify_upi Full Name`

## Local Setup
```
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```
The app starts on `http://localhost:5000`.

## Set Telegram Webhook
Telegram pushes updates to your server. Set the webhook like this (PowerShell example):
```
$BOT_TOKEN = "<your-token>"
$APP_BASE_URL = "https://your-app.railway.app"
$SECRET = "<same-as-TELEGRAM_WEBHOOK_SECRET>"

Invoke-WebRequest -Uri "https://api.telegram.org/bot$BOT_TOKEN/setWebhook?url=$APP_BASE_URL/webhook/telegram&secret_token=$SECRET"
```
This configures Telegram to send updates to `/webhook/telegram` with the secret header `X-Telegram-Bot-Api-Secret-Token`.

## Deploy on Railway
1. Create a new Railway project
2. Add this repo/folder
3. Set Environment Variables from above
4. Set Start Command: `python main.py`
5. Deploy
6. Set Telegram webhook using your Railway URL as shown above

## Admin Panel
- URL: `https://your-app.railway.app/admin`
- Login: enter the same `ADMIN_ID` you set in env
- Dashboard includes:
  - Stats: Total users, Active premium, Today's new signups
  - User table with actions: Grant 30 days, Revoke, Send Message
  - Broadcast to all premium users

## Premium Flow (USDT/UPI)
- Use `/premium` to see options.
- USDT: send 6 USDT to TRON/EVM address → `/verify <TXID>`.
- UPI (India): pay ₹499 to `UPI_ID` → `/verify_upi Full Name`.
- Admin can grant manually via `/admin_grant <TELEGRAM_USER_ID>`.
- Admin tools:
  - `/admin_list_pending` — Legacy USDT TX submissions
  - `/admin_list_upi` — Pending UPI submissions
  - `/admin_list_queue` — FIFO premium queue (for auto-monitor matching)
  - `/signals` — Premium-only content (user-facing)

## Signal Format
The bot sends signals like:
```
🔔 QuotexAI Pro SIGNAL — [BTC/USDT]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 Direction: UP 🟢
📈 Confidence: 4/5
⏱️ Expiry: 15 min
🔍 Analysis: EMA Golden Cross, RSI 58, Volume Spike
⚠️ Not financial advice.
```

## Notes
- All admin actions are logged in `admin_logs`
- Only admin endpoints live under `/admin`
- For production, consider `gunicorn main:app` as the start command

---

## How to get addresses
### TRON (TRC20)
- Trust Wallet: enable USDT (TRON), tap Receive → copy address (starts with `T...`).
- Put it in `.env` as `TRON_ADDRESS`.

### EVM (ETH/BSC/Polygon)
- MetaMask/Trust Wallet: open your Ethereum account → copy the 0x address.
- Put it in `.env` as `EVM_ADDRESS`.

## How to check transactions
### Tron
- https://tronscan.org → paste TX hash → confirm "To" = `TRON_ADDRESS`, token = USDT (TRC20), amount ≈ 6 USDT.

### Ethereum/BSC/Polygon
- https://etherscan.io, https://bscscan.com, https://polygonscan.com → paste TX hash → check token = USDT and "To" = `EVM_ADDRESS`.

## Admin commands
- `/admin_grant <TELEGRAM_USER_ID>` — Grants premium access for 30 days.
- `/admin_list_pending` — Lists pending USDT submissions.
- `/admin_list_upi` — Lists pending UPI submissions.
- `/admin_list_queue` — Shows the FIFO premium queue.

Important: Set `ADMIN_ID` to your numeric Telegram user ID (recommended) or your `@username`. Avoid arbitrary strings like names with symbols.
