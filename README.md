<div align="center">

# Quotex AI Pro — Your Telegram Signals Bot + Neon Admin Panel ⚡🤖

One bot. One panel. All signals. Delightfully simple payments and a neon-dark admin experience.

[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)
[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)](#)
[![Python](https://img.shields.io/badge/python-3.11+-3776AB.svg?logo=python&logoColor=white)](#)
[![Flask](https://img.shields.io/badge/Flask-3.x-000000.svg?logo=flask)](#)
[![Database](https://img.shields.io/badge/Database-SQLite-003B57.svg?logo=sqlite&logoColor=white)](#)
[![Telegram](https://img.shields.io/badge/Telegram-Bot-26A5E4.svg?logo=telegram&logoColor=white)](#)
[![License](https://img.shields.io/badge/license-UNLICENSED-lightgrey.svg)](#license)

</div>

---

## Table of Contents
- [Overview](#overview)
- [Screenshots / Demo](#screenshots--demo)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Usage Examples](#usage-examples)
- [Features](#features)
- [Folder Structure](#folder-structure)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## Overview
Quotex AI Pro is a production-ready Telegram bot with a sleek neon-dark admin panel. It delivers trading signals, tracks usage, and streamlines payments.

- Built with Python + Flask and the official-style Telegram Bot API library.
- Uses a simple, portable SQLite database (`bot.db`). No cloud DB needed.
- Admins can review receipts (JPG/PNG/WebP/PDF), approve/reject, grant days/credits, and broadcast.
- User-friendly payment flow for UPI (QR-only) and USDT.

Why it’s different:
- Focused signal UX for users. Minimal friction to subscribe, verify, and receive signals.
- Neon analytics-style admin UI that feels modern and fast.
- Zero DevOps: run locally or deploy anywhere that can run Flask.

---

## Screenshots / Demo
> Replace these placeholders with your actual screenshots or live demo links.

| UI | Preview |
| --- | --- |
| Admin Dashboard | ![Dashboard](docs/screenshots/admin-dashboard.png) |
| Verifications | ![Verifications](docs/screenshots/verifications.png) |
| Telegram Bot | ![Bot Chat](docs/screenshots/bot-chat.png) |

- Live demo: https://your-demo-url.example (optional)

---

## Tech Stack
- ⚙️ Backend: ![Flask](https://img.shields.io/badge/Flask-3.x-000000?logo=flask) + ![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
- 🤖 Bot: ![Telegram](https://img.shields.io/badge/pyTelegramBotAPI-4.x-26A5E4?logo=telegram&logoColor=white)
- 🗄️ Database: ![SQLite](https://img.shields.io/badge/SQLite-embedded-003B57?logo=sqlite&logoColor=white)
- 🧰 Utilities: ![Gunicorn](https://img.shields.io/badge/Gunicorn-21.x-499848) ![dotenv](https://img.shields.io/badge/dotenv-1.x-2B5F2F)

---

## Getting Started

### 1) Prerequisites
- Python 3.11+
- Telegram Bot token (from BotFather)

### 2) Clone and configure env
```bash
git clone https://github.com/virajverse/Quotex-AI-Pro.git
cd Quotex-AI-Pro
```
Copy environment file and edit values:
```bash
cp .env.example .env.local   # or copy manually on Windows
```
Key vars to set in `.env.local`:
- `BOT_TOKEN` — Telegram bot token
- `ADMIN_API_KEY` — Admin panel key (used for login and API)
- `SECRET_KEY` — Flask session secret
- Optional payments:
  - `UPI_QR_FILE_ID` or `UPI_QR_IMAGE_URL` (QR-only flow)
  - `USDT_TRC20_ADDRESS`, `EVM_ADDRESS`

### 3) Install dependencies
```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

### 4) Run locally
```bash
python backend/app.py
# App on http://127.0.0.1:5000
```

### 5) Webhook (optional for public hosting)
Set your public base URL (ngrok/Render/VPS) and restart the app, or set manually:
```bash
curl -s "https://api.telegram.org/bot$BOT_TOKEN/setWebhook?url=$WEBHOOK_BASE_URL/bot/$BOT_TOKEN&drop_pending_updates=true"
```

---

## Usage Examples

### Telegram commands
```text
/start            # Register / update profile
/premium          # Plans and payment options
/verify_upi ...   # Submit UPI txn id (if you use that flow)
/verify_usdt ...  # Submit USDT tx hash
```

### Admin panel
- Login: `http://127.0.0.1:5000/admin/login` (use `ADMIN_API_KEY`)
- Review verifications, click “View” to open receipts inline (images/PDF), Approve/Reject
- Broadcast messages to all or premium users

### REST examples
```bash
# Stats
curl -H "x-admin-key: $ADMIN_API_KEY" http://127.0.0.1:5000/api/stats

# Search users
curl -H "x-admin-key: $ADMIN_API_KEY" "http://127.0.0.1:5000/api/users?q=@username"
```

---

## Features
- ✔️ Neon-dark, analytics-style admin panel UI
- ✔️ QR-only UPI flow (no deep links)
- ✔️ Receipt review: JPG/PNG/WebP/PDF inline preview
- ✔️ Credit + daily limit tracking for signals
- ✔️ Broadcast to all or premium users
- ✔️ Simple SQLite — portable and zero-config
- ✔️ Typing indicators for heavy operations

---

## Folder Structure
```text
.
├─ backend/
│  ├─ app.py                # Flask app + Telegram handlers + Admin routes
│  ├─ utils.py              # Logging, signal formatting, helpers
│  ├─ sqlite_db.py          # SQLite models and queries
│  ├─ templates/
│  │  └─ admin/             # Admin HTML templates
│  └─ assets/               # Static assets (if any)
├─ .env.example             # Env template
├─ .env.local               # Local overrides (gitignored)
├─ requirements.txt         # Python deps
├─ bot.db                   # SQLite database (runtime)
└─ README.md
```

---

## Contributing
Contributions are welcome! To propose changes:
1. Fork the repo and create a feature branch.
2. Follow existing code style. Keep changes small and focused.
3. Add screenshots for UI changes (admin/panels).
4. Open a pull request with a clear description and testing notes.

---

## License
[![License](https://img.shields.io/badge/license-UNLICENSED-lightgrey.svg)](#)

This repository currently does not include an OSS license. All rights reserved. Contact the author for commercial use.

---

## Contact
- **GitHub**: https://github.com/virajverse
- **LinkedIn**: https://www.linkedin.com/in/ (add your handle)
- **Website**: https://your-website.example

If you ship with this template, drop a star ⭐ and share feedback — it helps a ton!
