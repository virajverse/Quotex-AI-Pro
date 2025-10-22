import os
import sys
import threading
import time
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import telebot

# Allow importing modules from repo root when Render root directory is backend/
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from database import Database

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required for the worker")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")
db = Database()


def scheduler_loop():
    while True:
        try:
            today = date.today()
            for delta in (3, 1):
                target = today + timedelta(days=delta)
                users = db.users_expiring_on(target, 1000)
                for u in users:
                    uid = u.get("telegram_id")
                    if not uid:
                        continue
                    action = f"notice_d{delta}"
                    if db.has_sent_notice_today(uid, action):
                        continue
                    try:
                        bot.send_message(uid, f"⏰ Reminder: Your premium expires in {delta} day(s). Renew to keep access to signals.")
                        db.record_notice(uid, action)
                    except Exception:
                        pass
            expired = db.users_expired_before(today, 1000)
            for u in expired:
                uid = u.get("telegram_id")
                if not uid:
                    continue
                try:
                    db.set_premium_status(uid, False)
                    bot.send_message(uid, "❗ Your premium has expired. Use /premium to renew.")
                    db.record_notice(uid, "notice_expired")
                except Exception:
                    pass
        except Exception:
            pass
        time.sleep(3600)


def main():
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
    # Long polling
    bot.infinity_polling(skip_pending=True, allowed_updates=["message"])


if __name__ == "__main__":
    main()
