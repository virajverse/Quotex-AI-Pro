import os
import sys
import re
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
    sleep_seconds = int(os.getenv("SCHEDULER_INTERVAL_SECONDS") or 3600)
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
        time.sleep(sleep_seconds)


def main():
    try:
        bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    @bot.message_handler(commands=["start"]) 
    def cmd_start(message):
        uid = message.from_user.id
        name = (message.from_user.first_name or "").strip()
        last = (getattr(message.from_user, "last_name", "") or "").strip()
        full_name = (name + (" " + last if last else "")).strip() or "User"
        try:
            db.create_user(uid, full_name, "")
            db.update_last_login(uid)
        except Exception:
            pass
        bot.reply_to(
            message,
            "Welcome to QuotexAI Pro!\nUse /status to check premium.\nUse /premium for upgrade options.\nUse /help for all commands.",
        )

    @bot.message_handler(commands=["help"]) 
    def cmd_help(message):
        text = (
            "Commands:\n"
            "/start - register and show intro\n"
            "/status - view premium status\n"
            "/premium - payment options\n"
            "/verify_upi <txn_id> - submit UPI txn id\n"
            "/verify_usdt <tx_hash> - submit USDT tx hash\n"
            "/id - show your Telegram ID"
        )
        bot.reply_to(message, text)

    @bot.message_handler(commands=["id"]) 
    def cmd_id(message):
        bot.reply_to(message, f"Your Telegram ID: {message.from_user.id}")

    @bot.message_handler(commands=["status"]) 
    def cmd_status(message):
        uid = message.from_user.id
        u = db.get_user_by_telegram(uid)
        if not u:
            bot.reply_to(message, "No profile yet. Send /start")
            return
        premium = bool(u.get("is_premium"))
        exp = u.get("expires_at") or "-"
        label = "ACTIVE" if premium and exp else "INACTIVE"
        bot.reply_to(message, f"Premium: {label}\nExpires: {exp}")

    UPI_ID = os.getenv("UPI_ID", "")
    USDT_TRC20_ADDRESS = os.getenv("USDT_TRC20_ADDRESS", "")
    EVM_ADDRESS = os.getenv("EVM_ADDRESS", "")

    @bot.message_handler(commands=["premium"]) 
    def cmd_premium(message):
        parts = ["Upgrade to Premium:"]
        if UPI_ID:
            parts.append(f"• UPI: {UPI_ID}")
        if USDT_TRC20_ADDRESS:
            parts.append(f"• USDT (TRC20): {USDT_TRC20_ADDRESS}")
        if EVM_ADDRESS:
            parts.append(f"• USDT (ERC20): {EVM_ADDRESS}")
        parts.append("After payment, send /verify_upi <txn_id> or /verify_usdt <tx_hash>.")
        bot.reply_to(message, "\n".join(parts))

    @bot.message_handler(commands=["verify_upi"]) 
    def cmd_verify_upi(message):
        m = re.match(r"^/verify_upi\s+(\S+)$", message.text.strip())
        if not m:
            bot.reply_to(message, "Usage: /verify_upi <txn_id>")
            return
        txn_id = m.group(1)
        try:
            db.add_pending_verification(message.from_user.id, "upi", txn_id)
            bot.reply_to(message, "UPI verification submitted. We'll review and grant access soon.")
        except Exception as e:
            bot.reply_to(message, f"Error: {e}")

    @bot.message_handler(commands=["verify_usdt"]) 
    def cmd_verify_usdt(message):
        m = re.match(r"^/verify_usdt\s+(\S+)$", message.text.strip())
        if not m:
            bot.reply_to(message, "Usage: /verify_usdt <tx_hash>")
            return
        txh = m.group(1)
        try:
            db.add_pending_verification(message.from_user.id, "usdt", txh)
            bot.reply_to(message, "USDT verification submitted. We'll review and grant access soon.")
        except Exception as e:
            bot.reply_to(message, f"Error: {e}")

    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
    # Long polling
    bot.infinity_polling(skip_pending=True, allowed_updates=["message"], long_polling_timeout=30)


if __name__ == "__main__":
    main()
