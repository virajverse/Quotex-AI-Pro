import os
from datetime import datetime
from flask import Flask, jsonify
from dotenv import load_dotenv
import telebot
from telebot import types

from database import Database
from admin import create_admin_blueprint
from webhook import create_webhook_blueprint
from analysis import generate_signal_with_chart

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = os.getenv("ADMIN_ID", "")
APP_BASE_URL = os.getenv("APP_BASE_URL", "https://your-app.railway.app")
USDT_TRC20_ADDRESS = os.getenv("USDT_TRC20_ADDRESS", "")
TRON_ADDRESS = os.getenv("TRON_ADDRESS", USDT_TRC20_ADDRESS)
EVM_ADDRESS = os.getenv("EVM_ADDRESS", "")
UPI_ID = os.getenv("UPI_ID", "")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev_secret")

db = Database(os.getenv("DATABASE_PATH", "data.db"))

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")

pending = {}


def main_keyboard(uid: int | None = None):
    kb = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    if uid and is_logged_in(uid):
        kb.row(types.KeyboardButton("🚀 GET STARTED"), types.KeyboardButton("📈 LIVE SIGNALS"))
        kb.row(types.KeyboardButton("📊 ANALYSIS TOOLS"), types.KeyboardButton("📅 PLAN STATUS"))
        kb.row(types.KeyboardButton("👤 PROFILE"), types.KeyboardButton("💬 SUPPORT"))
        kb.row(types.KeyboardButton("⚠️ RISK DISCLAIMER"))
    else:
        kb.row(types.KeyboardButton("✅ SIGN UP"), types.KeyboardButton("🔑 LOGIN"))
        kb.row(types.KeyboardButton("🚀 GET STARTED"), types.KeyboardButton("❓ HOW IT WORKS"))
        kb.row(types.KeyboardButton("📈 LIVE SIGNALS"), types.KeyboardButton("📊 ANALYSIS TOOLS"))
        kb.row(types.KeyboardButton("📅 PLAN STATUS"), types.KeyboardButton("💬 SUPPORT"))
        kb.row(types.KeyboardButton("⚠️ RISK DISCLAIMER"))
    return kb


@bot.message_handler(commands=["start"])    
def start_cmd(message):
    text = (
        "Welcome to *QuotexAI Pro*\n\n"
        "Choose an option below to continue."
    )
    bot.send_message(message.chat.id, text, reply_markup=main_keyboard(message.chat.id))


def is_logged_in(uid: int) -> bool:
    u = db.get_user_by_telegram(uid)
    return bool(u and u.get("logged_in"))


def premium_active(uid: int) -> bool:
    return db.is_premium_active(uid)


@bot.message_handler(commands=["signals"])    
def signals_cmd(message):
    uid = message.chat.id
    if not premium_active(uid):
        bot.send_message(uid, "Premium required. Go to PLAN STATUS to upgrade.")
        return
    send_sample_signal(uid)


def send_sample_signal(chat_id: int):
    signal = generate_signal_with_chart("BTC/USDT")
    bot.send_message(chat_id, signal, parse_mode=None)


def is_admin_user(message) -> bool:
    aid = (ADMIN_ID or "").strip()
    if not aid:
        return False
    uid_match = aid == str(message.from_user.id)
    uname = (getattr(message.from_user, "username", None) or "").lower()
    uname_match = aid.lower().lstrip('@') == uname if uname else False
    return uid_match or uname_match


@bot.message_handler(commands=["premium"])    
def premium_cmd(message):
    tron_addr = TRON_ADDRESS or USDT_TRC20_ADDRESS or "YOUR_TRON_ADDRESS"
    evm_addr = EVM_ADDRESS or "YOUR_EVM_ADDRESS"
    upi = UPI_ID or "yourname@oksbi"
    lines = [
        "💰 Premium: ₹499 (~6 USDT)",
        "\n🔗 USDT Payment Options:",
        f"• TRC20 (TRON): `{tron_addr}`",
        f"• EVM (ETH/BSC/Polygon): `{evm_addr}`",
        "After payment, use: `/verify TRANSACTION_ID`",
        "",
        "🇮🇳 Pay via UPI (India Only)",
        "Amount: ₹499",
        f"UPI ID: `{upi}`",
        "Use any UPI app (PhonePe, GPay, Paytm)",
        "After payment, reply with:",
        "`/verify_upi YOUR_NAME`",
    ]
    bot.send_message(message.chat.id, "\n".join(lines), parse_mode="Markdown")
    try:
        db.enqueue_premium_request(message.chat.id)
    except Exception:
        pass


@bot.message_handler(commands=["verify"])    
def verify_cmd(message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /verify <TRANSACTION_ID>")
        return
    tx_id = parts[1].strip()
    if not tx_id:
        bot.send_message(message.chat.id, "Please provide a valid transaction ID.")
        return
    db.add_verification(message.chat.id, tx_id, status="pending")
    bot.send_message(message.chat.id, "✅ Verification submitted. We'll review and update your premium shortly.")


@bot.message_handler(commands=["verify_upi"])    
def verify_upi_cmd(message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /verify_upi Full Name")
        return
    full_name = parts[1].strip()
    if not full_name or len(full_name) < 2:
        bot.send_message(message.chat.id, "Please provide your full name linked to UPI.")
        return
    db.add_pending_verification(message.chat.id, "upi", full_name)
    bot.send_message(message.chat.id, "✅ UPI verification request submitted. Admin will verify shortly.")


@bot.message_handler(commands=["admin_grant"])    
def admin_grant_cmd(message):
    if not is_admin_user(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        bot.send_message(message.chat.id, "Usage: /admin_grant <TELEGRAM_USER_ID>")
        return
    target_id = int(parts[1].strip())
    db.grant_premium(target_id, 30)
    bot.send_message(message.chat.id, f"Granted premium for 30 days to {target_id}.")


@bot.message_handler(commands=["admin_list_pending"])    
def admin_list_pending_cmd(message):
    if not is_admin_user(message):
        return
    items = db.list_pending_verifications(50)
    if not items:
        bot.send_message(message.chat.id, "No pending verifications.")
        return
    lines = ["Pending Verifications:"]
    for v in items[:20]:
        lines.append(f"#{v['id']} | uid:{v['telegram_id']} | tx:{v['tx_id']} | {v['created_at']}")
    bot.send_message(message.chat.id, "\n".join(lines))


@bot.message_handler(commands=["admin_list_upi"])    
def admin_list_upi_cmd(message):
    if not is_admin_user(message):
        return
    items = db.list_pending_verifications_by_type("upi", 50)
    if not items:
        bot.send_message(message.chat.id, "No pending UPI verifications.")
        return
    lines = ["Pending UPI Verifications:"]
    for v in items[:20]:
        lines.append(f"#{v['id']} | uid:{v['telegram_id']} | name:{v['data']} | {v['timestamp']}")
    bot.send_message(message.chat.id, "\n".join(lines))


@bot.message_handler(commands=["admin_list_queue"])    
def admin_list_queue_cmd(message):
    if not is_admin_user(message):
        return
    items = db.list_premium_queue(50)
    if not items:
        bot.send_message(message.chat.id, "Premium queue is empty.")
        return
    lines = ["Premium Queue (oldest first):"]
    for v in items[:20]:
        lines.append(f"#{v['id']} | uid:{v['telegram_id']} | matched:{v['matched_payment_id']} | {v['created_at']}")
    bot.send_message(message.chat.id, "\n".join(lines))


@bot.message_handler(func=lambda m: True)    
def router(message):
    uid = message.chat.id
    text = (message.text or "").strip()
    state = pending.get(uid, {})

    # Avoid duplicate replies for slash commands handled above
    if text.startswith('/'):
        return

    if state.get("stage") == "signup_name":
        state["name"] = text
        state["stage"] = "signup_email"
        pending[uid] = state
        bot.send_message(uid, "Enter your email:")
        return
    if state.get("stage") == "signup_email":
        name = state.get("name") or ""
        email = text
        db.create_user(uid, name, email)
        pending.pop(uid, None)
        bot.send_message(uid, "Signup complete. You're logged in.", reply_markup=main_keyboard(uid))
        return
    if state.get("stage") == "login_email":
        email = text
        u = db.get_user_by_email(email)
        if not u:
            bot.send_message(uid, "Email not found. Please SIGN UP.")
            pending.pop(uid, None)
            return
        db.link_email_to_telegram(email, uid)
        bot.send_message(uid, "Login successful.", reply_markup=main_keyboard(uid))
        pending.pop(uid, None)
        return

    if text == "✅ SIGN UP":
        if is_logged_in(uid):
            bot.send_message(uid, "You're already logged in.", reply_markup=main_keyboard(uid))
            return
        pending[uid] = {"stage": "signup_name"}
        bot.send_message(uid, "Enter your name:")
        return

    if text == "🔑 LOGIN":
        if is_logged_in(uid):
            bot.send_message(uid, "You're already logged in.", reply_markup=main_keyboard(uid))
            return
        pending[uid] = {"stage": "login_email"}
        bot.send_message(uid, "Enter your registered email:")
        return

    if text == "🚀 GET STARTED":
        if not is_logged_in(uid):
            bot.send_message(uid, "Please SIGN UP or LOGIN first.")
            return
        if premium_active(uid):
            bot.send_message(uid, "You're premium. Use LIVE SIGNALS and ANALYSIS TOOLS.")
            return
        bot.send_message(uid, "To unlock 30-day premium: use /premium to view payment options (USDT/UPI).")
        return

    if text == "❓ HOW IT WORKS":
        bot.send_message(uid, "We deliver curated signals. Upgrade for full access.")
        return

    if text == "📈 LIVE SIGNALS":
        if premium_active(uid):
            send_sample_signal(uid)
        else:
            bot.send_message(uid, "Premium required. See PLAN STATUS.")
        return

    if text == "📊 ANALYSIS TOOLS":
        if premium_active(uid):
            bot.send_message(uid, "Tools coming soon.")
        else:
            bot.send_message(uid, "Premium required. See PLAN STATUS.")
        return

    if text == "📅 PLAN STATUS":
        u = db.get_user_by_telegram(uid)
        if not u or not u.get("logged_in"):
            bot.send_message(uid, "Please SIGN UP or LOGIN first.")
            return
        if premium_active(uid):
            bot.send_message(uid, f"Premium active until {u.get('expires_at')}")
        else:
            bot.send_message(uid, "No active plan. Use /premium to view payment options (USDT/UPI).")
        return

    if text == "👤 PROFILE":
        u = db.get_user_by_telegram(uid)
        if not u or not u.get("logged_in"):
            bot.send_message(uid, "Please SIGN UP or LOGIN first.", reply_markup=main_keyboard(uid))
            return
        lines = [
            "👤 Profile",
            f"Name: {u.get('name') or '-'}",
            f"Email: {u.get('email') or '-'}",
            f"Premium: {'Active' if u.get('is_premium') else 'Inactive'}",
        ]
        if u.get('is_premium') and u.get('expires_at'):
            lines.append(f"Expires: {u.get('expires_at')}")
        bot.send_message(uid, "\n".join(lines), reply_markup=main_keyboard(uid))
        return

    if text == "💬 SUPPORT":
        bot.send_message(uid, "Support: @your_support")
        return

    if text == "⚠️ RISK DISCLAIMER":
        bot.send_message(uid, "Trading involves risk. Not financial advice.")
        return

    bot.send_message(uid, "Unknown command. Use the menu.")


app.register_blueprint(create_webhook_blueprint(bot, db))
app.register_blueprint(create_admin_blueprint(db, bot), url_prefix="/admin")


@app.route("/health")    
def health():
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
