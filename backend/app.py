import os
import logging
from functools import wraps

from flask import Flask, request, jsonify, render_template, redirect, url_for, session, flash
from flask_cors import CORS

import telebot
from telebot import types

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from . import database as db
from . import utils

utils.setup_logger()
logger = logging.getLogger("app")

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": os.getenv("FRONTEND_ORIGIN", "*")}})

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "").strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
SECRET_KEY = os.getenv("SECRET_KEY", "").strip()
SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@support").strip()

# Session secret key for Admin Panel
if SECRET_KEY:
    app.secret_key = SECRET_KEY
else:
    app.secret_key = os.urandom(24)
    logging.getLogger("app").warning("SECRET_KEY not set; using random key (sessions reset on restart)")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", threaded=False, disable_web_page_preview=True) if BOT_TOKEN else None
if bot and WEBHOOK_BASE_URL:
    try:
        bot.remove_webhook()
        bot.set_webhook(url=f"{WEBHOOK_BASE_URL}/bot/{BOT_TOKEN}", drop_pending_updates=True)
    except Exception:
        logger.exception("Failed to set webhook")

def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not ADMIN_API_KEY or request.headers.get("x-admin-key", "") != ADMIN_API_KEY:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


# ----- Admin Panel (UI) -----
def ui_login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("admin_authed"):
            return redirect(url_for("admin_login"))
        return fn(*args, **kwargs)
    return wrapper


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        key = (request.form.get("key") or "").strip()
        if ADMIN_API_KEY and key == ADMIN_API_KEY:
            session["admin_authed"] = True
            flash("Welcome!", "success")
            return redirect(url_for("admin_dashboard"))
        flash("Invalid key", "danger")
    return render_template("admin/login.html")


@app.get("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))


@app.get("/admin/")
@ui_login_required
def admin_dashboard():
    stats = db.get_stats()
    return render_template("admin/dashboard.html", stats=stats)


@app.get("/admin/users")
@ui_login_required
def admin_users():
    q = request.args.get("q", "")
    items = db.search_users_admin(q)
    return render_template("admin/users.html", q=q, items=items)


@app.post("/admin/grant")
@ui_login_required
def admin_grant():
    ident = (request.form.get("ident") or "").strip()
    days = int(request.form.get("days") or 0)
    if not ident or days <= 0:
        flash("Provide ident and positive days", "warning")
        return redirect(url_for("admin_users", q=ident))
    user = db.resolve_user_by_ident(ident)
    if not user:
        flash("User not found", "danger")
        return redirect(url_for("admin_users", q=ident))
    new_exp = db.grant_premium_by_user_id(user["id"], days)
    db.log_admin("grant", {"user_id": user["id"], "ident": ident, "days": days}, performed_by="panel")
    if bot:
        utils.send_safe(bot, user["telegram_id"], f"🎉 Premium +{days}d. New expiry: <b>{utils.format_ts_iso(new_exp)}</b>")
    flash("Premium granted", "success")
    return redirect(url_for("admin_users", q=ident))


@app.post("/admin/revoke")
@ui_login_required
def admin_revoke():
    ident = (request.form.get("ident") or "").strip()
    if not ident:
        flash("Provide ident", "warning")
        return redirect(url_for("admin_users", q=ident))
    user = db.resolve_user_by_ident(ident)
    if not user:
        flash("User not found", "danger")
        return redirect(url_for("admin_users", q=ident))
    db.revoke_premium_by_user_id(user["id"])
    db.log_admin("revoke", {"user_id": user["id"], "ident": ident}, performed_by="panel")
    if bot:
        utils.send_safe(bot, user["telegram_id"], "⚠️ Your premium has been revoked.")
    flash("Premium revoked", "success")
    return redirect(url_for("admin_users", q=ident))


@app.post("/admin/message")
@ui_login_required
def admin_message():
    ident = (request.form.get("ident") or "").strip()
    text = (request.form.get("text") or "").strip()
    if not ident or not text:
        flash("Provide ident and text", "warning")
        return redirect(url_for("admin_users", q=ident))
    user = db.resolve_user_by_ident(ident)
    if not user:
        flash("User not found", "danger")
        return redirect(url_for("admin_users", q=ident))
    sent = 1 if (bot and utils.send_safe(bot, user["telegram_id"], text)) else 0
    db.log_admin("message", {"user_id": user["id"], "ident": ident, "text_len": len(text), "sent": sent}, performed_by="panel")
    flash("Message sent" if sent else "Message failed", "success" if sent else "danger")
    return redirect(url_for("admin_users", q=ident))


@app.get("/admin/broadcast")
@ui_login_required
def admin_broadcast_page():
    return render_template("admin/broadcast.html")


@app.post("/admin/broadcast")
@ui_login_required
def admin_broadcast_post():
    text = (request.form.get("text") or "").strip()
    premium_only = bool(request.form.get("premium_only"))
    if not text:
        flash("Provide text", "warning")
        return redirect(url_for("admin_broadcast_page"))
    users = db.list_users_for_broadcast(premium_only=premium_only)
    sent = 0
    for u in users:
        if bot and utils.send_safe(bot, u["telegram_id"], text):
            sent += 1
    db.log_admin("broadcast", {"premium_only": premium_only, "text_len": len(text), "count": len(users), "sent": sent}, performed_by="panel")
    flash(f"Broadcast sent: {sent}/{len(users)}", "success")
    return redirect(url_for("admin_broadcast_page"))


@app.post("/admin/cron")
@ui_login_required
def admin_cron():
    result = utils.run_cron(db, bot)
    db.log_admin("cron", result, performed_by="panel")
    flash(f"Cron run: notices={result.get('notices')} expired={result.get('expired')}", "success")
    return redirect(url_for("admin_dashboard"))

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.get("/health/db")
def health_db():
    try:
        return jsonify({"ok": True, "total_users": db.get_total_users()})
    except Exception:
        logger.exception("DB health failed")
        return jsonify({"ok": False, "error": "db_unavailable"}), 500

@app.route("/bot/<token>", methods=["POST", "GET"])
def telegram_webhook(token: str):
    if not BOT_TOKEN or token != BOT_TOKEN:
        return jsonify({"ok": False, "error": "forbidden"}), 403
    if request.method == "GET":
        return jsonify({"ok": True})
    try:
        update = types.Update.de_json(request.get_data(as_text=True))
        bot.process_new_updates([update])
    except Exception:
        logger.exception("Update processing failed")
        return jsonify({"ok": False}), 200
    return jsonify({"ok": True})

if bot:
    # ----- Main Menu UI -----
    def build_main_menu(user):
        kb = types.InlineKeyboardMarkup(row_width=2)
        registered = bool(user)
        if not registered:
            kb.add(
                types.InlineKeyboardButton("✅ SIGN UP", callback_data="menu:signup"),
                types.InlineKeyboardButton("🔑 LOGIN", callback_data="menu:login"),
            )
        else:
            kb.add(types.InlineKeyboardButton("👤 PROFILE", callback_data="menu:profile"))
        kb.add(
            types.InlineKeyboardButton("🚀 GET STARTED", callback_data="menu:get_started"),
            types.InlineKeyboardButton("❓ HOW IT WORKS", callback_data="menu:how"),
        )
        kb.add(
            types.InlineKeyboardButton("📈 LIVE SIGNALS", callback_data="menu:signals"),
            types.InlineKeyboardButton("📊 ANALYSIS TOOLS", callback_data="menu:tools"),
        )
        kb.add(
            types.InlineKeyboardButton("🕒 MARKET HOURS", callback_data="menu:hours"),
            types.InlineKeyboardButton("📅 PLAN STATUS", callback_data="menu:plan"),
        )
        kb.add(
            types.InlineKeyboardButton("💬 SUPPORT", callback_data="menu:support"),
        )
        kb.add(types.InlineKeyboardButton("⚠️ RISK DISCLAIMER", callback_data="menu:disclaimer"))
        return kb

    def build_assets_kb():
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("BTC/USDT", callback_data="sig:BTCUSDT"),
            types.InlineKeyboardButton("ETH/USDT", callback_data="sig:ETHUSDT"),
        )
        kb.add(
            types.InlineKeyboardButton("EUR/USD", callback_data="sig:EURUSD"),
            types.InlineKeyboardButton("GBP/JPY", callback_data="sig:GBPJPY"),
        )
        kb.add(
            types.InlineKeyboardButton("GOLD", callback_data="sig:GOLD"),
            types.InlineKeyboardButton("NASDAQ", callback_data="sig:NASDAQ"),
        )
        return kb

    def build_timeframes_kb(asset_code: str):
        kb = types.InlineKeyboardMarkup(row_width=3)
        kb.add(
            types.InlineKeyboardButton("1m", callback_data=f"tf:{asset_code}:1m"),
            types.InlineKeyboardButton("3m", callback_data=f"tf:{asset_code}:3m"),
            types.InlineKeyboardButton("5m", callback_data=f"tf:{asset_code}:5m"),
        )
        return kb

    @bot.message_handler(commands=["start"])
    def cmd_start(m: types.Message):
        u = m.from_user
        db.upsert_user(u.id, u.username or "", u.first_name or "", u.last_name or "", u.language_code or None)
        user = db.get_user_by_telegram_id(u.id)
        msg = "You have an active premium subscription." if user and user.get("premium_active") else "You do not have an active premium subscription."
        utils.send_safe(bot, u.id, f"👋 Welcome, {utils.escape_html(u.first_name or 'friend')}!\n\n{msg}\nUse /menu to open the main menu.")
        try:
            bot.send_message(u.id, "Main Menu:", reply_markup=build_main_menu(user))
        except Exception:
            pass

    @bot.message_handler(commands=["help"])
    def cmd_help(m: types.Message):
        utils.send_safe(bot, m.chat.id, "Commands:\n/id\n/status\n/premium\n/menu\n/signal\n/hours\n/verify_upi <txn_id>\n/verify_usdt <tx_hash>")

    @bot.message_handler(commands=["menu"])
    def cmd_menu(m: types.Message):
        user = db.get_user_by_telegram_id(m.from_user.id)
        try:
            bot.send_message(m.chat.id, "Main Menu:", reply_markup=build_main_menu(user))
        except Exception:
            pass

    @bot.message_handler(commands=["signal"])
    def cmd_signal(m: types.Message):
        user = db.get_user_by_telegram_id(m.from_user.id)
        if not user or not user.get("premium_active"):
            utils.send_safe(bot, m.chat.id, "📈 Live signals are for premium users. Use /status or PLAN STATUS to check your subscription.")
            return
        try:
            bot.send_message(m.chat.id, "Select an asset:", reply_markup=build_assets_kb())
        except Exception:
            pass

    @bot.message_handler(commands=["hours"])
    def cmd_hours(m: types.Message):
        msg = utils.market_hours_message()
        utils.send_safe(bot, m.chat.id, msg)

    @bot.message_handler(commands=["id"])
    def cmd_id(m: types.Message):
        utils.send_safe(bot, m.chat.id, f"Your Telegram ID: <code>{m.from_user.id}</code>")

    @bot.message_handler(commands=["status"])
    def cmd_status(m: types.Message):
        user = db.get_user_by_telegram_id(m.from_user.id)
        if not user or not user.get("premium_active"):
            utils.send_safe(bot, m.chat.id, "❌ Premium status: Inactive")
            return
        utils.send_safe(bot, m.chat.id, f"✅ Premium: Active\nExpires: <b>{utils.format_ts_iso(user.get('premium_expires_at'))}</b>")

    @bot.message_handler(commands=["premium"])
    def cmd_premium(m: types.Message):
        lines = ["Payment options:"]
        if os.getenv("UPI_ID"): lines.append(f"- UPI: <code>{utils.escape_html(os.getenv('UPI_ID'))}</code>")
        if os.getenv("USDT_TRC20_ADDRESS"): lines.append(f"- USDT TRC20: <code>{utils.escape_html(os.getenv('USDT_TRC20_ADDRESS'))}</code>")
        if len(lines) == 1: lines.append("- Not configured. Contact admin.")
        lines.append("\nAfter payment, send /verify_upi <txn_id> or /verify_usdt <tx_hash>.")
        utils.send_safe(bot, m.chat.id, "\n".join(lines))

    @bot.message_handler(commands=["verify_upi"])
    def cmd_verify_upi(m: types.Message):
        parts = (m.text or "").split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            utils.send_safe(bot, m.chat.id, "Usage: /verify_upi <txn_id>")
            return
        user = db.get_user_by_telegram_id(m.from_user.id) or (
            db.upsert_user(m.from_user.id, m.from_user.username or "", m.from_user.first_name or "", m.from_user.last_name or "", m.from_user.language_code or None) or
            db.get_user_by_telegram_id(m.from_user.id)
        )
        db.insert_verification(user["id"], "upi", "pending", tx_id=parts[1].strip(), tx_hash=None, amount=None, currency=None, request_data={"from":"bot"})
        utils.send_safe(bot, m.chat.id, "✅ UPI verification received. We'll review and update you.")

    @bot.message_handler(commands=["verify_usdt"])
    def cmd_verify_usdt(m: types.Message):
        parts = (m.text or "").split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            utils.send_safe(bot, m.chat.id, "Usage: /verify_usdt <tx_hash>")
            return
        txh = parts[1].strip()
        user = db.get_user_by_telegram_id(m.from_user.id) or (
            db.upsert_user(m.from_user.id, m.from_user.username or "", m.from_user.first_name or "", m.from_user.last_name or "", m.from_user.language_code or None) or
            db.get_user_by_telegram_id(m.from_user.id)
        )
        res = utils.verify_transaction(txh)
        status = "auto_pass" if res.get("found") and res.get("success") else "pending"
        method = "usdt_trc20" if res.get("network") == "tron" else "evm"
        db.insert_verification(user["id"], method, status, tx_id=None, tx_hash=txh, amount=None, currency="USDT", request_data=res, notes=None)
        utils.send_safe(bot, m.chat.id, "✅ Tx received. " + ("Auto-verified." if status == "auto_pass" else "Awaiting manual review."))

    @bot.message_handler(func=lambda m: True, content_types=["text"])
    def on_text(m: types.Message):
        try:
            db.touch_user_activity(m.from_user.id, saw=True, messaged=True)
        except Exception:
            pass
        txt = (m.text or "").strip().lower()
        if txt in ("signal", "signals", "get signal", "get signals"):
            user = db.get_user_by_telegram_id(m.from_user.id)
            if not user or not user.get("premium_active"):
                utils.send_safe(bot, m.chat.id, "📈 Live signals are for premium users. Use /status or PLAN STATUS to check your subscription.")
                return
            try:
                bot.send_message(m.chat.id, "Select an asset:", reply_markup=build_assets_kb())
            except Exception:
                pass

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("menu:"))
    def on_menu_click(call: types.CallbackQuery):
        action = call.data.split(":", 1)[1]
        uid = call.from_user.id
        user = db.get_user_by_telegram_id(uid)
        text = None

        if action in ("signup", "login"):
            if not user:
                db.upsert_user(uid, call.from_user.username or "", call.from_user.first_name or "", call.from_user.last_name or "", call.from_user.language_code or None)
                user = db.get_user_by_telegram_id(uid)
            text = "✅ You are now registered."
        elif action == "profile":
            p = user or {}
            status = "Active" if p.get("premium_active") else "Inactive"
            exp = utils.format_ts_iso(p.get("premium_expires_at")) if p else "N/A"
            text = (
                f"👤 Profile\n"
                f"Name: {utils.escape_html((call.from_user.first_name or '').strip())}\n"
                f"Username: @{utils.escape_html(call.from_user.username or '')}\n"
                f"Status: {status}\n"
                f"Expiry: {exp}"
            )
        elif action == "get_started":
            text = (
                "🚀 Get Started\n"
                "1) Tap SIGN UP or /start to register.\n"
                "2) Pay via /premium options.\n"
                "3) After grant, you will receive LIVE SIGNALS here."
            )
        elif action == "how":
            text = (
                "❓ How it works\n"
                "- We verify your payment (UPI/USDT).\n"
                "- Admin grants premium.\n"
                "- Signals and updates are sent right in this chat."
            )
        elif action == "signals":
            if user and user.get("premium_active"):
                try:
                    bot.send_message(call.message.chat.id, "Select an asset:", reply_markup=build_assets_kb())
                except Exception:
                    pass
                text = None
            else:
                text = "📈 Live signals are for premium users. Check PLAN STATUS for your subscription."
        elif action == "tools":
            text = "📊 Analysis tools coming soon."
        elif action == "hours":
            text = utils.market_hours_message()
        elif action == "plan":
            if not user or not user.get("premium_active"):
                text = "❌ Premium status: Inactive"
            else:
                text = f"✅ Premium: Active\nExpires: <b>{utils.format_ts_iso(user.get('premium_expires_at'))}</b>"
        elif action == "support":
            text = f"💬 Support: {utils.escape_html(SUPPORT_CONTACT)}"
        elif action == "disclaimer":
            text = (
                "⚠️ Risk Disclaimer\n"
                "Trading involves risk. Past performance is not indicative of future results."
            )

        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass

        if text:
            try:
                bot.send_message(call.message.chat.id, text, reply_markup=build_main_menu(db.get_user_by_telegram_id(uid)))
            except Exception:
                pass
        # Update the menu on the original message too (hide signup/login after registration)
        try:
            bot.edit_message_reply_markup(chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=build_main_menu(db.get_user_by_telegram_id(uid)))
        except Exception:
            pass

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("sig:"))
    def on_signal_asset(call: types.CallbackQuery):
        uid = call.from_user.id
        user = db.get_user_by_telegram_id(uid)
        if not user or not user.get("premium_active"):
            try:
                bot.answer_callback_query(call.id, "Premium required")
            except Exception:
                pass
            return
        asset_code = call.data.split(":", 1)[1]
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        try:
            bot.send_message(call.message.chat.id, f"Timeframe for {asset_code}:", reply_markup=build_timeframes_kb(asset_code))
        except Exception:
            pass

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("tf:"))
    def on_signal_timeframe(call: types.CallbackQuery):
        uid = call.from_user.id
        user = db.get_user_by_telegram_id(uid)
        if not user or not user.get("premium_active"):
            try:
                bot.answer_callback_query(call.id, "Premium required")
            except Exception:
                pass
            return
        _, asset_code, tf = call.data.split(":", 2)
        code_to_pair = {
            "BTCUSDT": "BTC/USDT",
            "ETHUSDT": "ETH/USDT",
            "EURUSD": "EUR/USD",
            "GBPJPY": "GBP/JPY",
            "GOLD": "GOLD",
            "NASDAQ": "NASDAQ",
        }
        pair = code_to_pair.get(asset_code, asset_code)
        quota = db.consume_signal_by_telegram_id(uid)
        if not quota.get("ok"):
            msg = (
                "❗ Daily signal limit reached and no credits left.\n"
                "Each extra signal costs ₹150. Contact Support to top-up credits: "
                f"{utils.escape_html(SUPPORT_CONTACT)}"
            )
            utils.send_safe(bot, call.message.chat.id, msg)
            try:
                bot.answer_callback_query(call.id)
            except Exception:
                pass
            return
        text = utils.generate_smart_signal(pair, tf)
        footer = (
            f"\nRemaining today: {max(quota.get('daily_limit',0)-quota.get('used_today',0),0)} · "
            f"Credits: {quota.get('credits',0)} ({'daily' if quota.get('source')=='daily' else 'credit'} used)"
        )
        text = text + "\n" + footer
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        utils.send_safe(bot, call.message.chat.id, text)

    # ----- Admin: signal credits and limit -----
    @app.post("/admin/add_credits")
    @ui_login_required
    def admin_add_credits():
        ident = (request.form.get("ident") or "").strip()
        count = int(request.form.get("count") or 0)
        if not ident or count == 0:
            flash("Provide ident and non-zero count", "warning")
            return redirect(url_for("admin_users", q=ident))
        user = db.resolve_user_by_ident(ident)
        if not user:
            flash("User not found", "danger")
            return redirect(url_for("admin_users", q=ident))
        db.add_signal_credits_by_user_id(user["id"], count)
        db.log_admin("add_credits", {"user_id": user["id"], "count": count}, performed_by="panel")
        flash("Credits updated", "success")
        return redirect(url_for("admin_users", q=ident))

    @app.post("/admin/set_limit")
    @ui_login_required
    def admin_set_limit():
        ident = (request.form.get("ident") or "").strip()
        limit = int(request.form.get("limit") or 0)
        if not ident or limit <= 0:
            flash("Provide ident and positive limit", "warning")
            return redirect(url_for("admin_users", q=ident))
        user = db.resolve_user_by_ident(ident)
        if not user:
            flash("User not found", "danger")
            return redirect(url_for("admin_users", q=ident))
        db.set_signal_limit_by_user_id(user["id"], limit)
        db.log_admin("set_limit", {"user_id": user["id"], "limit": limit}, performed_by="panel")
        flash("Daily limit updated", "success")
        return redirect(url_for("admin_users", q=ident))

    @app.post("/api/add_credits")
    @require_admin
    def api_add_credits():
        d = request.get_json(silent=True) or {}
        ident = (d.get("ident") or "").strip()
        count = int(d.get("count") or 0)
        if not ident or count == 0:
            return jsonify({"ok": False, "error": "bad_request"}), 400
        user = db.resolve_user_by_ident(ident)
        if not user:
            return jsonify({"ok": False, "error": "user_not_found"}), 404
        db.add_signal_credits_by_user_id(user["id"], count)
        db.log_admin("add_credits", {"user_id": user["id"], "count": count}, performed_by="api")
        return jsonify({"ok": True})

    @app.post("/api/set_limit")
    @require_admin
    def api_set_limit():
        d = request.get_json(silent=True) or {}
        ident = (d.get("ident") or "").strip()
        limit = int(d.get("limit") or 0)
        if not ident or limit <= 0:
            return jsonify({"ok": False, "error": "bad_request"}), 400
        user = db.resolve_user_by_ident(ident)
        if not user:
            return jsonify({"ok": False, "error": "user_not_found"}), 404
        db.set_signal_limit_by_user_id(user["id"], limit)
        db.log_admin("set_limit", {"user_id": user["id"], "limit": limit}, performed_by="api")
        return jsonify({"ok": True})

@app.get("/api/stats")
@require_admin
def api_stats():
    return jsonify({"ok": True, "stats": db.get_stats()})

@app.get("/api/users")
@require_admin
def api_users():
    return jsonify({"ok": True, "items": db.search_users(request.args.get("q",""))})

@app.post("/api/grant")
@require_admin
def api_grant():
    d = request.get_json(silent=True) or {}
    ident, days = (d.get("ident") or "").strip(), int(d.get("days") or 0)
    if not ident or days <= 0: return jsonify({"ok": False, "error": "bad_request"}), 400
    user = db.resolve_user_by_ident(ident)
    if not user: return jsonify({"ok": False, "error": "user_not_found"}), 404
    new_exp = db.grant_premium_by_user_id(user["id"], days)
    db.log_admin("grant", {"user_id": user["id"], "ident": ident, "days": days}, performed_by="api")
    if bot: utils.send_safe(bot, user["telegram_id"], f"🎉 Premium +{days}d. New expiry: <b>{utils.format_ts_iso(new_exp)}</b>")
    return jsonify({"ok": True, "new_expires_at": utils.to_iso(new_exp)})

@app.post("/api/revoke")
@require_admin
def api_revoke():
    d = request.get_json(silent=True) or {}
    ident = (d.get("ident") or "").strip()
    if not ident: return jsonify({"ok": False, "error": "bad_request"}), 400
    user = db.resolve_user_by_ident(ident)
    if not user: return jsonify({"ok": False, "error": "user_not_found"}), 404
    db.revoke_premium_by_user_id(user["id"])
    db.log_admin("revoke", {"user_id": user["id"], "ident": ident}, performed_by="api")
    if bot: utils.send_safe(bot, user["telegram_id"], "⚠️ Your premium has been revoked.")
    return jsonify({"ok": True})

@app.post("/api/message")
@require_admin
def api_message():
    d = request.get_json(silent=True) or {}
    ident, text = (d.get("ident") or "").strip(), (d.get("text") or "").strip()
    if not ident or not text: return jsonify({"ok": False, "error": "bad_request"}), 400
    user = db.resolve_user_by_ident(ident)
    if not user: return jsonify({"ok": False, "error": "user_not_found"}), 404
    sent = 1 if (bot and utils.send_safe(bot, user["telegram_id"], text)) else 0
    db.log_admin("message", {"user_id": user["id"], "ident": ident, "text_len": len(text)}, performed_by="api")
    return jsonify({"ok": True, "sent": sent})

@app.post("/api/broadcast")
@require_admin
def api_broadcast():
    d = request.get_json(silent=True) or {}
    text = (d.get("text") or "").strip()
    premium_only = bool(d.get("premium_only", False))
    if not text: return jsonify({"ok": False, "error": "bad_request"}), 400
    users = db.list_users_for_broadcast(premium_only=premium_only)
    sent = 0
    for u in users:
        if bot and utils.send_safe(bot, u["telegram_id"], text): sent += 1
    db.log_admin("broadcast", {"premium_only": premium_only, "text_len": len(text), "count": len(users)}, performed_by="api")
    return jsonify({"ok": True, "attempted": len(users), "sent": sent})

@app.post("/api/cron")
@require_admin
def api_cron():
    result = utils.run_cron(db, bot)
    db.log_admin("cron", result, performed_by="api")
    return jsonify({"ok": True, **result})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
