import os
from datetime import date, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import telebot

from database import Database

load_dotenv()

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "*")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": FRONTEND_ORIGIN if FRONTEND_ORIGIN else "*"}})

db = Database()
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown") if BOT_TOKEN else None


def require_key(req):
    key = req.headers.get("x-admin-key", "")
    if not ADMIN_API_KEY or key != ADMIN_API_KEY:
        return False
    return True

def require_key_header_or_query(req):
    if require_key(req):
        return True
    qkey = (req.args.get("key") or "").strip()
    return bool(ADMIN_API_KEY and qkey == ADMIN_API_KEY)


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.get("/health/db")
def health_db():
    try:
        total = db.stats_total_users()
        return jsonify({"ok": True, "total_users": int(total)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/stats")
def api_stats():
    if not require_key(request):
        return jsonify({"error": "unauthorized"}), 401
    data = {
        "total_users": db.stats_total_users(),
        "active_premium": db.stats_active_premium(),
        "new_signups_today": db.stats_new_signups_today(),
        "logs": db.recent_admin_logs(20),
    }
    return jsonify(data)


@app.get("/api/users")
def api_users():
    if not require_key(request):
        return jsonify({"error": "unauthorized"}), 401
    q = (request.args.get("q") or "").strip()
    users = db.search_users(q) if q else db.list_users()
    return jsonify({"users": users})


@app.post("/api/grant")
def api_grant():
    if not require_key(request):
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    ident = (payload.get("ident") or "").strip()
    days = int(payload.get("days") or 30)
    if not ident:
        return jsonify({"error": "ident required"}), 400
    target = ident
    if ident.isdigit():
        telegram_id = int(ident)
        db.grant_premium(telegram_id, days)
    else:
        u = db.get_user_by_email(ident)
        if not u:
            return jsonify({"error": "user not found"}), 404
        telegram_id = int(u.get("telegram_id") or 0)
        if not telegram_id:
            return jsonify({"error": "user has no telegram id"}), 400
        db.grant_premium(telegram_id, days)
    db.admin_log("api", f"grant_premium_{days}", str(target))
    return jsonify({"ok": True})


@app.post("/api/revoke")
def api_revoke():
    if not require_key(request):
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    ident = (payload.get("ident") or "").strip()
    if not ident:
        return jsonify({"error": "ident required"}), 400
    telegram_id = None
    if ident.isdigit():
        telegram_id = int(ident)
    else:
        u = db.get_user_by_email(ident)
        if u:
            telegram_id = int(u.get("telegram_id") or 0)
    if not telegram_id:
        return jsonify({"error": "user not found"}), 404
    db.revoke_premium(telegram_id)
    db.admin_log("api", "revoke_premium", str(ident))
    return jsonify({"ok": True})


@app.post("/api/message")
def api_message():
    if not require_key(request):
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    ident = (payload.get("ident") or "").strip()
    text = (payload.get("text") or "").strip()
    if not ident or not text:
        return jsonify({"error": "ident and text required"}), 400
    telegram_id = None
    if ident.isdigit():
        telegram_id = int(ident)
    else:
        u = db.get_user_by_email(ident)
        if u:
            telegram_id = int(u.get("telegram_id") or 0)
    if not telegram_id:
        return jsonify({"error": "user not found"}), 404
    if not bot:
        return jsonify({"error": "bot not configured"}), 500
    try:
        bot.send_message(telegram_id, text)
        db.admin_log("api", "send_message", str(telegram_id))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/api/broadcast")
def api_broadcast():
    if not require_key(request):
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    text = (payload.get("text") or "").strip()
    if not text:
        return jsonify({"error": "text required"}), 400
    ids = db.get_premium_user_ids()
    sent = 0
    if not bot:
        return jsonify({"error": "bot not configured"}), 500
    for uid in ids:
        try:
            bot.send_message(uid, text)
            sent += 1
        except Exception:
            pass
    db.admin_log("api", "broadcast_premium", f"{sent}/{len(ids)}")
    return jsonify({"ok": True, "sent": sent, "total": len(ids)})


@app.post("/api/cron")
def api_cron():
    # Allow admin key via header or ?key= for UptimeRobot
    if not require_key_header_or_query(request):
        return jsonify({"error": "unauthorized"}), 401
    summary = {"notices": 0, "expired": 0}
    today = date.today()
    # 3-day and 1-day reminders
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
                if bot:
                    bot.send_message(uid, f"⏰ Reminder: Your premium expires in {delta} day(s). Renew to keep access to signals.")
                db.record_notice(uid, action)
                summary["notices"] += 1
            except Exception:
                pass
    # Mark expired
    expired = db.users_expired_before(today, 1000)
    for u in expired:
        uid = u.get("telegram_id")
        if not uid:
            continue
        try:
            db.set_premium_status(uid, False)
            if bot:
                bot.send_message(uid, "❗ Your premium has expired. Use /premium to renew.")
            db.record_notice(uid, "notice_expired")
            summary["expired"] += 1
        except Exception:
            pass
    return jsonify({"ok": True, **summary})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
