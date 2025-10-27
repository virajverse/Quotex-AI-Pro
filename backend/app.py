import os
import logging
import threading
import mimetypes
from datetime import datetime, timezone, timedelta
from functools import wraps

from flask import Flask, request, jsonify, render_template, redirect, url_for, session, flash, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
import shutil
import io
import tempfile

import telebot
from telebot import types
import json
from urllib.parse import quote
from typing import Optional

try:
    from dotenv import load_dotenv
    # Load default .env
    load_dotenv()
    # Also load .env.local if present (override values)
    try:
        load_dotenv(".env.local", override=True)
    except Exception:
        pass
except Exception:
    pass

# Database backend: SQLite only (Supabase/Postgres removed)
from . import utils
from . import sqlite_db as db

# Initialize/seed if available
try:
    if hasattr(db, 'init_db'):
        db.init_db()
    if hasattr(db, 'ensure_default_products'):
        db.ensure_default_products()
except Exception:
    pass

utils.setup_logger()
logger = logging.getLogger("app")

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": os.getenv("FRONTEND_ORIGIN", "*")}})

FREE_SAMPLES: dict[int, str] = {}

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "").strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
SECRET_KEY = os.getenv("SECRET_KEY", "").strip()
SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@support").strip()
REQUIRED_CHANNEL_URL = os.getenv("REQUIRED_CHANNEL_URL", "https://t.me/QuotexAI_Pro").strip()
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "").strip()

# Session secret key for Admin Panel
if SECRET_KEY:
    app.secret_key = SECRET_KEY
else:
    app.secret_key = os.urandom(24)
    logging.getLogger("app").warning("SECRET_KEY not set; using random key (sessions reset on restart)")

bot = telebot.TeleBot(
    BOT_TOKEN,
    parse_mode="HTML",
    threaded=True,
    disable_web_page_preview=True,
    num_threads=int(os.getenv("BOT_THREADS", "4")) if hasattr(telebot, 'apihelper') or True else None,
) if BOT_TOKEN else None
if bot:
    if WEBHOOK_BASE_URL:
        try:
            bot.remove_webhook()
            bot.set_webhook(url=f"{WEBHOOK_BASE_URL}/bot/{BOT_TOKEN}", drop_pending_updates=True)
        except Exception:
            logger.exception("Failed to set webhook")
    else:
        # Ensure any old webhook is removed when running in polling mode
        try:
            bot.remove_webhook()
        except Exception:
            pass
        # Local/dev: start background polling so inline buttons work without a public webhook
        def _polling():
            try:
                bot.infinity_polling(skip_pending=True, timeout=10)
            except Exception:
                logger.exception("Polling failed")
        try:
            threading.Thread(target=_polling, name="bot-polling", daemon=True).start()
        except Exception:
            logger.exception("Failed to start polling thread")

    # ----- Channel membership gate -----
    def _parse_channel_chat_id() -> Optional[str]:
        uname = REQUIRED_CHANNEL
        if not uname and REQUIRED_CHANNEL_URL:
            try:
                if "t.me/" in REQUIRED_CHANNEL_URL:
                    seg = REQUIRED_CHANNEL_URL.rsplit("/", 1)[-1].strip()
                    if seg:
                        uname = ("@" + seg) if not seg.startswith("@") else seg
            except Exception:
                uname = None
        return uname or None

    def _join_channel_url() -> Optional[str]:
        if REQUIRED_CHANNEL_URL:
            return REQUIRED_CHANNEL_URL
        cid = _parse_channel_chat_id()
        if cid and cid.startswith("@"):
            return f"https://t.me/{cid[1:]}"
        return None

    def _build_join_kb():
        kb = types.InlineKeyboardMarkup(row_width=1)
        url = _join_channel_url() or "https://t.me/"
        kb.add(types.InlineKeyboardButton("🔔 Join Channel", url=url))
        kb.add(types.InlineKeyboardButton("✅ I've Joined", callback_data="chk:joined"))
        return kb

    def _is_channel_member(uid: int) -> Optional[bool]:
        chat_id = _parse_channel_chat_id()
        if not chat_id:
            return True
        try:
            cm = bot.get_chat_member(chat_id, uid)
            st = getattr(cm, 'status', None) or (cm.status if hasattr(cm, 'status') else None)
            return st in ("member", "administrator", "creator")
        except Exception:
            return False

    def _require_channel(chat_id: int, uid: int) -> bool:
        ok = _is_channel_member(uid)
        if ok:
            return True
        try:
            bot.send_message(chat_id, "📢 Please join our channel to use this bot:", reply_markup=_build_join_kb())
        except Exception:
            pass
        return False

@app.get("/favicon.ico")
@app.get("/favicon.png")
@app.get("/apple-touch-icon.png")
def favicon_route():
    try:
        name = request.path.rsplit("/", 1)[-1]
        assets_dir = os.path.join(os.path.dirname(__file__), "assets")
        # Try exact name, then sensible fallbacks
        candidates = [
            os.path.join(assets_dir, name),
            os.path.join(assets_dir, "favicon.ico"),
            os.path.join(assets_dir, "favicon.png"),
            os.path.join(assets_dir, "apple-touch-icon.png"),
        ]
        for p in candidates:
            if os.path.exists(p):
                mt = "image/x-icon" if p.endswith(".ico") else "image/png"
                return send_file(p, mimetype=mt, cache_timeout=86400)
    except Exception:
        pass
    return ("", 404)

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
    credits = int(request.form.get("credits") or 0)
    if not ident or (days <= 0 and credits == 0):
        flash("Provide ident and positive days and/or credits", "warning")
        return redirect(url_for("admin_users", q=ident))
    user = db.resolve_user_by_ident(ident)
    if not user:
        flash("User not found", "danger")
        return redirect(url_for("admin_users", q=ident))
    new_exp = None
    if days > 0:
        new_exp = db.grant_premium_by_user_id(user["id"], days)
    if credits != 0:
        try:
            db.add_signal_credits_by_user_id(user["id"], credits)
        except Exception:
            pass
    db.log_admin("grant", {"user_id": user["id"], "ident": ident, "days": days, "credits": credits}, performed_by="panel")
    if bot:
        parts = []
        if days > 0:
            parts.append(f"Premium +{days}d. New expiry: <b>{utils.format_ts_iso(new_exp)}</b>")
        if credits != 0:
            parts.append(f"Signal credits {'+' if credits>0 else ''}{credits}")
        if parts:
            utils.send_safe(bot, user["telegram_id"], "🎉 " + " \u2022 ".join(parts))
    flash("Updated user: " + ("premium " if days>0 else "") + ("and " if days>0 and credits!=0 else "") + ("credits" if credits!=0 else ""), "success")
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
    keys = [
        "UI_HIDE_SIGNUP","UI_HIDE_LOGIN","UI_HIDE_PROFILE","UI_HIDE_GET_STARTED","UI_HIDE_HOW",
        "UI_HIDE_LIVE_SIGNALS","UI_HIDE_TOOLS","UI_HIDE_PERF24H","UI_HIDE_HOURS","UI_HIDE_PLAN",
        "UI_HIDE_SUPPORT","UI_HIDE_DISCLAIMER","UI_HIDE_SELECT_PLAN"
    ]
    toggles = {}
    for k in keys:
        try:
            v = db.get_setting(k)
            toggles[k] = str(v).lower() in ("1","true","yes","on")
        except Exception:
            toggles[k] = False
    return render_template("admin/broadcast.html", toggles=toggles)


@app.post("/admin/broadcast")
@ui_login_required
def admin_broadcast_post():
    text = (request.form.get("text") or "").strip()
    premium_only = bool(request.form.get("premium_only"))
    # Save UI toggle settings
    keys = [
        "UI_HIDE_SIGNUP","UI_HIDE_LOGIN","UI_HIDE_PROFILE","UI_HIDE_GET_STARTED","UI_HIDE_HOW",
        "UI_HIDE_LIVE_SIGNALS","UI_HIDE_TOOLS","UI_HIDE_PERF24H","UI_HIDE_HOURS","UI_HIDE_PLAN",
        "UI_HIDE_SUPPORT","UI_HIDE_DISCLAIMER","UI_HIDE_SELECT_PLAN"
    ]
    for k in keys:
        try:
            db.set_setting(k, "1" if request.form.get(k) else "0")
        except Exception:
            pass
    # Handle image upload (JPG)
    file = None
    try:
        file = request.files.get("image")
    except Exception:
        file = None
    img_bytes = None
    if file and getattr(file, 'filename', ''):
        fname = (file.filename or '').lower()
        ctype = (file.mimetype or '').lower()
        if fname.endswith('.jpg') or fname.endswith('.jpeg') or 'jpeg' in ctype:
            try:
                data = file.read()
                if data:
                    img_bytes = data
            except Exception:
                img_bytes = None
        else:
            flash("Only JPG/JPEG images are allowed", "warning")
            return redirect(url_for("admin_broadcast_page"))
    # If neither text nor image, just report settings saved
    if not text and not img_bytes:
        flash("Settings updated", "success")
        return redirect(url_for("admin_broadcast_page"))
    # Send broadcast
    users = db.list_users_for_broadcast(premium_only=premium_only)
    sent = 0
    for u in users:
        if not bot:
            continue
        try:
            if img_bytes:
                bio = io.BytesIO(img_bytes)
                bio.name = "broadcast.jpg"
                bot.send_photo(u["telegram_id"], bio, caption=(text or None))
                sent += 1
            else:
                if utils.send_safe(bot, u["telegram_id"], text):
                    sent += 1
        except Exception:
            continue
    db.log_admin("broadcast", {"premium_only": premium_only, "text_len": len(text), "count": len(users), "sent": sent, "has_image": bool(img_bytes)}, performed_by="panel")
    flash(f"Broadcast sent: {sent}/{len(users)}", "success")
    return redirect(url_for("admin_broadcast_page"))


@app.get("/admin/branding")
@ui_login_required
def admin_branding_get():
    assets_dir = os.path.join(os.path.dirname(__file__), "assets")
    fav_ico = os.path.join(assets_dir, "favicon.ico")
    fav_png = os.path.join(assets_dir, "favicon.png")
    touch_png = os.path.join(assets_dir, "apple-touch-icon.png")
    ctx = {
        "has_favicon": os.path.exists(fav_ico) or os.path.exists(fav_png),
        "has_touch": os.path.exists(touch_png),
    }
    return render_template("admin/branding.html", **ctx)


@app.post("/admin/branding")
@ui_login_required
def admin_branding_post():
    assets_dir = os.path.join(os.path.dirname(__file__), "assets")
    try:
        os.makedirs(assets_dir, exist_ok=True)
    except Exception:
        pass
    # Handle favicon upload (.ico or .png)
    fav = None
    touch = None
    try:
        fav = request.files.get("favicon")
    except Exception:
        fav = None
    try:
        touch = request.files.get("apple_touch")
    except Exception:
        touch = None
    saved = []
    if fav and getattr(fav, 'filename', ''):
        fname = secure_filename(fav.filename)
        lower = fname.lower()
        if lower.endswith('.ico'):
            path = os.path.join(assets_dir, 'favicon.ico')
            fav.save(path)
            saved.append('favicon.ico')
        elif lower.endswith('.png'):
            path = os.path.join(assets_dir, 'favicon.png')
            fav.save(path)
            saved.append('favicon.png')
        else:
            flash("Favicon must be .ico or .png", "warning")
    if touch and getattr(touch, 'filename', ''):
        fname = secure_filename(touch.filename)
        lower = fname.lower()
        if lower.endswith('.png'):
            path = os.path.join(assets_dir, 'apple-touch-icon.png')
            touch.save(path)
            saved.append('apple-touch-icon.png')
        else:
            flash("Apple Touch Icon must be .png", "warning")
    # Auto-copy favicon.png to apple-touch-icon.png if not provided
    try:
        fav_png = os.path.join(assets_dir, 'favicon.png')
        touch_png = os.path.join(assets_dir, 'apple-touch-icon.png')
        if os.path.exists(fav_png) and not os.path.exists(touch_png) and 'apple-touch-icon.png' not in saved:
            import shutil as _sh
            _sh.copyfile(fav_png, touch_png)
            saved.append('apple-touch-icon.png (copied)')
    except Exception:
        pass
    if saved:
        flash("Saved: " + ", ".join(saved), "success")
        db.log_admin("branding", {"saved": saved}, performed_by="panel")
    else:
        flash("No files uploaded", "warning")
    return redirect(url_for("admin_branding_get"))


# ----- Admin: Verifications / Orders / Products -----
@app.get("/admin/verifications")
@ui_login_required
def admin_verifications():
    status = (request.args.get("status") or "").strip() or None
    method = (request.args.get("method") or "").strip() or None
    items = []
    try:
        items = db.list_verifications(status=status, method=method, limit=200)
    except Exception:
        logger.exception("list_verifications failed")
    return render_template("admin/verifications.html", items=items, status=status or "", method=method or "")


@app.get("/admin/verification/<int:vid>")
@ui_login_required
def admin_verification_detail(vid: int):
    v = db.get_verification(vid)
    if not v:
        flash("Verification not found", "danger")
        return redirect(url_for("admin_verifications"))
    user = None
    order = None
    product = None
    try:
        user = db.get_user_by_id(v.get("user_id")) if hasattr(db, "get_user_by_id") else None
    except Exception:
        user = None
    try:
        if v.get("order_id") and hasattr(db, "get_order"):
            order = db.get_order(v.get("order_id"))
            if order and order.get("product_id") and hasattr(db, "get_product"):
                product = db.get_product(order.get("product_id"))
    except Exception:
        pass
    return render_template("admin/verification_detail.html", v=v, user=user, order=order, product=product)

@app.get("/admin/verification/<int:vid>/receipt")
@ui_login_required
def admin_verification_receipt(vid: int):
    v = db.get_verification(vid)
    if not v:
        return "Not found", 404
    file_id = None
    # Prefer order attachment
    try:
        if v.get("order_id"):
            order = db.get_order(v.get("order_id"))
            if order and order.get("receipt_file_id"):
                file_id = order.get("receipt_file_id")
    except Exception:
        pass
    # Fallback: parse from verification.request_data
    if not file_id:
        raw = v.get("request_data")
        if raw:
            try:
                data = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(data, dict):
                    file_id = data.get("file_id")
            except Exception:
                pass
    # Final fallback: latest user receipt
    if not file_id:
        try:
            file_id = db.get_latest_user_receipt_file_id(v.get("user_id"))
        except Exception:
            file_id = None
    if not file_id:
        return "Receipt not found", 404
    if not bot:
        return "Bot unavailable", 503
    try:
        f = bot.get_file(file_id)
        data = bot.download_file(f.file_path)
        # Guess mime from file path extension
        p = (getattr(f, 'file_path', '') or '')
        filename = os.path.basename(p) or 'receipt'
        guessed, _ = mimetypes.guess_type(filename)
        mime = guessed or 'application/octet-stream'
        # If unknown, try detect by content for PDF/images
        if mime == 'application/octet-stream' or not mime:
            head = data[:16] if isinstance(data, (bytes, bytearray)) else b''
            try:
                if head.startswith(b'%PDF'):
                    mime = 'application/pdf'
                elif head[:3] == b'\xff\xd8\xff':
                    mime = 'image/jpeg'
                elif head.startswith(b'\x89PNG\r\n\x1a\n'):
                    mime = 'image/png'
                elif head.startswith(b'GIF87a') or head.startswith(b'GIF89a'):
                    mime = 'image/gif'
                elif head.startswith(b'BM'):
                    mime = 'image/bmp'
                elif head.startswith(b'II*\x00') or head.startswith(b'MM\x00*'):
                    mime = 'image/tiff'
                elif head[:4] == b'RIFF' and (data[8:12] if len(data) >= 12 else b'') == b'WEBP':
                    mime = 'image/webp'
            except Exception:
                pass
        resp = send_file(io.BytesIO(data), mimetype=mime)
        try:
            disp = 'inline'
            resp.headers['Content-Disposition'] = f"{disp}; filename=\"{filename}\""
        except Exception:
            pass
        return resp
    except Exception:
        logger.exception("Failed to fetch receipt file")
        return "Failed to fetch receipt", 500


@app.post("/admin/verification/<int:vid>/approve")
@ui_login_required
def admin_verification_approve(vid: int):
    days = int(request.form.get("days") or 0)
    credits = int((request.form.get("credits") or 0))
    v = db.get_verification(vid)
    if not v:
        flash("Verification not found", "danger")
        return redirect(url_for("admin_verifications"))
    user = db.get_user_by_id(v.get("user_id")) if hasattr(db, "get_user_by_id") else None
    if not user:
        flash("User not found", "danger")
        return redirect(url_for("admin_verifications"))
    # if order linked and no days specified, use product days
    if days <= 0 and v.get("order_id") and hasattr(db, "get_order") and hasattr(db, "get_product"):
        try:
            order = db.get_order(v.get("order_id"))
            prod = db.get_product(order.get("product_id")) if order else None
            if prod and prod.get("days"):
                days = int(prod.get("days"))
        except Exception:
            pass
    if days <= 0:
        days = 30
    try:
        new_exp = db.grant_premium_by_user_id(user["id"], days)
        if credits != 0:
            try:
                db.add_signal_credits_by_user_id(user["id"], credits)
            except Exception:
                pass
        if hasattr(db, "set_order_status") and v.get("order_id"):
            try:
                db.set_order_status(v.get("order_id"), "approved")
            except Exception:
                pass
        if hasattr(db, "set_verification_status"):
            db.set_verification_status(vid, "approved", notes=f"Approved {days}d; credits {credits}")
        db.log_admin("verification_approve", {"verification_id": vid, "user_id": user["id"], "days": days, "credits": credits}, performed_by="panel")
        if bot:
            parts = [f"Premium +{days}d. New expiry: <b>{utils.format_ts_iso(new_exp)}</b>"]
            if credits != 0:
                parts.append(f"Signal credits {'+' if credits>0 else ''}{credits}")
            utils.send_safe(bot, user["telegram_id"], "✅ Payment approved. " + " \u2022 ".join(parts))
        flash("Verification approved and premium granted", "success")
    except Exception:
        logger.exception("approve failed")
        flash("Approve failed", "danger")
    return redirect(url_for("admin_verification_detail", vid=vid))


@app.post("/admin/verification/<int:vid>/reject")
@ui_login_required
def admin_verification_reject(vid: int):
    reason = (request.form.get("reason") or "").strip()
    v = db.get_verification(vid)
    if not v:
        flash("Verification not found", "danger")
        return redirect(url_for("admin_verifications"))
    try:
        if hasattr(db, "set_verification_status"):
            db.set_verification_status(vid, "rejected", notes=reason or None)
        if hasattr(db, "set_order_status") and v.get("order_id"):
            try:
                db.set_order_status(v.get("order_id"), "rejected")
            except Exception:
                pass
        db.log_admin("verification_reject", {"verification_id": vid, "reason": reason}, performed_by="panel")
        flash("Verification rejected", "success")
    except Exception:
        logger.exception("reject failed")
        flash("Reject failed", "danger")
    return redirect(url_for("admin_verification_detail", vid=vid))


@app.get("/admin/orders")
@ui_login_required
def admin_orders():
    status = (request.args.get("status") or "").strip() or None
    items = []
    try:
        items = db.list_orders(status=status, limit=200) if hasattr(db, "list_orders") else []
    except Exception:
        logger.exception("list_orders failed")
    return render_template("admin/orders.html", items=items, status=status or "")


@app.get("/admin/products")
@ui_login_required
def admin_products():
    items = []
    try:
        items = db.list_products(active_only=False)
    except Exception:
        logger.exception("list_products failed")
    return render_template("admin/products.html", items=items)


@app.post("/admin/products/create")
@ui_login_required
def admin_products_create():
    name = (request.form.get("name") or "").strip()
    days = int(request.form.get("days") or 0)
    price_inr = request.form.get("price_inr")
    price_usdt = request.form.get("price_usdt")
    desc = (request.form.get("description") or "").strip()
    if not name or days <= 0:
        flash("Provide name and positive days", "warning")
        return redirect(url_for("admin_products"))
    try:
        p_inr = float(price_inr) if price_inr else None
        p_usdt = float(price_usdt) if price_usdt else None
    except Exception:
        p_inr, p_usdt = None, None
    try:
        db.create_product(name=name, days=days, price_inr=p_inr, price_usdt=p_usdt, description=desc)
        db.log_admin("product_create", {"name": name, "days": days}, performed_by="panel")
        flash("Product created", "success")
    except Exception:
        logger.exception("create_product failed")
        flash("Create failed", "danger")
    return redirect(url_for("admin_products"))


@app.post("/admin/products/update")
@ui_login_required
def admin_products_update():
    pid = int(request.form.get("id") or 0)
    if pid <= 0:
        flash("Invalid product id", "warning")
        return redirect(url_for("admin_products"))
    fields = {}
    for key in ("name", "description"):
        val = request.form.get(key)
        if val is not None and val.strip() != "":
            fields[key] = val.strip()
    for key in ("days", "price_inr", "price_usdt"):
        val = request.form.get(key)
        if val is not None and val != "":
            try:
                fields[key] = int(val) if key == "days" else float(val)
            except Exception:
                pass
    active = request.form.get("active")
    if active is not None:
        fields["active"] = (active == "1" or active.lower() == "true")
    try:
        db.update_product(pid, **fields)
        db.log_admin("product_update", {"id": pid, **fields}, performed_by="panel")
        flash("Product updated", "success")
    except Exception:
        logger.exception("update_product failed")
        flash("Update failed", "danger")
    return redirect(url_for("admin_products"))


@app.post("/admin/cron")
@ui_login_required
def admin_cron():
    result = utils.run_cron(db, bot)
    db.log_admin("cron", result, performed_by="panel")
    flash(
        f"Cron run: notices={result.get('notices')} expired={result.get('expired')} evaluated={result.get('evaluated')}",
        "success",
    )
    return redirect(url_for("admin_dashboard"))

@app.get("/admin/performance")
@ui_login_required
def admin_performance():
    try:
        report = utils.generate_24h_served_report(db)
    except Exception:
        report = "No data or evaluation failed."
    return render_template("admin/performance.html", report=report)

@app.get("/admin/db/download")
@ui_login_required
def admin_db_download():
    try:
        path = getattr(db, "DB_PATH", None)
        if not path or not os.path.exists(path):
            return send_file(io.BytesIO(b""), as_attachment=True, download_name="bot.db")
        return send_file(path, as_attachment=True, download_name=os.path.basename(path))
    except Exception:
        logger.exception("db download failed")
        flash("Download failed", "danger")
        return redirect(url_for("admin_dashboard"))

@app.post("/admin/db/upload")
@ui_login_required
def admin_db_upload():
    try:
        f = request.files.get("file")
        if not f or not f.filename:
            flash("Choose a file", "warning")
            return redirect(url_for("admin_dashboard"))
        fn = secure_filename(f.filename)
        dst = getattr(db, "DB_PATH", None)
        if not dst:
            flash("SQLite path unavailable", "danger")
            return redirect(url_for("admin_dashboard"))
        tmp = os.path.join(os.path.dirname(dst), fn + ".upload")
        f.save(tmp)
        ok = False
        try:
            with open(tmp, "rb") as fh:
                sig = fh.read(16)
                ok = sig.startswith(b"SQLite format 3")
        except Exception:
            ok = False
        if not ok:
            try:
                os.remove(tmp)
            except Exception:
                pass
            flash("Invalid SQLite file", "danger")
            return redirect(url_for("admin_dashboard"))
        try:
            if os.path.exists(dst):
                shutil.copy2(dst, dst + ".bak")
        except Exception:
            pass
        shutil.move(tmp, dst)
        flash("Database replaced", "success")
    except Exception:
        logger.exception("db upload failed")
        flash("Upload failed", "danger")
    return redirect(url_for("admin_dashboard"))

@app.route("/health", methods=["GET", "HEAD"])
@app.route("/health/", methods=["GET", "HEAD"])
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
    try:
        bot.set_my_commands([
            telebot.types.BotCommand("start", "Start / show menu"),
            telebot.types.BotCommand("menu", "Show menu"),
            telebot.types.BotCommand("signal", "Get live signals"),
            telebot.types.BotCommand("hours", "Market hours"),
            telebot.types.BotCommand("status", "Premium status"),
            telebot.types.BotCommand("pricing", "View pricing plans"),
            telebot.types.BotCommand("premium", "Payment options"),
            telebot.types.BotCommand("help", "Help"),
        ])
    except Exception:
        pass
    # ----- Main Menu UI -----
    def build_main_menu(user):
        kb = types.InlineKeyboardMarkup(row_width=2)
        premium = _user_has_premium(user)
        def _b(key: str) -> bool:
            try:
                v = db.get_setting(key)
                return str(v).lower() in ("1","true","yes","on")
            except Exception:
                return False
        # Auth row
        if not _b("UI_HIDE_SIGNUP") and not bool(user):
            kb.add(types.InlineKeyboardButton("✅ SIGN UP", callback_data="menu:signup"))
        if not _b("UI_HIDE_LOGIN") and not bool(user):
            kb.add(types.InlineKeyboardButton("🔑 LOGIN", callback_data="menu:login"))
        if not _b("UI_HIDE_PROFILE") and bool(user):
            kb.add(types.InlineKeyboardButton("👤 PROFILE", callback_data="menu:profile"))
        # Getting started / how
        row = []
        if not _b("UI_HIDE_GET_STARTED") and not premium:
            row.append(types.InlineKeyboardButton("🚀 GET STARTED", callback_data="menu:get_started"))
        if not _b("UI_HIDE_HOW"):
            row.append(types.InlineKeyboardButton("❓ HOW IT WORKS", callback_data="menu:how"))
        if row:
            kb.add(*row)
        # Main actions
        row = []
        if not _b("UI_HIDE_LIVE_SIGNALS"):
            row.append(types.InlineKeyboardButton("📈 LIVE SIGNALS", callback_data="menu:signals"))
        if not _b("UI_HIDE_TOOLS"):
            row.append(types.InlineKeyboardButton("📊 ANALYSIS TOOLS", callback_data="menu:tools"))
        if row:
            kb.add(*row)
        if not _b("UI_HIDE_PERF24H"):
            kb.add(types.InlineKeyboardButton("🔥 24H VIP PROFIT", callback_data="menu:perf24h"))
        row = []
        if not _b("UI_HIDE_HOURS"):
            row.append(types.InlineKeyboardButton("🕒 MARKET HOURS", callback_data="menu:hours"))
        if not _b("UI_HIDE_PLAN"):
            row.append(types.InlineKeyboardButton("📅 PLAN STATUS", callback_data="menu:plan"))
        if row:
            kb.add(*row)
        if not _b("UI_HIDE_SUPPORT"):
            kb.add(types.InlineKeyboardButton("💬 SUPPORT", callback_data="menu:support"))
        if not _b("UI_HIDE_DISCLAIMER"):
            kb.add(types.InlineKeyboardButton("⚠️ RISK DISCLAIMER", callback_data="menu:disclaimer"))
        return kb

    # --- Assets: OTC vs LIVE (user-provided FX list) ---
    PAIRS_BASE = [
        "USD/COP","USD/INR","USD/ARS","USD/BDT","USD/DZD","USD/BRL","GBP/USD","EUR/GBP","NZD/CAD",
        "USD/EGP","EUR/NZD","USD/IDR","CHF/JPY","USD/MXN","EUR/AUD","AUD/JPY","CAD/CHF","USD/CAD",
        "AUD/CAD","EUR/CHF","AUD/NZD","USD/NGN","NZD/JPY","AUD/USD","EUR/CAD","EUR/JPY","EUR/USD",
        "GBP/AUD","GBP/JPY","NZD/CHF","USD/CHF","USD/JPY","USD/PKR","USD/TRY","NZD/USD","USD/PHP",
        "CAD/JPY","GBP/CAD","USD/ZAR","EUR/SGD","GBP/CHF","GBP/NZD",
    ]

    def build_assets_kb():
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("OTC FX", callback_data="assets:otc"),
            types.InlineKeyboardButton("LIVE FX", callback_data="assets:live"),
        )
        kb.add(
            types.InlineKeyboardButton("🏠 Main Menu", callback_data="menu:root"),
            types.InlineKeyboardButton("👤 Profile", callback_data="menu:profile"),
        )
        return kb

    def build_assets_list_kb(category: str):
        kb = types.InlineKeyboardMarkup(row_width=2)
        for p in PAIRS_BASE:
            code = p.replace("/", "")
            label = f"{p} (OTC)" if category == "otc" else p
            cb = f"sig:{code}_OTC" if category == "otc" else f"sig:{code}"
            kb.add(types.InlineKeyboardButton(label, callback_data=cb))
        kb.add(types.InlineKeyboardButton("⬅️ Back", callback_data="back:assets"))
        return kb

    def build_timeframes_kb(asset_code: str):
        kb = types.InlineKeyboardMarkup(row_width=3)
        kb.add(
            types.InlineKeyboardButton("1m", callback_data=f"tf:{asset_code}:1m"),
            types.InlineKeyboardButton("3m", callback_data=f"tf:{asset_code}:3m"),
            types.InlineKeyboardButton("5m", callback_data=f"tf:{asset_code}:5m"),
        )
        kb.add(
            types.InlineKeyboardButton("⬅️ Assets", callback_data="back:assets"),
            types.InlineKeyboardButton("🏠 Main Menu", callback_data="menu:root"),
            types.InlineKeyboardButton("👤 Profile", callback_data="menu:profile"),
        )
        return kb

    def build_signal_nav_kb(asset_code: str):
        kb = types.InlineKeyboardMarkup(row_width=3)
        kb.add(
            types.InlineKeyboardButton("🔁 More", callback_data=f"sig:{asset_code}"),
            types.InlineKeyboardButton("⬅️ Assets", callback_data="back:assets"),
        )
        kb.add(
            types.InlineKeyboardButton("🏠 Main Menu", callback_data="menu:root"),
            types.InlineKeyboardButton("👤 Profile", callback_data="menu:profile"),
        )
        return kb

    def build_basic_nav_kb():
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("🏠 Main Menu", callback_data="menu:root"),
            types.InlineKeyboardButton("👤 Profile", callback_data="menu:profile"),
        )
        return kb

    def build_main_reply_kb(user):
        kb = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True, one_time_keyboard=False, selective=False)
        premium = _user_has_premium(user)
        def _b(key: str) -> bool:
            try:
                v = db.get_setting(key)
                return str(v).lower() in ("1","true","yes","on")
            except Exception:
                return False
        if not bool(user):
            row = []
            if not _b("UI_HIDE_SIGNUP"): row.append(types.KeyboardButton("✅ SIGN UP"))
            if not _b("UI_HIDE_LOGIN"): row.append(types.KeyboardButton("🔑 LOGIN"))
            if row: kb.add(*row)
        else:
            if not _b("UI_HIDE_PROFILE"):
                kb.add(types.KeyboardButton("👤 PROFILE"))
        row = []
        if not _b("UI_HIDE_GET_STARTED") and not premium: row.append(types.KeyboardButton("🚀 GET STARTED"))
        if not _b("UI_HIDE_HOW"): row.append(types.KeyboardButton("❓ HOW IT WORKS"))
        if row: kb.add(*row)
        row = []
        if not _b("UI_HIDE_LIVE_SIGNALS"): row.append(types.KeyboardButton("📈 LIVE SIGNALS"))
        if not _b("UI_HIDE_TOOLS"): row.append(types.KeyboardButton("📊 ANALYSIS TOOLS"))
        if row: kb.add(*row)
        if not _b("UI_HIDE_PERF24H"): kb.add(types.KeyboardButton("🔥 24H VIP PROFIT"))
        row = []
        if not _b("UI_HIDE_HOURS"): row.append(types.KeyboardButton("🕒 MARKET HOURS"))
        if not _b("UI_HIDE_PLAN"): row.append(types.KeyboardButton("📅 PLAN STATUS"))
        if row: kb.add(*row)
        if not _b("UI_HIDE_SELECT_PLAN"): kb.add(types.KeyboardButton("SELECT A PLAN"))
        if not _b("UI_HIDE_SUPPORT"): kb.add(types.KeyboardButton("💬 SUPPORT"))
        if not _b("UI_HIDE_DISCLAIMER"): kb.add(types.KeyboardButton("⚠️ RISK DISCLAIMER"))
        return kb

    def build_products_reply_kb():
        kb = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
        try:
            items = db.list_products(active_only=True)
        except Exception:
            items = []
        for p in items or []:
            label = f"{p.get('name')} — {p.get('days')}d"
            kb.add(types.KeyboardButton(label))
        kb.add(types.KeyboardButton("⬅️ back"), types.KeyboardButton("🏠 home"))
        return kb

    def pricing_message() -> str:
        try:
            items = db.list_products(active_only=True)
        except Exception:
            items = []
        if not items:
            return "Pricing unavailable right now."
        lines = [
            "💰 Pricing",
        ]
        for p in items:
            inr = p.get('price_inr')
            usdt = p.get('price_usdt')
            price_bits = []
            if inr is not None:
                try:
                    price_bits.append(f"₹{int(inr) if float(inr).is_integer() else inr}")
                except Exception:
                    price_bits.append(f"₹{inr}")
            if usdt is not None:
                price_bits.append(f"${usdt} USDT")
            price = ", ".join(price_bits) if price_bits else "N/A"
            lines.append(f"• {p.get('name')} — {p.get('days')}d: {price}")
        lines.append("")
        lines.append("Select a plan below to continue.")
        return "\n".join(lines)

    def send_pricing_card(chat_id: int):
        stop_loading = start_chat_action(chat_id, "typing")
        try:
            try:
                items = db.list_products(active_only=True)
            except Exception:
                items = []
            if not items:
                try:
                    bot.send_message(chat_id, "Pricing unavailable right now.")
                except Exception:
                    pass
                return
            img = None
            try:
                from PIL import Image, ImageDraw, ImageFont, ImageFilter
                w, h = 1000, 120 + 180 * max(1, len(items))
                bg = Image.new("RGB", (w, h), (10, 12, 22))
                draw = ImageDraw.Draw(bg, "RGBA")
                for i in range(0, h, 4):
                    c = 12 + int(8 * (i / h))
                    draw.line([(0, i), (w, i)], fill=(c, c, c + 6, 255))
                accent = (0, 188, 255)
                title = "Pricing"
                try:
                    fp = os.getenv("FONT_PATH")
                    font_title = ImageFont.truetype(fp, 60) if fp else ImageFont.load_default()
                    font_sub = ImageFont.truetype(fp, 30) if fp else ImageFont.load_default()
                    font_card = ImageFont.truetype(fp, 36) if fp else ImageFont.load_default()
                    font_small = ImageFont.truetype(fp, 28) if fp else ImageFont.load_default()
                except Exception:
                    font_title = ImageFont.load_default(); font_sub = ImageFont.load_default(); font_card = ImageFont.load_default(); font_small = ImageFont.load_default()
                draw.rectangle([30, 30, w - 30, 110], outline=(30, 34, 56), width=2)
                draw.text((50, 50), title, fill=(240, 240, 255), font=font_title)
                draw.text((50, 100), "Select a plan below", fill=(160, 170, 190), font=font_sub)
                y = 150
                for p in items:
                    box_h = 150
                    draw.rectangle([30, y, w - 30, y + box_h], fill=(18, 20, 36), outline=(40, 45, 70), width=2)
                    draw.rectangle([30, y, 36, y + box_h], fill=accent)
                    name = f"{p.get('name')} — {p.get('days')}d"
                    inr = p.get('price_inr')
                    usdt = p.get('price_usdt')
                    price_bits = []
                    if inr is not None:
                        try:
                            price_bits.append(f"₹{int(inr) if float(inr).is_integer() else inr}")
                        except Exception:
                            price_bits.append(f"₹{inr}")
                    if usdt is not None:
                        price_bits.append(f"${usdt} USDT")
                    price = ", ".join(price_bits) if price_bits else "N/A"
                    draw.text((60, y + 30), name, fill=(235, 238, 255), font=font_card)
                    draw.text((60, y + 90), price, fill=(0, 200, 180), font=font_small)
                    y += box_h + 20
                img = bg
            except Exception:
                img = None
            if img is None:
                try:
                    bot.send_message(chat_id, pricing_message())
                except Exception:
                    pass
                return
            try:
                bio = io.BytesIO()
                img.save(bio, format="PNG")
                bio.seek(0)
                bot.send_photo(chat_id, photo=bio, caption="Select a plan below.")
            except Exception:
                try:
                    bot.send_message(chat_id, pricing_message())
                except Exception:
                    pass
        finally:
            try:
                stop_loading()
            except Exception:
                pass

    SIGNAL_LAST: dict[int, str] = {}
    ASSETS_STATE: dict[int, dict] = {}

    def build_assets_reply_kb():
        # Kept minimal; use inline keyboards for actual asset selection
        kb = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
        kb.add(types.KeyboardButton("🏠 HOME"))
        return kb

    def build_timeframes_reply_kb():
        kb = types.ReplyKeyboardMarkup(row_width=3, resize_keyboard=True)
        kb.add(types.KeyboardButton("1m"), types.KeyboardButton("3m"), types.KeyboardButton("5m"))
        kb.add(types.KeyboardButton("⬅️ BACK"), types.KeyboardButton("🏠 HOME"))
        return kb

    def _send_kb_quietly(chat_id: int, kb):
        try:
            # Send an invisible character and KEEP the message so the reply keyboard persists
            bot.send_message(chat_id, "\u2063", reply_markup=kb)
        except Exception:
            pass

    def start_chat_action(chat_id: int, action: str = "typing", interval: float = 4.0):
        stop = threading.Event()
        def run():
            try:
                while not stop.is_set():
                    try:
                        bot.send_chat_action(chat_id, action)
                    except Exception:
                        pass
                    stop.wait(interval)
            except Exception:
                pass
        threading.Thread(target=run, daemon=True).start()
        return stop.set

    def build_payment_kb():
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("✅ Verify UPI", callback_data="pay:verify_upi"),
            types.InlineKeyboardButton("✅ Verify USDT", callback_data="pay:verify_usdt"),
        )
        kb.add(
            types.InlineKeyboardButton("🏠 Main Menu", callback_data="menu:root"),
            types.InlineKeyboardButton("👤 Profile", callback_data="menu:profile"),
        )
        return kb

    def build_payment_reply_kb():
        kb = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
        kb.add(types.KeyboardButton("📷 Scan UPI"), types.KeyboardButton("✅ Verify USDT"))
        kb.add(types.KeyboardButton("🧾 Upload Receipt"), types.KeyboardButton("👁️ View Receipt"))
        kb.add(types.KeyboardButton("🏠 Main Menu"), types.KeyboardButton("👤 Profile"))
        return kb

    def build_quick_assets_reply_kb():
        kb = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
        kb.add(types.KeyboardButton("LIVE FX"))
        kb.add(types.KeyboardButton("🏠 Main Menu"), types.KeyboardButton("👤 Profile"))
        return kb

    def build_assets_reply_page_kb(category: str, page: int = 0, page_size: int = 10):
        pairs = PAIRS_BASE[:]
        start = max(page, 0) * page_size
        end = start + page_size
        page_pairs = pairs[start:end]
        kb = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
        # Title row via message, buttons are only pairs
        for i in range(0, len(page_pairs), 2):
            row = page_pairs[i:i+2]
            if len(row) == 2:
                kb.add(types.KeyboardButton(row[0]), types.KeyboardButton(row[1]))
            else:
                kb.add(types.KeyboardButton(row[0]))
        nav_left = "◀ Prev" if start > 0 else None
        nav_right = "Next ▶" if end < len(pairs) else None
        nav_row = []
        if nav_left:
            nav_row.append(types.KeyboardButton(nav_left))
        if nav_right:
            nav_row.append(types.KeyboardButton(nav_right))
        if nav_row:
            kb.add(*nav_row)
        kb.add(types.KeyboardButton("⬅️ Categories"))
        kb.add(types.KeyboardButton("🏠 Main Menu"), types.KeyboardButton("👤 Profile"))
        return kb

    def _show_assets_reply(chat_id: int, category: str, page: int = 0):
        try:
            _send_kb_quietly(chat_id, build_assets_reply_page_kb(category, page))
        except Exception:
            pass
        title = "Select OTC asset:" if category == "otc" else "Select LIVE asset:"
        try:
            bot.send_message(chat_id, title)
        except Exception:
            pass

    def build_upi_open_kb(upi_url: str):
        try:
            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.add(types.InlineKeyboardButton("🔗 Open UPI App", url=upi_url))
            return kb
        except Exception:
            return None

    def _upi_url(amount: Optional[float] = None, note: Optional[str] = None) -> Optional[str]:
        pa = (os.getenv("UPI_ID") or "").strip()
        if not pa:
            return None
        pn = (os.getenv("UPI_NAME") or os.getenv("BUSINESS_NAME") or "Payment").strip()
        pairs = [("pa", pa), ("pn", pn), ("cu", "INR")]
        if amount is not None:
            try:
                pairs.append(("am", f"{float(amount):.0f}" if float(amount).is_integer() else f"{float(amount):.2f}"))
            except Exception:
                pairs.append(("am", str(amount)))
        if note:
            pairs.append(("tn", note))
        q = "&".join([f"{k}={quote(str(v), safe='')}" for k, v in pairs])
        return f"upi://pay?{q}"

    UPI_QR_STORE = os.path.join(os.path.dirname(__file__), 'upi_qr.txt')

    def _read_qr_fid() -> Optional[str]:
        try:
            with open(UPI_QR_STORE, 'r', encoding='utf-8') as f:
                v = (f.read() or '').strip()
                return v or None
        except Exception:
            return None

    def _write_qr_fid(fid: str):
        try:
            with open(UPI_QR_STORE, 'w', encoding='utf-8') as f:
                f.write((fid or '').strip())
        except Exception:
            pass

    def send_upi_qr(chat_id: int, amount: Optional[float] = None, note: Optional[str] = None):
        upi_url = _upi_url(amount, note)
        caption = "Scan this UPI QR to pay. After payment, tap '🧾 Upload Receipt' to submit your screenshot."
        # Priority: file_id -> image URL -> generated QR URL -> fallback text
        fid = (os.getenv("UPI_QR_FILE_ID") or "").strip() or _read_qr_fid()
        img_url = (os.getenv("UPI_QR_IMAGE_URL") or "").strip()
        if fid:
            try:
                bot.send_photo(chat_id, fid, caption=caption)
                return
            except Exception:
                pass
        qr_from_upi = None
        if upi_url:
            try:
                qr_from_upi = f"https://quickchart.io/qr?text={quote(upi_url, safe='')}&margin=2&size=400"
            except Exception:
                qr_from_upi = None
        try:
            if img_url:
                bot.send_photo(chat_id, img_url, caption=caption)
                return
            if qr_from_upi:
                bot.send_photo(chat_id, qr_from_upi, caption=caption)
                return
        except Exception:
            pass
        # Fallback text
        try:
            text = "Scan & Pay via UPI."
            bot.send_message(chat_id, text, reply_markup=build_payment_reply_kb())
        except Exception:
            pass

    def build_products_kb():
        kb = types.InlineKeyboardMarkup(row_width=1)
        try:
            items = db.list_products(active_only=True)
        except Exception:
            items = []
        for p in items or []:
            label = f"{p.get('name')} — {p.get('days')}d"
            kb.add(types.InlineKeyboardButton(label, callback_data=f"plan:{p.get('id')}"))
        kb.add(
            types.InlineKeyboardButton("🏠 Main Menu", callback_data="menu:root"),
            types.InlineKeyboardButton("👤 Profile", callback_data="menu:profile"),
        )
        return kb

    def _user_has_premium(u):
        try:
            if not u:
                return False
            if "premium_active" in u:
                return bool(u.get("premium_active"))
            if "is_premium" in u:
                return bool(u.get("is_premium"))
        except Exception:
            pass

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("plan:"))
    def on_plan_select(call: types.CallbackQuery):
        if not _require_channel(call.message.chat.id, call.from_user.id):
            try:
                bot.answer_callback_query(call.id)
            except Exception:
                pass
            return
        pid = int(call.data.split(":", 1)[1])
        uid = call.from_user.id
        user = db.get_user_by_telegram_id(uid) or (
            db.upsert_user(uid, call.from_user.username or "", call.from_user.first_name or "", call.from_user.last_name or "", call.from_user.language_code or None) or
            db.get_user_by_telegram_id(uid)
        )
        p = db.get_product(pid)
        if not p:
            try:
                bot.answer_callback_query(call.id, "Plan not found")
            except Exception:
                pass
            return
        # Create pending order
        try:
            db.create_order(user_id=user['id'], product_id=pid, method=None, amount=None, currency=None, status='pending')
        except Exception:
            pass
        upi = os.getenv("UPI_ID")
        tron = os.getenv("USDT_TRC20_ADDRESS") or os.getenv("TRON_ADDRESS")
        evm = os.getenv("EVM_ADDRESS")
        price_bits = []
        if p.get('price_inr') is not None: price_bits.append(f"₹{int(p['price_inr'])}")
        if p.get('price_usdt') is not None: price_bits.append(f"${p['price_usdt']} USDT")
        lines = [
            f"Plan: <b>{utils.escape_html(p.get('name'))}</b> — {p.get('days')} days",
            f"Price: {', '.join(price_bits) or 'N/A'}",
            "",
            "Pay to any of these (then Verify):",
            f"• UPI: <code>{utils.escape_html(upi)}</code>" if upi else None,
            f"• USDT TRC20: <code>{utils.escape_html(tron)}</code>" if tron else None,
            f"• USDT (EVM): <code>{utils.escape_html(evm)}</code>" if evm else None,
            "",
            "After paying, tap Verify and send the ID/hash or upload receipt (🧾).",
        ]
        text = "\n".join([x for x in lines if x])
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        try:
            bot.send_message(call.message.chat.id, text, reply_markup=build_payment_reply_kb())
        except Exception:
            pass

    # --- Receipt upload handlers ---
    @bot.message_handler(content_types=["photo"])
    def on_receipt_photo(m: types.Message):
        # Admin quick set QR: send photo with caption '#qr'
        try:
            admin_id = os.getenv("ADMIN_ID")
            if admin_id and str(m.from_user.id) == str(admin_id) and m.caption and "#qr" in m.caption.lower():
                file_id = m.photo[-1].file_id if m.photo else None
                if file_id:
                    _write_qr_fid(file_id)
                    utils.send_safe(bot, m.chat.id, "✅ UPI QR updated.")
                    return True
        except Exception:
            pass
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        user = db.get_user_by_telegram_id(m.from_user.id) or (
            db.upsert_user(m.from_user.id, m.from_user.username or "", m.from_user.first_name or "", m.from_user.last_name or "", m.from_user.language_code or None) or
            db.get_user_by_telegram_id(m.from_user.id)
        )
        try:
            file_id = m.photo[-1].file_id if m.photo else None
            caption = m.caption or ""
            vid = db.insert_verification(user["id"], "receipt", "pending", tx_id=None, tx_hash=None, amount=None, currency=None, request_data={"file_id": file_id, "caption": caption}, notes=None)
            try:
                order = db.get_latest_pending_order_by_user_and_method(user['id'], None)
                if order:
                    db.update_order_receipt(order['id'], receipt_file_id=file_id, caption=caption)
                    db.set_order_status(order['id'], 'submitted')
                    db.update_verification_order(vid, order['id'])
            except Exception:
                pass
            utils.send_safe(bot, m.chat.id, "🧾 Receipt received. We'll review and update you.")
            admin_id = os.getenv("ADMIN_ID")
            if admin_id:
                try:
                    bot.copy_message(chat_id=int(admin_id), from_chat_id=m.chat.id, message_id=m.message_id)
                except Exception:
                    pass
        except Exception:
            pass

    @bot.message_handler(content_types=["document"])
    def on_receipt_doc(m: types.Message):
        # Admin quick set QR via document (image file) with caption '#qr'
        try:
            admin_id = os.getenv("ADMIN_ID")
            if admin_id and str(m.from_user.id) == str(admin_id) and m.caption and "#qr" in m.caption.lower() and m.document:
                file_id = m.document.file_id
                if file_id:
                    _write_qr_fid(file_id)
                    utils.send_safe(bot, m.chat.id, "✅ UPI QR updated.")
                    return True
        except Exception:
            pass
        if not _require_channel(m.chat.id, m.from_user.id):
            return False
        user = db.get_user_by_telegram_id(m.from_user.id) or (
            db.upsert_user(m.from_user.id, m.from_user.username or "", m.from_user.first_name or "", m.from_user.last_name or "", m.from_user.language_code or None) or
            db.get_user_by_telegram_id(m.from_user.id)
        )
        try:
            file_id = m.document.file_id if m.document else None
            caption = m.caption or ""
            vid = db.insert_verification(user["id"], "receipt", "pending", tx_id=None, tx_hash=None, amount=None, currency=None, request_data={"file_id": file_id, "caption": caption, "mime": getattr(m.document, 'mime_type', None)}, notes=None)
            try:
                order = db.get_latest_pending_order_by_user_and_method(user['id'], None)
                if order:
                    db.update_order_receipt(order['id'], receipt_file_id=file_id, caption=caption)
                    db.set_order_status(order['id'], 'submitted')
                    db.update_verification_order(vid, order['id'])
            except Exception:
                pass
            utils.send_safe(bot, m.chat.id, "🧾 Receipt received. We'll review and update you.")
            admin_id = os.getenv("ADMIN_ID")
            if admin_id:
                try:
                    bot.copy_message(chat_id=int(admin_id), from_chat_id=m.chat.id, message_id=m.message_id)
                except Exception:
                    pass
        except Exception:
            pass
        return False

    def _user_expiry(u):
        if not u:
            return None
        return u.get("premium_expires_at") or u.get("premium_until")

    @bot.message_handler(commands=["start"])
    def cmd_start(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        u = m.from_user
        # Do not auto-register; show SIGN UP / LOGIN until user explicitly registers
        user = db.get_user_by_telegram_id(u.id)
        msg = "You have an active premium subscription." if _user_has_premium(user) else "You do not have an active premium subscription."
        utils.send_safe(bot, u.id, f"👋 Welcome, {utils.escape_html(u.first_name or 'friend')}!\n\n{msg}\nUse the keyboard below or /menu.")
        try:
            bot.send_message(u.id, "Choose an option:", reply_markup=build_main_reply_kb(user))
        except Exception:
            pass

    @bot.message_handler(commands=["help"])
    def cmd_help(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        utils.send_safe(bot, m.chat.id, "Commands:\n/id\n/status\n/pricing\n/premium\n/menu\n/signal\n/hours\n/verify_upi <txn_id>\n/verify_usdt <tx_hash>")

    @bot.message_handler(commands=["pricing"])
    def cmd_pricing(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        try:
            send_pricing_card(m.chat.id)
        except Exception as e:
            logger.exception("Failed to send pricing")
            try:
                bot.send_message(m.chat.id, pricing_message())
            except Exception:
                pass

    @bot.message_handler(commands=["menu"])
    def cmd_menu(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        user = db.get_user_by_telegram_id(m.from_user.id)
        try:
            bot.send_message(m.chat.id, "Choose an option:", reply_markup=build_main_reply_kb(user))
        except Exception:
            pass

    @bot.message_handler(commands=["signal"])
    def cmd_signal(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        user = db.get_user_by_telegram_id(m.from_user.id)
        uid = m.from_user.id
        if not _user_has_premium(user):
            # Allow 1 free sample per day
            today = datetime.now(timezone.utc).date().isoformat()
            if FREE_SAMPLES.get(uid) == today:
                utils.send_safe(bot, m.chat.id, "🎟️ Free sample used for today. Upgrade to premium to continue.")
                return
        try:
            _send_kb_quietly(m.chat.id, build_quick_assets_reply_kb())
        except Exception:
            pass
        try:
            bot.send_message(m.chat.id, "Choose category:")
        except Exception:
            pass

    @bot.message_handler(commands=["hours"])
    def cmd_hours(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        msg = utils.market_hours_message_for_pairs(PAIRS_BASE)
        try:
            bot.send_message(m.chat.id, msg, reply_markup=build_basic_nav_kb())
        except Exception:
            pass

    @bot.message_handler(commands=["id"])
    def cmd_id(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        utils.send_safe(bot, m.chat.id, f"Your Telegram ID: <code>{m.from_user.id}</code>")

    @bot.message_handler(commands=["status"])
    def cmd_status(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        user = db.get_user_by_telegram_id(m.from_user.id)
        if not _user_has_premium(user):
            try:
                bot.send_message(m.chat.id, "❌ Premium status: Inactive", reply_markup=build_basic_nav_kb())
            except Exception:
                pass
            return
        try:
            bot.send_message(m.chat.id, f"✅ Premium: Active\nExpires: <b>{utils.format_ts_iso(_user_expiry(user))}</b>", reply_markup=build_basic_nav_kb())
        except Exception:
            pass

    @bot.message_handler(commands=["premium"])
    def cmd_premium(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        # Show available plans first
        try:
            items = db.list_products(active_only=True)
        except Exception:
            items = []
        if not items:
            # fallback to direct addresses
            upi = os.getenv("UPI_ID")
            usdt = os.getenv("USDT_TRC20_ADDRESS") or os.getenv("TRON_ADDRESS")
            evm = os.getenv("EVM_ADDRESS")
            lines = [
                "💳 Payment options:",
                f"- UPI: <code>{utils.escape_html(upi)}</code>" if upi else "- UPI: not configured",
                f"- USDT TRC20: <code>{utils.escape_html(usdt)}</code>" if usdt else "- USDT TRC20: not configured",
                f"- USDT (EVM: ETH/BSC/Polygon): <code>{utils.escape_html(evm)}</code>" if evm else "- USDT (EVM): not configured",
            ]
            try:
                bot.send_message(m.chat.id, "\n".join(lines), reply_markup=build_payment_reply_kb())
            except Exception:
                pass
            return
        # Show pricing card then show plans as a reply keyboard
        try:
            send_pricing_card(m.chat.id)
        except Exception:
            pass
        try:
            _send_kb_quietly(m.chat.id, build_products_reply_kb())
        except Exception:
            pass

    @bot.message_handler(commands=["verify_upi"])
    def cmd_verify_upi(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        parts = (m.text or "").split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            utils.send_safe(bot, m.chat.id, "Usage: /verify_upi <txn_id>")
            return
        user = db.get_user_by_telegram_id(m.from_user.id) or (
            db.upsert_user(m.from_user.id, m.from_user.username or "", m.from_user.first_name or "", m.from_user.last_name or "", m.from_user.language_code or None) or
            db.get_user_by_telegram_id(m.from_user.id)
        )
        vid = db.insert_verification(user["id"], "upi", "pending", tx_id=parts[1].strip(), tx_hash=None, amount=None, currency=None, request_data={"from":"bot"})
        try:
            order = db.get_latest_pending_order_by_user_and_method(user['id'], None)
            if order:
                db.update_order_method(order['id'], 'upi')
                db.update_order_tx(order['id'], tx_id=parts[1].strip(), tx_hash=None)
                db.set_order_status(order['id'], 'submitted')
                db.update_verification_order(vid, order['id'])
        except Exception:
            pass
        utils.send_safe(bot, m.chat.id, "✅ UPI verification received. We'll review and update you.")

    @bot.message_handler(commands=["verify_usdt"])
    def cmd_verify_usdt(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
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
        vid = db.insert_verification(user["id"], method, status, tx_id=None, tx_hash=txh, amount=None, currency="USDT", request_data=res, notes=None)
        try:
            order = db.get_latest_pending_order_by_user_and_method(user['id'], None)
            if order:
                db.update_order_method(order['id'], 'usdt')
                db.update_order_tx(order['id'], tx_id=None, tx_hash=txh)
                db.set_order_status(order['id'], 'submitted')
                db.update_verification_order(vid, order['id'])
        except Exception:
            pass
        utils.send_safe(bot, m.chat.id, "✅ Tx received. " + ("Auto-verified." if status == "auto_pass" else "Awaiting manual review."))

    @bot.message_handler(func=lambda m: True, content_types=["text"])
    def on_text(m: types.Message):
        if not _require_channel(m.chat.id, m.from_user.id):
            return
        try:
            db.touch_user_activity(m.from_user.id, saw=True, messaged=True)
        except Exception:
            pass
        txt = (m.text or "").strip().lower()
        raw_text = (m.text or "").strip()
        user = db.get_user_by_telegram_id(m.from_user.id)
        # Reply keyboard actions
        if "sign up" in txt or "signup" in txt or "login" in txt:
            if not user:
                db.upsert_user(m.from_user.id, m.from_user.username or "", m.from_user.first_name or "", m.from_user.last_name or "", m.from_user.language_code or None)
                user = db.get_user_by_telegram_id(m.from_user.id)
            # Show profile immediately and refresh keyboard (LOGIN/SIGN UP hidden now)
            p = user or {}
            status = "Active" if _user_has_premium(p) else "Inactive"
            exp = utils.format_ts_iso(_user_expiry(p)) if p else "N/A"
            text = (
                f"👤 Profile\n"
                f"Name: {utils.escape_html((m.from_user.first_name or '').strip())}\n"
                f"Username: @{utils.escape_html(m.from_user.username or '')}\n"
                f"Status: {status}\n"
                f"Expiry: {exp}"
            )
            try:
                bot.send_message(m.chat.id, text, reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        # Back/Home from reply keyboards
        if "back" in txt or "home" in txt:
            try:
                bot.send_message(m.chat.id, "Choose an option:", reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        # Main Menu button
        if "main menu" in txt:
            try:
                bot.send_message(m.chat.id, "Choose an option:", reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        # Verify buttons from reply keyboard
        if "verify upi" in txt or "scan upi" in txt or "scan" == txt:
            try:
                urow = db.get_user_by_telegram_id(m.from_user.id)
                if urow and hasattr(db, 'get_latest_pending_order_by_user_and_method'):
                    order = db.get_latest_pending_order_by_user_and_method(urow['id'], None)
                    if order:
                        db.update_order_method(order['id'], 'upi')
                # Try amount from selected order's product
                amt = None
                note = None
                if order and order.get('product_id'):
                    prod = db.get_product(order.get('product_id'))
                    if prod:
                        amt = prod.get('price_inr') if prod.get('price_inr') is not None else None
                        note = f"{prod.get('name')} {prod.get('days')}d"
            except Exception:
                amt = None
                note = None
            try:
                bot.send_message(m.chat.id, "📷 Scan the UPI QR below to pay. After paying, tap '🧾 Upload Receipt' and send the screenshot.", reply_markup=build_payment_reply_kb())
            except Exception:
                pass
            try:
                send_upi_qr(m.chat.id, amount=amt, note=note)
            except Exception:
                pass
            return
        if "verify usdt" in txt:
            try:
                urow = db.get_user_by_telegram_id(m.from_user.id)
                if urow and hasattr(db, 'get_latest_pending_order_by_user_and_method'):
                    order = db.get_latest_pending_order_by_user_and_method(urow['id'], None)
                    if order:
                        db.update_order_method(order['id'], 'usdt')
            except Exception:
                pass
            try:
                bot.send_message(m.chat.id, "Send your USDT tx hash using:\n/verify_usdt TX_HASH", reply_markup=build_payment_reply_kb())
            except Exception:
                pass
            return
        if "upload receipt" in txt:
            # Hint user to send photo/document; handlers will capture it
            try:
                bot.send_message(m.chat.id, "Please upload your payment receipt as a Photo or Document here. You can add a caption if needed.", reply_markup=build_payment_reply_kb())
            except Exception:
                pass
            return
        if "view receipt" in txt:
            try:
                urow = db.get_user_by_telegram_id(m.from_user.id)
                fid = db.get_latest_user_receipt_file_id(urow['id']) if urow else None
            except Exception:
                fid = None
            if not fid:
                try:
                    bot.send_message(m.chat.id, "No receipt found. Tap 'Upload Receipt' to send one.", reply_markup=build_payment_reply_kb())
                except Exception:
                    pass
                return
            # Try sending as photo first, fall back to document
            try:
                bot.send_photo(m.chat.id, fid, caption="Your latest receipt", reply_markup=build_payment_reply_kb())
            except Exception:
                try:
                    bot.send_document(m.chat.id, fid, caption="Your latest receipt", reply_markup=build_payment_reply_kb())
                except Exception:
                    try:
                        bot.send_message(m.chat.id, "Receipt found but could not display.")
                    except Exception:
                        pass
            return
        # Handle selecting a specific plan from reply keyboard
        try:
            items = db.list_products(active_only=True)
        except Exception:
            items = []
        chosen = None
        for p in items or []:
            if raw_text == f"{p.get('name')} — {p.get('days')}d" or raw_text == f"{p.get('name')} - {p.get('days')}d":
                chosen = p
                break
        if chosen:
            try:
                user = user or db.get_user_by_telegram_id(m.from_user.id) or (
                    db.upsert_user(m.from_user.id, m.from_user.username or "", m.from_user.first_name or "", m.from_user.last_name or "", m.from_user.language_code or None) or
                    db.get_user_by_telegram_id(m.from_user.id)
                )
            except Exception:
                pass
            try:
                if user:
                    db.create_order(user_id=user['id'], product_id=chosen.get('id'), method=None, amount=None, currency=None, status='pending')
            except Exception:
                pass
            upi = os.getenv("UPI_ID")
            tron = os.getenv("USDT_TRC20_ADDRESS") or os.getenv("TRON_ADDRESS")
            evm = os.getenv("EVM_ADDRESS")
            price_bits = []
            if chosen.get('price_inr') is not None: price_bits.append(f"₹{int(chosen['price_inr'])}")
            if chosen.get('price_usdt') is not None: price_bits.append(f"${chosen['price_usdt']} USDT")
            lines = [
                f"Plan: <b>{utils.escape_html(chosen.get('name'))}</b> — {chosen.get('days')} days",
                f"Price: {', '.join(price_bits) or 'N/A'}",
                "",
                "Pay to any of these (then Verify):",
                f"• UPI: <code>{utils.escape_html(upi)}</code>" if upi else None,
                f"• USDT TRC20: <code>{utils.escape_html(tron)}</code>" if tron else None,
                f"• USDT (EVM): <code>{utils.escape_html(evm)}</code>" if evm else None,
                "",
                "After paying, tap Verify and send the ID/hash or upload receipt (🧾).",
            ]
            text = "\n".join([x for x in lines if x])
            try:
                bot.send_message(m.chat.id, text, reply_markup=build_payment_reply_kb())
            except Exception:
                pass
            return
        if "profile" in txt:
            p = user or {}
            status = "Active" if _user_has_premium(p) else "Inactive"
            exp = utils.format_ts_iso(_user_expiry(p)) if p else "N/A"
            text = (
                f"👤 Profile\n"
                f"Name: {utils.escape_html((m.from_user.first_name or '').strip())}\n"
                f"Username: @{utils.escape_html(m.from_user.username or '')}\n"
                f"Status: {status}\n"
                f"Expiry: {exp}"
            )
            try:
                bot.send_message(m.chat.id, text, reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        if "get started" in txt:
            text = (
                "🚀 Get Started\n"
                "1) Tap SIGN UP or /start to register.\n"
                "2) Pay via /premium options.\n"
                "3) After grant, you will receive LIVE SIGNALS here."
            )
            try:
                bot.send_message(m.chat.id, text, reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        if "buy premium" in txt or "payment" in txt or txt == "premium" or "renew" in txt or "select a plan" in txt:
            # Show pricing then plans as reply keyboard
            try:
                items = db.list_products(active_only=True)
            except Exception:
                items = []
            if not items:
                cmd_premium(m)
                return
            try:
                send_pricing_card(m.chat.id)
            except Exception:
                pass
            try:
                _send_kb_quietly(m.chat.id, build_products_reply_kb())
            except Exception:
                pass
            return
        if "how it works" in txt:
            text = (
                "❓ How it works\n"
                "- We verify your payment (UPI/USDT).\n"
                "- Admin grants premium.\n"
                "- Signals and updates are sent right in this chat."
            )
            try:
                bot.send_message(m.chat.id, text, reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        if "market hours" in txt:
            msg = utils.market_hours_message()
            try:
                bot.send_message(m.chat.id, msg, reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        if "24h" in txt or "24 hour" in txt or "24-hour" in txt or "vip profit" in txt or "profit report" in txt or "24h profit" in txt:
            try:
                report = utils.generate_24h_served_report(db)
                bot.send_message(m.chat.id, report, reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        if "plan status" in txt or txt == "status":
            if not _user_has_premium(user):
                try:
                    bot.send_message(m.chat.id, "❌ Premium status: Inactive", reply_markup=build_main_reply_kb(user))
                except Exception:
                    pass
            else:
                try:
                    bot.send_message(m.chat.id, f"✅ Premium: Active\nExpires: <b>{utils.format_ts_iso(_user_expiry(user))}</b>", reply_markup=build_main_reply_kb(user))
                except Exception:
                    pass
            return
        if "support" in txt:
            try:
                bot.send_message(m.chat.id, f"💬 Support: {utils.escape_html(SUPPORT_CONTACT)}", reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        if "risk disclaimer" in txt or "disclaimer" in txt:
            text = (
                "⚠️ Risk Disclaimer\n"
                "Trading involves risk. Past performance is not indicative of future results."
            )
            try:
                bot.send_message(m.chat.id, text, reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        if "analysis tools" in txt:
            try:
                bot.send_message(m.chat.id, "📊 Analysis tools coming soon.", reply_markup=build_main_reply_kb(user))
            except Exception:
                pass
            return
        if "live signals" in txt:
            uid = m.from_user.id
            if not _user_has_premium(user):
                today = datetime.now(timezone.utc).date().isoformat()
                if FREE_SAMPLES.get(uid) == today:
                    utils.send_safe(bot, m.chat.id, "🎟️ Free sample used for today. Upgrade to premium to continue.")
                    return
            try:
                bot.send_message(m.chat.id, "Choose category:", reply_markup=build_quick_assets_reply_kb())
            except Exception:
                pass
            return

        # Quick categories via reply keyboard
        if txt == "live fx":
            ASSETS_STATE[m.from_user.id] = {"cat": "live", "page": 0}
            _show_assets_reply(m.chat.id, "live", 0)
            return
        if txt in ("◀ prev", "next ▶", "⬅️ categories"):
            st = ASSETS_STATE.get(m.from_user.id) or {"cat": "live", "page": 0}
            cat = st.get("cat", "live")
            page = int(st.get("page", 0))
            if txt == "◀ prev" and page > 0:
                page -= 1
            elif txt == "next ▶":
                total_pages = (len(PAIRS_BASE) - 1) // 10
                if page < total_pages:
                    page += 1
            elif txt == "⬅️ categories":
                try:
                    _send_kb_quietly(m.chat.id, build_quick_assets_reply_kb())
                except Exception:
                    pass
                return
            ASSETS_STATE[m.from_user.id] = {"cat": cat, "page": page}
            _show_assets_reply(m.chat.id, cat, page)
            return

        # If user taps a pair from the reply list
        if txt.upper() in {p.upper() for p in PAIRS_BASE}:
            pair_txt = txt.upper()
            code = pair_txt.replace("/", "")
            st = ASSETS_STATE.get(m.from_user.id) or {"cat": "live"}
            cat = st.get("cat", "live")
            if cat == "otc":
                code = f"{code}_OTC"
            SIGNAL_LAST[m.from_user.id] = code
            _send_kb_quietly(m.chat.id, build_timeframes_reply_kb())
            return

        # Existing shortcuts
        if txt in ("signal", "signals", "get signal", "get signals"):
            uid = m.from_user.id
            if not _user_has_premium(user):
                today = datetime.now(timezone.utc).date().isoformat()
                if FREE_SAMPLES.get(uid) == today:
                    utils.send_safe(bot, m.chat.id, "🎟️ Free sample used for today. Upgrade to premium to continue.")
                    return
            try:
                _send_kb_quietly(m.chat.id, build_quick_assets_reply_kb())
            except Exception:
                pass
            try:
                bot.send_message(m.chat.id, "Choose category:")
            except Exception:
                pass

        asset_map = {
            "btc/usdt": "BTCUSDT",
            "eth/usdt": "ETHUSDT",
            "eur/usd": "EURUSD",
            "gbp/jpy": "GBPJPY",
            "gold": "GOLD",
            "nasdaq": "NASDAQ",
        }
        if txt in asset_map:
            SIGNAL_LAST[m.from_user.id] = asset_map[txt]
            _send_kb_quietly(m.chat.id, build_timeframes_reply_kb())
            return
        if txt in ("1m", "3m", "5m"):
            asset_code = SIGNAL_LAST.get(m.from_user.id)
            if not asset_code:
                _send_kb_quietly(m.chat.id, build_assets_reply_kb())
                return
            uid = m.from_user.id
            user = db.get_user_by_telegram_id(uid)
            # Derive pair string from last selected code
            code = asset_code[:-4] if asset_code.endswith("_OTC") else asset_code
            if len(code) == 6 and code.isalpha():
                pair = f"{code[:3]}/{code[3:]}"
            else:
                code_to_pair = {
                    "BTCUSDT": "BTC/USDT",
                    "ETHUSDT": "ETH/USDT",
                    "GOLD": "GOLD",
                    "NASDAQ": "NASDAQ",
                }
                pair = code_to_pair.get(code, code)
            # Check market status before consuming quota
            try:
                open_now = utils._market_open_for_asset(pair)
            except Exception:
                open_now = True
            if not open_now:
                try:
                    nxt = utils.next_open_for_asset(pair)
                    when = utils.format_ts_iso(nxt) if nxt else "-"
                    bot.send_message(m.chat.id, f"Market closed for {pair}. Next open: <b>{when}</b>")
                except Exception:
                    pass
                return
            # Consume quota only if open
            if not _user_has_premium(user):
                today = datetime.now(timezone.utc).date().isoformat()
                if FREE_SAMPLES.get(uid) == today:
                    utils.send_safe(bot, m.chat.id, "🎟️ Free sample used for today. Upgrade to premium.")
                    return
                FREE_SAMPLES[uid] = today
                quota = {"ok": True, "source": "free", "used_today": 1, "daily_limit": 1, "credits": 0}
            else:
                quota = db.consume_signal_by_telegram_id(uid)
                if not quota.get("ok"):
                    msg = (
                        "❗ Daily signal limit reached and no credits left.\n"
                        "Each extra signal costs ₹150. Contact Support to top-up credits: "
                        f"{utils.escape_html(SUPPORT_CONTACT)}"
                    )
                    utils.send_safe(bot, m.chat.id, msg)
                    return
            stop_loading_tf = start_chat_action(m.chat.id, "typing")
            try:
                text = utils.generate_ensemble_signal(pair, txt)
            finally:
                try:
                    stop_loading_tf()
                except Exception:
                    pass
            footer = (
                f"\nRemaining today: {max(quota.get('daily_limit',0)-quota.get('used_today',0),0)} · "
                f"Credits: {quota.get('credits',0)} ({'daily' if quota.get('source')=='daily' else ('credit' if quota.get('source')=='credit' else 'free')} used)"
            )
            try:
                # Send immediately without waiting for price
                base_msg = bot.send_message(m.chat.id, text + "\n" + footer, reply_markup=build_timeframes_reply_kb())
                direction = utils.direction_from_signal_text(text) or ""
                def _fmt(v):
                    if v is None:
                        return "-"
                    v = float(v)
                    if v >= 100: return f"{v:.2f}"
                    if v >= 1: return f"{v:.4f}"
                    return f"{v:.6f}"
                def _after_send():
                    try:
                        entry_time_iso = datetime.now(timezone.utc).isoformat()
                        entry_price = utils.get_entry_price(pair, txt)
                        if entry_price is None:
                            for alt in ("1m", "5m", "3m"):
                                try:
                                    entry_price = utils.get_entry_price(pair, alt)
                                    if entry_price is not None:
                                        break
                                except Exception:
                                    pass
                        if entry_price is None:
                            try:
                                entry_price = utils.get_close_at_time(pair, txt, entry_time_iso)
                            except Exception:
                                entry_price = None
                        # Post a follow-up line with entry price
                        if direction in ("UP", "DOWN"):
                            details = f"Entry price: <code>{_fmt(entry_price)}</code> (update in {txt})"
                        else:
                            details = f"Entry price: <code>{_fmt(entry_price)}</code>"
                        details_msg = bot.send_message(m.chat.id, details)
                        # Log served signal
                        try:
                            urow = db.get_user_by_telegram_id(uid) or (
                                db.upsert_user(uid, m.from_user.username or "", m.from_user.first_name or "", m.from_user.last_name or "", m.from_user.language_code or None) or 
                                db.get_user_by_telegram_id(uid)
                            )
                            if urow and hasattr(db, 'insert_signal_log'):
                                db.insert_signal_log(
                                    user_id=urow.get('id'),
                                    telegram_id=uid,
                                    pair=pair,
                                    timeframe=txt,
                                    direction=direction,
                                    entry_price=entry_price,
                                    source=quota.get('source'),
                                    message_id=getattr(base_msg, 'message_id', None),
                                    raw_text=text,
                                    entry_time=entry_time_iso
                                )
                                # Log details line as separate message so admin delete can remove it
                                db.insert_signal_log(
                                    user_id=urow.get('id'),
                                    telegram_id=uid,
                                    pair=pair,
                                    timeframe=txt,
                                    direction=direction,
                                    entry_price=entry_price,
                                    source='details',
                                    message_id=getattr(details_msg, 'message_id', None),
                                    raw_text=details,
                                    entry_time=entry_time_iso
                                )
                        except Exception:
                            pass
                        # Schedule updates only for real trades
                        try:
                            if direction in ("UP", "DOWN"):
                                for tf_label, delay in [("1m", 55), ("3m", 175), ("5m", 295)]:
                                    def _post_update(tf_label=tf_label, entry_price=entry_price, urow=urow):
                                        try:
                                            now_iso = datetime.now(timezone.utc).isoformat()
                                            new_price = utils.get_entry_price(pair, tf_label)
                                            if new_price is None:
                                                for alt in ("1m", "5m"):
                                                    new_price = utils.get_entry_price(pair, alt)
                                                    if new_price is not None:
                                                        break
                                            if new_price is None:
                                                new_price = utils.get_close_at_time(pair, tf_label, now_iso)
                                            if entry_price is not None and new_price is not None and entry_price:
                                                ch = (float(new_price) - float(entry_price)) / float(entry_price) * 100.0
                                                delta = f"{ch:+.2f}%"
                                            else:
                                                delta = "-"
                                            upd = (
                                                f"⏱ {tf_label} update for {pair}\n"
                                                f"Entry: <code>{_fmt(entry_price)}</code> → Now: <code>{_fmt(new_price)}</code>\n"
                                                f"Change: {delta}"
                                            )
                                            upd_msg = bot.send_message(m.chat.id, upd, reply_markup=build_timeframes_reply_kb())
                                            # Log update message id
                                            try:
                                                if urow and hasattr(db, 'insert_signal_log'):
                                                    db.insert_signal_log(
                                                        user_id=urow.get('id'),
                                                        telegram_id=uid,
                                                        pair=pair,
                                                        timeframe=tf_label,
                                                        direction=direction,
                                                        entry_price=new_price,
                                                        source=f'update:{tf_label}',
                                                        message_id=getattr(upd_msg, 'message_id', None),
                                                        raw_text=upd,
                                                        entry_time=now_iso
                                                    )
                                            except Exception:
                                                pass
                                        except Exception:
                                            pass
                                    threading.Timer(delay, _post_update).start()
                        except Exception:
                            pass
                    except Exception:
                        pass
                threading.Thread(target=_after_send, daemon=True).start()
            except Exception:
                pass
            return
        if txt in ("🏠 home", "home", "🏠"):
            _send_kb_quietly(m.chat.id, build_main_reply_kb(user))
            return
        if txt.startswith("⬅️ back"):
            _send_kb_quietly(m.chat.id, build_assets_reply_kb())
            return

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("menu:"))
    def on_menu_click(call: types.CallbackQuery):
        action = call.data.split(":", 1)[1]
        uid = call.from_user.id
        user = db.get_user_by_telegram_id(uid)
        if not _require_channel(call.message.chat.id, uid):
            try:
                bot.answer_callback_query(call.id)
            except Exception:
                pass
            return
        text = None

        if action in ("signup", "login"):
            if not user:
                db.upsert_user(uid, call.from_user.username or "", call.from_user.first_name or "", call.from_user.last_name or "", call.from_user.language_code or None)
                user = db.get_user_by_telegram_id(uid)
            text = "✅ You are now registered."
        elif action == "profile":
            p = user or {}
            status = "Active" if _user_has_premium(p) else "Inactive"
            exp = utils.format_ts_iso(_user_expiry(p)) if p else "N/A"
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
            # Gate again before sending keyboards
            if not _require_channel(call.message.chat.id, uid):
                try:
                    bot.answer_callback_query(call.id)
                except Exception:
                    pass
                return
            today = datetime.now(timezone.utc).date().isoformat()
            if _user_has_premium(user) or FREE_SAMPLES.get(uid) != today:
                try:
                    _send_kb_quietly(call.message.chat.id, build_quick_assets_reply_kb())
                except Exception:
                    pass
                try:
                    bot.send_message(call.message.chat.id, "Choose category:")
                except Exception:
                    pass
                text = None
            else:
                text = "📈 Live signals are for premium users. Check PLAN STATUS for your subscription."
        elif action == "tools":
            text = "📊 Analysis tools coming soon."
        elif action == "hours":
            text = utils.market_hours_message_for_pairs(PAIRS_BASE)
        elif action == "perf24h":
            text = utils.generate_24h_served_report(db)
        elif action == "plan":
            if not _user_has_premium(user):
                text = "❌ Premium status: Inactive"
            else:
                text = f"✅ Premium: Active\nExpires: <b>{utils.format_ts_iso(_user_expiry(user))}</b>"
        elif action == "support":
            text = f"💬 Support: {utils.escape_html(SUPPORT_CONTACT)}"
        elif action == "disclaimer":
            text = (
                "⚠️ Risk Disclaimer\n"
                "Trading involves risk. Past performance is not indicative of future results."
            )
        elif action == "root":
            text = "Choose an option:"

        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass

        if text:
            try:
                if action == "root":
                    bot.send_message(call.message.chat.id, text, reply_markup=build_main_reply_kb(db.get_user_by_telegram_id(uid)))
                else:
                    bot.send_message(call.message.chat.id, text, reply_markup=build_basic_nav_kb())
            except Exception:
                pass
        # Update inline markup if present on the original message
        try:
            bot.edit_message_reply_markup(chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=build_main_menu(db.get_user_by_telegram_id(uid)))
        except Exception:
            pass

    @bot.callback_query_handler(func=lambda c: c.data == "chk:joined")
    def on_check_joined(call: types.CallbackQuery):
        uid = call.from_user.id
        ok = _is_channel_member(uid)
        try:
            bot.answer_callback_query(call.id, "Verified ✅" if ok else "Not joined yet ❌")
        except Exception:
            pass
        if ok:
            try:
                urow = db.get_user_by_telegram_id(uid)
                bot.send_message(call.message.chat.id, "Thanks for joining! Choose an option:", reply_markup=build_main_reply_kb(urow))
            except Exception:
                pass

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("pay:"))
    def on_pay_click(call: types.CallbackQuery):
        action = call.data.split(":", 1)[1]
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        if action == "verify_upi":
            txt = "📷 Scan the UPI QR below to pay. After paying, tap '🧾 Upload Receipt' and send the screenshot."
        elif action == "verify_usdt":
            txt = "Send your USDT tx hash using:\n<code>/verify_usdt TX_HASH</code>"
        else:
            txt = "Payment support"
        # Remember chosen method on latest pending order
        try:
            uid = call.from_user.id
            order = db.get_latest_pending_order_by_user_and_method(db.get_user_by_telegram_id(uid)['id'], None)
            if order:
                db.update_order_method(order['id'], 'upi' if action == 'verify_upi' else 'usdt')
        except Exception:
            pass
        try:
            bot.send_message(call.message.chat.id, txt, reply_markup=build_payment_reply_kb())
        except Exception:
            pass
        # For UPI, also send QR with amount if available from latest order
        if action == "verify_upi":
            amt = None
            note = None
            try:
                uid = call.from_user.id
                urow = db.get_user_by_telegram_id(uid)
                order = db.get_latest_pending_order_by_user_and_method(urow['id'], None) if urow else None
                prod = db.get_product(order.get('product_id')) if order and order.get('product_id') else None
                amt = prod.get('price_inr') if prod and prod.get('price_inr') is not None else None
                note = f"{prod.get('name')} {prod.get('days')}d" if prod else None
            except Exception:
                pass
            try:
                send_upi_qr(call.message.chat.id, amount=amt, note=note)
            except Exception:
                pass

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("back:"))
    def on_back_click(call: types.CallbackQuery):
        action = call.data.split(":", 1)[1]
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        if action == "assets":
            try:
                _send_kb_quietly(call.message.chat.id, build_quick_assets_reply_kb())
            except Exception:
                pass
            try:
                bot.send_message(call.message.chat.id, "Choose category:")
            except Exception:
                pass

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("assets:"))
    def on_assets_category(call: types.CallbackQuery):
        cat = call.data.split(":", 1)[1]
        if cat == "otc":
            cat = "live"
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        # Hide the inline keyboard and switch to reply keyboard list under chat
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        try:
            uid = call.from_user.id
            ASSETS_STATE[uid] = {"cat": cat, "page": 0}
            _show_assets_reply(call.message.chat.id, cat, 0)
        except Exception:
            pass

    @bot.callback_query_handler(func=lambda c: c.data == "back:assets")
    def on_assets_back(call: types.CallbackQuery):
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        # Show the reply keyboard under chat and a hint message (no inline picker)
        try:
            _send_kb_quietly(call.message.chat.id, build_quick_assets_reply_kb())
        except Exception:
            pass
        try:
            bot.send_message(call.message.chat.id, "Choose category:")
        except Exception:
            pass

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("sig:"))
    def on_signal_asset(call: types.CallbackQuery):
        uid = call.from_user.id
        user = db.get_user_by_telegram_id(uid)
        if not _user_has_premium(user):
            # Allow navigation into timeframes if free sample not yet used today
            today = datetime.now(timezone.utc).date().isoformat()
            if FREE_SAMPLES.get(uid) == today:
                try:
                    bot.answer_callback_query(call.id, "Free sample used today. Upgrade to premium.")
                except Exception:
                    pass
                return
        asset_code = call.data.split(":", 1)[1]
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        # Hide the inline keyboard and switch to reply keyboard for timeframes
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        try:
            SIGNAL_LAST[uid] = asset_code
            _send_kb_quietly(call.message.chat.id, build_timeframes_reply_kb())
            bot.send_message(call.message.chat.id, f"Choose timeframe for {asset_code}:")
        except Exception:
            pass

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("tf:"))
    def on_signal_timeframe(call: types.CallbackQuery):
        uid = call.from_user.id
        user = db.get_user_by_telegram_id(uid)
        if not _user_has_premium(user):
            today = datetime.now(timezone.utc).date().isoformat()
            if FREE_SAMPLES.get(uid) == today:
                try:
                    bot.answer_callback_query(call.id, "Free sample used today. Upgrade to premium.")
                except Exception:
                    pass
                return
        _, asset_code, tf = call.data.split(":", 2)
        code = asset_code[:-4] if asset_code.endswith("_OTC") else asset_code
        pair = (f"{code[:3]}/{code[3:]}" if len(code) == 6 else code)
        # Do not consume quota if market closed; show next open time instead
        try:
            open_now = utils._market_open_for_asset(pair)
        except Exception:
            open_now = True
        if not open_now:
            try:
                nxt = utils.next_open_for_asset(pair)
                when = utils.format_ts_iso(nxt) if nxt else "-"
                bot.answer_callback_query(call.id)
            except Exception:
                pass
            try:
                bot.send_message(call.message.chat.id, f"Market closed for {pair}. Next open: <b>{when}</b>")
            except Exception:
                pass
            return
        if _user_has_premium(user):
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
        else:
            # Mark free sample as used
            FREE_SAMPLES[uid] = datetime.now(timezone.utc).date().isoformat()
            quota = {"ok": True, "source": "free", "used_today": 1, "daily_limit": 1, "credits": 0}
        stop_loading_tf = start_chat_action(call.message.chat.id, "typing")
        try:
            text = utils.generate_ensemble_signal(pair, tf)
        finally:
            try:
                stop_loading_tf()
            except Exception:
                pass
        footer = (
            f"\nRemaining today: {max(quota.get('daily_limit',0)-quota.get('used_today',0),0)} · "
            f"Credits: {quota.get('credits',0)} ({'daily' if quota.get('source')=='daily' else ('credit' if quota.get('source')=='credit' else 'free')} used)"
        )
        # Send base signal immediately
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        try:
            base_msg = bot.send_message(call.message.chat.id, text + "\n" + footer, reply_markup=build_signal_nav_kb(asset_code))
            direction = utils.direction_from_signal_text(text) or ""
            def _fmt2(v):
                if v is None:
                    return "-"
                v = float(v)
                if v >= 100: return f"{v:.2f}"
                if v >= 1: return f"{v:.4f}"
                return f"{v:.6f}"
            def _after_send2():
                try:
                    entry_time_iso = datetime.now(timezone.utc).isoformat()
                    entry_price = utils.get_entry_price(pair, tf)
                    if entry_price is None:
                        for alt in ("1m", "5m", "3m"):
                            try:
                                entry_price = utils.get_entry_price(pair, alt)
                                if entry_price is not None:
                                    break
                            except Exception:
                                pass
                    if entry_price is None:
                        try:
                            entry_price = utils.get_close_at_time(pair, tf, entry_time_iso)
                        except Exception:
                            entry_price = None
                    # Follow-up message for entry
                    if direction in ("UP", "DOWN"):
                        details = f"Entry price: <code>{_fmt2(entry_price)}</code> (update in {tf})"
                    else:
                        details = f"Entry price: <code>{_fmt2(entry_price)}</code>"
                    details_msg2 = bot.send_message(call.message.chat.id, details)
                    # Log served signal
                    try:
                        urow = db.get_user_by_telegram_id(uid) or (
                            db.upsert_user(uid, call.from_user.username or "", call.from_user.first_name or "", call.from_user.last_name or "", call.from_user.language_code or None) or 
                            db.get_user_by_telegram_id(uid)
                        )
                        if urow and hasattr(db, 'insert_signal_log'):
                            db.insert_signal_log(
                                user_id=urow.get('id'),
                                telegram_id=uid,
                                pair=pair,
                                timeframe=tf,
                                direction=direction,
                                entry_price=entry_price,
                                source=quota.get('source'),
                                message_id=getattr(base_msg, 'message_id', None),
                                raw_text=text,
                                entry_time=entry_time_iso
                            )
                            # Log details line as separate message
                            db.insert_signal_log(
                                user_id=urow.get('id'),
                                telegram_id=uid,
                                pair=pair,
                                timeframe=tf,
                                direction=direction,
                                entry_price=entry_price,
                                source='details',
                                message_id=getattr(details_msg2, 'message_id', None),
                                raw_text=details,
                                entry_time=entry_time_iso
                            )
                    except Exception:
                        pass
                    # Schedule updates only for real trades
                    try:
                        if direction in ("UP", "DOWN"):
                            for tf_label, delay in [("1m", 55), ("3m", 175), ("5m", 295)]:
                                def _post_update2(tf_label=tf_label, entry_price=entry_price):
                                    try:
                                        now_iso = datetime.now(timezone.utc).isoformat()
                                        new_price = utils.get_entry_price(pair, tf_label)
                                        if new_price is None:
                                            for alt in ("1m", "5m"):
                                                new_price = utils.get_entry_price(pair, alt)
                                                if new_price is not None:
                                                    break
                                        if new_price is None:
                                            new_price = utils.get_close_at_time(pair, tf_label, now_iso)
                                        if entry_price is not None and new_price is not None and entry_price:
                                            ch = (float(new_price) - float(entry_price)) / float(entry_price) * 100.0
                                            delta = f"{ch:+.2f}%"
                                        else:
                                            delta = "-"
                                        upd = (
                                            f"⏱ {tf_label} update for {pair}\n"
                                            f"Entry: <code>{_fmt2(entry_price)}</code> → Now: <code>{_fmt2(new_price)}</code>\n"
                                            f"Change: {delta}"
                                        )
                                        bot.send_message(call.message.chat.id, upd, reply_markup=build_signal_nav_kb(asset_code))
                                    except Exception:
                                        pass
                                threading.Timer(delay, _post_update2).start()
                    except Exception:
                        pass
                except Exception:
                    pass
            threading.Thread(target=_after_send2, daemon=True).start()
        except Exception:
            pass

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

    @app.post("/admin/delete_chat")
    @ui_login_required
    def admin_delete_chat():
        ident = (request.form.get("ident") or "").strip()
        max_msgs = int(request.form.get("limit") or 500)
        full = (request.form.get("full") or "").strip().lower() in ("1", "true", "on", "yes")
        if not ident:
            flash("Provide ident", "warning")
            return redirect(url_for("admin_users", q=ident))
        user = db.resolve_user_by_ident(ident)
        if not user:
            flash("User not found", "danger")
            return redirect(url_for("admin_users", q=ident))
        rows = []
        try:
            rows = db.list_signal_logs_by_user(user["id"], max_msgs)
        except Exception:
            rows = []
        attempted = len(rows)
        deleted = 0
        failed = 0
        if bot:
            for r in rows:
                tg = r.get("telegram_id")
                mid = r.get("message_id")
                if not tg or not mid:
                    continue
                try:
                    bot.delete_message(int(tg), int(mid))
                    deleted += 1
                except Exception:
                    failed += 1
        deleted_extra = 0
        tg_id = int(user.get("telegram_id") or 0)
        probe_msg = None
        if bot and tg_id and (attempted == 0 or deleted < attempted):
            windows = 3 if full else 1
            for _ in range(windows):
                try:
                    probe_msg = bot.send_message(tg_id, "…")
                    top_id = getattr(probe_msg, 'message_id', None)
                    if top_id:
                        lo = max(int(top_id) - max_msgs, 1)
                        for mid in range(int(top_id), lo - 1, -1):
                            try:
                                bot.delete_message(tg_id, mid)
                                deleted_extra += 1
                            except Exception:
                                pass
                except Exception:
                    pass
                finally:
                    if probe_msg is not None:
                        try:
                            bot.delete_message(tg_id, int(getattr(probe_msg, 'message_id', 0)))
                        except Exception:
                            pass
        try:
            db.delete_signal_logs_by_user(user["id"])
        except Exception:
            pass
        db.log_admin("delete_chat", {"user_id": user["id"], "ident": ident, "attempted": attempted, "deleted": deleted, "failed": failed, "deleted_extra": deleted_extra}, performed_by="panel")
        if deleted_extra:
            flash(f"Deleted {deleted}/{attempted} via logs + {deleted_extra} via sweep; logs cleared", "success")
        else:
            flash(f"Deleted {deleted}/{attempted} messages; logs cleared", "success")
        try:
            if bot and tg_id:
                kb = types.InlineKeyboardMarkup()
                kb.add(types.InlineKeyboardButton("START", callback_data="menu:get_started"))
                bot.send_message(tg_id, "Reset complete. Tap START to begin.", reply_markup=kb)
        except Exception:
            pass
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
    credits = int(d.get("credits") or 0)
    if not ident or (days <= 0 and credits == 0): return jsonify({"ok": False, "error": "bad_request"}), 400
    user = db.resolve_user_by_ident(ident)
    if not user: return jsonify({"ok": False, "error": "user_not_found"}), 404
    new_exp = None
    if days > 0:
        new_exp = db.grant_premium_by_user_id(user["id"], days)
    if credits != 0:
        try:
            db.add_signal_credits_by_user_id(user["id"], credits)
        except Exception:
            pass
    db.log_admin("grant", {"user_id": user["id"], "ident": ident, "days": days, "credits": credits}, performed_by="api")
    if bot:
        parts = []
        if days > 0:
            parts.append(f"Premium +{days}d. New expiry: <b>{utils.format_ts_iso(new_exp)}</b>")
        if credits != 0:
            parts.append(f"Signal credits {'+' if credits>0 else ''}{credits}")
        if parts:
            utils.send_safe(bot, user["telegram_id"], "🎉 " + " \u2022 ".join(parts))
    return jsonify({"ok": True, "new_expires_at": utils.to_iso(new_exp), "credits_delta": credits})

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
