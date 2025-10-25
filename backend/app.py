import os
import logging
import threading
from datetime import datetime, timezone, timedelta
from functools import wraps

from flask import Flask, request, jsonify, render_template, redirect, url_for, session, flash
from flask_cors import CORS

import telebot
from telebot import types
import json

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

# Dynamic DB backend: Postgres if DATABASE_URL set, else SQLite
from . import utils
try:
    # Optional dual-DB access for sync operations
    from . import sqlite_db as sdb
except Exception:
    sdb = None
try:
    from . import database as pdb
except Exception:
    pdb = None
try:
    if os.getenv("DATABASE_URL", "").strip():
        from . import database as db
    else:
        from . import sqlite_db as db
except Exception:
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

# ---- Sync readiness helpers ----
def _pdb_ready() -> bool:
    try:
        return bool(pdb) and bool(getattr(pdb, "pool", None))
    except Exception:
        return False

def _sdb_ready() -> bool:
    try:
        return bool(sdb)
    except Exception:
        return False

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
    flash(f"Cron run: notices={result.get('notices')} expired={result.get('expired')}", "success")
    return redirect(url_for("admin_dashboard"))

@app.post("/admin/sync/push_users")
@ui_login_required
def admin_sync_push_users():
    if not _pdb_ready():
        flash("Supabase DATABASE_URL not configured.", "danger")
        return redirect(url_for("admin_dashboard"))
    if not _sdb_ready():
        flash("Local SQLite not available.", "danger")
        return redirect(url_for("admin_dashboard"))
    rows = []
    try:
        # Pull all local users (SQLite)
        rows = sdb.list_all_users_full()
    except Exception:
        rows = []
    pushed = 0
    try:
        from datetime import datetime
        with pdb.get_conn() as c, c.cursor() as cur:
            for r in rows:
                tg = r.get("telegram_id")
                if not tg:
                    continue
                username = (r.get("username") or "").strip()
                ident = (f"@{username.lower()}" if username else f"tg:{tg}")
                cur.execute(
                    """
                    INSERT INTO users (telegram_id, ident, username, first_name, last_name, lang_code,
                                       premium_active, premium_expires_at,
                                       signal_daily_limit, signal_used_today, signal_day, signal_credits,
                                       last_seen_at, last_message_at, created_at, updated_at)
                    VALUES (%s,%s, NULLIF(%s,''), %s,%s,%s,
                            %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, COALESCE(%s, NOW()), NOW())
                    ON CONFLICT (telegram_id) DO UPDATE SET
                      ident=EXCLUDED.ident,
                      username=EXCLUDED.username,
                      first_name=EXCLUDED.first_name,
                      last_name=EXCLUDED.last_name,
                      lang_code=EXCLUDED.lang_code,
                      premium_active=EXCLUDED.premium_active,
                      premium_expires_at=EXCLUDED.premium_expires_at,
                      signal_daily_limit=EXCLUDED.signal_daily_limit,
                      signal_used_today=EXCLUDED.signal_used_today,
                      signal_day=EXCLUDED.signal_day,
                      signal_credits=EXCLUDED.signal_credits,
                      last_seen_at=COALESCE(EXCLUDED.last_seen_at, users.last_seen_at),
                      last_message_at=COALESCE(EXCLUDED.last_message_at, users.last_message_at),
                      updated_at=NOW()
                    """,
                    (
                        tg,
                        ident,
                        username,
                        r.get("first_name"),
                        r.get("last_name"),
                        r.get("lang_code"),
                        bool(r.get("is_premium", False)),
                        r.get("premium_until"),
                        int(r.get("signal_daily_limit") or 0),
                        int(r.get("signal_daily_used") or 0),
                        r.get("signal_last_used_date"),
                        int(r.get("signal_credits") or 0),
                        r.get("last_active"),
                        r.get("last_message"),
                        r.get("created_at"),
                    ),
                )
                pushed += 1
    except Exception:
        logger.exception("sync push users failed")
    db.log_admin("sync_push_users", {"pushed": pushed}, performed_by="panel")
    flash(f"Pushed {pushed} users to Supabase", "success")
    return redirect(url_for("admin_dashboard"))

@app.post("/admin/sync/pull_users")
@ui_login_required
def admin_sync_pull_users():
    if not _pdb_ready():
        flash("Supabase DATABASE_URL not configured.", "danger")
        return redirect(url_for("admin_dashboard"))
    if not _sdb_ready():
        flash("Local SQLite not available.", "danger")
        return redirect(url_for("admin_dashboard"))
    rows = []
    try:
        rows = pdb.list_all_users_full()
    except Exception:
        rows = []
    pulled = 0
    for r in rows:
        try:
            sdb.upsert_user_full(r)
            pulled += 1
        except Exception:
            pass
    db.log_admin("sync_pull_users", {"pulled": pulled}, performed_by="panel")
    flash(f"Pulled {pulled} users from Supabase", "success")
    return redirect(url_for("admin_dashboard"))

@app.post("/admin/sync/pull_all")
@ui_login_required
def admin_sync_pull_all():
    if not _pdb_ready() or not _sdb_ready():
        flash("Configure Supabase DATABASE_URL and ensure local SQLite is available.", "danger")
        return redirect(url_for("admin_dashboard"))
    counts = {"users": 0, "products": 0, "orders": 0, "verifications": 0, "signal_logs": 0}
    # Users
    try:
        for r in pdb.list_all_users_full():
            try:
                sdb.upsert_user_full(r)
                counts["users"] += 1
            except Exception:
                pass
    except Exception:
        logger.exception("pull users failed")
    # Products
    try:
        for p in pdb.list_all_products_full():
            try:
                sdb.upsert_product_full(p)
                counts["products"] += 1
            except Exception:
                pass
    except Exception:
        logger.exception("pull products failed")
    # Orders
    try:
        for o in pdb.list_all_orders_full():
            try:
                sdb.upsert_order_full(o)
                counts["orders"] += 1
            except Exception:
                pass
    except Exception:
        logger.exception("pull orders failed")
    # Verifications
    try:
        for v in pdb.list_all_verifications_full():
            try:
                sdb.upsert_verification_full(v)
                counts["verifications"] += 1
            except Exception:
                pass
    except Exception:
        logger.exception("pull verifications failed")
    # Signal logs
    try:
        for sl in pdb.list_all_signal_logs_full():
            try:
                sdb.insert_signal_log_full(sl)
                counts["signal_logs"] += 1
            except Exception:
                pass
    except Exception:
        logger.exception("pull signal_logs failed")
    db.log_admin("sync_pull_all", counts, performed_by="panel")
    flash(f"Pull complete: {counts}", "success")
    return redirect(url_for("admin_dashboard"))

@app.post("/admin/sync/push_all")
@ui_login_required
def admin_sync_push_all():
    if not _pdb_ready() or not _sdb_ready():
        flash("Configure Supabase DATABASE_URL and ensure local SQLite is available.", "danger")
        return redirect(url_for("admin_dashboard"))
    counts = {"users": 0, "products": 0, "orders": 0, "verifications": 0, "signal_logs": 0}
    try:
        with pdb.get_conn() as c, c.cursor() as cur:
            # Users
            try:
                for r in sdb.list_all_users_full():
                    tg = r.get("telegram_id")
                    if not tg:
                        continue
                    username = (r.get("username") or "").strip()
                    ident = (f"@{username.lower()}" if username else f"tg:{tg}")
                    cur.execute(
                        """
                        INSERT INTO users (telegram_id, ident, username, first_name, last_name, lang_code,
                                           premium_active, premium_expires_at,
                                           signal_daily_limit, signal_used_today, signal_day, signal_credits,
                                           last_seen_at, last_message_at, created_at, updated_at)
                        VALUES (%s,%s, NULLIF(%s,''), %s,%s,%s,
                                %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, COALESCE(%s, NOW()), NOW())
                        ON CONFLICT (telegram_id) DO UPDATE SET
                          ident=EXCLUDED.ident,
                          username=EXCLUDED.username,
                          first_name=EXCLUDED.first_name,
                          last_name=EXCLUDED.last_name,
                          lang_code=EXCLUDED.lang_code,
                          premium_active=EXCLUDED.premium_active,
                          premium_expires_at=EXCLUDED.premium_expires_at,
                          signal_daily_limit=EXCLUDED.signal_daily_limit,
                          signal_used_today=EXCLUDED.signal_used_today,
                          signal_day=EXCLUDED.signal_day,
                          signal_credits=EXCLUDED.signal_credits,
                          last_seen_at=COALESCE(EXCLUDED.last_seen_at, users.last_seen_at),
                          last_message_at=COALESCE(EXCLUDED.last_message_at, users.last_message_at),
                          updated_at=NOW()
                        """,
                        (
                            tg,
                            ident,
                            username,
                            r.get("first_name"),
                            r.get("last_name"),
                            r.get("lang_code"),
                            bool(r.get("is_premium", False)),
                            r.get("premium_until"),
                            int(r.get("signal_daily_limit") or 0),
                            int(r.get("signal_daily_used") or 0),
                            r.get("signal_last_used_date"),
                            int(r.get("signal_credits") or 0),
                            r.get("last_active"),
                            r.get("last_message"),
                            r.get("created_at"),
                        ),
                    )
                    counts["users"] += 1
            except Exception:
                logger.exception("push users failed")
            # Products
            try:
                for p in sdb.list_all_products_full():
                    cur.execute(
                        """
                        INSERT INTO products (id, name, description, days, price_inr, price_usdt, active, created_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s, COALESCE(%s, NOW()))
                        ON CONFLICT (id) DO UPDATE SET
                          name=EXCLUDED.name,
                          description=EXCLUDED.description,
                          days=EXCLUDED.days,
                          price_inr=EXCLUDED.price_inr,
                          price_usdt=EXCLUDED.price_usdt,
                          active=EXCLUDED.active
                        """,
                        (
                            p.get("id"), p.get("name"), p.get("description"), p.get("days"), p.get("price_inr"), p.get("price_usdt"), bool(p.get("active")), p.get("created_at")
                        ),
                    )
                    counts["products"] += 1
            except Exception:
                logger.exception("push products failed")
            # Orders
            try:
                for o in sdb.list_all_orders_full():
                    tg = o.get("src_user_telegram_id")
                    if not tg:
                        continue
                    cur.execute("SELECT id FROM users WHERE telegram_id=%s", (int(tg),))
                    ru = cur.fetchone()
                    if not ru:
                        continue
                    uid = int(ru["id"]) if isinstance(ru, dict) else int(ru[0])
                    cur.execute(
                        """
                        INSERT INTO orders (id, user_id, product_id, method, status, amount, currency, tx_id, tx_hash, receipt_file_id, notes, created_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, COALESCE(%s, NOW()))
                        ON CONFLICT (id) DO UPDATE SET
                          user_id=EXCLUDED.user_id,
                          product_id=EXCLUDED.product_id,
                          method=EXCLUDED.method,
                          status=EXCLUDED.status,
                          amount=EXCLUDED.amount,
                          currency=EXCLUDED.currency,
                          tx_id=EXCLUDED.tx_id,
                          tx_hash=EXCLUDED.tx_hash,
                          receipt_file_id=EXCLUDED.receipt_file_id,
                          notes=COALESCE(EXCLUDED.notes, orders.notes)
                        """,
                        (
                            o.get("id"), uid, o.get("product_id"), o.get("method"), o.get("status"), o.get("amount"), o.get("currency"), o.get("tx_id"), o.get("tx_hash"), o.get("receipt_file_id"), o.get("notes"), o.get("created_at")
                        ),
                    )
                    counts["orders"] += 1
            except Exception:
                logger.exception("push orders failed")
            # Verifications
            try:
                for v in sdb.list_all_verifications_full():
                    tg = v.get("src_user_telegram_id")
                    if not tg:
                        continue
                    cur.execute("SELECT id FROM users WHERE telegram_id=%s", (int(tg),))
                    ru = cur.fetchone()
                    if not ru:
                        continue
                    uid = int(ru["id"]) if isinstance(ru, dict) else int(ru[0])
                    cur.execute(
                        """
                        INSERT INTO verifications (id, user_id, method, status, tx_id, tx_hash, amount, currency, request_data, notes, created_at, order_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s, CAST(%s AS JSONB), %s, COALESCE(%s, NOW()), %s)
                        ON CONFLICT (id) DO UPDATE SET
                          user_id=EXCLUDED.user_id,
                          method=EXCLUDED.method,
                          status=EXCLUDED.status,
                          tx_id=EXCLUDED.tx_id,
                          tx_hash=EXCLUDED.tx_hash,
                          amount=EXCLUDED.amount,
                          currency=EXCLUDED.currency,
                          request_data=COALESCE(EXCLUDED.request_data, verifications.request_data),
                          notes=COALESCE(EXCLUDED.notes, verifications.notes),
                          order_id=COALESCE(EXCLUDED.order_id, verifications.order_id)
                        """,
                        (
                            v.get("id"), uid, v.get("method"), v.get("status"), v.get("tx_id"), v.get("tx_hash"), v.get("amount"), v.get("currency"),
                            json.dumps(v.get("request_data")) if ("request_data" in v and v.get("request_data") is not None) else None,
                            v.get("notes"), v.get("created_at"), v.get("order_id")
                        ),
                    )
                    counts["verifications"] += 1
            except Exception:
                logger.exception("push verifications failed")
            # Signal logs
            try:
                for sl in sdb.list_all_signal_logs_full():
                    tg = sl.get("telegram_id")
                    if not tg:
                        continue
                    cur.execute("SELECT id FROM users WHERE telegram_id=%s", (int(tg),))
                    ru = cur.fetchone()
                    if not ru:
                        continue
                    uid = int(ru["id"]) if isinstance(ru, dict) else int(ru[0])
                    cur.execute(
                        """
                        INSERT INTO signal_logs (id, user_id, telegram_id, pair, timeframe, direction, entry_price, entry_time, source, message_id, raw_text,
                                                 exit_price, exit_time, pnl_pct, outcome, evaluated_at, created_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s, COALESCE(%s, NOW()), %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()))
                        ON CONFLICT (id) DO UPDATE SET
                          user_id=EXCLUDED.user_id,
                          telegram_id=EXCLUDED.telegram_id,
                          pair=EXCLUDED.pair,
                          timeframe=EXCLUDED.timeframe,
                          direction=EXCLUDED.direction,
                          entry_price=EXCLUDED.entry_price,
                          entry_time=EXCLUDED.entry_time,
                          source=EXCLUDED.source,
                          message_id=EXCLUDED.message_id,
                          raw_text=EXCLUDED.raw_text,
                          exit_price=EXCLUDED.exit_price,
                          exit_time=EXCLUDED.exit_time,
                          pnl_pct=EXCLUDED.pnl_pct,
                          outcome=EXCLUDED.outcome,
                          evaluated_at=EXCLUDED.evaluated_at
                        """,
                        (
                            sl.get("id"), uid, int(tg), sl.get("pair"), sl.get("timeframe"), sl.get("direction"), sl.get("entry_price"), sl.get("entry_time"),
                            sl.get("source"), sl.get("message_id"), sl.get("raw_text"), sl.get("exit_price"), sl.get("exit_time"), sl.get("pnl_pct"), sl.get("outcome"), sl.get("evaluated_at"), sl.get("created_at")
                        ),
                    )
                    counts["signal_logs"] += 1
            except Exception:
                logger.exception("push signal_logs failed")
    except Exception:
        logger.exception("push_all failed")
    db.log_admin("sync_push_all", counts, performed_by="panel")
    flash(f"Push complete: {counts}", "success")
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
            telebot.types.BotCommand("premium", "Payment options"),
            telebot.types.BotCommand("help", "Help"),
        ])
    except Exception:
        pass
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
        kb.add(types.InlineKeyboardButton("🔥 24H VIP PROFIT", callback_data="menu:perf24h"))
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
        kb.add(
            types.InlineKeyboardButton("🏠 Main Menu", callback_data="menu:root"),
            types.InlineKeyboardButton("👤 Profile", callback_data="menu:profile"),
        )
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
        registered = bool(user)
        if not registered:
            kb.add(types.KeyboardButton("✅ SIGN UP"), types.KeyboardButton("🔑 LOGIN"))
        else:
            kb.add(types.KeyboardButton("👤 PROFILE"))
        kb.add(types.KeyboardButton("🚀 GET STARTED"), types.KeyboardButton("❓ HOW IT WORKS"))
        kb.add(types.KeyboardButton("📈 LIVE SIGNALS"), types.KeyboardButton("📊 ANALYSIS TOOLS"))
        kb.add(types.KeyboardButton("🔥 24H VIP PROFIT"))
        kb.add(types.KeyboardButton("🕒 MARKET HOURS"), types.KeyboardButton("📅 PLAN STATUS"))
        kb.add(types.KeyboardButton("💳 BUY PREMIUM"))
        kb.add(types.KeyboardButton("🧾 UPLOAD RECEIPT"))
        kb.add(types.KeyboardButton("💬 SUPPORT"))
        kb.add(types.KeyboardButton("⚠️ RISK DISCLAIMER"))
        return kb

    SIGNAL_LAST: dict[int, str] = {}

    def build_assets_reply_kb():
        kb = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
        kb.add(types.KeyboardButton("BTC/USDT"), types.KeyboardButton("ETH/USDT"))
        kb.add(types.KeyboardButton("EUR/USD"), types.KeyboardButton("GBP/JPY"))
        kb.add(types.KeyboardButton("GOLD"), types.KeyboardButton("NASDAQ"))
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
            bot.send_message(call.message.chat.id, text, reply_markup=build_payment_kb())
        except Exception:
            pass

    # --- Receipt upload handlers ---
    @bot.message_handler(content_types=["photo"])
    def on_receipt_photo(m: types.Message):
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
        utils.send_safe(bot, m.chat.id, "Commands:\n/id\n/status\n/premium\n/menu\n/signal\n/hours\n/verify_upi <txn_id>\n/verify_usdt <tx_hash>")

    @bot.message_handler(commands=["menu"])
    def cmd_menu(m: types.Message):
        user = db.get_user_by_telegram_id(m.from_user.id)
        try:
            bot.send_message(m.chat.id, "Choose an option:", reply_markup=build_main_reply_kb(user))
        except Exception:
            pass

    @bot.message_handler(commands=["signal"])
    def cmd_signal(m: types.Message):
        user = db.get_user_by_telegram_id(m.from_user.id)
        uid = m.from_user.id
        if not _user_has_premium(user):
            # Allow 1 free sample per day
            today = datetime.now(timezone.utc).date().isoformat()
            if FREE_SAMPLES.get(uid) == today:
                utils.send_safe(bot, m.chat.id, "🎟️ Free sample used for today. Upgrade to premium to continue.")
                return
            _send_kb_quietly(m.chat.id, build_assets_reply_kb())
            return
        _send_kb_quietly(m.chat.id, build_assets_reply_kb())

    @bot.message_handler(commands=["hours"])
    def cmd_hours(m: types.Message):
        msg = utils.market_hours_message()
        try:
            bot.send_message(m.chat.id, msg, reply_markup=build_basic_nav_kb())
        except Exception:
            pass

    @bot.message_handler(commands=["id"])
    def cmd_id(m: types.Message):
        utils.send_safe(bot, m.chat.id, f"Your Telegram ID: <code>{m.from_user.id}</code>")

    @bot.message_handler(commands=["status"])
    def cmd_status(m: types.Message):
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
                bot.send_message(m.chat.id, "\n".join(lines), reply_markup=build_payment_kb())
            except Exception:
                pass
            return
        # Build plans list
        lines = ["Select a plan:"]
        for p in items:
            price_bits = []
            if p.get('price_inr') is not None: price_bits.append(f"₹{int(p['price_inr'])}")
            if p.get('price_usdt') is not None: price_bits.append(f"${p['price_usdt']} USDT")
            lines.append(f"• {p.get('name')} — {p.get('days')}d ({', '.join(price_bits)})")
        try:
            bot.send_message(m.chat.id, "\n".join(lines), reply_markup=build_products_kb())
        except Exception:
            pass

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
        try:
            db.touch_user_activity(m.from_user.id, saw=True, messaged=True)
        except Exception:
            pass
        txt = (m.text or "").strip().lower()
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
        if "buy premium" in txt or "payment" in txt or txt == "premium" or "renew" in txt:
            # Show plans via inline keyboard
            try:
                items = db.list_products(active_only=True)
            except Exception:
                items = []
            if not items:
                cmd_premium(m)
                return
            lines = ["Select a plan:"]
            for p in items:
                price_bits = []
                if p.get('price_inr') is not None: price_bits.append(f"₹{int(p['price_inr'])}")
                if p.get('price_usdt') is not None: price_bits.append(f"${p['price_usdt']} USDT")
                lines.append(f"• {p.get('name')} — {p.get('days')}d ({', '.join(price_bits)})")
            try:
                bot.send_message(m.chat.id, "\n".join(lines), reply_markup=build_products_kb())
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
                _send_kb_quietly(m.chat.id, build_assets_reply_kb())
                return
            _send_kb_quietly(m.chat.id, build_assets_reply_kb())
            return

        # Existing shortcuts
        if txt in ("signal", "signals", "get signal", "get signals"):
            uid = m.from_user.id
            if not _user_has_premium(user):
                today = datetime.now(timezone.utc).date().isoformat()
                if FREE_SAMPLES.get(uid) == today:
                    utils.send_safe(bot, m.chat.id, "🎟️ Free sample used for today. Upgrade to premium to continue.")
                    return
                _send_kb_quietly(m.chat.id, build_assets_reply_kb())
            else:
                _send_kb_quietly(m.chat.id, build_assets_reply_kb())

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
            code_to_pair = {
                "BTCUSDT": "BTC/USDT",
                "ETHUSDT": "ETH/USDT",
                "EURUSD": "EUR/USD",
                "GBPJPY": "GBP/JPY",
                "GOLD": "GOLD",
                "NASDAQ": "NASDAQ",
            }
            pair = code_to_pair.get(asset_code, asset_code)
            text = utils.generate_ensemble_signal(pair, txt)
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
            if _user_has_premium(user):
                _send_kb_quietly(call.message.chat.id, build_assets_reply_kb())
                text = None
            else:
                today = datetime.now(timezone.utc).date().isoformat()
                if FREE_SAMPLES.get(uid) != today:
                    _send_kb_quietly(call.message.chat.id, build_assets_reply_kb())
                    text = None
                else:
                    text = "📈 Live signals are for premium users. Check PLAN STATUS for your subscription."
        elif action == "tools":
            text = "📊 Analysis tools coming soon."
        elif action == "hours":
            text = utils.market_hours_message()
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

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("pay:"))
    def on_pay_click(call: types.CallbackQuery):
        action = call.data.split(":", 1)[1]
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        if action == "verify_upi":
            txt = "Send your UPI transaction id using:\n<code>/verify_upi TXN_ID</code>"
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
            bot.send_message(call.message.chat.id, txt, reply_markup=build_basic_nav_kb())
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
                bot.send_message(call.message.chat.id, "Select an asset:", reply_markup=build_assets_kb())
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
        try:
            bot.send_message(call.message.chat.id, f"Timeframe for {asset_code}:", reply_markup=build_timeframes_kb(asset_code))
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
        code_to_pair = {
            "BTCUSDT": "BTC/USDT",
            "ETHUSDT": "ETH/USDT",
            "EURUSD": "EUR/USD",
            "GBPJPY": "GBP/JPY",
            "GOLD": "GOLD",
            "NASDAQ": "NASDAQ",
        }
        pair = code_to_pair.get(asset_code, asset_code)
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
        text = utils.generate_ensemble_signal(pair, tf)
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
