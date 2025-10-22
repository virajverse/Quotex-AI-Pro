import os
from flask import Blueprint, render_template, request, redirect, url_for, session, flash


def create_admin_blueprint(db, bot):
    admin_bp = Blueprint("admin", __name__, template_folder="templates")

    ADMIN_ID = os.getenv("ADMIN_ID", "")

    @admin_bp.before_request
    def check_admin():
        if request.endpoint in ("admin.login",):
            return
        if not session.get("admin_ok"):
            return redirect(url_for("admin.login"))

    @admin_bp.route("/login", methods=["GET", "POST"])
    def login():
        error = None
        if request.method == "POST":
            tid = request.form.get("telegram_id", "").strip()
            if tid and tid == ADMIN_ID:
                session["admin_ok"] = True
                session["admin_id"] = tid
                return redirect(url_for("admin.dashboard"))
            error = "Access Denied"
        return render_template("admin.html", view="login", error=error)

    @admin_bp.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("admin.login"))

    @admin_bp.route("/", methods=["GET"])    
    def dashboard():
        q = request.args.get("q", "").strip()
        users = db.search_users(q) if q else db.list_users()
        stats = {
            "total_users": db.stats_total_users(),
            "active_premium": db.stats_active_premium(),
            "new_signups_today": db.stats_new_signups_today(),
        }
        logs = db.recent_admin_logs(20)
        return render_template(
            "admin.html",
            view="dashboard",
            users=users,
            stats=stats,
            logs=logs,
        )

    @admin_bp.route("/grant", methods=["POST"])    
    def grant():
        ident = request.form.get("ident", "").strip()
        days = int(request.form.get("days", 30) or 30)
        target = ident
        if ident.isdigit():
            telegram_id = int(ident)
            db.grant_premium(telegram_id, days)
        else:
            u = db.get_user_by_email(ident)
            if not u:
                flash("User not found", "danger")
                return redirect(url_for("admin.dashboard"))
            telegram_id = int(u.get("telegram_id") or 0)
            if not telegram_id:
                flash("User has no Telegram ID linked", "warning")
                return redirect(url_for("admin.dashboard"))
            db.grant_premium(telegram_id, days)
        db.admin_log(session.get("admin_id", ""), f"grant_premium_{days}", str(target))
        flash("Premium granted", "success")
        return redirect(url_for("admin.dashboard"))

    @admin_bp.route("/revoke", methods=["POST"])    
    def revoke():
        ident = request.form.get("ident", "").strip()
        target = ident
        telegram_id = None
        if ident.isdigit():
            telegram_id = int(ident)
        else:
            u = db.get_user_by_email(ident)
            if u:
                telegram_id = int(u.get("telegram_id") or 0)
        if not telegram_id:
            flash("User not found", "danger")
            return redirect(url_for("admin.dashboard"))
        db.revoke_premium(telegram_id)
        db.admin_log(session.get("admin_id", ""), "revoke_premium", str(target))
        flash("Premium revoked", "warning")
        return redirect(url_for("admin.dashboard"))

    @admin_bp.route("/message", methods=["POST"])    
    def message_user():
        ident = request.form.get("ident", "").strip()
        text = request.form.get("text", "").strip()
        telegram_id = None
        if ident.isdigit():
            telegram_id = int(ident)
        else:
            u = db.get_user_by_email(ident)
            if u:
                telegram_id = int(u.get("telegram_id") or 0)
        if not telegram_id:
            flash("User not found", "danger")
            return redirect(url_for("admin.dashboard"))
        if not text:
            flash("Message empty", "warning")
            return redirect(url_for("admin.dashboard"))
        try:
            bot.send_message(telegram_id, text)
            db.admin_log(session.get("admin_id", ""), "send_message", str(telegram_id))
            flash("Message sent", "success")
        except Exception as e:
            flash("Failed to send message", "danger")
        return redirect(url_for("admin.dashboard"))

    @admin_bp.route("/broadcast", methods=["POST"])    
    def broadcast():
        text = request.form.get("text", "").strip()
        if not text:
            flash("Message empty", "warning")
            return redirect(url_for("admin.dashboard"))
        ids = db.get_premium_user_ids()
        sent = 0
        for uid in ids:
            try:
                bot.send_message(uid, text)
                sent += 1
            except Exception:
                pass
        db.admin_log(session.get("admin_id", ""), "broadcast_premium", f"{sent}/{len(ids)}")
        flash(f"Broadcast sent to {sent} users", "success")
        return redirect(url_for("admin.dashboard"))

    return admin_bp
