import os
from flask import Blueprint, request, abort
from telebot import types


def create_webhook_blueprint(bot, db):
    webhook_bp = Blueprint("webhook", __name__, template_folder="templates")

    TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")

    @webhook_bp.route("/webhook/telegram", methods=["POST"])    
    def telegram_webhook():
        if TELEGRAM_WEBHOOK_SECRET:
            token = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if token != TELEGRAM_WEBHOOK_SECRET:
                abort(401)
        data = request.get_json(force=True, silent=True)
        if not data:
            return "ok"
        update = types.Update.de_json(data)
        bot.process_new_updates([update])
        return "ok"

    return webhook_bp
