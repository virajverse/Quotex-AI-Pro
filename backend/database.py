import os
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.pool
import psycopg2.extras

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL and "sslmode=" not in DATABASE_URL:
    DATABASE_URL += ("&" if "?" in DATABASE_URL else "?") + "sslmode=require"
if not DATABASE_URL:
    logger.warning("DATABASE_URL not set")

pool: Optional[psycopg2.pool.SimpleConnectionPool] = None
if DATABASE_URL:
    pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=int(os.getenv("DB_POOL_MAX", "8")),
        dsn=DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

@contextmanager
def get_conn():
    if not pool:
        raise RuntimeError("Database pool not initialized")
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)

def to_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None: return None
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def _row_public_user(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": r["id"],
        "telegram_id": r["telegram_id"],
        "ident": r["ident"],
        "username": r.get("username"),
        "premium_active": r.get("premium_active"),
        "premium_expires_at": to_iso(r.get("premium_expires_at")),
        "created_at": to_iso(r.get("created_at")),
        "updated_at": to_iso(r.get("updated_at")),
    }

def get_total_users() -> int:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("SELECT COUNT(*) c FROM users")
        return int(cur.fetchone()["c"])

def get_stats() -> Dict[str, Any]:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("SELECT COUNT(*) total FROM users")
        total = int(cur.fetchone()["total"])
        cur.execute("SELECT COUNT(*) active FROM users WHERE premium_active")
        active = int(cur.fetchone()["active"])
        cur.execute("""
            SELECT
              (SELECT COUNT(*) FROM users WHERE premium_active AND (premium_expires_at::date = CURRENT_DATE + INTERVAL '1 day')) AS expiring_1d,
              (SELECT COUNT(*) FROM users WHERE premium_active AND (premium_expires_at::date = CURRENT_DATE + INTERVAL '3 days')) AS expiring_3d
        """)
        r = cur.fetchone()
        return {"total_users": total, "active_premium": active, "expiring_1d": int(r["expiring_1d"]), "expiring_3d": int(r["expiring_3d"])}

def upsert_user(telegram_id: int, username: str, first_name: str, last_name: str, lang_code: Optional[str]):
    username = (username or "").strip()
    ident = f"@{username.lower()}" if username else f"tg:{telegram_id}"
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""
        INSERT INTO users (telegram_id, ident, username, first_name, last_name, lang_code, last_seen_at, last_message_at)
        VALUES (%s,%s, NULLIF(%s,''), %s,%s,%s, NOW(), NOW())
        ON CONFLICT (telegram_id) DO UPDATE
          SET username=EXCLUDED.username, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
              lang_code=EXCLUDED.lang_code, ident=EXCLUDED.ident, last_seen_at=NOW()
        """, (telegram_id, ident, username, first_name, last_name, lang_code))

def touch_user_activity(telegram_id: int, saw: bool, messaged: bool):
    sets = []
    if saw: sets.append("last_seen_at=NOW()")
    if messaged: sets.append("last_message_at=NOW()")
    if not sets: return
    with get_conn() as c, c.cursor() as cur:
        cur.execute(f"UPDATE users SET {', '.join(sets)} WHERE telegram_id=%s", (telegram_id,))

def get_user_by_telegram_id(telegram_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE telegram_id=%s", (telegram_id,))
        r = cur.fetchone()
        return dict(r) if r else None

def resolve_user_by_ident(ident: str) -> Optional[Dict[str, Any]]:
    ident = (ident or "").strip()
    with get_conn() as c, c.cursor() as cur:
        if ident.isdigit():
            cur.execute("SELECT * FROM users WHERE telegram_id=%s", (int(ident),))
        elif ident.startswith("@"):
            cur.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(%s)", (ident[1:],))
        elif ident.startswith("tg:") and ident[3:].isdigit():
            cur.execute("SELECT * FROM users WHERE telegram_id=%s", (int(ident[3:]),))
        else:
            cur.execute("SELECT * FROM users WHERE ident=%s OR LOWER(username)=LOWER(%s)", (ident, ident))
        r = cur.fetchone()
        return dict(r) if r else None

def search_users(q: str) -> List[Dict[str, Any]]:
    q = (q or "").strip()
    with get_conn() as c, c.cursor() as cur:
        if not q:
            cur.execute("SELECT * FROM users ORDER BY id DESC LIMIT 50")
        elif q.isdigit():
            cur.execute("SELECT * FROM users WHERE telegram_id=%s OR id=%s ORDER BY id DESC LIMIT 50", (int(q), int(q)))
        else:
            cur.execute("SELECT * FROM users WHERE LOWER(username) LIKE LOWER(%s) OR LOWER(ident) LIKE LOWER(%s) ORDER BY id DESC LIMIT 50", (f"%{q}%", f"%{q}%"))
        return [_row_public_user(dict(r)) for r in cur.fetchall()]

def search_users_admin(q: str) -> List[Dict[str, Any]]:
    q = (q or "").strip()
    with get_conn() as c, c.cursor() as cur:
        if not q:
            cur.execute("SELECT * FROM users ORDER BY id DESC LIMIT 50")
        elif q.isdigit():
            cur.execute("SELECT * FROM users WHERE telegram_id=%s OR id=%s ORDER BY id DESC LIMIT 50", (int(q), int(q)))
        else:
            cur.execute("SELECT * FROM users WHERE LOWER(username) LIKE LOWER(%s) OR LOWER(ident) LIKE LOWER(%s) ORDER BY id DESC LIMIT 50", (f"%{q}%", f"%{q}%"))
        return [dict(r) for r in cur.fetchall()]

def grant_premium_by_user_id(user_id: int, days: int) -> datetime:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""
        UPDATE users SET premium_active=true,
               premium_expires_at=CASE
                 WHEN premium_expires_at IS NULL OR premium_expires_at < NOW() THEN NOW() + (%s||' days')::interval
                 ELSE premium_expires_at + (%s||' days')::interval
               END
        WHERE id=%s
        RETURNING premium_expires_at
        """, (days, days, user_id))
        return cur.fetchone()["premium_expires_at"]

def revoke_premium_by_user_id(user_id: int):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE users SET premium_active=false WHERE id=%s", (user_id,))

def list_users_for_broadcast(premium_only: bool) -> List[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        if premium_only:
            cur.execute("SELECT id, telegram_id FROM users WHERE premium_active")
        else:
            cur.execute("SELECT id, telegram_id FROM users")
        return [dict(r) for r in cur.fetchall()]

def insert_verification(user_id: int, method: str, status: str, tx_id: Optional[str], tx_hash: Optional[str], amount, currency, request_data=None, notes: Optional[str]=None) -> int:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""
        INSERT INTO verifications (user_id, method, status, tx_id, tx_hash, amount, currency, request_data, notes)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
        """, (user_id, method, status, tx_id, tx_hash, amount, currency, psycopg2.extras.Json(request_data or {}), notes))
        return int(cur.fetchone()["id"])

def log_admin(action: str, detail: Dict[str, Any], performed_by: str = "system", ip: Optional[str] = None):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("INSERT INTO admin_logs (action, performed_by, ip, detail) VALUES (%s,%s,%s,%s)", (action, performed_by, ip, psycopg2.extras.Json(detail or {})))

def get_users_expiring_in_days(days: int) -> List[Dict[str, Any]]:
    field = "reminded_d1_at" if days == 1 else "reminded_d3_at" if days == 3 else None
    if not field: return []
    with get_conn() as c, c.cursor() as cur:
        cur.execute(f"""
        SELECT id, telegram_id, premium_expires_at FROM users
        WHERE premium_active
          AND premium_expires_at::date = (CURRENT_DATE + INTERVAL '{days} days')
          AND {field} IS NULL
        """)
        return [dict(r) for r in cur.fetchall()]

def set_reminded(user_id: int, days: int):
    field = "reminded_d1_at" if days == 1 else "reminded_d3_at" if days == 3 else None
    if not field: return
    with get_conn() as c, c.cursor() as cur:
        cur.execute(f"UPDATE users SET {field}=NOW() WHERE id=%s", (user_id,))

def expire_past_due() -> int:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE users SET premium_active=false WHERE premium_active AND premium_expires_at < NOW()")
        return cur.rowcount


def consume_signal_by_telegram_id(telegram_id: int) -> Dict[str, Any]:
    """Consume one signal use for the given telegram user.
    Resets daily usage if day changed. Falls back to credits when daily limit reached.
    Returns {ok:bool, source:'daily'|'credit'|None, used_today:int, daily_limit:int, credits:int}
    """
    with get_conn() as c, c.cursor() as cur:
        cur.execute("SELECT id, signal_daily_limit, signal_used_today, signal_day, signal_credits FROM users WHERE telegram_id=%s FOR UPDATE", (telegram_id,))
        r = cur.fetchone()
        if not r:
            return {"ok": False, "error": "user_not_found"}
        uid = r["id"]
        limit = int(r.get("signal_daily_limit") or 0)
        used = int(r.get("signal_used_today") or 0)
        day = r.get("signal_day")
        credits = int(r.get("signal_credits") or 0)

        # Reset day if needed
        cur.execute("SELECT CURRENT_DATE AS today")
        today = cur.fetchone()["today"]
        if (day is None) or (day != today):
            used = 0
            cur.execute("UPDATE users SET signal_used_today=0, signal_day=CURRENT_DATE WHERE id=%s", (uid,))

        if used < limit:
            cur.execute("UPDATE users SET signal_used_today=signal_used_today+1 WHERE id=%s", (uid,))
            used += 1
            return {"ok": True, "source": "daily", "used_today": used, "daily_limit": limit, "credits": credits}
        if credits > 0:
            cur.execute("UPDATE users SET signal_credits=signal_credits-1 WHERE id=%s", (uid,))
            credits -= 1
            return {"ok": True, "source": "credit", "used_today": used, "daily_limit": limit, "credits": credits}
        return {"ok": False, "source": None, "used_today": used, "daily_limit": limit, "credits": credits}


def add_signal_credits_by_user_id(user_id: int, count: int):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE users SET signal_credits=GREATEST(signal_credits + %s, 0) WHERE id=%s", (count, user_id))


def set_signal_limit_by_user_id(user_id: int, limit: int):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE users SET signal_daily_limit=%s WHERE id=%s", (limit, user_id))
