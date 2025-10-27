import os
import logging
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL and "sslmode=" not in DATABASE_URL:
    DATABASE_URL += ("&" if "?" in DATABASE_URL else "?") + "sslmode=require"
if DATABASE_URL and "connect_timeout=" not in DATABASE_URL:
    DATABASE_URL += ("&" if "?" in DATABASE_URL else "?") + f"connect_timeout={int(os.getenv('DB_CONNECT_TIMEOUT','5'))}"
if not DATABASE_URL:
    logger.warning("DATABASE_URL not set")

pool: Optional[ConnectionPool] = None
if DATABASE_URL:
    # Connection pool for psycopg3
    pool = ConnectionPool(
        DATABASE_URL,
        min_size=1,
        max_size=int(os.getenv("DB_POOL_MAX", "8")),
    )

def ping() -> bool:
    """Return True if we can open a short connection and run SELECT 1."""
    if not pool:
        return False
    try:
        with pool.connection() as c, c.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception:
        return False

@contextmanager
def get_conn():
    if not pool:
        raise RuntimeError("Database pool not initialized")
    # psycopg3 pool provides a context manager yielding a connection
    with pool.connection() as conn:
        # Ensure dict rows everywhere by default
        try:
            conn.row_factory = dict_row
        except Exception:
            pass
        try:
            yield conn
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

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

def list_all_users_full() -> List[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT id, telegram_id, ident, username, first_name, last_name, lang_code,
                   premium_active, premium_expires_at,
                   signal_daily_limit, signal_used_today, signal_day, signal_credits,
                   last_seen_at, last_message_at, created_at, updated_at
            FROM users
            ORDER BY id ASC
            """
        )
        return [dict(r) for r in cur.fetchall()]

def list_all_products_full() -> List[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT id, name, description, days, price_inr, price_usdt, active, created_at
            FROM products
            ORDER BY id ASC
            """
        )
        return [dict(r) for r in cur.fetchall()]

def list_all_orders_full() -> List[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT o.*, u.telegram_id AS src_user_telegram_id
            FROM orders o
            JOIN users u ON u.id = o.user_id
            ORDER BY o.id ASC
            """
        )
        return [dict(r) for r in cur.fetchall()]

def list_all_verifications_full() -> List[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT v.*, u.telegram_id AS src_user_telegram_id
            FROM verifications v
            JOIN users u ON u.id = v.user_id
            ORDER BY v.id ASC
            """
        )
        return [dict(r) for r in cur.fetchall()]

def list_all_signal_logs_full(limit: int = 100000) -> List[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT id, user_id, telegram_id, pair, timeframe, direction, entry_price, entry_time, source, message_id, raw_text,
                   exit_price, exit_time, pnl_pct, outcome, evaluated_at, created_at
            FROM signal_logs
            ORDER BY id ASC
            LIMIT %s
            """,
            (limit,)
        )
        return [dict(r) for r in cur.fetchall()]

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

def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE id=%s", (user_id,))
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

def list_signal_logs_by_user(user_id: int, limit: int = 1000) -> list[dict]:
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT id, telegram_id, message_id
            FROM signal_logs
            WHERE user_id=%s AND message_id IS NOT NULL
            ORDER BY id DESC
            LIMIT %s
            """,
            (user_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]

def delete_signal_logs_by_user(user_id: int):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("DELETE FROM signal_logs WHERE user_id=%s", (user_id,))

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

def list_verifications(status: Optional[str] = None, method: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        if status and method:
            cur.execute("SELECT * FROM verifications WHERE status=%s AND method=%s ORDER BY id DESC LIMIT %s", (status, method, limit))
        elif status:
            cur.execute("SELECT * FROM verifications WHERE status=%s ORDER BY id DESC LIMIT %s", (status, limit))
        elif method:
            cur.execute("SELECT * FROM verifications WHERE method=%s ORDER BY id DESC LIMIT %s", (method, limit))
        else:
            cur.execute("SELECT * FROM verifications ORDER BY id DESC LIMIT %s", (limit,))
        return [dict(r) for r in cur.fetchall()]

def get_verification(verification_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM verifications WHERE id=%s", (verification_id,))
        r = cur.fetchone()
        return dict(r) if r else None

def set_verification_status(verification_id: int, status: str, notes: Optional[str] = None):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE verifications SET status=%s, notes=COALESCE(%s, notes), verified_at=NOW() WHERE id=%s", (status, notes, verification_id))

def list_orders(status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    with get_conn() as c, c.cursor() as cur:
        if status:
            cur.execute("SELECT * FROM orders WHERE status=%s ORDER BY id DESC LIMIT %s", (status, limit))
        else:
            cur.execute("SELECT * FROM orders ORDER BY id DESC LIMIT %s", (limit,))
        return [dict(r) for r in cur.fetchall()]

def insert_verification(user_id: int, method: str, status: str, tx_id: Optional[str], tx_hash: Optional[str], amount, currency, request_data=None, notes: Optional[str]=None) -> int:
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
        INSERT INTO verifications (user_id, method, status, tx_id, tx_hash, amount, currency, request_data, notes)
        VALUES (%s,%s,%s,%s,%s,%s,%s, CAST(%s AS JSONB), %s)
        RETURNING id
        """,
            (
                user_id,
                method,
                status,
                tx_id,
                tx_hash,
                amount,
                currency,
                json.dumps(request_data or {}),
                notes,
            ),
        )
        return int(cur.fetchone()["id"])

def log_admin(action: str, detail: Dict[str, Any], performed_by: str = "system", ip: Optional[str] = None):
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO admin_logs (action, performed_by, ip, detail) VALUES (%s,%s,%s, CAST(%s AS JSONB))",
            (action, performed_by, ip, json.dumps(detail or {})),
        )

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

# -------- Served signal logs (Postgres) --------
def insert_signal_log(user_id: int, telegram_id: int, pair: str, timeframe: str, direction: str, entry_price, source: str | None, message_id: int | None, raw_text: str | None, entry_time: str | None = None) -> int:
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            INSERT INTO signal_logs (user_id, telegram_id, pair, timeframe, direction, entry_price, entry_time, source, message_id, raw_text)
            VALUES (%s,%s,%s,%s,%s,%s,COALESCE(%s, NOW()),%s,%s,%s)
            RETURNING id
            """,
            (user_id, telegram_id, pair, timeframe, direction, entry_price, entry_time, source, message_id, raw_text),
        )
        return int(cur.fetchone()["id"])

def update_signal_evaluation(log_id: int, exit_price, exit_time_iso: str | None, pnl_pct: float | None, outcome: str | None):
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            UPDATE signal_logs
            SET exit_price=COALESCE(%s, exit_price),
                exit_time=COALESCE(%s::timestamptz, exit_time),
                pnl_pct=COALESCE(%s, pnl_pct),
                outcome=COALESCE(%s, outcome),
                evaluated_at=NOW()
            WHERE id=%s
            """,
            (exit_price, exit_time_iso, pnl_pct, outcome, log_id),
        )

def list_signal_logs_since(hours: int = 24, pairs: list[str] | None = None) -> list[dict]:
    with get_conn() as c, c.cursor() as cur:
        if pairs:
            cur.execute(
                """
                SELECT * FROM signal_logs
                WHERE entry_time >= (NOW() - (%s||' hours')::interval)
                  AND pair = ANY(%s)
                ORDER BY id DESC
                """,
                (hours, pairs),
            )
        else:
            cur.execute(
                """
                SELECT * FROM signal_logs
                WHERE entry_time >= (NOW() - (%s||' hours')::interval)
                ORDER BY id DESC
                """,
                (hours,),
            )
        return [dict(r) for r in cur.fetchall()]

# ---------- Products & Orders (Postgres) ----------

def ensure_default_products():
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            days INT NOT NULL,
            price_inr NUMERIC(18,2),
            price_usdt NUMERIC(18,6),
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        cur.execute("SELECT COUNT(*) c FROM products")
        if int(cur.fetchone()["c"]) == 0:
            plans = [
                ("Weekly", "7-day access", 7, 199.0, 3.0),
                ("Monthly", "30-day access", 30, 499.0, 7.0),
                ("Quarterly", "90-day access", 90, 1299.0, 18.0),
            ]
            for name, desc, days, inr, usdt in plans:
                cur.execute(
                    "INSERT INTO products (name, description, days, price_inr, price_usdt, active) VALUES (%s,%s,%s,%s,%s,TRUE)",
                    (name, desc, days, inr, usdt),
                )
        # Ensure a default UPI-only credit product exists
        try:
            cur.execute("SELECT id FROM products WHERE LOWER(name) LIKE LOWER('%credit%') AND days=0 LIMIT 1")
            r = cur.fetchone()
            if not r:
                cur.execute(
                    "INSERT INTO products (name, description, days, price_inr, price_usdt, active) VALUES (%s,%s,%s,%s,%s,TRUE)",
                    ("Credits x1", "Top-up credits (UPI only)", 0, 15.0, None),
                )
        except Exception:
            pass
        # Orders table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            product_id BIGINT REFERENCES products(id) ON DELETE SET NULL,
            method TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            amount NUMERIC(18,6),
            currency TEXT,
            tx_id TEXT,
            tx_hash TEXT,
            receipt_file_id TEXT,
            notes TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        # Add order_id to verifications if missing
        try:
            cur.execute("ALTER TABLE verifications ADD COLUMN order_id BIGINT REFERENCES orders(id)")
        except Exception:
            pass
        # Signal logs (served signals)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_logs (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                telegram_id BIGINT,
                pair TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price NUMERIC(24,10),
                entry_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                source TEXT,
                message_id BIGINT,
                raw_text TEXT,
                exit_price NUMERIC(24,10),
                exit_time TIMESTAMPTZ,
                pnl_pct NUMERIC(18,6),
                outcome TEXT,
                evaluated_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

def list_products(active_only: bool = True):
    with get_conn() as c, c.cursor() as cur:
        if active_only:
            cur.execute("SELECT * FROM products WHERE active ORDER BY id ASC")
        else:
            cur.execute("SELECT * FROM products ORDER BY id ASC")
        return [dict(r) for r in cur.fetchall()]

def get_product(product_id: int):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM products WHERE id=%s", (product_id,))
        r = cur.fetchone()
        return dict(r) if r else None

def create_order(user_id: int, product_id: int, method: str | None, amount: float | None, currency: str | None, status: str = 'pending') -> int:
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (user_id, product_id, method, status, amount, currency) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
            (user_id, product_id, method, status, amount, currency),
        )
        return int(cur.fetchone()["id"])

def set_order_status(order_id: int, status: str):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE orders SET status=%s WHERE id=%s", (status, order_id))

def update_order_tx(order_id: int, tx_id: str | None = None, tx_hash: str | None = None):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE orders SET tx_id=COALESCE(%s, tx_id), tx_hash=COALESCE(%s, tx_hash) WHERE id=%s", (tx_id, tx_hash, order_id))

def update_order_method(order_id: int, method: str):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE orders SET method=%s WHERE id=%s", (method, order_id))

def update_order_receipt(order_id: int, receipt_file_id: str, caption: str | None):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE orders SET receipt_file_id=%s, notes=COALESCE(%s, notes) WHERE id=%s", (receipt_file_id, caption, order_id))

def get_latest_pending_order_by_user_and_method(user_id: int, method: str | None):
    with get_conn() as c, c.cursor() as cur:
        if method:
            cur.execute("SELECT * FROM orders WHERE user_id=%s AND status IN ('pending','submitted') AND method=%s ORDER BY id DESC LIMIT 1", (user_id, method))
        else:
            cur.execute("SELECT * FROM orders WHERE user_id=%s AND status IN ('pending','submitted') ORDER BY id DESC LIMIT 1", (user_id,))
        r = cur.fetchone()
        return dict(r) if r else None

def get_order(order_id: int):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM orders WHERE id=%s", (order_id,))
        r = cur.fetchone()
        return dict(r) if r else None

def update_verification_order(verification_id: int, order_id: int):
    with get_conn() as c, c.cursor() as cur:
        try:
            cur.execute("UPDATE verifications SET order_id=%s WHERE id=%s", (order_id, verification_id))
        except Exception:
            # Column may be missing if schema not updated yet
            pass
