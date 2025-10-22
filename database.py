import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional


class Database:
    def __init__(self, path: str = None):
        self.path = path or os.getenv("DATABASE_PATH", "data.db")
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE,
                    name TEXT,
                    email TEXT UNIQUE,
                    is_premium INTEGER DEFAULT 0,
                    expires_at TEXT,
                    last_login TEXT,
                    logged_in INTEGER DEFAULT 0
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT DEFAULT (datetime('now')),
                    admin_id TEXT,
                    action TEXT,
                    target TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS verifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT DEFAULT (datetime('now')),
                    telegram_id INTEGER,
                    tx_id TEXT,
                    status TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS premium_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE,
                    created_at TEXT DEFAULT (datetime('now')),
                    matched_payment_id INTEGER
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT DEFAULT (datetime('now')),
                    network TEXT,
                    tx_hash TEXT UNIQUE,
                    from_address TEXT,
                    to_address TEXT,
                    amount REAL,
                    status TEXT,
                    matched_telegram_id INTEGER,
                    raw_json TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_verifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    type TEXT NOT NULL CHECK(type IN ('upi','usdt')),
                    data TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()

    def create_user(self, telegram_id: int, name: str, email: str):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO users (telegram_id, name, email, last_login, logged_in) VALUES (?, ?, ?, datetime('now'), 1)",
                (telegram_id, name, email),
            )
            cur.execute(
                "UPDATE users SET name=COALESCE(?, name), email=COALESCE(?, email), last_login=datetime('now'), logged_in=1 WHERE telegram_id=?",
                (name, email, telegram_id),
            )
            conn.commit()

    def update_last_login(self, telegram_id: int):
        with self._connect() as conn:
            conn.execute(
                "UPDATE users SET last_login=datetime('now') WHERE telegram_id=?",
                (telegram_id,),
            )
            conn.commit()

    def set_logged_in(self, telegram_id: int, logged_in: bool):
        with self._connect() as conn:
            conn.execute(
                "UPDATE users SET logged_in=? WHERE telegram_id=?",
                (1 if logged_in else 0, telegram_id),
            )
            conn.commit()

    def get_user_by_telegram(self, telegram_id: int):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM users WHERE telegram_id=?", (telegram_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def get_user_by_email(self, email: str):
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM users WHERE email=?", (email,))
            row = cur.fetchone()
            return dict(row) if row else None

    def link_email_to_telegram(self, email: str, telegram_id: int):
        with self._connect() as conn:
            conn.execute(
                "UPDATE users SET telegram_id=?, last_login=datetime('now'), logged_in=1 WHERE email=?",
                (telegram_id, email),
            )
            conn.commit()

    def grant_premium(self, telegram_id: int, days: int = 30):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT expires_at FROM users WHERE telegram_id=?", (telegram_id,))
            row = cur.fetchone()
            now = datetime.utcnow()
            if row and row[0]:
                try:
                    current_expiry = datetime.fromisoformat(row[0])
                except Exception:
                    current_expiry = now
            else:
                current_expiry = now
            base = current_expiry if current_expiry > now else now
            new_expiry = base + timedelta(days=days)
            cur.execute(
                "UPDATE users SET is_premium=1, expires_at=?, logged_in=1 WHERE telegram_id=?",
                (new_expiry.date().isoformat(), telegram_id),
            )
            conn.commit()

    def revoke_premium(self, telegram_id: int):
        with self._connect() as conn:
            conn.execute(
                "UPDATE users SET is_premium=0, expires_at=NULL WHERE telegram_id=?",
                (telegram_id,),
            )
            conn.commit()

    def is_premium_active(self, telegram_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT is_premium, expires_at FROM users WHERE telegram_id=?",
                (telegram_id,),
            )
            row = cur.fetchone()
            if not row:
                return False
            is_premium = row[0] == 1
            if not is_premium:
                return False
            expires_at = row[1]
            if not expires_at:
                return False
            try:
                exp = datetime.fromisoformat(expires_at)
            except Exception:
                return False
            return exp.date() >= datetime.utcnow().date()

    def list_users(self, limit: int = 200):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT id, telegram_id, name, email, is_premium, expires_at, last_login, logged_in FROM users ORDER BY last_login DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]

    def search_users(self, q: str, limit: int = 200):
        like = f"%{q}%"
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT id, telegram_id, name, email, is_premium, expires_at, last_login, logged_in FROM users WHERE CAST(telegram_id AS TEXT) LIKE ? OR name LIKE ? OR email LIKE ? ORDER BY last_login DESC LIMIT ?",
                (like, like, like, limit),
            )
            return [dict(r) for r in cur.fetchall()]

    def stats_total_users(self) -> int:
        with self._connect() as conn:
            cur = conn.execute("SELECT COUNT(*) FROM users")
            return cur.fetchone()[0]

    def stats_active_premium(self) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT COUNT(*) FROM users WHERE is_premium=1 AND expires_at >= date('now')"
            )
            return cur.fetchone()[0]

    def stats_new_signups_today(self) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT COUNT(*) FROM users WHERE date(last_login)=date('now')"
            )
            return cur.fetchone()[0]

    def get_premium_user_ids(self):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT telegram_id FROM users WHERE is_premium=1 AND expires_at >= date('now') AND telegram_id IS NOT NULL"
            )
            return [r[0] for r in cur.fetchall()]

    def admin_log(self, admin_id: str, action: str, target: str):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO admin_logs (admin_id, action, target) VALUES (?, ?, ?)",
                (admin_id, action, target),
            )
            conn.commit()

    def recent_admin_logs(self, limit: int = 20):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT created_at, admin_id, action, target FROM admin_logs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()] 

    def add_verification(self, telegram_id: int, tx_id: str, status: str = "pending"):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO verifications (telegram_id, tx_id, status) VALUES (?, ?, ?)",
                (telegram_id, tx_id, status),
            )
            conn.commit()

    def list_pending_verifications(self, limit: int = 50):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT id, created_at, telegram_id, tx_id, status FROM verifications WHERE status='pending' ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]

    def set_verification_status(self, verification_id: int, status: str):
        with self._connect() as conn:
            conn.execute(
                "UPDATE verifications SET status=? WHERE id=?",
                (status, verification_id),
            )
            conn.commit()

    def enqueue_premium_request(self, telegram_id: int):
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM premium_queue WHERE telegram_id=? AND matched_payment_id IS NOT NULL",
                (telegram_id,),
            )
            conn.execute(
                "INSERT OR IGNORE INTO premium_queue (telegram_id) VALUES (?)",
                (telegram_id,),
            )
            conn.commit()

    def remove_from_queue(self, telegram_id: int):
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM premium_queue WHERE telegram_id=?",
                (telegram_id,),
            )
            conn.commit()

    def pop_next_premium_request(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, telegram_id, created_at FROM premium_queue WHERE matched_payment_id IS NULL ORDER BY created_at ASC LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return None
            queue_id = row[0]
            conn.execute(
                "UPDATE premium_queue SET matched_payment_id=-1 WHERE id=?",
                (queue_id,),
            )
            conn.commit()
            return {"id": row[0], "telegram_id": row[1], "created_at": row[2]}

    def mark_queue_matched(self, queue_id: int, payment_id: int):
        with self._connect() as conn:
            conn.execute(
                "UPDATE premium_queue SET matched_payment_id=? WHERE id=?",
                (payment_id, queue_id),
            )
            conn.commit()

    def list_premium_queue(self, limit: int = 50):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT id, telegram_id, created_at, matched_payment_id FROM premium_queue ORDER BY created_at ASC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]

    def log_payment(self, network: str, tx_hash: str, from_address: str, to_address: str, amount: float, status: str, matched_telegram_id: Optional[int] = None, raw: Optional[dict] = None):
        raw_str = json.dumps(raw, ensure_ascii=False) if raw is not None else None
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO payment_logs (network, tx_hash, from_address, to_address, amount, status, matched_telegram_id, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (network, tx_hash, from_address, to_address, amount, status, matched_telegram_id, raw_str),
            )
            if cur.lastrowid:
                conn.commit()
                return cur.lastrowid, True
            cur.execute(
                "SELECT id, status, matched_telegram_id FROM payment_logs WHERE tx_hash=?",
                (tx_hash,),
            )
            row = cur.fetchone()
            conn.commit()
            if not row:
                return None, False
            return row[0], False

    def update_payment_status(self, payment_id: int, status: str, matched_telegram_id: Optional[int] = None):
        with self._connect() as conn:
            conn.execute(
                "UPDATE payment_logs SET status=?, matched_telegram_id=COALESCE(?, matched_telegram_id) WHERE id=?",
                (status, matched_telegram_id, payment_id),
            )
            conn.commit()

    def get_payment_by_tx(self, tx_hash: str):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT id, network, tx_hash, from_address, to_address, amount, status, matched_telegram_id FROM payment_logs WHERE tx_hash=?",
                (tx_hash,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def recent_payment_logs(self, limit: int = 50):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT id, created_at, network, tx_hash, from_address, to_address, amount, status, matched_telegram_id FROM payment_logs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]

    def add_pending_verification(self, telegram_id: int, vtype: str, data: str):
        if vtype not in ("upi", "usdt"):
            raise ValueError("invalid verification type")
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO pending_verifications (telegram_id, type, data) VALUES (?, ?, ?)",
                (telegram_id, vtype, data),
            )
            conn.commit()

    def list_pending_verifications_by_type(self, vtype: str, limit: int = 50):
        if vtype not in ("upi", "usdt"):
            raise ValueError("invalid verification type")
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT id, telegram_id, type, data, timestamp FROM pending_verifications WHERE type=? ORDER BY timestamp DESC LIMIT ?",
                (vtype, limit),
            )
            return [dict(r) for r in cur.fetchall()]
