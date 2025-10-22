import json
import os
import sqlite3
import time
import logging
import socket
from urllib.parse import urlparse, parse_qs, unquote
from datetime import datetime, timedelta, date
from typing import Optional
try:
    import psycopg2  # type: ignore
    import psycopg2.extras as pg_extras  # type: ignore
except Exception:  # psycopg2 is optional; only needed when DATABASE_URL is set
    psycopg2 = None
    pg_extras = None

try:
    import dns.resolver as dnsresolver  # type: ignore
except Exception:
    dnsresolver = None


def get_ipv4_address(hostname: str) -> str:
    """Resolve the first IPv4 address for hostname. If none found, return the original hostname.
    Logs any resolution errors via the 'database' logger.
    """
    logger = logging.getLogger("database")
    try:
        # family=AF_INET filters IPv4
        infos = socket.getaddrinfo(hostname, None, family=socket.AF_INET)
        for info in infos:
            try:
                addr = info[4][0]
                if addr:
                    return addr
            except Exception:
                continue
        logger.warning(f"No IPv4 A-record found for {hostname}; using hostname fallback")
    except Exception as e:
        logger.error(f"IPv4 resolution failed for {hostname}: {e}")
    if dnsresolver is not None:
        try:
            r = dnsresolver.Resolver()
            r.timeout = 2.0
            r.lifetime = 2.0
            r.nameservers = ["1.1.1.1", "8.8.8.8"]
            answers = r.resolve(hostname, "A")
            for ans in answers:
                ip = ans.to_text()
                if ip:
                    return ip
        except Exception as e:
            logger.error(f"dnspython IPv4 resolution failed for {hostname}: {e}")
    return hostname


class Database:
    def __init__(self, path: str = None):
        # Prefer Postgres if DATABASE_URL is provided; fallback to SQLite
        self.pg_url = (os.getenv("DATABASE_URL", "") or "").strip()
        self.is_pg = bool(self.pg_url.startswith("postgres"))
        self.pg_pooler_url = (os.getenv("DATABASE_URL_POOLER", "") or "").strip()
        self.path = path or os.getenv("DATABASE_PATH", "data.db")
        if not self.is_pg:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._init_db()
        self.logger = logging.getLogger("database")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter("[%(asctime)s] %(levelname)s in %(name)s: %(message)s")
            handler.setFormatter(fmt)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def _connect(self):
        if self.is_pg:
            if psycopg2 is None:
                raise RuntimeError("psycopg2 is required for Postgres but is not installed")
            # Parse DATABASE_URL and build connection kwargs
            parsed = urlparse(self.pg_url)
            host = parsed.hostname or "localhost"
            port = parsed.port or 5432
            dbname = (parsed.path or "/postgres").lstrip("/")
            user = unquote(parsed.username) if parsed.username else None
            password = unquote(parsed.password) if parsed.password else None
            q = parse_qs(parsed.query)
            # Force sslmode=require for hosted Supabase unless explicitly set in URL
            sslmode = (q.get("sslmode", ["require"]))[0] or "require"
            # Resolve to IPv4 address explicitly to avoid IPv6-only endpoints on Render free tier
            ipv4_host = get_ipv4_address(host)
            if ipv4_host != host:
                self.logger.info(f"DNS IPv4 resolution: {host} -> {ipv4_host}")
            # Use 'host' as hostname and 'hostaddr' as IPv4 to force IPv4 while keeping TLS hostname
            kwargs = {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "password": password,
                "sslmode": sslmode,
                "connect_timeout": 10,
                "application_name": "QuotexAI Pro",
            }
            if ipv4_host and ipv4_host != host:
                kwargs["hostaddr"] = ipv4_host
            # Retry logic: 1 initial + 2 retries
            last_err = None
            for attempt in range(3):
                try:
                    return psycopg2.connect(**kwargs)
                except psycopg2.OperationalError as e:
                    last_err = e
                    self.logger.error(f"Postgres connect failed (attempt {attempt+1}/3): {e}")
                    if attempt < 2:
                        time.sleep(2)
                        continue
            # Pooler fallback if provided
            if self.pg_pooler_url:
                parsed_p = urlparse(self.pg_pooler_url)
                host_p = parsed_p.hostname or host
                port_p = parsed_p.port or 6543
                dbname_p = (parsed_p.path or f"/{dbname}").lstrip("/")
                user_p = unquote(parsed_p.username) if parsed_p.username else user
                password_p = unquote(parsed_p.password) if parsed_p.password else password
                qp = parse_qs(parsed_p.query)
                sslmode_p = (qp.get("sslmode", [sslmode]))[0] or sslmode
                ipv4_host_p = get_ipv4_address(host_p)
                if ipv4_host_p != host_p:
                    self.logger.info(f"DNS IPv4 resolution (pooler): {host_p} -> {ipv4_host_p}")
                kwargs_p = {
                    "host": host_p,
                    "port": port_p,
                    "dbname": dbname_p,
                    "user": user_p,
                    "password": password_p,
                    "sslmode": sslmode_p,
                    "connect_timeout": 10,
                    "application_name": "QuotexAI Pro",
                }
                if ipv4_host_p and ipv4_host_p != host_p:
                    kwargs_p["hostaddr"] = ipv4_host_p
                for attempt in range(3):
                    try:
                        return psycopg2.connect(**kwargs_p)
                    except psycopg2.OperationalError as e:
                        self.logger.error(f"Postgres pooler connect failed (attempt {attempt+1}/3): {e}")
                        if attempt < 2:
                            time.sleep(2)
                            continue
                        raise
            raise last_err
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        if self.is_pg:
            # Schema should be created via supabase_schema.sql
            return
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

    def _cursor(self, conn):
        if self.is_pg:
            return conn.cursor(cursor_factory=pg_extras.RealDictCursor)
        return conn.cursor()

    def _execute(self, conn, sql: str, params: tuple = ()):  # helper to unify placeholders
        if self.is_pg:
            # Normalize SQLite-specific syntax to Postgres
            sql = sql.replace("?", "%s")
            sql = sql.replace("datetime('now')", "NOW()")
            sql = sql.replace("date('now')", "CURRENT_DATE")
            s = sql.lstrip()
            if s.upper().startswith("INSERT OR IGNORE INTO "):
                # Convert to INSERT ... ON CONFLICT DO NOTHING
                sql = sql.replace("INSERT OR IGNORE INTO", "INSERT INTO", 1) + " ON CONFLICT DO NOTHING"
            cur = self._cursor(conn)
            cur.execute(sql, params)
            return cur
        cur = self._cursor(conn)
        cur.execute(sql, params)
        return cur

    def create_user(self, telegram_id: int, name: str, email: str):
        ts = datetime.utcnow()
        with self._connect() as conn:
            self._execute(
                conn,
                "INSERT OR IGNORE INTO users (telegram_id, name, email, last_login, logged_in) VALUES (?, ?, ?, ?, ?)",
                (telegram_id, name, email, ts, True),
            )
            self._execute(
                conn,
                "UPDATE users SET name=COALESCE(?, name), email=COALESCE(?, email), last_login=?, logged_in=? WHERE telegram_id=?",
                (name, email, ts, True, telegram_id),
            )
            conn.commit()

    def update_last_login(self, telegram_id: int):
        ts = datetime.utcnow()
        with self._connect() as conn:
            self._execute(
                conn,
                "UPDATE users SET last_login=? WHERE telegram_id=?",
                (ts, telegram_id),
            )
            conn.commit()

    def set_logged_in(self, telegram_id: int, logged_in: bool):
        with self._connect() as conn:
            self._execute(
                conn,
                "UPDATE users SET logged_in=? WHERE telegram_id=?",
                (bool(logged_in), telegram_id),
            )
            conn.commit()

    def get_user_by_telegram(self, telegram_id: int):
        with self._connect() as conn:
            cur = self._execute(conn, "SELECT * FROM users WHERE telegram_id=?", (telegram_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_user_by_email(self, email: str):
        with self._connect() as conn:
            cur = self._execute(conn, "SELECT * FROM users WHERE email=?", (email,))
            row = cur.fetchone()
            return dict(row) if row else None

    def link_email_to_telegram(self, email: str, telegram_id: int):
        ts = datetime.utcnow()
        with self._connect() as conn:
            self._execute(
                conn,
                "UPDATE users SET telegram_id=?, last_login=?, logged_in=? WHERE email=?",
                (telegram_id, ts, True, email),
            )
            conn.commit()

    def grant_premium(self, telegram_id: int, days: int = 30):
        with self._connect() as conn:
            cur = self._execute(conn, "SELECT expires_at FROM users WHERE telegram_id=?", (telegram_id,))
            row = cur.fetchone()
            now = datetime.utcnow()
            if row:
                r = dict(row)
                val = r.get("expires_at")
                try:
                    if isinstance(val, date):
                        current_expiry = datetime.combine(val, datetime.min.time())
                    elif isinstance(val, str):
                        current_expiry = datetime.fromisoformat(val)
                    else:
                        current_expiry = now
                except Exception:
                    current_expiry = now
            else:
                current_expiry = now
            base = current_expiry if current_expiry > now else now
            new_expiry = base + timedelta(days=days)
            self._execute(
                conn,
                "UPDATE users SET is_premium=?, expires_at=?, logged_in=? WHERE telegram_id=?",
                (True, new_expiry.date(), True, telegram_id),
            )
            conn.commit()

    def revoke_premium(self, telegram_id: int):
        with self._connect() as conn:
            self._execute(
                conn,
                "UPDATE users SET is_premium=?, expires_at=NULL WHERE telegram_id=?",
                (False, telegram_id),
            )
            conn.commit()

    def is_premium_active(self, telegram_id: int) -> bool:
        with self._connect() as conn:
            cur = self._execute(conn, "SELECT is_premium, expires_at FROM users WHERE telegram_id=?", (telegram_id,))
            row = cur.fetchone()
            if not row:
                return False
            r = dict(row)
            is_premium = bool(r.get("is_premium"))
            if not is_premium:
                return False
            expires_at = r.get("expires_at")
            if not expires_at:
                return False
            try:
                if isinstance(expires_at, date):
                    expd = expires_at
                else:
                    expd = datetime.fromisoformat(str(expires_at)).date()
            except Exception:
                return False
            return expd >= datetime.utcnow().date()

    def list_users(self):
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT id, telegram_id, name, email, is_premium, expires_at, last_login, logged_in FROM users ORDER BY last_login DESC",
                (),
            )
            return [dict(r) for r in cur.fetchall()]

    # Compatibility wrapper per new API requirement
    def get_user(self, telegram_id: int):
        return self.get_user_by_telegram(telegram_id)

    def search_users(self, q: str, limit: int = 200):
        like = f"%{q}%"
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT id, telegram_id, name, email, is_premium, expires_at, last_login, logged_in FROM users WHERE CAST(telegram_id AS TEXT) LIKE ? OR name LIKE ? OR email LIKE ? ORDER BY last_login DESC LIMIT ?",
                (like, like, like, limit),
            )
            return [dict(r) for r in cur.fetchall()]

    def stats_total_users(self) -> int:
        with self._connect() as conn:
            cur = self._execute(conn, "SELECT COUNT(*) AS c FROM users")
            row = cur.fetchone()
            return int(dict(row)["c"]) if row else 0

    def stats_active_premium(self) -> int:
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT COUNT(*) AS c FROM users WHERE is_premium=? AND expires_at >= date('now')",
                (True,),
            )
            row = cur.fetchone()
            return int(dict(row)["c"]) if row else 0

    def stats_new_signups_today(self) -> int:
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT COUNT(*) AS c FROM users WHERE date(last_login)=date('now')",
                (),
            )
            row = cur.fetchone()
            return int(dict(row)["c"]) if row else 0

    def get_premium_user_ids(self):
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT telegram_id FROM users WHERE is_premium=? AND expires_at >= date('now') AND telegram_id IS NOT NULL",
                (True,),
            )
            rows = cur.fetchall()
            out = []
            for r in rows:
                d = dict(r)
                out.append(d.get("telegram_id"))
            return out

    def admin_log(self, admin_id: str, action: str, target: str):
        with self._connect() as conn:
            self._execute(
                conn,
                "INSERT INTO admin_logs (admin_id, action, target) VALUES (?, ?, ?)",
                (admin_id, action, target),
            )
            conn.commit()

    def recent_admin_logs(self, limit: int = 20):
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT created_at, admin_id, action, target FROM admin_logs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()] 

    def add_verification(self, telegram_id: int, tx_id: str, status: str = "pending"):
        with self._connect() as conn:
            self._execute(
                conn,
                "INSERT INTO verifications (telegram_id, tx_id, status) VALUES (?, ?, ?)",
                (telegram_id, tx_id, status),
            )
            conn.commit()

    def list_pending_verifications(self, limit: int = 50):
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT id, created_at, telegram_id, tx_id, status FROM verifications WHERE status='pending' ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]

    def set_verification_status(self, verification_id: int, status: str):
        with self._connect() as conn:
            self._execute(
                conn,
                "UPDATE verifications SET status=? WHERE id=?",
                (status, verification_id),
            )
            conn.commit()

    def enqueue_premium_request(self, telegram_id: int):
        with self._connect() as conn:
            self._execute(
                conn,
                "DELETE FROM premium_queue WHERE telegram_id=? AND matched_payment_id IS NOT NULL",
                (telegram_id,),
            )
            self._execute(
                conn,
                "INSERT OR IGNORE INTO premium_queue (telegram_id) VALUES (?)",
                (telegram_id,),
            )
            conn.commit()

    def remove_from_queue(self, telegram_id: int):
        with self._connect() as conn:
            self._execute(
                conn,
                "DELETE FROM premium_queue WHERE telegram_id=?",
                (telegram_id,),
            )
            conn.commit()

    def pop_next_premium_request(self):
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT id, telegram_id, created_at FROM premium_queue WHERE matched_payment_id IS NULL ORDER BY created_at ASC LIMIT 1",
                (),
            )
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            queue_id = d.get("id")
            self._execute(conn, "UPDATE premium_queue SET matched_payment_id=-1 WHERE id=?", (queue_id,))
            conn.commit()
            return {"id": d.get("id"), "telegram_id": d.get("telegram_id"), "created_at": d.get("created_at")}

    def mark_queue_matched(self, queue_id: int, payment_id: int):
        with self._connect() as conn:
            self._execute(
                conn,
                "UPDATE premium_queue SET matched_payment_id=? WHERE id=?",
                (payment_id, queue_id),
            )
            conn.commit()

    def list_premium_queue(self, limit: int = 50):
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT id, telegram_id, created_at, matched_payment_id FROM premium_queue ORDER BY created_at ASC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]

    def log_payment(self, network: str, tx_hash: str, from_address: str, to_address: str, amount: float, status: str, matched_telegram_id: Optional[int] = None, raw: Optional[dict] = None):
        raw_str = json.dumps(raw, ensure_ascii=False) if raw is not None else None
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "INSERT OR IGNORE INTO payment_logs (network, tx_hash, from_address, to_address, amount, status, matched_telegram_id, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (network, tx_hash, from_address, to_address, amount, status, matched_telegram_id, raw_str),
            )
            inserted = (cur.rowcount == 1)
            if inserted:
                conn.commit()
                # fetch id
                sel = self._execute(conn, "SELECT id FROM payment_logs WHERE tx_hash=?", (tx_hash,))
                r = sel.fetchone()
                return (dict(r).get("id") if r else None), True
            cur = self._execute(
                conn,
                "SELECT id, status, matched_telegram_id FROM payment_logs WHERE tx_hash=?",
                (tx_hash,),
            )
            row = cur.fetchone()
            conn.commit()
            if not row:
                return None, False
            d = dict(row)
            return d.get("id"), False

    def update_payment_status(self, payment_id: int, status: str, matched_telegram_id: Optional[int] = None):
        with self._connect() as conn:
            self._execute(
                conn,
                "UPDATE payment_logs SET status=?, matched_telegram_id=COALESCE(?, matched_telegram_id) WHERE id=?",
                (status, matched_telegram_id, payment_id),
            )
            conn.commit()

    def get_payment_by_tx(self, tx_hash: str):
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT id, network, tx_hash, from_address, to_address, amount, status, matched_telegram_id FROM payment_logs WHERE tx_hash=?",
                (tx_hash,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def recent_payment_logs(self, limit: int = 50):
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT id, created_at, network, tx_hash, from_address, to_address, amount, status, matched_telegram_id FROM payment_logs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]

    def add_pending_verification(self, telegram_id: int, vtype: str, data: str):
        if vtype not in ("upi", "usdt"):
            raise ValueError("invalid verification type")
        with self._connect() as conn:
            self._execute(
                conn,
                "INSERT INTO pending_verifications (telegram_id, type, data) VALUES (?, ?, ?)",
                (telegram_id, vtype, data),
            )
            conn.commit()

    def list_pending_verifications_by_type(self, vtype: str, limit: int = 50):
        if vtype not in ("upi", "usdt"):
            raise ValueError("invalid verification type")
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT id, telegram_id, type, data, timestamp FROM pending_verifications WHERE type=? ORDER BY timestamp DESC LIMIT ?",
                (vtype, limit),
            )
            return [dict(r) for r in cur.fetchall()]

    # Expiry reminder helpers
    def users_expiring_on(self, target_date: date, limit: int = 1000):
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT telegram_id, expires_at FROM users WHERE is_premium=? AND telegram_id IS NOT NULL AND expires_at=? ORDER BY telegram_id ASC LIMIT ?",
                (True, target_date, limit),
            )
            return [dict(r) for r in cur.fetchall()]

    def users_expired_before(self, today: date, limit: int = 1000):
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT telegram_id, expires_at FROM users WHERE is_premium=? AND telegram_id IS NOT NULL AND expires_at<? ORDER BY expires_at ASC LIMIT ?",
                (True, today, limit),
            )
            return [dict(r) for r in cur.fetchall()]

    def set_premium_status(self, telegram_id: int, is_premium: bool):
        with self._connect() as conn:
            self._execute(
                conn,
                "UPDATE users SET is_premium=? WHERE telegram_id=?",
                (bool(is_premium), telegram_id),
            )
            conn.commit()

    def has_sent_notice_today(self, telegram_id: int, action: str) -> bool:
        with self._connect() as conn:
            cur = self._execute(
                conn,
                "SELECT 1 AS x FROM admin_logs WHERE action=? AND target=? AND date(created_at)=date('now') LIMIT 1",
                (action, str(telegram_id)),
            )
            return cur.fetchone() is not None

    def record_notice(self, telegram_id: int, action: str):
        self.admin_log("system", action, str(telegram_id))
