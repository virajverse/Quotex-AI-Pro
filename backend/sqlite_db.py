import os
import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

# SQLite database file path
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'bot.db')

def get_conn():
    """Get a SQLite database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

def init_db():
    """Initialize the SQLite database with required tables."""
    with get_conn() as conn:
        cursor = conn.cursor()
        
        # Users table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            username TEXT,
            first_name TEXT NOT NULL,
            last_name TEXT,
            lang_code TEXT,
            is_premium BOOLEAN DEFAULT 0,
            premium_until TIMESTAMP,
            is_admin BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_active TIMESTAMP,
            last_message TIMESTAMP,
            signal_credits INTEGER DEFAULT 0,
            signal_daily_used INTEGER DEFAULT 0,
            signal_daily_limit INTEGER DEFAULT 3,
            signal_last_used_date DATE,
            last_reminded_days INTEGER
        )
        ''')
        
        # Verifications table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS verifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            method TEXT NOT NULL,
            status TEXT NOT NULL,
            tx_id TEXT,
            tx_hash TEXT,
            amount REAL,
            currency TEXT,
            request_data TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        ''')
        
        # Admin logs table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            detail TEXT NOT NULL,
            performed_by TEXT NOT NULL,
            ip TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        # Products table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            days INTEGER NOT NULL,
            price_inr REAL,
            price_usdt REAL,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        # Orders table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER,
            method TEXT,                 -- 'upi' | 'usdt_trc20' | 'evm' | 'receipt'
            status TEXT NOT NULL DEFAULT 'pending', -- 'pending' | 'submitted' | 'approved' | 'rejected'
            amount REAL,
            currency TEXT,
            tx_id TEXT,
            tx_hash TEXT,
            receipt_file_id TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
        ''')

        # Add order_id column to verifications if not exists
        try:
            cursor.execute('ALTER TABLE verifications ADD COLUMN order_id INTEGER')
        except Exception:
            pass

        # Signal logs table (served signals)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS signal_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            telegram_id INTEGER,
            pair TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_price REAL,
            entry_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            message_id INTEGER,
            raw_text TEXT,
            exit_price REAL,
            exit_time TIMESTAMP,
            pnl_pct REAL,
            outcome TEXT,
            evaluated_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        ''')
        
        conn.commit()

# -------- More sync helpers (products, orders, verifications, signal_logs) --------
def list_all_products_full() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT id, name, description, days, price_inr, price_usdt, active, created_at
        FROM products ORDER BY id ASC
        ''')
        return [dict(r) for r in cursor.fetchall()]

def list_all_orders_full() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT o.*, u.telegram_id AS src_user_telegram_id
        FROM orders o
        JOIN users u ON u.id = o.user_id
        ORDER BY o.id ASC
        ''')
        return [dict(r) for r in cursor.fetchall()]

def list_all_verifications_full() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT v.*, u.telegram_id AS src_user_telegram_id
        FROM verifications v
        JOIN users u ON u.id = v.user_id
        ORDER BY v.id ASC
        ''')
        return [dict(r) for r in cursor.fetchall()]

def list_all_signal_logs_full(limit: int = 100000) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT id, user_id, telegram_id, pair, timeframe, direction, entry_price, entry_time, source, message_id, raw_text,
               exit_price, exit_time, pnl_pct, outcome, evaluated_at, created_at
        FROM signal_logs
        ORDER BY id ASC
        LIMIT ?
        ''', (limit,))
        return [dict(r) for r in cursor.fetchall()]

def upsert_product_full(row: Dict[str, Any]):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO products (id, name, description, days, price_inr, price_usdt, active, created_at)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            description=excluded.description,
            days=excluded.days,
            price_inr=excluded.price_inr,
            price_usdt=excluded.price_usdt,
            active=excluded.active
        ''', (
            row.get('id'), row.get('name'), row.get('description'), row.get('days'),
            row.get('price_inr'), row.get('price_usdt'),
            1 if (row.get('active') in (1, True, 't', 'true', '1')) else 0,
            row.get('created_at')
        ))
        conn.commit()

def upsert_order_full(row: Dict[str, Any]):
    tg = row.get('src_user_telegram_id') or row.get('telegram_id')
    if not tg:
        return
    u = get_user_by_telegram_id(int(tg))
    if not u:
        return
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO orders (id, user_id, product_id, method, status, amount, currency, tx_id, tx_hash, receipt_file_id, notes, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            user_id=excluded.user_id,
            product_id=excluded.product_id,
            method=excluded.method,
            status=excluded.status,
            amount=excluded.amount,
            currency=excluded.currency,
            tx_id=excluded.tx_id,
            tx_hash=excluded.tx_hash,
            receipt_file_id=excluded.receipt_file_id,
            notes=COALESCE(excluded.notes, notes)
        ''', (
            row.get('id'), u.get('id'), row.get('product_id'), row.get('method'), row.get('status'),
            row.get('amount'), row.get('currency'), row.get('tx_id'), row.get('tx_hash'), row.get('receipt_file_id'), row.get('notes'), row.get('created_at')
        ))
        conn.commit()

def upsert_verification_full(row: Dict[str, Any]):
    tg = row.get('src_user_telegram_id') or row.get('telegram_id')
    if not tg:
        return
    u = get_user_by_telegram_id(int(tg))
    if not u:
        return
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO verifications (id, user_id, method, status, tx_id, tx_hash, amount, currency, request_data, notes, created_at, order_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            user_id=excluded.user_id,
            method=excluded.method,
            status=excluded.status,
            tx_id=excluded.tx_id,
            tx_hash=excluded.tx_hash,
            amount=excluded.amount,
            currency=excluded.currency,
            request_data=COALESCE(excluded.request_data, request_data),
            notes=COALESCE(excluded.notes, notes),
            order_id=COALESCE(excluded.order_id, order_id)
        ''', (
            row.get('id'), u.get('id'), row.get('method'), row.get('status'), row.get('tx_id'), row.get('tx_hash'),
            row.get('amount'), row.get('currency'),
            (row.get('request_data') if isinstance(row.get('request_data'), str) else str(row.get('request_data')) if row.get('request_data') is not None else None),
            row.get('notes'), row.get('created_at'), row.get('order_id')
        ))
        conn.commit()

def insert_signal_log_full(row: Dict[str, Any]):
    tg = row.get('telegram_id') or row.get('src_user_telegram_id')
    if not tg:
        return
    u = get_user_by_telegram_id(int(tg))
    if not u:
        return
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO signal_logs (id, user_id, telegram_id, pair, timeframe, direction, entry_price, entry_time, source, message_id, raw_text,
                                 exit_price, exit_time, pnl_pct, outcome, evaluated_at, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            user_id=excluded.user_id,
            telegram_id=excluded.telegram_id,
            pair=excluded.pair,
            timeframe=excluded.timeframe,
            direction=excluded.direction,
            entry_price=excluded.entry_price,
            entry_time=excluded.entry_time,
            source=excluded.source,
            message_id=excluded.message_id,
            raw_text=excluded.raw_text,
            exit_price=excluded.exit_price,
            exit_time=excluded.exit_time,
            pnl_pct=excluded.pnl_pct,
            outcome=excluded.outcome,
            evaluated_at=excluded.evaluated_at
        ''', (
            row.get('id'), u.get('id'), int(tg), row.get('pair'), row.get('timeframe'), row.get('direction'), row.get('entry_price'), row.get('entry_time'),
            row.get('source'), row.get('message_id'), row.get('raw_text'), row.get('exit_price'), row.get('exit_time'), row.get('pnl_pct'), row.get('outcome'), row.get('evaluated_at'), row.get('created_at')
        ))
        conn.commit()

# Initialize database on import
init_db()

def to_iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None

def _row_public_user(r: sqlite3.Row) -> Dict[str, Any]:
    return {
        'id': r['id'],
        'username': r['username'],
        'first_name': r['first_name'],
        'last_name': r['last_name'],
        'is_premium': bool(r['is_premium']),
        'premium_until': to_iso(datetime.fromisoformat(r['premium_until'])) if r['premium_until'] else None,
        'created_at': to_iso(datetime.fromisoformat(r['created_at'])) if r['created_at'] else None,
    }

def get_total_users() -> int:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) as count FROM users')
        return cursor.fetchone()['count']

def get_stats() -> Dict[str, Any]:
    with get_conn() as conn:
        cursor = conn.cursor()
        # Total users
        cursor.execute('SELECT COUNT(*) as total_users FROM users')
        total_users = int(cursor.fetchone()['total_users'])
        # Active premium
        cursor.execute('SELECT COUNT(*) as c FROM users WHERE is_premium = 1')
        active_premium = int(cursor.fetchone()['c'])
        # Expiring soon (1d and 3d)
        cursor.execute("""
            SELECT
              (SELECT COUNT(*) FROM users WHERE is_premium = 1 AND DATE(premium_until) = DATE('now','+1 day')) AS d1,
              (SELECT COUNT(*) FROM users WHERE is_premium = 1 AND DATE(premium_until) = DATE('now','+3 day')) AS d3
        """)
        row = cursor.fetchone()
        expiring_1d = int(row['d1']) if row else 0
        expiring_3d = int(row['d3']) if row else 0
        return {
            'total_users': total_users,
            'active_premium': active_premium,
            'expiring_1d': expiring_1d,
            'expiring_3d': expiring_3d,
        }

def upsert_user(telegram_id: int, username: str, first_name: str, last_name: str, lang_code: Optional[str]):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO users (telegram_id, username, first_name, last_name, lang_code, last_active)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(telegram_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            lang_code = COALESCE(excluded.lang_code, lang_code),
            last_active = CURRENT_TIMESTAMP
        RETURNING *
        ''', (telegram_id, username, first_name, last_name, lang_code))
        
        user = dict(cursor.fetchone())
        conn.commit()
        return user

def get_user_by_telegram_id(telegram_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE telegram_id = ?', (telegram_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

def search_users(q: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT * FROM users 
        WHERE username LIKE ? OR first_name LIKE ? OR last_name LIKE ?
        LIMIT 50
        ''', (f'%{q}%', f'%{q}%', f'%{q}%'))
        
        return [dict(row) for row in cursor.fetchall()]

def list_verifications(status: Optional[str] = None, method: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        if status and method:
            cursor.execute('SELECT * FROM verifications WHERE status=? AND method=? ORDER BY id DESC LIMIT ?', (status, method, limit))
        elif status:
            cursor.execute('SELECT * FROM verifications WHERE status=? ORDER BY id DESC LIMIT ?', (status, limit))
        elif method:
            cursor.execute('SELECT * FROM verifications WHERE method=? ORDER BY id DESC LIMIT ?', (method, limit))
        else:
            cursor.execute('SELECT * FROM verifications ORDER BY id DESC LIMIT ?', (limit,))
        return [dict(row) for row in cursor.fetchall()]

def get_verification(verification_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM verifications WHERE id=?', (verification_id,))
        r = cursor.fetchone()
        return dict(r) if r else None

def set_verification_status(verification_id: int, status: str, notes: Optional[str] = None):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE verifications SET status=?, notes=COALESCE(?, notes) WHERE id=?', (status, notes, verification_id))
        conn.commit()

def list_orders(status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        if status:
            cursor.execute('SELECT * FROM orders WHERE status=? ORDER BY id DESC LIMIT ?', (status, limit))
        else:
            cursor.execute('SELECT * FROM orders ORDER BY id DESC LIMIT ?', (limit,))
        return [dict(row) for row in cursor.fetchall()]

# -------- Products --------
def create_product(name: str, days: int, price_inr: Optional[float], price_usdt: Optional[float], description: Optional[str] = None, active: bool = True) -> int:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO products (name, description, days, price_inr, price_usdt, active)
        VALUES (?,?,?,?,?,?)
        ''', (name, description, days, price_inr, price_usdt, 1 if active else 0))
        conn.commit()
        return cursor.lastrowid

def update_product(product_id: int, name: Optional[str] = None, days: Optional[int] = None, price_inr: Optional[float] = None, price_usdt: Optional[float] = None, description: Optional[str] = None, active: Optional[bool] = None):
    with get_conn() as conn:
        cursor = conn.cursor()
        sets = []
        params = []
        if name is not None: sets.append('name=?'); params.append(name)
        if description is not None: sets.append('description=?'); params.append(description)
        if days is not None: sets.append('days=?'); params.append(days)
        if price_inr is not None: sets.append('price_inr=?'); params.append(price_inr)
        if price_usdt is not None: sets.append('price_usdt=?'); params.append(price_usdt)
        if active is not None: sets.append('active=?'); params.append(1 if active else 0)
        if not sets:
            return
        params.append(product_id)
        cursor.execute(f"UPDATE products SET {', '.join(sets)} WHERE id=?", tuple(params))
        conn.commit()

def list_products(active_only: bool = True) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        if active_only:
            cursor.execute('SELECT * FROM products WHERE active=1 ORDER BY id ASC')
        else:
            cursor.execute('SELECT * FROM products ORDER BY id ASC')
        return [dict(row) for row in cursor.fetchall()]

def get_product(product_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM products WHERE id=?', (product_id,))
        r = cursor.fetchone()
        return dict(r) if r else None

def ensure_default_products():
    """Seed default subscription products if none exist."""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) c FROM products')
        cnt = int(cursor.fetchone()['c'])
        if cnt > 0:
            return
        plans = [
            {"name": "Weekly", "days": 7, "price_inr": 199.0, "price_usdt": 3.0, "description": "7-day access"},
            {"name": "Monthly", "days": 30, "price_inr": 499.0, "price_usdt": 7.0, "description": "30-day access"},
            {"name": "Quarterly", "days": 90, "price_inr": 1299.0, "price_usdt": 18.0, "description": "90-day access"},
        ]
        for p in plans:
            cursor.execute(
                'INSERT INTO products (name, description, days, price_inr, price_usdt, active) VALUES (?,?,?,?,?,1)',
                (p['name'], p['description'], p['days'], p['price_inr'], p['price_usdt'])
            )
        conn.commit()

def delete_product(product_id: int):
    # soft delete
    update_product(product_id, active=False)

# -------- Orders --------
def create_order(user_id: int, product_id: Optional[int], method: Optional[str], amount: Optional[float], currency: Optional[str], status: str = 'pending') -> int:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO orders (user_id, product_id, method, status, amount, currency)
        VALUES (?,?,?,?,?,?)
        ''', (user_id, product_id, method, status, amount, currency))
        conn.commit()
        return cursor.lastrowid

def set_order_status(order_id: int, status: str):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE orders SET status=? WHERE id=?', (status, order_id))
        conn.commit()

def update_order_tx(order_id: int, tx_id: Optional[str] = None, tx_hash: Optional[str] = None):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE orders SET tx_id = COALESCE(?, tx_id), tx_hash = COALESCE(?, tx_hash) WHERE id=?', (tx_id, tx_hash, order_id))
        conn.commit()

def update_order_method(order_id: int, method: str):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE orders SET method=? WHERE id=?', (method, order_id))
        conn.commit()

def update_order_receipt(order_id: int, receipt_file_id: str, caption: Optional[str]):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE orders SET receipt_file_id=?, notes=COALESCE(?, notes) WHERE id=?', (receipt_file_id, caption, order_id))
        conn.commit()

def get_latest_pending_order_by_user_and_method(user_id: int, method: Optional[str]) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        if method:
            cursor.execute('SELECT * FROM orders WHERE user_id=? AND status IN ("pending","submitted") AND method=? ORDER BY id DESC LIMIT 1', (user_id, method))
        else:
            cursor.execute('SELECT * FROM orders WHERE user_id=? AND status IN ("pending","submitted") ORDER BY id DESC LIMIT 1', (user_id,))
        r = cursor.fetchone()
        return dict(r) if r else None

def get_order(order_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM orders WHERE id=?', (order_id,))
        r = cursor.fetchone()
        return dict(r) if r else None

def search_users_admin(q: str) -> List[Dict[str, Any]]:
    """Return rows shaped for admin UI expectations.
    Fields: id, telegram_id, ident, username, premium_active, premium_expires_at,
            signal_used_today, signal_daily_limit, signal_credits
    """
    with get_conn() as conn:
        cursor = conn.cursor()
        if not q:
            cursor.execute('SELECT * FROM users ORDER BY id DESC LIMIT 50')
        elif q.isdigit():
            cursor.execute('SELECT * FROM users WHERE telegram_id = ? OR id = ? ORDER BY id DESC LIMIT 50', (int(q), int(q)))
        else:
            ql = f"%{q}%"
            cursor.execute('''
                SELECT * FROM users
                WHERE LOWER(username) LIKE LOWER(?) OR LOWER(first_name) LIKE LOWER(?) OR LOWER(last_name) LIKE LOWER(?)
                ORDER BY id DESC LIMIT 50
            ''', (ql, ql, ql))
        items = []
        for r in cursor.fetchall():
            d = dict(r)
            ident = f"@{(d.get('username') or '').strip()}" if d.get('username') else f"tg:{d.get('telegram_id')}"
            items.append({
                'id': d.get('id'),
                'telegram_id': d.get('telegram_id'),
                'ident': ident,
                'username': d.get('username'),
                'premium_active': bool(d.get('is_premium')),
                'premium_expires_at': d.get('premium_until'),
                'signal_used_today': d.get('signal_daily_used') or 0,
                'signal_daily_limit': d.get('signal_daily_limit') or 0,
                'signal_credits': d.get('signal_credits') or 0,
            })
        return items

def grant_premium_by_user_id(user_id: int, days: int):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        UPDATE users 
        SET is_premium = 1,
            premium_until = CASE 
                WHEN premium_until IS NULL OR premium_until < CURRENT_TIMESTAMP 
                THEN datetime('now', ? || ' days')
                ELSE datetime(premium_until, ? || ' days')
            END
        WHERE id = ?
        ''', (f'+{days}', f'+{days}', user_id))
        conn.commit()

def revoke_premium_by_user_id(user_id: int):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_premium = 0, premium_until = NULL WHERE id = ?', (user_id,))
        conn.commit()

def insert_verification(user_id: int, method: str, status: str, tx_id: Optional[str], 
                      tx_hash: Optional[str], amount: float, currency: str, 
                      request_data: Optional[Dict] = None, notes: Optional[str] = None):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO verifications 
        (user_id, method, status, tx_id, tx_hash, amount, currency, request_data, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, method, status, tx_id, tx_hash, amount, currency, 
             str(request_data) if request_data else None, notes))
        vid = cursor.lastrowid
        conn.commit()
        return vid

def update_verification_order(verification_id: int, order_id: int):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE verifications SET order_id = ? WHERE id = ?', (order_id, verification_id))
        conn.commit()

def log_admin(action: str, detail: Dict[str, Any], performed_by: str = "system", ip: Optional[str] = None):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO admin_logs (action, detail, performed_by, ip)
        VALUES (?, ?, ?, ?)
        ''', (action, str(detail), performed_by, ip))
        conn.commit()

def list_users_for_broadcast(premium_only: bool) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        if premium_only:
            cursor.execute('SELECT id, telegram_id FROM users WHERE is_premium = 1')
        else:
            cursor.execute('SELECT id, telegram_id FROM users')
        return [dict(row) for row in cursor.fetchall()]

def consume_signal_by_telegram_id(telegram_id: int) -> Dict[str, Any]:
    with get_conn() as conn:
        cursor = conn.cursor()
        
        # Get user's current state
        cursor.execute('''
        SELECT id, signal_credits, signal_daily_used, signal_daily_limit, 
               signal_last_used_date, is_premium
        FROM users 
        WHERE telegram_id = ?
        ''', (telegram_id,))
        
        user = cursor.fetchone()
        if not user:
            return {'ok': False, 'error': 'User not found'}
            
        user_id = user['id']
        today = datetime.now().date().isoformat()
        
        # Reset daily usage if it's a new day
        if user['signal_last_used_date'] != today:
            cursor.execute('''
            UPDATE users 
            SET signal_daily_used = 0, 
                signal_last_used_date = ?
            WHERE id = ?
            ''', (today, user_id))
            daily_used = 0
        else:
            daily_used = user['signal_daily_used']
        
        # Check daily limit
        if daily_used < user['signal_daily_limit']:
            # Use from daily limit
            cursor.execute('''
            UPDATE users 
            SET signal_daily_used = signal_daily_used + 1,
                signal_last_used_date = ?
            WHERE id = ?
            ''', (today, user_id))
            conn.commit()
            return {
                'ok': True,
                'source': 'daily',
                'used_today': daily_used + 1,
                'daily_limit': user['signal_daily_limit'],
                'credits': user['signal_credits']
            }
        # Check credits if daily limit reached
        elif user['signal_credits'] > 0:
            cursor.execute('''
            UPDATE users 
            SET signal_credits = signal_credits - 1,
                signal_last_used_date = ?
            WHERE id = ?
            ''', (today, user_id))
            conn.commit()
            return {
                'ok': True,
                'source': 'credit',
                'used_today': daily_used,
                'daily_limit': user['signal_daily_limit'],
                'credits': user['signal_credits'] - 1
            }
        else:
            return {
                'ok': False,
                'error': 'Daily limit reached and no credits available',
                'used_today': daily_used,
                'daily_limit': user['signal_daily_limit'],
                'credits': 0
            }

def resolve_user_by_ident(ident: str) -> Optional[Dict[str, Any]]:
    ident = (ident or '').strip()
    if not ident:
        return None
    with get_conn() as conn:
        cursor = conn.cursor()
        if ident.isdigit():
            cursor.execute('SELECT * FROM users WHERE telegram_id = ?', (int(ident),))
        elif ident.startswith('@'):
            cursor.execute('SELECT * FROM users WHERE LOWER(username) = LOWER(?)', (ident[1:],))
        elif ident.startswith('tg:') and ident[3:].isdigit():
            cursor.execute('SELECT * FROM users WHERE telegram_id = ?', (int(ident[3:]),))
        else:
            # Fallback: try username and telegram id text
            cursor.execute('SELECT * FROM users WHERE LOWER(username) = LOWER(?)', (ident,))
        row = cursor.fetchone()
        return dict(row) if row else None

def get_users_expiring_in_days(days: int) -> List[Dict[str, Any]]:
    if days not in (1, 3):
        return []
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, telegram_id, premium_until as premium_expires_at
            FROM users
            WHERE is_premium = 1
              AND DATE(premium_until) = DATE('now', ?)
              AND (last_reminded_days IS NULL OR last_reminded_days != ?)
        ''', (f'+{days} day', days))
        return [dict(row) for row in cursor.fetchall()]

def set_reminded(user_id: int, days: int):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET last_reminded_days = ? WHERE id = ?', (days, user_id))
        conn.commit()

def expire_past_due() -> int:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_premium = 0 WHERE is_premium = 1 AND premium_until < CURRENT_TIMESTAMP')
        conn.commit()
        return cursor.rowcount if hasattr(cursor, 'rowcount') else 0

# -------- Sync helpers (SQLite full rows) --------
def list_all_users_full() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT id, telegram_id, username, first_name, last_name, lang_code,
               is_premium, premium_until,
               created_at, last_active, last_message,
               signal_credits, signal_daily_used, signal_daily_limit, signal_last_used_date
        FROM users
        ORDER BY id ASC
        ''')
        return [dict(r) for r in cursor.fetchall()]

def upsert_user_full(row: Dict[str, Any]):
    tg = int(row.get('telegram_id')) if row.get('telegram_id') else None
    if not tg:
        return
    username = (row.get('username') or '').strip()
    first_name = row.get('first_name')
    last_name = row.get('last_name')
    lang_code = row.get('lang_code')
    # Map PG -> SQLite
    is_premium = 1 if row.get('premium_active') or row.get('is_premium') else 0
    premium_until = row.get('premium_expires_at') or row.get('premium_until')
    last_active = row.get('last_seen_at') or row.get('last_active')
    last_message = row.get('last_message_at') or row.get('last_message')
    created_at = row.get('created_at')
    signal_daily_limit = row.get('signal_daily_limit') or 0
    signal_daily_used = row.get('signal_used_today') or row.get('signal_daily_used') or 0
    signal_last_used_date = row.get('signal_day') or row.get('signal_last_used_date')
    signal_credits = row.get('signal_credits') or 0
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO users (telegram_id, username, first_name, last_name, lang_code,
                           is_premium, premium_until, created_at, last_active, last_message,
                           signal_credits, signal_daily_used, signal_daily_limit, signal_last_used_date)
        VALUES (?,?,?,?,?, ?,?,?,?, ?,?,?,?,?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_name=excluded.last_name,
            lang_code=COALESCE(excluded.lang_code, lang_code),
            is_premium=excluded.is_premium,
            premium_until=excluded.premium_until,
            last_active=COALESCE(excluded.last_active, last_active),
            last_message=COALESCE(excluded.last_message, last_message),
            signal_credits=excluded.signal_credits,
            signal_daily_used=excluded.signal_daily_used,
            signal_daily_limit=excluded.signal_daily_limit,
            signal_last_used_date=excluded.signal_last_used_date
        ''', (
            tg, username, first_name, last_name, lang_code,
            is_premium, premium_until, created_at, last_active, last_message,
            int(signal_credits), int(signal_daily_used), int(signal_daily_limit), signal_last_used_date
        ))
        conn.commit()

def add_signal_credits_by_user_id(user_id: int, count: int):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        UPDATE users 
        SET signal_credits = signal_credits + ?
        WHERE id = ?
        ''', (count, user_id))
        conn.commit()

def set_signal_limit_by_user_id(user_id: int, limit: int):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        UPDATE users 
        SET signal_daily_limit = ?
        WHERE id = ?
        ''', (limit, user_id))
        conn.commit()

def touch_user_activity(telegram_id: int, saw: bool, messaged: bool):
    if not (saw or messaged):
        return
    with get_conn() as conn:
        cursor = conn.cursor()
        sets = []
        if saw:
            sets.append("last_active = CURRENT_TIMESTAMP")
        if messaged:
            sets.append("last_message = CURRENT_TIMESTAMP")
        sql = f"UPDATE users SET {', '.join(sets)} WHERE telegram_id = ?"
        cursor.execute(sql, (telegram_id,))
        conn.commit()


# -------- Served signal logs --------
def insert_signal_log(user_id: int, telegram_id: int, pair: str, timeframe: str, direction: str, entry_price: Optional[float], source: Optional[str], message_id: Optional[int], raw_text: Optional[str], entry_time: Optional[str] = None) -> int:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO signal_logs (user_id, telegram_id, pair, timeframe, direction, entry_price, entry_time, source, message_id, raw_text)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ''', (user_id, telegram_id, pair, timeframe, direction, entry_price, entry_time, source, message_id, raw_text))
        conn.commit()
        return cursor.lastrowid

def update_signal_evaluation(log_id: int, exit_price: Optional[float], exit_time_iso: Optional[str], pnl_pct: Optional[float], outcome: Optional[str]):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        UPDATE signal_logs
        SET exit_price = ?, exit_time = ?, pnl_pct = ?, outcome = ?, evaluated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        ''', (exit_price, exit_time_iso, pnl_pct, outcome, log_id))
        conn.commit()

def list_signal_logs_since(hours: int = 24, pairs: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        base = 'SELECT * FROM signal_logs WHERE entry_time >= datetime("now", ?)' 
        params: List[Any] = [f'-{int(hours)} hours']
        if pairs:
            placeholders = ','.join(['?'] * len(pairs))
            base += f' AND pair IN ({placeholders})'
            params.extend(pairs)
        base += ' ORDER BY id DESC'
        cursor.execute(base, tuple(params))
        return [dict(r) for r in cursor.fetchall()]

def list_signal_logs_pending(limit: int = 200) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT * FROM signal_logs
        WHERE outcome IS NULL OR outcome = ''
        ORDER BY id ASC
        LIMIT ?
        ''', (limit,))
        return [dict(r) for r in cursor.fetchall()]

def list_signal_logs_by_user(user_id: int, limit: int = 1000) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT id, telegram_id, message_id
        FROM signal_logs
        WHERE user_id = ? AND message_id IS NOT NULL
        ORDER BY id DESC
        LIMIT ?
        ''', (user_id, limit))
        return [dict(r) for r in cursor.fetchall()]

def delete_signal_logs_by_user(user_id: int):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM signal_logs WHERE user_id = ?', (user_id,))
        conn.commit()
