import logging
import os
import random
import time as time_module
from datetime import datetime, timezone, time, timedelta
from typing import Optional, Dict, Any, List, Tuple

import requests
from html import escape as html_escape
from zoneinfo import ZoneInfo

def setup_logger():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

def escape_html(s: str) -> str:
    return html_escape(s or "", quote=False)

def to_iso(dt) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def format_ts_iso(dt) -> str:
    iso = to_iso(dt)
    if not iso:
        return "N/A"
    d = datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    return d.strftime("%Y-%m-%d %H:%M UTC")

def send_safe(bot, chat_id: int, text: str) -> bool:
    try:
        bot.send_message(chat_id, text)
        return True
    except Exception as e:
        logging.getLogger("bot").warning("send failed: %s", e)
        return False

# ---------- Fast cache (short-lived, in-memory) ----------
_FAST_CACHE: dict[str, tuple[float, any]] = {}

def _cache_get(key: str):
    exp_val = _FAST_CACHE.get(key)
    if not exp_val:
        return None
    exp, val = exp_val
    if time_module.time() > exp:
        try:
            del _FAST_CACHE[key]
        except Exception:
            pass
        return None
    return val

def _cache_set(key: str, val, ttl_sec: int = 4):
    try:
        _FAST_CACHE[key] = (time_module.time() + max(1, int(os.getenv("FAST_CACHE_TTL", str(ttl_sec)))), val)
    except Exception:
        pass

def verify_transaction(tx_hash: str) -> Dict[str, Any]:
    res = {"network": None, "found": False, "success": False}
    timeout = (5, 10)

    # TronGrid
    try:
        tg_key = os.getenv("TRONGRID_API_KEY", "").strip()
        if tg_key and len(tx_hash) >= 64:
            headers = {"TRON-PRO-API-KEY": tg_key}
            r = requests.get(f"https://api.trongrid.io/v1/transactions/{tx_hash}", headers=headers, timeout=timeout)
            if r.ok:
                data = r.json()
                items = data.get("data") or []
                if items:
                    res.update({"network": "tron", "found": True})
                    ret = items[0].get("ret") or []
                    succ = any(x.get("contractRet") == "SUCCESS" for x in ret)
                    res["success"] = bool(succ)
                    return res
    except Exception:
        pass

    # Etherscan-family
    def scan(api, key):
        try:
            if not key:
                return None
            u = f"{api}?module=transaction&action=gettxreceiptstatus&txhash={tx_hash}&apikey={key}"
            r = requests.get(u, timeout=timeout)
            if r.ok:
                j = r.json()
                status = (j.get("result") or {}).get("status")
                return {"found": True, "success": status == "1"}
        except Exception:
            return None
        return None

    for net, api, key in [
        ("ethereum", "https://api.etherscan.io/api", os.getenv("ETHERSCAN_API_KEY", "").strip()),
        ("bsc", "https://api.bscscan.com/api", os.getenv("BSCSCAN_API_KEY", "").strip()),
        ("polygon", "https://api.polygonscan.com/api", os.getenv("POLYGONSCAN_API_KEY", "").strip()),
    ]:
        r = scan(api, key)
        if r:
            res.update({"network": net, **r})
            break

    return res

def run_cron(db, bot) -> Dict[str, Any]:
    notices = 0

    for days, emoji in [(3, "⏰"), (1, "⚠️")]:
        users = db.get_users_expiring_in_days(days)
        for u in users:
            msg = f"{emoji} Reminder: Your premium expires in {days} day(s) on {format_ts_iso(u.get('premium_expires_at'))}."
            if bot:
                send_safe(bot, u["telegram_id"], msg)
            db.set_reminded(u["id"], days)
            notices += 1

    expired = db.expire_past_due()
    evaluated = evaluate_pending_signals(db)
    return {"notices": notices, "expired": expired, "evaluated": evaluated}


def evaluate_pending_signals(db, max_batch: int = 200) -> int:
    """Evaluate served signal logs missing outcome/pnl."""
    try:
        rows = db.list_signal_logs_pending(limit=max_batch)
    except Exception:
        return 0
    if not rows:
        return 0
    evaluated = 0
    for r in rows:
        pair = r.get("pair")
        tf = r.get("timeframe") or "5m"
        entry_time = r.get("entry_time") or r.get("created_at")
        direction = (r.get("direction") or "").upper()
        if not pair or not entry_time or direction not in ("UP", "DOWN"):
            continue
        cls = _classify_asset(pair)
        ev = None
        try:
            if cls == "crypto":
                ev = _eval_option_a_crypto(pair, tf, entry_time, direction)
            else:
                if os.getenv("FINNHUB_API_KEY", "").strip():
                    ev = _eval_option_a_finnhub(pair, tf, entry_time, direction)
        except Exception:
            ev = None
        if not ev or ev.get("exit_price") is None:
            continue
        try:
            db.update_signal_evaluation(
                r.get("id"),
                ev.get("exit_price"),
                ev.get("exit_time"),
                ev.get("pnl_pct"),
                ev.get("outcome"),
            )
            evaluated += 1
        except Exception:
            continue
    return evaluated


def generate_signal() -> str:
    """Generate a formatted, imaginary trading signal.
    Output is plain text with pair, direction, confidence, reason, and disclaimer.
    """
    assets = [
        "BTC/USDT",
        "ETH/USDT",
        "EUR/USD",
        "GBP/JPY",
        "GOLD",
        "NASDAQ",
    ]
    pair = random.choice(assets)

    direction_up = random.choice([True, False])
    direction = "UP" if direction_up else "DOWN"
    emoji = "📈" if direction_up else "📉"

    confidence = random.randint(3, 5)

    reasons_up = [
        "EMA50 crossed above EMA200, bullish bias",
        "MACD histogram turning positive",
        "RSI(14) above 55 indicating momentum",
        "Higher lows on 5m timeframe",
        "Price holding above VWAP",
    ]
    reasons_down = [
        "EMA50 crossed below EMA200, bearish bias",
        "MACD histogram turning negative",
        "RSI(14) below 45 indicating weakness",
        "Lower highs on 5m timeframe",
        "Price rejecting below VWAP",
    ]
    reason = ", ".join(random.sample(reasons_up if direction_up else reasons_down, k=2))

    return (
        f"{pair}\n"
        f"{emoji} Direction: {direction}\n"
        f"💡 Confidence: {confidence}/5\n"
        f"Reason: {reason}.\n"
        f"⚠️ This is not financial advice."
    )


def _fmt(dt: datetime, tz: Optional[ZoneInfo] = None) -> str:
    tz = tz or ZoneInfo(os.getenv("TIMEZONE", "UTC"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M %Z")


def _next_weekday(dt: datetime, weekday: int) -> datetime:
    # Monday=0 ... Sunday=6
    days_ahead = (weekday - dt.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return dt + timedelta(days=days_ahead)


def market_hours_message() -> str:
    """Build a human-friendly market hours status for key instruments.
    Shows OPEN/CLOSED and next open/close in configured timezone.
    """
    disp_tz = ZoneInfo(os.getenv("TIMEZONE", "UTC"))
    now_utc = datetime.now(timezone.utc)
    now_disp = now_utc.astimezone(disp_tz)

    # --- Crypto (24/7) ---
    crypto_lines = []
    for name in ("BTC/USDT", "ETH/USDT"):
        crypto_lines.append(f"{name} — OPEN (24/7)")

    # --- Forex (24x5) ---
    # Open: Sun >= 21:00 UTC to Fri < 21:00 UTC
    def fx_is_open(t: datetime) -> bool:
        wd = t.weekday()  # Mon=0..Sun=6
        hour = t.hour
        if wd in (0, 1, 2, 3):
            return True
        if wd == 4:
            return hour < 21
        if wd == 6:
            return hour >= 21
        return False

    def fx_next_open_close(t: datetime) -> Dict[str, Optional[datetime]]:
        open_now = fx_is_open(t)
        if open_now:
            # next close is Friday 21:00 UTC
            days_to_fri = (4 - t.weekday()) % 7
            close_day = (t + timedelta(days=days_to_fri)).date()
            close_dt = datetime.combine(close_day, time(21, 0), tzinfo=timezone.utc)
            if close_dt <= t:
                # already past close today; next week's Friday
                close_dt += timedelta(days=7)
            return {"open": None, "close": close_dt}
        else:
            # next open is Sunday 21:00 UTC
            days_to_sun = (6 - t.weekday()) % 7
            open_day = (t + timedelta(days=days_to_sun)).date()
            open_dt = datetime.combine(open_day, time(21, 0), tzinfo=timezone.utc)
            if open_dt <= t:
                open_dt += timedelta(days=7)
            return {"open": open_dt, "close": None}

    fx_pairs = ("EUR/USD", "GBP/JPY")
    fx_open = fx_is_open(now_utc)
    fx_times = fx_next_open_close(now_utc)
    if fx_open:
        fx_line = f"{', '.join(fx_pairs)} — OPEN (24x5) · Next close: {_fmt(fx_times['close'], disp_tz)}"
    else:
        fx_line = f"{', '.join(fx_pairs)} — CLOSED (Weekend) · Next open: {_fmt(fx_times['open'], disp_tz)}"

    # --- Gold (XAUUSD) approximate retail hours ---
    # Open: Sun 23:00 UTC → Fri 22:00 UTC, daily 1h break 22:00–23:00 UTC
    def gold_is_open(t: datetime) -> bool:
        wd = t.weekday()
        h = t.hour
        # daily break 22:00-23:00 UTC
        in_break = (h == 22)
        if in_break:
            return False
        if wd == 6:  # Sunday
            return h >= 23
        if wd in (0, 1, 2, 3):  # Mon-Thu
            return True
        if wd == 4:  # Friday
            return h < 22
        return False  # Saturday

    def gold_next_open_close(t: datetime) -> Dict[str, Optional[datetime]]:
        open_now = gold_is_open(t)
        if open_now:
            # Next close: if Fri before 22:00 then Fri 22:00, else next daily break 22:00
            wd = t.weekday()
            # close for the day is 22:00 UTC
            today_2200 = datetime.combine(t.date(), time(22, 0), tzinfo=timezone.utc)
            if t < today_2200:
                close_dt = today_2200
            else:
                # if passed 22:00 and not Friday close, next day 22:00
                close_dt = today_2200 + timedelta(days=1)
            # On Friday after 22:00 the market is closed until Sunday 23:00
            if wd == 4 and t >= today_2200:
                # next close already occurred; keep as today_2200
                pass
            return {"open": None, "close": close_dt}
        else:
            # If in daily break: next open at 23:00 UTC same day; else Sunday 23:00 UTC
            wd = t.weekday()
            h = t.hour
            if h == 22 and wd in (0, 1, 2, 3):  # Mon-Thu break
                open_dt = datetime.combine(t.date(), time(23, 0), tzinfo=timezone.utc)
            elif wd == 6 and h < 23:  # Sunday before open
                open_dt = datetime.combine(t.date(), time(23, 0), tzinfo=timezone.utc)
            else:
                # next Sunday 23:00 UTC
                next_sun = _next_weekday(t, 6)
                open_dt = datetime.combine(next_sun.date(), time(23, 0), tzinfo=timezone.utc)
            return {"open": open_dt, "close": None}

    gold_open = gold_is_open(now_utc)
    gold_times = gold_next_open_close(now_utc)
    gold_line = (
        f"GOLD — {'OPEN' if gold_open else 'CLOSED'} · "
        f"{'Next close: ' + _fmt(gold_times['close'], disp_tz) if gold_open else 'Next open: ' + _fmt(gold_times['open'], disp_tz)}"
    )

    # --- NASDAQ cash session ---
    ny = ZoneInfo("America/New_York")
    now_ny = now_utc.astimezone(ny)
    wd_ny = now_ny.weekday()
    open_start = time(9, 30)
    open_end = time(16, 0)
    in_window = (wd_ny < 5) and (open_start <= now_ny.time() < open_end)
    if in_window:
        close_dt_ny = datetime.combine(now_ny.date(), open_end, tzinfo=ny)
        ndq_line = f"NASDAQ — OPEN (US cash) · Next close: {_fmt(close_dt_ny.astimezone(timezone.utc))}"
    else:
        # compute next weekday open 9:30 NY
        next_day = now_ny
        while True:
            next_day = (next_day + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            if next_day.weekday() < 5:
                break
        open_dt_ny = datetime.combine(next_day.date(), open_start, tzinfo=ny)
        ndq_line = f"NASDAQ — CLOSED (US cash) · Next open: {_fmt(open_dt_ny.astimezone(timezone.utc))}"

    lines = [
        f"🕒 Now: {now_disp.strftime('%Y-%m-%d %H:%M %Z')}",
        "",
        *crypto_lines,
        fx_line,
        gold_line,
        ndq_line,
    ]
    return "\n".join(lines)


def market_hours_message_for_pairs(pairs: list[str]) -> str:
    disp_tz = ZoneInfo(os.getenv("TIMEZONE", "UTC"))
    now_utc = datetime.now(timezone.utc)
    now_disp = now_utc.astimezone(disp_tz)

    # FX open/close helpers (24x5) UTC
    def fx_is_open(t: datetime) -> bool:
        wd = t.weekday(); h = t.hour
        if wd in (0,1,2,3):
            return True
        if wd == 4:
            return h < 21
        if wd == 6:
            return h >= 21
        return False

    def fx_next_open_close(t: datetime) -> dict:
        if fx_is_open(t):
            days_to_fri = (4 - t.weekday()) % 7
            close_day = (t + timedelta(days=days_to_fri)).date()
            close_dt = datetime.combine(close_day, time(21, 0), tzinfo=timezone.utc)
            if close_dt <= t:
                close_dt += timedelta(days=7)
            return {"open": None, "close": close_dt}
        days_to_sun = (6 - t.weekday()) % 7
        open_day = (t + timedelta(days=days_to_sun)).date()
        open_dt = datetime.combine(open_day, time(21, 0), tzinfo=timezone.utc)
        if open_dt <= t:
            open_dt += timedelta(days=7)
        return {"open": open_dt, "close": None}

    fx_open = fx_is_open(now_utc)
    fx_times = fx_next_open_close(now_utc)
    fx_header = (
        f"LIVE FX — OPEN (24x5) · Next close: {_fmt(fx_times['close'], disp_tz)}"
        if fx_open else
        f"LIVE FX — CLOSED (Weekend) · Next open: {_fmt(fx_times['open'], disp_tz)}"
    )

    pairs = [p for p in pairs or [] if "/" in (p or "")]  # only valid
    open_pairs: list[str] = []
    closed_pairs: list[tuple[str, str]] = []  # (pair, next_open_str)
    for p in pairs:
        is_open = _market_open_for_asset(p, now_utc)
        if is_open:
            open_pairs.append(p)
        else:
            nxt_dt = next_open_for_asset(p, now_utc)
            nxt_str = _fmt(nxt_dt, disp_tz) if nxt_dt else "Unknown"
            closed_pairs.append((p, nxt_str))

    lines = [
        f"🕒 Now: {now_disp.strftime('%Y-%m-%d %H:%M %Z')}",
        fx_header,
        "",
        "LIVE now:",
        *( [f"• {p}" for p in open_pairs] if open_pairs else ["• None"] ),
        "",
        "CLOSED now:",
        *( [f"• {p} — Next open: {n}" for (p, n) in closed_pairs] if closed_pairs else ["• None"] ),
    ]
    return "\n".join(lines)

# ---------- Served signals (real) helpers ----------
def direction_from_signal_text(text: str) -> Optional[str]:
    t = (text or "").upper()
    if "DIRECTION:" in t:
        if "DIRECTION: UP" in t:
            return "UP"
        if "DIRECTION: DOWN" in t:
            return "DOWN"
    # Fallback keywords
    if "BUY" in t and "SELL" not in t:
        return "UP"
    if "SELL" in t and "BUY" not in t:
        return "DOWN"
    return None

def get_entry_price(pair: str, timeframe: str) -> Optional[float]:
    # Try provider priority for non-crypto if keys exist
    # Otherwise for crypto use Binance klines
    cls = _classify_asset(pair)
    if cls == "crypto":
        sym = _binance_symbol(pair)
        if not sym:
            return None
        kl = fetch_klines_binance(sym, timeframe, limit=2)
        if kl and len(kl) >= 1:
            try:
                return float(kl[-1][4])
            except Exception:
                pass
        # Fallback: try closes endpoint
        try:
            closes = fetch_ohlc_binance(sym, timeframe, limit=2)
            if closes:
                return float(closes[-1])
        except Exception:
            pass
        return None
    # Non-crypto: attempt Finnhub/Twelve/AlphaVantage closes (last close as proxy)
    normalized_tf = "1m" if timeframe == "3m" else timeframe
    # 1) Finnhub OHLC (normalized tf)
    closes = None
    try:
        closes = fetch_ohlc_finnhub(pair, normalized_tf, limit=2)
    except Exception:
        closes = None
    if closes:
        return float(closes[-1])
    # 2) Finnhub klines 1m (last close)
    try:
        kl = fetch_klines_finnhub(pair, "1m", limit=1)
        if kl:
            return float(kl[-1][4])
    except Exception:
        pass
    # 3) Finnhub OHLC 1m/5m fallbacks
    for tf_try in ("1m", "5m"):
        try:
            c2 = fetch_ohlc_finnhub(pair, tf_try, limit=2)
            if c2:
                return float(c2[-1])
        except Exception:
            pass
    # 4) Other providers if configured
    try:
        closes = fetch_ohlc_alphavantage(pair, normalized_tf, limit=2)
        if closes:
            return float(closes[-1])
    except Exception:
        pass
    try:
        closes = fetch_ohlc_twelvedata(pair, normalized_tf, limit=2)
        if closes:
            return float(closes[-1])
    except Exception:
        pass
    return None

def _seconds_for_tf(tf: str) -> int:
    return 60 if tf == "1m" else 180 if tf == "3m" else 300

def get_close_at_time(pair: str, timeframe: str, entry_iso: str) -> Optional[float]:
    try:
        from datetime import datetime
        entry_dt = datetime.fromisoformat(entry_iso.replace("Z", "+00:00"))
        entry_ms = int(entry_dt.timestamp() * 1000)
    except Exception:
        return None
    cls = _classify_asset(pair)
    if cls == "crypto":
        sym = _binance_symbol(pair)
        if not sym:
            return None
        for tf_try in (timeframe, "1m", "5m"):
            try:
                kl = fetch_klines_binance(sym, tf_try, limit=400)
                if not kl:
                    continue
                for k in kl:
                    ot, ct = int(k[0]), int(k[6])
                    if ot <= entry_ms < ct or entry_ms <= ot:
                        return float(k[4])
            except Exception:
                continue
        return None
    # Non-crypto via Finnhub if available
    if os.getenv("FINNHUB_API_KEY", "").strip():
        norm_tf = "1m" if timeframe == "3m" else timeframe
        for tf_try in (norm_tf, "1m", "5m"):
            try:
                kl = fetch_klines_finnhub(pair, tf_try, limit=400)
                if not kl:
                    continue
                for k in kl:
                    ot, ct = int(k[0]), int(k[6])
                    if ot <= entry_ms < ct or entry_ms <= ot:
                        return float(k[4])
            except Exception:
                continue
    return None

def _eval_option_a_crypto(pair: str, timeframe: str, entry_iso: str, direction: str) -> Optional[Dict[str, Any]]:
    sym = _binance_symbol(pair)
    if not sym:
        return None
    kl = fetch_klines_binance(sym, timeframe, limit=400)
    if not kl:
        return None
    try:
        from datetime import datetime
        entry_dt = datetime.fromisoformat(entry_iso.replace("Z", "+00:00"))
        entry_ms = int(entry_dt.timestamp() * 1000)
    except Exception:
        return None
    # Find bar that contains or starts after entry time
    idx = None
    for i, k in enumerate(kl):
        ot, ct = int(k[0]), int(k[6])
        if ot <= entry_ms < ct:
            idx = i
            break
        if entry_ms <= ot:
            idx = i
            break
    if idx is None:
        return None
    exit_idx = idx + 4
    if exit_idx >= len(kl):
        return None  # not enough candles yet
    entry_price = float(kl[idx][4])
    exit_price = float(kl[exit_idx][4])
    sign = 1.0 if direction == "UP" else -1.0
    pnl = (exit_price - entry_price) / entry_price * 100.0 * sign
    exit_iso = datetime.fromtimestamp(int(kl[exit_idx][6]) / 1000.0, tz=timezone.utc).isoformat()
    return {"entry_price": entry_price, "exit_price": exit_price, "pnl_pct": pnl, "exit_time": exit_iso, "outcome": ("WIN" if pnl > 0 else "LOSS")}

def _eval_option_a_finnhub(pair: str, timeframe: str, entry_iso: str, direction: str) -> Optional[Dict[str, Any]]:
    kl = fetch_klines_finnhub(pair, timeframe, limit=400)
    if not kl:
        return None
    try:
        from datetime import datetime
        entry_dt = datetime.fromisoformat(entry_iso.replace("Z", "+00:00"))
        entry_ms = int(entry_dt.timestamp() * 1000)
    except Exception:
        return None
    idx = None
    for i, k in enumerate(kl):
        ot, ct = int(k[0]), int(k[6])
        if ot <= entry_ms < ct:
            idx = i
            break
        if entry_ms <= ot:
            idx = i
            break
    if idx is None:
        return None
    exit_idx = idx + 4
    if exit_idx >= len(kl):
        return None
    entry_price = float(kl[idx][4])
    exit_price = float(kl[exit_idx][4])
    sign = 1.0 if direction == "UP" else -1.0
    pnl = (exit_price - entry_price) / entry_price * 100.0 * sign
    exit_iso = datetime.fromtimestamp(int(kl[exit_idx][6]) / 1000.0, tz=timezone.utc).isoformat()
    return {"entry_price": entry_price, "exit_price": exit_price, "pnl_pct": pnl, "exit_time": exit_iso, "outcome": ("WIN" if pnl > 0 else "LOSS")}

def generate_24h_served_report(db, hours: int = 24) -> str:
    items = []
    try:
        items = db.list_signal_logs_since(hours=hours)
    except Exception:
        items = []
    if not items:
        return "No served signals in the last 24 hours."
    by_pair: Dict[str, List[Dict[str, Any]]] = {}
    for r in items:
        by_pair.setdefault(r.get("pair"), []).append(r)
    # Evaluate unevaluated crypto signals if horizon passed
    lines: List[str] = ["📈 24H PERFORMANCE — Served Signals (Real)", ""]
    total = 0
    wins = 0
    losses = 0
    total_pnl = 0.0
    for pair, logs in by_pair.items():
        lines.append(f"🔹 {pair}")
        pair_w = 0
        pair_l = 0
        pair_pnl = 0.0
        for r in reversed(logs):
            direction = (r.get("direction") or "").upper()
            tf = r.get("timeframe") or "5m"
            entry_time = r.get("entry_time") or r.get("created_at")
            entry_price = r.get("entry_price")
            outcome = r.get("outcome")
            pnl = r.get("pnl_pct")
            exit_price = r.get("exit_price")
            exit_time = r.get("exit_time")
            # Attempt evaluation if missing
            cls = _classify_asset(pair)
            ev = None
            if not outcome and entry_time:
                try:
                    if cls == "crypto":
                        ev = _eval_option_a_crypto(pair, tf, entry_time, direction)
                    else:
                        if os.getenv("FINNHUB_API_KEY", "").strip():
                            ev = _eval_option_a_finnhub(pair, tf, entry_time, direction)
                except Exception:
                    ev = None
            if ev and ev.get("exit_price") is not None:
                try:
                    db.update_signal_evaluation(r.get("id"), ev.get("exit_price"), ev.get("exit_time"), ev.get("pnl_pct"), ev.get("outcome"))
                except Exception:
                    pass
                pnl = ev.get("pnl_pct")
                exit_price = ev.get("exit_price")
                exit_time = ev.get("exit_time")
                outcome = ev.get("outcome")
                if entry_price is None:
                    entry_price = ev.get("entry_price")
            # Accumulate metrics
            if outcome:
                total += 1
                if outcome == "WIN":
                    wins += 1
                    pair_w += 1
                else:
                    losses += 1
                    pair_l += 1
                if pnl is not None:
                    total_pnl += float(pnl)
                    pair_pnl += float(pnl)
            # Line item
            def _fmt(v):
                if v is None:
                    return "-"
                v = float(v)
                if v >= 100: return f"{v:.2f}"
                if v >= 1: return f"{v:.4f}"
                return f"{v:.6f}"
            lines.append(
                f"  • {tf} {direction or '-'}  Entry: {_fmt(entry_price)}  → Exit: {_fmt(exit_price)}  P/L: {(f'{pnl:+.2f}%' if pnl is not None else '-')}  {(outcome or 'PENDING')}"
            )
        # Pair summary
        trades = pair_w + pair_l
        if trades > 0:
            wr = pair_w / trades * 100.0
            lines.append(f"   WinRate: {wr:.1f}%  Trades: {trades}  Net P/L: {pair_pnl:+.2f}%")
        lines.append("")
    # Overall summary
    wr_all = (wins / total * 100.0) if total > 0 else 0.0
    lines.extend([
        f"📡 Trades: {total}",
        f"📊 Win Rate: {wr_all:.2f}%",
        f"💰 Total P/L: {total_pnl:+.2f}%",
    ])
    missing = [p for p in by_pair.keys() if _classify_asset(p) != "crypto"]
    if missing and not os.getenv("FINNHUB_API_KEY", "").strip():
        lines.append("")
        lines.append("ℹ️ FX/Gold/Index evaluation needs an API key (Finnhub/TwelveData/AlphaVantage). Crypto is fully live via Binance.")
    return "\n".join(lines)


def _seeded_rng(asset: str, timeframe: str) -> random.Random:
    # Seeded by asset + timeframe + current UTC minute for deterministic short-term behavior
    minute_key = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
    seed = f"{asset}|{timeframe}|{minute_key}"
    rnd = random.Random()
    rnd.seed(seed)
    return rnd


def _classify_asset(asset: str) -> str:
    a = asset.upper()
    if "BTC" in a or "ETH" in a: return "crypto"
    if "/" in a and any(fx in a for fx in ("USD","EUR","GBP","JPY","INR")): return "forex"
    if "GOLD" in a or "XAU" in a: return "gold"
    if "NASDAQ" in a or "NDQ" in a: return "index"
    return "other"


def _market_open_for_asset(asset: str, now_utc: Optional[datetime] = None) -> bool:
    now_utc = now_utc or datetime.now(timezone.utc)
    cls = _classify_asset(asset)
    if cls == "crypto":
        return True
    if cls == "forex":
        # 24x5 window approximation
        wd, hr = now_utc.weekday(), now_utc.hour
        if wd in (0,1,2,3): return True
        if wd == 4: return hr < 21
        if wd == 6: return hr >= 21
        return False
    if cls == "gold":
        # Similar to gold_is_open()
        wd, hr = now_utc.weekday(), now_utc.hour
        if hr == 22: return False
        if wd == 6: return hr >= 23
        if wd in (0,1,2,3): return True
        if wd == 4: return hr < 22
        return False
    if cls == "index":
        # Use NASDAQ cash hours for demo (NY 9:30-16:00)
        ny = ZoneInfo("America/New_York")
        t = now_utc.astimezone(ny)
        return t.weekday()<5 and time(9,30) <= t.time() < time(16,0)
    return True


def next_open_for_asset(asset: str, now_utc: Optional[datetime] = None) -> Optional[datetime]:
    """Return next market open time (UTC) for the given asset, or None if already open or unknown.
    Uses the same approximations as market_hours_message().
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    cls = _classify_asset(asset)
    if cls == "crypto":
        return None  # 24/7
    if cls == "forex":
        # Next open: Sunday 21:00 UTC if currently closed
        # Closed windows: Fri >= 21:00 UTC until Sun < 21:00 UTC
        wd = now_utc.weekday()
        hr = now_utc.hour
        open_now = (wd in (0,1,2,3)) or (wd == 4 and hr < 21) or (wd == 6 and hr >= 21)
        if open_now:
            return None
        # compute upcoming Sunday 21:00 UTC
        days_to_sun = (6 - wd) % 7
        open_day = (now_utc + timedelta(days=days_to_sun)).date()
        open_dt = datetime.combine(open_day, time(21, 0), tzinfo=timezone.utc)
        if open_dt <= now_utc:
            open_dt += timedelta(days=7)
        return open_dt
    if cls == "gold":
        # Daily break 22:00-23:00 UTC; weekend closed until Sun 23:00 UTC
        wd, hr = now_utc.weekday(), now_utc.hour
        # If in daily break Mon-Thu 22:00-23:00 UTC
        if hr == 22 and wd in (0,1,2,3,4):
            return datetime.combine(now_utc.date(), time(23, 0), tzinfo=timezone.utc)
        # Sunday before 23:00 → open at 23:00
        if wd == 6 and hr < 23:
            return datetime.combine(now_utc.date(), time(23, 0), tzinfo=timezone.utc)
        # If Friday after 22:00 or Saturday → next Sunday 23:00
        if wd == 5 or (wd == 4 and hr >= 22):
            next_sun = _next_weekday(now_utc, 6)
            return datetime.combine(next_sun.date(), time(23, 0), tzinfo=timezone.utc)
        return None  # otherwise considered open
    if cls == "index":
        # NASDAQ cash session 9:30-16:00 America/New_York
        ny = ZoneInfo("America/New_York")
        t = now_utc.astimezone(ny)
        if t.weekday() < 5 and time(9,30) <= t.time() < time(16,0):
            return None
        # find next weekday at 9:30
        next_day = t
        while True:
            next_day = (next_day + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            if next_day.weekday() < 5:
                break
        open_dt_ny = datetime.combine(next_day.date(), time(9, 30), tzinfo=ny)
        return open_dt_ny.astimezone(timezone.utc)
    return None


def generate_smart_signal(asset: Optional[str] = None, timeframe: str = "5m") -> str:
    """Multi-confirmation signal with live data if available.
    Priority: Finnhub -> TwelveData -> AlphaVantage. Falls back to pseudo if none.
    """
    assets = ["BTC/USDT", "ETH/USDT", "EUR/USD", "GBP/JPY", "GOLD", "NASDAQ"]
    pair = asset or random.choice(assets)

    # Try live indicators
    live = get_live_indicators(pair, timeframe)
    if live and live.get("ok"):
        rsi = live.get("rsi", 50)
        macd_hist = live.get("macd_hist", 0)
        ema_fast_over_slow = live.get("ema_fast_over_slow", False)
        bb_pos = live.get("bb_pos", 0)
        stoch = live.get("stoch", 50)
        volume_spike = False  # Not computed without volume; keep neutral
        bullish_engulf = False
        bearish_engulf = False
        demand_zone = False
        supply_zone = False
        fvg_bull = False
        fvg_bear = False
    else:
        rng = _seeded_rng(pair, timeframe)
        # Pseudo indicators
        rsi = rng.randint(10, 90)
        macd_hist = rng.uniform(-2.0, 2.0)
        ema_fast_over_slow = rng.random() > 0.45
        bb_pos = rng.uniform(-2.0, 2.0)
        stoch = rng.randint(10, 90)
        volume_spike = rng.random() < 0.35
        bullish_engulf = rng.random() < 0.3
        bearish_engulf = rng.random() < 0.3
        demand_zone = rng.random() < 0.35
        supply_zone = rng.random() < 0.35
        fvg_bull = rng.random() < 0.25
        fvg_bear = rng.random() < 0.25

    # Scores
    up_score = 0.0
    down_score = 0.0
    reasons_up = []
    reasons_down = []

    # Indicator-based confirmations
    if rsi < 30:
        up_score += 0.7; reasons_up.append("RSI(14) oversold")
    if rsi > 70:
        down_score += 0.7; reasons_down.append("RSI(14) overbought")

    if macd_hist > 0:
        up_score += 0.6; reasons_up.append("MACD histogram rising")
    if macd_hist < 0:
        down_score += 0.6; reasons_down.append("MACD histogram falling")

    if ema_fast_over_slow:
        up_score += 0.6; reasons_up.append("EMA20 above EMA50")
    else:
        down_score += 0.6; reasons_down.append("EMA20 below EMA50")

    if bb_pos < -1.0:
        up_score += 0.5; reasons_up.append("Price near/below lower Bollinger Band")
    if bb_pos > 1.0:
        down_score += 0.5; reasons_down.append("Price near/above upper Bollinger Band")

    if stoch < 20:
        up_score += 0.3; reasons_up.append("Stochastic oversold")
    if stoch > 80:
        down_score += 0.3; reasons_down.append("Stochastic overbought")

    # Price action
    if bullish_engulf: up_score += 0.5; reasons_up.append("Bullish engulfing")
    if bearish_engulf: down_score += 0.5; reasons_down.append("Bearish engulfing")
    if demand_zone: up_score += 0.4; reasons_up.append("At demand zone")
    if supply_zone: down_score += 0.4; reasons_down.append("At supply zone")

    # Volume/momentum
    if volume_spike and macd_hist > 0:
        up_score += 0.4; reasons_up.append("Volume spike with positive momentum")
    if volume_spike and macd_hist < 0:
        down_score += 0.4; reasons_down.append("Volume spike with negative momentum")

    # SMC
    if fvg_bull: up_score += 0.4; reasons_up.append("Bullish FVG context")
    if fvg_bear: down_score += 0.4; reasons_down.append("Bearish FVG context")

    # Simple news/illiquid filter stub
    open_now = _market_open_for_asset(pair)
    if not open_now:
        # Penalize confidence during closed/illiquid windows
        up_score *= 0.6
        down_score *= 0.6

    # Direction and confidence
    direction_up = up_score >= down_score
    direction = "UP" if direction_up else "DOWN"
    emoji = "📈" if direction_up else "📉"
    top_reasons = reasons_up if direction_up else reasons_down
    # Build concise reasons (max 3)
    reason_text = ", ".join(top_reasons[:3]) or ("EMA crossover" if direction_up else "Momentum slowdown")

    # Confidence maps difference into 1..5 with floor 2 and cap 5
    gap = abs(up_score - down_score)
    base = 2.0 + min(3.0, gap * 2.0)  # 2..5
    confidence = int(round(base))

    return (
        f"{pair}\n"
        f"{emoji} Direction: {direction}\n"
        f"💡 Confidence: {confidence}/5\n"
        f"Reason: {reason_text}.\n"
        f"⚠️ This is not financial advice."
    )


# ---------- Live data adapters ----------

def get_live_indicators(pair: str, timeframe: str) -> Dict[str, Any]:
    """Fetch OHLCV if possible and compute indicators including ADX/ATR.
    Fallback to closes-only indicators.
    Returns {ok, rsi, macd_hist, ema_fast_over_slow, bb_pos, stoch, adx, atrp}
    or {ok: False}
    """
    cls = _classify_asset(pair)
    kl = None
    try:
        if cls == "crypto":
            sym = _binance_symbol(pair)
            if sym:
                kl = fetch_klines_binance(sym, timeframe, limit=240)
        else:
            if os.getenv("FINNHUB_API_KEY", "").strip():
                kl = fetch_klines_finnhub(pair, timeframe, limit=240)
    except Exception:
        kl = None
    if kl and len(kl) >= 60:
        if timeframe == "3m":
            try:
                if len(kl) >= 3:
                    ivs = []
                    for i in range(1, min(len(kl), 50)):
                        ivs.append(int(kl[i][0]) - int(kl[i-1][0]))
                    iv = sum(ivs)//len(ivs) if ivs else 60000
                    if iv <= 60000 + 1000:
                        kl = _resample_klines(kl, 3)
            except Exception:
                pass
        return {"ok": True, **compute_indicators_ohlc(kl)}
    # Fallback: closes-only
    closes = None
    for src in (fetch_ohlc_finnhub, fetch_ohlc_twelvedata, fetch_ohlc_alphavantage):
        try:
            closes = src(pair, timeframe, limit=200)
        except Exception:
            closes = None
        if closes:
            break
    if not closes or len(closes) < 60:
        return {"ok": False}
    base = compute_indicators(closes)
    base.update({"adx": None, "atrp": None})
    return {"ok": True, **base}


# ---------- Ensemble signal (high-confluence, MTF) ----------
def _score_from_live(live: Dict[str, Any]) -> Dict[str, Any]:
    rsi = float(live.get("rsi", 50))
    macd_hist = float(live.get("macd_hist", 0))
    ema_fast_over_slow = bool(live.get("ema_fast_over_slow", False))
    bb_pos = float(live.get("bb_pos", 0))
    stoch = float(live.get("stoch", 50))
    score = 0
    reasons_up: list[str] = []
    reasons_down: list[str] = []
    # RSI bands
    if rsi >= 55: score += 1; reasons_up.append("RSI>55")
    if rsi <= 45: score -= 1; reasons_down.append("RSI<45")
    # MACD hist sign
    if macd_hist > 0: score += 1; reasons_up.append("MACD>0")
    if macd_hist < 0: score -= 1; reasons_down.append("MACD<0")
    # EMA ribbon proxy
    if ema_fast_over_slow: score += 1; reasons_up.append("EMA20>EMA50")
    else: score -= 1; reasons_down.append("EMA20<EMA50")
    # Bollinger position extremes (context)
    if bb_pos > 1.0: score -= 0.5; reasons_down.append("Near upper BB")
    if bb_pos < -1.0: score += 0.5; reasons_up.append("Near lower BB")
    # Stochastic tilt
    if stoch >= 60: score -= 0.5; reasons_down.append("Stoch>60")
    if stoch <= 40: score += 0.5; reasons_up.append("Stoch<40")
    return {
        "score": score,
        "dir": "UP" if score > 0 else ("DOWN" if score < 0 else "FLAT"),
        "reasons_up": reasons_up,
        "reasons_down": reasons_down,
        "m": {
            "rsi": rsi,
            "macd_hist": macd_hist,
            "ema_fast_over_slow": ema_fast_over_slow,
            "bb_pos": bb_pos,
            "stoch": stoch,
            "adx": live.get("adx"),
            "atrp": live.get("atrp"),
        }
    }

def _mtf_from_base_1m(pair: str) -> Optional[Dict[str, Dict[str, Any]]]:
    # Short-cache key
    ckey = f"mtf1m:{pair.upper()}"
    cached = _cache_get(ckey)
    if cached:
        return cached
    cls = _classify_asset(pair)
    kl: Optional[List[list]] = None
    try:
        if cls == "crypto":
            sym = _binance_symbol(pair)
            if sym:
                # fetch once (1m) and resample
                kl = fetch_klines_binance(sym, "1m", limit=240)
        else:
            if os.getenv("FINNHUB_API_KEY", "").strip():
                kl = fetch_klines_finnhub(pair, "1m", limit=240)
    except Exception:
        kl = None
    if not kl or len(kl) < 120:
        return None
    # Build 1m, 3m, 5m frames
    live_1m = compute_indicators_ohlc(kl)
    kl3 = _resample_klines(kl, 3)
    kl5 = _resample_klines(kl, 5)
    live_3m = compute_indicators_ohlc(kl3)
    live_5m = compute_indicators_ohlc(kl5)
    out = {
        "1m": _score_from_live({"ok": True, **live_1m}),
        "3m": _score_from_live({"ok": True, **live_3m}),
        "5m": _score_from_live({"ok": True, **live_5m}),
    }
    _cache_set(ckey, out, ttl_sec=4)
    return out

def _fetch_mtf(pair: str) -> Dict[str, Dict[str, Any]]:
    # Try fast path from a single 1m fetch
    fast = _mtf_from_base_1m(pair)
    if fast:
        return fast
    # Fallback: fetch each timeframe separately
    tfs = ["1m", "3m", "5m"]
    out: Dict[str, Dict[str, Any]] = {}
    for tf in tfs:
        live = get_live_indicators(pair, tf)
        if live and live.get("ok"):
            out[tf] = _score_from_live(live)
    return out

def _aggregate_scores(mtf: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    if not mtf:
        return {"ok": False}
    total = 0.0
    n = 0
    up_votes = 0
    down_votes = 0
    up_reasons: list[str] = []
    down_reasons: list[str] = []
    for tf, s in mtf.items():
        sc = float(s.get("score", 0))
        total += sc
        n += 1
        if s.get("dir") == "UP": up_votes += 1; up_reasons.extend(s.get("reasons_up", [])[:2])
        if s.get("dir") == "DOWN": down_votes += 1; down_reasons.extend(s.get("reasons_down", [])[:2])
    avg = total / max(n, 1)
    # Mode configuration
    mode = os.getenv("ENSEMBLE_MODE", "pro").strip().lower()
    need_votes = 3 if mode == "pro" else 2
    avg_need_up = 2.0 if mode == "pro" else 1.5
    avg_need_dn = -2.0 if mode == "pro" else -1.5
    # Conservative: require 3/3 TF agreement + strength gates (pro)
    def gates(direction: str) -> bool:
        # Collect metrics
        rsis = [float(s.get("m", {}).get("rsi", 50)) for s in mtf.values()]
        emas = [bool(s.get("m", {}).get("ema_fast_over_slow", False)) for s in mtf.values()]
        bb_pos = [float(s.get("m", {}).get("bb_pos", 0)) for s in mtf.values()]
        adxs = [s.get("m", {}).get("adx") for s in mtf.values()]
        atrps = [s.get("m", {}).get("atrp") for s in mtf.values()]
        # RSI strength
        if direction == "UP":
            need_rsi = 2 if mode != "aggressive" else 1
            if sum(1 for r in rsis if r >= (55 if mode == "pro" else 53)) < need_rsi: return False
            med = sorted(rsis)[1]
            if med < (52 if mode == "pro" else 50): return False
            if sum(1 for e in emas if e) < (2 if mode != "aggressive" else 1): return False
            # Avoid chasing extremes
            lim = 1.5 if mode == "pro" else (1.8 if mode == "balanced" else 2.0)
            if sum(1 for b in bb_pos if b > lim) >= 2: return False
        else:
            need_rsi = 2 if mode != "aggressive" else 1
            if sum(1 for r in rsis if r <= (45 if mode == "pro" else 47)) < need_rsi: return False
            med = sorted(rsis)[1]
            if med > (48 if mode == "pro" else 50): return False
            if sum(1 for e in emas if not e) < (2 if mode != "aggressive" else 1): return False
            lim = -1.5 if mode == "pro" else (-1.8 if mode == "balanced" else -2.0)
            if sum(1 for b in bb_pos if b < lim) >= 2: return False
        # ADX/ATR gates (if available)
        min_adx = float(os.getenv("ENSEMBLE_MIN_ADX", "18"))
        need_adx = 2 if mode != "aggressive" else 1
        adx_ok = sum(1 for a in adxs if (a is not None and a >= min_adx)) >= need_adx
        atr_min = float(os.getenv("ENSEMBLE_MIN_ATR_PCT", "0.02"))
        atr_max = float(os.getenv("ENSEMBLE_MAX_ATR_PCT", "2.5"))
        need_atr = 2 if mode != "aggressive" else 1
        atr_ok = sum(1 for a in atrps if (a is not None and atr_min <= a <= atr_max)) >= need_atr
        if not (adx_ok and atr_ok):
            return False
        return True

    if up_votes >= need_votes and avg >= avg_need_up and gates("UP"):
        return {"ok": True, "dir": "UP", "confidence": min(5, 2 + int(avg)), "reasons": list(dict.fromkeys(up_reasons))[:3]}
    if down_votes >= need_votes and avg <= avg_need_dn and gates("DOWN"):
        return {"ok": True, "dir": "DOWN", "confidence": min(5, 2 + int(abs(avg))), "reasons": list(dict.fromkeys(down_reasons))[:3]}
    return {"ok": False}

def _news_risk_window(asset: str, now_utc: Optional[datetime] = None) -> bool:
    if os.getenv("STRICT_NEWS_FILTER", "1").strip() not in ("1", "true", "True"):
        return False
    now_utc = now_utc or datetime.now(timezone.utc)
    cls = _classify_asset(asset)
    if cls not in ("forex", "gold", "index"):
        return False
    m = now_utc.minute
    # Block around top/bottom of hour which often aligns with releases
    return m in (0, 1, 2, 30, 31, 32)

def _force_signal_from_tf(pair: str) -> Optional[Dict[str, Any]]:
    for tf_try in ("5m", "3m", "1m"):
        live = get_live_indicators(pair, tf_try)
        if not (live and live.get("ok")):
            continue
        s = _score_from_live(live)
        d = s.get("dir")
        if d == "FLAT":
            mh = float(live.get("macd_hist", 0))
            if mh > 0:
                d = "UP"
            elif mh < 0:
                d = "DOWN"
            else:
                d = "UP" if bool(live.get("ema_fast_over_slow", False)) else "DOWN"
        # Enforce ADX/ATR gates on fallback to avoid weak setups
        metrics = s.get("m", {})
        adx = metrics.get("adx")
        atrp = metrics.get("atrp")
        min_adx = float(os.getenv("ENSEMBLE_MIN_ADX", "18"))
        atr_min = float(os.getenv("ENSEMBLE_MIN_ATR_PCT", "0.02"))
        atr_max = float(os.getenv("ENSEMBLE_MAX_ATR_PCT", "2.5"))
        if adx is None or adx < min_adx:
            continue
        if atrp is None or not (atr_min <= atrp <= atr_max):
            continue
        reasons = s.get("reasons_up", []) if d == "UP" else s.get("reasons_down", [])
        conf = max(2, min(4, 2 + int(abs(float(s.get("score", 0))))))
        return {"dir": d, "confidence": conf, "reasons": list(dict.fromkeys(reasons))[:3]}
    return None

def generate_ensemble_signal(asset: Optional[str] = None, timeframe: str = "5m") -> str:
    assets = ["BTC/USDT", "ETH/USDT", "EUR/USD", "GBP/JPY", "GOLD", "NASDAQ"]
    pair = asset or random.choice(assets)
    # Session filter (toggle via STRICT_SESSION_FILTER)
    if os.getenv("STRICT_SESSION_FILTER", "1").strip() in ("1", "true", "True") and not _market_open_for_asset(pair):
        return (
            f"{pair}\n"
            f"⏳ Direction: NO-TRADE\n"
            f"💡 Confidence: 0/5\n"
            f"Reason: Session closed/illiquid.\n"
            f"⚠️ This is not financial advice."
        )
    # News risk filter
    if _news_risk_window(pair):
        return (
            f"{pair}\n"
            f"⏳ Direction: NO-TRADE\n"
            f"💡 Confidence: 0/5\n"
            f"Reason: High-impact news window.\n"
            f"⚠️ This is not financial advice."
        )
    mtf = _fetch_mtf(pair)
    agg = _aggregate_scores(mtf)
    if not agg.get("ok"):
        forced = _force_signal_from_tf(pair)
        if forced:
            direction = forced["dir"]
            emoji = "📈" if direction == "UP" else "📉"
            confidence = int(forced.get("confidence", 3))
            reasons = forced.get("reasons", [])
            reason_text = ", ".join(reasons) if reasons else ("Single-TF bias (EMA+MACD+RSI)")
            return (
                f"{pair}\n"
                f"{emoji} Direction: {direction}\n"
                f"💡 Confidence: {confidence}/5\n"
                f"Reason: {reason_text}.\n"
                f"⚠️ This is not financial advice."
            )
        # Absolute fallback
        direction = "UP"
        emoji = "📈"
        confidence = 2
        reason_text = "Fallback bias"
        return (
            f"{pair}\n"
            f"{emoji} Direction: {direction}\n"
            f"💡 Confidence: {confidence}/5\n"
            f"Reason: {reason_text}.\n"
            f"⚠️ This is not financial advice."
        )
    direction = agg["dir"]
    emoji = "📈" if direction == "UP" else "📉"
    confidence = int(agg.get("confidence", 3))
    reasons = agg.get("reasons", [])
    reason_text = ", ".join(reasons) if reasons else ("MTF EMA+MACD+RSI confluence" if direction=="UP" else "MTF EMA+MACD+RSI pressure")
    return (
        f"{pair}\n"
        f"{emoji} Direction: {direction}\n"
        f"💡 Confidence: {confidence}/5\n"
        f"Reason: {reason_text}.\n"
        f"⚠️ This is not financial advice."
    )


def _tf_maps(timeframe: str) -> Dict[str, Any]:
    # Finnhub does not support 3-minute resolution; map 3m -> 1m for API calls
    return {
        "finnhub": {"1m": ("1", 60), "3m": ("1", 60), "5m": ("5", 300)},
        "twelvedata": {"1m": "1min", "3m": "3min", "5m": "5min"},
        "alphavantage": {"1m": "1min", "3m": "5min", "5m": "5min"},
    }


def _classify_and_symbol_for_provider(pair: str, provider: str) -> Optional[Dict[str, str]]:
    cls = _classify_asset(pair)
    up = pair.upper()
    if provider == "finnhub":
        if cls == "crypto":
            sym = "BINANCE:" + up.replace("/", "")  # BTC/USDT -> BINANCE:BTCUSDT
            return {"endpoint": "crypto", "symbol": sym}
        if cls == "forex":
            base, quote = up.split("/")
            return {"endpoint": "forex", "symbol": f"OANDA:{base}_{quote}"}
        if cls == "gold":
            return {"endpoint": "forex", "symbol": "OANDA:XAU_USD"}
        if cls == "index":
            return {"endpoint": "stock", "symbol": "QQQ"}  # proxy
        return None
    if provider == "twelvedata":
        if cls in ("crypto", "forex"):
            return {"symbol": pair.replace("XAU/USD", "XAU/USD")}
        if cls == "gold":
            return {"symbol": "XAU/USD"}
        if cls == "index":
            return {"symbol": "QQQ"}
        return {"symbol": pair}
    if provider == "alphavantage":
        if cls == "crypto":
            base, quote = up.split("/")
            if quote == "USDT": quote = "USD"
            return {"fn": "CRYPTO_INTRADAY", "from": base, "to": quote}
        if cls == "forex" or cls == "gold":
            base, quote = up.split("/") if "/" in up else ("XAU", "USD")
            return {"fn": "FX_INTRADAY", "from": base, "to": quote}
        if cls == "index":
            return {"fn": "TIME_SERIES_INTRADAY", "symbol": "QQQ"}
        return None
    return None


def fetch_ohlc_finnhub(pair: str, timeframe: str, limit: int = 200):
    key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not key:
        return None
    maps = _tf_maps(timeframe)["finnhub"]
    if timeframe not in maps:
        return None
    res, sec = maps[timeframe]
    m = _classify_and_symbol_for_provider(pair, "finnhub")
    if not m:
        return None
    now = int(time_module.time())
    frm = now - sec * (limit + 10)
    endpoint = m["endpoint"]
    symbol = m["symbol"]
    base = "https://finnhub.io/api/v1"
    if endpoint == "crypto":
        url = f"{base}/crypto/candle?symbol={symbol}&resolution={res}&from={frm}&to={now}&token={key}"
    elif endpoint == "forex":
        url = f"{base}/forex/candle?symbol={symbol}&resolution={res}&from={frm}&to={now}&token={key}"
    else:
        url = f"{base}/stock/candle?symbol={symbol}&resolution={res}&from={frm}&to={now}&token={key}"
    r = requests.get(url, timeout=5)
    if r.status_code != 200:
        return None
    j = r.json()
    if j.get("s") != "ok":
        return None
    closes = j.get("c") or []
    return closes[-limit:]


def fetch_klines_finnhub(pair: str, timeframe: str, limit: int = 200) -> Optional[List[list]]:
    key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not key:
        return None
    maps = _tf_maps(timeframe)["finnhub"]
    if timeframe not in maps:
        return None
    res, sec = maps[timeframe]
    m = _classify_and_symbol_for_provider(pair, "finnhub")
    if not m:
        return None
    now = int(time_module.time())
    frm = now - sec * (limit + 10)
    endpoint = m["endpoint"]
    symbol = m["symbol"]
    base = "https://finnhub.io/api/v1"
    if endpoint == "crypto":
        url = f"{base}/crypto/candle?symbol={symbol}&resolution={res}&from={frm}&to={now}&token={key}"
    elif endpoint == "forex":
        url = f"{base}/forex/candle?symbol={symbol}&resolution={res}&from={frm}&to={now}&token={key}"
    else:
        url = f"{base}/stock/candle?symbol={symbol}&resolution={res}&from={frm}&to={now}&token={key}"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return None
        j = r.json()
        if j.get("s") != "ok":
            return None
        t = j.get("t") or []
        o = j.get("o") or []
        h = j.get("h") or []
        l = j.get("l") or []
        c = j.get("c") or []
        v = j.get("v") or []
        kl = []
        for i in range(min(len(t), len(c))):
            ot_ms = int(t[i]) * 1000
            ct_ms = ot_ms + sec * 1000
            kl.append([ot_ms, float(o[i]), float(h[i]), float(l[i]), float(c[i]), float(v[i]) if i < len(v) else 0.0, ct_ms])
        return kl[-limit:]
    except Exception:
        return None

def fetch_ohlc_twelvedata(pair: str, timeframe: str, limit: int = 200):
    key = os.getenv("TWELVEDATA_API_KEY", "").strip()
    if not key:
        return None
    maps = _tf_maps(timeframe)["twelvedata"]
    if timeframe not in maps:
        return None
    interval = maps[timeframe]
    m = _classify_and_symbol_for_provider(pair, "twelvedata")
    sym = m["symbol"]
    url = (
        f"https://api.twelvedata.com/time_series?symbol={sym}&interval={interval}&outputsize={limit}&apikey={key}&format=JSON&dp=6"
    )
    r = requests.get(url, timeout=8)
    if r.status_code != 200:
        return None
    j = r.json()
    vals = j.get("values")
    if not vals:
        return None
    # API returns newest first
    closes = [float(v["close"]) for v in reversed(vals)]
    return closes[-limit:]


def fetch_ohlc_alphavantage(pair: str, timeframe: str, limit: int = 200):
    key = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
    if not key:
        return None
    maps = _tf_maps(timeframe)["alphavantage"]
    if timeframe not in maps:
        return None
    interval = maps[timeframe]
    m = _classify_and_symbol_for_provider(pair, "alphavantage")
    if not m:
        return None
    base = "https://www.alphavantage.co/query"
    if m.get("fn") == "CRYPTO_INTRADAY":
        url = f"{base}?function=CRYPTO_INTRADAY&symbol={m['from']}&market={m['to']}&interval={interval}&apikey={key}"
        key_name = f"Time Series Crypto ({interval})"
    elif m.get("fn") == "FX_INTRADAY":
        url = f"{base}?function=FX_INTRADAY&from_symbol={m['from']}&to_symbol={m['to']}&interval={interval}&apikey={key}"
        key_name = f"Time Series FX ({interval})"
    else:
        url = f"{base}?function=TIME_SERIES_INTRADAY&symbol={m['symbol']}&interval={interval}&apikey={key}"
        key_name = f"Time Series ({interval})"
    r = requests.get(url, timeout=8)
    if r.status_code != 200:
        return None
    j = r.json()
    data = j.get(key_name)
    if not data:
        return None
    # Dict of time -> fields, newest first
    items = list(data.items())
    items.sort()  # oldest first
    closes = [float(v.get("4. close") or v.get("close")) for _, v in items if v]
    return closes[-limit:]


def compute_indicators(closes: list) -> Dict[str, Any]:
    """Compute RSI(14), MACD(12,26,9) hist, EMA20>EMA50 flag, BB position z, Stochastic(14)."""
    if len(closes) < 60:
        return {"rsi": 50, "macd_hist": 0, "ema_fast_over_slow": False, "bb_pos": 0, "stoch": 50}
    def ema(series, period):
        k = 2/(period+1)
        ema_val = series[0]
        for x in series[1:]:
            ema_val = x*k + ema_val*(1-k)
        return ema_val
    # RSI(14)
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    def rsi_calc(gs, ls, period=14):
        if len(gs) < period:
            return 50
        avg_gain = sum(gs[:period])/period
        avg_loss = sum(ls[:period])/period
        for i in range(period, len(gs)):
            avg_gain = (avg_gain*(period-1) + gs[i]) / period
            avg_loss = (avg_loss*(period-1) + ls[i]) / period
        rs = (avg_gain / avg_loss) if avg_loss != 0 else 999
        return 100 - (100 / (1 + rs))
    rsi_val = rsi_calc(gains, losses, 14)
    # MACD
    ema12 = ema(closes[-120:], 12)
    ema26 = ema(closes[-120:], 26)
    macd = ema12 - ema26
    # For signal, approximate using last 26 points of macd with EMA9; with a simple series approach
    macd_series = []
    s = closes[-150:]
    for i in range(len(s)):
        sub = s[:i+1]
        e12 = ema(sub, 12)
        e26 = ema(sub, 26)
        macd_series.append(e12 - e26)
    signal = ema(macd_series, 9)
    macd_hist = macd - signal
    # EMAs for trend
    e20 = ema(closes[-200:], 20)
    e50 = ema(closes[-200:], 50)
    ema_fast_over_slow = e20 > e50
    # Bollinger position
    win = closes[-20:]
    sma20 = sum(win)/20
    std20 = (sum((x - sma20)**2 for x in win)/20) ** 0.5
    bb_pos = 0 if std20 == 0 else (closes[-1] - sma20) / (2*std20)
    # Stochastic(14)
    w = closes[-14:]
    lo, hi = min(w), max(w)
    stoch = 50 if hi == lo else (closes[-1] - lo) / (hi - lo) * 100
    return {
        "rsi": rsi_val,
        "macd_hist": macd_hist,
        "ema_fast_over_slow": ema_fast_over_slow,
        "bb_pos": bb_pos,
        "stoch": stoch,
    }

def compute_indicators_ohlc(kl: List[list]) -> Dict[str, Any]:
    """Compute indicators from OHLCV klines.
    kl item: [openTimeMs, open, high, low, close, volume, closeTimeMs]
    """
    highs = [float(k[2]) for k in kl]
    lows = [float(k[3]) for k in kl]
    closes = [float(k[4]) for k in kl]
    out = compute_indicators(closes)
    # ADX(14) and ATR via Wilder smoothing
    period = 14
    if len(kl) < period + 2:
        out.update({"adx": None, "atrp": None})
        return out
    trs: List[float] = []
    pdm: List[float] = []
    mdm: List[float] = []
    for i in range(1, len(kl)):
        high, prev_high = highs[i], highs[i-1]
        low, prev_low = lows[i], lows[i-1]
        close_prev = closes[i-1]
        up = high - prev_high
        down = prev_low - low
        plus_dm = up if (up > down and up > 0) else 0.0
        minus_dm = down if (down > up and down > 0) else 0.0
        tr = max(high - low, abs(high - close_prev), abs(low - close_prev))
        trs.append(tr)
        pdm.append(plus_dm)
        mdm.append(minus_dm)
    if len(trs) < period:
        out.update({"adx": None, "atrp": None})
        return out
    tr14 = sum(trs[:period])
    pdm14 = sum(pdm[:period])
    mdm14 = sum(mdm[:period])
    dis: List[float] = []
    for i in range(period, len(trs)):
        tr14 = tr14 - (tr14 / period) + trs[i]
        pdm14 = pdm14 - (pdm14 / period) + pdm[i]
        mdm14 = mdm14 - (mdm14 / period) + mdm[i]
        if tr14 <= 0:
            continue
        dip = 100.0 * (pdm14 / tr14)
        din = 100.0 * (mdm14 / tr14)
        denom = (dip + din)
        if denom <= 0:
            continue
        dx = 100.0 * abs(dip - din) / denom
        dis.append(dx)
    if not dis:
        out.update({"adx": None, "atrp": None})
        return out
    # Smooth DX to ADX
    adx = sum(dis[:period]) / min(period, len(dis))
    for i in range(period, len(dis)):
        adx = ((adx * (period - 1)) + dis[i]) / period
    atr14 = tr14 / period if period else 0.0
    last_close = closes[-1] if closes else 0.0
    atrp = (atr14 / last_close * 100.0) if last_close else None
    out.update({"adx": adx, "atrp": atrp})
    return out

def _resample_klines(kl: List[list], group: int) -> List[list]:
    out: List[list] = []
    if group <= 1:
        return kl
    n = len(kl)
    take = (n // group) * group
    base = kl[:take]
    for i in range(0, take, group):
        seg = base[i:i+group]
        if not seg:
            continue
        ot = int(seg[0][0])
        ct = int(seg[-1][6]) if len(seg[-1]) > 6 else ot + group*60000
        o = float(seg[0][1])
        h = max(float(x[2]) for x in seg)
        l = min(float(x[3]) for x in seg)
        c = float(seg[-1][4])
        v = sum(float(x[5]) for x in seg)
        out.append([ot, o, h, l, c, v, ct])
    return out

def _binance_symbol(pair: str) -> Optional[str]:
    p = (pair or "").upper().replace("/", "")
    if not p:
        return None
    return p

def fetch_ohlc_binance(symbol: str, timeframe: str, limit: int = 300) -> Optional[List[float]]:
    try:
        interval = timeframe
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={min(max(limit,1),1000)}"
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        closes = [float(item[4]) for item in data if item and len(item) > 5]
        return closes
    except Exception:
        return None

def fetch_klines_binance(symbol: str, timeframe: str, limit: int = 300) -> Optional[List[list]]:
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={timeframe}&limit={min(max(limit,1),1000)}"
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, list):
            return None
        return data
    except Exception:
        return None

def _bars_for_tf(timeframe: str) -> int:
    if timeframe == "1m":
        return 1440
    if timeframe == "3m":
        return 480
    return 288

def _dir_from_indicators(closes: List[float]) -> int:
    ind = compute_indicators(closes)
    up = 0.0
    dn = 0.0
    rsi = ind.get("rsi", 50)
    macd_hist = ind.get("macd_hist", 0)
    ema_fast_over_slow = ind.get("ema_fast_over_slow", False)
    bb_pos = ind.get("bb_pos", 0)
    stoch = ind.get("stoch", 50)
    if rsi < 30:
        up += 0.7
    if rsi > 70:
        dn += 0.7
    if macd_hist > 0:
        up += 0.6
    if macd_hist < 0:
        dn += 0.6
    if ema_fast_over_slow:
        up += 0.6
    else:
        dn += 0.6
    if bb_pos < -1.0:
        up += 0.5
    if bb_pos > 1.0:
        dn += 0.5
    if stoch < 20:
        up += 0.3
    if stoch > 80:
        dn += 0.3
    return 1 if up >= dn else -1

def _segments_from_dirs(closes: List[float], dirs: List[int]) -> List[Tuple[int,int,int]]:
    segs: List[Tuple[int,int,int]] = []
    if not closes or not dirs:
        return segs
    start = 0
    cur = dirs[0]
    for i in range(1, len(dirs)):
        if dirs[i] != cur:
            segs.append((start, i, cur))
            start = i
            cur = dirs[i]
    segs.append((start, len(dirs)-1, cur))
    return segs

def generate_24h_performance_report(pairs: Optional[List[str]] = None, timeframe: str = "5m") -> str:
    pairs = pairs or [
        "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","ADAUSDT","XRPUSDT",
        "DOGEUSDT","MATICUSDT","LTCUSDT","GRTUSDT","JASMYUSDT","CAKEUSDT",
        "ARBUSDT","SANDUSDT","TIAUSDT","KNCUSDT","XLMUSDT"
    ]
    bars = _bars_for_tf(timeframe)
    warmup = 200
    limit = warmup + bars + 2
    lines: List[str] = ["📈 VIP Channel's Profit in the last 24 hours", "", ""]
    total_profit = 0.0
    total_trades = 0
    wins = 0
    winners: List[str] = []
    losers: List[str] = []
    for p in pairs:
        sym = _binance_symbol(p)
        if not sym:
            continue
        kl = fetch_klines_binance(sym, timeframe, limit)
        if not kl or len(kl) < warmup + 10:
            continue
        kl = kl[-(warmup+bars):]
        c = [float(x[4]) for x in kl]
        c24 = c[-bars:]
        open_price = c24[0]
        close_price = c24[-1]
        price_delta = (close_price - open_price) / (open_price or 1) * 100.0
        dirs: List[int] = []
        start_idx = max(60, len(c) - bars)
        for t in range(start_idx, len(c)-1):
            sub = c[:t+1]
            d = _dir_from_indicators(sub)
            dirs.append(d)
        if not dirs:
            continue
        c_region = c[start_idx:]
        segs = _segments_from_dirs(c_region, dirs)
        pair_profit = 0.0
        pair_trades = 0
        pair_wins = 0
        for s,e,sgn in segs:
            if e <= s or e >= len(c_region):
                continue
            entry = c_region[s]
            exitp = c_region[e]
            pct = (exitp - entry) / entry * 100.0 * (1 if sgn>0 else -1)
            pair_profit += pct
            pair_trades += 1
            if pct > 0:
                pair_wins += 1
        total_profit += pair_profit
        total_trades += pair_trades
        wins += pair_wins
        if pair_trades == 0:
            continue
        emoji = "🟢" if pair_profit >= 0 else "🚫"
        label = p.replace("/", "").upper()
        def _fmt_price(v: float) -> str:
            if v >= 100:
                return f"{v:.2f}"
            if v >= 1:
                return f"{v:.4f}"
            return f"{v:.6f}"
        wl = f"{pair_wins}/{max(pair_trades - pair_wins,0)}"
        lines.append(f"{label:<11}: {pair_profit:+.2f}% {emoji} | Open: {_fmt_price(open_price)}  Close: {_fmt_price(close_price)} | PriceΔ: {price_delta:+.2f}% | Trades W/L: {wl}")
        if pair_profit >= 0:
            winners.append(label)
        else:
            losers.append(label)
    lines.append("")
    if total_trades > 0:
        avg = total_profit / total_trades
        wr = (wins / total_trades) * 100.0
        lines.extend([
            f"💰 Total Profit: {total_profit:.2f}% Profit",
            f"💹 Average Profit/Trade: {avg:.2f}%",
            f"📡 Signal Calls: {total_trades}",
            f"📊 Win Rate: {wr:.2f}%",
            f"🟢 Profit Trades: {wins}",
            f"🚫 Loss Trades: {total_trades - wins}",
        ])
        if winners:
            lines.append("✅ Winners: " + ", ".join(winners))
    else:
        lines.append("No trades generated. Check Finnhub access and pair list.")
    return "\n".join(lines)
