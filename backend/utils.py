import logging
import os
import random
import time as time_module
from datetime import datetime, timezone, time, timedelta
from typing import Optional, Dict, Any, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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

_RETRY = Retry(total=3, connect=3, read=3, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=frozenset(["GET", "POST"]))
_HTTP = requests.Session()
_HTTP.mount("https://", HTTPAdapter(max_retries=_RETRY, pool_connections=10, pool_maxsize=10))
_HTTP.mount("http://", HTTPAdapter(max_retries=_RETRY, pool_connections=10, pool_maxsize=10))

def safe_request(method: str, url: str, headers: Optional[dict] = None, timeout=10):
    try:
        return _HTTP.request(method.upper(), url, headers=headers, timeout=timeout)
    except Exception:
        try:
            h = dict(headers or {})
            h["Connection"] = "close"
            return requests.request(method.upper(), url, headers=h, timeout=timeout)
        except Exception:
            return None

def verify_transaction(tx_hash: str) -> Dict[str, Any]:
    res = {"network": None, "found": False, "success": False}
    timeout = (5, 10)

    # TronGrid
    try:
        tg_key = os.getenv("TRONGRID_API_KEY", "").strip()
        if tg_key and len(tx_hash) >= 64:
            headers = {"TRON-PRO-API-KEY": tg_key}
            r = safe_request("GET", f"https://api.trongrid.io/v1/transactions/{tx_hash}", headers=headers, timeout=timeout)
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
            r = safe_request("GET", u, timeout=timeout)
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

# ----- IST sessions and per-pair active windows -----
IST_TZ = ZoneInfo("Asia/Kolkata")

# Sessions (approx.) like your image — IST
SESSIONS_IST: list[tuple[str, time, time, list[str]]] = [
    ("Sydney", time(3, 30), time(12, 30), ["AUD/USD", "NZD/USD"]),
    ("Tokyo", time(5, 30), time(14, 30), ["USD/JPY", "AUD/USD"]),
    ("London", time(13, 30), time(22, 30), ["EUR/USD", "GBP/USD", "USD/CHF"]),
    ("New York", time(18, 30), time(3, 30), ["USD/CAD", "EUR/USD", "GBP/USD"]),
]

# Per-pair most-active windows (IST) — from your image
FX_PAIR_WINDOWS_IST: dict[str, tuple[time, time]] = {
    "EUR/USD": (time(13, 30), time(22, 30)),
    "USD/JPY": (time(5, 30), time(14, 30)),
    "GBP/USD": (time(13, 30), time(22, 30)),
    "AUD/USD": (time(5, 30), time(12, 30)),
    "USD/CHF": (time(13, 30), time(22, 30)),
    "USD/CAD": (time(18, 30), time(3, 30)),  # crosses midnight
    "NZD/USD": (time(4, 30), time(11, 30)),
}

def _ist_in_window(now_utc: datetime, start: time, end: time) -> bool:
    now_ist = now_utc.astimezone(IST_TZ)
    t = now_ist.time()
    if start <= end:
        return start <= t < end
    # Overnight window
    return t >= start or t < end

def is_pair_active_now(pair: str, now_utc: Optional[datetime] = None) -> bool:
    """Return True if pair is in its IST most-active window and FX is open (24x5)."""
    now_utc = now_utc or datetime.now(timezone.utc)
    if _classify_asset(pair) != "forex":
        return _market_open_for_asset(pair, now_utc)
    window = FX_PAIR_WINDOWS_IST.get((pair or "").upper())
    if not window:
        return _market_open_for_asset(pair, now_utc)
    if not _market_open_for_asset(pair, now_utc):
        return False
    return _ist_in_window(now_utc, window[0], window[1])

def next_active_for_pair(pair: str, now_utc: Optional[datetime] = None) -> Optional[datetime]:
    """Next start of the pair's IST active window (UTC), honoring FX 24x5."""
    now_utc = now_utc or datetime.now(timezone.utc)
    if _classify_asset(pair) != "forex":
        return next_open_for_asset(pair, now_utc)
    window = FX_PAIR_WINDOWS_IST.get((pair or "").upper())
    if not window:
        return next_open_for_asset(pair, now_utc)
    now_ist = now_utc.astimezone(IST_TZ)
    for dd in range(0, 8):
        day = (now_ist + timedelta(days=dd)).date()
        start_dt_ist = datetime.combine(day, window[0], tzinfo=IST_TZ)
        start_dt_utc = start_dt_ist.astimezone(timezone.utc)
        if start_dt_utc <= now_utc:
            continue
        if _market_open_for_asset(pair, start_dt_utc):
            return start_dt_utc
    return None


 


def market_hours_message_for_pairs(pairs: list[str]) -> str:
    disp_tz = ZoneInfo(os.getenv("TIMEZONE", "UTC"))
    now_utc = datetime.now(timezone.utc)
    now_disp = now_utc.astimezone(disp_tz)

    # Headline 24x5 status
    fx_open = _market_open_for_asset("EUR/USD", now_utc)
    fx_next = next_open_for_asset("EUR/USD", now_utc)
    fx_header = (
        "LIVE FX — OPEN (24x5)"
        if fx_open else f"LIVE FX — CLOSED (Weekend) · Next open: {_fmt(fx_next, disp_tz)}"
    )

    # Sessions (IST)
    def fmt_clock(ti: time) -> str:
        base = datetime.now(IST_TZ).replace(hour=ti.hour, minute=ti.minute, second=0, microsecond=0)
        return base.strftime("%I:%M %p")

    sessions_lines = [
        f"{name}: {fmt_clock(st)} – {fmt_clock(en)} IST  ·  {', '.join(pairs)}"
        for (name, st, en, pairs) in SESSIONS_IST
    ]

    # Pair windows (IST)
    pairs = [p for p in pairs or [] if "/" in (p or "")]  # only valid
    pair_lines = [
        f"{p}: {fmt_clock(FX_PAIR_WINDOWS_IST[p.upper()][0])} – {fmt_clock(FX_PAIR_WINDOWS_IST[p.upper()][1])} IST"
        for p in pairs if p.upper() in FX_PAIR_WINDOWS_IST
    ]

    # Live vs closed by active windows
    open_pairs: list[str] = []
    closed_pairs: list[tuple[str, str]] = []
    for p in pairs:
        if is_pair_active_now(p, now_utc):
            open_pairs.append(p)
        else:
            nxt_dt = next_active_for_pair(p, now_utc)
            nxt_str = _fmt(nxt_dt, IST_TZ) if nxt_dt else "Unknown"
            closed_pairs.append((p, nxt_str))

    lines = [
        f"🕒 Now: {now_disp.strftime('%Y-%m-%d %H:%M %Z')}",
        fx_header,
        "",
        "Sessions (IST):",
        *sessions_lines,
        "",
        "Pairs (IST):",
        *pair_lines,
        "",
        "LIVE now:",
        *( [f"• {p}" for p in open_pairs] if open_pairs else ["• None"] ),
        "",
        "CLOSED now:",
        *( [f"• {p} — Next active: {n}" for (p, n) in closed_pairs] if closed_pairs else ["• None"] ),
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
    # 4b) Yahoo Finance (1m/5m) near-real-time without key
    try:
        closes = fetch_ohlc_yahoo_fx(pair, normalized_tf, limit=2)
        if closes:
            return float(closes[-1])
    except Exception:
        pass
    # 5) Free FX spot fallback (no key required)
    if cls == "forex":
        try:
            spot = fetch_fx_spot_free(pair)
            if spot is not None:
                return float(spot)
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
    # Yahoo fallback (no key)
    try:
        kl = fetch_klines_yahoo_fx(pair, interval="1m", range_s="2h")
        if kl:
            for k in kl:
                ot, ct = int(k[0]), int(k[6])
                if ot <= entry_ms < ct or entry_ms <= ot:
                    return float(k[4])
    except Exception:
        pass
    return None

def fetch_ohlc_yahoo_fx(pair: str, timeframe: str, limit: int = 200) -> Optional[List[float]]:
    """Fetch near-real-time closes for FX via Yahoo Finance chart API (no key).
    timeframe: "1m", "3m", "5m" (3m maps to 1m here).
    Returns list of closes.
    """
    up = (pair or "").upper().replace("/", "") + "=X"
    if timeframe == "3m":
        interval = "1m"
        range_s = "1h"
    elif timeframe == "5m":
        interval = "5m"
        range_s = "1d"
    else:
        interval = "1m"
        range_s = "1h"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{up}?interval={interval}&range={range_s}"
    r = safe_request("GET", url, timeout=5)
    if not r or r.status_code != 200:
        return None
    j = r.json() or {}
    res = ((j.get("chart") or {}).get("result") or [])
    if not res:
        return None
    r0 = res[0]
    q = (((r0.get("indicators") or {}).get("quote") or []) or [{}])[0]
    closes = q.get("close") or []
    # Filter None values and keep last N
    vals = [float(x) for x in closes if x is not None]
    return vals[-min(len(vals), max(1, limit)):] if vals else None

def fetch_klines_yahoo_fx(pair: str, interval: str = "1m", range_s: str = "2h") -> Optional[List[list]]:
    """Fetch OHLCV klines from Yahoo Finance chart API (no key).
    Returns list: [openTimeMs, open, high, low, close, volume, closeTimeMs]
    """
    up = (pair or "").upper().replace("/", "") + "=X"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{up}?interval={interval}&range={range_s}"
    r = safe_request("GET", url, timeout=5)
    if not r or r.status_code != 200:
        return None
    j = r.json() or {}
    res = ((j.get("chart") or {}).get("result") or [])
    if not res:
        return None
    r0 = res[0]
    ts = r0.get("timestamp") or []
    ind = (r0.get("indicators") or {})
    q = ((ind.get("quote") or []) or [{}])[0]
    o = q.get("open") or []
    h = q.get("high") or []
    l = q.get("low") or []
    c = q.get("close") or []
    v = q.get("volume") or []
    if not ts or not c:
        return None
    sec = 60 if interval in ("1m", "2m", "3m") else 300
    out: List[list] = []
    for i in range(min(len(ts), len(c))):
        t = int(ts[i])
        ot = t * 1000
        ct = (t + sec) * 1000
        try:
            oo = float(o[i]) if i < len(o) and o[i] is not None else float(c[i])
            hh = float(h[i]) if i < len(h) and h[i] is not None else float(c[i])
            ll = float(l[i]) if i < len(l) and l[i] is not None else float(c[i])
            cc = float(c[i])
            vv = float(v[i]) if i < len(v) and v[i] is not None else 0.0
        except Exception:
            continue
        out.append([ot, oo, hh, ll, cc, vv, ct])
    return out[-min(len(out), 1000):] if out else None

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
    a = (asset or "").upper()
    # Only support FX pairs now
    if "/" in a and any(ccy in a for ccy in ("USD","EUR","GBP","JPY","INR","CHF","AUD","CAD","NZD")):
        return "forex"
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


 

def _sessions_active_now_ist() -> str:
    now_ist = datetime.now(timezone.utc).astimezone(IST_TZ)
    def in_window(st: time, en: time) -> bool:
        t = now_ist.time()
        return (st <= t < en) if st <= en else (t >= st or t < en)
    active = [name for (name, st, en, _p) in SESSIONS_IST if in_window(st, en)]
    return " / ".join(active) if active else "-"

def _pair_window_text(pair: str) -> str:
    p = (pair or "").upper()
    w = FX_PAIR_WINDOWS_IST.get(p)
    if not w:
        return "-"
    def fmt(ti: time) -> str:
        base = datetime.now(IST_TZ).replace(hour=ti.hour, minute=ti.minute, second=0, microsecond=0)
        return base.strftime("%I:%M %p")
    return f"{fmt(w[0])}–{fmt(w[1])} IST"

def fetch_fx_spot_free(pair: str) -> Optional[float]:
    try:
        base, quote = (pair or "").upper().split("/")
    except Exception:
        return None
    # Try Frankfurter API
    try:
        r = safe_request("GET", f"https://api.frankfurter.app/latest?from={base}&to={quote}", timeout=6)
        if r and r.status_code == 200:
            j = r.json() or {}
            rates = j.get("rates") or {}
            val = rates.get(quote)
            if isinstance(val, (int, float)):
                return float(val)
    except Exception:
        pass
    # Fallback: exchangerate.host
    try:
        r = safe_request("GET", f"https://api.exchangerate.host/convert?from={base}&to={quote}", timeout=6)
        if r and r.status_code == 200:
            j = r.json() or {}
            val = j.get("result")
            if isinstance(val, (int, float)):
                return float(val)
    except Exception:
        pass
    return None


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
    # TwelveData native 3m OHLCV for FX (if available)
    if (not kl or len(kl) < 60) and cls == "forex" and timeframe == "3m":
        try:
            td = fetch_klines_twelvedata(pair, timeframe, limit=240)
            if td and len(td) >= 60:
                kl = td
        except Exception:
            pass
    # Yahoo FX OHLCV fallback if no Finnhub klines (for FX)
    if (not kl or len(kl) < 60) and cls == "forex":
        try:
            if timeframe == "5m":
                kl = fetch_klines_yahoo_fx(pair, interval="5m", range_s="1d")
            else:
                kl = fetch_klines_yahoo_fx(pair, interval="1m", range_s="1h")
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
    for src in (fetch_ohlc_finnhub, fetch_ohlc_twelvedata, fetch_ohlc_alphavantage, fetch_ohlc_yahoo_fx):
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
    # Yahoo FX OHLCV fallback for 1m if no Finnhub
    if (not kl or len(kl) < 120) and cls == "forex":
        try:
            kl = fetch_klines_yahoo_fx(pair, interval="1m", range_s="1h")
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
    assets = ["EUR/USD", "USD/JPY", "GBP/USD", "AUD/USD", "USD/CHF", "USD/CAD", "NZD/USD"]
    pair = asset or random.choice(assets)
    # Session filter (toggle via STRICT_SESSION_FILTER)
    now_ist = datetime.now(ZoneInfo(os.getenv("TIMEZONE", "Asia/Kolkata")))
    upd = now_ist.strftime("%H:%M:%S %Z")
    if os.getenv("STRICT_SESSION_FILTER", "1").strip() in ("1", "true", "True") and not _market_open_for_asset(pair):
        return (
            f"{pair} · TF: {timeframe}\n"
            f"⏳ Direction: NO-TRADE\n"
            f"💡 Confidence: 0/5\n"
            f"Reason: Session closed/illiquid.\n"
            f"Updated: {upd}\n"
            f"⚠️ This is not financial advice."
        )
    # News risk filter
    if _news_risk_window(pair):
        return (
            f"{pair} · TF: {timeframe}\n"
            f"⏳ Direction: NO-TRADE\n"
            f"💡 Confidence: 0/5\n"
            f"Reason: High-impact news window.\n"
            f"Updated: {upd}\n"
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
            sess_names = _sessions_active_now_ist()
            win_txt = _pair_window_text(pair)
            return (
                f"{pair} · TF: {timeframe}\n"
                f"{emoji} Direction: {direction}\n"
                f"💡 Confidence: {confidence}/5\n"
                f"Reason: {reason_text}.\n"
                f"Session: {sess_names} · Active window: {win_txt}\n"
                f"Updated: {upd}\n"
                f"⚠️ This is not financial advice."
            )
        # Absolute fallback
        direction = "UP"
        emoji = "📈"
        confidence = 2
        reason_text = "Fallback bias"
        sess_names = _sessions_active_now_ist()
        win_txt = _pair_window_text(pair)
        return (
            f"{pair} · TF: {timeframe}\n"
            f"{emoji} Direction: {direction}\n"
            f"💡 Confidence: {confidence}/5\n"
            f"Reason: {reason_text}.\n"
            f"Session: {sess_names} · Active window: {win_txt}\n"
            f"Updated: {upd}\n"
            f"⚠️ This is not financial advice."
        )
    direction = agg["dir"]
    emoji = "📈" if direction == "UP" else "📉"
    confidence = int(agg.get("confidence", 3))
    reasons = agg.get("reasons", [])
    reason_text = ", ".join(reasons) if reasons else ("MTF EMA+MACD+RSI confluence" if direction=="UP" else "MTF EMA+MACD+RSI pressure")
    sess_names = _sessions_active_now_ist()
    win_txt = _pair_window_text(pair)
    return (
        f"{pair} · TF: {timeframe}\n"
        f"{emoji} Direction: {direction}\n"
        f"💡 Confidence: {confidence}/5\n"
        f"Reason: {reason_text}.\n"
        f"Session: {sess_names} · Active window: {win_txt}\n"
        f"Updated: {upd}\n"
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
    r = safe_request("GET", url, timeout=5)
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
        r = safe_request("GET", url, timeout=5)
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
    r = safe_request("GET", url, timeout=8)
    if r.status_code != 200:
        return None
    j = r.json()
    vals = j.get("values")
    if not vals:
        return None
    # API returns newest first
    closes = [float(v["close"]) for v in reversed(vals)]
    return closes[-limit:]

def fetch_klines_twelvedata(pair: str, timeframe: str, limit: int = 240) -> Optional[List[list]]:
    """Fetch OHLCV klines via TwelveData time_series (needs TWELVEDATA_API_KEY).
    Returns list: [openTimeMs, open, high, low, close, volume, closeTimeMs]
    """
    key = os.getenv("TWELVEDATA_API_KEY", "").strip()
    if not key:
        return None
    maps = _tf_maps(timeframe)["twelvedata"]
    if timeframe not in maps:
        return None
    interval = maps[timeframe]
    m = _classify_and_symbol_for_provider(pair, "twelvedata")
    sym = m["symbol"] if m else pair
    url = (
        f"https://api.twelvedata.com/time_series?symbol={sym}&interval={interval}&outputsize={limit}&apikey={key}&format=JSON&dp=6&order=ASC"
    )
    try:
        r = safe_request("GET", url, timeout=5)
        if r.status_code != 200:
            return None
        j = r.json()
        vals = j.get("values")
        if not vals:
            return None
        # API often returns newest first when order not specified; we forced ASC
        # Build klines list in ascending time
        if timeframe == "1m":
            sec = 60
        elif timeframe == "3m":
            sec = 180
        elif timeframe == "5m":
            sec = 300
        else:
            sec = 60
        out = []
        for v in vals:
            ts_str = v.get("datetime")
            try:
                # TwelveData datetime is e.g. "2023-07-26 14:33:00"
                dt = datetime.fromisoformat(ts_str)
                ot_ms = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
            except Exception:
                continue
            o = float(v.get("open"))
            h = float(v.get("high"))
            l = float(v.get("low"))
            c = float(v.get("close"))
            vol = float(v.get("volume")) if v.get("volume") is not None else 0.0
            ct_ms = ot_ms + sec * 1000
            out.append([ot_ms, o, h, l, c, vol, ct_ms])
        return out[-limit:] if out else None
    except Exception:
        return None


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
    r = safe_request("GET", url, timeout=8)
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
        r = safe_request("GET", url, timeout=10)
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
        r = safe_request("GET", url, timeout=5)
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


def fetch_ohlc_binance(symbol: str, timeframe: str, limit: int = 300) -> Optional[List[float]]:
    try:
        interval = timeframe
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={min(max(limit,1),1000)}"
        r = safe_request("GET", url, timeout=10)
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
        r = safe_request("GET", url, timeout=5)
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, list):
            return None
        return data
    except Exception:
        return None
