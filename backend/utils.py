import logging
import os
import random
import time as time_module
from datetime import datetime, timezone, time, timedelta
from typing import Optional, Dict, Any

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
    return {"notices": notices, "expired": expired}


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
    """Fetch OHLC and compute indicators using provider priority.
    Returns {ok, rsi, macd_hist, ema_fast_over_slow, bb_pos, stoch} or {ok: False}
    """
    closes = None
    # Finnhub
    try:
        closes = fetch_ohlc_finnhub(pair, timeframe, limit=200)
    except Exception:
        closes = None
    if not closes:
        try:
            closes = fetch_ohlc_twelvedata(pair, timeframe, limit=200)
        except Exception:
            closes = None
    if not closes:
        try:
            closes = fetch_ohlc_alphavantage(pair, timeframe, limit=200)
        except Exception:
            closes = None
    if not closes or len(closes) < 35:
        return {"ok": False}
    return {"ok": True, **compute_indicators(closes)}


def _tf_maps(timeframe: str) -> Dict[str, Any]:
    return {
        "finnhub": {"1m": ("1", 60), "3m": ("3", 180), "5m": ("5", 300)},
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
    r = requests.get(url, timeout=8)
    if r.status_code != 200:
        return None
    j = r.json()
    if j.get("s") != "ok":
        return None
    closes = j.get("c") or []
    return closes[-limit:]


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
    r = requests.get(url, timeout=12)
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
