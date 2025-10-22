import math
from datetime import datetime, timezone, timedelta
from typing import List, Optional

import pandas as pd


IST_OFFSET = timedelta(hours=5, minutes=30)
SEPARATOR = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"


class SignalDataMissingError(Exception):
    """Raised when input data frame does not contain enough information."""


def _ist_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(timezone(offset=IST_OFFSET))


def _format_time_ist(ts: Optional[datetime] = None) -> str:
    if ts is None:
        ts = _ist_now()
    return ts.strftime("%I:%M %p IST").lstrip("0")


def _compute_indicators(df: pd.DataFrame) -> dict:
    required_cols = {"close", "volume"}
    if not required_cols.issubset(df.columns):
        raise SignalDataMissingError(f"DataFrame must include columns: {required_cols}")
    if len(df) < 50:
        raise SignalDataMissingError("Need at least 50 rows to compute EMAs and RSI")

    closes = df["close"].astype(float)
    ema50_series = closes.ewm(span=min(50, len(df)), adjust=False).mean()
    ema200_series = closes.ewm(span=min(200, len(df)), adjust=False).mean()
    ema50 = ema50_series.iloc[-1]
    ema200 = ema200_series.iloc[-1]

    delta = closes.diff().dropna()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss.replace({0: math.nan})
    rsi = 100 - (100 / (1 + rs.iloc[-1])) if not math.isnan(rs.iloc[-1]) else 50

    ema12 = closes.ewm(span=12, adjust=False).mean()
    ema26 = closes.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    macd_hist = (macd - signal).iloc[-1]

    volume = df["volume"].astype(float)
    vol_avg = volume.rolling(window=20).mean().iloc[-1]
    vol_last = volume.iloc[-1]

    return {
        "ema50": ema50,
        "ema200": ema200,
        "rsi": rsi,
        "macd_hist": macd_hist,
        "volume_avg": vol_avg,
        "volume_last": vol_last,
        "last_close": closes.iloc[-1],
    }


def _confidence_score(indicators: dict, direction: str) -> int:
    score = 0
    if indicators["ema50"] > indicators["ema200"] and direction == "UP":
        score += 1
    if indicators["ema50"] < indicators["ema200"] and direction == "DOWN":
        score += 1

    if 40 <= indicators["rsi"] <= 60:
        score += 1

    if indicators["macd_hist"] > 0 and direction == "UP":
        score += 1
    if indicators["macd_hist"] < 0 and direction == "DOWN":
        score += 1

    if indicators["volume_last"] > indicators["volume_avg"]:
        score += 1

    return min(score, 5)


def _determine_direction(indicators: dict) -> str:
    bullish = indicators["ema50"] >= indicators["ema200"] and indicators["macd_hist"] >= 0
    return "UP" if bullish else "DOWN"


def _generate_ascii_chart(prices: List[float], times: List[str], ema50: float, ema200: float) -> str:
    if len(prices) < 5:
        raise SignalDataMissingError("Need last five price points to draw chart")

    segment = prices[-5:]
    min_price = min(segment)
    max_price = max(segment)
    if max_price == min_price:
        max_price += 1
        min_price -= 1

    levels = [min_price + (max_price - min_price) * i / 4 for i in reversed(range(5))]
    level_labels = [f"{round(level, 2):>8}" for level in levels]

    scaled_prices = []
    for price in segment:
        idx = round((price - min_price) / (max_price - min_price) * 4)
        scaled_prices.append(idx)

    ema50_level = round((ema50 - min_price) / (max_price - min_price) * 4)
    ema200_level = round((ema200 - min_price) / (max_price - min_price) * 4)

    columns = len(segment)
    grid = [[" " for _ in range(columns)] for _ in range(5)]
    for i in range(columns - 1):
        current_level = scaled_prices[i]
        next_level = scaled_prices[i + 1]
        if next_level > current_level:
            grid[4 - current_level][i] = "/"
            grid[4 - next_level][i + 1] = "\\"
        elif next_level < current_level:
            grid[4 - current_level][i] = "\\"
            grid[4 - next_level][i + 1] = "/"
        else:
            grid[4 - current_level][i] = "-"
            grid[4 - next_level][i + 1] = "-"

    grid[4 - scaled_prices[-1]][-1] = "▲"

    ema50_level = max(0, min(4, ema50_level))
    ema200_level = max(0, min(4, ema200_level))

    ema50_marker = " ← EMA 50"
    ema200_marker = " ← EMA 200"

    chart_lines = ["📈 PRICE ACTION (5-min)"]
    for idx, level in enumerate(levels):
        line = f"{level_labels[idx]} ────────────────"
        row_content = "".join(grid[idx])
        line += row_content
        if idx == 4 - ema50_level:
            line += ema50_marker
            ema50_marker = ""
        if idx == 4 - ema200_level:
            line += ema200_marker
            ema200_marker = ""
        chart_lines.append(line)

    chart_lines.append(" ".join(times[-5:]))
    return "\n".join(chart_lines)


def _build_sample_dataframe() -> pd.DataFrame:
    index = pd.date_range(end=pd.Timestamp.utcnow(), periods=60, freq="5min")
    base_price = 68000
    closes = [base_price + i * 20 + (i % 3) * 15 for i in range(60)]
    volumes = [1000 + (i % 5) * 80 for i in range(60)]
    return pd.DataFrame({"close": closes, "volume": volumes}, index=index)


def generate_signal_with_chart(asset: str, df: Optional[pd.DataFrame] = None) -> str:
    if df is None:
        df = _build_sample_dataframe()

    indicators = _compute_indicators(df)
    prices = df["close"].astype(float).tolist()
    times = df.index.strftime("%H:%M").tolist() if isinstance(df.index, pd.DatetimeIndex) else ["" for _ in range(len(df))]
    times = [t for t in times if t]
    if len(times) < 5:
        times = [f"{i * 5:02d}:00" for i in range(len(prices))]

    direction = _determine_direction(indicators)
    score = _confidence_score(indicators, direction)
    if score < 3:
        score = 3

    chart = _generate_ascii_chart(prices, times, indicators["ema50"], indicators["ema200"])

    analysis_lines = [
        f"• ✅ EMA Golden Cross: {indicators['ema50']:.2f} vs {indicators['ema200']:.2f}",
        f"• ✅ RSI(14): {indicators['rsi']:.2f}",
        f"• ✅ MACD Hist: {indicators['macd_hist']:.4f}",
    ]
    analysis_text = "\n".join(analysis_lines)

    direction_label = "UP 🟢" if direction == "UP" else "DOWN 🔴"
    now_ist = _format_time_ist()

    message = (
        f"🔔 QUOTEXAI PRO SIGNAL — {asset}\n"
        f"{SEPARATOR}\n"
        f"{chart}\n"
        f"🎯 Direction: {direction_label}\n"
        f"📊 Confidence: {score}/5\n"
        f"⏱️ Expiry: 15 min\n"
        f"🔍 Analysis:\n{analysis_text}\n"
        f"⚠️ Trading involves high risk. Not financial advice.\n"
        f"🕒 Generated: {now_ist}"
    )
    return message
