import os
import time
import math
import random
import requests
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field


app = FastAPI(title="Professional Futures Signal Bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BINGX_BASE_URL = "https://open-api.bingx.com"

TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
LEVERAGE = int(os.getenv("LEVERAGE", "10"))

MIN_SCORE = int(os.getenv("MIN_SCORE", "84"))
MIN_RR = float(os.getenv("MIN_RR", "0.75"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.25"))

TP1_POSITION_PERCENT = float(os.getenv("TP1_POSITION_PERCENT", "8"))
TP2_POSITION_PERCENT = float(os.getenv("TP2_POSITION_PERCENT", "15"))
TP3_POSITION_PERCENT = float(os.getenv("TP3_POSITION_PERCENT", "25"))

MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "10"))
DEFAULT_DEPOSIT = float(os.getenv("DEFAULT_DEPOSIT", "1000"))
DEFAULT_RISK_PERCENT = float(os.getenv("DEFAULT_RISK_PERCENT", "0.5"))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "80"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "3600"))

SYMBOL_COOLDOWN: Dict[str, float] = {}
BLOCKED_SYMBOLS: Dict[str, float] = {}

LIQUID_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "INJ", "NEAR", "ARB", "OP", "APT", "SUI", "SEI", "DOT", "LTC",
    "BCH", "UNI", "AAVE", "FIL", "ATOM", "ETC", "TRX", "MATIC", "WLD",
    "TIA", "ORDI", "FTM", "RUNE", "ENA", "JUP", "PYTH", "STRK", "DYDX",
    "TON", "COMP", "STX", "TRB", "JTO", "DYM", "ICP", "APT", "GALA",
    "PEPE", "1000PEPE", "FET", "RNDR", "IMX", "APE"
}


class ManualSignalInput(BaseModel):
    symbol: str = Field(default="NEAR/USDT")
    direction: str = Field(default="LONG")

    entry_min: float = Field(default=0.0)
    entry_max: float = Field(default=0.0)
    stop_loss: float = Field(default=0.0)

    deposit: float = Field(default=1000.0)
    risk_percent: float = Field(default=0.5)

    trend_confirmed: bool = Field(default=True)
    volume_confirmed: bool = Field(default=True)
    btc_confirmed: bool = Field(default=True)
    momentum_confirmed: bool = Field(default=True)
    vwap_confirmed: bool = Field(default=True)
    fakeout_detected: bool = Field(default=False)
    candle_closed_against_signal: bool = Field(default=False)

    send_to_telegram: bool = Field(default=False)


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.upper().replace("/", "-").strip()
    if symbol.endswith("USDT") and "-" not in symbol:
        symbol = symbol.replace("USDT", "-USDT")
    if not symbol.endswith("-USDT"):
        symbol = symbol.replace("-", "") + "-USDT"
    return symbol


def display_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-", "/")


def normalize_direction(direction: str) -> str:
    direction = direction.upper().strip()
    if direction not in ["LONG", "SHORT"]:
        return "LONG"
    return direction


def base_from_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-USDT", "")


def is_good_symbol(symbol: str) -> bool:
    symbol = normalize_symbol(symbol)
    base = base_from_symbol(symbol)

    if not symbol.endswith("-USDT"):
        return False

    if base not in LIQUID_BASES:
        return False

    bad = ["USD", "EUR", "GBP", "JPY", "AAPL", "TSLA", "NVDA", "META", "GOOG"]
    if any(x in base for x in bad):
        return False

    return True


def is_on_cooldown(symbol: str) -> bool:
    last = SYMBOL_COOLDOWN.get(symbol)
    if not last:
        return False
    return time.time() - last < SIGNAL_COOLDOWN_SECONDS


def set_cooldown(symbol: str):
    SYMBOL_COOLDOWN[symbol] = time.time()


def is_blocked(symbol: str) -> bool:
    until = BLOCKED_SYMBOLS.get(symbol)
    if not until:
        return False
    if time.time() > until:
        BLOCKED_SYMBOLS.pop(symbol, None)
        return False
    return True


def get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception:
        return None


def get_symbols() -> List[str]:
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts"
    data = get_json(url)

    if not data:
        return []

    result = []

    for item in data.get("data", []):
        symbol = item.get("symbol")
        if symbol and is_good_symbol(symbol):
            result.append(normalize_symbol(symbol))

    random.shuffle(result)
    return result[:MAX_SYMBOLS]


def get_klines(symbol: str, interval: str, limit: int = 260) -> Optional[List[dict]]:
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"

    params = {
        "symbol": normalize_symbol(symbol),
        "interval": interval,
        "limit": limit
    }

    data = get_json(url, params=params)

    if not data:
        return None

    raw = data.get("data", [])

    if not raw:
        return None

    candles = []

    for c in raw:
        try:
            candles.append({
                "time": int(c["time"]),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c["volume"]),
            })
        except Exception:
            continue

    candles.sort(key=lambda x: x["time"])

    if len(candles) < 60:
        return None

    return candles


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    result = [values[0]]
    for price in values[1:]:
        result.append(price * k + result[-1] * (1 - k))
    return result


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(candles: List[dict], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )

        trs.append(tr)

    return sum(trs[-period:]) / period


def vwap_like(candles: List[dict], period: int = 48) -> Optional[float]:
    if len(candles) < period:
        return None

    total_pv = 0
    total_v = 0

    for c in candles[-period:]:
        typical = (c["high"] + c["low"] + c["close"]) / 3
        volume = c["volume"]
        total_pv += typical * volume
        total_v += volume

    if total_v == 0:
        return None

    return total_pv / total_v


def trend_state(candles: List[dict]) -> str:
    closes = [c["close"] for c in candles]

    if len(closes) < 200:
        return "NEUTRAL"

    ema50 = ema(closes, 50)[-1]
    ema200 = ema(closes, 200)[-1]
    price = closes[-1]

    if price > ema50 > ema200:
        return "BULLISH"

    if price < ema50 < ema200:
        return "BEARISH"

    if price > ema200:
        return "SOFT_BULLISH"

    if price < ema200:
        return "SOFT_BEARISH"

    return "NEUTRAL"


def volume_ratio(candles: List[dict], period: int = 30) -> float:
    if len(candles) < period + 1:
        return 0.0

    avg = sum(c["volume"] for c in candles[-period - 1:-1]) / period

    if avg <= 0:
        return 0.0

    return candles[-1]["volume"] / avg


def recent_move(candles: List[dict], lookback: int = 12) -> float:
    if len(candles) < lookback + 1:
        return 0.0

    old = candles[-lookback]["close"]
    new = candles[-1]["close"]

    if old <= 0:
        return 0.0

    return (new - old) / old * 100


def make_tp(entry: float, direction: str, position_percent: float) -> float:
    price_move = position_percent / LEVERAGE / 100

    if direction == "LONG":
        return entry * (1 + price_move)

    return entry * (1 - price_move)


def price_move_percent(entry: float, target: float, direction: str) -> float:
    if direction == "LONG":
        return (target - entry) / entry * 100
    return (entry - target) / entry * 100


def calc_risk(entry: float, sl: float) -> float:
    return abs(entry - sl) / entry * 100 * LEVERAGE


def calculate_position(entry: float, sl: float, deposit: float, risk_percent: float) -> dict:
    risk_amount = deposit * risk_percent / 100
    stop_distance = abs(entry - sl)

    if entry <= 0 or sl <= 0 or stop_distance <= 0:
        return {
            "risk_amount": round(risk_amount, 2),
            "position_size_usdt": None,
            "coin_amount": None,
            "margin_10x": None,
            "error": "Неверный entry или SL"
        }

    coin_amount = risk_amount / stop_distance
    position_size = coin_amount * entry

    return {
        "risk_amount": round(risk_amount, 2),
        "position_size_usdt": round(position_size, 2),
        "coin_amount": round(coin_amount, 8),
        "margin_10x": round(position_size / 10, 2),
        "error": None
    }


def status_from_score(score: int, fakeout: bool) -> str:
    if fakeout:
        return "FAKEOUT_RISK"
    if score >= MIN_SCORE:
        return "ACTIVE"
    if score >= 70:
        return "WAIT"
    return "CANCELLED"


def status_emoji(status: str) -> str:
    if status == "ACTIVE":
        return "🟢"
    if status == "WAIT":
        return "🟡"
    if status == "FAKEOUT_RISK":
        return "⚠️"
    return "🔴"


def detect_btc_status() -> str:
    btc = get_klines("BTC-USDT", "1h", 260)

    if not btc:
        return "NEUTRAL"

    return trend_state(btc)


def momentum_confirm(c1: List[dict], c5: List[dict], direction: str) -> bool:
    if len(c1) < 20 or len(c5) < 20:
        return False

    closes1 = [c["close"] for c in c1]
    closes5 = [c["close"] for c in c5]

    ema9_1 = ema(closes1, 9)[-1]
    ema9_5 = ema(closes5, 9)[-1]

    last1 = c1[-1]
    prev1 = c1[-2]
    last5 = c5[-1]

    if direction == "LONG":
        return (
            last1["close"] > last1["open"]
            and last1["close"] > prev1["close"]
            and last1["close"] > ema9_1
            and last5["close"] > ema9_5
        )

    return (
        last1["close"] < last1["open"]
        and last1["close"] < prev1["close"]
        and last1["close"] < ema9_1
        and last5["close"] < ema9_5
    )


def build_signal(
    symbol: str,
    direction: str,
    strategy: str,
    entry: float,
    sl: float,
    score: int,
    vol_ratio: float,
    rr: float,
    reason: str,
    deposit: float,
    risk_percent: float
) -> dict:
    tp1 = make_tp(entry, direction, TP1_POSITION_PERCENT)
    tp2 = make_tp(entry, direction, TP2_POSITION_PERCENT)
    tp3 = make_tp(entry, direction, TP3_POSITION_PERCENT)

    risk_position_percent = calc_risk(entry, sl)
    pos = calculate_position(entry, sl, deposit, risk_percent)

    status = status_from_score(score, fakeout=False)

    return {
        "symbol": display_symbol(symbol),
        "direction": direction,
        "strategy": strategy,
        "status": status,
        "score": score,
        "entry": round(entry, 8),
        "sl": round(sl, 8),
        "tp1": round(tp1, 8),
        "tp2": round(tp2, 8),
        "tp3": round(tp3, 8),
        "rr": round(rr, 2),
        "volume_ratio": round(vol_ratio, 2),
        "risk_position_percent": round(risk_position_percent, 2),
        "position": pos,
        "reason": reason,
    }


def evaluate_breakout(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    closes15 = [c["close"] for c in c15]
    last = c15[-1]
    prev = c15[-2]
    price = last["close"]

    a = atr(c15)
    vw = vwap_like(c15)
    rs = rsi(closes15)
    vr = volume_ratio(c15)

    if a is None or vw is None or rs is None:
        return None

    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    highs = [c["high"] for c in c15[-32:-2]]
    lows = [c["low"] for c in c15[-32:-2]]

    score = 60

    if vr >= MIN_VOLUME_RATIO:
        score += 10
    if vr >= 1.6:
        score += 5
    if momentum_confirm(c1, c5, direction):
        score += 10

    if direction == "LONG":
        level = max(highs)
        broke = price > level * 1.001 and prev["close"] <= level * 1.002

        if not broke:
            return None

        if btc_status == "BEARISH":
            return None

        if trend1h == "BEARISH":
            return None

        if price < vw * 0.995:
            return None

        if not (52 <= rs <= 82):
            return None

        if trend1h in ["BULLISH", "SOFT_BULLISH"]:
            score += 7

        if trend4h in ["BULLISH", "SOFT_BULLISH"]:
            score += 3

        sl = min(level - a * 0.18, min(c["low"] for c in c15[-8:]) - a * 0.05)

    else:
        level = min(lows)
        broke = price < level * 0.999 and prev["close"] >= level * 0.998

        if not broke:
            return None

        if btc_status == "BULLISH":
            return None

        if trend1h == "BULLISH":
            return None

        if price > vw * 1.005:
            return None

        if not (18 <= rs <= 48):
            return None

        if trend1h in ["BEARISH", "SOFT_BEARISH"]:
            score += 7

        if trend4h in ["BEARISH", "SOFT_BEARISH"]:
            score += 3

        sl = max(level + a * 0.18, max(c["high"] for c in c15[-8:]) + a * 0.05)

    risk_pos = calc_risk(price, sl)

    if risk_pos > MAX_RISK_POSITION_PERCENT:
        return None

    tp1 = make_tp(price, direction, TP1_POSITION_PERCENT)
    reward = price_move_percent(price, tp1, direction)
    risk_price = abs(price - sl) / price * 100
    rr = reward / risk_price if risk_price > 0 else 0

    if rr < MIN_RR:
        return None

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy="🚀 Breakout Momentum",
        entry=price,
        sl=sl,
        score=min(score, 95),
        vol_ratio=vr,
        rr=rr,
        reason="Пробой уровня с объёмом и подтверждением 1m/5m",
        deposit=deposit,
        risk_percent=risk_percent
    )


def evaluate_pullback(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    closes15 = [c["close"] for c in c15]
    last = c15[-1]
    prev = c15[-2]
    price = last["close"]

    a = atr(c15)
    vw = vwap_like(c15)
    rs = rsi(closes15)
    vr = volume_ratio(c15)

    if a is None or vw is None or rs is None:
        return None

    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    score = 60

    if vr >= MIN_VOLUME_RATIO:
        score += 8
    if momentum_confirm(c1, c5, direction):
        score += 12

    if direction == "LONG":
        if btc_status == "BEARISH":
            return None

        if trend1h not in ["BULLISH", "SOFT_BULLISH"]:
            return None

        pulled_to_vwap = price >= vw * 0.985 and price <= vw * 1.015
        bounce = last["close"] > last["open"] and last["close"] > prev["close"]

        if not pulled_to_vwap or not bounce:
            return None

        if rs > 62:
            return None

        if trend4h in ["BULLISH", "SOFT_BULLISH"]:
            score += 8

        sl = min(last["low"] - a * 0.2, min(c["low"] for c in c15[-8:]) - a * 0.05)

    else:
        if btc_status == "BULLISH":
            return None

        if trend1h not in ["BEARISH", "SOFT_BEARISH"]:
            return None

        pulled_to_vwap = price >= vw * 0.985 and price <= vw * 1.015
        rejection = last["close"] < last["open"] and last["close"] < prev["close"]

        if not pulled_to_vwap or not rejection:
            return None

        if rs < 38:
            return None

        if trend4h in ["BEARISH", "SOFT_BEARISH"]:
            score += 8

        sl = max(last["high"] + a * 0.2, max(c["high"] for c in c15[-8:]) + a * 0.05)

    risk_pos = calc_risk(price, sl)

    if risk_pos > MAX_RISK_POSITION_PERCENT:
        return None

    tp1 = make_tp(price, direction, TP1_POSITION_PERCENT)
    reward = price_move_percent(price, tp1, direction)
    risk_price = abs(price - sl) / price * 100
    rr = reward / risk_price if risk_price > 0 else 0

    if rr < MIN_RR:
        return None

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy="📌 Trend Pullback",
        entry=price,
        sl=sl,
        score=min(score, 95),
        vol_ratio=vr,
        rr=rr,
        reason="Откат к VWAP/зоне по направлению 1h-тренда",
        deposit=deposit,
        risk_percent=risk_percent
    )


def evaluate_sweep(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    closes15 = [c["close"] for c in c15]
    last = c15[-1]
    prev = c15[-2]
    price = last["close"]

    a = atr(c15)
    vw = vwap_like(c15)
    rs = rsi(closes15)
    vr = volume_ratio(c15)

    if a is None or vw is None or rs is None:
        return None

    trend1h = trend_state(c1h)

    recent_high = max(c["high"] for c in c15[-28:-3])
    recent_low = min(c["low"] for c in c15[-28:-3])

    score = 62

    if vr >= MIN_VOLUME_RATIO:
        score += 8
    if momentum_confirm(c1, c5, direction):
        score += 12

    if direction == "LONG":
        swept = prev["low"] < recent_low * 0.998
        reclaimed = prev["close"] > recent_low
        confirm = last["close"] > last["open"] and last["close"] > prev["close"]

        if not swept or not reclaimed or not confirm:
            return None

        if btc_status == "BEARISH":
            return None

        if trend1h == "BEARISH":
            return None

        if price < vw * 0.975:
            return None

        if rs > 58:
            return None

        sl = min(prev["low"] - a * 0.08, min(c["low"] for c in c15[-8:]) - a * 0.05)

    else:
        swept = prev["high"] > recent_high * 1.002
        reclaimed = prev["close"] < recent_high
        confirm = last["close"] < last["open"] and last["close"] < prev["close"]

        if not swept or not reclaimed or not confirm:
            return None

        if btc_status == "BULLISH":
            return None

        if trend1h == "BULLISH":
            return None

        if price > vw * 1.025:
            return None

        if rs < 42:
            return None

        sl = max(prev["high"] + a * 0.08, max(c["high"] for c in c15[-8:]) + a * 0.05)

    risk_pos = calc_risk(price, sl)

    if risk_pos > MAX_RISK_POSITION_PERCENT:
        return None

    tp1 = make_tp(price, direction, TP1_POSITION_PERCENT)
    reward = price_move_percent(price, tp1, direction)
    risk_price = abs(price - sl) / price * 100
    rr = reward / risk_price if risk_price > 0 else 0

    if rr < MIN_RR:
        return None

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy="🧲 Sweep Reclaim",
        entry=price,
        sl=sl,
        score=min(score, 95),
        vol_ratio=vr,
        rr=rr,
        reason="Снятие ликвидности за уровень и возврат обратно",
        deposit=deposit,
        risk_percent=risk_percent
    )


def analyze_symbol(symbol: str, direction: Optional[str], deposit: float, risk_percent: float) -> Optional[dict]:
    symbol = normalize_symbol(symbol)

    if is_blocked(symbol) or is_on_cooldown(symbol):
        return None

    c15 = get_klines(symbol, "15m", 260)
    c5 = get_klines(symbol, "5m", 180)
    c1 = get_klines(symbol, "1m", 120)
    c1h = get_klines(symbol, "1h", 260)
    c4h = get_klines(symbol, "4h", 260)

    if not c15 or not c5 or not c1 or not c1h or not c4h:
        return None

    btc_status = detect_btc_status()

    directions = [normalize_direction(direction)] if direction else ["LONG", "SHORT"]

    candidates = []

    for d in directions:
        for func in [evaluate_breakout, evaluate_pullback, evaluate_sweep]:
            signal = func(symbol, d, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent)
            if signal:
                candidates.append(signal)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (x["score"], x["rr"], x["volume_ratio"]),
        reverse=True
    )

    return candidates[0]


def build_message(signal: dict) -> str:
    emoji = status_emoji(signal["status"])
    arrow = "📈" if signal["direction"] == "LONG" else "📉"
    mode = "TEST SIGNAL" if TEST_MODE else "TRADE SIGNAL"

    pos = signal["position"]

    if pos["error"]:
        pos_text = f"⚠️ RM Error: {pos['error']}"
    else:
        pos_text = (
            f"⚠️ Risk: {DEFAULT_RISK_PERCENT}% = {pos['risk_amount']} USDT\n"
            f"📦 Position: {pos['position_size_usdt']} USDT\n"
            f"💵 Margin x10: {pos['margin_10x']} USDT"
        )

    return f"""
{emoji} <b>Professional Futures Signal Bot</b> · <b>{mode}</b>

{arrow} <b>{signal['direction']} {signal['symbol']}</b>
Стратегия: <b>{signal['strategy']}</b>

🎯 Score: <b>{signal['score']}/100</b>
📊 Status: <b>{signal['status']}</b>

🎯 Entry: <code>{signal['entry']}</code>
🛑 SL: <code>{signal['sl']}</code>

✅ TP1: <code>{signal['tp1']}</code>
✅ TP2: <code>{signal['tp2']}</code>
✅ TP3: <code>{signal['tp3']}</code>

📊 RR до TP1: <b>{signal['rr']}</b>
📊 Volume: x<b>{signal['volume_ratio']}</b>
🛡 Risk до SL: <b>{signal['risk_position_percent']}%</b> по позиции

{pos_text}

🧠 Причина:
{signal['reason']}

⚠️ Не финансовый совет. Сначала TEST/минимальная сумма.
""".strip()


def send_telegram_message(text: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {
            "ok": False,
            "error": "TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не указаны"
        }

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception as e:
        return {
            "ok": False,
            "error": str(e)
        }


@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Professional Futures Signal Bot</title>
</head>
<body style="background:#020617;color:#e5e7eb;font-family:Arial;padding:40px;">
    <h1>✅ Professional Futures Signal Bot работает</h1>
    <p>Endpoints:</p>
    <pre>
GET /health
GET /auto-signal?symbol=NEAR/USDT
GET /auto-signal?symbol=NEAR/USDT&direction=LONG
GET /scan?send_to_telegram=false
GET /test-telegram
    </pre>
    <p>Start Command:</p>
    <pre>uvicorn bot:app --host 0.0.0.0 --port $PORT</pre>
</body>
</html>
"""


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Professional Futures Signal Bot",
        "test_mode": TEST_MODE,
        "min_score": MIN_SCORE,
        "min_volume_ratio": MIN_VOLUME_RATIO,
        "leverage": LEVERAGE
    }


@app.get("/test-telegram")
def test_telegram():
    return send_telegram_message("✅ Professional Futures Signal Bot подключён к Telegram.")


@app.get("/auto-signal")
def auto_signal(
    symbol: str = Query(default="NEAR/USDT"),
    direction: Optional[str] = Query(default=None),
    deposit: float = Query(default=DEFAULT_DEPOSIT),
    risk_percent: float = Query(default=DEFAULT_RISK_PERCENT),
    send_to_telegram: bool = Query(default=False)
):
    signal = analyze_symbol(symbol, direction, deposit, risk_percent)

    if not signal:
        return {
            "ok": False,
            "symbol": display_symbol(symbol),
            "direction": direction,
            "message": "Сильного сигнала нет. Вход запрещён."
        }

    message = build_message(signal)

    telegram = None
    if send_to_telegram:
        telegram = send_telegram_message(message)
        set_cooldown(normalize_symbol(symbol))

    return {
        "ok": True,
        "signal": signal,
        "message": message,
        "telegram": telegram
    }


@app.get("/scan")
def scan(
    send_to_telegram: bool = Query(default=False),
    deposit: float = Query(default=DEFAULT_DEPOSIT),
    risk_percent: float = Query(default=DEFAULT_RISK_PERCENT)
):
    symbols = get_symbols()

    best = None
    checked = 0

    for symbol in symbols:
        checked += 1

        signal = analyze_symbol(symbol, None, deposit, risk_percent)

        if not signal:
            continue

        if best is None:
            best = signal
        else:
            current_key = (signal["score"], signal["rr"], signal["volume_ratio"])
            best_key = (best["score"], best["rr"], best["volume_ratio"])

            if current_key > best_key:
                best = signal

    if not best:
        return {
            "ok": False,
            "checked": checked,
            "message": "Сильных сигналов сейчас нет."
        }

    message = build_message(best)

    telegram = None
    if send_to_telegram:
        telegram = send_telegram_message(message)
        set_cooldown(normalize_symbol(best["symbol"]))

    return {
        "ok": True,
        "checked": checked,
        "signal": best,
        "message": message,
        "telegram": telegram
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("bot:app", host="0.0.0.0", port=port)
