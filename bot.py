import os
import asyncio
import random
import time
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv


load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BINGX_BASE_URL = "https://open-api.bingx.com"

ENTRY_TIMEFRAME = os.getenv("ENTRY_TIMEFRAME", "15m")
CONFIRM_TIMEFRAME = os.getenv("CONFIRM_TIMEFRAME", "5m")
TREND_TIMEFRAME = os.getenv("TREND_TIMEFRAME", "1h")
MACRO_TIMEFRAME = os.getenv("MACRO_TIMEFRAME", "4h")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "120"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "220"))

LEVERAGE = int(os.getenv("LEVERAGE", "20"))

MIN_POSITION_PROFIT_PERCENT = float(os.getenv("MIN_POSITION_PROFIT_PERCENT", "20"))
MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "5"))
MIN_RR = float(os.getenv("MIN_RR", "2.0"))

MIN_CONFLUENCE_SCORE = int(os.getenv("MIN_CONFLUENCE_SCORE", "9"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "14400"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "2"))
DAILY_MAX_SIGNALS = int(os.getenv("DAILY_MAX_SIGNALS", "4"))

BTC_SYMBOL = "BTC-USDT"

SENT_SIGNALS = {}
SYMBOL_COOLDOWN = {}

STATS = {
    "signals_today": 0,
    "signals_total": 0,
    "current_day": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
}


def reset_daily_stats_if_needed():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if STATS["current_day"] != today:
        STATS["current_day"] = today
        STATS["signals_today"] = 0


def now_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def is_on_cooldown(symbol):
    last_time = SYMBOL_COOLDOWN.get(symbol)
    if not last_time:
        return False
    return time.time() - last_time < SIGNAL_COOLDOWN_SECONDS


def set_cooldown(symbol):
    SYMBOL_COOLDOWN[symbol] = time.time()


async def send_telegram_message(session, text, button_url=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    if button_url:
        payload["reply_markup"] = {
            "inline_keyboard": [
                [{"text": "Открыть на BingX", "url": button_url}]
            ]
        }

    async with session.post(url, json=payload, timeout=30) as resp:
        data = await resp.text()
        if resp.status != 200:
            print("Telegram error:", resp.status, data)
        return data


async def get_symbols(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts"

    async with session.get(url, timeout=30) as resp:
        data = await resp.json()

    symbols = []

    for item in data.get("data", []):
        symbol = item.get("symbol")
        if symbol and symbol.endswith("-USDT"):
            symbols.append(symbol)

    random.shuffle(symbols)

    if BTC_SYMBOL not in symbols:
        symbols.append(BTC_SYMBOL)

    return symbols[:MAX_SYMBOLS]


async def get_klines(session, symbol, interval, limit=220):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    async with session.get(url, params=params, timeout=30) as resp:
        data = await resp.json()

    raw = data.get("data", [])

    if not raw or len(raw) < 100:
        return None

    candles = []

    for candle in raw:
        try:
            candles.append({
                "time": int(candle["time"]),
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": float(candle["volume"]),
            })
        except Exception:
            continue

    candles.sort(key=lambda x: x["time"])
    return candles


def ema(values, period):
    if not values:
        return []

    multiplier = 2 / (period + 1)
    result = [values[0]]

    for price in values[1:]:
        result.append((price - result[-1]) * multiplier + result[-1])

    return result


def sma(values, period):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def stddev(values, period):
    if len(values) < period:
        return None
    mean = sma(values, period)
    variance = sum((x - mean) ** 2 for x in values[-period:]) / period
    return variance ** 0.5


def calculate_rsi(values, period=14):
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    true_ranges = []

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        true_ranges.append(tr)

    return sum(true_ranges[-period:]) / period


def calculate_macd(values):
    if len(values) < 35:
        return None

    ema12 = ema(values, 12)
    ema26 = ema(values, 26)

    macd_line = []

    for i in range(len(values)):
        macd_line.append(ema12[i] - ema26[i])

    signal_line = ema(macd_line, 9)

    return {
        "macd": macd_line[-1],
        "signal": signal_line[-1],
        "hist": macd_line[-1] - signal_line[-1],
        "prev_hist": macd_line[-2] - signal_line[-2],
    }


def calculate_bollinger(values, period=20, mult=2):
    mid = sma(values, period)
    sd = stddev(values, period)

    if mid is None or sd is None:
        return None

    return {
        "upper": mid + sd * mult,
        "middle": mid,
        "lower": mid - sd * mult,
    }


def calculate_vwap_like(candles, period=48):
    if len(candles) < period:
        return None

    total_pv = 0
    total_volume = 0

    for c in candles[-period:]:
        typical_price = (c["high"] + c["low"] + c["close"]) / 3
        volume = c["volume"]
        total_pv += typical_price * volume
        total_volume += volume

    if total_volume == 0:
        return None

    return total_pv / total_volume


def calculate_adx(candles, period=14):
    if len(candles) < period + 2:
        return None

    plus_dm = []
    minus_dm = []
    true_ranges = []

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_high = candles[i - 1]["high"]
        prev_low = candles[i - 1]["low"]
        prev_close = candles[i - 1]["close"]

        up_move = high - prev_high
        down_move = prev_low - low

        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0)

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        true_ranges.append(tr)

    atr_sum = sum(true_ranges[-period:])

    if atr_sum == 0:
        return None

    plus_di = 100 * (sum(plus_dm[-period:]) / atr_sum)
    minus_di = 100 * (sum(minus_dm[-period:]) / atr_sum)

    dx = abs(plus_di - minus_di) / max(plus_di + minus_di, 1e-9) * 100

    return {
        "adx": dx,
        "plus_di": plus_di,
        "minus_di": minus_di,
    }


def trend_filter(candles):
    closes = [c["close"] for c in candles]

    ema20_list = ema(closes, 20)
    ema50_list = ema(closes, 50)
    ema100_list = ema(closes, 100)

    price = closes[-1]
    ema20 = ema20_list[-1]
    ema50 = ema50_list[-1]
    ema100 = ema100_list[-1]

    if price > ema20 > ema50 > ema100:
        return "STRONG_BULLISH"

    if price < ema20 < ema50 < ema100:
        return "STRONG_BEARISH"

    if price > ema20 > ema50:
        return "BULLISH"

    if price < ema20 < ema50:
        return "BEARISH"

    return "NEUTRAL"


def btc_market_filter(btc_1h):
    closes = [c["close"] for c in btc_1h]

    ema20_list = ema(closes, 20)
    ema50_list = ema(closes, 50)
    ema100_list = ema(closes, 100)

    price = closes[-1]
    ema20 = ema20_list[-1]
    ema50 = ema50_list[-1]
    ema100 = ema100_list[-1]

    prev = closes[-4] if len(closes) >= 4 else closes[-2]
    change = ((price - prev) / prev) * 100

    if price > ema20 > ema50 > ema100 and change > 0.10:
        return "STRONG_BULLISH", change

    if price < ema20 < ema50 < ema100 and change < -0.10:
        return "STRONG_BEARISH", change

    if price > ema20 > ema50:
        return "BULLISH", change

    if price < ema20 < ema50:
        return "BEARISH", change

    return "NEUTRAL", change


def position_profit_percent(entry, target, side):
    if side == "LONG":
        price_move = (target - entry) / entry * 100
    else:
        price_move = (entry - target) / entry * 100

    return price_move * LEVERAGE


def price_move_percent(entry, target, side):
    if side == "LONG":
        return (target - entry) / entry * 100
    return (entry - target) / entry * 100


def format_price(x):
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.8f}"


def find_supports(candles_1h, candles_4h):
    levels = []

    for c in candles_1h[-120:]:
        levels.append(c["low"])

    for c in candles_4h[-80:]:
        levels.append(c["low"])

    return sorted(set(levels))


def find_resistances(candles_1h, candles_4h):
    levels = []

    for c in candles_1h[-120:]:
        levels.append(c["high"])

    for c in candles_4h[-80:]:
        levels.append(c["high"])

    return sorted(set(levels))


def nearest_support(entry, candles_1h, candles_4h):
    supports = find_supports(candles_1h, candles_4h)
    candidates = [x for x in supports if x < entry * 0.997]

    if not candidates:
        return None

    return max(candidates)


def nearest_resistance(entry, candles_1h, candles_4h):
    resistances = find_resistances(candles_1h, candles_4h)
    candidates = [x for x in resistances if x > entry * 1.003]

    if not candidates:
        return None

    return min(candidates)


def current_support_zone(price, candles_1h, candles_4h):
    supports = find_supports(candles_1h, candles_4h)

    near = []

    for level in supports:
        distance = abs(price - level) / price * 100
        if distance <= 0.45:
            near.append(level)

    if not near:
        return None

    return max(near)


def current_resistance_zone(price, candles_1h, candles_4h):
    resistances = find_resistances(candles_1h, candles_4h)

    near = []

    for level in resistances:
        distance = abs(price - level) / price * 100
        if distance <= 0.45:
            near.append(level)

    if not near:
        return None

    return min(near)


def detect_large_danger_candle(candles, atr):
    last = candles[-1]
    body = abs(last["close"] - last["open"])
    full_range = last["high"] - last["low"]

    if atr <= 0:
        return True

    if full_range > atr * 2.2:
        return True

    if body > atr * 1.6:
        return True

    return False


def build_common_signal(
    symbol,
    side,
    setup_type,
    price,
    entry_zone_low,
    entry_zone_high,
    sl,
    tp1,
    tp2,
    tp3,
    rsi,
    volume_ratio,
    btc_status,
    trend_1h,
    trend_4h,
    checks,
):
    if side == "LONG":
        if sl >= price:
            return None
        if tp1 <= price:
            return None
    else:
        if sl <= price:
            return None
        if tp1 >= price:
            return None

    risk_price_percent = abs(sl - price) / price * 100
    risk_position_percent = risk_price_percent * LEVERAGE

    if risk_position_percent > MAX_RISK_POSITION_PERCENT:
        return None

    tp1_position_profit = position_profit_percent(price, tp1, side)
    tp2_position_profit = position_profit_percent(price, tp2, side)
    tp3_position_profit = position_profit_percent(price, tp3, side)

    if tp1_position_profit < MIN_POSITION_PROFIT_PERCENT:
        return None

    reward_price_percent = price_move_percent(price, tp1, side)
    rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

    if rr < MIN_RR:
        return None

    passed = [name for name, ok in checks if ok]

    if len(passed) < MIN_CONFLUENCE_SCORE:
        return None

    quality = min(99, 55 + len(passed) * 3)

    signal_id = f"{symbol}:V7:{setup_type}:{side}:{round(price, 6)}"

    if signal_id in SENT_SIGNALS:
        return None

    return {
        "symbol": symbol,
        "side": side,
        "setup_type": setup_type,
        "quality": quality,
        "entry": price,
        "entry_zone_low": entry_zone_low,
        "entry_zone_high": entry_zone_high,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "tp1_position_profit": tp1_position_profit,
        "tp2_position_profit": tp2_position_profit,
        "tp3_position_profit": tp3_position_profit,
        "risk_price_percent": risk_price_percent,
        "risk_position_percent": risk_position_percent,
        "rr": rr,
        "rsi": rsi,
        "volume_ratio": volume_ratio,
        "btc_status": btc_status,
        "trend_1h": trend_1h,
        "trend_4h": trend_4h,
        "confluence_score": len(passed),
        "max_confluence_score": len(checks),
        "reasons": passed,
        "signal_id": signal_id,
    }


def build_pullback_signal(symbol, side, candles_15m, candles_5m, candles_1h, candles_4h, btc_status):
    closes_15 = [c["close"] for c in candles_15m]
    volumes_15 = [c["volume"] for c in candles_15m]

    last_15 = candles_15m[-1]
    prev_15 = candles_15m[-2]

    price = last_15["close"]

    ema20_15 = ema(closes_15, 20)[-1]
    ema50_15 = ema(closes_15, 50)[-1]
    ema100_15 = ema(closes_15, 100)[-1]

    rsi = calculate_rsi(closes_15, 14)
    atr = calculate_atr(candles_15m, 14)
    macd = calculate_macd(closes_15)
    bb = calculate_bollinger(closes_15, 20, 2)
    vwap = calculate_vwap_like(candles_15m, 48)
    adx = calculate_adx(candles_15m, 14)

    if rsi is None or atr is None or macd is None or bb is None or vwap is None or adx is None:
        return None

    if detect_large_danger_candle(candles_15m, atr):
        return None

    trend_1h = trend_filter(candles_1h)
    trend_4h = trend_filter(candles_4h)

    avg_volume = sum(volumes_15[-30:]) / 30
    volume_ratio = last_15["volume"] / avg_volume if avg_volume > 0 else 0

    closes_5 = [c["close"] for c in candles_5m]
    last_5 = candles_5m[-1]
    prev_5 = candles_5m[-2]

    ema20_5 = ema(closes_5, 20)[-1]
    ema50_5 = ema(closes_5, 50)[-1]

    checks = []

    if side == "LONG":
        target = nearest_resistance(price, candles_1h, candles_4h)

        if target is None:
            return None

        local_low = min(c["low"] for c in candles_15m[-12:])
        sl = local_low - atr * 0.15

        tp1 = target
        tp2 = price * (1 + (35 / LEVERAGE) / 100)
        tp3 = price * (1 + (60 / LEVERAGE) / 100)

        if tp2 <= tp1:
            tp2 = tp1 * 1.005

        if tp3 <= tp2:
            tp3 = tp2 * 1.008

        touched_ema_zone = (
            prev_15["low"] <= ema20_15 * 1.004
            or prev_15["low"] <= ema50_15 * 1.004
            or last_15["low"] <= ema20_15 * 1.004
            or last_15["low"] <= ema50_15 * 1.004
        )

        candle_confirmation = (
            last_15["close"] > last_15["open"]
            and last_15["close"] > prev_15["close"]
            and last_15["close"] > ema20_15
        )

        five_min_confirmation = (
            last_5["close"] > last_5["open"]
            and last_5["close"] > ema20_5
            and ema20_5 > ema50_5
            and last_5["close"] > prev_5["close"]
        )

        tp1_profit = position_profit_percent(price, tp1, "LONG")

        checks = [
            ("4h bullish trend", trend_4h in ["BULLISH", "STRONG_BULLISH"]),
            ("1h bullish trend", trend_1h in ["BULLISH", "STRONG_BULLISH"]),
            ("BTC не против LONG", btc_status not in ["BEARISH", "STRONG_BEARISH"]),
            ("15m EMA trend bullish", price > ema20_15 > ema50_15 > ema100_15),
            ("откат к EMA20/EMA50", touched_ema_zone),
            ("15m свеча отскока", candle_confirmation),
            ("5m подтверждение входа", five_min_confirmation),
            ("RSI рабочая зона", 40 <= rsi <= 58),
            ("MACD подтверждает LONG", macd["hist"] > 0 and macd["hist"] >= macd["prev_hist"]),
            ("цена выше VWAP", price > vwap),
            ("Bollinger не перекуплен", price < bb["upper"]),
            ("объём выше среднего", volume_ratio >= 1.20),
            ("ADX тренд есть", adx["adx"] >= 18),
            ("до сопротивления есть +20%+", tp1_profit >= MIN_POSITION_PROFIT_PERCENT),
        ]

        entry_zone_low = price - atr * 0.15
        entry_zone_high = price + atr * 0.08

        return build_common_signal(
            symbol, "LONG", "Professional Pullback LONG",
            price, entry_zone_low, entry_zone_high,
            sl, tp1, tp2, tp3,
            rsi, volume_ratio, btc_status, trend_1h, trend_4h, checks
        )

    else:
        target = nearest_support(price, candles_1h, candles_4h)

        if target is None:
            return None

        local_high = max(c["high"] for c in candles_15m[-12:])
        sl = local_high + atr * 0.15

        tp1 = target
        tp2 = price * (1 - (35 / LEVERAGE) / 100)
        tp3 = price * (1 - (60 / LEVERAGE) / 100)

        if tp2 >= tp1:
            tp2 = tp1 * 0.995

        if tp3 >= tp2:
            tp3 = tp2 * 0.992

        touched_ema_zone = (
            prev_15["high"] >= ema20_15 * 0.996
            or prev_15["high"] >= ema50_15 * 0.996
            or last_15["high"] >= ema20_15 * 0.996
            or last_15["high"] >= ema50_15 * 0.996
        )

        candle_confirmation = (
            last_15["close"] < last_15["open"]
            and last_15["close"] < prev_15["close"]
            and last_15["close"] < ema20_15
        )

        five_min_confirmation = (
            last_5["close"] < last_5["open"]
            and last_5["close"] < ema20_5
            and ema20_5 < ema50_5
            and last_5["close"] < prev_5["close"]
        )

        tp1_profit = position_profit_percent(price, tp1, "SHORT")

        checks = [
            ("4h bearish trend", trend_4h in ["BEARISH", "STRONG_BEARISH"]),
            ("1h bearish trend", trend_1h in ["BEARISH", "STRONG_BEARISH"]),
            ("BTC не против SHORT", btc_status not in ["BULLISH", "STRONG_BULLISH"]),
            ("15m EMA trend bearish", price < ema20_15 < ema50_15 < ema100_15),
            ("откат к EMA20/EMA50", touched_ema_zone),
            ("15m свеча rejection", candle_confirmation),
            ("5m подтверждение входа", five_min_confirmation),
            ("RSI рабочая зона", 42 <= rsi <= 62),
            ("MACD подтверждает SHORT", macd["hist"] < 0 and macd["hist"] <= macd["prev_hist"]),
            ("цена ниже VWAP", price < vwap),
            ("Bollinger не перепродан", price > bb["lower"]),
            ("объём выше среднего", volume_ratio >= 1.20),
            ("ADX тренд есть", adx["adx"] >= 18),
            ("до поддержки есть +20%+", tp1_profit >= MIN_POSITION_PROFIT_PERCENT),
        ]

        entry_zone_low = price - atr * 0.08
        entry_zone_high = price + atr * 0.15

        return build_common_signal(
            symbol, "SHORT", "Professional Pullback SHORT",
            price, entry_zone_low, entry_zone_high,
            sl, tp1, tp2, tp3,
            rsi, volume_ratio, btc_status, trend_1h, trend_4h, checks
        )


def build_bounce_signal(symbol, side, candles_15m, candles_5m, candles_1h, candles_4h, btc_status):
    closes_15 = [c["close"] for c in candles_15m]
    volumes_15 = [c["volume"] for c in candles_15m]

    last_15 = candles_15m[-1]
    prev_15 = candles_15m[-2]

    price = last_15["close"]

    rsi = calculate_rsi(closes_15, 14)
    atr = calculate_atr(candles_15m, 14)
    macd = calculate_macd(closes_15)
    bb = calculate_bollinger(closes_15, 20, 2)
    vwap = calculate_vwap_like(candles_15m, 48)
    adx = calculate_adx(candles_15m, 14)

    if rsi is None or atr is None or macd is None or bb is None or vwap is None or adx is None:
        return None

    if detect_large_danger_candle(candles_15m, atr):
        return None

    trend_1h = trend_filter(candles_1h)
    trend_4h = trend_filter(candles_4h)

    avg_volume = sum(volumes_15[-30:]) / 30
    volume_ratio = last_15["volume"] / avg_volume if avg_volume > 0 else 0

    closes_5 = [c["close"] for c in candles_5m]
    last_5 = candles_5m[-1]
    prev_5 = candles_5m[-2]

    ema20_5 = ema(closes_5, 20)[-1]
    ema50_5 = ema(closes_5, 50)[-1]

    checks = []

    if side == "LONG":
        support = current_support_zone(price, candles_1h, candles_4h)
        resistance = nearest_resistance(price, candles_1h, candles_4h)

        if support is None or resistance is None:
            return None

        near_support = abs(price - support) / price * 100 <= 0.45

        bounce_candle = (
            last_15["close"] > last_15["open"]
            and last_15["low"] <= support * 1.004
            and last_15["close"] > support
            and last_15["close"] > prev_15["close"]
        )

        five_min_bounce = (
            last_5["close"] > last_5["open"]
            and last_5["close"] > ema20_5
            and last_5["close"] > prev_5["close"]
        )

        tp1 = resistance
        tp2 = price * (1 + (35 / LEVERAGE) / 100)
        tp3 = price * (1 + (60 / LEVERAGE) / 100)

        if tp2 <= tp1:
            tp2 = tp1 * 1.005

        if tp3 <= tp2:
            tp3 = tp2 * 1.008

        sl = support - atr * 0.25

        tp1_profit = position_profit_percent(price, tp1, "LONG")

        checks = [
            ("цена у 1h/4h поддержки", near_support),
            ("15m свеча отскока от поддержки", bounce_candle),
            ("5m подтверждает отскок", five_min_bounce),
            ("BTC не против LONG", btc_status not in ["BEARISH", "STRONG_BEARISH"]),
            ("4h не сильный bearish", trend_4h != "STRONG_BEARISH"),
            ("1h не сильный bearish", trend_1h != "STRONG_BEARISH"),
            ("RSI показывает зону отскока", 28 <= rsi <= 55),
            ("MACD улучшается", macd["hist"] >= macd["prev_hist"]),
            ("цена не выше верхней Bollinger", price < bb["upper"]),
            ("цена рядом/выше VWAP", price >= vwap * 0.995),
            ("объём подтверждает отскок", volume_ratio >= 1.15),
            ("ADX не запрещает вход", adx["adx"] >= 14),
            ("до сопротивления есть +20%+", tp1_profit >= MIN_POSITION_PROFIT_PERCENT),
        ]

        entry_zone_low = max(support, price - atr * 0.20)
        entry_zone_high = price + atr * 0.10

        return build_common_signal(
            symbol, "LONG", "Support Bounce LONG",
            price, entry_zone_low, entry_zone_high,
            sl, tp1, tp2, tp3,
            rsi, volume_ratio, btc_status, trend_1h, trend_4h, checks
        )

    else:
        resistance = current_resistance_zone(price, candles_1h, candles_4h)
        support = nearest_support(price, candles_1h, candles_4h)

        if resistance is None or support is None:
            return None

        near_resistance = abs(price - resistance) / price * 100 <= 0.45

        rejection_candle = (
            last_15["close"] < last_15["open"]
            and last_15["high"] >= resistance * 0.996
            and last_15["close"] < resistance
            and last_15["close"] < prev_15["close"]
        )

        five_min_rejection = (
            last_5["close"] < last_5["open"]
            and last_5["close"] < ema20_5
            and last_5["close"] < prev_5["close"]
        )

        tp1 = support
        tp2 = price * (1 - (35 / LEVERAGE) / 100)
        tp3 = price * (1 - (60 / LEVERAGE) / 100)

        if tp2 >= tp1:
            tp2 = tp1 * 0.995

        if tp3 >= tp2:
            tp3 = tp2 * 0.992

        sl = resistance + atr * 0.25

        tp1_profit = position_profit_percent(price, tp1, "SHORT")

        checks = [
            ("цена у 1h/4h сопротивления", near_resistance),
            ("15m свеча rejection от сопротивления", rejection_candle),
            ("5m подтверждает отбой вниз", five_min_rejection),
            ("BTC не против SHORT", btc_status not in ["BULLISH", "STRONG_BULLISH"]),
            ("4h не сильный bullish", trend_4h != "STRONG_BULLISH"),
            ("1h не сильный bullish", trend_1h != "STRONG_BULLISH"),
            ("RSI показывает зону отбоя", 45 <= rsi <= 72),
            ("MACD ухудшается", macd["hist"] <= macd["prev_hist"]),
            ("цена не ниже нижней Bollinger", price > bb["lower"]),
            ("цена рядом/ниже VWAP", price <= vwap * 1.005),
            ("объём подтверждает отбой", volume_ratio >= 1.15),
            ("ADX не запрещает вход", adx["adx"] >= 14),
            ("до поддержки есть +20%+", tp1_profit >= MIN_POSITION_PROFIT_PERCENT),
        ]

        entry_zone_low = price - atr * 0.10
        entry_zone_high = min(resistance, price + atr * 0.20)

        return build_common_signal(
            symbol, "SHORT", "Resistance Rejection SHORT",
            price, entry_zone_low, entry_zone_high,
            sl, tp1, tp2, tp3,
            rsi, volume_ratio, btc_status, trend_1h, trend_4h, checks
        )


def analyze_symbol(symbol, candles_15m, candles_5m, candles_1h, candles_4h, btc_status):
    if is_on_cooldown(symbol):
        return None

    candidates = []

    for side in ["LONG", "SHORT"]:
        pullback_signal = build_pullback_signal(
            symbol,
            side,
            candles_15m,
            candles_5m,
            candles_1h,
            candles_4h,
            btc_status,
        )

        if pullback_signal:
            candidates.append(pullback_signal)

        bounce_signal = build_bounce_signal(
            symbol,
            side,
            candles_15m,
            candles_5m,
            candles_1h,
            candles_4h,
            btc_status,
        )

        if bounce_signal:
            candidates.append(bounce_signal)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            x["confluence_score"],
            x["quality"],
            x["rr"],
            x["tp1_position_profit"],
        ),
        reverse=True,
    )

    return candidates[0]


def make_message(signal):
    arrow = "📈" if signal["side"] == "LONG" else "📉"
    fire = "🔥" if signal["quality"] >= 88 else "🎯"

    reasons_text = "\n".join([f"✅ {r}" for r in signal["reasons"]])

    return f"""
{fire} <b>{signal["symbol"].replace("-", "/")}</b>
{arrow} <b>{signal["side"]}</b>

Тип: <b>✅ Professional Signal v7</b>
Сетап: <b>{signal["setup_type"]}</b>
Качество: <b>{signal["quality"]}%</b>
Проверка условий: <b>{signal["confluence_score"]}/{signal["max_confluence_score"]}</b>

<b>Тренд:</b>
4h: <b>{signal["trend_4h"]}</b>
1h: <b>{signal["trend_1h"]}</b>

Плечо: <b>{LEVERAGE}x</b>

🎯 Зона входа:
<code>{format_price(signal["entry_zone_low"])}</code> – <code>{format_price(signal["entry_zone_high"])}</code>

Текущая расчетная цена: <code>{format_price(signal["entry"])}</code>
🛑 SL: <code>{format_price(signal["sl"])}</code>

✅ TP1: <code>{format_price(signal["tp1"])}</code> ≈ <b>+{signal["tp1_position_profit"]:.1f}%</b>
✅ TP2: <code>{format_price(signal["tp2"])}</code> ≈ <b>+{signal["tp2_position_profit"]:.1f}%</b>
✅ TP3: <code>{format_price(signal["tp3"])}</code> ≈ <b>+{signal["tp3_position_profit"]:.1f}%</b>

🛡 Риск до SL: <b>{signal["risk_price_percent"]:.2f}% цены</b> / около <b>{signal["risk_position_percent"]:.1f}%</b> по позиции
📊 RR до TP1: <b>{signal["rr"]:.2f}</b>

₿ BTC: <b>{signal["btc_status"]}</b>
📊 RSI: <b>{signal["rsi"]:.1f}</b>
📊 Объём: <b>x{signal["volume_ratio"]:.2f}</b>

<b>Что подтвердилось:</b>
{reasons_text}

<b>План ведения:</b>
• TP1: закрыть часть позиции
• После TP1: перенести SL в безубыток
• TP2/TP3: держать остаток по ситуации
• Если цена ушла далеко от зоны входа — сделку лучше пропустить

⚠️ 20x — высокий риск. Даже профессиональный фильтр не гарантирует прибыль.
⚠️ Риск-менеджмент: не более 0.5% от депозита на сделку.
⚠️ Не финансовый совет.

🕒 {now_text()}
""".strip()


def bingx_url(symbol):
    pair = symbol.replace("-", "")
    return f"https://bingx.com/en/futures/forward/{pair}"


async def scan_loop():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")

    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID не задан")

    async with aiohttp.ClientSession() as session:
        await send_telegram_message(
            session,
            f"✅ BingX Signal Scanner v7 Professional Bounce + Pullback Mode запущен.\n"
            f"Логика: 4h + 1h + 15m + 5m + EMA + RSI + MACD + Bollinger + VWAP + ATR + ADX + объём + уровни.\n"
            f"Ищет: Pullback по тренду + Bounce от поддержки + Rejection от сопротивления.\n"
            f"Плечо: {LEVERAGE}x\n"
            f"TP1 минимум: {MIN_POSITION_PROFIT_PERCENT:.0f}%+ по позиции\n"
            f"Риск максимум: {MAX_RISK_POSITION_PERCENT:.1f}% по позиции\n"
            f"RR минимум: {MIN_RR}\n"
            f"Минимум подтверждений: {MIN_CONFLUENCE_SCORE}\n"
            f"Ежечасные обновления отключены."
        )

        while True:
            reset_daily_stats_if_needed()

            try:
                print("Scanning...", now_text())

                btc_1h = await get_klines(session, BTC_SYMBOL, interval=TREND_TIMEFRAME, limit=220)

                if not btc_1h:
                    print("BTC data unavailable")
                    await asyncio.sleep(60)
                    continue

                btc_status, btc_change = btc_market_filter(btc_1h)

                print(f"BTC status: {btc_status}, change: {btc_change:.2f}%")

                symbols = await get_symbols(session)

                checked = 0
                found = 0

                for symbol in symbols:
                    reset_daily_stats_if_needed()

                    if symbol == BTC_SYMBOL:
                        continue

                    if STATS["signals_today"] >= DAILY_MAX_SIGNALS:
                        break

                    checked += 1

                    try:
                        candles_15m = await get_klines(session, symbol, interval=ENTRY_TIMEFRAME, limit=220)
                        candles_5m = await get_klines(session, symbol, interval=CONFIRM_TIMEFRAME, limit=220)
                        candles_1h = await get_klines(session, symbol, interval=TREND_TIMEFRAME, limit=220)
                        candles_4h = await get_klines(session, symbol, interval=MACRO_TIMEFRAME, limit=220)

                        if (
                            candles_15m is None
                            or candles_5m is None
                            or candles_1h is None
                            or candles_4h is None
                        ):
                            continue

                        signal = analyze_symbol(
                            symbol,
                            candles_15m,
                            candles_5m,
                            candles_1h,
                            candles_4h,
                            btc_status,
                        )

                        if not signal:
                            continue

                        await send_telegram_message(
                            session,
                            make_message(signal),
                            button_url=bingx_url(symbol),
                        )

                        SENT_SIGNALS[signal["signal_id"]] = time.time()
                        set_cooldown(symbol)

                        found += 1
                        STATS["signals_total"] += 1
                        STATS["signals_today"] += 1

                        await asyncio.sleep(2)

                        if found >= MAX_SIGNALS_PER_SCAN:
                            break

                    except Exception as e:
                        print(f"Error with {symbol}:", e)

                print(
                    f"Scan finished. Checked: {checked}. "
                    f"Signals found: {found}. Today: {STATS['signals_today']}"
                )

                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

            except Exception as e:
                print("Main loop error:", e)
                await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(scan_loop())
