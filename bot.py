import os
import re
import time
import random
import asyncio
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv


load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BINGX_BASE_URL = "https://open-api.bingx.com"

TIMEFRAME = os.getenv("TIMEFRAME", "15m")
CONFIRM_TIMEFRAME = os.getenv("CONFIRM_TIMEFRAME", "5m")
TREND_TIMEFRAME = os.getenv("TREND_TIMEFRAME", "1h")
MACRO_TIMEFRAME = os.getenv("MACRO_TIMEFRAME", "4h")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "90"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "300"))

LEVERAGE = int(os.getenv("LEVERAGE", "10"))

MIN_POSITION_PROFIT_PERCENT = float(os.getenv("MIN_POSITION_PROFIT_PERCENT", "20"))
MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "18"))
MIN_RR = float(os.getenv("MIN_RR", "1.25"))
MIN_QUALITY = int(os.getenv("MIN_QUALITY", "88"))

MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.50"))
MIN_MOMENTUM_ATR = float(os.getenv("MIN_MOMENTUM_ATR", "0.25"))
MAX_LATE_ENTRY_ATR = float(os.getenv("MAX_LATE_ENTRY_ATR", "0.95"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "10800"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "2"))
DAILY_MAX_SIGNALS = int(os.getenv("DAILY_MAX_SIGNALS", "5"))

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


def format_price(x):
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.8f}"


def is_on_cooldown(symbol):
    last_time = SYMBOL_COOLDOWN.get(symbol)
    if not last_time:
        return False
    return time.time() - last_time < SIGNAL_COOLDOWN_SECONDS


def set_cooldown(symbol):
    SYMBOL_COOLDOWN[symbol] = time.time()


def is_normal_crypto_symbol(symbol):
    if not symbol.endswith("-USDT"):
        return False

    base = symbol.replace("-USDT", "").upper()

    bad_words = [
        "NCS", "QCOM", "AAPL", "TSLA", "MSFT", "AMZN", "GOOG", "META",
        "NVDA", "NFLX", "INTC", "COIN", "NASDAQ", "SPX", "DOW",
        "USOIL", "UKOIL", "BRENT", "WTI", "GOLD", "SILVER", "XAU",
        "XAG", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF"
    ]

    if any(x in base for x in bad_words):
        return False

    if "USD" in base:
        return False

    if len(base) > 14:
        return False

    if not re.match(r"^[A-Z0-9]+$", base):
        return False

    return True


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
        if symbol and is_normal_crypto_symbol(symbol):
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

    price = closes[-1]
    ema20 = ema20_list[-1]
    ema50 = ema50_list[-1]

    prev = closes[-4] if len(closes) >= 4 else closes[-2]
    change = ((price - prev) / prev) * 100

    if price > ema20 > ema50 and change > 0.10:
        return "BULLISH", change

    if price < ema20 < ema50 and change < -0.10:
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


def get_resistance_near_price(price, candles_1h, candles_4h):
    highs = []

    for c in candles_1h[-120:]:
        highs.append(c["high"])

    for c in candles_4h[-80:]:
        highs.append(c["high"])

    near = []

    for level in highs:
        distance = abs(price - level) / price * 100
        if distance <= 0.80 and level >= price * 0.996:
            near.append(level)

    if not near:
        return None

    return min(near, key=lambda x: abs(price - x))


def get_support_near_price(price, candles_1h, candles_4h):
    lows = []

    for c in candles_1h[-120:]:
        lows.append(c["low"])

    for c in candles_4h[-80:]:
        lows.append(c["low"])

    near = []

    for level in lows:
        distance = abs(price - level) / price * 100
        if distance <= 0.80 and level <= price * 1.004:
            near.append(level)

    if not near:
        return None

    return min(near, key=lambda x: abs(price - x))


def next_support_below(entry, candles_1h, candles_4h):
    lows = []

    for c in candles_1h[-160:]:
        lows.append(c["low"])

    for c in candles_4h[-100:]:
        lows.append(c["low"])

    candidates = sorted(set([l for l in lows if l < entry * 0.995]), reverse=True)

    if not candidates:
        return None

    return candidates[0]


def next_resistance_above(entry, candles_1h, candles_4h):
    highs = []

    for c in candles_1h[-160:]:
        highs.append(c["high"])

    for c in candles_4h[-100:]:
        highs.append(c["high"])

    candidates = sorted(set([h for h in highs if h > entry * 1.005]))

    if not candidates:
        return None

    return candidates[0]


def candle_body(candle):
    return abs(candle["close"] - candle["open"])


def candle_range(candle):
    return candle["high"] - candle["low"]


def detect_large_danger_candle(candles, atr):
    last = candles[-1]

    body = candle_body(last)
    full_range = candle_range(last)

    if atr <= 0:
        return True

    if full_range > atr * 3.0:
        return True

    if body > atr * 2.4:
        return True

    return False


def momentum_is_strong(candles_5m, direction):
    last = candles_5m[-1]
    prev = candles_5m[-2]
    prev2 = candles_5m[-3]

    last_body = candle_body(last)
    prev_body = candle_body(prev)
    prev2_body = candle_body(prev2)

    if direction == "SHORT":
        return (
            last["close"] < last["open"]
            and prev["close"] <= prev["open"]
            and last["close"] < prev["close"]
            and prev["close"] <= prev2["close"]
            and last_body > prev_body * 0.8
            and last_body > prev2_body * 0.6
        )

    return (
        last["close"] > last["open"]
        and prev["close"] >= prev["open"]
        and last["close"] > prev["close"]
        and prev["close"] >= prev2["close"]
        and last_body > prev_body * 0.8
        and last_body > prev2_body * 0.6
    )


def price_already_moved_from_level(price, level, atr, direction):
    if atr <= 0:
        return False

    distance = abs(price - level)

    if distance < atr * MIN_MOMENTUM_ATR:
        return False

    if distance > atr * MAX_LATE_ENTRY_ATR:
        return False

    if direction == "SHORT":
        return price < level

    return price > level


def build_reversal_signal(symbol, side, candles_15m, candles_5m, candles_1h, candles_4h, btc_status):
    closes_15 = [c["close"] for c in candles_15m]
    highs_15 = [c["high"] for c in candles_15m]
    lows_15 = [c["low"] for c in candles_15m]
    volumes_15 = [c["volume"] for c in candles_15m]

    last_15 = candles_15m[-1]
    prev_15 = candles_15m[-2]

    price = last_15["close"]

    rsi = calculate_rsi(closes_15, 14)
    atr = calculate_atr(candles_15m, 14)
    macd = calculate_macd(closes_15)
    bb = calculate_bollinger(closes_15, 20, 2)
    vwap = calculate_vwap_like(candles_15m, 48)

    if rsi is None or atr is None or macd is None or bb is None or vwap is None:
        return None

    if detect_large_danger_candle(candles_15m, atr):
        return None

    trend_1h = trend_filter(candles_1h)
    trend_4h = trend_filter(candles_4h)

    avg_volume = sum(volumes_15[-30:]) / 30
    volume_ratio = last_15["volume"] / avg_volume if avg_volume > 0 else 0

    if volume_ratio < MIN_VOLUME_RATIO:
        return None

    if side == "SHORT":
        resistance = get_resistance_near_price(price, candles_1h, candles_4h)

        if resistance is None:
            return None

        if not price_already_moved_from_level(price, resistance, atr, "SHORT"):
            return None

        if not momentum_is_strong(candles_5m, "SHORT"):
            return None

        target = next_support_below(price, candles_1h, candles_4h)

        if target is None:
            return None

        tp1_profit = position_profit_percent(price, target, "SHORT")

        if tp1_profit < MIN_POSITION_PROFIT_PERCENT:
            return None

        recent_high = max(highs_15[-20:])

        rejection_candle = (
            last_15["high"] >= resistance * 0.996
            and last_15["close"] < last_15["open"]
            and last_15["close"] < prev_15["close"]
            and candle_body(last_15) >= atr * 0.30
        )

        rsi_ok = 58 <= rsi <= 82
        macd_turn = macd["hist"] < macd["prev_hist"] and macd["hist"] < 0
        bollinger_ok = price >= bb["middle"] and price <= bb["upper"] * 1.025
        vwap_ok = price >= vwap * 0.985

        sl = max(resistance + atr * 0.20, recent_high + atr * 0.10)

        if sl <= price:
            return None

        risk_price_percent = abs(sl - price) / price * 100
        risk_position_percent = risk_price_percent * LEVERAGE

        if risk_position_percent > MAX_RISK_POSITION_PERCENT:
            return None

        reward_price_percent = price_move_percent(price, target, "SHORT")
        rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

        if rr < MIN_RR:
            return None

        checks = [
            ("цена отбилась от сопротивления", price < resistance),
            ("15m rejection-свеча", rejection_candle),
            ("5m уже дал импульс вниз", True),
            ("цена уже начала движение вниз", True),
            ("RSI высокий", rsi_ok),
            ("MACD уже вниз", macd_turn),
            ("объём сильный", volume_ratio >= MIN_VOLUME_RATIO),
            ("Bollinger зона подходит", bollinger_ok),
            ("VWAP зона подходит", vwap_ok),
            ("BTC не против SHORT", btc_status != "BULLISH"),
            ("4h не сильный bullish", trend_4h != "STRONG_BULLISH"),
            ("1h не сильный bullish", trend_1h != "STRONG_BULLISH"),
            ("TP1 даёт нужный потенциал", tp1_profit >= MIN_POSITION_PROFIT_PERCENT),
            ("RR хороший", rr >= MIN_RR),
        ]

        tp1 = target
        tp2 = price * (1 - (35 / LEVERAGE) / 100)

        if tp2 >= tp1:
            tp2 = tp1 * 0.995

        entry = price

    else:
        support = get_support_near_price(price, candles_1h, candles_4h)

        if support is None:
            return None

        if not price_already_moved_from_level(price, support, atr, "LONG"):
            return None

        if not momentum_is_strong(candles_5m, "LONG"):
            return None

        target = next_resistance_above(price, candles_1h, candles_4h)

        if target is None:
            return None

        tp1_profit = position_profit_percent(price, target, "LONG")

        if tp1_profit < MIN_POSITION_PROFIT_PERCENT:
            return None

        recent_low = min(lows_15[-20:])

        bounce_candle = (
            last_15["low"] <= support * 1.004
            and last_15["close"] > last_15["open"]
            and last_15["close"] > prev_15["close"]
            and candle_body(last_15) >= atr * 0.30
        )

        rsi_ok = 18 <= rsi <= 45
        macd_turn = macd["hist"] > macd["prev_hist"] and macd["hist"] > 0
        bollinger_ok = price <= bb["middle"] and price >= bb["lower"] * 0.975
        vwap_ok = price <= vwap * 1.015

        sl = min(support - atr * 0.20, recent_low - atr * 0.10)

        if sl >= price:
            return None

        risk_price_percent = abs(price - sl) / price * 100
        risk_position_percent = risk_price_percent * LEVERAGE

        if risk_position_percent > MAX_RISK_POSITION_PERCENT:
            return None

        reward_price_percent = price_move_percent(price, target, "LONG")
        rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

        if rr < MIN_RR:
            return None

        checks = [
            ("цена отбилась от поддержки", price > support),
            ("15m bounce-свеча", bounce_candle),
            ("5m уже дал импульс вверх", True),
            ("цена уже начала движение вверх", True),
            ("RSI низкий", rsi_ok),
            ("MACD уже вверх", macd_turn),
            ("объём сильный", volume_ratio >= MIN_VOLUME_RATIO),
            ("Bollinger зона подходит", bollinger_ok),
            ("VWAP зона подходит", vwap_ok),
            ("BTC не против LONG", btc_status != "BEARISH"),
            ("4h не сильный bearish", trend_4h != "STRONG_BEARISH"),
            ("1h не сильный bearish", trend_1h != "STRONG_BEARISH"),
            ("TP1 даёт нужный потенциал", tp1_profit >= MIN_POSITION_PROFIT_PERCENT),
            ("RR хороший", rr >= MIN_RR),
        ]

        tp1 = target
        tp2 = price * (1 + (35 / LEVERAGE) / 100)

        if tp2 <= tp1:
            tp2 = tp1 * 1.005

        entry = price

    passed = [name for name, ok in checks if ok]

    if len(passed) < 11:
        return None

    quality = min(97, 50 + len(passed) * 4)

    if quality < MIN_QUALITY:
        return None

    signal_id = f"{symbol}:FAST_MOMENTUM_REVERSAL:{side}:{round(entry, 6)}"

    if signal_id in SENT_SIGNALS:
        return None

    return {
        "symbol": symbol,
        "side": side,
        "quality": quality,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "rsi": rsi,
        "volume_ratio": volume_ratio,
        "rr": rr,
        "risk_price_percent": risk_price_percent,
        "risk_position_percent": risk_position_percent,
        "signal_id": signal_id,
    }


def analyze_symbol(symbol, candles_15m, candles_5m, candles_1h, candles_4h, btc_status):
    if is_on_cooldown(symbol):
        return None

    candidates = []

    short_signal = build_reversal_signal(
        symbol,
        "SHORT",
        candles_15m,
        candles_5m,
        candles_1h,
        candles_4h,
        btc_status,
    )

    if short_signal:
        candidates.append(short_signal)

    long_signal = build_reversal_signal(
        symbol,
        "LONG",
        candles_15m,
        candles_5m,
        candles_1h,
        candles_4h,
        btc_status,
    )

    if long_signal:
        candidates.append(long_signal)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            x["quality"],
            x["rr"],
            x["volume_ratio"],
        ),
        reverse=True,
    )

    return candidates[0]


def make_message(signal):
    arrow = "📈" if signal["side"] == "LONG" else "📉"

    return f"""
🎯 <b>Reversal-сетап</b>

{arrow} <b>{signal["side"]} {signal["symbol"].replace("-", "/")}</b> · {TIMEFRAME}
Качество: <b>{signal["quality"]}%</b>

🎯 Зона входа: <code>{format_price(signal["entry"])}</code>
🛑 SL: <code>{format_price(signal["sl"])}</code>
✅ TP1: <code>{format_price(signal["tp1"])}</code>
✅ TP2: <code>{format_price(signal["tp2"])}</code>

⚠️Плечо max.: <b>{LEVERAGE}x</b>
⚠️Соблюдаем РМ до 0,5% от общего депозита.

Не финансовый совет. Фьючерсы несут высокий риск, особенно с плечом.
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
            f"✅ Fast Momentum Reversal Bot запущен.\n"
            f"Формат сигналов как в примере.\n"
            f"Фильтр: сильный объём x{MIN_VOLUME_RATIO}+ и движение уже началось.\n"
            f"Плечо max.: {LEVERAGE}x\n"
            f"Таймфрейм: {TIMEFRAME}"
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
                        candles_15m = await get_klines(session, symbol, interval=TIMEFRAME, limit=220)
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
