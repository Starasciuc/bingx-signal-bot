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
TREND_TIMEFRAME = os.getenv("TREND_TIMEFRAME", "1h")
MACRO_TIMEFRAME = os.getenv("MACRO_TIMEFRAME", "4h")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "120"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "300"))

LEVERAGE = int(os.getenv("LEVERAGE", "20"))

# Цель: минимум +20% по позиции
MIN_POSITION_PROFIT_PERCENT = float(os.getenv("MIN_POSITION_PROFIT_PERCENT", "20"))

# Профессиональные фильтры риска
MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "6"))
MIN_RR = float(os.getenv("MIN_RR", "1.8"))

# Ограничения
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "14400"))  # 4 часа
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "2"))
DAILY_MAX_SIGNALS = int(os.getenv("DAILY_MAX_SIGNALS", "5"))

BTC_SYMBOL = "BTC-USDT"

SENT_SIGNALS = {}
SYMBOL_COOLDOWN = {}

STATS = {
    "signals_total": 0,
    "signals_today": 0,
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


async def get_klines(session, symbol, interval, limit=200):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    async with session.get(url, params=params, timeout=30) as resp:
        data = await resp.json()

    raw = data.get("data", [])

    if not raw or len(raw) < 80:
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


def nearest_resistance(entry, candles_1h, candles_4h):
    highs = []

    for c in candles_1h[-80:]:
        highs.append(c["high"])

    for c in candles_4h[-50:]:
        highs.append(c["high"])

    candidates = sorted(set([h for h in highs if h > entry * 1.002]))

    if not candidates:
        return None

    return candidates[0]


def nearest_support(entry, candles_1h, candles_4h):
    lows = []

    for c in candles_1h[-80:]:
        lows.append(c["low"])

    for c in candles_4h[-50:]:
        lows.append(c["low"])

    candidates = sorted(set([l for l in lows if l < entry * 0.998]), reverse=True)

    if not candidates:
        return None

    return candidates[0]


def detect_large_danger_candle(candles, atr):
    last = candles[-1]
    body = abs(last["close"] - last["open"])
    full_range = last["high"] - last["low"]

    if atr <= 0:
        return True

    if full_range > atr * 2.5:
        return True

    if body > atr * 1.8:
        return True

    return False


def build_long_signal(symbol, candles_15m, candles_1h, candles_4h, btc_status):
    closes = [c["close"] for c in candles_15m]
    lows = [c["low"] for c in candles_15m]
    volumes = [c["volume"] for c in candles_15m]

    ema20_list = ema(closes, 20)
    ema50_list = ema(closes, 50)
    ema100_list = ema(closes, 100)

    last = candles_15m[-1]
    prev = candles_15m[-2]

    price = last["close"]
    ema20 = ema20_list[-1]
    ema50 = ema50_list[-1]
    ema100 = ema100_list[-1]

    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(candles_15m, 14)

    if rsi is None or atr is None:
        return None

    if detect_large_danger_candle(candles_15m, atr):
        return None

    trend_1h = trend_filter(candles_1h)
    trend_4h = trend_filter(candles_4h)

    if trend_4h not in ["BULLISH", "STRONG_BULLISH"]:
        return None

    if trend_1h not in ["BULLISH", "STRONG_BULLISH"]:
        return None

    if btc_status in ["BEARISH", "STRONG_BEARISH"]:
        return None

    strong_15m = price > ema20 > ema50 > ema100

    if not strong_15m:
        return None

    avg_volume = sum(volumes[-30:]) / 30
    volume_ratio = last["volume"] / avg_volume if avg_volume > 0 else 0

    if volume_ratio < 1.15:
        return None

    # Откат к EMA20/EMA50
    touched_ema_zone = (
        prev["low"] <= ema20 * 1.004
        or prev["low"] <= ema50 * 1.004
        or last["low"] <= ema20 * 1.004
        or last["low"] <= ema50 * 1.004
    )

    if not touched_ema_zone:
        return None

    # Подтверждение отскока
    bullish_reaction = (
        last["close"] > last["open"]
        and last["close"] > ema20
        and last["close"] > prev["close"]
    )

    if not bullish_reaction:
        return None

    if not (38 <= rsi <= 58):
        return None

    entry = price

    resistance = nearest_resistance(entry, candles_1h, candles_4h)

    if resistance is None:
        return None

    tp1_profit = position_profit_percent(entry, resistance, "LONG")

    # До первого сопротивления должно быть минимум +20% по позиции
    if tp1_profit < MIN_POSITION_PROFIT_PERCENT:
        return None

    recent_local_low = min(lows[-12:])
    natural_sl = recent_local_low - atr * 0.20

    if natural_sl >= entry:
        return None

    risk_price_percent = abs(entry - natural_sl) / entry * 100
    risk_position_percent = risk_price_percent * LEVERAGE

    if risk_position_percent > MAX_RISK_POSITION_PERCENT:
        return None

    reward_price_percent = price_move_percent(entry, resistance, "LONG")
    rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

    if rr < MIN_RR:
        return None

    tp1 = resistance
    tp2 = entry * (1 + (30 / LEVERAGE) / 100)
    tp3 = entry * (1 + (50 / LEVERAGE) / 100)

    if tp2 <= tp1:
        tp2 = tp1 * 1.004

    if tp3 <= tp2:
        tp3 = tp2 * 1.006

    entry_zone_low = entry - atr * 0.15
    entry_zone_high = entry + atr * 0.10

    quality = 72
    reasons = [
        "4h bullish trend",
        "1h bullish trend",
        "15m откат к EMA20/EMA50",
        "свеча подтверждения отскока",
        f"RSI в рабочей зоне: {rsi:.1f}",
        f"объём x{volume_ratio:.2f}",
        "до первого сопротивления есть +20%+ по позиции",
    ]

    if trend_4h == "STRONG_BULLISH":
        quality += 7
        reasons.append("4h strong bullish")

    if trend_1h == "STRONG_BULLISH":
        quality += 7
        reasons.append("1h strong bullish")

    if btc_status in ["BULLISH", "STRONG_BULLISH"]:
        quality += 6
        reasons.append("BTC поддерживает LONG")

    if volume_ratio >= 1.6:
        quality += 6
        reasons.append("сильный всплеск объёма")

    signal_id = f"{symbol}:PRO_PULLBACK:LONG:{round(entry, 6)}"

    if signal_id in SENT_SIGNALS:
        return None

    return {
        "symbol": symbol,
        "side": "LONG",
        "setup_type": "Professional Pullback LONG",
        "quality": min(99, quality),
        "entry": entry,
        "entry_zone_low": entry_zone_low,
        "entry_zone_high": entry_zone_high,
        "sl": natural_sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "tp1_position_profit": tp1_profit,
        "tp2_position_profit": position_profit_percent(entry, tp2, "LONG"),
        "tp3_position_profit": position_profit_percent(entry, tp3, "LONG"),
        "risk_price_percent": risk_price_percent,
        "risk_position_percent": risk_position_percent,
        "rr": rr,
        "rsi": rsi,
        "volume_ratio": volume_ratio,
        "btc_status": btc_status,
        "trend_1h": trend_1h,
        "trend_4h": trend_4h,
        "level_name": "первое сопротивление",
        "reasons": reasons,
        "signal_id": signal_id,
    }


def build_short_signal(symbol, candles_15m, candles_1h, candles_4h, btc_status):
    closes = [c["close"] for c in candles_15m]
    highs = [c["high"] for c in candles_15m]
    volumes = [c["volume"] for c in candles_15m]

    ema20_list = ema(closes, 20)
    ema50_list = ema(closes, 50)
    ema100_list = ema(closes, 100)

    last = candles_15m[-1]
    prev = candles_15m[-2]

    price = last["close"]
    ema20 = ema20_list[-1]
    ema50 = ema50_list[-1]
    ema100 = ema100_list[-1]

    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(candles_15m, 14)

    if rsi is None or atr is None:
        return None

    if detect_large_danger_candle(candles_15m, atr):
        return None

    trend_1h = trend_filter(candles_1h)
    trend_4h = trend_filter(candles_4h)

    if trend_4h not in ["BEARISH", "STRONG_BEARISH"]:
        return None

    if trend_1h not in ["BEARISH", "STRONG_BEARISH"]:
        return None

    if btc_status in ["BULLISH", "STRONG_BULLISH"]:
        return None

    strong_15m = price < ema20 < ema50 < ema100

    if not strong_15m:
        return None

    avg_volume = sum(volumes[-30:]) / 30
    volume_ratio = last["volume"] / avg_volume if avg_volume > 0 else 0

    if volume_ratio < 1.15:
        return None

    # Откат к EMA20/EMA50 сверху
    touched_ema_zone = (
        prev["high"] >= ema20 * 0.996
        or prev["high"] >= ema50 * 0.996
        or last["high"] >= ema20 * 0.996
        or last["high"] >= ema50 * 0.996
    )

    if not touched_ema_zone:
        return None

    # Подтверждение rejection вниз
    bearish_reaction = (
        last["close"] < last["open"]
        and last["close"] < ema20
        and last["close"] < prev["close"]
    )

    if not bearish_reaction:
        return None

    if not (42 <= rsi <= 62):
        return None

    entry = price

    support = nearest_support(entry, candles_1h, candles_4h)

    if support is None:
        return None

    tp1_profit = position_profit_percent(entry, support, "SHORT")

    # До первой поддержки должно быть минимум +20% по позиции
    if tp1_profit < MIN_POSITION_PROFIT_PERCENT:
        return None

    recent_local_high = max(highs[-12:])
    natural_sl = recent_local_high + atr * 0.20

    if natural_sl <= entry:
        return None

    risk_price_percent = abs(natural_sl - entry) / entry * 100
    risk_position_percent = risk_price_percent * LEVERAGE

    if risk_position_percent > MAX_RISK_POSITION_PERCENT:
        return None

    reward_price_percent = price_move_percent(entry, support, "SHORT")
    rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

    if rr < MIN_RR:
        return None

    tp1 = support
    tp2 = entry * (1 - (30 / LEVERAGE) / 100)
    tp3 = entry * (1 - (50 / LEVERAGE) / 100)

    if tp2 >= tp1:
        tp2 = tp1 * 0.996

    if tp3 >= tp2:
        tp3 = tp2 * 0.994

    entry_zone_low = entry - atr * 0.10
    entry_zone_high = entry + atr * 0.15

    quality = 72
    reasons = [
        "4h bearish trend",
        "1h bearish trend",
        "15m откат к EMA20/EMA50",
        "свеча подтверждения rejection",
        f"RSI в рабочей зоне: {rsi:.1f}",
        f"объём x{volume_ratio:.2f}",
        "до первой поддержки есть +20%+ по позиции",
    ]

    if trend_4h == "STRONG_BEARISH":
        quality += 7
        reasons.append("4h strong bearish")

    if trend_1h == "STRONG_BEARISH":
        quality += 7
        reasons.append("1h strong bearish")

    if btc_status in ["BEARISH", "STRONG_BEARISH"]:
        quality += 6
        reasons.append("BTC поддерживает SHORT")

    if volume_ratio >= 1.6:
        quality += 6
        reasons.append("сильный всплеск объёма")

    signal_id = f"{symbol}:PRO_PULLBACK:SHORT:{round(entry, 6)}"

    if signal_id in SENT_SIGNALS:
        return None

    return {
        "symbol": symbol,
        "side": "SHORT",
        "setup_type": "Professional Pullback SHORT",
        "quality": min(99, quality),
        "entry": entry,
        "entry_zone_low": entry_zone_low,
        "entry_zone_high": entry_zone_high,
        "sl": natural_sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "tp1_position_profit": tp1_profit,
        "tp2_position_profit": position_profit_percent(entry, tp2, "SHORT"),
        "tp3_position_profit": position_profit_percent(entry, tp3, "SHORT"),
        "risk_price_percent": risk_price_percent,
        "risk_position_percent": risk_position_percent,
        "rr": rr,
        "rsi": rsi,
        "volume_ratio": volume_ratio,
        "btc_status": btc_status,
        "trend_1h": trend_1h,
        "trend_4h": trend_4h,
        "level_name": "первая поддержка",
        "reasons": reasons,
        "signal_id": signal_id,
    }


def analyze_symbol(symbol, candles_15m, candles_1h, candles_4h, btc_status):
    if is_on_cooldown(symbol):
        return None

    candidates = []

    long_signal = build_long_signal(symbol, candles_15m, candles_1h, candles_4h, btc_status)

    if long_signal:
        candidates.append(long_signal)

    short_signal = build_short_signal(symbol, candles_15m, candles_1h, candles_4h, btc_status)

    if short_signal:
        candidates.append(short_signal)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            x["quality"],
            x["rr"],
            x["tp1_position_profit"],
            x["volume_ratio"],
        ),
        reverse=True
    )

    return candidates[0]


def make_message(signal):
    arrow = "📈" if signal["side"] == "LONG" else "📉"
    fire = "🔥" if signal["quality"] >= 85 else "🎯"
    reasons_text = "\n".join([f"• {r}" for r in signal["reasons"]])

    return f"""
{fire} <b>{signal["symbol"].replace("-", "/")}</b>
{arrow} <b>{signal["side"]}</b>

Тип: <b>✅ Professional Pullback</b>
Сетап: <b>{signal["setup_type"]}</b>
Качество: <b>{signal["quality"]}%</b>

<b>Тренд:</b>
4h: <b>{signal["trend_4h"]}</b>
1h: <b>{signal["trend_1h"]}</b>
15m: <b>откат по тренду</b>

Плечо: <b>{LEVERAGE}x</b>

🎯 Зона входа:
<code>{format_price(signal["entry_zone_low"])}</code> – <code>{format_price(signal["entry_zone_high"])}</code>

Текущая расчетная цена: <code>{format_price(signal["entry"])}</code>
🛑 SL: <code>{format_price(signal["sl"])}</code>

✅ TP1 ({signal["level_name"]}): <code>{format_price(signal["tp1"])}</code> ≈ <b>+{signal["tp1_position_profit"]:.1f}%</b>
✅ TP2: <code>{format_price(signal["tp2"])}</code> ≈ <b>+{signal["tp2_position_profit"]:.1f}%</b>
✅ TP3: <code>{format_price(signal["tp3"])}</code> ≈ <b>+{signal["tp3_position_profit"]:.1f}%</b>

🛡 Риск до SL: <b>{signal["risk_price_percent"]:.2f}% цены</b> / около <b>{signal["risk_position_percent"]:.1f}%</b> по позиции
📊 RR до TP1: <b>{signal["rr"]:.2f}</b>

₿ BTC: <b>{signal["btc_status"]}</b>
📊 RSI: <b>{signal["rsi"]:.1f}</b>
📊 Объём: <b>x{signal["volume_ratio"]:.2f}</b>

<b>Почему сигнал:</b>
{reasons_text}

<b>План ведения:</b>
• TP1: можно закрыть часть позиции
• После TP1: разумно перенести SL в безубыток
• TP2/TP3: держать остаток по ситуации

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
            f"✅ BingX Signal Scanner v5 Professional Pullback Mode запущен.\n"
            f"Логика: 4h + 1h тренд, 15m откат, поддержка/сопротивление.\n"
            f"Плечо: {LEVERAGE}x\n"
            f"TP1 минимум: {MIN_POSITION_PROFIT_PERCENT:.0f}%+ по позиции\n"
            f"Риск максимум: {MAX_RISK_POSITION_PERCENT:.1f}% по позиции\n"
            f"RR минимум: {MIN_RR}\n"
            f"Ежечасные обновления отключены."
        )

        while True:
            reset_daily_stats_if_needed()

            try:
                print("Scanning...", now_text())

                btc_1h = await get_klines(session, BTC_SYMBOL, interval=TREND_TIMEFRAME, limit=200)

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
                        candles_15m = await get_klines(session, symbol, interval=ENTRY_TIMEFRAME, limit=200)
                        candles_1h = await get_klines(session, symbol, interval=TREND_TIMEFRAME, limit=200)
                        candles_4h = await get_klines(session, symbol, interval=MACRO_TIMEFRAME, limit=200)

                        if candles_15m is None or candles_1h is None or candles_4h is None:
                            continue

                        signal = analyze_symbol(
                            symbol,
                            candles_15m,
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
