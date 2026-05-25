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

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "400"))

LEVERAGE = int(os.getenv("LEVERAGE", "10"))

TP1_POSITION_PERCENT = float(os.getenv("TP1_POSITION_PERCENT", "12"))
TP2_POSITION_PERCENT = float(os.getenv("TP2_POSITION_PERCENT", "25"))
TP3_POSITION_PERCENT = float(os.getenv("TP3_POSITION_PERCENT", "45"))

MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "10"))
MIN_RR_TO_TP1 = float(os.getenv("MIN_RR_TO_TP1", "1.0"))

MIN_QUALITY = int(os.getenv("MIN_QUALITY", "80"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.05"))

# Главное: защита от позднего входа
MIN_PREVIOUS_DROP_FOR_LONG = float(os.getenv("MIN_PREVIOUS_DROP_FOR_LONG", "1.0"))
MIN_PREVIOUS_RISE_FOR_SHORT = float(os.getenv("MIN_PREVIOUS_RISE_FOR_SHORT", "1.0"))
MAX_BOUNCE_FROM_LEVEL_ATR = float(os.getenv("MAX_BOUNCE_FROM_LEVEL_ATR", "1.25"))

LEVEL_DISTANCE_PERCENT = float(os.getenv("LEVEL_DISTANCE_PERCENT", "1.25"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "7200"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "3"))
DAILY_MAX_SIGNALS = int(os.getenv("DAILY_MAX_SIGNALS", "8"))

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


async def get_klines(session, symbol, interval, limit=260):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    async with session.get(url, params=params, timeout=30) as resp:
        data = await resp.json()

    raw = data.get("data", [])

    if not raw or len(raw) < 220:
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


def candle_body(candle):
    return abs(candle["close"] - candle["open"])


def upper_wick(candle):
    return candle["high"] - max(candle["open"], candle["close"])


def lower_wick(candle):
    return min(candle["open"], candle["close"]) - candle["low"]


def recent_move_percent(candles, lookback=12):
    if len(candles) < lookback + 1:
        return 0

    old_price = candles[-lookback]["close"]
    new_price = candles[-1]["close"]

    if old_price == 0:
        return 0

    return ((new_price - old_price) / old_price) * 100


def trend_ema_50_200(candles):
    closes = [c["close"] for c in candles]

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


def btc_market_filter(btc_1h):
    closes = [c["close"] for c in btc_1h]

    ema50 = ema(closes, 50)[-1]
    ema200 = ema(closes, 200)[-1]
    price = closes[-1]

    prev = closes[-4] if len(closes) >= 4 else closes[-2]
    change = ((price - prev) / prev) * 100

    if price > ema50 > ema200 and change > 0.10:
        return "BULLISH", change

    if price < ema50 < ema200 and change < -0.10:
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


def make_tp_by_percent(entry, side, position_percent):
    price_move_needed = position_percent / LEVERAGE

    if side == "LONG":
        return entry * (1 + price_move_needed / 100)

    return entry * (1 - price_move_needed / 100)


def collect_levels(candles_1h, candles_4h):
    highs = []
    lows = []

    for c in candles_1h[-160:]:
        highs.append(c["high"])
        lows.append(c["low"])

    for c in candles_4h[-100:]:
        highs.append(c["high"])
        lows.append(c["low"])

    return highs, lows


def nearest_resistance(price, candles_1h, candles_4h):
    highs, _ = collect_levels(candles_1h, candles_4h)

    near = []
    for level in highs:
        distance = abs(price - level) / price * 100
        if distance <= LEVEL_DISTANCE_PERCENT and level >= price * 0.990:
            near.append(level)

    if not near:
        return None

    return min(near, key=lambda x: abs(price - x))


def nearest_support(price, candles_1h, candles_4h):
    _, lows = collect_levels(candles_1h, candles_4h)

    near = []
    for level in lows:
        distance = abs(price - level) / price * 100
        if distance <= LEVEL_DISTANCE_PERCENT and level <= price * 1.010:
            near.append(level)

    if not near:
        return None

    return min(near, key=lambda x: abs(price - x))


def target_resistance_above(price, candles_1h, candles_4h):
    highs, _ = collect_levels(candles_1h, candles_4h)
    candidates = sorted(set([h for h in highs if h > price * 1.004]))

    if not candidates:
        return None

    return candidates[0]


def target_support_below(price, candles_1h, candles_4h):
    _, lows = collect_levels(candles_1h, candles_4h)
    candidates = sorted(set([l for l in lows if l < price * 0.996]), reverse=True)

    if not candidates:
        return None

    return candidates[0]


def momentum_confirm(candles_5m, side):
    last = candles_5m[-1]
    prev = candles_5m[-2]

    if side == "LONG":
        return (
            last["close"] > last["open"]
            and last["close"] > prev["close"]
        )

    return (
        last["close"] < last["open"]
        and last["close"] < prev["close"]
    )


def not_late_from_level(price, level, atr):
    if atr <= 0:
        return False

    distance = abs(price - level)

    return distance <= atr * MAX_BOUNCE_FROM_LEVEL_ATR


def build_reversal_signal(symbol, side, candles_15m, candles_5m, candles_1h, candles_4h, btc_status):
    closes_15 = [c["close"] for c in candles_15m]
    highs_15 = [c["high"] for c in candles_15m]
    lows_15 = [c["low"] for c in candles_15m]
    volumes_15 = [c["volume"] for c in candles_15m]

    last = candles_15m[-1]
    prev = candles_15m[-2]

    price = last["close"]

    rsi_15 = calculate_rsi(closes_15, 14)
    atr_15 = calculate_atr(candles_15m, 14)
    vwap_15 = calculate_vwap_like(candles_15m, 48)

    if rsi_15 is None or atr_15 is None or vwap_15 is None:
        return None

    trend_1h = trend_ema_50_200(candles_1h)
    trend_4h = trend_ema_50_200(candles_4h)

    avg_volume = sum(volumes_15[-30:]) / 30
    volume_ratio = last["volume"] / avg_volume if avg_volume > 0 else 0

    if volume_ratio < MIN_VOLUME_RATIO:
        return None

    recent_move = recent_move_percent(candles_15m, 12)

    checks = []

    if side == "LONG":
        # Главное правило: LONG только после падения, не после роста
        if recent_move > -MIN_PREVIOUS_DROP_FOR_LONG:
            return None

        support = nearest_support(price, candles_1h, candles_4h)

        if support is None:
            return None

        if not not_late_from_level(price, support, atr_15):
            return None

        level_touch = last["low"] <= support * 1.006

        bounce_candle = (
            last["close"] > last["open"]
            and lower_wick(last) >= candle_body(last) * 0.25
            and last["close"] > prev["close"]
        )

        if not level_touch or not bounce_candle:
            return None

        if not momentum_confirm(candles_5m, "LONG"):
            return None

        if btc_status == "BEARISH":
            return None

        if trend_4h == "BEARISH" and trend_1h == "BEARISH":
            return None

        rsi_ok = rsi_15 <= 50
        vwap_ok = price <= vwap_15 * 1.015

        sl = min(support - atr_15 * 0.20, min(lows_15[-10:]) - atr_15 * 0.05)

        if sl >= price:
            return None

        tp1 = make_tp_by_percent(price, "LONG", TP1_POSITION_PERCENT)
        tp2 = make_tp_by_percent(price, "LONG", TP2_POSITION_PERCENT)
        tp3 = make_tp_by_percent(price, "LONG", TP3_POSITION_PERCENT)

        level_target = target_resistance_above(price, candles_1h, candles_4h)

        if level_target is not None:
            if position_profit_percent(price, level_target, "LONG") >= TP2_POSITION_PERCENT:
                tp2 = level_target

        checks = [
            ("цена перед этим падала", recent_move <= -MIN_PREVIOUS_DROP_FOR_LONG),
            ("цена у поддержки", level_touch),
            ("15m bounce-свеча", bounce_candle),
            ("5m подтвердил вверх", True),
            ("RSI низкий/не перегрет", rsi_ok),
            ("цена около/ниже VWAP", vwap_ok),
            ("объём выше среднего", volume_ratio >= MIN_VOLUME_RATIO),
            ("BTC не против LONG", btc_status != "BEARISH"),
            ("EMA50/200 1h не запрещает LONG", trend_1h != "BEARISH"),
            ("EMA50/200 4h не запрещает LONG", trend_4h != "BEARISH"),
        ]

    else:
        # Главное правило: SHORT только после роста, не после падения
        if recent_move < MIN_PREVIOUS_RISE_FOR_SHORT:
            return None

        resistance = nearest_resistance(price, candles_1h, candles_4h)

        if resistance is None:
            return None

        if not not_late_from_level(price, resistance, atr_15):
            return None

        level_touch = last["high"] >= resistance * 0.994

        rejection_candle = (
            last["close"] < last["open"]
            and upper_wick(last) >= candle_body(last) * 0.25
            and last["close"] < prev["close"]
        )

        if not level_touch or not rejection_candle:
            return None

        if not momentum_confirm(candles_5m, "SHORT"):
            return None

        if btc_status == "BULLISH":
            return None

        if trend_4h == "BULLISH" and trend_1h == "BULLISH":
            return None

        rsi_ok = rsi_15 >= 50
        vwap_ok = price >= vwap_15 * 0.985

        sl = max(resistance + atr_15 * 0.20, max(highs_15[-10:]) + atr_15 * 0.05)

        if sl <= price:
            return None

        tp1 = make_tp_by_percent(price, "SHORT", TP1_POSITION_PERCENT)
        tp2 = make_tp_by_percent(price, "SHORT", TP2_POSITION_PERCENT)
        tp3 = make_tp_by_percent(price, "SHORT", TP3_POSITION_PERCENT)

        level_target = target_support_below(price, candles_1h, candles_4h)

        if level_target is not None:
            if position_profit_percent(price, level_target, "SHORT") >= TP2_POSITION_PERCENT:
                tp2 = level_target

        checks = [
            ("цена перед этим росла", recent_move >= MIN_PREVIOUS_RISE_FOR_SHORT),
            ("цена у сопротивления", level_touch),
            ("15m rejection-свеча", rejection_candle),
            ("5m подтвердил вниз", True),
            ("RSI высокий/не низкий", rsi_ok),
            ("цена около/выше VWAP", vwap_ok),
            ("объём выше среднего", volume_ratio >= MIN_VOLUME_RATIO),
            ("BTC не против SHORT", btc_status != "BULLISH"),
            ("EMA50/200 1h не запрещает SHORT", trend_1h != "BULLISH"),
            ("EMA50/200 4h не запрещает SHORT", trend_4h != "BULLISH"),
        ]

    risk_price_percent = abs(price - sl) / price * 100
    risk_position_percent = risk_price_percent * LEVERAGE

    if risk_position_percent > MAX_RISK_POSITION_PERCENT:
        return None

    reward_price_percent = price_move_percent(price, tp1, side)
    rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

    if rr < MIN_RR_TO_TP1:
        return None

    passed = [name for name, ok in checks if ok]

    if len(passed) < 7:
        return None

    quality = min(95, 55 + len(passed) * 5)

    if quality < MIN_QUALITY:
        return None

    signal_id = f"{symbol}:V12_LEVEL_REVERSAL_EMA50_200:{side}:{round(price, 6)}"

    if signal_id in SENT_SIGNALS:
        return None

    return {
        "symbol": symbol,
        "side": side,
        "quality": quality,
        "entry": price,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rsi": rsi_15,
        "volume_ratio": volume_ratio,
        "rr": rr,
        "risk_price_percent": risk_price_percent,
        "risk_position_percent": risk_position_percent,
        "tp1_position_profit": position_profit_percent(price, tp1, side),
        "tp2_position_profit": position_profit_percent(price, tp2, side),
        "tp3_position_profit": position_profit_percent(price, tp3, side),
        "recent_move": recent_move,
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
✅ TP3: <code>{format_price(signal["tp3"])}</code>

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
            f"✅ Level Reversal Bot v12 запущен.\n"
            f"Индикаторы: EMA50/200 + VWAP + RSI + Volume + ATR.\n"
            f"Проверка: 4h + 1h + 15m + 5m.\n"
            f"Главное правило: LONG только после падения, SHORT только после роста.\n"
            f"Плечо max.: {LEVERAGE}x\n"
            f"Таймфрейм: {TIMEFRAME}"
        )

        while True:
            reset_daily_stats_if_needed()

            try:
                print("Scanning...", now_text())

                btc_1h = await get_klines(session, BTC_SYMBOL, interval=TREND_TIMEFRAME, limit=260)

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
                        candles_15m = await get_klines(session, symbol, interval=TIMEFRAME, limit=260)
                        candles_5m = await get_klines(session, symbol, interval=CONFIRM_TIMEFRAME, limit=260)
                        candles_1h = await get_klines(session, symbol, interval=TREND_TIMEFRAME, limit=260)
                        candles_4h = await get_klines(session, symbol, interval=MACRO_TIMEFRAME, limit=260)

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
