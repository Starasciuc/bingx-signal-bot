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

TIMEFRAME = os.getenv("TIMEFRAME", "15m")
HIGHER_TIMEFRAME = os.getenv("HIGHER_TIMEFRAME", "1h")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "120"))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "300"))

LEVERAGE = int(os.getenv("LEVERAGE", "20"))

# Главная цель: TP1 от +20% по позиции
MIN_POSITION_PROFIT_PERCENT = float(os.getenv("MIN_POSITION_PROFIT_PERCENT", "20"))

# Безопасность
MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "6"))
MIN_RR = float(os.getenv("MIN_RR", "1.5"))

# Ограничения сигналов
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "10800"))
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


async def get_klines(session, symbol, interval=None, limit=160):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {
        "symbol": symbol,
        "interval": interval or TIMEFRAME,
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


def btc_market_filter(btc_candles):
    closes = [c["close"] for c in btc_candles]

    ema20 = ema(closes, 20)[-1]
    ema50 = ema(closes, 50)[-1]
    ema100 = ema(closes, 100)[-1]

    last_close = closes[-1]
    prev_close = closes[-6] if len(closes) >= 6 else closes[-2]

    btc_change = ((last_close - prev_close) / prev_close) * 100

    if last_close > ema20 > ema50 > ema100 and btc_change > 0.10:
        return "STRONG_BULLISH", btc_change

    if last_close < ema20 < ema50 < ema100 and btc_change < -0.10:
        return "STRONG_BEARISH", btc_change

    if last_close > ema20 > ema50:
        return "BULLISH", btc_change

    if last_close < ema20 < ema50:
        return "BEARISH", btc_change

    return "NEUTRAL", btc_change


def higher_tf_filter(candles_1h):
    closes = [c["close"] for c in candles_1h]

    ema20_1h = ema(closes, 20)[-1]
    ema50_1h = ema(closes, 50)[-1]
    ema100_1h = ema(closes, 100)[-1]

    price = closes[-1]

    if price > ema20_1h > ema50_1h > ema100_1h:
        return "STRONG_BULLISH"

    if price < ema20_1h < ema50_1h < ema100_1h:
        return "STRONG_BEARISH"

    if price > ema20_1h > ema50_1h:
        return "BULLISH"

    if price < ema20_1h < ema50_1h:
        return "BEARISH"

    return "NEUTRAL"


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


def build_signal(symbol, side, setup_type, price, atr, rsi, volume_ratio, btc_status, trend_1h, quality, reasons):
    max_risk_price_percent = MAX_RISK_POSITION_PERCENT / LEVERAGE
    max_risk_price_move = price * max_risk_price_percent / 100

    atr_stop = atr * 0.65
    stop_distance = min(atr_stop, max_risk_price_move)

    if stop_distance <= 0:
        return None

    # TP1 = +20% по позиции при 20x = примерно 1% движения цены
    # TP2 = +30% по позиции
    # TP3 = +50% по позиции
    if side == "LONG":
        sl = price - stop_distance
        tp1 = price * (1 + (MIN_POSITION_PROFIT_PERCENT / LEVERAGE) / 100)
        tp2 = price * (1 + (30 / LEVERAGE) / 100)
        tp3 = price * (1 + (50 / LEVERAGE) / 100)
    else:
        sl = price + stop_distance
        tp1 = price * (1 - (MIN_POSITION_PROFIT_PERCENT / LEVERAGE) / 100)
        tp2 = price * (1 - (30 / LEVERAGE) / 100)
        tp3 = price * (1 - (50 / LEVERAGE) / 100)

    risk_price_percent = abs(sl - price) / price * 100
    risk_position_percent = risk_price_percent * LEVERAGE

    if risk_position_percent > MAX_RISK_POSITION_PERCENT:
        return None

    tp1_profit = position_profit_percent(price, tp1, side)
    tp2_profit = position_profit_percent(price, tp2, side)
    tp3_profit = position_profit_percent(price, tp3, side)

    if tp1_profit < MIN_POSITION_PROFIT_PERCENT:
        return None

    reward_price_percent = price_move_percent(price, tp1, side)
    rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

    if rr < MIN_RR:
        return None

    signal_id = f"{symbol}:{TIMEFRAME}:{side}:SAFE_PROFIT:{round(price, 6)}"

    if signal_id in SENT_SIGNALS:
        return None

    return {
        "symbol": symbol,
        "side": side,
        "setup_type": setup_type,
        "quality": min(99, max(50, quality)),
        "entry": price,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rsi": rsi,
        "volume_ratio": volume_ratio,
        "risk_price_percent": risk_price_percent,
        "risk_position_percent": risk_position_percent,
        "tp1_position_profit": tp1_profit,
        "tp2_position_profit": tp2_profit,
        "tp3_position_profit": tp3_profit,
        "rr": rr,
        "reasons": reasons,
        "btc_status": btc_status,
        "trend_1h": trend_1h,
        "signal_id": signal_id,
    }


def analyze_symbol(symbol, candles, candles_1h, btc_status):
    if is_on_cooldown(symbol):
        return None

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    ema20_list = ema(closes, 20)
    ema50_list = ema(closes, 50)
    ema100_list = ema(closes, 100)

    last = candles[-1]
    prev = candles[-2]

    price = last["close"]
    ema20 = ema20_list[-1]
    ema50 = ema50_list[-1]
    ema100 = ema100_list[-1]

    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(candles, 14)

    if rsi is None or atr is None:
        return None

    trend_1h = higher_tf_filter(candles_1h)

    avg_volume = sum(volumes[-30:]) / 30
    volume_ratio = last["volume"] / avg_volume if avg_volume > 0 else 0

    recent_high = max(highs[-20:])
    recent_low = min(lows[-20:])

    bullish_15m = price > ema20 > ema50
    bearish_15m = price < ema20 < ema50

    strong_bullish_15m = ema20 > ema50 > ema100
    strong_bearish_15m = ema20 < ema50 < ema100

    candidates = []

    # SAFE LONG PROFIT
    if (
        bullish_15m
        and strong_bullish_15m
        and trend_1h in ["BULLISH", "STRONG_BULLISH"]
        and btc_status not in ["BEARISH", "STRONG_BEARISH"]
        and 40 <= rsi <= 58
        and volume_ratio >= 1.20
        and prev["low"] <= recent_low * 1.010
    ):
        quality = 70
        reasons = [
            "✅ SAFE PROFIT сигнал",
            "15m сильный bullish trend",
            f"1h trend: {trend_1h}",
            f"RSI в рабочей зоне: {rsi:.1f}",
            f"Объём выше среднего x{volume_ratio:.2f}",
            "Откат к зоне поддержки",
        ]

        if trend_1h == "STRONG_BULLISH":
            quality += 8
            reasons.append("1h trend сильный bullish")

        if btc_status in ["BULLISH", "STRONG_BULLISH"]:
            quality += 7
            reasons.append("BTC не мешает LONG")

        if btc_status == "STRONG_BULLISH":
            quality += 5
            reasons.append("BTC сильный bullish")

        if volume_ratio >= 1.6:
            quality += 7
            reasons.append("Сильный всплеск объёма")

        signal = build_signal(
            symbol, "LONG", "SAFE PROFIT LONG",
            price, atr, rsi, volume_ratio, btc_status, trend_1h,
            quality, reasons
        )

        if signal:
            candidates.append(signal)

    # SAFE SHORT PROFIT
    if (
        bearish_15m
        and strong_bearish_15m
        and trend_1h in ["BEARISH", "STRONG_BEARISH"]
        and btc_status not in ["BULLISH", "STRONG_BULLISH"]
        and 42 <= rsi <= 60
        and volume_ratio >= 1.20
        and prev["high"] >= recent_high * 0.990
    ):
        quality = 70
        reasons = [
            "✅ SAFE PROFIT сигнал",
            "15m сильный bearish trend",
            f"1h trend: {trend_1h}",
            f"RSI в рабочей зоне: {rsi:.1f}",
            f"Объём выше среднего x{volume_ratio:.2f}",
            "Откат к зоне сопротивления",
        ]

        if trend_1h == "STRONG_BEARISH":
            quality += 8
            reasons.append("1h trend сильный bearish")

        if btc_status in ["BEARISH", "STRONG_BEARISH"]:
            quality += 7
            reasons.append("BTC не мешает SHORT")

        if btc_status == "STRONG_BEARISH":
            quality += 5
            reasons.append("BTC сильный bearish")

        if volume_ratio >= 1.6:
            quality += 7
            reasons.append("Сильный всплеск объёма")

        signal = build_signal(
            symbol, "SHORT", "SAFE PROFIT SHORT",
            price, atr, rsi, volume_ratio, btc_status, trend_1h,
            quality, reasons
        )

        if signal:
            candidates.append(signal)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            x["quality"],
            x["rr"],
            x["volume_ratio"],
        ),
        reverse=True
    )

    best = candidates[0]

    if best["quality"] < 72:
        return None

    return best


def format_price(x):
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.8f}"


def make_message(signal):
    arrow = "📈" if signal["side"] == "LONG" else "📉"
    fire = "🔥" if signal["quality"] >= 85 else "🎯"
    reasons_text = "\n".join([f"• {r}" for r in signal["reasons"]])

    return f"""
{fire} <b>{signal["symbol"].replace("-", "/")}</b>
{arrow} <b>{signal["side"]}</b>

Тип: <b>✅ SAFE PROFIT сигнал</b>
Сетап: <b>{signal["setup_type"]}</b>
Качество: <b>{signal["quality"]}%</b>

Плечо: <b>{LEVERAGE}x</b>

🎯 Вход: <code>{format_price(signal["entry"])}</code>
🛑 SL: <code>{format_price(signal["sl"])}</code>

✅ TP1: <code>{format_price(signal["tp1"])}</code> ≈ <b>+{signal["tp1_position_profit"]:.1f}%</b>
✅ TP2: <code>{format_price(signal["tp2"])}</code> ≈ <b>+{signal["tp2_position_profit"]:.1f}%</b>
✅ TP3: <code>{format_price(signal["tp3"])}</code> ≈ <b>+{signal["tp3_position_profit"]:.1f}%</b>

🛡 Риск до SL: <b>{signal["risk_price_percent"]:.2f}% цены</b> / около <b>{signal["risk_position_percent"]:.1f}%</b> по позиции
📊 RR до TP1: <b>{signal["rr"]:.2f}</b>

₿ BTC: <b>{signal["btc_status"]}</b>
⏱ 1h trend: <b>{signal["trend_1h"]}</b>
📊 RSI: <b>{signal["rsi"]:.1f}</b>
📊 Объём: <b>x{signal["volume_ratio"]:.2f}</b>

<b>Почему сигнал:</b>
{reasons_text}

⚠️ 20x — высокий риск. Даже SAFE-сигнал может дать убыток.
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
            f"✅ BingX Signal Scanner v4.5 SAFE PROFIT MODE запущен.\n"
            f"Только безопасные и уверенные сигналы.\n"
            f"Плечо: {LEVERAGE}x\n"
            f"TP1: {MIN_POSITION_PROFIT_PERCENT:.0f}%+ по позиции\n"
            f"Максимальный риск: {MAX_RISK_POSITION_PERCENT:.1f}% по позиции\n"
            f"RR минимум: {MIN_RR}\n"
            f"Ежечасные обновления отключены."
        )

        while True:
            reset_daily_stats_if_needed()

            try:
                print("Scanning...", now_text())

                btc_candles = await get_klines(session, BTC_SYMBOL, interval=TIMEFRAME, limit=160)

                if not btc_candles:
                    print("BTC data unavailable")
                    await asyncio.sleep(60)
                    continue

                btc_status, btc_change = btc_market_filter(btc_candles)

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
                        candles = await get_klines(session, symbol, interval=TIMEFRAME, limit=160)
                        candles_1h = await get_klines(session, symbol, interval=HIGHER_TIMEFRAME, limit=160)

                        if candles is None or candles_1h is None:
                            continue

                        signal = analyze_symbol(symbol, candles, candles_1h, btc_status)

                        if not signal:
                            continue

                        await send_telegram_message(
                            session,
                            make_message(signal),
                            button_url=bingx_url(symbol)
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
