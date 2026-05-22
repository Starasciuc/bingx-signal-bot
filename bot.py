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

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "300"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "80"))
MIN_QUALITY = int(os.getenv("MIN_QUALITY", "74"))

LEVERAGE = int(os.getenv("LEVERAGE", "20"))
MIN_POSITION_PROFIT_PERCENT = float(os.getenv("MIN_POSITION_PROFIT_PERCENT", "10"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "10800"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "5"))
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "3600"))

BTC_SYMBOL = "BTC-USDT"

SENT_SIGNALS = {}
SYMBOL_COOLDOWN = {}

STATS = {
    "started_at": time.time(),
    "last_heartbeat": 0,
    "total_scans": 0,
    "signals_total": 0,
    "signals_hour": 0,
    "last_checked_symbols": 0,
    "last_btc_status": "UNKNOWN",
    "last_btc_change": 0.0,
}


def now_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def uptime_text():
    seconds = int(time.time() - STATS["started_at"])
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}ч {minutes}м"


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


async def send_heartbeat(session):
    text = f"""
🤖 <b>BingX Signal Scanner v4 работает</b>

⏱ Аптайм: <b>{uptime_text()}</b>
🔎 Сканов всего: <b>{STATS["total_scans"]}</b>
📊 Проверено пар в последнем скане: <b>{STATS["last_checked_symbols"]}</b>

₿ BTC-фильтр: <b>{STATS["last_btc_status"]}</b>
₿ BTC изменение: <b>{STATS["last_btc_change"]:.2f}%</b>

🎯 Сигналов за последний час: <b>{STATS["signals_hour"]}</b>
🎯 Сигналов всего: <b>{STATS["signals_total"]}</b>

⚙️ Плечо: <b>{LEVERAGE}x</b>
🎯 Минимум TP1: <b>{MIN_POSITION_PROFIT_PERCENT:.0f}%+</b> по позиции

🕒 {now_text()}
""".strip()

    await send_telegram_message(session, text)
    STATS["signals_hour"] = 0
    STATS["last_heartbeat"] = time.time()


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


async def get_klines(session, symbol, interval=None, limit=150):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {
        "symbol": symbol,
        "interval": interval or TIMEFRAME,
        "limit": limit,
    }

    async with session.get(url, params=params, timeout=30) as resp:
        data = await resp.json()

    raw = data.get("data", [])
    if not raw or len(raw) < 60:
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

    last_close = closes[-1]
    prev_close = closes[-6] if len(closes) >= 6 else closes[-2]

    btc_change = ((last_close - prev_close) / prev_close) * 100

    if last_close > ema20 > ema50 and btc_change > 0.15:
        return "BULLISH", btc_change

    if last_close < ema20 < ema50 and btc_change < -0.15:
        return "BEARISH", btc_change

    return "NEUTRAL", btc_change


def higher_tf_filter(candles_1h):
    closes = [c["close"] for c in candles_1h]

    ema20_1h = ema(closes, 20)[-1]
    ema50_1h = ema(closes, 50)[-1]
    ema100_1h = ema(closes, 100)[-1]

    price = closes[-1]

    if price > ema20_1h > ema50_1h:
        return "BULLISH"

    if price < ema20_1h < ema50_1h:
        return "BEARISH"

    if price > ema100_1h:
        return "SOFT_BULLISH"

    if price < ema100_1h:
        return "SOFT_BEARISH"

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
    last_rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(candles, 14)

    if last_rsi is None or atr is None:
        return None

    trend_1h = higher_tf_filter(candles_1h)

    recent_high = max(highs[-24:])
    recent_low = min(lows[-24:])
    avg_volume = sum(volumes[-30:]) / 30

    volume_ratio = last["volume"] / avg_volume if avg_volume > 0 else 0

    if volume_ratio < 1.15:
        return None

    signal = None
    setup_type = None
    quality = 50
    reasons = []

    bullish_structure = price > ema20 and ema20 > ema50
    bearish_structure = price < ema20 and ema20 < ema50

    strong_uptrend = ema20 > ema50 > ema100
    strong_downtrend = ema20 < ema50 < ema100

    # LONG Pullback / Reversal
    if (
        bullish_structure
        and prev["low"] <= recent_low * 1.006
        and last_rsi < 50
        and volume_ratio >= 1.15
    ):
        if btc_status == "BEARISH":
            return None

        if trend_1h in ["BEARISH", "SOFT_BEARISH"]:
            return None

        signal = "LONG"
        setup_type = "LONG Pullback"
        quality += 12

        if strong_uptrend:
            quality += 8
            reasons.append("15m EMA trend bullish")

        if trend_1h in ["BULLISH", "SOFT_BULLISH"]:
            quality += 10
            reasons.append(f"1h trend: {trend_1h}")

        if btc_status == "BULLISH":
            quality += 8
            reasons.append("BTC поддерживает LONG")

        if last_rsi < 42:
            quality += 8
            reasons.append(f"RSI остыл: {last_rsi:.1f}")
        else:
            quality += 4
            reasons.append(f"RSI: {last_rsi:.1f}")

        if volume_ratio >= 1.5:
            quality += 12
            reasons.append(f"Сильный объём x{volume_ratio:.2f}")
        else:
            quality += 6
            reasons.append(f"Объём x{volume_ratio:.2f}")

        entry = price
        sl = entry - atr * 1.1
        tp1 = entry * (1 + (MIN_POSITION_PROFIT_PERCENT / LEVERAGE) / 100)
        tp2 = entry * (1 + (20 / LEVERAGE) / 100)
        tp3 = entry * (1 + (30 / LEVERAGE) / 100)

    # SHORT Pullback / Reversal
    elif (
        bearish_structure
        and prev["high"] >= recent_high * 0.994
        and last_rsi > 50
        and volume_ratio >= 1.15
    ):
        if btc_status == "BULLISH":
            return None

        if trend_1h in ["BULLISH", "SOFT_BULLISH"]:
            return None

        signal = "SHORT"
        setup_type = "SHORT Pullback"
        quality += 12

        if strong_downtrend:
            quality += 8
            reasons.append("15m EMA trend bearish")

        if trend_1h in ["BEARISH", "SOFT_BEARISH"]:
            quality += 10
            reasons.append(f"1h trend: {trend_1h}")

        if btc_status == "BEARISH":
            quality += 8
            reasons.append("BTC поддерживает SHORT")

        if last_rsi > 58:
            quality += 8
            reasons.append(f"RSI высокий: {last_rsi:.1f}")
        else:
            quality += 4
            reasons.append(f"RSI: {last_rsi:.1f}")

        if volume_ratio >= 1.5:
            quality += 12
            reasons.append(f"Сильный объём x{volume_ratio:.2f}")
        else:
            quality += 6
            reasons.append(f"Объём x{volume_ratio:.2f}")

        entry = price
        sl = entry + atr * 1.1
        tp1 = entry * (1 - (MIN_POSITION_PROFIT_PERCENT / LEVERAGE) / 100)
        tp2 = entry * (1 - (20 / LEVERAGE) / 100)
        tp3 = entry * (1 - (30 / LEVERAGE) / 100)

    else:
        return None

    quality = min(97, max(50, quality))

    if quality < MIN_QUALITY:
        return None

    tp1_position_profit = position_profit_percent(entry, tp1, signal)
    tp2_position_profit = position_profit_percent(entry, tp2, signal)
    tp3_position_profit = position_profit_percent(entry, tp3, signal)

    if tp1_position_profit < MIN_POSITION_PROFIT_PERCENT:
        return None

    risk_price_percent = abs(sl - entry) / entry * 100
    risk_position_percent = risk_price_percent * LEVERAGE

    if risk_position_percent > 18:
        return None

    reward_price_percent = price_move_percent(entry, tp1, signal)
    rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

    if rr < 0.65:
        return None

    signal_id = f"{symbol}:{TIMEFRAME}:{signal}:{round(entry, 6)}"

    if signal_id in SENT_SIGNALS:
        return None

    SENT_SIGNALS[signal_id] = time.time()
    set_cooldown(symbol)

    return {
        "symbol": symbol,
        "side": signal,
        "setup_type": setup_type,
        "quality": quality,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rsi": last_rsi,
        "volume_ratio": volume_ratio,
        "risk_price_percent": risk_price_percent,
        "risk_position_percent": risk_position_percent,
        "tp1_position_profit": tp1_position_profit,
        "tp2_position_profit": tp2_position_profit,
        "tp3_position_profit": tp3_position_profit,
        "rr": rr,
        "reasons": reasons,
        "btc_status": btc_status,
        "trend_1h": trend_1h,
    }


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

Сетап: <b>{signal["setup_type"]}</b>
Качество: <b>{signal["quality"]}%</b>

Плечо: <b>{LEVERAGE}x</b>

🎯 Вход: <code>{format_price(signal["entry"])}</code>
🛑 SL: <code>{format_price(signal["sl"])}</code>

✅ TP1: <code>{format_price(signal["tp1"])}</code> ≈ <b>+{signal["tp1_position_profit"]:.1f}%</b>
✅ TP2: <code>{format_price(signal["tp2"])}</code> ≈ <b>+{signal["tp2_position_profit"]:.1f}%</b>
✅ TP3: <code>{format_price(signal["tp3"])}</code> ≈ <b>+{signal["tp3_position_profit"]:.1f}%</b>

📊 Риск до SL: <b>{signal["risk_price_percent"]:.2f}% цены</b> / около <b>{signal["risk_position_percent"]:.1f}%</b> по позиции
📊 RR до TP1: <b>{signal["rr"]:.2f}</b>

₿ BTC: <b>{signal["btc_status"]}</b>
⏱ 1h trend: <b>{signal["trend_1h"]}</b>
📊 RSI: <b>{signal["rsi"]:.1f}</b>
📊 Объём: <b>x{signal["volume_ratio"]:.2f}</b>

<b>Почему сигнал:</b>
{reasons_text}

⚠️ Риск-менеджмент: до 0.5% от депозита.
⚠️ Не финансовый совет. Фьючерсы несут высокий риск.

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
            f"✅ BingX Signal Scanner v4 запущен.\nПлечо: {LEVERAGE}x\nЦель: {MIN_POSITION_PROFIT_PERCENT:.0f}%+ по позиции."
        )

        STATS["last_heartbeat"] = time.time()

        while True:
            try:
                print("Scanning...", now_text())

                btc_candles = await get_klines(session, BTC_SYMBOL, interval=TIMEFRAME, limit=150)

                if not btc_candles:
                    print("BTC data unavailable")
                    await asyncio.sleep(60)
                    continue

                btc_status, btc_change = btc_market_filter(btc_candles)

                STATS["last_btc_status"] = btc_status
                STATS["last_btc_change"] = btc_change

                print(f"BTC status: {btc_status}, change: {btc_change:.2f}%")

                symbols = await get_symbols(session)
                checked = 0
                found = 0

                for symbol in symbols:
                    if symbol == BTC_SYMBOL:
                        continue

                    checked += 1

                    try:
                        candles = await get_klines(session, symbol, interval=TIMEFRAME, limit=150)
                        candles_1h = await get_klines(session, symbol, interval=HIGHER_TIMEFRAME, limit=150)

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

                        found += 1
                        STATS["signals_total"] += 1
                        STATS["signals_hour"] += 1

                        await asyncio.sleep(2)

                        if found >= MAX_SIGNALS_PER_SCAN:
                            break

                    except Exception as e:
                        print(f"Error with {symbol}:", e)

                STATS["total_scans"] += 1
                STATS["last_checked_symbols"] = checked

                print(f"Scan finished. Checked: {checked}. Signals found: {found}")

                if time.time() - STATS["last_heartbeat"] >= HEARTBEAT_SECONDS:
                    await send_heartbeat(session)

                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

            except Exception as e:
                print("Main loop error:", e)
                await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(scan_loop())
