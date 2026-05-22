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
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "300"))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "60"))
MIN_QUALITY = int(os.getenv("MIN_QUALITY", "72"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "10800"))  # 3 часа
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "5"))

BTC_SYMBOL = "BTC-USDT"

SENT_SIGNALS = {}
SYMBOL_COOLDOWN = {}


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


async def get_klines(session, symbol, limit=150):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {
        "symbol": symbol,
        "interval": TIMEFRAME,
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


def analyze_symbol(symbol, candles, btc_status):
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

    recent_high = max(highs[-24:])
    recent_low = min(lows[-24:])
    avg_volume = sum(volumes[-30:]) / 30

    volume_ratio = last["volume"] / avg_volume if avg_volume > 0 else 0

    if volume_ratio < 1.15:
        return None

    signal = None
    quality = 50
    reasons = []

    bullish_structure = price > ema20 and ema20 > ema50
    bearish_structure = price < ema20 and ema20 < ema50

    strong_uptrend = ema20 > ema50 > ema100
    strong_downtrend = ema20 < ema50 < ema100

    # LONG reversal / continuation from support
    if (
        bullish_structure
        and prev["low"] <= recent_low * 1.006
        and last_rsi < 48
        and volume_ratio >= 1.15
    ):
        if btc_status == "BEARISH":
            return None

        signal = "LONG"
        quality += 12

        if strong_uptrend:
            quality += 8
            reasons.append("EMA trend bullish")

        if btc_status == "BULLISH":
            quality += 8
            reasons.append("BTC поддерживает LONG")

        if last_rsi < 42:
            quality += 8
            reasons.append(f"RSI низкий: {last_rsi:.1f}")
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
        sl = entry - atr * 1.4
        tp1 = entry + atr * 1.8
        tp2 = entry + atr * 2.8

    # SHORT reversal / continuation from resistance
    elif (
        bearish_structure
        and prev["high"] >= recent_high * 0.994
        and last_rsi > 52
        and volume_ratio >= 1.15
    ):
        if btc_status == "BULLISH":
            return None

        signal = "SHORT"
        quality += 12

        if strong_downtrend:
            quality += 8
            reasons.append("EMA trend bearish")

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
        sl = entry + atr * 1.4
        tp1 = entry - atr * 1.8
        tp2 = entry - atr * 2.8

    else:
        return None

    quality = min(96, max(50, quality))

    if quality < MIN_QUALITY:
        return None

    risk_percent = abs(sl - entry) / entry * 100

    if risk_percent > 5:
        return None

    signal_id = f"{symbol}:{TIMEFRAME}:{signal}:{round(entry, 6)}"

    if signal_id in SENT_SIGNALS:
        return None

    SENT_SIGNALS[signal_id] = time.time()
    set_cooldown(symbol)

    return {
        "symbol": symbol,
        "side": signal,
        "quality": quality,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "rsi": last_rsi,
        "volume_ratio": volume_ratio,
        "risk_percent": risk_percent,
        "reasons": reasons,
        "btc_status": btc_status,
    }


def format_price(x):
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.8f}"


def make_message(signal):
    emoji = "📈" if signal["side"] == "LONG" else "📉"

    reasons_text = "\n".join([f"• {r}" for r in signal["reasons"]])

    return f"""
🎯 <b>Скринер точек входа v2</b>

{emoji} <b>{signal["side"]} {signal["symbol"].replace("-", "/")}</b> · {TIMEFRAME}
Качество: <b>{signal["quality"]}%</b>

🎯 Зона входа: <code>{format_price(signal["entry"])}</code>
🛑 SL: <code>{format_price(signal["sl"])}</code>
✅ TP1: <code>{format_price(signal["tp1"])}</code>
✅ TP2: <code>{format_price(signal["tp2"])}</code>

📊 RSI: <b>{signal["rsi"]:.1f}</b>
📊 Объём: <b>x{signal["volume_ratio"]:.2f}</b>
📊 Риск до SL: <b>{signal["risk_percent"]:.2f}%</b>
₿ BTC-фильтр: <b>{signal["btc_status"]}</b>

<b>Почему сигнал:</b>
{reasons_text}

⚠️ Плечо max.: 10x
⚠️ Риск до 0.5% от депозита

Не финансовый совет.
Фьючерсы несут высокий риск.

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
            "✅ BingX Signal Scanner v2 запущен.\nФильтры: BTC trend, ATR SL/TP, volume, антиспам."
        )

        while True:
            try:
                print("Scanning...", now_text())

                btc_candles = await get_klines(session, BTC_SYMBOL, limit=150)

                if not btc_candles:
                    print("BTC data unavailable")
                    await asyncio.sleep(60)
                    continue

                btc_status, btc_change = btc_market_filter(btc_candles)
                print(f"BTC status: {btc_status}, change: {btc_change:.2f}%")

                symbols = await get_symbols(session)
                found = 0

                for symbol in symbols:
                    if symbol == BTC_SYMBOL:
                        continue

                    try:
                        candles = await get_klines(session, symbol, limit=150)

                        if candles is None:
                            continue

                        signal = analyze_symbol(symbol, candles, btc_status)

                        if not signal:
                            continue

                        await send_telegram_message(
                            session,
                            make_message(signal),
                            button_url=bingx_url(symbol)
                        )

                        found += 1
                        await asyncio.sleep(2)

                        if found >= MAX_SIGNALS_PER_SCAN:
                            break

                    except Exception as e:
                        print(f"Error with {symbol}:", e)

                print(f"Scan finished. Signals found: {found}")
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

            except Exception as e:
                print("Main loop error:", e)
                await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(scan_loop())
