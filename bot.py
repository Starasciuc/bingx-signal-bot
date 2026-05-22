import os
import asyncio
import random
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv


load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BINGX_BASE_URL = "https://open-api.bingx.com"

TIMEFRAME = os.getenv("TIMEFRAME", "15m")
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "300"))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "40"))
MIN_QUALITY = int(os.getenv("MIN_QUALITY", "70"))

SENT_SIGNALS = set()


def now_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


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
    return symbols[:MAX_SYMBOLS]


async def get_klines(session, symbol):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {
        "symbol": symbol,
        "interval": TIMEFRAME,
        "limit": 120,
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

    recent_gains = gains[-period:]
    recent_losses = losses[-period:]

    avg_gain = sum(recent_gains) / period
    avg_loss = sum(recent_losses) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def analyze_symbol(symbol, candles):
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    ema20_list = ema(closes, 20)
    ema50_list = ema(closes, 50)

    last = candles[-1]
    prev = candles[-2]

    price = last["close"]
    ema20 = ema20_list[-1]
    ema50 = ema50_list[-1]
    last_rsi = calculate_rsi(closes, 14)

    if last_rsi is None:
        return None

    recent_high = max(highs[-20:])
    recent_low = min(lows[-20:])
    avg_volume = sum(volumes[-30:]) / 30

    signal = None
    quality = 50

    # SHORT reversal setup
    if (
        price < ema20
        and ema20 < ema50
        and prev["high"] >= recent_high * 0.995
        and last_rsi > 55
        and last["volume"] > avg_volume * 1.05
    ):
        signal = "SHORT"
        quality += 15
        quality += min(15, int((last["volume"] / avg_volume - 1) * 20))
        quality += min(10, int(last_rsi - 55))

        entry = price
        sl = recent_high * 1.003
        risk = sl - entry

        if risk <= 0:
            return None

        tp1 = entry - risk * 1.2
        tp2 = entry - risk * 2.0

    # LONG reversal setup
    elif (
        price > ema20
        and ema20 > ema50
        and prev["low"] <= recent_low * 1.005
        and last_rsi < 45
        and last["volume"] > avg_volume * 1.05
    ):
        signal = "LONG"
        quality += 15
        quality += min(15, int((last["volume"] / avg_volume - 1) * 20))
        quality += min(10, int(45 - last_rsi))

        entry = price
        sl = recent_low * 0.997
        risk = entry - sl

        if risk <= 0:
            return None

        tp1 = entry + risk * 1.2
        tp2 = entry + risk * 2.0

    else:
        return None

    quality = min(95, max(50, quality))

    if quality < MIN_QUALITY:
        return None

    if abs(sl - entry) / entry > 0.08:
        return None

    signal_id = f"{symbol}:{TIMEFRAME}:{signal}:{round(entry, 6)}"
    if signal_id in SENT_SIGNALS:
        return None

    SENT_SIGNALS.add(signal_id)

    return {
        "symbol": symbol,
        "side": signal,
        "quality": quality,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
    }


def format_price(x):
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.8f}"


def make_message(signal):
    emoji = "📈" if signal["side"] == "LONG" else "📉"

    return f"""
🎯 <b>Скринер точек входа</b>

{emoji} <b>{signal["side"]} {signal["symbol"].replace("-", "/")}</b> · {TIMEFRAME}
Качество: <b>{signal["quality"]}%</b>

🎯 Зона входа: <code>{format_price(signal["entry"])}</code>
🛑 SL: <code>{format_price(signal["sl"])}</code>
✅ TP1: <code>{format_price(signal["tp1"])}</code>
✅ TP2: <code>{format_price(signal["tp2"])}</code>

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
            "✅ BingX Signal Scanner запущен.\nБот начал сканировать рынок."
        )

        while True:
            try:
                print("Scanning...", now_text())

                symbols = await get_symbols(session)
                found = 0

                for symbol in symbols:
                    try:
                        candles = await get_klines(session, symbol)

                        if candles is None:
                            continue

                        signal = analyze_symbol(symbol, candles)

                        if not signal:
                            continue

                        await send_telegram_message(
                            session,
                            make_message(signal),
                            button_url=bingx_url(symbol)
                        )

                        found += 1
                        await asyncio.sleep(2)

                    except Exception as e:
                        print(f"Error with {symbol}:", e)

                print(f"Scan finished. Signals found: {found}")
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

            except Exception as e:
                print("Main loop error:", e)
                await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(scan_loop())
