import os
import re
import time
import json
import random
import sqlite3
import asyncio
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv


load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BINGX_BASE_URL = "https://open-api.bingx.com"

TIMEFRAME = os.getenv("TIMEFRAME", "15m")
CONFIRM_TIMEFRAME = os.getenv("CONFIRM_TIMEFRAME", "1m")
CONFIRM_5M_TIMEFRAME = os.getenv("CONFIRM_5M_TIMEFRAME", "5m")
TREND_TIMEFRAME = os.getenv("TREND_TIMEFRAME", "1h")
MACRO_TIMEFRAME = os.getenv("MACRO_TIMEFRAME", "4h")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "45"))
TRACK_INTERVAL_SECONDS = int(os.getenv("TRACK_INTERVAL_SECONDS", "20"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "120"))

LEVERAGE = int(os.getenv("LEVERAGE", "10"))

ENABLE_LONG = os.getenv("ENABLE_LONG", "true").lower() == "true"
ENABLE_SHORT = os.getenv("ENABLE_SHORT", "true").lower() == "true"
TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"

TP1_POSITION_PERCENT = float(os.getenv("TP1_POSITION_PERCENT", "8"))
TP2_POSITION_PERCENT = float(os.getenv("TP2_POSITION_PERCENT", "15"))
TP3_POSITION_PERCENT = float(os.getenv("TP3_POSITION_PERCENT", "25"))

MIN_RISK_POSITION_PERCENT = float(os.getenv("MIN_RISK_POSITION_PERCENT", "6"))
MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "8.5"))
MIN_RR_TO_TP1 = float(os.getenv("MIN_RR_TO_TP1", "0.95"))

MIN_QUALITY = int(os.getenv("MIN_QUALITY", "88"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.55"))
A_PLUS_VOLUME_RATIO = float(os.getenv("A_PLUS_VOLUME_RATIO", "1.85"))

BREAKOUT_CLOSE_PERCENT = float(os.getenv("BREAKOUT_CLOSE_PERCENT", "0.22"))
MAX_BREAKOUT_CHASE_POSITION_PERCENT = float(os.getenv("MAX_BREAKOUT_CHASE_POSITION_PERCENT", "10"))

MIN_PREVIOUS_RISE_FOR_LONG = float(os.getenv("MIN_PREVIOUS_RISE_FOR_LONG", "0.9"))
MIN_PREVIOUS_DROP_FOR_SHORT = float(os.getenv("MIN_PREVIOUS_DROP_FOR_SHORT", "0.9"))

LEVEL_DISTANCE_PERCENT = float(os.getenv("LEVEL_DISTANCE_PERCENT", "1.0"))
LEVEL_CLUSTER_ZONE_PERCENT = float(os.getenv("LEVEL_CLUSTER_ZONE_PERCENT", "0.35"))
MIN_LEVEL_TOUCHES = int(os.getenv("MIN_LEVEL_TOUCHES", "2"))

MIN_BODY_ATR_MULTIPLIER = float(os.getenv("MIN_BODY_ATR_MULTIPLIER", "0.35"))
MAX_BODY_ATR_MULTIPLIER = float(os.getenv("MAX_BODY_ATR_MULTIPLIER", "1.8"))

LONG_MIN_CLOSE_POSITION = float(os.getenv("LONG_MIN_CLOSE_POSITION", "0.65"))
SHORT_MAX_CLOSE_POSITION = float(os.getenv("SHORT_MAX_CLOSE_POSITION", "0.35"))

BTC_DANGEROUS_ATR_PERCENT = float(os.getenv("BTC_DANGEROUS_ATR_PERCENT", "2.2"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "5400"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "2"))
DAILY_MAX_SIGNALS = int(os.getenv("DAILY_MAX_SIGNALS", "8"))

PAIR_MAX_SL_BEFORE_BLOCK = int(os.getenv("PAIR_MAX_SL_BEFORE_BLOCK", "1"))
SIDE_MAX_CONSECUTIVE_SL = int(os.getenv("SIDE_MAX_CONSECUTIVE_SL", "2"))
SIDE_DISABLE_SECONDS = int(os.getenv("SIDE_DISABLE_SECONDS", "21600"))

MIN_CLOSED_TRADES_FOR_SIDE_CHECK = int(os.getenv("MIN_CLOSED_TRADES_FOR_SIDE_CHECK", "10"))
MIN_SIDE_WINRATE = float(os.getenv("MIN_SIDE_WINRATE", "50"))

USE_LIQUID_ONLY = os.getenv("USE_LIQUID_ONLY", "true").lower() == "true"

DB_PATH = os.getenv("DB_PATH", "bot_stats.sqlite3")
STATE_PATH = os.getenv("STATE_PATH", "bot_state.json")

BTC_SYMBOL = "BTC-USDT"

LIQUID_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "INJ", "NEAR", "ARB", "OP", "APT", "SUI", "SEI", "DOT", "LTC",
    "BCH", "UNI", "AAVE", "FIL", "ATOM", "ETC", "TRX", "MATIC", "WLD",
    "TIA", "ORDI", "FTM", "RUNE", "ENA", "JUP", "PYTH", "STRK", "DYDX",
    "TON", "COMP", "STX", "TRB", "JTO", "DYM", "ICP"
}

SENT_SIGNALS = {}
SYMBOL_COOLDOWN = {}
ACTIVE_SIGNALS = {}
BLOCKED_SYMBOLS = {}

SIDE_DISABLED_UNTIL = {
    "LONG": 0,
    "SHORT": 0,
}

STATS = {
    "signals_today": 0,
    "signals_total": 0,

    "long_positive": 0,
    "long_sl": 0,
    "long_tp1": 0,
    "long_tp2": 0,
    "long_tp3": 0,

    "short_positive": 0,
    "short_sl": 0,
    "short_tp1": 0,
    "short_tp2": 0,
    "short_tp3": 0,

    "long_consecutive_sl": 0,
    "short_consecutive_sl": 0,

    "pair_sl": {},
    "pair_positive": {},

    "current_day": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
}

KLINE_CACHE = {}


def now_ms():
    return int(time.time() * 1000)


def now_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def format_price(x):
    if x >= 100:
        return f"{x:.3f}"
    if x >= 1:
        return f"{x:.5f}"
    return f"{x:.8f}"


def base_from_symbol(symbol):
    return symbol.replace("-USDT", "").upper()


def prefix_for_side(side):
    return "long" if side == "LONG" else "short"


def bingx_url(symbol):
    pair = symbol.replace("-", "")
    return f"https://bingx.com/en/futures/forward/{pair}"


def reset_daily_stats_if_needed():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if STATS["current_day"] != today:
        STATS["current_day"] = today
        STATS["signals_today"] = 0
        save_state()


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            side TEXT,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            rr REAL,
            setup_score INTEGER,
            volume_ratio REAL,
            risk_position_percent REAL,
            btc_status TEXT,
            trend_1h TEXT,
            trend_4h TEXT,
            rsi REAL,
            created_at INTEGER,
            result TEXT,
            result_price REAL,
            result_at INTEGER
        )
    """)

    conn.commit()
    conn.close()


def log_signal(signal):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO signals (
            id, symbol, side, entry, sl, tp1, tp2, tp3, rr,
            setup_score, volume_ratio, risk_position_percent,
            btc_status, trend_1h, trend_4h, rsi, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        signal["signal_id"],
        signal["symbol"],
        signal["side"],
        signal["entry"],
        signal["sl"],
        signal["tp1"],
        signal["tp2"],
        signal["tp3"],
        signal["rr"],
        signal["setup_score"],
        signal["volume_ratio"],
        signal["risk_position_percent"],
        signal.get("btc_status"),
        signal.get("trend_1h"),
        signal.get("trend_4h"),
        signal.get("rsi"),
        int(signal["created_at"]),
    ))

    conn.commit()
    conn.close()


def log_result(signal, result, price):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        UPDATE signals
        SET result = ?, result_price = ?, result_at = ?
        WHERE id = ?
    """, (
        result,
        price,
        int(time.time()),
        signal["signal_id"],
    ))

    conn.commit()
    conn.close()


def save_state():
    state = {
        "SENT_SIGNALS": SENT_SIGNALS,
        "SYMBOL_COOLDOWN": SYMBOL_COOLDOWN,
        "ACTIVE_SIGNALS": ACTIVE_SIGNALS,
        "BLOCKED_SYMBOLS": BLOCKED_SYMBOLS,
        "SIDE_DISABLED_UNTIL": SIDE_DISABLED_UNTIL,
        "STATS": STATS,
    }

    try:
        tmp_path = STATE_PATH + ".tmp"

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

        os.replace(tmp_path, STATE_PATH)

    except Exception as e:
        print("State save error:", e)


def load_state():
    global SENT_SIGNALS
    global SYMBOL_COOLDOWN
    global ACTIVE_SIGNALS
    global BLOCKED_SYMBOLS
    global SIDE_DISABLED_UNTIL
    global STATS

    if not os.path.exists(STATE_PATH):
        return

    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)

        SENT_SIGNALS = state.get("SENT_SIGNALS", {})
        SYMBOL_COOLDOWN = state.get("SYMBOL_COOLDOWN", {})
        ACTIVE_SIGNALS = state.get("ACTIVE_SIGNALS", {})
        BLOCKED_SYMBOLS = state.get("BLOCKED_SYMBOLS", {})
        SIDE_DISABLED_UNTIL = state.get("SIDE_DISABLED_UNTIL", SIDE_DISABLED_UNTIL)

        loaded_stats = state.get("STATS", {})
        for key, value in loaded_stats.items():
            STATS[key] = value

        cleanup_memory()

    except Exception as e:
        print("State load error:", e)


def cleanup_memory():
    now = time.time()

    for signal_id in list(SENT_SIGNALS.keys()):
        if now - float(SENT_SIGNALS.get(signal_id, 0)) > 86400 * 3:
            SENT_SIGNALS.pop(signal_id, None)

    for symbol in list(BLOCKED_SYMBOLS.keys()):
        if now > float(BLOCKED_SYMBOLS.get(symbol, 0)):
            BLOCKED_SYMBOLS.pop(symbol, None)

    for signal_id, signal in list(ACTIVE_SIGNALS.items()):
        if now - float(signal.get("created_at", now)) > 86400:
            ACTIVE_SIGNALS.pop(signal_id, None)


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

    try:
        async with session.post(url, json=payload, timeout=30) as resp:
            data = await resp.text()

            if resp.status != 200:
                print("Telegram error:", resp.status, data)

            return data

    except Exception as e:
        print("Telegram send error:", e)
        return None


async def fetch_json(session, url, params=None, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=30) as resp:
                if resp.status == 200:
                    return await resp.json()

                text = await resp.text()

                if resp.status in [429, 500, 502, 503, 504]:
                    print(f"Temporary HTTP error {resp.status}. Retry {attempt + 1}/{retries}")
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue

                print("HTTP error:", resp.status, text[:300])
                return None

        except Exception as e:
            print("Fetch error:", e)
            await asyncio.sleep(1.5 * (attempt + 1))

    return None


def is_normal_crypto_symbol(symbol):
    if not symbol.endswith("-USDT"):
        return False

    base = base_from_symbol(symbol)

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

    if USE_LIQUID_ONLY and base not in LIQUID_BASES:
        return False

    return True


async def get_symbols(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts"
    data = await fetch_json(session, url)

    if not data:
        return []

    symbols = []

    for item in data.get("data", []):
        symbol = item.get("symbol")

        if symbol and is_normal_crypto_symbol(symbol):
            symbols.append(symbol)

    random.shuffle(symbols)
    return symbols[:MAX_SYMBOLS]


def cache_ttl_for_interval(interval):
    if interval == "4h":
        return 60 * 20
    if interval == "1h":
        return 60 * 6
    if interval == "15m":
        return 45
    if interval == "5m":
        return 20
    if interval == "1m":
        return 8

    return 20


async def get_klines(session, symbol, interval, limit=260, use_cache=True):
    cache_key = (symbol, interval, limit)
    ttl = cache_ttl_for_interval(interval)

    if use_cache and cache_key in KLINE_CACHE:
        cached_at, cached_value = KLINE_CACHE[cache_key]

        if time.time() - cached_at <= ttl:
            return cached_value

    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    data = await fetch_json(session, url, params=params)

    if not data:
        return None

    raw = data.get("data", [])

    min_len = 220 if interval in ["15m", "1h", "4h"] else 60

    if not raw or len(raw) < min_len:
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

    if len(candles) < min_len:
        return None

    KLINE_CACHE[cache_key] = (time.time(), candles)
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


def close_position_in_candle(candle):
    high = candle["high"]
    low = candle["low"]
    close = candle["close"]

    if high == low:
        return 0.5

    return (close - low) / (high - low)


def recent_move_percent(candles, lookback=12):
    if len(candles) < lookback + 1:
        return 0

    old_price = candles[-lookback]["close"]
    new_price = candles[-1]["close"]

    if old_price == 0:
        return 0

    return ((new_price - old_price) / old_price) * 100


def price_move_percent(entry, target, side):
    if side == "LONG":
        return (target - entry) / entry * 100

    return (entry - target) / entry * 100


def make_tp_by_percent(entry, side, position_percent):
    price_move_needed = position_percent / LEVERAGE

    if side == "LONG":
        return entry * (1 + price_move_needed / 100)

    return entry * (1 - price_move_needed / 100)


def apply_min_max_sl(entry, sl, side):
    risk_price_percent = abs(entry - sl) / entry * 100
    risk_position_percent = risk_price_percent * LEVERAGE

    if risk_position_percent < MIN_RISK_POSITION_PERCENT:
        min_price_move = (MIN_RISK_POSITION_PERCENT / LEVERAGE) / 100

        if side == "LONG":
            sl = entry * (1 - min_price_move)
        else:
            sl = entry * (1 + min_price_move)

        risk_price_percent = abs(entry - sl) / entry * 100
        risk_position_percent = risk_price_percent * LEVERAGE

    if risk_position_percent > MAX_RISK_POSITION_PERCENT:
        return None, None

    return sl, risk_position_percent


def trend_ema_50_200(candles):
    closed = candles[:-1] if len(candles) > 220 else candles
    closes = [c["close"] for c in closed]

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
    closed = btc_1h[:-1] if len(btc_1h) > 220 else btc_1h
    closes = [c["close"] for c in closed]

    ema50 = ema(closes, 50)[-1]
    ema200 = ema(closes, 200)[-1]
    price = closes[-1]

    prev = closes[-6] if len(closes) >= 6 else closes[-2]
    change = ((price - prev) / prev) * 100

    atr = calculate_atr(closed, 14)
    atr_percent = atr / price * 100 if atr and price else 0

    if atr_percent > BTC_DANGEROUS_ATR_PERCENT:
        return "DANGEROUS", change

    if price > ema50 > ema200 and change > 0.10:
        return "BULLISH", change

    if price < ema50 < ema200 and change < -0.10:
        return "BEARISH", change

    return "NEUTRAL", change


def is_swing_high(candles, i, left=3, right=3):
    high = candles[i]["high"]

    for j in range(1, left + 1):
        if candles[i - j]["high"] >= high:
            return False

    for j in range(1, right + 1):
        if candles[i + j]["high"] >= high:
            return False

    return True


def is_swing_low(candles, i, left=3, right=3):
    low = candles[i]["low"]

    for j in range(1, left + 1):
        if candles[i - j]["low"] <= low:
            return False

    for j in range(1, right + 1):
        if candles[i + j]["low"] <= low:
            return False

    return True


def collect_swing_levels(candles_1h, candles_4h):
    highs = []
    lows = []

    for candles, weight in [(candles_1h[:-1][-160:], 1), (candles_4h[:-1][-100:], 2)]:
        if len(candles) < 20:
            continue

        for i in range(4, len(candles) - 4):
            if is_swing_high(candles, i):
                highs.append({
                    "price": candles[i]["high"],
                    "weight": weight,
                    "time": candles[i]["time"],
                })

            if is_swing_low(candles, i):
                lows.append({
                    "price": candles[i]["low"],
                    "weight": weight,
                    "time": candles[i]["time"],
                })

    return highs, lows


def cluster_levels(levels, price, zone_percent=LEVEL_CLUSTER_ZONE_PERCENT):
    clusters = []

    for level in levels:
        level_price = level["price"]
        placed = False

        for cluster in clusters:
            distance = abs(level_price - cluster["price"]) / price * 100

            if distance <= zone_percent:
                total_weight = cluster["weight"] + level["weight"]

                cluster["price"] = (
                    cluster["price"] * cluster["weight"] + level_price * level["weight"]
                ) / total_weight

                cluster["weight"] = total_weight
                cluster["touches"] += 1
                cluster["last_time"] = max(cluster.get("last_time", 0), level.get("time", 0))

                placed = True
                break

        if not placed:
            clusters.append({
                "price": level_price,
                "weight": level["weight"],
                "touches": 1,
                "last_time": level.get("time", 0),
            })

    return clusters


def nearest_resistance(price, candles_1h, candles_4h):
    highs, _ = collect_swing_levels(candles_1h, candles_4h)
    clusters = cluster_levels(highs, price)

    candidates = []

    for cluster in clusters:
        level = cluster["price"]
        distance = abs(price - level) / price * 100

        if (
            distance <= LEVEL_DISTANCE_PERCENT
            and level >= price * 0.995
            and cluster["touches"] >= MIN_LEVEL_TOUCHES
        ):
            candidates.append(cluster)

    if not candidates:
        return None

    best = max(candidates, key=lambda x: (x["touches"], x["weight"]))
    return best["price"]


def nearest_support(price, candles_1h, candles_4h):
    _, lows = collect_swing_levels(candles_1h, candles_4h)
    clusters = cluster_levels(lows, price)

    candidates = []

    for cluster in clusters:
        level = cluster["price"]
        distance = abs(price - level) / price * 100

        if (
            distance <= LEVEL_DISTANCE_PERCENT
            and level <= price * 1.005
            and cluster["touches"] >= MIN_LEVEL_TOUCHES
        ):
            candidates.append(cluster)

    if not candidates:
        return None

    best = max(candidates, key=lambda x: (x["touches"], x["weight"]))
    return best["price"]


def is_side_enabled(side):
    if side == "LONG" and not ENABLE_LONG:
        return False

    if side == "SHORT" and not ENABLE_SHORT:
        return False

    disabled_until = float(SIDE_DISABLED_UNTIL.get(side, 0))

    if time.time() < disabled_until:
        return False

    return True


def is_symbol_blocked(symbol):
    until = float(BLOCKED_SYMBOLS.get(symbol, 0))

    if not until:
        return False

    if time.time() > until:
        BLOCKED_SYMBOLS.pop(symbol, None)
        save_state()
        return False

    return True


def is_on_cooldown(symbol):
    last_time = SYMBOL_COOLDOWN.get(symbol)

    if not last_time:
        return False

    return time.time() - float(last_time) < SIGNAL_COOLDOWN_SECONDS


def set_cooldown(symbol):
    SYMBOL_COOLDOWN[symbol] = time.time()
    save_state()


def side_stats(side):
    prefix = prefix_for_side(side)

    positive = STATS[f"{prefix}_positive"]
    negative = STATS[f"{prefix}_sl"]
    total = positive + negative
    winrate = (positive / total * 100) if total > 0 else 0

    return positive, negative, total, winrate


def apply_adaptive_rules(signal, result):
    side = signal["side"]
    symbol = signal["symbol"]
    prefix = prefix_for_side(side)

    notes = []

    if result == "SL" and not signal.get("counted_sl"):
        signal["counted_sl"] = True

        STATS[f"{prefix}_sl"] += 1
        STATS[f"{prefix}_consecutive_sl"] += 1
        STATS["pair_sl"][symbol] = STATS["pair_sl"].get(symbol, 0) + 1

        if STATS["pair_sl"][symbol] >= PAIR_MAX_SL_BEFORE_BLOCK:
            BLOCKED_SYMBOLS[symbol] = time.time() + 86400
            notes.append(f"🚫 Пара {symbol.replace('-', '/')} заблокирована на 24ч после SL.")

        if STATS[f"{prefix}_consecutive_sl"] >= SIDE_MAX_CONSECUTIVE_SL:
            SIDE_DISABLED_UNTIL[side] = time.time() + SIDE_DISABLE_SECONDS
            notes.append(f"⛔ {side} отключён на 6 часов: {STATS[f'{prefix}_consecutive_sl']} SL подряд.")

    elif result in ["TP1", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2", "TP2", "TP3"]:
        STATS[f"{prefix}_consecutive_sl"] = 0

        if not signal.get("counted_positive"):
            signal["counted_positive"] = True
            STATS[f"{prefix}_positive"] += 1
            STATS["pair_positive"][symbol] = STATS["pair_positive"].get(symbol, 0) + 1

        if result == "TP1":
            STATS[f"{prefix}_tp1"] += 1
        elif result == "TP2":
            STATS[f"{prefix}_tp2"] += 1
        elif result == "TP3":
            STATS[f"{prefix}_tp3"] += 1

    positive, negative, total, winrate = side_stats(side)

    if total >= MIN_CLOSED_TRADES_FOR_SIDE_CHECK and winrate < MIN_SIDE_WINRATE:
        SIDE_DISABLED_UNTIL[side] = time.time() + SIDE_DISABLE_SECONDS
        notes.append(f"⛔ {side} отключён на 6 часов: winrate {winrate:.1f}% после {total} сделок.")

    save_state()
    return notes


def momentum_confirm(candles_1m, candles_5m, side):
    c1 = candles_1m[:-1]
    c5 = candles_5m[:-1]

    closes_1m = [c["close"] for c in c1]
    closes_5m = [c["close"] for c in c5]

    if len(closes_1m) < 20 or len(closes_5m) < 20:
        return False

    ema9_1m = ema(closes_1m, 9)[-1]
    ema9_5m = ema(closes_5m, 9)[-1]

    last_1m = c1[-1]
    prev_1m = c1[-2]
    last_5m = c5[-1]
    prev_5m = c5[-2]

    if side == "LONG":
        return (
            last_1m["close"] > last_1m["open"]
            and last_1m["close"] > prev_1m["high"]
            and last_1m["close"] > ema9_1m
            and last_5m["close"] > last_5m["open"]
            and last_5m["close"] > prev_5m["close"]
            and last_5m["close"] > ema9_5m
        )

    return (
        last_1m["close"] < last_1m["open"]
        and last_1m["close"] < prev_1m["low"]
        and last_1m["close"] < ema9_1m
        and last_5m["close"] < last_5m["open"]
        and last_5m["close"] < prev_5m["close"]
        and last_5m["close"] < ema9_5m
    )


def quick_prefilter(candles_15m):
    if candles_15m is None or len(candles_15m) < 220:
        return False

    closed = candles_15m[:-1]
    last = closed[-1]
    volumes = [c["volume"] for c in closed]

    if len(volumes) < 32:
        return False

    avg_volume = sum(volumes[-31:-1]) / 30

    if avg_volume <= 0:
        return False

    volume_ratio = last["volume"] / avg_volume

    if volume_ratio < MIN_VOLUME_RATIO:
        return False

    atr = calculate_atr(closed, 14)

    if atr is None:
        return False

    body = candle_body(last)

    if body < atr * 0.25:
        return False

    return True


def build_breakout_signal(
    symbol,
    side,
    candles_15m,
    candles_1m,
    candles_5m,
    candles_1h,
    candles_4h,
    btc_status,
):
    if not is_side_enabled(side):
        return None

    if is_symbol_blocked(symbol):
        return None

    if is_on_cooldown(symbol):
        return None

    if btc_status == "DANGEROUS":
        return None

    closed_15m = candles_15m[:-1]

    if len(closed_15m) < 220:
        return None

    closes = [c["close"] for c in closed_15m]
    highs = [c["high"] for c in closed_15m]
    lows = [c["low"] for c in closed_15m]
    volumes = [c["volume"] for c in closed_15m]

    last = closed_15m[-1]
    prev = closed_15m[-2]
    price = last["close"]

    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(closed_15m, 14)
    vwap = calculate_vwap_like(closed_15m, 48)

    if rsi is None or atr is None or vwap is None:
        return None

    body = candle_body(last)

    if body < atr * MIN_BODY_ATR_MULTIPLIER:
        return None

    if body > atr * MAX_BODY_ATR_MULTIPLIER:
        return None

    trend_1h = trend_ema_50_200(candles_1h)
    trend_4h = trend_ema_50_200(candles_4h)

    if len(volumes) < 32:
        return None

    avg_volume = sum(volumes[-31:-1]) / 30
    volume_ratio = last["volume"] / avg_volume if avg_volume > 0 else 0

    if volume_ratio < MIN_VOLUME_RATIO:
        return None

    if not momentum_confirm(candles_1m, candles_5m, side):
        return None

    close_pos = close_position_in_candle(last)

    if side == "LONG":
        if not ENABLE_LONG:
            return None

        if btc_status == "BEARISH":
            return None

        if trend_1h in ["BEARISH", "SOFT_BEARISH"]:
            return None

        recent_move = recent_move_percent(closed_15m, 12)

        if recent_move < MIN_PREVIOUS_RISE_FOR_LONG:
            return None

        resistance = nearest_resistance(prev["close"], candles_1h, candles_4h)

        if resistance is None:
            return None

        breakout_close = price > resistance * (1 + BREAKOUT_CLOSE_PERCENT / 100)
        prev_under_level = prev["close"] <= resistance * 1.002

        if not breakout_close or not prev_under_level:
            return None

        if close_pos < LONG_MIN_CLOSE_POSITION:
            return None

        if price < vwap:
            return None

        if rsi < 55 or rsi > 78:
            return None

        chase_position_percent = ((price - resistance) / resistance * 100) * LEVERAGE

        if chase_position_percent > MAX_BREAKOUT_CHASE_POSITION_PERCENT:
            return None

        sl = min(resistance - atr * 0.12, min(lows[-6:]) - atr * 0.04)

        if sl >= price:
            return None

        sl, risk_position_percent = apply_min_max_sl(price, sl, "LONG")

        if sl is None:
            return None

        tp1 = make_tp_by_percent(price, "LONG", TP1_POSITION_PERCENT)
        tp2 = make_tp_by_percent(price, "LONG", TP2_POSITION_PERCENT)
        tp3 = make_tp_by_percent(price, "LONG", TP3_POSITION_PERCENT)

        checks = [
            breakout_close,
            prev_under_level,
            volume_ratio >= MIN_VOLUME_RATIO,
            volume_ratio >= A_PLUS_VOLUME_RATIO,
            btc_status != "BEARISH",
            trend_1h not in ["BEARISH", "SOFT_BEARISH"],
            trend_4h != "BEARISH",
            price > vwap,
            rsi >= 55 and rsi <= 78,
            chase_position_percent <= MAX_BREAKOUT_CHASE_POSITION_PERCENT,
            close_pos >= LONG_MIN_CLOSE_POSITION,
            body >= atr * MIN_BODY_ATR_MULTIPLIER,
            body <= atr * MAX_BODY_ATR_MULTIPLIER,
        ]

    else:
        if not ENABLE_SHORT:
            return None

        if btc_status == "BULLISH":
            return None

        if trend_1h in ["BULLISH", "SOFT_BULLISH"]:
            return None

        recent_move = recent_move_percent(closed_15m, 12)

        if recent_move > -MIN_PREVIOUS_DROP_FOR_SHORT:
            return None

        support = nearest_support(prev["close"], candles_1h, candles_4h)

        if support is None:
            return None

        breakout_close = price < support * (1 - BREAKOUT_CLOSE_PERCENT / 100)
        prev_above_level = prev["close"] >= support * 0.998

        if not breakout_close or not prev_above_level:
            return None

        if close_pos > SHORT_MAX_CLOSE_POSITION:
            return None

        if price > vwap:
            return None

        if rsi > 45 or rsi < 22:
            return None

        chase_position_percent = ((support - price) / support * 100) * LEVERAGE

        if chase_position_percent > MAX_BREAKOUT_CHASE_POSITION_PERCENT:
            return None

        sl = max(support + atr * 0.12, max(highs[-6:]) + atr * 0.04)

        if sl <= price:
            return None

        sl, risk_position_percent = apply_min_max_sl(price, sl, "SHORT")

        if sl is None:
            return None

        tp1 = make_tp_by_percent(price, "SHORT", TP1_POSITION_PERCENT)
        tp2 = make_tp_by_percent(price, "SHORT", TP2_POSITION_PERCENT)
        tp3 = make_tp_by_percent(price, "SHORT", TP3_POSITION_PERCENT)

        checks = [
            breakout_close,
            prev_above_level,
            volume_ratio >= MIN_VOLUME_RATIO,
            volume_ratio >= A_PLUS_VOLUME_RATIO,
            btc_status != "BULLISH",
            trend_1h not in ["BULLISH", "SOFT_BULLISH"],
            trend_4h != "BULLISH",
            price < vwap,
            rsi <= 45 and rsi >= 22,
            chase_position_percent <= MAX_BREAKOUT_CHASE_POSITION_PERCENT,
            close_pos <= SHORT_MAX_CLOSE_POSITION,
            body >= atr * MIN_BODY_ATR_MULTIPLIER,
            body <= atr * MAX_BODY_ATR_MULTIPLIER,
        ]

    reward_price_percent = price_move_percent(price, tp1, side)
    risk_price_percent = abs(price - sl) / price * 100
    rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

    if rr < MIN_RR_TO_TP1:
        return None

    passed = sum(1 for ok in checks if ok)

    setup_score = min(95, 58 + passed * 3)

    if volume_ratio >= 2.0:
        setup_score += 2

    if rr >= 1.15:
        setup_score += 1

    setup_score = min(95, setup_score)

    if setup_score < MIN_QUALITY:
        return None

    signal_id = f"{symbol}:V23_BREAKOUT_MOMENTUM:{side}:{round(price, 8)}:{last['time']}"

    if signal_id in SENT_SIGNALS:
        return None

    return {
        "symbol": symbol,
        "side": side,
        "setup_score": setup_score,
        "entry": price,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr": rr,
        "volume_ratio": volume_ratio,
        "risk_position_percent": risk_position_percent,
        "signal_id": signal_id,
        "created_at": time.time(),
        "created_at_ms": now_ms(),
        "last_checked_ms": now_ms(),
        "status": "ACTIVE",
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "counted_positive": False,
        "counted_sl": False,
        "btc_status": btc_status,
        "trend_1h": trend_1h,
        "trend_4h": trend_4h,
        "rsi": rsi,
    }


def analyze_symbol(symbol, candles_15m, candles_1m, candles_5m, candles_1h, candles_4h, btc_status):
    candidates = []

    long_signal = build_breakout_signal(
        symbol,
        "LONG",
        candles_15m,
        candles_1m,
        candles_5m,
        candles_1h,
        candles_4h,
        btc_status,
    )

    if long_signal:
        candidates.append(long_signal)

    short_signal = build_breakout_signal(
        symbol,
        "SHORT",
        candles_15m,
        candles_1m,
        candles_5m,
        candles_1h,
        candles_4h,
        btc_status,
    )

    if short_signal:
        candidates.append(short_signal)

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x["setup_score"], x["rr"], x["volume_ratio"]), reverse=True)
    return candidates[0]


def make_signal_message(signal):
    arrow = "📈" if signal["side"] == "LONG" else "📉"
    mode_text = "TEST SIGNAL" if TEST_MODE else "TRADE SIGNAL"

    return f"""
🚀 <b>V23 Breakout Momentum Scalper</b> · <b>{mode_text}</b>
🔥 <b>A+ CLOSED BREAKOUT SIGNAL</b>

{arrow} <b>{signal["side"]} {signal["symbol"].replace("-", "/")}</b> · {TIMEFRAME}
Setup score: <b>{signal["setup_score"]}/100</b>

🎯 Вход: <code>{format_price(signal["entry"])}</code>
🛑 SL: <code>{format_price(signal["sl"])}</code>
✅ TP1: <code>{format_price(signal["tp1"])}</code>
✅ TP2: <code>{format_price(signal["tp2"])}</code>
✅ TP3: <code>{format_price(signal["tp3"])}</code>

⚠️ Плечо max.: <b>{LEVERAGE}x</b>
🛡 Риск до SL: около <b>{signal["risk_position_percent"]:.1f}%</b> по позиции
📊 RR до TP1: <b>{signal["rr"]:.2f}</b>
📊 Volume spike: x<b>{signal["volume_ratio"]:.2f}</b>
📉 RSI: <b>{signal["rsi"]:.1f}</b>

🌍 BTC режим: <b>{signal["btc_status"]}</b>
📈 Trend 1h: <b>{signal["trend_1h"]}</b>
🧭 Trend 4h: <b>{signal["trend_4h"]}</b>

Логика V23:
• Только закрытые свечи
• Swing-level support/resistance
• Закрытый пробой уровня
• 1m + 5m подтверждают импульс
• Volume spike выше среднего
• Фильтр против fake breakout
• BTC dangerous market filter
• TP1 быстрый: {TP1_POSITION_PERCENT:.0f}% по позиции

Пара блокируется после {PAIR_MAX_SL_BEFORE_BLOCK} SL.
Направление отключается после {SIDE_MAX_CONSECUTIVE_SL} SL подряд.

⚠️ Не финансовый совет. Сначала TEST/минимальная сумма.
""".strip()


def make_result_message(signal, result, price):
    side = signal["side"]
    symbol = signal["symbol"].replace("-", "/")

    notes = apply_adaptive_rules(signal, result)

    if result == "SL":
        icon = "❌"
        title = "SL сработал до TP1"
    elif result == "TP1":
        icon = "✅"
        title = "TP1 достигнут — сделка позитивная"
    elif result == "TP2":
        icon = "✅✅"
        title = "TP2 достигнут"
    elif result == "TP3":
        icon = "🔥"
        title = "TP3 достигнут"
    elif result == "PROFIT_AFTER_TP1":
        icon = "🟢"
        title = "Возврат после TP1 — сделка позитивная"
    elif result == "PROFIT_AFTER_TP2":
        icon = "🟢🟢"
        title = "Возврат после TP2 — сделка позитивная"
    else:
        icon = "ℹ️"
        title = result

    long_pos, long_neg, long_total, long_wr = side_stats("LONG")
    short_pos, short_neg, short_total, short_wr = side_stats("SHORT")

    adaptive_text = ""

    if notes:
        adaptive_text = "\n\n<b>Адаптация:</b>\n" + "\n".join(notes)

    return f"""
{icon} <b>{title}</b>

<b>{side} {symbol}</b>
Вход: <code>{format_price(signal["entry"])}</code>
Текущая/уровень: <code>{format_price(price)}</code>

TP1: <code>{format_price(signal["tp1"])}</code>
TP2: <code>{format_price(signal["tp2"])}</code>
TP3: <code>{format_price(signal["tp3"])}</code>
SL: <code>{format_price(signal["sl"])}</code>

📊 <b>Статистика:</b>

📈 LONG:
Позитивные: <b>{long_pos}</b>
SL до TP1: <b>{long_neg}</b>
Winrate: <b>{long_wr:.1f}%</b>

📉 SHORT:
Позитивные: <b>{short_pos}</b>
SL до TP1: <b>{short_neg}</b>
Winrate: <b>{short_wr:.1f}%</b>

🚫 Заблокировано пар: <b>{len(BLOCKED_SYMBOLS)}</b>
Активных сигналов: <b>{len(ACTIVE_SIGNALS)}</b>
{adaptive_text}
""".strip()


def make_start_message():
    return (
        f"✅ V23 Breakout Momentum Scalper Bot запущен.\n"
        f"Режим: {'TEST' if TEST_MODE else 'TRADE'}\n"
        f"Логика: закрытые пробои + swing-levels + momentum + volume + BTC filter.\n"
        f"LONG: пробой сопротивления вверх.\n"
        f"SHORT: пробой поддержки вниз.\n"
        f"Фильтры: 15m + 5m + 1m + 1h + 4h + BTC + VWAP + RSI + Volume.\n"
        f"Volume filter: x{MIN_VOLUME_RATIO}\n"
        f"A+ Volume: x{A_PLUS_VOLUME_RATIO}+\n"
        f"TP1: {TP1_POSITION_PERCENT:.0f}% по позиции.\n"
        f"Risk до SL: {MIN_RISK_POSITION_PERCENT:.1f}-{MAX_RISK_POSITION_PERCENT:.1f}% по позиции.\n"
        f"Пара блокируется после {PAIR_MAX_SL_BEFORE_BLOCK} SL.\n"
        f"Направление отключается после {SIDE_MAX_CONSECUTIVE_SL} SL подряд.\n"
        f"Плечо max.: {LEVERAGE}x.\n"
        f"SQLite: {DB_PATH}\n"
        f"State: {STATE_PATH}"
    )


def check_hit(signal, candles):
    side = signal["side"]
    new_candles = []

    for c in candles:
        if c["time"] > signal.get("last_checked_ms", signal["created_at_ms"]):
            new_candles.append(c)

    if not new_candles:
        return None, candles[-1]["close"]

    for c in new_candles:
        high = c["high"]
        low = c["low"]

        signal["last_checked_ms"] = c["time"]

        if side == "LONG":
            if signal["tp2_hit"] and low <= signal["entry"]:
                return "PROFIT_AFTER_TP2", signal["entry"]

            if signal["tp1_hit"] and low <= signal["entry"]:
                return "PROFIT_AFTER_TP1", signal["entry"]

            if not signal["tp1_hit"] and low <= signal["sl"]:
                return "SL", signal["sl"]

            if not signal["tp1_hit"] and high >= signal["tp1"]:
                signal["tp1_hit"] = True
                return "TP1", signal["tp1"]

            if signal["tp1_hit"] and not signal["tp2_hit"] and high >= signal["tp2"]:
                signal["tp2_hit"] = True
                return "TP2", signal["tp2"]

            if signal["tp2_hit"] and not signal["tp3_hit"] and high >= signal["tp3"]:
                signal["tp3_hit"] = True
                return "TP3", signal["tp3"]

        else:
            if signal["tp2_hit"] and high >= signal["entry"]:
                return "PROFIT_AFTER_TP2", signal["entry"]

            if signal["tp1_hit"] and high >= signal["entry"]:
                return "PROFIT_AFTER_TP1", signal["entry"]

            if not signal["tp1_hit"] and high >= signal["sl"]:
                return "SL", signal["sl"]

            if not signal["tp1_hit"] and low <= signal["tp1"]:
                signal["tp1_hit"] = True
                return "TP1", signal["tp1"]

            if signal["tp1_hit"] and not signal["tp2_hit"] and low <= signal["tp2"]:
                signal["tp2_hit"] = True
                return "TP2", signal["tp2"]

            if signal["tp2_hit"] and not signal["tp3_hit"] and low <= signal["tp3"]:
                signal["tp3_hit"] = True
                return "TP3", signal["tp3"]

    last_price = new_candles[-1]["close"]
    return None, last_price


async def track_active_signals(session):
    if not ACTIVE_SIGNALS:
        return

    finished = []

    for signal_id, signal in list(ACTIVE_SIGNALS.items()):
        try:
            candles = await get_klines(
                session,
                signal["symbol"],
                interval="1m",
                limit=120,
                use_cache=False,
            )

            if candles is None:
                continue

            result, price = check_hit(signal, candles)

            if not result:
                continue

            log_result(signal, result, price)

            await send_telegram_message(
                session,
                make_result_message(signal, result, price),
                button_url=bingx_url(signal["symbol"]),
            )

            if result in ["SL", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
                finished.append(signal_id)

            await asyncio.sleep(1)

        except Exception as e:
            print("Tracker error:", e)

    for signal_id in finished:
        ACTIVE_SIGNALS.pop(signal_id, None)

    if finished:
        save_state()


async def scan_loop():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")

    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID не задан")

    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector) as session:
        await send_telegram_message(session, make_start_message())

        last_track_time = 0

        while True:
            reset_daily_stats_if_needed()
            cleanup_memory()

            try:
                if time.time() - last_track_time >= TRACK_INTERVAL_SECONDS:
                    await track_active_signals(session)
                    last_track_time = time.time()

                print("Scanning...", now_text())

                btc_1h = await get_klines(
                    session,
                    BTC_SYMBOL,
                    interval=TREND_TIMEFRAME,
                    limit=260,
                )

                if not btc_1h:
                    print("BTC data unavailable")
                    await asyncio.sleep(60)
                    continue

                btc_status, btc_change = btc_market_filter(btc_1h)
                print(f"BTC status: {btc_status}, change: {btc_change:.2f}%")

                symbols = await get_symbols(session)

                if not symbols:
                    print("No symbols available")
                    await asyncio.sleep(60)
                    continue

                checked = 0
                prefiltered = 0
                found = 0

                for symbol in symbols:
                    reset_daily_stats_if_needed()

                    if STATS["signals_today"] >= DAILY_MAX_SIGNALS:
                        break

                    if is_symbol_blocked(symbol):
                        continue

                    checked += 1

                    try:
                        candles_15m = await get_klines(
                            session,
                            symbol,
                            interval=TIMEFRAME,
                            limit=260,
                        )

                        if not quick_prefilter(candles_15m):
                            continue

                        prefiltered += 1

                        candles_1m = await get_klines(
                            session,
                            symbol,
                            interval=CONFIRM_TIMEFRAME,
                            limit=120,
                        )

                        candles_5m = await get_klines(
                            session,
                            symbol,
                            interval=CONFIRM_5M_TIMEFRAME,
                            limit=160,
                        )

                        candles_1h = await get_klines(
                            session,
                            symbol,
                            interval=TREND_TIMEFRAME,
                            limit=260,
                        )

                        candles_4h = await get_klines(
                            session,
                            symbol,
                            interval=MACRO_TIMEFRAME,
                            limit=260,
                        )

                        if (
                            candles_15m is None
                            or candles_1m is None
                            or candles_5m is None
                            or candles_1h is None
                            or candles_4h is None
                        ):
                            continue

                        signal = analyze_symbol(
                            symbol,
                            candles_15m,
                            candles_1m,
                            candles_5m,
                            candles_1h,
                            candles_4h,
                            btc_status,
                        )

                        if not signal:
                            continue

                        await send_telegram_message(
                            session,
                            make_signal_message(signal),
                            button_url=bingx_url(symbol),
                        )

                        SENT_SIGNALS[signal["signal_id"]] = time.time()
                        ACTIVE_SIGNALS[signal["signal_id"]] = signal

                        set_cooldown(symbol)
                        log_signal(signal)

                        found += 1
                        STATS["signals_total"] += 1
                        STATS["signals_today"] += 1

                        save_state()

                        await asyncio.sleep(2)

                        if found >= MAX_SIGNALS_PER_SCAN:
                            break

                    except Exception as e:
                        print(f"Error with {symbol}:", e)

                long_pos, long_neg, long_total, long_wr = side_stats("LONG")
                short_pos, short_neg, short_total, short_wr = side_stats("SHORT")

                print(
                    f"Scan finished. Checked: {checked}. "
                    f"Prefiltered: {prefiltered}. "
                    f"Signals found: {found}. Active: {len(ACTIVE_SIGNALS)}. "
                    f"LONG WR: {long_wr:.1f}% | SHORT WR: {short_wr:.1f}% | "
                    f"Blocked: {len(BLOCKED_SYMBOLS)}"
                )

                save_state()
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

            except Exception as e:
                print("Main loop error:", e)
                save_state()
                await asyncio.sleep(30)


if __name__ == "__main__":
    init_db()
    load_state()
    asyncio.run(scan_loop())
