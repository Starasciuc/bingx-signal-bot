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
CONFIRM_1M = os.getenv("CONFIRM_1M", "1m")
CONFIRM_5M = os.getenv("CONFIRM_5M", "5m")
TREND_TIMEFRAME = os.getenv("TREND_TIMEFRAME", "1h")
MACRO_TIMEFRAME = os.getenv("MACRO_TIMEFRAME", "4h")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "35"))
TRACK_INTERVAL_SECONDS = int(os.getenv("TRACK_INTERVAL_SECONDS", "25"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "450"))

LEVERAGE = int(os.getenv("LEVERAGE", "10"))
TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"

ENABLE_LONG = os.getenv("ENABLE_LONG", "true").lower() == "true"
ENABLE_SHORT = os.getenv("ENABLE_SHORT", "true").lower() == "true"

USE_LIQUID_ONLY = os.getenv("USE_LIQUID_ONLY", "true").lower() == "true"

MIN_QUALITY = int(os.getenv("MIN_QUALITY", "87"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.35"))
A_PLUS_VOLUME_RATIO = float(os.getenv("A_PLUS_VOLUME_RATIO", "1.50"))

TP1_POSITION_PERCENT = float(os.getenv("TP1_POSITION_PERCENT", "8"))
TP2_POSITION_PERCENT = float(os.getenv("TP2_POSITION_PERCENT", "15"))
TP3_POSITION_PERCENT = float(os.getenv("TP3_POSITION_PERCENT", "25"))

MIN_RISK_POSITION_PERCENT = float(os.getenv("MIN_RISK_POSITION_PERCENT", "6"))
MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "10"))
MIN_RR_TO_TP1 = float(os.getenv("MIN_RR_TO_TP1", "0.85"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "5400"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "3"))
DAILY_MAX_SIGNALS = int(os.getenv("DAILY_MAX_SIGNALS", "10"))

PAIR_MAX_SL_BEFORE_BLOCK = int(os.getenv("PAIR_MAX_SL_BEFORE_BLOCK", "1"))
SIDE_MAX_CONSECUTIVE_SL = int(os.getenv("SIDE_MAX_CONSECUTIVE_SL", "2"))
STRATEGY_MAX_CONSECUTIVE_SL = int(os.getenv("STRATEGY_MAX_CONSECUTIVE_SL", "2"))

DISABLE_SECONDS = int(os.getenv("DISABLE_SECONDS", "21600"))

MIN_CLOSED_TRADES_FOR_CHECK = int(os.getenv("MIN_CLOSED_TRADES_FOR_CHECK", "8"))
MIN_WINRATE = float(os.getenv("MIN_WINRATE", "50"))

BTC_SYMBOL = "BTC-USDT"

LIQUID_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "INJ", "NEAR", "ARB", "OP", "APT", "SUI", "SEI", "DOT", "LTC",
    "BCH", "UNI", "AAVE", "FIL", "ATOM", "ETC", "TRX", "MATIC", "WLD",
    "TIA", "ORDI", "FTM", "RUNE", "ENA", "JUP", "PYTH", "STRK", "DYDX",
    "TON", "COMP", "STX", "TRB", "JTO", "DYM", "ICP"
}

STRATEGIES = [
    "BREAKOUT_MOMENTUM",
    "SWEEP_RETEST",
    "REVERSAL_BOUNCE",
]

SENT_SIGNALS = {}
SYMBOL_COOLDOWN = {}
ACTIVE_SIGNALS = {}

BLOCKED_SYMBOLS = {}
SIDE_DISABLED_UNTIL = {"LONG": 0, "SHORT": 0}
STRATEGY_DISABLED_UNTIL = {s: 0 for s in STRATEGIES}

STATS = {
    "signals_today": 0,
    "signals_total": 0,
    "current_day": datetime.now(timezone.utc).strftime("%Y-%m-%d"),

    "pair_sl": {},
    "pair_positive": {},

    "side": {
        "LONG": {"positive": 0, "sl": 0, "consecutive_sl": 0},
        "SHORT": {"positive": 0, "sl": 0, "consecutive_sl": 0},
    },

    "strategy": {
        s: {"positive": 0, "sl": 0, "consecutive_sl": 0}
        for s in STRATEGIES
    }
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
        return f"{x:.3f}"
    if x >= 1:
        return f"{x:.5f}"
    return f"{x:.8f}"


def base_from_symbol(symbol):
    return symbol.replace("-USDT", "").upper()


def is_strategy_enabled(strategy):
    return time.time() >= STRATEGY_DISABLED_UNTIL.get(strategy, 0)


def is_side_enabled(side):
    if side == "LONG" and not ENABLE_LONG:
        return False

    if side == "SHORT" and not ENABLE_SHORT:
        return False

    return time.time() >= SIDE_DISABLED_UNTIL.get(side, 0)


def is_symbol_blocked(symbol):
    until = BLOCKED_SYMBOLS.get(symbol, 0)

    if not until:
        return False

    if time.time() > until:
        BLOCKED_SYMBOLS.pop(symbol, None)
        return False

    return True


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
        return "BULLISH"

    if price < ema50 < ema200 and change < -0.10:
        return "BEARISH"

    return "NEUTRAL"


def recent_move_percent(candles, lookback=12):
    if len(candles) < lookback + 1:
        return 0

    old_price = candles[-lookback]["close"]
    new_price = candles[-1]["close"]

    if old_price == 0:
        return 0

    return ((new_price - old_price) / old_price) * 100


def collect_levels(candles_1h, candles_4h):
    highs = []
    lows = []

    for c in candles_1h[-140:]:
        highs.append(c["high"])
        lows.append(c["low"])

    for c in candles_4h[-80:]:
        highs.append(c["high"])
        lows.append(c["low"])

    return highs, lows


def nearest_resistance(price, candles_1h, candles_4h, max_distance=1.25):
    highs, _ = collect_levels(candles_1h, candles_4h)

    levels = []

    for level in highs:
        distance = abs(price - level) / price * 100
        if distance <= max_distance and level >= price * 0.985:
            levels.append(level)

    if not levels:
        return None

    return min(levels, key=lambda x: abs(price - x))


def nearest_support(price, candles_1h, candles_4h, max_distance=1.25):
    _, lows = collect_levels(candles_1h, candles_4h)

    levels = []

    for level in lows:
        distance = abs(price - level) / price * 100
        if distance <= max_distance and level <= price * 1.015:
            levels.append(level)

    if not levels:
        return None

    return min(levels, key=lambda x: abs(price - x))


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


def momentum_confirm(candles_1m, candles_5m, side):
    closes_1m = [c["close"] for c in candles_1m]
    closes_5m = [c["close"] for c in candles_5m]

    if len(closes_1m) < 20 or len(closes_5m) < 20:
        return False

    ema9_1m = ema(closes_1m, 9)[-1]
    ema9_5m = ema(closes_5m, 9)[-1]

    last_1m = candles_1m[-1]
    prev_1m = candles_1m[-2]
    last_5m = candles_5m[-1]
    prev_5m = candles_5m[-2]

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


def side_stats(side):
    s = STATS["side"][side]
    total = s["positive"] + s["sl"]
    winrate = (s["positive"] / total * 100) if total else 0
    return s["positive"], s["sl"], total, winrate


def strategy_stats(strategy):
    s = STATS["strategy"][strategy]
    total = s["positive"] + s["sl"]
    winrate = (s["positive"] / total * 100) if total else 0
    return s["positive"], s["sl"], total, winrate


def build_signal_base(symbol, side, strategy, entry, sl, volume_ratio, quality, rr_bonus=0):
    sl, risk_position_percent = apply_min_max_sl(entry, sl, side)

    if sl is None:
        return None

    tp1 = make_tp_by_percent(entry, side, TP1_POSITION_PERCENT)
    tp2 = make_tp_by_percent(entry, side, TP2_POSITION_PERCENT)
    tp3 = make_tp_by_percent(entry, side, TP3_POSITION_PERCENT)

    reward_price_percent = price_move_percent(entry, tp1, side)
    risk_price_percent = abs(entry - sl) / entry * 100
    rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

    if rr < MIN_RR_TO_TP1:
        return None

    quality = min(95, quality + rr_bonus)

    if quality < MIN_QUALITY:
        return None

    signal_id = f"{symbol}:V23:{strategy}:{side}:{round(entry, 6)}"

    if signal_id in SENT_SIGNALS:
        return None

    return {
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "quality": quality,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr": rr,
        "volume_ratio": volume_ratio,
        "risk_position_percent": risk_position_percent,
        "signal_id": signal_id,
        "created_at": time.time(),
        "created_at_ms": int(time.time() * 1000),
        "last_checked_ms": int(time.time() * 1000),
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "counted_positive": False,
        "counted_sl": False,
    }


def build_breakout_strategy(symbol, side, candles_15m, candles_1m, candles_5m, candles_1h, candles_4h, btc_status):
    strategy = "BREAKOUT_MOMENTUM"

    if not is_strategy_enabled(strategy) or not is_side_enabled(side):
        return None

    closes = [c["close"] for c in candles_15m]
    highs = [c["high"] for c in candles_15m]
    lows = [c["low"] for c in candles_15m]
    volumes = [c["volume"] for c in candles_15m]

    last = candles_15m[-1]
    prev = candles_15m[-2]
    price = last["close"]

    rsi = calculate_rsi(closes)
    atr = calculate_atr(candles_15m)
    vwap = calculate_vwap_like(candles_15m)

    if rsi is None or atr is None or vwap is None:
        return None

    trend_1h = trend_ema_50_200(candles_1h)
    trend_4h = trend_ema_50_200(candles_4h)

    avg_volume = sum(volumes[-30:]) / 30
    volume_ratio = last["volume"] / avg_volume if avg_volume else 0

    if volume_ratio < A_PLUS_VOLUME_RATIO:
        return None

    if not momentum_confirm(candles_1m, candles_5m, side):
        return None

    if side == "LONG":
        if btc_status == "BEARISH":
            return None

        if trend_1h in ["BEARISH", "SOFT_BEARISH"]:
            return None

        if recent_move_percent(candles_15m, 12) < 0.7:
            return None

        resistance = nearest_resistance(prev["close"], candles_1h, candles_4h)

        if resistance is None:
            return None

        breakout = price > resistance * 1.0018 and prev["close"] <= resistance * 1.002

        if not breakout:
            return None

        if price < vwap:
            return None

        if rsi < 55 or rsi > 78:
            return None

        sl = min(resistance - atr * 0.12, min(lows[-6:]) - atr * 0.04)
        quality = 88

    else:
        if btc_status == "BULLISH":
            return None

        if trend_1h in ["BULLISH", "SOFT_BULLISH"]:
            return None

        if recent_move_percent(candles_15m, 12) > -0.7:
            return None

        support = nearest_support(prev["close"], candles_1h, candles_4h)

        if support is None:
            return None

        breakout = price < support * 0.9982 and prev["close"] >= support * 0.998

        if not breakout:
            return None

        if price > vwap:
            return None

        if rsi > 45 or rsi < 22:
            return None

        sl = max(support + atr * 0.12, max(highs[-6:]) + atr * 0.04)
        quality = 88

    if volume_ratio >= 2:
        quality += 2

    return build_signal_base(symbol, side, strategy, price, sl, volume_ratio, quality)


def build_sweep_retest_strategy(symbol, side, candles_15m, candles_1m, candles_5m, candles_1h, candles_4h, btc_status):
    strategy = "SWEEP_RETEST"

    if not is_strategy_enabled(strategy) or not is_side_enabled(side):
        return None

    closes = [c["close"] for c in candles_15m]
    highs = [c["high"] for c in candles_15m]
    lows = [c["low"] for c in candles_15m]
    volumes = [c["volume"] for c in candles_15m]

    last = candles_15m[-1]
    prev = candles_15m[-2]
    price = last["close"]

    rsi = calculate_rsi(closes)
    atr = calculate_atr(candles_15m)
    vwap = calculate_vwap_like(candles_15m)

    if rsi is None or atr is None or vwap is None:
        return None

    trend_1h = trend_ema_50_200(candles_1h)
    trend_4h = trend_ema_50_200(candles_4h)

    avg_volume = sum(volumes[-30:]) / 30
    volume_ratio = last["volume"] / avg_volume if avg_volume else 0

    if volume_ratio < A_PLUS_VOLUME_RATIO:
        return None

    if not momentum_confirm(candles_1m, candles_5m, side):
        return None

    if side == "LONG":
        if btc_status == "BEARISH":
            return None

        if trend_1h == "BEARISH":
            return None

        support = nearest_support(price, candles_1h, candles_4h)

        if support is None:
            return None

        swept = prev["low"] < support * 0.998
        reclaimed = prev["close"] > support
        retest_hold = last["low"] >= support * 0.996 and last["close"] > support

        if not swept or not reclaimed or not retest_hold:
            return None

        if rsi > 55:
            return None

        if price < vwap * 0.982:
            return None

        sl = min(prev["low"] - atr * 0.05, min(lows[-8:]) - atr * 0.04)
        quality = 89

    else:
        if btc_status == "BULLISH":
            return None

        if trend_1h in ["BULLISH", "SOFT_BULLISH"]:
            return None

        resistance = nearest_resistance(price, candles_1h, candles_4h)

        if resistance is None:
            return None

        swept = prev["high"] > resistance * 1.002
        reclaimed = prev["close"] < resistance
        retest_hold = last["high"] <= resistance * 1.004 and last["close"] < resistance

        if not swept or not reclaimed or not retest_hold:
            return None

        if rsi < 50:
            return None

        if price > vwap * 1.005:
            return None

        sl = max(prev["high"] + atr * 0.05, max(highs[-8:]) + atr * 0.04)
        quality = 89

    if volume_ratio >= 2:
        quality += 2

    return build_signal_base(symbol, side, strategy, price, sl, volume_ratio, quality)


def build_reversal_strategy(symbol, side, candles_15m, candles_1m, candles_5m, candles_1h, candles_4h, btc_status):
    strategy = "REVERSAL_BOUNCE"

    if not is_strategy_enabled(strategy) or not is_side_enabled(side):
        return None

    closes = [c["close"] for c in candles_15m]
    highs = [c["high"] for c in candles_15m]
    lows = [c["low"] for c in candles_15m]
    volumes = [c["volume"] for c in candles_15m]

    last = candles_15m[-1]
    prev = candles_15m[-2]
    price = last["close"]

    rsi = calculate_rsi(closes)
    atr = calculate_atr(candles_15m)
    vwap = calculate_vwap_like(candles_15m)

    if rsi is None or atr is None or vwap is None:
        return None

    trend_1h = trend_ema_50_200(candles_1h)

    avg_volume = sum(volumes[-30:]) / 30
    volume_ratio = last["volume"] / avg_volume if avg_volume else 0

    if volume_ratio < A_PLUS_VOLUME_RATIO:
        return None

    if not momentum_confirm(candles_1m, candles_5m, side):
        return None

    if side == "LONG":
        if btc_status == "BEARISH":
            return None

        if trend_1h == "BEARISH":
            return None

        if recent_move_percent(candles_15m, 12) > -1.5:
            return None

        support = nearest_support(price, candles_1h, candles_4h)

        if support is None:
            return None

        touch = last["low"] <= support * 1.004
        bounce = last["close"] > last["open"] and last["close"] > prev["close"]

        if not touch or not bounce:
            return None

        if rsi > 50:
            return None

        sl = min(support - atr * 0.10, min(lows[-8:]) - atr * 0.04)
        quality = 87

    else:
        if btc_status == "BULLISH":
            return None

        if trend_1h in ["BULLISH", "SOFT_BULLISH"]:
            return None

        if recent_move_percent(candles_15m, 12) < 1.5:
            return None

        resistance = nearest_resistance(price, candles_1h, candles_4h)

        if resistance is None:
            return None

        touch = last["high"] >= resistance * 0.996
        rejection = last["close"] < last["open"] and last["close"] < prev["close"]

        if not touch or not rejection:
            return None

        if rsi < 58:
            return None

        if price > vwap * 1.005:
            return None

        sl = max(resistance + atr * 0.10, max(highs[-8:]) + atr * 0.04)
        quality = 87

    if volume_ratio >= 2:
        quality += 2

    return build_signal_base(symbol, side, strategy, price, sl, volume_ratio, quality)


def analyze_symbol(symbol, candles_15m, candles_1m, candles_5m, candles_1h, candles_4h, btc_status):
    if is_symbol_blocked(symbol) or is_on_cooldown(symbol):
        return None

    candidates = []

    for side in ["LONG", "SHORT"]:
        if not is_side_enabled(side):
            continue

        for builder in [
            build_breakout_strategy,
            build_sweep_retest_strategy,
            build_reversal_strategy,
        ]:
            signal = builder(
                symbol,
                side,
                candles_15m,
                candles_1m,
                candles_5m,
                candles_1h,
                candles_4h,
                btc_status,
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
        reverse=True,
    )

    return candidates[0]


def make_signal_message(signal):
    arrow = "📈" if signal["side"] == "LONG" else "📉"
    mode_text = "TEST SIGNAL" if TEST_MODE else "TRADE SIGNAL"

    strategy_icons = {
        "BREAKOUT_MOMENTUM": "🚀 Breakout Momentum",
        "SWEEP_RETEST": "🧲 Sweep + Retest",
        "REVERSAL_BOUNCE": "🔁 Reversal Bounce",
    }

    strategy_name = strategy_icons.get(signal["strategy"], signal["strategy"])

    return f"""
🎯 <b>V23 Adaptive Strategy Manager</b> · <b>{mode_text}</b>
{strategy_name}

{arrow} <b>{signal["side"]} {signal["symbol"].replace("-", "/")}</b> · {TIMEFRAME}
Качество: <b>{signal["quality"]}%</b>

🎯 Вход: <code>{format_price(signal["entry"])}</code>
🛑 SL: <code>{format_price(signal["sl"])}</code>
✅ TP1: <code>{format_price(signal["tp1"])}</code>
✅ TP2: <code>{format_price(signal["tp2"])}</code>
✅ TP3: <code>{format_price(signal["tp3"])}</code>

⚠️ Плечо max.: <b>{LEVERAGE}x</b>
🛡 Риск до SL: около <b>{signal["risk_position_percent"]:.1f}%</b> по позиции
📊 RR до TP1: <b>{signal["rr"]:.2f}</b>
📊 Volume: x<b>{signal["volume_ratio"]:.2f}</b>

После TP1 сделка считается позитивной.
Пара блокируется после 1 SL.
Направление отключается после 2 SL подряд.
Стратегия отключается после 2 SL подряд.

⚠️ Не финансовый совет. Сначала TEST/минимальная сумма.
""".strip()


def apply_adaptive_rules(signal, result):
    side = signal["side"]
    strategy = signal["strategy"]
    symbol = signal["symbol"]

    notes = []

    if result == "SL" and not signal.get("counted_sl"):
        signal["counted_sl"] = True

        STATS["side"][side]["sl"] += 1
        STATS["side"][side]["consecutive_sl"] += 1

        STATS["strategy"][strategy]["sl"] += 1
        STATS["strategy"][strategy]["consecutive_sl"] += 1

        STATS["pair_sl"][symbol] = STATS["pair_sl"].get(symbol, 0) + 1

        if STATS["pair_sl"][symbol] >= PAIR_MAX_SL_BEFORE_BLOCK:
            BLOCKED_SYMBOLS[symbol] = time.time() + 86400
            notes.append(f"🚫 Пара {symbol.replace('-', '/')} заблокирована на 24ч после SL.")

        if STATS["side"][side]["consecutive_sl"] >= SIDE_MAX_CONSECUTIVE_SL:
            SIDE_DISABLED_UNTIL[side] = time.time() + DISABLE_SECONDS
            notes.append(f"⛔ {side} отключён на 6 часов после серии SL.")

        if STATS["strategy"][strategy]["consecutive_sl"] >= STRATEGY_MAX_CONSECUTIVE_SL:
            STRATEGY_DISABLED_UNTIL[strategy] = time.time() + DISABLE_SECONDS
            notes.append(f"⛔ Стратегия {strategy} отключена на 6 часов после серии SL.")

    elif result in ["TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        STATS["side"][side]["consecutive_sl"] = 0
        STATS["strategy"][strategy]["consecutive_sl"] = 0

        if not signal.get("counted_positive"):
            signal["counted_positive"] = True
            STATS["side"][side]["positive"] += 1
            STATS["strategy"][strategy]["positive"] += 1
            STATS["pair_positive"][symbol] = STATS["pair_positive"].get(symbol, 0) + 1

        if result == "TP1":
            STATS["side"][side].setdefault("tp1", 0)
        elif result == "TP2":
            STATS["side"][side].setdefault("tp2", 0)
        elif result == "TP3":
            STATS["side"][side].setdefault("tp3", 0)

    side_pos, side_sl, side_total, side_wr = side_stats(side)
    strat_pos, strat_sl, strat_total, strat_wr = strategy_stats(strategy)

    if side_total >= MIN_CLOSED_TRADES_FOR_CHECK and side_wr < MIN_WINRATE:
        SIDE_DISABLED_UNTIL[side] = time.time() + DISABLE_SECONDS
        notes.append(f"⛔ {side} отключён: winrate {side_wr:.1f}% после {side_total} сделок.")

    if strat_total >= MIN_CLOSED_TRADES_FOR_CHECK and strat_wr < MIN_WINRATE:
        STRATEGY_DISABLED_UNTIL[strategy] = time.time() + DISABLE_SECONDS
        notes.append(f"⛔ {strategy} отключена: winrate {strat_wr:.1f}% после {strat_total} сделок.")

    return notes


def make_result_message(signal, result, price):
    notes = apply_adaptive_rules(signal, result)

    side = signal["side"]
    symbol = signal["symbol"].replace("-", "/")
    strategy = signal["strategy"]

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

    long_pos, long_sl, long_total, long_wr = side_stats("LONG")
    short_pos, short_sl, short_total, short_wr = side_stats("SHORT")

    strategy_lines = []
    for s in STRATEGIES:
        p, sl, total, wr = strategy_stats(s)
        status = "OFF" if time.time() < STRATEGY_DISABLED_UNTIL.get(s, 0) else "ON"
        strategy_lines.append(f"{s}: {p}/{sl} WR {wr:.1f}% [{status}]")

    adaptive_text = ""
    if notes:
        adaptive_text = "\n\n<b>Адаптация:</b>\n" + "\n".join(notes)

    return f"""
{icon} <b>{title}</b>

<b>{side} {symbol}</b>
Стратегия: <b>{strategy}</b>

Вход: <code>{format_price(signal["entry"])}</code>
Текущая/уровень: <code>{format_price(price)}</code>

TP1: <code>{format_price(signal["tp1"])}</code>
TP2: <code>{format_price(signal["tp2"])}</code>
TP3: <code>{format_price(signal["tp3"])}</code>
SL: <code>{format_price(signal["sl"])}</code>

📊 <b>Направления:</b>
📈 LONG: {long_pos} позитив / {long_sl} SL / WR {long_wr:.1f}%
📉 SHORT: {short_pos} позитив / {short_sl} SL / WR {short_wr:.1f}%

🧠 <b>Стратегии:</b>
{chr(10).join(strategy_lines)}

🚫 Заблокировано пар: <b>{len(BLOCKED_SYMBOLS)}</b>
{adaptive_text}
""".strip()


def bingx_url(symbol):
    pair = symbol.replace("-", "")
    return f"https://bingx.com/en/futures/forward/{pair}"


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

    return None, new_candles[-1]["close"]


async def track_active_signals(session):
    if not ACTIVE_SIGNALS:
        return

    finished = []

    for signal_id, signal in list(ACTIVE_SIGNALS.items()):
        try:
            candles = await get_klines(session, signal["symbol"], interval="1m", limit=120)

            if candles is None:
                continue

            result, price = check_hit(signal, candles)

            if not result:
                continue

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


async def scan_loop():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")

    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID не задан")

    async with aiohttp.ClientSession() as session:
        await send_telegram_message(
            session,
            f"✅ V23 Adaptive Strategy Manager Bot запущен.\n"
            f"Режим: {'TEST' if TEST_MODE else 'TRADE'}\n"
            f"Стратегии: Breakout Momentum, Sweep + Retest, Reversal Bounce.\n"
            f"Бот сам отключает слабые стратегии, направления и пары.\n"
            f"После TP1 сделка считается позитивной.\n"
            f"Пара блокируется после {PAIR_MAX_SL_BEFORE_BLOCK} SL.\n"
            f"Направление отключается после {SIDE_MAX_CONSECUTIVE_SL} SL подряд.\n"
            f"Стратегия отключается после {STRATEGY_MAX_CONSECUTIVE_SL} SL подряд.\n"
            f"Плечо max.: {LEVERAGE}x.\n"
            f"Сначала тестируй минимальной суммой."
        )

        last_track_time = 0

        while True:
            reset_daily_stats_if_needed()

            try:
                if time.time() - last_track_time >= TRACK_INTERVAL_SECONDS:
                    await track_active_signals(session)
                    last_track_time = time.time()

                print("Scanning...", now_text())

                btc_1h = await get_klines(session, BTC_SYMBOL, interval=TREND_TIMEFRAME, limit=260)

                if not btc_1h:
                    print("BTC data unavailable")
                    await asyncio.sleep(60)
                    continue

                btc_status = btc_market_filter(btc_1h)
                symbols = await get_symbols(session)

                checked = 0
                found = 0

                for symbol in symbols:
                    reset_daily_stats_if_needed()

                    if STATS["signals_today"] >= DAILY_MAX_SIGNALS:
                        break

                    if is_symbol_blocked(symbol):
                        continue

                    checked += 1

                    try:
                        candles_15m = await get_klines(session, symbol, TIMEFRAME, 260)
                        candles_1m = await get_klines(session, symbol, CONFIRM_1M, 120)
                        candles_5m = await get_klines(session, symbol, CONFIRM_5M, 160)
                        candles_1h = await get_klines(session, symbol, TREND_TIMEFRAME, 260)
                        candles_4h = await get_klines(session, symbol, MACRO_TIMEFRAME, 260)

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

                        STATS["signals_total"] += 1
                        STATS["signals_today"] += 1

                        found += 1

                        await asyncio.sleep(2)

                        if found >= MAX_SIGNALS_PER_SCAN:
                            break

                    except Exception as e:
                        print(f"Error with {symbol}:", e)

                print(
                    f"Scan finished. Checked: {checked}. "
                    f"Found: {found}. Active: {len(ACTIVE_SIGNALS)}. "
                    f"Blocked: {len(BLOCKED_SYMBOLS)}."
                )

                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

            except Exception as e:
                print("Main loop error:", e)
                await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(scan_loop())
