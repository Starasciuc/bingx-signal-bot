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
CONFIRM_TIMEFRAME = os.getenv("CONFIRM_TIMEFRAME", "1m")
TREND_TIMEFRAME = os.getenv("TREND_TIMEFRAME", "1h")
MACRO_TIMEFRAME = os.getenv("MACRO_TIMEFRAME", "4h")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "35"))
TRACK_INTERVAL_SECONDS = int(os.getenv("TRACK_INTERVAL_SECONDS", "30"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "450"))

LEVERAGE = int(os.getenv("LEVERAGE", "10"))

ENABLE_LONG = os.getenv("ENABLE_LONG", "true").lower() == "true"
ENABLE_SHORT = os.getenv("ENABLE_SHORT", "true").lower() == "true"
TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"

TRADE_ONLY_A_PLUS = os.getenv("TRADE_ONLY_A_PLUS", "true").lower() == "true"

LONG_TP1_POSITION_PERCENT = float(os.getenv("LONG_TP1_POSITION_PERCENT", "14"))
LONG_TP2_POSITION_PERCENT = float(os.getenv("LONG_TP2_POSITION_PERCENT", "24"))
LONG_TP3_POSITION_PERCENT = float(os.getenv("LONG_TP3_POSITION_PERCENT", "36"))
LONG_MIN_RISK_POSITION_PERCENT = float(os.getenv("LONG_MIN_RISK_POSITION_PERCENT", "9"))
LONG_MAX_RISK_POSITION_PERCENT = float(os.getenv("LONG_MAX_RISK_POSITION_PERCENT", "13"))

SHORT_TP1_POSITION_PERCENT = float(os.getenv("SHORT_TP1_POSITION_PERCENT", "10"))
SHORT_TP2_POSITION_PERCENT = float(os.getenv("SHORT_TP2_POSITION_PERCENT", "18"))
SHORT_TP3_POSITION_PERCENT = float(os.getenv("SHORT_TP3_POSITION_PERCENT", "30"))
SHORT_MIN_RISK_POSITION_PERCENT = float(os.getenv("SHORT_MIN_RISK_POSITION_PERCENT", "8"))
SHORT_MAX_RISK_POSITION_PERCENT = float(os.getenv("SHORT_MAX_RISK_POSITION_PERCENT", "12"))

MIN_RR_TO_TP1 = float(os.getenv("MIN_RR_TO_TP1", "0.9"))

MIN_QUALITY = int(os.getenv("MIN_QUALITY", "86"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.35"))

A_PLUS_VOLUME_RATIO = float(os.getenv("A_PLUS_VOLUME_RATIO", "1.40"))
A_PLUS_MIN_QUALITY = int(os.getenv("A_PLUS_MIN_QUALITY", "87"))

MIN_PREVIOUS_DROP_FOR_LONG = float(os.getenv("MIN_PREVIOUS_DROP_FOR_LONG", "1.6"))
MIN_PREVIOUS_RISE_FOR_SHORT = float(os.getenv("MIN_PREVIOUS_RISE_FOR_SHORT", "1.5"))

LEVEL_DISTANCE_PERCENT = float(os.getenv("LEVEL_DISTANCE_PERCENT", "1.20"))

LONG_MAX_ALREADY_MOVED_POSITION_PERCENT = float(os.getenv("LONG_MAX_ALREADY_MOVED_POSITION_PERCENT", "2.2"))
SHORT_MAX_ALREADY_MOVED_POSITION_PERCENT = float(os.getenv("SHORT_MAX_ALREADY_MOVED_POSITION_PERCENT", "1.7"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "5400"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "3"))
DAILY_MAX_SIGNALS = int(os.getenv("DAILY_MAX_SIGNALS", "8"))

PAIR_MAX_SL_BEFORE_BLOCK = int(os.getenv("PAIR_MAX_SL_BEFORE_BLOCK", "1"))
SIDE_MAX_CONSECUTIVE_SL = int(os.getenv("SIDE_MAX_CONSECUTIVE_SL", "2"))
SIDE_DISABLE_SECONDS = int(os.getenv("SIDE_DISABLE_SECONDS", "21600"))

MIN_CLOSED_TRADES_FOR_SIDE_CHECK = int(os.getenv("MIN_CLOSED_TRADES_FOR_SIDE_CHECK", "10"))
MIN_SIDE_WINRATE = float(os.getenv("MIN_SIDE_WINRATE", "50"))

USE_LIQUID_ONLY = os.getenv("USE_LIQUID_ONLY", "true").lower() == "true"

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


def is_side_enabled(side):
    if side == "LONG" and not ENABLE_LONG:
        return False

    if side == "SHORT" and not ENABLE_SHORT:
        return False

    disabled_until = SIDE_DISABLED_UNTIL.get(side, 0)
    if time.time() < disabled_until:
        return False

    return True


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
        return "BULLISH", change

    if price < ema50 < ema200 and change < -0.10:
        return "BEARISH", change

    return "NEUTRAL", change


def market_regime_allows(side, btc_status, trend_1h, trend_4h, price, vwap):
    if side == "SHORT":
        strong_bull_market = (
            btc_status == "BULLISH"
            and trend_1h in ["BULLISH", "SOFT_BULLISH"]
            and trend_4h in ["BULLISH", "SOFT_BULLISH"]
            and price > vwap
        )

        if strong_bull_market:
            return False

        return True

    if side == "LONG":
        strong_bear_market = (
            btc_status == "BEARISH"
            and trend_1h in ["BEARISH", "SOFT_BEARISH"]
            and trend_4h in ["BEARISH", "SOFT_BEARISH"]
            and price < vwap
        )

        if strong_bear_market:
            return False

        return True

    return False


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


def price_move_percent(entry, target, side):
    if side == "LONG":
        return (target - entry) / entry * 100

    return (entry - target) / entry * 100


def make_tp_by_percent(entry, side, position_percent):
    price_move_needed = position_percent / LEVERAGE

    if side == "LONG":
        return entry * (1 + price_move_needed / 100)

    return entry * (1 - price_move_needed / 100)


def apply_min_max_sl(entry, sl, side, min_risk_position_percent, max_risk_position_percent):
    risk_price_percent = abs(entry - sl) / entry * 100
    risk_position_percent = risk_price_percent * LEVERAGE

    if risk_position_percent < min_risk_position_percent:
        min_price_move = (min_risk_position_percent / LEVERAGE) / 100

        if side == "LONG":
            sl = entry * (1 - min_price_move)
        else:
            sl = entry * (1 + min_price_move)

        risk_price_percent = abs(entry - sl) / entry * 100
        risk_position_percent = risk_price_percent * LEVERAGE

    if risk_position_percent > max_risk_position_percent:
        return None, None

    return sl, risk_position_percent


def moved_from_level_position_percent(price, level, side):
    if level <= 0:
        return 999

    if side == "LONG":
        move = (price - level) / level * 100
    else:
        move = (level - price) / level * 100

    return move * LEVERAGE


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


def balanced_1m_confirmation(candles_confirm, side):
    closes = [c["close"] for c in candles_confirm]

    if len(closes) < 20:
        return False, False

    ema9 = ema(closes, 9)[-1]
    last = candles_confirm[-1]
    prev = candles_confirm[-2]

    if side == "LONG":
        normal_confirm = (
            last["close"] > last["open"]
            and last["close"] > prev["close"]
            and last["close"] > ema9
        )

        strong_confirm = (
            last["close"] > last["open"]
            and last["close"] > prev["high"]
            and last["close"] > ema9
        )

        return normal_confirm, strong_confirm

    normal_confirm = (
        last["close"] < last["open"]
        and last["close"] < prev["close"]
        and last["close"] < ema9
    )

    strong_confirm = (
        last["close"] < last["open"]
        and last["close"] < prev["low"]
        and last["close"] < ema9
    )

    return normal_confirm, strong_confirm


def build_signal(symbol, side, candles_15m, candles_confirm, candles_1h, candles_4h, btc_status):
    if not is_side_enabled(side):
        return None

    if is_symbol_blocked(symbol):
        return None

    closes_15 = [c["close"] for c in candles_15m]
    highs_15 = [c["high"] for c in candles_15m]
    lows_15 = [c["low"] for c in candles_15m]
    volumes_15 = [c["volume"] for c in candles_15m]

    last = candles_15m[-1]
    prev = candles_15m[-2]
    price = last["close"]

    rsi = calculate_rsi(closes_15, 14)
    atr = calculate_atr(candles_15m, 14)
    vwap = calculate_vwap_like(candles_15m, 48)

    if rsi is None or atr is None or vwap is None:
        return None

    trend_1h = trend_ema_50_200(candles_1h)
    trend_4h = trend_ema_50_200(candles_4h)

    if not market_regime_allows(side, btc_status, trend_1h, trend_4h, price, vwap):
        return None

    avg_volume = sum(volumes_15[-30:]) / 30
    volume_ratio = last["volume"] / avg_volume if avg_volume > 0 else 0

    if volume_ratio < MIN_VOLUME_RATIO:
        return None

    recent_move = recent_move_percent(candles_15m, 12)
    normal_confirm, strong_confirm = balanced_1m_confirmation(candles_confirm, side)

    if not normal_confirm:
        return None

    if side == "LONG":
        if recent_move > -MIN_PREVIOUS_DROP_FOR_LONG:
            return None

        support = nearest_support(price, candles_1h, candles_4h)
        if support is None:
            return None

        moved = moved_from_level_position_percent(price, support, "LONG")
        if moved > LONG_MAX_ALREADY_MOVED_POSITION_PERCENT:
            return None

        level_touch = last["low"] <= support * 1.006

        bounce_candle = (
            last["close"] > last["open"]
            and last["close"] > prev["close"]
            and lower_wick(last) >= candle_body(last) * 0.30
        )

        if not level_touch or not bounce_candle:
            return None

        if btc_status == "BEARISH":
            return None

        if trend_4h == "BEARISH" and trend_1h == "BEARISH":
            return None

        if trend_1h == "BEARISH":
            return None

        rsi_ok = rsi <= 50
        vwap_ok = price >= vwap * 0.982 and price <= vwap * 1.018

        sl = min(support - atr * 0.18, min(lows_15[-8:]) - atr * 0.04)

        if sl >= price:
            return None

        sl, risk_position_percent = apply_min_max_sl(
            price,
            sl,
            "LONG",
            LONG_MIN_RISK_POSITION_PERCENT,
            LONG_MAX_RISK_POSITION_PERCENT,
        )

        if sl is None:
            return None

        tp1 = make_tp_by_percent(price, "LONG", LONG_TP1_POSITION_PERCENT)
        tp2 = make_tp_by_percent(price, "LONG", LONG_TP2_POSITION_PERCENT)
        tp3 = make_tp_by_percent(price, "LONG", LONG_TP3_POSITION_PERCENT)

        checks = [
            recent_move <= -MIN_PREVIOUS_DROP_FOR_LONG,
            level_touch,
            bounce_candle,
            rsi_ok,
            vwap_ok,
            volume_ratio >= MIN_VOLUME_RATIO,
            btc_status != "BEARISH",
            trend_1h != "BEARISH",
            trend_4h != "BEARISH",
            moved <= LONG_MAX_ALREADY_MOVED_POSITION_PERCENT,
            normal_confirm,
        ]

    else:
        if recent_move < MIN_PREVIOUS_RISE_FOR_SHORT:
            return None

        resistance = nearest_resistance(price, candles_1h, candles_4h)
        if resistance is None:
            return None

        moved = moved_from_level_position_percent(price, resistance, "SHORT")
        if moved > SHORT_MAX_ALREADY_MOVED_POSITION_PERCENT:
            return None

        level_touch = last["high"] >= resistance * 0.994

        rejection_candle = (
            last["close"] < last["open"]
            and last["close"] < prev["close"]
            and upper_wick(last) >= candle_body(last) * 0.30
        )

        if not level_touch or not rejection_candle:
            return None

        if btc_status == "BULLISH":
            return None

        if trend_4h == "BULLISH" and trend_1h == "BULLISH":
            return None

        if trend_1h in ["BULLISH", "SOFT_BULLISH"]:
            return None

        rsi_ok = rsi >= 55
        vwap_ok = price <= vwap * 1.005 and price >= vwap * 0.982

        sl = max(resistance + atr * 0.18, max(highs_15[-8:]) + atr * 0.04)

        if sl <= price:
            return None

        sl, risk_position_percent = apply_min_max_sl(
            price,
            sl,
            "SHORT",
            SHORT_MIN_RISK_POSITION_PERCENT,
            SHORT_MAX_RISK_POSITION_PERCENT,
        )

        if sl is None:
            return None

        tp1 = make_tp_by_percent(price, "SHORT", SHORT_TP1_POSITION_PERCENT)
        tp2 = make_tp_by_percent(price, "SHORT", SHORT_TP2_POSITION_PERCENT)
        tp3 = make_tp_by_percent(price, "SHORT", SHORT_TP3_POSITION_PERCENT)

        checks = [
            recent_move >= MIN_PREVIOUS_RISE_FOR_SHORT,
            level_touch,
            rejection_candle,
            rsi_ok,
            vwap_ok,
            volume_ratio >= MIN_VOLUME_RATIO,
            btc_status != "BULLISH",
            trend_1h not in ["BULLISH", "SOFT_BULLISH"],
            trend_4h != "BULLISH",
            moved <= SHORT_MAX_ALREADY_MOVED_POSITION_PERCENT,
            normal_confirm,
        ]

    reward_price_percent = price_move_percent(price, tp1, side)
    risk_price_percent = abs(price - sl) / price * 100
    rr = reward_price_percent / risk_price_percent if risk_price_percent > 0 else 0

    if rr < MIN_RR_TO_TP1:
        return None

    passed = sum(1 for ok in checks if ok)

    quality = min(95, 52 + passed * 3)

    if strong_confirm:
        quality += 3

    if volume_ratio >= A_PLUS_VOLUME_RATIO:
        quality += 2

    quality = min(95, quality)

    if quality < MIN_QUALITY:
        return None

    signal_grade = "A+" if strong_confirm and volume_ratio >= A_PLUS_VOLUME_RATIO and quality >= A_PLUS_MIN_QUALITY else "B"

    if TRADE_ONLY_A_PLUS and signal_grade != "A+":
        return None

    signal_id = f"{symbol}:V20_2_A_PLUS_SOFT:{side}:{round(price, 6)}"

    if signal_id in SENT_SIGNALS:
        return None

    return {
        "symbol": symbol,
        "side": side,
        "quality": quality,
        "grade": signal_grade,
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
        "created_at_ms": int(time.time() * 1000),
        "last_checked_ms": int(time.time() * 1000),
        "status": "ACTIVE",
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "counted_positive": False,
        "counted_sl": False,
    }


def analyze_symbol(symbol, candles_15m, candles_confirm, candles_1h, candles_4h, btc_status):
    if is_on_cooldown(symbol):
        return None

    candidates = []

    short_signal = build_signal(symbol, "SHORT", candles_15m, candles_confirm, candles_1h, candles_4h, btc_status)
    if short_signal:
        candidates.append(short_signal)

    long_signal = build_signal(symbol, "LONG", candles_15m, candles_confirm, candles_1h, candles_4h, btc_status)
    if long_signal:
        candidates.append(long_signal)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            x["grade"] == "A+",
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

    return f"""
🎯 <b>V20.2 A+ Professional Soft</b> · <b>{mode_text}</b>
🔥 <b>A+ SIGNAL ONLY</b>

{arrow} <b>{signal["side"]} {signal["symbol"].replace("-", "/")}</b> · {TIMEFRAME}
Качество: <b>{signal["quality"]}%</b>

🎯 Зона входа: <code>{format_price(signal["entry"])}</code>
🛑 SL: <code>{format_price(signal["sl"])}</code>
✅ TP1: <code>{format_price(signal["tp1"])}</code>
✅ TP2: <code>{format_price(signal["tp2"])}</code>
✅ TP3: <code>{format_price(signal["tp3"])}</code>

⚠️ Плечо max.: <b>{LEVERAGE}x</b>
🛡 Риск до SL: около <b>{signal["risk_position_percent"]:.1f}%</b> по позиции
📊 RR до TP1: <b>{signal["rr"]:.2f}</b>
📊 Volume: x<b>{signal["volume_ratio"]:.2f}</b>

Бот отправляет только A+ сигналы.
B-сигналы полностью отфильтрованы.

A+ условия смягчены:
• качество {A_PLUS_MIN_QUALITY}+
• объём x{A_PLUS_VOLUME_RATIO}+

После TP1 сделка считается позитивной.
Пара блокируется после 1 SL.
Направление отключается после 2 SL подряд.

⚠️ Не финансовый совет. Сначала тест/минимальная сумма.
""".strip()


def prefix_for_side(side):
    return "long" if side == "LONG" else "short"


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
        notes.append(f"⛔ {side} отключён на 6 часов: winrate {winrate:.1f}% после {total} закрытых сделок.")

    return notes


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

    last_price = new_candles[-1]["close"]
    return None, last_price


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
            f"✅ V20.2 A+ Professional Soft Bot запущен.\n"
            f"Режим: {'TEST' if TEST_MODE else 'TRADE'}\n"
            f"Бот отправляет только A+ сигналы.\n"
            f"B-сигналы полностью отфильтрованы.\n"
            f"MIN_QUALITY: {MIN_QUALITY}\n"
            f"A+ Quality: {A_PLUS_MIN_QUALITY}+\n"
            f"Volume filter: x{MIN_VOLUME_RATIO}\n"
            f"A+ Volume: x{A_PLUS_VOLUME_RATIO}+\n"
            f"Пара блокируется после {PAIR_MAX_SL_BEFORE_BLOCK} SL.\n"
            f"Направление отключается после {SIDE_MAX_CONSECUTIVE_SL} SL подряд.\n"
            f"Плечо max.: {LEVERAGE}x."
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

                btc_status, btc_change = btc_market_filter(btc_1h)
                print(f"BTC status: {btc_status}, change: {btc_change:.2f}%")

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
                        candles_15m = await get_klines(session, symbol, interval=TIMEFRAME, limit=260)
                        candles_confirm = await get_klines(session, symbol, interval=CONFIRM_TIMEFRAME, limit=260)
                        candles_1h = await get_klines(session, symbol, interval=TREND_TIMEFRAME, limit=260)
                        candles_4h = await get_klines(session, symbol, interval=MACRO_TIMEFRAME, limit=260)

                        if (
                            candles_15m is None
                            or candles_confirm is None
                            or candles_1h is None
                            or candles_4h is None
                        ):
                            continue

                        signal = analyze_symbol(
                            symbol,
                            candles_15m,
                            candles_confirm,
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

                        found += 1
                        STATS["signals_total"] += 1
                        STATS["signals_today"] += 1

                        await asyncio.sleep(2)

                        if found >= MAX_SIGNALS_PER_SCAN:
                            break

                    except Exception as e:
                        print(f"Error with {symbol}:", e)

                long_pos, long_neg, long_total, long_wr = side_stats("LONG")
                short_pos, short_neg, short_total, short_wr = side_stats("SHORT")

                print(
                    f"Scan finished. Checked: {checked}. "
                    f"Signals found: {found}. Active: {len(ACTIVE_SIGNALS)}. "
                    f"LONG WR: {long_wr:.1f}% | SHORT WR: {short_wr:.1f}% | "
                    f"Blocked: {len(BLOCKED_SYMBOLS)}"
                )

                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

            except Exception as e:
                print("Main loop error:", e)
                await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(scan_loop())
