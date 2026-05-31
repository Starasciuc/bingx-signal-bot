import os
import time
import json
import random
import asyncio
import requests
import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse


app = FastAPI(title="Professional Adaptive Futures Bot AUTO V3 PRO")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BINGX_BASE_URL = "https://open-api.bingx.com"

TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
LEVERAGE = int(os.getenv("LEVERAGE", "10"))

MIN_SCORE = int(os.getenv("MIN_SCORE", "84"))
MIN_RR = float(os.getenv("MIN_RR", "0.75"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.25"))

TP1_POSITION_PERCENT = float(os.getenv("TP1_POSITION_PERCENT", "8"))
TP2_POSITION_PERCENT = float(os.getenv("TP2_POSITION_PERCENT", "15"))
TP3_POSITION_PERCENT = float(os.getenv("TP3_POSITION_PERCENT", "25"))

TP1_CLOSE_PERCENT = float(os.getenv("TP1_CLOSE_PERCENT", "50"))

MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "10"))

DEFAULT_DEPOSIT = float(os.getenv("DEFAULT_DEPOSIT", "1000"))
DEFAULT_RISK_PERCENT = float(os.getenv("DEFAULT_RISK_PERCENT", "0.5"))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "80"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "3600"))

PAIR_BLOCK_SECONDS = int(os.getenv("PAIR_BLOCK_SECONDS", "86400"))
SIDE_DISABLE_SECONDS = int(os.getenv("SIDE_DISABLE_SECONDS", "21600"))
STRATEGY_DISABLE_SECONDS = int(os.getenv("STRATEGY_DISABLE_SECONDS", "21600"))

PAIR_MAX_SL = int(os.getenv("PAIR_MAX_SL", "1"))
SIDE_MAX_CONSECUTIVE_SL = int(os.getenv("SIDE_MAX_CONSECUTIVE_SL", "2"))
STRATEGY_MAX_CONSECUTIVE_SL = int(os.getenv("STRATEGY_MAX_CONSECUTIVE_SL", "2"))

AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"

AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "1500"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "120"))

ENABLE_NEWS_FILTER = os.getenv("ENABLE_NEWS_FILTER", "true").lower() == "true"
ENABLE_FUNDING_FILTER = os.getenv("ENABLE_FUNDING_FILTER", "true").lower() == "true"
ENABLE_OI_FILTER = os.getenv("ENABLE_OI_FILTER", "true").lower() == "true"
ENABLE_LATE_ENTRY_FILTER = os.getenv("ENABLE_LATE_ENTRY_FILTER", "true").lower() == "true"

MAX_RECENT_MOVE_PERCENT = float(os.getenv("MAX_RECENT_MOVE_PERCENT", "4.5"))
MAX_DISTANCE_FROM_VWAP_PERCENT = float(os.getenv("MAX_DISTANCE_FROM_VWAP_PERCENT", "3.5"))

MAX_ABS_FUNDING_RATE = float(os.getenv("MAX_ABS_FUNDING_RATE", "0.0008"))
FUNDING_EXTREME_RATE = float(os.getenv("FUNDING_EXTREME_RATE", "0.0015"))

NEWS_MAX_AGE_SECONDS = int(os.getenv("NEWS_MAX_AGE_SECONDS", "21600"))
NEWS_CACHE_SECONDS = int(os.getenv("NEWS_CACHE_SECONDS", "900"))

NEWS_RSS_URLS = os.getenv(
    "NEWS_RSS_URLS",
    "https://feeds.bloomberg.com/markets/news.rss,"
    "https://www.cnbc.com/id/100003114/device/rss/rss.html,"
    "https://www.coindesk.com/arc/outboundfeeds/rss/"
).split(",")

STATE_FILE = "bot_state.json"

STRATEGIES = [
    "BREAKOUT_MOMENTUM",
    "TREND_PULLBACK",
    "SWEEP_RECLAIM",
]

LIQUID_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "INJ", "NEAR", "ARB", "OP", "APT", "SUI", "SEI", "DOT", "LTC",
    "BCH", "UNI", "AAVE", "FIL", "ATOM", "ETC", "TRX", "MATIC", "WLD",
    "TIA", "ORDI", "FTM", "RUNE", "ENA", "JUP", "PYTH", "STRK", "DYDX",
    "TON", "COMP", "STX", "TRB", "JTO", "DYM", "ICP", "GALA", "FET",
    "RNDR", "IMX", "APE"
}


def default_state():
    return {
        "active_signals": {},
        "sent_signals": {},
        "symbol_cooldown": {},
        "blocked_symbols": {},
        "side_disabled_until": {
            "LONG": 0,
            "SHORT": 0,
        },
        "strategy_disabled_until": {
            "BREAKOUT_MOMENTUM": 0,
            "TREND_PULLBACK": 0,
            "SWEEP_RECLAIM": 0,
        },
        "stats": {
            "side": {
                "LONG": {
                    "positive": 0,
                    "sl": 0,
                    "consecutive_sl": 0,
                    "tp1": 0,
                    "tp2": 0,
                    "tp3": 0,
                },
                "SHORT": {
                    "positive": 0,
                    "sl": 0,
                    "consecutive_sl": 0,
                    "tp1": 0,
                    "tp2": 0,
                    "tp3": 0,
                },
            },
            "strategy": {
                "BREAKOUT_MOMENTUM": {
                    "positive": 0,
                    "sl": 0,
                    "consecutive_sl": 0,
                },
                "TREND_PULLBACK": {
                    "positive": 0,
                    "sl": 0,
                    "consecutive_sl": 0,
                },
                "SWEEP_RECLAIM": {
                    "positive": 0,
                    "sl": 0,
                    "consecutive_sl": 0,
                },
            },
            "pair_sl": {},
            "pair_positive": {},
        },
        "auto": {
            "last_scan_time": 0,
            "last_track_time": 0,
            "last_scan_result": None,
            "last_track_result": None,
            "last_error": None,
        },
        "news": {
            "last_checked": 0,
            "risk": "UNKNOWN",
            "bias": "NEUTRAL",
            "headline": "",
            "score_adjustment": 0,
        }
    }


def load_state():
    if not os.path.exists(STATE_FILE):
        return default_state()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        base = default_state()

        for key, value in base.items():
            if key not in state:
                state[key] = value

        if "auto" not in state:
            state["auto"] = base["auto"]

        if "news" not in state:
            state["news"] = base["news"]

        return state

    except Exception:
        return default_state()


def save_state(state):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


STATE = load_state()


def now_ts():
    return time.time()


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.upper().replace("/", "-").strip()

    if symbol.endswith("USDT") and "-" not in symbol:
        symbol = symbol.replace("USDT", "-USDT")

    if not symbol.endswith("-USDT"):
        symbol = symbol.replace("-", "") + "-USDT"

    return symbol


def display_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-", "/")


def base_from_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-USDT", "")


def normalize_direction(direction: Optional[str]) -> Optional[str]:
    if direction is None:
        return None

    direction = direction.upper().strip()

    if direction not in ["LONG", "SHORT"]:
        return None

    return direction


def is_good_symbol(symbol: str) -> bool:
    symbol = normalize_symbol(symbol)
    base = base_from_symbol(symbol)

    if base not in LIQUID_BASES:
        return False

    bad = ["USD", "EUR", "GBP", "JPY", "AAPL", "TSLA", "NVDA", "META", "GOOG"]

    if any(x in base for x in bad):
        return False

    return True


def is_on_cooldown(symbol: str) -> bool:
    symbol = normalize_symbol(symbol)
    last = STATE["symbol_cooldown"].get(symbol)

    if not last:
        return False

    return now_ts() - last < SIGNAL_COOLDOWN_SECONDS


def set_cooldown(symbol: str):
    STATE["symbol_cooldown"][normalize_symbol(symbol)] = now_ts()
    save_state(STATE)


def is_blocked(symbol: str) -> bool:
    symbol = normalize_symbol(symbol)
    until = STATE["blocked_symbols"].get(symbol)

    if not until:
        return False

    if now_ts() > until:
        STATE["blocked_symbols"].pop(symbol, None)
        save_state(STATE)
        return False

    return True


def is_side_enabled(side: str) -> bool:
    return now_ts() >= STATE["side_disabled_until"].get(side, 0)


def is_strategy_enabled(strategy: str) -> bool:
    return now_ts() >= STATE["strategy_disabled_until"].get(strategy, 0)


def get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception:
        return None


def get_symbols() -> List[str]:
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts"
    data = get_json(url)

    if not data:
        return []

    result = []

    for item in data.get("data", []):
        symbol = item.get("symbol")

        if symbol and is_good_symbol(symbol):
            result.append(normalize_symbol(symbol))

    random.shuffle(result)
    return result[:MAX_SYMBOLS]


def get_klines(symbol: str, interval: str, limit: int = 260) -> Optional[List[dict]]:
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"

    params = {
        "symbol": normalize_symbol(symbol),
        "interval": interval,
        "limit": limit,
    }

    data = get_json(url, params=params)

    if not data:
        return None

    raw = data.get("data", [])

    if not raw:
        return None

    candles = []

    for c in raw:
        try:
            candles.append({
                "time": int(c["time"]),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c["volume"]),
            })
        except Exception:
            continue

    candles.sort(key=lambda x: x["time"])

    if len(candles) < 60:
        return None

    return candles


def extract_float_from_nested(data: Any, keys: List[str]) -> Optional[float]:
    if isinstance(data, dict):
        for k, v in data.items():
            if k in keys:
                try:
                    return float(v)
                except Exception:
                    pass

            nested = extract_float_from_nested(v, keys)
            if nested is not None:
                return nested

    if isinstance(data, list):
        for item in data:
            nested = extract_float_from_nested(item, keys)
            if nested is not None:
                return nested

    return None


def get_funding_rate(symbol: str) -> Optional[float]:
    if not ENABLE_FUNDING_FILTER:
        return None

    symbol = normalize_symbol(symbol)

    endpoints = [
        "/openApi/swap/v2/quote/premiumIndex",
        "/openApi/swap/v2/quote/fundingRate",
    ]

    for endpoint in endpoints:
        data = get_json(
            f"{BINGX_BASE_URL}{endpoint}",
            params={"symbol": symbol}
        )

        if not data:
            continue

        value = extract_float_from_nested(
            data,
            ["lastFundingRate", "fundingRate", "funding_rate", "rate"]
        )

        if value is not None:
            return value

    return None


def get_open_interest(symbol: str) -> Optional[float]:
    if not ENABLE_OI_FILTER:
        return None

    symbol = normalize_symbol(symbol)

    endpoints = [
        "/openApi/swap/v2/quote/openInterest",
        "/openApi/swap/v2/quote/openInterestStat",
    ]

    for endpoint in endpoints:
        data = get_json(
            f"{BINGX_BASE_URL}{endpoint}",
            params={"symbol": symbol}
        )

        if not data:
            continue

        value = extract_float_from_nested(
            data,
            ["openInterest", "open_interest", "sumOpenInterest", "value"]
        )

        if value is not None:
            return value

    return None


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []

    k = 2 / (period + 1)
    result = [values[0]]

    for price in values[1:]:
        result.append(price * k + result[-1] * (1 - k))

    return result


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(candles: List[dict], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )

        trs.append(tr)

    return sum(trs[-period:]) / period


def vwap_like(candles: List[dict], period: int = 48) -> Optional[float]:
    if len(candles) < period:
        return None

    total_pv = 0
    total_v = 0

    for c in candles[-period:]:
        typical = (c["high"] + c["low"] + c["close"]) / 3
        volume = c["volume"]

        total_pv += typical * volume
        total_v += volume

    if total_v == 0:
        return None

    return total_pv / total_v


def trend_state(candles: List[dict]) -> str:
    closes = [c["close"] for c in candles]

    if len(closes) < 200:
        return "NEUTRAL"

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


def volume_ratio(candles: List[dict], period: int = 30) -> float:
    if len(candles) < period + 1:
        return 0.0

    avg = sum(c["volume"] for c in candles[-period - 1:-1]) / period

    if avg <= 0:
        return 0.0

    return candles[-1]["volume"] / avg


def recent_move_percent(candles: List[dict], lookback: int = 8) -> float:
    if len(candles) < lookback + 1:
        return 0.0

    old_price = candles[-lookback]["close"]
    new_price = candles[-1]["close"]

    if old_price <= 0:
        return 0.0

    return (new_price - old_price) / old_price * 100


def distance_from_vwap_percent(price: float, vwap_value: float) -> float:
    if vwap_value <= 0:
        return 0.0

    return abs(price - vwap_value) / vwap_value * 100


def late_entry_blocked(direction: str, candles: List[dict], price: float, vwap_value: float) -> bool:
    if not ENABLE_LATE_ENTRY_FILTER:
        return False

    move = recent_move_percent(candles, lookback=8)
    vwap_distance = distance_from_vwap_percent(price, vwap_value)

    if direction == "LONG" and move > MAX_RECENT_MOVE_PERCENT:
        return True

    if direction == "SHORT" and move < -MAX_RECENT_MOVE_PERCENT:
        return True

    if vwap_distance > MAX_DISTANCE_FROM_VWAP_PERCENT:
        return True

    return False


def make_tp(entry: float, direction: str, position_percent: float) -> float:
    price_move = position_percent / LEVERAGE / 100

    if direction == "LONG":
        return entry * (1 + price_move)

    return entry * (1 - price_move)


def price_move_percent(entry: float, target: float, direction: str) -> float:
    if direction == "LONG":
        return (target - entry) / entry * 100

    return (entry - target) / entry * 100


def calc_risk_position(entry: float, sl: float) -> float:
    return abs(entry - sl) / entry * 100 * LEVERAGE


def calculate_position(entry: float, sl: float, deposit: float, risk_percent: float) -> dict:
    risk_amount = deposit * risk_percent / 100
    stop_distance = abs(entry - sl)

    if entry <= 0 or sl <= 0 or stop_distance <= 0:
        return {
            "risk_amount": round(risk_amount, 2),
            "position_size_usdt": None,
            "coin_amount": None,
            "margin_10x": None,
            "error": "Неверный entry или SL",
        }

    coin_amount = risk_amount / stop_distance
    position_size = coin_amount * entry

    return {
        "risk_amount": round(risk_amount, 2),
        "position_size_usdt": round(position_size, 2),
        "coin_amount": round(coin_amount, 8),
        "margin_10x": round(position_size / 10, 2),
        "error": None,
    }


def detect_btc_status() -> str:
    btc = get_klines("BTC-USDT", "1h", 260)

    if not btc:
        return "NEUTRAL"

    return trend_state(btc)


def momentum_confirm(c1: List[dict], c5: List[dict], direction: str) -> bool:
    if len(c1) < 20 or len(c5) < 20:
        return False

    closes1 = [c["close"] for c in c1]
    closes5 = [c["close"] for c in c5]

    ema9_1 = ema(closes1, 9)[-1]
    ema9_5 = ema(closes5, 9)[-1]

    last1 = c1[-1]
    prev1 = c1[-2]
    last5 = c5[-1]

    if direction == "LONG":
        return (
            last1["close"] > last1["open"]
            and last1["close"] > prev1["close"]
            and last1["close"] > ema9_1
            and last5["close"] > ema9_5
        )

    return (
        last1["close"] < last1["open"]
        and last1["close"] < prev1["close"]
        and last1["close"] < ema9_1
        and last5["close"] < ema9_5
    )


def analyze_funding_oi(symbol: str, direction: str) -> dict:
    funding = get_funding_rate(symbol)
    oi = get_open_interest(symbol)

    blocked = False
    score_adjustment = 0
    reason = []

    if funding is not None:
        if abs(funding) >= FUNDING_EXTREME_RATE:
            blocked = True
            reason.append(f"Funding экстремальный: {funding:.6f}")

        elif direction == "LONG" and funding > MAX_ABS_FUNDING_RATE:
            score_adjustment -= 5
            reason.append(f"Funding перегрет для LONG: {funding:.6f}")

        elif direction == "SHORT" and funding < -MAX_ABS_FUNDING_RATE:
            score_adjustment -= 5
            reason.append(f"Funding перегрет для SHORT: {funding:.6f}")

        else:
            score_adjustment += 2
            reason.append(f"Funding нормальный: {funding:.6f}")

    else:
        reason.append("Funding недоступен")

    if oi is not None:
        score_adjustment += 2
        reason.append(f"OI доступен: {round(oi, 2)}")
    else:
        reason.append("OI недоступен")

    return {
        "blocked": blocked,
        "score_adjustment": score_adjustment,
        "funding": funding,
        "open_interest": oi,
        "reason": "; ".join(reason),
    }


def parse_rss_headlines(url: str) -> List[str]:
    headlines = []

    try:
        response = requests.get(url.strip(), timeout=REQUEST_TIMEOUT)
        root = ET.fromstring(response.content)

        for item in root.findall(".//item")[:10]:
            title = item.findtext("title") or ""
            description = item.findtext("description") or ""

            text = f"{title} {description}".strip()

            if text:
                headlines.append(text)

    except Exception:
        pass

    return headlines


def analyze_news_filter(direction: str) -> dict:
    if not ENABLE_NEWS_FILTER:
        return {
            "risk": "OFF",
            "bias": "NEUTRAL",
            "blocked": False,
            "score_adjustment": 0,
            "headline": "",
        }

    cached = STATE.get("news", {})
    last_checked = cached.get("last_checked", 0)

    if now_ts() - last_checked < NEWS_CACHE_SECONDS:
        risk = cached.get("risk", "UNKNOWN")
        bias = cached.get("bias", "NEUTRAL")

        return {
            "risk": risk,
            "bias": bias,
            "blocked": risk == "HIGH",
            "score_adjustment": cached.get("score_adjustment", 0),
            "headline": cached.get("headline", ""),
        }

    bearish_words = [
        "sec", "lawsuit", "hack", "exploit", "outflow", "selloff", "crash",
        "liquidation", "fraud", "ban", "probe", "investigation", "hawkish",
        "inflation hotter", "rate hike", "recession", "risk-off", "default",
        "bankruptcy", "shutdown", "delist", "sanction"
    ]

    bullish_words = [
        "etf inflow", "approval", "rate cut", "dovish", "rally", "breakout",
        "institutional", "blackrock", "accumulation", "record inflow",
        "bullish", "adoption", "partnership", "treasury buying"
    ]

    high_risk_words = [
        "hack", "exploit", "sec lawsuit", "fraud", "bankruptcy", "liquidation",
        "ban", "investigation", "emergency", "crash"
    ]

    headlines = []

    for url in NEWS_RSS_URLS:
        headlines.extend(parse_rss_headlines(url))

    combined = " ".join(headlines).lower()

    risk = "LOW"
    bias = "NEUTRAL"
    score_adjustment = 0
    headline = headlines[0] if headlines else ""

    bearish_hits = sum(1 for word in bearish_words if word in combined)
    bullish_hits = sum(1 for word in bullish_words if word in combined)
    high_hits = sum(1 for word in high_risk_words if word in combined)

    if high_hits >= 1:
        risk = "HIGH"
        score_adjustment = -20

    elif bearish_hits >= 3 or bullish_hits >= 3:
        risk = "MEDIUM"

    if bullish_hits > bearish_hits:
        bias = "BULLISH"

    elif bearish_hits > bullish_hits:
        bias = "BEARISH"

    if direction == "LONG" and bias == "BULLISH" and risk != "HIGH":
        score_adjustment += 4

    if direction == "SHORT" and bias == "BEARISH" and risk != "HIGH":
        score_adjustment += 4

    if direction == "LONG" and bias == "BEARISH":
        score_adjustment -= 6

    if direction == "SHORT" and bias == "BULLISH":
        score_adjustment -= 6

    STATE["news"] = {
        "last_checked": now_ts(),
        "risk": risk,
        "bias": bias,
        "headline": headline,
        "score_adjustment": score_adjustment,
    }

    save_state(STATE)

    return {
        "risk": risk,
        "bias": bias,
        "blocked": risk == "HIGH",
        "score_adjustment": score_adjustment,
        "headline": headline,
    }


def build_signal(
    symbol: str,
    direction: str,
    strategy: str,
    entry: float,
    sl: float,
    score: int,
    vol_ratio: float,
    reason: str,
    deposit: float,
    risk_percent: float,
    extra_filters: dict
) -> Optional[dict]:
    risk_pos = calc_risk_position(entry, sl)

    if risk_pos > MAX_RISK_POSITION_PERCENT:
        return None

    if extra_filters.get("blocked"):
        return None

    score += extra_filters.get("score_adjustment", 0)

    tp1 = make_tp(entry, direction, TP1_POSITION_PERCENT)
    tp2 = make_tp(entry, direction, TP2_POSITION_PERCENT)
    tp3 = make_tp(entry, direction, TP3_POSITION_PERCENT)

    reward = price_move_percent(entry, tp1, direction)
    risk_price = abs(entry - sl) / entry * 100
    rr = reward / risk_price if risk_price > 0 else 0

    if rr < MIN_RR:
        return None

    if score < MIN_SCORE:
        return None

    signal_id = f"{normalize_symbol(symbol)}:{strategy}:{direction}:{round(entry, 8)}"

    if signal_id in STATE["sent_signals"]:
        return None

    pos = calculate_position(entry, sl, deposit, risk_percent)

    return {
        "id": signal_id,
        "symbol": normalize_symbol(symbol),
        "display_symbol": display_symbol(symbol),
        "direction": direction,
        "strategy": strategy,
        "status": "ACTIVE",
        "score": min(score, 95),
        "entry": round(entry, 8),
        "sl": round(sl, 8),
        "tp1": round(tp1, 8),
        "tp2": round(tp2, 8),
        "tp3": round(tp3, 8),
        "rr": round(rr, 2),
        "volume_ratio": round(vol_ratio, 2),
        "risk_position_percent": round(risk_pos, 2),
        "position": pos,
        "reason": reason,
        "filters": extra_filters,
        "created_at": now_ts(),
        "last_checked_time": int(now_ts() * 1000),
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "counted_positive": False,
        "counted_sl": False,
    }


def combine_extra_filters(symbol: str, direction: str) -> dict:
    funding_oi = analyze_funding_oi(symbol, direction)
    news = analyze_news_filter(direction)

    blocked = funding_oi.get("blocked", False) or news.get("blocked", False)

    score_adjustment = (
        funding_oi.get("score_adjustment", 0)
        + news.get("score_adjustment", 0)
    )

    return {
        "blocked": blocked,
        "score_adjustment": score_adjustment,
        "funding": funding_oi,
        "news": news,
    }


def evaluate_breakout(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    if not is_strategy_enabled("BREAKOUT_MOMENTUM"):
        return None

    closes15 = [c["close"] for c in c15]
    last = c15[-1]
    prev = c15[-2]
    price = last["close"]

    a = atr(c15)
    vw = vwap_like(c15)
    rs = rsi(closes15)
    vr = volume_ratio(c15)

    if a is None or vw is None or rs is None:
        return None

    if late_entry_blocked(direction, c15, price, vw):
        return None

    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    highs = [c["high"] for c in c15[-32:-2]]
    lows = [c["low"] for c in c15[-32:-2]]

    score = 60

    if vr >= MIN_VOLUME_RATIO:
        score += 10

    if vr >= 1.6:
        score += 5

    if momentum_confirm(c1, c5, direction):
        score += 10

    if direction == "LONG":
        level = max(highs)
        broke = price > level * 1.001 and prev["close"] <= level * 1.002

        if not broke:
            return None

        if btc_status == "BEARISH":
            return None

        if trend1h == "BEARISH":
            return None

        if price < vw * 0.995:
            return None

        if not (52 <= rs <= 82):
            return None

        if trend1h in ["BULLISH", "SOFT_BULLISH"]:
            score += 7

        if trend4h in ["BULLISH", "SOFT_BULLISH"]:
            score += 3

        sl = min(level - a * 0.18, min(c["low"] for c in c15[-8:]) - a * 0.05)

    else:
        level = min(lows)
        broke = price < level * 0.999 and prev["close"] >= level * 0.998

        if not broke:
            return None

        if btc_status == "BULLISH":
            return None

        if trend1h == "BULLISH":
            return None

        if price > vw * 1.005:
            return None

        if not (18 <= rs <= 48):
            return None

        if trend1h in ["BEARISH", "SOFT_BEARISH"]:
            score += 7

        if trend4h in ["BEARISH", "SOFT_BEARISH"]:
            score += 3

        sl = max(level + a * 0.18, max(c["high"] for c in c15[-8:]) + a * 0.05)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy="BREAKOUT_MOMENTUM",
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr,
        reason="Пробой уровня с объёмом и подтверждением 1m/5m.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=combine_extra_filters(symbol, direction),
    )


def evaluate_pullback(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    if not is_strategy_enabled("TREND_PULLBACK"):
        return None

    closes15 = [c["close"] for c in c15]
    last = c15[-1]
    prev = c15[-2]
    price = last["close"]

    a = atr(c15)
    vw = vwap_like(c15)
    rs = rsi(closes15)
    vr = volume_ratio(c15)

    if a is None or vw is None or rs is None:
        return None

    if late_entry_blocked(direction, c15, price, vw):
        return None

    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    score = 60

    if vr >= MIN_VOLUME_RATIO:
        score += 8

    if momentum_confirm(c1, c5, direction):
        score += 12

    if direction == "LONG":
        if btc_status == "BEARISH":
            return None

        if trend1h not in ["BULLISH", "SOFT_BULLISH"]:
            return None

        pulled_to_vwap = price >= vw * 0.985 and price <= vw * 1.015
        bounce = last["close"] > last["open"] and last["close"] > prev["close"]

        if not pulled_to_vwap or not bounce:
            return None

        if rs > 62:
            return None

        if trend4h in ["BULLISH", "SOFT_BULLISH"]:
            score += 8

        sl = min(last["low"] - a * 0.2, min(c["low"] for c in c15[-8:]) - a * 0.05)

    else:
        if btc_status == "BULLISH":
            return None

        if trend1h not in ["BEARISH", "SOFT_BEARISH"]:
            return None

        pulled_to_vwap = price >= vw * 0.985 and price <= vw * 1.015
        rejection = last["close"] < last["open"] and last["close"] < prev["close"]

        if not pulled_to_vwap or not rejection:
            return None

        if rs < 38:
            return None

        if trend4h in ["BEARISH", "SOFT_BEARISH"]:
            score += 8

        sl = max(last["high"] + a * 0.2, max(c["high"] for c in c15[-8:]) + a * 0.05)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy="TREND_PULLBACK",
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr,
        reason="Откат к VWAP по направлению 1h-тренда.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=combine_extra_filters(symbol, direction),
    )


def evaluate_sweep(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    if not is_strategy_enabled("SWEEP_RECLAIM"):
        return None

    closes15 = [c["close"] for c in c15]
    last = c15[-1]
    prev = c15[-2]
    price = last["close"]

    a = atr(c15)
    vw = vwap_like(c15)
    rs = rsi(closes15)
    vr = volume_ratio(c15)

    if a is None or vw is None or rs is None:
        return None

    if late_entry_blocked(direction, c15, price, vw):
        return None

    trend1h = trend_state(c1h)

    recent_high = max(c["high"] for c in c15[-28:-3])
    recent_low = min(c["low"] for c in c15[-28:-3])

    score = 62

    if vr >= MIN_VOLUME_RATIO:
        score += 8

    if momentum_confirm(c1, c5, direction):
        score += 12

    if direction == "LONG":
        swept = prev["low"] < recent_low * 0.998
        reclaimed = prev["close"] > recent_low
        confirm = last["close"] > last["open"] and last["close"] > prev["close"]

        if not swept or not reclaimed or not confirm:
            return None

        if btc_status == "BEARISH":
            return None

        if trend1h == "BEARISH":
            return None

        if price < vw * 0.975:
            return None

        if rs > 58:
            return None

        sl = min(prev["low"] - a * 0.08, min(c["low"] for c in c15[-8:]) - a * 0.05)

    else:
        swept = prev["high"] > recent_high * 1.002
        reclaimed = prev["close"] < recent_high
        confirm = last["close"] < last["open"] and last["close"] < prev["close"]

        if not swept or not reclaimed or not confirm:
            return None

        if btc_status == "BULLISH":
            return None

        if trend1h == "BULLISH":
            return None

        if price > vw * 1.025:
            return None

        if rs < 42:
            return None

        sl = max(prev["high"] + a * 0.08, max(c["high"] for c in c15[-8:]) + a * 0.05)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy="SWEEP_RECLAIM",
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr,
        reason="Снятие ликвидности за уровень и возврат обратно.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=combine_extra_filters(symbol, direction),
    )


def analyze_symbol(symbol: str, direction: Optional[str], deposit: float, risk_percent: float) -> Optional[dict]:
    symbol = normalize_symbol(symbol)

    if is_blocked(symbol) or is_on_cooldown(symbol):
        return None

    c15 = get_klines(symbol, "15m", 260)
    c5 = get_klines(symbol, "5m", 180)
    c1 = get_klines(symbol, "1m", 120)
    c1h = get_klines(symbol, "1h", 260)
    c4h = get_klines(symbol, "4h", 260)

    if not c15 or not c5 or not c1 or not c1h or not c4h:
        return None

    btc_status = detect_btc_status()

    normalized_direction = normalize_direction(direction)
    directions = [normalized_direction] if normalized_direction else ["LONG", "SHORT"]

    candidates = []

    for d in directions:
        if not is_side_enabled(d):
            continue

        for func in [evaluate_breakout, evaluate_pullback, evaluate_sweep]:
            signal = func(symbol, d, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent)

            if signal:
                candidates.append(signal)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (x["score"], x["rr"], x["volume_ratio"]),
        reverse=True
    )

    return candidates[0]


def build_message(signal: dict) -> str:
    arrow = "📈" if signal["direction"] == "LONG" else "📉"
    mode = "TEST" if TEST_MODE else "TRADE"

    strategy_names = {
        "BREAKOUT_MOMENTUM": "Пробой уровня",
        "TREND_PULLBACK": "Откат по тренду",
        "SWEEP_RECLAIM": "Снятие ликвидности"
    }

    strategy_text = strategy_names.get(signal["strategy"], signal["strategy"])

    pos = signal["position"]

    if pos.get("error"):
        risk_text = f"⚠️ Ошибка RM: {pos['error']}"
    else:
        risk_text = (
            f"Риск: {DEFAULT_RISK_PERCENT}% депозита\n"
            f"Размер позиции: {pos['position_size_usdt']} USDT\n"
            f"Маржа x10: {pos['margin_10x']} USDT"
        )

    filters = signal.get("filters", {})
    funding = filters.get("funding", {})
    news = filters.get("news", {})

    funding_text = funding.get("reason", "Funding/OI: нет данных")

    news_text = (
        f"Новости: риск {news.get('risk', 'UNKNOWN')}, "
        f"фон {news.get('bias', 'NEUTRAL')}"
    )

    if news.get("headline"):
        news_text += f"\nГлавная новость: {news.get('headline')[:180]}"

    return f"""
🎯 <b>{mode} SIGNAL</b>

{arrow} <b>{signal['direction']} {signal['display_symbol']}</b>

<b>Вход:</b> <code>{signal['entry']}</code>
<b>Stop Loss:</b> <code>{signal['sl']}</code>

<b>Take Profit:</b>
TP1: <code>{signal['tp1']}</code>
TP2: <code>{signal['tp2']}</code>
TP3: <code>{signal['tp3']}</code>

<b>Почему вход:</b>
{strategy_text}
{signal['reason']}

<b>Фильтры:</b>
{funding_text}
{news_text}

<b>Качество:</b> {signal['score']}/100
<b>RR до TP1:</b> {signal['rr']}
<b>Объём:</b> x{signal['volume_ratio']}
<b>Риск до SL:</b> {signal['risk_position_percent']}% по позиции

{risk_text}

<b>После TP1:</b>
Закрыть примерно {TP1_CLOSE_PERCENT:.0f}% позиции и перенести SL в безубыток.

⚠️ Не финансовый совет.
""".strip()


def send_telegram_message(text: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {
            "ok": False,
            "error": "TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не указаны"
        }

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
        }


def save_signal(signal: dict):
    STATE["active_signals"][signal["id"]] = signal
    STATE["sent_signals"][signal["id"]] = now_ts()
    set_cooldown(signal["symbol"])
    save_state(STATE)


def apply_result(signal: dict, result: str):
    side = signal["direction"]
    strategy = signal["strategy"]
    symbol = normalize_symbol(signal["symbol"])

    notes = []

    if result == "SL" and not signal.get("counted_sl"):
        signal["counted_sl"] = True

        STATE["stats"]["side"][side]["sl"] += 1
        STATE["stats"]["side"][side]["consecutive_sl"] += 1

        STATE["stats"]["strategy"][strategy]["sl"] += 1
        STATE["stats"]["strategy"][strategy]["consecutive_sl"] += 1

        STATE["stats"]["pair_sl"][symbol] = STATE["stats"]["pair_sl"].get(symbol, 0) + 1

        if STATE["stats"]["pair_sl"][symbol] >= PAIR_MAX_SL:
            STATE["blocked_symbols"][symbol] = now_ts() + PAIR_BLOCK_SECONDS
            notes.append(f"🚫 {display_symbol(symbol)} заблокирован на 24ч после SL.")

        if STATE["stats"]["side"][side]["consecutive_sl"] >= SIDE_MAX_CONSECUTIVE_SL:
            STATE["side_disabled_until"][side] = now_ts() + SIDE_DISABLE_SECONDS
            notes.append(f"⛔ {side} отключён на 6 часов после серии SL.")

        if STATE["stats"]["strategy"][strategy]["consecutive_sl"] >= STRATEGY_MAX_CONSECUTIVE_SL:
            STATE["strategy_disabled_until"][strategy] = now_ts() + STRATEGY_DISABLE_SECONDS
            notes.append(f"⛔ Стратегия {strategy} отключена на 6 часов после серии SL.")

    elif result in ["TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        STATE["stats"]["side"][side]["consecutive_sl"] = 0
        STATE["stats"]["strategy"][strategy]["consecutive_sl"] = 0

        if not signal.get("counted_positive"):
            signal["counted_positive"] = True
            STATE["stats"]["side"][side]["positive"] += 1
            STATE["stats"]["strategy"][strategy]["positive"] += 1
            STATE["stats"]["pair_positive"][symbol] = STATE["stats"]["pair_positive"].get(symbol, 0) + 1

        if result == "TP1":
            STATE["stats"]["side"][side]["tp1"] += 1

        if result == "TP2":
            STATE["stats"]["side"][side]["tp2"] += 1

        if result == "TP3":
            STATE["stats"]["side"][side]["tp3"] += 1

    save_state(STATE)
    return notes


def check_signal_hit(signal: dict, candles: List[dict]):
    side = signal["direction"]

    last_checked_time = signal.get("last_checked_time", 0)
    new_candles = [c for c in candles if c["time"] > last_checked_time]

    if not new_candles:
        return None, candles[-1]["close"]

    for c in new_candles:
        high = c["high"]
        low = c["low"]

        signal["last_checked_time"] = c["time"]

        if side == "LONG":
            if signal.get("tp2_hit") and low <= signal["entry"]:
                return "PROFIT_AFTER_TP2", signal["entry"]

            if signal.get("tp1_hit") and low <= signal["entry"]:
                return "PROFIT_AFTER_TP1", signal["entry"]

            if not signal.get("tp1_hit") and low <= signal["sl"]:
                return "SL", signal["sl"]

            if not signal.get("tp1_hit") and high >= signal["tp1"]:
                signal["tp1_hit"] = True
                return "TP1", signal["tp1"]

            if signal.get("tp1_hit") and not signal.get("tp2_hit") and high >= signal["tp2"]:
                signal["tp2_hit"] = True
                return "TP2", signal["tp2"]

            if signal.get("tp2_hit") and not signal.get("tp3_hit") and high >= signal["tp3"]:
                signal["tp3_hit"] = True
                return "TP3", signal["tp3"]

        else:
            if signal.get("tp2_hit") and high >= signal["entry"]:
                return "PROFIT_AFTER_TP2", signal["entry"]

            if signal.get("tp1_hit") and high >= signal["entry"]:
                return "PROFIT_AFTER_TP1", signal["entry"]

            if not signal.get("tp1_hit") and high >= signal["sl"]:
                return "SL", signal["sl"]

            if not signal.get("tp1_hit") and low <= signal["tp1"]:
                signal["tp1_hit"] = True
                return "TP1", signal["tp1"]

            if signal.get("tp1_hit") and not signal.get("tp2_hit") and low <= signal["tp2"]:
                signal["tp2_hit"] = True
                return "TP2", signal["tp2"]

            if signal.get("tp2_hit") and not signal.get("tp3_hit") and low <= signal["tp3"]:
                signal["tp3_hit"] = True
                return "TP3", signal["tp3"]

    return None, new_candles[-1]["close"]


def build_result_message(signal: dict, result: str, price: float, notes: List[str]) -> str:
    if result == "SL":
        title = "❌ Stop Loss"
        status_text = "SL сработал до TP1. Сделка отрицательная."
    elif result == "TP1":
        title = "✅ TP1 достигнут"
        status_text = f"Сделка позитивная. Закрыть ~{TP1_CLOSE_PERCENT:.0f}% позиции и SL в безубыток."
    elif result == "TP2":
        title = "✅ TP2 достигнут"
        status_text = "Хорошее движение. Сделка позитивная."
    elif result == "TP3":
        title = "🔥 TP3 достигнут"
        status_text = "Отличная сделка. Полная цель достигнута."
    elif result == "PROFIT_AFTER_TP1":
        title = "🟢 Возврат после TP1"
        status_text = "Цена вернулась после TP1, но сделка уже позитивная."
    elif result == "PROFIT_AFTER_TP2":
        title = "🟢 Возврат после TP2"
        status_text = "Цена вернулась после TP2, сделка позитивная."
    else:
        title = f"ℹ️ {result}"
        status_text = "Обновление по сделке."

    adaptive_text = ""

    if notes:
        adaptive_text = "\n\n<b>Адаптация бота:</b>\n" + "\n".join(notes)

    return f"""
{title}

<b>{signal['direction']} {signal['display_symbol']}</b>
Стратегия: <b>{signal['strategy']}</b>

Вход: <code>{signal['entry']}</code>
Текущая цена: <code>{round(price, 8)}</code>

TP1: <code>{signal['tp1']}</code>
TP2: <code>{signal['tp2']}</code>
TP3: <code>{signal['tp3']}</code>
SL: <code>{signal['sl']}</code>

{status_text}
{adaptive_text}
""".strip()


def scan_best_signal(deposit: float, risk_percent: float) -> dict:
    symbols = get_symbols()

    best = None
    checked = 0

    for symbol in symbols:
        checked += 1

        signal = analyze_symbol(symbol, None, deposit, risk_percent)

        if not signal:
            continue

        if best is None:
            best = signal
        else:
            current_key = (signal["score"], signal["rr"], signal["volume_ratio"])
            best_key = (best["score"], best["rr"], best["volume_ratio"])

            if current_key > best_key:
                best = signal

    if not best:
        return {
            "ok": False,
            "checked": checked,
            "message": "Сильных сигналов сейчас нет."
        }

    return {
        "ok": True,
        "checked": checked,
        "signal": best,
        "message": build_message(best),
    }


def track_active_signals(send_to_telegram: bool = True) -> dict:
    if not STATE["active_signals"]:
        return {
            "ok": True,
            "message": "Активных сигналов нет.",
            "results": [],
            "active_left": 0,
        }

    results = []
    finished = []

    for signal_id, signal in list(STATE["active_signals"].items()):
        candles = get_klines(signal["symbol"], "1m", 120)

        if not candles:
            continue

        result, price = check_signal_hit(signal, candles)
        STATE["active_signals"][signal_id] = signal

        if not result:
            continue

        notes = apply_result(signal, result)
        message = build_result_message(signal, result, price, notes)

        telegram = None

        if send_to_telegram:
            telegram = send_telegram_message(message)

        results.append({
            "signal_id": signal_id,
            "symbol": signal["display_symbol"],
            "direction": signal["direction"],
            "strategy": signal["strategy"],
            "result": result,
            "price": price,
            "telegram": telegram,
        })

        if result in ["SL", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
            finished.append(signal_id)

    for signal_id in finished:
        STATE["active_signals"].pop(signal_id, None)

    save_state(STATE)

    return {
        "ok": True,
        "checked": len(STATE["active_signals"]) + len(finished),
        "results": results,
        "active_left": len(STATE["active_signals"]),
    }


async def auto_worker():
    await asyncio.sleep(10)

    while True:
        try:
            current_time = now_ts()

            if AUTO_TRACK_ENABLED:
                last_track = STATE["auto"].get("last_track_time", 0)

                if current_time - last_track >= AUTO_TRACK_SECONDS:
                    result = track_active_signals(send_to_telegram=True)
                    STATE["auto"]["last_track_time"] = current_time
                    STATE["auto"]["last_track_result"] = result
                    save_state(STATE)

            if AUTO_SCAN_ENABLED:
                last_scan = STATE["auto"].get("last_scan_time", 0)

                if current_time - last_scan >= AUTO_SCAN_SECONDS:
                    result = scan_best_signal(DEFAULT_DEPOSIT, DEFAULT_RISK_PERCENT)
                    STATE["auto"]["last_scan_time"] = current_time
                    STATE["auto"]["last_scan_result"] = result

                    if result.get("ok"):
                        signal = result["signal"]
                        message = result["message"]

                        telegram = send_telegram_message(message)
                        result["telegram"] = telegram

                        save_signal(signal)

                    save_state(STATE)

            await asyncio.sleep(15)

        except Exception as e:
            STATE["auto"]["last_error"] = str(e)
            save_state(STATE)
            await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    text = (
        "✅ Professional Adaptive Futures Bot AUTO V3 PRO запущен.\n\n"
        f"Режим: {'TEST' if TEST_MODE else 'TRADE'}\n"
        f"Auto Scan: {'ON' if AUTO_SCAN_ENABLED else 'OFF'}\n"
        f"Auto Track: {'ON' if AUTO_TRACK_ENABLED else 'OFF'}\n"
        f"News Filter: {'ON' if ENABLE_NEWS_FILTER else 'OFF'}\n"
        f"Funding Filter: {'ON' if ENABLE_FUNDING_FILTER else 'OFF'}\n"
        f"OI Filter: {'ON' if ENABLE_OI_FILTER else 'OFF'}\n"
        f"Late Entry Filter: {'ON' if ENABLE_LATE_ENTRY_FILTER else 'OFF'}\n"
        f"Scan interval: {AUTO_SCAN_SECONDS} сек.\n"
        f"Track interval: {AUTO_TRACK_SECONDS} сек.\n\n"
        "Бот будет сам искать сигналы, фильтровать новости/funding/OI и отслеживать TP/SL."
    )

    send_telegram_message(text)

    asyncio.create_task(auto_worker())


@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Professional Adaptive Futures Bot AUTO V3 PRO</title>
</head>
<body style="background:#020617;color:#e5e7eb;font-family:Arial;padding:40px;">
    <h1>✅ Professional Adaptive Futures Bot AUTO V3 PRO работает</h1>
    <pre>
GET /health
GET /scan?send_to_telegram=false
GET /auto-signal?symbol=NEAR/USDT
GET /track
GET /stats
GET /auto-status
GET /news-status
GET /test-telegram
GET /reset-state
    </pre>
    <p>Start Command:</p>
    <pre>uvicorn bot:app --host 0.0.0.0 --port $PORT</pre>
</body>
</html>
"""


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Professional Adaptive Futures Bot AUTO V3 PRO",
        "test_mode": TEST_MODE,
        "min_score": MIN_SCORE,
        "min_volume_ratio": MIN_VOLUME_RATIO,
        "news_filter": ENABLE_NEWS_FILTER,
        "funding_filter": ENABLE_FUNDING_FILTER,
        "oi_filter": ENABLE_OI_FILTER,
        "late_entry_filter": ENABLE_LATE_ENTRY_FILTER,
        "auto_scan_enabled": AUTO_SCAN_ENABLED,
        "auto_track_enabled": AUTO_TRACK_ENABLED,
        "auto_scan_seconds": AUTO_SCAN_SECONDS,
        "auto_track_seconds": AUTO_TRACK_SECONDS,
        "active_signals": len(STATE["active_signals"]),
        "blocked_symbols": len(STATE["blocked_symbols"]),
    }


@app.get("/auto-status")
def auto_status():
    return {
        "ok": True,
        "auto": STATE.get("auto", {}),
        "active_signals": len(STATE["active_signals"]),
        "blocked_symbols": len(STATE["blocked_symbols"]),
    }


@app.get("/news-status")
def news_status():
    return {
        "ok": True,
        "news": STATE.get("news", {}),
        "rss_urls": NEWS_RSS_URLS,
    }


@app.get("/test-telegram")
def test_telegram():
    return send_telegram_message("✅ Professional Adaptive Futures Bot AUTO V3 PRO подключён к Telegram.")


@app.get("/auto-signal")
def auto_signal(
    symbol: str = Query(default="NEAR/USDT"),
    direction: Optional[str] = Query(default=None),
    deposit: float = Query(default=DEFAULT_DEPOSIT),
    risk_percent: float = Query(default=DEFAULT_RISK_PERCENT),
    send_to_telegram: bool = Query(default=False)
):
    signal = analyze_symbol(symbol, direction, deposit, risk_percent)

    if not signal:
        return {
            "ok": False,
            "symbol": display_symbol(symbol),
            "direction": direction,
            "message": "Сильного сигнала нет. Вход запрещён."
        }

    message = build_message(signal)

    telegram = None

    if send_to_telegram:
        telegram = send_telegram_message(message)
        save_signal(signal)

    return {
        "ok": True,
        "signal": signal,
        "message": message,
        "telegram": telegram,
    }


@app.get("/scan")
def scan(
    send_to_telegram: bool = Query(default=False),
    deposit: float = Query(default=DEFAULT_DEPOSIT),
    risk_percent: float = Query(default=DEFAULT_RISK_PERCENT)
):
    result = scan_best_signal(deposit, risk_percent)

    if not result.get("ok"):
        return result

    telegram = None

    if send_to_telegram:
        telegram = send_telegram_message(result["message"])
        save_signal(result["signal"])

    result["telegram"] = telegram
    return result


@app.get("/track")
def track(send_to_telegram: bool = Query(default=True)):
    return track_active_signals(send_to_telegram=send_to_telegram)


@app.get("/stats")
def stats():
    return {
        "ok": True,
        "stats": STATE["stats"],
        "active_signals": len(STATE["active_signals"]),
        "blocked_symbols": {
            display_symbol(k): int(v - now_ts())
            for k, v in STATE["blocked_symbols"].items()
            if v > now_ts()
        },
        "side_disabled_until": STATE["side_disabled_until"],
        "strategy_disabled_until": STATE["strategy_disabled_until"],
    }


@app.get("/reset-state")
def reset_state():
    global STATE
    STATE = default_state()
    save_state(STATE)

    return {
        "ok": True,
        "message": "State reset completed."
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("bot:app", host="0.0.0.0", port=port)
