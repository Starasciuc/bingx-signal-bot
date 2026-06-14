import os
import time
import json
import random
import asyncio
import requests
from threading import Lock
from typing import Optional, List, Any

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse


APP_NAME = "Professional Futures Bot V11 Market Regime Trader"
DEPLOY_MARKER = "V11_MARKET_REGIME_ENGINE_2026_06_14"

app = FastAPI(title=APP_NAME)

# =========================
# ENV / SETTINGS
# =========================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

API_KEY = os.getenv("API_KEY", "")

BINGX_BASE_URL = "https://open-api.bingx.com"
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")
STATE_LOCK = Lock()

TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
LEVERAGE = int(os.getenv("LEVERAGE", "10"))

DEFAULT_DEPOSIT = float(os.getenv("DEFAULT_DEPOSIT", "1000"))
DEFAULT_RISK_PERCENT = float(os.getenv("DEFAULT_RISK_PERCENT", "0.5"))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "180"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))

AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "90"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "45"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "900"))
SIGNAL_MAX_LIFETIME_SECONDS = int(os.getenv("SIGNAL_MAX_LIFETIME_SECONDS", "21600"))
SENT_SIGNALS_KEEP_SECONDS = int(os.getenv("SENT_SIGNALS_KEEP_SECONDS", "1209600"))

SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK = os.getenv("SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK", "true").lower() == "true"
DEBUG_NO_SIGNAL_REPORT_ENABLED = os.getenv("DEBUG_NO_SIGNAL_REPORT_ENABLED", "false").lower() == "true"
DEBUG_NO_SIGNAL_REPORT_SECONDS = int(os.getenv("DEBUG_NO_SIGNAL_REPORT_SECONDS", "10800"))

# Trading costs
FEE_RATE = float(os.getenv("FEE_RATE", "0.0005"))
SLIPPAGE_RATE = float(os.getenv("SLIPPAGE_RATE", "0.0003"))

# Signal quality
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "88"))
A_PLUS_MIN_RR = float(os.getenv("A_PLUS_MIN_RR", "0.95"))
A_PLUS_MIN_VOLUME_RATIO = float(os.getenv("A_PLUS_MIN_VOLUME_RATIO", "0.90"))

WEAK_MIN_SCORE = int(os.getenv("WEAK_MIN_SCORE", "82"))
WEAK_MIN_RR = float(os.getenv("WEAK_MIN_RR", "0.75"))
WEAK_MIN_VOLUME_RATIO = float(os.getenv("WEAK_MIN_VOLUME_RATIO", "0.75"))
ALLOW_WEAK_SIGNALS = os.getenv("ALLOW_WEAK_SIGNALS", "false").lower() == "true"

MIN_TARGET_ROI_PERCENT = float(os.getenv("MIN_TARGET_ROI_PERCENT", "10"))

# Risk multipliers
STRONG_RISK_MULTIPLIER = float(os.getenv("STRONG_RISK_MULTIPLIER", "0.35"))
WEAK_RISK_MULTIPLIER = float(os.getenv("WEAK_RISK_MULTIPLIER", "0.10"))
FAST_RISK_MULTIPLIER = float(os.getenv("FAST_RISK_MULTIPLIER", "0.12"))
STRUCTURE_RISK_MULTIPLIER = float(os.getenv("STRUCTURE_RISK_MULTIPLIER", "0.18"))
EXTREME_RISK_MULTIPLIER = float(os.getenv("EXTREME_RISK_MULTIPLIER", "0.06"))

# Max risk by position ROI to SL at leverage
FAST_MAX_RISK_POSITION_PERCENT = float(os.getenv("FAST_MAX_RISK_POSITION_PERCENT", "14"))
STRUCTURE_MAX_RISK_POSITION_PERCENT = float(os.getenv("STRUCTURE_MAX_RISK_POSITION_PERCENT", "42"))
SWING_MAX_RISK_POSITION_PERCENT = float(os.getenv("SWING_MAX_RISK_POSITION_PERCENT", "85"))
EXTREME_MAX_RISK_POSITION_PERCENT = float(os.getenv("EXTREME_MAX_RISK_POSITION_PERCENT", "20"))

# TP ROI targets
FAST_TP1_ROI = float(os.getenv("FAST_TP1_ROI", "10"))
FAST_TP2_ROI = float(os.getenv("FAST_TP2_ROI", "18"))
FAST_TP3_ROI = float(os.getenv("FAST_TP3_ROI", "28"))

TREND_TP1_ROI = float(os.getenv("TREND_TP1_ROI", "10"))
TREND_TP2_ROI = float(os.getenv("TREND_TP2_ROI", "24"))
TREND_TP3_ROI = float(os.getenv("TREND_TP3_ROI", "40"))

RANGE_TP1_ROI = float(os.getenv("RANGE_TP1_ROI", "10"))
RANGE_TP2_ROI = float(os.getenv("RANGE_TP2_ROI", "22"))
RANGE_TP3_ROI = float(os.getenv("RANGE_TP3_ROI", "38"))

EXTREME_TP1_ROI = float(os.getenv("EXTREME_TP1_ROI", "10"))
EXTREME_TP2_ROI = float(os.getenv("EXTREME_TP2_ROI", "18"))
EXTREME_TP3_ROI = float(os.getenv("EXTREME_TP3_ROI", "30"))

TP1_CLOSE_PERCENT = float(os.getenv("TP1_CLOSE_PERCENT", "70"))

# Filters
ENABLE_FUNDING_FILTER = os.getenv("ENABLE_FUNDING_FILTER", "true").lower() == "true"
ENABLE_OI_FILTER = os.getenv("ENABLE_OI_FILTER", "true").lower() == "true"

MAX_ABS_FUNDING_RATE = float(os.getenv("MAX_ABS_FUNDING_RATE", "0.0010"))
FUNDING_EXTREME_RATE = float(os.getenv("FUNDING_EXTREME_RATE", "0.0020"))

QUALITY_ONLY_MODE = os.getenv("QUALITY_ONLY_MODE", "true").lower() == "true"

ULTRA_VOLATILITY_GUARD_ENABLED = os.getenv("ULTRA_VOLATILITY_GUARD_ENABLED", "true").lower() == "true"
ALLOW_ULTRA_RISKY_SYMBOLS = os.getenv("ALLOW_ULTRA_RISKY_SYMBOLS", "false").lower() == "true"

MAX_5M_CANDLE_RANGE_NORMAL = float(os.getenv("MAX_5M_CANDLE_RANGE_NORMAL", "4.8"))
MAX_15M_BLOCK_RANGE_NORMAL = float(os.getenv("MAX_15M_BLOCK_RANGE_NORMAL", "9.5"))
MAX_ATR5_PERCENT_NORMAL = float(os.getenv("MAX_ATR5_PERCENT_NORMAL", "1.85"))
MAX_ATR15_PERCENT_NORMAL = float(os.getenv("MAX_ATR15_PERCENT_NORMAL", "3.20"))

MIN_QUOTE_VOLUME_NORMAL_USDT = float(os.getenv("MIN_QUOTE_VOLUME_NORMAL_USDT", "1200000"))
MIN_DYNAMIC_QUOTE_VOLUME_USDT = float(os.getenv("MIN_DYNAMIC_QUOTE_VOLUME_USDT", "2000000"))

EXTREME_DYNAMIC_ENABLED = os.getenv("EXTREME_DYNAMIC_ENABLED", "true").lower() == "true"
EXTREME_DYNAMIC_TOP_N = int(os.getenv("EXTREME_DYNAMIC_TOP_N", "120"))
EXTREME_DYNAMIC_MIN_CHANGE_PERCENT = float(os.getenv("EXTREME_DYNAMIC_MIN_CHANGE_PERCENT", "4.5"))

ALLOW_RISKY_EXTREME_TRADES = os.getenv("ALLOW_RISKY_EXTREME_TRADES", "false").lower() == "true"
ALLOW_RISKY_EXTREME_LONGS = os.getenv("ALLOW_RISKY_EXTREME_LONGS", "false").lower() == "true"
RISKY_EXTREME_MIN_CHANGE_PERCENT = float(os.getenv("RISKY_EXTREME_MIN_CHANGE_PERCENT", "18.0"))
RISKY_EXTREME_MIN_VOLUME_RATIO = float(os.getenv("RISKY_EXTREME_MIN_VOLUME_RATIO", "1.35"))

PAIR_BLOCK_SECONDS = int(os.getenv("PAIR_BLOCK_SECONDS", "43200"))
PAIR_MAX_SL = int(os.getenv("PAIR_MAX_SL", "2"))
STRATEGY_SIDE_DISABLE_SECONDS = int(os.getenv("STRATEGY_SIDE_DISABLE_SECONDS", "604800"))
STRATEGY_SIDE_MAX_CONSECUTIVE_SL = int(os.getenv("STRATEGY_SIDE_MAX_CONSECUTIVE_SL", "2"))
PRO_MIN_TRADES_TO_BLOCK = int(os.getenv("PRO_MIN_TRADES_TO_BLOCK", "3"))
PRO_MIN_WR_TO_ALLOW = float(os.getenv("PRO_MIN_WR_TO_ALLOW", "40"))


QUALITY_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "LTC", "BCH",
    "DOT", "TRX", "NEAR", "APT", "SUI", "SEI", "INJ", "AAVE", "UNI", "ATOM", "FIL",
    "ETC", "OP", "ARB", "TON", "ICP", "RNDR", "FET", "IMX", "AR", "MKR", "LDO",
    "CRV", "ENA", "JUP", "PYTH", "STRK", "DYDX", "RUNE", "TIA", "STX", "COMP", "TAO",
    "WLD", "JTO", "GALA", "APE", "SNX"
}

RISKY_BASES = {
    "VELVET", "BEAT", "COLLECT", "SPACE", "GOBLIN", "MAGMA", "FOLKS", "FIGHT",
    "HMSTR", "GUA", "DOGS", "CATI", "MEME", "NOT", "1000SATS", "1000PEPE",
    "PEPE", "BONK", "WIF", "PNUT", "ACT", "GOAT", "MOODENG", "NEIRO",
    "TURBO", "BOME", "HYPE", "OPN", "WLFI", "PUMP", "AERO", "MANTA", "ARKM",
    "SIREN", "MAVIA"
}

EXTREME_BASES = RISKY_BASES | {
    "TAO", "SEI", "INJ", "SUI", "APT", "WLD", "ENA", "JUP", "PYTH"
}

STRATEGIES = [
    "FAST_REACTION_PRO",
    "TREND_CONTINUATION_PRO",
    "RANGE_STRUCTURE_PRO",
    "STRUCTURE_SWING_PRO",
    "EXTREME_CONTEXT_PRO",
]


# =========================
# SECURITY
# =========================

def require_api_key(key: str):
    if API_KEY and key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")


# =========================
# STATE
# =========================

def now_ts() -> float:
    return time.time()


def strategy_side_default():
    return {f"{s}:{side}": 0 for s in STRATEGIES for side in ["LONG", "SHORT"]}


def strategy_side_stats_default():
    return {
        f"{s}:{side}": {"positive": 0, "sl": 0, "consecutive_sl": 0}
        for s in STRATEGIES
        for side in ["LONG", "SHORT"]
    }


def default_state():
    return {
        "active_signals": {},
        "sent_signals": {},
        "symbol_cooldown": {},
        "blocked_symbols": {},
        "strategy_side_hard_disabled_until": strategy_side_default(),
        "stats": {
            "side": {
                "LONG": {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0},
                "SHORT": {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0},
            },
            "strategy": {
                s: {"positive": 0, "sl": 0, "consecutive_sl": 0}
                for s in STRATEGIES
            },
            "strategy_side": strategy_side_stats_default(),
            "grade": {
                "STRONG": {"positive": 0, "sl": 0},
                "WEAK": {"positive": 0, "sl": 0},
            },
            "pair_sl": {},
            "pair_positive": {},
        },
        "auto": {
            "last_scan_time": 0,
            "last_track_time": 0,
            "last_scan_result": None,
            "last_track_result": None,
            "last_no_signal_report_time": 0,
            "last_error": None,
        }
    }


def ensure_state_structure(state: dict) -> dict:
    base = default_state()

    for key, value in base.items():
        if key not in state:
            state[key] = value

    if "stats" not in state:
        state["stats"] = base["stats"]

    for key, value in base["stats"].items():
        if key not in state["stats"]:
            state["stats"][key] = value

    for strategy in STRATEGIES:
        if strategy not in state["stats"]["strategy"]:
            state["stats"]["strategy"][strategy] = {"positive": 0, "sl": 0, "consecutive_sl": 0}

        for side in ["LONG", "SHORT"]:
            key = f"{strategy}:{side}"

            if key not in state["strategy_side_hard_disabled_until"]:
                state["strategy_side_hard_disabled_until"][key] = 0

            if key not in state["stats"]["strategy_side"]:
                state["stats"]["strategy_side"][key] = {"positive": 0, "sl": 0, "consecutive_sl": 0}

    for side in ["LONG", "SHORT"]:
        if side not in state["stats"]["side"]:
            state["stats"]["side"][side] = {
                "positive": 0,
                "sl": 0,
                "consecutive_sl": 0,
                "tp1": 0,
                "tp2": 0,
                "tp3": 0,
            }

    for grade in ["STRONG", "WEAK"]:
        if grade not in state["stats"]["grade"]:
            state["stats"]["grade"][grade] = {"positive": 0, "sl": 0}

    if "pair_sl" not in state["stats"]:
        state["stats"]["pair_sl"] = {}

    if "pair_positive" not in state["stats"]:
        state["stats"]["pair_positive"] = {}

    return state


def load_state():
    if not os.path.exists(STATE_FILE):
        return default_state()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        return ensure_state_structure(state)
    except Exception:
        return default_state()


def save_state(state):
    try:
        with STATE_LOCK:
            ensure_state_structure(state)
            tmp_file = STATE_FILE + ".tmp"

            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)

            os.replace(tmp_file, STATE_FILE)
    except Exception:
        pass


STATE = load_state()


def ensure_stats_structure():
    global STATE
    STATE = ensure_state_structure(STATE)


def cleanup_state():
    current_time = now_ts()

    for signal_id, ts in list(STATE.get("sent_signals", {}).items()):
        if current_time - ts > SENT_SIGNALS_KEEP_SECONDS:
            STATE["sent_signals"].pop(signal_id, None)

    for symbol, until in list(STATE.get("blocked_symbols", {}).items()):
        if current_time > until:
            STATE["blocked_symbols"].pop(symbol, None)

    for symbol, ts in list(STATE.get("symbol_cooldown", {}).items()):
        if current_time - ts > SIGNAL_COOLDOWN_SECONDS * 3:
            STATE["symbol_cooldown"].pop(symbol, None)

    save_state(STATE)


# =========================
# BASIC HELPERS
# =========================

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


def is_risky_base(base: str) -> bool:
    return base.upper() in RISKY_BASES


def is_quality_base(base: str) -> bool:
    return base.upper() in QUALITY_BASES


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

    if not symbol.endswith("-USDT"):
        return False

    bad_exact = {"USDC", "BUSD", "FDUSD", "TUSD", "USD", "EUR", "GBP", "JPY"}
    bad_fragments = ["AAPL", "TSLA", "NVDA", "META", "GOOG", "AMZN", "MSFT"]

    if base in bad_exact:
        return False

    if any(x in base for x in bad_fragments):
        return False

    if len(base) < 2 or len(base) > 18:
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


def calc_winrate(positive: int, sl: int) -> float:
    total = positive + sl
    if total <= 0:
        return 0.0
    return round(positive / total * 100, 1)


def get_strategy_side_winrate(strategy: str, direction: str) -> tuple:
    ensure_stats_structure()
    key = f"{strategy}:{direction}"
    s = STATE.get("stats", {}).get("strategy_side", {}).get(key, {})
    positive = int(s.get("positive", 0))
    sl = int(s.get("sl", 0))
    trades = positive + sl
    wr = calc_winrate(positive, sl)
    return trades, wr


def is_statistically_bad_strategy_side(strategy: str, direction: str) -> bool:
    trades, wr = get_strategy_side_winrate(strategy, direction)
    return trades >= PRO_MIN_TRADES_TO_BLOCK and wr < PRO_MIN_WR_TO_ALLOW


def is_strategy_side_enabled(strategy: str, side: str) -> bool:
    ensure_stats_structure()

    if is_statistically_bad_strategy_side(strategy, side):
        return False

    key = f"{strategy}:{side}"
    return now_ts() >= STATE["strategy_side_hard_disabled_until"].get(key, 0)


# =========================
# HTTP / MARKET DATA
# =========================

def get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception:
        return None


def _extract_change_percent(item: dict) -> Optional[float]:
    for key in ["priceChangePercent", "priceChangeRate", "changePercent", "change", "changeRate", "riseFallRate"]:
        if key in item:
            try:
                value = float(item.get(key))
                if abs(value) <= 2:
                    value *= 100
                return value
            except Exception:
                pass
    return None


def _extract_quote_volume_usdt(item: dict) -> Optional[float]:
    for key in ["quoteVolume", "quoteVol", "turnover", "volumeUSDT", "amount", "q"]:
        if key in item:
            try:
                return float(item.get(key) or 0)
            except Exception:
                pass
    return None


def get_dynamic_extreme_symbols(limit: int = None) -> List[str]:
    if not EXTREME_DYNAMIC_ENABLED:
        return []

    limit = limit or EXTREME_DYNAMIC_TOP_N

    endpoints = [
        "/openApi/swap/v2/quote/ticker",
        "/openApi/swap/v2/quote/ticker/24hr",
    ]

    movers = []

    for endpoint in endpoints:
        data = get_json(f"{BINGX_BASE_URL}{endpoint}")
        raw = data.get("data", []) if isinstance(data, dict) else []

        if isinstance(raw, dict):
            raw = list(raw.values()) if not raw.get("symbol") else [raw]

        if not raw:
            continue

        for item in raw:
            if not isinstance(item, dict):
                continue

            symbol = item.get("symbol") or item.get("s")

            if not symbol:
                continue

            symbol = normalize_symbol(symbol)

            if not is_good_symbol(symbol):
                continue

            change = _extract_change_percent(item)

            if change is None:
                continue

            quote_vol = _extract_quote_volume_usdt(item)
            base = base_from_symbol(symbol)

            if quote_vol is not None and quote_vol > 0:
                if quote_vol < MIN_DYNAMIC_QUOTE_VOLUME_USDT:
                    continue

            min_change = RISKY_EXTREME_MIN_CHANGE_PERCENT if is_risky_base(base) else EXTREME_DYNAMIC_MIN_CHANGE_PERCENT

            if is_risky_base(base) and not ALLOW_RISKY_EXTREME_TRADES:
                continue

            if abs(change) >= min_change:
                movers.append((abs(change), symbol))

        if movers:
            break

    movers.sort(reverse=True)

    result = []

    for _, symbol in movers:
        if symbol not in result:
            result.append(symbol)

        if len(result) >= limit:
            break

    return result


def get_symbols() -> List[str]:
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts"
    data = get_json(url)

    if not data:
        return []

    all_symbols = []
    priority_symbols = []
    dynamic_extremes = get_dynamic_extreme_symbols()

    for item in data.get("data", []):
        symbol = item.get("symbol")

        if not symbol:
            continue

        symbol = normalize_symbol(symbol)

        if not is_good_symbol(symbol):
            continue

        base = base_from_symbol(symbol)
        all_symbols.append(symbol)

        is_priority = (
            is_quality_base(base)
            or symbol in dynamic_extremes
            or base in EXTREME_BASES
        )

        if is_priority:
            priority_symbols.append(symbol)

    random.shuffle(all_symbols)

    result = []

    source_symbols = dynamic_extremes + priority_symbols

    if not QUALITY_ONLY_MODE:
        source_symbols += all_symbols

    for symbol in source_symbols:
        if symbol not in result:
            result.append(symbol)

        if len(result) >= MAX_SYMBOLS:
            break

    return result


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

    if len(candles) < 50:
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

    endpoints = [
        "/openApi/swap/v2/quote/premiumIndex",
        "/openApi/swap/v2/quote/fundingRate",
    ]

    for endpoint in endpoints:
        data = get_json(f"{BINGX_BASE_URL}{endpoint}", params={"symbol": normalize_symbol(symbol)})

        if not data:
            continue

        value = extract_float_from_nested(data, ["lastFundingRate", "fundingRate", "funding_rate", "rate"])

        if value is not None:
            return value

    return None


def get_open_interest(symbol: str) -> Optional[float]:
    if not ENABLE_OI_FILTER:
        return None

    endpoints = [
        "/openApi/swap/v2/quote/openInterest",
        "/openApi/swap/v2/quote/openInterestStat",
    ]

    for endpoint in endpoints:
        data = get_json(f"{BINGX_BASE_URL}{endpoint}", params={"symbol": normalize_symbol(symbol)})

        if not data:
            continue

        value = extract_float_from_nested(data, ["openInterest", "open_interest", "sumOpenInterest", "value"])

        if value is not None:
            return value

    return None


# =========================
# INDICATORS
# =========================

def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []

    k = 2 / (period + 1)
    result = [values[0]]

    for price in values[1:]:
        result.append(price * k + result[-1] * (1 - k))

    return result


def ema_value(candles: List[dict], period: int) -> Optional[float]:
    values = [c["close"] for c in candles]

    if len(values) < period:
        return None

    return ema(values, period)[-1]


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


def candle_body(c: dict) -> float:
    return abs(c.get("close", 0) - c.get("open", 0))


def candle_range(c: dict) -> float:
    return max(c.get("high", 0) - c.get("low", 0), 0)


def candle_range_percent(c: dict) -> float:
    close = c.get("close", 0)

    if close <= 0:
        return 0.0

    return (c.get("high", 0) - c.get("low", 0)) / close * 100


def candle_close_position(c: dict) -> float:
    high = c.get("high", 0)
    low = c.get("low", 0)
    close = c.get("close", 0)
    rng = high - low

    if rng <= 0:
        return 0.5

    return (close - low) / rng


def has_exhaustion_rejection(candle: dict, direction: str) -> bool:
    rng = candle_range(candle)

    if rng <= 0:
        return False

    body = candle_body(candle)
    upper_wick = candle.get("high", 0) - max(candle.get("open", 0), candle.get("close", 0))
    lower_wick = min(candle.get("open", 0), candle.get("close", 0)) - candle.get("low", 0)
    close_pos = candle_close_position(candle)

    if direction == "LONG":
        return upper_wick > max(body * 1.4, rng * 0.28) and close_pos < 0.62

    return lower_wick > max(body * 1.4, rng * 0.28) and close_pos > 0.38


def confirmed_5m_followthrough(c5: List[dict], direction: str) -> bool:
    if len(c5) < 4:
        return False

    last = c5[-1]
    prev = c5[-2]
    before = c5[-3]

    if has_exhaustion_rejection(last, direction):
        return False

    if direction == "LONG":
        return (
            last["close"] > last["open"]
            and last["close"] > prev["close"]
            and prev["close"] >= before["close"] * 0.997
        )

    return (
        last["close"] < last["open"]
        and last["close"] < prev["close"]
        and prev["close"] <= before["close"] * 1.003
    )


def confirmed_15m_direction(c15: List[dict], direction: str) -> bool:
    if len(c15) < 3:
        return False

    last = c15[-1]
    prev = c15[-2]

    if direction == "LONG":
        return last["close"] >= prev["close"] * 0.997 or last["close"] > last["open"]

    return last["close"] <= prev["close"] * 1.003 or last["close"] < last["open"]


def recent_failed_push(c5: List[dict], direction: str) -> bool:
    if len(c5) < 10:
        return False

    recent = c5[-8:]
    price = c5[-1]["close"]
    first = recent[0]["close"]

    if first <= 0:
        return False

    move = (price - first) / first * 100
    last = c5[-1]
    prev = c5[-2]

    if direction == "LONG":
        return move > 0.9 and (has_exhaustion_rejection(last, "LONG") or last["close"] < prev["close"])

    return move < -0.9 and (has_exhaustion_rejection(last, "SHORT") or last["close"] > prev["close"])


# =========================
# FILTERS
# =========================

def market_too_violent(symbol: str, c5: List[dict], c15: List[dict]) -> Optional[str]:
    if not ULTRA_VOLATILITY_GUARD_ENABLED or ALLOW_ULTRA_RISKY_SYMBOLS:
        return None

    if len(c5) < 30 or len(c15) < 20:
        return None

    base = base_from_symbol(symbol)
    price = c5[-1]["close"]

    if price <= 0:
        return "bad_price"

    if is_risky_base(base):
        return f"risky_base:{base}"

    a5 = atr(c5)
    a15 = atr(c15)

    atr5p = (a5 / price * 100) if a5 else 0.0
    atr15p = (a15 / price * 100) if a15 else 0.0

    max5 = max(candle_range_percent(c) for c in c5[-8:])

    block15_high = max(c["high"] for c in c15[-4:])
    block15_low = min(c["low"] for c in c15[-4:])
    block15 = (block15_high - block15_low) / price * 100 if price > 0 else 0

    if max5 >= MAX_5M_CANDLE_RANGE_NORMAL:
        return f"5m_range_{round(max5, 2)}%"

    if block15 >= MAX_15M_BLOCK_RANGE_NORMAL:
        return f"15m_block_{round(block15, 2)}%"

    if atr5p >= MAX_ATR5_PERCENT_NORMAL:
        return f"atr5_{round(atr5p, 2)}%"

    if atr15p >= MAX_ATR15_PERCENT_NORMAL:
        return f"atr15_{round(atr15p, 2)}%"

    return None


def detect_btc_status() -> str:
    btc = get_klines("BTC-USDT", "1h", 260)

    if not btc:
        return "NEUTRAL"

    return trend_state(btc)


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
            score_adjustment -= 3
            reason.append(f"Funding перегрет для LONG: {funding:.6f}")

        elif direction == "SHORT" and funding < -MAX_ABS_FUNDING_RATE:
            score_adjustment -= 3
            reason.append(f"Funding перегрет для SHORT: {funding:.6f}")

        else:
            score_adjustment += 2
            reason.append(f"Funding нормальный: {funding:.6f}")

    else:
        reason.append("Funding недоступен")

    if oi is not None:
        score_adjustment += 1
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


def combine_filters(symbol: str, direction: str, btc_status: str) -> dict:
    funding_oi = analyze_funding_oi(symbol, direction)

    btc_against = (
        (direction == "LONG" and btc_status == "BEARISH")
        or (direction == "SHORT" and btc_status == "BULLISH")
    )

    return {
        "blocked": funding_oi.get("blocked", False) or btc_against,
        "score_adjustment": funding_oi.get("score_adjustment", 0),
        "funding": funding_oi,
        "btc_status": btc_status,
        "btc_against": btc_against,
    }


# =========================
# MARKET REGIME ENGINE
# =========================

def classify_market_profile(symbol, c1, c5, c15, c1h, c4h, btc_status) -> dict:
    base = base_from_symbol(symbol)

    if len(c5) < 80 or len(c15) < 100 or len(c1h) < 80 or len(c4h) < 50:
        return {"regime": "UNKNOWN", "avoid_reason": "not_enough_candles"}

    price = c5[-1]["close"]

    if price <= 0:
        return {"regime": "AVOID", "avoid_reason": "bad_price"}

    closes5 = [c["close"] for c in c5]
    closes15 = [c["close"] for c in c15]

    a5 = atr(c5)
    a15 = atr(c15)
    vw15 = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    if a5 is None or a15 is None or vw15 is None or rs5 is None or rs15 is None:
        return {"regime": "UNKNOWN", "avoid_reason": "indicators_unavailable"}

    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    move5_6 = recent_move_percent(c5, lookback=6)
    move5_12 = recent_move_percent(c5, lookback=12)
    move15_6 = recent_move_percent(c15, lookback=6)
    move15_12 = recent_move_percent(c15, lookback=12)

    recent_1h = c1h[-72:]
    range_high = max(c["high"] for c in recent_1h)
    range_low = min(c["low"] for c in recent_1h)

    if range_low <= 0 or range_high <= range_low:
        return {"regime": "UNKNOWN", "avoid_reason": "bad_range"}

    range_width = (range_high - range_low) / range_low * 100
    range_pos = (price - range_low) / (range_high - range_low)
    distance_vwap = distance_from_vwap_percent(price, vw15)

    violent = market_too_violent(symbol, c5, c15)

    # Risky-монеты не идут в обычные стратегии. Только EXTREME.
    if is_risky_base(base):
        if not ALLOW_RISKY_EXTREME_TRADES:
            return {
                "regime": "AVOID",
                "direction_bias": None,
                "speed": "EXTREME",
                "quality": 0,
                "avoid_reason": f"risky_base_blocked:{base}",
            }

        direction_bias = "LONG" if move15_12 > 0 else "SHORT"

        return {
            "regime": "EXTREME_ONLY",
            "direction_bias": direction_bias,
            "speed": "EXTREME",
            "quality": 78,
            "expected_hold": "10–90 минут",
            "allowed_trade_classes": ["EXTREME"],
            "range_low": range_low,
            "range_high": range_high,
            "range_width": round(range_width, 2),
            "range_pos": round(range_pos, 3),
            "move15_12": round(move15_12, 2),
            "volume_ratio": round(vr5, 2),
            "avoid_reason": None,
        }

    if violent:
        return {
            "regime": "AVOID",
            "direction_bias": None,
            "speed": "NONE",
            "quality": 0,
            "range_low": range_low,
            "range_high": range_high,
            "range_width": round(range_width, 2),
            "range_pos": round(range_pos, 3),
            "avoid_reason": violent,
        }

    # FAST MOMENTUM
    fast_up = move5_6 >= 0.75 or move15_6 >= 1.10 or move5_12 >= 1.30
    fast_down = move5_6 <= -0.75 or move15_6 <= -1.10 or move5_12 <= -1.30

    if fast_up or fast_down:
        direction_bias = "LONG" if fast_up else "SHORT"

        if direction_bias == "LONG" and btc_status == "BEARISH":
            return {"regime": "AVOID", "direction_bias": "LONG", "avoid_reason": "btc_against_fast_long"}

        if direction_bias == "SHORT" and btc_status == "BULLISH":
            return {"regime": "AVOID", "direction_bias": "SHORT", "avoid_reason": "btc_against_fast_short"}

        quality = 82

        if vr5 >= 1.00:
            quality += 4
        if vr5 >= 1.25:
            quality += 4
        if distance_vwap <= 2.2:
            quality += 4
        if direction_bias == "LONG" and 42 <= rs5 <= 74:
            quality += 3
        if direction_bias == "SHORT" and 26 <= rs5 <= 58:
            quality += 3

        return {
            "regime": "FAST_MOMENTUM",
            "direction_bias": direction_bias,
            "speed": "FAST",
            "quality": min(quality, 96),
            "expected_hold": "10–90 минут",
            "allowed_trade_classes": ["FAST", "EXTREME"],
            "range_low": range_low,
            "range_high": range_high,
            "range_width": round(range_width, 2),
            "range_pos": round(range_pos, 3),
            "move5_6": round(move5_6, 2),
            "move15_6": round(move15_6, 2),
            "volume_ratio": round(vr5, 2),
            "avoid_reason": None,
        }

    # TREND CONTINUATION
    bullish_context = (
        btc_status in ["BULLISH", "SOFT_BULLISH", "NEUTRAL"]
        and (
            trend1h in ["BULLISH", "SOFT_BULLISH"]
            or trend4h in ["BULLISH", "SOFT_BULLISH"]
        )
        and price >= vw15 * 0.985
    )

    bearish_context = (
        btc_status in ["BEARISH", "SOFT_BEARISH", "NEUTRAL"]
        and (
            trend1h in ["BEARISH", "SOFT_BEARISH"]
            or trend4h in ["BEARISH", "SOFT_BEARISH"]
        )
        and price <= vw15 * 1.015
    )

    if bullish_context:
        quality = 84

        if trend1h in ["BULLISH", "SOFT_BULLISH"]:
            quality += 4
        if trend4h in ["BULLISH", "SOFT_BULLISH"]:
            quality += 4
        if btc_status in ["BULLISH", "SOFT_BULLISH"]:
            quality += 4
        if vr5 >= 0.95:
            quality += 3

        return {
            "regime": "TREND_CONTINUATION",
            "direction_bias": "LONG",
            "speed": "MEDIUM",
            "quality": min(quality, 96),
            "expected_hold": "30 минут – 4 часа",
            "allowed_trade_classes": ["TREND", "STRUCTURE"],
            "range_low": range_low,
            "range_high": range_high,
            "range_width": round(range_width, 2),
            "range_pos": round(range_pos, 3),
            "move15_12": round(move15_12, 2),
            "volume_ratio": round(vr5, 2),
            "avoid_reason": None,
        }

    if bearish_context:
        quality = 84

        if trend1h in ["BEARISH", "SOFT_BEARISH"]:
            quality += 4
        if trend4h in ["BEARISH", "SOFT_BEARISH"]:
            quality += 4
        if btc_status in ["BEARISH", "SOFT_BEARISH"]:
            quality += 4
        if vr5 >= 0.95:
            quality += 3

        return {
            "regime": "TREND_CONTINUATION",
            "direction_bias": "SHORT",
            "speed": "MEDIUM",
            "quality": min(quality, 96),
            "expected_hold": "30 минут – 4 часа",
            "allowed_trade_classes": ["TREND", "STRUCTURE"],
            "range_low": range_low,
            "range_high": range_high,
            "range_width": round(range_width, 2),
            "range_pos": round(range_pos, 3),
            "move15_12": round(move15_12, 2),
            "volume_ratio": round(vr5, 2),
            "avoid_reason": None,
        }

    # RANGE STRUCTURE
    if 1.8 <= range_width <= 16:
        if range_pos <= 0.30 and btc_status != "BEARISH":
            quality = 84
            if rs5 >= 30:
                quality += 3
            if vr5 >= 0.85:
                quality += 3

            return {
                "regime": "RANGE_STRUCTURE",
                "direction_bias": "LONG",
                "speed": "SLOW",
                "quality": min(quality, 94),
                "expected_hold": "1–8 часов",
                "allowed_trade_classes": ["RANGE", "STRUCTURE", "SWING"],
                "range_low": range_low,
                "range_high": range_high,
                "range_width": round(range_width, 2),
                "range_pos": round(range_pos, 3),
                "volume_ratio": round(vr5, 2),
                "avoid_reason": None,
            }

        if range_pos >= 0.70 and btc_status != "BULLISH":
            quality = 84
            if rs5 <= 70:
                quality += 3
            if vr5 >= 0.85:
                quality += 3

            return {
                "regime": "RANGE_STRUCTURE",
                "direction_bias": "SHORT",
                "speed": "SLOW",
                "quality": min(quality, 94),
                "expected_hold": "1–8 часов",
                "allowed_trade_classes": ["RANGE", "STRUCTURE", "SWING"],
                "range_low": range_low,
                "range_high": range_high,
                "range_width": round(range_width, 2),
                "range_pos": round(range_pos, 3),
                "volume_ratio": round(vr5, 2),
                "avoid_reason": None,
            }

    return {
        "regime": "NO_TRADE_ZONE",
        "direction_bias": None,
        "speed": "NONE",
        "quality": 0,
        "range_low": range_low,
        "range_high": range_high,
        "range_width": round(range_width, 2),
        "range_pos": round(range_pos, 3),
        "avoid_reason": "middle_of_range_or_no_clear_reaction",
    }


# =========================
# RISK / SIGNAL BUILDING
# =========================

def make_tp_by_roi(entry: float, direction: str, roi_percent: float) -> float:
    price_move = roi_percent / LEVERAGE / 100

    if direction == "LONG":
        return entry * (1 + price_move)

    return entry * (1 - price_move)


def price_move_percent(entry: float, target: float, direction: str) -> float:
    if direction == "LONG":
        return (target - entry) / entry * 100

    return (entry - target) / entry * 100


def calc_risk_position(entry: float, sl: float) -> float:
    return abs(entry - sl) / entry * 100 * LEVERAGE


def estimate_trade_cost_percent() -> float:
    return (FEE_RATE * 2 + SLIPPAGE_RATE * 2) * 100


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
        "margin_10x": round(position_size / LEVERAGE, 2),
        "error": None,
    }


def model_probability_from_score(score: int, rr: float, volume_ratio_value: float, profile_quality: int) -> int:
    """
    Это НЕ реальная гарантия вероятности.
    Это модельная оценка качества сетапа для Telegram.
    """
    probability = 45
    probability += max(score - 80, 0) * 1.1
    probability += max(profile_quality - 80, 0) * 0.6
    probability += min(rr, 2.0) * 5
    probability += min(volume_ratio_value, 2.0) * 2

    return int(max(45, min(probability, 82)))


def classify_signal(score: int, rr: float, volume_ratio_value: float, filters: dict, strategy: str, direction: str) -> Optional[dict]:
    if filters.get("blocked"):
        return None

    if not is_strategy_side_enabled(strategy, direction):
        return None

    if is_statistically_bad_strategy_side(strategy, direction):
        return None

    if (
        score >= A_PLUS_MIN_SCORE
        and rr >= A_PLUS_MIN_RR
        and volume_ratio_value >= A_PLUS_MIN_VOLUME_RATIO
    ):
        return {
            "grade": "STRONG",
            "risk_multiplier": STRONG_RISK_MULTIPLIER,
        }

    if not ALLOW_WEAK_SIGNALS:
        return None

    if (
        score >= WEAK_MIN_SCORE
        and rr >= WEAK_MIN_RR
        and volume_ratio_value >= WEAK_MIN_VOLUME_RATIO
    ):
        return {
            "grade": "WEAK",
            "risk_multiplier": WEAK_RISK_MULTIPLIER,
        }

    return None


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
    filters: dict,
    profile: dict,
    tp1: float,
    tp2: float,
    tp3: float,
    trade_class: str,
    trade_style: str,
    expected_hold: str,
    rr_target: str = "TP1",
    max_risk_position_percent: Optional[float] = None,
    risk_multiplier_override: Optional[float] = None,
) -> Optional[dict]:
    base = base_from_symbol(symbol)

    if is_risky_base(base) and strategy != "EXTREME_CONTEXT_PRO":
        return None

    if entry <= 0 or sl <= 0:
        return None

    risk_pos = calc_risk_position(entry, sl)

    if max_risk_position_percent is None:
        if trade_class == "FAST":
            max_risk_position_percent = FAST_MAX_RISK_POSITION_PERCENT
        elif trade_class == "EXTREME":
            max_risk_position_percent = EXTREME_MAX_RISK_POSITION_PERCENT
        elif trade_class == "SWING":
            max_risk_position_percent = SWING_MAX_RISK_POSITION_PERCENT
        else:
            max_risk_position_percent = STRUCTURE_MAX_RISK_POSITION_PERCENT

    if risk_pos > max_risk_position_percent:
        return None

    score += filters.get("score_adjustment", 0)
    score += int(profile.get("quality", 0) * 0.05)

    raw_reward_tp1 = price_move_percent(entry, tp1, direction)
    raw_reward_tp2 = price_move_percent(entry, tp2, direction)
    raw_reward_tp3 = price_move_percent(entry, tp3, direction)

    target_roi_tp1 = raw_reward_tp1 * LEVERAGE
    target_roi_tp2 = raw_reward_tp2 * LEVERAGE
    target_roi_tp3 = raw_reward_tp3 * LEVERAGE

    if target_roi_tp1 < MIN_TARGET_ROI_PERCENT:
        return None

    risk_price = abs(entry - sl) / entry * 100
    trade_cost = estimate_trade_cost_percent()

    if rr_target == "TP3":
        rr_reward = raw_reward_tp3
    elif rr_target == "TP2":
        rr_reward = raw_reward_tp2
    else:
        rr_reward = raw_reward_tp1

    rr_net_reward = max(rr_reward - trade_cost, 0)
    rr = rr_net_reward / risk_price if risk_price > 0 else 0

    grade_data = classify_signal(score, rr, vol_ratio, filters, strategy, direction)

    if grade_data is None:
        return None

    grade = grade_data["grade"]

    if risk_multiplier_override is not None:
        risk_multiplier = risk_multiplier_override
    else:
        risk_multiplier = grade_data["risk_multiplier"]

    if trade_class == "FAST":
        risk_multiplier = min(risk_multiplier, FAST_RISK_MULTIPLIER)
    elif trade_class == "STRUCTURE":
        risk_multiplier = min(risk_multiplier, STRUCTURE_RISK_MULTIPLIER)
    elif trade_class == "EXTREME":
        risk_multiplier = min(risk_multiplier, EXTREME_RISK_MULTIPLIER)

    adjusted_risk_percent = risk_percent * risk_multiplier

    signal_id = f"{normalize_symbol(symbol)}:{strategy}:{direction}:{grade}:{round(entry, 8)}"

    if signal_id in STATE["sent_signals"]:
        return None

    probability = model_probability_from_score(
        score=score,
        rr=rr,
        volume_ratio_value=vol_ratio,
        profile_quality=int(profile.get("quality", 0)),
    )

    pos = calculate_position(entry, sl, deposit, adjusted_risk_percent)

    return {
        "id": signal_id,
        "symbol": normalize_symbol(symbol),
        "display_symbol": display_symbol(symbol),
        "direction": direction,
        "strategy": strategy,
        "grade": grade,
        "signal_strength": "СИЛЬНЫЙ" if grade == "STRONG" else "СЛАБЫЙ / осторожный",
        "model_probability": probability,
        "risk_multiplier": risk_multiplier,
        "status": "ACTIVE",
        "trade_class": trade_class,
        "trade_style": trade_style,
        "expected_hold": expected_hold,
        "market_profile": profile,
        "market_regime": profile.get("regime"),
        "market_speed": profile.get("speed"),
        "profile_quality": profile.get("quality"),
        "score": min(max(score, 0), 98),
        "entry": round(entry, 8),
        "sl": round(sl, 8),
        "tp1": round(tp1, 8),
        "tp2": round(tp2, 8),
        "tp3": round(tp3, 8),
        "rr": round(rr, 2),
        "rr_target": rr_target,
        "raw_reward_to_tp1_percent": round(raw_reward_tp1, 4),
        "raw_reward_to_tp2_percent": round(raw_reward_tp2, 4),
        "raw_reward_to_tp3_percent": round(raw_reward_tp3, 4),
        "target_roi_tp1": round(target_roi_tp1, 2),
        "target_roi_tp2": round(target_roi_tp2, 2),
        "target_roi_tp3": round(target_roi_tp3, 2),
        "estimated_trade_cost_percent": round(trade_cost, 4),
        "volume_ratio": round(vol_ratio, 2),
        "risk_position_percent": round(risk_pos, 2),
        "risk_percent": adjusted_risk_percent,
        "position": pos,
        "reason": reason,
        "filters": filters,
        "created_at": now_ts(),
        "last_checked_time": int(now_ts() * 1000),
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "counted_positive": False,
        "counted_sl": False,
        "counted_tp1": False,
        "counted_tp2": False,
        "counted_tp3": False,
    }


# =========================
# STRATEGIES
# =========================

def evaluate_fast_reaction_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent, profile):
    strategy = "FAST_REACTION_PRO"

    if profile.get("regime") != "FAST_MOMENTUM":
        return None

    if direction != profile.get("direction_bias"):
        return None

    price = c5[-1]["close"]
    closes5 = [c["close"] for c in c5]
    closes15 = [c["close"] for c in c15]

    a5 = atr(c5)
    vw15 = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    ema21_5 = ema_value(c5, 21)
    ema50_15 = ema_value(c15, 50)

    if a5 is None or vw15 is None or rs5 is None or rs15 is None or ema21_5 is None or ema50_15 is None:
        return None

    if recent_failed_push(c5, direction):
        return None

    pullback_zone_long = (
        ema21_5 * 0.987 <= price <= ema21_5 * 1.018
        or vw15 * 0.985 <= price <= vw15 * 1.016
    )

    pullback_zone_short = (
        ema21_5 * 0.982 <= price <= ema21_5 * 1.010
        or vw15 * 0.984 <= price <= vw15 * 1.014
    )

    score = 84

    if vr5 >= 1.00:
        score += 4
    if vr5 >= 1.25:
        score += 4
    if confirmed_5m_followthrough(c5, direction):
        score += 6
    if confirmed_15m_direction(c15, direction):
        score += 3

    if direction == "LONG":
        if btc_status == "BEARISH":
            return None
        if not pullback_zone_long:
            return None
        if not confirmed_5m_followthrough(c5, "LONG"):
            return None
        if rs5 > 74 or rs15 > 72:
            return None
        if price < ema50_15 * 0.975:
            return None

        sl = min(
            min(c["low"] for c in c5[-24:]) - a5 * 0.45,
            price * 0.992
        )

        reason = (
            "FAST LONG: монета уже дала быстрый импульс, но вход не на вершине. "
            "Цена вернулась к EMA/VWAP, затем 5m снова показал покупателя. "
            "Это быстрая сделка, её не нужно держать как structure."
        )

    else:
        if btc_status == "BULLISH":
            return None
        if not pullback_zone_short:
            return None
        if not confirmed_5m_followthrough(c5, "SHORT"):
            return None
        if rs5 < 26 or rs15 < 28:
            return None
        if price > ema50_15 * 1.025:
            return None

        sl = max(
            max(c["high"] for c in c5[-24:]) + a5 * 0.45,
            price * 1.008
        )

        reason = (
            "FAST SHORT: монета уже дала быстрый импульс вниз, но вход не в самом низу. "
            "Был откат к EMA/VWAP, затем 5m снова показал продавца. "
            "Это быстрая сделка, её не нужно держать как structure."
        )

    filters = combine_filters(symbol, direction, btc_status)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=max(vr5, 0.80),
        reason=reason,
        deposit=deposit,
        risk_percent=risk_percent,
        filters=filters,
        profile=profile,
        tp1=make_tp_by_roi(price, direction, FAST_TP1_ROI),
        tp2=make_tp_by_roi(price, direction, FAST_TP2_ROI),
        tp3=make_tp_by_roi(price, direction, FAST_TP3_ROI),
        trade_class="FAST",
        trade_style="⚡ FAST REACTION / быстрый импульс после отката",
        expected_hold="10–90 минут",
        rr_target="TP1",
        max_risk_position_percent=FAST_MAX_RISK_POSITION_PERCENT,
        risk_multiplier_override=FAST_RISK_MULTIPLIER,
    )


def evaluate_trend_continuation_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent, profile):
    strategy = "TREND_CONTINUATION_PRO"

    if profile.get("regime") != "TREND_CONTINUATION":
        return None

    if direction != profile.get("direction_bias"):
        return None

    price = c5[-1]["close"]

    closes5 = [c["close"] for c in c5]
    closes15 = [c["close"] for c in c15]

    a5 = atr(c5)
    a15 = atr(c15)
    vw15 = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    ema21_5 = ema_value(c5, 21)
    ema50_15 = ema_value(c15, 50)
    ema100_15 = ema_value(c15, 100)

    if a5 is None or a15 is None or vw15 is None or rs5 is None or rs15 is None:
        return None

    if ema21_5 is None or ema50_15 is None or ema100_15 is None:
        return None

    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    if recent_failed_push(c5, direction):
        return None

    score = 84

    if vr5 >= 0.90:
        score += 3
    if vr5 >= 1.10:
        score += 4
    if confirmed_5m_followthrough(c5, direction):
        score += 5
    if confirmed_15m_direction(c15, direction):
        score += 4

    if direction == "LONG":
        if btc_status == "BEARISH":
            return None

        htf_ok = trend1h in ["BULLISH", "SOFT_BULLISH"] or trend4h in ["BULLISH", "SOFT_BULLISH"]

        if not htf_ok:
            return None

        pullback_zone = (
            ema21_5 * 0.988 <= price <= ema21_5 * 1.018
            or vw15 * 0.985 <= price <= vw15 * 1.018
        )

        if not pullback_zone:
            return None

        if not confirmed_5m_followthrough(c5, "LONG"):
            return None

        if rs5 > 74 or rs15 > 72:
            return None

        if price < ema50_15 * 0.985 or price < ema100_15 * 0.975:
            return None

        if btc_status in ["BULLISH", "SOFT_BULLISH"]:
            score += 4
        if trend1h in ["BULLISH", "SOFT_BULLISH"]:
            score += 4
        if trend4h in ["BULLISH", "SOFT_BULLISH"]:
            score += 3

        sl = min(
            min(c["low"] for c in c15[-18:]) - a15 * 0.25,
            min(c["low"] for c in c5[-30:]) - a5 * 0.45,
        )

        reason = (
            "TREND LONG: BTC/старшие таймфреймы не против. "
            "Цена находится в трендовом откате к EMA/VWAP и дала подтверждение покупателя на 5m. "
            "Сделке можно дать больше времени, чем fast-входу."
        )

    else:
        if btc_status == "BULLISH":
            return None

        htf_ok = trend1h in ["BEARISH", "SOFT_BEARISH"] or trend4h in ["BEARISH", "SOFT_BEARISH"]

        if not htf_ok:
            return None

        pullback_zone = (
            ema21_5 * 0.982 <= price <= ema21_5 * 1.012
            or vw15 * 0.982 <= price <= vw15 * 1.014
        )

        if not pullback_zone:
            return None

        if not confirmed_5m_followthrough(c5, "SHORT"):
            return None

        if rs5 < 26 or rs15 < 28:
            return None

        if price > ema50_15 * 1.015 or price > ema100_15 * 1.025:
            return None

        if btc_status in ["BEARISH", "SOFT_BEARISH"]:
            score += 4
        if trend1h in ["BEARISH", "SOFT_BEARISH"]:
            score += 4
        if trend4h in ["BEARISH", "SOFT_BEARISH"]:
            score += 3

        sl = max(
            max(c["high"] for c in c15[-18:]) + a15 * 0.25,
            max(c["high"] for c in c5[-30:]) + a5 * 0.45,
        )

        reason = (
            "TREND SHORT: BTC/старшие таймфреймы не против. "
            "Цена находится в трендовом откате к EMA/VWAP и дала подтверждение продавца на 5m. "
            "Сделке можно дать больше времени, чем fast-входу."
        )

    filters = combine_filters(symbol, direction, btc_status)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=max(vr5, 0.85),
        reason=reason,
        deposit=deposit,
        risk_percent=risk_percent,
        filters=filters,
        profile=profile,
        tp1=make_tp_by_roi(price, direction, TREND_TP1_ROI),
        tp2=make_tp_by_roi(price, direction, TREND_TP2_ROI),
        tp3=make_tp_by_roi(price, direction, TREND_TP3_ROI),
        trade_class="STRUCTURE",
        trade_style="📈 TREND CONTINUATION / трендовый откат",
        expected_hold="30 минут – 4 часа",
        rr_target="TP2",
        max_risk_position_percent=STRUCTURE_MAX_RISK_POSITION_PERCENT,
        risk_multiplier_override=STRUCTURE_RISK_MULTIPLIER,
    )


def evaluate_range_structure_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent, profile):
    strategy = "RANGE_STRUCTURE_PRO"

    if profile.get("regime") != "RANGE_STRUCTURE":
        return None

    if direction != profile.get("direction_bias"):
        return None

    price = c5[-1]["close"]
    range_low = float(profile.get("range_low", 0))
    range_high = float(profile.get("range_high", 0))

    if range_low <= 0 or range_high <= range_low:
        return None

    closes5 = [c["close"] for c in c5]
    closes15 = [c["close"] for c in c15]

    a5 = atr(c5)
    a15 = atr(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    if a5 is None or a15 is None or rs5 is None or rs15 is None:
        return None

    if recent_failed_push(c5, direction):
        return None

    score = 86

    if vr5 >= 0.85:
        score += 3
    if vr5 >= 1.05:
        score += 3
    if confirmed_5m_followthrough(c5, direction):
        score += 5
    if confirmed_15m_direction(c15, direction):
        score += 4

    if direction == "LONG":
        if btc_status == "BEARISH":
            return None
        if not confirmed_5m_followthrough(c5, "LONG"):
            return None
        if not confirmed_15m_direction(c15, "LONG"):
            return None
        if rs5 > 68 or rs15 > 66:
            return None

        sl = min(
            range_low - a15 * 0.60,
            min(c["low"] for c in c5[-36:]) - a5 * 0.50,
        )

        mid = range_low + (range_high - range_low) * 0.52
        upper = range_low + (range_high - range_low) * 0.76

        tp1 = max(make_tp_by_roi(price, "LONG", RANGE_TP1_ROI), mid)
        tp2 = max(make_tp_by_roi(price, "LONG", RANGE_TP2_ROI), upper)
        tp3 = max(make_tp_by_roi(price, "LONG", RANGE_TP3_ROI), range_high * 0.995)

        reason = (
            f"RANGE LONG: цена у нижней части 1H-диапазона {round(range_low, 8)}–{round(range_high, 8)}. "
            "Продавец не смог продолжить движение ниже, появился возврат покупателя. "
            "Это не быстрый скальп — сделке нужно время до середины/верхней зоны диапазона."
        )

    else:
        if btc_status == "BULLISH":
            return None
        if not confirmed_5m_followthrough(c5, "SHORT"):
            return None
        if not confirmed_15m_direction(c15, "SHORT"):
            return None
        if rs5 < 32 or rs15 < 34:
            return None

        sl = max(
            range_high + a15 * 0.60,
            max(c["high"] for c in c5[-36:]) + a5 * 0.50,
        )

        mid = range_low + (range_high - range_low) * 0.48
        lower = range_low + (range_high - range_low) * 0.24

        tp1 = min(make_tp_by_roi(price, "SHORT", RANGE_TP1_ROI), mid)
        tp2 = min(make_tp_by_roi(price, "SHORT", RANGE_TP2_ROI), lower)
        tp3 = min(make_tp_by_roi(price, "SHORT", RANGE_TP3_ROI), range_low * 1.005)

        reason = (
            f"RANGE SHORT: цена у верхней части 1H-диапазона {round(range_low, 8)}–{round(range_high, 8)}. "
            "Покупатель не смог закрепиться выше, появился возврат продавца. "
            "Это не быстрый скальп — сделке нужно время до середины/нижней зоны диапазона."
        )

    filters = combine_filters(symbol, direction, btc_status)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=max(vr5, 0.80),
        reason=reason,
        deposit=deposit,
        risk_percent=risk_percent,
        filters=filters,
        profile=profile,
        tp1=tp1,
        tp2=tp2,
        tp3=tp3,
        trade_class="STRUCTURE",
        trade_style="🏗 RANGE STRUCTURE / сделке нужно время",
        expected_hold="1–8 часов",
        rr_target="TP2",
        max_risk_position_percent=STRUCTURE_MAX_RISK_POSITION_PERCENT,
        risk_multiplier_override=STRUCTURE_RISK_MULTIPLIER,
    )


def evaluate_extreme_context_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent, profile):
    strategy = "EXTREME_CONTEXT_PRO"

    base = base_from_symbol(symbol)
    price = c5[-1]["close"]

    if len(c1h) < 30 or len(c15) < 80 or len(c5) < 60:
        return None

    old24 = c1h[-25]["close"] if len(c1h) >= 25 else c1h[0]["close"]

    if old24 <= 0:
        return None

    change24 = (price - old24) / old24 * 100
    risky = is_risky_base(base)

    if risky and not ALLOW_RISKY_EXTREME_TRADES:
        return None

    if abs(change24) < 6 and base not in EXTREME_BASES:
        return None

    if risky and abs(change24) < RISKY_EXTREME_MIN_CHANGE_PERCENT:
        return None

    closes5 = [c["close"] for c in c5]
    closes15 = [c["close"] for c in c15]

    ema21_5 = ema_value(c5, 21)
    vw15 = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)
    a5 = atr(c5)

    if ema21_5 is None or vw15 is None or rs5 is None or rs15 is None or a5 is None:
        return None

    if risky and vr5 < RISKY_EXTREME_MIN_VOLUME_RATIO:
        return None

    recent_high = max(c["high"] for c in c5[-36:])
    recent_low = min(c["low"] for c in c5[-36:])

    pullback_from_high = (recent_high - price) / recent_high * 100 if recent_high > 0 else 0
    bounce_from_low = (price - recent_low) / recent_low * 100 if recent_low > 0 else 0

    score = 84

    if abs(change24) >= 12:
        score += 4
    if abs(change24) >= 25:
        score += 4
    if vr5 >= 1.08:
        score += 4
    if vr5 >= 1.30:
        score += 4

    if direction == "LONG":
        if risky and not ALLOW_RISKY_EXTREME_LONGS:
            return None
        if btc_status == "BEARISH":
            return None

        continuation = (
            change24 > 6
            and 2.0 <= pullback_from_high <= 18.0
            and price > ema21_5
            and confirmed_5m_followthrough(c5, "LONG")
        )

        reversal = (
            change24 < -10
            and 3.0 <= bounce_from_low <= 16.0
            and price > ema21_5
            and price > vw15 * 0.995
            and confirmed_5m_followthrough(c5, "LONG")
        )

        if not (continuation or reversal):
            return None

        if rs5 > 76 or rs15 > 74:
            return None

        if recent_failed_push(c5, "LONG"):
            return None

        sl = min(
            recent_low - a5 * 0.55,
            min(c["low"] for c in c5[-18:]) - a5 * 0.25,
        )

        if continuation:
            reason = (
                f"EXTREME LONG continuation: монета сильная за 24ч ({round(change24, 2)}%). "
                f"Вход не на вершине: был откат от high примерно {round(pullback_from_high, 2)}%, затем покупатель вернулся."
            )
        else:
            reason = (
                f"EXTREME LONG reversal: монета сильно снижалась за 24ч ({round(change24, 2)}%). "
                f"Появился отскок от локального дна примерно {round(bounce_from_low, 2)}% и возврат выше EMA/VWAP."
            )

    else:
        if btc_status == "BULLISH":
            return None

        continuation = (
            change24 < -6
            and 2.0 <= bounce_from_low <= 18.0
            and price < ema21_5
            and confirmed_5m_followthrough(c5, "SHORT")
        )

        reversal = (
            change24 > 12
            and 3.0 <= pullback_from_high <= 22.0
            and price < ema21_5
            and price < vw15 * 1.005
            and confirmed_5m_followthrough(c5, "SHORT")
        )

        if not (continuation or reversal):
            return None

        if rs5 < 24 or rs15 < 26:
            return None

        if recent_failed_push(c5, "SHORT"):
            return None

        sl = max(
            recent_high + a5 * 0.55,
            max(c["high"] for c in c5[-18:]) + a5 * 0.25,
        )

        if continuation:
            reason = (
                f"EXTREME SHORT continuation: монета слабая за 24ч ({round(change24, 2)}%). "
                f"Был откат от low примерно {round(bounce_from_low, 2)}%, затем продавец вернулся."
            )
        else:
            reason = (
                f"EXTREME SHORT reversal: монета перегрета за 24ч ({round(change24, 2)}%). "
                f"Появился откат от high примерно {round(pullback_from_high, 2)}% и слом EMA/VWAP вниз."
            )

    filters = combine_filters(symbol, direction, btc_status)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=max(vr5, 0.90),
        reason=reason,
        deposit=deposit,
        risk_percent=risk_percent,
        filters=filters,
        profile=profile,
        tp1=make_tp_by_roi(price, direction, EXTREME_TP1_ROI),
        tp2=make_tp_by_roi(price, direction, EXTREME_TP2_ROI),
        tp3=make_tp_by_roi(price, direction, EXTREME_TP3_ROI),
        trade_class="EXTREME",
        trade_style="🔥 EXTREME CONTEXT / быстрый рискованный mover",
        expected_hold="10–90 минут",
        rr_target="TP1",
        max_risk_position_percent=EXTREME_MAX_RISK_POSITION_PERCENT,
        risk_multiplier_override=EXTREME_RISK_MULTIPLIER,
    )


def choose_strategy_functions_by_profile(profile: dict, symbol: str):
    regime = profile.get("regime")
    base = base_from_symbol(symbol)

    if is_risky_base(base):
        return [evaluate_extreme_context_pro]

    if regime == "EXTREME_ONLY":
        return [evaluate_extreme_context_pro]

    if regime == "FAST_MOMENTUM":
        return [
            evaluate_fast_reaction_pro,
            evaluate_extreme_context_pro,
        ]

    if regime == "TREND_CONTINUATION":
        return [
            evaluate_trend_continuation_pro,
            evaluate_fast_reaction_pro,
        ]

    if regime == "RANGE_STRUCTURE":
        return [
            evaluate_range_structure_pro,
        ]

    return []


# =========================
# ANALYSIS / SCAN
# =========================

def analyze_symbol(
    symbol: str,
    direction: Optional[str],
    deposit: float,
    risk_percent: float,
    btc_status: Optional[str] = None
) -> Optional[dict]:
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

    if btc_status is None:
        btc_status = detect_btc_status()

    profile = classify_market_profile(symbol, c1, c5, c15, c1h, c4h, btc_status)

    if profile.get("regime") in ["AVOID", "UNKNOWN", "NO_TRADE_ZONE"]:
        return None

    normalized_direction = normalize_direction(direction)

    if normalized_direction:
        directions = [normalized_direction]
    else:
        bias = normalize_direction(profile.get("direction_bias"))

        if not bias:
            return None

        directions = [bias]

    funcs = choose_strategy_functions_by_profile(profile, symbol)

    if not funcs:
        return None

    candidates = []

    for d in directions:
        for func in funcs:
            signal = func(symbol, d, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent, profile)

            if signal:
                candidates.append(signal)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            1 if x["grade"] == "STRONG" else 0,
            x.get("model_probability", 0),
            x.get("profile_quality", 0),
            x["score"],
            x["rr"],
            x.get("target_roi_tp2") or 0,
            x["volume_ratio"],
        ),
        reverse=True,
    )

    return candidates[0]


def scan_best_signal(deposit: float, risk_percent: float) -> dict:
    cleanup_state()
    symbols = get_symbols()

    best = None
    checked = 0
    btc_status = detect_btc_status()

    for symbol in symbols:
        checked += 1

        signal = analyze_symbol(
            symbol=symbol,
            direction=None,
            deposit=deposit,
            risk_percent=risk_percent,
            btc_status=btc_status,
        )

        if not signal:
            continue

        if best is None:
            best = signal
        else:
            current_key = (
                1 if signal["grade"] == "STRONG" else 0,
                signal.get("model_probability", 0),
                signal.get("profile_quality", 0),
                signal["score"],
                signal["rr"],
                signal.get("target_roi_tp2") or 0,
                signal["volume_ratio"],
            )

            best_key = (
                1 if best["grade"] == "STRONG" else 0,
                best.get("model_probability", 0),
                best.get("profile_quality", 0),
                best["score"],
                best["rr"],
                best.get("target_roi_tp2") or 0,
                best["volume_ratio"],
            )

            if current_key > best_key:
                best = signal

    if not best:
        return {
            "ok": False,
            "checked": checked,
            "btc_status": btc_status,
            "message": "Сильных сигналов сейчас нет.",
        }

    return {
        "ok": True,
        "checked": checked,
        "btc_status": btc_status,
        "signal": best,
        "message": build_message(best),
    }


# =========================
# TELEGRAM MESSAGES
# =========================

def strategy_title(strategy: str) -> str:
    names = {
        "FAST_REACTION_PRO": "⚡ Fast Reaction Pro",
        "TREND_CONTINUATION_PRO": "📈 Trend Continuation Pro",
        "RANGE_STRUCTURE_PRO": "🏗 Range Structure Pro",
        "STRUCTURE_SWING_PRO": "🏗 Structure Swing Pro",
        "EXTREME_CONTEXT_PRO": "🔥 Extreme Context Pro",
    }
    return names.get(strategy, strategy)


def regime_title(regime: str) -> str:
    names = {
        "FAST_MOMENTUM": "⚡ Быстрый импульс",
        "TREND_CONTINUATION": "📈 Трендовое продолжение",
        "RANGE_STRUCTURE": "🏗 Структурный диапазон",
        "EXTREME_ONLY": "🔥 Extreme mover",
    }
    return names.get(regime, regime or "UNKNOWN")


def build_message(signal: dict) -> str:
    mode = "TEST" if TEST_MODE else "LIVE"
    arrow = "📈" if signal["direction"] == "LONG" else "📉"

    profile = signal.get("market_profile", {})
    filters = signal.get("filters", {})
    funding = filters.get("funding", {})

    pos = signal.get("position", {})

    if pos.get("error"):
        risk_text = f"⚠️ Ошибка RM: {pos['error']}"
    else:
        risk_text = (
            f"Риск депозита: <b>{signal['risk_percent']:.3f}%</b>\n"
            f"Размер позиции: <b>{pos.get('position_size_usdt')} USDT</b>\n"
            f"Маржа x{LEVERAGE}: <b>{pos.get('margin_10x')} USDT</b>"
        )

    grade_icon = "🟢" if signal["grade"] == "STRONG" else "🟡"
    grade_text = "СИЛЬНЫЙ СИГНАЛ" if signal["grade"] == "STRONG" else "СЛАБЫЙ / ОСТОРОЖНЫЙ СИГНАЛ"

    rr_target = signal.get("rr_target", "TP1")

    range_text = ""

    if profile.get("range_low") and profile.get("range_high"):
        range_text = (
            f"\nДиапазон 1H: <code>{round(profile.get('range_low'), 8)}</code> – "
            f"<code>{round(profile.get('range_high'), 8)}</code>"
            f"\nПозиция в диапазоне: <b>{profile.get('range_pos', 'n/a')}</b>"
        )

    funding_text = funding.get("reason", "Funding/OI: нет данных")

    warning = ""
    if signal["grade"] != "STRONG":
        warning = "\n⚠️ Это слабый сигнал. Риск уменьшен. Вход только если ты осознанно разрешил WEAK-сигналы."

    return f"""
{grade_icon} <b>{mode} · {grade_text}</b>

{arrow} <b>{signal['direction']} {signal['display_symbol']}</b>

<b>Стратегия:</b> {strategy_title(signal['strategy'])}
<b>Режим рынка:</b> {regime_title(signal.get('market_regime'))}
<b>Тип сделки:</b> {signal.get('trade_style')}
<b>Ожидание:</b> {signal.get('expected_hold')}

<b>Модельная вероятность:</b> {signal.get('model_probability')}%
<b>Качество:</b> {signal.get('score')}/100
<b>Профиль рынка:</b> {signal.get('profile_quality')}/100
<b>BTC:</b> {filters.get('btc_status', 'NEUTRAL')}

<b>Вход:</b> <code>{signal['entry']}</code>
<b>Stop Loss:</b> <code>{signal['sl']}</code>

<b>Take Profit:</b>
TP1: <code>{signal['tp1']}</code> · ≈ {signal.get('target_roi_tp1')}% ROI
TP2: <code>{signal['tp2']}</code> · ≈ {signal.get('target_roi_tp2')}% ROI
TP3: <code>{signal['tp3']}</code> · ≈ {signal.get('target_roi_tp3')}% ROI

<b>RR до {rr_target} net:</b> {signal.get('rr')}
<b>Объём:</b> x{signal.get('volume_ratio')}
<b>Риск до SL:</b> {signal.get('risk_position_percent')}% по позиции
<b>Издержки:</b> ~{signal.get('estimated_trade_cost_percent')}%

<b>Почему вход:</b>
{signal['reason']}

<b>Контекст:</b>{range_text}
Funding/OI: {funding_text}

<b>Risk Management:</b>
{risk_text}

<b>После TP1:</b>
Закрыть примерно {TP1_CLOSE_PERCENT:.0f}% позиции и перенести SL в безубыток.
{warning}

⚠️ Не финансовый совет. Вероятность — модельная оценка качества сетапа, не гарантия.
""".strip()


def build_stats_text() -> str:
    ensure_stats_structure()

    long_stats = STATE["stats"]["side"]["LONG"]
    short_stats = STATE["stats"]["side"]["SHORT"]

    long_wr = calc_winrate(long_stats["positive"], long_stats["sl"])
    short_wr = calc_winrate(short_stats["positive"], short_stats["sl"])

    strong_stats = STATE["stats"]["grade"].get("STRONG", {"positive": 0, "sl": 0})
    weak_stats = STATE["stats"]["grade"].get("WEAK", {"positive": 0, "sl": 0})

    strong_wr = calc_winrate(strong_stats.get("positive", 0), strong_stats.get("sl", 0))
    weak_wr = calc_winrate(weak_stats.get("positive", 0), weak_stats.get("sl", 0))

    strategy_lines = []

    for strategy in STRATEGIES:
        s = STATE["stats"]["strategy"].get(strategy, {"positive": 0, "sl": 0})
        wr = calc_winrate(s.get("positive", 0), s.get("sl", 0))
        strategy_lines.append(
            f"{strategy}: {s.get('positive', 0)} позитив / {s.get('sl', 0)} SL / WR {wr}%"
        )

    return f"""
📊 <b>Статистика V11:</b>

📈 LONG: {long_stats['positive']} позитив / {long_stats['sl']} SL / WR {long_wr}%
📉 SHORT: {short_stats['positive']} позитив / {short_stats['sl']} SL / WR {short_wr}%

🟢 STRONG: {strong_stats.get('positive', 0)} позитив / {strong_stats.get('sl', 0)} SL / WR {strong_wr}%
🟡 WEAK: {weak_stats.get('positive', 0)} позитив / {weak_stats.get('sl', 0)} SL / WR {weak_wr}%

🧠 <b>Стратегии:</b>
{chr(10).join(strategy_lines)}
""".strip()


def send_telegram_message(text: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {
            "ok": False,
            "error": "TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не указаны",
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


# =========================
# SIGNAL SAVE / TRACKING
# =========================

def save_signal(signal: dict):
    STATE["active_signals"][signal["id"]] = signal
    STATE["sent_signals"][signal["id"]] = now_ts()
    set_cooldown(signal["symbol"])
    save_state(STATE)


def apply_result(signal: dict, result: str):
    ensure_stats_structure()

    side = signal["direction"]
    strategy = signal["strategy"]
    symbol = normalize_symbol(signal["symbol"])
    grade = signal.get("grade", "STRONG")
    strategy_side_key = f"{strategy}:{side}"

    notes = []

    if result == "SL" and not signal.get("counted_sl"):
        signal["counted_sl"] = True

        STATE["stats"]["side"][side]["sl"] += 1
        STATE["stats"]["side"][side]["consecutive_sl"] += 1

        STATE["stats"]["strategy"][strategy]["sl"] += 1
        STATE["stats"]["strategy"][strategy]["consecutive_sl"] += 1

        STATE["stats"]["strategy_side"][strategy_side_key]["sl"] += 1
        STATE["stats"]["strategy_side"][strategy_side_key]["consecutive_sl"] += 1

        STATE["stats"]["grade"][grade]["sl"] += 1

        STATE["stats"]["pair_sl"][symbol] = STATE["stats"]["pair_sl"].get(symbol, 0) + 1

        if STATE["stats"]["pair_sl"][symbol] >= PAIR_MAX_SL:
            STATE["blocked_symbols"][symbol] = now_ts() + PAIR_BLOCK_SECONDS
            notes.append(f"🚫 {display_symbol(symbol)} заблокирован после серии SL.")

        if STATE["stats"]["strategy_side"][strategy_side_key]["consecutive_sl"] >= STRATEGY_SIDE_MAX_CONSECUTIVE_SL:
            STATE["strategy_side_hard_disabled_until"][strategy_side_key] = now_ts() + STRATEGY_SIDE_DISABLE_SECONDS
            STATE["stats"]["strategy_side"][strategy_side_key]["consecutive_sl"] = 0
            notes.append(f"⛔ {strategy_side_key} отключён после серии SL.")

    elif result in ["TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        STATE["stats"]["side"][side]["consecutive_sl"] = 0
        STATE["stats"]["strategy"][strategy]["consecutive_sl"] = 0
        STATE["stats"]["strategy_side"][strategy_side_key]["consecutive_sl"] = 0

        if not signal.get("counted_positive"):
            signal["counted_positive"] = True

            STATE["stats"]["side"][side]["positive"] += 1
            STATE["stats"]["strategy"][strategy]["positive"] += 1
            STATE["stats"]["strategy_side"][strategy_side_key]["positive"] += 1
            STATE["stats"]["grade"][grade]["positive"] += 1
            STATE["stats"]["pair_positive"][symbol] = STATE["stats"]["pair_positive"].get(symbol, 0) + 1

        if result == "TP1" and not signal.get("counted_tp1"):
            signal["counted_tp1"] = True
            STATE["stats"]["side"][side]["tp1"] += 1

        if result == "TP2" and not signal.get("counted_tp2"):
            signal["counted_tp2"] = True
            STATE["stats"]["side"][side]["tp2"] += 1

        if result == "TP3" and not signal.get("counted_tp3"):
            signal["counted_tp3"] = True
            STATE["stats"]["side"][side]["tp3"] += 1

    save_state(STATE)
    return notes


def is_signal_expired(signal: dict) -> bool:
    created_at = signal.get("created_at", 0)
    return bool(created_at and now_ts() - created_at > SIGNAL_MAX_LIFETIME_SECONDS)


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

            if not signal.get("tp1_hit") and high >= signal["tp1"]:
                signal["tp1_hit"] = True

                if high >= signal["tp2"]:
                    signal["tp2_hit"] = True

                if high >= signal["tp3"]:
                    signal["tp3_hit"] = True

                return "TP1", signal["tp1"]

            if not signal.get("tp1_hit") and low <= signal["sl"]:
                return "SL", signal["sl"]

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

            if not signal.get("tp1_hit") and low <= signal["tp1"]:
                signal["tp1_hit"] = True

                if low <= signal["tp2"]:
                    signal["tp2_hit"] = True

                if low <= signal["tp3"]:
                    signal["tp3_hit"] = True

                return "TP1", signal["tp1"]

            if not signal.get("tp1_hit") and high >= signal["sl"]:
                return "SL", signal["sl"]

            if signal.get("tp1_hit") and not signal.get("tp2_hit") and low <= signal["tp2"]:
                signal["tp2_hit"] = True
                return "TP2", signal["tp2"]

            if signal.get("tp2_hit") and not signal.get("tp3_hit") and low <= signal["tp3"]:
                signal["tp3_hit"] = True
                return "TP3", signal["tp3"]

    return None, new_candles[-1]["close"]


def build_result_message(signal: dict, result: str, price: Optional[float], notes: List[str]) -> str:
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
    elif result == "EXPIRED":
        title = "⌛ Сигнал устарел"
        status_text = "Сигнал не достиг TP/SL за установленное время и удалён из активных."
    else:
        title = f"ℹ️ {result}"
        status_text = "Обновление по сделке."

    adaptive_text = ""

    if notes:
        adaptive_text = "\n\n<b>Адаптация бота:</b>\n" + "\n".join(notes)

    stats_text = build_stats_text()

    return f"""
{title}

<b>{signal.get('grade', 'STRONG')} · {signal['direction']} {signal['display_symbol']}</b>
<b>Стратегия:</b> {strategy_title(signal['strategy'])}
<b>Режим:</b> {regime_title(signal.get('market_regime'))}

Вход: <code>{signal['entry']}</code>
Текущая цена: <code>{'n/a' if price is None else round(price, 8)}</code>

TP1: <code>{signal['tp1']}</code>
TP2: <code>{signal['tp2']}</code>
TP3: <code>{signal['tp3']}</code>
SL: <code>{signal['sl']}</code>

{status_text}

{stats_text}
{adaptive_text}
""".strip()


def track_active_signals(send_to_telegram: bool = True) -> dict:
    cleanup_state()

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
        if is_signal_expired(signal):
            message = build_result_message(signal, "EXPIRED", None, [])
            telegram = send_telegram_message(message) if send_to_telegram else None

            results.append({
                "signal_id": signal_id,
                "symbol": signal.get("display_symbol"),
                "grade": signal.get("grade"),
                "direction": signal.get("direction"),
                "strategy": signal.get("strategy"),
                "result": "EXPIRED",
                "price": None,
                "telegram": telegram,
            })

            finished.append(signal_id)
            continue

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
            "grade": signal.get("grade"),
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


# =========================
# AUTO WORKER
# =========================

async def auto_worker():
    await asyncio.sleep(10)

    while True:
        try:
            current_time = now_ts()

            if AUTO_TRACK_ENABLED:
                last_track = STATE["auto"].get("last_track_time", 0)

                if current_time - last_track >= AUTO_TRACK_SECONDS:
                    result = await asyncio.to_thread(track_active_signals, True)
                    STATE["auto"]["last_track_time"] = current_time
                    STATE["auto"]["last_track_result"] = result
                    save_state(STATE)

            if AUTO_SCAN_ENABLED:
                last_scan = STATE["auto"].get("last_scan_time", 0)

                if current_time - last_scan >= AUTO_SCAN_SECONDS:
                    result = await asyncio.to_thread(scan_best_signal, DEFAULT_DEPOSIT, DEFAULT_RISK_PERCENT)

                    STATE["auto"]["last_scan_time"] = current_time
                    STATE["auto"]["last_scan_result"] = result

                    if result.get("ok"):
                        signal = result["signal"]
                        message = result["message"]

                        telegram = send_telegram_message(message)
                        result["telegram"] = telegram

                        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
                            save_signal(signal)
                        else:
                            STATE["auto"]["last_error"] = f"Telegram не отправил сигнал: {telegram}"

                    else:
                        last_report = STATE["auto"].get("last_no_signal_report_time", 0)

                        if DEBUG_NO_SIGNAL_REPORT_ENABLED and current_time - last_report >= DEBUG_NO_SIGNAL_REPORT_SECONDS:
                            report = (
                                "🧠 <b>Диагностика V11</b>\n\n"
                                f"Проверено пар: {result.get('checked', 0)}\n"
                                f"BTC статус: {result.get('btc_status', 'NEUTRAL')}\n"
                                "Сильных сигналов сейчас нет.\n\n"
                                "Что это значит:\n"
                                "• нет понятной реакции рынка;\n"
                                "• цена в середине диапазона;\n"
                                "• вход поздний после импульса;\n"
                                "• нет отката к EMA/VWAP;\n"
                                "• BTC против направления;\n"
                                "• RR или риск до SL слабый.\n\n"
                                "Бот продолжает сканировать."
                            )

                            send_telegram_message(report)
                            STATE["auto"]["last_no_signal_report_time"] = current_time

                    save_state(STATE)

            await asyncio.sleep(10)

        except Exception as e:
            STATE["auto"]["last_error"] = str(e)
            save_state(STATE)
            await asyncio.sleep(30)


# =========================
# ROUTES
# =========================

@app.on_event("startup")
async def startup_event():
    text = (
        f"✅ <b>{APP_NAME} запущен</b>\n"
        f"Deploy marker: <code>{DEPLOY_MARKER}</code>\n\n"
        f"Режим: {'TEST' if TEST_MODE else 'LIVE'}\n"
        f"Auto Scan: {'ON' if AUTO_SCAN_ENABLED else 'OFF'} / {AUTO_SCAN_SECONDS} сек.\n"
        f"Auto Track: {'ON' if AUTO_TRACK_ENABLED else 'OFF'} / {AUTO_TRACK_SECONDS} сек.\n"
        f"Weak signals: {'ON' if ALLOW_WEAK_SIGNALS else 'OFF'}\n"
        f"Quality only: {'ON' if QUALITY_ONLY_MODE else 'OFF'}\n"
        f"Risky extreme: {'ON' if ALLOW_RISKY_EXTREME_TRADES else 'OFF'}\n"
        f"Max symbols: {MAX_SYMBOLS}\n\n"
        "V11 логика: бот сначала определяет режим рынка "
        "FAST / TREND / RANGE / EXTREME, потом выбирает стратегию. "
        "В Telegram будет стратегия, сила сигнала, модельная вероятность, вход, SL, TP и время отработки."
    )

    send_telegram_message(text)
    asyncio.create_task(auto_worker())


@app.get("/", response_class=HTMLResponse)
def home():
    return f"""
<!DOCTYPE html>
<html>
<head>
    <title>{APP_NAME}</title>
</head>
<body style="background:#020617;color:#e5e7eb;font-family:Arial;padding:40px;">
    <h1>✅ {APP_NAME} работает</h1>
    <p>Deploy marker: <b>{DEPLOY_MARKER}</b></p>
    <pre>
GET /health
GET /version
GET /scan?send_to_telegram=false
GET /auto-signal?symbol=NEAR/USDT
GET /track
GET /stats
GET /auto-status
GET /test-telegram?key=YOUR_API_KEY
GET /reset-state?key=YOUR_API_KEY
    </pre>
</body>
</html>
"""


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": APP_NAME,
        "deploy_marker": DEPLOY_MARKER,
        "test_mode": TEST_MODE,
        "auto_scan_enabled": AUTO_SCAN_ENABLED,
        "auto_track_enabled": AUTO_TRACK_ENABLED,
        "active_signals": len(STATE["active_signals"]),
        "blocked_symbols": len(STATE["blocked_symbols"]),
        "max_symbols": MAX_SYMBOLS,
        "weak_signals": ALLOW_WEAK_SIGNALS,
        "quality_only_mode": QUALITY_ONLY_MODE,
    }


@app.get("/version")
def version():
    return {
        "ok": True,
        "service": APP_NAME,
        "deploy_marker": DEPLOY_MARKER,
        "logic": "V11 Market Regime Engine",
        "regimes": ["FAST_MOMENTUM", "TREND_CONTINUATION", "RANGE_STRUCTURE", "EXTREME_ONLY"],
        "strong_min_score": A_PLUS_MIN_SCORE,
        "weak_min_score": WEAK_MIN_SCORE,
        "min_target_roi_tp1": MIN_TARGET_ROI_PERCENT,
        "leverage": LEVERAGE,
    }


@app.get("/auto-status")
def auto_status():
    return {
        "ok": True,
        "auto": STATE.get("auto", {}),
        "active_signals": len(STATE["active_signals"]),
        "blocked_symbols": len(STATE["blocked_symbols"]),
    }


@app.get("/test-telegram")
def test_telegram(key: str = Query(default="")):
    require_api_key(key)
    return send_telegram_message(f"✅ {APP_NAME} подключён к Telegram. Deploy marker: {DEPLOY_MARKER}")


@app.get("/auto-signal")
def auto_signal(
    symbol: str = Query(default="NEAR/USDT"),
    direction: Optional[str] = Query(default=None),
    deposit: float = Query(default=DEFAULT_DEPOSIT),
    risk_percent: float = Query(default=DEFAULT_RISK_PERCENT),
    send_to_telegram: bool = Query(default=False),
    key: str = Query(default="")
):
    if send_to_telegram:
        require_api_key(key)

    btc_status = detect_btc_status()

    signal = analyze_symbol(
        symbol=symbol,
        direction=direction,
        deposit=deposit,
        risk_percent=risk_percent,
        btc_status=btc_status,
    )

    if not signal:
        return {
            "ok": False,
            "symbol": display_symbol(symbol),
            "direction": direction,
            "btc_status": btc_status,
            "message": "Сильного сигнала нет. Вход запрещён.",
        }

    message = build_message(signal)
    telegram = None

    if send_to_telegram:
        telegram = send_telegram_message(message)

        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
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
    risk_percent: float = Query(default=DEFAULT_RISK_PERCENT),
    key: str = Query(default="")
):
    if send_to_telegram:
        require_api_key(key)

    result = scan_best_signal(deposit, risk_percent)

    if not result.get("ok"):
        return result

    telegram = None

    if send_to_telegram:
        telegram = send_telegram_message(result["message"])

        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
            save_signal(result["signal"])

    result["telegram"] = telegram
    return result


@app.get("/track")
def track(
    send_to_telegram: bool = Query(default=True),
    key: str = Query(default="")
):
    if send_to_telegram:
        require_api_key(key)

    return track_active_signals(send_to_telegram=send_to_telegram)


@app.get("/stats")
def stats():
    ensure_stats_structure()

    return {
        "ok": True,
        "stats": STATE["stats"],
        "stats_text": build_stats_text(),
        "active_signals": len(STATE["active_signals"]),
        "blocked_symbols": {
            display_symbol(k): int(v - now_ts())
            for k, v in STATE["blocked_symbols"].items()
            if v > now_ts()
        },
        "strategy_side_hard_disabled_until": {
            k: int(v - now_ts())
            for k, v in STATE["strategy_side_hard_disabled_until"].items()
            if v > now_ts()
        },
    }


@app.get("/cleanup-state")
def cleanup_state_endpoint(key: str = Query(default="")):
    require_api_key(key)
    cleanup_state()

    return {
        "ok": True,
        "message": "State cleanup completed.",
        "sent_signals": len(STATE.get("sent_signals", {})),
        "cooldowns": len(STATE.get("symbol_cooldown", {})),
        "blocked_symbols": len(STATE.get("blocked_symbols", {})),
    }


@app.get("/reset-state")
def reset_state(key: str = Query(default="")):
    require_api_key(key)

    global STATE
    STATE = default_state()
    save_state(STATE)

    return {
        "ok": True,
        "message": "State reset completed.",
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("bot:app", host="0.0.0.0", port=port)
