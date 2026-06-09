import os
import time
import json
import random
import asyncio
import requests
from typing import Optional, List, Any, Tuple

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse


APP_NAME = "Professional Adaptive Futures Bot AUTO V5.5 HTF Confirm Balanced"
app = FastAPI(title=APP_NAME)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
BINGX_BASE_URL = "https://open-api.bingx.com"
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")
API_KEY = os.getenv("API_KEY", "")

TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
DEFAULT_DEPOSIT = float(os.getenv("DEFAULT_DEPOSIT", "1000"))
DEFAULT_RISK_PERCENT = float(os.getenv("DEFAULT_RISK_PERCENT", "0.5"))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "220"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))

AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "180"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "45"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "1200"))
PAIR_BLOCK_SECONDS = int(os.getenv("PAIR_BLOCK_SECONDS", "21600"))
STRATEGY_SIDE_DISABLE_SECONDS = int(os.getenv("STRATEGY_SIDE_DISABLE_SECONDS", "7200"))
PAIR_MAX_SL = int(os.getenv("PAIR_MAX_SL", "3"))
STRATEGY_SIDE_MAX_CONSECUTIVE_SL = int(os.getenv("STRATEGY_SIDE_MAX_CONSECUTIVE_SL", "4"))

SIGNAL_MAX_LIFETIME_SECONDS = int(os.getenv("SIGNAL_MAX_LIFETIME_SECONDS", "21600"))
SENT_SIGNALS_KEEP_SECONDS = int(os.getenv("SENT_SIGNALS_KEEP_SECONDS", "604800"))
SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK = os.getenv("SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK", "true").lower() == "true"

DEBUG_NO_SIGNAL_REPORT_ENABLED = os.getenv("DEBUG_NO_SIGNAL_REPORT_ENABLED", "true").lower() == "true"
DEBUG_NO_SIGNAL_REPORT_SECONDS = int(os.getenv("DEBUG_NO_SIGNAL_REPORT_SECONDS", "7200"))

# V5.5 HTF Confirm Balanced: больше сигналов, но A+ остаётся качественным.
BALANCED_PRO_MODE = os.getenv("BALANCED_PRO_MODE", "true").lower() == "true"
USE_CLOSED_CANDLES_ONLY = os.getenv("USE_CLOSED_CANDLES_ONLY", "true").lower() == "true"

# A+ не слишком редкий, но требует качества.
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "82"))
A_PLUS_MIN_VOLUME_RATIO = float(os.getenv("A_PLUS_MIN_VOLUME_RATIO", "1.12"))
A_PLUS_MIN_RR = float(os.getenv("A_PLUS_MIN_RR", "0.80"))
A_PLUS_RISK_MULTIPLIER = float(os.getenv("A_PLUS_RISK_MULTIPLIER", "1.0"))

# B живее, чтобы бот реально давал заявки/сигналы. Риск ниже.
B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "68"))
B_MIN_VOLUME_RATIO = float(os.getenv("B_MIN_VOLUME_RATIO", "0.85"))
B_MIN_RR = float(os.getenv("B_MIN_RR", "0.45"))
B_RISK_MULTIPLIER = float(os.getenv("B_RISK_MULTIPLIER", "0.25"))

# Level B ещё чуть живее, потому что рынок не всегда даёт идеальный ретест.
LEVEL_B_MIN_SCORE = int(os.getenv("LEVEL_B_MIN_SCORE", "66"))
LEVEL_B_MIN_VOLUME_RATIO = float(os.getenv("LEVEL_B_MIN_VOLUME_RATIO", "0.82"))
LEVEL_B_MIN_RR = float(os.getenv("LEVEL_B_MIN_RR", "0.40"))
LEVEL_SIGNAL_SCORE_BONUS = int(os.getenv("LEVEL_SIGNAL_SCORE_BONUS", "3"))

# Stats-aware A+ — не душим слишком рано, но плохие связки режем.
STATS_AWARE_A_PLUS_ENABLED = os.getenv("STATS_AWARE_A_PLUS_ENABLED", "true").lower() == "true"
A_PLUS_MIN_STRATEGY_TRADES = int(os.getenv("A_PLUS_MIN_STRATEGY_TRADES", "18"))
A_PLUS_MIN_STRATEGY_WR = float(os.getenv("A_PLUS_MIN_STRATEGY_WR", "52"))

# Risk/TP model.
MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "14"))
TP1_R_MULTIPLIER = float(os.getenv("TP1_R_MULTIPLIER", "0.75"))
TP2_R_MULTIPLIER = float(os.getenv("TP2_R_MULTIPLIER", "1.25"))
TP3_R_MULTIPLIER = float(os.getenv("TP3_R_MULTIPLIER", "1.90"))
MIN_TP1_PRICE_MOVE_PERCENT = float(os.getenv("MIN_TP1_PRICE_MOVE_PERCENT", "0.45"))
TP1_CLOSE_PERCENT = float(os.getenv("TP1_CLOSE_PERCENT", "50"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.0005"))
SLIPPAGE_RATE = float(os.getenv("SLIPPAGE_RATE", "0.0003"))

# Late entry filters. Defaults are not too strict.
ENABLE_LATE_ENTRY_FILTER = os.getenv("ENABLE_LATE_ENTRY_FILTER", "true").lower() == "true"
MAX_RECENT_MOVE_PERCENT = float(os.getenv("MAX_RECENT_MOVE_PERCENT", "7.5"))
MAX_DISTANCE_FROM_VWAP_PERCENT = float(os.getenv("MAX_DISTANCE_FROM_VWAP_PERCENT", "5.8"))
MAX_LEVEL_LONG_15M_MOVE_PERCENT = float(os.getenv("MAX_LEVEL_LONG_15M_MOVE_PERCENT", "8.0"))

# Space filter: professional, but soft. If no next level found, signal is allowed.
ENABLE_SPACE_TO_TARGET_FILTER = os.getenv("ENABLE_SPACE_TO_TARGET_FILTER", "true").lower() == "true"
MIN_SPACE_TO_TARGET_PERCENT_A_PLUS = float(os.getenv("MIN_SPACE_TO_TARGET_PERCENT_A_PLUS", "0.45"))
MIN_SPACE_TO_TARGET_PERCENT_B = float(os.getenv("MIN_SPACE_TO_TARGET_PERCENT_B", "0.25"))

# Funding/OI filters — funding extreme blocks, OI availability alone does not add fake points.
ENABLE_FUNDING_FILTER = os.getenv("ENABLE_FUNDING_FILTER", "true").lower() == "true"
ENABLE_OI_FILTER = os.getenv("ENABLE_OI_FILTER", "true").lower() == "true"
MAX_ABS_FUNDING_RATE = float(os.getenv("MAX_ABS_FUNDING_RATE", "0.0012"))
FUNDING_EXTREME_RATE = float(os.getenv("FUNDING_EXTREME_RATE", "0.0025"))

# BTC regime: not too strict. Against BTC can pass as B only.
ALLOW_COUNTER_BTC_B_SIGNALS = os.getenv("ALLOW_COUNTER_BTC_B_SIGNALS", "true").lower() == "true"
COUNTER_BTC_RISK_MULTIPLIER = float(os.getenv("COUNTER_BTC_RISK_MULTIPLIER", "0.18"))

# V5.5 HTF Confirm Balanced: 1H/4H подтверждение для A+ и B.
# A+ требует подтверждение и 1H, и 4H.
# B не душим: достаточно 1H или 4H, но если оба старших ТФ против — сигнал режется.
HTF_CONFIRMATION_ENABLED = os.getenv("HTF_CONFIRMATION_ENABLED", "true").lower() == "true"
A_PLUS_REQUIRES_1H_4H_CONFIRM = os.getenv("A_PLUS_REQUIRES_1H_4H_CONFIRM", "true").lower() == "true"
B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM = os.getenv("B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM", "true").lower() == "true"
ALLOW_B_WHEN_1H_NEUTRAL_4H_CONFIRMS = os.getenv("ALLOW_B_WHEN_1H_NEUTRAL_4H_CONFIRMS", "true").lower() == "true"
BLOCK_IF_BOTH_HTF_AGAINST = os.getenv("BLOCK_IF_BOTH_HTF_AGAINST", "true").lower() == "true"
HTF_1H_CONFIRM_BONUS = int(os.getenv("HTF_1H_CONFIRM_BONUS", "5"))
HTF_4H_CONFIRM_BONUS = int(os.getenv("HTF_4H_CONFIRM_BONUS", "4"))
HTF_FULL_CONFIRM_BONUS = int(os.getenv("HTF_FULL_CONFIRM_BONUS", "3"))
HTF_NO_CONFIRM_B_RISK_MULTIPLIER = float(os.getenv("HTF_NO_CONFIRM_B_RISK_MULTIPLIER", "0.14"))

# Impulse Pullback Pro: designed to produce more B signals.
IMPULSE_PULLBACK_ENABLED = os.getenv("IMPULSE_PULLBACK_ENABLED", "true").lower() == "true"
IMPULSE_PULLBACK_RISK_MULTIPLIER = float(os.getenv("IMPULSE_PULLBACK_RISK_MULTIPLIER", "0.22"))
IMPULSE_MIN_MOVE_5M_PERCENT = float(os.getenv("IMPULSE_MIN_MOVE_5M_PERCENT", "0.75"))
IMPULSE_PULLBACK_MIN_PERCENT = float(os.getenv("IMPULSE_PULLBACK_MIN_PERCENT", "0.15"))
IMPULSE_PULLBACK_MAX_PERCENT = float(os.getenv("IMPULSE_PULLBACK_MAX_PERCENT", "3.60"))
IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT = float(os.getenv("IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT", "3.20"))
IMPULSE_MIN_VOLUME_RATIO = float(os.getenv("IMPULSE_MIN_VOLUME_RATIO", "0.85"))

STRATEGIES = [
    "LEVEL_SWEEP_BOUNCE_LONG",
    "LEVEL_RESISTANCE_REJECT_SHORT",
    "LEVEL_BREAK_RETEST_LONG",
    "LEVEL_BREAK_RETEST_SHORT",
    "TREND_PULLBACK_PRO",
    "VWAP_RECLAIM_PRO",
    "IMPULSE_PULLBACK_PRO",
]

LIQUID_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "INJ", "NEAR", "ARB", "OP", "APT", "SUI", "SEI", "DOT", "LTC",
    "BCH", "UNI", "AAVE", "FIL", "ATOM", "ETC", "TRX", "MATIC", "POL",
    "WLD", "TIA", "ORDI", "FTM", "RUNE", "ENA", "JUP", "PYTH", "STRK",
    "DYDX", "TON", "COMP", "STX", "TRB", "JTO", "DYM", "ICP", "GALA",
    "FET", "RNDR", "RENDER", "IMX", "APE", "AR", "MKR", "SNX", "LDO",
    "CRV", "GMT", "PEPE", "1000PEPE", "WIF", "BONK", "NOT", "ONDO",
    "BLUR", "MEME", "AI", "ACE", "ARKM", "PENDLE", "BIGTIME", "ZRO",
    "ZK", "TAO", "1000SATS", "SAGA", "MANTA", "ALT", "PIXEL", "PORTAL",
    "AEVO", "W", "OMNI", "TNSR", "BB", "PEOPLE", "ORDI", "1000SHIB",
}


def now_ts() -> float:
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
    return direction if direction in ["LONG", "SHORT"] else None


def is_good_symbol(symbol: str) -> bool:
    symbol = normalize_symbol(symbol)
    base = base_from_symbol(symbol)
    if base not in LIQUID_BASES:
        return False
    bad = ["USD", "EUR", "GBP", "JPY", "AAPL", "TSLA", "NVDA", "META", "GOOG"]
    return not any(x in base for x in bad)


def strategy_side_default() -> dict:
    return {f"{strategy}:{side}": 0 for strategy in STRATEGIES for side in ["LONG", "SHORT"]}


def strategy_side_grade_default() -> dict:
    return {f"{strategy}:{side}:{grade}": 0 for strategy in STRATEGIES for side in ["LONG", "SHORT"] for grade in ["A+", "B"]}


def strategy_stats_default() -> dict:
    return {strategy: {"positive": 0, "sl": 0, "consecutive_sl": 0} for strategy in STRATEGIES}


def strategy_side_stats_default() -> dict:
    return {f"{strategy}:{side}": {"positive": 0, "sl": 0, "consecutive_sl": 0} for strategy in STRATEGIES for side in ["LONG", "SHORT"]}


def strategy_side_grade_stats_default() -> dict:
    return {
        f"{strategy}:{side}:{grade}": {"positive": 0, "sl": 0, "consecutive_sl": 0}
        for strategy in STRATEGIES
        for side in ["LONG", "SHORT"]
        for grade in ["A+", "B"]
    }


def default_state() -> dict:
    return {
        "active_signals": {},
        "sent_signals": {},
        "symbol_cooldown": {},
        "blocked_symbols": {},
        "strategy_side_hard_disabled_until": strategy_side_default(),
        "strategy_side_grade_disabled_until": strategy_side_grade_default(),
        "stats": {
            "side": {
                "LONG": {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0},
                "SHORT": {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0},
            },
            "strategy": strategy_stats_default(),
            "strategy_side": strategy_side_stats_default(),
            "strategy_side_grade": strategy_side_grade_stats_default(),
            "grade": {"A+": {"positive": 0, "sl": 0}, "B": {"positive": 0, "sl": 0}},
            "pair_sl": {},
            "pair_positive": {},
            "closed_trades": [],
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

    for side in ["LONG", "SHORT"]:
        state["stats"]["side"].setdefault(side, {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0})

    for grade in ["A+", "B"]:
        state["stats"]["grade"].setdefault(grade, {"positive": 0, "sl": 0})

    state.setdefault("strategy_side_hard_disabled_until", {})
    state.setdefault("strategy_side_grade_disabled_until", {})
    state["stats"].setdefault("strategy", {})
    state["stats"].setdefault("strategy_side", {})
    state["stats"].setdefault("strategy_side_grade", {})
    state["stats"].setdefault("closed_trades", [])
    state["stats"].setdefault("pair_sl", {})
    state["stats"].setdefault("pair_positive", {})

    for strategy in STRATEGIES:
        state["stats"]["strategy"].setdefault(strategy, {"positive": 0, "sl": 0, "consecutive_sl": 0})
        for side in ["LONG", "SHORT"]:
            ss = f"{strategy}:{side}"
            state["strategy_side_hard_disabled_until"].setdefault(ss, 0)
            state["stats"]["strategy_side"].setdefault(ss, {"positive": 0, "sl": 0, "consecutive_sl": 0})
            for grade in ["A+", "B"]:
                ssg = f"{strategy}:{side}:{grade}"
                state["strategy_side_grade_disabled_until"].setdefault(ssg, 0)
                state["stats"]["strategy_side_grade"].setdefault(ssg, {"positive": 0, "sl": 0, "consecutive_sl": 0})
    return state


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return ensure_state_structure(json.load(f))
    except Exception:
        return default_state()


def save_state(state: dict):
    try:
        ensure_state_structure(state)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


STATE = load_state()


def ensure_stats_structure():
    global STATE
    STATE = ensure_state_structure(STATE)


def calc_winrate(positive: int, sl: int) -> float:
    total = positive + sl
    return round(positive / total * 100, 1) if total > 0 else 0.0


def calc_profit_factor_from_closed() -> float:
    trades = STATE.get("stats", {}).get("closed_trades", [])
    wins = sum(float(t.get("r_multiple", 0)) for t in trades if float(t.get("r_multiple", 0)) > 0)
    losses = abs(sum(float(t.get("r_multiple", 0)) for t in trades if float(t.get("r_multiple", 0)) < 0))
    if losses <= 0:
        return round(wins, 2) if wins > 0 else 0.0
    return round(wins / losses, 2)


def build_stats_text() -> str:
    ensure_stats_structure()
    long_stats = STATE["stats"]["side"]["LONG"]
    short_stats = STATE["stats"]["side"]["SHORT"]
    long_wr = calc_winrate(long_stats["positive"], long_stats["sl"])
    short_wr = calc_winrate(short_stats["positive"], short_stats["sl"])

    lines = []
    for strategy in STRATEGIES:
        s = STATE["stats"]["strategy"].get(strategy, {"positive": 0, "sl": 0})
        wr = calc_winrate(s.get("positive", 0), s.get("sl", 0))
        lines.append(f"{strategy}: {s.get('positive', 0)} позитив / {s.get('sl', 0)} SL / WR {wr}%")

    a_stats = STATE["stats"]["grade"].get("A+", {"positive": 0, "sl": 0})
    b_stats = STATE["stats"]["grade"].get("B", {"positive": 0, "sl": 0})
    a_wr = calc_winrate(a_stats.get("positive", 0), a_stats.get("sl", 0))
    b_wr = calc_winrate(b_stats.get("positive", 0), b_stats.get("sl", 0))

    pf = calc_profit_factor_from_closed()

    return f"""
📊 <b>Статистика V5.5 HTF Confirm Balanced:</b>

📈 LONG: {long_stats['positive']} позитив / {long_stats['sl']} SL / WR {long_wr}%
📉 SHORT: {short_stats['positive']} позитив / {short_stats['sl']} SL / WR {short_wr}%

🏆 A+: {a_stats.get('positive', 0)} позитив / {a_stats.get('sl', 0)} SL / WR {a_wr}%
⚠️ B: {b_stats.get('positive', 0)} позитив / {b_stats.get('sl', 0)} SL / WR {b_wr}%

📐 Profit Factor по закрытым R-сделкам: {pf}

🧠 <b>Стратегии:</b>
{chr(10).join(lines)}
""".strip()


def is_on_cooldown(symbol: str) -> bool:
    symbol = normalize_symbol(symbol)
    last = STATE["symbol_cooldown"].get(symbol)
    return bool(last and now_ts() - last < SIGNAL_COOLDOWN_SECONDS)


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


def cleanup_state():
    current = now_ts()
    for signal_id, ts in list(STATE.get("sent_signals", {}).items()):
        if current - ts > SENT_SIGNALS_KEEP_SECONDS:
            STATE["sent_signals"].pop(signal_id, None)
    for symbol, until in list(STATE.get("blocked_symbols", {}).items()):
        if current > until:
            STATE["blocked_symbols"].pop(symbol, None)
    for symbol, ts in list(STATE.get("symbol_cooldown", {}).items()):
        if current - ts > SIGNAL_COOLDOWN_SECONDS * 3:
            STATE["symbol_cooldown"].pop(symbol, None)
    save_state(STATE)


def is_strategy_side_enabled(strategy: str, side: str) -> bool:
    ensure_stats_structure()
    return now_ts() >= STATE["strategy_side_hard_disabled_until"].get(f"{strategy}:{side}", 0)


def is_strategy_side_grade_enabled(strategy: str, side: str, grade: str) -> bool:
    ensure_stats_structure()
    return now_ts() >= STATE["strategy_side_grade_disabled_until"].get(f"{strategy}:{side}:{grade}", 0)


def get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception:
        return None


def interval_to_ms(interval: str) -> int:
    mapping = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
        "4h": 14_400_000,
        "1d": 86_400_000,
    }
    return mapping.get(interval, 60_000)


def remove_unclosed_candle(candles: Optional[List[dict]], interval: str) -> Optional[List[dict]]:
    if not candles or not USE_CLOSED_CANDLES_ONLY:
        return candles
    if len(candles) < 3:
        return candles
    current_ms = int(time.time() * 1000)
    last_open_time = int(candles[-1]["time"])
    if current_ms < last_open_time + interval_to_ms(interval):
        return candles[:-1]
    return candles


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
    data = get_json(url, params={"symbol": normalize_symbol(symbol), "interval": interval, "limit": limit})
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
    return candles if len(candles) >= 50 else None


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
    elif isinstance(data, list):
        for item in data:
            nested = extract_float_from_nested(item, keys)
            if nested is not None:
                return nested
    return None


def get_funding_rate(symbol: str) -> Optional[float]:
    if not ENABLE_FUNDING_FILTER:
        return None
    endpoints = ["/openApi/swap/v2/quote/premiumIndex", "/openApi/swap/v2/quote/fundingRate"]
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
    endpoints = ["/openApi/swap/v2/quote/openInterest", "/openApi/swap/v2/quote/openInterestStat"]
    for endpoint in endpoints:
        data = get_json(f"{BINGX_BASE_URL}{endpoint}", params={"symbol": normalize_symbol(symbol)})
        if not data:
            continue
        value = extract_float_from_nested(data, ["openInterest", "open_interest", "sumOpenInterest", "value"])
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
    gains, losses = [], []
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
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs[-period:]) / period


def vwap_like(candles: List[dict], period: int = 48) -> Optional[float]:
    if len(candles) < period:
        return None
    total_pv, total_v = 0.0, 0.0
    for c in candles[-period:]:
        typical = (c["high"] + c["low"] + c["close"]) / 3
        total_pv += typical * c["volume"]
        total_v += c["volume"]
    return total_pv / total_v if total_v > 0 else None


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
    return candles[-1]["volume"] / avg if avg > 0 else 0.0


def recent_move_percent(candles: List[dict], lookback: int = 8) -> float:
    if len(candles) < lookback + 1:
        return 0.0
    old_price = candles[-lookback]["close"]
    new_price = candles[-1]["close"]
    return (new_price - old_price) / old_price * 100 if old_price > 0 else 0.0


def distance_from_vwap_percent(price: float, vwap_value: float) -> float:
    return abs(price - vwap_value) / vwap_value * 100 if vwap_value > 0 else 0.0


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


def candle_close_position(candle: dict) -> float:
    high, low, close = candle.get("high", 0), candle.get("low", 0), candle.get("close", 0)
    rng = high - low
    return (close - low) / rng if rng > 0 else 0.5


def find_swing_support_levels(candles: List[dict], lookback: int = 120) -> List[float]:
    if len(candles) < 40:
        return []
    window = candles[-lookback:] if len(candles) >= lookback else candles[:]
    levels = []
    for i in range(3, len(window) - 3):
        low = window[i]["low"]
        if low <= min(window[i - 1]["low"], window[i - 2]["low"], window[i - 3]["low"]) and low <= min(window[i + 1]["low"], window[i + 2]["low"], window[i + 3]["low"]):
            levels.append(low)
    return merge_levels(levels)


def find_swing_resistance_levels(candles: List[dict], lookback: int = 120) -> List[float]:
    if len(candles) < 40:
        return []
    window = candles[-lookback:] if len(candles) >= lookback else candles[:]
    levels = []
    for i in range(3, len(window) - 3):
        high = window[i]["high"]
        if high >= max(window[i - 1]["high"], window[i - 2]["high"], window[i - 3]["high"]) and high >= max(window[i + 1]["high"], window[i + 2]["high"], window[i + 3]["high"]):
            levels.append(high)
    return merge_levels(levels)


def merge_levels(levels: List[float], threshold_percent: float = 0.38) -> List[float]:
    if not levels:
        return []
    levels = sorted(levels)
    merged = []
    for level in levels:
        if not merged:
            merged.append(level)
            continue
        if abs(level - merged[-1]) / merged[-1] * 100 <= threshold_percent:
            merged[-1] = (merged[-1] + level) / 2
        else:
            merged.append(level)
    return merged


def nearest_below(price: float, levels: List[float], max_distance_percent: float = 8.0) -> Optional[float]:
    candidates = [level for level in levels if level < price]
    if not candidates:
        return None
    level = max(candidates)
    return level if abs(price - level) / level * 100 <= max_distance_percent else None


def nearest_above(price: float, levels: List[float], max_distance_percent: float = 8.0) -> Optional[float]:
    candidates = [level for level in levels if level > price]
    if not candidates:
        return None
    level = min(candidates)
    return level if abs(level - price) / price * 100 <= max_distance_percent else None


def nearest_near(price: float, levels: List[float], max_distance_percent: float = 1.8) -> Optional[float]:
    if not levels or price <= 0:
        return None
    level = min(levels, key=lambda x: abs(x - price))
    return level if abs(level - price) / price * 100 <= max_distance_percent else None


def space_to_next_level_percent(price: float, direction: str, support_levels: List[float], resistance_levels: List[float]) -> Optional[float]:
    if price <= 0:
        return None
    if direction == "LONG":
        levels_above = [lvl for lvl in resistance_levels if lvl > price]
        if not levels_above:
            return None
        target = min(levels_above)
        return (target - price) / price * 100
    levels_below = [lvl for lvl in support_levels if lvl < price]
    if not levels_below:
        return None
    target = max(levels_below)
    return (price - target) / price * 100


def has_near_level(price_level: float, levels: List[float], max_distance_percent: float) -> bool:
    if price_level <= 0:
        return False
    return any(abs(lvl - price_level) / price_level * 100 <= max_distance_percent for lvl in levels if lvl > 0)


def level_mtf_strength(level: float, c1h: List[dict], c4h: List[dict], kind: str) -> dict:
    if kind == "support":
        levels_1h = find_swing_support_levels(c1h, lookback=120)
        levels_4h = find_swing_support_levels(c4h, lookback=120)
    else:
        levels_1h = find_swing_resistance_levels(c1h, lookback=120)
        levels_4h = find_swing_resistance_levels(c4h, lookback=120)

    confirmed_1h = has_near_level(level, levels_1h, 1.2)
    confirmed_4h = has_near_level(level, levels_4h, 1.8)
    bonus = (6 if confirmed_1h else 0) + (3 if confirmed_4h else 0)
    return {
        "level_1h_confirmed": confirmed_1h,
        "level_4h_confirmed": confirmed_4h,
        "level_strength_bonus": bonus,
        "level_strength_note": f"1H {'OK' if confirmed_1h else 'NO'} / 4H {'OK' if confirmed_4h else 'NO'}",
    }


def detect_btc_status() -> str:
    btc = remove_unclosed_candle(get_klines("BTC-USDT", "1h", 260), "1h")
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
            score_adjustment -= 2
            reason.append(f"Funding перегрет для LONG: {funding:.6f}")
        elif direction == "SHORT" and funding < -MAX_ABS_FUNDING_RATE:
            score_adjustment -= 2
            reason.append(f"Funding перегрет для SHORT: {funding:.6f}")
        else:
            score_adjustment += 1
            reason.append(f"Funding нормальный: {funding:.6f}")
    else:
        reason.append("Funding недоступен")

    if oi is not None:
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


def is_trend_confirming_direction(trend: str, direction: str) -> bool:
    if direction == "LONG":
        return trend in ["BULLISH", "SOFT_BULLISH"]
    return trend in ["BEARISH", "SOFT_BEARISH"]


def is_trend_hard_against_direction(trend: str, direction: str) -> bool:
    if direction == "LONG":
        return trend == "BEARISH"
    return trend == "BULLISH"


def htf_confirmation_data(direction: str, trend1h: str, trend4h: str) -> dict:
    """
    Balanced HTF confirmation:
    - A+ должен иметь подтверждение 1H и 4H.
    - B должен иметь хотя бы одно подтверждение 1H/4H.
    - Если оба старших ТФ явно против направления — сигнал блокируется.
    """
    confirm_1h = is_trend_confirming_direction(trend1h, direction)
    confirm_4h = is_trend_confirming_direction(trend4h, direction)
    against_1h = is_trend_hard_against_direction(trend1h, direction)
    against_4h = is_trend_hard_against_direction(trend4h, direction)
    full_confirm = confirm_1h and confirm_4h
    any_confirm = confirm_1h or confirm_4h
    both_against = against_1h and against_4h

    score_bonus = 0
    if confirm_1h:
        score_bonus += HTF_1H_CONFIRM_BONUS
    if confirm_4h:
        score_bonus += HTF_4H_CONFIRM_BONUS
    if full_confirm:
        score_bonus += HTF_FULL_CONFIRM_BONUS

    if full_confirm:
        note = f"HTF: 1H {trend1h} + 4H {trend4h} подтверждают направление."
    elif any_confirm:
        note = f"HTF: частичное подтверждение — 1H {trend1h}, 4H {trend4h}. B разрешён, A+ запрещён."
    elif both_against:
        note = f"HTF: 1H {trend1h} и 4H {trend4h} против направления — сигнал заблокирован."
    else:
        note = f"HTF: нет сильного подтверждения — 1H {trend1h}, 4H {trend4h}."

    return {
        "trend1h": trend1h,
        "trend4h": trend4h,
        "htf_1h_confirmed": confirm_1h,
        "htf_4h_confirmed": confirm_4h,
        "htf_full_confirmed": full_confirm,
        "htf_any_confirmed": any_confirm,
        "htf_1h_against": against_1h,
        "htf_4h_against": against_4h,
        "htf_both_against": both_against,
        "htf_score_bonus": score_bonus,
        "htf_note": note,
    }


def attach_htf_confirmation(filters: dict, direction: str, market_data: dict) -> dict:
    if not HTF_CONFIRMATION_ENABLED:
        return filters

    htf = htf_confirmation_data(direction, market_data.get("trend1h", "NEUTRAL"), market_data.get("trend4h", "NEUTRAL"))
    filters.update(htf)
    filters["score_adjustment"] = filters.get("score_adjustment", 0) + htf.get("htf_score_bonus", 0)

    if BLOCK_IF_BOTH_HTF_AGAINST and htf.get("htf_both_against"):
        filters["blocked"] = True

    return filters

def combine_extra_filters(symbol: str, direction: str, btc_status: str) -> dict:
    funding_oi = analyze_funding_oi(symbol, direction)

    btc_against = (
        (direction == "LONG" and btc_status == "BEARISH")
        or (direction == "SHORT" and btc_status == "BULLISH")
    )

    blocked = funding_oi.get("blocked", False)

    filters = {
        "blocked": blocked,
        "score_adjustment": funding_oi.get("score_adjustment", 0),
        "funding": funding_oi,
        "btc_status": btc_status,
        "btc_against": btc_against,
    }

    # Чтобы были сигналы: против BTC не режем полностью, а переводим в B с малым риском.
    if btc_against and ALLOW_COUNTER_BTC_B_SIGNALS:
        filters["force_grade"] = "B"
        filters["risk_multiplier_override"] = COUNTER_BTC_RISK_MULTIPLIER
        filters["allow_btc_countertrend_bounce"] = True
        filters["countertrend_note"] = "BTC против направления: сигнал разрешён только как B с уменьшенным риском."
    elif btc_against:
        filters["blocked"] = True

    return filters


def get_strategy_winrate(strategy: str) -> Tuple[int, float]:
    ensure_stats_structure()
    s = STATE["stats"]["strategy"].get(strategy, {})
    positive, sl = int(s.get("positive", 0)), int(s.get("sl", 0))
    return positive + sl, calc_winrate(positive, sl)


def can_strategy_be_a_plus(strategy: str, direction: str) -> bool:
    if not STATS_AWARE_A_PLUS_ENABLED:
        return True
    trades, wr = get_strategy_winrate(strategy)
    if trades >= A_PLUS_MIN_STRATEGY_TRADES and wr < A_PLUS_MIN_STRATEGY_WR:
        return False
    return True


def is_level_strategy(strategy: str) -> bool:
    return strategy in {
        "LEVEL_SWEEP_BOUNCE_LONG",
        "LEVEL_RESISTANCE_REJECT_SHORT",
        "LEVEL_BREAK_RETEST_LONG",
        "LEVEL_BREAK_RETEST_SHORT",
    }


def estimate_trade_cost_percent() -> float:
    return (FEE_RATE * 2 + SLIPPAGE_RATE * 2) * 100


def calc_risk_position(entry: float, sl: float) -> float:
    return abs(entry - sl) / entry * 100 * LEVERAGE if entry > 0 else 999


def price_move_percent(entry: float, target: float, direction: str) -> float:
    if entry <= 0:
        return 0
    if direction == "LONG":
        return (target - entry) / entry * 100
    return (entry - target) / entry * 100


def make_dynamic_tps(entry: float, sl: float, direction: str) -> Tuple[float, float, float]:
    risk = abs(entry - sl)
    min_move = MIN_TP1_PRICE_MOVE_PERCENT / 100
    if risk <= 0 or entry <= 0:
        if direction == "LONG":
            return entry * (1 + min_move), entry * (1 + min_move * 1.8), entry * (1 + min_move * 2.8)
        return entry * (1 - min_move), entry * (1 - min_move * 1.8), entry * (1 - min_move * 2.8)

    if direction == "LONG":
        tp1 = max(entry + risk * TP1_R_MULTIPLIER, entry * (1 + min_move))
        tp2 = max(entry + risk * TP2_R_MULTIPLIER, entry * (1 + min_move * 1.8))
        tp3 = max(entry + risk * TP3_R_MULTIPLIER, entry * (1 + min_move * 2.8))
    else:
        tp1 = min(entry - risk * TP1_R_MULTIPLIER, entry * (1 - min_move))
        tp2 = min(entry - risk * TP2_R_MULTIPLIER, entry * (1 - min_move * 1.8))
        tp3 = min(entry - risk * TP3_R_MULTIPLIER, entry * (1 - min_move * 2.8))

    return tp1, tp2, tp3


def calculate_position(entry: float, sl: float, deposit: float, risk_percent: float) -> dict:
    risk_amount = deposit * risk_percent / 100
    stop_distance = abs(entry - sl)
    if entry <= 0 or sl <= 0 or stop_distance <= 0:
        return {
            "risk_amount": round(risk_amount, 2),
            "position_size_usdt": None,
            "coin_amount": None,
            "margin_usdt": None,
            "error": "Неверный entry или SL",
        }
    coin_amount = risk_amount / stop_distance
    position_size = coin_amount * entry
    return {
        "risk_amount": round(risk_amount, 2),
        "position_size_usdt": round(position_size, 2),
        "coin_amount": round(coin_amount, 8),
        "margin_usdt": round(position_size / LEVERAGE, 2),
        "error": None,
    }


def classify_signal(score: int, rr: float, volume: float, filters: dict, strategy: str, direction: str) -> Optional[dict]:
    if filters.get("blocked"):
        return None

    funding = filters.get("funding", {})

    b_score, b_rr, b_vol = B_MIN_SCORE, B_MIN_RR, B_MIN_VOLUME_RATIO
    if is_level_strategy(strategy):
        b_score, b_rr, b_vol = min(b_score, LEVEL_B_MIN_SCORE), min(b_rr, LEVEL_B_MIN_RR), min(b_vol, LEVEL_B_MIN_VOLUME_RATIO)

    htf_full = filters.get("htf_full_confirmed", False)
    htf_any = filters.get("htf_any_confirmed", False)
    htf_both_against = filters.get("htf_both_against", False)

    if HTF_CONFIRMATION_ENABLED and htf_both_against:
        return None

    # Если какой-то другой фильтр уже решил, что сигнал только B — уважаем это.
    if filters.get("force_grade") == "B":
        if score >= b_score and rr >= b_rr and volume >= b_vol and not funding.get("blocked"):
            if HTF_CONFIRMATION_ENABLED and B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM and not htf_any:
                # Чтобы бот не молчал полностью, можно оставить такой сетап только если он очень сильный,
                # но с ещё меньшим риском. По умолчанию лучше не пропускать без HTF.
                return None
            return {"grade": "B", "risk_multiplier": filters.get("risk_multiplier_override", B_RISK_MULTIPLIER)}
        return None

    # A+ — теперь действительно старше-таймфреймовый сигнал.
    a_plus_htf_allowed = True
    if HTF_CONFIRMATION_ENABLED and A_PLUS_REQUIRES_1H_4H_CONFIRM:
        a_plus_htf_allowed = htf_full

    # Для level-стратегий старый level-confirm оставляем как дополнительный плюс, но не замену HTF.
    level_a_plus_allowed = True
    if is_level_strategy(strategy):
        level_a_plus_allowed = filters.get("level_1h_confirmed", True) or score >= A_PLUS_MIN_SCORE + 8

    if (
        score >= A_PLUS_MIN_SCORE
        and rr >= A_PLUS_MIN_RR
        and volume >= A_PLUS_MIN_VOLUME_RATIO
        and not funding.get("blocked")
        and can_strategy_be_a_plus(strategy, direction)
        and level_a_plus_allowed
        and a_plus_htf_allowed
    ):
        return {"grade": "A+", "risk_multiplier": A_PLUS_RISK_MULTIPLIER}

    # B — живой, но тоже должен уважать 1H/4H.
    if score >= b_score and rr >= b_rr and volume >= b_vol and not funding.get("blocked"):
        if HTF_CONFIRMATION_ENABLED and B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM and not htf_any:
            return None
        return {"grade": "B", "risk_multiplier": filters.get("risk_multiplier_override", B_RISK_MULTIPLIER)}

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
    extra_filters: dict
) -> Optional[dict]:
    risk_pos = calc_risk_position(entry, sl)
    if risk_pos > MAX_RISK_POSITION_PERCENT:
        return None

    score += extra_filters.get("score_adjustment", 0)
    if is_level_strategy(strategy):
        score += LEVEL_SIGNAL_SCORE_BONUS

    # Space-to-target: не слишком строгий, чтобы заявки были.
    space = extra_filters.get("space_to_target_percent")
    if ENABLE_SPACE_TO_TARGET_FILTER and space is not None:
        if score >= A_PLUS_MIN_SCORE and space < MIN_SPACE_TO_TARGET_PERCENT_A_PLUS:
            extra_filters["force_grade"] = "B"
            extra_filters["space_note"] = f"До ближайшего уровня мало места: {round(space, 2)}%. A+ запрещён, но B разрешён."
        elif score < A_PLUS_MIN_SCORE and space < MIN_SPACE_TO_TARGET_PERCENT_B:
            return None

    tp1, tp2, tp3 = make_dynamic_tps(entry, sl, direction)

    risk_price = abs(entry - sl) / entry * 100 if entry > 0 else 0
    trade_cost = estimate_trade_cost_percent()

    # Классифицируем по TP2, потому что TP1 — это частичная фиксация/де-риск.
    raw_reward_tp2 = price_move_percent(entry, tp2, direction)
    net_reward_tp2 = max(raw_reward_tp2 - trade_cost, 0)
    rr = net_reward_tp2 / risk_price if risk_price > 0 else 0

    grade_data = classify_signal(score, rr, vol_ratio, extra_filters, strategy, direction)
    if not grade_data:
        return None

    grade = grade_data["grade"]

    if not is_strategy_side_enabled(strategy, direction):
        return None
    if not is_strategy_side_grade_enabled(strategy, direction, grade):
        return None

    signal_id = f"{normalize_symbol(symbol)}:{strategy}:{direction}:{grade}:{round(entry, 8)}"
    if signal_id in STATE["sent_signals"]:
        return None

    risk_multiplier = grade_data["risk_multiplier"]
    adjusted_risk_percent = risk_percent * risk_multiplier
    pos = calculate_position(entry, sl, deposit, adjusted_risk_percent)

    raw_reward_tp1 = price_move_percent(entry, tp1, direction)
    net_reward_tp1 = max(raw_reward_tp1 - trade_cost, 0)

    return {
        "id": signal_id,
        "symbol": normalize_symbol(symbol),
        "display_symbol": display_symbol(symbol),
        "direction": direction,
        "strategy": strategy,
        "grade": grade,
        "risk_multiplier": risk_multiplier,
        "status": "ACTIVE",
        "score": min(max(score, 0), 98),
        "entry": round(entry, 8),
        "sl": round(sl, 8),
        "tp1": round(tp1, 8),
        "tp2": round(tp2, 8),
        "tp3": round(tp3, 8),
        "rr": round(rr, 2),
        "rr_basis": "TP2 net",
        "raw_reward_to_tp1_percent": round(raw_reward_tp1, 4),
        "net_reward_to_tp1_percent": round(net_reward_tp1, 4),
        "raw_reward_to_tp2_percent": round(raw_reward_tp2, 4),
        "net_reward_to_tp2_percent": round(net_reward_tp2, 4),
        "estimated_trade_cost_percent": round(trade_cost, 4),
        "volume_ratio": round(vol_ratio, 2),
        "risk_position_percent": round(risk_pos, 2),
        "space_to_target_percent": None if space is None else round(space, 3),
        "risk_percent": adjusted_risk_percent,
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
        "counted_tp1": False,
        "counted_tp2": False,
        "counted_tp3": False,
    }


def common_market_data(c15, c5, c1, c1h, c4h):
    closes15 = [c["close"] for c in c15]
    closes5 = [c["close"] for c in c5]
    closes1 = [c["close"] for c in c1]
    return {
        "closes15": closes15,
        "closes5": closes5,
        "closes1": closes1,
        "a5": atr(c5),
        "a15": atr(c15),
        "vw": vwap_like(c15),
        "rs5": rsi(closes5),
        "rs15": rsi(closes15),
        "vr5": volume_ratio(c5, 24),
        "trend1h": trend_state(c1h),
        "trend4h": trend_state(c4h),
        "ema9_1": ema(closes1, 9)[-1] if len(closes1) >= 10 else None,
        "ema21_5": ema(closes5, 21)[-1] if len(closes5) >= 22 else None,
        "ema50_15": ema(closes15, 50)[-1] if len(closes15) >= 51 else None,
    }


def attach_space_filter(filters: dict, price: float, direction: str, c15: List[dict]):
    support_levels = find_swing_support_levels(c15, lookback=140)
    resistance_levels = find_swing_resistance_levels(c15, lookback=140)
    filters["space_to_target_percent"] = space_to_next_level_percent(price, direction, support_levels, resistance_levels)
    return filters


def evaluate_level_sweep_bounce_long(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "LEVEL_SWEEP_BOUNCE_LONG"
    if direction != "LONG" or len(c15) < 100 or len(c5) < 80:
        return None

    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not all([d["a5"], d["vw"], d["rs5"], d["rs15"], d["ema21_5"], d["ema50_15"]]):
        return None

    last, prev = c5[-1], c5[-2]
    price = last["close"]
    if late_entry_blocked(direction, c5, price, d["vw"]):
        return None

    levels = find_swing_support_levels(c15, 140)
    level = nearest_below(price, levels, 8.0) or nearest_near(price, levels, 2.0)
    if not level:
        return None

    window = c5[-18:]
    sweep_low = min(c["low"] for c in window)
    swept = sweep_low < level * 0.998
    reclaimed = any(c["close"] > level * 1.0005 for c in c5[-5:])
    bounce = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and last["close"] >= d["ema21_5"] * 0.992

    if not (swept and reclaimed and bounce):
        return None
    if d["rs5"] > 76 or d["rs15"] > 72:
        return None

    score = 60
    if btc_status in ["BULLISH", "SOFT_BULLISH"]:
        score += 7
    if d["trend1h"] in ["BULLISH", "SOFT_BULLISH"]:
        score += 7
    if d["trend4h"] in ["BULLISH", "SOFT_BULLISH"]:
        score += 3
    if d["vr5"] >= 0.90:
        score += 6
    if d["vr5"] >= 1.15:
        score += 5
    if price > d["vw"] * 0.99:
        score += 3
    if abs(sweep_low - level) / level * 100 <= 1.1:
        score += 4
    if candle_close_position(last) >= 0.55:
        score += 3

    sl = min(sweep_low - d["a5"] * 0.10, min(c["low"] for c in c5[-10:]) - d["a5"] * 0.04)

    mtf = level_mtf_strength(level, c1h, c4h, "support")
    score += mtf["level_strength_bonus"]

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters = attach_htf_confirmation(filters, direction, d)
    filters = attach_space_filter(filters, price, direction, c15)
    filters.update(mtf)

    return build_signal(
        symbol, direction, strategy, price, sl, score, d["vr5"],
        "Поддержка удержалась: sweep ниже уровня → reclaim → зелёное подтверждение. Более живой B/A+ режим.",
        deposit, risk_percent, filters
    )


def evaluate_level_resistance_reject_short(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "LEVEL_RESISTANCE_REJECT_SHORT"
    if direction != "SHORT" or len(c15) < 100 or len(c5) < 80:
        return None

    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not all([d["a5"], d["vw"], d["rs5"], d["rs15"], d["ema21_5"], d["ema50_15"]]):
        return None

    last, prev = c5[-1], c5[-2]
    price = last["close"]
    if late_entry_blocked(direction, c5, price, d["vw"]):
        return None

    levels = find_swing_resistance_levels(c15, 140)
    level = nearest_above(price, levels, 8.0) or nearest_near(price, levels, 2.0)
    if not level:
        return None

    window = c5[-18:]
    sweep_high = max(c["high"] for c in window)
    swept = sweep_high > level * 1.002
    rejected = any(c["close"] < level * 0.9995 for c in c5[-5:])
    rejection = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and last["close"] <= d["ema21_5"] * 1.008

    if not (swept and rejected and rejection):
        return None
    if d["rs5"] < 24 or d["rs15"] < 28:
        return None

    score = 60
    if btc_status in ["BEARISH", "SOFT_BEARISH"]:
        score += 7
    if d["trend1h"] in ["BEARISH", "SOFT_BEARISH"]:
        score += 7
    if d["trend4h"] in ["BEARISH", "SOFT_BEARISH"]:
        score += 3
    if d["vr5"] >= 0.90:
        score += 6
    if d["vr5"] >= 1.15:
        score += 5
    if price < d["vw"] * 1.01:
        score += 3
    if recent_move_percent(c5, 12) > 2.2:
        score += 3
    if candle_close_position(last) <= 0.45:
        score += 3

    sl = max(sweep_high + d["a5"] * 0.10, max(c["high"] for c in c5[-10:]) + d["a5"] * 0.04)

    mtf = level_mtf_strength(level, c1h, c4h, "resistance")
    score += mtf["level_strength_bonus"]

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters = attach_htf_confirmation(filters, direction, d)
    filters = attach_space_filter(filters, price, direction, c15)
    filters.update(mtf)

    return build_signal(
        symbol, direction, strategy, price, sl, score, d["vr5"],
        "Сопротивление удержалось: sweep выше уровня → возврат ниже → красное подтверждение. Более живой B/A+ режим.",
        deposit, risk_percent, filters
    )


def evaluate_level_break_retest_long(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "LEVEL_BREAK_RETEST_LONG"
    if direction != "LONG" or len(c15) < 100 or len(c5) < 80:
        return None

    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not all([d["a5"], d["vw"], d["rs5"], d["rs15"], d["ema21_5"], d["ema50_15"]]):
        return None
    last, prev = c5[-1], c5[-2]
    price = last["close"]
    if late_entry_blocked(direction, c5, price, d["vw"]):
        return None
    if recent_move_percent(c15, 8) > MAX_LEVEL_LONG_15M_MOVE_PERCENT:
        return None

    levels = find_swing_resistance_levels(c15, 140)
    level = nearest_below(price, levels, 4.0)
    if not level:
        return None

    had_below = any(c["close"] < level * 0.999 for c in c15[-12:-2])
    now_above = c15[-1]["close"] > level * 1.001 or c15[-2]["close"] > level * 1.001
    touched = last["low"] <= level * 1.008
    held = last["close"] > level * 0.999
    confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998

    # Для большего числа заявок разрешаем fresh breakout без идеального ретеста, но только B.
    fresh_breakout = price > level * 1.004 and last["close"] > last["open"] and d["vr5"] >= 1.0
    if not ((had_below and now_above and touched and held and confirm) or fresh_breakout):
        return None

    if d["rs5"] > 78 or d["rs15"] > 75:
        return None
    if price < d["ema21_5"] * 0.992:
        return None

    score = 61
    if btc_status in ["BULLISH", "SOFT_BULLISH"]:
        score += 7
    if d["trend1h"] in ["BULLISH", "SOFT_BULLISH"]:
        score += 7
    if d["trend4h"] in ["BULLISH", "SOFT_BULLISH"]:
        score += 3
    if d["vr5"] >= 0.90:
        score += 6
    if d["vr5"] >= 1.18:
        score += 5
    if price > d["vw"]:
        score += 3
    if touched:
        score += 4

    recent_low = min(c["low"] for c in c5[-10:])
    sl = min(level - d["a5"] * 0.15, recent_low - d["a5"] * 0.05)

    mtf = level_mtf_strength(level, c1h, c4h, "resistance")
    score += mtf["level_strength_bonus"]

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters = attach_htf_confirmation(filters, direction, d)
    filters = attach_space_filter(filters, price, direction, c15)
    filters.update(mtf)

    if fresh_breakout and not (touched and held):
        filters["force_grade"] = "B"
        filters["risk_multiplier_override"] = min(filters.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.20)
        filters["anti_fakeout_note"] = "Fresh breakout без идеального ретеста: разрешён только B с малым риском."

    return build_signal(
        symbol, direction, strategy, price, sl, score, d["vr5"],
        "Пробой сопротивления → удержание/ретест сверху или fresh breakout с объёмом. A+ только при хорошем подтверждении.",
        deposit, risk_percent, filters
    )


def evaluate_level_break_retest_short(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "LEVEL_BREAK_RETEST_SHORT"
    if direction != "SHORT" or len(c15) < 100 or len(c5) < 80:
        return None

    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not all([d["a5"], d["vw"], d["rs5"], d["rs15"], d["ema21_5"], d["ema50_15"]]):
        return None
    last, prev = c5[-1], c5[-2]
    price = last["close"]
    if late_entry_blocked(direction, c5, price, d["vw"]):
        return None

    levels = find_swing_support_levels(c15, 140)
    level = nearest_above(price, levels, 4.0)
    if not level:
        return None

    had_above = any(c["close"] > level * 1.001 for c in c15[-12:-2])
    now_below = c15[-1]["close"] < level * 0.999 or c15[-2]["close"] < level * 0.999
    touched = last["high"] >= level * 0.992
    held = last["close"] < level * 1.001
    confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002

    fresh_breakdown = price < level * 0.996 and last["close"] < last["open"] and d["vr5"] >= 1.0
    if not ((had_above and now_below and touched and held and confirm) or fresh_breakdown):
        return None

    if d["rs5"] < 22 or d["rs15"] < 25:
        return None
    if price > d["ema21_5"] * 1.008:
        return None

    score = 61
    if btc_status in ["BEARISH", "SOFT_BEARISH"]:
        score += 7
    if d["trend1h"] in ["BEARISH", "SOFT_BEARISH"]:
        score += 7
    if d["trend4h"] in ["BEARISH", "SOFT_BEARISH"]:
        score += 3
    if d["vr5"] >= 0.90:
        score += 6
    if d["vr5"] >= 1.18:
        score += 5
    if price < d["vw"]:
        score += 3
    if touched:
        score += 4

    recent_high = max(c["high"] for c in c5[-10:])
    sl = max(level + d["a5"] * 0.15, recent_high + d["a5"] * 0.05)

    mtf = level_mtf_strength(level, c1h, c4h, "support")
    score += mtf["level_strength_bonus"]

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters = attach_htf_confirmation(filters, direction, d)
    filters = attach_space_filter(filters, price, direction, c15)
    filters.update(mtf)

    if fresh_breakdown and not (touched and held):
        filters["force_grade"] = "B"
        filters["risk_multiplier_override"] = min(filters.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.20)
        filters["anti_fakeout_note"] = "Fresh breakdown без идеального ретеста: разрешён только B с малым риском."

    return build_signal(
        symbol, direction, strategy, price, sl, score, d["vr5"],
        "Пробой поддержки → удержание/ретест снизу или fresh breakdown с объёмом. A+ только при хорошем подтверждении.",
        deposit, risk_percent, filters
    )


def evaluate_trend_pullback_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "TREND_PULLBACK_PRO"
    if direction not in ["LONG", "SHORT"] or len(c15) < 120 or len(c5) < 80:
        return None

    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not all([d["a5"], d["vw"], d["rs5"], d["rs15"], d["ema21_5"], d["ema50_15"]]):
        return None
    last, prev = c5[-1], c5[-2]
    price = last["close"]
    if late_entry_blocked(direction, c5, price, d["vw"]):
        return None

    score = 56
    if direction == "LONG":
        trend_ok = d["trend1h"] in ["BULLISH", "SOFT_BULLISH", "NEUTRAL"] and btc_status != "BEARISH"
        near_zone = abs(price - d["ema21_5"]) / d["ema21_5"] * 100 <= 1.35 or abs(price - d["vw"]) / d["vw"] * 100 <= 1.65
        confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and price > d["ema50_15"] * 0.982
        if not (trend_ok and near_zone and confirm):
            return None
        if d["rs5"] > 73 or d["rs15"] > 71:
            return None
        sl = min(last["low"] - d["a5"] * 0.22, min(c["low"] for c in c5[-12:]) - d["a5"] * 0.05)
        if btc_status in ["BULLISH", "SOFT_BULLISH"]:
            score += 6
        if d["trend1h"] in ["BULLISH", "SOFT_BULLISH"]:
            score += 8
        if price > d["vw"]:
            score += 3
        reason = "Откат к EMA/VWAP в направлении 1H/15m структуры → зелёное подтверждение. Более частый профессиональный B/A+ сетап."
    else:
        trend_ok = d["trend1h"] in ["BEARISH", "SOFT_BEARISH", "NEUTRAL"] and btc_status != "BULLISH"
        near_zone = abs(price - d["ema21_5"]) / d["ema21_5"] * 100 <= 1.35 or abs(price - d["vw"]) / d["vw"] * 100 <= 1.65
        confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and price < d["ema50_15"] * 1.018
        if not (trend_ok and near_zone and confirm):
            return None
        if d["rs5"] < 27 or d["rs15"] < 29:
            return None
        sl = max(last["high"] + d["a5"] * 0.22, max(c["high"] for c in c5[-12:]) + d["a5"] * 0.05)
        if btc_status in ["BEARISH", "SOFT_BEARISH"]:
            score += 6
        if d["trend1h"] in ["BEARISH", "SOFT_BEARISH"]:
            score += 8
        if price < d["vw"]:
            score += 3
        reason = "Откат к EMA/VWAP в направлении 1H/15m структуры → красное подтверждение. Более частый профессиональный B/A+ сетап."

    if d["trend4h"] in ["BULLISH", "SOFT_BULLISH"] and direction == "LONG":
        score += 3
    if d["trend4h"] in ["BEARISH", "SOFT_BEARISH"] and direction == "SHORT":
        score += 3
    if d["vr5"] >= 0.85:
        score += 5
    if d["vr5"] >= 1.10:
        score += 4

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters = attach_htf_confirmation(filters, direction, d)
    filters = attach_space_filter(filters, price, direction, c15)
    if filters.get("btc_against"):
        return None

    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], reason, deposit, risk_percent, filters)


def evaluate_vwap_reclaim_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "VWAP_RECLAIM_PRO"
    if direction not in ["LONG", "SHORT"] or len(c15) < 100 or len(c5) < 80:
        return None

    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not all([d["a5"], d["vw"], d["rs5"], d["rs15"], d["ema21_5"]]):
        return None
    last, prev = c5[-1], c5[-2]
    price = last["close"]
    if distance_from_vwap_percent(price, d["vw"]) > 2.4:
        return None

    score = 55
    if direction == "LONG":
        was_below = any(c["close"] < d["vw"] * 0.996 for c in c5[-10:-2])
        reclaimed = last["close"] > d["vw"] * 1.001 and last["close"] > last["open"]
        if not (was_below and reclaimed and price > d["ema21_5"] * 0.995):
            return None
        if btc_status == "BEARISH":
            return None
        if d["rs5"] > 74:
            return None
        sl = min(min(c["low"] for c in c5[-8:]) - d["a5"] * 0.06, d["vw"] - d["a5"] * 0.22)
        if btc_status in ["BULLISH", "SOFT_BULLISH"]:
            score += 6
        if d["trend1h"] in ["BULLISH", "SOFT_BULLISH", "NEUTRAL"]:
            score += 5
        reason = "VWAP reclaim: цена вернулась выше VWAP после локального давления, подтверждение зелёной свечой."
    else:
        was_above = any(c["close"] > d["vw"] * 1.004 for c in c5[-10:-2])
        lost = last["close"] < d["vw"] * 0.999 and last["close"] < last["open"]
        if not (was_above and lost and price < d["ema21_5"] * 1.005):
            return None
        if btc_status == "BULLISH":
            return None
        if d["rs5"] < 26:
            return None
        sl = max(max(c["high"] for c in c5[-8:]) + d["a5"] * 0.06, d["vw"] + d["a5"] * 0.22)
        if btc_status in ["BEARISH", "SOFT_BEARISH"]:
            score += 6
        if d["trend1h"] in ["BEARISH", "SOFT_BEARISH", "NEUTRAL"]:
            score += 5
        reason = "VWAP loss: цена вернулась ниже VWAP после локального выкупа, подтверждение красной свечой."

    if d["vr5"] >= 0.80:
        score += 5
    if d["vr5"] >= 1.05:
        score += 4
    if abs(price - d["vw"]) / d["vw"] * 100 <= 1.2:
        score += 3

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters = attach_htf_confirmation(filters, direction, d)
    filters = attach_space_filter(filters, price, direction, c15)
    # VWAP reclaim — частая стратегия, по умолчанию B, A+ только если прям супер.
    if score < A_PLUS_MIN_SCORE + 5:
        filters["force_grade"] = "B"
        filters["risk_multiplier_override"] = min(filters.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.22)

    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], reason, deposit, risk_percent, filters)


def evaluate_impulse_pullback_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "IMPULSE_PULLBACK_PRO"
    if not IMPULSE_PULLBACK_ENABLED or direction not in ["LONG", "SHORT"] or len(c15) < 120 or len(c5) < 80 or len(c1) < 40:
        return None

    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not all([d["a5"], d["vw"], d["rs5"], d["rs15"], d["ema9_1"], d["ema21_5"], d["ema50_15"]]):
        return None

    last, prev = c5[-1], c5[-2]
    price = last["close"]
    if d["vr5"] < IMPULSE_MIN_VOLUME_RATIO:
        return None
    if distance_from_vwap_percent(price, d["vw"]) > IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT:
        return None

    old_price = c5[-15]["close"]
    recent_before_confirm = c5[-12:-3]
    if not recent_before_confirm or old_price <= 0:
        return None

    score = 56

    if direction == "LONG":
        if btc_status == "BEARISH" or d["trend1h"] == "BEARISH":
            return None
        impulse_high = max(c["high"] for c in recent_before_confirm)
        impulse_move = (impulse_high - old_price) / old_price * 100
        if impulse_move < IMPULSE_MIN_MOVE_5M_PERCENT:
            return None
        pullback_low = min(c["low"] for c in c5[-7:-1])
        pullback_percent = (impulse_high - pullback_low) / impulse_high * 100 if impulse_high > 0 else 0
        if not (IMPULSE_PULLBACK_MIN_PERCENT <= pullback_percent <= IMPULSE_PULLBACK_MAX_PERCENT):
            return None
        confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and c1[-1]["close"] > d["ema9_1"] * 0.998
        if not confirm or price < d["ema21_5"] * 0.992:
            return None
        if d["rs5"] > 78 or d["rs15"] > 75:
            return None
        sl = min(pullback_low - d["a5"] * 0.12, min(c["low"] for c in c5[-10:]) - d["a5"] * 0.04)
        reason = "Impulse Pullback Pro: импульс вверх → нормальный откат → зелёное подтверждение. B-only, чтобы чаще ловить движение."
        if btc_status in ["BULLISH", "SOFT_BULLISH"]:
            score += 6
        if d["trend1h"] in ["BULLISH", "SOFT_BULLISH"]:
            score += 6
        if price > d["vw"]:
            score += 3
    else:
        if btc_status == "BULLISH" or d["trend1h"] == "BULLISH":
            return None
        impulse_low = min(c["low"] for c in recent_before_confirm)
        impulse_move = (old_price - impulse_low) / old_price * 100
        if impulse_move < IMPULSE_MIN_MOVE_5M_PERCENT:
            return None
        pullback_high = max(c["high"] for c in c5[-7:-1])
        pullback_percent = (pullback_high - impulse_low) / impulse_low * 100 if impulse_low > 0 else 0
        if not (IMPULSE_PULLBACK_MIN_PERCENT <= pullback_percent <= IMPULSE_PULLBACK_MAX_PERCENT):
            return None
        confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and c1[-1]["close"] < d["ema9_1"] * 1.002
        if not confirm or price > d["ema21_5"] * 1.008:
            return None
        if d["rs5"] < 22 or d["rs15"] < 25:
            return None
        sl = max(pullback_high + d["a5"] * 0.12, max(c["high"] for c in c5[-10:]) + d["a5"] * 0.04)
        reason = "Impulse Pullback Pro: импульс вниз → нормальный откат → красное подтверждение. B-only, чтобы чаще ловить движение."
        if btc_status in ["BEARISH", "SOFT_BEARISH"]:
            score += 6
        if d["trend1h"] in ["BEARISH", "SOFT_BEARISH"]:
            score += 6
        if price < d["vw"]:
            score += 3

    if d["trend4h"] in ["BULLISH", "SOFT_BULLISH"] and direction == "LONG":
        score += 3
    if d["trend4h"] in ["BEARISH", "SOFT_BEARISH"] and direction == "SHORT":
        score += 3
    if d["vr5"] >= 0.90:
        score += 5
    if d["vr5"] >= 1.12:
        score += 4

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters = attach_htf_confirmation(filters, direction, d)
    filters = attach_space_filter(filters, price, direction, c15)
    filters["force_grade"] = "B"
    filters["risk_multiplier_override"] = IMPULSE_PULLBACK_RISK_MULTIPLIER
    filters["anti_fakeout_note"] = "Impulse Pullback Pro: только B, частая стратегия после отката."

    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], reason, deposit, risk_percent, filters)


def analyze_symbol(symbol: str, direction: Optional[str], deposit: float, risk_percent: float, btc_status_override: Optional[str] = None) -> Optional[dict]:
    symbol = normalize_symbol(symbol)
    if is_blocked(symbol) or is_on_cooldown(symbol):
        return None

    c15 = remove_unclosed_candle(get_klines(symbol, "15m", 260), "15m")
    c5 = remove_unclosed_candle(get_klines(symbol, "5m", 180), "5m")
    c1 = remove_unclosed_candle(get_klines(symbol, "1m", 120), "1m")
    c1h = remove_unclosed_candle(get_klines(symbol, "1h", 260), "1h")
    c4h = remove_unclosed_candle(get_klines(symbol, "4h", 260), "4h")

    if not c15 or not c5 or not c1 or not c1h or not c4h:
        return None

    btc_status = btc_status_override or detect_btc_status()
    normalized_direction = normalize_direction(direction)
    directions = [normalized_direction] if normalized_direction else ["LONG", "SHORT"]

    candidates = []
    funcs = [
        evaluate_level_sweep_bounce_long,
        evaluate_level_resistance_reject_short,
        evaluate_level_break_retest_long,
        evaluate_level_break_retest_short,
        evaluate_trend_pullback_pro,
        evaluate_vwap_reclaim_pro,
        evaluate_impulse_pullback_pro,
    ]

    for d in directions:
        for func in funcs:
            try:
                signal = func(symbol, d, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent)
                if signal:
                    candidates.append(signal)
            except Exception:
                continue

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            1 if x["grade"] == "A+" else 0,
            x["score"],
            x["rr"],
            x["volume_ratio"],
            0 if x.get("space_to_target_percent") is None else x.get("space_to_target_percent"),
        ),
        reverse=True
    )
    return candidates[0]


def build_message(signal: dict) -> str:
    arrow = "📈" if signal["direction"] == "LONG" else "📉"
    mode = "TEST" if TEST_MODE else "TRADE"

    strategy_names = {
        "LEVEL_BREAK_RETEST_SHORT": "📉 Пробой поддержки + ретест/fresh breakdown",
        "LEVEL_SWEEP_BOUNCE_LONG": "🟢 Отскок от поддержки после sweep",
        "LEVEL_BREAK_RETEST_LONG": "📈 Пробой сопротивления + ретест/fresh breakout",
        "LEVEL_RESISTANCE_REJECT_SHORT": "🔴 Отбой от сопротивления после sweep",
        "TREND_PULLBACK_PRO": "📌 Trend Pullback Pro",
        "VWAP_RECLAIM_PRO": "🧭 VWAP Reclaim/Loss Pro",
        "IMPULSE_PULLBACK_PRO": "⚡ Impulse Pullback Pro",
    }
    strategy_text = strategy_names.get(signal["strategy"], signal["strategy"])

    pos = signal["position"]
    if pos.get("error"):
        risk_text = f"⚠️ Ошибка RM: {pos['error']}"
    else:
        risk_text = (
            f"Риск: {signal['risk_percent']:.3f}% депозита\n"
            f"Размер позиции: {pos['position_size_usdt']} USDT\n"
            f"Маржа x{LEVERAGE}: {pos.get('margin_usdt')} USDT"
        )

    filters = signal.get("filters", {})
    funding = filters.get("funding", {})
    funding_text = funding.get("reason", "Funding/OI: нет данных")
    notes = []
    for key in ["htf_note", "anti_fakeout_note", "countertrend_note", "space_note"]:
        if filters.get(key):
            notes.append(filters.get(key))

    grade_text = "A+ SIGNAL" if signal["grade"] == "A+" else "B SIGNAL"
    caution = "\n⚠️ B-сигнал: вход осторожнее, риск уменьшен." if signal["grade"] == "B" else ""

    space_txt = signal.get("space_to_target_percent")
    space_line = f"\n<b>Место до ближайшего уровня:</b> {space_txt}%" if space_txt is not None else ""

    return f"""
🎯 <b>{mode} {grade_text}</b>

{arrow} <b>{signal['direction']} {signal['display_symbol']}</b>

<b>Стратегия:</b> {strategy_text}

<b>Вход:</b> <code>{signal['entry']}</code>
<b>Stop Loss:</b> <code>{signal['sl']}</code>

<b>Take Profit:</b>
TP1: <code>{signal['tp1']}</code>
TP2: <code>{signal['tp2']}</code>
TP3: <code>{signal['tp3']}</code>

<b>Почему вход:</b>
{signal['reason']}

<b>Фильтры:</b>
BTC: {filters.get('btc_status', 'NEUTRAL')}
{funding_text}
{chr(10).join(notes)}

<b>Качество:</b> {signal['score']}/100
<b>RR:</b> {signal['rr']} ({signal.get('rr_basis', 'TP2 net')})
<b>TP1 gross/net:</b> {signal.get('raw_reward_to_tp1_percent', 0)}% / {signal.get('net_reward_to_tp1_percent', 0)}%
<b>TP2 gross/net:</b> {signal.get('raw_reward_to_tp2_percent', 0)}% / {signal.get('net_reward_to_tp2_percent', 0)}%
<b>Издержки:</b> ~{signal.get('estimated_trade_cost_percent', 0)}%
<b>Объём:</b> x{signal['volume_ratio']}
<b>Риск до SL:</b> {signal['risk_position_percent']}% по позиции{space_line}

{risk_text}
{caution}

<b>После TP1:</b>
Закрыть примерно {TP1_CLOSE_PERCENT:.0f}% позиции и перенести SL в безубыток.

⚠️ Не финансовый совет.
""".strip()


def send_telegram_message(text: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не указаны"}

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
        return {"ok": False, "error": str(e)}


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
    grade = signal.get("grade", "A+")
    ss = f"{strategy}:{side}"
    ssg = f"{strategy}:{side}:{grade}"
    notes = []

    r_multiple = 0.0

    if result == "SL" and not signal.get("counted_sl"):
        signal["counted_sl"] = True
        STATE["stats"]["side"][side]["sl"] += 1
        STATE["stats"]["side"][side]["consecutive_sl"] += 1
        STATE["stats"]["strategy"][strategy]["sl"] += 1
        STATE["stats"]["strategy"][strategy]["consecutive_sl"] += 1
        STATE["stats"]["strategy_side"][ss]["sl"] += 1
        STATE["stats"]["strategy_side"][ss]["consecutive_sl"] += 1
        STATE["stats"]["strategy_side_grade"][ssg]["sl"] += 1
        STATE["stats"]["strategy_side_grade"][ssg]["consecutive_sl"] += 1
        STATE["stats"]["grade"][grade]["sl"] += 1
        STATE["stats"]["pair_sl"][symbol] = STATE["stats"]["pair_sl"].get(symbol, 0) + 1
        r_multiple = -1.0

        if STATE["stats"]["pair_sl"][symbol] >= PAIR_MAX_SL:
            STATE["blocked_symbols"][symbol] = now_ts() + PAIR_BLOCK_SECONDS
            notes.append(f"🚫 {display_symbol(symbol)} заблокирован после серии SL.")

        if STATE["stats"]["strategy_side_grade"][ssg]["consecutive_sl"] >= STRATEGY_SIDE_MAX_CONSECUTIVE_SL:
            if grade == "B":
                STATE["strategy_side_grade_disabled_until"][ssg] = now_ts() + STRATEGY_SIDE_DISABLE_SECONDS
                STATE["stats"]["strategy_side_grade"][ssg]["consecutive_sl"] = 0
                notes.append(f"⛔ B {strategy} {side} отключён временно. A+ не блокируется.")
            else:
                STATE["strategy_side_hard_disabled_until"][ss] = now_ts() + STRATEGY_SIDE_DISABLE_SECONDS
                STATE["stats"]["strategy_side"][ss]["consecutive_sl"] = 0
                STATE["stats"]["strategy_side_grade"][ssg]["consecutive_sl"] = 0
                notes.append(f"⛔ A+ {strategy} {side} дал серию SL — связка временно отключена.")

    elif result in ["TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        STATE["stats"]["side"][side]["consecutive_sl"] = 0
        STATE["stats"]["strategy"][strategy]["consecutive_sl"] = 0
        STATE["stats"]["strategy_side"][ss]["consecutive_sl"] = 0
        STATE["stats"]["strategy_side_grade"][ssg]["consecutive_sl"] = 0

        if not signal.get("counted_positive"):
            signal["counted_positive"] = True
            STATE["stats"]["side"][side]["positive"] += 1
            STATE["stats"]["strategy"][strategy]["positive"] += 1
            STATE["stats"]["strategy_side"][ss]["positive"] += 1
            STATE["stats"]["strategy_side_grade"][ssg]["positive"] += 1
            STATE["stats"]["grade"][grade]["positive"] += 1
            STATE["stats"]["pair_positive"][symbol] = STATE["stats"]["pair_positive"].get(symbol, 0) + 1

        risk_price = abs(signal["entry"] - signal["sl"])
        if risk_price > 0:
            if result in ["TP1", "PROFIT_AFTER_TP1"]:
                r_multiple = 0.35  # partial TP1 + BE model
            elif result in ["TP2", "PROFIT_AFTER_TP2"]:
                r_multiple = 0.75
            elif result == "TP3":
                r_multiple = 1.20

        if result == "TP1" and not signal.get("counted_tp1"):
            signal["counted_tp1"] = True
            STATE["stats"]["side"][side]["tp1"] += 1
        if result == "TP2" and not signal.get("counted_tp2"):
            signal["counted_tp2"] = True
            STATE["stats"]["side"][side]["tp2"] += 1
        if result == "TP3" and not signal.get("counted_tp3"):
            signal["counted_tp3"] = True
            STATE["stats"]["side"][side]["tp3"] += 1

    if result in ["SL", "TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        STATE["stats"]["closed_trades"].append({
            "time": int(now_ts()),
            "symbol": symbol,
            "strategy": strategy,
            "side": side,
            "grade": grade,
            "result": result,
            "r_multiple": round(r_multiple, 3),
        })
        STATE["stats"]["closed_trades"] = STATE["stats"]["closed_trades"][-500:]

    save_state(STATE)
    return notes


def check_signal_hit(signal: dict, candles: List[dict]):
    side = signal["direction"]
    last_checked = signal.get("last_checked_time", 0)
    new_candles = [c for c in candles if c["time"] > last_checked]
    if not new_candles:
        return None, candles[-1]["close"]

    for c in new_candles:
        high, low = c["high"], c["low"]
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


def build_result_message(signal: dict, result: str, price: Optional[float], notes: List[str]) -> str:
    strategy_text = signal.get("strategy", "")
    if result == "SL":
        title = "❌ Stop Loss"
        status = "SL сработал до TP1. Сделка отрицательная."
    elif result == "TP1":
        title = "✅ TP1 достигнут"
        status = f"Сделка позитивная. Закрыть ~{TP1_CLOSE_PERCENT:.0f}% позиции и SL в безубыток."
    elif result == "TP2":
        title = "✅ TP2 достигнут"
        status = "Хорошее движение. Сделка позитивная."
    elif result == "TP3":
        title = "🔥 TP3 достигнут"
        status = "Отличная сделка. Полная цель достигнута."
    elif result == "PROFIT_AFTER_TP1":
        title = "🟢 Возврат после TP1"
        status = "Цена вернулась после TP1, но сделка уже позитивная."
    elif result == "PROFIT_AFTER_TP2":
        title = "🟢 Возврат после TP2"
        status = "Цена вернулась после TP2, сделка позитивная."
    elif result == "EXPIRED":
        title = "⌛ Сигнал устарел"
        status = "Сигнал не достиг TP/SL за установленное время и удалён из активных."
    else:
        title = f"ℹ️ {result}"
        status = "Обновление по сделке."

    adaptive = "\n\n<b>Адаптация бота:</b>\n" + "\n".join(notes) if notes else ""
    stats_text = build_stats_text()

    return f"""
{title}

<b>{signal.get('grade', 'A+')} · {signal['direction']} {signal['display_symbol']}</b>
<b>Стратегия:</b> {strategy_text}

Вход: <code>{signal['entry']}</code>
Текущая цена: <code>{'n/a' if price is None else round(price, 8)}</code>

TP1: <code>{signal['tp1']}</code>
TP2: <code>{signal['tp2']}</code>
TP3: <code>{signal['tp3']}</code>
SL: <code>{signal['sl']}</code>

{status}

{stats_text}
{adaptive}
""".strip()


def scan_best_signal(deposit: float, risk_percent: float) -> dict:
    cleanup_state()
    symbols = get_symbols()
    btc_status = detect_btc_status()

    best = None
    checked = 0
    found = 0

    for symbol in symbols:
        checked += 1
        signal = analyze_symbol(symbol, None, deposit, risk_percent, btc_status_override=btc_status)
        if not signal:
            continue
        found += 1
        if best is None:
            best = signal
            continue

        current_key = (1 if signal["grade"] == "A+" else 0, signal["score"], signal["rr"], signal["volume_ratio"])
        best_key = (1 if best["grade"] == "A+" else 0, best["score"], best["rr"], best["volume_ratio"])
        if current_key > best_key:
            best = signal

    if not best:
        return {
            "ok": False,
            "checked": checked,
            "found_candidates": found,
            "btc_status": btc_status,
            "message": "Сильных сигналов сейчас нет. V5.5 режим живее, но учитывает 1H/4H, но фильтры не дали нормальный вход."
        }

    return {
        "ok": True,
        "checked": checked,
        "found_candidates": found,
        "btc_status": btc_status,
        "signal": best,
        "message": build_message(best),
    }


def is_signal_expired(signal: dict) -> bool:
    created = signal.get("created_at", 0)
    return bool(created and now_ts() - created > SIGNAL_MAX_LIFETIME_SECONDS)


def track_active_signals(send_to_telegram: bool = True) -> dict:
    cleanup_state()
    if not STATE["active_signals"]:
        return {"ok": True, "message": "Активных сигналов нет.", "results": [], "active_left": 0}

    results, finished = [], []

    for signal_id, signal in list(STATE["active_signals"].items()):
        if is_signal_expired(signal):
            message = build_result_message(signal, "EXPIRED", None, [])
            telegram = send_telegram_message(message) if send_to_telegram else None
            results.append({
                "signal_id": signal_id,
                "symbol": signal.get("display_symbol"),
                "grade": signal.get("grade", "A+"),
                "direction": signal.get("direction"),
                "strategy": signal.get("strategy"),
                "result": "EXPIRED",
                "price": None,
                "telegram": telegram,
            })
            finished.append(signal_id)
            continue

        candles = remove_unclosed_candle(get_klines(signal["symbol"], "1m", 120), "1m")
        if not candles:
            continue

        result, price = check_signal_hit(signal, candles)
        STATE["active_signals"][signal_id] = signal

        if not result:
            continue

        notes = apply_result(signal, result)
        message = build_result_message(signal, result, price, notes)
        telegram = send_telegram_message(message) if send_to_telegram else None

        results.append({
            "signal_id": signal_id,
            "symbol": signal["display_symbol"],
            "grade": signal.get("grade", "A+"),
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
    return {"ok": True, "checked": len(STATE["active_signals"]) + len(finished), "results": results, "active_left": len(STATE["active_signals"])}


async def auto_worker():
    await asyncio.sleep(10)

    while True:
        try:
            current = now_ts()

            if AUTO_TRACK_ENABLED:
                last_track = STATE["auto"].get("last_track_time", 0)
                if current - last_track >= AUTO_TRACK_SECONDS:
                    result = track_active_signals(send_to_telegram=True)
                    STATE["auto"]["last_track_time"] = current
                    STATE["auto"]["last_track_result"] = result
                    save_state(STATE)

            if AUTO_SCAN_ENABLED:
                last_scan = STATE["auto"].get("last_scan_time", 0)
                if current - last_scan >= AUTO_SCAN_SECONDS:
                    result = scan_best_signal(DEFAULT_DEPOSIT, DEFAULT_RISK_PERCENT)
                    STATE["auto"]["last_scan_time"] = current
                    STATE["auto"]["last_scan_result"] = result

                    if result.get("ok"):
                        signal = result["signal"]
                        telegram = send_telegram_message(result["message"])
                        result["telegram"] = telegram
                        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
                            save_signal(signal)
                        else:
                            STATE["auto"]["last_error"] = f"Telegram не отправил сигнал: {telegram}"
                    else:
                        last_report = STATE["auto"].get("last_no_signal_report_time", 0)
                        if DEBUG_NO_SIGNAL_REPORT_ENABLED and current - last_report >= DEBUG_NO_SIGNAL_REPORT_SECONDS:
                            report = (
                                "🧠 <b>Диагностика V5.5 HTF Confirm Balanced</b>\n\n"
                                f"BTC regime: {result.get('btc_status', 'NEUTRAL')}\n"
                                f"Проверено пар: {result.get('checked', 0)}\n"
                                f"Кандидатов найдено: {result.get('found_candidates', 0)}\n"
                                "Сигнала на отправку пока нет.\n\n"
                                "Бот стал живее: добавлены Trend Pullback, VWAP Reclaim, Fresh Breakout/Breakdown B-only и мягкие B-пороги.\n"
                                "Если заявок всё равно мало — снизь PRO/B параметры в .env: B_MIN_SCORE, B_MIN_RR, B_MIN_VOLUME_RATIO."
                            )
                            send_telegram_message(report)
                            STATE["auto"]["last_no_signal_report_time"] = current

                    save_state(STATE)

            await asyncio.sleep(15)

        except Exception as e:
            STATE["auto"]["last_error"] = str(e)
            save_state(STATE)
            await asyncio.sleep(30)


def is_authorized(api_key: Optional[str]) -> bool:
    if not API_KEY:
        return True
    return api_key == API_KEY


def unauthorized_response():
    return {"ok": False, "error": "Unauthorized. Provide valid api_key."}


@app.on_event("startup")
async def startup_event():
    text = (
        f"✅ {APP_NAME} запущен.\n\n"
        f"Режим: {'TEST' if TEST_MODE else 'TRADE'}\n"
        f"Auto Scan: {'ON' if AUTO_SCAN_ENABLED else 'OFF'} / {AUTO_SCAN_SECONDS} сек.\n"
        f"Auto Track: {'ON' if AUTO_TRACK_ENABLED else 'OFF'} / {AUTO_TRACK_SECONDS} сек.\n"
        f"Closed candles only: {'ON' if USE_CLOSED_CANDLES_ONLY else 'OFF'}\n"
        f"A+ score/RR/volume: {A_PLUS_MIN_SCORE}+ / {A_PLUS_MIN_RR} / x{A_PLUS_MIN_VOLUME_RATIO}\n"
        f"B score/RR/volume: {B_MIN_SCORE}+ / {B_MIN_RR} / x{B_MIN_VOLUME_RATIO}\n"
        f"Level B score/RR/volume: {LEVEL_B_MIN_SCORE}+ / {LEVEL_B_MIN_RR} / x{LEVEL_B_MIN_VOLUME_RATIO}\n"
        f"B risk: x{B_RISK_MULTIPLIER}\n"
        f"Impulse Pullback: {'ON' if IMPULSE_PULLBACK_ENABLED else 'OFF'} / risk x{IMPULSE_PULLBACK_RISK_MULTIPLIER}\n"
        f"Space filter: {'ON' if ENABLE_SPACE_TO_TARGET_FILTER else 'OFF'}\n"
        f"HTF 1H/4H confirm: {'ON' if HTF_CONFIRMATION_ENABLED else 'OFF'} | A+ both TF: {'ON' if A_PLUS_REQUIRES_1H_4H_CONFIRM else 'OFF'} | B any TF: {'ON' if B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM else 'OFF'}\n"
        f"API key protection: {'ON' if bool(API_KEY) else 'OFF'}\n\n"
        "V5.5 цель: больше заявок, но без входов по незакрытой свече и с более честным RR по TP2."
    )
    send_telegram_message(text)
    asyncio.create_task(auto_worker())


@app.get("/", response_class=HTMLResponse)
def home():
    return f"""
<!DOCTYPE html>
<html>
<head><title>{APP_NAME}</title></head>
<body style="background:#020617;color:#e5e7eb;font-family:Arial;padding:40px;">
    <h1>✅ {APP_NAME} работает</h1>
    <pre>
GET /health
GET /scan?send_to_telegram=false
GET /auto-signal?symbol=NEAR/USDT
GET /track
GET /stats
GET /auto-status
GET /test-telegram
GET /cleanup-state?api_key=...
GET /reset-state?api_key=...
    </pre>
</body>
</html>
"""


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": APP_NAME,
        "test_mode": TEST_MODE,
        "leverage": LEVERAGE,
        "balanced_pro_mode": BALANCED_PRO_MODE,
        "use_closed_candles_only": USE_CLOSED_CANDLES_ONLY,
        "a_plus_min_score": A_PLUS_MIN_SCORE,
        "a_plus_min_volume_ratio": A_PLUS_MIN_VOLUME_RATIO,
        "a_plus_min_rr": A_PLUS_MIN_RR,
        "b_min_score": B_MIN_SCORE,
        "b_min_volume_ratio": B_MIN_VOLUME_RATIO,
        "b_min_rr": B_MIN_RR,
        "level_b_min_score": LEVEL_B_MIN_SCORE,
        "level_b_min_rr": LEVEL_B_MIN_RR,
        "level_b_min_volume_ratio": LEVEL_B_MIN_VOLUME_RATIO,
        "auto_scan_enabled": AUTO_SCAN_ENABLED,
        "auto_track_enabled": AUTO_TRACK_ENABLED,
        "auto_scan_seconds": AUTO_SCAN_SECONDS,
        "auto_track_seconds": AUTO_TRACK_SECONDS,
        "active_signals": len(STATE["active_signals"]),
        "blocked_symbols": len(STATE["blocked_symbols"]),
        "htf_confirmation_enabled": HTF_CONFIRMATION_ENABLED,
        "a_plus_requires_1h_4h_confirm": A_PLUS_REQUIRES_1H_4H_CONFIRM,
        "b_requires_at_least_one_htf_confirm": B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM,
        "api_key_enabled": bool(API_KEY),
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
def test_telegram():
    return send_telegram_message(f"✅ {APP_NAME} подключён к Telegram.")


@app.get("/auto-signal")
def auto_signal(
    symbol: str = Query(default="NEAR/USDT"),
    direction: Optional[str] = Query(default=None),
    deposit: float = Query(default=DEFAULT_DEPOSIT),
    risk_percent: float = Query(default=DEFAULT_RISK_PERCENT),
    send_to_telegram: bool = Query(default=False),
    api_key: Optional[str] = Query(default=None),
):
    if send_to_telegram and not is_authorized(api_key):
        return unauthorized_response()

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
        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
            save_signal(signal)

    return {"ok": True, "signal": signal, "message": message, "telegram": telegram}


@app.get("/scan")
def scan(
    send_to_telegram: bool = Query(default=False),
    deposit: float = Query(default=DEFAULT_DEPOSIT),
    risk_percent: float = Query(default=DEFAULT_RISK_PERCENT),
    api_key: Optional[str] = Query(default=None),
):
    if send_to_telegram and not is_authorized(api_key):
        return unauthorized_response()

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
    api_key: Optional[str] = Query(default=None),
):
    if send_to_telegram and not is_authorized(api_key):
        return unauthorized_response()
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
        "strategy_side_grade_disabled_until": {
            k: int(v - now_ts())
            for k, v in STATE["strategy_side_grade_disabled_until"].items()
            if v > now_ts()
        },
    }


@app.get("/cleanup-state")
def cleanup_state_endpoint(api_key: Optional[str] = Query(default=None)):
    if not is_authorized(api_key):
        return unauthorized_response()
    cleanup_state()
    return {
        "ok": True,
        "message": "State cleanup completed.",
        "sent_signals": len(STATE.get("sent_signals", {})),
        "cooldowns": len(STATE.get("symbol_cooldown", {})),
        "blocked_symbols": len(STATE.get("blocked_symbols", {})),
    }


@app.get("/reset-state")
def reset_state(api_key: Optional[str] = Query(default=None)):
    if not is_authorized(api_key):
        return unauthorized_response()
    global STATE
    STATE = default_state()
    save_state(STATE)
    return {"ok": True, "message": "State reset completed."}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("bot:app", host="0.0.0.0", port=port)
