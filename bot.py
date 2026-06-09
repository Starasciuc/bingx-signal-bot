
import os
import time
import json
import random
import asyncio
from typing import Optional, List, Dict, Any, Tuple

import requests
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

APP_NAME = "Professional Adaptive Futures Bot AUTO V6.1 PROFESSIONAL BALANCED CLEAN"
DEPLOY_MARKER = "V6_1_PRO_BALANCED_CLEAN_2026_06_09"
app = FastAPI(title=APP_NAME)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
BINGX_BASE_URL = "https://open-api.bingx.com"
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")
API_KEY = os.getenv("API_KEY", "").strip()

TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
DEFAULT_DEPOSIT = float(os.getenv("DEFAULT_DEPOSIT", "1000"))
DEFAULT_RISK_PERCENT = float(os.getenv("DEFAULT_RISK_PERCENT", "0.5"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "220"))

AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "180"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "45"))
DEBUG_NO_SIGNAL_REPORT_ENABLED = os.getenv("DEBUG_NO_SIGNAL_REPORT_ENABLED", "true").lower() == "true"
DEBUG_NO_SIGNAL_REPORT_SECONDS = int(os.getenv("DEBUG_NO_SIGNAL_REPORT_SECONDS", "7200"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "1200"))
SIGNAL_MAX_LIFETIME_SECONDS = int(os.getenv("SIGNAL_MAX_LIFETIME_SECONDS", "21600"))
SENT_SIGNALS_KEEP_SECONDS = int(os.getenv("SENT_SIGNALS_KEEP_SECONDS", "604800"))
PAIR_MAX_SL = int(os.getenv("PAIR_MAX_SL", "3"))
PAIR_BLOCK_SECONDS = int(os.getenv("PAIR_BLOCK_SECONDS", "21600"))
STRATEGY_SIDE_MAX_CONSECUTIVE_SL = int(os.getenv("STRATEGY_SIDE_MAX_CONSECUTIVE_SL", "4"))
STRATEGY_SIDE_DISABLE_SECONDS = int(os.getenv("STRATEGY_SIDE_DISABLE_SECONDS", "7200"))
SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK = os.getenv("SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK", "true").lower() == "true"
USE_CLOSED_CANDLES_ONLY = os.getenv("USE_CLOSED_CANDLES_ONLY", "true").lower() == "true"

# V6.1: Render env не должен случайно вернуть старые мягкие пороги.
ALLOW_ENV_STRATEGY_OVERRIDES = os.getenv("ALLOW_ENV_STRATEGY_OVERRIDES", "false").lower() == "true"
def strategy_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default))) if ALLOW_ENV_STRATEGY_OVERRIDES else default
def strategy_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default))) if ALLOW_ENV_STRATEGY_OVERRIDES else default

# Professional balanced thresholds: не молчун, но слабые B больше не проходят.
A_PLUS_MIN_SCORE = strategy_int("A_PLUS_MIN_SCORE", 86)
A_PLUS_MIN_VOLUME_RATIO = strategy_float("A_PLUS_MIN_VOLUME_RATIO", 1.20)
A_PLUS_MIN_RR = strategy_float("A_PLUS_MIN_RR", 1.00)
A_PLUS_RISK_MULTIPLIER = strategy_float("A_PLUS_RISK_MULTIPLIER", 1.00)
B_MIN_SCORE = strategy_int("B_MIN_SCORE", 76)
B_MIN_VOLUME_RATIO = strategy_float("B_MIN_VOLUME_RATIO", 1.02)
B_MIN_RR = strategy_float("B_MIN_RR", 0.70)
B_RISK_MULTIPLIER = strategy_float("B_RISK_MULTIPLIER", 0.20)
LEVEL_B_MIN_SCORE = strategy_int("LEVEL_B_MIN_SCORE", 74)
LEVEL_B_MIN_VOLUME_RATIO = strategy_float("LEVEL_B_MIN_VOLUME_RATIO", 1.00)
LEVEL_B_MIN_RR = strategy_float("LEVEL_B_MIN_RR", 0.65)
LEVEL_SIGNAL_SCORE_BONUS = strategy_int("LEVEL_SIGNAL_SCORE_BONUS", 3)

# Short guard: шорты не отключены полностью, но они должны быть сильнее лонгов.
PRO_BALANCED_GUARD_ENABLED = os.getenv("PRO_BALANCED_GUARD_ENABLED", "true").lower() == "true"
SHORT_B_ENABLED = os.getenv("SHORT_B_ENABLED", "true").lower() == "true"
SHORT_B_MIN_SCORE = strategy_int("SHORT_B_MIN_SCORE", 82)
SHORT_B_MIN_RR = strategy_float("SHORT_B_MIN_RR", 0.78)
SHORT_B_MIN_VOLUME_RATIO = strategy_float("SHORT_B_MIN_VOLUME_RATIO", 1.08)
SHORT_BLOCK_IF_BTC_BULLISH = os.getenv("SHORT_BLOCK_IF_BTC_BULLISH", "true").lower() == "true"
SHORT_BLOCK_IF_1H_BULLISH = os.getenv("SHORT_BLOCK_IF_1H_BULLISH", "true").lower() == "true"
SHORT_B_REQUIRES_1H_OR_4H_BEARISH = os.getenv("SHORT_B_REQUIRES_1H_OR_4H_BEARISH", "true").lower() == "true"
LONG_BLOCK_IF_BTC_AND_1H_BEARISH = os.getenv("LONG_BLOCK_IF_BTC_AND_1H_BEARISH", "true").lower() == "true"

# HTF confirmation.
HTF_CONFIRMATION_ENABLED = os.getenv("HTF_CONFIRMATION_ENABLED", "true").lower() == "true"
A_PLUS_REQUIRES_1H_4H_CONFIRM = os.getenv("A_PLUS_REQUIRES_1H_4H_CONFIRM", "true").lower() == "true"
B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM = os.getenv("B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM", "true").lower() == "true"
BLOCK_IF_BOTH_HTF_AGAINST = os.getenv("BLOCK_IF_BOTH_HTF_AGAINST", "true").lower() == "true"
HTF_1H_CONFIRM_BONUS = strategy_int("HTF_1H_CONFIRM_BONUS", 5)
HTF_4H_CONFIRM_BONUS = strategy_int("HTF_4H_CONFIRM_BONUS", 4)
HTF_FULL_CONFIRM_BONUS = strategy_int("HTF_FULL_CONFIRM_BONUS", 3)

# Risk model.
MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "14"))
TP1_R_MULTIPLIER = float(os.getenv("TP1_R_MULTIPLIER", "0.75"))
TP2_R_MULTIPLIER = float(os.getenv("TP2_R_MULTIPLIER", "1.25"))
TP3_R_MULTIPLIER = float(os.getenv("TP3_R_MULTIPLIER", "1.90"))
MIN_TP1_PRICE_MOVE_PERCENT = float(os.getenv("MIN_TP1_PRICE_MOVE_PERCENT", "0.45"))
TP1_CLOSE_PERCENT = float(os.getenv("TP1_CLOSE_PERCENT", "50"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.0005"))
SLIPPAGE_RATE = float(os.getenv("SLIPPAGE_RATE", "0.0003"))

# Anti-chase and target space.
ENABLE_ANTI_CHASE_FILTER = os.getenv("ENABLE_ANTI_CHASE_FILTER", "true").lower() == "true"
CHASE_LOOKBACK_CANDLES_5M = int(os.getenv("CHASE_LOOKBACK_CANDLES_5M", "18"))
MAX_CHASE_MOVE_5M_PERCENT = float(os.getenv("MAX_CHASE_MOVE_5M_PERCENT", "4.8"))
EXTREME_CHASE_MOVE_5M_PERCENT = float(os.getenv("EXTREME_CHASE_MOVE_5M_PERCENT", "8.5"))
MIN_PULLBACK_AFTER_CHASE_PERCENT = float(os.getenv("MIN_PULLBACK_AFTER_CHASE_PERCENT", "0.55"))
MIN_PULLBACK_AFTER_EXTREME_PERCENT = float(os.getenv("MIN_PULLBACK_AFTER_EXTREME_PERCENT", "1.20"))
MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT = float(os.getenv("MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT", "2.8"))
ENABLE_LATE_ENTRY_FILTER = os.getenv("ENABLE_LATE_ENTRY_FILTER", "true").lower() == "true"
MAX_RECENT_MOVE_PERCENT = float(os.getenv("MAX_RECENT_MOVE_PERCENT", "7.5"))
MAX_DISTANCE_FROM_VWAP_PERCENT = float(os.getenv("MAX_DISTANCE_FROM_VWAP_PERCENT", "5.8"))
ENABLE_SPACE_TO_TARGET_FILTER = os.getenv("ENABLE_SPACE_TO_TARGET_FILTER", "true").lower() == "true"
MIN_SPACE_TO_TARGET_PERCENT_A_PLUS = float(os.getenv("MIN_SPACE_TO_TARGET_PERCENT_A_PLUS", "0.45"))
MIN_SPACE_TO_TARGET_PERCENT_B = float(os.getenv("MIN_SPACE_TO_TARGET_PERCENT_B", "0.28"))

# External filters.
ENABLE_FUNDING_FILTER = os.getenv("ENABLE_FUNDING_FILTER", "true").lower() == "true"
ENABLE_OI_FILTER = os.getenv("ENABLE_OI_FILTER", "true").lower() == "true"
MAX_ABS_FUNDING_RATE = float(os.getenv("MAX_ABS_FUNDING_RATE", "0.0012"))
FUNDING_EXTREME_RATE = float(os.getenv("FUNDING_EXTREME_RATE", "0.0025"))
ALLOW_COUNTER_BTC_B_SIGNALS = os.getenv("ALLOW_COUNTER_BTC_B_SIGNALS", "false").lower() == "true"
COUNTER_BTC_RISK_MULTIPLIER = float(os.getenv("COUNTER_BTC_RISK_MULTIPLIER", "0.14"))

# Impulse Pullback Pro — частая, но только B и с малым риском.
IMPULSE_PULLBACK_ENABLED = os.getenv("IMPULSE_PULLBACK_ENABLED", "true").lower() == "true"
IMPULSE_PULLBACK_RISK_MULTIPLIER = float(os.getenv("IMPULSE_PULLBACK_RISK_MULTIPLIER", "0.20"))
IMPULSE_MIN_MOVE_5M_PERCENT = float(os.getenv("IMPULSE_MIN_MOVE_5M_PERCENT", "0.90"))
IMPULSE_PULLBACK_MIN_PERCENT = float(os.getenv("IMPULSE_PULLBACK_MIN_PERCENT", "0.18"))
IMPULSE_PULLBACK_MAX_PERCENT = float(os.getenv("IMPULSE_PULLBACK_MAX_PERCENT", "3.20"))
IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT = float(os.getenv("IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT", "3.00"))
IMPULSE_MIN_VOLUME_RATIO = float(os.getenv("IMPULSE_MIN_VOLUME_RATIO", "1.00"))

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
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "INJ", "NEAR", "ARB", "OP",
    "APT", "SUI", "SEI", "DOT", "LTC", "BCH", "UNI", "AAVE", "FIL", "ATOM", "ETC", "TRX", "MATIC", "POL",
    "WLD", "TIA", "ORDI", "FTM", "RUNE", "ENA", "JUP", "PYTH", "STRK", "DYDX", "TON", "COMP", "STX",
    "TRB", "JTO", "DYM", "ICP", "GALA", "FET", "RNDR", "RENDER", "IMX", "APE", "AR", "MKR", "SNX",
    "LDO", "CRV", "GMT", "PEPE", "1000PEPE", "WIF", "BONK", "NOT", "ONDO", "BLUR", "MEME", "AI",
    "ACE", "ARKM", "PENDLE", "BIGTIME", "ZRO", "ZK", "TAO", "1000SATS", "SAGA", "MANTA", "ALT", "PIXEL",
    "PORTAL", "AEVO", "W", "OMNI", "TNSR", "BB", "PEOPLE", "1000SHIB",
}

# ---------- small helpers ----------
def now_ts() -> float:
    return time.time()

def normalize_symbol(symbol: str) -> str:
    s = symbol.upper().replace("/", "-").strip()
    if s.endswith("USDT") and "-" not in s:
        s = s[:-4] + "-USDT"
    if not s.endswith("-USDT"):
        s = s.replace("-", "") + "-USDT"
    return s

def display_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-", "/")

def base_from_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-USDT", "")

def normalize_direction(direction: Optional[str]) -> Optional[str]:
    if not direction:
        return None
    d = direction.upper().strip()
    return d if d in ["LONG", "SHORT"] else None

def is_good_symbol(symbol: str) -> bool:
    base = base_from_symbol(symbol)
    bad = ["USD", "EUR", "GBP", "JPY", "AAPL", "TSLA", "NVDA", "META", "GOOG"]
    return base in LIQUID_BASES and not any(x in base for x in bad)

def calc_winrate(positive: int, sl: int) -> float:
    total = positive + sl
    return round(positive / total * 100, 1) if total > 0 else 0.0

def strategy_side_default() -> Dict[str, int]:
    return {f"{s}:{side}": 0 for s in STRATEGIES for side in ["LONG", "SHORT"]}

def strategy_side_grade_default() -> Dict[str, int]:
    return {f"{s}:{side}:{g}": 0 for s in STRATEGIES for side in ["LONG", "SHORT"] for g in ["A+", "B"]}

def stat_zero() -> Dict[str, int]:
    return {"positive": 0, "sl": 0, "consecutive_sl": 0}

def default_state() -> Dict[str, Any]:
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
            "grade": {"A+": {"positive": 0, "sl": 0}, "B": {"positive": 0, "sl": 0}},
            "strategy": {s: stat_zero() for s in STRATEGIES},
            "strategy_side": {f"{s}:{side}": stat_zero() for s in STRATEGIES for side in ["LONG", "SHORT"]},
            "strategy_side_grade": {f"{s}:{side}:{g}": stat_zero() for s in STRATEGIES for side in ["LONG", "SHORT"] for g in ["A+", "B"]},
            "pair_sl": {},
            "pair_positive": {},
            "closed_trades": [],
        },
        "auto": {"last_scan_time": 0, "last_track_time": 0, "last_no_signal_report_time": 0, "last_scan_result": None, "last_track_result": None, "last_error": None},
    }

def ensure_state_structure(state: Dict[str, Any]) -> Dict[str, Any]:
    base = default_state()
    for k, v in base.items():
        state.setdefault(k, v)
    state.setdefault("stats", base["stats"])
    for k, v in base["stats"].items():
        state["stats"].setdefault(k, v)
    for side in ["LONG", "SHORT"]:
        state["stats"]["side"].setdefault(side, base["stats"]["side"][side])
    for g in ["A+", "B"]:
        state["stats"]["grade"].setdefault(g, {"positive": 0, "sl": 0})
    for s in STRATEGIES:
        state["stats"]["strategy"].setdefault(s, stat_zero())
        for side in ["LONG", "SHORT"]:
            ss = f"{s}:{side}"
            state["stats"]["strategy_side"].setdefault(ss, stat_zero())
            state["strategy_side_hard_disabled_until"].setdefault(ss, 0)
            for g in ["A+", "B"]:
                ssg = f"{s}:{side}:{g}"
                state["stats"]["strategy_side_grade"].setdefault(ssg, stat_zero())
                state["strategy_side_grade_disabled_until"].setdefault(ssg, 0)
    return state

def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return ensure_state_structure(json.load(f))
    except Exception:
        return default_state()

def save_state(state: Dict[str, Any]) -> None:
    try:
        ensure_state_structure(state)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

STATE = load_state()

def ensure_stats_structure() -> None:
    global STATE
    STATE = ensure_state_structure(STATE)

def build_stats_text() -> str:
    ensure_stats_structure()
    long_s = STATE["stats"]["side"]["LONG"]
    short_s = STATE["stats"]["side"]["SHORT"]
    a_s = STATE["stats"]["grade"]["A+"]
    b_s = STATE["stats"]["grade"]["B"]
    strategy_lines = []
    for s in STRATEGIES:
        st = STATE["stats"]["strategy"].get(s, stat_zero())
        strategy_lines.append(f"{s}: {st.get('positive',0)} позитив / {st.get('sl',0)} SL / WR {calc_winrate(st.get('positive',0), st.get('sl',0))}% [ON]")
    return f"""
📊 <b>Статистика {DEPLOY_MARKER}:</b>

📈 LONG: {long_s['positive']} позитив / {long_s['sl']} SL / WR {calc_winrate(long_s['positive'], long_s['sl'])}%
📉 SHORT: {short_s['positive']} позитив / {short_s['sl']} SL / WR {calc_winrate(short_s['positive'], short_s['sl'])}%

🏆 A+: {a_s.get('positive',0)} позитив / {a_s.get('sl',0)} SL / WR {calc_winrate(a_s.get('positive',0), a_s.get('sl',0))}%
⚠️ B: {b_s.get('positive',0)} позитив / {b_s.get('sl',0)} SL / WR {calc_winrate(b_s.get('positive',0), b_s.get('sl',0))}%

🧠 <b>Стратегии:</b>
{chr(10).join(strategy_lines)}
""".strip()

# ---------- network / data ----------
def get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception:
        return None

def interval_to_ms(interval: str) -> int:
    return {"1m": 60000, "3m": 180000, "5m": 300000, "15m": 900000, "30m": 1800000, "1h": 3600000, "4h": 14400000}.get(interval, 60000)

def remove_unclosed_candle(candles: Optional[List[dict]], interval: str) -> Optional[List[dict]]:
    if not candles or not USE_CLOSED_CANDLES_ONLY or len(candles) < 3:
        return candles
    current_ms = int(time.time() * 1000)
    last_open = int(candles[-1]["time"])
    return candles[:-1] if current_ms < last_open + interval_to_ms(interval) else candles

def get_symbols() -> List[str]:
    data = get_json(f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts")
    result = []
    if data:
        for item in data.get("data", []):
            sym = item.get("symbol")
            if sym and is_good_symbol(sym):
                result.append(normalize_symbol(sym))
    random.shuffle(result)
    return result[:MAX_SYMBOLS]

def get_klines(symbol: str, interval: str, limit: int = 260) -> Optional[List[dict]]:
    data = get_json(f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines", {"symbol": normalize_symbol(symbol), "interval": interval, "limit": limit})
    raw = data.get("data", []) if data else []
    candles = []
    for c in raw:
        try:
            candles.append({"time": int(c["time"]), "open": float(c["open"]), "high": float(c["high"]), "low": float(c["low"]), "close": float(c["close"]), "volume": float(c["volume"])})
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
            found = extract_float_from_nested(v, keys)
            if found is not None:
                return found
    if isinstance(data, list):
        for x in data:
            found = extract_float_from_nested(x, keys)
            if found is not None:
                return found
    return None

def get_funding_rate(symbol: str) -> Optional[float]:
    if not ENABLE_FUNDING_FILTER:
        return None
    for ep in ["/openApi/swap/v2/quote/premiumIndex", "/openApi/swap/v2/quote/fundingRate"]:
        data = get_json(f"{BINGX_BASE_URL}{ep}", {"symbol": normalize_symbol(symbol)})
        val = extract_float_from_nested(data, ["lastFundingRate", "fundingRate", "funding_rate", "rate"]) if data else None
        if val is not None:
            return val
    return None

def get_open_interest(symbol: str) -> Optional[float]:
    if not ENABLE_OI_FILTER:
        return None
    for ep in ["/openApi/swap/v2/quote/openInterest", "/openApi/swap/v2/quote/openInterestStat"]:
        data = get_json(f"{BINGX_BASE_URL}{ep}", {"symbol": normalize_symbol(symbol)})
        val = extract_float_from_nested(data, ["openInterest", "open_interest", "sumOpenInterest", "value"]) if data else None
        if val is not None:
            return val
    return None

# ---------- indicators ----------
def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for p in values[1:]:
        out.append(p * k + out[-1] * (1 - k))
    return out

def rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(abs(min(d, 0)))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)

def atr(candles: List[dict], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        high, low, prev = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(high - low, abs(high - prev), abs(low - prev)))
    return sum(trs[-period:]) / period

def vwap_like(candles: List[dict], period: int = 48) -> Optional[float]:
    if len(candles) < period:
        return None
    pv, vol = 0.0, 0.0
    for c in candles[-period:]:
        typical = (c["high"] + c["low"] + c["close"]) / 3
        pv += typical * c["volume"]
        vol += c["volume"]
    return pv / vol if vol > 0 else None

def volume_ratio(candles: List[dict], period: int = 30) -> float:
    if len(candles) < period + 1:
        return 0.0
    avg = sum(c["volume"] for c in candles[-period - 1:-1]) / period
    return candles[-1]["volume"] / avg if avg > 0 else 0.0

def trend_state(candles: List[dict]) -> str:
    closes = [c["close"] for c in candles]
    if len(closes) < 200:
        return "NEUTRAL"
    e50, e200, price = ema(closes, 50)[-1], ema(closes, 200)[-1], closes[-1]
    if price > e50 > e200:
        return "BULLISH"
    if price < e50 < e200:
        return "BEARISH"
    if price > e200:
        return "SOFT_BULLISH"
    if price < e200:
        return "SOFT_BEARISH"
    return "NEUTRAL"

def recent_move_percent(candles: List[dict], lookback: int = 8) -> float:
    if len(candles) < lookback + 1:
        return 0.0
    old, new = candles[-lookback]["close"], candles[-1]["close"]
    return (new - old) / old * 100 if old > 0 else 0.0

def distance_percent(a: float, b: float) -> float:
    return abs(a - b) / b * 100 if b > 0 else 999

def candle_close_position(c: dict) -> float:
    rng = c["high"] - c["low"]
    return (c["close"] - c["low"]) / rng if rng > 0 else 0.5

def merge_levels(levels: List[float], threshold_percent: float = 0.38) -> List[float]:
    if not levels:
        return []
    out = []
    for lvl in sorted(levels):
        if not out:
            out.append(lvl)
        elif abs(lvl - out[-1]) / out[-1] * 100 <= threshold_percent:
            out[-1] = (out[-1] + lvl) / 2
        else:
            out.append(lvl)
    return out

def find_swing_support_levels(candles: List[dict], lookback: int = 140) -> List[float]:
    if len(candles) < 40:
        return []
    w = candles[-lookback:] if len(candles) >= lookback else candles[:]
    lvls = []
    for i in range(3, len(w) - 3):
        low = w[i]["low"]
        if low <= min(w[i-1]["low"], w[i-2]["low"], w[i-3]["low"]) and low <= min(w[i+1]["low"], w[i+2]["low"], w[i+3]["low"]):
            lvls.append(low)
    return merge_levels(lvls)

def find_swing_resistance_levels(candles: List[dict], lookback: int = 140) -> List[float]:
    if len(candles) < 40:
        return []
    w = candles[-lookback:] if len(candles) >= lookback else candles[:]
    lvls = []
    for i in range(3, len(w) - 3):
        high = w[i]["high"]
        if high >= max(w[i-1]["high"], w[i-2]["high"], w[i-3]["high"]) and high >= max(w[i+1]["high"], w[i+2]["high"], w[i+3]["high"]):
            lvls.append(high)
    return merge_levels(lvls)

def nearest_below(price: float, levels: List[float], max_dist: float = 8.0) -> Optional[float]:
    cand = [x for x in levels if x < price]
    if not cand:
        return None
    lvl = max(cand)
    return lvl if abs(price - lvl) / lvl * 100 <= max_dist else None

def nearest_above(price: float, levels: List[float], max_dist: float = 8.0) -> Optional[float]:
    cand = [x for x in levels if x > price]
    if not cand:
        return None
    lvl = min(cand)
    return lvl if abs(lvl - price) / price * 100 <= max_dist else None

def nearest_near(price: float, levels: List[float], max_dist: float = 2.0) -> Optional[float]:
    if not levels or price <= 0:
        return None
    lvl = min(levels, key=lambda x: abs(x - price))
    return lvl if abs(lvl - price) / price * 100 <= max_dist else None

def has_near_level(level: float, levels: List[float], max_dist: float) -> bool:
    return any(abs(x - level) / level * 100 <= max_dist for x in levels) if level > 0 else False

def level_mtf_strength(level: float, c1h: List[dict], c4h: List[dict], kind: str) -> Dict[str, Any]:
    if kind == "support":
        l1, l4 = find_swing_support_levels(c1h), find_swing_support_levels(c4h)
    else:
        l1, l4 = find_swing_resistance_levels(c1h), find_swing_resistance_levels(c4h)
    ok1 = has_near_level(level, l1, 1.2)
    ok4 = has_near_level(level, l4, 1.8)
    return {"level_1h_confirmed": ok1, "level_4h_confirmed": ok4, "level_strength_bonus": (6 if ok1 else 0) + (3 if ok4 else 0), "level_strength_note": f"Level MTF: 1H {'OK' if ok1 else 'NO'} / 4H {'OK' if ok4 else 'NO'}"}

def space_to_next_level_percent(price: float, direction: str, supports: List[float], resistances: List[float]) -> Optional[float]:
    if direction == "LONG":
        above = [x for x in resistances if x > price]
        return None if not above else (min(above) - price) / price * 100
    below = [x for x in supports if x < price]
    return None if not below else (price - max(below)) / price * 100

# ---------- filters ----------
def detect_btc_status() -> str:
    btc = remove_unclosed_candle(get_klines("BTC-USDT", "1h", 260), "1h")
    return trend_state(btc) if btc else "NEUTRAL"

def analyze_funding_oi(symbol: str, direction: str) -> Dict[str, Any]:
    funding = get_funding_rate(symbol)
    oi = get_open_interest(symbol)
    blocked, adj, reasons = False, 0, []
    if funding is not None:
        if abs(funding) >= FUNDING_EXTREME_RATE:
            blocked = True
            reasons.append(f"Funding экстремальный: {funding:.6f}")
        elif direction == "LONG" and funding > MAX_ABS_FUNDING_RATE:
            adj -= 2; reasons.append(f"Funding перегрет для LONG: {funding:.6f}")
        elif direction == "SHORT" and funding < -MAX_ABS_FUNDING_RATE:
            adj -= 2; reasons.append(f"Funding перегрет для SHORT: {funding:.6f}")
        else:
            adj += 1; reasons.append(f"Funding нормальный: {funding:.6f}")
    else:
        reasons.append("Funding недоступен")
    reasons.append(f"OI доступен: {round(oi,2)}" if oi is not None else "OI недоступен")
    return {"blocked": blocked, "score_adjustment": adj, "funding": funding, "open_interest": oi, "reason": "; ".join(reasons)}

def is_trend_confirming_direction(trend: str, direction: str) -> bool:
    return trend in (["BULLISH", "SOFT_BULLISH"] if direction == "LONG" else ["BEARISH", "SOFT_BEARISH"])

def is_trend_hard_against_direction(trend: str, direction: str) -> bool:
    return trend == ("BEARISH" if direction == "LONG" else "BULLISH")

def htf_confirmation_data(direction: str, trend1h: str, trend4h: str) -> Dict[str, Any]:
    ok1 = is_trend_confirming_direction(trend1h, direction)
    ok4 = is_trend_confirming_direction(trend4h, direction)
    against1 = is_trend_hard_against_direction(trend1h, direction)
    against4 = is_trend_hard_against_direction(trend4h, direction)
    bonus = (HTF_1H_CONFIRM_BONUS if ok1 else 0) + (HTF_4H_CONFIRM_BONUS if ok4 else 0) + (HTF_FULL_CONFIRM_BONUS if ok1 and ok4 else 0)
    if ok1 and ok4:
        note = f"HTF: 1H {trend1h} + 4H {trend4h} подтверждают направление."
    elif ok1 or ok4:
        note = f"HTF: частичное подтверждение — 1H {trend1h}, 4H {trend4h}. A+ запрещён, B возможен."
    elif against1 and against4:
        note = f"HTF: 1H {trend1h} и 4H {trend4h} против направления — блок."
    else:
        note = f"HTF: нет сильного подтверждения — 1H {trend1h}, 4H {trend4h}."
    return {"trend1h": trend1h, "trend4h": trend4h, "htf_1h_confirmed": ok1, "htf_4h_confirmed": ok4, "htf_full_confirmed": ok1 and ok4, "htf_any_confirmed": ok1 or ok4, "htf_both_against": against1 and against4, "htf_score_bonus": bonus, "htf_note": note}

def combine_filters(symbol: str, direction: str, btc_status: str, market_data: Dict[str, Any]) -> Dict[str, Any]:
    funding = analyze_funding_oi(symbol, direction)
    filters = {"blocked": funding["blocked"], "score_adjustment": funding["score_adjustment"], "funding": funding, "btc_status": btc_status}
    htf = htf_confirmation_data(direction, market_data.get("trend1h", "NEUTRAL"), market_data.get("trend4h", "NEUTRAL"))
    filters.update(htf)
    filters["score_adjustment"] += htf.get("htf_score_bonus", 0)
    if HTF_CONFIRMATION_ENABLED and BLOCK_IF_BOTH_HTF_AGAINST and htf.get("htf_both_against"):
        filters["blocked"] = True
    btc_against = (direction == "LONG" and btc_status == "BEARISH") or (direction == "SHORT" and btc_status == "BULLISH")
    filters["btc_against"] = btc_against
    if btc_against and ALLOW_COUNTER_BTC_B_SIGNALS:
        filters["force_grade"] = "B"
        filters["risk_multiplier_override"] = COUNTER_BTC_RISK_MULTIPLIER
        filters["countertrend_note"] = "BTC против направления: только B с пониженным риском."
    elif btc_against and direction == "SHORT" and SHORT_BLOCK_IF_BTC_BULLISH:
        filters["blocked"] = True
    return filters

def attach_space_filter(filters: Dict[str, Any], price: float, direction: str, c15: List[dict]) -> Dict[str, Any]:
    supports = find_swing_support_levels(c15)
    resist = find_swing_resistance_levels(c15)
    filters["space_to_target_percent"] = space_to_next_level_percent(price, direction, supports, resist)
    return filters

def late_entry_blocked(direction: str, candles: List[dict], price: float, vw: float) -> bool:
    if not ENABLE_LATE_ENTRY_FILTER:
        return False
    move = recent_move_percent(candles, 8)
    if direction == "LONG" and move > MAX_RECENT_MOVE_PERCENT:
        return True
    if direction == "SHORT" and move < -MAX_RECENT_MOVE_PERCENT:
        return True
    return distance_percent(price, vw) > MAX_DISTANCE_FROM_VWAP_PERCENT

def anti_chase_blocked(direction: str, c5: List[dict]) -> bool:
    if not ENABLE_ANTI_CHASE_FILTER or len(c5) < max(CHASE_LOOKBACK_CANDLES_5M + 5, 40):
        return False
    closes = [c["close"] for c in c5]
    e21 = ema(closes, 21)[-1]
    price = closes[-1]
    old = c5[-CHASE_LOOKBACK_CANDLES_5M]["close"]
    if old <= 0 or e21 <= 0:
        return False
    move = (price - old) / old * 100
    dist_ema = distance_percent(price, e21)
    recent = c5[-8:]
    if direction == "LONG":
        high = max(c["high"] for c in c5[-CHASE_LOOKBACK_CANDLES_5M:])
        pullback = (high - min(c["low"] for c in recent)) / high * 100 if high > 0 else 0
        if move >= EXTREME_CHASE_MOVE_5M_PERCENT:
            return pullback < MIN_PULLBACK_AFTER_EXTREME_PERCENT or dist_ema > MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT
        if move >= MAX_CHASE_MOVE_5M_PERCENT:
            return pullback < MIN_PULLBACK_AFTER_CHASE_PERCENT and dist_ema > MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT
    else:
        low = min(c["low"] for c in c5[-CHASE_LOOKBACK_CANDLES_5M:])
        pullback = (max(c["high"] for c in recent) - low) / low * 100 if low > 0 else 0
        if move <= -EXTREME_CHASE_MOVE_5M_PERCENT:
            return pullback < MIN_PULLBACK_AFTER_EXTREME_PERCENT or dist_ema > MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT
        if move <= -MAX_CHASE_MOVE_5M_PERCENT:
            return pullback < MIN_PULLBACK_AFTER_CHASE_PERCENT and dist_ema > MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT
    return False

def pro_direction_guard_allows(score: int, rr: float, volume: float, filters: Dict[str, Any], direction: str, wanted_grade: str) -> bool:
    if not PRO_BALANCED_GUARD_ENABLED:
        return True
    btc = filters.get("btc_status", "NEUTRAL")
    t1 = filters.get("trend1h", "NEUTRAL")
    t4 = filters.get("trend4h", "NEUTRAL")
    if direction == "SHORT":
        if btc == "BULLISH" and SHORT_BLOCK_IF_BTC_BULLISH:
            return False
        if t1 == "BULLISH" and SHORT_BLOCK_IF_1H_BULLISH:
            return False
        if wanted_grade == "B":
            if not SHORT_B_ENABLED:
                return False
            if score < SHORT_B_MIN_SCORE or rr < SHORT_B_MIN_RR or volume < SHORT_B_MIN_VOLUME_RATIO:
                return False
            if SHORT_B_REQUIRES_1H_OR_4H_BEARISH and not (t1 in ["BEARISH", "SOFT_BEARISH"] or t4 in ["BEARISH", "SOFT_BEARISH"]):
                return False
    if direction == "LONG":
        if LONG_BLOCK_IF_BTC_AND_1H_BEARISH and btc == "BEARISH" and t1 == "BEARISH":
            return False
    return True

# ---------- signal build / classify ----------
def is_level_strategy(strategy: str) -> bool:
    return strategy.startswith("LEVEL_")

def estimate_trade_cost_percent() -> float:
    return (FEE_RATE * 2 + SLIPPAGE_RATE * 2) * 100

def calc_risk_position(entry: float, sl: float) -> float:
    return abs(entry - sl) / entry * 100 * LEVERAGE if entry > 0 else 999

def price_move_percent(entry: float, target: float, direction: str) -> float:
    if entry <= 0:
        return 0
    return (target - entry) / entry * 100 if direction == "LONG" else (entry - target) / entry * 100

def make_dynamic_tps(entry: float, sl: float, direction: str) -> Tuple[float, float, float]:
    risk = abs(entry - sl)
    min_move = MIN_TP1_PRICE_MOVE_PERCENT / 100
    if risk <= 0:
        return (entry * (1 + min_move), entry * (1 + min_move * 1.8), entry * (1 + min_move * 2.8)) if direction == "LONG" else (entry * (1 - min_move), entry * (1 - min_move * 1.8), entry * (1 - min_move * 2.8))
    if direction == "LONG":
        return max(entry + risk * TP1_R_MULTIPLIER, entry * (1 + min_move)), max(entry + risk * TP2_R_MULTIPLIER, entry * (1 + min_move * 1.8)), max(entry + risk * TP3_R_MULTIPLIER, entry * (1 + min_move * 2.8))
    return min(entry - risk * TP1_R_MULTIPLIER, entry * (1 - min_move)), min(entry - risk * TP2_R_MULTIPLIER, entry * (1 - min_move * 1.8)), min(entry - risk * TP3_R_MULTIPLIER, entry * (1 - min_move * 2.8))

def calculate_position(entry: float, sl: float, deposit: float, risk_percent: float) -> Dict[str, Any]:
    risk_amount = deposit * risk_percent / 100
    stop = abs(entry - sl)
    if entry <= 0 or stop <= 0:
        return {"risk_amount": round(risk_amount, 2), "position_size_usdt": None, "coin_amount": None, "margin_usdt": None, "error": "Неверный entry или SL"}
    coin = risk_amount / stop
    size = coin * entry
    return {"risk_amount": round(risk_amount, 2), "position_size_usdt": round(size, 2), "coin_amount": round(coin, 8), "margin_usdt": round(size / LEVERAGE, 2), "error": None}

def is_strategy_side_enabled(strategy: str, side: str) -> bool:
    ensure_stats_structure()
    return now_ts() >= STATE["strategy_side_hard_disabled_until"].get(f"{strategy}:{side}", 0)

def is_strategy_side_grade_enabled(strategy: str, side: str, grade: str) -> bool:
    ensure_stats_structure()
    return now_ts() >= STATE["strategy_side_grade_disabled_until"].get(f"{strategy}:{side}:{grade}", 0)

def get_strategy_winrate(strategy: str) -> Tuple[int, float]:
    st = STATE["stats"]["strategy"].get(strategy, stat_zero())
    return st.get("positive", 0) + st.get("sl", 0), calc_winrate(st.get("positive", 0), st.get("sl", 0))

def can_strategy_be_a_plus(strategy: str) -> bool:
    # Не душим рано: только после 18 сделок стратегия с WR ниже 52% не может быть A+.
    trades, wr = get_strategy_winrate(strategy)
    return not (trades >= 18 and wr < 52)

def classify_signal(score: int, rr: float, volume: float, filters: Dict[str, Any], strategy: str, direction: str) -> Optional[Dict[str, Any]]:
    if filters.get("blocked"):
        return None
    funding = filters.get("funding", {})
    if funding.get("blocked"):
        return None
    b_score, b_rr, b_vol = B_MIN_SCORE, B_MIN_RR, B_MIN_VOLUME_RATIO
    if is_level_strategy(strategy):
        b_score, b_rr, b_vol = min(b_score, LEVEL_B_MIN_SCORE), min(b_rr, LEVEL_B_MIN_RR), min(b_vol, LEVEL_B_MIN_VOLUME_RATIO)
    htf_full = filters.get("htf_full_confirmed", False)
    htf_any = filters.get("htf_any_confirmed", False)
    if filters.get("force_grade") == "B":
        if score >= b_score and rr >= b_rr and volume >= b_vol:
            if HTF_CONFIRMATION_ENABLED and B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM and not htf_any:
                return None
            if not pro_direction_guard_allows(score, rr, volume, filters, direction, "B"):
                return None
            return {"grade": "B", "risk_multiplier": filters.get("risk_multiplier_override", B_RISK_MULTIPLIER)}
        return None
    a_htf_allowed = (not HTF_CONFIRMATION_ENABLED) or (not A_PLUS_REQUIRES_1H_4H_CONFIRM) or htf_full
    level_a_allowed = True
    if is_level_strategy(strategy):
        level_a_allowed = filters.get("level_1h_confirmed", True) or score >= A_PLUS_MIN_SCORE + 8
    if score >= A_PLUS_MIN_SCORE and rr >= A_PLUS_MIN_RR and volume >= A_PLUS_MIN_VOLUME_RATIO and can_strategy_be_a_plus(strategy) and a_htf_allowed and level_a_allowed and pro_direction_guard_allows(score, rr, volume, filters, direction, "A+"):
        return {"grade": "A+", "risk_multiplier": A_PLUS_RISK_MULTIPLIER}
    if score >= b_score and rr >= b_rr and volume >= b_vol:
        if HTF_CONFIRMATION_ENABLED and B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM and not htf_any:
            return None
        if not pro_direction_guard_allows(score, rr, volume, filters, direction, "B"):
            return None
        return {"grade": "B", "risk_multiplier": filters.get("risk_multiplier_override", B_RISK_MULTIPLIER)}
    return None

def build_signal(symbol: str, direction: str, strategy: str, entry: float, sl: float, score: int, vol_ratio: float, reason: str, deposit: float, risk_percent: float, filters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if entry <= 0 or sl <= 0:
        return None
    risk_pos = calc_risk_position(entry, sl)
    if risk_pos > MAX_RISK_POSITION_PERCENT:
        return None
    score += filters.get("score_adjustment", 0)
    if is_level_strategy(strategy):
        score += LEVEL_SIGNAL_SCORE_BONUS
    space = filters.get("space_to_target_percent")
    if ENABLE_SPACE_TO_TARGET_FILTER and space is not None:
        if score >= A_PLUS_MIN_SCORE and space < MIN_SPACE_TO_TARGET_PERCENT_A_PLUS:
            filters["force_grade"] = "B"
            filters["space_note"] = f"До ближайшего уровня мало места: {round(space,2)}%. A+ запрещён, B возможен."
        elif score < A_PLUS_MIN_SCORE and space < MIN_SPACE_TO_TARGET_PERCENT_B:
            return None
    tp1, tp2, tp3 = make_dynamic_tps(entry, sl, direction)
    risk_price = abs(entry - sl) / entry * 100
    cost = estimate_trade_cost_percent()
    raw_tp2 = price_move_percent(entry, tp2, direction)
    net_tp2 = max(raw_tp2 - cost, 0)
    rr = net_tp2 / risk_price if risk_price > 0 else 0
    grade_data = classify_signal(score, rr, vol_ratio, filters, strategy, direction)
    if not grade_data:
        return None
    grade = grade_data["grade"]
    if not is_strategy_side_enabled(strategy, direction) or not is_strategy_side_grade_enabled(strategy, direction, grade):
        return None
    signal_id = f"{normalize_symbol(symbol)}:{strategy}:{direction}:{grade}:{round(entry, 8)}"
    if signal_id in STATE.get("sent_signals", {}):
        return None
    risk_multiplier = grade_data["risk_multiplier"]
    adjusted_risk = risk_percent * risk_multiplier
    pos = calculate_position(entry, sl, deposit, adjusted_risk)
    raw_tp1 = price_move_percent(entry, tp1, direction)
    return {"id": signal_id, "symbol": normalize_symbol(symbol), "display_symbol": display_symbol(symbol), "direction": direction, "strategy": strategy, "grade": grade, "risk_multiplier": risk_multiplier, "status": "ACTIVE", "score": min(max(int(score), 0), 98), "entry": round(entry, 8), "sl": round(sl, 8), "tp1": round(tp1, 8), "tp2": round(tp2, 8), "tp3": round(tp3, 8), "rr": round(rr, 2), "rr_basis": "TP2 net", "raw_reward_to_tp1_percent": round(raw_tp1, 4), "net_reward_to_tp1_percent": round(max(raw_tp1 - cost, 0), 4), "raw_reward_to_tp2_percent": round(raw_tp2, 4), "net_reward_to_tp2_percent": round(net_tp2, 4), "estimated_trade_cost_percent": round(cost, 4), "volume_ratio": round(vol_ratio, 2), "risk_position_percent": round(risk_pos, 2), "space_to_target_percent": None if space is None else round(space, 3), "risk_percent": adjusted_risk, "position": pos, "reason": reason, "filters": filters, "created_at": now_ts(), "last_checked_time": int(now_ts() * 1000), "tp1_hit": False, "tp2_hit": False, "tp3_hit": False, "counted_positive": False, "counted_sl": False, "counted_tp1": False, "counted_tp2": False, "counted_tp3": False}

# ---------- market data / strategies ----------
def common_market_data(c15: List[dict], c5: List[dict], c1: List[dict], c1h: List[dict], c4h: List[dict]) -> Dict[str, Any]:
    close15, close5, close1 = [c["close"] for c in c15], [c["close"] for c in c5], [c["close"] for c in c1]
    return {"closes15": close15, "closes5": close5, "closes1": close1, "a5": atr(c5), "a15": atr(c15), "vw": vwap_like(c15), "rs5": rsi(close5), "rs15": rsi(close15), "vr5": volume_ratio(c5, 24), "trend1h": trend_state(c1h), "trend4h": trend_state(c4h), "ema9_1": ema(close1, 9)[-1] if len(close1) >= 10 else None, "ema21_5": ema(close5, 21)[-1] if len(close5) >= 22 else None, "ema50_15": ema(close15, 50)[-1] if len(close15) >= 51 else None}

def valid_common(d: Dict[str, Any], keys: List[str]) -> bool:
    return all(d.get(k) is not None for k in keys)

def evaluate_level_sweep_bounce_long(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "LEVEL_SWEEP_BOUNCE_LONG"
    if direction != "LONG" or len(c15) < 100 or len(c5) < 80:
        return None
    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not valid_common(d, ["a5", "vw", "rs5", "rs15", "ema21_5", "ema50_15"]):
        return None
    last, prev, price = c5[-1], c5[-2], c5[-1]["close"]
    if late_entry_blocked(direction, c5, price, d["vw"]):
        return None
    levels = find_swing_support_levels(c15)
    level = nearest_below(price, levels, 8.0) or nearest_near(price, levels, 2.0)
    if not level:
        return None
    window = c5[-18:]
    sweep_low = min(c["low"] for c in window)
    swept = sweep_low < level * 0.998
    reclaimed = any(c["close"] > level * 1.0005 for c in c5[-5:])
    bounce = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and last["close"] >= d["ema21_5"] * 0.992
    if not (swept and reclaimed and bounce) or d["rs5"] > 76 or d["rs15"] > 72:
        return None
    score = 60
    score += 7 if btc_status in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 7 if d["trend1h"] in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 3 if d["trend4h"] in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 6 if d["vr5"] >= 0.95 else 0
    score += 5 if d["vr5"] >= 1.15 else 0
    score += 3 if price > d["vw"] * 0.99 else 0
    score += 4 if abs(sweep_low - level) / level * 100 <= 1.1 else 0
    score += 3 if candle_close_position(last) >= 0.55 else 0
    sl = min(sweep_low - d["a5"] * 0.10, min(c["low"] for c in c5[-10:]) - d["a5"] * 0.04)
    mtf = level_mtf_strength(level, c1h, c4h, "support")
    score += mtf["level_strength_bonus"]
    filters = combine_filters(symbol, direction, btc_status, d)
    attach_space_filter(filters, price, direction, c15)
    filters.update(mtf)
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], "Поддержка удержалась: sweep ниже уровня → reclaim → подтверждающая зелёная свеча. V6.1: вход не догоняющий, с HTF-фильтром.", deposit, risk_percent, filters)

def evaluate_level_resistance_reject_short(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "LEVEL_RESISTANCE_REJECT_SHORT"
    if direction != "SHORT" or len(c15) < 100 or len(c5) < 80:
        return None
    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not valid_common(d, ["a5", "vw", "rs5", "rs15", "ema21_5", "ema50_15"]):
        return None
    last, prev, price = c5[-1], c5[-2], c5[-1]["close"]
    if late_entry_blocked(direction, c5, price, d["vw"]):
        return None
    levels = find_swing_resistance_levels(c15)
    level = nearest_above(price, levels, 8.0) or nearest_near(price, levels, 2.0)
    if not level:
        return None
    window = c5[-18:]
    sweep_high = max(c["high"] for c in window)
    swept = sweep_high > level * 1.002
    rejected = any(c["close"] < level * 0.9995 for c in c5[-5:])
    rejection = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and last["close"] <= d["ema21_5"] * 1.006
    if not (swept and rejected and rejection):
        return None
    closes_below = sum(1 for c in c5[-4:] if c["close"] < level * 0.9995)
    price_away = price < sweep_high * 0.996
    lower_high = max(c["high"] for c in c5[-3:]) < sweep_high * 0.999
    if closes_below < 2 or not (price_away or lower_high):
        return None
    if d["rs5"] < 24 or d["rs15"] < 28:
        return None
    score = 60
    score += 7 if btc_status in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 7 if d["trend1h"] in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 3 if d["trend4h"] in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 6 if d["vr5"] >= 1.0 else 0
    score += 5 if d["vr5"] >= 1.18 else 0
    score += 3 if price < d["vw"] * 1.01 else 0
    score += 4 if closes_below >= 3 else 0
    score += 3 if candle_close_position(last) <= 0.45 else 0
    sl = max(sweep_high + d["a5"] * 0.10, max(c["high"] for c in c5[-10:]) + d["a5"] * 0.04)
    mtf = level_mtf_strength(level, c1h, c4h, "resistance")
    score += mtf["level_strength_bonus"]
    filters = combine_filters(symbol, direction, btc_status, d)
    attach_space_filter(filters, price, direction, c15)
    filters.update(mtf)
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], "Сопротивление удержалось: sweep выше → возврат ниже → второе подтверждение продавца/lower high. V6.1 не шортит первую красную свечу.", deposit, risk_percent, filters)

def evaluate_level_break_retest_long(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "LEVEL_BREAK_RETEST_LONG"
    if direction != "LONG" or len(c15) < 100 or len(c5) < 80:
        return None
    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not valid_common(d, ["a5", "vw", "rs5", "rs15", "ema21_5", "ema50_15"]):
        return None
    last, prev, price = c5[-1], c5[-2], c5[-1]["close"]
    if late_entry_blocked(direction, c5, price, d["vw"]):
        return None
    levels = find_swing_resistance_levels(c15)
    level = nearest_below(price, levels, 4.0)
    if not level:
        return None
    had_below = any(c["close"] < level * 0.999 for c in c15[-12:-2])
    now_above = c15[-1]["close"] > level * 1.001 or c15[-2]["close"] > level * 1.001
    touched = last["low"] <= level * 1.008
    held = last["close"] > level * 0.999
    confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998
    fresh = price > level * 1.004 and last["close"] > last["open"] and d["vr5"] >= 1.05 and recent_move_percent(c5, 6) < 3.5
    if not ((had_below and now_above and touched and held and confirm) or fresh):
        return None
    if d["rs5"] > 78 or d["rs15"] > 75 or price < d["ema21_5"] * 0.992:
        return None
    score = 61
    score += 7 if btc_status in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 7 if d["trend1h"] in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 3 if d["trend4h"] in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 6 if d["vr5"] >= 1.0 else 0
    score += 5 if d["vr5"] >= 1.18 else 0
    score += 3 if price > d["vw"] else 0
    score += 4 if touched else 0
    recent_low = min(c["low"] for c in c5[-10:])
    sl = min(level - d["a5"] * 0.15, recent_low - d["a5"] * 0.05)
    mtf = level_mtf_strength(level, c1h, c4h, "resistance")
    score += mtf["level_strength_bonus"]
    filters = combine_filters(symbol, direction, btc_status, d)
    attach_space_filter(filters, price, direction, c15)
    filters.update(mtf)
    if fresh and not (touched and held):
        filters["force_grade"] = "B"
        filters["risk_multiplier_override"] = min(filters.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.16)
        filters["anti_fakeout_note"] = "Fresh breakout без идеального ретеста: только B с малым риском."
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], "Пробой сопротивления → удержание/ретест сверху или осторожный fresh breakout. A+ только при полном HTF.", deposit, risk_percent, filters)

def evaluate_level_break_retest_short(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "LEVEL_BREAK_RETEST_SHORT"
    if direction != "SHORT" or len(c15) < 100 or len(c5) < 80:
        return None
    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not valid_common(d, ["a5", "vw", "rs5", "rs15", "ema21_5", "ema50_15"]):
        return None
    last, prev, price = c5[-1], c5[-2], c5[-1]["close"]
    if late_entry_blocked(direction, c5, price, d["vw"]):
        return None
    levels = find_swing_support_levels(c15)
    level = nearest_above(price, levels, 4.0)
    if not level:
        return None
    had_above = any(c["close"] > level * 1.001 for c in c15[-12:-2])
    now_below = c15[-1]["close"] < level * 0.999 or c15[-2]["close"] < level * 0.999
    touched = last["high"] >= level * 0.992
    held = last["close"] < level * 1.001
    confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002
    fresh = price < level * 0.996 and last["close"] < last["open"] and d["vr5"] >= 1.05 and recent_move_percent(c5, 6) > -3.5
    if not ((had_above and now_below and touched and held and confirm) or fresh):
        return None
    if d["rs5"] < 22 or d["rs15"] < 25 or price > d["ema21_5"] * 1.008:
        return None
    score = 61
    score += 7 if btc_status in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 7 if d["trend1h"] in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 3 if d["trend4h"] in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 6 if d["vr5"] >= 1.0 else 0
    score += 5 if d["vr5"] >= 1.18 else 0
    score += 3 if price < d["vw"] else 0
    score += 4 if touched else 0
    recent_high = max(c["high"] for c in c5[-10:])
    sl = max(level + d["a5"] * 0.15, recent_high + d["a5"] * 0.05)
    mtf = level_mtf_strength(level, c1h, c4h, "support")
    score += mtf["level_strength_bonus"]
    filters = combine_filters(symbol, direction, btc_status, d)
    attach_space_filter(filters, price, direction, c15)
    filters.update(mtf)
    if fresh and not (touched and held):
        filters["force_grade"] = "B"
        filters["risk_multiplier_override"] = min(filters.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.16)
        filters["anti_fakeout_note"] = "Fresh breakdown без идеального ретеста: только B с малым риском."
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], "Пробой поддержки → удержание/ретест снизу или осторожный fresh breakdown. SHORT проходит только через pro-guard.", deposit, risk_percent, filters)

def evaluate_trend_pullback_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "TREND_PULLBACK_PRO"
    if direction not in ["LONG", "SHORT"] or len(c15) < 120 or len(c5) < 80:
        return None
    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not valid_common(d, ["a5", "vw", "rs5", "rs15", "ema21_5", "ema50_15"]):
        return None
    last, prev, price = c5[-1], c5[-2], c5[-1]["close"]
    if late_entry_blocked(direction, c5, price, d["vw"]):
        return None
    score = 57
    near = distance_percent(price, d["ema21_5"]) <= 1.35 or distance_percent(price, d["vw"]) <= 1.65
    if direction == "LONG":
        trend_ok = d["trend1h"] in ["BULLISH", "SOFT_BULLISH", "NEUTRAL"] and btc_status != "BEARISH"
        confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and price > d["ema50_15"] * 0.982
        if not (trend_ok and near and confirm) or d["rs5"] > 73 or d["rs15"] > 71:
            return None
        sl = min(last["low"] - d["a5"] * 0.22, min(c["low"] for c in c5[-12:]) - d["a5"] * 0.05)
        reason = "Trend Pullback Pro LONG: откат к EMA/VWAP в направлении структуры → подтверждение продолжения."
    else:
        trend_ok = d["trend1h"] in ["BEARISH", "SOFT_BEARISH", "NEUTRAL"] and btc_status != "BULLISH"
        confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and price < d["ema50_15"] * 1.018
        if not (trend_ok and near and confirm) or d["rs5"] < 27 or d["rs15"] < 29:
            return None
        sl = max(last["high"] + d["a5"] * 0.22, max(c["high"] for c in c5[-12:]) + d["a5"] * 0.05)
        reason = "Trend Pullback Pro SHORT: откат к EMA/VWAP в направлении структуры → подтверждение продолжения."
    score += 6 if ((direction == "LONG" and btc_status in ["BULLISH", "SOFT_BULLISH"]) or (direction == "SHORT" and btc_status in ["BEARISH", "SOFT_BEARISH"])) else 0
    score += 8 if is_trend_confirming_direction(d["trend1h"], direction) else 0
    score += 3 if is_trend_confirming_direction(d["trend4h"], direction) else 0
    score += 5 if d["vr5"] >= 1.0 else 0
    score += 4 if d["vr5"] >= 1.12 else 0
    filters = combine_filters(symbol, direction, btc_status, d)
    attach_space_filter(filters, price, direction, c15)
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], reason, deposit, risk_percent, filters)

def evaluate_vwap_reclaim_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "VWAP_RECLAIM_PRO"
    if direction not in ["LONG", "SHORT"] or len(c15) < 100 or len(c5) < 80:
        return None
    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not valid_common(d, ["a5", "vw", "rs5", "rs15", "ema21_5"]):
        return None
    last, price = c5[-1], c5[-1]["close"]
    if distance_percent(price, d["vw"]) > 2.4:
        return None
    score = 56
    if direction == "LONG":
        was_below = any(c["close"] < d["vw"] * 0.996 for c in c5[-10:-2])
        reclaimed = last["close"] > d["vw"] * 1.001 and last["close"] > last["open"]
        if not (was_below and reclaimed and price > d["ema21_5"] * 0.995) or btc_status == "BEARISH" or d["rs5"] > 74:
            return None
        sl = min(min(c["low"] for c in c5[-8:]) - d["a5"] * 0.06, d["vw"] - d["a5"] * 0.22)
        reason = "VWAP Reclaim Pro LONG: возврат выше VWAP после давления, только аккуратный B/A+ при HTF."
    else:
        was_above = any(c["close"] > d["vw"] * 1.004 for c in c5[-10:-2])
        lost = last["close"] < d["vw"] * 0.999 and last["close"] < last["open"]
        if not (was_above and lost and price < d["ema21_5"] * 1.005) or btc_status == "BULLISH" or d["rs5"] < 26:
            return None
        sl = max(max(c["high"] for c in c5[-8:]) + d["a5"] * 0.06, d["vw"] + d["a5"] * 0.22)
        reason = "VWAP Loss Pro SHORT: возврат ниже VWAP после выкупа, проходит только через SHORT pro-guard."
    score += 5 if d["vr5"] >= 1.0 else 0
    score += 4 if d["vr5"] >= 1.10 else 0
    score += 3 if distance_percent(price, d["vw"]) <= 1.2 else 0
    score += 6 if ((direction == "LONG" and btc_status in ["BULLISH", "SOFT_BULLISH"]) or (direction == "SHORT" and btc_status in ["BEARISH", "SOFT_BEARISH"])) else 0
    score += 5 if is_trend_confirming_direction(d["trend1h"], direction) else 0
    filters = combine_filters(symbol, direction, btc_status, d)
    attach_space_filter(filters, price, direction, c15)
    if score < A_PLUS_MIN_SCORE + 5:
        filters["force_grade"] = "B"
        filters["risk_multiplier_override"] = min(filters.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.18)
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], reason, deposit, risk_percent, filters)

def evaluate_impulse_pullback_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "IMPULSE_PULLBACK_PRO"
    if not IMPULSE_PULLBACK_ENABLED or direction not in ["LONG", "SHORT"] or len(c15) < 120 or len(c5) < 80 or len(c1) < 40:
        return None
    d = common_market_data(c15, c5, c1, c1h, c4h)
    if not valid_common(d, ["a5", "vw", "rs5", "rs15", "ema9_1", "ema21_5", "ema50_15"]):
        return None
    last, prev, price = c5[-1], c5[-2], c5[-1]["close"]
    if d["vr5"] < IMPULSE_MIN_VOLUME_RATIO or distance_percent(price, d["vw"]) > IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT:
        return None
    old_price = c5[-15]["close"]
    recent = c5[-12:-3]
    if not recent or old_price <= 0:
        return None
    score = 57
    if direction == "LONG":
        if btc_status == "BEARISH" or d["trend1h"] == "BEARISH":
            return None
        impulse_high = max(c["high"] for c in recent)
        impulse_move = (impulse_high - old_price) / old_price * 100
        pull_low = min(c["low"] for c in c5[-7:-1])
        pull_pct = (impulse_high - pull_low) / impulse_high * 100 if impulse_high > 0 else 0
        confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and c1[-1]["close"] > d["ema9_1"] * 0.998
        if impulse_move < IMPULSE_MIN_MOVE_5M_PERCENT or not (IMPULSE_PULLBACK_MIN_PERCENT <= pull_pct <= IMPULSE_PULLBACK_MAX_PERCENT) or not confirm or price < d["ema21_5"] * 0.992 or d["rs5"] > 78 or d["rs15"] > 75:
            return None
        sl = min(pull_low - d["a5"] * 0.12, min(c["low"] for c in c5[-10:]) - d["a5"] * 0.04)
        reason = "Impulse Pullback Pro LONG: импульс → здоровый откат → подтверждение продолжения. Только B с малым риском."
    else:
        if btc_status == "BULLISH" or d["trend1h"] == "BULLISH":
            return None
        impulse_low = min(c["low"] for c in recent)
        impulse_move = (old_price - impulse_low) / old_price * 100
        pull_high = max(c["high"] for c in c5[-7:-1])
        pull_pct = (pull_high - impulse_low) / impulse_low * 100 if impulse_low > 0 else 0
        confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and c1[-1]["close"] < d["ema9_1"] * 1.002
        if impulse_move < IMPULSE_MIN_MOVE_5M_PERCENT or not (IMPULSE_PULLBACK_MIN_PERCENT <= pull_pct <= IMPULSE_PULLBACK_MAX_PERCENT) or not confirm or price > d["ema21_5"] * 1.008 or d["rs5"] < 22 or d["rs15"] < 25:
            return None
        sl = max(pull_high + d["a5"] * 0.12, max(c["high"] for c in c5[-10:]) + d["a5"] * 0.04)
        reason = "Impulse Pullback Pro SHORT: импульс вниз → здоровый откат → подтверждение продолжения. Только B и через SHORT pro-guard."
    score += 6 if ((direction == "LONG" and btc_status in ["BULLISH", "SOFT_BULLISH"]) or (direction == "SHORT" and btc_status in ["BEARISH", "SOFT_BEARISH"])) else 0
    score += 6 if is_trend_confirming_direction(d["trend1h"], direction) else 0
    score += 3 if is_trend_confirming_direction(d["trend4h"], direction) else 0
    score += 5 if d["vr5"] >= 1.05 else 0
    score += 4 if d["vr5"] >= 1.18 else 0
    filters = combine_filters(symbol, direction, btc_status, d)
    attach_space_filter(filters, price, direction, c15)
    filters["force_grade"] = "B"
    filters["risk_multiplier_override"] = IMPULSE_PULLBACK_RISK_MULTIPLIER
    filters["anti_fakeout_note"] = "Impulse Pullback Pro: B-only, риск уменьшен."
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], reason, deposit, risk_percent, filters)

# ---------- analysis / scanning ----------
def is_on_cooldown(symbol: str) -> bool:
    ts = STATE.get("symbol_cooldown", {}).get(normalize_symbol(symbol))
    return bool(ts and now_ts() - ts < SIGNAL_COOLDOWN_SECONDS)

def set_cooldown(symbol: str) -> None:
    STATE["symbol_cooldown"][normalize_symbol(symbol)] = now_ts()
    save_state(STATE)

def is_blocked(symbol: str) -> bool:
    sym = normalize_symbol(symbol)
    until = STATE.get("blocked_symbols", {}).get(sym)
    if not until:
        return False
    if now_ts() > until:
        STATE["blocked_symbols"].pop(sym, None)
        save_state(STATE)
        return False
    return True

def cleanup_state() -> None:
    current = now_ts()
    for sid, ts in list(STATE.get("sent_signals", {}).items()):
        if current - ts > SENT_SIGNALS_KEEP_SECONDS:
            STATE["sent_signals"].pop(sid, None)
    for sym, until in list(STATE.get("blocked_symbols", {}).items()):
        if current > until:
            STATE["blocked_symbols"].pop(sym, None)
    for sym, ts in list(STATE.get("symbol_cooldown", {}).items()):
        if current - ts > SIGNAL_COOLDOWN_SECONDS * 3:
            STATE["symbol_cooldown"].pop(sym, None)
    save_state(STATE)

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
    directions = [normalize_direction(direction)] if normalize_direction(direction) else ["LONG", "SHORT"]
    funcs = [evaluate_level_sweep_bounce_long, evaluate_level_resistance_reject_short, evaluate_level_break_retest_long, evaluate_level_break_retest_short, evaluate_trend_pullback_pro, evaluate_vwap_reclaim_pro, evaluate_impulse_pullback_pro]
    candidates = []
    for d in directions:
        if not d or anti_chase_blocked(d, c5):
            continue
        for fn in funcs:
            try:
                sig = fn(symbol, d, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent)
                if sig:
                    candidates.append(sig)
            except Exception:
                continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: (1 if x["grade"] == "A+" else 0, x["score"], x["rr"], x["volume_ratio"], 0 if x.get("space_to_target_percent") is None else x.get("space_to_target_percent")), reverse=True)
    return candidates[0]

def scan_best_signal(deposit: float, risk_percent: float) -> Dict[str, Any]:
    cleanup_state()
    symbols = get_symbols()
    btc_status = detect_btc_status()
    best, checked, found = None, 0, 0
    for sym in symbols:
        checked += 1
        sig = analyze_symbol(sym, None, deposit, risk_percent, btc_status_override=btc_status)
        if not sig:
            continue
        found += 1
        if best is None or (1 if sig["grade"] == "A+" else 0, sig["score"], sig["rr"], sig["volume_ratio"]) > (1 if best["grade"] == "A+" else 0, best["score"], best["rr"], best["volume_ratio"]):
            best = sig
    if not best:
        return {"ok": False, "checked": checked, "found_candidates": found, "btc_status": btc_status, "message": "Сильных сигналов сейчас нет. V6.1: профессиональный баланс, anti-chase и HTF guard не дали нормальный вход."}
    return {"ok": True, "checked": checked, "found_candidates": found, "btc_status": btc_status, "signal": best, "message": build_message(best)}

# ---------- Telegram / messages ----------
def send_telegram_message(text: str) -> Dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не указаны"}
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

def build_message(signal: dict) -> str:
    arrow = "📈" if signal["direction"] == "LONG" else "📉"
    mode = "TEST" if TEST_MODE else "TRADE"
    names = {"LEVEL_SWEEP_BOUNCE_LONG": "🟢 Отскок от поддержки после sweep", "LEVEL_RESISTANCE_REJECT_SHORT": "🔴 Отбой от сопротивления после sweep + second confirm", "LEVEL_BREAK_RETEST_LONG": "📈 Пробой сопротивления + ретест/fresh breakout", "LEVEL_BREAK_RETEST_SHORT": "📉 Пробой поддержки + ретест/fresh breakdown", "TREND_PULLBACK_PRO": "📌 Trend Pullback Pro", "VWAP_RECLAIM_PRO": "🧭 VWAP Reclaim/Loss Pro", "IMPULSE_PULLBACK_PRO": "⚡ Impulse Pullback Pro"}
    pos = signal["position"]
    risk_text = f"⚠️ Ошибка RM: {pos['error']}" if pos.get("error") else f"Риск: {signal['risk_percent']:.3f}% депозита\nРазмер позиции: {pos['position_size_usdt']} USDT\nМаржа x{LEVERAGE}: {pos.get('margin_usdt')} USDT"
    f = signal.get("filters", {})
    funding_text = f.get("funding", {}).get("reason", "Funding/OI: нет данных")
    notes = [f.get(k) for k in ["htf_note", "level_strength_note", "anti_fakeout_note", "countertrend_note", "space_note"] if f.get(k)]
    caution = "\n⚠️ B-сигнал: вход осторожнее, риск уменьшен." if signal["grade"] == "B" else ""
    space = signal.get("space_to_target_percent")
    space_line = f"\n<b>Место до ближайшего уровня:</b> {space}%" if space is not None else ""
    return f"""
🎯 <b>{mode} {'A+ SIGNAL' if signal['grade']=='A+' else 'B SIGNAL'}</b>

{arrow} <b>{signal['direction']} {signal['display_symbol']}</b>
<b>Стратегия:</b> {names.get(signal['strategy'], signal['strategy'])}

<b>Вход:</b> <code>{signal['entry']}</code>
<b>Stop Loss:</b> <code>{signal['sl']}</code>

<b>Take Profit:</b>
TP1: <code>{signal['tp1']}</code>
TP2: <code>{signal['tp2']}</code>
TP3: <code>{signal['tp3']}</code>

<b>Почему вход:</b>
{signal['reason']}

<b>Фильтры:</b>
BTC: {f.get('btc_status','NEUTRAL')}
{funding_text}
{chr(10).join(notes)}

<b>Качество:</b> {signal['score']}/100
<b>RR:</b> {signal['rr']} ({signal.get('rr_basis','TP2 net')})
<b>TP1 gross/net:</b> {signal.get('raw_reward_to_tp1_percent',0)}% / {signal.get('net_reward_to_tp1_percent',0)}%
<b>TP2 gross/net:</b> {signal.get('raw_reward_to_tp2_percent',0)}% / {signal.get('net_reward_to_tp2_percent',0)}%
<b>Издержки:</b> ~{signal.get('estimated_trade_cost_percent',0)}%
<b>Объём:</b> x{signal['volume_ratio']}
<b>Риск до SL:</b> {signal['risk_position_percent']}% по позиции{space_line}

{risk_text}
{caution}

<b>После TP1:</b>
Закрыть примерно {TP1_CLOSE_PERCENT:.0f}% позиции и перенести SL в безубыток.

⚠️ Не финансовый совет.
""".strip()

def save_signal(signal: dict) -> None:
    STATE["active_signals"][signal["id"]] = signal
    STATE["sent_signals"][signal["id"]] = now_ts()
    set_cooldown(signal["symbol"])
    save_state(STATE)

# ---------- tracking ----------
def check_signal_hit(signal: dict, candles: List[dict]):
    side = signal["direction"]
    last_checked = signal.get("last_checked_time", 0)
    new = [c for c in candles if c["time"] > last_checked]
    if not new:
        return None, candles[-1]["close"]
    for c in new:
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
                signal["tp1_hit"] = True; return "TP1", signal["tp1"]
            if signal.get("tp1_hit") and not signal.get("tp2_hit") and high >= signal["tp2"]:
                signal["tp2_hit"] = True; return "TP2", signal["tp2"]
            if signal.get("tp2_hit") and not signal.get("tp3_hit") and high >= signal["tp3"]:
                signal["tp3_hit"] = True; return "TP3", signal["tp3"]
        else:
            if signal.get("tp2_hit") and high >= signal["entry"]:
                return "PROFIT_AFTER_TP2", signal["entry"]
            if signal.get("tp1_hit") and high >= signal["entry"]:
                return "PROFIT_AFTER_TP1", signal["entry"]
            if not signal.get("tp1_hit") and high >= signal["sl"]:
                return "SL", signal["sl"]
            if not signal.get("tp1_hit") and low <= signal["tp1"]:
                signal["tp1_hit"] = True; return "TP1", signal["tp1"]
            if signal.get("tp1_hit") and not signal.get("tp2_hit") and low <= signal["tp2"]:
                signal["tp2_hit"] = True; return "TP2", signal["tp2"]
            if signal.get("tp2_hit") and not signal.get("tp3_hit") and low <= signal["tp3"]:
                signal["tp3_hit"] = True; return "TP3", signal["tp3"]
    return None, new[-1]["close"]

def apply_result(signal: dict, result: str) -> List[str]:
    ensure_stats_structure()
    side, strategy, grade, symbol = signal["direction"], signal["strategy"], signal.get("grade", "A+"), normalize_symbol(signal["symbol"])
    ss, ssg = f"{strategy}:{side}", f"{strategy}:{side}:{grade}"
    notes, r_mult = [], 0.0
    if result == "SL" and not signal.get("counted_sl"):
        signal["counted_sl"] = True
        for container, key in [(STATE["stats"]["side"], side), (STATE["stats"]["strategy"], strategy), (STATE["stats"]["strategy_side"], ss), (STATE["stats"]["strategy_side_grade"], ssg)]:
            container[key]["sl"] += 1
            if "consecutive_sl" in container[key]:
                container[key]["consecutive_sl"] += 1
        STATE["stats"]["side"][side]["consecutive_sl"] += 0
        STATE["stats"]["grade"][grade]["sl"] += 1
        STATE["stats"]["pair_sl"][symbol] = STATE["stats"]["pair_sl"].get(symbol, 0) + 1
        r_mult = -1.0
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
                notes.append(f"⛔ A+ {strategy} {side} дал серию SL — связка временно отключена.")
    elif result in ["TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        for container, key in [(STATE["stats"]["side"], side), (STATE["stats"]["strategy"], strategy), (STATE["stats"]["strategy_side"], ss), (STATE["stats"]["strategy_side_grade"], ssg)]:
            if "consecutive_sl" in container[key]:
                container[key]["consecutive_sl"] = 0
        if not signal.get("counted_positive"):
            signal["counted_positive"] = True
            STATE["stats"]["side"][side]["positive"] += 1
            STATE["stats"]["strategy"][strategy]["positive"] += 1
            STATE["stats"]["strategy_side"][ss]["positive"] += 1
            STATE["stats"]["strategy_side_grade"][ssg]["positive"] += 1
            STATE["stats"]["grade"][grade]["positive"] += 1
            STATE["stats"]["pair_positive"][symbol] = STATE["stats"]["pair_positive"].get(symbol, 0) + 1
        if result in ["TP1", "PROFIT_AFTER_TP1"]:
            r_mult = 0.35
        elif result in ["TP2", "PROFIT_AFTER_TP2"]:
            r_mult = 0.75
        elif result == "TP3":
            r_mult = 1.20
        if result == "TP1" and not signal.get("counted_tp1"):
            signal["counted_tp1"] = True; STATE["stats"]["side"][side]["tp1"] += 1
        if result == "TP2" and not signal.get("counted_tp2"):
            signal["counted_tp2"] = True; STATE["stats"]["side"][side]["tp2"] += 1
        if result == "TP3" and not signal.get("counted_tp3"):
            signal["counted_tp3"] = True; STATE["stats"]["side"][side]["tp3"] += 1
    if result in ["SL", "TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        STATE["stats"]["closed_trades"].append({"time": int(now_ts()), "symbol": symbol, "strategy": strategy, "side": side, "grade": grade, "result": result, "r_multiple": round(r_mult, 3)})
        STATE["stats"]["closed_trades"] = STATE["stats"]["closed_trades"][-500:]
    save_state(STATE)
    return notes

def build_result_message(signal: dict, result: str, price: Optional[float], notes: List[str]) -> str:
    title_map = {"SL": "❌ Stop Loss", "TP1": "✅ TP1 достигнут", "TP2": "✅ TP2 достигнут", "TP3": "🔥 TP3 достигнут", "PROFIT_AFTER_TP1": "🟢 Возврат после TP1", "PROFIT_AFTER_TP2": "🟢 Возврат после TP2", "EXPIRED": "⌛ Сигнал устарел"}
    status_map = {"SL": "SL сработал до TP1. Сделка отрицательная.", "TP1": f"Сделка позитивная. Закрыть ~{TP1_CLOSE_PERCENT:.0f}% позиции и SL в безубыток.", "TP2": "Хорошее движение. Сделка позитивная.", "TP3": "Отличная сделка. Полная цель достигнута.", "PROFIT_AFTER_TP1": "Цена вернулась после TP1, но сделка уже позитивная.", "PROFIT_AFTER_TP2": "Цена вернулась после TP2, сделка позитивная.", "EXPIRED": "Сигнал не достиг TP/SL за установленное время и удалён из активных."}
    adaptive = "\n\n<b>Адаптация бота:</b>\n" + "\n".join(notes) if notes else ""
    return f"""
{title_map.get(result, result)}

<b>{signal.get('grade','A+')} · {signal['direction']} {signal['display_symbol']}</b>
<b>Стратегия:</b> {signal.get('strategy','')}

Вход: <code>{signal['entry']}</code>
Текущая цена: <code>{'n/a' if price is None else round(price,8)}</code>

TP1: <code>{signal['tp1']}</code>
TP2: <code>{signal['tp2']}</code>
TP3: <code>{signal['tp3']}</code>
SL: <code>{signal['sl']}</code>

{status_map.get(result, 'Обновление по сделке.')}

{build_stats_text()}
{adaptive}
""".strip()

def is_signal_expired(signal: dict) -> bool:
    return bool(signal.get("created_at") and now_ts() - signal.get("created_at") > SIGNAL_MAX_LIFETIME_SECONDS)

def track_active_signals(send_to_telegram: bool = True) -> Dict[str, Any]:
    cleanup_state()
    if not STATE.get("active_signals"):
        return {"ok": True, "message": "Активных сигналов нет.", "results": [], "active_left": 0}
    results, finished = [], []
    for sid, sig in list(STATE["active_signals"].items()):
        if is_signal_expired(sig):
            msg = build_result_message(sig, "EXPIRED", None, [])
            telegram = send_telegram_message(msg) if send_to_telegram else None
            results.append({"signal_id": sid, "symbol": sig.get("display_symbol"), "result": "EXPIRED", "telegram": telegram})
            finished.append(sid)
            continue
        candles = remove_unclosed_candle(get_klines(sig["symbol"], "1m", 120), "1m")
        if not candles:
            continue
        result, price = check_signal_hit(sig, candles)
        STATE["active_signals"][sid] = sig
        if not result:
            continue
        notes = apply_result(sig, result)
        msg = build_result_message(sig, result, price, notes)
        telegram = send_telegram_message(msg) if send_to_telegram else None
        results.append({"signal_id": sid, "symbol": sig.get("display_symbol"), "grade": sig.get("grade"), "direction": sig.get("direction"), "strategy": sig.get("strategy"), "result": result, "price": price, "telegram": telegram})
        if result in ["SL", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
            finished.append(sid)
    for sid in finished:
        STATE["active_signals"].pop(sid, None)
    save_state(STATE)
    return {"ok": True, "checked": len(STATE["active_signals"]) + len(finished), "results": results, "active_left": len(STATE["active_signals"])}

# ---------- worker / routes ----------
async def auto_worker():
    await asyncio.sleep(8)
    while True:
        try:
            current = now_ts()
            if AUTO_TRACK_ENABLED and current - STATE["auto"].get("last_track_time", 0) >= AUTO_TRACK_SECONDS:
                res = track_active_signals(send_to_telegram=True)
                STATE["auto"]["last_track_time"] = current
                STATE["auto"]["last_track_result"] = res
                save_state(STATE)
            if AUTO_SCAN_ENABLED and current - STATE["auto"].get("last_scan_time", 0) >= AUTO_SCAN_SECONDS:
                res = scan_best_signal(DEFAULT_DEPOSIT, DEFAULT_RISK_PERCENT)
                STATE["auto"]["last_scan_time"] = current
                STATE["auto"]["last_scan_result"] = res
                if res.get("ok"):
                    telegram = send_telegram_message(res["message"])
                    res["telegram"] = telegram
                    if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
                        save_signal(res["signal"])
                    else:
                        STATE["auto"]["last_error"] = f"Telegram не отправил сигнал: {telegram}"
                else:
                    last_report = STATE["auto"].get("last_no_signal_report_time", 0)
                    if DEBUG_NO_SIGNAL_REPORT_ENABLED and current - last_report >= DEBUG_NO_SIGNAL_REPORT_SECONDS:
                        send_telegram_message(f"🧠 <b>Диагностика {DEPLOY_MARKER}</b>\n\nBTC regime: {res.get('btc_status','NEUTRAL')}\nПроверено пар: {res.get('checked',0)}\nКандидатов найдено: {res.get('found_candidates',0)}\nСигнала пока нет. V6.1 держит баланс: B живой, SHORT жёстче, anti-chase ON.")
                        STATE["auto"]["last_no_signal_report_time"] = current
                save_state(STATE)
            await asyncio.sleep(15)
        except Exception as e:
            STATE["auto"]["last_error"] = str(e)
            save_state(STATE)
            await asyncio.sleep(30)

def is_authorized(api_key: Optional[str]) -> bool:
    return True if not API_KEY else api_key == API_KEY

def unauthorized_response():
    return {"ok": False, "error": "Unauthorized. Provide valid api_key."}

@app.on_event("startup")
async def startup_event():
    text = (
        f"✅ {APP_NAME} запущен.\n\n"
        f"Deploy marker: {DEPLOY_MARKER}\n"
        f"Режим: {'TEST' if TEST_MODE else 'TRADE'}\n"
        f"Auto Scan: {'ON' if AUTO_SCAN_ENABLED else 'OFF'} / {AUTO_SCAN_SECONDS} сек.\n"
        f"Auto Track: {'ON' if AUTO_TRACK_ENABLED else 'OFF'} / {AUTO_TRACK_SECONDS} сек.\n"
        f"Closed candles only: {'ON' if USE_CLOSED_CANDLES_ONLY else 'OFF'}\n"
        f"A+ score/RR/volume: {A_PLUS_MIN_SCORE}+ / {A_PLUS_MIN_RR} / x{A_PLUS_MIN_VOLUME_RATIO}\n"
        f"B score/RR/volume: {B_MIN_SCORE}+ / {B_MIN_RR} / x{B_MIN_VOLUME_RATIO}\n"
        f"Level B score/RR/volume: {LEVEL_B_MIN_SCORE}+ / {LEVEL_B_MIN_RR} / x{LEVEL_B_MIN_VOLUME_RATIO}\n"
        f"B risk: x{B_RISK_MULTIPLIER}\n"
        f"Impulse Pullback: {'ON' if IMPULSE_PULLBACK_ENABLED else 'OFF'} / risk x{IMPULSE_PULLBACK_RISK_MULTIPLIER}\n"
        f"Anti-chase: {'ON' if ENABLE_ANTI_CHASE_FILTER else 'OFF'} / max {MAX_CHASE_MOVE_5M_PERCENT}% / extreme {EXTREME_CHASE_MOVE_5M_PERCENT}%\n"
        f"HTF confirm: {'ON' if HTF_CONFIRMATION_ENABLED else 'OFF'} | A+ 1H+4H: {'ON' if A_PLUS_REQUIRES_1H_4H_CONFIRM else 'OFF'} | B any HTF: {'ON' if B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM else 'OFF'}\n"
        f"V6.1 Pro Guard: {'ON' if PRO_BALANCED_GUARD_ENABLED else 'OFF'} | SHORT B: {'ON' if SHORT_B_ENABLED else 'OFF'} / {SHORT_B_MIN_SCORE}+ / RR {SHORT_B_MIN_RR} / vol x{SHORT_B_MIN_VOLUME_RATIO}\n"
        f"API key protection: {'ON' if bool(API_KEY) else 'OFF'}\n\n"
        "V6.1 цель: профессиональный баланс — сигналы остаются, но слабые B, догоняющие входы и опасные SHORT фильтруются жёстче."
    )
    send_telegram_message(text)
    asyncio.create_task(auto_worker())

@app.get("/", response_class=HTMLResponse)
def home():
    return f"""<!DOCTYPE html><html><head><title>{APP_NAME}</title></head><body style='background:#020617;color:#e5e7eb;font-family:Arial;padding:40px;'><h1>✅ {APP_NAME} работает</h1><pre>GET /health\nGET /version\nGET /scan?send_to_telegram=false\nGET /auto-signal?symbol=NEAR/USDT\nGET /track\nGET /stats\nGET /auto-status\nGET /test-telegram\nGET /reset-state?api_key=...</pre></body></html>"""

@app.get("/health")
def health():
    return {"status": "ok", "service": APP_NAME, "deploy_marker": DEPLOY_MARKER, "test_mode": TEST_MODE, "telegram_token_set": bool(TELEGRAM_BOT_TOKEN), "telegram_chat_id_set": bool(TELEGRAM_CHAT_ID), "a_plus_min_score": A_PLUS_MIN_SCORE, "b_min_score": B_MIN_SCORE, "b_min_rr": B_MIN_RR, "level_b_min_rr": LEVEL_B_MIN_RR, "short_b_enabled": SHORT_B_ENABLED, "short_b_min_score": SHORT_B_MIN_SCORE, "short_b_min_rr": SHORT_B_MIN_RR, "anti_chase_enabled": ENABLE_ANTI_CHASE_FILTER, "htf_confirmation_enabled": HTF_CONFIRMATION_ENABLED, "active_signals": len(STATE.get("active_signals", {}))}

@app.get("/version")
def version():
    return {"ok": True, "service": APP_NAME, "deploy_marker": DEPLOY_MARKER, "start_command_recommended": "python bot.py", "telegram_token_set": bool(TELEGRAM_BOT_TOKEN), "telegram_chat_id_set": bool(TELEGRAM_CHAT_ID)}

@app.get("/auto-status")
def auto_status():
    return {"ok": True, "auto": STATE.get("auto", {}), "active_signals": len(STATE.get("active_signals", {})), "blocked_symbols": len(STATE.get("blocked_symbols", {}))}

@app.get("/test-telegram")
def test_telegram():
    return send_telegram_message(f"✅ {APP_NAME} подключён к Telegram. Deploy marker: {DEPLOY_MARKER}")

@app.get("/auto-signal")
def auto_signal(symbol: str = Query(default="NEAR/USDT"), direction: Optional[str] = Query(default=None), deposit: float = Query(default=DEFAULT_DEPOSIT), risk_percent: float = Query(default=DEFAULT_RISK_PERCENT), send_to_telegram: bool = Query(default=False), api_key: Optional[str] = Query(default=None)):
    if send_to_telegram and not is_authorized(api_key):
        return unauthorized_response()
    sig = analyze_symbol(symbol, direction, deposit, risk_percent)
    if not sig:
        return {"ok": False, "symbol": display_symbol(symbol), "direction": direction, "message": "Сильного сигнала нет. Вход запрещён V6.1 фильтрами."}
    msg = build_message(sig)
    telegram = None
    if send_to_telegram:
        telegram = send_telegram_message(msg)
        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
            save_signal(sig)
    return {"ok": True, "signal": sig, "message": msg, "telegram": telegram}

@app.get("/scan")
def scan(send_to_telegram: bool = Query(default=False), deposit: float = Query(default=DEFAULT_DEPOSIT), risk_percent: float = Query(default=DEFAULT_RISK_PERCENT), api_key: Optional[str] = Query(default=None)):
    if send_to_telegram and not is_authorized(api_key):
        return unauthorized_response()
    res = scan_best_signal(deposit, risk_percent)
    if not res.get("ok"):
        return res
    telegram = None
    if send_to_telegram:
        telegram = send_telegram_message(res["message"])
        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
            save_signal(res["signal"])
    res["telegram"] = telegram
    return res

@app.get("/track")
def track(send_to_telegram: bool = Query(default=True), api_key: Optional[str] = Query(default=None)):
    if send_to_telegram and not is_authorized(api_key):
        return unauthorized_response()
    return track_active_signals(send_to_telegram=send_to_telegram)

@app.get("/stats")
def stats():
    ensure_stats_structure()
    return {"ok": True, "stats": STATE["stats"], "stats_text": build_stats_text(), "active_signals": len(STATE.get("active_signals", {})), "blocked_symbols": {display_symbol(k): int(v - now_ts()) for k, v in STATE.get("blocked_symbols", {}).items() if v > now_ts()}, "strategy_side_grade_disabled_until": {k: int(v - now_ts()) for k, v in STATE.get("strategy_side_grade_disabled_until", {}).items() if v > now_ts()}}

@app.get("/cleanup-state")
def cleanup_state_endpoint(api_key: Optional[str] = Query(default=None)):
    if not is_authorized(api_key):
        return unauthorized_response()
    cleanup_state()
    return {"ok": True, "message": "State cleanup completed."}

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
    port_raw = os.getenv("PORT", "10000")
    try:
        port = int(port_raw) if str(port_raw).strip() else 10000
    except Exception:
        port = 10000
    uvicorn.run("bot:app", host="0.0.0.0", port=port)
