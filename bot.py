import os
import time
import json
import random
import asyncio
import requests
from typing import Optional, List, Dict, Any, Tuple

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

APP_NAME = "Professional Adaptive Futures Bot AUTO V7.2 PROFESSIONAL TRADE MANAGEMENT"
DEPLOY_MARKER = "V7_2_PRO_TRADE_MANAGEMENT_2026_06_10"
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
DEBUG_NO_SIGNAL_REPORT_ENABLED = os.getenv("DEBUG_NO_SIGNAL_REPORT_ENABLED", "true").lower() == "true"
DEBUG_NO_SIGNAL_REPORT_SECONDS = int(os.getenv("DEBUG_NO_SIGNAL_REPORT_SECONDS", "7200"))
USE_CLOSED_CANDLES_ONLY = os.getenv("USE_CLOSED_CANDLES_ONLY", "true").lower() == "true"

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "1200"))
PAIR_BLOCK_SECONDS = int(os.getenv("PAIR_BLOCK_SECONDS", "21600"))
STRATEGY_DISABLE_SECONDS = int(os.getenv("STRATEGY_DISABLE_SECONDS", "7200"))
PAIR_MAX_SL = int(os.getenv("PAIR_MAX_SL", "3"))
STRATEGY_SIDE_GRADE_MAX_SL = int(os.getenv("STRATEGY_SIDE_GRADE_MAX_SL", "3"))
SIGNAL_MAX_LIFETIME_SECONDS = int(os.getenv("SIGNAL_MAX_LIFETIME_SECONDS", "21600"))
SENT_SIGNALS_KEEP_SECONDS = int(os.getenv("SENT_SIGNALS_KEEP_SECONDS", "604800"))
SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK = os.getenv("SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK", "true").lower() == "true"

# V7: не даём старым Render ENV случайно вернуть слишком мягкие параметры.
ALLOW_ENV_STRATEGY_OVERRIDES = os.getenv("ALLOW_ENV_STRATEGY_OVERRIDES", "false").lower() == "true"

def strategy_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default))) if ALLOW_ENV_STRATEGY_OVERRIDES else default

def strategy_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default))) if ALLOW_ENV_STRATEGY_OVERRIDES else default

# Классификация: A+ редкий, B рабочий, но не мусорный.
A_PLUS_MIN_SCORE = strategy_int("A_PLUS_MIN_SCORE", 88)
A_PLUS_MIN_RR = strategy_float("A_PLUS_MIN_RR", 1.05)
A_PLUS_MIN_VOLUME_RATIO = strategy_float("A_PLUS_MIN_VOLUME_RATIO", 1.18)
A_PLUS_RISK_MULTIPLIER = strategy_float("A_PLUS_RISK_MULTIPLIER", 1.0)

B_MIN_SCORE = strategy_int("B_MIN_SCORE", 77)
B_MIN_RR = strategy_float("B_MIN_RR", 0.72)
B_MIN_VOLUME_RATIO = strategy_float("B_MIN_VOLUME_RATIO", 1.00)
B_RISK_MULTIPLIER = strategy_float("B_RISK_MULTIPLIER", 0.24)

LEVEL_B_MIN_SCORE = strategy_int("LEVEL_B_MIN_SCORE", 75)
LEVEL_B_MIN_RR = strategy_float("LEVEL_B_MIN_RR", 0.66)
LEVEL_B_MIN_VOLUME_RATIO = strategy_float("LEVEL_B_MIN_VOLUME_RATIO", 0.98)

# SHORT отдельно: по статистике он чаще ломал результат, поэтому B-short только сильный.
SHORT_B_ENABLED = os.getenv("SHORT_B_ENABLED", "true").lower() == "true"
SHORT_B_MIN_SCORE = strategy_int("SHORT_B_MIN_SCORE", 83)
SHORT_B_MIN_RR = strategy_float("SHORT_B_MIN_RR", 0.82)
SHORT_B_MIN_VOLUME_RATIO = strategy_float("SHORT_B_MIN_VOLUME_RATIO", 1.08)
SHORT_A_PLUS_REQUIRES_BTC_NOT_BULLISH = os.getenv("SHORT_A_PLUS_REQUIRES_BTC_NOT_BULLISH", "true").lower() == "true"

# V7.1 Adaptive Trust Engine: бот больше не доверяет стратегии только из-за красивого score.
# Он смотрит фактическую статистику strategy + side + grade и сам понижает/блокирует слабые связки.
ADAPTIVE_TRUST_ENABLED = os.getenv("ADAPTIVE_TRUST_ENABLED", "true").lower() == "true"
TRUST_MIN_TRADES_FOR_GRADE = int(os.getenv("TRUST_MIN_TRADES_FOR_GRADE", "6"))
TRUST_MIN_TRADES_FOR_SIDE = int(os.getenv("TRUST_MIN_TRADES_FOR_SIDE", "8"))
TRUST_A_PLUS_MIN_SIDE_WR = float(os.getenv("TRUST_A_PLUS_MIN_SIDE_WR", "52"))
TRUST_B_MIN_GRADE_WR = float(os.getenv("TRUST_B_MIN_GRADE_WR", "48"))
TRUST_SIDE_BLOCK_WR = float(os.getenv("TRUST_SIDE_BLOCK_WR", "42"))
TRUST_STRATEGY_OFF_WR = float(os.getenv("TRUST_STRATEGY_OFF_WR", "40"))
TRUST_LOW_WR_DISABLE_SECONDS = int(os.getenv("TRUST_LOW_WR_DISABLE_SECONDS", "10800"))
TRUST_B_GLOBAL_WR_MIN = float(os.getenv("TRUST_B_GLOBAL_WR_MIN", "48"))
TRUST_B_SCORE_ADD_IF_WEAK = int(os.getenv("TRUST_B_SCORE_ADD_IF_WEAK", "4"))
TRUST_B_RR_ADD_IF_WEAK = float(os.getenv("TRUST_B_RR_ADD_IF_WEAK", "0.08"))
TRUST_B_VOL_ADD_IF_WEAK = float(os.getenv("TRUST_B_VOL_ADD_IF_WEAK", "0.04"))
TRUST_BAD_LONG_B_OFF = os.getenv("TRUST_BAD_LONG_B_OFF", "true").lower() == "true"
TRUST_BREAK_RETEST_SHORT_A_PLUS_MIN_WR = float(os.getenv("TRUST_BREAK_RETEST_SHORT_A_PLUS_MIN_WR", "52"))
TRUST_BREAK_RETEST_LONG_MIN_WR = float(os.getenv("TRUST_BREAK_RETEST_LONG_MIN_WR", "45"))

# Market regime first.
MARKET_REGIME_ENABLED = os.getenv("MARKET_REGIME_ENABLED", "true").lower() == "true"
NO_TRADE_IN_CHOP = os.getenv("NO_TRADE_IN_CHOP", "true").lower() == "true"
ALLOW_RANGE_EDGE_TRADES = os.getenv("ALLOW_RANGE_EDGE_TRADES", "true").lower() == "true"
ALLOW_COUNTERTREND_B_IN_RANGE_ONLY = os.getenv("ALLOW_COUNTERTREND_B_IN_RANGE_ONLY", "true").lower() == "true"
MIN_ADX_TREND = strategy_float("MIN_ADX_TREND", 18.0)
MAX_CHOP_ATR_PERCENT = strategy_float("MAX_CHOP_ATR_PERCENT", 0.35)

# Risk model.
MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "12"))
TP1_R_MULTIPLIER = float(os.getenv("TP1_R_MULTIPLIER", "0.75"))
TP2_R_MULTIPLIER = float(os.getenv("TP2_R_MULTIPLIER", "1.25"))
TP3_R_MULTIPLIER = float(os.getenv("TP3_R_MULTIPLIER", "1.90"))
MIN_TP1_PRICE_MOVE_PERCENT = float(os.getenv("MIN_TP1_PRICE_MOVE_PERCENT", "0.45"))
TP1_CLOSE_PERCENT = float(os.getenv("TP1_CLOSE_PERCENT", "50"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.0005"))
SLIPPAGE_RATE = float(os.getenv("SLIPPAGE_RATE", "0.0003"))

# Anti-chase.
ENABLE_ANTI_CHASE_FILTER = os.getenv("ENABLE_ANTI_CHASE_FILTER", "true").lower() == "true"
CHASE_LOOKBACK_CANDLES_5M = int(os.getenv("CHASE_LOOKBACK_CANDLES_5M", "18"))
MAX_CHASE_MOVE_5M_PERCENT = float(os.getenv("MAX_CHASE_MOVE_5M_PERCENT", "4.6"))
EXTREME_CHASE_MOVE_5M_PERCENT = float(os.getenv("EXTREME_CHASE_MOVE_5M_PERCENT", "8.0"))
MIN_PULLBACK_AFTER_CHASE_PERCENT = float(os.getenv("MIN_PULLBACK_AFTER_CHASE_PERCENT", "0.55"))
MIN_PULLBACK_AFTER_EXTREME_PERCENT = float(os.getenv("MIN_PULLBACK_AFTER_EXTREME_PERCENT", "1.20"))
MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT = float(os.getenv("MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT", "2.6"))
MAX_DISTANCE_FROM_VWAP_PERCENT = float(os.getenv("MAX_DISTANCE_FROM_VWAP_PERCENT", "4.8"))

# Space/funding.
ENABLE_SPACE_TO_TARGET_FILTER = os.getenv("ENABLE_SPACE_TO_TARGET_FILTER", "true").lower() == "true"
MIN_SPACE_TO_TARGET_PERCENT_A_PLUS = float(os.getenv("MIN_SPACE_TO_TARGET_PERCENT_A_PLUS", "0.45"))
MIN_SPACE_TO_TARGET_PERCENT_B = float(os.getenv("MIN_SPACE_TO_TARGET_PERCENT_B", "0.25"))
ENABLE_FUNDING_FILTER = os.getenv("ENABLE_FUNDING_FILTER", "true").lower() == "true"
ENABLE_OI_FILTER = os.getenv("ENABLE_OI_FILTER", "true").lower() == "true"
MAX_ABS_FUNDING_RATE = float(os.getenv("MAX_ABS_FUNDING_RATE", "0.0012"))
FUNDING_EXTREME_RATE = float(os.getenv("FUNDING_EXTREME_RATE", "0.0025"))

# Impulse Pullback: только B, малый риск, только по режиму.
IMPULSE_PULLBACK_ENABLED = os.getenv("IMPULSE_PULLBACK_ENABLED", "true").lower() == "true"
IMPULSE_PULLBACK_RISK_MULTIPLIER = float(os.getenv("IMPULSE_PULLBACK_RISK_MULTIPLIER", "0.20"))
IMPULSE_MIN_MOVE_5M_PERCENT = float(os.getenv("IMPULSE_MIN_MOVE_5M_PERCENT", "0.80"))
IMPULSE_PULLBACK_MIN_PERCENT = float(os.getenv("IMPULSE_PULLBACK_MIN_PERCENT", "0.18"))
IMPULSE_PULLBACK_MAX_PERCENT = float(os.getenv("IMPULSE_PULLBACK_MAX_PERCENT", "3.20"))
IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT = float(os.getenv("IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT", "3.00"))
IMPULSE_MIN_VOLUME_RATIO = float(os.getenv("IMPULSE_MIN_VOLUME_RATIO", "0.95"))

# V7.2 Professional Trade Management: не только вход, но и ведение сделки.
# Цель: не выбивать A+ короткой шпилькой, особенно когда BTC штормит.
PRO_TRADE_MANAGEMENT_ENABLED = os.getenv("PRO_TRADE_MANAGEMENT_ENABLED", "true").lower() == "true"
BTC_STORM_FILTER_ENABLED = os.getenv("BTC_STORM_FILTER_ENABLED", "true").lower() == "true"
BTC_STORM_LOOKBACK_1M = int(os.getenv("BTC_STORM_LOOKBACK_1M", "10"))
BTC_STORM_MOVE_1M_PERCENT = float(os.getenv("BTC_STORM_MOVE_1M_PERCENT", "0.45"))
BTC_STORM_LOOKBACK_5M = int(os.getenv("BTC_STORM_LOOKBACK_5M", "4"))
BTC_STORM_MOVE_5M_PERCENT = float(os.getenv("BTC_STORM_MOVE_5M_PERCENT", "0.75"))
BLOCK_B_DURING_BTC_STORM = os.getenv("BLOCK_B_DURING_BTC_STORM", "true").lower() == "true"
BTC_STORM_A_PLUS_RISK_MULTIPLIER = float(os.getenv("BTC_STORM_A_PLUS_RISK_MULTIPLIER", "0.35"))

# A+ = structural trade: даём цене немного воздуха, если стоп слишком короткий.
A_PLUS_STRUCTURAL_SL_ENABLED = os.getenv("A_PLUS_STRUCTURAL_SL_ENABLED", "true").lower() == "true"
A_PLUS_MIN_STOP_PRICE_MOVE_PERCENT = float(os.getenv("A_PLUS_MIN_STOP_PRICE_MOVE_PERCENT", "0.34"))
A_PLUS_EXTRA_ATR_BUFFER = float(os.getenv("A_PLUS_EXTRA_ATR_BUFFER", "0.18"))

# Soft Stop: первые минуты A+ не закрывается по одной шпильке, нужен close за SL.
A_PLUS_SOFT_STOP_ENABLED = os.getenv("A_PLUS_SOFT_STOP_ENABLED", "true").lower() == "true"
A_PLUS_SOFT_STOP_SECONDS = int(os.getenv("A_PLUS_SOFT_STOP_SECONDS", "300"))

# После TP1 не ставим защиту ровно в entry, даём маленький buffer от шума.
POST_TP1_BUFFER_ENABLED = os.getenv("POST_TP1_BUFFER_ENABLED", "true").lower() == "true"
POST_TP1_BE_BUFFER_PERCENT = float(os.getenv("POST_TP1_BE_BUFFER_PERCENT", "0.08"))

STRATEGIES = [
    "SWEEP_RECLAIM_LONG",
    "SWEEP_REJECT_SHORT",
    "BREAK_RETEST_LONG",
    "BREAK_RETEST_SHORT",
    "TREND_PULLBACK_LONG",
    "TREND_PULLBACK_SHORT",
    "IMPULSE_PULLBACK_PRO",
]

LIQUID_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "INJ", "NEAR", "ARB", "OP", "APT", "SUI", "SEI", "DOT", "LTC", "BCH", "UNI", "AAVE", "FIL", "ATOM", "ETC", "TRX", "MATIC", "POL", "WLD", "TIA", "ORDI", "FTM", "RUNE", "ENA", "JUP", "PYTH", "STRK", "DYDX", "TON", "COMP", "STX", "TRB", "JTO", "DYM", "ICP", "GALA", "FET", "RNDR", "RENDER", "IMX", "APE", "AR", "MKR", "SNX", "LDO", "CRV", "GMT", "PEPE", "1000PEPE", "WIF", "BONK", "NOT", "ONDO", "BLUR", "MEME", "AI", "ACE", "ARKM", "PENDLE", "BIGTIME", "ZRO", "ZK", "TAO", "1000SATS", "SAGA", "MANTA", "ALT", "PIXEL", "PORTAL", "AEVO", "W", "OMNI", "TNSR", "BB", "PEOPLE", "1000SHIB",
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


def calc_winrate(positive: int, sl: int) -> float:
    total = positive + sl
    return round(positive / total * 100, 1) if total > 0 else 0.0


def strategy_stats_default() -> dict:
    return {s: {"positive": 0, "sl": 0, "consecutive_sl": 0} for s in STRATEGIES}


def strategy_side_grade_default(value=0) -> dict:
    return {f"{s}:{side}:{grade}": value for s in STRATEGIES for side in ["LONG", "SHORT"] for grade in ["A+", "B"]}


def strategy_side_stats_default() -> dict:
    return {f"{s}:{side}": {"positive": 0, "sl": 0, "consecutive_sl": 0} for s in STRATEGIES for side in ["LONG", "SHORT"]}


def strategy_side_grade_stats_default() -> dict:
    return {f"{s}:{side}:{grade}": {"positive": 0, "sl": 0, "consecutive_sl": 0} for s in STRATEGIES for side in ["LONG", "SHORT"] for grade in ["A+", "B"]}


def default_state() -> dict:
    return {
        "version_marker": DEPLOY_MARKER,
        "active_signals": {},
        "sent_signals": {},
        "symbol_cooldown": {},
        "blocked_symbols": {},
        "strategy_side_grade_disabled_until": strategy_side_grade_default(0),
        "stats": {
            "side": {
                "LONG": {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0},
                "SHORT": {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0},
            },
            "grade": {"A+": {"positive": 0, "sl": 0}, "B": {"positive": 0, "sl": 0}},
            "strategy": strategy_stats_default(),
            "strategy_side": strategy_side_stats_default(),
            "strategy_side_grade": strategy_side_grade_stats_default(),
            "pair_sl": {},
            "pair_positive": {},
            "closed_trades": [],
        },
        "auto": {"last_scan_time": 0, "last_track_time": 0, "last_scan_result": None, "last_track_result": None, "last_no_signal_report_time": 0, "last_error": None},
    }


def ensure_state_structure(state: dict) -> dict:
    base = default_state()
    for k, v in base.items():
        state.setdefault(k, v)
    state.setdefault("stats", base["stats"])
    for k, v in base["stats"].items():
        state["stats"].setdefault(k, v)
    for side in ["LONG", "SHORT"]:
        state["stats"]["side"].setdefault(side, {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0})
    for grade in ["A+", "B"]:
        state["stats"]["grade"].setdefault(grade, {"positive": 0, "sl": 0})
    state.setdefault("strategy_side_grade_disabled_until", {})
    state["stats"].setdefault("strategy", {})
    state["stats"].setdefault("strategy_side", {})
    state["stats"].setdefault("strategy_side_grade", {})
    state["stats"].setdefault("pair_sl", {})
    state["stats"].setdefault("pair_positive", {})
    state["stats"].setdefault("closed_trades", [])
    for s in STRATEGIES:
        state["stats"]["strategy"].setdefault(s, {"positive": 0, "sl": 0, "consecutive_sl": 0})
        for side in ["LONG", "SHORT"]:
            ss = f"{s}:{side}"
            state["stats"]["strategy_side"].setdefault(ss, {"positive": 0, "sl": 0, "consecutive_sl": 0})
            for grade in ["A+", "B"]:
                ssg = f"{s}:{side}:{grade}"
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


def calc_profit_factor_from_closed() -> float:
    trades = STATE.get("stats", {}).get("closed_trades", [])
    wins = sum(float(t.get("r_multiple", 0)) for t in trades if float(t.get("r_multiple", 0)) > 0)
    losses = abs(sum(float(t.get("r_multiple", 0)) for t in trades if float(t.get("r_multiple", 0)) < 0))
    if losses <= 0:
        return round(wins, 2) if wins > 0 else 0.0
    return round(wins / losses, 2)



def _stat_wr(bucket: dict) -> Tuple[int, int, int, float]:
    positive = int(bucket.get("positive", 0))
    sl = int(bucket.get("sl", 0))
    total = positive + sl
    return positive, sl, total, calc_winrate(positive, sl)


def get_strategy_trust(strategy: str, direction: str, grade: str) -> dict:
    """
    V7.1 trust layer.
    Trust не заменяет сетап, а решает, можно ли стратегии доверять A+/B прямо сейчас.
    Использует уже накопленную статистику из bot_state.json.
    """
    ensure_stats_structure()
    if not ADAPTIVE_TRUST_ENABLED:
        return {"status": "OFF", "blocked": False, "a_plus_allowed": True, "b_allowed": True, "score_adjustment": 0, "risk_multiplier": 1.0, "note": "Trust Engine OFF"}

    stats = STATE.get("stats", {})
    side_bucket = stats.get("side", {}).get(direction, {})
    grade_bucket = stats.get("grade", {}).get(grade, {})
    strategy_bucket = stats.get("strategy", {}).get(strategy, {})
    ss_key = f"{strategy}:{direction}"
    ssg_key = f"{strategy}:{direction}:{grade}"
    ss_bucket = stats.get("strategy_side", {}).get(ss_key, {})
    ssg_bucket = stats.get("strategy_side_grade", {}).get(ssg_key, {})

    _, _, side_total, side_wr = _stat_wr(side_bucket)
    _, _, global_grade_total, global_grade_wr = _stat_wr(grade_bucket)
    _, _, strategy_total, strategy_wr = _stat_wr(strategy_bucket)
    _, _, ss_total, ss_wr = _stat_wr(ss_bucket)
    _, _, ssg_total, ssg_wr = _stat_wr(ssg_bucket)

    blocked = False
    a_plus_allowed = True
    b_allowed = True
    score_adjustment = 0
    risk_multiplier = 1.0
    status = "NEW"
    reasons = []

    # 1) Связка strategy+side очень плохая — вообще не даём новый сигнал по ней.
    if ss_total >= TRUST_MIN_TRADES_FOR_SIDE and ss_wr < TRUST_SIDE_BLOCK_WR:
        blocked = True
        status = "BLOCKED"
        reasons.append(f"strategy+side WR {ss_wr}% after {ss_total} trades < {TRUST_SIDE_BLOCK_WR}%")

    # 2) Вся стратегия провалилась — временно не доверяем ей.
    if strategy_total >= TRUST_MIN_TRADES_FOR_SIDE and strategy_wr < TRUST_STRATEGY_OFF_WR:
        blocked = True
        status = "BLOCKED"
        reasons.append(f"strategy WR {strategy_wr}% after {strategy_total} trades < {TRUST_STRATEGY_OFF_WR}%")

    # 3) A+ запрещаем, если конкретная сторона стратегии не доказала качество.
    if grade == "A+" and ss_total >= TRUST_MIN_TRADES_FOR_SIDE and ss_wr < TRUST_A_PLUS_MIN_SIDE_WR:
        a_plus_allowed = False
        status = "A+_DENIED"
        reasons.append(f"A+ denied: {strategy} {direction} WR {ss_wr}% < {TRUST_A_PLUS_MIN_SIDE_WR}%")

    # 4) B запрещаем, если именно B-связка слабая.
    if grade == "B" and ssg_total >= TRUST_MIN_TRADES_FOR_GRADE and ssg_wr < TRUST_B_MIN_GRADE_WR:
        b_allowed = False
        status = "B_DENIED"
        reasons.append(f"B denied: {ssg_key} WR {ssg_wr}% < {TRUST_B_MIN_GRADE_WR}%")

    # 5) Если общий B слабый, повышаем требования для всех B, но не отключаем полностью.
    if grade == "B" and global_grade_total >= 20 and global_grade_wr < TRUST_B_GLOBAL_WR_MIN:
        score_adjustment -= TRUST_B_SCORE_ADD_IF_WEAK
        risk_multiplier = min(risk_multiplier, 0.75)
        status = "B_STRICT"
        reasons.append(f"Global B WR {global_grade_wr}% < {TRUST_B_GLOBAL_WR_MIN}% → B stricter")

    # 6) Если общий LONG провален — B LONG временно не берём, пока статистика не улучшится.
    if TRUST_BAD_LONG_B_OFF and direction == "LONG" and grade == "B" and side_total >= 5 and side_wr < 45:
        b_allowed = False
        status = "B_DENIED"
        reasons.append(f"B LONG denied: LONG WR {side_wr}% after {side_total} trades")

    # 7) Особые правила по текущей фактической статистике пользователя.
    if strategy == "BREAK_RETEST_SHORT" and direction == "SHORT" and grade == "A+" and ss_total >= 10 and ss_wr < TRUST_BREAK_RETEST_SHORT_A_PLUS_MIN_WR:
        a_plus_allowed = False
        status = "A+_DENIED"
        reasons.append(f"BREAK_RETEST_SHORT A+ denied: WR {ss_wr}% < {TRUST_BREAK_RETEST_SHORT_A_PLUS_MIN_WR}%")

    if strategy == "BREAK_RETEST_LONG" and direction == "LONG" and ss_total >= 4 and ss_wr < TRUST_BREAK_RETEST_LONG_MIN_WR:
        blocked = True
        status = "BLOCKED"
        reasons.append(f"BREAK_RETEST_LONG blocked: WR {ss_wr}% < {TRUST_BREAK_RETEST_LONG_MIN_WR}%")

    # 8) Хорошим связкам даём приоритет.
    if ss_total >= 10 and ss_wr >= 58:
        status = "HIGH"
        score_adjustment += 3
        reasons.append(f"Trust HIGH: {strategy} {direction} WR {ss_wr}% after {ss_total} trades")
    elif ss_total >= 6 and ss_wr >= 52:
        status = "MEDIUM"
        score_adjustment += 1
        reasons.append(f"Trust MEDIUM: {strategy} {direction} WR {ss_wr}% after {ss_total} trades")
    elif ss_total >= 6:
        status = status if status not in ["NEW"] else "LOW"
        reasons.append(f"Trust LOW: {strategy} {direction} WR {ss_wr}% after {ss_total} trades")
    else:
        reasons.append(f"Trust NEW: {strategy} {direction}, sample {ss_total} trades")

    return {
        "status": status,
        "blocked": blocked,
        "a_plus_allowed": a_plus_allowed,
        "b_allowed": b_allowed,
        "score_adjustment": score_adjustment,
        "risk_multiplier": risk_multiplier,
        "strategy_side_trades": ss_total,
        "strategy_side_wr": ss_wr,
        "strategy_side_grade_trades": ssg_total,
        "strategy_side_grade_wr": ssg_wr,
        "global_grade_wr": global_grade_wr,
        "note": "Trust: " + " | ".join(reasons[:4]),
    }


def build_stats_text() -> str:
    ensure_stats_structure()
    long_stats = STATE["stats"]["side"]["LONG"]
    short_stats = STATE["stats"]["side"]["SHORT"]
    a_stats = STATE["stats"]["grade"].get("A+", {"positive": 0, "sl": 0})
    b_stats = STATE["stats"]["grade"].get("B", {"positive": 0, "sl": 0})
    lines = []
    for s in STRATEGIES:
        st = STATE["stats"]["strategy"].get(s, {"positive": 0, "sl": 0})
        lines.append(f"{s}: {st.get('positive', 0)} позитив / {st.get('sl', 0)} SL / WR {calc_winrate(st.get('positive', 0), st.get('sl', 0))}% [ON]")
    return f"""
📊 <b>Статистика {DEPLOY_MARKER}:</b>

📈 LONG: {long_stats['positive']} позитив / {long_stats['sl']} SL / WR {calc_winrate(long_stats['positive'], long_stats['sl'])}%
📉 SHORT: {short_stats['positive']} позитив / {short_stats['sl']} SL / WR {calc_winrate(short_stats['positive'], short_stats['sl'])}%

🏆 A+: {a_stats.get('positive', 0)} позитив / {a_stats.get('sl', 0)} SL / WR {calc_winrate(a_stats.get('positive', 0), a_stats.get('sl', 0))}%
⚠️ B: {b_stats.get('positive', 0)} позитив / {b_stats.get('sl', 0)} SL / WR {calc_winrate(b_stats.get('positive', 0), b_stats.get('sl', 0))}%

📐 Profit Factor по закрытым R-сделкам: {calc_profit_factor_from_closed()}

🧠 <b>Стратегии:</b>
{chr(10).join(lines)}
""".strip()


def get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception:
        return None


def interval_to_ms(interval: str) -> int:
    return {"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000, "4h": 14400000}.get(interval, 60000)


def remove_unclosed_candle(candles: Optional[List[dict]], interval: str) -> Optional[List[dict]]:
    if not candles or not USE_CLOSED_CANDLES_ONLY or len(candles) < 3:
        return candles
    current_ms = int(time.time() * 1000)
    last_open = int(candles[-1]["time"])
    if current_ms < last_open + interval_to_ms(interval):
        return candles[:-1]
    return candles


def get_symbols() -> List[str]:
    data = get_json(f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts")
    if not data:
        return []
    symbols = []
    for item in data.get("data", []):
        symbol = item.get("symbol")
        if symbol and is_good_symbol(symbol):
            symbols.append(normalize_symbol(symbol))
    random.shuffle(symbols)
    return symbols[:MAX_SYMBOLS]


def get_klines(symbol: str, interval: str, limit: int = 260) -> Optional[List[dict]]:
    data = get_json(f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines", params={"symbol": normalize_symbol(symbol), "interval": interval, "limit": limit})
    if not data:
        return None
    raw = data.get("data", [])
    candles = []
    for c in raw:
        try:
            candles.append({"time": int(c["time"]), "open": float(c["open"]), "high": float(c["high"]), "low": float(c["low"]), "close": float(c["close"]), "volume": float(c["volume"])})
        except Exception:
            continue
    candles.sort(key=lambda x: x["time"])
    return candles if len(candles) >= 50 else None


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for price in values[1:]:
        out.append(price * k + out[-1] * (1 - k))
    return out


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
    return 100 - 100 / (1 + rs)


def atr(candles: List[dict], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / period


def adx(candles: List[dict], period: int = 14) -> Optional[float]:
    if len(candles) < period * 2 + 2:
        return None
    plus_dm, minus_dm, tr_list = [], [], []
    for i in range(1, len(candles)):
        up = candles[i]["high"] - candles[i - 1]["high"]
        down = candles[i - 1]["low"] - candles[i]["low"]
        plus_dm.append(up if up > down and up > 0 else 0)
        minus_dm.append(down if down > up and down > 0 else 0)
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))
    dxs = []
    for i in range(period, len(tr_list)):
        tr_sum = sum(tr_list[i - period:i])
        if tr_sum <= 0:
            continue
        pdi = 100 * sum(plus_dm[i - period:i]) / tr_sum
        mdi = 100 * sum(minus_dm[i - period:i]) / tr_sum
        dx = abs(pdi - mdi) / max(pdi + mdi, 1e-9) * 100
        dxs.append(dx)
    if len(dxs) < period:
        return None
    return sum(dxs[-period:]) / period


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
    e50 = ema(closes, 50)[-1]
    e200 = ema(closes, 200)[-1]
    price = closes[-1]
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
    old = candles[-lookback]["close"]
    new = candles[-1]["close"]
    return (new - old) / old * 100 if old > 0 else 0.0


def candle_close_position(candle: dict) -> float:
    rng = candle["high"] - candle["low"]
    return (candle["close"] - candle["low"]) / rng if rng > 0 else 0.5


def merge_levels(levels: List[float], threshold_percent: float = 0.38) -> List[float]:
    if not levels:
        return []
    levels = sorted(levels)
    out = []
    for level in levels:
        if not out:
            out.append(level)
        elif abs(level - out[-1]) / out[-1] * 100 <= threshold_percent:
            out[-1] = (out[-1] + level) / 2
        else:
            out.append(level)
    return out


def find_swing_support_levels(candles: List[dict], lookback: int = 140) -> List[float]:
    if len(candles) < 40:
        return []
    window = candles[-lookback:]
    levels = []
    for i in range(3, len(window) - 3):
        low = window[i]["low"]
        if low <= min(window[i - 1]["low"], window[i - 2]["low"], window[i - 3]["low"]) and low <= min(window[i + 1]["low"], window[i + 2]["low"], window[i + 3]["low"]):
            levels.append(low)
    return merge_levels(levels)


def find_swing_resistance_levels(candles: List[dict], lookback: int = 140) -> List[float]:
    if len(candles) < 40:
        return []
    window = candles[-lookback:]
    levels = []
    for i in range(3, len(window) - 3):
        high = window[i]["high"]
        if high >= max(window[i - 1]["high"], window[i - 2]["high"], window[i - 3]["high"]) and high >= max(window[i + 1]["high"], window[i + 2]["high"], window[i + 3]["high"]):
            levels.append(high)
    return merge_levels(levels)


def nearest_below(price: float, levels: List[float], max_distance_percent: float = 8.0) -> Optional[float]:
    candidates = [l for l in levels if l < price]
    if not candidates:
        return None
    level = max(candidates)
    return level if abs(price - level) / level * 100 <= max_distance_percent else None


def nearest_above(price: float, levels: List[float], max_distance_percent: float = 8.0) -> Optional[float]:
    candidates = [l for l in levels if l > price]
    if not candidates:
        return None
    level = min(candidates)
    return level if abs(level - price) / price * 100 <= max_distance_percent else None


def nearest_near(price: float, levels: List[float], max_distance_percent: float = 2.0) -> Optional[float]:
    if not levels or price <= 0:
        return None
    level = min(levels, key=lambda x: abs(x - price))
    return level if abs(level - price) / price * 100 <= max_distance_percent else None


def has_near_level(level: float, levels: List[float], max_distance_percent: float) -> bool:
    return any(abs(l - level) / level * 100 <= max_distance_percent for l in levels if level > 0)


def level_mtf_strength(level: float, c1h: List[dict], c4h: List[dict], kind: str) -> dict:
    if kind == "support":
        l1 = find_swing_support_levels(c1h, 120)
        l4 = find_swing_support_levels(c4h, 120)
    else:
        l1 = find_swing_resistance_levels(c1h, 120)
        l4 = find_swing_resistance_levels(c4h, 120)
    c1_ok = has_near_level(level, l1, 1.2)
    c4_ok = has_near_level(level, l4, 1.8)
    return {"level_1h_confirmed": c1_ok, "level_4h_confirmed": c4_ok, "level_strength_bonus": (5 if c1_ok else 0) + (3 if c4_ok else 0), "level_strength_note": f"level MTF: 1H {'OK' if c1_ok else 'NO'} / 4H {'OK' if c4_ok else 'NO'}"}


def space_to_next_level_percent(price: float, direction: str, support_levels: List[float], resistance_levels: List[float]) -> Optional[float]:
    if price <= 0:
        return None
    if direction == "LONG":
        above = [l for l in resistance_levels if l > price]
        return None if not above else (min(above) - price) / price * 100
    below = [l for l in support_levels if l < price]
    return None if not below else (price - max(below)) / price * 100


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
    for endpoint in ["/openApi/swap/v2/quote/premiumIndex", "/openApi/swap/v2/quote/fundingRate"]:
        data = get_json(f"{BINGX_BASE_URL}{endpoint}", params={"symbol": normalize_symbol(symbol)})
        if data:
            value = extract_float_from_nested(data, ["lastFundingRate", "fundingRate", "funding_rate", "rate"])
            if value is not None:
                return value
    return None


def get_open_interest(symbol: str) -> Optional[float]:
    if not ENABLE_OI_FILTER:
        return None
    for endpoint in ["/openApi/swap/v2/quote/openInterest", "/openApi/swap/v2/quote/openInterestStat"]:
        data = get_json(f"{BINGX_BASE_URL}{endpoint}", params={"symbol": normalize_symbol(symbol)})
        if data:
            value = extract_float_from_nested(data, ["openInterest", "open_interest", "sumOpenInterest", "value"])
            if value is not None:
                return value
    return None


def detect_btc_status() -> str:
    btc = remove_unclosed_candle(get_klines("BTC-USDT", "1h", 260), "1h")
    return trend_state(btc) if btc else "NEUTRAL"


BTC_STORM_CACHE = {"ts": 0.0, "data": {"storm": False, "note": "BTC storm: cache empty", "move_1m": 0.0, "move_5m": 0.0}}


def detect_btc_storm() -> dict:
    """Возвращает режим BTC_STORM, чтобы не открывать слабые сделки во время резких BTC-движений."""
    if not BTC_STORM_FILTER_ENABLED:
        return {"storm": False, "note": "BTC storm filter OFF", "move_1m": 0.0, "move_5m": 0.0}
    current = now_ts()
    if current - BTC_STORM_CACHE.get("ts", 0) < 60:
        return BTC_STORM_CACHE.get("data", {})

    c1 = remove_unclosed_candle(get_klines("BTC-USDT", "1m", max(60, BTC_STORM_LOOKBACK_1M + 5)), "1m")
    c5 = remove_unclosed_candle(get_klines("BTC-USDT", "5m", max(60, BTC_STORM_LOOKBACK_5M + 5)), "5m")

    move_1m = 0.0
    move_5m = 0.0
    if c1 and len(c1) > BTC_STORM_LOOKBACK_1M and c1[-BTC_STORM_LOOKBACK_1M]["close"] > 0:
        move_1m = (c1[-1]["close"] - c1[-BTC_STORM_LOOKBACK_1M]["close"]) / c1[-BTC_STORM_LOOKBACK_1M]["close"] * 100
    if c5 and len(c5) > BTC_STORM_LOOKBACK_5M and c5[-BTC_STORM_LOOKBACK_5M]["close"] > 0:
        move_5m = (c5[-1]["close"] - c5[-BTC_STORM_LOOKBACK_5M]["close"]) / c5[-BTC_STORM_LOOKBACK_5M]["close"] * 100

    storm = abs(move_1m) >= BTC_STORM_MOVE_1M_PERCENT or abs(move_5m) >= BTC_STORM_MOVE_5M_PERCENT
    data = {
        "storm": storm,
        "move_1m": round(move_1m, 3),
        "move_5m": round(move_5m, 3),
        "note": f"BTC_STORM={'ON' if storm else 'OFF'} | 1m move {round(move_1m, 3)}% / 5m move {round(move_5m, 3)}%",
    }
    BTC_STORM_CACHE["ts"] = current
    BTC_STORM_CACHE["data"] = data
    return data


def is_confirming(trend: str, direction: str) -> bool:
    return trend in (["BULLISH", "SOFT_BULLISH"] if direction == "LONG" else ["BEARISH", "SOFT_BEARISH"])


def is_hard_against(trend: str, direction: str) -> bool:
    return (direction == "LONG" and trend == "BEARISH") or (direction == "SHORT" and trend == "BULLISH")


def common_market_data(c15: List[dict], c5: List[dict], c1: List[dict], c1h: List[dict], c4h: List[dict]) -> dict:
    closes15 = [c["close"] for c in c15]
    closes5 = [c["close"] for c in c5]
    closes1 = [c["close"] for c in c1]
    price = closes5[-1]
    a5 = atr(c5)
    a15 = atr(c15)
    vw = vwap_like(c15)
    e21_5 = ema(closes5, 21)[-1] if len(closes5) >= 22 else None
    e50_15 = ema(closes15, 50)[-1] if len(closes15) >= 51 else None
    e200_15 = ema(closes15, 200)[-1] if len(closes15) >= 200 else None
    e9_1 = ema(closes1, 9)[-1] if len(closes1) >= 10 else None
    return {
        "price": price,
        "closes15": closes15,
        "closes5": closes5,
        "a5": a5,
        "a15": a15,
        "adx15": adx(c15),
        "vw": vw,
        "rs5": rsi(closes5),
        "rs15": rsi(closes15),
        "vr5": volume_ratio(c5, 24),
        "trend1h": trend_state(c1h),
        "trend4h": trend_state(c4h),
        "ema9_1": e9_1,
        "ema21_5": e21_5,
        "ema50_15": e50_15,
        "ema200_15": e200_15,
        "atr15_percent": (a15 / price * 100) if a15 and price > 0 else None,
    }


def detect_market_regime(d: dict, btc_status: str, c15: List[dict]) -> dict:
    trend1h = d.get("trend1h", "NEUTRAL")
    trend4h = d.get("trend4h", "NEUTRAL")
    price = d.get("price", 0)
    adx15 = d.get("adx15") or 0
    atr15p = d.get("atr15_percent") or 0
    e50 = d.get("ema50_15")
    e200 = d.get("ema200_15")
    move12 = recent_move_percent(c15, 12)

    trend_up = trend1h in ["BULLISH", "SOFT_BULLISH"] and trend4h in ["BULLISH", "SOFT_BULLISH", "NEUTRAL"]
    trend_down = trend1h in ["BEARISH", "SOFT_BEARISH"] and trend4h in ["BEARISH", "SOFT_BEARISH", "NEUTRAL"]
    if e50 and e200:
        trend_up = trend_up and price >= e50 * 0.985 and e50 >= e200 * 0.985
        trend_down = trend_down and price <= e50 * 1.015 and e50 <= e200 * 1.015

    if atr15p > 0 and atr15p < MAX_CHOP_ATR_PERCENT and adx15 < MIN_ADX_TREND:
        regime = "CHOP"
        allowed = [] if NO_TRADE_IN_CHOP else ["LONG", "SHORT"]
    elif abs(move12) >= 5.5:
        regime = "EXPANSION_UP" if move12 > 0 else "EXPANSION_DOWN"
        allowed = ["LONG"] if move12 > 0 else ["SHORT"]
    elif trend_up and adx15 >= MIN_ADX_TREND:
        regime = "TREND_UP"
        allowed = ["LONG"]
    elif trend_down and adx15 >= MIN_ADX_TREND:
        regime = "TREND_DOWN"
        allowed = ["SHORT"]
    else:
        regime = "RANGE"
        allowed = ["LONG", "SHORT"] if ALLOW_RANGE_EDGE_TRADES else []

    # BTC hard guard: если BTC явно против, контртренд A+ запрещён, а B только в range/edge.
    if btc_status == "BULLISH" and regime != "RANGE":
        allowed = [x for x in allowed if x != "SHORT"]
    if btc_status == "BEARISH" and regime != "RANGE":
        allowed = [x for x in allowed if x != "LONG"]

    return {"regime": regime, "allowed_directions": allowed, "adx15": round(adx15, 2), "atr15_percent": round(atr15p, 3), "move12_15m": round(move12, 2), "regime_note": f"Regime: {regime}; allowed: {', '.join(allowed) if allowed else 'NO TRADE'}; ADX15 {round(adx15,1)}; ATR15 {round(atr15p,2)}%"}


def analyze_funding_oi(symbol: str, direction: str) -> dict:
    funding = get_funding_rate(symbol)
    oi = get_open_interest(symbol)
    blocked = False
    adj = 0
    reason = []
    if funding is not None:
        if abs(funding) >= FUNDING_EXTREME_RATE:
            blocked = True
            reason.append(f"Funding экстремальный: {funding:.6f}")
        elif direction == "LONG" and funding > MAX_ABS_FUNDING_RATE:
            adj -= 2; reason.append(f"Funding перегрет для LONG: {funding:.6f}")
        elif direction == "SHORT" and funding < -MAX_ABS_FUNDING_RATE:
            adj -= 2; reason.append(f"Funding перегрет для SHORT: {funding:.6f}")
        else:
            adj += 1; reason.append(f"Funding нормальный: {funding:.6f}")
    else:
        reason.append("Funding недоступен")
    reason.append(f"OI доступен: {round(oi, 2)}" if oi is not None else "OI недоступен")
    return {"blocked": blocked, "score_adjustment": adj, "funding": funding, "open_interest": oi, "reason": "; ".join(reason)}


def base_filters(symbol: str, direction: str, btc_status: str, d: dict, regime: dict) -> dict:
    funding = analyze_funding_oi(symbol, direction)
    btc_storm = detect_btc_storm()
    trend1h = d.get("trend1h", "NEUTRAL")
    trend4h = d.get("trend4h", "NEUTRAL")
    htf_1h = is_confirming(trend1h, direction)
    htf_4h = is_confirming(trend4h, direction)
    htf_any = htf_1h or htf_4h
    htf_full = htf_1h and htf_4h
    htf_both_against = is_hard_against(trend1h, direction) and is_hard_against(trend4h, direction)
    btc_against = (direction == "LONG" and btc_status == "BEARISH") or (direction == "SHORT" and btc_status == "BULLISH")
    blocked = funding.get("blocked", False)
    hard_block_reasons = []

    if MARKET_REGIME_ENABLED and direction not in regime.get("allowed_directions", []):
        blocked = True
        hard_block_reasons.append(f"direction {direction} not allowed in {regime.get('regime')}")
    if htf_both_against:
        blocked = True
        hard_block_reasons.append("1H и 4H против направления")
    if direction == "SHORT" and SHORT_A_PLUS_REQUIRES_BTC_NOT_BULLISH and btc_status == "BULLISH":
        # Не режем сразу все short в RANGE, но A+ потом запретим. Для TREND уже direction blocked.
        pass

    score_adj = funding.get("score_adjustment", 0)
    if htf_1h:
        score_adj += 5
    if htf_4h:
        score_adj += 4
    if htf_full:
        score_adj += 3

    return {
        "blocked": blocked,
        "hard_block_reasons": hard_block_reasons,
        "score_adjustment": score_adj,
        "funding": funding,
        "btc_status": btc_status,
        "btc_against": btc_against,
        "btc_storm": btc_storm.get("storm", False),
        "btc_storm_note": btc_storm.get("note", ""),
        "btc_storm_move_1m": btc_storm.get("move_1m", 0.0),
        "btc_storm_move_5m": btc_storm.get("move_5m", 0.0),
        "atr5": d.get("a5"),
        "atr15": d.get("a15"),
        "trend1h": trend1h,
        "trend4h": trend4h,
        "htf_1h_confirmed": htf_1h,
        "htf_4h_confirmed": htf_4h,
        "htf_any_confirmed": htf_any,
        "htf_full_confirmed": htf_full,
        "htf_both_against": htf_both_against,
        "regime": regime.get("regime", "UNKNOWN"),
        "regime_note": regime.get("regime_note", ""),
        "htf_note": f"HTF: 1H {trend1h} / 4H {trend4h}; full={htf_full}; any={htf_any}",
    }


def attach_space_filter(filters: dict, price: float, direction: str, c15: List[dict]) -> dict:
    supports = find_swing_support_levels(c15, 140)
    resistances = find_swing_resistance_levels(c15, 140)
    filters["space_to_target_percent"] = space_to_next_level_percent(price, direction, supports, resistances)
    return filters


def late_or_chase_blocked(direction: str, c5: List[dict], price: float, vw: float, filters: dict) -> bool:
    if vw and abs(price - vw) / vw * 100 > MAX_DISTANCE_FROM_VWAP_PERCENT:
        filters["blocked"] = True
        filters.setdefault("hard_block_reasons", []).append("цена слишком далеко от VWAP")
        return True
    if not ENABLE_ANTI_CHASE_FILTER or len(c5) < max(CHASE_LOOKBACK_CANDLES_5M + 5, 40):
        return False
    closes = [c["close"] for c in c5]
    e21 = ema(closes, 21)[-1]
    old = c5[-CHASE_LOOKBACK_CANDLES_5M]["close"]
    move = (price - old) / old * 100 if old > 0 else 0
    distance_ema = abs(price - e21) / e21 * 100 if e21 > 0 else 0
    recent = c5[-8:]
    if direction == "LONG":
        high = max(c["high"] for c in c5[-CHASE_LOOKBACK_CANDLES_5M:])
        pullback = (high - min(c["low"] for c in recent)) / high * 100 if high > 0 else 0
        if move >= EXTREME_CHASE_MOVE_5M_PERCENT and (pullback < MIN_PULLBACK_AFTER_EXTREME_PERCENT or distance_ema > MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT):
            filters["blocked"] = True; filters.setdefault("hard_block_reasons", []).append("anti-chase LONG extreme")
            return True
        if move >= MAX_CHASE_MOVE_5M_PERCENT and pullback < MIN_PULLBACK_AFTER_CHASE_PERCENT and distance_ema > MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT:
            filters["blocked"] = True; filters.setdefault("hard_block_reasons", []).append("anti-chase LONG")
            return True
    if direction == "SHORT":
        low = min(c["low"] for c in c5[-CHASE_LOOKBACK_CANDLES_5M:])
        pullback = (max(c["high"] for c in recent) - low) / low * 100 if low > 0 else 0
        if move <= -EXTREME_CHASE_MOVE_5M_PERCENT and (pullback < MIN_PULLBACK_AFTER_EXTREME_PERCENT or distance_ema > MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT):
            filters["blocked"] = True; filters.setdefault("hard_block_reasons", []).append("anti-chase SHORT extreme")
            return True
        if move <= -MAX_CHASE_MOVE_5M_PERCENT and pullback < MIN_PULLBACK_AFTER_CHASE_PERCENT and distance_ema > MAX_CHASE_DISTANCE_FROM_EMA21_PERCENT:
            filters["blocked"] = True; filters.setdefault("hard_block_reasons", []).append("anti-chase SHORT")
            return True
    return False


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
    if risk <= 0 or entry <= 0:
        return (entry * (1 + min_move), entry * (1 + min_move * 1.8), entry * (1 + min_move * 2.8)) if direction == "LONG" else (entry * (1 - min_move), entry * (1 - min_move * 1.8), entry * (1 - min_move * 2.8))
    if direction == "LONG":
        return max(entry + risk * TP1_R_MULTIPLIER, entry * (1 + min_move)), max(entry + risk * TP2_R_MULTIPLIER, entry * (1 + min_move * 1.8)), max(entry + risk * TP3_R_MULTIPLIER, entry * (1 + min_move * 2.8))
    return min(entry - risk * TP1_R_MULTIPLIER, entry * (1 - min_move)), min(entry - risk * TP2_R_MULTIPLIER, entry * (1 - min_move * 1.8)), min(entry - risk * TP3_R_MULTIPLIER, entry * (1 - min_move * 2.8))


def calculate_position(entry: float, sl: float, deposit: float, risk_percent: float) -> dict:
    risk_amount = deposit * risk_percent / 100
    stop = abs(entry - sl)
    if entry <= 0 or sl <= 0 or stop <= 0:
        return {"risk_amount": round(risk_amount, 2), "position_size_usdt": None, "coin_amount": None, "margin_usdt": None, "error": "Неверный entry/sl"}
    coin_amount = risk_amount / stop
    position_size = coin_amount * entry
    return {"risk_amount": round(risk_amount, 2), "position_size_usdt": round(position_size, 2), "coin_amount": round(coin_amount, 8), "margin_usdt": round(position_size / LEVERAGE, 2), "error": None}


def is_level_strategy(strategy: str) -> bool:
    return strategy in {"SWEEP_RECLAIM_LONG", "SWEEP_REJECT_SHORT", "BREAK_RETEST_LONG", "BREAK_RETEST_SHORT"}


def is_strategy_enabled(strategy: str, direction: str, grade: str) -> bool:
    ensure_stats_structure()
    return now_ts() >= STATE.get("strategy_side_grade_disabled_until", {}).get(f"{strategy}:{direction}:{grade}", 0)


def classify_signal(score: int, rr: float, vol: float, filters: dict, strategy: str, direction: str) -> Optional[dict]:
    if filters.get("blocked"):
        return None

    htf_full = filters.get("htf_full_confirmed", False)
    htf_any = filters.get("htf_any_confirmed", False)
    btc_status = filters.get("btc_status", "NEUTRAL")
    regime = filters.get("regime", "UNKNOWN")

    b_score, b_rr, b_vol = B_MIN_SCORE, B_MIN_RR, B_MIN_VOLUME_RATIO
    if is_level_strategy(strategy):
        b_score, b_rr, b_vol = min(b_score, LEVEL_B_MIN_SCORE), min(b_rr, LEVEL_B_MIN_RR), min(b_vol, LEVEL_B_MIN_VOLUME_RATIO)
    if direction == "SHORT":
        if not SHORT_B_ENABLED:
            b_score, b_rr, b_vol = 999, 999, 999
        else:
            b_score, b_rr, b_vol = max(b_score, SHORT_B_MIN_SCORE), max(b_rr, SHORT_B_MIN_RR), max(b_vol, SHORT_B_MIN_VOLUME_RATIO)

    # Если общий B уже показал слабость, не отключаем его полностью, а делаем сильно строже.
    if ADAPTIVE_TRUST_ENABLED:
        b_global = STATE.get("stats", {}).get("grade", {}).get("B", {"positive": 0, "sl": 0})
        _, _, b_total, b_wr = _stat_wr(b_global)
        if b_total >= 20 and b_wr < TRUST_B_GLOBAL_WR_MIN:
            b_score += TRUST_B_SCORE_ADD_IF_WEAK
            b_rr += TRUST_B_RR_ADD_IF_WEAK
            b_vol += TRUST_B_VOL_ADD_IF_WEAK

    # A+ теперь только market-regime aligned + full HTF + trust. Score не перебивает контекст.
    a_plus_allowed = htf_full and not filters.get("btc_against")
    if direction == "SHORT" and SHORT_A_PLUS_REQUIRES_BTC_NOT_BULLISH and btc_status == "BULLISH":
        a_plus_allowed = False
    if regime == "RANGE" and direction == "SHORT":
        a_plus_allowed = a_plus_allowed and filters.get("range_edge_quality", False)

    if score >= A_PLUS_MIN_SCORE and rr >= A_PLUS_MIN_RR and vol >= A_PLUS_MIN_VOLUME_RATIO and a_plus_allowed:
        trust = get_strategy_trust(strategy, direction, "A+")
        filters["trust"] = trust
        filters["trust_note"] = trust.get("note")
        if not trust.get("blocked") and trust.get("a_plus_allowed", True):
            risk_mult = A_PLUS_RISK_MULTIPLIER * float(trust.get("risk_multiplier", 1.0))
            return {"grade": "A+", "risk_multiplier": risk_mult, "trust": trust}

    # Если какой-то фильтр требует только B — уважаем это, но B тоже проходит через trust.
    if filters.get("force_grade") == "B":
        if score >= b_score and rr >= b_rr and vol >= b_vol and htf_any:
            trust = get_strategy_trust(strategy, direction, "B")
            filters["trust"] = trust
            filters["trust_note"] = trust.get("note")
            if not trust.get("blocked") and trust.get("b_allowed", True):
                risk_mult = filters.get("risk_multiplier_override", B_RISK_MULTIPLIER) * float(trust.get("risk_multiplier", 1.0))
                return {"grade": "B", "risk_multiplier": risk_mult, "trust": trust}
        return None

    # B: нужен хотя бы один HTF + режим не против + trust. Контртренд только в range и с меньшим риском.
    if score >= b_score and rr >= b_rr and vol >= b_vol and htf_any:
        risk_mult = filters.get("risk_multiplier_override", B_RISK_MULTIPLIER)
        if filters.get("btc_against") or (regime not in ["RANGE"] and direction not in filters.get("regime_allowed", [])):
            if not ALLOW_COUNTERTREND_B_IN_RANGE_ONLY or regime != "RANGE":
                return None
            risk_mult = min(risk_mult, 0.14)

        trust = get_strategy_trust(strategy, direction, "B")
        filters["trust"] = trust
        filters["trust_note"] = trust.get("note")
        if trust.get("blocked") or not trust.get("b_allowed", True):
            return None
        risk_mult = risk_mult * float(trust.get("risk_multiplier", 1.0))
        return {"grade": "B", "risk_multiplier": risk_mult, "trust": trust}

    return None


def apply_professional_sl_buffer(entry: float, sl: float, direction: str, score: int, filters: dict) -> float:
    """Для потенциальных A+ делаем стоп структурнее: не впритык к локальной шпильке."""
    if not PRO_TRADE_MANAGEMENT_ENABLED or not A_PLUS_STRUCTURAL_SL_ENABLED:
        return sl
    if score < A_PLUS_MIN_SCORE:
        return sl
    atr5 = filters.get("atr5") or 0
    if entry <= 0 or sl <= 0:
        return sl
    min_stop = entry * A_PLUS_MIN_STOP_PRICE_MOVE_PERCENT / 100
    current_stop = abs(entry - sl)
    extra = atr5 * A_PLUS_EXTRA_ATR_BUFFER if atr5 else 0
    required = max(min_stop, current_stop + extra)
    if required <= current_stop:
        return sl
    new_sl = entry - required if direction == "LONG" else entry + required
    filters["sl_management_note"] = (
        f"A+ structural SL: стоп расширен под шум/ATR; было {round(sl, 8)}, стало {round(new_sl, 8)}. "
        "Это может уменьшить размер позиции, но снижает риск выбивания шпилькой."
    )
    return new_sl


def build_signal(symbol: str, direction: str, strategy: str, entry: float, sl: float, score: int, vol_ratio: float, reason: str, deposit: float, risk_percent: float, filters: dict) -> Optional[dict]:
    score += filters.get("score_adjustment", 0)
    if is_level_strategy(strategy):
        score += 2

    # V7.2: сначала делаем потенциальный A+ стоп структурнее, потом считаем RR/risk.
    sl = apply_professional_sl_buffer(entry, sl, direction, score, filters)
    risk_pos = calc_risk_position(entry, sl)
    if risk_pos > MAX_RISK_POSITION_PERCENT:
        return None

    space = filters.get("space_to_target_percent")
    if ENABLE_SPACE_TO_TARGET_FILTER and space is not None:
        if score >= A_PLUS_MIN_SCORE and space < MIN_SPACE_TO_TARGET_PERCENT_A_PLUS:
            filters["force_grade"] = "B"
            filters["space_note"] = f"мало места до ближайшего уровня: {round(space, 2)}%, A+ запрещён"
        elif score < A_PLUS_MIN_SCORE and space < MIN_SPACE_TO_TARGET_PERCENT_B:
            return None

    tp1, tp2, tp3 = make_dynamic_tps(entry, sl, direction)
    risk_price = abs(entry - sl) / entry * 100 if entry > 0 else 0
    trade_cost = estimate_trade_cost_percent()
    raw_reward_tp2 = price_move_percent(entry, tp2, direction)
    net_reward_tp2 = max(raw_reward_tp2 - trade_cost, 0)
    rr = net_reward_tp2 / risk_price if risk_price > 0 else 0

    grade_data = classify_signal(score, rr, vol_ratio, filters, strategy, direction)
    if not grade_data:
        return None
    grade = grade_data["grade"]
    if grade_data.get("trust"):
        filters["trust"] = grade_data.get("trust")
        filters["trust_note"] = grade_data.get("trust", {}).get("note")
    if not is_strategy_enabled(strategy, direction, grade):
        return None

    signal_id = f"{normalize_symbol(symbol)}:{strategy}:{direction}:{grade}:{round(entry, 8)}"
    if signal_id in STATE.get("sent_signals", {}):
        return None

    adjusted_risk = risk_percent * grade_data["risk_multiplier"]
    pos = calculate_position(entry, sl, deposit, adjusted_risk)
    raw_tp1 = price_move_percent(entry, tp1, direction)
    net_tp1 = max(raw_tp1 - trade_cost, 0)

    return {
        "id": signal_id,
        "symbol": normalize_symbol(symbol),
        "display_symbol": display_symbol(symbol),
        "direction": direction,
        "strategy": strategy,
        "grade": grade,
        "risk_multiplier": grade_data["risk_multiplier"],
        "trust_status": filters.get("trust", {}).get("status", "NEW"),
        "trust_note": filters.get("trust_note"),
        "status": "ACTIVE",
        "score": min(max(score, 0), 98),
        "entry": round(entry, 8),
        "sl": round(sl, 8),
        "post_tp1_sl": round((entry - entry * POST_TP1_BE_BUFFER_PERCENT / 100) if direction == "LONG" else (entry + entry * POST_TP1_BE_BUFFER_PERCENT / 100), 8),
        "soft_stop_seconds": A_PLUS_SOFT_STOP_SECONDS if grade == "A+" and A_PLUS_SOFT_STOP_ENABLED else 0,
        "trade_management_note": "A+ soft-stop + TP1 buffer active" if grade == "A+" and A_PLUS_SOFT_STOP_ENABLED else "standard stop management",
        "tp1": round(tp1, 8),
        "tp2": round(tp2, 8),
        "tp3": round(tp3, 8),
        "rr": round(rr, 2),
        "rr_basis": "TP2 net",
        "raw_reward_to_tp1_percent": round(raw_tp1, 4),
        "net_reward_to_tp1_percent": round(net_tp1, 4),
        "raw_reward_to_tp2_percent": round(raw_reward_tp2, 4),
        "net_reward_to_tp2_percent": round(net_reward_tp2, 4),
        "estimated_trade_cost_percent": round(trade_cost, 4),
        "volume_ratio": round(vol_ratio, 2),
        "risk_position_percent": round(risk_pos, 2),
        "space_to_target_percent": None if space is None else round(space, 3),
        "risk_percent": adjusted_risk,
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


def prepare_context(symbol: str, direction: str, c15, c5, c1, c1h, c4h, btc_status: str) -> Optional[dict]:
    d = common_market_data(c15, c5, c1, c1h, c4h)
    required = [d.get("a5"), d.get("a15"), d.get("vw"), d.get("rs5"), d.get("rs15"), d.get("ema21_5"), d.get("ema50_15"), d.get("ema9_1")]
    if not all(required):
        return None
    regime = detect_market_regime(d, btc_status, c15)
    filters = base_filters(symbol, direction, btc_status, d, regime)
    filters["regime_allowed"] = regime.get("allowed_directions", [])
    filters = attach_space_filter(filters, d["price"], direction, c15)
    late_or_chase_blocked(direction, c5, d["price"], d["vw"], filters)
    return {"d": d, "regime": regime, "filters": filters}


def evaluate_sweep_reclaim_long(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "SWEEP_RECLAIM_LONG"
    if direction != "LONG" or len(c15) < 100 or len(c5) < 80:
        return None
    ctx = prepare_context(symbol, direction, c15, c5, c1, c1h, c4h, btc_status)
    if not ctx:
        return None
    d, filters = ctx["d"], ctx["filters"]
    last, prev = c5[-1], c5[-2]
    price = last["close"]
    levels = find_swing_support_levels(c15, 140)
    level = nearest_below(price, levels, 8.0) or nearest_near(price, levels, 2.0)
    if not level:
        return None
    window = c5[-18:]
    sweep_low = min(c["low"] for c in window)
    swept = sweep_low < level * 0.998
    reclaim_closes = [c for c in c5[-5:] if c["close"] > level * 1.0005]
    bounce = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and last["close"] >= d["ema21_5"] * 0.992 and candle_close_position(last) >= 0.55
    structure_ok = len(reclaim_closes) >= 1 and min(c["low"] for c in c5[-4:]) >= sweep_low * 1.0005
    if not (swept and reclaim_closes and bounce and structure_ok):
        return None
    if d["rs5"] > 76 or d["rs15"] > 74:
        return None
    score = 62
    score += 7 if btc_status in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 8 if d["trend1h"] in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 4 if d["trend4h"] in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 7 if d["vr5"] >= 1.0 else 0
    score += 5 if d["vr5"] >= 1.18 else 0
    score += 4 if price > d["vw"] * 0.99 else 0
    score += 4 if abs(sweep_low - level) / level * 100 <= 1.0 else 0
    mtf = level_mtf_strength(level, c1h, c4h, "support")
    filters.update(mtf)
    filters["score_adjustment"] += mtf["level_strength_bonus"]
    filters["setup_note"] = "liquidity sweep + reclaim + higher low after sweep"
    sl = min(sweep_low - d["a5"] * 0.12, min(c["low"] for c in c5[-10:]) - d["a5"] * 0.05)
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], "Market-regime LONG: sweep поддержки → reclaim → higher low/зелёное подтверждение. Это основной профессиональный сетап V7.", deposit, risk_percent, filters)


def evaluate_sweep_reject_short(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "SWEEP_REJECT_SHORT"
    if direction != "SHORT" or len(c15) < 100 or len(c5) < 80:
        return None
    ctx = prepare_context(symbol, direction, c15, c5, c1, c1h, c4h, btc_status)
    if not ctx:
        return None
    d, filters = ctx["d"], ctx["filters"]
    last, prev = c5[-1], c5[-2]
    price = last["close"]
    levels = find_swing_resistance_levels(c15, 140)
    level = nearest_above(price, levels, 8.0) or nearest_near(price, levels, 2.0)
    if not level:
        return None
    window = c5[-20:]
    sweep_high = max(c["high"] for c in window)
    swept = sweep_high > level * 1.002
    closed_back = len([c for c in c5[-6:] if c["close"] < level * 0.9995]) >= 2
    lower_high = max(c["high"] for c in c5[-4:]) < sweep_high * 0.999
    rejection = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.001 and last["close"] <= d["ema21_5"] * 1.004 and candle_close_position(last) <= 0.42
    if not (swept and closed_back and lower_high and rejection):
        return None
    if d["rs5"] < 25 or d["rs15"] < 28:
        return None
    score = 62
    score += 7 if btc_status in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 9 if d["trend1h"] in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 5 if d["trend4h"] in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 8 if d["vr5"] >= 1.05 else 0
    score += 5 if d["vr5"] >= 1.22 else 0
    score += 4 if price < d["vw"] * 1.005 else 0
    mtf = level_mtf_strength(level, c1h, c4h, "resistance")
    filters.update(mtf)
    filters["score_adjustment"] += mtf["level_strength_bonus"]
    filters["range_edge_quality"] = True
    filters["setup_note"] = "resistance sweep + 2 closes below + lower high"
    sl = max(sweep_high + d["a5"] * 0.12, max(c["high"] for c in c5[-10:]) + d["a5"] * 0.05)
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], "Market-regime SHORT: sweep сопротивления → 2 закрытия ниже → lower high → красное подтверждение. SHORT в V7 берётся только после второго подтверждения.", deposit, risk_percent, filters)


def evaluate_break_retest_long(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "BREAK_RETEST_LONG"
    if direction != "LONG" or len(c15) < 120 or len(c5) < 80:
        return None
    ctx = prepare_context(symbol, direction, c15, c5, c1, c1h, c4h, btc_status)
    if not ctx:
        return None
    d, filters = ctx["d"], ctx["filters"]
    last, prev = c5[-1], c5[-2]
    price = last["close"]
    levels = find_swing_resistance_levels(c15, 140)
    level = nearest_below(price, levels, 4.0)
    if not level:
        return None
    had_below = any(c["close"] < level * 0.999 for c in c15[-14:-3])
    broke = len([c for c in c15[-4:] if c["close"] > level * 1.001]) >= 1
    retest = min(c["low"] for c in c5[-8:]) <= level * 1.008
    held = min(c["close"] for c in c5[-5:]) > level * 0.998
    confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and price > d["ema21_5"] * 0.994
    if not (had_below and broke and retest and held and confirm):
        return None
    if d["rs5"] > 77 or d["rs15"] > 75:
        return None
    score = 63
    score += 7 if btc_status in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 9 if d["trend1h"] in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 5 if d["trend4h"] in ["BULLISH", "SOFT_BULLISH"] else 0
    score += 7 if d["vr5"] >= 1.0 else 0
    score += 5 if price > d["vw"] else 0
    mtf = level_mtf_strength(level, c1h, c4h, "resistance")
    filters.update(mtf)
    filters["score_adjustment"] += mtf["level_strength_bonus"]
    sl = min(level - d["a5"] * 0.18, min(c["low"] for c in c5[-10:]) - d["a5"] * 0.05)
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], "Пробой сопротивления → настоящий ретест сверху → удержание → подтверждение. Без ретеста V7 не догоняет breakout.", deposit, risk_percent, filters)


def evaluate_break_retest_short(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "BREAK_RETEST_SHORT"
    if direction != "SHORT" or len(c15) < 120 or len(c5) < 80:
        return None
    ctx = prepare_context(symbol, direction, c15, c5, c1, c1h, c4h, btc_status)
    if not ctx:
        return None
    d, filters = ctx["d"], ctx["filters"]
    last, prev = c5[-1], c5[-2]
    price = last["close"]
    levels = find_swing_support_levels(c15, 140)
    level = nearest_above(price, levels, 4.0)
    if not level:
        return None
    had_above = any(c["close"] > level * 1.001 for c in c15[-14:-3])
    broke = len([c for c in c15[-4:] if c["close"] < level * 0.999]) >= 1
    retest = max(c["high"] for c in c5[-8:]) >= level * 0.992
    held = max(c["close"] for c in c5[-5:]) < level * 1.002
    confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and price < d["ema21_5"] * 1.006
    if not (had_above and broke and retest and held and confirm):
        return None
    if d["rs5"] < 24 or d["rs15"] < 27:
        return None
    score = 63
    score += 8 if btc_status in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 10 if d["trend1h"] in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 5 if d["trend4h"] in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 8 if d["vr5"] >= 1.08 else 0
    score += 5 if price < d["vw"] else 0
    mtf = level_mtf_strength(level, c1h, c4h, "support")
    filters.update(mtf)
    filters["score_adjustment"] += mtf["level_strength_bonus"]
    sl = max(level + d["a5"] * 0.18, max(c["high"] for c in c5[-10:]) + d["a5"] * 0.05)
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], "Пробой поддержки → ретест снизу → удержание → красное подтверждение. SHORT только после структуры, не по первому пробою.", deposit, risk_percent, filters)


def evaluate_trend_pullback(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "TREND_PULLBACK_LONG" if direction == "LONG" else "TREND_PULLBACK_SHORT"
    if direction not in ["LONG", "SHORT"] or len(c15) < 120 or len(c5) < 80:
        return None
    ctx = prepare_context(symbol, direction, c15, c5, c1, c1h, c4h, btc_status)
    if not ctx:
        return None
    d, regime, filters = ctx["d"], ctx["regime"], ctx["filters"]
    if (direction == "LONG" and regime["regime"] not in ["TREND_UP", "EXPANSION_UP"]) or (direction == "SHORT" and regime["regime"] not in ["TREND_DOWN", "EXPANSION_DOWN"]):
        return None
    last, prev = c5[-1], c5[-2]
    price = last["close"]
    near_ema = abs(price - d["ema21_5"]) / d["ema21_5"] * 100 <= 1.25
    near_vwap = abs(price - d["vw"]) / d["vw"] * 100 <= 1.45
    if not (near_ema or near_vwap):
        return None
    score = 61
    if direction == "LONG":
        confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and price > d["ema50_15"] * 0.985
        if not confirm or d["rs5"] > 74 or btc_status == "BEARISH":
            return None
        sl = min(last["low"] - d["a5"] * 0.22, min(c["low"] for c in c5[-12:]) - d["a5"] * 0.06)
        score += 8 if d["trend1h"] in ["BULLISH", "SOFT_BULLISH"] else 0
        score += 5 if btc_status in ["BULLISH", "SOFT_BULLISH"] else 0
        reason = "Trend Pullback V7: рынок TREND_UP, откат к EMA/VWAP, зелёное подтверждение."
    else:
        confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and price < d["ema50_15"] * 1.015
        if not confirm or d["rs5"] < 26 or btc_status == "BULLISH":
            return None
        sl = max(last["high"] + d["a5"] * 0.22, max(c["high"] for c in c5[-12:]) + d["a5"] * 0.06)
        score += 10 if d["trend1h"] in ["BEARISH", "SOFT_BEARISH"] else 0
        score += 6 if btc_status in ["BEARISH", "SOFT_BEARISH"] else 0
        reason = "Trend Pullback V7: рынок TREND_DOWN, откат к EMA/VWAP, красное подтверждение."
    score += 5 if d["trend4h"] in (["BULLISH", "SOFT_BULLISH"] if direction == "LONG" else ["BEARISH", "SOFT_BEARISH"]) else 0
    score += 7 if d["vr5"] >= 1.0 else 0
    score += 5 if d["vr5"] >= 1.18 else 0
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], reason, deposit, risk_percent, filters)


def evaluate_impulse_pullback(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "IMPULSE_PULLBACK_PRO"
    if not IMPULSE_PULLBACK_ENABLED or direction not in ["LONG", "SHORT"] or len(c5) < 90 or len(c1) < 40:
        return None
    ctx = prepare_context(symbol, direction, c15, c5, c1, c1h, c4h, btc_status)
    if not ctx:
        return None
    d, regime, filters = ctx["d"], ctx["regime"], ctx["filters"]
    if direction == "LONG" and regime["regime"] not in ["TREND_UP", "EXPANSION_UP", "RANGE"]:
        return None
    if direction == "SHORT" and regime["regime"] not in ["TREND_DOWN", "EXPANSION_DOWN", "RANGE"]:
        return None
    if d["vr5"] < IMPULSE_MIN_VOLUME_RATIO or abs(d["price"] - d["vw"]) / d["vw"] * 100 > IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT:
        return None
    last, prev = c5[-1], c5[-2]
    old = c5[-15]["close"]
    segment = c5[-12:-3]
    score = 58
    price = last["close"]
    if direction == "LONG":
        if btc_status == "BEARISH" or d["trend1h"] == "BEARISH":
            return None
        high = max(c["high"] for c in segment)
        impulse = (high - old) / old * 100 if old > 0 else 0
        low_pb = min(c["low"] for c in c5[-7:-1])
        pullback = (high - low_pb) / high * 100 if high > 0 else 0
        confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and c1[-1]["close"] > d["ema9_1"] * 0.998 and price > d["ema21_5"] * 0.994
        if impulse < IMPULSE_MIN_MOVE_5M_PERCENT or not (IMPULSE_PULLBACK_MIN_PERCENT <= pullback <= IMPULSE_PULLBACK_MAX_PERCENT) or not confirm or d["rs5"] > 77:
            return None
        sl = min(low_pb - d["a5"] * 0.14, min(c["low"] for c in c5[-10:]) - d["a5"] * 0.05)
        score += 6 if btc_status in ["BULLISH", "SOFT_BULLISH"] else 0
        score += 6 if d["trend1h"] in ["BULLISH", "SOFT_BULLISH"] else 0
    else:
        if btc_status == "BULLISH" or d["trend1h"] == "BULLISH":
            return None
        low = min(c["low"] for c in segment)
        impulse = (old - low) / old * 100 if old > 0 else 0
        high_pb = max(c["high"] for c in c5[-7:-1])
        pullback = (high_pb - low) / low * 100 if low > 0 else 0
        confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and c1[-1]["close"] < d["ema9_1"] * 1.002 and price < d["ema21_5"] * 1.006
        if impulse < IMPULSE_MIN_MOVE_5M_PERCENT or not (IMPULSE_PULLBACK_MIN_PERCENT <= pullback <= IMPULSE_PULLBACK_MAX_PERCENT) or not confirm or d["rs5"] < 23:
            return None
        sl = max(high_pb + d["a5"] * 0.14, max(c["high"] for c in c5[-10:]) + d["a5"] * 0.05)
        score += 8 if btc_status in ["BEARISH", "SOFT_BEARISH"] else 0
        score += 8 if d["trend1h"] in ["BEARISH", "SOFT_BEARISH"] else 0
    score += 6 if d["vr5"] >= 1.05 else 0
    score += 4 if d["vr5"] >= 1.22 else 0
    filters["force_grade"] = "B"
    filters["risk_multiplier_override"] = IMPULSE_PULLBACK_RISK_MULTIPLIER
    filters["setup_note"] = "impulse -> healthy pullback -> 1m/5m confirmation; B-only"
    return build_signal(symbol, direction, strategy, price, sl, score, d["vr5"], "Impulse Pullback Pro V7: импульс → здоровый откат → подтверждение продолжения. Только B и уменьшенный риск.", deposit, risk_percent, filters)


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
    dirs = [normalize_direction(direction)] if normalize_direction(direction) else ["LONG", "SHORT"]
    funcs = [evaluate_sweep_reclaim_long, evaluate_sweep_reject_short, evaluate_break_retest_long, evaluate_break_retest_short, evaluate_trend_pullback, evaluate_impulse_pullback]
    candidates = []
    for d in dirs:
        for func in funcs:
            try:
                sig = func(symbol, d, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent)
                if sig:
                    candidates.append(sig)
            except Exception:
                continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: (1 if x["grade"] == "A+" else 0, x["score"], x["rr"], x["volume_ratio"], 0 if x.get("space_to_target_percent") is None else x.get("space_to_target_percent")), reverse=True)
    return candidates[0]


def is_on_cooldown(symbol: str) -> bool:
    ts = STATE.get("symbol_cooldown", {}).get(normalize_symbol(symbol))
    return bool(ts and now_ts() - ts < SIGNAL_COOLDOWN_SECONDS)


def set_cooldown(symbol: str):
    STATE["symbol_cooldown"][normalize_symbol(symbol)] = now_ts()
    save_state(STATE)


def is_blocked(symbol: str) -> bool:
    symbol = normalize_symbol(symbol)
    until = STATE.get("blocked_symbols", {}).get(symbol)
    if not until:
        return False
    if now_ts() > until:
        STATE["blocked_symbols"].pop(symbol, None)
        save_state(STATE)
        return False
    return True


def cleanup_state():
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


def save_signal(signal: dict):
    STATE["active_signals"][signal["id"]] = signal
    STATE["sent_signals"][signal["id"]] = now_ts()
    set_cooldown(signal["symbol"])
    save_state(STATE)


def send_telegram_message(text: str) -> dict:
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
    names = {
        "SWEEP_RECLAIM_LONG": "🟢 Sweep + Reclaim LONG",
        "SWEEP_REJECT_SHORT": "🔴 Sweep + Reject SHORT",
        "BREAK_RETEST_LONG": "📈 Break + Retest LONG",
        "BREAK_RETEST_SHORT": "📉 Break + Retest SHORT",
        "TREND_PULLBACK_LONG": "📌 Trend Pullback LONG",
        "TREND_PULLBACK_SHORT": "📌 Trend Pullback SHORT",
        "IMPULSE_PULLBACK_PRO": "⚡ Impulse Pullback Pro",
    }
    pos = signal["position"]
    risk_text = f"⚠️ Ошибка RM: {pos['error']}" if pos.get("error") else f"Риск: {signal['risk_percent']:.3f}% депозита\nРазмер позиции: {pos['position_size_usdt']} USDT\nМаржа x{LEVERAGE}: {pos.get('margin_usdt')} USDT"
    f = signal.get("filters", {})
    funding_text = f.get("funding", {}).get("reason", "Funding/OI: нет данных")
    notes = []
    for key in ["regime_note", "htf_note", "level_strength_note", "setup_note", "space_note", "trust_note", "btc_storm_note", "btc_storm_trade_note", "sl_management_note"]:
        if f.get(key):
            notes.append(f.get(key))
    if f.get("hard_block_reasons"):
        notes.append("Hard blocks: " + "; ".join(f.get("hard_block_reasons", [])))
    space = signal.get("space_to_target_percent")
    space_line = f"\n<b>Место до ближайшего уровня:</b> {space}%" if space is not None else ""
    caution = "\n⚠️ B-сигнал: вход осторожнее, риск уменьшен." if signal["grade"] == "B" else ""
    return f"""
🎯 <b>{mode} {'A+ SIGNAL' if signal['grade'] == 'A+' else 'B SIGNAL'}</b>

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
BTC: {f.get('btc_status', 'NEUTRAL')}
{funding_text}
{chr(10).join(notes)}

<b>Качество:</b> {signal['score']}/100
<b>Trust:</b> {signal.get('trust_status', 'NEW')}
<b>RR:</b> {signal['rr']} ({signal.get('rr_basis', 'TP2 net')})
<b>TP1 gross/net:</b> {signal.get('raw_reward_to_tp1_percent', 0)}% / {signal.get('net_reward_to_tp1_percent', 0)}%
<b>TP2 gross/net:</b> {signal.get('raw_reward_to_tp2_percent', 0)}% / {signal.get('net_reward_to_tp2_percent', 0)}%
<b>Издержки:</b> ~{signal.get('estimated_trade_cost_percent', 0)}%
<b>Объём:</b> x{signal['volume_ratio']}
<b>Риск до SL:</b> {signal['risk_position_percent']}% по позиции{space_line}

{risk_text}
{caution}

<b>Trade management:</b> {signal.get('trade_management_note', 'standard')}
<b>После TP1:</b>
Закрыть примерно {TP1_CLOSE_PERCENT:.0f}% позиции. Защитный SL после TP1: <code>{signal.get('post_tp1_sl', signal['entry'])}</code> — не ровно entry, а с buffer от шума.

⚠️ Не финансовый совет.
""".strip()


def scan_best_signal(deposit: float, risk_percent: float) -> dict:
    cleanup_state()
    symbols = get_symbols()
    btc_status = detect_btc_status()
    best, checked, found = None, 0, 0
    for symbol in symbols:
        checked += 1
        sig = analyze_symbol(symbol, None, deposit, risk_percent, btc_status_override=btc_status)
        if not sig:
            continue
        found += 1
        trust_rank = {"HIGH": 3, "MEDIUM": 2, "NEW": 1, "LOW": 0, "B_STRICT": 0, "A+_DENIED": -1, "B_DENIED": -1, "BLOCKED": -2}
        sig_key = (trust_rank.get(sig.get("trust_status", "NEW"), 1), 1 if sig["grade"] == "A+" else 0, sig["score"], sig["rr"], sig["volume_ratio"])
        best_key = (trust_rank.get(best.get("trust_status", "NEW"), 1), 1 if best["grade"] == "A+" else 0, best["score"], best["rr"], best["volume_ratio"]) if best else None
        if best is None or sig_key > best_key:
            best = sig
    if not best:
        return {"ok": False, "checked": checked, "found_candidates": found, "btc_status": btc_status, "message": "Сильных сигналов сейчас нет. V7 сначала фильтрует режим рынка, потом ищет сетап."}
    return {"ok": True, "checked": checked, "found_candidates": found, "btc_status": btc_status, "signal": best, "message": build_message(best)}


def is_signal_expired(signal: dict) -> bool:
    return bool(signal.get("created_at") and now_ts() - signal.get("created_at", 0) > SIGNAL_MAX_LIFETIME_SECONDS)


def check_signal_hit(signal: dict, candles: List[dict]):
    side = signal["direction"]
    last_checked = signal.get("last_checked_time", 0)
    new = [c for c in candles if c["time"] > last_checked]
    if not new:
        return None, candles[-1]["close"]

    soft_stop_seconds = int(signal.get("soft_stop_seconds", 0) or 0)
    created_at = float(signal.get("created_at", 0) or 0)
    post_tp1_sl = float(signal.get("post_tp1_sl", signal.get("entry", 0)) or signal.get("entry", 0))

    for c in new:
        high, low, close = c["high"], c["low"], c.get("close", 0)
        signal["last_checked_time"] = c["time"]
        candle_age = c["time"] / 1000 - created_at if created_at else 999999
        soft_stop_active = soft_stop_seconds > 0 and candle_age <= soft_stop_seconds and not signal.get("tp1_hit")

        if side == "LONG":
            if signal.get("tp2_hit") and low <= post_tp1_sl:
                return "PROFIT_AFTER_TP2", post_tp1_sl
            if signal.get("tp1_hit") and low <= post_tp1_sl:
                return "PROFIT_AFTER_TP1", post_tp1_sl
            if not signal.get("tp1_hit") and low <= signal["sl"]:
                if soft_stop_active and close > signal["sl"]:
                    # A+ soft-stop: шпилька под SL без закрытия — пока не считаем SL.
                    continue
                return "SL", signal["sl"]
            if not signal.get("tp1_hit") and high >= signal["tp1"]:
                signal["tp1_hit"] = True; return "TP1", signal["tp1"]
            if signal.get("tp1_hit") and not signal.get("tp2_hit") and high >= signal["tp2"]:
                signal["tp2_hit"] = True; return "TP2", signal["tp2"]
            if signal.get("tp2_hit") and not signal.get("tp3_hit") and high >= signal["tp3"]:
                signal["tp3_hit"] = True; return "TP3", signal["tp3"]
        else:
            if signal.get("tp2_hit") and high >= post_tp1_sl:
                return "PROFIT_AFTER_TP2", post_tp1_sl
            if signal.get("tp1_hit") and high >= post_tp1_sl:
                return "PROFIT_AFTER_TP1", post_tp1_sl
            if not signal.get("tp1_hit") and high >= signal["sl"]:
                if soft_stop_active and close < signal["sl"]:
                    # A+ soft-stop: шпилька выше SL без закрытия — пока не считаем SL.
                    continue
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
    side, strategy, grade = signal["direction"], signal["strategy"], signal.get("grade", "A+")
    symbol = normalize_symbol(signal["symbol"])
    ssg = f"{strategy}:{side}:{grade}"
    ss = f"{strategy}:{side}"
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
        if STATE["stats"]["strategy_side_grade"][ssg]["consecutive_sl"] >= STRATEGY_SIDE_GRADE_MAX_SL:
            STATE["strategy_side_grade_disabled_until"][ssg] = now_ts() + STRATEGY_DISABLE_SECONDS
            STATE["stats"]["strategy_side_grade"][ssg]["consecutive_sl"] = 0
            notes.append(f"⛔ {grade} {strategy} {side} временно отключён после серии SL.")
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
        if result in ["TP1", "PROFIT_AFTER_TP1"]: r_multiple = 0.35
        elif result in ["TP2", "PROFIT_AFTER_TP2"]: r_multiple = 0.75
        elif result == "TP3": r_multiple = 1.20
        if result == "TP1" and not signal.get("counted_tp1"):
            signal["counted_tp1"] = True; STATE["stats"]["side"][side]["tp1"] += 1
        if result == "TP2" and not signal.get("counted_tp2"):
            signal["counted_tp2"] = True; STATE["stats"]["side"][side]["tp2"] += 1
        if result == "TP3" and not signal.get("counted_tp3"):
            signal["counted_tp3"] = True; STATE["stats"]["side"][side]["tp3"] += 1
    if result in ["SL", "TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        STATE["stats"]["closed_trades"].append({"time": int(now_ts()), "symbol": symbol, "strategy": strategy, "side": side, "grade": grade, "result": result, "r_multiple": round(r_multiple, 3), "version": DEPLOY_MARKER})
        STATE["stats"]["closed_trades"] = STATE["stats"]["closed_trades"][-500:]
    save_state(STATE)
    return notes


def build_result_message(signal: dict, result: str, price: Optional[float], notes: List[str]) -> str:
    titles = {"SL": "❌ Stop Loss", "TP1": "✅ TP1 достигнут", "TP2": "✅ TP2 достигнут", "TP3": "🔥 TP3 достигнут", "PROFIT_AFTER_TP1": "🟢 Возврат после TP1", "PROFIT_AFTER_TP2": "🟢 Возврат после TP2", "EXPIRED": "⌛ Сигнал устарел"}
    status = {
        "SL": "SL сработал до TP1. Сделка отрицательная.",
        "TP1": f"Сделка позитивная. Закрыть ~{TP1_CLOSE_PERCENT:.0f}% позиции и SL в безубыток.",
        "TP2": "Хорошее движение. Сделка позитивная.",
        "TP3": "Отличная сделка. Полная цель достигнута.",
        "PROFIT_AFTER_TP1": "Цена вернулась после TP1, но сделка уже позитивная.",
        "PROFIT_AFTER_TP2": "Цена вернулась после TP2, сделка позитивная.",
        "EXPIRED": "Сигнал не достиг TP/SL за установленное время и удалён из активных.",
    }.get(result, "Обновление по сделке.")
    adaptive = "\n\n<b>Адаптация бота:</b>\n" + "\n".join(notes) if notes else ""
    return f"""
{titles.get(result, result)}

<b>{signal.get('grade', 'A+')} · {signal['direction']} {signal['display_symbol']}</b>
<b>Стратегия:</b> {signal.get('strategy', '')}

Вход: <code>{signal['entry']}</code>
Текущая цена: <code>{'n/a' if price is None else round(price, 8)}</code>

TP1: <code>{signal['tp1']}</code>
TP2: <code>{signal['tp2']}</code>
TP3: <code>{signal['tp3']}</code>
SL: <code>{signal['sl']}</code>

{status}

{build_stats_text()}
{adaptive}
""".strip()


def track_active_signals(send_to_telegram: bool = True) -> dict:
    cleanup_state()
    if not STATE.get("active_signals"):
        return {"ok": True, "message": "Активных сигналов нет.", "results": [], "active_left": 0}
    results, finished = [], []
    for sid, signal in list(STATE["active_signals"].items()):
        if is_signal_expired(signal):
            msg = build_result_message(signal, "EXPIRED", None, [])
            telegram = send_telegram_message(msg) if send_to_telegram else None
            results.append({"signal_id": sid, "symbol": signal.get("display_symbol"), "result": "EXPIRED", "telegram": telegram})
            finished.append(sid)
            continue
        candles = remove_unclosed_candle(get_klines(signal["symbol"], "1m", 120), "1m")
        if not candles:
            continue
        result, price = check_signal_hit(signal, candles)
        STATE["active_signals"][sid] = signal
        if not result:
            continue
        notes = apply_result(signal, result)
        msg = build_result_message(signal, result, price, notes)
        telegram = send_telegram_message(msg) if send_to_telegram else None
        results.append({"signal_id": sid, "symbol": signal["display_symbol"], "grade": signal.get("grade"), "direction": signal["direction"], "strategy": signal["strategy"], "result": result, "price": price, "telegram": telegram})
        if result in ["SL", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
            finished.append(sid)
    for sid in finished:
        STATE["active_signals"].pop(sid, None)
    save_state(STATE)
    return {"ok": True, "checked": len(STATE["active_signals"]) + len(finished), "results": results, "active_left": len(STATE["active_signals"])}


async def auto_worker():
    await asyncio.sleep(10)
    while True:
        try:
            current = now_ts()
            if AUTO_TRACK_ENABLED and current - STATE["auto"].get("last_track_time", 0) >= AUTO_TRACK_SECONDS:
                result = track_active_signals(send_to_telegram=True)
                STATE["auto"]["last_track_time"] = current
                STATE["auto"]["last_track_result"] = result
                save_state(STATE)
            if AUTO_SCAN_ENABLED and current - STATE["auto"].get("last_scan_time", 0) >= AUTO_SCAN_SECONDS:
                result = scan_best_signal(DEFAULT_DEPOSIT, DEFAULT_RISK_PERCENT)
                STATE["auto"]["last_scan_time"] = current
                STATE["auto"]["last_scan_result"] = result
                if result.get("ok"):
                    telegram = send_telegram_message(result["message"])
                    result["telegram"] = telegram
                    if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
                        save_signal(result["signal"])
                    else:
                        STATE["auto"]["last_error"] = f"Telegram не отправил сигнал: {telegram}"
                else:
                    last_report = STATE["auto"].get("last_no_signal_report_time", 0)
                    if DEBUG_NO_SIGNAL_REPORT_ENABLED and current - last_report >= DEBUG_NO_SIGNAL_REPORT_SECONDS:
                        send_telegram_message(f"🧠 <b>Диагностика {DEPLOY_MARKER}</b>\n\nBTC regime: {result.get('btc_status', 'NEUTRAL')}\nПроверено пар: {result.get('checked', 0)}\nКандидатов найдено: {result.get('found_candidates', 0)}\nСигнала пока нет. V7 сначала определяет режим рынка, потом ищет только разрешённые сетапы.")
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
        f"Market Regime Engine: {'ON' if MARKET_REGIME_ENABLED else 'OFF'} | CHOP no-trade: {'ON' if NO_TRADE_IN_CHOP else 'OFF'}\n"
        f"Adaptive Trust Engine: {'ON' if ADAPTIVE_TRUST_ENABLED else 'OFF'} | A+ side WR min {TRUST_A_PLUS_MIN_SIDE_WR}% | B grade WR min {TRUST_B_MIN_GRADE_WR}%\n"
        f"Pro Trade Management: {'ON' if PRO_TRADE_MANAGEMENT_ENABLED else 'OFF'} | BTC Storm: {'ON' if BTC_STORM_FILTER_ENABLED else 'OFF'} | A+ soft stop {A_PLUS_SOFT_STOP_SECONDS}s | TP1 buffer {POST_TP1_BE_BUFFER_PERCENT}%\n"
        f"A+ score/RR/volume: {A_PLUS_MIN_SCORE}+ / {A_PLUS_MIN_RR} / x{A_PLUS_MIN_VOLUME_RATIO}\n"
        f"B score/RR/volume: {B_MIN_SCORE}+ / {B_MIN_RR} / x{B_MIN_VOLUME_RATIO}\n"
        f"Level B score/RR/volume: {LEVEL_B_MIN_SCORE}+ / {LEVEL_B_MIN_RR} / x{LEVEL_B_MIN_VOLUME_RATIO}\n"
        f"SHORT B: {'ON' if SHORT_B_ENABLED else 'OFF'} / {SHORT_B_MIN_SCORE}+ / RR {SHORT_B_MIN_RR} / vol x{SHORT_B_MIN_VOLUME_RATIO}\n"
        f"Impulse Pullback: {'ON' if IMPULSE_PULLBACK_ENABLED else 'OFF'} / risk x{IMPULSE_PULLBACK_RISK_MULTIPLIER}\n"
        f"Anti-chase: {'ON' if ENABLE_ANTI_CHASE_FILTER else 'OFF'}\n"
        f"ALLOW_ENV_STRATEGY_OVERRIDES: {'ON' if ALLOW_ENV_STRATEGY_OVERRIDES else 'OFF'}\n\n"
        "V7.2 логика: market regime + strategy trust + professional trade management. A+ получает структурный SL, soft-stop первые минуты и TP1 buffer; B блокируется во время BTC storm."
    )
    send_telegram_message(text)
    asyncio.create_task(auto_worker())


@app.get("/", response_class=HTMLResponse)
def home():
    return f"""
<!DOCTYPE html><html><head><title>{APP_NAME}</title></head>
<body style="background:#020617;color:#e5e7eb;font-family:Arial;padding:40px;">
<h1>✅ {APP_NAME} работает</h1>
<p>Deploy marker: {DEPLOY_MARKER}</p>
<pre>GET /health
GET /version
GET /scan?send_to_telegram=false
GET /auto-signal?symbol=NEAR/USDT
GET /track
GET /stats
GET /auto-status
GET /test-telegram</pre>
</body></html>
"""


@app.get("/health")
def health():
    return {"status": "ok", "service": APP_NAME, "deploy_marker": DEPLOY_MARKER, "test_mode": TEST_MODE, "active_signals": len(STATE.get("active_signals", {})), "market_regime_enabled": MARKET_REGIME_ENABLED, "adaptive_trust_enabled": ADAPTIVE_TRUST_ENABLED, "pro_trade_management_enabled": PRO_TRADE_MANAGEMENT_ENABLED, "btc_storm_filter_enabled": BTC_STORM_FILTER_ENABLED, "a_plus_min_score": A_PLUS_MIN_SCORE, "b_min_score": B_MIN_SCORE, "short_b_enabled": SHORT_B_ENABLED, "api_key_enabled": bool(API_KEY)}


@app.get("/version")
def version():
    return {"ok": True, "service": APP_NAME, "deploy_marker": DEPLOY_MARKER, "telegram_token_set": bool(TELEGRAM_BOT_TOKEN), "telegram_chat_id_set": bool(TELEGRAM_CHAT_ID), "adaptive_trust_enabled": ADAPTIVE_TRUST_ENABLED, "start_command_recommended": "python bot.py"}


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
        return {"ok": False, "symbol": display_symbol(symbol), "direction": direction, "message": "Сильного сигнала нет. V7 запрещает вход, если режим/структура не совпали."}
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
    result = scan_best_signal(deposit, risk_percent)
    if result.get("ok") and send_to_telegram:
        telegram = send_telegram_message(result["message"])
        result["telegram"] = telegram
        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
            save_signal(result["signal"])
    return result


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
    return {"ok": True, "message": "State reset completed.", "deploy_marker": DEPLOY_MARKER}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT") or "10000")
    uvicorn.run(app, host="0.0.0.0", port=port)
