import os
import time
import json
import random
import asyncio
import requests
from typing import Optional, List, Any

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse


APP_NAME = "Professional Adaptive Futures Bot AUTO V10.0 PRO MARKET CONTEXT TRADER"
DEPLOY_MARKER = "V10_0_PRO_MARKET_CONTEXT_TRADER_2026_06_13"

app = FastAPI(title=APP_NAME)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BINGX_BASE_URL = "https://open-api.bingx.com"
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")

TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
LEVERAGE = int(os.getenv("LEVERAGE", "10"))

# A+ строгий
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "88"))
A_PLUS_MIN_VOLUME_RATIO = float(os.getenv("A_PLUS_MIN_VOLUME_RATIO", "1.10"))
A_PLUS_MIN_RR = float(os.getenv("A_PLUS_MIN_RR", "1.20"))
A_PLUS_RISK_MULTIPLIER = float(os.getenv("A_PLUS_RISK_MULTIPLIER", "1.0"))

# V4.6: A+ становится статистически умнее.
STATS_AWARE_A_PLUS_ENABLED = os.getenv("STATS_AWARE_A_PLUS_ENABLED", "true").lower() == "true"
A_PLUS_MIN_STRATEGY_TRADES = int(os.getenv("A_PLUS_MIN_STRATEGY_TRADES", "3"))
A_PLUS_MIN_STRATEGY_WR = float(os.getenv("A_PLUS_MIN_STRATEGY_WR", "45"))
MOMENTUM_SCALPER_CAN_A_PLUS = os.getenv("MOMENTUM_SCALPER_CAN_A_PLUS", "false").lower() == "true"

# B мягче, но риск ниже
B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "90"))
B_MIN_VOLUME_RATIO = float(os.getenv("B_MIN_VOLUME_RATIO", "1.30"))
B_MIN_RR = float(os.getenv("B_MIN_RR", "1.20"))
B_RISK_MULTIPLIER = float(os.getenv("B_RISK_MULTIPLIER", "0.05"))

TP1_POSITION_PERCENT = float(os.getenv("TP1_POSITION_PERCENT", "10"))
TP2_POSITION_PERCENT = float(os.getenv("TP2_POSITION_PERCENT", "20"))
TP3_POSITION_PERCENT = float(os.getenv("TP3_POSITION_PERCENT", "35"))
TP1_CLOSE_PERCENT = float(os.getenv("TP1_CLOSE_PERCENT", "50"))

MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "8"))

DEFAULT_DEPOSIT = float(os.getenv("DEFAULT_DEPOSIT", "1000"))
DEFAULT_RISK_PERCENT = float(os.getenv("DEFAULT_RISK_PERCENT", "0.5"))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "360"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))

SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "1200"))

PAIR_BLOCK_SECONDS = int(os.getenv("PAIR_BLOCK_SECONDS", "43200"))
STRATEGY_SIDE_DISABLE_SECONDS = int(os.getenv("STRATEGY_SIDE_DISABLE_SECONDS", "604800"))

PAIR_MAX_SL = int(os.getenv("PAIR_MAX_SL", "2"))
STRATEGY_SIDE_MAX_CONSECUTIVE_SL = int(os.getenv("STRATEGY_SIDE_MAX_CONSECUTIVE_SL", "2"))

AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"

AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "120"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "60"))

ENABLE_FUNDING_FILTER = os.getenv("ENABLE_FUNDING_FILTER", "true").lower() == "true"
ENABLE_OI_FILTER = os.getenv("ENABLE_OI_FILTER", "true").lower() == "true"
ENABLE_LATE_ENTRY_FILTER = os.getenv("ENABLE_LATE_ENTRY_FILTER", "true").lower() == "true"

MAX_RECENT_MOVE_PERCENT = float(os.getenv("MAX_RECENT_MOVE_PERCENT", "5.5"))
MAX_DISTANCE_FROM_VWAP_PERCENT = float(os.getenv("MAX_DISTANCE_FROM_VWAP_PERCENT", "4.5"))

# V4.6: параметры стратегий уровней.
LEVEL_SWEEP_LOOKBACK_CANDLES = int(os.getenv("LEVEL_SWEEP_LOOKBACK_CANDLES", "14"))
LEVEL_SWEEP_MAX_RECLAIM_DISTANCE_PERCENT = float(os.getenv("LEVEL_SWEEP_MAX_RECLAIM_DISTANCE_PERCENT", "7.0"))
# V4.8: осторожный LONG от поддержки при bearish BTC.
ALLOW_BEARISH_BTC_LEVEL_BOUNCE = os.getenv("ALLOW_BEARISH_BTC_LEVEL_BOUNCE", "false").lower() == "true"
BEARISH_BTC_BOUNCE_RISK_MULTIPLIER = float(os.getenv("BEARISH_BTC_BOUNCE_RISK_MULTIPLIER", "0.25"))
LEVEL_BREAK_RETEST_MAX_DISTANCE_PERCENT = float(os.getenv("LEVEL_BREAK_RETEST_MAX_DISTANCE_PERCENT", "3.2"))
# V4.8: зеркальные стратегии уровней — пробой сопротивления в LONG и rejection от сопротивления в SHORT.
LEVEL_RESISTANCE_REJECT_MAX_DISTANCE_PERCENT = float(os.getenv("LEVEL_RESISTANCE_REJECT_MAX_DISTANCE_PERCENT", "7.0"))

# V4.9: level-стратегии получают более живые B-сигналы, но A+ остаётся строгим.
LEVEL_ACTIVE_B_ENABLED = os.getenv("LEVEL_ACTIVE_B_ENABLED", "false").lower() == "true"
LEVEL_B_MIN_SCORE = int(os.getenv("LEVEL_B_MIN_SCORE", "84"))
LEVEL_B_MIN_VOLUME_RATIO = float(os.getenv("LEVEL_B_MIN_VOLUME_RATIO", "1.10"))
LEVEL_B_MIN_RR = float(os.getenv("LEVEL_B_MIN_RR", "0.95"))
LEVEL_SIGNAL_SCORE_BONUS = int(os.getenv("LEVEL_SIGNAL_SCORE_BONUS", "3"))

# V4.9: защита от покупки слишком позднего пробоя после сильного роста.
MAX_LEVEL_LONG_15M_MOVE_PERCENT = float(os.getenv("MAX_LEVEL_LONG_15M_MOVE_PERCENT", "6.5"))

# V5.0: анти-фейкаут защита для пробоев/ретестов уровней.
# Цель — не давать A+ на пробой, если цена быстро возвращается за уровень.
ANTI_FAKEOUT_LEVELS_ENABLED = os.getenv("ANTI_FAKEOUT_LEVELS_ENABLED", "true").lower() == "true"
LEVEL_RETEST_MAX_PIERCE_PERCENT = float(os.getenv("LEVEL_RETEST_MAX_PIERCE_PERCENT", "0.35"))
LEVEL_CONFIRM_CLOSE_BUFFER_PERCENT = float(os.getenv("LEVEL_CONFIRM_CLOSE_BUFFER_PERCENT", "0.10"))
LEVEL_MICRO_CONFIRM_CANDLES = int(os.getenv("LEVEL_MICRO_CONFIRM_CANDLES", "3"))
LEVEL_REJECTION_CLOSE_POSITION = float(os.getenv("LEVEL_REJECTION_CLOSE_POSITION", "0.45"))
LEVEL_BREAK_A_PLUS_NEEDS_MICRO_CONFIRM = os.getenv("LEVEL_BREAK_A_PLUS_NEEDS_MICRO_CONFIRM", "true").lower() == "true"
LEVEL_BREAK_RETEST_FORCE_B_ON_WEAK_CONFIRM = os.getenv("LEVEL_BREAK_RETEST_FORCE_B_ON_WEAK_CONFIRM", "true").lower() == "true"
LEVEL_BREAK_SHORT_BONUS_AFTER_CONFIRM = int(os.getenv("LEVEL_BREAK_SHORT_BONUS_AFTER_CONFIRM", "4"))

# V5.2 Balanced Risk-Aware Core Level Trader: сила уровня по старшим таймфреймам.
# A+ по уровню разрешается только если 15m уровень подтверждён 1H.
LEVEL_A_PLUS_REQUIRES_1H_CONFIRM = os.getenv("LEVEL_A_PLUS_REQUIRES_1H_CONFIRM", "true").lower() == "true"
LEVEL_1H_CONFIRM_DISTANCE_PERCENT = float(os.getenv("LEVEL_1H_CONFIRM_DISTANCE_PERCENT", "1.0"))
LEVEL_4H_CONFIRM_DISTANCE_PERCENT = float(os.getenv("LEVEL_4H_CONFIRM_DISTANCE_PERCENT", "1.5"))
LEVEL_STRENGTH_1H_BONUS = int(os.getenv("LEVEL_STRENGTH_1H_BONUS", "8"))
LEVEL_STRENGTH_4H_BONUS = int(os.getenv("LEVEL_STRENGTH_4H_BONUS", "4"))

# V4.9: периодический отчёт, если сигналов нет. Полезно для Background Worker.
DEBUG_NO_SIGNAL_REPORT_ENABLED = os.getenv("DEBUG_NO_SIGNAL_REPORT_ENABLED", "false").lower() == "true"
DEBUG_NO_SIGNAL_REPORT_SECONDS = int(os.getenv("DEBUG_NO_SIGNAL_REPORT_SECONDS", "10800"))

MAX_ABS_FUNDING_RATE = float(os.getenv("MAX_ABS_FUNDING_RATE", "0.0010"))
FUNDING_EXTREME_RATE = float(os.getenv("FUNDING_EXTREME_RATE", "0.0020"))

# V5.2 realistic execution model: RR is calculated after estimated fees/slippage.
FEE_RATE = float(os.getenv("FEE_RATE", "0.0005"))
SLIPPAGE_RATE = float(os.getenv("SLIPPAGE_RATE", "0.0003"))
SIGNAL_MAX_LIFETIME_SECONDS = int(os.getenv("SIGNAL_MAX_LIFETIME_SECONDS", "21600"))
SENT_SIGNALS_KEEP_SECONDS = int(os.getenv("SENT_SIGNALS_KEEP_SECONDS", "1209600"))
SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK = os.getenv("SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK", "true").lower() == "true"

# V5.3: осторожная B-only стратегия импульс -> откат -> подтверждение.
# ВАЖНО: настройки A+/B/Level B не менялись. Это только дополнительный модуль.
IMPULSE_PULLBACK_ENABLED = os.getenv("IMPULSE_PULLBACK_ENABLED", "true").lower() == "true"
IMPULSE_PULLBACK_RISK_MULTIPLIER = float(os.getenv("IMPULSE_PULLBACK_RISK_MULTIPLIER", "0.10"))
IMPULSE_MIN_MOVE_5M_PERCENT = float(os.getenv("IMPULSE_MIN_MOVE_5M_PERCENT", "1.20"))
IMPULSE_PULLBACK_MIN_PERCENT = float(os.getenv("IMPULSE_PULLBACK_MIN_PERCENT", "0.25"))
IMPULSE_PULLBACK_MAX_PERCENT = float(os.getenv("IMPULSE_PULLBACK_MAX_PERCENT", "2.80"))
IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT = float(os.getenv("IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT", "2.20"))
IMPULSE_MIN_VOLUME_RATIO = float(os.getenv("IMPULSE_MIN_VOLUME_RATIO", "1.05"))
IMPULSE_FORCE_B_ONLY = os.getenv("IMPULSE_FORCE_B_ONLY", "true").lower() == "true"

# V9.0: Pro opportunity model. Цель — не частые микро-сделки, а входы с реальным запасом движения.
MIN_TARGET_ROI_PERCENT = float(os.getenv("MIN_TARGET_ROI_PERCENT", "10"))  # ROI по позиции, с учётом плеча.
ALLOW_B_SIGNALS_EXPLICIT = os.getenv("ALLOW_B_SIGNALS_EXPLICIT", "false").lower() == "true"
# V10: B больше не считается рабочим сигналом. Даже если в Render осталась переменная B_SIGNALS_ENABLED=true,
# она НЕ включит B без отдельного ALLOW_B_SIGNALS_EXPLICIT=true.
B_SIGNALS_ENABLED = (os.getenv("B_SIGNALS_ENABLED", "false").lower() == "true") and ALLOW_B_SIGNALS_EXPLICIT
B_ONLY_WITH_HIGH_POTENTIAL = os.getenv("B_ONLY_WITH_HIGH_POTENTIAL", "true").lower() == "true"
EXTREME_DYNAMIC_ENABLED = os.getenv("EXTREME_DYNAMIC_ENABLED", "true").lower() == "true"
EXTREME_DYNAMIC_TOP_N = int(os.getenv("EXTREME_DYNAMIC_TOP_N", "120"))
EXTREME_DYNAMIC_MIN_CHANGE_PERCENT = float(os.getenv("EXTREME_DYNAMIC_MIN_CHANGE_PERCENT", "6"))
EXTREME_BASES = {
    "VELVET", "BEAT", "COLLECT", "SPACE", "GOBLIN", "MAGMA", "FOLKS", "FIGHT",
    "HMSTR", "GUA", "DOGS", "CATI", "MEME", "NOT", "1000SATS", "1000PEPE",
    "PEPE", "BONK", "WIF", "PNUT", "ACT", "GOAT", "MOODENG", "NEIRO",
    "TURBO", "BOME", "HYPE", "OPN", "PUMP", "SIREN", "MAVIA", "ARKM"
}

# V10: professional selective mode. После слабой статистики B и плохие связки выключены по умолчанию.
# Эти два сетапа в твоей статистике дали серию SL, поэтому они не должны снова лезть в рынок без явного разрешения.
PRO_BLOCKED_STRATEGY_SIDES = set(
    x.strip().upper()
    for x in os.getenv(
        "PRO_BLOCKED_STRATEGY_SIDES",
        "LEVEL_SWEEP_BOUNCE_LONG:LONG,LEVEL_RESISTANCE_REJECT_SHORT:SHORT"
    ).split(",")
    if x.strip()
)
PRO_MIN_TRADES_TO_BLOCK = int(os.getenv("PRO_MIN_TRADES_TO_BLOCK", "3"))
PRO_MIN_WR_TO_ALLOW = float(os.getenv("PRO_MIN_WR_TO_ALLOW", "40"))
PRO_DISABLE_B_AFTER_BAD_GLOBAL_STATS = os.getenv("PRO_DISABLE_B_AFTER_BAD_GLOBAL_STATS", "true").lower() == "true"

# V9: B-сигналы больше не считаются полноценными уверенными входами.
# Они допускаются только если потенциал высокий, риск маленький, а BTC/структура не против.


# V5.1: убираем «кашу» и оставляем 4 основные структуры уровня.
# Старые вспомогательные функции в файле остаются, но больше не вызываются.
STRATEGIES = [
    "LEVEL_SWEEP_BOUNCE_LONG",       # поддержка удержалась -> LONG
    "LEVEL_RESISTANCE_REJECT_SHORT", # сопротивление удержалось -> SHORT
    "LEVEL_BREAK_RETEST_SHORT",      # поддержка пробита -> SHORT
    "LEVEL_BREAK_RETEST_LONG",       # сопротивление пробито -> LONG
    "IMPULSE_PULLBACK_PRO",          # быстрый импульс -> откат к EMA/VWAP -> подтверждение
    "RANGE_STRUCTURE_PRO",         # диапазон: нижняя/верхняя граница -> возврат к середине
    "EXTREME_CONTEXT_PRO",         # сильные movers: continuation/reversal после подтверждения
]

LIQUID_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "INJ", "NEAR", "ARB", "OP", "APT", "SUI", "SEI", "DOT", "LTC",
    "BCH", "UNI", "AAVE", "FIL", "ATOM", "ETC", "TRX", "MATIC", "WLD",
    "TIA", "ORDI", "FTM", "RUNE", "ENA", "JUP", "PYTH", "STRK", "DYDX",
    "TON", "COMP", "STX", "TRB", "JTO", "DYM", "ICP", "GALA", "FET",
    "RNDR", "IMX", "APE", "AR", "MKR", "SNX", "LDO", "CRV", "GMT",
    "PEPE", "1000PEPE", "WIF", "BONK", "VELVET", "BEAT", "COLLECT", "SPACE", "GOBLIN", "MAGMA", "FOLKS", "FIGHT", "HMSTR", "GUA", "DOGS", "CATI", "MEME", "NOT", "1000SATS", "PNUT", "ACT", "GOAT", "MOODENG", "NEIRO", "TURBO", "BOME", "HYPE", "OPN"
}


def strategy_side_default():
    data = {}
    for strategy in STRATEGIES:
        data[f"{strategy}:LONG"] = 0
        data[f"{strategy}:SHORT"] = 0
    return data


def strategy_side_stats_default():
    data = {}
    for strategy in STRATEGIES:
        for side in ["LONG", "SHORT"]:
            data[f"{strategy}:{side}"] = {
                "positive": 0,
                "sl": 0,
                "consecutive_sl": 0,
            }
    return data


def strategy_side_grade_default():
    data = {}
    for strategy in STRATEGIES:
        for side in ["LONG", "SHORT"]:
            for grade in ["A+", "B"]:
                data[f"{strategy}:{side}:{grade}"] = 0
    return data


def strategy_side_grade_stats_default():
    data = {}
    for strategy in STRATEGIES:
        for side in ["LONG", "SHORT"]:
            for grade in ["A+", "B"]:
                data[f"{strategy}:{side}:{grade}"] = {
                    "positive": 0,
                    "sl": 0,
                    "consecutive_sl": 0,
                }
    return data


def default_state():
    return {
        "active_signals": {},
        "sent_signals": {},
        "symbol_cooldown": {},
        "blocked_symbols": {},

        # Старые глобальные блокировки направления оставлены только для совместимости.
        # Новый код НЕ блокирует весь LONG/SHORT, а блокирует только strategy+side.
        "side_disabled_until": {
            "LONG": 0,
            "SHORT": 0,
        },

        # Старые глобальные блокировки стратегии оставлены только для совместимости.
        # Новый код НЕ блокирует стратегию полностью.
        "strategy_disabled_until": {
            "BREAKOUT_MOMENTUM": 0,
            "TREND_PULLBACK": 0,
            "SWEEP_RECLAIM": 0,
            "MOMENTUM_SCALPER": 0,
        },

        # Legacy-поле оставлено только для совместимости со старым bot_state.json.
        # В V4.3 оно НЕ используется для блокировки новых сигналов,
        # чтобы старая B-блокировка не блокировала будущие A+.
        "strategy_side_disabled_until": strategy_side_default(),

        # Жёсткая блокировка strategy+side используется ТОЛЬКО после серии SL на A+.
        # B-сигналы никогда не пишут сюда.
        "strategy_side_hard_disabled_until": strategy_side_default(),

        # Grade-specific блокировка: B-сигналы не блокируют будущий A+
        # по той же стратегии и направлению.
        "strategy_side_grade_disabled_until": strategy_side_grade_default(),

        "stats": {
            "side": {
                "LONG": {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0},
                "SHORT": {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0},
            },
            "strategy": {
                "BREAKOUT_MOMENTUM": {"positive": 0, "sl": 0, "consecutive_sl": 0},
                "TREND_PULLBACK": {"positive": 0, "sl": 0, "consecutive_sl": 0},
                "SWEEP_RECLAIM": {"positive": 0, "sl": 0, "consecutive_sl": 0},
                "MOMENTUM_SCALPER": {"positive": 0, "sl": 0, "consecutive_sl": 0},
            },
            "strategy_side": strategy_side_stats_default(),
            "strategy_side_grade": strategy_side_grade_stats_default(),
            "grade": {
                "A+": {"positive": 0, "sl": 0},
                "B": {"positive": 0, "sl": 0},
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


def ensure_state_structure(state: dict):
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
        if strategy not in state["strategy_disabled_until"]:
            state["strategy_disabled_until"][strategy] = 0

        if strategy not in state["stats"]["strategy"]:
            state["stats"]["strategy"][strategy] = {"positive": 0, "sl": 0, "consecutive_sl": 0}

        for side in ["LONG", "SHORT"]:
            key = f"{strategy}:{side}"

            if key not in state["strategy_side_disabled_until"]:
                state["strategy_side_disabled_until"][key] = 0

            if "strategy_side_hard_disabled_until" not in state:
                state["strategy_side_hard_disabled_until"] = {}

            if key not in state["strategy_side_hard_disabled_until"]:
                state["strategy_side_hard_disabled_until"][key] = 0

            if key not in state["stats"]["strategy_side"]:
                state["stats"]["strategy_side"][key] = {"positive": 0, "sl": 0, "consecutive_sl": 0}

            if "strategy_side_grade_disabled_until" not in state:
                state["strategy_side_grade_disabled_until"] = {}

            if "strategy_side_grade" not in state["stats"]:
                state["stats"]["strategy_side_grade"] = {}

            for grade in ["A+", "B"]:
                grade_key = f"{strategy}:{side}:{grade}"

                if grade_key not in state["strategy_side_grade_disabled_until"]:
                    state["strategy_side_grade_disabled_until"][grade_key] = 0

                if grade_key not in state["stats"]["strategy_side_grade"]:
                    state["stats"]["strategy_side_grade"][grade_key] = {"positive": 0, "sl": 0, "consecutive_sl": 0}

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

    for grade in ["A+", "B"]:
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
    if total <= 0:
        return 0.0
    return round(positive / total * 100, 1)


def build_stats_text() -> str:
    ensure_stats_structure()

    long_stats = STATE["stats"]["side"]["LONG"]
    short_stats = STATE["stats"]["side"]["SHORT"]

    long_wr = calc_winrate(long_stats["positive"], long_stats["sl"])
    short_wr = calc_winrate(short_stats["positive"], short_stats["sl"])

    strategy_lines = []
    for strategy in STRATEGIES:
        s = STATE["stats"]["strategy"].get(strategy, {"positive": 0, "sl": 0})
        wr = calc_winrate(s.get("positive", 0), s.get("sl", 0))
        disabled_long = is_strategy_side_enabled(strategy, "LONG") is False
        disabled_short = is_strategy_side_enabled(strategy, "SHORT") is False

        status = "ON"
        if disabled_long and disabled_short:
            status = "OFF"
        elif disabled_long:
            status = "SHORT only"
        elif disabled_short:
            status = "LONG only"

        strategy_lines.append(
            f"{strategy}: {s.get('positive', 0)} позитив / {s.get('sl', 0)} SL / WR {wr}% [{status}]"
        )

    a_stats = STATE["stats"]["grade"].get("A+", {"positive": 0, "sl": 0})
    b_stats = STATE["stats"]["grade"].get("B", {"positive": 0, "sl": 0})

    a_wr = calc_winrate(a_stats.get("positive", 0), a_stats.get("sl", 0))
    b_wr = calc_winrate(b_stats.get("positive", 0), b_stats.get("sl", 0))

    return f"""
📊 <b>Статистика:</b>

📈 LONG: {long_stats['positive']} позитив / {long_stats['sl']} SL / WR {long_wr}%
📉 SHORT: {short_stats['positive']} позитив / {short_stats['sl']} SL / WR {short_wr}%

🏆 A+: {a_stats.get('positive', 0)} позитив / {a_stats.get('sl', 0)} SL / WR {a_wr}%
⚠️ B: {b_stats.get('positive', 0)} позитив / {b_stats.get('sl', 0)} SL / WR {b_wr}%

🧠 <b>Стратегии:</b>
{chr(10).join(strategy_lines)}
""".strip()


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

    # V9.0: больше не ограничиваемся старым ручным списком LIQUID_BASES.
    # Иначе бот пропускает реальные дневные лидеры роста/падения вроде VELVET/BEAT/COLLECT.
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


def is_strategy_side_enabled(strategy: str, side: str) -> bool:
    ensure_stats_structure()
    key = f"{strategy}:{side}"

    if key.upper() in PRO_BLOCKED_STRATEGY_SIDES:
        return False

    # V10: блокируем не только после consecutive SL, но и по общей плохой статистике связки.
    if is_statistically_bad_strategy_side(strategy, side):
        return False

    # V4.3: намеренно НЕ используем legacy strategy_side_disabled_until.
    # Если раньше B-сигнал заблокировал TREND_PULLBACK:SHORT,
    # эта старая блокировка больше не будет мешать A+.
    if "strategy_side_hard_disabled_until" not in STATE:
        STATE["strategy_side_hard_disabled_until"] = {}

    return now_ts() >= STATE["strategy_side_hard_disabled_until"].get(key, 0)


def is_strategy_side_grade_enabled(strategy: str, side: str, grade: str) -> bool:
    ensure_stats_structure()
    key = f"{strategy}:{side}:{grade}"
    return now_ts() >= STATE["strategy_side_grade_disabled_until"].get(key, 0)


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
                # Некоторые API возвращают 0.12 как 12%, другие — 12.0 как 12%.
                if abs(value) <= 2:
                    value *= 100
                return value
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
            if abs(change) >= EXTREME_DYNAMIC_MIN_CHANGE_PERCENT:
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
        if base in LIQUID_BASES or base in EXTREME_BASES or symbol in dynamic_extremes:
            priority_symbols.append(symbol)

    random.shuffle(all_symbols)

    # Сначала реальные movers + основные ликвидные, потом добираем остальной рынок.
    result = []
    for symbol in dynamic_extremes + priority_symbols + all_symbols:
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

    endpoints = [
        "/openApi/swap/v2/quote/premiumIndex",
        "/openApi/swap/v2/quote/fundingRate",
    ]

    for endpoint in endpoints:
        data = get_json(f"{BINGX_BASE_URL}{endpoint}", params={"symbol": normalize_symbol(symbol)})

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

    endpoints = [
        "/openApi/swap/v2/quote/openInterest",
        "/openApi/swap/v2/quote/openInterestStat",
    ]

    for endpoint in endpoints:
        data = get_json(f"{BINGX_BASE_URL}{endpoint}", params={"symbol": normalize_symbol(symbol)})

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


def estimate_trade_cost_percent() -> float:
    # Approximate round-trip trading cost in price-percent terms.
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
            score_adjustment -= 3
            reason.append(f"Funding немного перегрет для LONG: {funding:.6f}")

        elif direction == "SHORT" and funding < -MAX_ABS_FUNDING_RATE:
            score_adjustment -= 3
            reason.append(f"Funding немного перегрет для SHORT: {funding:.6f}")

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


def combine_extra_filters(symbol: str, direction: str, btc_status: str) -> dict:
    funding_oi = analyze_funding_oi(symbol, direction)

    btc_against = (
        (direction == "LONG" and btc_status == "BEARISH")
        or (direction == "SHORT" and btc_status == "BULLISH")
    )

    blocked = funding_oi.get("blocked", False) or btc_against

    score_adjustment = funding_oi.get("score_adjustment", 0)

    return {
        "blocked": blocked,
        "score_adjustment": score_adjustment,
        "funding": funding_oi,
        "btc_status": btc_status,
        "btc_against": btc_against,
    }



def get_strategy_winrate(strategy: str) -> tuple:
    """
    Возвращает (trades, winrate) по стратегии из текущей статистики.
    Если сделок мало, статистика считается недостаточной и не блокирует A+.
    """
    ensure_stats_structure()
    s = STATE.get("stats", {}).get("strategy", {}).get(strategy, {})
    positive = int(s.get("positive", 0))
    sl = int(s.get("sl", 0))
    trades = positive + sl
    wr = calc_winrate(positive, sl)
    return trades, wr


def get_strategy_side_winrate(strategy: str, direction: str) -> tuple:
    ensure_stats_structure()
    key = f"{strategy}:{direction}"
    s = STATE.get("stats", {}).get("strategy_side", {}).get(key, {})
    positive = int(s.get("positive", 0))
    sl = int(s.get("sl", 0))
    trades = positive + sl
    wr = calc_winrate(positive, sl)
    return trades, wr


def get_grade_winrate(grade: str) -> tuple:
    ensure_stats_structure()
    s = STATE.get("stats", {}).get("grade", {}).get(grade, {})
    positive = int(s.get("positive", 0))
    sl = int(s.get("sl", 0))
    trades = positive + sl
    wr = calc_winrate(positive, sl)
    return trades, wr


def is_statistically_bad_strategy_side(strategy: str, direction: str) -> bool:
    key = f"{strategy}:{direction}".upper()
    if key in PRO_BLOCKED_STRATEGY_SIDES:
        return True
    trades, wr = get_strategy_side_winrate(strategy, direction)
    return trades >= PRO_MIN_TRADES_TO_BLOCK and wr < PRO_MIN_WR_TO_ALLOW


def can_strategy_be_a_plus(strategy: str, direction: str) -> bool:
    """
    V4.6 Stats-Aware A+:
    - Momentum Scalper по умолчанию не может быть A+.
    - Если по стратегии уже есть достаточная статистика и WR ниже порога,
      стратегия временно не получает A+, а только B.
    - Новые стратегии с малой выборкой не душим заранее.
    """
    if strategy == "MOMENTUM_SCALPER" and not MOMENTUM_SCALPER_CAN_A_PLUS:
        return False

    # V10: если конкретная связка strategy+side уже доказала, что убыточная,
    # она не получает даже A+. Это исправляет проблему: B отключался, но A+ продолжал ловить SL.
    if is_statistically_bad_strategy_side(strategy, direction):
        return False

    if not STATS_AWARE_A_PLUS_ENABLED:
        return True

    trades, wr = get_strategy_winrate(strategy)

    if trades >= A_PLUS_MIN_STRATEGY_TRADES and wr < A_PLUS_MIN_STRATEGY_WR:
        return False

    return True

def is_level_strategy(strategy: str) -> bool:
    return strategy in {
        "LEVEL_BREAK_RETEST_SHORT",
        "LEVEL_SWEEP_BOUNCE_LONG",
        "LEVEL_BREAK_RETEST_LONG",
        "LEVEL_RESISTANCE_REJECT_SHORT",
    }


def classify_signal(score: int, rr: float, volume: float, filters: dict, strategy: str, direction: str) -> Optional[dict]:
    if filters.get("blocked"):
        return None

    if is_statistically_bad_strategy_side(strategy, direction):
        return None

    funding = filters.get("funding", {})

    # V4.8: контртрендовый LONG-отскок при bearish BTC может быть только B, не A+.
    if filters.get("force_grade") == "B":
        b_trades, b_wr = get_grade_winrate("B")
        if not B_SIGNALS_ENABLED:
            return None
        if PRO_DISABLE_B_AFTER_BAD_GLOBAL_STATS and b_trades >= PRO_MIN_TRADES_TO_BLOCK and b_wr < PRO_MIN_WR_TO_ALLOW:
            return None
        if B_ONLY_WITH_HIGH_POTENTIAL and not filters.get("high_potential", False):
            return None
        if (
            score >= B_MIN_SCORE
            and rr >= B_MIN_RR
            and volume >= B_MIN_VOLUME_RATIO
            and not funding.get("blocked")
            and (not filters.get("btc_against") or filters.get("allow_btc_countertrend_bounce"))
        ):
            return {
                "grade": "B",
                "risk_multiplier": filters.get("risk_multiplier_override", B_RISK_MULTIPLIER),
            }
        return None

    level_a_plus_allowed = True
    if is_level_strategy(strategy) and LEVEL_A_PLUS_REQUIRES_1H_CONFIRM:
        level_a_plus_allowed = filters.get("level_1h_confirmed", False)

    if (
        score >= A_PLUS_MIN_SCORE
        and rr >= A_PLUS_MIN_RR
        and volume >= A_PLUS_MIN_VOLUME_RATIO
        and not funding.get("blocked")
        and can_strategy_be_a_plus(strategy, direction)
        and level_a_plus_allowed
    ):
        return {
            "grade": "A+",
            "risk_multiplier": A_PLUS_RISK_MULTIPLIER,
        }

    b_score = B_MIN_SCORE
    b_rr = B_MIN_RR
    b_volume = B_MIN_VOLUME_RATIO

    # V4.9: уровневые B-сигналы разрешаем чуть живее,
    # потому что реальный рынок не всегда даёт идеальный учебниковый ретест.
    # A+ при этом остаётся строгим.
    if LEVEL_ACTIVE_B_ENABLED and is_level_strategy(strategy):
        b_score = LEVEL_B_MIN_SCORE
        b_rr = LEVEL_B_MIN_RR
        b_volume = LEVEL_B_MIN_VOLUME_RATIO

    b_trades, b_wr = get_grade_winrate("B")
    if not B_SIGNALS_ENABLED:
        return None
    if PRO_DISABLE_B_AFTER_BAD_GLOBAL_STATS and b_trades >= PRO_MIN_TRADES_TO_BLOCK and b_wr < PRO_MIN_WR_TO_ALLOW:
        return None

    if B_ONLY_WITH_HIGH_POTENTIAL and not filters.get("high_potential", False):
        return None

    if (
        score >= b_score
        and rr >= b_rr
        and volume >= b_volume
        and not funding.get("blocked")
        and (not filters.get("btc_against") or filters.get("allow_btc_countertrend_bounce"))
    ):
        return {
            "grade": "B",
            "risk_multiplier": filters.get("risk_multiplier_override", B_RISK_MULTIPLIER),
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
    extra_filters: dict
) -> Optional[dict]:
    risk_pos = calc_risk_position(entry, sl)

    if risk_pos > MAX_RISK_POSITION_PERCENT:
        return None

    score += extra_filters.get("score_adjustment", 0)

    if LEVEL_ACTIVE_B_ENABLED and is_level_strategy(strategy):
        score += LEVEL_SIGNAL_SCORE_BONUS

    tp1 = make_tp(entry, direction, TP1_POSITION_PERCENT)
    tp2 = make_tp(entry, direction, TP2_POSITION_PERCENT)
    tp3 = make_tp(entry, direction, TP3_POSITION_PERCENT)

    raw_reward = price_move_percent(entry, tp1, direction)
    risk_price = abs(entry - sl) / entry * 100
    trade_cost = estimate_trade_cost_percent()
    net_reward = max(raw_reward - trade_cost, 0)
    rr = net_reward / risk_price if risk_price > 0 else 0

    target_roi_tp1 = raw_reward * LEVERAGE
    target_roi_tp2 = price_move_percent(entry, make_tp(entry, direction, TP2_POSITION_PERCENT), direction) * LEVERAGE
    is_extreme = base_from_symbol(symbol) in EXTREME_BASES or strategy == "EXTREME_CONTEXT_PRO"
    high_potential = target_roi_tp1 >= MIN_TARGET_ROI_PERCENT
    extra_filters["high_potential"] = high_potential
    extra_filters["target_roi_tp1"] = round(target_roi_tp1, 2)
    extra_filters["target_roi_tp2"] = round(target_roi_tp2, 2)

    # V10: задача — минимум 10% ROI при x10 уже на TP1.
    # Если TP1 не даёт 10% ROI по позиции, сигнал не отправляется.
    if target_roi_tp1 < MIN_TARGET_ROI_PERCENT:
        return None

    # V10: слабые legacy B-кандидаты не конвертируются в сигнал.
    # Исключение: IMPULSE_PULLBACK_PRO выше больше не ставит force_grade=B.
    if extra_filters.get("force_grade") == "B":
        return None

    grade_data = classify_signal(score, rr, vol_ratio, extra_filters, strategy, direction)

    if grade_data is None:
        return None

    grade = grade_data["grade"]

    # V4.3:
    # B-блокировка проверяется только по grade=B и НЕ блокирует A+.
    # Жёсткая strategy+side блокировка действует только если именно A+ дал серию SL.
    if not is_strategy_side_enabled(strategy, direction):
        return None

    if not is_strategy_side_grade_enabled(strategy, direction, grade):
        return None

    risk_multiplier = grade_data["risk_multiplier"]
    adjusted_risk_percent = risk_percent * risk_multiplier

    signal_id = f"{normalize_symbol(symbol)}:{strategy}:{direction}:{grade}:{round(entry, 8)}"

    if signal_id in STATE["sent_signals"]:
        return None

    pos = calculate_position(entry, sl, deposit, adjusted_risk_percent)

    return {
        "id": signal_id,
        "symbol": normalize_symbol(symbol),
        "display_symbol": display_symbol(symbol),
        "direction": direction,
        "strategy": strategy,
        "grade": grade,
        "risk_multiplier": risk_multiplier,
        "status": "ACTIVE",
        "trade_style": extra_filters.get("trade_style") or ("⚡ FAST / быстрый импульс" if strategy in ["IMPULSE_PULLBACK_PRO", "EXTREME_CONTEXT_PRO"] and extra_filters.get("trade_class_override") != "STRUCTURE" else "🏗 STRUCTURE / нужно дать сделке время"),
        "expected_hold": extra_filters.get("expected_hold") or ("5–45 минут" if strategy in ["IMPULSE_PULLBACK_PRO", "EXTREME_CONTEXT_PRO"] and extra_filters.get("trade_class_override") != "STRUCTURE" else "1–6 часов"),
        "target_roi_tp1": extra_filters.get("target_roi_tp1"),
        "target_roi_tp2": extra_filters.get("target_roi_tp2"),
        "score": min(score, 98),
        "entry": round(entry, 8),
        "sl": round(sl, 8),
        "tp1": round(tp1, 8),
        "tp2": round(tp2, 8),
        "tp3": round(tp3, 8),
        "rr": round(rr, 2),
        "raw_reward_to_tp1_percent": round(raw_reward, 4),
        "net_reward_to_tp1_percent": round(net_reward, 4),
        "estimated_trade_cost_percent": round(trade_cost, 4),
        "volume_ratio": round(vol_ratio, 2),
        "risk_position_percent": round(risk_pos, 2),
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


def evaluate_breakout(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "BREAKOUT_MOMENTUM"

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

    if vr >= B_MIN_VOLUME_RATIO:
        score += 8

    if vr >= A_PLUS_MIN_VOLUME_RATIO:
        score += 4

    if vr >= 1.6:
        score += 4

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

        if not (50 <= rs <= 84):
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

        if not (16 <= rs <= 50):
            return None

        if trend1h in ["BEARISH", "SOFT_BEARISH"]:
            score += 7

        if trend4h in ["BEARISH", "SOFT_BEARISH"]:
            score += 3

        sl = max(level + a * 0.18, max(c["high"] for c in c15[-8:]) + a * 0.05)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr,
        reason="Пробой уровня с объёмом и подтверждением 1m/5m.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=combine_extra_filters(symbol, direction, btc_status),
    )


def evaluate_pullback(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "TREND_PULLBACK"

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

    if vr >= B_MIN_VOLUME_RATIO:
        score += 7

    if vr >= A_PLUS_MIN_VOLUME_RATIO:
        score += 3

    if momentum_confirm(c1, c5, direction):
        score += 12

    if direction == "LONG":
        if btc_status == "BEARISH":
            return None

        if trend1h not in ["BULLISH", "SOFT_BULLISH"]:
            return None

        pulled_to_vwap = price >= vw * 0.982 and price <= vw * 1.018
        bounce = last["close"] > last["open"] and last["close"] > prev["close"]

        if not pulled_to_vwap or not bounce:
            return None

        if rs > 66:
            return None

        if trend4h in ["BULLISH", "SOFT_BULLISH"]:
            score += 8

        sl = min(last["low"] - a * 0.2, min(c["low"] for c in c15[-8:]) - a * 0.05)

    else:
        if btc_status == "BULLISH":
            return None

        if trend1h not in ["BEARISH", "SOFT_BEARISH"]:
            return None

        pulled_to_vwap = price >= vw * 0.982 and price <= vw * 1.018
        rejection = last["close"] < last["open"] and last["close"] < prev["close"]

        if not pulled_to_vwap or not rejection:
            return None

        if rs < 34:
            return None

        if trend4h in ["BEARISH", "SOFT_BEARISH"]:
            score += 8

        sl = max(last["high"] + a * 0.2, max(c["high"] for c in c15[-8:]) + a * 0.05)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr,
        reason="Откат к VWAP по направлению 1h-тренда.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=combine_extra_filters(symbol, direction, btc_status),
    )


def evaluate_sweep(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "SWEEP_RECLAIM"

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

    if vr >= B_MIN_VOLUME_RATIO:
        score += 7

    if vr >= A_PLUS_MIN_VOLUME_RATIO:
        score += 3

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

        if price < vw * 0.970:
            return None

        if rs > 62:
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

        if price > vw * 1.030:
            return None

        if rs < 38:
            return None

        sl = max(prev["high"] + a * 0.08, max(c["high"] for c in c15[-8:]) + a * 0.05)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr,
        reason="Снятие ликвидности за уровень и возврат обратно.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=combine_extra_filters(symbol, direction, btc_status),
    )


def evaluate_momentum_scalper(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    strategy = "MOMENTUM_SCALPER"

    closes1 = [c["close"] for c in c1]
    closes5 = [c["close"] for c in c5]
    closes15 = [c["close"] for c in c15]

    if len(closes1) < 50 or len(closes5) < 50 or len(closes15) < 50:
        return None

    last1 = c1[-1]
    prev1 = c1[-2]
    last5 = c5[-1]
    price = last5["close"]

    a = atr(c5)
    vw = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    if a is None or vw is None or rs5 is None or rs15 is None:
        return None

    if late_entry_blocked(direction, c5, price, vw):
        return None

    ema9_1 = ema(closes1, 9)[-1]
    ema21_5 = ema(closes5, 21)[-1]
    ema50_15 = ema(closes15, 50)[-1]

    trend1h = trend_state(c1h)

    score = 58

    if vr5 >= 1.0:
        score += 8

    if vr5 >= 1.25:
        score += 5

    if direction == "LONG":
        if btc_status == "BEARISH":
            return None

        if trend1h == "BEARISH":
            return None

        impulse = (
            last1["close"] > last1["open"]
            and last1["close"] > prev1["close"]
            and last1["close"] > ema9_1
            and price > ema21_5
            and price > ema50_15
            and last5["close"] > last5["open"]
        )

        if not impulse:
            return None

        if not (48 <= rs5 <= 72):
            return None

        if not (45 <= rs15 <= 72):
            return None

        if price < vw * 0.992:
            return None

        score += 14

        if trend1h in ["BULLISH", "SOFT_BULLISH"]:
            score += 7

        sl = min(last5["low"] - a * 0.25, min(c["low"] for c in c5[-10:]) - a * 0.05)

    else:
        if btc_status == "BULLISH":
            return None

        if trend1h == "BULLISH":
            return None

        impulse = (
            last1["close"] < last1["open"]
            and last1["close"] < prev1["close"]
            and last1["close"] < ema9_1
            and price < ema21_5
            and price < ema50_15
            and last5["close"] < last5["open"]
        )

        if not impulse:
            return None

        if not (28 <= rs5 <= 52):
            return None

        if not (28 <= rs15 <= 55):
            return None

        if price > vw * 1.008:
            return None

        score += 14

        if trend1h in ["BEARISH", "SOFT_BEARISH"]:
            score += 7

        sl = max(last5["high"] + a * 0.25, max(c["high"] for c in c5[-10:]) + a * 0.05)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr5,
        reason="Короткий импульс 1m/5m по направлению 15m/1h, без позднего входа.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=combine_extra_filters(symbol, direction, btc_status),
    )


def evaluate_bear_continuation_retest(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    """
    BEAR_CONTINUATION_RETEST:
    Только SHORT. Логика: падение -> откат к VWAP/EMA/уровню -> слабость покупателей -> красная свеча подтверждения.
    Это не догоняет падение внизу, а ждёт ретест.
    """
    strategy = "BEAR_CONTINUATION_RETEST"

    if direction != "SHORT":
        return None

    if btc_status not in ["BEARISH", "SOFT_BEARISH"]:
        return None

    closes5 = [c["close"] for c in c5]
    closes15 = [c["close"] for c in c15]

    if len(closes5) < 80 or len(closes15) < 80:
        return None

    last5 = c5[-1]
    prev5 = c5[-2]
    price = last5["close"]

    a = atr(c5)
    vw = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    if a is None or vw is None or rs5 is None or rs15 is None:
        return None

    ema21_5 = ema(closes5, 21)[-1]
    ema50_15 = ema(closes15, 50)[-1]
    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    if trend1h == "BULLISH":
        return None

    # Цена должна быть ниже старшего давления, иначе это не bearish continuation.
    if price > ema50_15 * 1.004:
        return None

    # Было предыдущее снижение, затем откат. Не шортим без предшествующего импульса вниз.
    old_price = c5[-12]["close"]
    impulse_low = min(c["low"] for c in c5[-10:-3])
    impulse_move = (impulse_low - old_price) / old_price * 100 if old_price > 0 else 0

    if impulse_move > -0.35:
        return None

    # Ретест: цена/хай последней свечи вернулись к EMA21 5m или VWAP 15m.
    near_ema = abs(last5["high"] - ema21_5) / ema21_5 * 100 <= 0.65
    near_vwap = abs(last5["high"] - vw) / vw * 100 <= 0.9
    retest_zone = near_ema or near_vwap

    if not retest_zone:
        return None

    # Rejection: покупатели подняли цену к зоне, но свеча закрылась вниз.
    rejection = (
        last5["close"] < last5["open"]
        and last5["close"] < prev5["close"]
        and last5["close"] < ema21_5
    )

    if not rejection:
        return None

    # Откат должен быть слабее, чем импульс: не нужен мощный выкуп против шорта.
    impulse_vol = sum(c["volume"] for c in c5[-12:-6]) / 6
    pullback_vol = sum(c["volume"] for c in c5[-5:-1]) / 4
    weak_pullback = pullback_vol <= impulse_vol * 1.15 if impulse_vol > 0 else True

    if not weak_pullback:
        return None

    # RSI не должен быть экстремально перепродан, иначе высокий риск отскока.
    if rs5 < 24 or rs15 < 28:
        return None

    # Не догоняем слишком поздно после огромного движения.
    if late_entry_blocked(direction, c5, price, vw):
        return None

    score = 62

    if btc_status == "BEARISH":
        score += 8
    elif btc_status == "SOFT_BEARISH":
        score += 5

    if trend1h in ["BEARISH", "SOFT_BEARISH"]:
        score += 7

    if trend4h in ["BEARISH", "SOFT_BEARISH"]:
        score += 4

    if weak_pullback:
        score += 6

    if vr5 >= B_MIN_VOLUME_RATIO:
        score += 6

    if vr5 >= A_PLUS_MIN_VOLUME_RATIO:
        score += 4

    if price < vw and price < ema21_5:
        score += 4

    # Стоп за зону ретеста/последний локальный хай.
    recent_high = max(c["high"] for c in c5[-10:])
    sl = max(recent_high + a * 0.12, last5["high"] + a * 0.18)

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr5,
        reason="Падение → откат к VWAP/EMA → слабый выкуп → rejection вниз. SHORT только после ретеста, не внизу движения.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=combine_extra_filters(symbol, direction, btc_status),
    )



def find_swing_support_levels(candles: List[dict], lookback: int = 96) -> List[float]:
    """
    Ищет локальные уровни поддержки по swing-low на 15m.
    Возвращает сглаженный список уровней от нижних к верхним.
    """
    if len(candles) < 40:
        return []

    window = candles[-lookback:] if len(candles) >= lookback else candles[:]
    levels = []

    for i in range(3, len(window) - 3):
        low = window[i]["low"]
        left = [window[i - 1]["low"], window[i - 2]["low"], window[i - 3]["low"]]
        right = [window[i + 1]["low"], window[i + 2]["low"], window[i + 3]["low"]]

        if low <= min(left) and low <= min(right):
            levels.append(low)

    if not levels:
        return []

    levels.sort()
    merged = []

    for level in levels:
        if not merged:
            merged.append(level)
            continue

        # Объединяем близкие уровни, чтобы не плодить шум.
        if abs(level - merged[-1]) / merged[-1] * 100 <= 0.35:
            merged[-1] = (merged[-1] + level) / 2
        else:
            merged.append(level)

    return merged


def nearest_level_above(price: float, levels: List[float], max_distance_percent: float = 3.0) -> Optional[float]:
    candidates = [level for level in levels if level > price]
    if not candidates:
        return None

    level = min(candidates, key=lambda x: abs(x - price))
    if abs(level - price) / price * 100 > max_distance_percent:
        return None

    return level



def nearest_level_below(price: float, levels: List[float], max_distance_percent: float = 7.0) -> Optional[float]:
    """
    Ищет ближайшую поддержку ниже текущей цены.
    Нужна для sweep-bounce: цена могла уже отскочить от уровня на несколько процентов,
    но вход всё ещё может быть логичным после reclaim.
    """
    candidates = [level for level in levels if level < price]
    if not candidates:
        return None

    level = max(candidates)
    if abs(price - level) / level * 100 > max_distance_percent:
        return None

    return level


def find_swing_resistance_levels(candles: List[dict], lookback: int = 96) -> List[float]:
    """
    Ищет локальные уровни сопротивления по swing-high на 15m.
    Возвращает сглаженный список уровней от нижних к верхним.
    """
    if len(candles) < 40:
        return []

    window = candles[-lookback:] if len(candles) >= lookback else candles[:]
    levels = []

    for i in range(3, len(window) - 3):
        high = window[i]["high"]
        left = [window[i - 1]["high"], window[i - 2]["high"], window[i - 3]["high"]]
        right = [window[i + 1]["high"], window[i + 2]["high"], window[i + 3]["high"]]

        if high >= max(left) and high >= max(right):
            levels.append(high)

    if not levels:
        return []

    levels.sort()
    merged = []

    for level in levels:
        if not merged:
            merged.append(level)
            continue

        if abs(level - merged[-1]) / merged[-1] * 100 <= 0.35:
            merged[-1] = (merged[-1] + level) / 2
        else:
            merged.append(level)

    return merged


def nearest_resistance_below(price: float, levels: List[float], max_distance_percent: float = 3.2) -> Optional[float]:
    """
    Для breakout-long: сопротивление уже пробито и теперь находится ниже текущей цены.
    Ищем ближайший бывший resistance, который может стать support на ретесте.
    """
    candidates = [level for level in levels if level < price]
    if not candidates:
        return None

    level = max(candidates)
    if abs(price - level) / level * 100 > max_distance_percent:
        return None

    return level


def nearest_resistance_above(price: float, levels: List[float], max_distance_percent: float = 7.0) -> Optional[float]:
    """
    Для rejection-short: цена могла уже отойти вниз от сопротивления после sweep/rejection.
    Ищем ближайшее сопротивление выше текущей цены.
    """
    candidates = [level for level in levels if level > price]
    if not candidates:
        return None

    level = min(candidates)
    if abs(level - price) / price * 100 > max_distance_percent:
        return None

    return level

def nearest_level_near_price(price: float, levels: List[float], max_distance_percent: float = 1.2) -> Optional[float]:
    if not levels:
        return None

    level = min(levels, key=lambda x: abs(x - price))
    if abs(level - price) / price * 100 > max_distance_percent:
        return None

    return level


def candle_close_position(candle: dict) -> float:
    """
    Возвращает позицию закрытия внутри свечи:
    0 = закрылась у low, 1 = закрылась у high.
    Для SHORT rejection лучше, когда значение ближе к 0.
    Для LONG confirmation лучше, когда значение ближе к 1.
    """
    high = candle.get("high", 0)
    low = candle.get("low", 0)
    close = candle.get("close", 0)
    rng = high - low
    if rng <= 0:
        return 0.5
    return (close - low) / rng


def micro_confirm_below_level(c1: List[dict], level: float) -> bool:
    if len(c1) < LEVEL_MICRO_CONFIRM_CANDLES + 10:
        return False

    closes = [c["close"] for c in c1]
    ema9 = ema(closes, 9)[-1]
    recent = c1[-LEVEL_MICRO_CONFIRM_CANDLES:]
    buffer = 1 - LEVEL_CONFIRM_CLOSE_BUFFER_PERCENT / 100

    closes_below = all(c["close"] < level * buffer for c in recent)
    below_ema = recent[-1]["close"] < ema9
    lower_pressure = recent[-1]["close"] < recent[0]["open"]

    return closes_below and below_ema and lower_pressure


def micro_confirm_above_level(c1: List[dict], level: float) -> bool:
    if len(c1) < LEVEL_MICRO_CONFIRM_CANDLES + 10:
        return False

    closes = [c["close"] for c in c1]
    ema9 = ema(closes, 9)[-1]
    recent = c1[-LEVEL_MICRO_CONFIRM_CANDLES:]
    buffer = 1 + LEVEL_CONFIRM_CLOSE_BUFFER_PERCENT / 100

    closes_above = all(c["close"] > level * buffer for c in recent)
    above_ema = recent[-1]["close"] > ema9
    buy_pressure = recent[-1]["close"] > recent[0]["open"]

    return closes_above and above_ema and buy_pressure



def has_near_level(price_level: float, levels: List[float], max_distance_percent: float) -> bool:
    if price_level <= 0:
        return False
    for lvl in levels:
        if lvl <= 0:
            continue
        if abs(lvl - price_level) / price_level * 100 <= max_distance_percent:
            return True
    return False


def level_mtf_strength(level: float, c1h: List[dict], c4h: List[dict], kind: str) -> dict:
    """
    Проверяет, подтверждается ли 15m уровень на 1H/4H.
    kind: support или resistance.
    """
    if kind == "support":
        levels_1h = find_swing_support_levels(c1h, lookback=120)
        levels_4h = find_swing_support_levels(c4h, lookback=120)
    else:
        levels_1h = find_swing_resistance_levels(c1h, lookback=120)
        levels_4h = find_swing_resistance_levels(c4h, lookback=120)

    confirmed_1h = has_near_level(level, levels_1h, LEVEL_1H_CONFIRM_DISTANCE_PERCENT)
    confirmed_4h = has_near_level(level, levels_4h, LEVEL_4H_CONFIRM_DISTANCE_PERCENT)

    score_bonus = 0
    if confirmed_1h:
        score_bonus += LEVEL_STRENGTH_1H_BONUS
    if confirmed_4h:
        score_bonus += LEVEL_STRENGTH_4H_BONUS

    return {
        "level_1h_confirmed": confirmed_1h,
        "level_4h_confirmed": confirmed_4h,
        "level_strength_bonus": score_bonus,
        "level_strength_note": (
            f"1H {'OK' if confirmed_1h else 'NO'} / "
            f"4H {'OK' if confirmed_4h else 'NO'}"
        ),
    }

def evaluate_level_break_retest_short(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    """
    V5.0 LEVEL_BREAK_RETEST_SHORT:
    Пробой поддержки -> закрепление ниже -> ретест снизу -> rejection -> микро-подтверждение вниз -> SHORT.

    Главное изменение V5.0:
    - не даём A+ только по факту пробоя и объёма;
    - если цена быстро возвращается выше уровня, сигнал отменяется;
    - если подтверждение слабое, сигнал максимум B, а не A+.
    """
    strategy = "LEVEL_BREAK_RETEST_SHORT"

    if direction != "SHORT":
        return None

    if btc_status == "BULLISH":
        return None

    closes15 = [c["close"] for c in c15]
    closes5 = [c["close"] for c in c5]

    if len(closes15) < 100 or len(closes5) < 80 or len(c1) < 30:
        return None

    last5 = c5[-1]
    prev5 = c5[-2]
    price = last5["close"]

    a5 = atr(c5)
    vw = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    if a5 is None or vw is None or rs5 is None or rs15 is None:
        return None

    if late_entry_blocked(direction, c5, price, vw):
        return None

    ema21_5 = ema(closes5, 21)[-1]
    ema50_15 = ema(closes15, 50)[-1]
    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    if trend1h == "BULLISH":
        return None

    levels = find_swing_support_levels(c15, lookback=120)
    level = nearest_level_above(price, levels, max_distance_percent=LEVEL_BREAK_RETEST_MAX_DISTANCE_PERCENT)

    if level is None:
        return None

    recent_15 = c15[-10:]
    had_close_above = any(c["close"] > level * 1.001 for c in recent_15[:-2])
    now_below = c15[-1]["close"] < level * 0.998 or c15[-2]["close"] < level * 0.998

    if not had_close_above or not now_below:
        return None

    # Ретест снизу: цена вернулась к уровню, но не должна слишком глубоко прокалывать его вверх.
    touched_retest = last5["high"] >= level * 0.996
    max_allowed_pierce = level * (1 + LEVEL_RETEST_MAX_PIERCE_PERCENT / 100)
    too_deep_back_above = last5["high"] > max_allowed_pierce

    close_buffer = 1 - LEVEL_CONFIRM_CLOSE_BUFFER_PERCENT / 100
    rejected_below = (
        last5["close"] < level * close_buffer
        and last5["close"] < last5["open"]
        and last5["close"] < prev5["close"]
        and candle_close_position(last5) <= LEVEL_REJECTION_CLOSE_POSITION
    )

    if not touched_retest or too_deep_back_above or not rejected_below:
        return None

    # Анти-фейкаут: несколько последних 5m закрытий должны оставаться ниже уровня.
    recent5 = c5[-3:]
    closes_below_count = sum(1 for c in recent5 if c["close"] < level * close_buffer)
    two_5m_closes_below = closes_below_count >= 2

    if ANTI_FAKEOUT_LEVELS_ENABLED and not two_5m_closes_below:
        return None

    # Анти-фейкаут: последние 1m свечи не должны возвращаться выше уровня.
    micro_below = micro_confirm_below_level(c1, level)

    if ANTI_FAKEOUT_LEVELS_ENABLED and LEVEL_BREAK_A_PLUS_NEEDS_MICRO_CONFIRM and not micro_below:
        # Не отменяем полностью, но не даём A+ — только осторожный B.
        pass

    # Дополнительное давление: цена под EMA/VWAP или под EMA50 15m.
    if price > ema21_5 * 1.002:
        return None

    if price > ema50_15 * 1.004:
        return None

    # Не шортим, если уже слишком перепродано — высок риск отскока.
    if rs5 < 24 or rs15 < 28:
        return None

    # Откат к пробитому уровню не должен быть на экстремально сильном выкупе.
    recent_down_vol = sum(c["volume"] for c in c5[-12:-6]) / 6
    retest_vol = sum(c["volume"] for c in c5[-5:-1]) / 4
    if recent_down_vol > 0 and retest_vol > recent_down_vol * 1.25:
        return None

    score = 63

    if btc_status == "BEARISH":
        score += 8
    elif btc_status == "SOFT_BEARISH":
        score += 5

    if trend1h in ["BEARISH", "SOFT_BEARISH"]:
        score += 7

    if trend4h in ["BEARISH", "SOFT_BEARISH"]:
        score += 4

    if vr5 >= B_MIN_VOLUME_RATIO:
        score += 6

    if vr5 >= A_PLUS_MIN_VOLUME_RATIO:
        score += 4

    if price < vw:
        score += 3

    if abs(last5["high"] - level) / level * 100 <= 0.45:
        score += 3

    if two_5m_closes_below and micro_below:
        score += LEVEL_BREAK_SHORT_BONUS_AFTER_CONFIRM

    recent_high = max(c["high"] for c in c5[-8:])
    sl = max(level + a5 * 0.18, recent_high + a5 * 0.08)

    mtf = level_mtf_strength(level, c1h, c4h, kind="support")
    score += mtf.get("level_strength_bonus", 0)

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters.update(mtf)
    filters["anti_fakeout_note"] = (filters.get("anti_fakeout_note", "") + " " + mtf.get("level_strength_note", "")).strip()

    # Если подтверждение не идеальное, не даём A+ — только B.
    if LEVEL_BREAK_RETEST_FORCE_B_ON_WEAK_CONFIRM and not (two_5m_closes_below and micro_below):
        filters["force_grade"] = "B"
        filters["anti_fakeout_note"] = "A+ запрещён: нет полного 5m+1m подтверждения ниже пробитой поддержки."

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr5,
        reason="Пробой поддержки → закрепление ниже → ретест снизу → rejection + анти-фейкаут подтверждение. SHORT к следующей зоне поддержки.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=filters,
    )


def evaluate_level_sweep_bounce_long(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    """
    LEVEL_SWEEP_BOUNCE_LONG:
    Цена приходит к поддержке -> снимает ликвидность ниже -> возвращается выше -> зелёное подтверждение -> LONG.
    Не покупает поддержку вслепую, только после reclaim.
    """
    strategy = "LEVEL_SWEEP_BOUNCE_LONG"

    if direction != "LONG":
        return None

    # V4.8: bearish BTC больше не запрещает LONG от поддержки автоматически.
    # Такой сигнал разрешён только как осторожный B, после sweep/reclaim уровня.
    if btc_status == "BEARISH" and not ALLOW_BEARISH_BTC_LEVEL_BOUNCE:
        return None

    closes15 = [c["close"] for c in c15]
    closes5 = [c["close"] for c in c5]

    if len(closes15) < 100 or len(closes5) < 80:
        return None

    last5 = c5[-1]
    prev5 = c5[-2]
    price = last5["close"]

    a5 = atr(c5)
    vw = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    if a5 is None or vw is None or rs5 is None or rs15 is None:
        return None

    if late_entry_blocked(direction, c5, price, vw):
        return None

    ema21_5 = ema(closes5, 21)[-1]
    ema50_15 = ema(closes15, 50)[-1]
    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    # В bearish 1H обычные LONG не нужны, но sweep/reclaim от поддержки может дать короткий отскок.
    # Поэтому не запрещаем сразу, а требуем дальнейшие подтверждения ниже.
    if trend1h == "BEARISH" and btc_status != "BEARISH":
        return None

    levels = find_swing_support_levels(c15, lookback=140)

    # V4.6: ищем не только уровень прямо возле текущей цены.
    # Если был sweep 0.400 -> 0.380 и цена уже вернулась к 0.420,
    # старая логика могла пропустить сделку. Теперь ближайшая поддержка ниже цены
    # может быть до LEVEL_SWEEP_MAX_RECLAIM_DISTANCE_PERCENT от текущей цены.
    level = nearest_level_below(
        price,
        levels,
        max_distance_percent=LEVEL_SWEEP_MAX_RECLAIM_DISTANCE_PERCENT
    )

    if level is None:
        level = nearest_level_near_price(price, levels, max_distance_percent=2.0)

    if level is None:
        return None

    # Был sweep ниже поддержки и возврат выше уровня.
    sweep_window = c5[-LEVEL_SWEEP_LOOKBACK_CANDLES:]
    recent_lows = [c["low"] for c in sweep_window]
    sweep_low = min(recent_lows)
    swept = sweep_low < level * 0.997

    # Reclaim разрешаем не только в последней свече: иногда возврат уже произошёл,
    # а вход лучше появляется на следующем подтверждении.
    reclaimed = any(c["close"] > level * 1.001 for c in c5[-4:])

    if not swept or not reclaimed:
        return None

    # Подтверждение отскока: зелёная свеча и цена выше short EMA.
    bounce_confirm = (
        last5["close"] > last5["open"]
        and last5["close"] > prev5["close"]
        and last5["close"] >= ema21_5 * 0.995
    )

    if not bounce_confirm:
        return None

    # V4.8: если BTC bearish, LONG от поддержки разрешается только при явном reclaim:
    # цена выше уровня, свеча уверенно зелёная, объём хотя бы нормальный, вход не слишком далеко от уровня.
    if btc_status == "BEARISH":
        reclaim_distance = (price - level) / level * 100
        green_body = (last5["close"] - last5["open"]) / last5["open"] * 100 if last5["open"] > 0 else 0

        if price < level * 1.004:
            return None

        if reclaim_distance > 5.8:
            return None

        if green_body < 0.10:
            return None

        if vr5 < max(LEVEL_B_MIN_VOLUME_RATIO, 1.00):
            return None

    # Не покупаем, если старшая структура слишком сильно давит вниз.
    # Для bearish BTC допускаем только быстрый отскок, если цена не слишком далеко под EMA50 15m.
    if btc_status == "BEARISH":
        if price < ema50_15 * 0.975:
            return None
    elif price < ema50_15 * 0.985 and btc_status != "BULLISH":
        return None

    # RSI не должен быть слишком перегрет после отскока.
    if rs5 > 72 or rs15 > 68:
        return None

    score = 62

    if btc_status == "BULLISH":
        score += 8
    elif btc_status == "SOFT_BULLISH":
        score += 5
    elif btc_status == "BEARISH":
        # Контртрендовый отскок: баллы не завышаем, но даём шанс B при сильном reclaim.
        score += 1

    if trend1h in ["BULLISH", "SOFT_BULLISH"]:
        score += 7

    if trend4h in ["BULLISH", "SOFT_BULLISH"]:
        score += 4

    if vr5 >= B_MIN_VOLUME_RATIO:
        score += 6

    if vr5 >= A_PLUS_MIN_VOLUME_RATIO:
        score += 4

    if price > vw * 0.995:
        score += 3

    if abs(sweep_low - level) / level * 100 <= 0.8:
        score += 3

    sl = min(sweep_low - a5 * 0.12, min(c["low"] for c in c5[-10:]) - a5 * 0.05)

    mtf = level_mtf_strength(level, c1h, c4h, kind="support")
    score += mtf.get("level_strength_bonus", 0)

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters.update(mtf)
    filters["anti_fakeout_note"] = (filters.get("anti_fakeout_note", "") + " " + mtf.get("level_strength_note", "")).strip()

    if btc_status == "BEARISH" and ALLOW_BEARISH_BTC_LEVEL_BOUNCE:
        # Разрешаем только осторожный B-сигнал: это контртрендовый отскок, а не трендовый LONG.
        filters["blocked"] = filters.get("funding", {}).get("blocked", False)
        filters["btc_against"] = True
        filters["allow_btc_countertrend_bounce"] = True
        filters["force_grade"] = "B"
        filters["risk_multiplier_override"] = BEARISH_BTC_BOUNCE_RISK_MULTIPLIER
        filters["countertrend_note"] = "BTC bearish: LONG разрешён только как B от поддержки после sweep/reclaim."
        filters["score_adjustment"] = filters.get("score_adjustment", 0) - 2

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr5,
        reason="Поддержка удержалась: sweep ниже уровня → возврат выше поддержки → зелёное подтверждение. LONG на отскок.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=filters,
    )


def evaluate_level_break_retest_long(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    """
    LEVEL_BREAK_RETEST_LONG:
    Пробой сопротивления -> закрепление выше -> ретест уровня сверху -> подтверждение -> LONG.
    Не покупает просто зелёную свечу, ждёт превращения resistance в support.
    """
    strategy = "LEVEL_BREAK_RETEST_LONG"

    if direction != "LONG":
        return None

    if btc_status == "BEARISH":
        return None

    closes15 = [c["close"] for c in c15]
    closes5 = [c["close"] for c in c5]

    if len(closes15) < 100 or len(closes5) < 80:
        return None

    last5 = c5[-1]
    prev5 = c5[-2]
    price = last5["close"]

    a5 = atr(c5)
    vw = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    if a5 is None or vw is None or rs5 is None or rs15 is None:
        return None

    if late_entry_blocked(direction, c5, price, vw):
        return None

    # V4.9: не покупаем breakout-long, если монета уже сильно прошла вверх
    # до сигнала. Ждём настоящий ретест, а не догоняем памп.
    if recent_move_percent(c15, lookback=8) > MAX_LEVEL_LONG_15M_MOVE_PERCENT:
        return None

    ema21_5 = ema(closes5, 21)[-1]
    ema50_15 = ema(closes15, 50)[-1]
    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    if trend1h == "BEARISH":
        return None

    levels = find_swing_resistance_levels(c15, lookback=140)
    level = nearest_resistance_below(price, levels, max_distance_percent=LEVEL_BREAK_RETEST_MAX_DISTANCE_PERCENT)

    if level is None:
        return None

    recent_15 = c15[-10:]
    had_close_below = any(c["close"] < level * 0.999 for c in recent_15[:-2])
    now_above = c15[-1]["close"] > level * 1.002 or c15[-2]["close"] > level * 1.002

    if not had_close_below or not now_above:
        return None

    # Ретест сверху: цена вернулась к пробитому resistance, удержалась и дала зелёное подтверждение.
    touched_retest = last5["low"] <= level * 1.004
    held_above = last5["close"] > level * 1.001
    confirmed_up = last5["close"] > last5["open"] and last5["close"] > prev5["close"]

    if not touched_retest or not held_above or not confirmed_up:
        return None

    # V5.0 anti-fakeout: после пробоя сопротивления последние 5m/1m должны удерживаться выше уровня.
    close_buffer = 1 + LEVEL_CONFIRM_CLOSE_BUFFER_PERCENT / 100
    recent5 = c5[-3:]
    two_5m_closes_above = sum(1 for c in recent5 if c["close"] > level * close_buffer) >= 2
    micro_above = micro_confirm_above_level(c1, level)

    if ANTI_FAKEOUT_LEVELS_ENABLED and not two_5m_closes_above:
        return None

    # Ретест не должен слишком глубоко проваливаться обратно под уровень.
    min_allowed_pierce = level * (1 - LEVEL_RETEST_MAX_PIERCE_PERCENT / 100)
    if last5["low"] < min_allowed_pierce:
        return None

    if price < ema21_5 * 0.997:
        return None

    if price < ema50_15 * 0.992 and btc_status != "BULLISH":
        return None

    # Не покупаем слишком перегретый breakout после сильного рывка.
    if rs5 > 76 or rs15 > 72:
        return None

    # На ретесте не должно быть агрессивного продавца.
    recent_up_vol = sum(c["volume"] for c in c5[-12:-6]) / 6
    retest_vol = sum(c["volume"] for c in c5[-5:-1]) / 4
    if recent_up_vol > 0 and retest_vol > recent_up_vol * 1.45 and last5["close"] < level * 1.006:
        return None

    score = 63

    if btc_status == "BULLISH":
        score += 8
    elif btc_status == "SOFT_BULLISH":
        score += 5

    if trend1h in ["BULLISH", "SOFT_BULLISH"]:
        score += 7

    if trend4h in ["BULLISH", "SOFT_BULLISH"]:
        score += 4

    if vr5 >= B_MIN_VOLUME_RATIO:
        score += 6

    if vr5 >= A_PLUS_MIN_VOLUME_RATIO:
        score += 4

    if price > vw:
        score += 3

    if abs(last5["low"] - level) / level * 100 <= 0.45:
        score += 3

    recent_low = min(c["low"] for c in c5[-8:])
    sl = min(level - a5 * 0.18, recent_low - a5 * 0.08)

    mtf = level_mtf_strength(level, c1h, c4h, kind="resistance")
    score += mtf.get("level_strength_bonus", 0)

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters.update(mtf)
    filters["anti_fakeout_note"] = (filters.get("anti_fakeout_note", "") + " " + mtf.get("level_strength_note", "")).strip()

    # Если подтверждение пробоя вверх слабое, не даём A+ — только B.
    if LEVEL_BREAK_RETEST_FORCE_B_ON_WEAK_CONFIRM and not (two_5m_closes_above and micro_above):
        filters["force_grade"] = "B"
        filters["anti_fakeout_note"] = "A+ запрещён: нет полного 5m+1m подтверждения выше пробитого сопротивления."

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr5,
        reason="Пробой сопротивления → закрепление выше → ретест сверху → удержание уровня + анти-фейкаут подтверждение. LONG на продолжение роста.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=filters,
    )


def evaluate_level_resistance_reject_short(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    """
    LEVEL_RESISTANCE_REJECT_SHORT:
    Цена дошла до сопротивления -> сняла ликвидность выше -> вернулась ниже -> rejection -> SHORT.
    Это зеркальный вариант отскока от поддержки, только от сопротивления.
    """
    strategy = "LEVEL_RESISTANCE_REJECT_SHORT"

    if direction != "SHORT":
        return None

    if btc_status == "BULLISH":
        return None

    closes15 = [c["close"] for c in c15]
    closes5 = [c["close"] for c in c5]

    if len(closes15) < 100 or len(closes5) < 80:
        return None

    last5 = c5[-1]
    prev5 = c5[-2]
    price = last5["close"]

    a5 = atr(c5)
    vw = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    if a5 is None or vw is None or rs5 is None or rs15 is None:
        return None

    if late_entry_blocked(direction, c5, price, vw):
        return None

    ema21_5 = ema(closes5, 21)[-1]
    ema50_15 = ema(closes15, 50)[-1]
    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    if trend1h == "BULLISH":
        return None

    levels = find_swing_resistance_levels(c15, lookback=140)
    level = nearest_resistance_above(price, levels, max_distance_percent=LEVEL_RESISTANCE_REJECT_MAX_DISTANCE_PERCENT)

    if level is None:
        level = nearest_level_near_price(price, levels, max_distance_percent=2.0)

    if level is None:
        return None

    sweep_window = c5[-LEVEL_SWEEP_LOOKBACK_CANDLES:]
    sweep_high = max(c["high"] for c in sweep_window)
    swept = sweep_high > level * 1.0015

    rejected = any(c["close"] < level * 1.001 for c in c5[-4:])

    if not swept or not rejected:
        return None

    rejection_confirm = (
        last5["close"] < last5["open"]
        and last5["close"] < prev5["close"]
        and last5["close"] <= ema21_5 * 1.005
    )

    if not rejection_confirm:
        return None

    rejection_distance = (level - price) / level * 100 if level > 0 else 0
    if rejection_distance > 6.0:
        return None

    if price > ema50_15 * 1.025 and btc_status != "BEARISH":
        return None

    # Не шортим слишком перепроданный откат после rejection.
    if rs5 < 25 or rs15 < 30:
        return None

    score = 62

    if btc_status == "BEARISH":
        score += 8
    elif btc_status == "SOFT_BEARISH":
        score += 5

    if trend1h in ["BEARISH", "SOFT_BEARISH"]:
        score += 7

    if trend4h in ["BEARISH", "SOFT_BEARISH"]:
        score += 4

    if vr5 >= B_MIN_VOLUME_RATIO:
        score += 6

    if vr5 >= A_PLUS_MIN_VOLUME_RATIO:
        score += 4

    if price < vw * 1.005:
        score += 3

    # V4.9: если перед rejection был сильный рост, это усиливает идею короткого SHORT от сопротивления.
    if recent_move_percent(c5, lookback=12) > 3.0:
        score += 4

    if abs(sweep_high - level) / level * 100 <= 1.2:
        score += 3

    sl = max(sweep_high + a5 * 0.12, max(c["high"] for c in c5[-10:]) + a5 * 0.05)

    mtf = level_mtf_strength(level, c1h, c4h, kind="resistance")
    score += mtf.get("level_strength_bonus", 0)

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters.update(mtf)
    filters["anti_fakeout_note"] = (filters.get("anti_fakeout_note", "") + " " + mtf.get("level_strength_note", "")).strip()

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr5,
        reason="Сопротивление удержалось: sweep выше уровня → возврат ниже → красное подтверждение. SHORT от сопротивления.",
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=filters,
    )


def evaluate_impulse_pullback_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    """
    IMPULSE_PULLBACK_PRO:
    Осторожная стратегия только B: импульс -> откат к EMA/VWAP -> подтверждение.
    Цель — ловить быстрые сделки после нормального отката, а не покупать верх/шортить низ.
    """
    strategy = "IMPULSE_PULLBACK_PRO"

    if not IMPULSE_PULLBACK_ENABLED:
        return None

    if direction not in ["LONG", "SHORT"]:
        return None

    closes5 = [c["close"] for c in c5]
    closes15 = [c["close"] for c in c15]

    if len(closes5) < 80 or len(closes15) < 120 or len(c1) < 40:
        return None

    last5 = c5[-1]
    prev5 = c5[-2]
    price = last5["close"]

    a5 = atr(c5)
    vw = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)

    if a5 is None or vw is None or rs5 is None or rs15 is None:
        return None

    if vr5 < IMPULSE_MIN_VOLUME_RATIO:
        return None

    # Не берём импульс, если цена слишком далеко от VWAP: это часто поздний вход.
    if distance_from_vwap_percent(price, vw) > IMPULSE_MAX_DISTANCE_FROM_VWAP_PERCENT:
        return None

    ema9_1 = ema([c["close"] for c in c1], 9)[-1]
    ema21_5 = ema(closes5, 21)[-1]
    ema50_15 = ema(closes15, 50)[-1]
    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    # Смотрим импульс до отката: старое значение 12 свечей назад и экстремум до последних 3 свечей.
    old_price = c5[-15]["close"]
    recent_before_confirm = c5[-12:-3]
    if not recent_before_confirm or old_price <= 0:
        return None

    score = 58

    if direction == "LONG":
        if btc_status == "BEARISH" or trend1h == "BEARISH":
            return None

        impulse_high = max(c["high"] for c in recent_before_confirm)
        impulse_move = (impulse_high - old_price) / old_price * 100
        if impulse_move < IMPULSE_MIN_MOVE_5M_PERCENT:
            return None

        # Откат должен быть реальным, но не сломанным.
        pullback_low = min(c["low"] for c in c5[-6:-1])
        pullback_percent = (impulse_high - pullback_low) / impulse_high * 100 if impulse_high > 0 else 0
        if pullback_percent < IMPULSE_PULLBACK_MIN_PERCENT or pullback_percent > IMPULSE_PULLBACK_MAX_PERCENT:
            return None

        # Цена должна удерживать EMA/VWAP-зону и подтверждаться зелёной свечой.
        if price < ema21_5 * 0.995:
            return None
        if price < vw * 0.990 and btc_status != "BULLISH":
            return None
        if price < ema50_15 * 0.985 and trend1h != "BULLISH":
            return None

        confirm = (
            last5["close"] > last5["open"]
            and last5["close"] > prev5["close"]
            and c1[-1]["close"] > ema9_1
        )
        if not confirm:
            return None

        if rs5 > 74 or rs15 > 72:
            return None

        impulse_vol = sum(c["volume"] for c in c5[-12:-6]) / 6
        pullback_vol = sum(c["volume"] for c in c5[-6:-1]) / 5
        if impulse_vol > 0 and pullback_vol > impulse_vol * 1.25:
            return None

        score += 7 if btc_status == "BULLISH" else 4 if btc_status == "SOFT_BULLISH" else 0
        score += 6 if trend1h in ["BULLISH", "SOFT_BULLISH"] else 0
        score += 3 if trend4h in ["BULLISH", "SOFT_BULLISH"] else 0
        score += 5 if vr5 >= max(B_MIN_VOLUME_RATIO, IMPULSE_MIN_VOLUME_RATIO) else 0
        score += 3 if price > vw else 0
        score += 3 if pullback_percent <= 1.8 else 0

        sl = min(pullback_low - a5 * 0.14, min(c["low"] for c in c5[-8:]) - a5 * 0.05)
        reason = "Импульс вверх → контролируемый откат к EMA/VWAP → зелёное подтверждение. Только осторожный B LONG."

    else:
        if btc_status == "BULLISH" or trend1h == "BULLISH":
            return None

        impulse_low = min(c["low"] for c in recent_before_confirm)
        impulse_move = (old_price - impulse_low) / old_price * 100
        if impulse_move < IMPULSE_MIN_MOVE_5M_PERCENT:
            return None

        pullback_high = max(c["high"] for c in c5[-6:-1])
        pullback_percent = (pullback_high - impulse_low) / impulse_low * 100 if impulse_low > 0 else 0
        if pullback_percent < IMPULSE_PULLBACK_MIN_PERCENT or pullback_percent > IMPULSE_PULLBACK_MAX_PERCENT:
            return None

        if price > ema21_5 * 1.005:
            return None
        if price > vw * 1.010 and btc_status != "BEARISH":
            return None
        if price > ema50_15 * 1.015 and trend1h != "BEARISH":
            return None

        confirm = (
            last5["close"] < last5["open"]
            and last5["close"] < prev5["close"]
            and c1[-1]["close"] < ema9_1
        )
        if not confirm:
            return None

        if rs5 < 26 or rs15 < 28:
            return None

        impulse_vol = sum(c["volume"] for c in c5[-12:-6]) / 6
        pullback_vol = sum(c["volume"] for c in c5[-6:-1]) / 5
        if impulse_vol > 0 and pullback_vol > impulse_vol * 1.25:
            return None

        score += 7 if btc_status == "BEARISH" else 4 if btc_status == "SOFT_BEARISH" else 0
        score += 6 if trend1h in ["BEARISH", "SOFT_BEARISH"] else 0
        score += 3 if trend4h in ["BEARISH", "SOFT_BEARISH"] else 0
        score += 5 if vr5 >= max(B_MIN_VOLUME_RATIO, IMPULSE_MIN_VOLUME_RATIO) else 0
        score += 3 if price < vw else 0
        score += 3 if pullback_percent <= 1.8 else 0

        sl = max(pullback_high + a5 * 0.14, max(c["high"] for c in c5[-8:]) + a5 * 0.05)
        reason = "Импульс вниз → контролируемый откат к EMA/VWAP → красное подтверждение. Только осторожный B SHORT."

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters["trade_class_override"] = "FAST"
    filters["risk_multiplier_override"] = 0.35
    filters["anti_fakeout_note"] = "V10 FAST: импульсная сделка после отката. Не держать как долгую structure-сделку, если нет реакции."

    return build_signal(
        symbol=symbol,
        direction=direction,
        strategy=strategy,
        entry=price,
        sl=sl,
        score=score,
        vol_ratio=vr5,
        reason=reason,
        deposit=deposit,
        risk_percent=risk_percent,
        extra_filters=filters,
    )


def evaluate_range_structure_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    """
    V10 RANGE_STRUCTURE_PRO:
    Для рынка, который ходит туда-сюда в диапазоне. Это не быстрый скальп.
    Логика: цена у нижней/верхней части 1H-диапазона + подтверждение возврата + цель минимум 10% ROI при x10.
    """
    strategy = "RANGE_STRUCTURE_PRO"
    if direction not in ["LONG", "SHORT"]:
        return None
    if len(c1h) < 80 or len(c15) < 80 or len(c5) < 40:
        return None

    closes5 = [c["close"] for c in c5]
    closes15 = [c["close"] for c in c15]
    price = c5[-1]["close"]
    last5 = c5[-1]
    prev5 = c5[-2]
    a5 = atr(c5)
    vw = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)
    if a5 is None or vw is None or rs5 is None or rs15 is None:
        return None

    # Диапазон по 1H: достаточно широкий, чтобы TP1/TP2 имели смысл, но не хаотичная огромная свеча.
    recent = c1h[-72:]
    range_high = max(c["high"] for c in recent)
    range_low = min(c["low"] for c in recent)
    if range_low <= 0 or range_high <= range_low:
        return None
    range_width = (range_high - range_low) / range_low * 100
    if range_width < 2.2 or range_width > 14.0:
        return None
    pos = (price - range_low) / (range_high - range_low)
    trend1h = trend_state(c1h)
    trend4h = trend_state(c4h)

    # Если BTC против, обычные range-сделки не берём.
    if direction == "LONG" and btc_status == "BEARISH":
        return None
    if direction == "SHORT" and btc_status == "BULLISH":
        return None

    score = 78
    if vr5 >= 1.05:
        score += 5
    if vr5 >= A_PLUS_MIN_VOLUME_RATIO:
        score += 4

    if direction == "LONG":
        if pos > 0.30:
            return None
        if not (last5["close"] > last5["open"] and last5["close"] > prev5["close"]):
            return None
        if price < vw * 0.985:
            return None
        if rs5 < 28 or rs15 < 30 or rs5 > 62:
            return None
        if trend4h in ["BULLISH", "SOFT_BULLISH"]:
            score += 4
        if btc_status in ["BULLISH", "SOFT_BULLISH"]:
            score += 5
        sl = min(range_low - a5 * 0.30, min(c["low"] for c in c5[-12:]) - a5 * 0.08)
        reason = (
            f"V10 STRUCTURE RANGE: цена у нижней части диапазона 1H ({round(range_low,8)}–{round(range_high,8)}), "
            "есть возврат/зелёное подтверждение. Это не быстрая сделка: цель — движение к середине диапазона."
        )
    else:
        if pos < 0.70:
            return None
        if not (last5["close"] < last5["open"] and last5["close"] < prev5["close"]):
            return None
        if price > vw * 1.015:
            return None
        if rs5 > 72 or rs15 > 70 or rs5 < 38:
            return None
        if trend4h in ["BEARISH", "SOFT_BEARISH"]:
            score += 4
        if btc_status in ["BEARISH", "SOFT_BEARISH"]:
            score += 5
        sl = max(range_high + a5 * 0.30, max(c["high"] for c in c5[-12:]) + a5 * 0.08)
        reason = (
            f"V10 STRUCTURE RANGE: цена у верхней части диапазона 1H ({round(range_low,8)}–{round(range_high,8)}), "
            "есть rejection/красное подтверждение. Это не быстрая сделка: цель — движение к середине диапазона."
        )

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters["trade_class_override"] = "STRUCTURE"
    filters["trade_style"] = "🏗 STRUCTURE RANGE / сделке нужно дать время"
    filters["expected_hold"] = "1–6 часов"
    filters["anti_fakeout_note"] = "V10: range-сделка от границы диапазона. Если за 1–2 часа нет реакции, сетап слабеет."

    return build_signal(symbol, direction, strategy, price, sl, score, vr5, reason, deposit, risk_percent, filters)


def evaluate_extreme_context_pro(symbol, direction, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent):
    """
    V10 EXTREME_CONTEXT_PRO:
    Для рисковых и сильно движущихся монет. Разделяет:
    - EXTREME CONTINUATION: сильный рост/падение -> откат -> покупатель/продавец вернулся.
    - EXTREME REVERSAL: перегрев -> слом EMA/VWAP -> вход на откат/слив.
    """
    strategy = "EXTREME_CONTEXT_PRO"
    if direction not in ["LONG", "SHORT"] or len(c1h) < 30 or len(c15) < 80 or len(c5) < 60:
        return None

    base = base_from_symbol(symbol)
    price = c5[-1]["close"]
    old24 = c1h[-25]["close"] if len(c1h) >= 25 else c1h[0]["close"]
    if old24 <= 0:
        return None
    change24 = (price - old24) / old24 * 100
    if abs(change24) < 6 and base not in EXTREME_BASES:
        return None

    closes5 = [c["close"] for c in c5]
    closes15 = [c["close"] for c in c15]
    ema21_5 = ema(closes5, 21)[-1]
    ema50_15 = ema(closes15, 50)[-1]
    vw = vwap_like(c15)
    rs5 = rsi(closes5)
    rs15 = rsi(closes15)
    vr5 = volume_ratio(c5, period=24)
    a5 = atr(c5)
    if vw is None or rs5 is None or rs15 is None or a5 is None:
        return None

    last5, prev5 = c5[-1], c5[-2]
    recent_high = max(c["high"] for c in c5[-36:])
    recent_low = min(c["low"] for c in c5[-36:])
    pullback_from_high = (recent_high - price) / recent_high * 100 if recent_high > 0 else 0
    bounce_from_low = (price - recent_low) / recent_low * 100 if recent_low > 0 else 0
    score = 80
    if abs(change24) >= 12:
        score += 4
    if abs(change24) >= 25:
        score += 4
    if vr5 >= 1.08:
        score += 5
    if vr5 >= 1.30:
        score += 4

    if direction == "LONG":
        if btc_status == "BEARISH":
            return None
        continuation = change24 > 6 and 2.0 <= pullback_from_high <= 18.0 and price > ema21_5 and last5["close"] > last5["open"] and last5["close"] > prev5["close"]
        reversal = change24 < -10 and 3.0 <= bounce_from_low <= 16.0 and price > ema21_5 and price > vw * 0.995 and last5["close"] > last5["open"]
        if not (continuation or reversal):
            return None
        if rs5 > 76 or rs15 > 74:
            return None
        sl = min(recent_low - a5 * 0.18, min(c["low"] for c in c5[-10:]) - a5 * 0.08)
        if continuation:
            reason = f"V10 EXTREME CONTINUATION LONG: монета сильная за 24ч ({round(change24,2)}%), вход не на вершине: был откат {round(pullback_from_high,2)}% и возврат покупателя."
            filters_style = "🔥 EXTREME CONTINUATION / быстрый рискованный вход после отката"
        else:
            reason = f"V10 EXTREME REVERSAL LONG: монета сильно снижалась за 24ч ({round(change24,2)}%), появился отскок от локального дна {round(bounce_from_low,2)}% и возврат выше EMA/VWAP."
            filters_style = "🔥 EXTREME REVERSAL / быстрый рискованный отскок"
    else:
        if btc_status == "BULLISH":
            return None
        continuation = change24 < -6 and 2.0 <= bounce_from_low <= 18.0 and price < ema21_5 and last5["close"] < last5["open"] and last5["close"] < prev5["close"]
        reversal = change24 > 12 and 3.0 <= pullback_from_high <= 22.0 and price < ema21_5 and price < vw * 1.005 and last5["close"] < last5["open"]
        if not (continuation or reversal):
            return None
        if rs5 < 24 or rs15 < 26:
            return None
        sl = max(recent_high + a5 * 0.18, max(c["high"] for c in c5[-10:]) + a5 * 0.08)
        if continuation:
            reason = f"V10 EXTREME CONTINUATION SHORT: монета слабая за 24ч ({round(change24,2)}%), был откат {round(bounce_from_low,2)}% и продавец вернулся."
            filters_style = "🔥 EXTREME CONTINUATION / быстрый рискованный вход после отката"
        else:
            reason = f"V10 EXTREME REVERSAL SHORT: монета перегрета за 24ч ({round(change24,2)}%), пошёл слом EMA/VWAP и откат от high {round(pullback_from_high,2)}%."
            filters_style = "🔥 EXTREME REVERSAL / быстрый рискованный откат после перегрева"

    filters = combine_extra_filters(symbol, direction, btc_status)
    filters["trade_class_override"] = "FAST"
    filters["trade_style"] = filters_style
    filters["expected_hold"] = "10–60 минут"
    filters["risk_multiplier_override"] = 0.25
    filters["anti_fakeout_note"] = "V10: extreme-сделка только после отката/сломa, не вход на голой вершине/дне. Риск уменьшен."
    return build_signal(symbol, direction, strategy, price, sl, score, vr5, reason, deposit, risk_percent, filters)


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
        # V5.2 Balanced Risk-Aware Core Level Trader: только 4 основные стратегии по уровням.
        for func in [
            evaluate_range_structure_pro,
            evaluate_extreme_context_pro,
            evaluate_impulse_pullback_pro,
            evaluate_level_break_retest_short,
            evaluate_level_break_retest_long,
            evaluate_level_sweep_bounce_long,
            evaluate_level_resistance_reject_short,
        ]:
            signal = func(symbol, d, c15, c5, c1, c1h, c4h, btc_status, deposit, risk_percent)

            if signal:
                candidates.append(signal)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            1 if x["grade"] == "A+" else 0,
            x["score"],
            x["rr"],
            x["volume_ratio"]
        ),
        reverse=True
    )

    return candidates[0]


def build_message(signal: dict) -> str:
    arrow = "📈" if signal["direction"] == "LONG" else "📉"
    mode = "TEST" if TEST_MODE else "TRADE"

    strategy_names = {
        "BREAKOUT_MOMENTUM": "🚀 Пробой уровня",
        "TREND_PULLBACK": "📌 Откат по тренду",
        "SWEEP_RECLAIM": "🧲 Снятие ликвидности",
        "MOMENTUM_SCALPER": "⚡ Импульсный скальпинг",
        "BEAR_CONTINUATION_RETEST": "🐻 Продолжение падения после отката",
        "LEVEL_BREAK_RETEST_SHORT": "📉 Пробой поддержки + ретест",
        "LEVEL_SWEEP_BOUNCE_LONG": "🟢 Отскок от поддержки после sweep",
        "LEVEL_BREAK_RETEST_LONG": "📈 Пробой сопротивления + ретест",
        "LEVEL_RESISTANCE_REJECT_SHORT": "🔴 Отбой от сопротивления после sweep",
        "IMPULSE_PULLBACK_PRO": "⚡ Fast Impulse Pullback Pro",
        "RANGE_STRUCTURE_PRO": "🏗 Range Structure Pro",
        "EXTREME_CONTEXT_PRO": "🔥 Extreme Context Pro",
    }

    strategy_text = strategy_names.get(signal["strategy"], signal["strategy"])

    pos = signal["position"]

    if pos.get("error"):
        risk_text = f"⚠️ Ошибка RM: {pos['error']}"
    else:
        risk_text = (
            f"Риск: {signal['risk_percent']:.2f}% депозита\n"
            f"Размер позиции: {pos['position_size_usdt']} USDT\n"
            f"Маржа x10: {pos['margin_10x']} USDT"
        )

    filters = signal.get("filters", {})
    funding = filters.get("funding", {})
    funding_text = funding.get("reason", "Funding/OI: нет данных")
    countertrend_note = filters.get("countertrend_note", "")
    anti_fakeout_note = filters.get("anti_fakeout_note", "")

    if anti_fakeout_note:
        funding_text += f"\nAnti-fakeout: {anti_fakeout_note}"

    grade_text = "HIGH SIGNAL" if signal["grade"] == "A+" else "B SIGNAL"

    caution = ""
    if signal["grade"] == "B":
        caution = "\n⚠️ B-сигнал включён вручную через ALLOW_B_SIGNALS_EXPLICIT. Основной режим V10 — HIGH/A+."

    return f"""
🎯 <b>{mode} {grade_text}</b>

{arrow} <b>{signal['direction']} {signal['display_symbol']}</b>

<b>Стратегия:</b> {strategy_text}
<b>Тип сделки:</b> {signal.get('trade_style', 'STRUCTURE')}
<b>Ожидание:</b> {signal.get('expected_hold', '15–180 минут')}
<b>Цель по позиции:</b> TP1 ≈ {signal.get('target_roi_tp1', 'n/a')}% ROI / TP2 ≈ {signal.get('target_roi_tp2', 'n/a')}% ROI при x{LEVERAGE}

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
{countertrend_note}

<b>Качество:</b> {signal['score']}/100
<b>RR до TP1 net:</b> {signal['rr']}
<b>TP1 gross/net:</b> {signal.get('raw_reward_to_tp1_percent', 0)}% / {signal.get('net_reward_to_tp1_percent', 0)}%
<b>Издержки:</b> ~{signal.get('estimated_trade_cost_percent', 0)}%
<b>Объём:</b> x{signal['volume_ratio']}
<b>Риск до SL:</b> {signal['risk_position_percent']}% по позиции

{risk_text}
{caution}

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
    ensure_stats_structure()

    side = signal["direction"]
    strategy = signal["strategy"]
    symbol = normalize_symbol(signal["symbol"])
    grade = signal.get("grade", "A+")
    strategy_side_key = f"{strategy}:{side}"
    strategy_side_grade_key = f"{strategy}:{side}:{grade}"

    notes = []

    if result == "SL" and not signal.get("counted_sl"):
        signal["counted_sl"] = True

        STATE["stats"]["side"][side]["sl"] += 1
        STATE["stats"]["side"][side]["consecutive_sl"] += 1

        STATE["stats"]["strategy"][strategy]["sl"] += 1
        STATE["stats"]["strategy"][strategy]["consecutive_sl"] += 1

        STATE["stats"]["strategy_side"][strategy_side_key]["sl"] += 1
        STATE["stats"]["strategy_side"][strategy_side_key]["consecutive_sl"] += 1

        STATE["stats"]["strategy_side_grade"][strategy_side_grade_key]["sl"] += 1
        STATE["stats"]["strategy_side_grade"][strategy_side_grade_key]["consecutive_sl"] += 1

        STATE["stats"]["grade"][grade]["sl"] += 1

        STATE["stats"]["pair_sl"][symbol] = STATE["stats"]["pair_sl"].get(symbol, 0) + 1

        if STATE["stats"]["pair_sl"][symbol] >= PAIR_MAX_SL:
            STATE["blocked_symbols"][symbol] = now_ts() + PAIR_BLOCK_SECONDS
            notes.append(f"🚫 {display_symbol(symbol)} заблокирован после SL.")

        # Важно: B-сигналы блокируют только B-связку.
        # A+ по той же стратегии/направлению сможет пройти даже если B временно отключён.
        if STATE["stats"]["strategy_side_grade"][strategy_side_grade_key]["consecutive_sl"] >= STRATEGY_SIDE_MAX_CONSECUTIVE_SL:
            if grade == "B":
                STATE["strategy_side_grade_disabled_until"][strategy_side_grade_key] = now_ts() + STRATEGY_SIDE_DISABLE_SECONDS
                STATE["stats"]["strategy_side_grade"][strategy_side_grade_key]["consecutive_sl"] = 0
                notes.append(f"⛔ B {strategy} {side} отключён после серии SL. A+ по этой связке разрешён.")
            else:
                STATE["strategy_side_hard_disabled_until"][strategy_side_key] = now_ts() + STRATEGY_SIDE_DISABLE_SECONDS
                STATE["stats"]["strategy_side"][strategy_side_key]["consecutive_sl"] = 0
                STATE["stats"]["strategy_side_grade"][strategy_side_grade_key]["consecutive_sl"] = 0
                notes.append(f"⛔ A+ {strategy} {side} дал серию SL — вся связка отключена временно.")

    elif result in ["TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        STATE["stats"]["side"][side]["consecutive_sl"] = 0
        STATE["stats"]["strategy"][strategy]["consecutive_sl"] = 0
        STATE["stats"]["strategy_side"][strategy_side_key]["consecutive_sl"] = 0
        STATE["stats"]["strategy_side_grade"][strategy_side_grade_key]["consecutive_sl"] = 0

        if not signal.get("counted_positive"):
            signal["counted_positive"] = True

            STATE["stats"]["side"][side]["positive"] += 1
            STATE["stats"]["strategy"][strategy]["positive"] += 1
            STATE["stats"]["strategy_side"][strategy_side_key]["positive"] += 1
            STATE["stats"]["strategy_side_grade"][strategy_side_grade_key]["positive"] += 1
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
    strategy_names = {
        "BREAKOUT_MOMENTUM": "🚀 Пробой уровня",
        "TREND_PULLBACK": "📌 Откат по тренду",
        "SWEEP_RECLAIM": "🧲 Снятие ликвидности",
        "MOMENTUM_SCALPER": "⚡ Импульсный скальпинг",
        "BEAR_CONTINUATION_RETEST": "🐻 Продолжение падения после отката",
        "LEVEL_BREAK_RETEST_SHORT": "📉 Пробой поддержки + ретест",
        "LEVEL_SWEEP_BOUNCE_LONG": "🟢 Отскок от поддержки после sweep",
        "LEVEL_BREAK_RETEST_LONG": "📈 Пробой сопротивления + ретест",
        "LEVEL_RESISTANCE_REJECT_SHORT": "🔴 Отбой от сопротивления после sweep",
        "IMPULSE_PULLBACK_PRO": "⚡ Fast Impulse Pullback Pro",
        "RANGE_STRUCTURE_PRO": "🏗 Range Structure Pro",
        "EXTREME_CONTEXT_PRO": "🔥 Extreme Context Pro",
    }

    strategy_text = strategy_names.get(signal["strategy"], signal["strategy"])

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

<b>{signal.get('grade', 'A+')} · {signal['direction']} {signal['display_symbol']}</b>
<b>Стратегия:</b> {strategy_text}

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


def scan_best_signal(deposit: float, risk_percent: float) -> dict:
    cleanup_state()
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
            current_key = (
                1 if signal["grade"] == "A+" else 0,
                signal["score"],
                signal["rr"],
                signal.get("target_roi_tp2") or 0,
                signal["volume_ratio"]
            )
            best_key = (
                1 if best["grade"] == "A+" else 0,
                best["score"],
                best["rr"],
                best["volume_ratio"]
            )

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


def is_signal_expired(signal: dict) -> bool:
    created_at = signal.get("created_at", 0)
    return bool(created_at and now_ts() - created_at > SIGNAL_MAX_LIFETIME_SECONDS)


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
                "grade": signal.get("grade", "A+"),
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

                        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or telegram.get("ok") is True:
                            save_signal(signal)
                        else:
                            STATE["auto"]["last_error"] = f"Telegram не отправил сигнал: {telegram}"
                    else:
                        # V9: no-signal report по умолчанию выключен, чтобы Telegram не спамил.
                        last_report = STATE["auto"].get("last_no_signal_report_time", 0)
                        if DEBUG_NO_SIGNAL_REPORT_ENABLED and current_time - last_report >= DEBUG_NO_SIGNAL_REPORT_SECONDS:
                            report = (
                                "🧠 <b>Диагностика V10</b>\n\n"
                                f"Проверено пар: {result.get('checked', 0)}\n"
                                "Сильных A+/B сигналов пока нет.\n\n"
                                "Что это обычно значит:\n"
                                "• нет нормального ретеста уровня;\n"
                                "• вход получается поздним после импульса;\n"
                                "• RR слабый;\n"
                                "• объём не подтверждает;\n"
                                "• BTC-фильтр против направления;\n"
                                "• funding/OI не дают подтверждение.\n\n"
                                "Бот продолжает сканировать рынок."
                            )
                            send_telegram_message(report)
                            STATE["auto"]["last_no_signal_report_time"] = current_time

                    save_state(STATE)

            await asyncio.sleep(15)

        except Exception as e:
            STATE["auto"]["last_error"] = str(e)
            save_state(STATE)
            await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    text = (
        f"✅ {APP_NAME} запущен.\n"
        f"Deploy marker: {DEPLOY_MARKER}\n\n"
        f"Режим: {'TEST' if TEST_MODE else 'TRADE'}\n"
        f"Auto Scan: {'ON' if AUTO_SCAN_ENABLED else 'OFF'} / {AUTO_SCAN_SECONDS} сек.\n"
        f"Auto Track: {'ON' if AUTO_TRACK_ENABLED else 'OFF'} / {AUTO_TRACK_SECONDS} сек.\n"
        f"No-signal spam: {'ON' if DEBUG_NO_SIGNAL_REPORT_ENABLED else 'OFF'}\n\n"
        f"A+ pro: score {A_PLUS_MIN_SCORE}+ / RR {A_PLUS_MIN_RR}+ / vol x{A_PLUS_MIN_VOLUME_RATIO}\n"
        f"B signals: {'ON' if B_SIGNALS_ENABLED else 'OFF'} / V10 default OFF, только HIGH/A+ сигналы\n"
        f"Target model: TP1 {TP1_POSITION_PERCENT}% ROI / TP2 {TP2_POSITION_PERCENT}% ROI / TP3 {TP3_POSITION_PERCENT}% ROI at x{LEVERAGE}\n"
        f"Minimum target filter: {MIN_TARGET_ROI_PERCENT}% ROI by TP1\n"
        f"Dynamic extreme scanner: {'ON' if EXTREME_DYNAMIC_ENABLED else 'OFF'} / top {EXTREME_DYNAMIC_TOP_N} / ±{EXTREME_DYNAMIC_MIN_CHANGE_PERCENT}%\n"
        f"Max symbols: {MAX_SYMBOLS}\n\n"
        "V10 логика: BTC/рынок → лучший сетап → потенциал 10%+ ROI → только A+ по умолчанию; B выключен после слабой статистики.\n"
        "Сделка помечается как FAST или STRUCTURE, чтобы понимать: быстрый импульс или нужно дать цене время.\n"
        "TP/SL уведомления, статистика и блокировка strategy+side+grade сохранены."
    )

    send_telegram_message(text)

    asyncio.create_task(auto_worker())

@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Professional Adaptive Futures Bot AUTO V10 PRO MARKET CONTEXT TRADER</title>
</head>
<body style="background:#020617;color:#e5e7eb;font-family:Arial;padding:40px;">
    <h1>✅ Professional Adaptive Futures Bot AUTO V10 PRO MARKET CONTEXT TRADER работает</h1>
    <pre>
GET /health
GET /scan?send_to_telegram=false
GET /auto-signal?symbol=NEAR/USDT
GET /track
GET /stats
GET /auto-status
GET /test-telegram
GET /reset-state
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
        "fee_rate": FEE_RATE,
        "slippage_rate": SLIPPAGE_RATE,
        "signal_max_lifetime_seconds": SIGNAL_MAX_LIFETIME_SECONDS,
        "a_plus_min_score": A_PLUS_MIN_SCORE,
        "b_min_score": B_MIN_SCORE,
        "a_plus_min_volume_ratio": A_PLUS_MIN_VOLUME_RATIO,
        "b_min_volume_ratio": B_MIN_VOLUME_RATIO,
        "a_plus_min_rr": A_PLUS_MIN_RR,
        "b_min_rr": B_MIN_RR,
        "auto_scan_enabled": AUTO_SCAN_ENABLED,
        "auto_track_enabled": AUTO_TRACK_ENABLED,
        "bearish_btc_level_bounce": ALLOW_BEARISH_BTC_LEVEL_BOUNCE,
        "bearish_btc_bounce_risk_multiplier": BEARISH_BTC_BOUNCE_RISK_MULTIPLIER,
        "level_active_b_enabled": LEVEL_ACTIVE_B_ENABLED,
        "level_b_min_score": LEVEL_B_MIN_SCORE,
        "level_b_min_rr": LEVEL_B_MIN_RR,
        "level_b_min_volume_ratio": LEVEL_B_MIN_VOLUME_RATIO,
        "debug_no_signal_report_enabled": DEBUG_NO_SIGNAL_REPORT_ENABLED,
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


@app.get("/version")
def version():
    return {
        "ok": True,
        "service": APP_NAME,
        "deploy_marker": DEPLOY_MARKER,
        "a_plus_min_score": A_PLUS_MIN_SCORE,
        "b_min_score": B_MIN_SCORE,
        "target_roi_tp1": TP1_POSITION_PERCENT,
        "target_roi_tp2": TP2_POSITION_PERCENT,
        "dynamic_extreme_scanner": EXTREME_DYNAMIC_ENABLED,
        "no_signal_spam": DEBUG_NO_SIGNAL_REPORT_ENABLED,
    }


@app.get("/test-telegram")
def test_telegram():
    return send_telegram_message(f"✅ {APP_NAME} подключён к Telegram. Deploy marker: {DEPLOY_MARKER}")


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
    risk_percent: float = Query(default=DEFAULT_RISK_PERCENT)
):
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
def track(send_to_telegram: bool = Query(default=True)):
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
        "strategy_side_disabled_until": {
            k: int(v - now_ts())
            for k, v in STATE["strategy_side_disabled_until"].items()
            if v > now_ts()
        },
    }


@app.get("/cleanup-state")
def cleanup_state_endpoint():
    cleanup_state()
    return {
        "ok": True,
        "message": "State cleanup completed.",
        "sent_signals": len(STATE.get("sent_signals", {})),
        "cooldowns": len(STATE.get("symbol_cooldown", {})),
        "blocked_symbols": len(STATE.get("blocked_symbols", {})),
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
