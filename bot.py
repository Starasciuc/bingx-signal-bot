import os
import time
import json
import math
import random
import asyncio
import logging
import tempfile
import threading
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

# ============================================================
# V14.0 — PROFESSIONAL SIGNAL BOT (fixed & hardened)
#
# This bot ONLY sends signals to Telegram. It never opens trades.
# It does not predict market phase and does not guarantee profit.
#
# Core idea (unchanged):
# hot coin -> fresh imbalance -> micro pullback / liquidity grab ->
# EMA/VWAP reclaim/reject -> immediate continuation -> compact 5-target ladder.
# If the trade does not start paying quickly, it is expired.
#
# What changed vs V13.29 (full list in the chat message):
# - all blocking network work moved off the asyncio event loop (asyncio.to_thread)
#   so SL/TP tracking is never starved by a long scan;
# - atomic state persistence, thread-safe STATE access;
# - fixed broken constants (compression filter, micro-break distance);
# - local scalp stop now actually applies to all fast scalp modes;
# - circuit breaker (consecutive SL / daily SL limit) with auto-pause;
# - symmetric LONG and SHORT live-stats protection;
# - per-side active-signal limit (correlated risk control);
# - duplicate-signal protection (same symbol+side never active twice);
# - higher-TF indicators use CLOSED candles only (no look-ahead on 15m/1h);
# - RSI extreme filter for continuation entries (anti-chase);
# - Telegram message chunking + retry; API session with retry/backoff;
# - watchdog that alerts if scan/track loops go stale.
# ============================================================

APP_NAME = "Professional Adaptive Futures Signal Bot V14.0"
DEPLOY_MARKER = "V14_0_HARDENED_2026_07_08"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("signalbot")

# ------------------------------------------------------------
# Env helpers (all config through environment / .env)
# ------------------------------------------------------------

def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default)

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, "true" if default else "false").strip().lower() == "true"

# ------------------------------------------------------------
# Core config
# ------------------------------------------------------------

BINGX_BASE_URL = env_str("BINGX_BASE_URL", "https://open-api.bingx.com")
TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN")          # required, no hardcode
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID")              # required, no hardcode

STATE_FILE = env_str("STATE_FILE", "bot_state_v14.json")
LEVERAGE = env_int("LEVERAGE", 10)
TEST_MODE = env_bool("TEST_MODE", True)

AUTO_SCAN_ENABLED = env_bool("AUTO_SCAN_ENABLED", True)
AUTO_TRACK_ENABLED = env_bool("AUTO_TRACK_ENABLED", True)
AUTO_SCAN_SECONDS = env_int("AUTO_SCAN_SECONDS", 20)
AUTO_TRACK_SECONDS = env_int("AUTO_TRACK_SECONDS", 3)
REQUEST_TIMEOUT = env_float("REQUEST_TIMEOUT", 8)
API_THROTTLE_SECONDS = env_float("API_THROTTLE_SECONDS", 0.04)
MAX_CONTRACTS = env_int("MAX_CONTRACTS", 450)
MAX_ANALYZE_SYMBOLS = env_int("MAX_ANALYZE_SYMBOLS", 150)
HOT_SYMBOLS_TO_ANALYZE = env_int("HOT_SYMBOLS_TO_ANALYZE", 50)
DIAG_SECONDS = env_int("DIAG_SECONDS", 1200)
WATCHDOG_STALE_MINUTES = env_int("WATCHDOG_STALE_MINUTES", 10)

# --- Signal limits ---
A_PLUS_MIN_SCORE = env_int("A_PLUS_MIN_SCORE", 88)
B_MIN_SCORE = env_int("B_MIN_SCORE", 78)
MAX_ACTIVE_SIGNALS = env_int("MAX_ACTIVE_SIGNALS", 2)
MAX_ACTIVE_SAME_SIDE = env_int("MAX_ACTIVE_SAME_SIDE", 1)   # correlated-risk control
MAX_SIGNALS_PER_SCAN = env_int("MAX_SIGNALS_PER_SCAN", 2)
PAIR_COOLDOWN_SECONDS = env_int("PAIR_COOLDOWN_SECONDS", 900)
STRATEGY_COOLDOWN_SECONDS = env_int("STRATEGY_COOLDOWN_SECONDS", 180)

# --- Circuit breaker (new) ---
CIRCUIT_BREAKER_ENABLED = env_bool("CIRCUIT_BREAKER_ENABLED", True)
MAX_CONSECUTIVE_SL = env_int("MAX_CONSECUTIVE_SL", 3)
MAX_DAILY_SL = env_int("MAX_DAILY_SL", 6)
CIRCUIT_BREAKER_PAUSE_MINUTES = env_int("CIRCUIT_BREAKER_PAUSE_MINUTES", 120)

# --- Fast burst requirements ---
FAST_BURST_ENABLED = env_bool("FAST_BURST_ENABLED", True)
FAST_MIN_15M_MOVE = env_float("FAST_MIN_15M_MOVE", 0.0045)
FAST_MIN_30M_MOVE = env_float("FAST_MIN_30M_MOVE", 0.0070)
FAST_MAX_30M_MOVE = env_float("FAST_MAX_30M_MOVE", 0.090)
FAST_MIN_RANGE_RATIO = env_float("FAST_MIN_RANGE_RATIO", 0.85)
FAST_MIN_VOLUME_RATIO = env_float("FAST_MIN_VOLUME_RATIO", 0.45)
FAST_MIN_1M_CONFIRM = env_float("FAST_MIN_1M_CONFIRM", 0.0008)

# Reversal now requires a REAL counter-move, not 0.12% noise.
REVERSAL_ENABLED = env_bool("REVERSAL_ENABLED", True)
REVERSAL_MIN_30M_MOVE = env_float("REVERSAL_MIN_30M_MOVE", 0.020)
REVERSAL_MIN_LIVE_COUNTER_MOVE = env_float("REVERSAL_MIN_LIVE_COUNTER_MOVE", 0.0035)

LIVE_BYPASS_VOLUME_MOVE = env_float("LIVE_BYPASS_VOLUME_MOVE", 0.0040)
LIVE_BYPASS_RANGE_RATIO = env_float("LIVE_BYPASS_RANGE_RATIO", 1.40)
FAST_MAX_SPREAD_PROXY = env_float("FAST_MAX_SPREAD_PROXY", 0.030)

# FIXED: was 99.0 (filter dead). Prior 5m ranges must be <= 0.95x older ranges
# OR the current expansion must be strong. Flat-market protection now works.
EDGE_MIN_PRIOR_COMPRESSION = env_float("EDGE_MIN_PRIOR_COMPRESSION", 0.95)
# FIXED: was 0.00005 (0.005%, pure noise). Now a real 0.12% micro break.
EDGE_MIN_BREAKOUT_DISTANCE = env_float("EDGE_MIN_BREAKOUT_DISTANCE", 0.0012)
EDGE_REQUIRE_MICRO_SWEEP = env_bool("EDGE_REQUIRE_MICRO_SWEEP", False)
EDGE_MIN_TP5_FEASIBILITY = env_float("EDGE_MIN_TP5_FEASIBILITY", 0.50)

# --- Realtime pressure gate ---
HOT_MIN_SCORE = env_float("HOT_MIN_SCORE", 14)
HOT_MIN_LIVE_MOVE_3M = env_float("HOT_MIN_LIVE_MOVE_3M", 0.0006)
HOT_MIN_LIVE_RANGE_OR_VOLUME = env_float("HOT_MIN_LIVE_RANGE_OR_VOLUME", 0.70)
HOT_STALE_PENALTY_ENABLED = env_bool("HOT_STALE_PENALTY_ENABLED", True)
REALTIME_MIN_1M_RANGE_RATIO = env_float("REALTIME_MIN_1M_RANGE_RATIO", 0.45)
REALTIME_MIN_1M_VOLUME_RATIO = env_float("REALTIME_MIN_1M_VOLUME_RATIO", 0.25)
REALTIME_MIN_2M_MOVE = env_float("REALTIME_MIN_2M_MOVE", 0.00045)
REALTIME_CLOSE_LOCATION_LONG = env_float("REALTIME_CLOSE_LOCATION_LONG", 0.57)
REALTIME_CLOSE_LOCATION_SHORT = env_float("REALTIME_CLOSE_LOCATION_SHORT", 0.43)
REALTIME_REQUIRE_TWO_1M_CANDLES = env_bool("REALTIME_REQUIRE_TWO_1M_CANDLES", False)

# --- RSI anti-chase filter (new; applies to CONTINUATION entries only) ---
RSI_FILTER_ENABLED = env_bool("RSI_FILTER_ENABLED", True)
RSI_PERIOD = env_int("RSI_PERIOD", 14)
RSI_MAX_LONG_CONTINUATION = env_float("RSI_MAX_LONG_CONTINUATION", 84.0)
RSI_MIN_SHORT_CONTINUATION = env_float("RSI_MIN_SHORT_CONTINUATION", 16.0)

# --- Pullback/retest ---
PULLBACK_MIN = env_float("PULLBACK_MIN", 0.0015)
PULLBACK_MAX = env_float("PULLBACK_MAX", 0.0400)
RECLAIM_BUFFER = env_float("RECLAIM_BUFFER", 0.0005)
CLOSE_LOCATION_MIN_LONG = env_float("CLOSE_LOCATION_MIN_LONG", 0.52)
CLOSE_LOCATION_MAX_SHORT = env_float("CLOSE_LOCATION_MAX_SHORT", 0.48)

# --- Compact TP ladder ---
TP1_MOVE = env_float("TP1_MOVE", 0.0065)
TP2_MOVE = env_float("TP2_MOVE", 0.0120)
TP3_MOVE = env_float("TP3_MOVE", 0.0185)
TP4_MOVE = env_float("TP4_MOVE", 0.0260)
TP5_MOVE = env_float("TP5_MOVE", 0.0350)

# --- Risk / stop ---
SL_ATR_MULT = env_float("SL_ATR_MULT", 0.80)
MIN_SL_MOVE = env_float("MIN_SL_MOVE", 0.0085)
MAX_SL_MOVE = env_float("MAX_SL_MOVE", 0.0260)

# Local scalp stop. FIXED: now applies to ALL fast scalp setup modes.
# In V13.29 the set contained "AERO_STYLE_*" strings that no setup ever produced,
# so the trades this was designed for never got the compressed stop.
LOCAL_SCALP_STOP_ENABLED = env_bool("LOCAL_SCALP_STOP_ENABLED", True)
LOCAL_SCALP_MAX_SL_MOVE = env_float("LOCAL_SCALP_MAX_SL_MOVE", 0.0145)
LOCAL_SCALP_MIN_SL_MOVE = env_float("LOCAL_SCALP_MIN_SL_MOVE", 0.0065)
LOCAL_STOP_MODES = {
    "MARKET_DUMP_SHORT",
    "INSTANT_MOMENTUM_SHORT", "INSTANT_MOMENTUM_LONG",
    "CONTINUATION_LONG", "CONTINUATION_SHORT",
    "REVERSAL_LONG", "REVERSAL_SHORT",
}
FAST_RISK_MULT = env_float("FAST_RISK_MULT", 0.08)
A_RISK_MULT = env_float("A_RISK_MULT", 0.14)

# --- Quality gate. FIXED: SL check is now price-based (leverage-independent). ---
MAX_SCALP_SL_MOVE = env_float("MAX_SCALP_SL_MOVE", 0.0160)   # hard price-risk cap for any scalp
MIN_TP1_RR = env_float("MIN_TP1_RR", 0.30)
MIN_LADDER_RR_HARD = env_float("MIN_LADDER_RR_HARD", 0.70)
MIN_FINAL_RR_HARD = env_float("MIN_FINAL_RR_HARD", 1.20)
MIN_LIVE_VOL_NORMAL = env_float("MIN_LIVE_VOL_NORMAL", 0.50)
MIN_LIVE_VOL_STRONG_PRICE = env_float("MIN_LIVE_VOL_STRONG_PRICE", 0.30)
STRONG_1M3_MOVE = env_float("STRONG_1M3_MOVE", 0.0050)
STRONG_RANGE1 = env_float("STRONG_RANGE1", 1.25)
HEAVY_MIN_FINAL_RR = env_float("HEAVY_MIN_FINAL_RR", 1.30)
HEAVY_MAX_SL_MOVE = env_float("HEAVY_MAX_SL_MOVE", 0.0080)
HEAVY_MIN_LIVE_VOL = env_float("HEAVY_MIN_LIVE_VOL", 0.70)
HEAVY_BASES = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "LINK", "AVAX",
    "DOT", "LTC", "BCH", "XMR", "GMX", "AAVE", "UNI", "ATOM", "ETC", "FIL",
}

# --- Instant Edge fallback ---
INSTANT_EDGE_ENABLED = env_bool("INSTANT_EDGE_ENABLED", True)
INSTANT_MIN_1M3_MOVE = env_float("INSTANT_MIN_1M3_MOVE", 0.0055)
INSTANT_MIN_15M_MOVE = env_float("INSTANT_MIN_15M_MOVE", 0.0040)
INSTANT_MIN_VOL1 = env_float("INSTANT_MIN_VOL1", 0.45)
INSTANT_MIN_RANGE1 = env_float("INSTANT_MIN_RANGE1", 0.85)
INSTANT_MIN_VOL5 = env_float("INSTANT_MIN_VOL5", 0.55)
INSTANT_MIN_RANGE5 = env_float("INSTANT_MIN_RANGE5", 0.70)
INSTANT_CLOSE_LONG = env_float("INSTANT_CLOSE_LONG", 0.60)
INSTANT_CLOSE_SHORT = env_float("INSTANT_CLOSE_SHORT", 0.40)
INSTANT_MIN_BODY = env_float("INSTANT_MIN_BODY", 0.34)
INSTANT_MAX_30M_CHASE = env_float("INSTANT_MAX_30M_CHASE", 0.065)
INSTANT_ALLOW_STRONG_1M_EXCEPTION = env_bool("INSTANT_ALLOW_STRONG_1M_EXCEPTION", True)

# --- Trader-pattern quality gate ---
TRADER_PATTERN_GATE_ENABLED = env_bool("TRADER_PATTERN_GATE_ENABLED", True)
TRADER_MIN_SCORE = env_int("TRADER_MIN_SCORE", 80)
TRADER_ALLOW_B_SCORE = env_bool("TRADER_ALLOW_B_SCORE", True)
TRADER_MIN_ABS_1M3 = env_float("TRADER_MIN_ABS_1M3", 0.0048)
TRADER_MIN_ABS_15M = env_float("TRADER_MIN_ABS_15M", 0.0055)
TRADER_MIN_VOL1 = env_float("TRADER_MIN_VOL1", 0.52)
TRADER_MIN_VOL5 = env_float("TRADER_MIN_VOL5", 0.52)
TRADER_MIN_RANGE1 = env_float("TRADER_MIN_RANGE1", 0.85)
TRADER_MIN_RANGE5 = env_float("TRADER_MIN_RANGE5", 0.75)
TRADER_MIN_TP5_FEASIBILITY = env_float("TRADER_MIN_TP5_FEASIBILITY", 0.50)
TRADER_NEED_5M_DIRECTION = env_bool("TRADER_NEED_5M_DIRECTION", False)
TRADER_BLOCK_WEAK_CONTINUATION = env_bool("TRADER_BLOCK_WEAK_CONTINUATION", True)
TRADER_MAX_COUNTER_30M = env_float("TRADER_MAX_COUNTER_30M", 0.0100)
TRADER_REQUIRE_MICRO_BREAK = env_bool("TRADER_REQUIRE_MICRO_BREAK", True)
TRADER_CLOSE_LONG = env_float("TRADER_CLOSE_LONG", 0.57)
TRADER_CLOSE_SHORT = env_float("TRADER_CLOSE_SHORT", 0.43)
TRADER_HEAVY_ONLY_A_PLUS = env_bool("TRADER_HEAVY_ONLY_A_PLUS", True)

# --- AERO-style gate ---
AERO_STYLE_GATE_ENABLED = env_bool("AERO_STYLE_GATE_ENABLED", True)
AERO_SHORT_ENABLED = env_bool("AERO_SHORT_ENABLED", True)
AERO_LONG_ENABLED = env_bool("AERO_LONG_ENABLED", True)
AERO_MIN_PULLBACK = env_float("AERO_MIN_PULLBACK", 0.0045)
AERO_MAX_PULLBACK = env_float("AERO_MAX_PULLBACK", 0.0850)
AERO_MIN_1M3 = env_float("AERO_MIN_1M3", 0.0038)
AERO_MIN_RECENT_RANGE = env_float("AERO_MIN_RECENT_RANGE", 0.0140)
AERO_MIN_VOL1 = env_float("AERO_MIN_VOL1", 0.35)
AERO_MIN_VOL5 = env_float("AERO_MIN_VOL5", 0.45)
AERO_MIN_RANGE1 = env_float("AERO_MIN_RANGE1", 0.60)
AERO_MIN_RANGE5 = env_float("AERO_MIN_RANGE5", 0.65)
AERO_CLOSE_SHORT = env_float("AERO_CLOSE_SHORT", 0.48)
AERO_CLOSE_LONG = env_float("AERO_CLOSE_LONG", 0.52)
AERO_REQUIRE_EMA_REJECT = env_bool("AERO_REQUIRE_EMA_REJECT", True)
AERO_ALLOW_B_SCORE = env_bool("AERO_ALLOW_B_SCORE", True)

# --- Market Dump SHORT fallback. Tightened anti-knife defaults. ---
MARKET_DUMP_SHORT_ENABLED = env_bool("MARKET_DUMP_SHORT_ENABLED", True)
DUMP_MIN_1M3 = env_float("DUMP_MIN_1M3", 0.0048)
DUMP_MIN_15M = env_float("DUMP_MIN_15M", 0.0035)
DUMP_MIN_VOL1 = env_float("DUMP_MIN_VOL1", 0.40)
DUMP_MIN_VOL5 = env_float("DUMP_MIN_VOL5", 0.50)
DUMP_MIN_RANGE1 = env_float("DUMP_MIN_RANGE1", 0.45)
DUMP_MIN_RANGE5 = env_float("DUMP_MIN_RANGE5", 0.60)
DUMP_CLOSE_SHORT = env_float("DUMP_CLOSE_SHORT", 0.55)
DUMP_MIN_RECENT_RANGE = env_float("DUMP_MIN_RECENT_RANGE", 0.0100)
DUMP_MAX_LATE_30M = env_float("DUMP_MAX_LATE_30M", 0.080)   # was 0.095; less bottom-shorting
DUMP_REQUIRE_REJECT_OR_BREAK = env_bool("DUMP_REQUIRE_REJECT_OR_BREAK", True)  # was False

# --- Time stop ---
FAST_MAX_MINUTES_TO_TP1 = env_int("FAST_MAX_MINUTES_TO_TP1", 6)
FAST_HARD_EXPIRE_MINUTES = env_int("FAST_HARD_EXPIRE_MINUTES", 11)
FAST_MIN_PROGRESS_TO_KEEP = env_float("FAST_MIN_PROGRESS_TO_KEEP", 0.25)
FAST_CANCEL_IF_NO_PROGRESS = env_bool("FAST_CANCEL_IF_NO_PROGRESS", True)

# --- Market shock / context ---
BTC_SHOCK_15M_BLOCK = env_float("BTC_SHOCK_15M_BLOCK", 0.020)
ALLOW_LONG = env_bool("ALLOW_LONG", True)
ALLOW_SHORT = env_bool("ALLOW_SHORT", True)
LONG_BLOCK_BTC_BEAR = env_bool("LONG_BLOCK_BTC_BEAR", False)
LONG_MIN_1M_VOLUME_RATIO = env_float("LONG_MIN_1M_VOLUME_RATIO", 0.75)
LONG_MIN_1M_RANGE_RATIO = env_float("LONG_MIN_1M_RANGE_RATIO", 0.80)
LONG_MIN_3M_CONFIRM = env_float("LONG_MIN_3M_CONFIRM", 0.0012)
LONG_MIN_CLOSE_LOCATION = env_float("LONG_MIN_CLOSE_LOCATION", 0.72)
LONG_MAX_15M_CHASE = env_float("LONG_MAX_15M_CHASE", 0.040)
LONG_MAX_30M_CHASE = env_float("LONG_MAX_30M_CHASE", 0.070)
LONG_MIN_PULLBACK_AFTER_PUMP = env_float("LONG_MIN_PULLBACK_AFTER_PUMP", 0.0055)
LONG_MAX_PULLBACK_AFTER_PUMP = env_float("LONG_MAX_PULLBACK_AFTER_PUMP", 0.038)
LONG_REQUIRE_SWEEP_OR_RECLAIM = env_bool("LONG_REQUIRE_SWEEP_OR_RECLAIM", True)
LONG_REQUIRE_HIGHER_LOW = env_bool("LONG_REQUIRE_HIGHER_LOW", True)

# --- Live stats protection: now SYMMETRIC (both sides). ---
SIDE_STATS_PROTECTION = env_bool("SIDE_STATS_PROTECTION", True)
SIDE_STATS_MIN_CLOSED = env_int("SIDE_STATS_MIN_CLOSED", 4)
SIDE_STATS_MIN_WR = env_float("SIDE_STATS_MIN_WR", 40)

# --- Context-adaptive rules ---
CONTEXT_ADAPTIVE_ENABLED = env_bool("CONTEXT_ADAPTIVE_ENABLED", True)
BTC_DUMP_SHORT_BIAS_ENABLED = env_bool("BTC_DUMP_SHORT_BIAS_ENABLED", True)
LONG_ALLOW_BEAR_RELATIVE_STRENGTH = env_bool("LONG_ALLOW_BEAR_RELATIVE_STRENGTH", True)
LONG_BEAR_MIN_ALT_15M = env_float("LONG_BEAR_MIN_ALT_15M", 0.0065)
LONG_BEAR_MIN_ALT_30M = env_float("LONG_BEAR_MIN_ALT_30M", 0.0100)
LONG_BEAR_MIN_1M3 = env_float("LONG_BEAR_MIN_1M3", 0.0020)
LONG_BEAR_MIN_REL_STRENGTH_1H = env_float("LONG_BEAR_MIN_REL_STRENGTH_1H", 0.010)
LONG_BEAR_MIN_VOL1 = env_float("LONG_BEAR_MIN_VOL1", 0.90)
LONG_BEAR_MIN_RANGE1 = env_float("LONG_BEAR_MIN_RANGE1", 0.95)
LONG_BEAR_MIN_CLOSE_LOCATION = env_float("LONG_BEAR_MIN_CLOSE_LOCATION", 0.76)
# Tightened: shorting after >10% 30m collapse is a squeeze magnet, not an edge.
SHORT_DUMP_ALLOW_EXTENDED_30M = env_float("SHORT_DUMP_ALLOW_EXTENDED_30M", 0.100)
SHORT_DUMP_MIN_LIVE_1M3 = env_float("SHORT_DUMP_MIN_LIVE_1M3", -0.0014)
SHORT_DUMP_MIN_BOUNCE = env_float("SHORT_DUMP_MIN_BOUNCE", 0.0040)

# --- Ultra-risk blocks ---
ULTRA_RISK_5M_CANDLE = env_float("ULTRA_RISK_5M_CANDLE", 0.095)
ULTRA_RISK_15M_CANDLE = env_float("ULTRA_RISK_15M_CANDLE", 0.140)

QUALITY_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "LINK", "AVAX", "AAVE", "SUI", "TAO", "NEAR", "INJ",
    "OP", "ARB", "APT", "TIA", "ADA", "DOT", "MATIC", "TON", "LTC", "BCH", "ETC", "FIL", "ATOM",
    "UNI", "RUNE", "SEI", "FET", "WLD", "DOGE", "TRX", "ENA", "JUP", "ORDI",
    "PORTAL", "HOME", "TAC", "VELVET", "BEAT", "BLESS",
}

ULTRA_RISK_KEYWORDS = {
    "1000", "PEPE", "BONK", "WIF", "MEME", "DOGS", "CATI", "HMSTR", "GOBLIN", "MOG", "TURBO",
    "BOME", "NEIRO", "PNUT", "MOODENG", "ACT", "GOAT", "FIGHT", "BLEND", "MAGMA",
}

FALLBACK_SYMBOLS = [f"{b}-USDT" for b in [
    "BTC", "ETH", "SOL", "BNB", "XRP", "LINK", "AVAX", "AAVE", "SUI", "TAO", "NEAR", "INJ",
    "OP", "ARB", "APT", "TIA", "ADA", "DOT", "LTC", "BCH", "ETC", "FIL", "ATOM", "UNI",
    "RUNE", "SEI", "FET", "WLD", "DOGE", "TRX", "ENA", "JUP", "ORDI", "BEAT", "BLESS",
    "KAITO", "XLM", "WLFI", "PUMP", "PORTAL", "HOME", "TAC", "VELVET",
]]

# ------------------------------------------------------------
# Thread-safe shared state
# ------------------------------------------------------------

STATE: Dict[str, Any] = {}
STATE_LOCK = threading.RLock()
KLINE_CACHE: Dict[str, Tuple[float, Optional[List[Dict[str, float]]]]] = {}
KLINE_CACHE_LOCK = threading.Lock()
TICKER_CACHE: Dict[str, Tuple[float, Any]] = {}
SCAN_LOCK = threading.Lock()
TRACK_LOCK = threading.Lock()
HEARTBEAT = {"scan": 0.0, "track": 0.0, "watchdog_alerted": False}


def now_ts() -> int:
    return int(time.time())


def day_key() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def normalize_symbol(symbol: str) -> str:
    s = symbol.replace("/", "-").upper()
    if s.endswith("USDT") and "-" not in s:
        s = s.replace("USDT", "-USDT")
    return s


def display_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-", "/")


def base_asset(symbol: str) -> str:
    return normalize_symbol(symbol).split("-")[0]


def default_state() -> Dict[str, Any]:
    return {
        "active_signals": [],
        "stats": {"total": {"profit": 0, "sl": 0, "expired": 0}, "side": {}, "grade": {},
                  "strategy": {}, "symbol": {}, "type": {}},
        "pair_cooldown": {},
        "strategy_cooldown": {},
        "last_scan": {},
        "last_diag_ts": 0,
        "last_error": "",
        "circuit": {"consecutive_sl": 0, "daily_sl": 0, "daily_key": day_key(), "paused_until": 0},
    }


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        base = default_state()
        if isinstance(data, dict):
            for k, v in data.items():
                base[k] = v
        base.setdefault("circuit", default_state()["circuit"])
        return base
    except Exception as e:
        log.error("load_state failed, starting fresh: %r", e)
        return default_state()


def save_state() -> None:
    """Atomic write: temp file + os.replace so a crash mid-write never corrupts state."""
    try:
        with STATE_LOCK:
            payload = json.dumps(STATE, ensure_ascii=False, indent=2)
        d = os.path.dirname(os.path.abspath(STATE_FILE)) or "."
        fd, tmp = tempfile.mkstemp(dir=d, prefix=".state_", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
            os.replace(tmp, STATE_FILE)
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
    except Exception as e:
        log.error("save_state failed: %r", e)


def set_last_error(msg: str) -> None:
    with STATE_LOCK:
        STATE["last_error"] = msg[:500]


def inc_stat(bucket: str, key: str, result: str) -> None:
    with STATE_LOCK:
        stats = STATE.setdefault("stats", default_state()["stats"])
        d = stats.setdefault(bucket, {})
        item = d.setdefault(key, {"profit": 0, "sl": 0, "expired": 0})
        item[result] = item.get(result, 0) + 1


def update_circuit_breaker(result: str) -> None:
    with STATE_LOCK:
        c = STATE.setdefault("circuit", default_state()["circuit"])
        if c.get("daily_key") != day_key():
            c["daily_key"] = day_key()
            c["daily_sl"] = 0
        if result == "sl":
            c["consecutive_sl"] = int(c.get("consecutive_sl", 0)) + 1
            c["daily_sl"] = int(c.get("daily_sl", 0)) + 1
        elif result == "profit":
            c["consecutive_sl"] = 0
        tripped = CIRCUIT_BREAKER_ENABLED and (
            c["consecutive_sl"] >= MAX_CONSECUTIVE_SL or c["daily_sl"] >= MAX_DAILY_SL
        )
        if tripped and now_ts() >= int(c.get("paused_until", 0)):
            c["paused_until"] = now_ts() + CIRCUIT_BREAKER_PAUSE_MINUTES * 60
            send_telegram(
                f"⛔ CIRCUIT BREAKER\n"
                f"Подряд SL: {c['consecutive_sl']} · SL за день: {c['daily_sl']}\n"
                f"Новые сигналы приостановлены на {CIRCUIT_BREAKER_PAUSE_MINUTES} минут.\n"
                f"Активные сигналы продолжают отслеживаться."
            )
            c["consecutive_sl"] = 0


def circuit_paused() -> Tuple[bool, int]:
    with STATE_LOCK:
        until = int(STATE.get("circuit", {}).get("paused_until", 0))
    return now_ts() < until, until


def apply_result(signal: Dict[str, Any], result: str) -> None:
    if result not in ("profit", "sl", "expired"):
        return
    with STATE_LOCK:
        stats = STATE.setdefault("stats", default_state()["stats"])
        stats.setdefault("total", {"profit": 0, "sl": 0, "expired": 0})[result] += 1
    inc_stat("side", signal.get("side", "?"), result)
    inc_stat("grade", signal.get("grade", "?"), result)
    inc_stat("strategy", signal.get("strategy", "?"), result)
    inc_stat("symbol", signal.get("symbol", "?"), result)
    inc_stat("type", signal.get("trade_type", "?"), result)
    update_circuit_breaker(result)
    save_state()


def wr_text(item: Dict[str, int]) -> str:
    p = int(item.get("profit", 0)); sl = int(item.get("sl", 0)); exp = int(item.get("expired", 0))
    closed = p + sl + exp
    wr = p / closed * 100 if closed else 0.0
    return f"{p} профит / {sl} SL / {exp} expired / WR {wr:.1f}%"


def build_stats_text() -> str:
    with STATE_LOCK:
        stats = json.loads(json.dumps(STATE.get("stats", default_state()["stats"])))
    lines = ["📊 Статистика", f"Итого: {wr_text(stats.get('total', {}))}"]
    for title, key in [("Стороны", "side"), ("Классы", "grade"), ("Стратегии", "strategy"), ("Типы", "type")]:
        data = stats.get(key, {})
        if data:
            lines.append(f"\n{title}:")
            ordered = sorted(data.items(), key=lambda kv: -(kv[1].get("profit", 0) + kv[1].get("sl", 0) + kv[1].get("expired", 0)))
            for k, v in ordered[:12]:
                lines.append(f"{k}: {wr_text(v)}")
    return "\n".join(lines)

# ------------------------------------------------------------
# HTTP sessions with retry/backoff (rate-limit aware)
# ------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3,
        backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


API_SESSION = make_session()
TG_SESSION = make_session()
API_FAIL_STREAK = {"count": 0, "alerted": False}


def send_telegram(text: str) -> bool:
    """Chunked, retried Telegram send. Never raises."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        set_last_error("Telegram env missing: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks: List[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= 3800:
            chunks.append(remaining); break
        cut = remaining.rfind("\n", 0, 3800)
        cut = cut if cut > 500 else 3800
        chunks.append(remaining[:cut]); remaining = remaining[cut:].lstrip("\n")
    ok_all = True
    for chunk in chunks:
        try:
            r = TG_SESSION.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk}, timeout=10)
            if not r.ok:
                set_last_error(f"Telegram error {r.status_code}: {r.text[:200]}")
                ok_all = False
        except Exception as e:
            set_last_error(f"Telegram exception: {repr(e)}")
            ok_all = False
    return ok_all


def get_json(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    url = BINGX_BASE_URL + path
    try:
        time.sleep(API_THROTTLE_SECONDS + random.uniform(0, 0.02))  # jitter vs rate limit
        r = API_SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        API_FAIL_STREAK["count"] = 0
        if API_FAIL_STREAK["alerted"]:
            API_FAIL_STREAK["alerted"] = False
            send_telegram("✅ Источник данных BingX снова доступен.")
        return r.json()
    except Exception as e:
        set_last_error(f"get_json {path}: {repr(e)}")
        API_FAIL_STREAK["count"] += 1
        if API_FAIL_STREAK["count"] >= 25 and not API_FAIL_STREAK["alerted"]:
            API_FAIL_STREAK["alerted"] = True
            send_telegram(f"⚠️ Деградация API BingX: {API_FAIL_STREAK['count']} ошибок подряд. Последняя: {repr(e)[:200]}")
        return None

# ------------------------------------------------------------
# Klines
# ------------------------------------------------------------

def parse_klines(raw: Any) -> Optional[List[Dict[str, float]]]:
    if not raw:
        return None
    candles: List[Dict[str, float]] = []
    for c in raw:
        try:
            if isinstance(c, dict):
                candles.append({
                    "time": int(c.get("time") or c.get("openTime") or c.get("T") or 0),
                    "open": float(c.get("open")), "high": float(c.get("high")),
                    "low": float(c.get("low")), "close": float(c.get("close")),
                    "volume": float(c.get("volume") or c.get("vol") or 0),
                })
            elif isinstance(c, (list, tuple)) and len(c) >= 6:
                candles.append({
                    "time": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                    "low": float(c[3]), "close": float(c[4]), "volume": float(c[5]),
                })
        except Exception:
            continue
    candles = [x for x in candles if x["open"] > 0 and x["high"] > 0 and x["low"] > 0 and x["close"] > 0]
    candles.sort(key=lambda x: x["time"])
    return candles if len(candles) >= 30 else None


def get_klines(symbol: str, interval: str, limit: int = 180, cache_seconds: int = 20) -> Optional[List[Dict[str, float]]]:
    symbol = normalize_symbol(symbol)
    key = f"{symbol}:{interval}:{limit}"
    with KLINE_CACHE_LOCK:
        cached = KLINE_CACHE.get(key)
        if cached and time.time() - cached[0] < cache_seconds:
            return cached[1]
    for ep in ["/openApi/swap/v3/quote/klines", "/openApi/swap/v2/quote/klines"]:
        data = get_json(ep, {"symbol": symbol, "interval": interval, "limit": limit})
        if not data:
            continue
        candles = parse_klines(data.get("data"))
        if candles:
            with KLINE_CACHE_LOCK:
                KLINE_CACHE[key] = (time.time(), candles)
            return candles
    with KLINE_CACHE_LOCK:
        KLINE_CACHE[key] = (time.time(), None)
    return None


def closed_candles(candles: Optional[List[Dict[str, float]]]) -> Optional[List[Dict[str, float]]]:
    """Drop the last (still forming) candle.

    Look-ahead-bias fix: on 15m/1h a forming candle understates volume and range
    and its close is not final. Higher-TF trend/volume must use closed bars only.
    1m/5m live candles remain intentional — live pressure IS the scalp signal.
    """
    if not candles or len(candles) < 2:
        return candles
    return candles[:-1]


def is_good_contract_symbol(symbol: str) -> bool:
    s = normalize_symbol(symbol)
    if not s.endswith("-USDT"):
        return False
    b = base_asset(s)
    return not any(x in b for x in ["USD", "USDC", "BULL", "BEAR"])


def get_symbols() -> List[str]:
    cached = TICKER_CACHE.get("symbols")
    if cached and time.time() - cached[0] < 600:
        return cached[1] or FALLBACK_SYMBOLS
    data = get_json("/openApi/swap/v2/quote/contracts")
    out: List[str] = []
    if data and isinstance(data.get("data"), list):
        for item in data.get("data", []):
            s = item.get("symbol")
            if s and is_good_contract_symbol(s):
                out.append(normalize_symbol(s))
    if not out:
        out = FALLBACK_SYMBOLS[:]
    for s in FALLBACK_SYMBOLS:
        if s not in out:
            out.append(s)
    random.shuffle(out)
    quality = [s for s in out if base_asset(s) in QUALITY_BASES]
    rest = [s for s in out if base_asset(s) not in QUALITY_BASES]
    result = (quality + rest)[:MAX_CONTRACTS]
    TICKER_CACHE["symbols"] = (time.time(), result)
    return result

# ------------------------------------------------------------
# Indicators
# ------------------------------------------------------------

def closes(c: List[Dict[str, float]]) -> List[float]:
    return [x["close"] for x in c]


def ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def rsi(values: List[float], period: int = 14) -> float:
    """Wilder's RSI on closes. Returns 50.0 when not enough data (neutral)."""
    if len(values) < period + 1:
        return 50.0
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        d = values[i] - values[i - 1]
        if d >= 0:
            gains += d
        else:
            losses -= d
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(values)):
        d = values[i] - values[i - 1]
        g = max(d, 0.0)
        l = max(-d, 0.0)
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
    if avg_loss <= 1e-12:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def vwap(candles: List[Dict[str, float]], n: int = 48) -> float:
    part = candles[-n:] if len(candles) >= n else candles
    pv = sum(((x["high"] + x["low"] + x["close"]) / 3) * max(x["volume"], 0) for x in part)
    vv = sum(max(x["volume"], 0) for x in part)
    return pv / vv if vv > 0 else (part[-1]["close"] if part else 0.0)


def atr(candles: List[Dict[str, float]], n: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    part = trs[-n:] if len(trs) >= n else trs
    return sum(part) / len(part) if part else 0.0


def percent_change(candles: List[Dict[str, float]], bars: int) -> float:
    if len(candles) <= bars:
        return 0.0
    a = candles[-bars]["close"]; b = candles[-1]["close"]
    return (b - a) / a if a else 0.0


def volume_ratio(candles: List[Dict[str, float]], n: int = 30) -> float:
    if len(candles) < n + 2:
        return 1.0
    cur = candles[-1]["volume"]
    avg = sum(x["volume"] for x in candles[-n - 1:-1]) / n
    return cur / avg if avg > 0 else 1.0


def volume_ratio_closed(candles: List[Dict[str, float]], n: int = 30) -> float:
    """Volume ratio on the LAST CLOSED candle (look-ahead fix for higher TFs)."""
    cc = closed_candles(candles)
    return volume_ratio(cc, n) if cc else 1.0


def candle_range(c: Dict[str, float]) -> float:
    return max(c["high"] - c["low"], 0.0)


def candle_range_ratio(candles: List[Dict[str, float]], n: int = 20) -> float:
    if len(candles) < n + 2:
        return 1.0
    cur = candle_range(candles[-1])
    avg = sum(candle_range(x) for x in candles[-n - 1:-1]) / n
    return cur / avg if avg > 0 else 1.0


def close_location(c: Dict[str, float]) -> float:
    rng = max(c["high"] - c["low"], 1e-12)
    return (c["close"] - c["low"]) / rng


def prior_compression_ratio(c5: List[Dict[str, float]], n: int = 6) -> float:
    if len(c5) < n + 8:
        return 1.0
    prior = c5[-n - 1:-1]
    older = c5[-n - 8:-n - 1]
    prior_avg = sum(candle_range(x) for x in prior) / max(len(prior), 1)
    older_avg = sum(candle_range(x) for x in older) / max(len(older), 1)
    return prior_avg / older_avg if older_avg > 0 else 1.0


def micro_structure_break(c1: List[Dict[str, float]], side: str) -> Tuple[bool, str]:
    if len(c1) < 12:
        return False, "not enough 1m structure"
    last = c1[-1]
    prev_window = c1[-9:-1]
    if side == "LONG":
        ref = max(x["high"] for x in prev_window)
        distance = (last["close"] - ref) / max(ref, 1e-12)
        ok = last["close"] > ref * (1 + EDGE_MIN_BREAKOUT_DISTANCE) and last["close"] > last["open"]
        return ok, f"1m break LONG {distance*100:+.2f}%"
    ref = min(x["low"] for x in prev_window)
    distance = (ref - last["close"]) / max(ref, 1e-12)
    ok = last["close"] < ref * (1 - EDGE_MIN_BREAKOUT_DISTANCE) and last["close"] < last["open"]
    return ok, f"1m break SHORT {distance*100:+.2f}%"


def micro_sweep_reclaim(c1: List[Dict[str, float]], side: str) -> Tuple[bool, str]:
    if not EDGE_REQUIRE_MICRO_SWEEP:
        return True, "micro sweep disabled"
    if len(c1) < 16:
        return False, "not enough 1m for sweep"
    last = c1[-1]
    recent = c1[-13:-1]
    if side == "LONG":
        swept = min(x["low"] for x in c1[-6:-1]) <= min(x["low"] for x in recent) * 1.001
        reclaimed = last["close"] > last["open"] and close_location(last) >= 0.62
        return swept and reclaimed, "micro sweep/reclaim LONG" if swept and reclaimed else "no micro sweep/reclaim LONG"
    swept = max(x["high"] for x in c1[-6:-1]) >= max(x["high"] for x in recent) * 0.999
    rejected = last["close"] < last["open"] and close_location(last) <= 0.38
    return swept and rejected, "micro sweep/reject SHORT" if swept and rejected else "no micro sweep/reject SHORT"


def tp5_feasible(c5: List[Dict[str, float]], side: str) -> Tuple[bool, str]:
    if len(c5) < 8:
        return False, "not enough candles for TP5 feasibility"
    recent_abs_15m = abs(percent_change(c5, 3))
    needed = TP5_MOVE * EDGE_MIN_TP5_FEASIBILITY
    return recent_abs_15m >= needed, f"TP5 feasibility recent15m {recent_abs_15m*100:.2f}% / need {needed*100:.2f}%"


def upper_wick_ratio(c: Dict[str, float]) -> float:
    rng = max(c["high"] - c["low"], 1e-12)
    return (c["high"] - max(c["open"], c["close"])) / rng


def lower_wick_ratio(c: Dict[str, float]) -> float:
    rng = max(c["high"] - c["low"], 1e-12)
    return (min(c["open"], c["close"]) - c["low"]) / rng


def trend_state(candles: List[Dict[str, float]]) -> str:
    """Trend on CLOSED candles only (look-ahead fix)."""
    cc = closed_candles(candles)
    if not cc:
        return "UNKNOWN"
    cs = closes(cc)
    if len(cs) < 60:
        return "UNKNOWN"
    e21 = ema(cs, 21); e55 = ema(cs, 55); price = cs[-1]
    ch = percent_change(cc, min(20, len(cc) - 1))
    if price > e21 > e55 and ch > 0.003:
        return "UP"
    if price < e21 < e55 and ch < -0.003:
        return "DOWN"
    return "RANGE"


def btc_context() -> Dict[str, Any]:
    c15 = get_klines("BTC-USDT", "15m", 120, cache_seconds=45)
    c1h = get_klines("BTC-USDT", "1h", 120, cache_seconds=120)
    if not c15 or not c1h:
        return {"ok": False, "direction": "UNKNOWN", "text": "BTC data unavailable", "ch1h": 0.0, "ch6h": 0.0}
    cc15 = closed_candles(c15) or c15
    ch1h = percent_change(cc15, 4)
    ch6h = percent_change(cc15, 24)
    t1h = trend_state(c1h)
    direction = "RANGE"
    if ch1h < -0.004 or ch6h < -0.018 or t1h == "DOWN":
        direction = "BEAR"
    elif ch1h > 0.004 or ch6h > 0.018 or t1h == "UP":
        direction = "BULL"
    return {"ok": True, "direction": direction, "ch1h": ch1h, "ch6h": ch6h, "t1h": t1h,
            "text": f"BTC {direction}: 1h {ch1h*100:+.2f}%, 6h {ch6h*100:+.2f}%, 1H {t1h}"}

# ------------------------------------------------------------
# Hot symbol selection
# ------------------------------------------------------------

def ultra_risk_symbol(symbol: str, c5: List[Dict[str, float]], c15: List[Dict[str, float]]) -> bool:
    b = base_asset(symbol)
    if any(k in b for k in ULTRA_RISK_KEYWORDS):
        return True
    for c in c5[-18:]:
        if (c["high"] - c["low"]) / max(c["open"], 1e-12) > ULTRA_RISK_5M_CANDLE:
            return True
    for c in c15[-10:]:
        if (c["high"] - c["low"]) / max(c["open"], 1e-12) > ULTRA_RISK_15M_CANDLE:
            return True
    return False


def hot_score(symbol: str) -> Tuple[float, str]:
    c1 = get_klines(symbol, "1m", 60, cache_seconds=8)
    c5 = get_klines(symbol, "5m", 80, cache_seconds=18)
    if not c1 or not c5:
        return 0.0, "no candles"

    ch3m_signed = percent_change(c1, 3)
    ch3m = abs(ch3m_signed)
    ch15m_signed = percent_change(c5, 3)
    ch30m_signed = percent_change(c5, 6)
    ch15m = abs(ch15m_signed); ch30m = abs(ch30m_signed)
    vr1 = volume_ratio(c1, 20); vr5 = volume_ratio(c5, 20)
    rr1 = candle_range_ratio(c1, 20); rr5 = candle_range_ratio(c5, 20)

    live_score = ch3m * 14000 + min(rr1, 5.0) * 14 + min(vr1, 5.0) * 7
    recent_score = ch15m * 700 + ch30m * 320 + min(rr5, 5.0) * 7 + min(vr5, 5.0) * 4

    reversal_bonus = 0.0
    if REVERSAL_ENABLED:
        if ch30m_signed > REVERSAL_MIN_30M_MOVE and ch3m_signed < -REVERSAL_MIN_LIVE_COUNTER_MOVE:
            reversal_bonus = 35 + abs(ch3m_signed) * 7000
        elif ch30m_signed < -REVERSAL_MIN_30M_MOVE and ch3m_signed > REVERSAL_MIN_LIVE_COUNTER_MOVE:
            reversal_bonus = 35 + abs(ch3m_signed) * 7000

    score = live_score + recent_score + reversal_bonus

    dead_now = ch3m < HOT_MIN_LIVE_MOVE_3M and rr1 < 0.35 and vr1 < 0.45
    stale = ch30m >= 0.012 and ch3m < HOT_MIN_LIVE_MOVE_3M and rr1 < HOT_MIN_LIVE_RANGE_OR_VOLUME and vr1 < HOT_MIN_LIVE_RANGE_OR_VOLUME
    if HOT_STALE_PENALTY_ENABLED and stale and reversal_bonus <= 0:
        score *= 0.25
    if dead_now and reversal_bonus <= 0:
        score *= 0.12
    # Absorption: huge volume with no movement is blocked harder now (not just discounted).
    if vr1 > 20 and ch3m < 0.0005 and rr1 < 0.5:
        score *= 0.10
    if base_asset(symbol) in QUALITY_BASES:
        score += 2

    live_tag = "LIVE" if not dead_now and (ch3m >= HOT_MIN_LIVE_MOVE_3M or rr1 >= 0.8 or vr1 >= 0.8 or reversal_bonus > 0) else "STALE"
    mode_tag = "REV" if reversal_bonus > 0 else "MOM"
    note = (f"{live_tag}/{mode_tag}: 1m3 {ch3m_signed*100:+.2f}%, 15m {ch15m_signed*100:+.2f}%, "
            f"30m {ch30m_signed*100:+.2f}%, vol1 x{vr1:.2f}, vol5 x{vr5:.2f}, range1 x{rr1:.2f}, range5 x{rr5:.2f}")
    return score, note


def select_hot_symbols(symbols: List[str]) -> Tuple[List[str], List[str]]:
    scored: List[Tuple[float, str, str]] = []
    notes: List[str] = []
    for sym in symbols[:MAX_ANALYZE_SYMBOLS]:
        try:
            sc, note = hot_score(sym)
            if sc > 0:
                scored.append((sc, sym, note))
        except Exception as e:
            set_last_error(f"hot_score {sym}: {repr(e)}")
    scored.sort(reverse=True, key=lambda x: x[0])
    for sc, sym, note in scored[:12]:
        notes.append(f"{display_symbol(sym)} hot {sc:.1f}: {note}")
    selected = [sym for sc, sym, _ in scored if sc >= HOT_MIN_SCORE][:HOT_SYMBOLS_TO_ANALYZE]
    min_live = min(HOT_SYMBOLS_TO_ANALYZE, 50)
    if len(selected) < min_live:
        seen = set(selected)
        for sc, sym, _ in scored:
            if sym not in seen:
                selected.append(sym); seen.add(sym)
            if len(selected) >= min_live:
                break
    return selected[:MAX_ANALYZE_SYMBOLS], notes

# ------------------------------------------------------------
# Setup logic (trading logic preserved; constants fixed)
# ------------------------------------------------------------

def realtime_pressure_ok(c1: List[Dict[str, float]], side: str) -> Tuple[bool, str, Dict[str, float]]:
    if len(c1) < 30:
        return False, "not enough 1m pressure data", {}
    last = c1[-1]; prev = c1[-2]
    ch2m = (last["close"] - c1[-3]["close"]) / max(c1[-3]["close"], 1e-12)
    ch3m = percent_change(c1, 3)
    rr1 = candle_range_ratio(c1, 20)
    vr1 = volume_ratio(c1, 20)
    loc = close_location(last)
    body = abs(last["close"] - last["open"]) / max(last["high"] - last["low"], 1e-12)
    same_two_long = last["close"] > last["open"] and prev["close"] >= prev["open"]
    same_two_short = last["close"] < last["open"] and prev["close"] <= prev["open"]
    metrics = {"ch2m": ch2m, "ch3m": ch3m, "range1": rr1, "vol1": vr1, "loc": loc, "body": body}

    if rr1 < REALTIME_MIN_1M_RANGE_RATIO:
        return False, f"1m range not live x{rr1:.2f}", metrics
    if vr1 < REALTIME_MIN_1M_VOLUME_RATIO:
        return False, f"1m volume not live x{vr1:.2f}", metrics
    if body < 0.35:
        return False, f"1m body weak {body:.2f}", metrics
    if side == "LONG":
        if ch2m < REALTIME_MIN_2M_MOVE:
            return False, f"LONG 2m pressure weak {ch2m*100:.2f}%", metrics
        if loc < REALTIME_CLOSE_LOCATION_LONG:
            return False, f"LONG 1m close not near high {loc:.2f}", metrics
        if REALTIME_REQUIRE_TWO_1M_CANDLES and not same_two_long:
            return False, "LONG lacks two 1m bullish candles", metrics
    else:
        if ch2m > -REALTIME_MIN_2M_MOVE:
            return False, f"SHORT 2m pressure weak {ch2m*100:.2f}%", metrics
        if loc > REALTIME_CLOSE_LOCATION_SHORT:
            return False, f"SHORT 1m close not near low {loc:.2f}", metrics
        if REALTIME_REQUIRE_TWO_1M_CANDLES and not same_two_short:
            return False, "SHORT lacks two 1m bearish candles", metrics
    return True, f"live pressure ok: 2m {ch2m*100:+.2f}%, 3m {ch3m*100:+.2f}%, range1 x{rr1:.2f}, vol1 x{vr1:.2f}", metrics


def rsi_continuation_ok(c5: List[Dict[str, float]], side: str, setup_mode: str) -> Tuple[bool, str]:
    """Anti-chase: block CONTINUATION entries when 5m RSI is already at an extreme.
    Reversal setups are exempt — extreme RSI is exactly their premise."""
    if not RSI_FILTER_ENABLED or setup_mode.startswith("REVERSAL"):
        return True, "rsi filter n/a"
    r = rsi(closes(c5), RSI_PERIOD)
    if side == "LONG" and r > RSI_MAX_LONG_CONTINUATION:
        return False, f"RSI5m too hot for LONG continuation {r:.1f}"
    if side == "SHORT" and r < RSI_MIN_SHORT_CONTINUATION:
        return False, f"RSI5m too cold for SHORT continuation {r:.1f}"
    return True, f"RSI5m {r:.1f} ok"


def fast_context_ok(c1, c5, c15, side: str, vol: float) -> Tuple[bool, str, Dict[str, float]]:
    if len(c1) < 20 or len(c5) < 36 or len(c15) < 24:
        return False, "not enough candles", {}

    ch15m = percent_change(c5, 3)
    ch30m = percent_change(c5, 6)
    ch3m_1m = percent_change(c1, 3)
    rr = candle_range_ratio(c5, 20)
    compression = prior_compression_ratio(c5, 6)
    last = c5[-1]
    candle_move = (last["high"] - last["low"]) / max(last["open"], 1e-12)

    metrics = {"ch15m": ch15m, "ch30m": ch30m, "ch3m_1m": ch3m_1m, "range_ratio": rr,
               "compression": compression, "candle_move": candle_move, "vol": vol, "setup_mode": "unknown"}

    if candle_move > FAST_MAX_SPREAD_PROXY:
        return False, f"last 5m candle too wide/chase risk {candle_move*100:.2f}%", metrics

    # FIXED flat-filter: compression check now actually runs.
    # Either the market compressed before the impulse, or the current expansion is strong.
    if compression > EDGE_MIN_PRIOR_COMPRESSION and rr < 1.75:
        return False, f"no compression-to-expansion edge: compression x{compression:.2f}, range x{rr:.2f}", metrics

    micro_ok, micro_reason = micro_structure_break(c1, side)
    if not micro_ok:
        return False, micro_reason, metrics

    pressure_ok, pressure_reason, pressure_metrics = realtime_pressure_ok(c1, side)
    metrics.update(pressure_metrics)
    if not pressure_ok:
        return False, pressure_reason, metrics

    sweep_ok, sweep_reason = micro_sweep_reclaim(c1, side)
    if not sweep_ok:
        return False, sweep_reason, metrics

    feasible_ok, feasible_reason = tp5_feasible(c5, side)
    if not feasible_ok:
        return False, feasible_reason, metrics

    if side == "LONG":
        continuation = ch15m >= FAST_MIN_15M_MOVE and ch30m >= FAST_MIN_30M_MOVE and ch3m_1m >= FAST_MIN_1M_CONFIRM
        reversal = REVERSAL_ENABLED and ch30m <= -REVERSAL_MIN_30M_MOVE and ch3m_1m >= REVERSAL_MIN_LIVE_COUNTER_MOVE
        if not (continuation or reversal):
            return False, f"no LONG edge: 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, 1m3 {ch3m_1m*100:+.2f}%", metrics
        if ch30m > FAST_MAX_30M_MOVE:
            return False, f"late LONG chase 30m {ch30m*100:.2f}%", metrics
        if last["close"] <= last["open"] and not reversal:
            return False, "last 5m not bullish for continuation", metrics
        if close_location(last) < CLOSE_LOCATION_MIN_LONG and not reversal:
            return False, f"LONG close location weak {close_location(last):.2f}", metrics
        metrics["setup_mode"] = "REVERSAL_LONG" if reversal else "CONTINUATION_LONG"
    else:
        btc_dump_context = BTC_DUMP_SHORT_BIAS_ENABLED and ch15m <= -FAST_MIN_15M_MOVE * 0.70 and ch3m_1m <= -FAST_MIN_1M_CONFIRM
        continuation = (ch15m <= -FAST_MIN_15M_MOVE and ch30m <= -FAST_MIN_30M_MOVE and ch3m_1m <= -FAST_MIN_1M_CONFIRM) or btc_dump_context
        reversal = REVERSAL_ENABLED and ch30m >= REVERSAL_MIN_30M_MOVE and ch3m_1m <= -REVERSAL_MIN_LIVE_COUNTER_MOVE
        if not (continuation or reversal):
            return False, f"no SHORT edge: 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, 1m3 {ch3m_1m*100:+.2f}%", metrics
        if ch30m < -FAST_MAX_30M_MOVE:
            recent_low = min(x["low"] for x in c5[-8:])
            bounce = (max(x["high"] for x in c5[-5:]) - recent_low) / max(recent_low, 1e-12)
            if not (BTC_DUMP_SHORT_BIAS_ENABLED and ch30m >= -SHORT_DUMP_ALLOW_EXTENDED_30M and ch3m_1m <= SHORT_DUMP_MIN_LIVE_1M3 and bounce >= SHORT_DUMP_MIN_BOUNCE):
                return False, f"late SHORT chase 30m {ch30m*100:.2f}%", metrics
            metrics["dump_bounce"] = bounce
        if last["close"] >= last["open"] and not reversal:
            return False, "last 5m not bearish for continuation", metrics
        if close_location(last) > CLOSE_LOCATION_MAX_SHORT and not reversal:
            return False, f"SHORT close location weak {close_location(last):.2f}", metrics
        metrics["setup_mode"] = "REVERSAL_SHORT" if reversal else "CONTINUATION_SHORT"

    rsi_ok, rsi_reason = rsi_continuation_ok(c5, side, str(metrics["setup_mode"]))
    if not rsi_ok:
        return False, rsi_reason, metrics

    live_bypass = abs(ch3m_1m) >= LIVE_BYPASS_VOLUME_MOVE or metrics.get("range1", 1.0) >= LIVE_BYPASS_RANGE_RATIO
    if rr < FAST_MIN_RANGE_RATIO and not live_bypass:
        return False, f"range expansion weak x{rr:.2f}", metrics
    if vol < FAST_MIN_VOLUME_RATIO and not live_bypass:
        return False, f"volume weak x{vol:.2f}", metrics

    return True, (f"{metrics['setup_mode']} edge ok: 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, "
                  f"1m3 {ch3m_1m*100:+.2f}%, range5 x{rr:.2f}, vol15 x{vol:.2f}; "
                  f"{micro_reason}; {pressure_reason}; {sweep_reason}; {feasible_reason}; {rsi_reason}"), metrics


def side_live_stats_ok(side: str) -> Tuple[bool, str]:
    """Symmetric protection: if either side's live winrate is poor, only A+ setups pass."""
    if not SIDE_STATS_PROTECTION:
        return True, "side stats protection disabled"
    with STATE_LOCK:
        item = STATE.get("stats", {}).get("side", {}).get(side, {})
    closed = int(item.get("profit", 0)) + int(item.get("sl", 0)) + int(item.get("expired", 0))
    if closed < SIDE_STATS_MIN_CLOSED:
        return True, f"not enough {side} stats"
    wr = int(item.get("profit", 0)) / max(closed, 1) * 100.0
    if wr < SIDE_STATS_MIN_WR:
        return False, f"{side} stats weak: WR {wr:.1f}% after {closed}"
    return True, f"{side} stats ok"


def professional_long_reclaim_gate(symbol, c1, c5, c15, btc, metrics, setup_mode, e1, e5, vw5) -> Tuple[bool, str]:
    if len(c1) < 24 or len(c5) < 24 or len(c15) < 10:
        return False, "LONG gate: not enough candles"
    btc_dir = str(btc.get("direction", "UNKNOWN"))
    btc_ch1h = float(btc.get("ch1h", 0.0))
    last1 = c1[-1]; prev1 = c1[-2]; price = last1["close"]
    ch3m = percent_change(c1, 3)
    ch15m = metrics.get("ch15m", percent_change(c5, 3))
    ch30m = metrics.get("ch30m", percent_change(c5, 6))
    vol1 = metrics.get("vol1", volume_ratio(c1, 20))
    range1 = metrics.get("range1", candle_range_ratio(c1, 20))
    loc1 = close_location(last1)

    bear_rs_long = False
    if btc_dir == "BEAR":
        rel_strength_1h = ch15m - btc_ch1h
        bear_rs_long = (LONG_ALLOW_BEAR_RELATIVE_STRENGTH and ch15m >= LONG_BEAR_MIN_ALT_15M
                        and ch30m >= LONG_BEAR_MIN_ALT_30M and ch3m >= LONG_BEAR_MIN_1M3
                        and rel_strength_1h >= LONG_BEAR_MIN_REL_STRENGTH_1H and vol1 >= LONG_BEAR_MIN_VOL1
                        and range1 >= LONG_BEAR_MIN_RANGE1 and loc1 >= LONG_BEAR_MIN_CLOSE_LOCATION)
        if LONG_BLOCK_BTC_BEAR and not bear_rs_long:
            return False, f"LONG gate: BTC BEAR, no relative strength: alt15m {ch15m*100:+.2f}%, 1m3 {ch3m*100:+.2f}%"

    if ch3m < LONG_MIN_3M_CONFIRM:
        return False, f"LONG gate: weak 3m confirm {ch3m*100:.2f}%"
    if btc_dir == "BEAR" and LONG_ALLOW_BEAR_RELATIVE_STRENGTH and not bear_rs_long:
        return False, f"LONG gate: BTC BEAR, only relative-strength longs; alt15m {ch15m*100:+.2f}%, 1m3 {ch3m*100:+.2f}%"
    if vol1 < LONG_MIN_1M_VOLUME_RATIO:
        return False, f"LONG gate: weak 1m volume x{vol1:.2f}"
    if range1 < LONG_MIN_1M_RANGE_RATIO:
        return False, f"LONG gate: weak 1m range x{range1:.2f}"
    if loc1 < LONG_MIN_CLOSE_LOCATION:
        return False, f"LONG gate: 1m close not strong {loc1:.2f}"
    if last1["close"] <= last1["open"]:
        return False, "LONG gate: last 1m not bullish"
    if prev1["close"] < prev1["open"] and last1["close"] <= prev1["open"]:
        return False, "LONG gate: did not reclaim prior red candle"
    if price < e1 * (1 + RECLAIM_BUFFER):
        return False, "LONG gate: no 1m EMA reclaim"
    if setup_mode == "CONTINUATION_LONG" and (price < e5 * (1 + RECLAIM_BUFFER) or price < vw5 * (1 + RECLAIM_BUFFER)):
        return False, "LONG gate: no 5m EMA/VWAP reclaim"

    recent = c1[-16:-4]
    last_zone = c1[-5:]
    swept_low = min(x["low"] for x in last_zone[:-1]) <= min(x["low"] for x in recent) * 1.0015 if recent else False
    reclaimed = last1["close"] > max(x["close"] for x in c1[-5:-1]) and loc1 >= LONG_MIN_CLOSE_LOCATION
    higher_low = min(x["low"] for x in c1[-4:]) > min(x["low"] for x in c1[-10:-4]) * 0.998 if len(c1) >= 12 else False

    if LONG_REQUIRE_SWEEP_OR_RECLAIM and not (swept_low or reclaimed):
        return False, "LONG gate: no sweep/reclaim trigger"
    if LONG_REQUIRE_HIGHER_LOW and not (higher_low or swept_low):
        return False, "LONG gate: no higher-low/sweep structure"

    recent_high = max(x["high"] for x in c5[-18:])
    recent_low = min(x["low"] for x in c5[-10:])
    pullback = (recent_high - recent_low) / max(recent_high, 1e-12)
    if ch15m > LONG_MAX_15M_CHASE or ch30m > LONG_MAX_30M_CHASE:
        if not (LONG_MIN_PULLBACK_AFTER_PUMP <= pullback <= LONG_MAX_PULLBACK_AFTER_PUMP and (swept_low or reclaimed)):
            return False, f"LONG gate: late pump chase blocked 15m {ch15m*100:.2f}%, pullback {pullback*100:.2f}%"

    last5 = c5[-1]
    if upper_wick_ratio(last5) > 0.48 and close_location(last5) < 0.68:
        return False, "LONG gate: 5m upper wick/distribution"

    return True, (f"LONG gate ok: BTC {btc_dir}, 3m {ch3m*100:+.2f}%, vol1 x{vol1:.2f}, range1 x{range1:.2f}, "
                  f"closeLoc {loc1:.2f}, bearRS {bear_rs_long}, sweep {swept_low}, reclaim {reclaimed}, higherLow {higher_low}")


def _score_common(base: int, metrics: Dict[str, float], vol: float, symbol: str, strong: bool) -> int:
    """Recalibrated scoring: lower base + wider bonus range so score actually differentiates."""
    score = base
    score += min(10, int(abs(metrics.get("ch15m", 0)) * 650))
    score += min(8, int(abs(metrics.get("ch30m", 0)) * 430))
    score += min(6, int((vol - 1.0) * 7))
    score += min(6, int((metrics.get("range_ratio", 1.0) - 1.0) * 7))
    score += min(6, int((metrics.get("vol1", 1.0) - 1.0) * 7))
    score += min(6, int((metrics.get("range1", 1.0) - 1.0) * 7))
    if strong:
        score += 6
    if base_asset(symbol) in QUALITY_BASES:
        score += 1
    return max(0, min(100, score))


def fast_burst_setup(symbol, c1, c5, c15, c1h, btc, side) -> Optional[Dict[str, Any]]:
    if not FAST_BURST_ENABLED:
        return None
    if len(c1) < 30 or len(c5) < 48 or len(c15) < 40 or len(c1h) < 60:
        return None

    price = c1[-1]["close"]
    e5 = ema(closes(c5), 21)
    e1 = ema(closes(c1), 9)
    vw5 = vwap(c5, 36)
    vol = volume_ratio_closed(c15, 24)   # look-ahead fix: closed 15m volume
    t1h = trend_state(c1h)

    fast_ok, fast_reason, metrics = fast_context_ok(c1, c5, c15, side, vol)
    if not fast_ok:
        return None
    setup_mode = str(metrics.get("setup_mode", ""))
    is_reversal = setup_mode.startswith("REVERSAL")

    if side == "LONG":
        long_gate_ok, long_gate_reason = professional_long_reclaim_gate(symbol, c1, c5, c15, btc, metrics, setup_mode, e1, e5, vw5)
        if not long_gate_ok:
            return None
        metrics["long_gate_reason"] = long_gate_reason

    last5 = c5[-1]; prev5 = c5[-2]

    if side == "LONG":
        recent_high = max(x["high"] for x in c5[-18:])
        pullback_low = min(x["low"] for x in c5[-10:])
        pullback = (recent_high - pullback_low) / max(recent_high, 1e-12)
        if pullback < PULLBACK_MIN or pullback > PULLBACK_MAX:
            return None
        if is_reversal:
            if price < e1:
                return None
        else:
            if price < e1 or price < e5 * (1 + RECLAIM_BUFFER) or price < vw5 * (1 + RECLAIM_BUFFER):
                return None
            if last5["close"] <= prev5["high"] * 0.999 and last5["close"] <= prev5["close"]:
                return None
            if upper_wick_ratio(last5) > 0.42 and close_location(last5) < 0.72:
                return None
        level = min(pullback_low, min(x["low"] for x in c1[-12:]))
        strategy = "PRO_SCALPING_EDGE_LONG"
        trade_type = "SCALPING EDGE LONG"
        reason = (f"SCALPING EDGE LONG: не прогноз рынка, а короткая ситуация. Режим {setup_mode}: "
                  f"свежий дисбаланс вверх, микро-откат {pullback*100:.2f}%, live 1m pressure, "
                  f"sweep/reclaim и немедленное продолжение. {fast_reason}. {metrics.get('long_gate_reason', '')}.")
    else:
        recent_low = min(x["low"] for x in c5[-18:])
        bounce_high = max(x["high"] for x in c5[-10:])
        pullback = (bounce_high - recent_low) / max(recent_low, 1e-12)
        if pullback < PULLBACK_MIN or pullback > PULLBACK_MAX:
            return None
        if is_reversal:
            if price > e1:
                return None
        else:
            if price > e1 or price > e5 * (1 - RECLAIM_BUFFER) or price > vw5 * (1 - RECLAIM_BUFFER):
                return None
            if last5["close"] >= prev5["low"] * 1.001 and last5["close"] >= prev5["close"]:
                return None
            if lower_wick_ratio(last5) > 0.42 and close_location(last5) > 0.28:
                return None
        level = max(bounce_high, max(x["high"] for x in c1[-12:]))
        strategy = "PRO_SCALPING_EDGE_SHORT"
        trade_type = "SCALPING EDGE SHORT"
        reason = (f"SCALPING EDGE SHORT: не прогноз рынка, а короткая ситуация. Режим {setup_mode}: "
                  f"свежий дисбаланс вниз, микро-отскок {pullback*100:.2f}%, live 1m pressure "
                  f"и немедленное продолжение. {fast_reason}.")

    strong = vol >= 1.55 and metrics.get("range_ratio", 1.0) >= 1.55 and abs(metrics.get("ch3m_1m", 0)) >= FAST_MIN_1M_CONFIRM * 1.4
    score = _score_common(64, metrics, vol, symbol, strong)

    return {
        "symbol": symbol, "side": side, "strategy": strategy, "trade_type": trade_type,
        "score": score, "grade": "A+" if score >= A_PLUS_MIN_SCORE and vol >= 1.45 else "B",
        "entry": price, "level": level, "reason": reason, "pullback": pullback,
        "volume_ratio": vol, "range_ratio": metrics.get("range_ratio", 1.0),
        "compression": metrics.get("compression", 1.0),
        "ch15m": metrics.get("ch15m", 0.0), "ch30m": metrics.get("ch30m", 0.0),
        "ch3m_1m": metrics.get("ch3m_1m", 0.0), "vol1": metrics.get("vol1", 1.0),
        "range1": metrics.get("range1", 1.0), "ch2m": metrics.get("ch2m", 0.0),
        "setup_mode": setup_mode, "t1h": t1h, "btc_text": btc.get("text", ""),
    }


def instant_edge_setup(symbol, c1, c5, c15, c1h, btc, side) -> Optional[Dict[str, Any]]:
    if not INSTANT_EDGE_ENABLED:
        return None
    if len(c1) < 35 or len(c5) < 36 or len(c15) < 12 or len(c1h) < 40:
        return None

    price = c1[-1]["close"]
    last1 = c1[-1]; prev1 = c1[-2]
    ch3m = percent_change(c1, 3)
    ch15m = percent_change(c5, 3)
    ch30m = percent_change(c5, 6)
    vol1 = volume_ratio(c1, 20); range1 = candle_range_ratio(c1, 20)
    vol5 = volume_ratio(c5, 20); range5 = candle_range_ratio(c5, 20)
    loc = close_location(last1)
    body = abs(last1["close"] - last1["open"]) / max(last1["high"] - last1["low"], 1e-12)
    t1h = trend_state(c1h)

    if side == "LONG":
        if ch3m < INSTANT_MIN_1M3_MOVE:
            return None
        if ch15m < INSTANT_MIN_15M_MOVE and not (INSTANT_ALLOW_STRONG_1M_EXCEPTION and ch3m >= INSTANT_MIN_1M3_MOVE * 1.45):
            return None
        if ch30m > INSTANT_MAX_30M_CHASE and ch3m < INSTANT_MIN_1M3_MOVE * 1.35:
            return None
        if loc < INSTANT_CLOSE_LONG or last1["close"] <= last1["open"]:
            return None
        if prev1["close"] < prev1["open"] and last1["close"] <= prev1["open"]:
            return None
        had_reset = any(x["close"] < x["open"] for x in c1[-7:-1]) or min(x["low"] for x in c1[-5:]) <= min(x["low"] for x in c1[-14:-5]) * 1.002
        if not had_reset and ch30m > 0.025:
            return None
        level = min(x["low"] for x in c1[-10:])
        strategy, trade_type, setup_mode, direction_text = "PRO_INSTANT_EDGE_LONG", "INSTANT EDGE LONG", "INSTANT_MOMENTUM_LONG", "вверх"
    else:
        if ch3m > -INSTANT_MIN_1M3_MOVE:
            return None
        if ch15m > -INSTANT_MIN_15M_MOVE and not (INSTANT_ALLOW_STRONG_1M_EXCEPTION and abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.45):
            return None
        if ch30m < -INSTANT_MAX_30M_CHASE and abs(ch3m) < INSTANT_MIN_1M3_MOVE * 1.35:
            return None
        if loc > INSTANT_CLOSE_SHORT or last1["close"] >= last1["open"]:
            return None
        if prev1["close"] > prev1["open"] and last1["close"] >= prev1["open"]:
            return None
        had_reset = any(x["close"] > x["open"] for x in c1[-7:-1]) or max(x["high"] for x in c1[-5:]) >= max(x["high"] for x in c1[-14:-5]) * 0.998
        if not had_reset and ch30m < -0.025:
            return None
        level = max(x["high"] for x in c1[-10:])
        strategy, trade_type, setup_mode, direction_text = "PRO_INSTANT_EDGE_SHORT", "INSTANT EDGE SHORT", "INSTANT_MOMENTUM_SHORT", "вниз"

    if body < INSTANT_MIN_BODY or range1 < INSTANT_MIN_RANGE1:
        return None
    if vol1 < INSTANT_MIN_VOL1 and not (abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.35 and range1 >= 1.15):
        return None
    if range5 < INSTANT_MIN_RANGE5:
        return None
    if vol5 < INSTANT_MIN_VOL5 and not (abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.60):
        return None

    micro_ok, micro_reason = micro_structure_break(c1, side)
    if not micro_ok:
        return None

    rsi_ok, _ = rsi_continuation_ok(c5, side, setup_mode)
    if not rsi_ok:
        return None

    btc_dir = str(btc.get("direction", "UNKNOWN"))
    if side == "LONG" and btc_dir == "BEAR" and not (ch3m >= INSTANT_MIN_1M3_MOVE * 1.35 and ch15m >= INSTANT_MIN_15M_MOVE * 1.2):
        return None
    if side == "SHORT" and btc_dir == "BULL" and not (abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.35 and ch15m <= -INSTANT_MIN_15M_MOVE * 1.2):
        return None

    score = 66
    score += min(10, int(abs(ch3m) * 1000))
    score += min(8, int(abs(ch15m) * 700))
    score += min(6, int(max(0.0, vol1 - 0.8) * 6))
    score += min(6, int(max(0.0, range1 - 1.0) * 6))
    score += min(5, int(max(0.0, range5 - 1.0) * 4))
    score = max(0, min(100, score))

    reason = (f"INSTANT EDGE {side}: fallback для живого импульса. Цена движется {direction_text} сейчас: "
              f"1m3 {ch3m*100:+.2f}%, 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, Vol1 x{vol1:.2f}, "
              f"Range1 x{range1:.2f}, Vol5 x{vol5:.2f}, Range5 x{range5:.2f}, closeLoc {loc:.2f}. "
              f"{micro_reason}. Сделка проходит RR/SL/live-volume quality gate.")

    return {
        "symbol": symbol, "side": side, "strategy": strategy, "trade_type": trade_type,
        "score": score, "grade": "A+" if score >= A_PLUS_MIN_SCORE and vol1 >= 1.20 else "B",
        "entry": price, "level": level, "reason": reason, "pullback": 0.0,
        "volume_ratio": vol5, "range_ratio": range5, "compression": 1.0,
        "ch15m": ch15m, "ch30m": ch30m, "ch3m_1m": ch3m, "vol1": vol1, "range1": range1,
        "ch2m": (c1[-1]["close"] - c1[-3]["close"]) / max(c1[-3]["close"], 1e-12),
        "setup_mode": setup_mode, "t1h": t1h, "btc_text": btc.get("text", ""),
    }


def market_dump_short_setup(symbol, c1, c5, c15, c1h, btc) -> Optional[Dict[str, Any]]:
    if not MARKET_DUMP_SHORT_ENABLED or not ALLOW_SHORT:
        return None
    if len(c1) < 35 or len(c5) < 36 or len(c15) < 12 or len(c1h) < 40:
        return None

    side = "SHORT"
    price = c1[-1]["close"]
    last1 = c1[-1]; prev1 = c1[-2]
    ch3m = percent_change(c1, 3)
    ch15m = percent_change(c5, 3)
    ch30m = percent_change(c5, 6)
    vol1 = volume_ratio(c1, 20); vol5 = volume_ratio(c5, 20)
    range1 = candle_range_ratio(c1, 20); range5 = candle_range_ratio(c5, 20)
    loc = close_location(last1)
    recent_range = (max(x["high"] for x in c5[-6:]) - min(x["low"] for x in c5[-6:])) / max(price, 1e-12)
    btc_1h = float(btc.get("ch1h", 0.0) or 0.0)
    btc_6h = float(btc.get("ch6h", 0.0) or 0.0)

    if ch3m > -DUMP_MIN_1M3:
        return None
    market_dump_context = btc_1h <= -0.0025 or btc_6h <= -0.0100 or str(btc.get("direction", "")) == "BEAR"
    alt_dump_context = ch15m <= -DUMP_MIN_15M or ch30m <= -DUMP_MIN_15M * 1.25
    if not (market_dump_context or alt_dump_context):
        return None
    if ch30m < -DUMP_MAX_LATE_30M and not (abs(ch3m) >= DUMP_MIN_1M3 * 2.0 and range1 >= 1.35):
        return None
    if vol1 < DUMP_MIN_VOL1:
        return None
    if vol5 < DUMP_MIN_VOL5 and not (abs(ch3m) >= DUMP_MIN_1M3 * 1.65):
        return None
    if range1 < DUMP_MIN_RANGE1 or range5 < DUMP_MIN_RANGE5 or recent_range < DUMP_MIN_RECENT_RANGE:
        return None
    if loc > DUMP_CLOSE_SHORT or last1["close"] >= last1["open"]:
        return None

    fresh_low_break = last1["close"] < min(x["low"] for x in c1[-7:-1])
    failed_bounce = any(x["close"] > x["open"] for x in c1[-8:-1]) and last1["close"] < prev1["close"]
    lower_high_reject = max(x["high"] for x in c1[-4:]) < max(x["high"] for x in c1[-14:-4]) and last1["close"] < prev1["close"]
    # DUMP_REQUIRE_REJECT_OR_BREAK now defaults to True: no structure = no signal.
    if DUMP_REQUIRE_REJECT_OR_BREAK and not (fresh_low_break or failed_bounce or lower_high_reject):
        return None

    level = max(x["high"] for x in c1[-10:])
    score = 68
    score += min(10, int(abs(ch3m) * 1100))
    score += min(8, int(abs(min(ch15m, 0.0)) * 700))
    score += min(6, int(max(0.0, vol1 - 0.60) * 6))
    score += min(6, int(max(0.0, range1 - 0.80) * 5))
    score += min(5, int(max(0.0, range5 - 1.00) * 4))
    if market_dump_context:
        score += 3
    if fresh_low_break:
        score += 3
    score = max(0, min(100, score))

    reason = (f"MARKET DUMP SHORT: активный рыночный слив, dump-continuation, не прогноз. "
              f"BTC {btc.get('text', '')}; 1m3 {ch3m*100:+.2f}%, 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, "
              f"Vol1 x{vol1:.2f}, Vol5 x{vol5:.2f}, Range1 x{range1:.2f}, Range5 x{range5:.2f}. "
              f"Структура: freshLow {fresh_low_break}, failedBounce {failed_bounce}, lowerHighReject {lower_high_reject}. "
              f"Дальше — RR/SL/live-volume/trader quality gates.")

    return {
        "symbol": symbol, "side": side, "strategy": "PRO_MARKET_DUMP_SHORT", "trade_type": "MARKET DUMP SHORT",
        "score": score, "grade": "A+" if score >= A_PLUS_MIN_SCORE and vol1 >= 0.85 and range1 >= 1.15 else "B",
        "entry": price, "level": level, "reason": reason, "pullback": 0.0,
        "volume_ratio": vol5, "range_ratio": range5, "compression": 1.0,
        "ch15m": ch15m, "ch30m": ch30m, "ch3m_1m": ch3m, "vol1": vol1, "range1": range1,
        "ch2m": (c1[-1]["close"] - c1[-3]["close"]) / max(c1[-3]["close"], 1e-12),
        "setup_mode": "MARKET_DUMP_SHORT", "t1h": trend_state(c1h), "btc_text": btc.get("text", ""),
    }

# ------------------------------------------------------------
# Trade construction / final gates
# ------------------------------------------------------------

def calculate_fast_trade(setup: Dict[str, Any], c1, c5) -> Optional[Dict[str, Any]]:
    side = setup["side"]
    entry = setup["entry"]
    level = setup["level"]
    a5 = atr(c5, 14)
    atr_move = a5 / max(entry, 1e-12)
    instant = str(setup.get("setup_mode", "")).startswith("INSTANT")
    buffer = max(entry * (0.0016 if instant else 0.0022), a5 * (0.55 if instant else SL_ATR_MULT))

    if side == "LONG":
        recent_source = c1[-10:] + (c5[-2:] if instant else c5[-4:])
        recent_low = min(x["low"] for x in recent_source)
        sl = min(level, recent_low) - buffer
        sl = min(sl, entry * (1 - MIN_SL_MOVE))
        tps = [entry * (1 + m) for m in (TP1_MOVE, TP2_MOVE, TP3_MOVE, TP4_MOVE, TP5_MOVE)]
    else:
        recent_source = c1[-10:] + (c5[-2:] if instant else c5[-4:])
        recent_high = max(x["high"] for x in recent_source)
        sl = max(level, recent_high) + buffer
        sl = max(sl, entry * (1 + MIN_SL_MOVE))
        tps = [entry * (1 - m) for m in (TP1_MOVE, TP2_MOVE, TP3_MOVE, TP4_MOVE, TP5_MOVE)]

    risk = abs(entry - sl)
    risk_move = risk / max(entry, 1e-12)
    setup_mode = str(setup.get("setup_mode", ""))

    # Local scalp stop: compress a too-wide structural stop for FAST scalp modes.
    # FIXED: applies to all real setup modes; floor is ATR-aware so the stop
    # cannot sit inside pure 1m/5m noise.
    local_stop_used = False
    original_sl_move = 0.0
    if LOCAL_SCALP_STOP_ENABLED and risk_move > LOCAL_SCALP_MAX_SL_MOVE and setup_mode in LOCAL_STOP_MODES:
        noise_floor = max(LOCAL_SCALP_MIN_SL_MOVE, atr_move * 1.10)
        local_move = max(noise_floor, min(
            LOCAL_SCALP_MAX_SL_MOVE,
            max(TP1_MOVE * 1.20, abs(float(setup.get("ch3m_1m", 0.0) or 0.0)) * 1.10),
        ))
        if local_move > LOCAL_SCALP_MAX_SL_MOVE:
            # ATR noise floor exceeds the local cap: the coin is too volatile for a local scalp stop.
            return None
        sl = entry * (1 - local_move) if side == "LONG" else entry * (1 + local_move)
        local_stop_used = True
        original_sl_move = risk_move
        risk = abs(entry - sl)
        risk_move = risk / max(entry, 1e-12)

    if risk_move > MAX_SL_MOVE:
        return None

    rewards = [abs(tp - entry) for tp in tps]
    rr = rewards[0] / risk if risk > 0 else 0.0
    ladder_rr = (sum(rewards) / len(rewards)) / risk if risk > 0 else 0.0
    final_rr = rewards[-1] / risk if risk > 0 else 0.0
    roi_tp1 = rewards[0] / entry * LEVERAGE * 100
    roi_sl = risk_move * LEVERAGE * 100

    return {
        **setup,
        "sl": sl, "tp1": tps[0], "tp2": tps[1], "tp3": tps[2], "tp4": tps[3], "tp5": tps[4],
        "sl_move": risk_move,
        "rr": rr, "ladder_rr": ladder_rr, "final_rr": final_rr,
        "roi_tp1": roi_tp1, "roi_sl": roi_sl,
        "risk_mult": A_RISK_MULT if setup["grade"] == "A+" else FAST_RISK_MULT,
        "local_stop_used": local_stop_used, "original_sl_move": original_sl_move,
        "created_at": now_ts(), "status": "active",
        "tp1_hit": False, "tp2_hit": False, "tp3_hit": False, "tp4_hit": False, "tp5_hit": False,
    }


def professional_quality_gate(trade: Dict[str, Any], symbol: str) -> Tuple[bool, str, str]:
    """FIXED: SL checks are price-based (leverage-independent). Changing LEVERAGE in env
    no longer silently loosens or tightens the risk gate."""
    side = trade.get("side", "?")
    base = base_asset(symbol)
    rr = float(trade.get("rr", 0.0) or 0.0)
    ladder_rr = float(trade.get("ladder_rr", 0.0) or 0.0)
    final_rr = float(trade.get("final_rr", 0.0) or 0.0)
    sl_move = float(trade.get("sl_move", 1.0) or 1.0)
    vol1 = float(trade.get("vol1", 1.0) or 1.0)
    range1 = float(trade.get("range1", 1.0) or 1.0)
    ch3m = abs(float(trade.get("ch3m_1m", 0.0) or 0.0))

    if sl_move > MAX_SCALP_SL_MOVE:
        return False, "sl_price_too_high_block", f"{display_symbol(symbol)} {side}: SL price risk too high {sl_move*100:.2f}%"
    if rr < MIN_TP1_RR:
        return False, "tp1_rr_hard_block", f"{display_symbol(symbol)} {side}: TP1 RR too weak {rr:.2f}"
    if ladder_rr < MIN_LADDER_RR_HARD:
        return False, "ladder_rr_hard_block", f"{display_symbol(symbol)} {side}: ladder RR too weak {ladder_rr:.2f}"
    if final_rr < MIN_FINAL_RR_HARD:
        return False, "final_rr_hard_block", f"{display_symbol(symbol)} {side}: final RR too weak {final_rr:.2f}"

    if vol1 < MIN_LIVE_VOL_NORMAL:
        strong_price_exception = vol1 >= MIN_LIVE_VOL_STRONG_PRICE and ch3m >= STRONG_1M3_MOVE and range1 >= STRONG_RANGE1
        if not strong_price_exception:
            return False, "weak_live_volume_block", f"{display_symbol(symbol)} {side}: weak live volume x{vol1:.2f}, 1m3 {ch3m*100:.2f}%, range1 x{range1:.2f}"

    if base in HEAVY_BASES:
        if sl_move > HEAVY_MAX_SL_MOVE:
            return False, "heavy_coin_sl_block", f"{display_symbol(symbol)} {side}: heavy coin SL too wide {sl_move*100:.2f}%"
        if final_rr < HEAVY_MIN_FINAL_RR:
            return False, "heavy_coin_rr_block", f"{display_symbol(symbol)} {side}: heavy coin final RR too weak {final_rr:.2f}"
        if vol1 < HEAVY_MIN_LIVE_VOL:
            return False, "heavy_coin_volume_block", f"{display_symbol(symbol)} {side}: heavy coin live volume weak x{vol1:.2f}"

    return True, "ok", "quality ok"


def aero_style_gate(trade, symbol, c1, c5, c15, btc) -> Tuple[bool, str, str]:
    if not AERO_STYLE_GATE_ENABLED:
        return False, "aero_disabled", "aero-style disabled"
    if len(c1) < 35 or len(c5) < 20:
        return False, "aero_no_candles", f"{display_symbol(symbol)}: not enough candles for AERO gate"

    side = str(trade.get("side", ""))
    if side == "SHORT" and not AERO_SHORT_ENABLED:
        return False, "aero_short_disabled", f"{display_symbol(symbol)} SHORT: AERO short disabled"
    if side == "LONG" and not AERO_LONG_ENABLED:
        return False, "aero_long_disabled", f"{display_symbol(symbol)} LONG: AERO long disabled"

    entry = float(trade.get("entry", 0.0) or 0.0)
    if entry <= 0:
        return False, "aero_bad_entry", f"{display_symbol(symbol)} {side}: bad entry"

    ch3m = float(trade.get("ch3m_1m", 0.0) or 0.0)
    vol1 = float(trade.get("vol1", 1.0) or 1.0)
    vol5 = float(trade.get("volume_ratio", 1.0) or 1.0)
    range1 = float(trade.get("range1", 1.0) or 1.0)
    range5 = float(trade.get("range_ratio", 1.0) or 1.0)
    loc = close_location(c1[-1])
    e1 = ema([x["close"] for x in c1[-25:]], 9)
    e5 = ema([x["close"] for x in c5[-30:]], 9)
    recent_1m = c1[-18:]
    recent_5m = c5[-8:]
    recent_high = max(x["high"] for x in recent_1m + recent_5m[-3:])
    recent_low = min(x["low"] for x in recent_1m + recent_5m[-3:])
    recent_range = (recent_high - recent_low) / max(entry, 1e-12)

    if recent_range < AERO_MIN_RECENT_RANGE:
        return False, "aero_recent_range_block", f"{display_symbol(symbol)} {side}: recent range too small {recent_range*100:.2f}%"
    if vol1 < AERO_MIN_VOL1:
        return False, "aero_vol1_block", f"{display_symbol(symbol)} {side}: vol1 too weak x{vol1:.2f}"
    if vol5 < AERO_MIN_VOL5:
        return False, "aero_vol5_block", f"{display_symbol(symbol)} {side}: vol5 too weak x{vol5:.2f}"
    if range1 < AERO_MIN_RANGE1:
        return False, "aero_range1_block", f"{display_symbol(symbol)} {side}: range1 too weak x{range1:.2f}"
    if range5 < AERO_MIN_RANGE5:
        return False, "aero_range5_block", f"{display_symbol(symbol)} {side}: range5 too weak x{range5:.2f}"

    if side == "SHORT":
        pullback = (recent_high - entry) / max(entry, 1e-12)
        if pullback < AERO_MIN_PULLBACK:
            return False, "aero_pullback_block", f"{display_symbol(symbol)} SHORT: no upper pullback/reject {pullback*100:.2f}%"
        if pullback > AERO_MAX_PULLBACK:
            return False, "aero_spike_block", f"{display_symbol(symbol)} SHORT: spike too extreme {pullback*100:.2f}%"
        if ch3m > -AERO_MIN_1M3:
            return False, "aero_pressure_block", f"{display_symbol(symbol)} SHORT: no live breakdown 1m3 {ch3m*100:+.2f}%"
        if loc > AERO_CLOSE_SHORT or c1[-1]["close"] >= c1[-1]["open"]:
            return False, "aero_reject_close_block", f"{display_symbol(symbol)} SHORT: last 1m not rejected near low"
        if c1[-1]["close"] >= min(x["low"] for x in c1[-7:-1]):
            return False, "aero_breakdown_block", f"{display_symbol(symbol)} SHORT: no fresh local breakdown"
        if AERO_REQUIRE_EMA_REJECT and not (c1[-1]["close"] < e1 or c1[-1]["close"] < e5):
            return False, "aero_ema_reject_block", f"{display_symbol(symbol)} SHORT: no EMA rejection"
        return True, "aero_style_short_ok", (f"AERO-style SHORT ok: pullback/reject {pullback*100:.2f}%, "
                                             f"live breakdown 1m3 {ch3m*100:+.2f}%, range {recent_range*100:.2f}%")
    if side == "LONG":
        sweep = (entry - recent_low) / max(entry, 1e-12)
        if sweep < AERO_MIN_PULLBACK:
            return False, "aero_sweep_block", f"{display_symbol(symbol)} LONG: no lower sweep/reclaim {sweep*100:.2f}%"
        if sweep > AERO_MAX_PULLBACK:
            return False, "aero_spike_block", f"{display_symbol(symbol)} LONG: spike too extreme {sweep*100:.2f}%"
        if ch3m < AERO_MIN_1M3:
            return False, "aero_pressure_block", f"{display_symbol(symbol)} LONG: no live reclaim 1m3 {ch3m*100:+.2f}%"
        if loc < AERO_CLOSE_LONG or c1[-1]["close"] <= c1[-1]["open"]:
            return False, "aero_reclaim_close_block", f"{display_symbol(symbol)} LONG: last 1m not reclaimed near high"
        if c1[-1]["close"] <= max(x["high"] for x in c1[-7:-1]):
            return False, "aero_breakout_block", f"{display_symbol(symbol)} LONG: no fresh local breakout"
        if AERO_REQUIRE_EMA_REJECT and not (c1[-1]["close"] > e1 or c1[-1]["close"] > e5):
            return False, "aero_ema_reclaim_block", f"{display_symbol(symbol)} LONG: no EMA reclaim"
        return True, "aero_style_long_ok", (f"AERO-style LONG ok: sweep/reclaim {sweep*100:.2f}%, "
                                            f"live reclaim 1m3 {ch3m*100:+.2f}%, range {recent_range*100:.2f}%")
    return False, "aero_side_block", f"{display_symbol(symbol)}: unknown side {side}"


def trader_pattern_gate(trade, symbol, c1, c5, c15, btc) -> Tuple[bool, str, str]:
    if not TRADER_PATTERN_GATE_ENABLED:
        return True, "ok", "trader pattern gate disabled"

    side = str(trade.get("side", ""))
    base = base_asset(symbol)
    score = int(trade.get("score", 0) or 0)
    grade = str(trade.get("grade", "B"))
    setup_mode = str(trade.get("setup_mode", ""))
    ch3m = float(trade.get("ch3m_1m", 0.0) or 0.0)
    ch15m = float(trade.get("ch15m", 0.0) or 0.0)
    ch30m = float(trade.get("ch30m", 0.0) or 0.0)
    vol1 = float(trade.get("vol1", 1.0) or 1.0)
    vol5 = float(trade.get("volume_ratio", 1.0) or 1.0)
    range1 = float(trade.get("range1", 1.0) or 1.0)
    range5 = float(trade.get("range_ratio", 1.0) or 1.0)
    entry = float(trade.get("entry", 0.0) or 0.0)
    tp5 = float(trade.get("tp5", 0.0) or 0.0)

    aero_ok, aero_block, aero_reason = aero_style_gate(trade, symbol, c1, c5, c15, btc)

    if grade != "A+" and not TRADER_ALLOW_B_SCORE:
        return False, "trader_grade_block", f"{display_symbol(symbol)} {side}: B-class disabled by env"
    if score < TRADER_MIN_SCORE:
        if not (aero_ok and AERO_ALLOW_B_SCORE and score >= max(72, TRADER_MIN_SCORE - 10)):
            return False, "trader_score_block", f"{display_symbol(symbol)} {side}: score too low {score} < {TRADER_MIN_SCORE}"
    if grade != "A+":
        if abs(ch3m) < TRADER_MIN_ABS_1M3 * 1.20 and vol1 < TRADER_MIN_VOL1 * 1.20 and range1 < TRADER_MIN_RANGE1 * 1.15:
            if not aero_ok:
                return False, "trader_bplus_quality_block", (f"{display_symbol(symbol)} {side}: B+ not strong enough; "
                                                             f"1m3 {ch3m*100:+.2f}%, vol1 x{vol1:.2f}, range1 x{range1:.2f}")
    if base in HEAVY_BASES and TRADER_HEAVY_ONLY_A_PLUS and grade != "A+":
        return False, "trader_heavy_grade_block", f"{display_symbol(symbol)} {side}: heavy coin requires A+"

    if side == "LONG":
        if ch3m < TRADER_MIN_ABS_1M3:
            return False, "trader_live_pressure_block", f"{display_symbol(symbol)} LONG: weak live pressure 1m3 {ch3m*100:+.2f}%"
        if TRADER_NEED_5M_DIRECTION and c5[-1]["close"] <= c5[-2]["close"]:
            return False, "trader_5m_direction_block", f"{display_symbol(symbol)} LONG: last 5m not confirming up"
        if close_location(c1[-1]) < TRADER_CLOSE_LONG:
            return False, "trader_close_location_block", f"{display_symbol(symbol)} LONG: 1m close not near high"
        if TRADER_REQUIRE_MICRO_BREAK and c1[-1]["close"] <= max(x["high"] for x in c1[-6:-1]):
            return False, "trader_micro_break_block", f"{display_symbol(symbol)} LONG: no fresh 1m high break"
        aligned = ch15m >= TRADER_MIN_ABS_15M and ch30m >= -TRADER_MAX_COUNTER_30M
        reversal_exception = setup_mode.startswith("REVERSAL") and ch3m >= TRADER_MIN_ABS_1M3 * 1.5 and range1 >= TRADER_MIN_RANGE1 * 1.25
    else:
        if ch3m > -TRADER_MIN_ABS_1M3:
            return False, "trader_live_pressure_block", f"{display_symbol(symbol)} SHORT: weak live pressure 1m3 {ch3m*100:+.2f}%"
        if TRADER_NEED_5M_DIRECTION and c5[-1]["close"] >= c5[-2]["close"]:
            return False, "trader_5m_direction_block", f"{display_symbol(symbol)} SHORT: last 5m not confirming down"
        if close_location(c1[-1]) > TRADER_CLOSE_SHORT:
            return False, "trader_close_location_block", f"{display_symbol(symbol)} SHORT: 1m close not near low"
        dump_exception = setup_mode.startswith("MARKET_DUMP") and ch3m <= -TRADER_MIN_ABS_1M3 * 1.10 and range1 >= max(0.45, TRADER_MIN_RANGE1 * 0.60)
        if TRADER_REQUIRE_MICRO_BREAK and c1[-1]["close"] >= min(x["low"] for x in c1[-6:-1]) and not dump_exception:
            return False, "trader_micro_break_block", f"{display_symbol(symbol)} SHORT: no fresh 1m low break"
        aligned = ch15m <= -TRADER_MIN_ABS_15M and ch30m <= TRADER_MAX_COUNTER_30M
        reversal_exception = setup_mode.startswith("REVERSAL") and abs(ch3m) >= TRADER_MIN_ABS_1M3 * 1.5 and range1 >= TRADER_MIN_RANGE1 * 1.25

    if TRADER_BLOCK_WEAK_CONTINUATION and not (aligned or reversal_exception or aero_ok or setup_mode.startswith("MARKET_DUMP")):
        return False, "trader_structure_block", f"{display_symbol(symbol)} {side}: weak structure; 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, mode {setup_mode}"
    if vol1 < TRADER_MIN_VOL1 and not aero_ok:
        return False, "trader_vol1_block", f"{display_symbol(symbol)} {side}: live vol1 too weak x{vol1:.2f}"
    if vol5 < TRADER_MIN_VOL5 and not aero_ok:
        return False, "trader_vol5_block", f"{display_symbol(symbol)} {side}: vol5 too weak x{vol5:.2f}"
    if range1 < TRADER_MIN_RANGE1 and not aero_ok:
        return False, "trader_range1_block", f"{display_symbol(symbol)} {side}: range1 too weak x{range1:.2f}"
    if range5 < TRADER_MIN_RANGE5 and not aero_ok:
        return False, "trader_range5_block", f"{display_symbol(symbol)} {side}: range5 too weak x{range5:.2f}"

    if entry > 0 and tp5 > 0:
        need = abs(entry - tp5) / entry
        recent_move = max(abs(ch15m), abs(ch30m), abs(percent_change(c5, 6)))
        if recent_move < need * TRADER_MIN_TP5_FEASIBILITY:
            return False, "trader_tp5_feasibility_block", f"{display_symbol(symbol)} {side}: TP5 {need*100:.2f}% not feasible vs recent {recent_move*100:.2f}%"

    style_note = aero_reason if aero_ok else "standard trader-pattern ok"
    return True, "ok", (f"{style_note}; score {score}, grade {grade}, 1m3 {ch3m*100:+.2f}%, "
                        f"15m {ch15m*100:+.2f}%, vol1 x{vol1:.2f}, range1 x{range1:.2f}")

# ------------------------------------------------------------
# Cooldowns / dedupe
# ------------------------------------------------------------

def cooldown_ok(symbol: str, strategy: str) -> Tuple[bool, str]:
    t = now_ts()
    with STATE_LOCK:
        if t < STATE.setdefault("pair_cooldown", {}).get(symbol, 0):
            return False, "pair cooldown"
        if t < STATE.setdefault("strategy_cooldown", {}).get(strategy, 0):
            return False, "strategy cooldown"
    return True, "ok"


def duplicate_or_slot_block(candidate: Dict[str, Any]) -> Optional[str]:
    """Duplicate & correlated-risk protection:
    - never two active signals on the same symbol (any side);
    - per-side cap so we don't hold N correlated shorts on one BTC move."""
    with STATE_LOCK:
        active = STATE.get("active_signals", [])
        if any(s.get("symbol") == candidate["symbol"] for s in active):
            return "duplicate_symbol_active"
        same_side = sum(1 for s in active if s.get("side") == candidate["side"])
        if same_side >= MAX_ACTIVE_SAME_SIDE:
            return "same_side_slot_full"
        if len(active) >= MAX_ACTIVE_SIGNALS:
            return "active_slots_full"
    return None

# ------------------------------------------------------------
# Analysis
# ------------------------------------------------------------

def analyze_symbol(symbol: str, btc: Dict[str, Any], blocks: Dict[str, int], near_miss: List[str]) -> Optional[Dict[str, Any]]:
    symbol = normalize_symbol(symbol)
    c1 = get_klines(symbol, "1m", 120, cache_seconds=6)
    c5 = get_klines(symbol, "5m", 120, cache_seconds=15)
    c15 = get_klines(symbol, "15m", 120, cache_seconds=30)
    c1h = get_klines(symbol, "1h", 120, cache_seconds=90)

    if not c1 or not c5 or not c15 or not c1h:
        blocks["no_candles"] = blocks.get("no_candles", 0) + 1
        return None
    if ultra_risk_symbol(symbol, c5, c15):
        blocks["ultra_risk_block"] = blocks.get("ultra_risk_block", 0) + 1
        return None

    candidates: List[Dict[str, Any]] = []
    for side in ("LONG", "SHORT"):
        if side == "LONG" and not ALLOW_LONG:
            blocks["long_disabled"] = blocks.get("long_disabled", 0) + 1
            continue
        if side == "SHORT" and not ALLOW_SHORT:
            blocks["short_disabled"] = blocks.get("short_disabled", 0) + 1
            continue

        setup = fast_burst_setup(symbol, c1, c5, c15, c1h, btc, side)
        if not setup:
            setup = instant_edge_setup(symbol, c1, c5, c15, c1h, btc, side)
            if setup:
                blocks[f"instant_edge_{side.lower()}"] = blocks.get(f"instant_edge_{side.lower()}", 0) + 1
        if not setup and side == "SHORT":
            setup = market_dump_short_setup(symbol, c1, c5, c15, c1h, btc)
            if setup:
                blocks["market_dump_short"] = blocks.get("market_dump_short", 0) + 1
        if not setup:
            blocks[f"no_fast_{side.lower()}"] = blocks.get(f"no_fast_{side.lower()}", 0) + 1
            continue

        # Symmetric side-stats protection (was LONG-only before).
        stats_ok, stats_reason = side_live_stats_ok(side)
        if not stats_ok and setup.get("grade") != "A+":
            blocks[f"{side.lower()}_stats_protection_block"] = blocks.get(f"{side.lower()}_stats_protection_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: {stats_reason}; B-class skipped")
            continue

        co, _ = cooldown_ok(symbol, setup["strategy"])
        if not co:
            blocks["cooldown_block"] = blocks.get("cooldown_block", 0) + 1
            continue

        trade = calculate_fast_trade(setup, c1, c5)
        if not trade:
            blocks["sl_too_far_block"] = blocks.get("sl_too_far_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: SL too far / too volatile for local stop")
            continue

        if trade["score"] < B_MIN_SCORE:
            blocks["score_block"] = blocks.get("score_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: score {trade['score']}, vol x{trade['volume_ratio']:.2f}")
            continue

        q_ok, q_block, q_reason = professional_quality_gate(trade, symbol)
        if not q_ok:
            blocks[q_block] = blocks.get(q_block, 0) + 1
            if len(near_miss) < 8:
                near_miss.append(q_reason)
            continue

        t_ok, t_block, t_reason = trader_pattern_gate(trade, symbol, c1, c5, c15, btc)
        if not t_ok:
            blocks[t_block] = blocks.get(t_block, 0) + 1
            if len(near_miss) < 8:
                near_miss.append(t_reason)
            continue
        trade["trader_pattern_reason"] = t_reason
        candidates.append(trade)

    if not candidates:
        return None
    candidates.sort(key=lambda x: (x["grade"] == "A+", x["score"], x["ladder_rr"]), reverse=True)
    return candidates[0]

# ------------------------------------------------------------
# Formatting
# ------------------------------------------------------------

def format_price(x: Optional[float]) -> str:
    """Significant-digit formatting: safe for both 60000 and 0.0000000123."""
    if x is None or x <= 0:
        return "-"
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.5f}".rstrip("0").rstrip(".")
    # 6 significant digits for sub-1 prices
    digits = max(6, -int(math.floor(math.log10(x))) + 5)
    s = f"{x:.{min(digits, 12)}f}".rstrip("0").rstrip(".")
    return s if s not in ("", "0") else f"{x:.12f}"


def build_signal_message(s: Dict[str, Any]) -> str:
    arrow = "🟢" if s["side"] == "LONG" else "🔴"
    local_note = ""
    if s.get("local_stop_used"):
        local_note = (f"\n⚠️ Local scalp stop: структурная инвалидция была {s.get('original_sl_move', 0)*100:.2f}%, "
                      f"стоп сжат до {s.get('sl_move', 0)*100:.2f}% для быстрого исполнения.")
    return (
        f"{arrow} {s['side']} {display_symbol(s['symbol'])}\n"
        f"Класс: {s['grade']} · Score {s['score']} · {s['trade_type']}\n"
        f"Стратегия: {s['strategy']}\n\n"
        f"Вход: {format_price(s['entry'])}\n"
        f"TP1: {format_price(s['tp1'])} · ≈ {s['roi_tp1']:.1f}% ROI x{LEVERAGE}\n"
        f"TP2: {format_price(s['tp2'])}\n"
        f"TP3: {format_price(s['tp3'])}\n"
        f"TP4: {format_price(s['tp4'])}\n"
        f"TP5: {format_price(s['tp5'])}\n"
        f"SL: {format_price(s['sl'])} · риск {s['sl_move']*100:.2f}% цены ≈ {s['roi_sl']:.1f}% ROI x{LEVERAGE}{local_note}\n"
        f"RR TP1: {s['rr']:.2f} · Ladder RR: {s['ladder_rr']:.2f} · Final RR: {s['final_rr']:.2f}\n"
        f"Риск: multiplier x{s['risk_mult']:.2f} (информативно — бот не открывает сделки)\n\n"
        f"📌 Логика:\n{s['reason']}\n"
        f"15m: {s['ch15m']*100:+.2f}% · 30m: {s['ch30m']*100:+.2f}% · 1m3: {s['ch3m_1m']*100:+.2f}%\n"
        f"Vol15 x{s['volume_ratio']:.2f} · Range5 x{s['range_ratio']:.2f} · Vol1 x{s.get('vol1', 1.0):.2f} · Range1 x{s.get('range1', 1.0):.2f}\n"
        f"BTC: {s['btc_text']}\n\n"
        f"⏱ Scalping rule: если за {FAST_MAX_MINUTES_TO_TP1} минут нет движения к TP1 — сигнал expired.\n"
        f"❗ Это сигнал, а не рекомендация. Прибыль не гарантирована."
    )


def build_diagnostic(scan: Dict[str, Any]) -> str:
    blocks = scan.get("blocks", {})
    block_lines = [f"{k}: {v}" for k, v in sorted(blocks.items(), key=lambda kv: -kv[1])[:12]]
    hot = scan.get("hot_notes", [])[:8]
    near = scan.get("near_miss", [])[:8]
    paused, until = circuit_paused()
    cb_line = f"\n⛔ Circuit breaker активен до {time.strftime('%H:%M UTC', time.gmtime(until))}" if paused else ""
    with STATE_LOCK:
        total = STATE.get("stats", {}).get("total", {})
        last_err = STATE.get("last_error", "")
    return (
        f"🧪 Диагностика {DEPLOY_MARKER}\n"
        f"Проверено: {scan.get('checked', 0)} из universe {scan.get('universe', 0)}\n"
        f"Кандидатов: {scan.get('candidates', 0)} · отправлено: {scan.get('sent', 0)} · время: {scan.get('elapsed', 0):.0f}с\n"
        f"BTC: {scan.get('btc', 'unknown')}{cb_line}\n"
        f"Статистика: {wr_text(total)}\n\n"
        f"Hot symbols:\n" + ("\n".join(hot) if hot else "нет") +
        f"\n\nГлавные блокировки:\n" + ("\n".join(block_lines) if block_lines else "нет") +
        ("\n\nПочти прошли:\n" + "\n".join(near) if near else "") +
        f"\n\nLast error: {last_err}"
    )

# ------------------------------------------------------------
# Scan / track (pure sync; executed via asyncio.to_thread)
# ------------------------------------------------------------

def add_active_signal(s: Dict[str, Any]) -> None:
    with STATE_LOCK:
        STATE.setdefault("active_signals", []).append(s)
        STATE.setdefault("pair_cooldown", {})[s["symbol"]] = now_ts() + PAIR_COOLDOWN_SECONDS
        STATE.setdefault("strategy_cooldown", {})[s["strategy"]] = now_ts() + STRATEGY_COOLDOWN_SECONDS
    save_state()


def run_scan(manual: bool = False) -> Dict[str, Any]:
    if not SCAN_LOCK.acquire(blocking=False):
        return {"skipped": "scan already running"}
    try:
        start = time.time()
        HEARTBEAT["scan"] = start
        blocks: Dict[str, int] = {}
        near_miss: List[str] = []
        btc = btc_context()
        symbols = get_symbols()
        selected, hot_notes = select_hot_symbols(symbols)

        scan = {"checked": 0, "universe": len(symbols), "candidates": 0, "sent": 0,
                "blocks": blocks, "near_miss": near_miss, "hot_notes": hot_notes,
                "btc": btc.get("text", "BTC unknown"), "elapsed": 0}

        if not btc.get("ok"):
            blocks["btc_data_problem"] = 1
            with STATE_LOCK:
                STATE["last_scan"] = scan
            save_state()
            return scan

        paused, _ = circuit_paused()
        if paused:
            blocks["circuit_breaker_paused"] = 1
            with STATE_LOCK:
                STATE["last_scan"] = scan
            save_state()
            return scan

        found: List[Dict[str, Any]] = []
        for sym in selected:
            try:
                s = analyze_symbol(sym, btc, blocks, near_miss)
                scan["checked"] += 1
                if s:
                    found.append(s)
            except Exception as e:
                blocks["analyze_exception"] = blocks.get("analyze_exception", 0) + 1
                set_last_error(f"analyze {sym}: {repr(e)}")

        found.sort(key=lambda x: (x["grade"] == "A+", x["score"], x["ladder_rr"]), reverse=True)
        scan["candidates"] = len(found)

        sent = 0
        for s in found:
            if sent >= MAX_SIGNALS_PER_SCAN:
                break
            block_reason = duplicate_or_slot_block(s)
            if block_reason:
                blocks[block_reason] = blocks.get(block_reason, 0) + 1
                continue
            add_active_signal(s)
            send_telegram(build_signal_message(s))
            sent += 1

        scan["sent"] = sent
        scan["elapsed"] = time.time() - start
        with STATE_LOCK:
            STATE["last_scan"] = scan
        save_state()

        with STATE_LOCK:
            need_diag = manual or (sent == 0 and now_ts() - int(STATE.get("last_diag_ts", 0)) >= DIAG_SECONDS)
        if need_diag:
            send_telegram(build_diagnostic(scan))
            with STATE_LOCK:
                STATE["last_diag_ts"] = now_ts()
            save_state()
        return scan
    finally:
        SCAN_LOCK.release()


def current_price(symbol: str) -> Optional[float]:
    c = get_klines(symbol, "1m", 80, cache_seconds=4)
    return c[-1]["close"] if c else None


def target_hit(side: str, price: float, target: float) -> bool:
    return price >= target if side == "LONG" else price <= target


def sl_hit(side: str, price: float, sl: float) -> bool:
    return price <= sl if side == "LONG" else price >= sl


def directional_progress_ratio(s: Dict[str, Any], p: float) -> Tuple[bool, float]:
    entry = s["entry"]; tp1 = s["tp1"]
    full = abs(tp1 - entry)
    if full <= 0:
        return False, 0.0
    if s["side"] == "LONG":
        return p > entry, max(0.0, p - entry) / full
    return p < entry, max(0.0, entry - p) / full


def track_active_signals() -> None:
    if not TRACK_LOCK.acquire(blocking=False):
        return
    try:
        HEARTBEAT["track"] = time.time()
        with STATE_LOCK:
            active = list(STATE.get("active_signals", []))
        if not active:
            return

        remaining = []
        changed = False
        for s in active:
            p = current_price(s["symbol"])
            if p is None:
                remaining.append(s)
                continue
            side = s["side"]
            age_minutes = (now_ts() - int(s.get("created_at", now_ts()))) / 60.0

            if sl_hit(side, p, s["sl"]):
                apply_result(s, "sl")
                send_telegram(
                    f"❌ Stop Loss\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                    f"Стратегия: {s['strategy']}\nВход: {format_price(s['entry'])}\n"
                    f"SL: {format_price(s['sl'])}\nТекущая цена: {format_price(p)}\n\n{build_stats_text()}"
                )
                changed = True
                continue

            if FAST_CANCEL_IF_NO_PROGRESS and not s.get("tp1_hit") and age_minutes >= FAST_MAX_MINUTES_TO_TP1:
                directional, progress = directional_progress_ratio(s, p)
                if (not directional) or progress < FAST_MIN_PROGRESS_TO_KEEP:
                    apply_result(s, "expired")
                    send_telegram(
                        f"⏱ FAST TRADE EXPIRED\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                        f"Стратегия: {s['strategy']}\nЦена не реализовалась за {FAST_MAX_MINUTES_TO_TP1} минут.\n"
                        f"Вход: {format_price(s['entry'])}\nТекущая цена: {format_price(p)}\n"
                        f"TP1: {format_price(s['tp1'])}\nПрогресс к TP1: {progress*100:.1f}%\n\n{build_stats_text()}"
                    )
                    changed = True
                    continue

            if age_minutes >= FAST_HARD_EXPIRE_MINUTES and not s.get("tp1_hit"):
                apply_result(s, "expired")
                send_telegram(
                    f"⏱ HARD EXPIRE\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                    f"Стратегия: {s['strategy']}\nTP1 не достигнут за {FAST_HARD_EXPIRE_MINUTES} минут.\n"
                    f"Текущая цена: {format_price(p)}\n\n{build_stats_text()}"
                )
                changed = True
                continue

            hit_any = False
            for key in ["tp1", "tp2", "tp3", "tp4"]:
                if s.get(key) and not s.get(f"{key}_hit") and target_hit(side, p, s[key]):
                    s[f"{key}_hit"] = True
                    hit_any = True
                    send_telegram(
                        f"🎯 {key.upper()} HIT\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                        f"Стратегия: {s['strategy']}\n{key.upper()}: {format_price(s[key])}\n"
                        f"Текущая цена: {format_price(p)}"
                    )

            if s.get("tp5") and target_hit(side, p, s["tp5"]):
                s["tp5_hit"] = True
                apply_result(s, "profit")
                send_telegram(
                    f"✅ FULL LADDER TAKE PROFIT\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                    f"Стратегия: {s['strategy']}\nTP5 достигнут: {format_price(p)}\n"
                    f"Время в сделке: {age_minutes:.1f} мин\n\n{build_stats_text()}"
                )
                changed = True
                continue

            if hit_any:
                changed = True
            remaining.append(s)

        if changed:
            with STATE_LOCK:
                STATE["active_signals"] = remaining
            save_state()
    finally:
        TRACK_LOCK.release()

# ------------------------------------------------------------
# Async loops (event loop never blocked: sync work runs in threads)
# ------------------------------------------------------------

async def scan_loop():
    await asyncio.sleep(3)
    await asyncio.to_thread(
        send_telegram,
        f"✅ {APP_NAME} активирован.\nDeploy marker: {DEPLOY_MARKER}\n\n"
        f"Логика: торгуем не фазу рынка, а короткий дисбаланс: hot coin → sweep/reclaim → "
        f"EMA/VWAP → immediate continuation → 5 TP.\n"
        f"Time-stop: TP1 не двигается за {FAST_MAX_MINUTES_TO_TP1} мин — expired.\n"
        f"Targets: {TP1_MOVE*100:.2f}% / {TP2_MOVE*100:.2f}% / {TP3_MOVE*100:.2f}% / {TP4_MOVE*100:.2f}% / {TP5_MOVE*100:.2f}%.\n"
        f"Circuit breaker: пауза после {MAX_CONSECUTIVE_SL} подряд SL или {MAX_DAILY_SL} SL за день.\n"
        f"❗ Бот только присылает сигналы и не гарантирует прибыль."
    )
    try:
        scan = await asyncio.to_thread(run_scan, True)
        await asyncio.to_thread(send_telegram, build_diagnostic(scan))
    except Exception as e:
        set_last_error(f"first scan exception: {repr(e)}")
        await asyncio.to_thread(send_telegram, f"⚠️ Ошибка первого скана: {repr(e)}")

    while True:
        try:
            if AUTO_SCAN_ENABLED:
                await asyncio.to_thread(run_scan, False)
        except Exception as e:
            set_last_error(f"scan_loop: {repr(e)}")
            log.exception("scan_loop error")
        await asyncio.sleep(AUTO_SCAN_SECONDS)


async def track_loop():
    await asyncio.sleep(8)
    while True:
        try:
            if AUTO_TRACK_ENABLED:
                await asyncio.to_thread(track_active_signals)
        except Exception as e:
            set_last_error(f"track_loop: {repr(e)}")
            log.exception("track_loop error")
        await asyncio.sleep(AUTO_TRACK_SECONDS)


async def watchdog_loop():
    """Hang protection: if scan or track have not run recently, alert once to Telegram."""
    await asyncio.sleep(120)
    while True:
        try:
            now = time.time()
            scan_stale = AUTO_SCAN_ENABLED and (now - HEARTBEAT["scan"]) > WATCHDOG_STALE_MINUTES * 60
            track_stale = AUTO_TRACK_ENABLED and (now - HEARTBEAT["track"]) > WATCHDOG_STALE_MINUTES * 60
            if (scan_stale or track_stale) and not HEARTBEAT["watchdog_alerted"]:
                HEARTBEAT["watchdog_alerted"] = True
                await asyncio.to_thread(
                    send_telegram,
                    f"🚨 WATCHDOG: loop stale > {WATCHDOG_STALE_MINUTES} мин "
                    f"(scan_stale={scan_stale}, track_stale={track_stale}). Проверьте /health."
                )
            if not (scan_stale or track_stale):
                HEARTBEAT["watchdog_alerted"] = False
        except Exception as e:
            set_last_error(f"watchdog: {repr(e)}")
        await asyncio.sleep(60)

# ------------------------------------------------------------
# FastAPI app (lifespan instead of deprecated on_event)
# ------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global STATE
    STATE = load_state()
    HEARTBEAT["scan"] = time.time()
    HEARTBEAT["track"] = time.time()
    tasks = [
        asyncio.create_task(scan_loop()),
        asyncio.create_task(track_loop()),
        asyncio.create_task(watchdog_loop()),
    ]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        save_state()


app = FastAPI(title=APP_NAME, lifespan=lifespan)


@app.get("/")
def root():
    return HTMLResponse(
        f"<h3>{APP_NAME}</h3><p>{DEPLOY_MARKER}</p>"
        f"<p>Use /health /version /scan /auto-status /stats /test-telegram</p>"
    )


@app.get("/health")
def health():
    paused, until = circuit_paused()
    with STATE_LOCK:
        active = len(STATE.get("active_signals", []))
        last_err = STATE.get("last_error", "")
    return {
        "ok": True, "app": APP_NAME, "deploy": DEPLOY_MARKER,
        "active": active, "last_error": last_err,
        "circuit_paused": paused, "circuit_paused_until": until,
        "heartbeat_scan_age_s": round(time.time() - HEARTBEAT["scan"], 1),
        "heartbeat_track_age_s": round(time.time() - HEARTBEAT["track"], 1),
        "api_fail_streak": API_FAIL_STREAK["count"],
    }


@app.get("/version")
def version():
    return {"app": APP_NAME, "deploy_marker": DEPLOY_MARKER}


@app.get("/auto-status")
def auto_status():
    with STATE_LOCK:
        payload = {
            "app": APP_NAME, "deploy": DEPLOY_MARKER,
            "active_signals": STATE.get("active_signals", []),
            "last_scan": STATE.get("last_scan", {}),
            "last_error": STATE.get("last_error", ""),
            "stats": STATE.get("stats", {}),
            "circuit": STATE.get("circuit", {}),
        }
    return JSONResponse(payload)


@app.get("/scan")
async def manual_scan(send: bool = Query(True)):
    scan = await asyncio.to_thread(run_scan, True)
    if send and "skipped" not in scan:
        await asyncio.to_thread(send_telegram, build_diagnostic(scan))
    return JSONResponse(scan)


@app.get("/stats")
def stats():
    return HTMLResponse("<pre>" + build_stats_text() + "</pre>")


@app.get("/test-telegram")
def test_telegram():
    ok = send_telegram(f"✅ Test Telegram OK\n{APP_NAME}\n{DEPLOY_MARKER}")
    with STATE_LOCK:
        last_err = STATE.get("last_error", "")
    return {"sent": ok, "last_error": last_err}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    # ВАЖНО: workers=1. Несколько воркеров = несколько независимых STATE
    # и дублирующиеся сигналы в Telegram.
    uvicorn.run(app, host="0.0.0.0", port=port, workers=1)
