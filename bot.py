import os
import time
import json
import random
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

# ============================================================
# V13.25 — TRADER PATTERN QUALITY SCALPER
# Professional goal:
# Trade only short-lived market situations with immediate edge.
# No trend prediction, no market phase guessing.
#
# Core idea:
# hot coin -> fresh imbalance -> micro pullback/liquidity grab -> EMA/VWAP reclaim/reject
# -> immediate continuation -> compact 5-target exit.
#
# If the trade does not start paying quickly, it is not the setup and gets expired.
# Important: this bot sends signals/alerts. It does not guarantee profit.
# V13.25 fix: adds a trader-pattern gate based on the user examples.
# The bot should not send weak B-class noise: it needs leader/laggard pressure, real range, and a ladder that can realistically move 3-4%.
# ============================================================

APP_NAME = "Professional Adaptive Futures Bot AUTO V13.26 BALANCED TRADER SCALPER"
DEPLOY_MARKER = "V13_26_BALANCED_TRADER_SCALPER_2026_06_25"

app = FastAPI(title=APP_NAME)

BINGX_BASE_URL = "https://open-api.bingx.com"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

STATE_FILE = os.getenv("STATE_FILE", "bot_state_v13_26_balanced_trader_scalper.json")
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"

# --- Scan stability ---
AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "15"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "3"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
API_RETRIES = int(os.getenv("API_RETRIES", "3"))
API_THROTTLE_SECONDS = float(os.getenv("API_THROTTLE_SECONDS", "0.04"))
MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "450"))
MAX_ANALYZE_SYMBOLS = int(os.getenv("MAX_ANALYZE_SYMBOLS", "180"))
HOT_SYMBOLS_TO_ANALYZE = int(os.getenv("HOT_SYMBOLS_TO_ANALYZE", "60"))
DIAG_SECONDS = int(os.getenv("DIAG_SECONDS", "1200"))

# --- Signal limits ---
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "88"))
B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "80"))
MAX_ACTIVE_SIGNALS = int(os.getenv("MAX_ACTIVE_SIGNALS", "2"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "2"))
PAIR_COOLDOWN_SECONDS = int(os.getenv("PAIR_COOLDOWN_SECONDS", "600"))
STRATEGY_COOLDOWN_SECONDS = int(os.getenv("STRATEGY_COOLDOWN_SECONDS", "90"))

# --- Fast burst requirements ---
FAST_BURST_ENABLED = os.getenv("FAST_BURST_ENABLED", "true").lower() == "true"
FAST_MIN_15M_MOVE = float(os.getenv("FAST_MIN_15M_MOVE", "0.0045"))        # 1.0% in 15m
FAST_MIN_30M_MOVE = float(os.getenv("FAST_MIN_30M_MOVE", "0.0070"))        # 1.6% in 30m
FAST_MAX_30M_MOVE = float(os.getenv("FAST_MAX_30M_MOVE", "0.090"))        # avoid late vertical chase
FAST_MIN_RANGE_RATIO = float(os.getenv("FAST_MIN_RANGE_RATIO", "0.82"))   # current 5m range expansion
FAST_MIN_VOLUME_RATIO = float(os.getenv("FAST_MIN_VOLUME_RATIO", "0.35")) # current 15m volume expansion
FAST_MIN_1M_CONFIRM = float(os.getenv("FAST_MIN_1M_CONFIRM", "0.00055"))   # 0.15% last 3m direction
# V13.19: fast scalps can be either continuation OR blow-off reversal.
# Example from diagnostics: 30m +16%, last 3m -1% can be a valid SHORT scalp, not a rejection.
REVERSAL_ENABLED = os.getenv("REVERSAL_ENABLED", "true").lower() == "true"
REVERSAL_MIN_30M_MOVE = float(os.getenv("REVERSAL_MIN_30M_MOVE", "0.018"))
REVERSAL_MIN_LIVE_COUNTER_MOVE = float(os.getenv("REVERSAL_MIN_LIVE_COUNTER_MOVE", "0.0012"))
LIVE_BYPASS_VOLUME_MOVE = float(os.getenv("LIVE_BYPASS_VOLUME_MOVE", "0.0035"))
LIVE_BYPASS_RANGE_RATIO = float(os.getenv("LIVE_BYPASS_RANGE_RATIO", "1.35"))
FAST_MAX_SPREAD_PROXY = float(os.getenv("FAST_MAX_SPREAD_PROXY", "0.030"))# current 5m candle too wide/chase block
EDGE_MIN_PRIOR_COMPRESSION = float(os.getenv("EDGE_MIN_PRIOR_COMPRESSION", "99.0")) # prior 5m range should be smaller before expansion
EDGE_MIN_BREAKOUT_DISTANCE = float(os.getenv("EDGE_MIN_BREAKOUT_DISTANCE", "0.00005")) # 0.12% micro break beyond prior 1m structure
EDGE_REQUIRE_MICRO_SWEEP = os.getenv("EDGE_REQUIRE_MICRO_SWEEP", "false").lower() == "true"

# --- Realtime pressure gate ---
# Previous versions expired because they detected a pattern after the flow had already died.
# These filters require live 1m pressure at the exact signal moment.
HOT_MIN_SCORE = float(os.getenv("HOT_MIN_SCORE", "14"))
HOT_MIN_LIVE_MOVE_3M = float(os.getenv("HOT_MIN_LIVE_MOVE_3M", "0.0006"))
HOT_MIN_LIVE_RANGE_OR_VOLUME = float(os.getenv("HOT_MIN_LIVE_RANGE_OR_VOLUME", "0.70"))
HOT_STALE_PENALTY_ENABLED = os.getenv("HOT_STALE_PENALTY_ENABLED", "true").lower() == "true"
REALTIME_MIN_1M_RANGE_RATIO = float(os.getenv("REALTIME_MIN_1M_RANGE_RATIO", "0.45"))
REALTIME_MIN_1M_VOLUME_RATIO = float(os.getenv("REALTIME_MIN_1M_VOLUME_RATIO", "0.20"))
REALTIME_MIN_2M_MOVE = float(os.getenv("REALTIME_MIN_2M_MOVE", "0.00045"))
REALTIME_CLOSE_LOCATION_LONG = float(os.getenv("REALTIME_CLOSE_LOCATION_LONG", "0.57"))
REALTIME_CLOSE_LOCATION_SHORT = float(os.getenv("REALTIME_CLOSE_LOCATION_SHORT", "0.43"))
REALTIME_REQUIRE_TWO_1M_CANDLES = os.getenv("REALTIME_REQUIRE_TWO_1M_CANDLES", "false").lower() == "true"
EDGE_MIN_TP5_FEASIBILITY = float(os.getenv("EDGE_MIN_TP5_FEASIBILITY", "0.50")) # recent 15m move should cover most of TP5 distance

# --- Pullback/retest requirements ---
PULLBACK_MIN = float(os.getenv("PULLBACK_MIN", "0.0015"))                 # 0.25%
PULLBACK_MAX = float(os.getenv("PULLBACK_MAX", "0.0400"))                 # 3.0%
RECLAIM_BUFFER = float(os.getenv("RECLAIM_BUFFER", "0.0005"))
CLOSE_LOCATION_MIN_LONG = float(os.getenv("CLOSE_LOCATION_MIN_LONG", "0.52"))
CLOSE_LOCATION_MAX_SHORT = float(os.getenv("CLOSE_LOCATION_MAX_SHORT", "0.48"))

# --- Compact ladder TPs for fast 10-minute realization style ---
# These are intentionally more compact than slow ladder targets.
# Trader-example ladder: AERO/PORTAL/HOME/VELVET style targets are not tiny 0.3% scalps.
# TP1 should be reachable quickly, but TP5 should represent a real 3-4% move when volatility allows.
TP1_MOVE = float(os.getenv("TP1_MOVE", "0.0065"))
TP2_MOVE = float(os.getenv("TP2_MOVE", "0.0120"))
TP3_MOVE = float(os.getenv("TP3_MOVE", "0.0185"))
TP4_MOVE = float(os.getenv("TP4_MOVE", "0.0260"))
TP5_MOVE = float(os.getenv("TP5_MOVE", "0.0350"))

# --- Risk / stop ---
SL_ATR_MULT = float(os.getenv("SL_ATR_MULT", "0.80"))
MIN_SL_MOVE = float(os.getenv("MIN_SL_MOVE", "0.0100"))                  # min 1.0% price risk
MAX_SL_MOVE = float(os.getenv("MAX_SL_MOVE", "0.0260"))                  # technical invalidation cap for example-style ladder
FAST_RISK_MULT = float(os.getenv("FAST_RISK_MULT", "0.08"))
A_RISK_MULT = float(os.getenv("A_RISK_MULT", "0.14"))

# --- V13.22 professional quality gate ---
# Blocks mathematically bad scalps like: TP1 small, SL huge, weak live volume, poor ladder RR.
MAX_SCALP_SL_ROI = float(os.getenv("MAX_SCALP_SL_ROI", "18.0"))
MIN_TP1_RR = float(os.getenv("MIN_TP1_RR", "0.20"))
MIN_LADDER_RR_HARD = float(os.getenv("MIN_LADDER_RR_HARD", "0.62"))
MIN_FINAL_RR_HARD = float(os.getenv("MIN_FINAL_RR_HARD", "1.15"))
MIN_LIVE_VOL_NORMAL = float(os.getenv("MIN_LIVE_VOL_NORMAL", "0.50"))
MIN_LIVE_VOL_STRONG_PRICE = float(os.getenv("MIN_LIVE_VOL_STRONG_PRICE", "0.30"))
STRONG_1M3_MOVE = float(os.getenv("STRONG_1M3_MOVE", "0.0050"))
STRONG_RANGE1 = float(os.getenv("STRONG_RANGE1", "1.25"))
HEAVY_MIN_FINAL_RR = float(os.getenv("HEAVY_MIN_FINAL_RR", "1.25"))
HEAVY_MAX_SL_ROI = float(os.getenv("HEAVY_MAX_SL_ROI", "13.0"))
HEAVY_MIN_LIVE_VOL = float(os.getenv("HEAVY_MIN_LIVE_VOL", "0.70"))
HEAVY_BASES = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "LINK", "AVAX",
    "DOT", "LTC", "BCH", "XMR", "GMX", "AAVE", "UNI", "ATOM", "ETC", "FIL"
}

# --- V13.24 Instant Edge fallback ---
# This mode catches the examples-style micro-moment when the coin is moving NOW,
# but the older pullback/EMA/VWAP setup is too slow and returns no_fast.
# It is not a loose mode: final RR/SL/live-volume quality gate still applies after trade construction.
INSTANT_EDGE_ENABLED = os.getenv("INSTANT_EDGE_ENABLED", "true").lower() == "true"
INSTANT_MIN_1M3_MOVE = float(os.getenv("INSTANT_MIN_1M3_MOVE", "0.0055"))
INSTANT_MIN_15M_MOVE = float(os.getenv("INSTANT_MIN_15M_MOVE", "0.0040"))
INSTANT_MIN_VOL1 = float(os.getenv("INSTANT_MIN_VOL1", "0.45"))
INSTANT_MIN_RANGE1 = float(os.getenv("INSTANT_MIN_RANGE1", "0.85"))
INSTANT_MIN_VOL5 = float(os.getenv("INSTANT_MIN_VOL5", "0.55"))
INSTANT_MIN_RANGE5 = float(os.getenv("INSTANT_MIN_RANGE5", "0.70"))
INSTANT_CLOSE_LONG = float(os.getenv("INSTANT_CLOSE_LONG", "0.60"))
INSTANT_CLOSE_SHORT = float(os.getenv("INSTANT_CLOSE_SHORT", "0.40"))
INSTANT_MIN_BODY = float(os.getenv("INSTANT_MIN_BODY", "0.34"))
INSTANT_MAX_30M_CHASE = float(os.getenv("INSTANT_MAX_30M_CHASE", "0.065"))
INSTANT_ALLOW_STRONG_1M_EXCEPTION = os.getenv("INSTANT_ALLOW_STRONG_1M_EXCEPTION", "true").lower() == "true"

# --- V13.25 trader-pattern quality gate ---
# Built from the examples: AERO/PORTAL/HOME/WLD/VELVET are not random hot ticks.
# They are either continuation after a controlled pullback/reject, or a leader/laggard relative-strength exception.
TRADER_PATTERN_GATE_ENABLED = os.getenv("TRADER_PATTERN_GATE_ENABLED", "true").lower() == "true"
TRADER_MIN_SCORE = int(os.getenv("TRADER_MIN_SCORE", "82"))
TRADER_ALLOW_B_SCORE = os.getenv("TRADER_ALLOW_B_SCORE", "true").lower() == "true"
TRADER_MIN_ABS_1M3 = float(os.getenv("TRADER_MIN_ABS_1M3", "0.0048"))
TRADER_MIN_ABS_15M = float(os.getenv("TRADER_MIN_ABS_15M", "0.0055"))
TRADER_MIN_ABS_30M = float(os.getenv("TRADER_MIN_ABS_30M", "0.0080"))
TRADER_MIN_VOL1 = float(os.getenv("TRADER_MIN_VOL1", "0.52"))
TRADER_MIN_VOL5 = float(os.getenv("TRADER_MIN_VOL5", "0.52"))
TRADER_MIN_RANGE1 = float(os.getenv("TRADER_MIN_RANGE1", "0.85"))
TRADER_MIN_RANGE5 = float(os.getenv("TRADER_MIN_RANGE5", "0.75"))
TRADER_MIN_TP5_FEASIBILITY = float(os.getenv("TRADER_MIN_TP5_FEASIBILITY", "0.50"))
TRADER_NEED_5M_DIRECTION = os.getenv("TRADER_NEED_5M_DIRECTION", "false").lower() == "true"
TRADER_BLOCK_WEAK_CONTINUATION = os.getenv("TRADER_BLOCK_WEAK_CONTINUATION", "true").lower() == "true"
TRADER_MAX_COUNTER_30M = float(os.getenv("TRADER_MAX_COUNTER_30M", "0.0100"))
TRADER_REQUIRE_MICRO_BREAK = os.getenv("TRADER_REQUIRE_MICRO_BREAK", "true").lower() == "true"
TRADER_CLOSE_LONG = float(os.getenv("TRADER_CLOSE_LONG", "0.57"))
TRADER_CLOSE_SHORT = float(os.getenv("TRADER_CLOSE_SHORT", "0.43"))
TRADER_HEAVY_ONLY_A_PLUS = os.getenv("TRADER_HEAVY_ONLY_A_PLUS", "true").lower() == "true"


# --- Time stop / no-stall logic ---
FAST_MAX_MINUTES_TO_TP1 = int(os.getenv("FAST_MAX_MINUTES_TO_TP1", "6"))
FAST_HARD_EXPIRE_MINUTES = int(os.getenv("FAST_HARD_EXPIRE_MINUTES", "11"))
FAST_MIN_PROGRESS_TO_KEEP = float(os.getenv("FAST_MIN_PROGRESS_TO_KEEP", "0.25"))
FAST_CANCEL_IF_NO_PROGRESS = os.getenv("FAST_CANCEL_IF_NO_PROGRESS", "true").lower() == "true"

# --- Market shock context ---
# We do not trade market phase/trend. BTC is used only as a shock filter.
BTC_SHOCK_15M_BLOCK = float(os.getenv("BTC_SHOCK_15M_BLOCK", "0.020")) # avoid alt scalp during violent BTC shock

# --- Side control / professional LONG repair ---
# SHORT is already working in live stats. LONG is now stricter and must look like a real reclaim,
# not a late buy at the end of a pump.
ALLOW_LONG = os.getenv("ALLOW_LONG", "true").lower() == "true"
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "true").lower() == "true"
LONG_BLOCK_BTC_BEAR = os.getenv("LONG_BLOCK_BTC_BEAR", "false").lower() == "true"
LONG_MIN_1M_VOLUME_RATIO = float(os.getenv("LONG_MIN_1M_VOLUME_RATIO", "0.75"))
LONG_MIN_1M_RANGE_RATIO = float(os.getenv("LONG_MIN_1M_RANGE_RATIO", "0.80"))
LONG_MIN_3M_CONFIRM = float(os.getenv("LONG_MIN_3M_CONFIRM", "0.0012"))     # 0.12% in 3m
LONG_MIN_CLOSE_LOCATION = float(os.getenv("LONG_MIN_CLOSE_LOCATION", "0.72"))
LONG_MAX_15M_CHASE = float(os.getenv("LONG_MAX_15M_CHASE", "0.040"))        # above this needs pullback/sweep
LONG_MAX_30M_CHASE = float(os.getenv("LONG_MAX_30M_CHASE", "0.070"))
LONG_MIN_PULLBACK_AFTER_PUMP = float(os.getenv("LONG_MIN_PULLBACK_AFTER_PUMP", "0.0055"))
LONG_MAX_PULLBACK_AFTER_PUMP = float(os.getenv("LONG_MAX_PULLBACK_AFTER_PUMP", "0.038"))
LONG_REQUIRE_SWEEP_OR_RECLAIM = os.getenv("LONG_REQUIRE_SWEEP_OR_RECLAIM", "true").lower() == "true"
LONG_REQUIRE_HIGHER_LOW = os.getenv("LONG_REQUIRE_HIGHER_LOW", "true").lower() == "true"
LONG_STATS_PROTECTION = os.getenv("LONG_STATS_PROTECTION", "true").lower() == "true"
LONG_STATS_MIN_CLOSED = int(os.getenv("LONG_STATS_MIN_CLOSED", "4"))
LONG_STATS_MIN_WR = float(os.getenv("LONG_STATS_MIN_WR", "40"))

# --- V13.21 context-adaptive rules ---
# Professional idea: BTC direction is not a simple long/short switch.
# LONG is allowed in a bearish market only if the coin is showing clear relative strength
# and live reclaim pressure. SHORT is prioritized during BTC dump, but not chased without
# a bounce/reject structure.
CONTEXT_ADAPTIVE_ENABLED = os.getenv("CONTEXT_ADAPTIVE_ENABLED", "true").lower() == "true"
BTC_DUMP_SHORT_BIAS_ENABLED = os.getenv("BTC_DUMP_SHORT_BIAS_ENABLED", "true").lower() == "true"
BTC_DUMP_1H = float(os.getenv("BTC_DUMP_1H", "-0.012"))
BTC_DUMP_6H = float(os.getenv("BTC_DUMP_6H", "-0.025"))
LONG_ALLOW_BEAR_RELATIVE_STRENGTH = os.getenv("LONG_ALLOW_BEAR_RELATIVE_STRENGTH", "true").lower() == "true"
LONG_BEAR_MIN_ALT_15M = float(os.getenv("LONG_BEAR_MIN_ALT_15M", "0.0065"))
LONG_BEAR_MIN_ALT_30M = float(os.getenv("LONG_BEAR_MIN_ALT_30M", "0.0100"))
LONG_BEAR_MIN_1M3 = float(os.getenv("LONG_BEAR_MIN_1M3", "0.0020"))
LONG_BEAR_MIN_REL_STRENGTH_1H = float(os.getenv("LONG_BEAR_MIN_REL_STRENGTH_1H", "0.010"))
LONG_BEAR_MIN_VOL1 = float(os.getenv("LONG_BEAR_MIN_VOL1", "0.90"))
LONG_BEAR_MIN_RANGE1 = float(os.getenv("LONG_BEAR_MIN_RANGE1", "0.95"))
LONG_BEAR_MIN_CLOSE_LOCATION = float(os.getenv("LONG_BEAR_MIN_CLOSE_LOCATION", "0.76"))
SHORT_DUMP_ALLOW_EXTENDED_30M = float(os.getenv("SHORT_DUMP_ALLOW_EXTENDED_30M", "0.145"))
SHORT_DUMP_MIN_LIVE_1M3 = float(os.getenv("SHORT_DUMP_MIN_LIVE_1M3", "-0.0014"))
SHORT_DUMP_MIN_BOUNCE = float(os.getenv("SHORT_DUMP_MIN_BOUNCE", "0.0025"))


# --- Ultra-risk blocks ---
ULTRA_RISK_5M_CANDLE = float(os.getenv("ULTRA_RISK_5M_CANDLE", "0.095"))
ULTRA_RISK_15M_CANDLE = float(os.getenv("ULTRA_RISK_15M_CANDLE", "0.140"))

SCALP_STRATEGIES = {"PRO_SCALPING_EDGE_LONG", "PRO_SCALPING_EDGE_SHORT"}

QUALITY_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "LINK", "AVAX", "AAVE", "SUI", "TAO", "NEAR", "INJ",
    "OP", "ARB", "APT", "TIA", "ADA", "DOT", "MATIC", "TON", "LTC", "BCH", "ETC", "FIL", "ATOM",
    "UNI", "RUNE", "SEI", "FET", "WLD", "DOGE", "TRX", "ENA", "JUP", "ORDI",
    "PORTAL", "HOME", "TAC", "VELVET", "BEAT", "BLESS"
}

# Do not include VELVET here; user gave a successful VELVET long example.
ULTRA_RISK_KEYWORDS = {
    "1000", "PEPE", "BONK", "WIF", "MEME", "DOGS", "CATI", "HMSTR", "GOBLIN", "MOG", "TURBO",
    "BOME", "NEIRO", "PNUT", "MOODENG", "ACT", "GOAT", "FIGHT", "BLEND", "MAGMA"
}

FALLBACK_SYMBOLS = [f"{b}-USDT" for b in [
    "BTC", "ETH", "SOL", "BNB", "XRP", "LINK", "AVAX", "AAVE", "SUI", "TAO", "NEAR", "INJ",
    "OP", "ARB", "APT", "TIA", "ADA", "DOT", "LTC", "BCH", "ETC", "FIL", "ATOM", "UNI",
    "RUNE", "SEI", "FET", "WLD", "DOGE", "TRX", "ENA", "JUP", "ORDI", "BEAT", "BLESS",
    "KAITO", "XLM", "WLFI", "PUMP", "PORTAL", "HOME", "TAC", "VELVET"
]]

STATE: Dict[str, Any] = {}
KLINE_CACHE: Dict[str, Tuple[float, Optional[List[Dict[str, float]]]]] = {}
TICKER_CACHE: Dict[str, Tuple[float, Optional[List[str]]]] = {}

# ============================================================
# State / utilities
# ============================================================

def now_ts() -> int:
    return int(time.time())


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
        "stats": {
            "total": {"profit": 0, "sl": 0, "expired": 0},
            "side": {},
            "grade": {},
            "strategy": {},
            "symbol": {},
            "type": {},
        },
        "pair_cooldown": {},
        "strategy_cooldown": {},
        "last_scan": {},
        "last_diag_ts": 0,
        "last_error": "",
    }


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        base = default_state()
        if isinstance(data, dict):
            base.update(data)
        return base
    except Exception:
        return default_state()


def save_state() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def inc_stat(bucket: str, key: str, result: str) -> None:
    stats = STATE.setdefault("stats", default_state()["stats"])
    d = stats.setdefault(bucket, {})
    item = d.setdefault(key, {"profit": 0, "sl": 0, "expired": 0})
    item[result] = item.get(result, 0) + 1


def apply_result(signal: Dict[str, Any], result: str) -> None:
    if result not in ("profit", "sl", "expired"):
        return
    stats = STATE.setdefault("stats", default_state()["stats"])
    stats.setdefault("total", {"profit": 0, "sl": 0, "expired": 0})[result] += 1
    inc_stat("side", signal.get("side", "?"), result)
    inc_stat("grade", signal.get("grade", "?"), result)
    inc_stat("strategy", signal.get("strategy", "?"), result)
    inc_stat("symbol", signal.get("symbol", "?"), result)
    inc_stat("type", signal.get("trade_type", "?"), result)
    save_state()


def wr_text(item: Dict[str, int]) -> str:
    p = int(item.get("profit", 0))
    sl = int(item.get("sl", 0))
    exp = int(item.get("expired", 0))
    closed = p + sl + exp
    wr = p / closed * 100 if closed else 0.0
    return f"{p} профит / {sl} SL / {exp} expired / WR {wr:.1f}%"


def build_stats_text() -> str:
    stats = STATE.setdefault("stats", default_state()["stats"])
    lines = ["📊 Статистика", f"Итого: {wr_text(stats.get('total', {}))}"]
    for title, key in [("Стороны", "side"), ("Классы", "grade"), ("Стратегии", "strategy"), ("Типы", "type")]:
        data = stats.get(key, {})
        if data:
            lines.append(f"\n{title}:")
            for k, v in sorted(data.items(), key=lambda kv: -(kv[1].get("profit", 0) + kv[1].get("sl", 0) + kv[1].get("expired", 0)))[:12]:
                lines.append(f"{k}: {wr_text(v)}")
    return "\n".join(lines)

# ============================================================
# Telegram / API
# ============================================================

def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        STATE["last_error"] = "Telegram env missing: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID"
        save_state()
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:3900]}, timeout=10)
        if not r.ok:
            STATE["last_error"] = f"Telegram error {r.status_code}: {r.text[:250]}"
            save_state()
            return False
        return True
    except Exception as e:
        STATE["last_error"] = f"Telegram exception: {repr(e)}"
        save_state()
        return False


def get_json(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    url = BINGX_BASE_URL + path
    last_err = None
    for attempt in range(API_RETRIES):
        try:
            time.sleep(API_THROTTLE_SECONDS)
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code} {path}"
                time.sleep(0.25 * (attempt + 1))
                continue
            return r.json()
        except Exception as e:
            last_err = f"get_json {path}: {repr(e)}"
            time.sleep(0.35 * (attempt + 1))
    STATE["last_error"] = last_err or "unknown API error"
    save_state()
    return None


def parse_klines(raw: Any) -> Optional[List[Dict[str, float]]]:
    if not raw:
        return None
    candles: List[Dict[str, float]] = []
    for c in raw:
        try:
            if isinstance(c, dict):
                candles.append({
                    "time": int(c.get("time") or c.get("openTime") or c.get("T") or 0),
                    "open": float(c.get("open")),
                    "high": float(c.get("high")),
                    "low": float(c.get("low")),
                    "close": float(c.get("close")),
                    "volume": float(c.get("volume") or c.get("vol") or 0),
                })
            elif isinstance(c, (list, tuple)) and len(c) >= 6:
                candles.append({
                    "time": int(c[0]),
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                })
        except Exception:
            continue
    candles = [x for x in candles if x["open"] > 0 and x["high"] > 0 and x["low"] > 0 and x["close"] > 0]
    candles.sort(key=lambda x: x["time"])
    return candles if len(candles) >= 30 else None


def get_klines(symbol: str, interval: str, limit: int = 180, cache_seconds: int = 20) -> Optional[List[Dict[str, float]]]:
    symbol = normalize_symbol(symbol)
    key = f"{symbol}:{interval}:{limit}"
    cached = KLINE_CACHE.get(key)
    if cached and time.time() - cached[0] < cache_seconds:
        return cached[1]
    for ep in ["/openApi/swap/v3/quote/klines", "/openApi/swap/v2/quote/klines"]:
        data = get_json(ep, {"symbol": symbol, "interval": interval, "limit": limit})
        if not data:
            continue
        candles = parse_klines(data.get("data"))
        if candles:
            KLINE_CACHE[key] = (time.time(), candles)
            return candles
    KLINE_CACHE[key] = (time.time(), None)
    return None


def is_good_contract_symbol(symbol: str) -> bool:
    s = normalize_symbol(symbol)
    if not s.endswith("-USDT"):
        return False
    b = base_asset(s)
    if any(x in b for x in ["USD", "USDC", "BULL", "BEAR"]):
        return False
    return True


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
    # Ensure important user examples are always included if contracts exist/fallback is needed.
    for s in FALLBACK_SYMBOLS:
        if s not in out:
            out.append(s)
    random.shuffle(out)
    quality = [s for s in out if base_asset(s) in QUALITY_BASES]
    rest = [s for s in out if base_asset(s) not in QUALITY_BASES]
    result = (quality + rest)[:MAX_CONTRACTS]
    TICKER_CACHE["symbols"] = (time.time(), result)
    return result

# ============================================================
# Indicators
# ============================================================

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
    a = candles[-bars]["close"]
    b = candles[-1]["close"]
    return (b - a) / a if a else 0.0


def volume_ratio(candles: List[Dict[str, float]], n: int = 30) -> float:
    if len(candles) < n + 2:
        return 1.0
    cur = candles[-1]["volume"]
    avg = sum(x["volume"] for x in candles[-n - 1:-1]) / n
    return cur / avg if avg > 0 else 1.0


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
    """Lower values mean the market compressed before the impulse.
    A good scalp often comes after short compression then range expansion.
    """
    if len(c5) < n + 8:
        return 1.0
    prior = c5[-n-1:-1]
    older = c5[-n-8:-n-1]
    prior_avg = sum(candle_range(x) for x in prior) / max(len(prior), 1)
    older_avg = sum(candle_range(x) for x in older) / max(len(older), 1)
    return prior_avg / older_avg if older_avg > 0 else 1.0


def micro_structure_break(c1: List[Dict[str, float]], side: str) -> Tuple[bool, str]:
    """Require immediate 1m continuation, not a slow/stuck drift.
    LONG: latest close must break above recent 1m highs.
    SHORT: latest close must break below recent 1m lows.
    """
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
    """Liquidity-grab filter. We want a tiny stop-hunt / failed micro move, then reclaim/reject.
    This is optional but enabled by default because it matches discretionary scalping better.
    """
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
    """If recent velocity cannot realistically cover TP5, skip.
    The examples reached all takes quickly; this blocks slow setups.
    """
    if len(c5) < 8:
        return False, "not enough candles for TP5 feasibility"
    recent_abs_15m = abs(percent_change(c5, 3))
    needed = TP5_MOVE * EDGE_MIN_TP5_FEASIBILITY
    return recent_abs_15m >= needed, f"TP5 feasibility recent15m {recent_abs_15m*100:.2f}% / need {needed*100:.2f}%"


def upper_wick_ratio(c: Dict[str, float]) -> float:
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    rng = max(h - l, 1e-12)
    return (h - max(o, cl)) / rng


def lower_wick_ratio(c: Dict[str, float]) -> float:
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    rng = max(h - l, 1e-12)
    return (min(o, cl) - l) / rng


def trend_state(candles: List[Dict[str, float]]) -> str:
    cs = closes(candles)
    if len(cs) < 60:
        return "UNKNOWN"
    e21 = ema(cs, 21)
    e55 = ema(cs, 55)
    price = cs[-1]
    ch = percent_change(candles, min(20, len(candles) - 1))
    if price > e21 > e55 and ch > 0.003:
        return "UP"
    if price < e21 < e55 and ch < -0.003:
        return "DOWN"
    return "RANGE"


def btc_context() -> Dict[str, Any]:
    c15 = get_klines("BTC-USDT", "15m", 120, cache_seconds=45)
    c1h = get_klines("BTC-USDT", "1h", 120, cache_seconds=120)
    if not c15 or not c1h:
        return {"ok": False, "direction": "UNKNOWN", "text": "BTC data unavailable", "ch1h": 0.0}
    ch1h = percent_change(c15, 4)
    ch6h = percent_change(c15, 24)
    t1h = trend_state(c1h)
    direction = "RANGE"
    if ch1h < -0.004 or ch6h < -0.018 or t1h == "DOWN":
        direction = "BEAR"
    elif ch1h > 0.004 or ch6h > 0.018 or t1h == "UP":
        direction = "BULL"
    return {
        "ok": True,
        "direction": direction,
        "ch1h": ch1h,
        "ch6h": ch6h,
        "t1h": t1h,
        "text": f"BTC {direction}: 1h {ch1h*100:+.2f}%, 6h {ch6h*100:+.2f}%, 1H {t1h}",
    }

# ============================================================
# Hot symbol selection
# ============================================================

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
    """Live-first hot score.
    V13.19 intentionally avoids using 15m candles here to keep scans fast.
    Deep analysis still loads 15m/1h only for selected candidates.
    """
    c1 = get_klines(symbol, "1m", 60, cache_seconds=8)
    c5 = get_klines(symbol, "5m", 80, cache_seconds=18)
    if not c1 or not c5:
        return 0.0, "no candles"

    ch3m_signed = percent_change(c1, 3)
    ch3m = abs(ch3m_signed)
    ch15m_signed = percent_change(c5, 3)
    ch30m_signed = percent_change(c5, 6)
    ch15m = abs(ch15m_signed)
    ch30m = abs(ch30m_signed)
    vr1 = volume_ratio(c1, 20)
    vr5 = volume_ratio(c5, 20)
    rr1 = candle_range_ratio(c1, 20)
    rr5 = candle_range_ratio(c5, 20)

    # Real-time pressure matters more than old 30m movement.
    live_score = ch3m * 14000 + min(rr1, 5.0) * 14 + min(vr1, 5.0) * 7
    recent_score = ch15m * 700 + ch30m * 320 + min(rr5, 5.0) * 7 + min(vr5, 5.0) * 4

    # Reversal bonus: coin was stretched one way, but 1m flow is now counter-moving.
    reversal_bonus = 0.0
    if REVERSAL_ENABLED:
        if ch30m_signed > REVERSAL_MIN_30M_MOVE and ch3m_signed < -REVERSAL_MIN_LIVE_COUNTER_MOVE:
            reversal_bonus = 35 + abs(ch3m_signed) * 7000
        elif ch30m_signed < -REVERSAL_MIN_30M_MOVE and ch3m_signed > REVERSAL_MIN_LIVE_COUNTER_MOVE:
            reversal_bonus = 35 + abs(ch3m_signed) * 7000

    score = live_score + recent_score + reversal_bonus

    # Penalize coins that moved earlier but are dead right now.
    dead_now = ch3m < HOT_MIN_LIVE_MOVE_3M and rr1 < 0.35 and vr1 < 0.45
    stale = ch30m >= 0.012 and ch3m < HOT_MIN_LIVE_MOVE_3M and rr1 < HOT_MIN_LIVE_RANGE_OR_VOLUME and vr1 < HOT_MIN_LIVE_RANGE_OR_VOLUME
    if HOT_STALE_PENALTY_ENABLED and stale and reversal_bonus <= 0:
        score *= 0.25
    if dead_now and reversal_bonus <= 0:
        score *= 0.12

    # Huge volume without range/movement is absorption, not immediate scalp flow.
    if vr1 > 20 and ch3m < 0.0005 and rr1 < 0.5:
        score *= 0.20

    if base_asset(symbol) in QUALITY_BASES:
        score += 2

    live_tag = "LIVE" if not dead_now and (ch3m >= HOT_MIN_LIVE_MOVE_3M or rr1 >= 0.8 or vr1 >= 0.8 or reversal_bonus > 0) else "STALE"
    mode_tag = "REV" if reversal_bonus > 0 else "MOM"
    note = (
        f"{live_tag}/{mode_tag}: 1m3 {ch3m_signed*100:+.2f}%, "
        f"15m {ch15m_signed*100:+.2f}%, 30m {ch30m_signed*100:+.2f}%, "
        f"vol1 x{vr1:.2f}, vol5 x{vr5:.2f}, range1 x{rr1:.2f}, range5 x{rr5:.2f}"
    )
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
            STATE["last_error"] = f"hot_score {sym}: {repr(e)}"
    scored.sort(reverse=True, key=lambda x: x[0])

    for sc, sym, note in scored[:12]:
        notes.append(f"{display_symbol(sym)} hot {sc:.1f}: {note}")

    selected = [sym for sc, sym, _ in scored if sc >= HOT_MIN_SCORE][:HOT_SYMBOLS_TO_ANALYZE]

    # Keep the bot alive: if the market is quiet and strict hot score returns too few,
    # still analyze the best live-ranked names. The deeper fast filters remain in place.
    min_live_candidates = min(HOT_SYMBOLS_TO_ANALYZE, 50)
    if len(selected) < min_live_candidates:
        seen = set(selected)
        for sc, sym, _ in scored:
            if sym not in seen:
                selected.append(sym)
                seen.add(sym)
            if len(selected) >= min_live_candidates:
                break

    return selected[:MAX_ANALYZE_SYMBOLS], notes

# ============================================================
# Setup logic
# ============================================================

def realtime_pressure_ok(c1: List[Dict[str, float]], side: str) -> Tuple[bool, str, Dict[str, float]]:
    """Live 1m pressure gate.
    This is the key V13.18 fix: a signal is allowed only if the coin is moving right now.
    Expired signals usually came from patterns where the flow had already stopped.
    """
    if len(c1) < 30:
        return False, "not enough 1m pressure data", {}

    last = c1[-1]
    prev = c1[-2]
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


def fast_context_ok(c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], side: str, vol: float) -> Tuple[bool, str, Dict[str, float]]:
    """V13.19 fast context.
    Allows two professional scalp types:
    1) continuation: 15m/30m and 1m pressure agree;
    2) blow-off reversal: 30m is stretched one way, but live 1m pressure flips hard the other way.
    This fixes the prior issue where TIMI-like +16% 30m then -1% 1m dump was rejected as no_fast_short.
    """
    if len(c1) < 20 or len(c5) < 36 or len(c15) < 24:
        return False, "not enough candles", {}

    ch15m = percent_change(c5, 3)
    ch30m = percent_change(c5, 6)
    ch3m_1m = percent_change(c1, 3)
    rr = candle_range_ratio(c5, 20)
    compression = prior_compression_ratio(c5, 6)
    last = c5[-1]
    candle_move = (last["high"] - last["low"]) / max(last["open"], 1e-12)

    metrics = {
        "ch15m": ch15m,
        "ch30m": ch30m,
        "ch3m_1m": ch3m_1m,
        "range_ratio": rr,
        "compression": compression,
        "candle_move": candle_move,
        "vol": vol,
        "setup_mode": "unknown",
    }

    if candle_move > FAST_MAX_SPREAD_PROXY:
        return False, f"last 5m candle too wide/chase risk {candle_move*100:.2f}%", metrics

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

    # continuation vs blow-off reversal classification
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
            # During market-wide dumps many examples realize quickly to the downside.
            # Still avoid blind chasing: require a small bounce/reject structure before continuing.
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

    # For fast scalps, live velocity/range can bypass weak 15m volume.
    live_bypass = abs(ch3m_1m) >= LIVE_BYPASS_VOLUME_MOVE or metrics.get("range1", 1.0) >= LIVE_BYPASS_RANGE_RATIO

    if rr < FAST_MIN_RANGE_RATIO and not live_bypass:
        return False, f"range expansion weak x{rr:.2f}", metrics
    if vol < FAST_MIN_VOLUME_RATIO and not live_bypass:
        return False, f"volume weak x{vol:.2f}", metrics

    return True, (
        f"{metrics['setup_mode']} edge ok: 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, "
        f"1m3 {ch3m_1m*100:+.2f}%, range5 x{rr:.2f}, vol15 x{vol:.2f}, "
        f"range1 x{metrics.get('range1', 1.0):.2f}, vol1 x{metrics.get('vol1', 1.0):.2f}; "
        f"{micro_reason}; {pressure_reason}; {sweep_reason}; {feasible_reason}"
    ), metrics


def long_live_stats_ok() -> Tuple[bool, str]:
    """Protect the bot from repeatedly taking bad LONGs while still allowing recovery later.
    If live LONG stats are poor, allow only very high-quality LONGs by blocking B-class setups upstream.
    """
    if not LONG_STATS_PROTECTION:
        return True, "long stats protection disabled"
    stats = STATE.setdefault("stats", default_state()["stats"])
    item = stats.get("side", {}).get("LONG", {})
    closed = int(item.get("profit", 0)) + int(item.get("sl", 0)) + int(item.get("expired", 0))
    if closed < LONG_STATS_MIN_CLOSED:
        return True, "not enough LONG stats"
    wr = int(item.get("profit", 0)) / max(closed, 1) * 100.0
    if wr < LONG_STATS_MIN_WR:
        return False, f"LONG stats weak: WR {wr:.1f}% after {closed}"
    return True, "LONG stats ok"


def professional_long_reclaim_gate(
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
    c15: List[Dict[str, float]],
    btc: Dict[str, Any],
    metrics: Dict[str, float],
    setup_mode: str,
    e1: float,
    e5: float,
    vw5: float,
) -> Tuple[bool, str]:
    """Strict LONG-only repair.

    Live results showed LONG was buying weak bounces / late pumps.
    A valid LONG now needs a real reclaim pattern:
    - BTC must not be BEAR by default;
    - strong 1m pressure, close near high, volume/range alive;
    - price must reclaim 1m EMA and be near/above 5m EMA/VWAP;
    - no buying vertical 15m/30m extension unless there was a controlled pullback;
    - prefer liquidity sweep / higher-low reclaim.
    """
    if len(c1) < 24 or len(c5) < 24 or len(c15) < 10:
        return False, "LONG gate: not enough candles"

    btc_dir = str(btc.get("direction", "UNKNOWN"))
    btc_ch1h = float(btc.get("ch1h", 0.0))
    btc_ch6h = float(btc.get("ch6h", 0.0))

    last1 = c1[-1]
    prev1 = c1[-2]
    price = last1["close"]
    ch3m = percent_change(c1, 3)
    ch15m = metrics.get("ch15m", percent_change(c5, 3))
    ch30m = metrics.get("ch30m", percent_change(c5, 6))
    vol1 = metrics.get("vol1", volume_ratio(c1, 20))
    range1 = metrics.get("range1", candle_range_ratio(c1, 20))
    loc1 = close_location(last1)

    # BTC bearish does not automatically forbid LONG. But a LONG against a bearish BTC
    # must be a leader/relative-strength coin, not a weak bounce. This is how coins like
    # VELVET can still be traded LONG while the general market is heavy.
    bear_rs_long = False
    if btc_dir == "BEAR":
        rel_strength_1h = ch15m - btc_ch1h
        bear_rs_long = (
            LONG_ALLOW_BEAR_RELATIVE_STRENGTH
            and ch15m >= LONG_BEAR_MIN_ALT_15M
            and ch30m >= LONG_BEAR_MIN_ALT_30M
            and ch3m >= LONG_BEAR_MIN_1M3
            and rel_strength_1h >= LONG_BEAR_MIN_REL_STRENGTH_1H
            and vol1 >= LONG_BEAR_MIN_VOL1
            and range1 >= LONG_BEAR_MIN_RANGE1
            and loc1 >= LONG_BEAR_MIN_CLOSE_LOCATION
        )
        if LONG_BLOCK_BTC_BEAR and not bear_rs_long:
            return False, (
                f"LONG gate: BTC BEAR and coin has no relative strength: "
                f"alt15m {ch15m*100:+.2f}%, alt30m {ch30m*100:+.2f}%, "
                f"1m3 {ch3m*100:+.2f}%, rel1h {rel_strength_1h*100:+.2f}%"
            )

    if ch3m < LONG_MIN_3M_CONFIRM:
        return False, f"LONG gate: weak 3m confirm {ch3m*100:.2f}%"
    if btc_dir == "BEAR" and LONG_ALLOW_BEAR_RELATIVE_STRENGTH and not bear_rs_long:
        return False, (
            f"LONG gate: BTC BEAR, only relative-strength longs allowed; "
            f"alt15m {ch15m*100:+.2f}%, alt30m {ch30m*100:+.2f}%, 1m3 {ch3m*100:+.2f}%"
        )
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

    # Must reclaim micro trend. For continuation LONG, also avoid being below 5m EMA/VWAP.
    if price < e1 * (1 + RECLAIM_BUFFER):
        return False, "LONG gate: no 1m EMA reclaim"
    if setup_mode == "CONTINUATION_LONG" and (price < e5 * (1 + RECLAIM_BUFFER) or price < vw5 * (1 + RECLAIM_BUFFER)):
        return False, "LONG gate: no 5m EMA/VWAP reclaim"

    # Liquidity sweep / higher-low reclaim. This avoids buying a random bounce with no trap.
    recent = c1[-16:-4]
    last_zone = c1[-5:]
    swept_low = min(x["low"] for x in last_zone[:-1]) <= min(x["low"] for x in recent) * 1.0015 if recent else False
    reclaimed = last1["close"] > max(x["close"] for x in c1[-5:-1]) and loc1 >= LONG_MIN_CLOSE_LOCATION
    higher_low = min(x["low"] for x in c1[-4:]) > min(x["low"] for x in c1[-10:-4]) * 0.998 if len(c1) >= 12 else False

    if LONG_REQUIRE_SWEEP_OR_RECLAIM and not (swept_low or reclaimed):
        return False, "LONG gate: no sweep/reclaim trigger"
    if LONG_REQUIRE_HIGHER_LOW and not (higher_low or swept_low):
        return False, "LONG gate: no higher-low/sweep structure"

    # Anti-chase: after a big pump, only buy if there was a real controlled pullback first.
    recent_high = max(x["high"] for x in c5[-18:])
    recent_low = min(x["low"] for x in c5[-10:])
    pullback = (recent_high - recent_low) / max(recent_high, 1e-12)
    if ch15m > LONG_MAX_15M_CHASE or ch30m > LONG_MAX_30M_CHASE:
        if not (LONG_MIN_PULLBACK_AFTER_PUMP <= pullback <= LONG_MAX_PULLBACK_AFTER_PUMP and (swept_low or reclaimed)):
            return False, f"LONG gate: late pump chase blocked 15m {ch15m*100:.2f}%, 30m {ch30m*100:.2f}%, pullback {pullback*100:.2f}%"

    # Avoid buying into a distribution wick.
    last5 = c5[-1]
    if upper_wick_ratio(last5) > 0.48 and close_location(last5) < 0.68:
        return False, "LONG gate: 5m upper wick/distribution"

    return True, (
        f"LONG professional gate ok: BTC {btc_dir}, 3m {ch3m*100:+.2f}%, "
        f"vol1 x{vol1:.2f}, range1 x{range1:.2f}, closeLoc {loc1:.2f}, "
        f"bearRS {bear_rs_long}, sweep {swept_low}, reclaim {reclaimed}, higherLow {higher_low}"
    )

def fast_burst_setup(symbol: str, c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], c1h: List[Dict[str, float]], btc: Dict[str, Any], side: str) -> Optional[Dict[str, Any]]:
    """Scalping Edge setup: no trend prediction.
    We only require a tradable micro-event: fresh imbalance + micro sweep/reclaim + immediate continuation.
    BTC/1H are informational, not directional gates, except violent BTC shock.
    """
    if not FAST_BURST_ENABLED:
        return None
    if len(c1) < 30 or len(c5) < 48 or len(c15) < 40 or len(c1h) < 60:
        return None

    price = c1[-1]["close"]
    e5 = ema(closes(c5), 21)
    e1 = ema(closes(c1), 9)
    vw5 = vwap(c5, 36)
    vol = volume_ratio(c15, 24)
    t1h = trend_state(c1h)

    # Market phase is not traded as a prediction. BTC is a context filter:
    # - during BTC shock down, avoid LONG unless the coin later passes relative-strength LONG gate;
    # - allow SHORT during dump because that is exactly when many alts realize quickly.
    btc_ch1h = float(btc.get("ch1h", 0.0))
    if abs(btc_ch1h) >= BTC_SHOCK_15M_BLOCK and side == "LONG":
        # Do not hard-block here; professional_long_reclaim_gate can still allow an exceptional RS long.
        pass

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

    last5 = c5[-1]
    prev5 = c5[-2]

    if side == "LONG":
        recent_high = max(x["high"] for x in c5[-18:])
        pullback_low = min(x["low"] for x in c5[-10:])
        pullback = (recent_high - pullback_low) / max(recent_high, 1e-12)
        if pullback < PULLBACK_MIN or pullback > PULLBACK_MAX:
            return None
        if is_reversal:
            # Blow-off reversal LONG: do not wait for 5m EMA/VWAP reclaim; that is often too late.
            # Require live 1m reclaim only; fast_context already confirmed pressure and micro break.
            if price < e1:
                return None
        else:
            if price < e1 or price < e5 * (1 + RECLAIM_BUFFER) or price < vw5 * (1 + RECLAIM_BUFFER):
                return None
            # Entry must be continuation, not a mid-range hesitation.
            if last5["close"] <= prev5["high"] * 0.999 and last5["close"] <= prev5["close"]:
                return None
            if upper_wick_ratio(last5) > 0.42 and close_location(last5) < 0.72:
                return None
        level = min(pullback_low, min(x["low"] for x in c1[-12:]))
        strategy = "PRO_SCALPING_EDGE_LONG"
        trade_type = "SCALPING EDGE LONG"
        reason = (
            f"SCALPING EDGE LONG: не прогноз рынка, а короткая ситуация. "
            f"Режим {setup_mode}: свежий дисбаланс вверх, микро-откат/перехват {pullback*100:.2f}%, "
            f"live 1m pressure, sweep/reclaim и немедленное продолжение. {fast_reason}. "
            f"{metrics.get('long_gate_reason', '')}."
        )
    else:
        recent_low = min(x["low"] for x in c5[-18:])
        bounce_high = max(x["high"] for x in c5[-10:])
        pullback = (bounce_high - recent_low) / max(recent_low, 1e-12)
        if pullback < PULLBACK_MIN or pullback > PULLBACK_MAX:
            return None
        if is_reversal:
            # Blow-off reversal SHORT: do not wait for 5m EMA/VWAP loss; that is often too late.
            # Require live 1m reject only; fast_context already confirmed pressure and micro break.
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
        reason = (
            f"SCALPING EDGE SHORT: не прогноз рынка, а короткая ситуация. "
            f"Режим {setup_mode}: свежий дисбаланс вниз, микро-отскок/перехват {pullback*100:.2f}%, "
            f"live 1m pressure и немедленное продолжение. {fast_reason}."
        )

    strong = vol >= 1.55 and metrics.get("range_ratio", 1.0) >= 1.55 and abs(metrics.get("ch3m_1m", 0)) >= FAST_MIN_1M_CONFIRM * 1.4
    score = 74
    score += min(12, int(abs(metrics.get("ch15m", 0)) * 650))
    score += min(10, int(abs(metrics.get("ch30m", 0)) * 430))
    score += min(8, int((vol - 1.0) * 7))
    score += min(8, int((metrics.get("range_ratio", 1.0) - 1.0) * 7))
    score += min(8, int((metrics.get("vol1", 1.0) - 1.0) * 7))
    score += min(8, int((metrics.get("range1", 1.0) - 1.0) * 7))
    # Market phase does not add or subtract. Only actual speed/liquidity edge matters.
    if strong:
        score += 7
    if base_asset(symbol) in QUALITY_BASES:
        score += 1
    score = max(0, min(100, score))

    return {
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "trade_type": trade_type,
        "score": score,
        "grade": "A+" if score >= A_PLUS_MIN_SCORE and vol >= 1.45 else "B",
        "entry": price,
        "level": level,
        "reason": reason,
        "pullback": pullback,
        "volume_ratio": vol,
        "range_ratio": metrics.get("range_ratio", 1.0),
        "compression": metrics.get("compression", 1.0),
        "ch15m": metrics.get("ch15m", 0.0),
        "ch30m": metrics.get("ch30m", 0.0),
        "ch3m_1m": metrics.get("ch3m_1m", 0.0),
        "vol1": metrics.get("vol1", 1.0),
        "range1": metrics.get("range1", 1.0),
        "ch2m": metrics.get("ch2m", 0.0),
        "setup_mode": setup_mode,
        "t1h": t1h,
        "btc_text": btc.get("text", ""),
    }



def instant_edge_setup(symbol: str, c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], c1h: List[Dict[str, float]], btc: Dict[str, Any], side: str) -> Optional[Dict[str, Any]]:
    """V13.24 fallback: instant momentum/reclaim scalp.

    This is for situations visible in diagnostics such as SYRUP/FOLKS:
    live 1m impulse is present, but the older fast_burst setup rejects the trade because it
    waits for a perfect 5m pullback/reclaim. We still keep strict quality filters after this.
    """
    if not INSTANT_EDGE_ENABLED:
        return None
    if len(c1) < 35 or len(c5) < 36 or len(c15) < 12 or len(c1h) < 40:
        return None

    price = c1[-1]["close"]
    last1 = c1[-1]
    prev1 = c1[-2]
    ch3m = percent_change(c1, 3)
    ch15m = percent_change(c5, 3)
    ch30m = percent_change(c5, 6)
    vol1 = volume_ratio(c1, 20)
    range1 = candle_range_ratio(c1, 20)
    vol5 = volume_ratio(c5, 20)
    range5 = candle_range_ratio(c5, 20)
    loc = close_location(last1)
    body = abs(last1["close"] - last1["open"]) / max(last1["high"] - last1["low"], 1e-12)
    t1h = trend_state(c1h)

    # Live impulse must be real, not a dead hot-list artifact.
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
        # Avoid buying after multiple vertical green candles without any micro reset.
        had_reset = any(x["close"] < x["open"] for x in c1[-7:-1]) or min(x["low"] for x in c1[-5:]) <= min(x["low"] for x in c1[-14:-5]) * 1.002
        if not had_reset and ch30m > 0.025:
            return None
        level = min(x["low"] for x in c1[-10:])
        strategy = "PRO_INSTANT_EDGE_LONG"
        trade_type = "INSTANT EDGE LONG"
        setup_mode = "INSTANT_MOMENTUM_LONG"
        direction_text = "вверх"
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
        strategy = "PRO_INSTANT_EDGE_SHORT"
        trade_type = "INSTANT EDGE SHORT"
        setup_mode = "INSTANT_MOMENTUM_SHORT"
        direction_text = "вниз"

    if body < INSTANT_MIN_BODY:
        return None
    if range1 < INSTANT_MIN_RANGE1:
        return None
    if vol1 < INSTANT_MIN_VOL1 and not (abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.35 and range1 >= 1.15):
        return None
    if range5 < INSTANT_MIN_RANGE5:
        return None
    if vol5 < INSTANT_MIN_VOL5 and not (abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.60):
        return None

    # Keep a micro structure break; this prevents entering the middle of a random candle.
    micro_ok, micro_reason = micro_structure_break(c1, side)
    if not micro_ok:
        return None

    # BTC is context, not a hard phase filter. Against BTC pressure, demand stronger live impulse.
    btc_dir = str(btc.get("direction", "UNKNOWN"))
    if side == "LONG" and btc_dir == "BEAR" and not (ch3m >= INSTANT_MIN_1M3_MOVE * 1.35 and ch15m >= INSTANT_MIN_15M_MOVE * 1.2):
        return None
    if side == "SHORT" and btc_dir == "BULL" and not (abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.35 and ch15m <= -INSTANT_MIN_15M_MOVE * 1.2):
        return None

    score = 78
    score += min(10, int(abs(ch3m) * 1000))
    score += min(8, int(abs(ch15m) * 700))
    score += min(6, int(max(0.0, vol1 - 0.8) * 6))
    score += min(6, int(max(0.0, range1 - 1.0) * 6))
    score += min(5, int(max(0.0, range5 - 1.0) * 4))
    score = max(0, min(100, score))

    reason = (
        f"INSTANT EDGE {side}: профессиональный fallback для живого импульса. "
        f"Цена движется {direction_text} сейчас: 1m3 {ch3m*100:+.2f}%, 15m {ch15m*100:+.2f}%, "
        f"30m {ch30m*100:+.2f}%, Vol1 x{vol1:.2f}, Range1 x{range1:.2f}, "
        f"Vol5 x{vol5:.2f}, Range5 x{range5:.2f}, closeLoc {loc:.2f}. "
        f"{micro_reason}. Сделка всё равно проходит RR/SL/live-volume quality gate."
    )

    return {
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "trade_type": trade_type,
        "score": score,
        "grade": "A+" if score >= A_PLUS_MIN_SCORE and vol1 >= 1.20 else "B",
        "entry": price,
        "level": level,
        "reason": reason,
        "pullback": 0.0,
        "volume_ratio": vol5,
        "range_ratio": range5,
        "compression": 1.0,
        "ch15m": ch15m,
        "ch30m": ch30m,
        "ch3m_1m": ch3m,
        "vol1": vol1,
        "range1": range1,
        "ch2m": (c1[-1]["close"] - c1[-3]["close"]) / max(c1[-3]["close"], 1e-12),
        "setup_mode": setup_mode,
        "t1h": t1h,
        "btc_text": btc.get("text", ""),
    }

def calculate_fast_trade(setup: Dict[str, Any], c1: List[Dict[str, float]], c5: List[Dict[str, float]]) -> Optional[Dict[str, Any]]:
    side = setup["side"]
    entry = setup["entry"]
    level = setup["level"]
    a = atr(c5, 14)
    instant = str(setup.get("setup_mode", "")).startswith("INSTANT")
    buffer = max(entry * (0.0016 if instant else 0.0022), a * (0.55 if instant else SL_ATR_MULT))

    if side == "LONG":
        recent_source = c1[-10:] + (c5[-2:] if instant else c5[-4:])
        recent_low = min(x["low"] for x in recent_source)
        sl = min(level, recent_low) - buffer
        sl = min(sl, entry * (1 - MIN_SL_MOVE))
        tp1 = entry * (1 + TP1_MOVE)
        tp2 = entry * (1 + TP2_MOVE)
        tp3 = entry * (1 + TP3_MOVE)
        tp4 = entry * (1 + TP4_MOVE)
        tp5 = entry * (1 + TP5_MOVE)
    else:
        recent_source = c1[-10:] + (c5[-2:] if instant else c5[-4:])
        recent_high = max(x["high"] for x in recent_source)
        sl = max(level, recent_high) + buffer
        sl = max(sl, entry * (1 + MIN_SL_MOVE))
        tp1 = entry * (1 - TP1_MOVE)
        tp2 = entry * (1 - TP2_MOVE)
        tp3 = entry * (1 - TP3_MOVE)
        tp4 = entry * (1 - TP4_MOVE)
        tp5 = entry * (1 - TP5_MOVE)

    risk = abs(entry - sl)
    risk_move = risk / max(entry, 1e-12)
    if risk_move > MAX_SL_MOVE:
        return None

    rewards = [abs(tp1 - entry), abs(tp2 - entry), abs(tp3 - entry), abs(tp4 - entry), abs(tp5 - entry)]
    rr = rewards[0] / risk if risk > 0 else 0.0
    ladder_rr = (sum(rewards) / len(rewards)) / risk if risk > 0 else 0.0
    final_rr = rewards[-1] / risk if risk > 0 else 0.0
    roi_tp1 = rewards[0] / entry * LEVERAGE * 100
    roi_sl = risk / entry * LEVERAGE * 100

    return {
        **setup,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "tp4": tp4,
        "tp5": tp5,
        "rr": rr,
        "ladder_rr": ladder_rr,
        "final_rr": final_rr,
        "roi_tp1": roi_tp1,
        "roi_sl": roi_sl,
        "risk_mult": A_RISK_MULT if setup["grade"] == "A+" else FAST_RISK_MULT,
        "created_at": now_ts(),
        "status": "active",
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "tp4_hit": False,
        "tp5_hit": False,
    }



def professional_quality_gate(trade: Dict[str, Any], symbol: str) -> Tuple[bool, str, str]:
    """Final professional quality filter.

    This is intentionally hard. A fast scalp is not allowed when:
    - stop risk is much larger than the reward ladder;
    - TP5 does not at least compensate risk;
    - live 1m volume is weak without a strong price/range exception;
    - heavy/slow coins have wide SL and weak RR.
    """
    side = trade.get("side", "?")
    base = base_asset(symbol)
    rr = float(trade.get("rr", 0.0) or 0.0)
    ladder_rr = float(trade.get("ladder_rr", 0.0) or 0.0)
    final_rr = float(trade.get("final_rr", 0.0) or 0.0)
    roi_sl = float(trade.get("roi_sl", 999.0) or 999.0)
    vol1 = float(trade.get("vol1", 1.0) or 1.0)
    range1 = float(trade.get("range1", 1.0) or 1.0)
    ch3m = abs(float(trade.get("ch3m_1m", 0.0) or 0.0))

    if roi_sl > MAX_SCALP_SL_ROI:
        return False, "sl_roi_too_high_block", f"{display_symbol(symbol)} {side}: SL risk too high {roi_sl:.1f}% ROI"

    if rr < MIN_TP1_RR:
        return False, "tp1_rr_hard_block", f"{display_symbol(symbol)} {side}: TP1 RR too weak {rr:.2f}"

    if ladder_rr < MIN_LADDER_RR_HARD:
        return False, "ladder_rr_hard_block", f"{display_symbol(symbol)} {side}: ladder RR too weak {ladder_rr:.2f}"

    if final_rr < MIN_FINAL_RR_HARD:
        return False, "final_rr_hard_block", f"{display_symbol(symbol)} {side}: final RR too weak {final_rr:.2f}"

    if vol1 < MIN_LIVE_VOL_NORMAL:
        strong_price_exception = (
            vol1 >= MIN_LIVE_VOL_STRONG_PRICE
            and ch3m >= STRONG_1M3_MOVE
            and range1 >= STRONG_RANGE1
        )
        if not strong_price_exception:
            return (
                False,
                "weak_live_volume_block",
                f"{display_symbol(symbol)} {side}: weak live volume x{vol1:.2f}, 1m3 {ch3m*100:.2f}%, range1 x{range1:.2f}"
            )

    if base in HEAVY_BASES:
        if roi_sl > HEAVY_MAX_SL_ROI:
            return False, "heavy_coin_sl_block", f"{display_symbol(symbol)} {side}: heavy coin SL too wide {roi_sl:.1f}% ROI"
        if final_rr < HEAVY_MIN_FINAL_RR:
            return False, "heavy_coin_rr_block", f"{display_symbol(symbol)} {side}: heavy coin final RR too weak {final_rr:.2f}"
        if vol1 < HEAVY_MIN_LIVE_VOL:
            return False, "heavy_coin_volume_block", f"{display_symbol(symbol)} {side}: heavy coin live volume weak x{vol1:.2f}"

    return True, "ok", "quality ok"


def trader_pattern_gate(trade: Dict[str, Any], symbol: str, c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], btc: Dict[str, Any]) -> Tuple[bool, str, str]:
    """Example-style final gate.

    The goal is to block signals that are technically valid but not trader-quality:
    - weak B-class entries with no live volume;
    - tiny or stale continuation;
    - counter-direction entries without true reversal strength;
    - target ladders that require more movement than the recent market has shown;
    - heavy coins unless the setup is genuinely A+.
    """
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
    vol5 = float(trade.get("volume_ratio", trade.get("vol5", 1.0)) or 1.0)
    range1 = float(trade.get("range1", 1.0) or 1.0)
    range5 = float(trade.get("range_ratio", trade.get("range5", 1.0)) or 1.0)
    entry = float(trade.get("entry", 0.0) or 0.0)
    tp5 = float(trade.get("tp5", 0.0) or 0.0)

    if grade != "A+" and not TRADER_ALLOW_B_SCORE:
        return False, "trader_grade_block", f"{display_symbol(symbol)} {side}: B-class skipped by env; set TRADER_ALLOW_B_SCORE=true to allow B+"

    if score < TRADER_MIN_SCORE:
        return False, "trader_score_block", f"{display_symbol(symbol)} {side}: trader score too low {score} < {TRADER_MIN_SCORE}"

    # Balanced B+ mode: B setups are allowed, but only if the current tape is alive.
    # This keeps the bot from going silent while still blocking random weak B entries.
    if grade != "A+":
        if abs(ch3m) < TRADER_MIN_ABS_1M3 * 1.20 and vol1 < TRADER_MIN_VOL1 * 1.20 and range1 < TRADER_MIN_RANGE1 * 1.15:
            return False, "trader_bplus_quality_block", (
                f"{display_symbol(symbol)} {side}: B+ not strong enough; 1m3 {ch3m*100:+.2f}%, "
                f"vol1 x{vol1:.2f}, range1 x{range1:.2f}"
            )

    if base in HEAVY_BASES and TRADER_HEAVY_ONLY_A_PLUS and grade != "A+":
        return False, "trader_heavy_grade_block", f"{display_symbol(symbol)} {side}: heavy coin requires A+"

    # Directional pressure must exist now. Examples are not slow predictions.
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
        if TRADER_REQUIRE_MICRO_BREAK and c1[-1]["close"] >= min(x["low"] for x in c1[-6:-1]):
            return False, "trader_micro_break_block", f"{display_symbol(symbol)} SHORT: no fresh 1m low break"
        aligned = ch15m <= -TRADER_MIN_ABS_15M and ch30m <= TRADER_MAX_COUNTER_30M
        reversal_exception = setup_mode.startswith("REVERSAL") and abs(ch3m) >= TRADER_MIN_ABS_1M3 * 1.5 and range1 >= TRADER_MIN_RANGE1 * 1.25

    if TRADER_BLOCK_WEAK_CONTINUATION and not (aligned or reversal_exception):
        return False, "trader_structure_block", (
            f"{display_symbol(symbol)} {side}: weak structure; 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, mode {setup_mode}"
        )

    if vol1 < TRADER_MIN_VOL1:
        return False, "trader_vol1_block", f"{display_symbol(symbol)} {side}: live vol1 too weak x{vol1:.2f}"
    if vol5 < TRADER_MIN_VOL5:
        return False, "trader_vol5_block", f"{display_symbol(symbol)} {side}: vol5 too weak x{vol5:.2f}"
    if range1 < TRADER_MIN_RANGE1:
        return False, "trader_range1_block", f"{display_symbol(symbol)} {side}: range1 too weak x{range1:.2f}"
    if range5 < TRADER_MIN_RANGE5:
        return False, "trader_range5_block", f"{display_symbol(symbol)} {side}: range5 too weak x{range5:.2f}"

    # TP5 should be plausible from current market expansion, not a fantasy target.
    if entry > 0 and tp5 > 0:
        need = abs(entry - tp5) / entry
        recent_move = max(abs(ch15m), abs(ch30m), abs(percent_change(c5, 6)))
        if recent_move < need * TRADER_MIN_TP5_FEASIBILITY:
            return False, "trader_tp5_feasibility_block", (
                f"{display_symbol(symbol)} {side}: TP5 move {need*100:.2f}% not feasible vs recent {recent_move*100:.2f}%"
            )

    return True, "ok", (
        f"trader-pattern ok: score {score}, grade {grade}, 1m3 {ch3m*100:+.2f}%, "
        f"15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, vol1 x{vol1:.2f}, range1 x{range1:.2f}"
    )

def cooldown_ok(symbol: str, strategy: str) -> Tuple[bool, str]:
    t = now_ts()
    if t < STATE.setdefault("pair_cooldown", {}).get(symbol, 0):
        return False, "pair cooldown"
    if t < STATE.setdefault("strategy_cooldown", {}).get(strategy, 0):
        return False, "strategy cooldown"
    return True, "ok"


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
        if not setup:
            blocks[f"no_fast_{side.lower()}"] = blocks.get(f"no_fast_{side.lower()}", 0) + 1
            continue

        if side == "LONG":
            long_stats_ok, long_stats_reason = long_live_stats_ok()
            if not long_stats_ok and setup.get("grade") != "A+":
                blocks["long_stats_protection_block"] = blocks.get("long_stats_protection_block", 0) + 1
                if len(near_miss) < 8:
                    near_miss.append(f"{display_symbol(symbol)} LONG: {long_stats_reason}; B-class long skipped")
                continue

        co, reason = cooldown_ok(symbol, setup["strategy"])
        if not co:
            blocks["cooldown_block"] = blocks.get("cooldown_block", 0) + 1
            continue

        trade = calculate_fast_trade(setup, c1, c5)
        if not trade:
            blocks["sl_too_far_block"] = blocks.get("sl_too_far_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: SL too far")
            continue

        if trade["score"] < B_MIN_SCORE:
            blocks["score_block"] = blocks.get("score_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: score {trade['score']}, vol x{trade['volume_ratio']:.2f}, range x{trade['range_ratio']:.2f}")
            continue

        # V13.23 balanced professional quality gate.
        # Still blocks XMR-style bad scalps: huge SL, weak RR, weak live volume.
        # But thresholds are not over-tight, so the bot can remain alive during the day.
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

# ============================================================
# Formatting / scanning / tracking
# ============================================================

def format_price(x: Optional[float]) -> str:
    if x is None:
        return "-"
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.5f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def build_signal_message(s: Dict[str, Any]) -> str:
    arrow = "🟢" if s["side"] == "LONG" else "🔴"
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
        f"SL: {format_price(s['sl'])} · риск до SL ≈ {s['roi_sl']:.1f}% ROI x{LEVERAGE}\n"
        f"RR TP1: {s['rr']:.2f} · Ladder RR: {s['ladder_rr']:.2f} · Final RR: {s['final_rr']:.2f}\n"
        f"Риск: multiplier x{s['risk_mult']:.2f}\n\n"
        f"📌 Логика:\n{s['reason']}\n"
        f"15m: {s['ch15m']*100:+.2f}% · 30m: {s['ch30m']*100:+.2f}% · 1m3: {s['ch3m_1m']*100:+.2f}%\n"
        f"Volume15 x{s['volume_ratio']:.2f} · Range5 x{s['range_ratio']:.2f} · Vol1 x{s.get('vol1', 1.0):.2f} · Range1 x{s.get('range1', 1.0):.2f}\n"
        f"BTC: {s['btc_text']}\n\n"
        f"⏱ Scalping rule: если за {FAST_MAX_MINUTES_TO_TP1} минут нет движения к TP1 — сигнал expired. Фаза рынка не важна; важна быстрая реализация."
    )


def build_diagnostic(scan: Dict[str, Any]) -> str:
    blocks = scan.get("blocks", {})
    block_lines = [f"{k}: {v}" for k, v in sorted(blocks.items(), key=lambda kv: -kv[1])[:12]]
    hot = scan.get("hot_notes", [])[:8]
    near = scan.get("near_miss", [])[:8]
    return (
        f"🧪 Диагностика V13.24 Instant Edge Quality Scalper\n"
        f"Проверено: {scan.get('checked', 0)} из universe {scan.get('universe', 0)}\n"
        f"Кандидатов: {scan.get('candidates', 0)} · отправлено: {scan.get('sent', 0)} · время: {scan.get('elapsed', 0):.0f}с\n"
        f"BTC: {scan.get('btc', 'unknown')}\n"
        f"Статистика: {wr_text(STATE.get('stats', {}).get('total', {}))}\n\n"
        f"Hot symbols:\n" + ("\n".join(hot) if hot else "нет") +
        f"\n\nГлавные блокировки:\n" + ("\n".join(block_lines) if block_lines else "нет") +
        ("\n\nПочти прошли:\n" + "\n".join(near) if near else "") +
        f"\n\nLast error: {STATE.get('last_error', '')}"
    )


def add_active_signal(s: Dict[str, Any]) -> None:
    STATE.setdefault("active_signals", []).append(s)
    STATE.setdefault("pair_cooldown", {})[s["symbol"]] = now_ts() + PAIR_COOLDOWN_SECONDS
    STATE.setdefault("strategy_cooldown", {})[s["strategy"]] = now_ts() + STRATEGY_COOLDOWN_SECONDS
    save_state()


def run_scan(manual: bool = False) -> Dict[str, Any]:
    start = time.time()
    blocks: Dict[str, int] = {}
    near_miss: List[str] = []
    btc = btc_context()
    symbols = get_symbols()
    selected, hot_notes = select_hot_symbols(symbols)

    scan = {
        "checked": 0,
        "universe": len(symbols),
        "candidates": 0,
        "sent": 0,
        "blocks": blocks,
        "near_miss": near_miss,
        "hot_notes": hot_notes,
        "btc": btc.get("text", "BTC unknown"),
        "elapsed": 0,
    }

    if not btc.get("ok"):
        blocks["btc_data_problem"] = 1
        STATE["last_scan"] = scan
        save_state()
        return scan

    # Before scanning, refresh old active signals so expired/TP/SL positions do not block the market scan.
    try:
        track_active_signals()
    except Exception as e:
        STATE["last_error"] = f"pre-scan track_active_signals: {repr(e)}"
        save_state()

    found: List[Dict[str, Any]] = []
    for sym in selected:
        try:
            s = analyze_symbol(sym, btc, blocks, near_miss)
            scan["checked"] += 1
            if s:
                found.append(s)
        except Exception as e:
            blocks["analyze_exception"] = blocks.get("analyze_exception", 0) + 1
            STATE["last_error"] = f"analyze {sym}: {repr(e)}"

    found.sort(key=lambda x: (x["grade"] == "A+", x["score"], x["ladder_rr"]), reverse=True)
    scan["candidates"] = len(found)

    sent = 0
    free_slots = max(0, MAX_ACTIVE_SIGNALS - len(STATE.get("active_signals", [])))
    send_limit = min(MAX_SIGNALS_PER_SCAN, free_slots)
    if send_limit <= 0 and found:
        blocks["active_slots_full_send_block"] = blocks.get("active_slots_full_send_block", 0) + 1
        if len(near_miss) < 8:
            near_miss.append(f"found {len(found)} candidate(s), but active slots are full")
    for s in found[:send_limit]:
        add_active_signal(s)
        send_telegram(build_signal_message(s))
        sent += 1

    scan["sent"] = sent
    scan["elapsed"] = time.time() - start
    STATE["last_scan"] = scan
    save_state()

    if manual or (sent == 0 and now_ts() - STATE.get("last_diag_ts", 0) >= DIAG_SECONDS):
        send_telegram(build_diagnostic(scan))
        STATE["last_diag_ts"] = now_ts()
        save_state()

    return scan


def current_price(symbol: str) -> Optional[float]:
    c = get_klines(symbol, "1m", 80, cache_seconds=4)
    return c[-1]["close"] if c else None


def target_hit(side: str, price: float, target: float) -> bool:
    return price >= target if side == "LONG" else price <= target


def sl_hit(side: str, price: float, sl: float) -> bool:
    return price <= sl if side == "LONG" else price >= sl


def directional_progress_ratio(s: Dict[str, Any], p: float) -> Tuple[bool, float]:
    entry = s["entry"]
    tp1 = s["tp1"]
    full = abs(tp1 - entry)
    if full <= 0:
        return False, 0.0
    if s["side"] == "LONG":
        directional = p > entry
        progress = max(0.0, p - entry) / full
    else:
        directional = p < entry
        progress = max(0.0, entry - p) / full
    return directional, progress


def track_active_signals() -> None:
    active = STATE.setdefault("active_signals", [])
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
                f"❌ Stop Loss\n"
                f"{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                f"Стратегия: {s['strategy']}\n"
                f"Вход: {format_price(s['entry'])}\n"
                f"SL: {format_price(s['sl'])}\n"
                f"Текущая цена: {format_price(p)}\n\n"
                f"{build_stats_text()}"
            )
            changed = True
            continue

        if FAST_CANCEL_IF_NO_PROGRESS and not s.get("tp1_hit") and age_minutes >= FAST_MAX_MINUTES_TO_TP1:
            directional, progress = directional_progress_ratio(s, p)
            if (not directional) or progress < FAST_MIN_PROGRESS_TO_KEEP:
                apply_result(s, "expired")
                send_telegram(
                    f"⏱ FAST TRADE EXPIRED\n"
                    f"{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                    f"Стратегия: {s['strategy']}\n"
                    f"Цена не реализовалась за {FAST_MAX_MINUTES_TO_TP1} минут.\n"
                    f"Вход: {format_price(s['entry'])}\n"
                    f"Текущая цена: {format_price(p)}\n"
                    f"TP1: {format_price(s['tp1'])}\n"
                    f"Прогресс к TP1: {progress*100:.1f}%\n\n"
                    f"{build_stats_text()}"
                )
                changed = True
                continue

        if age_minutes >= FAST_HARD_EXPIRE_MINUTES and not s.get("tp1_hit"):
            apply_result(s, "expired")
            send_telegram(
                f"⏱ HARD EXPIRE\n"
                f"{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                f"Стратегия: {s['strategy']}\n"
                f"TP1 не достигнут за {FAST_HARD_EXPIRE_MINUTES} минут.\n"
                f"Текущая цена: {format_price(p)}\n\n"
                f"{build_stats_text()}"
            )
            changed = True
            continue

        hit_any = False
        for key in ["tp1", "tp2", "tp3", "tp4"]:
            if s.get(key) and not s.get(f"{key}_hit") and target_hit(side, p, s[key]):
                s[f"{key}_hit"] = True
                hit_any = True
                send_telegram(
                    f"🎯 {key.upper()} HIT\n"
                    f"{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                    f"Стратегия: {s['strategy']}\n"
                    f"{key.upper()}: {format_price(s[key])}\n"
                    f"Текущая цена: {format_price(p)}"
                )

        if s.get("tp5") and target_hit(side, p, s["tp5"]):
            s["tp5_hit"] = True
            apply_result(s, "profit")
            send_telegram(
                f"✅ FULL LADDER TAKE PROFIT\n"
                f"{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                f"Стратегия: {s['strategy']}\n"
                f"TP5 достигнут: {format_price(p)}\n"
                f"Время в сделке: {age_minutes:.1f} мин\n\n"
                f"{build_stats_text()}"
            )
            changed = True
            continue

        if hit_any:
            changed = True

        remaining.append(s)

    if changed:
        STATE["active_signals"] = remaining
        save_state()

# ============================================================
# Background tasks / HTTP endpoints
# ============================================================

async def scan_loop():
    await asyncio.sleep(3)
    send_telegram(
        f"✅ {APP_NAME} активирован.\n"
        f"Deploy marker: {DEPLOY_MARKER}\n\n"
        f"Mode: TRADER PATTERN QUALITY SCALPER.\n"
        f"Логика: торгуем не фазу рынка, а только короткий дисбаланс: hot coin → sweep/reclaim → EMA/VWAP → immediate continuation → 5 TP.\n"
        f"Time-stop: если TP1 не двигается за {FAST_MAX_MINUTES_TO_TP1} мин — expired.\n"
        f"Compact targets: {TP1_MOVE*100:.2f}% / {TP2_MOVE*100:.2f}% / {TP3_MOVE*100:.2f}% / {TP4_MOVE*100:.2f}% / {TP5_MOVE*100:.2f}%.\n"
        f"Risk multiplier: B x{FAST_RISK_MULT:.2f}, A+ x{A_RISK_MULT:.2f}."
    )
    try:
        scan = run_scan(manual=True)
        send_telegram(build_diagnostic(scan))
    except Exception as e:
        STATE["last_error"] = f"first scan exception: {repr(e)}"
        save_state()
        send_telegram(f"⚠️ Ошибка первого скана: {repr(e)}")

    while True:
        try:
            if AUTO_SCAN_ENABLED:
                run_scan(manual=False)
        except Exception as e:
            STATE["last_error"] = f"scan_loop: {repr(e)}"
            save_state()
            send_telegram(f"⚠️ Ошибка auto-scan: {repr(e)}")
        await asyncio.sleep(AUTO_SCAN_SECONDS)


async def track_loop():
    await asyncio.sleep(8)
    while True:
        try:
            if AUTO_TRACK_ENABLED:
                track_active_signals()
        except Exception as e:
            STATE["last_error"] = f"track_loop: {repr(e)}"
            save_state()
        await asyncio.sleep(AUTO_TRACK_SECONDS)


@app.on_event("startup")
async def startup_event():
    global STATE
    STATE = load_state()
    asyncio.create_task(scan_loop())
    asyncio.create_task(track_loop())


@app.get("/")
def root():
    return HTMLResponse(
        f"<h3>{APP_NAME}</h3>"
        f"<p>{DEPLOY_MARKER}</p>"
        f"<p>Use /health /version /scan /auto-status /stats /test-telegram</p>"
    )


@app.get("/health")
def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "deploy": DEPLOY_MARKER,
        "active": len(STATE.get("active_signals", [])),
        "last_error": STATE.get("last_error", ""),
    }


@app.get("/version")
def version():
    return {"app": APP_NAME, "deploy_marker": DEPLOY_MARKER}


@app.get("/auto-status")
def auto_status():
    return JSONResponse({
        "app": APP_NAME,
        "deploy": DEPLOY_MARKER,
        "active_signals": STATE.get("active_signals", []),
        "last_scan": STATE.get("last_scan", {}),
        "last_error": STATE.get("last_error", ""),
        "stats": STATE.get("stats", {}),
    })


@app.get("/scan")
def manual_scan(send: bool = Query(True)):
    scan = run_scan(manual=True)
    if send:
        send_telegram(build_diagnostic(scan))
    return JSONResponse(scan)


@app.get("/stats")
def stats():
    return HTMLResponse("<pre>" + build_stats_text() + "</pre>")


@app.get("/test-telegram")
def test_telegram():
    ok = send_telegram(f"✅ Test Telegram OK\n{APP_NAME}\n{DEPLOY_MARKER}")
    return {"sent": ok, "last_error": STATE.get("last_error", "")}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
