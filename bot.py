import os
import time
import json
import math
import random
import asyncio
import traceback
from typing import Optional, List, Dict, Any, Tuple

import requests
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

APP_NAME = "Professional Adaptive Futures Bot AUTO V8.0 CLEAN PRO TRADER"
DEPLOY_MARKER = "V8_0_CLEAN_PRO_TRADER_2026_06_11"
app = FastAPI(title=APP_NAME)

# =====================
# ENV / SETTINGS
# =====================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
BINGX_BASE_URL = "https://open-api.bingx.com"
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")
API_KEY = os.getenv("API_KEY", "")

TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
DEFAULT_DEPOSIT = float(os.getenv("DEFAULT_DEPOSIT", "1000"))
DEFAULT_RISK_PERCENT = float(os.getenv("DEFAULT_RISK_PERCENT", "0.5"))

AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "120"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "45"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "320"))
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "900"))
SIGNAL_MAX_LIFETIME_SECONDS = int(os.getenv("SIGNAL_MAX_LIFETIME_SECONDS", "21600"))
DEBUG_NO_SIGNAL_REPORT_ENABLED = os.getenv("DEBUG_NO_SIGNAL_REPORT_ENABLED", "true").lower() == "true"
DEBUG_NO_SIGNAL_REPORT_SECONDS = int(os.getenv("DEBUG_NO_SIGNAL_REPORT_SECONDS", "900"))
SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK = os.getenv("SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK", "true").lower() == "true"

# Signal quality. A+ remains strict; B is usable but lower risk.
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "86"))
A_PLUS_MIN_RR = float(os.getenv("A_PLUS_MIN_RR", "1.00"))
A_PLUS_MIN_VOLUME_RATIO = float(os.getenv("A_PLUS_MIN_VOLUME_RATIO", "1.15"))
A_PLUS_RISK_MULTIPLIER = float(os.getenv("A_PLUS_RISK_MULTIPLIER", "1.0"))

B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "74"))
B_MIN_RR = float(os.getenv("B_MIN_RR", "0.62"))
B_MIN_VOLUME_RATIO = float(os.getenv("B_MIN_VOLUME_RATIO", "0.90"))
B_RISK_MULTIPLIER = float(os.getenv("B_RISK_MULTIPLIER", "0.22"))

# Stronger rules for short B because most bad results came from weak shorts.
SHORT_B_MIN_SCORE = int(os.getenv("SHORT_B_MIN_SCORE", "78"))
SHORT_B_MIN_RR = float(os.getenv("SHORT_B_MIN_RR", "0.70"))
SHORT_B_MIN_VOLUME_RATIO = float(os.getenv("SHORT_B_MIN_VOLUME_RATIO", "0.98"))

# Extreme movers: active coins like HMSTR, MAGMA, GUA etc. Risk is tiny by design.
EXTREME_MOVER_ENABLED = os.getenv("EXTREME_MOVER_ENABLED", "true").lower() == "true"
DYNAMIC_EXTREME_SCANNER_ENABLED = os.getenv("DYNAMIC_EXTREME_SCANNER_ENABLED", "true").lower() == "true"
DYNAMIC_EXTREME_TOP_N = int(os.getenv("DYNAMIC_EXTREME_TOP_N", "70"))
DYNAMIC_EXTREME_MIN_24H_MOVE_PERCENT = float(os.getenv("DYNAMIC_EXTREME_MIN_24H_MOVE_PERCENT", "10"))
EXTREME_MIN_24H_MOVE_PERCENT = float(os.getenv("EXTREME_MIN_24H_MOVE_PERCENT", "8"))
EXTREME_MIN_6H_MOVE_PERCENT = float(os.getenv("EXTREME_MIN_6H_MOVE_PERCENT", "3.5"))
EXTREME_PULLBACK_MIN_PERCENT = float(os.getenv("EXTREME_PULLBACK_MIN_PERCENT", "0.8"))
EXTREME_PULLBACK_MAX_PERCENT = float(os.getenv("EXTREME_PULLBACK_MAX_PERCENT", "10"))
EXTREME_A_PLUS_RISK_MULTIPLIER = float(os.getenv("EXTREME_A_PLUS_RISK_MULTIPLIER", "0.15"))
EXTREME_B_ENABLED = os.getenv("EXTREME_B_ENABLED", "true").lower() == "true"
EXTREME_B_RISK_MULTIPLIER = float(os.getenv("EXTREME_B_RISK_MULTIPLIER", "0.05"))
EXTREME_B_MIN_SCORE = int(os.getenv("EXTREME_B_MIN_SCORE", "80"))
EXTREME_B_MIN_RR = float(os.getenv("EXTREME_B_MIN_RR", "0.70"))
EXTREME_B_MIN_VOLUME_RATIO = float(os.getenv("EXTREME_B_MIN_VOLUME_RATIO", "0.95"))

# BTC Master filter. Hard impulse against trade blocks B; A+ exception must be very strong.
BTC_MASTER_ENABLED = os.getenv("BTC_MASTER_ENABLED", "true").lower() == "true"
BTC_FAST_1M_PERCENT = float(os.getenv("BTC_FAST_1M_PERCENT", "0.20"))
BTC_FAST_5M_PERCENT = float(os.getenv("BTC_FAST_5M_PERCENT", "0.38"))
BTC_FAST_15M_PERCENT = float(os.getenv("BTC_FAST_15M_PERCENT", "0.55"))
BTC_STORM_15M_PERCENT = float(os.getenv("BTC_STORM_15M_PERCENT", "0.85"))
ALLOW_A_PLUS_BTC_EXCEPTION = os.getenv("ALLOW_A_PLUS_BTC_EXCEPTION", "true").lower() == "true"
BTC_COUNTER_B_RISK_MULTIPLIER = float(os.getenv("BTC_COUNTER_B_RISK_MULTIPLIER", "0.08"))

# Risk and trade management.
MAX_RISK_POSITION_PERCENT = float(os.getenv("MAX_RISK_POSITION_PERCENT", "15"))
MIN_SL_PRICE_MOVE_PERCENT = float(os.getenv("MIN_SL_PRICE_MOVE_PERCENT", "0.25"))
TP1_R_MULTIPLIER = float(os.getenv("TP1_R_MULTIPLIER", "0.75"))
TP2_R_MULTIPLIER = float(os.getenv("TP2_R_MULTIPLIER", "1.30"))
TP3_R_MULTIPLIER = float(os.getenv("TP3_R_MULTIPLIER", "2.00"))
MIN_TP1_PRICE_MOVE_PERCENT = float(os.getenv("MIN_TP1_PRICE_MOVE_PERCENT", "0.45"))
TP1_CLOSE_PERCENT = float(os.getenv("TP1_CLOSE_PERCENT", "50"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.0005"))
SLIPPAGE_RATE = float(os.getenv("SLIPPAGE_RATE", "0.0003"))
A_PLUS_SOFT_STOP_SECONDS = int(os.getenv("A_PLUS_SOFT_STOP_SECONDS", "300"))
TP1_BE_BUFFER_PERCENT = float(os.getenv("TP1_BE_BUFFER_PERCENT", "0.08"))

# Anti-chase: block entries after strong move unless pullback continuation proves itself.
ANTI_CHASE_ENABLED = os.getenv("ANTI_CHASE_ENABLED", "true").lower() == "true"
MAX_CHASE_MOVE_5M_PERCENT = float(os.getenv("MAX_CHASE_MOVE_5M_PERCENT", "4.5"))
HARD_CHASE_MOVE_5M_PERCENT = float(os.getenv("HARD_CHASE_MOVE_5M_PERCENT", "7.5"))
MIN_PULLBACK_AFTER_CHASE_PERCENT = float(os.getenv("MIN_PULLBACK_AFTER_CHASE_PERCENT", "0.55"))
MAX_DISTANCE_FROM_VWAP_PERCENT = float(os.getenv("MAX_DISTANCE_FROM_VWAP_PERCENT", "4.8"))

PAIR_BLOCK_SECONDS = int(os.getenv("PAIR_BLOCK_SECONDS", "21600"))
PAIR_MAX_SL = int(os.getenv("PAIR_MAX_SL", "3"))
STRATEGY_SIDE_DISABLE_SECONDS = int(os.getenv("STRATEGY_SIDE_DISABLE_SECONDS", "7200"))
STRATEGY_SIDE_MAX_CONSECUTIVE_SL = int(os.getenv("STRATEGY_SIDE_MAX_CONSECUTIVE_SL", "4"))

STRATEGIES = [
    "SWEEP_RECLAIM_LONG",
    "SWEEP_REJECT_SHORT",
    "BREAK_RETEST_LONG",
    "BREAK_RETEST_SHORT",
    "TREND_PULLBACK_LONG",
    "TREND_PULLBACK_SHORT",
    "IMPULSE_PULLBACK_PRO",
    "EXTREME_MOVER_PULLBACK_PRO",
]

CORE_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "INJ", "NEAR", "ARB", "OP",
    "APT", "SUI", "SEI", "DOT", "LTC", "BCH", "UNI", "AAVE", "FIL", "ATOM", "ETC", "TRX", "POL",
    "WLD", "TIA", "ORDI", "FTM", "RUNE", "ENA", "JUP", "PYTH", "STRK", "DYDX", "TON", "COMP",
    "STX", "TRB", "JTO", "ICP", "GALA", "FET", "RNDR", "RENDER", "IMX", "APE", "AR", "MKR",
    "SNX", "LDO", "CRV", "GMT", "ONDO", "PENDLE", "ZRO", "ZK", "TAO", "SAGA", "MANTA", "ALT",
    "PIXEL", "PORTAL", "AEVO", "W", "OMNI", "TNSR", "BB", "PEOPLE", "BLUR", "AI", "ACE", "ARKM",
}

EXTREME_BASES = {
    "HMSTR", "GUA", "DOGS", "CATI", "MEME", "NOT", "1000SATS", "1000PEPE", "PEPE", "BONK", "WIF",
    "PNUT", "ACT", "GOAT", "MOODENG", "NEIRO", "TURBO", "BOME", "MAGMA", "VELVET", "FOLKS", "STG",
    "BEAT", "FIGHT", "BLEND", "FLOKI", "SHIB", "1000SHIB", "MOG", "BRETT", "MEW", "POPCAT",
}

# =====================
# UTILS / STATE
# =====================
def now_ts() -> float:
    return time.time()


def normalize_symbol(symbol: str) -> str:
    s = symbol.upper().replace("/", "-").replace("_", "-").strip()
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
    if direction is None:
        return None
    d = direction.upper().strip()
    return d if d in {"LONG", "SHORT"} else None


def get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception:
        return None


def send_telegram_message(text: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing"}
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        return requests.post(url, json=payload, timeout=REQUEST_TIMEOUT).json()
    except Exception as e:
        return {"ok": False, "error": str(e)}


def empty_stats_for_strategy() -> Dict[str, Dict[str, int]]:
    return {s: {"positive": 0, "sl": 0, "consecutive_sl": 0} for s in STRATEGIES}


def empty_strategy_side() -> Dict[str, Dict[str, int]]:
    return {f"{s}:{side}": {"positive": 0, "sl": 0, "consecutive_sl": 0} for s in STRATEGIES for side in ["LONG", "SHORT"]}


def empty_strategy_side_grade() -> Dict[str, Dict[str, int]]:
    return {f"{s}:{side}:{g}": {"positive": 0, "sl": 0, "consecutive_sl": 0} for s in STRATEGIES for side in ["LONG", "SHORT"] for g in ["A+", "B"]}


def default_state() -> dict:
    return {
        "active_signals": {},
        "sent_signals": {},
        "symbol_cooldown": {},
        "blocked_symbols": {},
        "strategy_side_disabled_until": {f"{s}:{side}": 0 for s in STRATEGIES for side in ["LONG", "SHORT"]},
        "strategy_side_grade_disabled_until": {f"{s}:{side}:{g}": 0 for s in STRATEGIES for side in ["LONG", "SHORT"] for g in ["A+", "B"]},
        "stats": {
            "side": {"LONG": {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0},
                     "SHORT": {"positive": 0, "sl": 0, "consecutive_sl": 0, "tp1": 0, "tp2": 0, "tp3": 0}},
            "grade": {"A+": {"positive": 0, "sl": 0}, "B": {"positive": 0, "sl": 0}},
            "strategy": empty_stats_for_strategy(),
            "strategy_side": empty_strategy_side(),
            "strategy_side_grade": empty_strategy_side_grade(),
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
            "worker_started_at": int(now_ts()),
        },
        "version": DEPLOY_MARKER,
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
    state.setdefault("strategy_side_disabled_until", {})
    state.setdefault("strategy_side_grade_disabled_until", {})
    for s in STRATEGIES:
        state["stats"].setdefault("strategy", {}).setdefault(s, {"positive": 0, "sl": 0, "consecutive_sl": 0})
        for side in ["LONG", "SHORT"]:
            ss = f"{s}:{side}"
            state["stats"].setdefault("strategy_side", {}).setdefault(ss, {"positive": 0, "sl": 0, "consecutive_sl": 0})
            state["strategy_side_disabled_until"].setdefault(ss, 0)
            for g in ["A+", "B"]:
                ssg = f"{s}:{side}:{g}"
                state["stats"].setdefault("strategy_side_grade", {}).setdefault(ssg, {"positive": 0, "sl": 0, "consecutive_sl": 0})
                state["strategy_side_grade_disabled_until"].setdefault(ssg, 0)
    return state


def load_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return ensure_state_structure(json.load(f))
    except Exception:
        pass
    return default_state()


STATE = load_state()


def save_state(state: dict = None):
    try:
        st = ensure_state_structure(state or STATE)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def calc_winrate(pos: int, sl: int) -> float:
    total = pos + sl
    return round(pos / total * 100, 1) if total > 0 else 0.0


def calc_profit_factor_from_closed() -> float:
    trades = STATE.get("stats", {}).get("closed_trades", [])
    wins = sum(float(t.get("r_multiple", 0)) for t in trades if float(t.get("r_multiple", 0)) > 0)
    losses = abs(sum(float(t.get("r_multiple", 0)) for t in trades if float(t.get("r_multiple", 0)) < 0))
    if losses <= 0:
        return round(wins, 2) if wins > 0 else 0.0
    return round(wins / losses, 2)


def stats_line(name: str, data: dict) -> str:
    p, sl = int(data.get("positive", 0)), int(data.get("sl", 0))
    return f"{name}: {p} позитив / {sl} SL / WR {calc_winrate(p, sl)}%"


def build_stats_text() -> str:
    ensure_state_structure(STATE)
    long_s = STATE["stats"]["side"]["LONG"]
    short_s = STATE["stats"]["side"]["SHORT"]
    a_s = STATE["stats"]["grade"].get("A+", {})
    b_s = STATE["stats"]["grade"].get("B", {})
    lines = []
    for s in STRATEGIES:
        data = STATE["stats"]["strategy"].get(s, {})
        enabled = "ON"
        # Show disabled if any side is currently disabled.
        now = now_ts()
        if STATE.get("strategy_side_disabled_until", {}).get(f"{s}:LONG", 0) > now or STATE.get("strategy_side_disabled_until", {}).get(f"{s}:SHORT", 0) > now:
            enabled = "PARTLY OFF"
        lines.append(f"{stats_line(s, data)} [{enabled}]")
    return f"""
📊 <b>Статистика {DEPLOY_MARKER}:</b>

📈 LONG: {long_s.get('positive', 0)} позитив / {long_s.get('sl', 0)} SL / WR {calc_winrate(long_s.get('positive', 0), long_s.get('sl', 0))}%
📉 SHORT: {short_s.get('positive', 0)} позитив / {short_s.get('sl', 0)} SL / WR {calc_winrate(short_s.get('positive', 0), short_s.get('sl', 0))}%

🏆 A+: {a_s.get('positive', 0)} позитив / {a_s.get('sl', 0)} SL / WR {calc_winrate(a_s.get('positive', 0), a_s.get('sl', 0))}%
⚠️ B: {b_s.get('positive', 0)} позитив / {b_s.get('sl', 0)} SL / WR {calc_winrate(b_s.get('positive', 0), b_s.get('sl', 0))}%
📐 Profit Factor по закрытым R-сделкам: {calc_profit_factor_from_closed()}

🧠 <b>Стратегии:</b>
{chr(10).join(lines)}
""".strip()

# =====================
# MARKET DATA / INDICATORS
# =====================
def interval_to_ms(interval: str) -> int:
    return {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}.get(interval, 60_000)


def remove_unclosed(candles: Optional[List[dict]], interval: str) -> Optional[List[dict]]:
    if not candles or len(candles) < 3:
        return candles
    now_ms = int(time.time() * 1000)
    last_open = int(candles[-1].get("time", 0))
    if now_ms < last_open + interval_to_ms(interval):
        return candles[:-1]
    return candles


def get_klines(symbol: str, interval: str, limit: int = 260) -> Optional[List[dict]]:
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    data = get_json(url, {"symbol": normalize_symbol(symbol), "interval": interval, "limit": limit})
    if not data:
        return None
    raw = data.get("data", [])
    candles = []
    for c in raw:
        try:
            candles.append({
                "time": int(c["time"]), "open": float(c["open"]), "high": float(c["high"]),
                "low": float(c["low"]), "close": float(c["close"]), "volume": float(c["volume"]),
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["time"])
    return candles if len(candles) >= 50 else None


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
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
    return 100 - (100 / (1 + rs))


def atr(candles: List[dict], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        high, low, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(high - low, abs(high - pc), abs(low - pc)))
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


def move_percent(candles: List[dict], lookback: int) -> float:
    if len(candles) < lookback + 1:
        return 0.0
    old = candles[-lookback]["close"]
    new = candles[-1]["close"]
    return (new - old) / old * 100 if old > 0 else 0.0


def candle_close_position(c: dict) -> float:
    rng = c["high"] - c["low"]
    return (c["close"] - c["low"]) / rng if rng > 0 else 0.5


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


def merge_levels(levels: List[float], threshold_percent: float = 0.40) -> List[float]:
    if not levels:
        return []
    levels = sorted(levels)
    out = []
    for lvl in levels:
        if not out or abs(lvl - out[-1]) / out[-1] * 100 > threshold_percent:
            out.append(lvl)
        else:
            out[-1] = (out[-1] + lvl) / 2
    return out


def find_support_levels(candles: List[dict], lookback: int = 140) -> List[float]:
    w = candles[-lookback:] if len(candles) >= lookback else candles
    levels = []
    for i in range(3, len(w) - 3):
        low = w[i]["low"]
        if low <= min(w[i - 3]["low"], w[i - 2]["low"], w[i - 1]["low"], w[i + 1]["low"], w[i + 2]["low"], w[i + 3]["low"]):
            levels.append(low)
    return merge_levels(levels)


def find_resistance_levels(candles: List[dict], lookback: int = 140) -> List[float]:
    w = candles[-lookback:] if len(candles) >= lookback else candles
    levels = []
    for i in range(3, len(w) - 3):
        high = w[i]["high"]
        if high >= max(w[i - 3]["high"], w[i - 2]["high"], w[i - 1]["high"], w[i + 1]["high"], w[i + 2]["high"], w[i + 3]["high"]):
            levels.append(high)
    return merge_levels(levels)


def nearest_below(price: float, levels: List[float], max_distance_percent: float = 8.0) -> Optional[float]:
    c = [l for l in levels if l < price]
    if not c:
        return None
    lvl = max(c)
    return lvl if abs(price - lvl) / price * 100 <= max_distance_percent else None


def nearest_above(price: float, levels: List[float], max_distance_percent: float = 8.0) -> Optional[float]:
    c = [l for l in levels if l > price]
    if not c:
        return None
    lvl = min(c)
    return lvl if abs(lvl - price) / price * 100 <= max_distance_percent else None


def nearest_near(price: float, levels: List[float], max_distance_percent: float = 2.0) -> Optional[float]:
    if not levels or price <= 0:
        return None
    lvl = min(levels, key=lambda x: abs(x - price))
    return lvl if abs(lvl - price) / price * 100 <= max_distance_percent else None

# =====================
# SYMBOLS / TICKERS
# =====================
def parse_ticker_24h() -> Dict[str, float]:
    """Return {SYMBOL: 24h percent change}; tolerant to BingX response variations."""
    endpoints = [
        "/openApi/swap/v2/quote/ticker",
        "/openApi/swap/v2/quote/ticker/24hr",
    ]
    out = {}
    for ep in endpoints:
        data = get_json(f"{BINGX_BASE_URL}{ep}")
        if not data:
            continue
        raw = data.get("data", data)
        if isinstance(raw, dict):
            raw = raw.get("list", raw.get("data", []))
        if not isinstance(raw, list):
            continue
        for item in raw:
            try:
                sym = item.get("symbol") or item.get("s")
                if not sym:
                    continue
                n = normalize_symbol(sym)
                # Common fields: priceChangePercent, priceChangeRate, change, riseFallRate.
                val = None
                for key in ["priceChangePercent", "priceChangeRate", "change", "riseFallRate", "priceChangeRatio"]:
                    if key in item and item[key] is not None:
                        val = float(item[key])
                        break
                if val is None and item.get("openPrice") and item.get("lastPrice"):
                    op, lp = float(item["openPrice"]), float(item["lastPrice"])
                    val = (lp - op) / op * 100 if op > 0 else 0
                if val is None:
                    continue
                # Some APIs return fraction, e.g. 0.12 = 12%.
                if abs(val) < 1.5:
                    val *= 100
                out[n] = val
            except Exception:
                continue
        if out:
            return out
    return out


def get_contract_symbols() -> List[str]:
    data = get_json(f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts")
    symbols = []
    if data:
        for item in data.get("data", []):
            sym = item.get("symbol")
            if sym:
                symbols.append(normalize_symbol(sym))
    return list(dict.fromkeys(symbols))


def is_allowed_symbol(symbol: str, dynamic_extremes: Optional[set] = None) -> bool:
    base = base_from_symbol(symbol)
    if dynamic_extremes and normalize_symbol(symbol) in dynamic_extremes:
        return True
    if base in CORE_BASES or base in EXTREME_BASES:
        return True
    return False


def get_symbols() -> Tuple[List[str], Dict[str, float], List[str]]:
    all_contracts = get_contract_symbols()
    change_map = parse_ticker_24h()
    dynamic = []
    if DYNAMIC_EXTREME_SCANNER_ENABLED and change_map:
        candidates = [(s, abs(chg)) for s, chg in change_map.items() if s in all_contracts and abs(chg) >= DYNAMIC_EXTREME_MIN_24H_MOVE_PERCENT]
        candidates.sort(key=lambda x: x[1], reverse=True)
        dynamic = [s for s, _ in candidates[:DYNAMIC_EXTREME_TOP_N]]
    dynamic_set = set(dynamic)
    selected = [s for s in all_contracts if is_allowed_symbol(s, dynamic_set)]
    # Prioritize dynamic movers and core liquid symbols.
    selected.sort(key=lambda s: (0 if s in dynamic_set else 1, -abs(change_map.get(s, 0))))
    if len(selected) > MAX_SYMBOLS:
        selected = selected[:MAX_SYMBOLS]
    return selected, change_map, dynamic[:20]

# =====================
# BTC MASTER / REGIME
# =====================
def get_btc_context() -> dict:
    c1 = remove_unclosed(get_klines("BTC-USDT", "1m", 80), "1m")
    c5 = remove_unclosed(get_klines("BTC-USDT", "5m", 120), "5m")
    c15 = remove_unclosed(get_klines("BTC-USDT", "15m", 120), "15m")
    c1h = remove_unclosed(get_klines("BTC-USDT", "1h", 260), "1h")
    c4h = remove_unclosed(get_klines("BTC-USDT", "4h", 260), "4h")
    if not all([c1, c5, c15, c1h, c4h]):
        return {"mode": "UNKNOWN", "direction": "NEUTRAL", "note": "BTC data unavailable", "hard_up": False, "hard_down": False, "storm": False}
    m1 = move_percent(c1, 3)
    m5 = move_percent(c5, 3)
    m15 = move_percent(c15, 2)
    t1h = trend_state(c1h)
    t4h = trend_state(c4h)
    closes5 = [c["close"] for c in c5]
    ema21_5 = ema(closes5, 21)[-1]
    price = closes5[-1]
    above = price > ema21_5
    hard_up = (m1 >= BTC_FAST_1M_PERCENT) or (m5 >= BTC_FAST_5M_PERCENT) or (m15 >= BTC_FAST_15M_PERCENT)
    hard_down = (m1 <= -BTC_FAST_1M_PERCENT) or (m5 <= -BTC_FAST_5M_PERCENT) or (m15 <= -BTC_FAST_15M_PERCENT)
    storm = abs(m15) >= BTC_STORM_15M_PERCENT or abs(m5) >= BTC_FAST_5M_PERCENT * 1.6
    if hard_up and above:
        mode, direction = "BTC_FAST_UP", "UP"
    elif hard_down and not above:
        mode, direction = "BTC_FAST_DOWN", "DOWN"
    elif t1h in ["BULLISH", "SOFT_BULLISH"] and above:
        mode, direction = "BTC_SLOW_UP", "UP"
    elif t1h in ["BEARISH", "SOFT_BEARISH"] and not above:
        mode, direction = "BTC_SLOW_DOWN", "DOWN"
    else:
        mode, direction = "BTC_RANGE", "NEUTRAL"
    return {
        "mode": mode, "direction": direction, "trend1h": t1h, "trend4h": t4h,
        "move1m": round(m1, 3), "move5m": round(m5, 3), "move15m": round(m15, 3),
        "hard_up": hard_up, "hard_down": hard_down, "storm": storm,
        "note": f"{mode}; 1m {m1:.2f}% / 5m {m5:.2f}% / 15m {m15:.2f}%; 1H {t1h}; 4H {t4h}",
    }


def btc_allows(direction: str, btc: dict, score: int, is_extreme: bool) -> Tuple[bool, str, Optional[str], Optional[float], int]:
    """Return allowed, note, forced_grade, risk_override, score_adjustment."""
    if not BTC_MASTER_ENABLED:
        return True, "BTC master off", None, None, 0
    against = (direction == "LONG" and btc.get("hard_down")) or (direction == "SHORT" and btc.get("hard_up"))
    soft_against = (direction == "LONG" and btc.get("direction") == "DOWN") or (direction == "SHORT" and btc.get("direction") == "UP")
    with_btc = (direction == "LONG" and btc.get("direction") == "UP") or (direction == "SHORT" and btc.get("direction") == "DOWN")
    if against:
        if ALLOW_A_PLUS_BTC_EXCEPTION and score >= 92 and not btc.get("storm") and is_extreme:
            return True, "BTC fast против, но разрешено только как редкое A+ exception для extreme-сетапа.", "A+", min(EXTREME_A_PLUS_RISK_MULTIPLIER, 0.12), -3
        return False, "BTC fast/storm против направления — вход заблокирован.", None, None, 0
    if soft_against:
        return True, "BTC мягко против: только B с микрориском.", "B", BTC_COUNTER_B_RISK_MULTIPLIER, -4
    if with_btc:
        return True, "BTC подтверждает направление.", None, None, 5
    return True, "BTC нейтральный: сетап оценивается по монете.", None, None, 0


def market_regime(c15: List[dict], c1h: List[dict], btc: dict) -> str:
    m15 = move_percent(c15, 8)
    t1h = trend_state(c1h)
    closes = [c["close"] for c in c15]
    e21 = ema(closes, 21)[-1] if len(closes) > 22 else closes[-1]
    price = closes[-1]
    if abs(m15) < 0.45 and t1h == "NEUTRAL":
        return "RANGE"
    if m15 > 4.5:
        return "EXPANSION_UP"
    if m15 < -4.5:
        return "EXPANSION_DOWN"
    if price > e21 and t1h in ["BULLISH", "SOFT_BULLISH"]:
        return "TREND_UP"
    if price < e21 and t1h in ["BEARISH", "SOFT_BEARISH"]:
        return "TREND_DOWN"
    if btc.get("storm") and abs(m15) < 0.8:
        return "CHOP"
    return "RANGE"

# =====================
# SIGNAL CORE
# =====================
def is_on_cooldown(symbol: str) -> bool:
    ts = STATE.get("symbol_cooldown", {}).get(normalize_symbol(symbol), 0)
    return bool(ts and now_ts() - ts < SIGNAL_COOLDOWN_SECONDS)


def is_symbol_blocked(symbol: str) -> bool:
    n = normalize_symbol(symbol)
    until = STATE.get("blocked_symbols", {}).get(n, 0)
    if until and until > now_ts():
        return True
    if until:
        STATE["blocked_symbols"].pop(n, None)
        save_state()
    return False


def strategy_grade_enabled(strategy: str, side: str, grade: str) -> bool:
    if STATE.get("strategy_side_disabled_until", {}).get(f"{strategy}:{side}", 0) > now_ts():
        return False
    if STATE.get("strategy_side_grade_disabled_until", {}).get(f"{strategy}:{side}:{grade}", 0) > now_ts():
        return False
    return True


def get_wr_for_key(section: str, key: str) -> Tuple[int, float]:
    d = STATE.get("stats", {}).get(section, {}).get(key, {})
    p, sl = int(d.get("positive", 0)), int(d.get("sl", 0))
    return p + sl, calc_winrate(p, sl)


def strategy_trust(strategy: str, side: str, grade: Optional[str] = None) -> Tuple[str, int]:
    trades, wr = get_wr_for_key("strategy_side", f"{strategy}:{side}")
    bonus = 0
    trust = "NEW"
    if trades >= 8:
        if wr >= 60:
            trust, bonus = "HIGH", 6
        elif wr >= 50:
            trust, bonus = "MEDIUM", 2
        elif wr >= 43:
            trust, bonus = "LOW", -4
        else:
            trust, bonus = "BAD", -12
    if grade:
        gtr, gwr = get_wr_for_key("strategy_side_grade", f"{strategy}:{side}:{grade}")
        if gtr >= 6 and gwr < 45:
            return "BAD_GRADE", -14
        if gtr >= 6 and gwr >= 58:
            bonus += 3
    return trust, bonus


def calc_cost_percent() -> float:
    return (FEE_RATE * 2 + SLIPPAGE_RATE * 2) * 100


def price_move_percent(entry: float, target: float, direction: str) -> float:
    if direction == "LONG":
        return (target - entry) / entry * 100 if entry > 0 else 0
    return (entry - target) / entry * 100 if entry > 0 else 0


def calc_risk_position(entry: float, sl: float) -> float:
    return abs(entry - sl) / entry * 100 * LEVERAGE if entry > 0 else 999


def make_tps(entry: float, sl: float, direction: str) -> Tuple[float, float, float]:
    risk = max(abs(entry - sl), entry * MIN_TP1_PRICE_MOVE_PERCENT / 100)
    if direction == "LONG":
        return entry + risk * TP1_R_MULTIPLIER, entry + risk * TP2_R_MULTIPLIER, entry + risk * TP3_R_MULTIPLIER
    return entry - risk * TP1_R_MULTIPLIER, entry - risk * TP2_R_MULTIPLIER, entry - risk * TP3_R_MULTIPLIER


def ensure_min_sl(entry: float, sl: float, direction: str, atr_val: float, grade_hint: str) -> float:
    min_dist = entry * MIN_SL_PRICE_MOVE_PERCENT / 100
    atr_buffer = (atr_val or min_dist) * (0.35 if grade_hint == "A+" else 0.25)
    required = max(min_dist, atr_buffer)
    if abs(entry - sl) >= required:
        return sl
    if direction == "LONG":
        return entry - required
    return entry + required


def calculate_position(entry: float, sl: float, deposit: float, risk_percent: float) -> dict:
    risk_amount = deposit * risk_percent / 100
    dist = abs(entry - sl)
    if entry <= 0 or sl <= 0 or dist <= 0:
        return {"risk_amount": round(risk_amount, 2), "position_size_usdt": None, "coin_amount": None, "margin_usdt": None, "error": "bad entry/sl"}
    coin = risk_amount / dist
    position = coin * entry
    return {"risk_amount": round(risk_amount, 2), "position_size_usdt": round(position, 2), "coin_amount": round(coin, 8), "margin_usdt": round(position / LEVERAGE, 2), "error": None}


def anti_chase_allows(direction: str, c5: List[dict]) -> Tuple[bool, str, bool]:
    """allowed, note, is_pullback_continuation"""
    if not ANTI_CHASE_ENABLED or len(c5) < 40:
        return True, "Anti-chase off/insufficient", True
    closes = [c["close"] for c in c5]
    e21 = ema(closes, 21)[-1]
    price = closes[-1]
    vw = vwap_like(c5, 48) or e21
    recent_move = move_percent(c5, 12)
    recent = c5[-8:]
    if direction == "LONG" and recent_move > MAX_CHASE_MOVE_5M_PERCENT:
        high = max(c["high"] for c in c5[-18:])
        pullback = (high - min(c["low"] for c in recent)) / high * 100 if high > 0 else 0
        continued = price > e21 * 0.995 and price > vw * 0.992 and c5[-1]["close"] > c5[-1]["open"]
        if recent_move > HARD_CHASE_MOVE_5M_PERCENT and pullback < 1.1:
            return False, f"LONG chase blocked: move {recent_move:.2f}% без отката.", False
        if pullback >= MIN_PULLBACK_AFTER_CHASE_PERCENT and continued:
            return True, f"LONG after extension allowed: был pullback {pullback:.2f}% и удержание EMA/VWAP.", True
        return False, f"LONG chase blocked: move {recent_move:.2f}%, pullback {pullback:.2f}% слабый.", False
    if direction == "SHORT" and recent_move < -MAX_CHASE_MOVE_5M_PERCENT:
        low = min(c["low"] for c in c5[-18:])
        pullback = (max(c["high"] for c in recent) - low) / low * 100 if low > 0 else 0
        continued = price < e21 * 1.005 and price < vw * 1.008 and c5[-1]["close"] < c5[-1]["open"]
        if abs(recent_move) > HARD_CHASE_MOVE_5M_PERCENT and pullback < 1.1:
            return False, f"SHORT chase blocked: падение {recent_move:.2f}% без отката.", False
        if pullback >= MIN_PULLBACK_AFTER_CHASE_PERCENT and continued:
            return True, f"SHORT after extension allowed: был pullback {pullback:.2f}% и продолжение вниз.", True
        return False, f"SHORT chase blocked: падение {recent_move:.2f}%, pullback {pullback:.2f}% слабый.", False
    return True, "No chase extension", True


def classify_and_build(symbol: str, direction: str, strategy: str, entry: float, sl: float, score: int, vol: float,
                       reason: str, deposit: float, risk_percent: float, meta: dict, atr_val: float) -> Optional[dict]:
    # Strategy trust adjusts score and can block weak combos.
    trust, trust_bonus = strategy_trust(strategy, direction)
    score += trust_bonus
    if trust == "BAD" and score < A_PLUS_MIN_SCORE + 8:
        meta["blocked_reason"] = f"Strategy trust BAD for {strategy}:{direction}"
        return None

    forced_grade = meta.get("force_grade")
    is_extreme = strategy == "EXTREME_MOVER_PULLBACK_PRO"

    # Provisional grade by score/RR after SL is normalized.
    sl_a = ensure_min_sl(entry, sl, direction, atr_val, "A+")
    tp1, tp2, tp3 = make_tps(entry, sl_a, direction)
    risk_pct = abs(entry - sl_a) / entry * 100 if entry > 0 else 0
    rr = max(price_move_percent(entry, tp2, direction) - calc_cost_percent(), 0) / risk_pct if risk_pct > 0 else 0

    b_score, b_rr, b_vol = (SHORT_B_MIN_SCORE, SHORT_B_MIN_RR, SHORT_B_MIN_VOLUME_RATIO) if direction == "SHORT" else (B_MIN_SCORE, B_MIN_RR, B_MIN_VOLUME_RATIO)
    if is_extreme:
        b_score, b_rr, b_vol = EXTREME_B_MIN_SCORE, EXTREME_B_MIN_RR, EXTREME_B_MIN_VOLUME_RATIO

    grade = None
    if forced_grade == "A+":
        if score >= A_PLUS_MIN_SCORE and rr >= A_PLUS_MIN_RR and vol >= A_PLUS_MIN_VOLUME_RATIO:
            grade = "A+"
    elif forced_grade == "B":
        if score >= b_score and rr >= b_rr and vol >= b_vol:
            grade = "B"
    else:
        if score >= A_PLUS_MIN_SCORE and rr >= A_PLUS_MIN_RR and vol >= A_PLUS_MIN_VOLUME_RATIO:
            grade = "A+"
        elif score >= b_score and rr >= b_rr and vol >= b_vol:
            grade = "B"

    if not grade:
        return None
    if is_extreme and grade == "B" and not EXTREME_B_ENABLED:
        return None

    trust_g, trust_grade_bonus = strategy_trust(strategy, direction, grade)
    if trust_g == "BAD_GRADE" and grade == "B":
        return None
    if grade == "A+" and trust == "BAD":
        return None

    # Recompute SL with final grade.
    sl = ensure_min_sl(entry, sl, direction, atr_val, grade)
    if calc_risk_position(entry, sl) > MAX_RISK_POSITION_PERCENT:
        return None
    tp1, tp2, tp3 = make_tps(entry, sl, direction)
    risk_pct = abs(entry - sl) / entry * 100 if entry > 0 else 0
    raw_tp2 = price_move_percent(entry, tp2, direction)
    rr = max(raw_tp2 - calc_cost_percent(), 0) / risk_pct if risk_pct > 0 else 0

    if rr < (A_PLUS_MIN_RR if grade == "A+" else b_rr):
        return None
    if not strategy_grade_enabled(strategy, direction, grade):
        return None

    if is_extreme:
        risk_mult = EXTREME_A_PLUS_RISK_MULTIPLIER if grade == "A+" else EXTREME_B_RISK_MULTIPLIER
    else:
        risk_mult = A_PLUS_RISK_MULTIPLIER if grade == "A+" else B_RISK_MULTIPLIER
    if meta.get("risk_multiplier_override") is not None:
        risk_mult = min(risk_mult, float(meta["risk_multiplier_override"]))

    signal_id = f"{normalize_symbol(symbol)}:{strategy}:{direction}:{grade}:{round(entry, 8)}"
    if signal_id in STATE.get("sent_signals", {}):
        return None

    adjusted_risk = risk_percent * risk_mult
    pos = calculate_position(entry, sl, deposit, adjusted_risk)
    return {
        "id": signal_id,
        "symbol": normalize_symbol(symbol),
        "display_symbol": display_symbol(symbol),
        "direction": direction,
        "strategy": strategy,
        "grade": grade,
        "score": max(0, min(98, int(score))),
        "trust": trust,
        "entry": round(entry, 8), "sl": round(sl, 8), "tp1": round(tp1, 8), "tp2": round(tp2, 8), "tp3": round(tp3, 8),
        "rr": round(rr, 2), "rr_basis": "TP2 net", "volume_ratio": round(vol, 2),
        "risk_position_percent": round(calc_risk_position(entry, sl), 2),
        "risk_percent": adjusted_risk, "risk_multiplier": risk_mult, "position": pos,
        "reason": reason,
        "filters": meta,
        "created_at": now_ts(), "last_checked_time": int(now_ts() * 1000),
        "tp1_hit": False, "tp2_hit": False, "tp3_hit": False,
        "counted_positive": False, "counted_sl": False, "counted_tp1": False, "counted_tp2": False, "counted_tp3": False,
    }


def common_data(c15, c5, c1, c1h, c4h) -> Optional[dict]:
    try:
        closes15 = [c["close"] for c in c15]
        closes5 = [c["close"] for c in c5]
        closes1 = [c["close"] for c in c1]
        return {
            "price": c5[-1]["close"], "last5": c5[-1], "prev5": c5[-2],
            "a5": atr(c5), "a15": atr(c15), "vw15": vwap_like(c15), "vw5": vwap_like(c5),
            "rs5": rsi(closes5), "rs15": rsi(closes15), "vr5": volume_ratio(c5, 24),
            "ema9_1": ema(closes1, 9)[-1], "ema21_5": ema(closes5, 21)[-1], "ema50_15": ema(closes15, 50)[-1],
            "trend1h": trend_state(c1h), "trend4h": trend_state(c4h),
            "regime": None,
        }
    except Exception:
        return None


def base_meta(symbol: str, direction: str, btc: dict, d: dict, strategy: str, score: int, is_extreme: bool) -> Optional[dict]:
    allowed, note, fg, risk_override, adj = btc_allows(direction, btc, score, is_extreme)
    if not allowed:
        return None
    chase_allowed, chase_note, pullback_ok = anti_chase_allows(direction, d["c5"])
    if not chase_allowed:
        return None
    meta = {
        "btc_mode": btc.get("mode"), "btc_note": btc.get("note"), "btc_filter_note": note,
        "trend1h": d.get("trend1h"), "trend4h": d.get("trend4h"), "regime": d.get("regime"),
        "anti_chase_note": chase_note,
        "is_extreme": is_extreme,
        "score_adjustment": adj,
    }
    if fg:
        meta["force_grade"] = fg
    if risk_override is not None:
        meta["risk_multiplier_override"] = risk_override
    return meta

# =====================
# STRATEGIES
# =====================
def eval_sweep_reclaim_long(symbol, c15, c5, c1, c1h, c4h, btc, change24, deposit, risk_percent):
    direction, strategy = "LONG", "SWEEP_RECLAIM_LONG"
    dd = common_data(c15, c5, c1, c1h, c4h)
    if not dd or not all([dd["a5"], dd["vw15"], dd["rs5"], dd["ema21_5"]]): return None
    dd.update({"c5": c5, "regime": market_regime(c15, c1h, btc)})
    if dd["regime"] in ["CHOP", "EXPANSION_DOWN"]: return None
    price, last, prev = dd["price"], dd["last5"], dd["prev5"]
    level = nearest_below(price, find_support_levels(c15), 7.5) or nearest_near(price, find_support_levels(c15), 2.0)
    if not level: return None
    window = c5[-18:]
    sweep_low = min(c["low"] for c in window)
    swept = sweep_low < level * 0.998
    reclaimed = any(c["close"] > level * 1.0005 for c in c5[-5:])
    confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and price > dd["ema21_5"] * 0.99
    if not (swept and reclaimed and confirm): return None
    if dd["rs5"] and dd["rs5"] > 78: return None
    score = 61 + (8 if dd["trend1h"] in ["BULLISH", "SOFT_BULLISH"] else 0) + (4 if dd["trend4h"] in ["BULLISH", "SOFT_BULLISH"] else 0)
    score += (6 if dd["vr5"] >= 0.90 else 0) + (5 if dd["vr5"] >= 1.15 else 0) + (3 if price > dd["vw15"] * 0.99 else 0)
    score += 4 if candle_close_position(last) >= 0.55 else 0
    meta = base_meta(symbol, direction, btc, dd, strategy, score, False)
    if not meta: return None
    score += meta.get("score_adjustment", 0)
    sl = min(sweep_low - dd["a5"] * 0.15, min(c["low"] for c in c5[-10:]) - dd["a5"] * 0.05)
    return classify_and_build(symbol, direction, strategy, price, sl, score, dd["vr5"], "Sweep support → reclaim → зелёное подтверждение.", deposit, risk_percent, meta, dd["a5"])


def eval_sweep_reject_short(symbol, c15, c5, c1, c1h, c4h, btc, change24, deposit, risk_percent):
    direction, strategy = "SHORT", "SWEEP_REJECT_SHORT"
    dd = common_data(c15, c5, c1, c1h, c4h)
    if not dd or not all([dd["a5"], dd["vw15"], dd["rs5"], dd["ema21_5"]]): return None
    dd.update({"c5": c5, "regime": market_regime(c15, c1h, btc)})
    if dd["regime"] in ["CHOP", "EXPANSION_UP"]: return None
    price, last, prev = dd["price"], dd["last5"], dd["prev5"]
    level = nearest_above(price, find_resistance_levels(c15), 7.5) or nearest_near(price, find_resistance_levels(c15), 2.0)
    if not level: return None
    window = c5[-18:]
    sweep_high = max(c["high"] for c in window)
    swept = sweep_high > level * 1.002
    rejected = any(c["close"] < level * 0.9995 for c in c5[-5:])
    lower_high = max(c["high"] for c in c5[-4:]) < sweep_high * 1.0005
    confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and price < dd["ema21_5"] * 1.01
    if not (swept and rejected and lower_high and confirm): return None
    if dd["rs5"] and dd["rs5"] < 22: return None
    score = 62 + (8 if dd["trend1h"] in ["BEARISH", "SOFT_BEARISH"] else 0) + (4 if dd["trend4h"] in ["BEARISH", "SOFT_BEARISH"] else 0)
    score += (6 if dd["vr5"] >= 0.90 else 0) + (5 if dd["vr5"] >= 1.15 else 0) + (3 if price < dd["vw15"] * 1.01 else 0)
    score += 4 if candle_close_position(last) <= 0.45 else 0
    meta = base_meta(symbol, direction, btc, dd, strategy, score, False)
    if not meta: return None
    score += meta.get("score_adjustment", 0)
    sl = max(sweep_high + dd["a5"] * 0.15, max(c["high"] for c in c5[-10:]) + dd["a5"] * 0.05)
    return classify_and_build(symbol, direction, strategy, price, sl, score, dd["vr5"], "Sweep resistance → rejection → lower high → красное подтверждение.", deposit, risk_percent, meta, dd["a5"])


def eval_break_retest(symbol, direction, c15, c5, c1, c1h, c4h, btc, change24, deposit, risk_percent):
    strategy = "BREAK_RETEST_LONG" if direction == "LONG" else "BREAK_RETEST_SHORT"
    dd = common_data(c15, c5, c1, c1h, c4h)
    if not dd or not all([dd["a5"], dd["vw15"], dd["rs5"], dd["ema21_5"]]): return None
    dd.update({"c5": c5, "regime": market_regime(c15, c1h, btc)})
    price, last, prev = dd["price"], dd["last5"], dd["prev5"]
    if direction == "LONG":
        if dd["regime"] in ["CHOP", "TREND_DOWN", "EXPANSION_UP"]: return None
        level = nearest_below(price, find_resistance_levels(c15), 4.5)
        if not level: return None
        had_below = any(c["close"] < level * 0.999 for c in c15[-12:-2])
        now_above = c15[-1]["close"] > level * 1.001 or c15[-2]["close"] > level * 1.001
        touched = last["low"] <= level * 1.010
        held = last["close"] > level * 0.999
        confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998
        if not (had_below and now_above and touched and held and confirm): return None
        if dd["rs5"] and dd["rs5"] > 78: return None
        score = 61 + (8 if dd["trend1h"] in ["BULLISH", "SOFT_BULLISH"] else 0) + (4 if dd["trend4h"] in ["BULLISH", "SOFT_BULLISH"] else 0)
        score += (6 if dd["vr5"] >= 0.90 else 0) + (5 if dd["vr5"] >= 1.15 else 0) + (4 if touched else 0) + (3 if price > dd["vw15"] else 0)
        sl = min(level - dd["a5"] * 0.18, min(c["low"] for c in c5[-10:]) - dd["a5"] * 0.05)
        reason = "Пробой сопротивления → ретест сверху → удержание → продолжение LONG."
    else:
        if dd["regime"] in ["CHOP", "TREND_UP", "EXPANSION_DOWN"]: return None
        level = nearest_above(price, find_support_levels(c15), 4.5)
        if not level: return None
        had_above = any(c["close"] > level * 1.001 for c in c15[-12:-2])
        now_below = c15[-1]["close"] < level * 0.999 or c15[-2]["close"] < level * 0.999
        touched = last["high"] >= level * 0.990
        held = last["close"] < level * 1.001
        confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002
        if not (had_above and now_below and touched and held and confirm): return None
        if dd["rs5"] and dd["rs5"] < 22: return None
        score = 61 + (8 if dd["trend1h"] in ["BEARISH", "SOFT_BEARISH"] else 0) + (4 if dd["trend4h"] in ["BEARISH", "SOFT_BEARISH"] else 0)
        score += (6 if dd["vr5"] >= 0.90 else 0) + (5 if dd["vr5"] >= 1.15 else 0) + (4 if touched else 0) + (3 if price < dd["vw15"] else 0)
        sl = max(level + dd["a5"] * 0.18, max(c["high"] for c in c5[-10:]) + dd["a5"] * 0.05)
        reason = "Пробой поддержки → ретест снизу → удержание → продолжение SHORT."
    meta = base_meta(symbol, direction, btc, dd, strategy, score, False)
    if not meta: return None
    score += meta.get("score_adjustment", 0)
    return classify_and_build(symbol, direction, strategy, price, sl, score, dd["vr5"], reason, deposit, risk_percent, meta, dd["a5"])


def eval_trend_pullback(symbol, direction, c15, c5, c1, c1h, c4h, btc, change24, deposit, risk_percent):
    strategy = "TREND_PULLBACK_LONG" if direction == "LONG" else "TREND_PULLBACK_SHORT"
    dd = common_data(c15, c5, c1, c1h, c4h)
    if not dd or not all([dd["a5"], dd["vw15"], dd["vr5"], dd["rs5"], dd["ema21_5"], dd["ema50_15"]]): return None
    dd.update({"c5": c5, "regime": market_regime(c15, c1h, btc)})
    price, last, prev = dd["price"], dd["last5"], dd["prev5"]
    near_zone = abs(price - dd["ema21_5"]) / dd["ema21_5"] * 100 <= 1.7 or abs(price - dd["vw15"]) / dd["vw15"] * 100 <= 2.2
    if not near_zone: return None
    if direction == "LONG":
        if dd["regime"] not in ["TREND_UP", "RANGE"]: return None
        if dd["trend1h"] == "BEARISH": return None
        confirm = last["close"] > last["open"] and last["close"] >= prev["close"] * 0.998 and price > dd["ema50_15"] * 0.982
        if not confirm or (dd["rs5"] and dd["rs5"] > 76): return None
        score = 58 + (9 if dd["trend1h"] in ["BULLISH", "SOFT_BULLISH"] else 0) + (4 if dd["trend4h"] in ["BULLISH", "SOFT_BULLISH"] else 0)
        score += (6 if dd["vr5"] >= 0.85 else 0) + (5 if dd["vr5"] >= 1.12 else 0) + (3 if price > dd["vw15"] else 0)
        sl = min(min(c["low"] for c in c5[-12:]) - dd["a5"] * 0.08, price - dd["a5"] * 0.45)
        reason = "Trend pullback LONG: откат к EMA/VWAP по тренду → подтверждение."
    else:
        if dd["regime"] not in ["TREND_DOWN", "RANGE"]: return None
        if dd["trend1h"] == "BULLISH": return None
        confirm = last["close"] < last["open"] and last["close"] <= prev["close"] * 1.002 and price < dd["ema50_15"] * 1.018
        if not confirm or (dd["rs5"] and dd["rs5"] < 24): return None
        score = 58 + (9 if dd["trend1h"] in ["BEARISH", "SOFT_BEARISH"] else 0) + (4 if dd["trend4h"] in ["BEARISH", "SOFT_BEARISH"] else 0)
        score += (6 if dd["vr5"] >= 0.85 else 0) + (5 if dd["vr5"] >= 1.12 else 0) + (3 if price < dd["vw15"] else 0)
        sl = max(max(c["high"] for c in c5[-12:]) + dd["a5"] * 0.08, price + dd["a5"] * 0.45)
        reason = "Trend pullback SHORT: откат к EMA/VWAP по тренду → подтверждение."
    meta = base_meta(symbol, direction, btc, dd, strategy, score, False)
    if not meta: return None
    score += meta.get("score_adjustment", 0)
    return classify_and_build(symbol, direction, strategy, price, sl, score, dd["vr5"], reason, deposit, risk_percent, meta, dd["a5"])


def eval_impulse_pullback(symbol, direction, c15, c5, c1, c1h, c4h, btc, change24, deposit, risk_percent):
    strategy = "IMPULSE_PULLBACK_PRO"
    dd = common_data(c15, c5, c1, c1h, c4h)
    if not dd or not all([dd["a5"], dd["vw15"], dd["ema9_1"], dd["ema21_5"], dd["rs5"], dd["rs15"]]): return None
    dd.update({"c5": c5, "regime": market_regime(c15, c1h, btc)})
    if dd["regime"] == "CHOP": return None
    price, last, prev = dd["price"], dd["last5"], dd["prev5"]
    if abs(price - dd["vw15"]) / dd["vw15"] * 100 > 3.5: return None
    if dd["vr5"] < 0.85: return None
    old = c5[-15]["close"]
    score = 56
    if direction == "LONG":
        if dd["trend1h"] == "BEARISH": return None
        high = max(c["high"] for c in c5[-12:-3])
        impulse = (high - old) / old * 100 if old > 0 else 0
        pullback_low = min(c["low"] for c in c5[-7:-1])
        pullback = (high - pullback_low) / high * 100 if high > 0 else 0
        confirm = last["close"] > last["open"] and c1[-1]["close"] > dd["ema9_1"] * 0.998 and price > dd["ema21_5"] * 0.99
        if impulse < 0.75 or not (0.15 <= pullback <= 3.8) or not confirm: return None
        if dd["rs5"] > 79 or dd["rs15"] > 76: return None
        sl = min(pullback_low - dd["a5"] * 0.12, min(c["low"] for c in c5[-10:]) - dd["a5"] * 0.04)
        reason = "Impulse pullback LONG: импульс → откат → продолжение."
        score += 6 if dd["trend1h"] in ["BULLISH", "SOFT_BULLISH"] else 0
    else:
        if dd["trend1h"] == "BULLISH": return None
        low = min(c["low"] for c in c5[-12:-3])
        impulse = (old - low) / old * 100 if old > 0 else 0
        pullback_high = max(c["high"] for c in c5[-7:-1])
        pullback = (pullback_high - low) / low * 100 if low > 0 else 0
        confirm = last["close"] < last["open"] and c1[-1]["close"] < dd["ema9_1"] * 1.002 and price < dd["ema21_5"] * 1.01
        if impulse < 0.75 or not (0.15 <= pullback <= 3.8) or not confirm: return None
        if dd["rs5"] < 21 or dd["rs15"] < 24: return None
        sl = max(pullback_high + dd["a5"] * 0.12, max(c["high"] for c in c5[-10:]) + dd["a5"] * 0.04)
        reason = "Impulse pullback SHORT: импульс → откат → продолжение."
        score += 6 if dd["trend1h"] in ["BEARISH", "SOFT_BEARISH"] else 0
    score += (5 if dd["vr5"] >= 0.9 else 0) + (5 if dd["vr5"] >= 1.12 else 0)
    meta = base_meta(symbol, direction, btc, dd, strategy, score, False)
    if not meta: return None
    meta["force_grade"] = "B"
    meta["risk_multiplier_override"] = min(meta.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.20)
    score += meta.get("score_adjustment", 0)
    return classify_and_build(symbol, direction, strategy, price, sl, score, dd["vr5"], reason, deposit, risk_percent, meta, dd["a5"])


def eval_extreme_mover(symbol, direction, c15, c5, c1, c1h, c4h, btc, change24, deposit, risk_percent):
    if not EXTREME_MOVER_ENABLED:
        return None
    strategy = "EXTREME_MOVER_PULLBACK_PRO"
    base = base_from_symbol(symbol)
    is_extreme_symbol = base in EXTREME_BASES or abs(change24) >= EXTREME_MIN_24H_MOVE_PERCENT
    if not is_extreme_symbol:
        return None
    dd = common_data(c15, c5, c1, c1h, c4h)
    if not dd or not all([dd["a5"], dd["vw15"], dd["ema21_5"], dd["vr5"], dd["rs5"]]): return None
    dd.update({"c5": c5, "regime": market_regime(c15, c1h, btc)})
    price, last = dd["price"], dd["last5"]
    # Detect 6h move from 15m candles (24 candles = 6h).
    m6 = move_percent(c15, 24)
    if abs(change24) < EXTREME_MIN_24H_MOVE_PERCENT and abs(m6) < EXTREME_MIN_6H_MOVE_PERCENT:
        return None
    score = 64
    if direction == "LONG":
        # LONG after strong up move pullback or recovery after capitulation reclaim.
        if change24 > 0 or m6 > 0:
            high = max(c["high"] for c in c15[-32:])
            pullback_low = min(c["low"] for c in c5[-18:])
            pullback = (high - pullback_low) / high * 100 if high > 0 else 0
            confirm = price > dd["ema21_5"] * 0.99 and price > dd["vw15"] * 0.985 and last["close"] > last["open"]
            if not (EXTREME_PULLBACK_MIN_PERCENT <= pullback <= EXTREME_PULLBACK_MAX_PERCENT and confirm): return None
            reason = f"Extreme mover LONG: 24h {change24:.1f}% / 6h {m6:.1f}% → pullback {pullback:.1f}% → удержание EMA/VWAP."
            sl = min(pullback_low - dd["a5"] * 0.25, price - dd["a5"] * 0.9)
        else:
            low = min(c["low"] for c in c15[-32:])
            reclaim = price > dd["vw15"] and price > dd["ema21_5"] * 0.995 and last["close"] > last["open"] and dd["vr5"] >= 1.05
            if not reclaim or dd["rs5"] > 70: return None
            reason = f"Extreme recovery LONG: 24h {change24:.1f}% / 6h {m6:.1f}% → reclaim VWAP после сильного падения."
            sl = min(low - dd["a5"] * 0.20, price - dd["a5"] * 0.9)
    else:
        # SHORT after strong down move pullback continuation OR failed pump reversal.
        if change24 < 0 or m6 < 0:
            low = min(c["low"] for c in c15[-32:])
            pullback_high = max(c["high"] for c in c5[-18:])
            pullback = (pullback_high - low) / low * 100 if low > 0 else 0
            lower_high = max(c["high"] for c in c5[-5:]) < pullback_high * 1.0005
            confirm = price < dd["ema21_5"] * 1.01 and price < dd["vw15"] * 1.015 and last["close"] < last["open"] and lower_high
            if not (EXTREME_PULLBACK_MIN_PERCENT <= pullback <= EXTREME_PULLBACK_MAX_PERCENT and confirm): return None
            reason = f"Extreme mover SHORT: 24h {change24:.1f}% / 6h {m6:.1f}% → pullback {pullback:.1f}% → lower high и продолжение вниз."
            sl = max(pullback_high + dd["a5"] * 0.25, price + dd["a5"] * 0.9)
        else:
            high = max(c["high"] for c in c15[-32:])
            lower_high = max(c["high"] for c in c5[-8:]) < high * 0.995
            lost = price < dd["vw15"] * 0.995 and price < dd["ema21_5"] * 1.005 and last["close"] < last["open"] and lower_high
            if not lost or dd["rs5"] < 30: return None
            reason = f"Extreme failed pump SHORT: 24h {change24:.1f}% / 6h {m6:.1f}% → потеря VWAP/EMA после пампа."
            sl = max(high + dd["a5"] * 0.20, price + dd["a5"] * 0.9)
    if dd["vr5"] < 0.95: return None
    score += 7 if dd["vr5"] >= 1.1 else 0
    score += 6 if abs(change24) >= 18 else 0
    score += 4 if abs(m6) >= 6 else 0
    meta = base_meta(symbol, direction, btc, dd, strategy, score, True)
    if not meta: return None
    # Extreme B is allowed only with tiny risk; A+ remains strict.
    if score < A_PLUS_MIN_SCORE:
        meta["force_grade"] = "B"
        meta["risk_multiplier_override"] = EXTREME_B_RISK_MULTIPLIER
    else:
        meta["risk_multiplier_override"] = EXTREME_A_PLUS_RISK_MULTIPLIER
    meta["extreme_note"] = "Extreme mover: риск сильно уменьшен; вход только после отката/reclaim, не chase."
    score += meta.get("score_adjustment", 0)
    return classify_and_build(symbol, direction, strategy, price, sl, score, dd["vr5"], reason, deposit, risk_percent, meta, dd["a5"])

# =====================
# ANALYSIS / SCAN
# =====================
def analyze_symbol(symbol: str, direction: Optional[str], deposit: float, risk_percent: float, btc: Optional[dict] = None, change24: float = 0.0) -> Optional[dict]:
    symbol = normalize_symbol(symbol)
    if is_symbol_blocked(symbol) or is_on_cooldown(symbol):
        return None
    c15 = remove_unclosed(get_klines(symbol, "15m", 260), "15m")
    c5 = remove_unclosed(get_klines(symbol, "5m", 180), "5m")
    c1 = remove_unclosed(get_klines(symbol, "1m", 120), "1m")
    c1h = remove_unclosed(get_klines(symbol, "1h", 260), "1h")
    c4h = remove_unclosed(get_klines(symbol, "4h", 260), "4h")
    if not all([c15, c5, c1, c1h, c4h]):
        return None
    btc = btc or get_btc_context()
    directions = [normalize_direction(direction)] if normalize_direction(direction) else ["LONG", "SHORT"]
    candidates = []
    for d in directions:
        funcs = [
            eval_sweep_reclaim_long if d == "LONG" else eval_sweep_reject_short,
            lambda sym, C15, C5, C1, C1h, C4h, B, Ch, Dep, Risk, d=d: eval_break_retest(sym, d, C15, C5, C1, C1h, C4h, B, Ch, Dep, Risk),
            lambda sym, C15, C5, C1, C1h, C4h, B, Ch, Dep, Risk, d=d: eval_trend_pullback(sym, d, C15, C5, C1, C1h, C4h, B, Ch, Dep, Risk),
            lambda sym, C15, C5, C1, C1h, C4h, B, Ch, Dep, Risk, d=d: eval_impulse_pullback(sym, d, C15, C5, C1, C1h, C4h, B, Ch, Dep, Risk),
            lambda sym, C15, C5, C1, C1h, C4h, B, Ch, Dep, Risk, d=d: eval_extreme_mover(sym, d, C15, C5, C1, C1h, C4h, B, Ch, Dep, Risk),
        ]
        for f in funcs:
            try:
                sig = f(symbol, c15, c5, c1, c1h, c4h, btc, change24, deposit, risk_percent)
                if sig:
                    candidates.append(sig)
            except Exception:
                continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: (
        1 if x["grade"] == "A+" else 0,
        {"HIGH": 3, "MEDIUM": 2, "NEW": 1, "LOW": 0, "BAD": -2}.get(x.get("trust", "NEW"), 0),
        x["score"], x["rr"], x["volume_ratio"]
    ), reverse=True)
    return candidates[0]


def cleanup_state():
    cur = now_ts()
    for k, ts in list(STATE.get("sent_signals", {}).items()):
        if cur - ts > 604800:
            STATE["sent_signals"].pop(k, None)
    for k, ts in list(STATE.get("symbol_cooldown", {}).items()):
        if cur - ts > SIGNAL_COOLDOWN_SECONDS * 3:
            STATE["symbol_cooldown"].pop(k, None)
    for k, until in list(STATE.get("blocked_symbols", {}).items()):
        if cur > until:
            STATE["blocked_symbols"].pop(k, None)
    save_state()


def scan_best_signal(deposit: float, risk_percent: float) -> dict:
    cleanup_state()
    btc = get_btc_context()
    symbols, change_map, dynamic_preview = get_symbols()
    checked = 0
    found = 0
    errors = 0
    best = None
    almost = []
    for sym in symbols:
        checked += 1
        try:
            sig = analyze_symbol(sym, None, deposit, risk_percent, btc=btc, change24=change_map.get(sym, 0.0))
            if not sig:
                continue
            found += 1
            if len(almost) < 5:
                almost.append(f"{display_symbol(sym)} {sig['grade']} {sig['direction']} {sig['strategy']} score {sig['score']}")
            if not best:
                best = sig
            else:
                key = (1 if sig["grade"] == "A+" else 0, {"HIGH": 3, "MEDIUM": 2, "NEW": 1, "LOW": 0}.get(sig.get("trust", "NEW"), 0), sig["score"], sig["rr"])
                bkey = (1 if best["grade"] == "A+" else 0, {"HIGH": 3, "MEDIUM": 2, "NEW": 1, "LOW": 0}.get(best.get("trust", "NEW"), 0), best["score"], best["rr"])
                if key > bkey:
                    best = sig
        except Exception:
            errors += 1
            continue
    if not best:
        return {"ok": False, "checked": checked, "found_candidates": found, "errors": errors, "btc": btc, "dynamic_preview": dynamic_preview,
                "message": "Сигнала нет. Бот сканирует, но не нашёл подтверждённый сетап после BTC/anti-chase/trust фильтров."}
    return {"ok": True, "checked": checked, "found_candidates": found, "errors": errors, "btc": btc, "dynamic_preview": dynamic_preview, "signal": best, "message": build_message(best)}

# =====================
# MESSAGE / TRACKING
# =====================
def strategy_display(strategy: str) -> str:
    return {
        "SWEEP_RECLAIM_LONG": "🟢 Sweep Reclaim LONG",
        "SWEEP_REJECT_SHORT": "🔴 Sweep Reject SHORT",
        "BREAK_RETEST_LONG": "📈 Break Retest LONG",
        "BREAK_RETEST_SHORT": "📉 Break Retest SHORT",
        "TREND_PULLBACK_LONG": "📌 Trend Pullback LONG",
        "TREND_PULLBACK_SHORT": "📌 Trend Pullback SHORT",
        "IMPULSE_PULLBACK_PRO": "⚡ Impulse Pullback Pro",
        "EXTREME_MOVER_PULLBACK_PRO": "🚀 Extreme Mover Pullback Pro",
    }.get(strategy, strategy)


def build_message(signal: dict) -> str:
    arrow = "📈" if signal["direction"] == "LONG" else "📉"
    mode = "TEST" if TEST_MODE else "TRADE"
    pos = signal.get("position", {})
    risk_text = f"Риск: {signal['risk_percent']:.3f}% депозита\nРазмер позиции: {pos.get('position_size_usdt')} USDT\nМаржа x{LEVERAGE}: {pos.get('margin_usdt')} USDT" if not pos.get("error") else f"⚠️ RM: {pos.get('error')}"
    filters = signal.get("filters", {})
    notes = []
    for key in ["btc_note", "btc_filter_note", "anti_chase_note", "extreme_note"]:
        if filters.get(key): notes.append(filters[key])
    caution = "\n⚠️ B-сигнал: риск уменьшен." if signal["grade"] == "B" else ""
    return f"""
🎯 <b>{mode} {'A+ SIGNAL' if signal['grade'] == 'A+' else 'B SIGNAL'}</b>

{arrow} <b>{signal['direction']} {signal['display_symbol']}</b>
<b>Стратегия:</b> {strategy_display(signal['strategy'])}
<b>Trust:</b> {signal.get('trust', 'NEW')}

<b>Вход:</b> <code>{signal['entry']}</code>
<b>Stop Loss:</b> <code>{signal['sl']}</code>

<b>Take Profit:</b>
TP1: <code>{signal['tp1']}</code>
TP2: <code>{signal['tp2']}</code>
TP3: <code>{signal['tp3']}</code>

<b>Почему вход:</b>
{signal['reason']}

<b>Фильтры:</b>
{chr(10).join(notes)}

<b>Качество:</b> {signal['score']}/100
<b>RR:</b> {signal['rr']} ({signal.get('rr_basis', 'TP2 net')})
<b>Объём:</b> x{signal['volume_ratio']}
<b>Риск до SL:</b> {signal['risk_position_percent']}% по позиции

{risk_text}
{caution}

<b>После TP1:</b> закрыть ~{TP1_CLOSE_PERCENT:.0f}% и ставить защитный SL с buffer {TP1_BE_BUFFER_PERCENT}%.
⚠️ Не финансовый совет.
""".strip()


def save_signal(signal: dict):
    STATE["active_signals"][signal["id"]] = signal
    STATE["sent_signals"][signal["id"]] = now_ts()
    STATE["symbol_cooldown"][signal["symbol"]] = now_ts()
    save_state()


def check_signal_hit(signal: dict, candles: List[dict]):
    side = signal["direction"]
    last_checked = signal.get("last_checked_time", 0)
    new = [c for c in candles if c["time"] > last_checked]
    if not new:
        return None, candles[-1]["close"]
    created = signal.get("created_at", 0)
    soft_stop_active = signal.get("grade") == "A+" and now_ts() - created <= A_PLUS_SOFT_STOP_SECONDS
    for c in new:
        signal["last_checked_time"] = c["time"]
        high, low, close = c["high"], c["low"], c["close"]
        if side == "LONG":
            if signal.get("tp2_hit") and low <= signal["entry"] * (1 - TP1_BE_BUFFER_PERCENT / 100): return "PROFIT_AFTER_TP2", signal["entry"]
            if signal.get("tp1_hit") and low <= signal["entry"] * (1 - TP1_BE_BUFFER_PERCENT / 100): return "PROFIT_AFTER_TP1", signal["entry"]
            if not signal.get("tp1_hit") and low <= signal["sl"]:
                if soft_stop_active and close > signal["sl"]: continue
                return "SL", signal["sl"]
            if not signal.get("tp1_hit") and high >= signal["tp1"]: signal["tp1_hit"] = True; return "TP1", signal["tp1"]
            if signal.get("tp1_hit") and not signal.get("tp2_hit") and high >= signal["tp2"]: signal["tp2_hit"] = True; return "TP2", signal["tp2"]
            if signal.get("tp2_hit") and not signal.get("tp3_hit") and high >= signal["tp3"]: signal["tp3_hit"] = True; return "TP3", signal["tp3"]
        else:
            if signal.get("tp2_hit") and high >= signal["entry"] * (1 + TP1_BE_BUFFER_PERCENT / 100): return "PROFIT_AFTER_TP2", signal["entry"]
            if signal.get("tp1_hit") and high >= signal["entry"] * (1 + TP1_BE_BUFFER_PERCENT / 100): return "PROFIT_AFTER_TP1", signal["entry"]
            if not signal.get("tp1_hit") and high >= signal["sl"]:
                if soft_stop_active and close < signal["sl"]: continue
                return "SL", signal["sl"]
            if not signal.get("tp1_hit") and low <= signal["tp1"]: signal["tp1_hit"] = True; return "TP1", signal["tp1"]
            if signal.get("tp1_hit") and not signal.get("tp2_hit") and low <= signal["tp2"]: signal["tp2_hit"] = True; return "TP2", signal["tp2"]
            if signal.get("tp2_hit") and not signal.get("tp3_hit") and low <= signal["tp3"]: signal["tp3_hit"] = True; return "TP3", signal["tp3"]
    return None, new[-1]["close"]


def apply_result(signal: dict, result: str) -> List[str]:
    ensure_state_structure(STATE)
    side, strategy, grade, symbol = signal["direction"], signal["strategy"], signal.get("grade", "A+"), normalize_symbol(signal["symbol"])
    ss, ssg = f"{strategy}:{side}", f"{strategy}:{side}:{grade}"
    notes = []
    r_mult = 0.0
    if result == "SL" and not signal.get("counted_sl"):
        signal["counted_sl"] = True
        for sec, key in [("side", side), ("strategy", strategy), ("strategy_side", ss), ("strategy_side_grade", ssg)]:
            STATE["stats"][sec][key]["sl"] += 1
            if "consecutive_sl" in STATE["stats"][sec][key]: STATE["stats"][sec][key]["consecutive_sl"] += 1
        STATE["stats"]["grade"][grade]["sl"] += 1
        STATE["stats"]["pair_sl"][symbol] = STATE["stats"]["pair_sl"].get(symbol, 0) + 1
        r_mult = -1.0
        if STATE["stats"]["pair_sl"][symbol] >= PAIR_MAX_SL:
            STATE["blocked_symbols"][symbol] = now_ts() + PAIR_BLOCK_SECONDS
            notes.append(f"🚫 {display_symbol(symbol)} временно заблокирован после серии SL.")
        if STATE["stats"]["strategy_side_grade"][ssg]["consecutive_sl"] >= STRATEGY_SIDE_MAX_CONSECUTIVE_SL:
            STATE["strategy_side_grade_disabled_until"][ssg] = now_ts() + STRATEGY_SIDE_DISABLE_SECONDS
            STATE["stats"]["strategy_side_grade"][ssg]["consecutive_sl"] = 0
            notes.append(f"⛔ {grade} {strategy} {side} временно отключён после серии SL.")
    elif result in ["TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        for sec, key in [("side", side), ("strategy", strategy), ("strategy_side", ss), ("strategy_side_grade", ssg)]:
            if "consecutive_sl" in STATE["stats"][sec][key]: STATE["stats"][sec][key]["consecutive_sl"] = 0
        if not signal.get("counted_positive"):
            signal["counted_positive"] = True
            for sec, key in [("side", side), ("strategy", strategy), ("strategy_side", ss), ("strategy_side_grade", ssg)]:
                STATE["stats"][sec][key]["positive"] += 1
            STATE["stats"]["grade"][grade]["positive"] += 1
            STATE["stats"]["pair_positive"][symbol] = STATE["stats"]["pair_positive"].get(symbol, 0) + 1
        r_mult = {"TP1": 0.35, "PROFIT_AFTER_TP1": 0.35, "TP2": 0.75, "PROFIT_AFTER_TP2": 0.75, "TP3": 1.25}.get(result, 0.35)
        if result == "TP1" and not signal.get("counted_tp1"):
            signal["counted_tp1"] = True; STATE["stats"]["side"][side]["tp1"] += 1
        if result == "TP2" and not signal.get("counted_tp2"):
            signal["counted_tp2"] = True; STATE["stats"]["side"][side]["tp2"] += 1
        if result == "TP3" and not signal.get("counted_tp3"):
            signal["counted_tp3"] = True; STATE["stats"]["side"][side]["tp3"] += 1
    if result in ["SL", "TP1", "TP2", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
        STATE["stats"]["closed_trades"].append({"time": int(now_ts()), "symbol": symbol, "strategy": strategy, "side": side, "grade": grade, "result": result, "r_multiple": r_mult})
        STATE["stats"]["closed_trades"] = STATE["stats"]["closed_trades"][-500:]
    save_state()
    return notes


def build_result_message(signal: dict, result: str, price: Optional[float], notes: List[str]) -> str:
    titles = {"SL": "❌ Stop Loss", "TP1": "✅ TP1 достигнут", "TP2": "✅ TP2 достигнут", "TP3": "🔥 TP3 достигнут", "PROFIT_AFTER_TP1": "🟢 Возврат после TP1", "PROFIT_AFTER_TP2": "🟢 Возврат после TP2", "EXPIRED": "⌛ Сигнал устарел"}
    status = {
        "SL": "SL сработал до TP1. Сделка отрицательная.",
        "TP1": f"Сделка позитивная. Закрыть ~{TP1_CLOSE_PERCENT:.0f}% позиции и поставить защитный SL с buffer.",
        "TP2": "Хорошее движение. Сделка позитивная.", "TP3": "Отличная сделка. TP3 достигнут.",
        "PROFIT_AFTER_TP1": "Цена вернулась после TP1, но сделка уже позитивная.", "PROFIT_AFTER_TP2": "Цена вернулась после TP2, сделка позитивная.",
        "EXPIRED": "Сигнал устарел без TP/SL и удалён из активных.",
    }.get(result, "Обновление")
    adaptive = "\n\n<b>Адаптация бота:</b>\n" + "\n".join(notes) if notes else ""
    return f"""
{titles.get(result, result)}

<b>{signal.get('grade')} · {signal['direction']} {signal['display_symbol']}</b>
<b>Стратегия:</b> {strategy_display(signal.get('strategy', ''))}

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


def is_signal_expired(signal: dict) -> bool:
    return now_ts() - signal.get("created_at", 0) > SIGNAL_MAX_LIFETIME_SECONDS


def track_active_signals(send_to_telegram: bool = True) -> dict:
    cleanup_state()
    if not STATE.get("active_signals"):
        return {"ok": True, "message": "Активных сигналов нет.", "results": [], "active_left": 0}
    results, finished = [], []
    for sid, sig in list(STATE["active_signals"].items()):
        if is_signal_expired(sig):
            msg = build_result_message(sig, "EXPIRED", None, [])
            tg = send_telegram_message(msg) if send_to_telegram else None
            results.append({"signal_id": sid, "result": "EXPIRED", "telegram": tg})
            finished.append(sid)
            continue
        candles = remove_unclosed(get_klines(sig["symbol"], "1m", 120), "1m")
        if not candles:
            continue
        result, price = check_signal_hit(sig, candles)
        STATE["active_signals"][sid] = sig
        if not result:
            continue
        notes = apply_result(sig, result)
        msg = build_result_message(sig, result, price, notes)
        tg = send_telegram_message(msg) if send_to_telegram else None
        results.append({"signal_id": sid, "symbol": sig.get("display_symbol"), "result": result, "price": price, "telegram": tg})
        if result in ["SL", "TP3", "PROFIT_AFTER_TP1", "PROFIT_AFTER_TP2"]:
            finished.append(sid)
    for sid in finished:
        STATE["active_signals"].pop(sid, None)
    save_state()
    return {"ok": True, "results": results, "active_left": len(STATE.get("active_signals", {}))}

# =====================
# AUTO WORKER / ROUTES
# =====================
async def auto_worker():
    await asyncio.sleep(5)
    while True:
        try:
            cur = now_ts()
            if AUTO_TRACK_ENABLED and cur - STATE["auto"].get("last_track_time", 0) >= AUTO_TRACK_SECONDS:
                tr = track_active_signals(True)
                STATE["auto"]["last_track_time"] = cur
                STATE["auto"]["last_track_result"] = tr
                save_state()
            if AUTO_SCAN_ENABLED and cur - STATE["auto"].get("last_scan_time", 0) >= AUTO_SCAN_SECONDS:
                result = scan_best_signal(DEFAULT_DEPOSIT, DEFAULT_RISK_PERCENT)
                STATE["auto"]["last_scan_time"] = cur
                STATE["auto"]["last_scan_result"] = {k: v for k, v in result.items() if k not in ["signal", "message"]}
                if result.get("ok"):
                    signal = result["signal"]
                    tg = send_telegram_message(result["message"])
                    result["telegram"] = tg
                    if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or tg.get("ok") is True:
                        save_signal(signal)
                    else:
                        STATE["auto"]["last_error"] = f"Telegram failed: {tg}"
                else:
                    if DEBUG_NO_SIGNAL_REPORT_ENABLED and cur - STATE["auto"].get("last_no_signal_report_time", 0) >= DEBUG_NO_SIGNAL_REPORT_SECONDS:
                        btc = result.get("btc", {})
                        dyn = ", ".join(display_symbol(s) for s in result.get("dynamic_preview", [])[:8]) or "нет"
                        report = (
                            f"🧠 <b>Scan Watchdog {DEPLOY_MARKER}</b>\n\n"
                            f"Бот работает и сканирует рынок.\n"
                            f"Проверено пар: {result.get('checked', 0)}\n"
                            f"Кандидатов найдено: {result.get('found_candidates', 0)}\n"
                            f"Ошибок по парам: {result.get('errors', 0)}\n"
                            f"BTC: {btc.get('note', 'n/a')}\n"
                            f"Dynamic extreme preview: {dyn}\n\n"
                            f"Сигнала на отправку пока нет: BTC/anti-chase/trust/RR фильтры не дали подтверждённый вход."
                        )
                        send_telegram_message(report)
                        STATE["auto"]["last_no_signal_report_time"] = cur
                save_state()
            await asyncio.sleep(10)
        except Exception as e:
            STATE["auto"]["last_error"] = str(e) + "\n" + traceback.format_exc()[-1000:]
            save_state()
            await asyncio.sleep(30)


def is_authorized(api_key: Optional[str]) -> bool:
    return True if not API_KEY else api_key == API_KEY


@app.on_event("startup")
async def startup_event():
    STATE["version"] = DEPLOY_MARKER
    STATE["auto"]["worker_started_at"] = int(now_ts())
    save_state()
    text = (
        f"✅ {APP_NAME} запущен.\n\n"
        f"Deploy marker: {DEPLOY_MARKER}\n"
        f"Режим: {'TEST' if TEST_MODE else 'TRADE'}\n"
        f"Auto Scan: {'ON' if AUTO_SCAN_ENABLED else 'OFF'} / {AUTO_SCAN_SECONDS} сек.\n"
        f"Auto Track: {'ON' if AUTO_TRACK_ENABLED else 'OFF'} / {AUTO_TRACK_SECONDS} сек.\n"
        f"Scan Watchdog: ON / no-signal report every {DEBUG_NO_SIGNAL_REPORT_SECONDS}s\n"
        f"A+ score/RR/volume: {A_PLUS_MIN_SCORE}+ / {A_PLUS_MIN_RR} / x{A_PLUS_MIN_VOLUME_RATIO}\n"
        f"B score/RR/volume: {B_MIN_SCORE}+ / {B_MIN_RR} / x{B_MIN_VOLUME_RATIO} / risk x{B_RISK_MULTIPLIER}\n"
        f"SHORT B guard: {SHORT_B_MIN_SCORE}+ / {SHORT_B_MIN_RR} / x{SHORT_B_MIN_VOLUME_RATIO}\n"
        f"BTC Master: {'ON' if BTC_MASTER_ENABLED else 'OFF'} / fast 1m-5m-15m {BTC_FAST_1M_PERCENT}%/{BTC_FAST_5M_PERCENT}%/{BTC_FAST_15M_PERCENT}%\n"
        f"Anti-chase: {'ON' if ANTI_CHASE_ENABLED else 'OFF'} / max {MAX_CHASE_MOVE_5M_PERCENT}% / hard {HARD_CHASE_MOVE_5M_PERCENT}%\n"
        f"Extreme Mover Pro: {'ON' if EXTREME_MOVER_ENABLED else 'OFF'} / strategy EXTREME_MOVER_PULLBACK_PRO\n"
        f"Dynamic Extreme Scanner: {'ON' if DYNAMIC_EXTREME_SCANNER_ENABLED else 'OFF'} / top {DYNAMIC_EXTREME_TOP_N} / min 24h ±{DYNAMIC_EXTREME_MIN_24H_MOVE_PERCENT}%\n"
        f"Extreme B micro-risk: {'ON' if EXTREME_B_ENABLED else 'OFF'} / risk x{EXTREME_B_RISK_MULTIPLIER}\n\n"
        "V8.0 clean logic: BTC first → regime → pullback/retest/sweep → trust → risk. Бот должен сканировать стабильно и не молчать без watchdog-отчётов."
    )
    send_telegram_message(text)
    asyncio.create_task(auto_worker())


@app.get("/", response_class=HTMLResponse)
def home():
    return f"""<html><body style='background:#020617;color:#e5e7eb;font-family:Arial;padding:32px;'>
<h2>✅ {APP_NAME}</h2><pre>Deploy marker: {DEPLOY_MARKER}
/health
/version
/auto-status
/scan
/track
/stats
/test-telegram</pre></body></html>"""


@app.get("/health")
def health():
    return {"ok": True, "service": APP_NAME, "deploy_marker": DEPLOY_MARKER, "auto_scan": AUTO_SCAN_ENABLED, "auto_track": AUTO_TRACK_ENABLED, "active_signals": len(STATE.get("active_signals", {})), "last_error": STATE.get("auto", {}).get("last_error")}


@app.get("/version")
def version():
    return {"ok": True, "service": APP_NAME, "deploy_marker": DEPLOY_MARKER, "extreme_mover": EXTREME_MOVER_ENABLED, "dynamic_extreme": DYNAMIC_EXTREME_SCANNER_ENABLED, "btc_master": BTC_MASTER_ENABLED}


@app.get("/auto-status")
def auto_status():
    return {"ok": True, "service": APP_NAME, "deploy_marker": DEPLOY_MARKER, "auto": STATE.get("auto", {}), "active_signals": len(STATE.get("active_signals", {})), "blocked_symbols": len(STATE.get("blocked_symbols", {}))}


@app.get("/test-telegram")
def test_telegram():
    return send_telegram_message(f"✅ {APP_NAME} подключён к Telegram.\nDeploy marker: {DEPLOY_MARKER}")


@app.get("/scan")
def scan(send_to_telegram: bool = Query(default=False), deposit: float = Query(default=DEFAULT_DEPOSIT), risk_percent: float = Query(default=DEFAULT_RISK_PERCENT), api_key: Optional[str] = Query(default=None)):
    if send_to_telegram and not is_authorized(api_key):
        return {"ok": False, "error": "unauthorized"}
    result = scan_best_signal(deposit, risk_percent)
    if result.get("ok") and send_to_telegram:
        tg = send_telegram_message(result["message"])
        result["telegram"] = tg
        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or tg.get("ok") is True:
            save_signal(result["signal"])
    return result


@app.get("/auto-signal")
def auto_signal(symbol: str = Query(default="BTC/USDT"), direction: Optional[str] = Query(default=None), send_to_telegram: bool = Query(default=False), deposit: float = Query(default=DEFAULT_DEPOSIT), risk_percent: float = Query(default=DEFAULT_RISK_PERCENT), api_key: Optional[str] = Query(default=None)):
    if send_to_telegram and not is_authorized(api_key):
        return {"ok": False, "error": "unauthorized"}
    btc = get_btc_context()
    change_map = parse_ticker_24h()
    sig = analyze_symbol(symbol, direction, deposit, risk_percent, btc=btc, change24=change_map.get(normalize_symbol(symbol), 0.0))
    if not sig:
        return {"ok": False, "symbol": display_symbol(symbol), "direction": direction, "btc": btc, "message": "Сигнала нет."}
    msg = build_message(sig)
    tg = None
    if send_to_telegram:
        tg = send_telegram_message(msg)
        if not SAVE_SIGNAL_ONLY_IF_TELEGRAM_OK or tg.get("ok") is True:
            save_signal(sig)
    return {"ok": True, "signal": sig, "message": msg, "telegram": tg}


@app.get("/track")
def track(send_to_telegram: bool = Query(default=True), api_key: Optional[str] = Query(default=None)):
    if send_to_telegram and not is_authorized(api_key):
        return {"ok": False, "error": "unauthorized"}
    return track_active_signals(send_to_telegram)


@app.get("/stats")
def stats():
    ensure_state_structure(STATE)
    return {"ok": True, "deploy_marker": DEPLOY_MARKER, "stats": STATE.get("stats"), "stats_text": build_stats_text(), "active_signals": len(STATE.get("active_signals", {})), "blocked_symbols": STATE.get("blocked_symbols", {})}


@app.get("/reset-state")
def reset_state(api_key: Optional[str] = Query(default=None)):
    if not is_authorized(api_key):
        return {"ok": False, "error": "unauthorized"}
    global STATE
    STATE = default_state()
    save_state()
    return {"ok": True, "message": "State reset completed", "deploy_marker": DEPLOY_MARKER}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
