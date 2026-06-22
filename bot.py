import os
import time
import json
import math
import random
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

# ============================================================
# V13.11 — Professional Ladder Scalp + Level Trader
# Goal: catch clean discretionary-style scalps like BLESS/BEAT:
# active coin -> breakout/retest or pullback/reclaim -> ladder targets, while keeping verified level logic.
# ============================================================

APP_NAME = "Professional Adaptive Futures Bot AUTO V13.11 PROFESSIONAL LADDER SCALP LEVEL TRADER"
DEPLOY_MARKER = "V13_11_PRO_LADDER_SCALP_LEVEL_TRADER_2026_06_21"

app = FastAPI(title=APP_NAME)

BINGX_BASE_URL = "https://open-api.bingx.com"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

STATE_FILE = os.getenv("STATE_FILE", "bot_state_v13_11.json")
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"

# --- Scan stability ---
AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "60"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "20"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
API_RETRIES = int(os.getenv("API_RETRIES", "3"))
API_THROTTLE_SECONDS = float(os.getenv("API_THROTTLE_SECONDS", "0.10"))
MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "450"))
MAX_ANALYZE_SYMBOLS = int(os.getenv("MAX_ANALYZE_SYMBOLS", "220"))
DIAG_SECONDS = int(os.getenv("DIAG_SECONDS", "1800"))

# --- Signal quality ---
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "90"))
B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "86"))
MIN_TP1_ROI_X10 = float(os.getenv("MIN_TP1_ROI_X10", "10.0"))
MIN_TP1_PRICE_MOVE = MIN_TP1_ROI_X10 / max(LEVERAGE, 1) / 100.0  # 10% ROI x10 ~= 1% price
MIN_RR_A = float(os.getenv("MIN_RR_A", "1.05"))
MIN_RR_B = float(os.getenv("MIN_RR_B", "0.95"))
MAX_ACTIVE_SIGNALS = int(os.getenv("MAX_ACTIVE_SIGNALS", "8"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "6"))
# V13.8: do not flood, but do not silence good momentum-pullback opportunities
MAX_SIGNALS_PER_DAY = int(os.getenv("MAX_SIGNALS_PER_DAY", "0"))  # 0 = no hard daily cap
PAIR_COOLDOWN_SECONDS = int(os.getenv("PAIR_COOLDOWN_SECONDS", "900"))
STRATEGY_COOLDOWN_SECONDS = int(os.getenv("STRATEGY_COOLDOWN_SECONDS", "600"))

# --- Level parameters ---
LEVEL_CLUSTER_TOL = float(os.getenv("LEVEL_CLUSTER_TOL", "0.0065"))  # 0.65%
LEVEL_NEAR_TOL = float(os.getenv("LEVEL_NEAR_TOL", "0.0125"))      # 1.25%
RETEST_TOL = float(os.getenv("RETEST_TOL", "0.016"))               # 1.6%
MIN_LEVEL_TOUCHES_A = int(os.getenv("MIN_LEVEL_TOUCHES_A", "3"))
MIN_LEVEL_TOUCHES_B = int(os.getenv("MIN_LEVEL_TOUCHES_B", "2"))
MIN_REACTIONS_A = int(os.getenv("MIN_REACTIONS_A", "2"))
MIN_REACTIONS_B = int(os.getenv("MIN_REACTIONS_B", "1"))
MAX_TOUCHES_EFFECTIVE = int(os.getenv("MAX_TOUCHES_EFFECTIVE", "7"))

# --- Volume / confirmation ---
MIN_VOLUME_A = float(os.getenv("MIN_VOLUME_A", "1.10"))
MIN_VOLUME_B = float(os.getenv("MIN_VOLUME_B", "0.95"))
LOW_VOLUME_CAP = int(os.getenv("LOW_VOLUME_CAP", "76"))
VERY_LOW_VOLUME = float(os.getenv("VERY_LOW_VOLUME", "0.60"))
REQUIRE_15M_CONFIRM = os.getenv("REQUIRE_15M_CONFIRM", "true").lower() == "true"

# --- V13.9 professional statistical quality gates ---
ADAPTIVE_BLOCK_ENABLED = os.getenv("ADAPTIVE_BLOCK_ENABLED", "true").lower() == "true"
ADAPTIVE_MIN_TRADES = int(os.getenv("ADAPTIVE_MIN_TRADES", "3"))
ADAPTIVE_MIN_WR = float(os.getenv("ADAPTIVE_MIN_WR", "45"))
BAD_STRATEGY_BLOCK_HOURS = int(os.getenv("BAD_STRATEGY_BLOCK_HOURS", "24"))
LONG_NEEDS_HTF_NOT_DOWN = os.getenv("LONG_NEEDS_HTF_NOT_DOWN", "true").lower() == "true"
LONG_MIN_VOLUME = float(os.getenv("LONG_MIN_VOLUME", "0.95"))
LONG_MIN_RR = float(os.getenv("LONG_MIN_RR", "0.95"))
CALM_BREAKOUT_LONG_A_ONLY = os.getenv("CALM_BREAKOUT_LONG_A_ONLY", "true").lower() == "true"
CALM_BREAKDOWN_SHORT_A_ONLY = os.getenv("CALM_BREAKDOWN_SHORT_A_ONLY", "true").lower() == "true"
CALM_LONG_MIN_VOLUME = float(os.getenv("CALM_LONG_MIN_VOLUME", "1.20"))
CALM_LONG_MIN_RR = float(os.getenv("CALM_LONG_MIN_RR", "1.10"))
CALM_SHORT_MIN_VOLUME = float(os.getenv("CALM_SHORT_MIN_VOLUME", "1.15"))
CALM_SHORT_MIN_RR = float(os.getenv("CALM_SHORT_MIN_RR", "1.05"))
GLOBAL_B_MIN_TRADES = int(os.getenv("GLOBAL_B_MIN_TRADES", "5"))
GLOBAL_B_MIN_WR = float(os.getenv("GLOBAL_B_MIN_WR", "48"))
WEAK_TYPE_MIN_WR = float(os.getenv("WEAK_TYPE_MIN_WR", "48"))

# --- Anti-chase ---
BIG_MOVE_6H = float(os.getenv("BIG_MOVE_6H", "0.060"))       # 6%
BIG_MOVE_24H = float(os.getenv("BIG_MOVE_24H", "0.120"))     # 12%
REQUIRED_PULLBACK_AFTER_BIG_MOVE = float(os.getenv("REQUIRED_PULLBACK_AFTER_BIG_MOVE", "0.012"))
ULTRA_RISK_5M_CANDLE = float(os.getenv("ULTRA_RISK_5M_CANDLE", "0.060"))  # 6% candle
ULTRA_RISK_15M_CANDLE = float(os.getenv("ULTRA_RISK_15M_CANDLE", "0.090"))

# --- Risk multipliers ---
A_RISK_MULT = float(os.getenv("A_RISK_MULT", "0.50"))
B_RISK_MULT = float(os.getenv("B_RISK_MULT", "0.08"))
FAR_SL_RISK_MULT = float(os.getenv("FAR_SL_RISK_MULT", "0.08"))
TP1_CLOSE_PERCENT = float(os.getenv("TP1_CLOSE_PERCENT", "70"))

# --- V13.11 Professional ladder scalp mode ---
# This mode is for trades like BLESS: active but not ultra-risk asset, micro pullback/reclaim,
# tight technical invalidation and 5 ladder targets.
LADDER_SCALP_ENABLED = os.getenv("LADDER_SCALP_ENABLED", "true").lower() == "true"
LADDER_MIN_VOLUME = float(os.getenv("LADDER_MIN_VOLUME", "0.85"))
LADDER_A_VOLUME = float(os.getenv("LADDER_A_VOLUME", "1.05"))
LADDER_MIN_RR = float(os.getenv("LADDER_MIN_RR", "0.90"))
LADDER_MIN_1H_MOVE = float(os.getenv("LADDER_MIN_1H_MOVE", "0.004"))      # 0.4% in ~1h
LADDER_MAX_1H_MOVE = float(os.getenv("LADDER_MAX_1H_MOVE", "0.045"))      # avoid vertical chase
LADDER_PULLBACK_MIN = float(os.getenv("LADDER_PULLBACK_MIN", "0.0025"))   # 0.25%
LADDER_PULLBACK_MAX = float(os.getenv("LADDER_PULLBACK_MAX", "0.022"))    # 2.2%
LADDER_SL_ATR_MULT = float(os.getenv("LADDER_SL_ATR_MULT", "0.55"))
LADDER_TP1_MOVE = float(os.getenv("LADDER_TP1_MOVE", "0.010"))            # 10% ROI at x10
LADDER_TP2_MOVE = float(os.getenv("LADDER_TP2_MOVE", "0.016"))
LADDER_TP3_MOVE = float(os.getenv("LADDER_TP3_MOVE", "0.023"))
LADDER_TP4_MOVE = float(os.getenv("LADDER_TP4_MOVE", "0.032"))
LADDER_TP5_MOVE = float(os.getenv("LADDER_TP5_MOVE", "0.042"))
LADDER_RISK_MULT = float(os.getenv("LADDER_RISK_MULT", "0.18"))
SCALP_STRATEGIES = {"PRO_LADDER_SCALP_LONG", "PRO_LADDER_SCALP_SHORT"}

QUALITY_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "LINK", "AVAX", "AAVE", "SUI", "TAO", "NEAR", "INJ",
    "OP", "ARB", "APT", "TIA", "ADA", "DOT", "MATIC", "TON", "LTC", "BCH", "ETC", "FIL", "ATOM",
    "UNI", "RUNE", "SEI", "FET", "WLD", "DOGE", "TRX", "ENA", "JUP", "ORDI"
}

ULTRA_RISK_KEYWORDS = {
    "1000", "PEPE", "BONK", "WIF", "MEME", "DOGS", "CATI", "HMSTR", "GOBLIN", "MOG", "TURBO",
    "BOME", "NEIRO", "PNUT", "MOODENG", "ACT", "GOAT", "FIGHT", "BLEND", "VELVET", "MAGMA"
}

FALLBACK_SYMBOLS = [f"{b}-USDT" for b in [
    "BTC","ETH","SOL","BNB","XRP","LINK","AVAX","AAVE","SUI","TAO","NEAR","INJ","OP","ARB",
    "APT","TIA","ADA","DOT","LTC","BCH","ETC","FIL","ATOM","UNI","RUNE","SEI","FET","WLD",
    "DOGE","TRX","ENA","JUP","ORDI","BEAT","BLESS","KAITO","XLM","WLFI","PUMP"
]]

STATE: Dict[str, Any] = {}
KLINE_CACHE: Dict[str, Tuple[float, Optional[List[Dict[str, float]]]]] = {}
TICKER_CACHE: Dict[str, Tuple[float, Optional[List[str]]]] = {}

# ============================================================
# State / utilities
# ============================================================

def now_ts() -> int:
    return int(time.time())


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def normalize_symbol(symbol: str) -> str:
    s = symbol.replace("/", "-").upper()
    if s.endswith("USDT") and "-" not in s:
        s = s.replace("USDT", "-USDT")
    return s


def display_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-", "/")


def base_asset(symbol: str) -> str:
    return normalize_symbol(symbol).split("-")[0]


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        base = default_state()
        base.update(data if isinstance(data, dict) else {})
        return base
    except Exception:
        return default_state()


def save_state() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def default_state() -> Dict[str, Any]:
    return {
        "active_signals": [],
        "stats": {
            "total": {"profit": 0, "sl": 0},
            "side": {},
            "grade": {},
            "strategy": {},
            "type": {},
            "symbol": {},
            "strategy_side_grade": {},
            "strategy_side": {},
        },
        "pair_cooldown": {},
        "strategy_cooldown": {},
        "hard_block_until": {},
        "last_diag_ts": 0,
        "last_scan": {},
        "last_error": "",
        "startup_sent": False,
    }


def inc_stat(bucket: str, key: str, result: str) -> None:
    stats = STATE.setdefault("stats", default_state()["stats"])
    d = stats.setdefault(bucket, {})
    item = d.setdefault(key, {"profit": 0, "sl": 0})
    item[result] = item.get(result, 0) + 1


def apply_result(signal: Dict[str, Any], result: str) -> None:
    if result not in ("profit", "sl"):
        return
    stats = STATE.setdefault("stats", default_state()["stats"])
    stats.setdefault("total", {"profit": 0, "sl": 0})[result] += 1
    inc_stat("side", signal.get("side", "?"), result)
    inc_stat("grade", signal.get("grade", "?"), result)
    inc_stat("strategy", signal.get("strategy", "?"), result)
    inc_stat("type", signal.get("trade_type", "?"), result)
    inc_stat("symbol", signal.get("symbol", "?"), result)
    inc_stat("strategy_side_grade", f"{signal.get('strategy')}:{signal.get('side')}:{signal.get('grade')}", result)
    inc_stat("strategy_side", f"{signal.get('strategy')}:{signal.get('side')}", result)

    # V13.9 adaptive hard block: if a strategy/side keeps losing, block it for a day.
    ss = f"{signal.get('strategy')}:{signal.get('side')}"
    item = stats.get("strategy_side", {}).get(ss, {})
    closed = item.get("profit", 0) + item.get("sl", 0)
    wr = (item.get("profit", 0) / closed * 100.0) if closed else 0.0
    if closed >= ADAPTIVE_MIN_TRADES and wr < ADAPTIVE_MIN_WR:
        until = now_ts() + BAD_STRATEGY_BLOCK_HOURS * 3600
        STATE.setdefault("hard_block_until", {})[ss] = until
        STATE.setdefault("hard_block_until", {})[signal.get("strategy")] = until

    save_state()


def wr_text(item: Dict[str, int]) -> str:
    p = item.get("profit", 0)
    s = item.get("sl", 0)
    t = p + s
    wr = p / t * 100 if t else 0.0
    return f"{p} профит / {s} SL / WR {wr:.1f}%"


def build_stats_text() -> str:
    stats = STATE.setdefault("stats", default_state()["stats"])
    lines = ["📊 Статистика", f"Итого: {wr_text(stats.get('total', {}))}"]
    for title, key in [("Стороны", "side"), ("Классы", "grade"), ("Стратегии", "strategy"), ("Типы", "type")]:
        data = stats.get(key, {})
        if data:
            lines.append(f"\n{title}:")
            for k, v in sorted(data.items(), key=lambda kv: -(kv[1].get("profit",0)+kv[1].get("sl",0)))[:10]:
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
            STATE["last_error"] = f"Telegram error {r.status_code}: {r.text[:200]}"
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
            data = r.json()
            return data
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
                    "time": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                    "low": float(c[3]), "close": float(c[4]), "volume": float(c[5]),
                })
        except Exception:
            continue
    candles = [x for x in candles if x["open"] > 0 and x["high"] > 0 and x["low"] > 0 and x["close"] > 0]
    candles.sort(key=lambda x: x["time"])
    return candles if len(candles) >= 40 else None


def get_klines(symbol: str, interval: str, limit: int = 180, cache_seconds: int = 30) -> Optional[List[Dict[str, float]]]:
    symbol = normalize_symbol(symbol)
    key = f"{symbol}:{interval}:{limit}"
    cached = KLINE_CACHE.get(key)
    if cached and time.time() - cached[0] < cache_seconds:
        return cached[1]

    endpoints = [
        "/openApi/swap/v3/quote/klines",
        "/openApi/swap/v2/quote/klines",
    ]
    for ep in endpoints:
        data = get_json(ep, {"symbol": symbol, "interval": interval, "limit": limit})
        if not data:
            continue
        raw = data.get("data")
        candles = parse_klines(raw)
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
    # Quality first, then random rotation
    quality = [s for s in out if base_asset(s) in QUALITY_BASES]
    rest = [s for s in out if base_asset(s) not in QUALITY_BASES]
    random.shuffle(rest)
    result = (quality + rest)[:MAX_CONTRACTS]
    TICKER_CACHE["symbols"] = (time.time(), result)
    return result

# ============================================================
# Indicators / market context
# ============================================================

def closes(c: List[Dict[str, float]]) -> List[float]: return [x["close"] for x in c]
def highs(c: List[Dict[str, float]]) -> List[float]: return [x["high"] for x in c]
def lows(c: List[Dict[str, float]]) -> List[float]: return [x["low"] for x in c]

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
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
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
    avg = sum(x["volume"] for x in candles[-n-1:-1]) / n
    return cur / avg if avg > 0 else 1.0


def trend_state(candles: List[Dict[str, float]]) -> str:
    cs = closes(candles)
    if len(cs) < 60:
        return "UNKNOWN"
    e21 = ema(cs, 21)
    e55 = ema(cs, 55)
    price = cs[-1]
    ch = percent_change(candles, min(20, len(candles)-1))
    if price > e21 > e55 and ch > 0.003:
        return "UP"
    if price < e21 < e55 and ch < -0.003:
        return "DOWN"
    return "RANGE"


def btc_context() -> Dict[str, Any]:
    c15 = get_klines("BTC-USDT", "15m", 120, cache_seconds=45)
    c1h = get_klines("BTC-USDT", "1h", 180, cache_seconds=120)
    c4h = get_klines("BTC-USDT", "4h", 120, cache_seconds=240)
    if not c15 or not c1h or not c4h:
        return {"ok": False, "text": "BTC data unavailable", "direction": "UNKNOWN"}
    ch15 = percent_change(c15, 4)
    ch6h = percent_change(c15, 24)
    t1h = trend_state(c1h)
    t4h = trend_state(c4h)
    direction = "RANGE"
    if ch6h < -0.018 or (t1h == "DOWN" and ch15 < -0.002):
        direction = "BEAR"
    elif ch6h > 0.018 or (t1h == "UP" and ch15 > 0.002):
        direction = "BULL"
    return {
        "ok": True,
        "direction": direction,
        "t1h": t1h,
        "t4h": t4h,
        "ch15": ch15,
        "ch6h": ch6h,
        "text": f"BTC {direction}: 15m {ch15*100:+.2f}%, 6h {ch6h*100:+.2f}%, 1H {t1h}, 4H {t4h}",
    }

# ============================================================
# Professional level verification
# ============================================================

def pivot_levels(candles: List[Dict[str, float]], kind: str, left: int = 3, right: int = 3) -> List[float]:
    vals = []
    for i in range(left, len(candles) - right):
        window = candles[i-left:i+right+1]
        if kind == "support":
            if candles[i]["low"] == min(x["low"] for x in window):
                vals.append(candles[i]["low"])
        else:
            if candles[i]["high"] == max(x["high"] for x in window):
                vals.append(candles[i]["high"])
    return vals


def cluster_levels(levels: List[float], tol: float) -> List[float]:
    levels = sorted([x for x in levels if x > 0])
    if not levels:
        return []
    clusters: List[List[float]] = [[levels[0]]]
    for lv in levels[1:]:
        ref = sum(clusters[-1]) / len(clusters[-1])
        if abs(lv - ref) / ref <= tol:
            clusters[-1].append(lv)
        else:
            clusters.append([lv])
    return [sum(c) / len(c) for c in clusters]


def level_quality(candles: List[Dict[str, float]], level: float, kind: str, tf_weight: float = 1.0) -> Dict[str, Any]:
    if not candles or level <= 0:
        return {"score": 0, "touches": 0, "reactions": 0, "noise": True}
    tol = LEVEL_CLUSTER_TOL
    avg_atr = atr(candles, 14)
    min_reaction = max(level * 0.004, avg_atr * 0.9)
    touches = 0
    reactions = 0
    last_touch_i = -999
    for i, c in enumerate(candles[-160:]):
        idx = len(candles) - min(160, len(candles)) + i
        touched = False
        if kind == "support":
            touched = abs(c["low"] - level) / level <= tol or (c["low"] <= level <= c["high"])
            reacted = touched and (c["high"] - level) >= min_reaction
        else:
            touched = abs(c["high"] - level) / level <= tol or (c["low"] <= level <= c["high"])
            reacted = touched and (level - c["low"]) >= min_reaction
        if touched and idx - last_touch_i >= 4:  # separate touches only
            touches += 1
            last_touch_i = idx
            if reacted:
                reactions += 1
    # Cap excessive touches: too many touches = noisy zone, not magical level
    effective = min(touches, MAX_TOUCHES_EFFECTIVE)
    noise_penalty = 0
    if touches > 14:
        noise_penalty = 8
    score = (effective * 8 + min(reactions, 6) * 9) * tf_weight - noise_penalty
    return {"score": max(0, score), "touches": touches, "reactions": reactions, "noise": touches > 18}


def verified_levels(c1h: List[Dict[str, float]], c4h: List[Dict[str, float]]) -> Dict[str, Any]:
    price = c1h[-1]["close"]
    raw_sup = pivot_levels(c1h[-140:], "support") + pivot_levels(c4h[-100:], "support")
    raw_res = pivot_levels(c1h[-140:], "resistance") + pivot_levels(c4h[-100:], "resistance")
    supports = [lv for lv in cluster_levels(raw_sup, LEVEL_CLUSTER_TOL) if lv < price]
    resistances = [lv for lv in cluster_levels(raw_res, LEVEL_CLUSTER_TOL) if lv > price]
    supports = sorted(supports, key=lambda x: abs(price - x))[:5]
    resistances = sorted(resistances, key=lambda x: abs(price - x))[:5]

    def best(levels: List[float], kind: str) -> Optional[Dict[str, Any]]:
        out = []
        for lv in levels:
            q1 = level_quality(c1h, lv, kind, 1.0)
            q4 = level_quality(c4h, lv, kind, 1.25)
            q = {
                "level": lv,
                "score": q1["score"] + q4["score"],
                "touches": q1["touches"] + q4["touches"],
                "reactions": q1["reactions"] + q4["reactions"],
                "noise": q1["noise"] or q4["noise"],
                "distance": abs(price - lv) / price,
            }
            out.append(q)
        return sorted(out, key=lambda z: (z["score"], -z["distance"]), reverse=True)[0] if out else None
    return {"support": best(supports, "support"), "resistance": best(resistances, "resistance")}

# ============================================================
# Confirmation / signal construction
# ============================================================

def candle_body_ok(c: Dict[str, float], side: str) -> bool:
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    rng = max(h - l, 1e-12)
    body = abs(cl - o) / rng
    upper = (h - max(o, cl)) / rng
    lower = (min(o, cl) - l) / rng
    if side == "LONG":
        return cl > o and body >= 0.22 and upper <= 0.55
    return cl < o and body >= 0.22 and lower <= 0.55


def two_candle_confirmation(c5: List[Dict[str, float]], side: str, ema5: float, vw: float) -> bool:
    if len(c5) < 4:
        return False
    a, b = c5[-2], c5[-1]
    if side == "LONG":
        return a["close"] > ema5 and b["close"] > ema5 and b["close"] > min(vw, ema5) and candle_body_ok(b, side)
    return a["close"] < ema5 and b["close"] < ema5 and b["close"] < max(vw, ema5) and candle_body_ok(b, side)


def htf_confirm(c15: List[Dict[str, float]], side: str, level: float) -> bool:
    if len(c15) < 3:
        return False
    last = c15[-1]
    prev = c15[-2]
    if side == "LONG":
        return last["close"] > level and (last["close"] >= prev["close"] or candle_body_ok(last, side))
    return last["close"] < level and (last["close"] <= prev["close"] or candle_body_ok(last, side))

def closes_above(candles: List[Dict[str, float]], level: float, n: int = 2, buffer: float = 0.0010) -> bool:
    if len(candles) < n:
        return False
    return all(c["close"] > level * (1 + buffer) for c in candles[-n:])


def closes_below(candles: List[Dict[str, float]], level: float, n: int = 2, buffer: float = 0.0010) -> bool:
    if len(candles) < n:
        return False
    return all(c["close"] < level * (1 - buffer) for c in candles[-n:])


def professional_reject_confirm(c5: List[Dict[str, float]], c15: List[Dict[str, float]], side: str, level: float, ema5: float, vw: float) -> bool:
    """Reject/reclaim must be confirmed; do not trade a level just because price touched it."""
    if len(c5) < 4 or len(c15) < 4:
        return False
    if side == "SHORT":
        # Price must have tested above/near resistance, then CLOSED back below it.
        tested = max(x["high"] for x in c15[-6:]) >= level * (1 - LEVEL_CLUSTER_TOL)
        failed_hold = not closes_above(c15, level, 2, 0.0008)
        returned = closes_below(c5, level, 2, 0.0005) and c5[-1]["close"] < max(ema5, vw)
        momentum_down = c5[-1]["close"] < c5[-2]["close"] and candle_body_ok(c5[-1], "SHORT")
        return tested and failed_hold and returned and momentum_down
    else:
        tested = min(x["low"] for x in c15[-6:]) <= level * (1 + LEVEL_CLUSTER_TOL)
        failed_break = not closes_below(c15, level, 2, 0.0008)
        reclaimed = closes_above(c5, level, 2, 0.0005) and c5[-1]["close"] > min(ema5, vw)
        momentum_up = c5[-1]["close"] > c5[-2]["close"] and candle_body_ok(c5[-1], "LONG")
        return tested and failed_break and reclaimed and momentum_up


def breakout_hold_confirm(c5: List[Dict[str, float]], c15: List[Dict[str, float]], side: str, level: float, ema5: float, vw: float) -> bool:
    """Breakout-hold mode: if level is accepted, trade retest/continuation, not the opposite reject."""
    if len(c5) < 4 or len(c15) < 6:
        return False
    if side == "LONG":
        accepted = closes_above(c15, level, 2, 0.0010)
        retest = min(x["low"] for x in c5[-8:]) <= level * (1 + RETEST_TOL)
        held = c5[-1]["close"] > level and c5[-1]["close"] > min(ema5, vw)
        return accepted and retest and held and two_candle_confirmation(c5, "LONG", ema5, vw)
    else:
        accepted = closes_below(c15, level, 2, 0.0010)
        retest = max(x["high"] for x in c5[-8:]) >= level * (1 - RETEST_TOL)
        held = c5[-1]["close"] < level and c5[-1]["close"] < max(ema5, vw)
        return accepted and retest and held and two_candle_confirmation(c5, "SHORT", ema5, vw)


def build_secondary_intraday_level(c15: List[Dict[str, float]], price: float, kind: str) -> Optional[Dict[str, Any]]:
    """Fallback active intraday level so the bot works even when 1H/4H levels are far away.
    It is allowed only as B unless other confirmations are very strong."""
    if len(c15) < 60:
        return None
    if kind == "support":
        raw = pivot_levels(c15[-80:], "support", 2, 2)
        levels = [lv for lv in cluster_levels(raw, LEVEL_CLUSTER_TOL) if lv < price]
    else:
        raw = pivot_levels(c15[-80:], "resistance", 2, 2)
        levels = [lv for lv in cluster_levels(raw, LEVEL_CLUSTER_TOL) if lv > price]
    if not levels:
        return None
    lv = sorted(levels, key=lambda x: abs(price-x))[0]
    q = level_quality(c15, lv, kind, 0.70)
    if q["touches"] < 2 or q["reactions"] < 1:
        return None
    return {"level": lv, "score": q["score"], "touches": q["touches"], "reactions": q["reactions"], "noise": q["noise"], "distance": abs(price-lv)/price, "secondary": True}


def recent_broken_level(c15: List[Dict[str, float]], price: float, side: str) -> Optional[Dict[str, Any]]:
    """V13.8: find a recently broken level now acting as support/resistance.
    This is for calm continuation trades: breakout -> pullback -> hold/reject -> continuation.
    It prevents the bot from only trading old HTF levels and missing clean trend moves.
    """
    if len(c15) < 70 or price <= 0:
        return None
    if side == "LONG":
        raw = pivot_levels(c15[-100:-3], "resistance", 2, 2)
        levels = [lv for lv in cluster_levels(raw, LEVEL_CLUSTER_TOL) if lv < price and abs(price - lv) / price <= RETEST_TOL]
        kind = "resistance"
    else:
        raw = pivot_levels(c15[-100:-3], "support", 2, 2)
        levels = [lv for lv in cluster_levels(raw, LEVEL_CLUSTER_TOL) if lv > price and abs(price - lv) / price <= RETEST_TOL]
        kind = "support"
    if not levels:
        return None
    lv = sorted(levels, key=lambda x: abs(price - x))[0]
    q = level_quality(c15, lv, kind, 0.75)
    # For continuation we accept fewer touches, but not zero-quality random levels.
    if q["touches"] < 2 or q["reactions"] < 1:
        return None
    return {
        "level": lv,
        "score": min(q["score"], 42),
        "touches": q["touches"],
        "reactions": q["reactions"],
        "noise": q["noise"],
        "distance": abs(price - lv) / price,
        "secondary": True,
        "continuation": True,
    }


def calm_momentum_context(c15: List[Dict[str, float]], c1h: List[Dict[str, float]], side: str) -> bool:
    """Trend must be real but not exhausted. We want the pullback in an active move, not a late chase."""
    if len(c15) < 40 or len(c1h) < 40:
        return False
    ch1h = (c15[-1]["close"] - c15[-4]["close"]) / max(c15[-4]["close"], 1e-12)
    ch4h = (c15[-1]["close"] - c15[-16]["close"]) / max(c15[-16]["close"], 1e-12)
    t1h = trend_state(c1h)
    if side == "LONG":
        # strong enough to matter, but not a vertical overextension
        return t1h != "DOWN" and ch1h > 0.0015 and ch4h > 0.003 and ch4h < 0.090
    return t1h != "UP" and ch1h < -0.0015 and ch4h < -0.003 and ch4h > -0.090


def calm_pullback_confirmation(c5: List[Dict[str, float]], c15: List[Dict[str, float]], side: str, level: float, ema5: float, vw: float) -> bool:
    """Confirm pullback/retest, not a chase. Similar to how a discretionary trader enters after level retest."""
    if len(c5) < 12 or len(c15) < 8:
        return False
    if side == "LONG":
        had_pullback = min(x["low"] for x in c5[-10:]) <= level * (1 + RETEST_TOL)
        held_level = c5[-1]["close"] > level and c5[-2]["close"] > level * 0.998
        reclaimed_ma = c5[-1]["close"] > min(ema5, vw) and c5[-1]["close"] > c5[-2]["close"]
        htf_ok = c15[-1]["close"] >= c15[-2]["close"] or c15[-1]["close"] > level
        return had_pullback and held_level and reclaimed_ma and htf_ok and two_candle_confirmation(c5, "LONG", ema5, vw)
    had_pullback = max(x["high"] for x in c5[-10:]) >= level * (1 - RETEST_TOL)
    held_level = c5[-1]["close"] < level and c5[-2]["close"] < level * 1.002
    rejected_ma = c5[-1]["close"] < max(ema5, vw) and c5[-1]["close"] < c5[-2]["close"]
    htf_ok = c15[-1]["close"] <= c15[-2]["close"] or c15[-1]["close"] < level
    return had_pullback and held_level and rejected_ma and htf_ok and two_candle_confirmation(c5, "SHORT", ema5, vw)


def ultra_risk_symbol(symbol: str, c5: List[Dict[str, float]], c15: List[Dict[str, float]]) -> bool:
    b = base_asset(symbol)
    if any(k in b for k in ULTRA_RISK_KEYWORDS):
        return True
    for c in c5[-20:]:
        if (c["high"] - c["low"]) / max(c["open"], 1e-12) > ULTRA_RISK_5M_CANDLE:
            return True
    for c in c15[-12:]:
        if (c["high"] - c["low"]) / max(c["open"], 1e-12) > ULTRA_RISK_15M_CANDLE:
            return True
    return False


def anti_chase_ok(side: str, c15: List[Dict[str, float]], current: float, near_level: bool) -> Tuple[bool, str]:
    ch6h = percent_change(c15, 24)
    ch24h = percent_change(c15, min(96, len(c15)-1))
    recent_high = max(x["high"] for x in c15[-24:])
    recent_low = min(x["low"] for x in c15[-24:])
    pullback_from_high = (recent_high - current) / recent_high if recent_high else 0
    bounce_from_low = (current - recent_low) / recent_low if recent_low else 0

    if side == "SHORT" and (ch6h < -BIG_MOVE_6H or ch24h < -BIG_MOVE_24H):
        if not near_level and bounce_from_low < REQUIRED_PULLBACK_AFTER_BIG_MOVE:
            return False, f"late SHORT blocked: move6h {ch6h*100:.1f}%, bounce only {bounce_from_low*100:.1f}%"
    if side == "LONG" and (ch6h > BIG_MOVE_6H or ch24h > BIG_MOVE_24H):
        if not near_level and pullback_from_high < REQUIRED_PULLBACK_AFTER_BIG_MOVE:
            return False, f"late LONG blocked: move6h {ch6h*100:.1f}%, pullback only {pullback_from_high*100:.1f}%"
    return True, "ok"



def calculate_ladder_scalp_trade(symbol: str, side: str, entry: float, level: float, candles: List[Dict[str, float]]) -> Dict[str, Any]:
    """V13.11: tight invalidation + ladder targets for active scalps.
    TP1 is still about 10% ROI at x10, while TP5 can catch 30-40%+ ROI moves.
    """
    a = atr(candles, 14)
    buffer = max(entry * 0.0018, a * LADDER_SL_ATR_MULT)
    if side == "LONG":
        recent_low = min(x["low"] for x in candles[-10:])
        sl = min(level, recent_low) - buffer
        # Avoid absurdly tight SL in noisy crypto, but keep it technical.
        sl = min(sl, entry * 0.992)
        tp1 = entry * (1 + LADDER_TP1_MOVE)
        tp2 = entry * (1 + LADDER_TP2_MOVE)
        tp3 = entry * (1 + LADDER_TP3_MOVE)
        tp4 = entry * (1 + LADDER_TP4_MOVE)
        tp5 = entry * (1 + LADDER_TP5_MOVE)
    else:
        recent_high = max(x["high"] for x in candles[-10:])
        sl = max(level, recent_high) + buffer
        sl = max(sl, entry * 1.008)
        tp1 = entry * (1 - LADDER_TP1_MOVE)
        tp2 = entry * (1 - LADDER_TP2_MOVE)
        tp3 = entry * (1 - LADDER_TP3_MOVE)
        tp4 = entry * (1 - LADDER_TP4_MOVE)
        tp5 = entry * (1 - LADDER_TP5_MOVE)
    risk = abs(entry - sl)
    reward = abs(tp1 - entry)
    rr = reward / risk if risk > 0 else 0.0
    roi_tp1 = reward / entry * LEVERAGE * 100
    roi_sl = risk / entry * LEVERAGE * 100
    return {"entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3, "tp4": tp4, "tp5": tp5, "rr": rr, "roi_tp1": roi_tp1, "roi_sl": roi_sl}


def ladder_scalp_setup(c5: List[Dict[str, float]], c15: List[Dict[str, float]], c1h: List[Dict[str, float]], side: str, ema5: float, vw: float) -> Optional[Dict[str, Any]]:
    """Discretionary-style active scalp detector.
    It looks for: active move, controlled pullback, reclaim/reject, and no vertical chase.
    """
    if not LADDER_SCALP_ENABLED or len(c5) < 36 or len(c15) < 32 or len(c1h) < 40:
        return None
    price = c5[-1]["close"]
    ch1h = (c15[-1]["close"] - c15[-5]["close"]) / max(c15[-5]["close"], 1e-12)
    ch2h = (c15[-1]["close"] - c15[-9]["close"]) / max(c15[-9]["close"], 1e-12)
    t1h = trend_state(c1h)

    if side == "LONG":
        if t1h == "DOWN" and ch1h < LADDER_MIN_1H_MOVE:
            return None
        if ch1h < LADDER_MIN_1H_MOVE or ch1h > LADDER_MAX_1H_MOVE or ch2h > 0.075:
            return None
        high = max(x["high"] for x in c5[-24:])
        low = min(x["low"] for x in c5[-16:])
        pullback = (high - low) / max(high, 1e-12)
        if pullback < LADDER_PULLBACK_MIN or pullback > LADDER_PULLBACK_MAX:
            return None
        # Entry after reclaim, not at the top of the first impulse.
        if not (c5[-1]["close"] > ema5 and c5[-1]["close"] > vw and c5[-1]["close"] > c5[-2]["high"] * 0.999):
            return None
        if c15[-1]["close"] < c15[-2]["close"] and c15[-1]["close"] < ema(closes(c15), 21):
            return None
        level = max(low, min(x["close"] for x in c5[-10:]))
        return {"level": level, "score": 36, "touches": 2, "reactions": 1, "noise": False, "distance": abs(price-level)/price, "secondary": True, "scalp": True, "pullback": pullback, "ch1h": ch1h}

    # SHORT ladder scalp: active down move -> controlled bounce -> reject.
    if t1h == "UP" and ch1h > -LADDER_MIN_1H_MOVE:
        return None
    if ch1h > -LADDER_MIN_1H_MOVE or ch1h < -LADDER_MAX_1H_MOVE or ch2h < -0.075:
        return None
    high = max(x["high"] for x in c5[-16:])
    low = min(x["low"] for x in c5[-24:])
    bounce = (high - low) / max(low, 1e-12)
    if bounce < LADDER_PULLBACK_MIN or bounce > LADDER_PULLBACK_MAX:
        return None
    if not (c5[-1]["close"] < ema5 and c5[-1]["close"] < vw and c5[-1]["close"] < c5[-2]["low"] * 1.001):
        return None
    if c15[-1]["close"] > c15[-2]["close"] and c15[-1]["close"] > ema(closes(c15), 21):
        return None
    level = min(high, max(x["close"] for x in c5[-10:]))
    return {"level": level, "score": 36, "touches": 2, "reactions": 1, "noise": False, "distance": abs(price-level)/price, "secondary": True, "scalp": True, "pullback": bounce, "ch1h": ch1h}

def calculate_trade(symbol: str, side: str, entry: float, level: float, opposite: Optional[Dict[str, Any]], candles: List[Dict[str, float]]) -> Dict[str, float]:
    a = atr(candles, 14)
    buffer = max(entry * 0.0025, a * 0.45)
    if side == "LONG":
        recent_low = min(x["low"] for x in candles[-12:])
        sl = min(level, recent_low) - buffer
        min_tp1 = entry * (1 + MIN_TP1_PRICE_MOVE * 1.05)
        structural = opposite["level"] if opposite else entry * 1.025
        tp1 = max(min_tp1, min(structural, entry * 1.032))
        if tp1 <= entry:
            tp1 = min_tp1
        tp2 = entry + (tp1 - entry) * 1.8
        tp3 = entry + (tp1 - entry) * 2.8
    else:
        recent_high = max(x["high"] for x in candles[-12:])
        sl = max(level, recent_high) + buffer
        min_tp1 = entry * (1 - MIN_TP1_PRICE_MOVE * 1.05)
        structural = opposite["level"] if opposite else entry * 0.975
        tp1 = min(min_tp1, max(structural, entry * 0.968))
        if tp1 >= entry:
            tp1 = min_tp1
        tp2 = entry - (entry - tp1) * 1.8
        tp3 = entry - (entry - tp1) * 2.8
    risk = abs(entry - sl)
    reward = abs(tp1 - entry)
    rr = reward / risk if risk > 0 else 0.0
    roi_tp1 = reward / entry * LEVERAGE * 100
    roi_sl = risk / entry * LEVERAGE * 100
    return {"entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3, "rr": rr, "roi_tp1": roi_tp1, "roi_sl": roi_sl}


def score_signal(side: str, strategy: str, trade_type: str, level: Dict[str, Any], btc: Dict[str, Any], t1h: str, t4h: str, vol: float, rr: float, dist: float, strong_reversal: bool) -> Tuple[int, List[str]]:
    score = 45
    notes = []

    # level quality: cap touches effect; reaction matters more than touch count
    touches_eff = min(level.get("touches", 0), MAX_TOUCHES_EFFECTIVE)
    reactions_eff = min(level.get("reactions", 0), 6)
    score += touches_eff * 3 + reactions_eff * 5
    if level.get("noise"):
        score -= 8
        notes.append("level noisy/excessive touches")

    if dist <= 0.004:
        score += 10
    elif dist <= LEVEL_NEAR_TOL:
        score += 6
    else:
        score -= 4

    # BTC alignment
    btc_dir = btc.get("direction")
    if side == "LONG":
        if btc_dir == "BULL": score += 10
        elif btc_dir == "RANGE": score += 3
        elif btc_dir == "BEAR": score -= 14; notes.append("BTC bearish vs LONG")
    else:
        if btc_dir == "BEAR": score += 10
        elif btc_dir == "RANGE": score += 3
        elif btc_dir == "BULL": score -= 14; notes.append("BTC bullish vs SHORT")

    # HTF alignment
    against_1h = (side == "LONG" and t1h == "DOWN") or (side == "SHORT" and t1h == "UP")
    against_4h = (side == "LONG" and t4h == "DOWN") or (side == "SHORT" and t4h == "UP")
    with_1h = (side == "LONG" and t1h == "UP") or (side == "SHORT" and t1h == "DOWN")
    with_4h = (side == "LONG" and t4h == "UP") or (side == "SHORT" and t4h == "DOWN")
    if with_1h: score += 8
    if with_4h: score += 8
    if against_1h:
        score -= 10
        notes.append("1H против")
    if against_4h:
        score -= 8
        notes.append("4H против")

    if strong_reversal:
        score += 10
        notes.append("strong reclaim/reject")

    if vol >= 1.25: score += 8
    elif vol >= 0.95: score += 4
    elif vol < VERY_LOW_VOLUME:
        score -= 12
        notes.append(f"very low volume x{vol:.2f}")
    elif vol < MIN_VOLUME_B:
        score -= 6
        notes.append(f"low volume x{vol:.2f}")

    if rr >= 1.25: score += 8
    elif rr >= 1.0: score += 5
    elif rr >= 0.75: score += 1
    else:
        score -= 8
        notes.append(f"weak RR {rr:.2f}")

    # Professional caps: no fake Score 100 if core flaws exist
    cap = 100
    if vol < VERY_LOW_VOLUME:
        cap = min(cap, LOW_VOLUME_CAP)
    if rr < 0.85:
        cap = min(cap, 82)
    if against_1h and not strong_reversal:
        cap = min(cap, 82)
    if against_4h and not strong_reversal:
        cap = min(cap, 80)
    if level.get("touches", 0) > 16:
        cap = min(cap, 84)
    if level.get("secondary"):
        cap = min(cap, 86)
    if btc.get("direction") == "BEAR" and side == "LONG" and not strong_reversal:
        cap = min(cap, 80)
    if btc.get("direction") == "BULL" and side == "SHORT" and not strong_reversal:
        cap = min(cap, 80)

    return max(0, min(int(score), cap)), notes


def item_wr(item: Dict[str, int]) -> Tuple[int, float]:
    p = int(item.get("profit", 0))
    sl = int(item.get("sl", 0))
    total = p + sl
    wr = (p / total * 100.0) if total else 0.0
    return total, wr


def adaptive_stats_gate(strategy: str, side: str, trade_type: str) -> Tuple[bool, str]:
    if not ADAPTIVE_BLOCK_ENABLED:
        return True, "ok"
    stats = STATE.setdefault("stats", default_state()["stats"])
    checks = [
        ("strategy", strategy),
        ("strategy_side", f"{strategy}:{side}"),
        ("type", trade_type),
        ("side", side),
    ]
    for bucket, key in checks:
        item = stats.get(bucket, {}).get(key, {})
        closed, wr = item_wr(item)
        # side is broader, so require more sample before blocking all LONG/SHORT
        min_trades = max(6, ADAPTIVE_MIN_TRADES) if bucket == "side" else ADAPTIVE_MIN_TRADES
        if closed >= min_trades and wr < ADAPTIVE_MIN_WR:
            return False, f"adaptive block {bucket}:{key} WR {wr:.1f}% after {closed}"
    return True, "ok"


def grade_stats_gate(grade: str, strategy: str, side: str, trade_type: str) -> Tuple[bool, str]:
    """Second-stage stat filter after grade is known.
    In live stats B is losing, so B is blocked when global/strategy/type stats are weak.
    This preserves quality instead of forcing more weak trades.
    """
    if not ADAPTIVE_BLOCK_ENABLED:
        return True, "ok"
    stats = STATE.setdefault("stats", default_state()["stats"])

    if grade == "B":
        b_item = stats.get("grade", {}).get("B", {})
        closed, wr = item_wr(b_item)
        if closed >= GLOBAL_B_MIN_TRADES and wr < GLOBAL_B_MIN_WR:
            return False, f"B class blocked: WR {wr:.1f}% after {closed}"

        # Momentum continuation B trades are disabled. They were the main source of recent SLs.
        if strategy in ("PRO_CALM_BREAKOUT_PULLBACK_LONG", "PRO_CALM_BREAKDOWN_PULLBACK_SHORT"):
            return False, f"{strategy} allowed only as A+"

    # Block weak trade types/strategy-side once they have enough live evidence.
    for bucket, key, threshold in [
        ("type", trade_type, WEAK_TYPE_MIN_WR),
        ("strategy_side", f"{strategy}:{side}", ADAPTIVE_MIN_WR),
        ("strategy", strategy, ADAPTIVE_MIN_WR),
    ]:
        item = stats.get(bucket, {}).get(key, {})
        closed, wr = item_wr(item)
        if closed >= ADAPTIVE_MIN_TRADES and wr < threshold:
            return False, f"stat block {bucket}:{key} WR {wr:.1f}% after {closed}"
    return True, "ok"


def micro_structure_ok(c5: List[Dict[str, float]], c15: List[Dict[str, float]], side: str) -> bool:
    """Final price-action check: avoid entering against the last micro impulse."""
    if len(c5) < 5 or len(c15) < 3:
        return False
    a, b = c5[-2], c5[-1]
    last15 = c15[-1]
    if side == "LONG":
        if b["close"] <= a["close"]:
            return False
        if b["low"] < min(x["low"] for x in c5[-4:-1]) and b["close"] < (b["high"] + b["low"]) / 2:
            return False
        if last15["close"] < last15["open"] and (last15["open"] - last15["close"]) > (last15["high"] - last15["low"]) * 0.45:
            return False
        return True
    if b["close"] >= a["close"]:
        return False
    if b["high"] > max(x["high"] for x in c5[-4:-1]) and b["close"] > (b["high"] + b["low"]) / 2:
        return False
    if last15["close"] > last15["open"] and (last15["close"] - last15["open"]) > (last15["high"] - last15["low"]) * 0.45:
        return False
    return True


def htf_professional_gate(strategy: str, side: str, btc: Dict[str, Any], t1h: str, t4h: str, vol: float, rr: float, strong: bool) -> Tuple[bool, str]:
    """Professional context filter.
    Counter-trend trades are allowed only as real level reversals with volume and RR, not as B momentum.
    """
    btc_dir = btc.get("direction")
    if side == "LONG":
        if btc_dir == "BEAR" and not strong:
            return False, "BTC bearish vs LONG"
        if t1h == "DOWN" and not (strong and vol >= 1.15 and rr >= 1.05):
            return False, "1H DOWN: LONG needs strong reclaim + volume + RR"
        if t4h == "DOWN" and not (strong and vol >= 1.20 and rr >= 1.10):
            return False, "4H DOWN: LONG blocked unless very strong reclaim"
    else:
        if btc_dir == "BULL" and not strong:
            return False, "BTC bullish vs SHORT"
        if t1h == "UP" and not (strong and vol >= 1.15 and rr >= 1.05):
            return False, "1H UP: SHORT needs strong reject + volume + RR"
        if t4h == "UP" and not (strong and vol >= 1.20 and rr >= 1.10):
            return False, "4H UP: SHORT blocked unless very strong reject"
    return True, "ok"


def cooldown_ok(symbol: str, strategy: str, side: str = "") -> Tuple[bool, str]:
    t = now_ts()
    if t < STATE.setdefault("pair_cooldown", {}).get(symbol, 0):
        return False, "pair cooldown"
    if t < STATE.setdefault("strategy_cooldown", {}).get(strategy, 0):
        return False, "strategy cooldown"
    hb = STATE.setdefault("hard_block_until", {})
    if t < hb.get(strategy, 0):
        return False, "strategy hard block"
    if side and t < hb.get(f"{strategy}:{side}", 0):
        return False, "strategy-side hard block"
    return True, "ok"


def analyze_symbol(symbol: str, btc: Dict[str, Any], blocks: Dict[str, int], near_miss: List[str]) -> Optional[Dict[str, Any]]:
    symbol = normalize_symbol(symbol)
    c15 = get_klines(symbol, "15m", 180)
    if not c15:
        blocks["no_klines_15m"] = blocks.get("no_klines_15m", 0) + 1
        return None
    c5 = get_klines(symbol, "5m", 160)
    if not c5:
        blocks["no_klines_5m"] = blocks.get("no_klines_5m", 0) + 1
        return None
    c1h = get_klines(symbol, "1h", 180)
    c4h = get_klines(symbol, "4h", 120)
    if not c1h or not c4h:
        blocks["no_htf"] = blocks.get("no_htf", 0) + 1
        return None

    if ultra_risk_symbol(symbol, c5, c15):
        blocks["ultra_risk_block"] = blocks.get("ultra_risk_block", 0) + 1
        return None

    price = c5[-1]["close"]
    e5 = ema(closes(c5), 21)
    vw = vwap(c5, 48)
    vol = volume_ratio(c15, 30)
    t1h = trend_state(c1h)
    t4h = trend_state(c4h)
    lv = verified_levels(c1h, c4h)
    support = lv.get("support")
    resistance = lv.get("resistance")

    candidates: List[Dict[str, Any]] = []

    def add_candidate(side: str, strategy: str, trade_type: str, level: Dict[str, Any], opposite: Optional[Dict[str, Any]], reason: str, strong: bool):
        if not level:
            return
        dist = abs(price - level["level"]) / price
        if dist > max(LEVEL_NEAR_TOL, RETEST_TOL):
            blocks["level_distance_block"] = blocks.get("level_distance_block", 0) + 1
            return
        min_touches = MIN_LEVEL_TOUCHES_A if strong else MIN_LEVEL_TOUCHES_B
        min_reacts = MIN_REACTIONS_A if strong else MIN_REACTIONS_B
        if level.get("touches", 0) < min_touches or level.get("reactions", 0) < min_reacts:
            blocks["weak_level_block"] = blocks.get("weak_level_block", 0) + 1
            return
        if REQUIRE_15M_CONFIRM and not htf_confirm(c15, side, level["level"]):
            blocks["confirm_15m_block"] = blocks.get("confirm_15m_block", 0) + 1
            return
        if not two_candle_confirmation(c5, side, e5, vw):
            blocks["confirm_5m_block"] = blocks.get("confirm_5m_block", 0) + 1
            return
        ok, chase_reason = anti_chase_ok(side, c15, price, dist <= RETEST_TOL)
        if not ok:
            blocks["anti_chase_block"] = blocks.get("anti_chase_block", 0) + 1
            return
        co, co_reason = cooldown_ok(symbol, strategy, side)
        if not co:
            blocks["cooldown_block"] = blocks.get("cooldown_block", 0) + 1
            return

        tr = calculate_ladder_scalp_trade(symbol, side, price, level["level"], c15) if strategy in SCALP_STRATEGIES else calculate_trade(symbol, side, price, level["level"], opposite, c15)
        if tr["roi_tp1"] < MIN_TP1_ROI_X10:
            blocks["tp1_roi_block"] = blocks.get("tp1_roi_block", 0) + 1
            return

        if not micro_structure_ok(c5, c15, side):
            blocks["micro_structure_block"] = blocks.get("micro_structure_block", 0) + 1
            return

        htf_ok, htf_reason = htf_professional_gate(strategy, side, btc, t1h, t4h, vol, tr["rr"], strong)
        if not htf_ok:
            blocks["htf_professional_block"] = blocks.get("htf_professional_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side} {strategy}: {htf_reason}")
            return

        # V13.10: professional gates before scoring. These prevent statistics-killing weak continuation entries.
        gate_ok, gate_reason = adaptive_stats_gate(strategy, side, trade_type)
        if not gate_ok:
            blocks["adaptive_stats_block"] = blocks.get("adaptive_stats_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side} {strategy}: {gate_reason}")
            return

        if side == "LONG":
            if LONG_NEEDS_HTF_NOT_DOWN and t1h == "DOWN" and btc.get("direction") != "BULL":
                blocks["long_1h_down_block"] = blocks.get("long_1h_down_block", 0) + 1
                return
            if vol < LONG_MIN_VOLUME:
                blocks["long_volume_block"] = blocks.get("long_volume_block", 0) + 1
                return
            if tr["rr"] < LONG_MIN_RR:
                blocks["long_rr_block"] = blocks.get("long_rr_block", 0) + 1
                return

        if strategy == "PRO_CALM_BREAKOUT_PULLBACK_LONG":
            # This strategy currently has weak stats. It can pass only as a real high-quality A+ continuation.
            if t1h != "UP" or btc.get("direction") == "BEAR" or vol < CALM_LONG_MIN_VOLUME or tr["rr"] < CALM_LONG_MIN_RR:
                blocks["calm_long_quality_block"] = blocks.get("calm_long_quality_block", 0) + 1
                return

        if strategy == "PRO_CALM_BREAKDOWN_PULLBACK_SHORT":
            # Recent short continuation failed too; allow only high-quality A+ breakdown continuation.
            if t1h != "DOWN" or btc.get("direction") == "BULL" or vol < CALM_SHORT_MIN_VOLUME or tr["rr"] < CALM_SHORT_MIN_RR:
                blocks["calm_short_quality_block"] = blocks.get("calm_short_quality_block", 0) + 1
                return

        if strategy in SCALP_STRATEGIES:
            if vol < LADDER_MIN_VOLUME or tr["rr"] < LADDER_MIN_RR:
                blocks["ladder_scalp_quality_block"] = blocks.get("ladder_scalp_quality_block", 0) + 1
                return

        score, notes = score_signal(side, strategy, trade_type, level, btc, t1h, t4h, vol, tr["rr"], dist, strong)
        if strategy in SCALP_STRATEGIES:
            score += 8
            notes.append("ladder scalp setup")
            if vol < LADDER_A_VOLUME:
                score = min(score, 86)
            if tr["rr"] < 1.0:
                score = min(score, 86)
        grade = None
        min_rr = None
        if score >= A_PLUS_MIN_SCORE and tr["rr"] >= MIN_RR_A and vol >= MIN_VOLUME_A:
            grade = "A+"
            min_rr = MIN_RR_A
        elif score >= B_MIN_SCORE and tr["rr"] >= MIN_RR_B and vol >= MIN_VOLUME_B:
            if CALM_BREAKOUT_LONG_A_ONLY and strategy == "PRO_CALM_BREAKOUT_PULLBACK_LONG":
                blocks["calm_long_b_forbidden"] = blocks.get("calm_long_b_forbidden", 0) + 1
                return
            if CALM_BREAKDOWN_SHORT_A_ONLY and strategy == "PRO_CALM_BREAKDOWN_PULLBACK_SHORT":
                blocks["calm_short_b_forbidden"] = blocks.get("calm_short_b_forbidden", 0) + 1
                return
            grade = "B"
            min_rr = MIN_RR_B
        else:
            blocks["score_rr_volume_block"] = blocks.get("score_rr_volume_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side} {strategy}: score {score}, RR {tr['rr']:.2f}, vol x{vol:.2f}")
            return

        g_ok, g_reason = grade_stats_gate(grade, strategy, side, trade_type)
        if not g_ok:
            blocks["grade_stats_block"] = blocks.get("grade_stats_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side} {strategy}: {g_reason}")
            return

        risk_mult = LADDER_RISK_MULT if strategy in SCALP_STRATEGIES else (A_RISK_MULT if grade == "A+" else B_RISK_MULT)
        if tr["roi_sl"] > 20:
            risk_mult = min(risk_mult, FAR_SL_RISK_MULT)
        candidates.append({
            "symbol": symbol, "side": side, "grade": grade, "score": score, "strategy": strategy,
            "trade_type": trade_type, "entry": tr["entry"], "tp1": tr["tp1"], "tp2": tr["tp2"],
            "tp3": tr["tp3"], "tp4": tr.get("tp4"), "tp5": tr.get("tp5"), "sl": tr["sl"], "rr": tr["rr"], "roi_tp1": tr["roi_tp1"],
            "roi_sl": tr["roi_sl"], "risk_mult": risk_mult, "level": level["level"],
            "level_touches": level.get("touches", 0), "level_reactions": level.get("reactions", 0),
            "support": support["level"] if support else None, "resistance": resistance["level"] if resistance else None,
            "btc_text": btc.get("text", ""), "t1h": t1h, "t4h": t4h, "volume_ratio": vol,
            "reason": reason, "notes": notes, "created_at": now_ts(), "status": "active", "tp1_hit": False,
        })

    # Add secondary 15m levels if HTF levels are too far/missing. This prevents silence,
    # but these levels are lower priority and can only pass with proper confirmation.
    if not support:
        support = build_secondary_intraday_level(c15, price, "support")
        if support:
            blocks["secondary_support_used"] = blocks.get("secondary_support_used", 0) + 1
    if not resistance:
        resistance = build_secondary_intraday_level(c15, price, "resistance")
        if resistance:
            blocks["secondary_resistance_used"] = blocks.get("secondary_resistance_used", 0) + 1

    # Scenario switch rules:
    # 1) If resistance is accepted above, SHORT reject is forbidden; only LONG retest can pass.
    # 2) If support is accepted below, LONG reclaim is forbidden; only SHORT retest can pass.
    # 3) Reject/reclaim requires real close back through the level, not only a wick/touch.

    resistance_accepted_above = bool(resistance and closes_above(c15, resistance["level"], 2, 0.0010))
    support_accepted_below = bool(support and closes_below(c15, support["level"], 2, 0.0010))

    # 1. Support holds/reclaims -> LONG. Not allowed if support has already accepted below.
    if support:
        s = support["level"]
        if support_accepted_below:
            blocks["support_broken_no_long"] = blocks.get("support_broken_no_long", 0) + 1
        elif professional_reject_confirm(c5, c15, "LONG", s, e5, vw):
            strong = vol >= 0.90 and (t1h != "DOWN" or btc.get("direction") != "BEAR")
            add_candidate(
                "LONG", "PRO_SUPPORT_RECLAIM_CONFIRMED", "SUPPORT RECLAIM LONG",
                support, resistance,
                f"Поддержка {s:.8g}: цена сделала тест/вынос и ДВЕ 5m свечи вернулись выше уровня. Это подтверждённый reclaim, не ранний вход.",
                strong,
            )

    # 2. Resistance rejects -> SHORT. Not allowed if resistance has already accepted above.
    if resistance:
        r = resistance["level"]
        if resistance_accepted_above:
            blocks["resistance_broken_no_short"] = blocks.get("resistance_broken_no_short", 0) + 1
        elif professional_reject_confirm(c5, c15, "SHORT", r, e5, vw):
            strong = vol >= 0.90 and (t1h != "UP" or btc.get("direction") != "BULL")
            add_candidate(
                "SHORT", "PRO_RESISTANCE_REJECT_CONFIRMED", "RESISTANCE REJECT SHORT",
                resistance, support,
                f"Сопротивление {r:.8g}: цена не закрепилась выше, две 5m свечи вернулись ниже уровня и продавец подтвердил reject.",
                strong,
            )

    # 3. Support broke and retested from below -> SHORT.
    if support:
        s = support["level"]
        if breakout_hold_confirm(c5, c15, "SHORT", s, e5, vw):
            add_candidate(
                "SHORT", "PRO_SUPPORT_BREAK_RETEST_CONFIRMED", "SUPPORT FAILED / RETEST SHORT",
                support, None,
                f"Поддержка {s:.8g} принята ниже: цена закрепилась под уровнем, retest снизу не вернул уровень. SHORT по подтверждённому break-retest.",
                vol >= 0.80,
            )

    # 4. Resistance broke and retested from above -> LONG.
    if resistance:
        r = resistance["level"]
        if breakout_hold_confirm(c5, c15, "LONG", r, e5, vw):
            add_candidate(
                "LONG", "PRO_RESISTANCE_BREAK_RETEST_CONFIRMED", "RESISTANCE BREAK / RETEST LONG",
                resistance, None,
                f"Сопротивление {r:.8g} принято выше: цена закрепилась над уровнем, retest сверху удержан. LONG по подтверждённому breakout-retest.",
                vol >= 0.80,
            )


    # 5. V13.11 professional ladder scalp: active coin, controlled pullback, reclaim/reject.
    # This is designed for setups like BLESS: small but clean ladder targets, not averaging into a falling knife.
    if LADDER_SCALP_ENABLED:
        if btc.get("direction") != "BEAR":
            sc = ladder_scalp_setup(c5, c15, c1h, "LONG", e5, vw)
            if sc and vol >= LADDER_MIN_VOLUME:
                add_candidate(
                    "LONG", "PRO_LADDER_SCALP_LONG", "LADDER SCALP LONG",
                    sc, resistance,
                    f"Активный скальп от уровня: монета дала движение, затем контролируемый откат {sc.get('pullback',0)*100:.2f}% и reclaim EMA/VWAP. Вход не на вертикальной свече; цели идут лестницей.",
                    vol >= LADDER_A_VOLUME and t1h != "DOWN",
                )
        if btc.get("direction") != "BULL":
            sc = ladder_scalp_setup(c5, c15, c1h, "SHORT", e5, vw)
            if sc and vol >= LADDER_MIN_VOLUME:
                add_candidate(
                    "SHORT", "PRO_LADDER_SCALP_SHORT", "LADDER SCALP SHORT",
                    sc, support,
                    f"Активный скальп от уровня: монета дала движение вниз, затем контролируемый отскок {sc.get('pullback',0)*100:.2f}% и reject EMA/VWAP. SHORT после отката, не на дне.",
                    vol >= LADDER_A_VOLUME and t1h != "UP",
                )

    # 6. V13.8 calm continuation: trend move -> broken intraday level -> pullback -> hold/reject.
    # This is the mode that can catch moves like BEAT without buying a vertical top.
    if btc.get("direction") != "BEAR" and calm_momentum_context(c15, c1h, "LONG"):
        br = recent_broken_level(c15, price, "LONG")
        if br and calm_pullback_confirmation(c5, c15, "LONG", br["level"], e5, vw):
            add_candidate(
                "LONG", "PRO_CALM_BREAKOUT_PULLBACK_LONG", "CALM BREAKOUT PULLBACK LONG",
                br, resistance,
                f"Активное движение вверх: пробитый intraday-уровень {br['level']:.8g} удержан на откате. Это не погоня за свечой, а вход после retest/reclaim.",
                vol >= 0.75 and t1h != "DOWN",
            )

    if btc.get("direction") != "BULL" and calm_momentum_context(c15, c1h, "SHORT"):
        br = recent_broken_level(c15, price, "SHORT")
        if br and calm_pullback_confirmation(c5, c15, "SHORT", br["level"], e5, vw):
            add_candidate(
                "SHORT", "PRO_CALM_BREAKDOWN_PULLBACK_SHORT", "CALM BREAKDOWN PULLBACK SHORT",
                br, support,
                f"Активное движение вниз: пробитый intraday-уровень {br['level']:.8g} удержан снизу после отскока. SHORT после retest, не догоняющий вход на дне.",
                vol >= 0.75 and t1h != "UP",
            )

    if not candidates:
        return None
    candidates.sort(key=lambda x: (x["grade"] == "A+", x["score"], x["rr"]), reverse=True)
    return candidates[0]

# ============================================================
# Signal messages / scan / tracking
# ============================================================

def format_price(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def build_signal_message(s: Dict[str, Any]) -> str:
    arrow = "🟢" if s["side"] == "LONG" else "🔴"
    return (
        f"{arrow} {s['side']} {display_symbol(s['symbol'])}\n"
        f"Класс: {s['grade']} · Score {s['score']} · {s['trade_type']}\n"
        f"Стратегия: {s['strategy']}\n\n"
        f"Вход: {format_price(s['entry'])}\n"
        f"TP1: {format_price(s['tp1'])} · ≈ {s['roi_tp1']:.1f}% ROI при x{LEVERAGE}\n"
        f"TP2: {format_price(s['tp2'])}\n"
        f"TP3: {format_price(s['tp3'])}\n"
        f"SL: {format_price(s['sl'])} · риск до SL ≈ {s['roi_sl']:.1f}% ROI при x{LEVERAGE}\n"
        f"RR к TP1: {s['rr']:.2f}\n"
        f"Риск: multiplier x{s['risk_mult']:.2f}\n\n"
        f"📌 Профессиональная логика уровня:\n{s['reason']}\n"
        f"Уровень: {format_price(s['level'])} · touches {s['level_touches']} · reactions {s['level_reactions']}\n"
        f"1H {s['t1h']} · 4H {s['t4h']} · volume x{s['volume_ratio']:.2f}\n"
        f"BTC: {s['btc_text']}\n"
        f"Support: {format_price(s['support']) if s.get('support') else '-'} · Resistance: {format_price(s['resistance']) if s.get('resistance') else '-'}\n\n"
        f"Сделка не является гарантией прибыли. SL возможен; размер позиции должен соответствовать риску."
    )


def build_diagnostic(scan: Dict[str, Any]) -> str:
    blocks = scan.get("blocks", {})
    block_lines = [f"{k}: {v}" for k, v in sorted(blocks.items(), key=lambda kv: -kv[1])[:10]]
    near = scan.get("near_miss", [])[:8]
    return (
        f"🧪 Диагностика V13.11 Ladder Scalp Level Trader\n"
        f"Проверено: {scan.get('checked', 0)} из universe {scan.get('universe', 0)}\n"
        f"Кандидатов: {scan.get('candidates', 0)} · отправлено: {scan.get('sent', 0)} · время: {scan.get('elapsed', 0):.0f}с\n"
        f"BTC: {scan.get('btc', 'unknown')}\n"
        f"Статистика: {wr_text(STATE.get('stats', {}).get('total', {}))}\n\n"
        f"Главные блокировки:\n" + ("\n".join(block_lines) if block_lines else "нет") +
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
    quality = [s for s in symbols if base_asset(s) in QUALITY_BASES]
    rest = [s for s in symbols if base_asset(s) not in QUALITY_BASES]
    selected = (quality + rest)[:MAX_ANALYZE_SYMBOLS]

    scan = {
        "checked": 0, "universe": len(symbols), "candidates": 0, "sent": 0,
        "blocks": blocks, "near_miss": near_miss, "btc": btc.get("text", "BTC unknown"), "elapsed": 0,
    }
    if not btc.get("ok"):
        blocks["btc_data_problem"] = 1
        STATE["last_scan"] = scan
        save_state()
        return scan

    found: List[Dict[str, Any]] = []
    for sym in selected:
        if len(STATE.get("active_signals", [])) >= MAX_ACTIVE_SIGNALS:
            blocks["active_slots_full"] = blocks.get("active_slots_full", 0) + 1
            break
        try:
            s = analyze_symbol(sym, btc, blocks, near_miss)
            scan["checked"] += 1
            if s:
                found.append(s)
        except Exception as e:
            blocks["analyze_exception"] = blocks.get("analyze_exception", 0) + 1
            STATE["last_error"] = f"analyze {sym}: {repr(e)}"

    found.sort(key=lambda x: (x["grade"] == "A+", x["score"], x["rr"]), reverse=True)
    scan["candidates"] = len(found)
    sent = 0
    for s in found[:MAX_SIGNALS_PER_SCAN]:
        add_active_signal(s)
        send_telegram(build_signal_message(s))
        sent += 1
    scan["sent"] = sent
    scan["elapsed"] = time.time() - start
    STATE["last_scan"] = scan
    save_state()

    # Send occasional diagnostics if no signal, or always on manual scan
    if manual or sent == 0 and (now_ts() - STATE.get("last_diag_ts", 0) >= DIAG_SECONDS):
        send_telegram(build_diagnostic(scan))
        STATE["last_diag_ts"] = now_ts()
        save_state()
    return scan


def current_price(symbol: str) -> Optional[float]:
    c = get_klines(symbol, "1m", 80, cache_seconds=8)
    if not c:
        return None
    return c[-1]["close"]


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
        hit_tp1 = p >= s["tp1"] if side == "LONG" else p <= s["tp1"]
        hit_sl = p <= s["sl"] if side == "LONG" else p >= s["sl"]
        if hit_tp1:
            apply_result(s, "profit")
            send_telegram(
                f"✅ TAKE PROFIT\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                f"Стратегия: {s['strategy']}\nTP1 достигнут: {format_price(p)}\n\n{build_stats_text()}"
            )
            changed = True
            continue
        if hit_sl:
            apply_result(s, "sl")
            send_telegram(
                f"❌ Stop Loss\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                f"Стратегия: {s['strategy']}\nВход: {format_price(s['entry'])}\nSL: {format_price(s['sl'])}\nТекущая цена: {format_price(p)}\n\n{build_stats_text()}"
            )
            changed = True
            continue
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
        f"✅ {APP_NAME} активирован и работает.\n"
        f"Deploy marker: {DEPLOY_MARKER}\n\n"
        f"Level mode: verified levels + ladder scalp pullback/reclaim.\n"
        f"A+ {A_PLUS_MIN_SCORE}+ / B {B_MIN_SCORE}+ · TP1 min {MIN_TP1_ROI_X10:.0f}% ROI x{LEVERAGE}.\n"
        f"V13.11: ловит BLESS-style setup: active move → controlled pullback → EMA/VWAP reclaim → ladder TP.\n"
        f"Quality: micro-structure + HTF gate + grade-stat filter. API retries {API_RETRIES}, analyze {MAX_ANALYZE_SYMBOLS}."
    )
    # first diagnostic scan is always useful
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
    return HTMLResponse(f"<h3>{APP_NAME}</h3><p>{DEPLOY_MARKER}</p><p>Use /health /version /scan /auto-status /stats</p>")


@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME, "deploy": DEPLOY_MARKER, "active": len(STATE.get("active_signals", [])), "last_error": STATE.get("last_error", "")}


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
    # Background Worker mode for Render: python bot.py
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
