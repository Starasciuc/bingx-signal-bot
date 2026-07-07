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
# V15.0 — LEVEL REJECTION / REVERSAL SCALPER
#
# Rebuilt around the trader's real examples (GRASS SHORT, PUMP SHORT):
# both trades entered after price reached a real support/resistance
# zone (multiple prior touches on 15m) and showed rejection there -
# NOT continuation of the prior move. This is a mean-reversion model,
# the opposite of V14 (which traded continuation of momentum).
#
# Reverse-engineered ladder from the two real examples:
#   TP1 ~0.9%, TP2 ~1.5%, TP3 ~2.1%, TP4 ~3.1%, TP5 ~4.1% from entry
#   Averaging level ~1.8-2.0x the TP5 distance from entry
#
# Averaging behavior (per user): position size does NOT increase on
# averaging - this is effectively a wider stop with the entry price
# re-based to the average of the two touches, not a martingale size-up.
# Risk stays linear/bounded, which is why this is acceptable to
# automate. A hard kill-switch beyond the averaging level is mandatory
# so risk is never truly unbounded.
#
# Design principles carried over from V14:
# - ONE entry model, symmetric for LONG/SHORT, no fallback stack.
# - No silent threshold-softening when the market is quiet. No setup
#    found = no signal sent. Silence is a valid, good outcome.
# - Single quality gate, no per-symbol carve-outs.
# ============================================================

APP_NAME = "Professional Adaptive Futures Bot V15.0 LEVEL REJECTION SCALPER"
DEPLOY_MARKER = "V15_0_LEVEL_REJECTION_2026_07_07"

app = FastAPI(title=APP_NAME)

BINGX_BASE_URL = "https://open-api.bingx.com"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

STATE_FILE = os.getenv("STATE_FILE", "bot_state_v15_0_level_rejection.json")
LEVERAGE = int(os.getenv("LEVERAGE", "10"))

# --- Scan cadence ---
AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "20"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "3"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
API_RETRIES = int(os.getenv("API_RETRIES", "3"))
API_THROTTLE_SECONDS = float(os.getenv("API_THROTTLE_SECONDS", "0.04"))
MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "400"))
MAX_ANALYZE_SYMBOLS = int(os.getenv("MAX_ANALYZE_SYMBOLS", "150"))
HOT_SYMBOLS_TO_ANALYZE = int(os.getenv("HOT_SYMBOLS_TO_ANALYZE", "40"))
DIAG_SECONDS = int(os.getenv("DIAG_SECONDS", "1800"))

# --- Signal / slot limits ---
MAX_ACTIVE_SIGNALS = int(os.getenv("MAX_ACTIVE_SIGNALS", "2"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "1"))
PAIR_COOLDOWN_SECONDS = int(os.getenv("PAIR_COOLDOWN_SECONDS", "900"))
STRATEGY_COOLDOWN_SECONDS = int(os.getenv("STRATEGY_COOLDOWN_SECONDS", "180"))

# ============================================================
# THE ENTRY MODEL (single source of truth, symmetric both sides)
# Level rejection / reversal model, reverse-engineered from real
# trader examples (GRASS SHORT, PUMP SHORT).
# ============================================================

# Step 1: there must be a real recent impulse INTO the level (the move
# that built the level's relevance right now). Same magnitude filter
# as before, but this describes the approach move, not the trade
# direction - trade direction is the opposite of this move.
APPROACH_MIN_30M_MOVE = float(os.getenv("APPROACH_MIN_30M_MOVE", "0.020"))   # 2.0% run into the level in 30m
APPROACH_MIN_1H_MOVE = float(os.getenv("APPROACH_MIN_1H_MOVE", "0.030"))     # 3.0% run into the level in 1h
APPROACH_MAX_1H_MOVE = float(os.getenv("APPROACH_MAX_1H_MOVE", "0.25"))      # beyond this: too parabolic/manipulated, skip

# Step 2: level detection on 15m candles. A "real" level needs multiple
# prior touches within a tight tolerance - not a single wick.
LEVEL_LOOKBACK_CANDLES = int(os.getenv("LEVEL_LOOKBACK_CANDLES", "80"))   # ~20h of 15m candles
LEVEL_TOUCH_TOLERANCE = float(os.getenv("LEVEL_TOUCH_TOLERANCE", "0.0035"))  # 0.35% cluster tolerance
LEVEL_MIN_TOUCHES = int(os.getenv("LEVEL_MIN_TOUCHES", "3"))              # min touches incl. the current one
LEVEL_MIN_AGE_CANDLES = int(os.getenv("LEVEL_MIN_AGE_CANDLES", "6"))      # earliest touch must be at least this old
LEVEL_PROXIMITY = float(os.getenv("LEVEL_PROXIMITY", "0.0045"))           # current price must be within 0.45% of the level

# Step 3: rejection confirmation - BOTH required together.
# 3a. Rejection candle: long wick at the level on the confirming candle.
REJECT_MIN_WICK_RATIO = float(os.getenv("REJECT_MIN_WICK_RATIO", "0.38"))  # wick / total range
# 3b. Close location far from the extreme: the move has already started.
REJECT_CLOSE_LONG_MAX = float(os.getenv("REJECT_CLOSE_LONG_MAX", "0.42"))  # SHORT: close in lower 42% of range
REJECT_CLOSE_SHORT_MIN = float(os.getenv("REJECT_CLOSE_SHORT_MIN", "0.58"))  # LONG: close in upper 58% of range
REJECT_LOOKBACK_CANDLES = int(os.getenv("REJECT_LOOKBACK_CANDLES", "3"))   # how many recent 5m candles may hold the rejection

# Step 4: live follow-through - price must already be moving away from
# the level right now (1m), not just the 5m rejection candle sitting
# there. This distinguishes "rejection just happened" from "rejection
# happened 20 minutes ago and now it's stale".
FOLLOW_MIN_1M3_MOVE = float(os.getenv("FOLLOW_MIN_1M3_MOVE", "0.0022"))    # 0.22% in last 3 x 1m candles, away from level
FOLLOW_MIN_VOL1 = float(os.getenv("FOLLOW_MIN_VOL1", "0.75"))
FOLLOW_MIN_RANGE1 = float(os.getenv("FOLLOW_MIN_RANGE1", "0.75"))

# --- Stop loss: structural, beyond the level itself ---
SL_ATR_MULT = float(os.getenv("SL_ATR_MULT", "0.50"))
SL_LEVEL_BUFFER = float(os.getenv("SL_LEVEL_BUFFER", "0.0020"))   # extra 0.20% beyond the level extreme
MIN_SL_MOVE = float(os.getenv("MIN_SL_MOVE", "0.0060"))
MAX_SL_MOVE = float(os.getenv("MAX_SL_MOVE", "0.0220"))

# --- Take-profit ladder: fixed % from entry, matching the real
# examples (GRASS: 0.95/1.54/2.14/3.14/4.14, PUMP: 0.88/1.51/2.08/3.09/4.10)
TP1_MOVE = float(os.getenv("TP1_MOVE", "0.0090"))
TP2_MOVE = float(os.getenv("TP2_MOVE", "0.0150"))
TP3_MOVE = float(os.getenv("TP3_MOVE", "0.0210"))
TP4_MOVE = float(os.getenv("TP4_MOVE", "0.0310"))
TP5_MOVE = float(os.getenv("TP5_MOVE", "0.0410"))

# --- Averaging level: fixed % from entry (against the position),
# matching the real examples (GRASS 7.30%, PUMP 8.00%). Position size
# does NOT increase on averaging (per user) - this simply re-bases the
# effective entry to the midpoint of the two touches. A hard kill-switch
# stop beyond the averaging level is mandatory: risk must stay bounded.
AVERAGING_ENABLED = os.getenv("AVERAGING_ENABLED", "true").lower() == "true"
AVERAGING_MOVE = float(os.getenv("AVERAGING_MOVE", "0.0750"))          # ~7.5%, midpoint of the two examples
KILL_SWITCH_EXTRA_MOVE = float(os.getenv("KILL_SWITCH_EXTRA_MOVE", "0.0350"))  # additional 3.5% beyond averaging level = hard stop

# --- Quality score gate ---
MIN_SCORE = int(os.getenv("MIN_SCORE", "76"))
A_PLUS_SCORE = int(os.getenv("A_PLUS_SCORE", "88"))

# --- Time stop. Reversal trades are given more room than pure momentum
# scalps since the thesis is "rejection has started", not "still moving". ---
MAX_MINUTES_TO_TP1 = int(os.getenv("MAX_MINUTES_TO_TP1", "12"))
HARD_EXPIRE_MINUTES = int(os.getenv("HARD_EXPIRE_MINUTES", "25"))
MIN_PROGRESS_TO_KEEP = float(os.getenv("MIN_PROGRESS_TO_KEEP", "0.20"))

# --- Symbol universe risk filters ---
ULTRA_RISK_5M_CANDLE = float(os.getenv("ULTRA_RISK_5M_CANDLE", "0.09"))
ULTRA_RISK_15M_CANDLE = float(os.getenv("ULTRA_RISK_15M_CANDLE", "0.13"))
ULTRA_RISK_KEYWORDS = {
    "1000", "PEPE", "BONK", "WIF", "MEME", "DOGS", "CATI", "HMSTR", "GOBLIN", "MOG", "TURBO",
    "BOME", "NEIRO", "PNUT", "MOODENG", "ACT", "GOAT", "FIGHT", "BLEND", "MAGMA"
}

QUALITY_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "LINK", "AVAX", "AAVE", "SUI", "TAO", "NEAR", "INJ",
    "OP", "ARB", "APT", "TIA", "ADA", "DOT", "MATIC", "TON", "LTC", "BCH", "ETC", "FIL", "ATOM",
    "UNI", "RUNE", "SEI", "FET", "WLD", "DOGE", "TRX", "ENA", "JUP", "ORDI"
}

FALLBACK_SYMBOLS = [f"{b}-USDT" for b in [
    "BTC", "ETH", "SOL", "BNB", "XRP", "LINK", "AVAX", "AAVE", "SUI", "TAO", "NEAR", "INJ",
    "OP", "ARB", "APT", "TIA", "ADA", "DOT", "LTC", "BCH", "ETC", "FIL", "ATOM", "UNI",
    "RUNE", "SEI", "FET", "WLD", "DOGE", "TRX", "ENA", "JUP", "ORDI"
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
            "symbol": {},
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
    inc_stat("symbol", signal.get("symbol", "?"), result)
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
    for title, key in [("Стороны", "side"), ("Классы", "grade")]:
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


def volume_ratio(candles: List[Dict[str, float]], n: int = 20) -> float:
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


def btc_context() -> Dict[str, Any]:
    c15 = get_klines("BTC-USDT", "15m", 120, cache_seconds=45)
    if not c15:
        return {"ok": False, "text": "BTC data unavailable"}
    ch1h = percent_change(c15, 4)
    ch6h = percent_change(c15, 24)
    direction = "RANGE"
    if ch1h < -0.004 or ch6h < -0.018:
        direction = "BEAR"
    elif ch1h > 0.004 or ch6h > 0.018:
        direction = "BULL"
    return {
        "ok": True,
        "direction": direction,
        "ch1h": ch1h,
        "ch6h": ch6h,
        "text": f"BTC {direction}: 1h {ch1h*100:+.2f}%, 6h {ch6h*100:+.2f}%",
    }


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

# ============================================================
# Hot symbol selection (cheap pre-filter, not the entry logic itself)
# ============================================================

def hot_score(symbol: str) -> float:
    c1 = get_klines(symbol, "1m", 40, cache_seconds=8)
    c5 = get_klines(symbol, "5m", 40, cache_seconds=18)
    if not c1 or not c5:
        return 0.0
    ch3m = abs(percent_change(c1, 3))
    ch15m = abs(percent_change(c5, 3))
    ch30m = abs(percent_change(c5, 6))
    vr1 = volume_ratio(c1, 20)
    rr1 = candle_range_ratio(c1, 20)
    score = ch3m * 12000 + ch15m * 500 + ch30m * 220 + min(rr1, 4.0) * 10 + min(vr1, 4.0) * 6
    if base_asset(symbol) in QUALITY_BASES:
        score += 2
    return score


def select_hot_symbols(symbols: List[str]) -> List[str]:
    scored: List[Tuple[float, str]] = []
    for sym in symbols[:MAX_ANALYZE_SYMBOLS]:
        try:
            sc = hot_score(sym)
            if sc > 0:
                scored.append((sc, sym))
        except Exception as e:
            STATE["last_error"] = f"hot_score {sym}: {repr(e)}"
    scored.sort(reverse=True, key=lambda x: x[0])
    return [sym for _, sym in scored[:HOT_SYMBOLS_TO_ANALYZE]]

# ============================================================
# THE single entry model: level rejection / reversal
# ============================================================

def find_levels(c15: List[Dict[str, float]]) -> Tuple[List[float], List[float]]:
    """Find resistance/support zones on 15m candles: cluster recent
    highs/lows and keep only clusters with >= LEVEL_MIN_TOUCHES within
    LEVEL_TOUCH_TOLERANCE, where the earliest touch is old enough to
    count as a "real" prior structure, not just the current candle.

    Returns (resistance_levels, support_levels), each a list of price
    levels sorted by touch count descending.
    """
    window = c15[-LEVEL_LOOKBACK_CANDLES:] if len(c15) > LEVEL_LOOKBACK_CANDLES else c15
    if len(window) < LEVEL_MIN_AGE_CANDLES + 3:
        return [], []

    highs = [(i, x["high"]) for i, x in enumerate(window)]
    lows = [(i, x["low"]) for i, x in enumerate(window)]

    def cluster(points: List[Tuple[int, float]]) -> List[Tuple[float, int, int]]:
        # returns (level_price, touch_count, earliest_index)
        clusters: List[List[Tuple[int, float]]] = []
        pts_sorted = sorted(points, key=lambda p: p[1])
        for idx, price in pts_sorted:
            placed = False
            for cl in clusters:
                ref = sum(p for _, p in cl) / len(cl)
                if abs(price - ref) / max(ref, 1e-12) <= LEVEL_TOUCH_TOLERANCE:
                    cl.append((idx, price))
                    placed = True
                    break
            if not placed:
                clusters.append([(idx, price)])
        out = []
        for cl in clusters:
            avg_price = sum(p for _, p in cl) / len(cl)
            touches = len(cl)
            earliest = min(i for i, _ in cl)
            out.append((avg_price, touches, earliest))
        return out

    res_clusters = cluster(highs)
    sup_clusters = cluster(lows)

    last_idx = len(window) - 1
    resistances = [
        price for price, touches, earliest in res_clusters
        if touches >= LEVEL_MIN_TOUCHES and (last_idx - earliest) >= LEVEL_MIN_AGE_CANDLES
    ]
    supports = [
        price for price, touches, earliest in sup_clusters
        if touches >= LEVEL_MIN_TOUCHES and (last_idx - earliest) >= LEVEL_MIN_AGE_CANDLES
    ]
    resistances.sort()
    supports.sort(reverse=True)
    return resistances, supports


def nearest_level(price: float, levels: List[float]) -> Optional[float]:
    if not levels:
        return None
    return min(levels, key=lambda lv: abs(lv - price))


def rejection_confirmed(c5: List[Dict[str, float]], level: float, side: str) -> Tuple[bool, str]:
    """Both conditions required together:
    - a rejection candle (long wick at the level) within the recent window
    - close location of the latest candle far from the extreme (move started)
    """
    recent = c5[-REJECT_LOOKBACK_CANDLES:]
    if not recent:
        return False, "no candles"

    last = c5[-1]
    loc = close_location(last)

    if side == "SHORT":
        wick_ok = any(upper_wick_ratio(c) >= REJECT_MIN_WICK_RATIO for c in recent)
        close_ok = loc <= REJECT_CLOSE_LONG_MAX
        touched = any(c["high"] >= level * (1 - LEVEL_PROXIMITY * 0.6) for c in recent)
        if not touched:
            return False, "no candle touched the resistance zone"
        if not wick_ok:
            return False, "no rejection wick at resistance"
        if not close_ok:
            return False, f"close location too high {loc:.2f}, move not confirmed yet"
        return True, f"rejection confirmed: wick at resistance, close loc {loc:.2f}"

    wick_ok = any(lower_wick_ratio(c) >= REJECT_MIN_WICK_RATIO for c in recent)
    close_ok = loc >= REJECT_CLOSE_SHORT_MIN
    touched = any(c["low"] <= level * (1 + LEVEL_PROXIMITY * 0.6) for c in recent)
    if not touched:
        return False, "no candle touched the support zone"
    if not wick_ok:
        return False, "no rejection wick at support"
    if not close_ok:
        return False, f"close location too low {loc:.2f}, move not confirmed yet"
    return True, f"rejection confirmed: wick at support, close loc {loc:.2f}"


def upper_wick_ratio(c: Dict[str, float]) -> float:
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    rng = max(h - l, 1e-12)
    return (h - max(o, cl)) / rng


def lower_wick_ratio(c: Dict[str, float]) -> float:
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    rng = max(h - l, 1e-12)
    return (min(o, cl) - l) / rng


def build_entry_signal(symbol: str, c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], side: str, btc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Level rejection / reversal entry model. Symmetric for LONG/SHORT.

    SHORT: price ran up into a real resistance zone (multiple 15m
    touches), showed a rejection candle there, closed away from the
    high, and is now following through downward live.

    LONG: mirror image at a support zone.
    """
    if len(c1) < 30 or len(c5) < 20 or len(c15) < LEVEL_MIN_AGE_CANDLES + 10:
        return None

    price = c1[-1]["close"]

    # Step 1: real approach move into the level.
    ch30m = percent_change(c5, 6)
    ch1h = percent_change(c15, 4)
    if side == "SHORT":
        if ch30m < APPROACH_MIN_30M_MOVE and ch1h < APPROACH_MIN_1H_MOVE:
            return None
        if ch1h > APPROACH_MAX_1H_MOVE:
            return None
    else:
        if ch30m > -APPROACH_MIN_30M_MOVE and ch1h > -APPROACH_MIN_1H_MOVE:
            return None
        if ch1h < -APPROACH_MAX_1H_MOVE:
            return None

    # Step 2: a real level nearby with multiple prior touches.
    resistances, supports = find_levels(c15)
    if side == "SHORT":
        level = nearest_level(price, [lv for lv in resistances if lv >= price * (1 - LEVEL_PROXIMITY)])
    else:
        level = nearest_level(price, [lv for lv in supports if lv <= price * (1 + LEVEL_PROXIMITY)])
    if level is None:
        return None
    if abs(level - price) / max(price, 1e-12) > LEVEL_PROXIMITY:
        return None

    # Step 3: rejection confirmed (both wick + close location required).
    reject_ok, reject_reason = rejection_confirmed(c5, level, side)
    if not reject_ok:
        return None

    # Step 4: live follow-through away from the level, right now.
    ch3m = percent_change(c1, 3)
    vol1 = volume_ratio(c1, 20)
    range1 = candle_range_ratio(c1, 20)
    if side == "SHORT":
        if ch3m > -FOLLOW_MIN_1M3_MOVE:
            return None
    else:
        if ch3m < FOLLOW_MIN_1M3_MOVE:
            return None
    if vol1 < FOLLOW_MIN_VOL1:
        return None
    if range1 < FOLLOW_MIN_RANGE1:
        return None

    return {
        "symbol": symbol,
        "side": side,
        "entry": price,
        "level": level,
        "level_touches_note": reject_reason,
        "ch30m": ch30m,
        "ch1h": ch1h,
        "ch3m_1m": ch3m,
        "vol1": vol1,
        "range1": range1,
        "btc_text": btc.get("text", ""),
    }


def calculate_trade(setup: Dict[str, Any], c1: List[Dict[str, float]], c5: List[Dict[str, float]]) -> Optional[Dict[str, Any]]:
    """Stop is structural, beyond the level itself. TP ladder and the
    averaging level are fixed percentages from entry, matching the
    real trader examples. A hard kill-switch stop sits beyond the
    averaging level so risk stays bounded even without a size increase.
    """
    side = setup["side"]
    entry = setup["entry"]
    level = setup["level"]
    a = atr(c5, 14)
    buffer = max(entry * SL_LEVEL_BUFFER, a * SL_ATR_MULT)

    if side == "SHORT":
        sl = level + buffer
        sl = max(sl, entry * (1 + MIN_SL_MOVE))
        tp1 = entry * (1 - TP1_MOVE)
        tp2 = entry * (1 - TP2_MOVE)
        tp3 = entry * (1 - TP3_MOVE)
        tp4 = entry * (1 - TP4_MOVE)
        tp5 = entry * (1 - TP5_MOVE)
        avg_price = entry * (1 + AVERAGING_MOVE)
        kill_switch = avg_price * (1 + KILL_SWITCH_EXTRA_MOVE)
    else:
        sl = level - buffer
        sl = min(sl, entry * (1 - MIN_SL_MOVE))
        tp1 = entry * (1 + TP1_MOVE)
        tp2 = entry * (1 + TP2_MOVE)
        tp3 = entry * (1 + TP3_MOVE)
        tp4 = entry * (1 + TP4_MOVE)
        tp5 = entry * (1 + TP5_MOVE)
        avg_price = entry * (1 - AVERAGING_MOVE)
        kill_switch = avg_price * (1 - KILL_SWITCH_EXTRA_MOVE)

    risk = abs(entry - sl)
    risk_move = risk / max(entry, 1e-12)
    if risk_move > MAX_SL_MOVE:
        return None
    if risk_move < MIN_SL_MOVE * 0.8:
        return None

    rewards = [abs(tp1 - entry), abs(tp2 - entry), abs(tp3 - entry), abs(tp4 - entry), abs(tp5 - entry)]
    rr_tp1 = rewards[0] / risk if risk > 0 else 0.0
    ladder_rr = (sum(rewards) / len(rewards)) / risk if risk > 0 else 0.0
    final_rr = rewards[-1] / risk if risk > 0 else 0.0

    roi_tp1 = rewards[0] / entry * LEVERAGE * 100
    roi_sl = risk / entry * LEVERAGE * 100
    roi_kill = abs(kill_switch - entry) / entry * LEVERAGE * 100

    return {
        **setup,
        "sl": sl,
        "tp1": tp1, "tp2": tp2, "tp3": tp3, "tp4": tp4, "tp5": tp5,
        "avg_price": avg_price,
        "kill_switch": kill_switch,
        "averaged": False,
        "rr_tp1": rr_tp1,
        "ladder_rr": ladder_rr,
        "final_rr": final_rr,
        "risk_move": risk_move,
        "roi_tp1": roi_tp1,
        "roi_sl": roi_sl,
        "roi_kill": roi_kill,
        "created_at": now_ts(),
        "status": "active",
        "tp1_hit": False, "tp2_hit": False, "tp3_hit": False, "tp4_hit": False, "tp5_hit": False,
    }


def score_trade(trade: Dict[str, Any]) -> int:
    """Single quality score. No per-symbol carve-outs."""
    score = 70
    score += min(10, int(abs(trade["ch1h"]) * 60))
    score += min(8, int(abs(trade["ch30m"]) * 150))
    score += min(8, int(max(0.0, trade["vol1"] - 0.75) * 10))
    score += min(8, int(max(0.0, trade["range1"] - 0.75) * 10))
    score += min(6, int(abs(trade["ch3m_1m"]) * 900))
    if base_asset(trade["symbol"]) in QUALITY_BASES:
        score += 1
    return max(0, min(100, score))


def cooldown_ok(symbol: str, side: str) -> bool:
    t = now_ts()
    if t < STATE.setdefault("pair_cooldown", {}).get(symbol, 0):
        return False
    if t < STATE.setdefault("strategy_cooldown", {}).get(side, 0):
        return False
    return True


def analyze_symbol(symbol: str, btc: Dict[str, Any], blocks: Dict[str, int], near_miss: List[str]) -> Optional[Dict[str, Any]]:
    symbol = normalize_symbol(symbol)
    c1 = get_klines(symbol, "1m", 100, cache_seconds=6)
    c5 = get_klines(symbol, "5m", 100, cache_seconds=15)
    c15 = get_klines(symbol, "15m", 60, cache_seconds=30)

    if not c1 or not c5 or not c15:
        blocks["no_candles"] = blocks.get("no_candles", 0) + 1
        return None

    if ultra_risk_symbol(symbol, c5, c15):
        blocks["ultra_risk_block"] = blocks.get("ultra_risk_block", 0) + 1
        return None

    candidates: List[Dict[str, Any]] = []
    for side in ("LONG", "SHORT"):
        setup = build_entry_signal(symbol, c1, c5, c15, side, btc)
        if not setup:
            blocks[f"no_setup_{side.lower()}"] = blocks.get(f"no_setup_{side.lower()}", 0) + 1
            continue

        if not cooldown_ok(symbol, side):
            blocks["cooldown_block"] = blocks.get("cooldown_block", 0) + 1
            continue

        trade = calculate_trade(setup, c1, c5)
        if not trade:
            blocks["rr_or_sl_block"] = blocks.get("rr_or_sl_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: failed RR/SL/feasibility check")
            continue

        trade["score"] = score_trade(trade)
        trade["grade"] = "A+" if trade["score"] >= A_PLUS_SCORE else "B"

        if trade["score"] < MIN_SCORE:
            blocks["score_block"] = blocks.get("score_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: score {trade['score']} < {MIN_SCORE}")
            continue

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
    level_word = "поддержка" if s["side"] == "LONG" else "сопротивление"
    return (
        f"{arrow} {s['side']} {display_symbol(s['symbol'])}\n"
        f"Класс: {s['grade']} · Score {s['score']}\n\n"
        f"Вход: {format_price(s['entry'])}\n"
        f"Уровень ({level_word}): {format_price(s['level'])}\n\n"
        f"TP1: {format_price(s['tp1'])} · RR {s['rr_tp1']:.2f} · ≈{s['roi_tp1']:.1f}% ROI x{LEVERAGE}\n"
        f"TP2: {format_price(s['tp2'])}\n"
        f"TP3: {format_price(s['tp3'])}\n"
        f"TP4: {format_price(s['tp4'])}\n"
        f"TP5: {format_price(s['tp5'])}\n"
        f"SL (структурный): {format_price(s['sl'])} · риск {s['risk_move']*100:.2f}% · ≈{s['roi_sl']:.1f}% ROI x{LEVERAGE}\n"
        f"Усреднение: {format_price(s['avg_price'])}\n"
        f"⛔ Kill-switch (аварийный стоп после усреднения): {format_price(s['kill_switch'])} · ≈{s['roi_kill']:.1f}% ROI x{LEVERAGE}\n\n"
        f"Ladder RR: {s['ladder_rr']:.2f} · Final RR: {s['final_rr']:.2f}\n\n"
        f"📌 Сетап: подход к уровню 30m {s['ch30m']*100:+.2f}% / 1h {s['ch1h']*100:+.2f}%, "
        f"{s['level_touches_note']}, живое продолжение 1m3 {s['ch3m_1m']*100:+.2f}% "
        f"(vol1 x{s['vol1']:.2f}, range1 x{s['range1']:.2f}).\n"
        f"BTC: {s['btc_text']}\n\n"
        f"⏱ Time-stop: если TP1 не двигается за {MAX_MINUTES_TO_TP1} мин — expired."
    )


def build_diagnostic(scan: Dict[str, Any]) -> str:
    blocks = scan.get("blocks", {})
    block_lines = [f"{k}: {v}" for k, v in sorted(blocks.items(), key=lambda kv: -kv[1])[:12]]
    near = scan.get("near_miss", [])[:8]
    return (
        f"🧪 Диагностика V15.0 Level Rejection Scalper\n"
        f"Проверено: {scan.get('checked', 0)} из universe {scan.get('universe', 0)}\n"
        f"Кандидатов: {scan.get('candidates', 0)} · отправлено: {scan.get('sent', 0)} · время: {scan.get('elapsed', 0):.0f}с\n"
        f"BTC: {scan.get('btc', 'unknown')}\n"
        f"Статистика: {wr_text(STATE.get('stats', {}).get('total', {}))}\n\n"
        f"Блокировки:\n" + ("\n".join(block_lines) if block_lines else "нет") +
        ("\n\nПочти прошли:\n" + "\n".join(near) if near else "") +
        f"\n\nLast error: {STATE.get('last_error', '')}"
    )


def add_active_signal(s: Dict[str, Any]) -> None:
    STATE.setdefault("active_signals", []).append(s)
    STATE.setdefault("pair_cooldown", {})[s["symbol"]] = now_ts() + PAIR_COOLDOWN_SECONDS
    STATE.setdefault("strategy_cooldown", {})[s["side"]] = now_ts() + STRATEGY_COOLDOWN_SECONDS
    save_state()


def run_scan(manual: bool = False) -> Dict[str, Any]:
    start = time.time()
    blocks: Dict[str, int] = {}
    near_miss: List[str] = []
    btc = btc_context()
    symbols = get_symbols()
    selected = select_hot_symbols(symbols)

    scan = {
        "checked": 0, "universe": len(symbols), "candidates": 0, "sent": 0,
        "blocks": blocks, "near_miss": near_miss,
        "btc": btc.get("text", "BTC unknown"), "elapsed": 0,
    }

    if not btc.get("ok"):
        blocks["btc_data_problem"] = 1
        STATE["last_scan"] = scan
        save_state()
        return scan

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
        blocks["active_slots_full"] = blocks.get("active_slots_full", 0) + 1
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


def averaging_hit(side: str, price: float, avg_price: float) -> bool:
    return price <= avg_price if side == "LONG" else price >= avg_price


def kill_switch_hit(side: str, price: float, kill_switch: float) -> bool:
    return price <= kill_switch if side == "LONG" else price >= kill_switch


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

        # Kill-switch: hard stop beyond the averaging level. Checked first,
        # applies whether or not averaging has already triggered, and is
        # the only true "unbounded risk" backstop in this model.
        if kill_switch_hit(side, p, s["kill_switch"]):
            apply_result(s, "sl")
            send_telegram(
                f"🛑 KILL-SWITCH STOP\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                f"{'Усреднение было активно. ' if s.get('averaged') else ''}"
                f"Вход: {format_price(s['entry'])}\n"
                f"Kill-switch: {format_price(s['kill_switch'])}\n"
                f"Текущая цена: {format_price(p)}\n\n{build_stats_text()}"
            )
            changed = True
            continue

        # Averaging trigger: re-base the effective entry to the midpoint
        # of the original entry and the averaging price. Position size
        # does NOT increase (per user spec) - this only shifts the
        # reference point used for TP/progress tracking going forward.
        if AVERAGING_ENABLED and not s.get("averaged") and averaging_hit(side, p, s["avg_price"]):
            original_entry = s["entry"]
            new_effective_entry = (original_entry + s["avg_price"]) / 2.0
            s["averaged"] = True
            s["pre_average_entry"] = original_entry
            s["entry"] = new_effective_entry
            send_telegram(
                f"🔁 AVERAGING TRIGGERED\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                f"Первый вход: {format_price(original_entry)}\n"
                f"Усреднение по: {format_price(s['avg_price'])}\n"
                f"Новая средняя цена: {format_price(new_effective_entry)}\n"
                f"⛔ Kill-switch остаётся в силе: {format_price(s['kill_switch'])}\n"
                f"Позиция управляется дальше от новой средней."
            )
            changed = True
            # fall through - still check SL/TP/expiry against the (unchanged) sl this cycle

        if not s.get("averaged") and sl_hit(side, p, s["sl"]):
            apply_result(s, "sl")
            send_telegram(
                f"❌ Stop Loss\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                f"Вход: {format_price(s['entry'])}\nSL: {format_price(s['sl'])}\n"
                f"Текущая цена: {format_price(p)}\n\n{build_stats_text()}"
            )
            changed = True
            continue

        if not s.get("tp1_hit") and age_minutes >= MAX_MINUTES_TO_TP1:
            directional, progress = directional_progress_ratio(s, p)
            if (not directional) or progress < MIN_PROGRESS_TO_KEEP:
                apply_result(s, "expired")
                send_telegram(
                    f"⏱ EXPIRED (no progress)\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                    f"Вход: {format_price(s['entry'])}\nТекущая цена: {format_price(p)}\n"
                    f"Прогресс к TP1: {progress*100:.1f}%\n\n{build_stats_text()}"
                )
                changed = True
                continue

        if age_minutes >= HARD_EXPIRE_MINUTES and not s.get("tp1_hit"):
            apply_result(s, "expired")
            send_telegram(
                f"⏱ HARD EXPIRE\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                f"TP1 не достигнут за {HARD_EXPIRE_MINUTES} мин.\nТекущая цена: {format_price(p)}\n\n{build_stats_text()}"
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
                    f"{key.upper()}: {format_price(s[key])}\nТекущая цена: {format_price(p)}"
                )

        if s.get("tp5") and target_hit(side, p, s["tp5"]):
            s["tp5_hit"] = True
            apply_result(s, "profit")
            send_telegram(
                f"✅ FULL LADDER TAKE PROFIT\n{s['grade']} · {side} {display_symbol(s['symbol'])}\n"
                f"TP5: {format_price(p)}\nВремя в сделке: {age_minutes:.1f} мин\n\n{build_stats_text()}"
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
        f"Модель: level rejection / reversal. Вход только после подхода к реальному "
        f"уровню (>= {LEVEL_MIN_TOUCHES} касаний на 15m) и подтверждённого отторжения "
        f"(фитиль + close location), с живым продолжением 1m.\n"
        f"SL структурный за уровнем. Усреднение ~{AVERAGING_MOVE*100:.1f}% (без увеличения объёма), "
        f"kill-switch ещё +{KILL_SWITCH_EXTRA_MOVE*100:.1f}% дальше.\n"
        f"Никаких fallback-режимов. Нет сетапа — нет сигнала."
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
        f"<h3>{APP_NAME}</h3><p>{DEPLOY_MARKER}</p>"
        f"<p>Use /health /version /scan /auto-status /stats /test-telegram</p>"
    )


@app.get("/health")
def health():
    return {
        "ok": True, "app": APP_NAME, "deploy": DEPLOY_MARKER,
        "active": len(STATE.get("active_signals", [])),
        "last_error": STATE.get("last_error", ""),
    }


@app.get("/version")
def version():
    return {"app": APP_NAME, "deploy_marker": DEPLOY_MARKER}


@app.get("/auto-status")
def auto_status():
    return JSONResponse({
        "app": APP_NAME, "deploy": DEPLOY_MARKER,
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
