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
# V13.18 — LIVE MARKET SCALPER
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
# ============================================================

APP_NAME = "Professional Adaptive Futures Bot AUTO V13.18 LIVE MARKET SCALPER"
DEPLOY_MARKER = "V13_18_LIVE_MARKET_SCALPER_2026_06_24"

app = FastAPI(title=APP_NAME)

BINGX_BASE_URL = "https://open-api.bingx.com"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

STATE_FILE = os.getenv("STATE_FILE", "bot_state_v13_18_live_market_scalper.json")
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"

# --- Scan stability ---
AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "15"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "3"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
API_RETRIES = int(os.getenv("API_RETRIES", "3"))
API_THROTTLE_SECONDS = float(os.getenv("API_THROTTLE_SECONDS", "0.08"))
MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "450"))
MAX_ANALYZE_SYMBOLS = int(os.getenv("MAX_ANALYZE_SYMBOLS", "300"))
HOT_SYMBOLS_TO_ANALYZE = int(os.getenv("HOT_SYMBOLS_TO_ANALYZE", "75"))
DIAG_SECONDS = int(os.getenv("DIAG_SECONDS", "1200"))

# --- Signal limits ---
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "88"))
B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "84"))
MAX_ACTIVE_SIGNALS = int(os.getenv("MAX_ACTIVE_SIGNALS", "2"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "2"))
PAIR_COOLDOWN_SECONDS = int(os.getenv("PAIR_COOLDOWN_SECONDS", "600"))
STRATEGY_COOLDOWN_SECONDS = int(os.getenv("STRATEGY_COOLDOWN_SECONDS", "90"))

# --- Fast burst requirements ---
FAST_BURST_ENABLED = os.getenv("FAST_BURST_ENABLED", "true").lower() == "true"
FAST_MIN_15M_MOVE = float(os.getenv("FAST_MIN_15M_MOVE", "0.0065"))        # 1.0% in 15m
FAST_MIN_30M_MOVE = float(os.getenv("FAST_MIN_30M_MOVE", "0.0100"))        # 1.6% in 30m
FAST_MAX_30M_MOVE = float(os.getenv("FAST_MAX_30M_MOVE", "0.090"))        # avoid late vertical chase
FAST_MIN_RANGE_RATIO = float(os.getenv("FAST_MIN_RANGE_RATIO", "1.08"))   # current 5m range expansion
FAST_MIN_VOLUME_RATIO = float(os.getenv("FAST_MIN_VOLUME_RATIO", "0.95")) # current 15m volume expansion
FAST_MIN_1M_CONFIRM = float(os.getenv("FAST_MIN_1M_CONFIRM", "0.0007"))   # 0.15% last 3m direction
FAST_MAX_SPREAD_PROXY = float(os.getenv("FAST_MAX_SPREAD_PROXY", "0.030"))# current 5m candle too wide/chase block
EDGE_MIN_PRIOR_COMPRESSION = float(os.getenv("EDGE_MIN_PRIOR_COMPRESSION", "99.0")) # prior 5m range should be smaller before expansion
EDGE_MIN_BREAKOUT_DISTANCE = float(os.getenv("EDGE_MIN_BREAKOUT_DISTANCE", "0.00005")) # 0.12% micro break beyond prior 1m structure
EDGE_REQUIRE_MICRO_SWEEP = os.getenv("EDGE_REQUIRE_MICRO_SWEEP", "false").lower() == "true"

# --- Realtime pressure gate ---
# Previous versions expired because they detected a pattern after the flow had already died.
# These filters require live 1m pressure at the exact signal moment.
HOT_MIN_SCORE = float(os.getenv("HOT_MIN_SCORE", "18"))
HOT_MIN_LIVE_MOVE_3M = float(os.getenv("HOT_MIN_LIVE_MOVE_3M", "0.0006"))
HOT_MIN_LIVE_RANGE_OR_VOLUME = float(os.getenv("HOT_MIN_LIVE_RANGE_OR_VOLUME", "0.70"))
HOT_STALE_PENALTY_ENABLED = os.getenv("HOT_STALE_PENALTY_ENABLED", "true").lower() == "true"
REALTIME_MIN_1M_RANGE_RATIO = float(os.getenv("REALTIME_MIN_1M_RANGE_RATIO", "0.85"))
REALTIME_MIN_1M_VOLUME_RATIO = float(os.getenv("REALTIME_MIN_1M_VOLUME_RATIO", "0.75"))
REALTIME_MIN_2M_MOVE = float(os.getenv("REALTIME_MIN_2M_MOVE", "0.00055"))
REALTIME_CLOSE_LOCATION_LONG = float(os.getenv("REALTIME_CLOSE_LOCATION_LONG", "0.57"))
REALTIME_CLOSE_LOCATION_SHORT = float(os.getenv("REALTIME_CLOSE_LOCATION_SHORT", "0.43"))
REALTIME_REQUIRE_TWO_1M_CANDLES = os.getenv("REALTIME_REQUIRE_TWO_1M_CANDLES", "false").lower() == "true"
EDGE_MIN_TP5_FEASIBILITY = float(os.getenv("EDGE_MIN_TP5_FEASIBILITY", "0.28")) # recent 15m move must cover 70% of TP5

# --- Pullback/retest requirements ---
PULLBACK_MIN = float(os.getenv("PULLBACK_MIN", "0.0015"))                 # 0.25%
PULLBACK_MAX = float(os.getenv("PULLBACK_MAX", "0.0400"))                 # 3.0%
RECLAIM_BUFFER = float(os.getenv("RECLAIM_BUFFER", "0.0005"))
CLOSE_LOCATION_MIN_LONG = float(os.getenv("CLOSE_LOCATION_MIN_LONG", "0.52"))
CLOSE_LOCATION_MAX_SHORT = float(os.getenv("CLOSE_LOCATION_MAX_SHORT", "0.48"))

# --- Compact ladder TPs for fast 10-minute realization style ---
# These are intentionally more compact than slow ladder targets.
TP1_MOVE = float(os.getenv("TP1_MOVE", "0.0035"))
TP2_MOVE = float(os.getenv("TP2_MOVE", "0.0068"))
TP3_MOVE = float(os.getenv("TP3_MOVE", "0.0105"))
TP4_MOVE = float(os.getenv("TP4_MOVE", "0.0155"))
TP5_MOVE = float(os.getenv("TP5_MOVE", "0.0220"))

# --- Risk / stop ---
SL_ATR_MULT = float(os.getenv("SL_ATR_MULT", "0.80"))
MIN_SL_MOVE = float(os.getenv("MIN_SL_MOVE", "0.0090"))                  # min 0.9% price risk
MAX_SL_MOVE = float(os.getenv("MAX_SL_MOVE", "0.0450"))                  # block if SL > 4.5%
FAST_RISK_MULT = float(os.getenv("FAST_RISK_MULT", "0.08"))
A_RISK_MULT = float(os.getenv("A_RISK_MULT", "0.14"))

# --- Time stop / no-stall logic ---
FAST_MAX_MINUTES_TO_TP1 = int(os.getenv("FAST_MAX_MINUTES_TO_TP1", "8"))
FAST_HARD_EXPIRE_MINUTES = int(os.getenv("FAST_HARD_EXPIRE_MINUTES", "14"))
FAST_MIN_PROGRESS_TO_KEEP = float(os.getenv("FAST_MIN_PROGRESS_TO_KEEP", "0.30"))
FAST_CANCEL_IF_NO_PROGRESS = os.getenv("FAST_CANCEL_IF_NO_PROGRESS", "true").lower() == "true"

# --- Market shock context ---
# We do not trade market phase/trend. BTC is used only as a shock filter.
BTC_SHOCK_15M_BLOCK = float(os.getenv("BTC_SHOCK_15M_BLOCK", "0.020")) # avoid alt scalp during violent BTC shock

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
    c1 = get_klines(symbol, "1m", 60, cache_seconds=8)
    c5 = get_klines(symbol, "5m", 80, cache_seconds=18)
    c15 = get_klines(symbol, "15m", 80, cache_seconds=30)
    if not c1 or not c5 or not c15:
        return 0.0, "no candles"

    ch3m_signed = percent_change(c1, 3)
    ch3m = abs(ch3m_signed)
    ch15m = abs(percent_change(c5, 3))
    ch30m = abs(percent_change(c5, 6))
    vr1 = volume_ratio(c1, 20)
    vr15 = volume_ratio(c15, 24)
    rr1 = candle_range_ratio(c1, 20)
    rr5 = candle_range_ratio(c5, 20)

    # V13.18: live-first hotness. Big old 30m move is not enough.
    # A coin should be selected because it has movement/range/volume NOW.
    live_flow = min(vr1, 5.0) * min(max(rr1, 0.05), 4.0)
    live_score = ch3m * 9000 + live_flow * 9 + min(rr1, 4.0) * 7
    recent_score = ch15m * 850 + ch30m * 420 + min(vr15, 5.0) * 5 + min(rr5, 4.0) * 5
    score = live_score + recent_score

    # Penalize coins that moved earlier but are dead on the last 1m candles.
    stale = ch30m >= 0.012 and ch3m < HOT_MIN_LIVE_MOVE_3M and rr1 < HOT_MIN_LIVE_RANGE_OR_VOLUME and vr1 < HOT_MIN_LIVE_RANGE_OR_VOLUME
    dead_now = ch3m < HOT_MIN_LIVE_MOVE_3M and rr1 < 0.45 and vr1 < 0.55
    if HOT_STALE_PENALTY_ENABLED and stale:
        score *= 0.25
    if dead_now:
        score *= 0.15

    # Avoid BNB-like false hotness: huge volume but almost no price/range movement.
    if vr1 > 20 and ch3m < 0.0005 and rr1 < 0.5:
        score *= 0.20

    if base_asset(symbol) in QUALITY_BASES:
        score += 2

    live_tag = "LIVE" if not dead_now and (ch3m >= HOT_MIN_LIVE_MOVE_3M or rr1 >= 1.0 or vr1 >= 1.0) else "STALE"
    return score, (
        f"{live_tag}: 1m3 {ch3m_signed*100:+.2f}%, 15m {ch15m*100:.2f}%, 30m {ch30m*100:.2f}%, "
        f"vol1 x{vr1:.2f}, vol15 x{vr15:.2f}, range1 x{rr1:.2f}, range5 x{rr5:.2f}"
    )

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
    }

    if candle_move > FAST_MAX_SPREAD_PROXY:
        return False, f"last 5m candle too wide/chase risk {candle_move*100:.2f}%", metrics

    if compression > EDGE_MIN_PRIOR_COMPRESSION and rr < 1.75:  # disabled by default in V13.16 unless env lowers EDGE_MIN_PRIOR_COMPRESSION
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
        if ch15m < FAST_MIN_15M_MOVE:
            return False, f"slow LONG 15m {ch15m*100:.2f}%", metrics
        if ch30m < FAST_MIN_30M_MOVE:
            return False, f"slow LONG 30m {ch30m*100:.2f}%", metrics
        if ch30m > FAST_MAX_30M_MOVE:
            return False, f"late LONG chase 30m {ch30m*100:.2f}%", metrics
        if ch3m_1m < FAST_MIN_1M_CONFIRM:
            return False, f"no 1m acceleration LONG {ch3m_1m*100:.2f}%", metrics
        if last["close"] <= last["open"]:
            return False, "last 5m not bullish", metrics
        if close_location(last) < CLOSE_LOCATION_MIN_LONG:
            return False, f"LONG close location weak {close_location(last):.2f}", metrics
    else:
        if ch15m > -FAST_MIN_15M_MOVE:
            return False, f"slow SHORT 15m {ch15m*100:.2f}%", metrics
        if ch30m > -FAST_MIN_30M_MOVE:
            return False, f"slow SHORT 30m {ch30m*100:.2f}%", metrics
        if ch30m < -FAST_MAX_30M_MOVE:
            return False, f"late SHORT chase 30m {ch30m*100:.2f}%", metrics
        if ch3m_1m > -FAST_MIN_1M_CONFIRM:
            return False, f"no 1m acceleration SHORT {ch3m_1m*100:.2f}%", metrics
        if last["close"] >= last["open"]:
            return False, "last 5m not bearish", metrics
        if close_location(last) > CLOSE_LOCATION_MAX_SHORT:
            return False, f"SHORT close location weak {close_location(last):.2f}", metrics

    if rr < FAST_MIN_RANGE_RATIO:
        return False, f"range expansion weak x{rr:.2f}", metrics
    if vol < FAST_MIN_VOLUME_RATIO:
        return False, f"volume weak x{vol:.2f}", metrics

    return True, (
        f"edge ok: 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, "
        f"1m3 {ch3m_1m*100:+.2f}%, range x{rr:.2f}, vol x{vol:.2f}, "
        f"compression x{compression:.2f}; {micro_reason}; {pressure_reason}; {sweep_reason}; {feasible_reason}"
    ), metrics


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

    # Market phase is not traded. BTC is only a shock filter; we avoid signals during violent BTC moves.
    btc_ch1h = float(btc.get("ch1h", 0.0))
    if abs(btc_ch1h) >= BTC_SHOCK_15M_BLOCK:
        return None

    fast_ok, fast_reason, metrics = fast_context_ok(c1, c5, c15, side, vol)
    if not fast_ok:
        return None

    last5 = c5[-1]
    prev5 = c5[-2]

    if side == "LONG":
        recent_high = max(x["high"] for x in c5[-18:])
        pullback_low = min(x["low"] for x in c5[-10:])
        pullback = (recent_high - pullback_low) / max(recent_high, 1e-12)
        if pullback < PULLBACK_MIN or pullback > PULLBACK_MAX:
            return None
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
            f"Свежий дисбаланс вверх, микро-откат {pullback*100:.2f}%, sweep/reclaim, "
            f"возврат выше 1m/5m EMA и VWAP, немедленное продолжение. {fast_reason}."
        )
    else:
        recent_low = min(x["low"] for x in c5[-18:])
        bounce_high = max(x["high"] for x in c5[-10:])
        pullback = (bounce_high - recent_low) / max(recent_low, 1e-12)
        if pullback < PULLBACK_MIN or pullback > PULLBACK_MAX:
            return None
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
            f"Свежий дисбаланс вниз, микро-отскок {pullback*100:.2f}%, sweep/reject, "
            f"возврат ниже 1m/5m EMA и VWAP, немедленное продолжение. {fast_reason}."
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
        "t1h": t1h,
        "btc_text": btc.get("text", ""),
    }


def calculate_fast_trade(setup: Dict[str, Any], c1: List[Dict[str, float]], c5: List[Dict[str, float]]) -> Optional[Dict[str, Any]]:
    side = setup["side"]
    entry = setup["entry"]
    level = setup["level"]
    a = atr(c5, 14)
    buffer = max(entry * 0.0022, a * SL_ATR_MULT)

    if side == "LONG":
        recent_low = min(x["low"] for x in c1[-15:] + c5[-4:])
        sl = min(level, recent_low) - buffer
        sl = min(sl, entry * (1 - MIN_SL_MOVE))
        tp1 = entry * (1 + TP1_MOVE)
        tp2 = entry * (1 + TP2_MOVE)
        tp3 = entry * (1 + TP3_MOVE)
        tp4 = entry * (1 + TP4_MOVE)
        tp5 = entry * (1 + TP5_MOVE)
    else:
        recent_high = max(x["high"] for x in c1[-15:] + c5[-4:])
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
        setup = fast_burst_setup(symbol, c1, c5, c15, c1h, btc, side)
        if not setup:
            blocks[f"no_fast_{side.lower()}"] = blocks.get(f"no_fast_{side.lower()}", 0) + 1
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

        # Professional gate: if TP5 cannot compensate structure risk, skip.
        if trade["final_rr"] < 0.70 or trade["ladder_rr"] < 0.35:
            blocks["weak_ladder_rr_block"] = blocks.get("weak_ladder_rr_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: ladderRR {trade['ladder_rr']:.2f}, finalRR {trade['final_rr']:.2f}")
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
        f"🧪 Диагностика V13.18 Live Market Scalper\n"
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

    found.sort(key=lambda x: (x["grade"] == "A+", x["score"], x["ladder_rr"]), reverse=True)
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
        f"Mode: LIVE MARKET SCALPER.\n"
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
