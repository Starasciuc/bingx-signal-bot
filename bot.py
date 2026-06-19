import os
import time
import json
import math
import random
import asyncio
from typing import Optional, List, Dict, Any, Tuple
from collections import Counter, defaultdict

import requests
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

APP_NAME = "Professional Adaptive Futures Bot AUTO V13.0 PROFESSIONAL LEVEL TRADER"
DEPLOY_MARKER = "V13_0_PROFESSIONAL_LEVEL_TRADER_2026_06_19"

app = FastAPI(title=APP_NAME)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
BINGX_BASE_URL = "https://open-api.bingx.com"

STATE_FILE = os.getenv("STATE_FILE", "bot_state_v13_0.json")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
LEVERAGE = float(os.getenv("LEVERAGE", "10"))

AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "75"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "20"))
DIAG_SECONDS = int(os.getenv("DIAG_SECONDS", "1800"))

A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "90"))
B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "85"))
MIN_TP1_ROI_X10 = float(os.getenv("MIN_TP1_ROI_X10", "10"))
MIN_TP1_PRICE_MOVE = MIN_TP1_ROI_X10 / max(LEVERAGE, 1)  # 10% ROI at x10 = 1% price move

MAX_SIGNALS_PER_DAY = int(os.getenv("MAX_SIGNALS_PER_DAY", "3"))
MAX_ACTIVE_SIGNALS = int(os.getenv("MAX_ACTIVE_SIGNALS", "2"))
PAIR_COOLDOWN_SECONDS = int(os.getenv("PAIR_COOLDOWN_SECONDS", "7200"))
STRATEGY_COOLDOWN_SECONDS = int(os.getenv("STRATEGY_COOLDOWN_SECONDS", "1800"))

MAX_ANALYZE_SYMBOLS = int(os.getenv("MAX_ANALYZE_SYMBOLS", "130"))
MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "500"))
DYNAMIC_MOVERS_LIMIT = int(os.getenv("DYNAMIC_MOVERS_LIMIT", "80"))

# Level-trading guardrails.
NEAR_LEVEL_MAX_PCT = float(os.getenv("NEAR_LEVEL_MAX_PCT", "0.85"))
ANTI_CHASE_MOVE_PCT = float(os.getenv("ANTI_CHASE_MOVE_PCT", "6.0"))
ANTI_CHASE_LOOKBACK_15M = int(os.getenv("ANTI_CHASE_LOOKBACK_15M", "8"))
REBOUND_FOR_LATE_SHORT_PCT = float(os.getenv("REBOUND_FOR_LATE_SHORT_PCT", "1.4"))
PULLBACK_FOR_LATE_LONG_PCT = float(os.getenv("PULLBACK_FOR_LATE_LONG_PCT", "1.4"))
MIN_VOLUME_RATIO_A = float(os.getenv("MIN_VOLUME_RATIO_A", "1.12"))
MIN_VOLUME_RATIO_B = float(os.getenv("MIN_VOLUME_RATIO_B", "0.95"))
MIN_RR_A = float(os.getenv("MIN_RR_A", "1.15"))
MIN_RR_B = float(os.getenv("MIN_RR_B", "0.90"))

QUALITY_SYMBOLS = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT", "LINK-USDT", "AVAX-USDT",
    "AAVE-USDT", "SUI-USDT", "TAO-USDT", "NEAR-USDT", "INJ-USDT", "APT-USDT", "ARB-USDT",
    "OP-USDT", "ADA-USDT", "DOGE-USDT", "DOT-USDT", "LTC-USDT", "UNI-USDT", "TRX-USDT",
    "FIL-USDT", "ETC-USDT", "ATOM-USDT", "WLD-USDT", "SEI-USDT", "TIA-USDT", "TONCOIN-USDT",
]

ULTRA_RISK_KEYWORDS = [
    "1000", "PEPE", "BONK", "WIF", "MEME", "DOGS", "CAT", "HMSTR", "GOBLIN", "FART", "BABY",
    "MOODENG", "PNUT", "GOAT", "BOME", "TURBO", "PUMP", "SHIB", "FLOKI", "MOG", "SATS",
]


def now_ts() -> int:
    return int(time.time())


def normalize_symbol(symbol: str) -> str:
    s = (symbol or "").upper().replace("/", "-").replace("_", "-")
    if s.endswith("USDT") and "-" not in s:
        s = s[:-4] + "-USDT"
    return s


def display_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-", "/")


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def load_state() -> Dict[str, Any]:
    base = {
        "active_signals": {},
        "closed_signals": [],
        "stats": {
            "profit": 0,
            "sl": 0,
            "by_side": {},
            "by_grade": {},
            "by_strategy": {},
            "by_symbol": {},
            "by_setup": {},
        },
        "blocks": {},
        "cooldowns": {},
        "auto": {
            "last_scan_time": 0,
            "last_track_time": 0,
            "last_diag_time": 0,
            "last_error": "",
            "last_scan_result": {},
        },
        "signals_today": {"date": "", "count": 0},
    }
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            # shallow merge to keep compatibility
            for k, v in base.items():
                data.setdefault(k, v)
            data["stats"].setdefault("by_setup", {})
            return data
    except Exception:
        pass
    return base


STATE = load_state()


def save_state() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
    except Exception as e:
        STATE.setdefault("auto", {})["last_error"] = f"save_state: {e}"


def day_key() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def reset_daily_counter_if_needed() -> None:
    d = day_key()
    if STATE.get("signals_today", {}).get("date") != d:
        STATE["signals_today"] = {"date": d, "count": 0}


def stat_bucket(name: str, key: str) -> Dict[str, int]:
    b = STATE["stats"].setdefault(name, {})
    if key not in b:
        b[key] = {"profit": 0, "sl": 0}
    return b[key]


def wr_text(profit: int, sl: int) -> str:
    total = profit + sl
    wr = (profit / total * 100.0) if total else 0.0
    return f"{profit} профит / {sl} SL / WR {wr:.1f}%"


def build_stats_text() -> str:
    st = STATE.get("stats", {})
    profit = int(st.get("profit", 0))
    sl = int(st.get("sl", 0))
    total = profit + sl
    wr = profit / total * 100 if total else 0
    lines = [
        "📊 <b>Статистика</b>",
        f"Итого: {profit} профит / {sl} SL / WR {wr:.1f}%",
    ]
    for title, key in [("Стороны", "by_side"), ("Классы", "by_grade"), ("Стратегии", "by_strategy"), ("Типы", "by_setup")]:
        data = st.get(key, {}) or {}
        if data:
            lines.append(f"\n<b>{title}:</b>")
            for k, v in sorted(data.items()):
                lines.append(f"{k}: {wr_text(int(v.get('profit', 0)), int(v.get('sl', 0)))}")
    return "\n".join(lines)


def update_stats(signal: Dict[str, Any], result: str) -> None:
    positive = result == "profit"
    STATE["stats"]["profit" if positive else "sl"] = int(STATE["stats"].get("profit" if positive else "sl", 0)) + 1
    fields = [
        ("by_side", signal.get("side", "?")),
        ("by_grade", signal.get("grade", "?")),
        ("by_strategy", signal.get("strategy", "?")),
        ("by_symbol", signal.get("symbol", "?")),
        ("by_setup", signal.get("setup_type", "?")),
    ]
    for bucket, key in fields:
        b = stat_bucket(bucket, key)
        b["profit" if positive else "sl"] = int(b.get("profit" if positive else "sl", 0)) + 1
    save_state()


def get_wr_for_strategy_side(strategy: str, side: str, grade: str = "") -> Tuple[int, int, float]:
    # Stats in this clean bot are mostly broad; block by strategy+side in closed list.
    profit = sl = 0
    for s in STATE.get("closed_signals", [])[-200:]:
        if s.get("strategy") == strategy and s.get("side") == side and (not grade or s.get("grade") == grade):
            if s.get("result") == "profit":
                profit += 1
            elif s.get("result") == "sl":
                sl += 1
    total = profit + sl
    wr = profit / total * 100 if total else 50.0
    return profit, sl, wr


def adaptive_allows(strategy: str, side: str, grade: str) -> Tuple[bool, str]:
    # Block only with enough evidence, not after 1 unlucky trade.
    profit, sl, wr = get_wr_for_strategy_side(strategy, side)
    if profit + sl >= 5 and wr < 35:
        return False, f"adaptive_block: {strategy} {side} WR {wr:.1f}% после {profit+sl} сделок"
    if grade == "B":
        p2, s2, wr2 = get_wr_for_strategy_side(strategy, side, "B")
        if p2 + s2 >= 4 and wr2 < 40:
            return False, f"adaptive_block: B {strategy} {side} WR {wr2:.1f}%"
    return True, ""


def send_telegram_message(text: str) -> Dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не указаны"}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text[:3900],
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_json(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    url = path if path.startswith("http") else f"{BINGX_BASE_URL}{path}"
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        return r.json()
    except Exception as e:
        STATE["auto"]["last_error"] = f"get_json {path}: {e}"
        return None


_KLINE_CACHE: Dict[Tuple[str, str, int], Tuple[int, Optional[List[Dict[str, float]]]]] = {}
_TICKER_CACHE: Tuple[int, Dict[str, Dict[str, Any]]] = (0, {})
_SYMBOL_CACHE: Tuple[int, List[str]] = (0, [])


def get_klines(symbol: str, interval: str, limit: int = 240, ttl: int = 40) -> Optional[List[Dict[str, float]]]:
    symbol = normalize_symbol(symbol)
    key = (symbol, interval, limit)
    ts, cached = _KLINE_CACHE.get(key, (0, None))
    if now_ts() - ts < ttl:
        return cached
    data = get_json("/openApi/swap/v3/quote/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    raw = (data or {}).get("data") or []
    candles = []
    for c in raw:
        try:
            candles.append({
                "time": int(c.get("time") or c.get("T") or 0),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c.get("volume", 0)),
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["time"])
    result = candles if len(candles) >= max(30, min(limit // 3, 80)) else None
    _KLINE_CACHE[key] = (now_ts(), result)
    return result


def get_tickers() -> Dict[str, Dict[str, Any]]:
    global _TICKER_CACHE
    ts, cached = _TICKER_CACHE
    if now_ts() - ts < 45 and cached:
        return cached
    data = get_json("/openApi/swap/v2/quote/ticker")
    out = {}
    raw = (data or {}).get("data") or []
    if isinstance(raw, dict):
        raw = [raw]
    for t in raw:
        sym = normalize_symbol(str(t.get("symbol", "")))
        if not sym.endswith("-USDT"):
            continue
        out[sym] = t
    _TICKER_CACHE = (now_ts(), out)
    return out


def is_ultra_risk(symbol: str, candles15: Optional[List[Dict[str, float]]] = None) -> bool:
    s = normalize_symbol(symbol).replace("-USDT", "")
    if any(k in s for k in ULTRA_RISK_KEYWORDS):
        return True
    if candles15 and len(candles15) >= 20:
        # block coins with insane candle bodies/wicks.
        for c in candles15[-12:]:
            if c["open"] > 0 and (c["high"] - c["low"]) / c["open"] * 100 >= 9:
                return True
        if candles15[-1]["close"] > 0:
            recent = pct(candles15[-1]["close"], candles15[-12]["close"])
            if abs(recent) >= 22:
                return True
    return False


def get_symbols() -> List[str]:
    global _SYMBOL_CACHE
    ts, cached = _SYMBOL_CACHE
    if now_ts() - ts < 300 and cached:
        return cached
    data = get_json("/openApi/swap/v2/quote/contracts")
    result = []
    for item in (data or {}).get("data", []) or []:
        sym = normalize_symbol(str(item.get("symbol", "")))
        if sym.endswith("-USDT") and not any(x in sym for x in ["USDC", "BUSD"]):
            result.append(sym)
    if not result:
        result = QUALITY_SYMBOLS[:]
    result = list(dict.fromkeys(result))[:MAX_CONTRACTS]
    _SYMBOL_CACHE = (now_ts(), result)
    return result


def get_scan_universe() -> List[str]:
    tickers = get_tickers()
    all_symbols = get_symbols()
    dynamic = []
    for sym, t in tickers.items():
        if sym not in all_symbols:
            continue
        chg = safe_float(t.get("priceChangePercent") or t.get("priceChangeRate") or t.get("changeRate"), 0)
        # Some APIs return 0.12 for 12%, some return 12. Normalize approximately.
        chg_abs = abs(chg * 100 if abs(chg) < 1 else chg)
        quote_vol = safe_float(t.get("quoteVolume") or t.get("volume") or 0)
        if chg_abs >= 0.6:
            dynamic.append((chg_abs, quote_vol, sym))
    dynamic.sort(reverse=True)
    quality = [s for s in QUALITY_SYMBOLS if s in all_symbols]
    dyn = [s for _, _, s in dynamic[:DYNAMIC_MOVERS_LIMIT]]
    rest = [s for s in all_symbols if s not in set(quality + dyn)]
    random.shuffle(rest)
    return list(dict.fromkeys(quality + dyn + rest))[:MAX_ANALYZE_SYMBOLS]


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


def atr(candles: List[Dict[str, float]], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return 0.0
    return sum(trs[-period:]) / min(period, len(trs))


def vwap(candles: List[Dict[str, float]], lookback: int = 48) -> float:
    sub = candles[-lookback:]
    pv = sum(((x["high"] + x["low"] + x["close"]) / 3) * max(x["volume"], 0) for x in sub)
    vol = sum(max(x["volume"], 0) for x in sub)
    return pv / vol if vol > 0 else (sum(closes(sub)) / len(sub) if sub else 0.0)


def volume_ratio(candles: List[Dict[str, float]], lookback: int = 30) -> float:
    if len(candles) < 5:
        return 1.0
    last = candles[-1]["volume"]
    prev = [x["volume"] for x in candles[-lookback-1:-1] if x["volume"] > 0]
    avg = sum(prev) / len(prev) if prev else last
    return last / avg if avg else 1.0


def body_ratio(c: Dict[str, float]) -> float:
    rng = c["high"] - c["low"]
    if rng <= 0:
        return 0.0
    return abs(c["close"] - c["open"]) / rng


def upper_wick_ratio(c: Dict[str, float]) -> float:
    rng = c["high"] - c["low"]
    if rng <= 0:
        return 0.0
    return (c["high"] - max(c["open"], c["close"])) / rng


def lower_wick_ratio(c: Dict[str, float]) -> float:
    rng = c["high"] - c["low"]
    if rng <= 0:
        return 0.0
    return (min(c["open"], c["close"]) - c["low"]) / rng


def two_candle_confirmation(candles: List[Dict[str, float]], side: str) -> bool:
    if len(candles) < 3:
        return False
    a, b = candles[-2], candles[-1]
    if side == "LONG":
        return a["close"] > a["open"] and b["close"] > b["open"] and b["close"] >= a["close"] and upper_wick_ratio(b) < 0.55
    return a["close"] < a["open"] and b["close"] < b["open"] and b["close"] <= a["close"] and lower_wick_ratio(b) < 0.55


def htf_confirmation(candles15: List[Dict[str, float]], side: str) -> bool:
    if len(candles15) < 5:
        return False
    last = candles15[-1]
    e20 = ema(closes(candles15[-40:]), 20)
    vw = vwap(candles15, 48)
    if side == "LONG":
        return last["close"] > e20 and last["close"] > vw * 0.995 and lower_wick_ratio(last) > 0.10
    return last["close"] < e20 and last["close"] < vw * 1.005 and upper_wick_ratio(last) > 0.10


def find_pivots(candles: List[Dict[str, float]], left: int = 2, right: int = 2) -> Tuple[List[float], List[float]]:
    highs, lows = [], []
    if len(candles) < left + right + 3:
        return highs, lows
    for i in range(left, len(candles) - right):
        h = candles[i]["high"]
        l = candles[i]["low"]
        if all(h >= candles[j]["high"] for j in range(i-left, i+right+1) if j != i):
            highs.append(h)
        if all(l <= candles[j]["low"] for j in range(i-left, i+right+1) if j != i):
            lows.append(l)
    return highs[-12:], lows[-12:]


def cluster_levels(levels: List[float], tolerance_pct: float = 0.55) -> List[Tuple[float, int]]:
    clusters: List[List[float]] = []
    for lv in sorted([x for x in levels if x > 0]):
        placed = False
        for cl in clusters:
            center = sum(cl) / len(cl)
            if abs(pct(lv, center)) <= tolerance_pct:
                cl.append(lv)
                placed = True
                break
        if not placed:
            clusters.append([lv])
    out = [(sum(cl) / len(cl), len(cl)) for cl in clusters]
    out.sort(key=lambda x: x[1], reverse=True)
    return out[:10]


def get_levels(c1h: List[Dict[str, float]], c4h: List[Dict[str, float]], price: float) -> Dict[str, Any]:
    h1, l1 = find_pivots(c1h[-120:], 2, 2)
    h4, l4 = find_pivots(c4h[-100:], 2, 2)
    high_levels = cluster_levels(h1 + h4, 0.65)
    low_levels = cluster_levels(l1 + l4, 0.65)
    supports = sorted([(lv, touches) for lv, touches in low_levels if lv < price], key=lambda x: price - x[0])
    resistances = sorted([(lv, touches) for lv, touches in high_levels if lv > price], key=lambda x: x[0] - price)
    return {
        "support": supports[0] if supports else (min([x["low"] for x in c1h[-48:]]), 1),
        "resistance": resistances[0] if resistances else (max([x["high"] for x in c1h[-48:]]), 1),
        "supports": supports[:4],
        "resistances": resistances[:4],
    }


def classify_trend(candles: List[Dict[str, float]]) -> str:
    vals = closes(candles)
    if len(vals) < 60:
        return "UNKNOWN"
    e20 = ema(vals[-80:], 20)
    e50 = ema(vals[-100:], 50)
    last = vals[-1]
    move = pct(last, vals[-12]) if len(vals) >= 12 else 0
    if last > e20 > e50 and move > 0.4:
        return "UP"
    if last < e20 < e50 and move < -0.4:
        return "DOWN"
    return "RANGE"


def btc_context() -> Dict[str, Any]:
    c15 = get_klines("BTC-USDT", "15m", 120, ttl=30) or []
    c1h = get_klines("BTC-USDT", "1h", 120, ttl=60) or []
    c4h = get_klines("BTC-USDT", "4h", 120, ttl=120) or []
    ctx = {"ok": False, "regime": "UNKNOWN", "strong_up": False, "strong_down": False, "text": "BTC data unavailable"}
    if len(c15) < 20 or len(c1h) < 30:
        return ctx
    m15 = pct(c15[-1]["close"], c15[-8]["close"])
    h1 = pct(c1h[-1]["close"], c1h[-6]["close"])
    t1 = classify_trend(c1h)
    t4 = classify_trend(c4h) if len(c4h) >= 60 else "UNKNOWN"
    strong_down = m15 <= -0.8 or h1 <= -1.8 or (t1 == "DOWN" and h1 < -0.8)
    strong_up = m15 >= 0.8 or h1 >= 1.8 or (t1 == "UP" and h1 > 0.8)
    if strong_down:
        regime = "IMPULSE_DOWN" if m15 <= -1.2 else "TREND_DOWN"
    elif strong_up:
        regime = "IMPULSE_UP" if m15 >= 1.2 else "TREND_UP"
    elif t1 == "RANGE":
        regime = "RANGE"
    else:
        regime = t1
    return {
        "ok": True,
        "regime": regime,
        "strong_up": strong_up,
        "strong_down": strong_down,
        "t1h": t1,
        "t4h": t4,
        "m15": m15,
        "h1": h1,
        "text": f"BTC {regime}: 15m {m15:+.2f}%, 6h {h1:+.2f}%, 1H {t1}, 4H {t4}",
    }


def recent_move(candles: List[Dict[str, float]], lookback: int) -> float:
    if len(candles) <= lookback:
        return 0.0
    return pct(candles[-1]["close"], candles[-lookback]["close"])


def rebound_from_recent_low(candles: List[Dict[str, float]], lookback: int = 12) -> float:
    sub = candles[-lookback:]
    low = min(x["low"] for x in sub) if sub else 0
    return pct(candles[-1]["close"], low) if low else 0


def pullback_from_recent_high(candles: List[Dict[str, float]], lookback: int = 12) -> float:
    sub = candles[-lookback:]
    high = max(x["high"] for x in sub) if sub else 0
    return pct(candles[-1]["close"], high) if high else 0


def build_signal(symbol: str, side: str, strategy: str, setup_type: str, grade: str, score: int,
                 entry: float, sl: float, tp1: float, tp2: float, tp3: float,
                 reason: str, btc: Dict[str, Any], levels: Dict[str, Any], risk_mult: float) -> Dict[str, Any]:
    sid = f"{normalize_symbol(symbol)}-{side}-{int(time.time())}-{random.randint(100,999)}"
    move_tp1 = abs(pct(tp1, entry))
    risk_pct = abs(pct(sl, entry))
    return {
        "id": sid,
        "symbol": normalize_symbol(symbol),
        "side": side,
        "strategy": strategy,
        "setup_type": setup_type,
        "grade": grade,
        "score": score,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "risk_multiplier": risk_mult,
        "opened_at": now_ts(),
        "status": "active",
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "reason": reason,
        "btc_text": btc.get("text", ""),
        "support": levels.get("support", [0])[0],
        "resistance": levels.get("resistance", [0])[0],
        "tp1_roi_x10": move_tp1 * LEVERAGE,
        "risk_roi_x10": risk_pct * LEVERAGE,
    }


def signal_message(sig: Dict[str, Any]) -> str:
    direction = "🟢 LONG" if sig["side"] == "LONG" else "🔴 SHORT"
    risk_note = "меньший риск" if sig["grade"] == "B" else "основной риск"
    return "\n".join([
        f"{direction} <b>{display_symbol(sig['symbol'])}</b>",
        f"Класс: <b>{sig['grade']}</b> · Score {sig['score']} · {sig['setup_type']}",
        f"Стратегия: <b>{sig['strategy']}</b>",
        "",
        f"Вход: <code>{sig['entry']:.8g}</code>",
        f"TP1: <code>{sig['tp1']:.8g}</code> · ≈ {sig['tp1_roi_x10']:.1f}% ROI при x{LEVERAGE:g}",
        f"TP2: <code>{sig['tp2']:.8g}</code>",
        f"TP3: <code>{sig['tp3']:.8g}</code>",
        f"SL: <code>{sig['sl']:.8g}</code> · риск до SL ≈ {sig['risk_roi_x10']:.1f}% ROI при x{LEVERAGE:g}",
        f"Риск: {risk_note} · multiplier x{sig.get('risk_multiplier', 1):.2f}",
        "",
        "📌 <b>Профессиональная логика уровня:</b>",
        sig.get("reason", ""),
        "",
        f"BTC: {sig.get('btc_text', '')}",
        f"Support: {sig.get('support', 0):.8g} · Resistance: {sig.get('resistance', 0):.8g}",
        "\nСделка не является гарантией прибыли. SL возможен; размер позиции должен соответствовать риску.",
    ])


def analyze_symbol(symbol: str, btc: Dict[str, Any], counters: Counter) -> Optional[Dict[str, Any]]:
    symbol = normalize_symbol(symbol)
    c15 = get_klines(symbol, "15m", 180)
    if not c15:
        counters["no_klines_15m"] += 1
        return None
    if is_ultra_risk(symbol, c15):
        counters["ultra_risk_block"] += 1
        return None
    c5 = get_klines(symbol, "5m", 160)
    c1h = get_klines(symbol, "1h", 180)
    c4h = get_klines(symbol, "4h", 120)
    if not c5 or not c1h or not c4h:
        counters["no_klines_htf"] += 1
        return None

    price = c5[-1]["close"]
    if price <= 0:
        counters["bad_price"] += 1
        return None

    levels = get_levels(c1h, c4h, price)
    support, support_touches = levels["support"]
    resistance, resistance_touches = levels["resistance"]
    atr15 = atr(c15, 14)
    atr_pct = atr15 / price * 100 if price else 0
    volr = volume_ratio(c15, 30)
    t1 = classify_trend(c1h)
    t4 = classify_trend(c4h)
    move2h = recent_move(c15, ANTI_CHASE_LOOKBACK_15M)
    rebound = rebound_from_recent_low(c15, ANTI_CHASE_LOOKBACK_15M)
    pullback = pullback_from_recent_high(c15, ANTI_CHASE_LOOKBACK_15M)
    e5 = ema(closes(c5[-80:]), 20)
    e15 = ema(closes(c15[-80:]), 20)
    vw15 = vwap(c15, 48)
    last15 = c15[-1]
    last5 = c5[-1]

    candidates: List[Dict[str, Any]] = []

    # LONG: professional level bounce/reclaim from support, not chasing after a pump.
    dist_support = abs(pct(price, support)) if support else 999
    support_sweep = min(x["low"] for x in c15[-4:]) < support * (1 - max(0.0015, atr_pct/100*0.25)) and price > support
    holding_support = dist_support <= max(NEAR_LEVEL_MAX_PCT, atr_pct * 1.2) and price > support
    long_reclaim = price > e5 and price > e15 * 0.995 and price > vw15 * 0.992
    long_confirm = two_candle_confirmation(c5, "LONG") and htf_confirmation(c15, "LONG")
    no_late_long = not (move2h > ANTI_CHASE_MOVE_PCT and pullback > -PULLBACK_FOR_LATE_LONG_PCT)
    btc_allows_long = not btc.get("strong_down")
    htf_allows_long = t4 != "DOWN" and (t1 in ["UP", "RANGE"] or support_sweep)

    if btc_allows_long and htf_allows_long and no_late_long and (support_sweep or holding_support) and long_reclaim and long_confirm:
        sl = min(support, min(x["low"] for x in c15[-5:])) - max(atr15 * 0.55, price * 0.0035)
        # target to resistance; require enough room. If resistance too close, use measured move but still level-aware.
        natural_tp1 = min(resistance * 0.995, price * (1 + max(MIN_TP1_PRICE_MOVE/100, 0.012)))
        if natural_tp1 <= price:
            natural_tp1 = price * 1.012
        tp1 = natural_tp1
        tp2 = min(resistance * 1.002, price + (tp1 - price) * 1.8) if resistance > price else price + (tp1 - price) * 1.8
        tp3 = price + (tp1 - price) * 2.8
        risk = price - sl
        reward = tp1 - price
        rr = reward / risk if risk > 0 else 0
        roi = abs(pct(tp1, price)) * LEVERAGE
        score = 50
        score += 12 if support_sweep else 6
        score += 10 if support_touches >= 2 else 4
        score += 10 if t1 == "UP" else 5 if t1 == "RANGE" else 0
        score += 8 if t4 != "DOWN" else 0
        score += 8 if volr >= MIN_VOLUME_RATIO_A else 4 if volr >= MIN_VOLUME_RATIO_B else 0
        score += 8 if rr >= MIN_RR_A else 3 if rr >= MIN_RR_B else 0
        score += 6 if lower_wick_ratio(last15) > 0.25 else 0
        score += 5 if btc.get("regime") in ["TREND_UP", "RANGE", "UP"] else 0
        if roi >= MIN_TP1_ROI_X10 and rr >= MIN_RR_B:
            grade = "A+" if score >= A_PLUS_MIN_SCORE and rr >= MIN_RR_A and volr >= MIN_VOLUME_RATIO_A else "B" if score >= B_MIN_SCORE else "LOW"
            if grade != "LOW":
                reason = (
                    f"Цена работает от поддержки {support:.8g}: sweep/удержание уровня, возврат выше EMA/VWAP. "
                    f"Это не покупка вершины: движение за 2ч {move2h:+.2f}%, откат от high {pullback:+.2f}%. "
                    f"1H {t1}, 4H {t4}, volume x{volr:.2f}, RR {rr:.2f}."
                )
                candidates.append(build_signal(symbol, "LONG", "PRO_LEVEL_RECLAIM", "LEVEL BOUNCE / RECLAIM", grade, int(score), price, sl, tp1, tp2, tp3, reason, btc, levels, 1.0 if grade == "A+" else 0.45))
        else:
            counters["long_tp_rr_block"] += 1
    else:
        if not btc_allows_long: counters["btc_long_block"] += 1
        elif not htf_allows_long: counters["htf_long_block"] += 1
        elif not no_late_long: counters["anti_chase_long_block"] += 1
        elif not (support_sweep or holding_support): counters["no_support_level_long"] += 1
        elif not long_confirm: counters["no_confirm_long"] += 1

    # SHORT: professional resistance reject/retest, not shorting a dump at the bottom.
    dist_res = abs(pct(price, resistance)) if resistance else 999
    resistance_sweep = max(x["high"] for x in c15[-4:]) > resistance * (1 + max(0.0015, atr_pct/100*0.25)) and price < resistance
    rejecting_resistance = dist_res <= max(NEAR_LEVEL_MAX_PCT, atr_pct * 1.2) and price < resistance
    short_reject = price < e5 and price < e15 * 1.005 and price < vw15 * 1.008
    short_confirm = two_candle_confirmation(c5, "SHORT") and htf_confirmation(c15, "SHORT")
    # If already dumped, short only after meaningful rebound to resistance/EMA and rejection.
    no_late_short = not (move2h < -ANTI_CHASE_MOVE_PCT and rebound < REBOUND_FOR_LATE_SHORT_PCT)
    btc_allows_short = not btc.get("strong_up")
    htf_allows_short = t4 != "UP" and (t1 in ["DOWN", "RANGE"] or resistance_sweep)

    if btc_allows_short and htf_allows_short and no_late_short and (resistance_sweep or rejecting_resistance) and short_reject and short_confirm:
        sl = max(resistance, max(x["high"] for x in c15[-5:])) + max(atr15 * 0.55, price * 0.0035)
        natural_tp1 = max(support * 1.005, price * (1 - max(MIN_TP1_PRICE_MOVE/100, 0.012)))
        if natural_tp1 >= price:
            natural_tp1 = price * 0.988
        tp1 = natural_tp1
        tp2 = max(support * 0.998, price - (price - tp1) * 1.8) if support < price else price - (price - tp1) * 1.8
        tp3 = price - (price - tp1) * 2.8
        risk = sl - price
        reward = price - tp1
        rr = reward / risk if risk > 0 else 0
        roi = abs(pct(tp1, price)) * LEVERAGE
        score = 50
        score += 12 if resistance_sweep else 6
        score += 10 if resistance_touches >= 2 else 4
        score += 10 if t1 == "DOWN" else 5 if t1 == "RANGE" else 0
        score += 8 if t4 != "UP" else 0
        score += 8 if volr >= MIN_VOLUME_RATIO_A else 4 if volr >= MIN_VOLUME_RATIO_B else 0
        score += 8 if rr >= MIN_RR_A else 3 if rr >= MIN_RR_B else 0
        score += 6 if upper_wick_ratio(last15) > 0.25 else 0
        score += 5 if btc.get("regime") in ["TREND_DOWN", "IMPULSE_DOWN", "RANGE", "DOWN"] else 0
        if roi >= MIN_TP1_ROI_X10 and rr >= MIN_RR_B:
            grade = "A+" if score >= A_PLUS_MIN_SCORE and rr >= MIN_RR_A and volr >= MIN_VOLUME_RATIO_A else "B" if score >= B_MIN_SCORE else "LOW"
            if grade != "LOW":
                reason = (
                    f"Цена работает от сопротивления {resistance:.8g}: sweep/reject или удержание ниже уровня, возврат ниже EMA/VWAP. "
                    f"Это не шорт дна: движение за 2ч {move2h:+.2f}%, отскок от low {rebound:+.2f}%. "
                    f"1H {t1}, 4H {t4}, volume x{volr:.2f}, RR {rr:.2f}."
                )
                candidates.append(build_signal(symbol, "SHORT", "PRO_LEVEL_REJECT", "LEVEL REJECT / RETEST", grade, int(score), price, sl, tp1, tp2, tp3, reason, btc, levels, 1.0 if grade == "A+" else 0.45))
        else:
            counters["short_tp_rr_block"] += 1
    else:
        if not btc_allows_short: counters["btc_short_block"] += 1
        elif not htf_allows_short: counters["htf_short_block"] += 1
        elif not no_late_short: counters["anti_chase_short_block"] += 1
        elif not (resistance_sweep or rejecting_resistance): counters["no_resistance_level_short"] += 1
        elif not short_confirm: counters["no_confirm_short"] += 1

    if not candidates:
        counters["no_candidate"] += 1
        return None
    candidates.sort(key=lambda s: (s["grade"] == "A+", s["score"], s["tp1_roi_x10"]), reverse=True)
    best = candidates[0]
    ok, reason = adaptive_allows(best["strategy"], best["side"], best["grade"])
    if not ok:
        counters["adaptive_block"] += 1
        counters[reason] += 1
        return None
    return best


def can_send_signal(sig: Dict[str, Any], counters: Counter) -> Tuple[bool, str]:
    reset_daily_counter_if_needed()
    if STATE["signals_today"].get("count", 0) >= MAX_SIGNALS_PER_DAY:
        counters["daily_limit_block"] += 1
        return False, "daily signal limit"
    if len(STATE.get("active_signals", {})) >= MAX_ACTIVE_SIGNALS:
        counters["active_limit_block"] += 1
        return False, "active signal limit"
    cd = STATE.setdefault("cooldowns", {})
    pair_key = f"pair:{sig['symbol']}"
    strat_key = f"strategy:{sig['strategy']}:{sig['side']}"
    t = now_ts()
    if cd.get(pair_key, 0) > t:
        counters["pair_cooldown_block"] += 1
        return False, "pair cooldown"
    if cd.get(strat_key, 0) > t:
        counters["strategy_cooldown_block"] += 1
        return False, "strategy cooldown"
    return True, ""


def save_new_signal(sig: Dict[str, Any]) -> None:
    STATE.setdefault("active_signals", {})[sig["id"]] = sig
    reset_daily_counter_if_needed()
    STATE["signals_today"]["count"] = int(STATE["signals_today"].get("count", 0)) + 1
    cd = STATE.setdefault("cooldowns", {})
    cd[f"pair:{sig['symbol']}"] = now_ts() + PAIR_COOLDOWN_SECONDS
    cd[f"strategy:{sig['strategy']}:{sig['side']}"] = now_ts() + STRATEGY_COOLDOWN_SECONDS
    save_state()


def scan_market(send_to_telegram: bool = True, force_diag: bool = False) -> Dict[str, Any]:
    start = now_ts()
    counters: Counter = Counter()
    sent = []
    candidates = []
    btc = btc_context()
    if not btc.get("ok"):
        counters["btc_data_problem"] += 1
    symbols = get_scan_universe()
    counters["universe"] = len(symbols)
    for sym in symbols:
        if now_ts() - start > 70:
            counters["scan_timeout"] += 1
            break
        counters["checked"] += 1
        try:
            sig = analyze_symbol(sym, btc, counters)
            if sig:
                candidates.append(sig)
        except Exception as e:
            counters["analyze_error"] += 1
            STATE["auto"]["last_error"] = f"{sym}: {e}"
    candidates.sort(key=lambda s: (s["grade"] == "A+", s["score"], s["tp1_roi_x10"]), reverse=True)
    for sig in candidates[:5]:
        ok, reason = can_send_signal(sig, counters)
        if not ok:
            continue
        save_new_signal(sig)
        sent.append(sig)
        if send_to_telegram:
            send_telegram_message(signal_message(sig))
        # quality mode: max 1 signal per scan cycle
        break
    result = {
        "checked": counters.get("checked", 0),
        "candidates": len(candidates),
        "sent": len(sent),
        "btc": btc,
        "blocks": dict(counters),
        "duration": now_ts() - start,
        "near_miss": [
            {"symbol": display_symbol(s["symbol"]), "side": s["side"], "grade": s["grade"], "score": s["score"], "strategy": s["strategy"]}
            for s in candidates[:5]
        ],
    }
    STATE["auto"]["last_scan_result"] = result
    STATE["auto"]["last_scan_time"] = now_ts()
    save_state()
    if force_diag or (not sent and now_ts() - int(STATE["auto"].get("last_diag_time", 0)) >= DIAG_SECONDS):
        msg = build_diag_message(result)
        send_telegram_message(msg)
        STATE["auto"]["last_diag_time"] = now_ts()
        save_state()
    return result


def build_diag_message(result: Dict[str, Any]) -> str:
    blocks = result.get("blocks", {})
    top_blocks = sorted([(k, v) for k, v in blocks.items() if isinstance(v, int) and v > 0 and k not in ["checked", "universe"]], key=lambda x: x[1], reverse=True)[:10]
    st = STATE.get("stats", {})
    p, sl = int(st.get("profit", 0)), int(st.get("sl", 0))
    total = p + sl
    wr = p / total * 100 if total else 0
    lines = [
        "🧪 <b>Диагностика V13 Level Trader</b>",
        f"Проверено: {result.get('checked', 0)} из universe {blocks.get('universe', 0)}",
        f"Кандидатов: {result.get('candidates', 0)} · отправлено: {result.get('sent', 0)} · время: {result.get('duration', 0)}с",
        f"BTC: {result.get('btc', {}).get('text', 'unknown')}",
        f"Статистика: {p} профит / {sl} SL / WR {wr:.1f}%",
    ]
    if top_blocks:
        lines.append("\n<b>Главные блокировки:</b>")
        for k, v in top_blocks:
            lines.append(f"{k}: {v}")
    nm = result.get("near_miss") or []
    if nm:
        lines.append("\n<b>Ближайшие кандидаты:</b>")
        for x in nm[:5]:
            lines.append(f"{x['symbol']} {x['side']} {x['grade']} score {x['score']} · {x['strategy']}")
    if STATE.get("auto", {}).get("last_error"):
        lines.append(f"\nLast error: {STATE['auto']['last_error']}")
    return "\n".join(lines)


def track_active_signals() -> Dict[str, Any]:
    active = list(STATE.get("active_signals", {}).items())
    closed = []
    for sid, sig in active:
        try:
            c = get_klines(sig["symbol"], "1m", 80, ttl=12)
            if not c:
                continue
            price = c[-1]["close"]
            side = sig["side"]
            hit_tp1 = price >= sig["tp1"] if side == "LONG" else price <= sig["tp1"]
            hit_tp2 = price >= sig["tp2"] if side == "LONG" else price <= sig["tp2"]
            hit_tp3 = price >= sig["tp3"] if side == "LONG" else price <= sig["tp3"]
            hit_sl = price <= sig["sl"] if side == "LONG" else price >= sig["sl"]
            if hit_tp1 and not sig.get("tp1_hit"):
                sig["tp1_hit"] = True
                send_telegram_message(f"✅ <b>TP1</b> {display_symbol(sig['symbol'])} {sig['side']}\nЦена: {price:.8g}\nСделка уже считается позитивной по статистике, даже если остаток потом откатится.")
            if hit_tp2 and not sig.get("tp2_hit"):
                sig["tp2_hit"] = True
                send_telegram_message(f"✅ <b>TP2</b> {display_symbol(sig['symbol'])} {sig['side']}\nЦена: {price:.8g}")
            if hit_tp3:
                sig["tp3_hit"] = True
                sig["status"] = "closed"
                sig["result"] = "profit"
                closed.append((sid, sig, "TP3"))
                continue
            if hit_sl:
                sig["status"] = "closed"
                sig["result"] = "profit" if sig.get("tp1_hit") else "sl"
                closed.append((sid, sig, "SL_AFTER_TP1" if sig.get("tp1_hit") else "SL"))
                continue
            STATE["active_signals"][sid] = sig
        except Exception as e:
            STATE["auto"]["last_error"] = f"track {sid}: {e}"
    for sid, sig, event in closed:
        STATE["active_signals"].pop(sid, None)
        STATE.setdefault("closed_signals", []).append(sig)
        update_stats(sig, sig["result"])
        if event == "SL" and sig["result"] == "sl":
            msg = f"❌ <b>Stop Loss</b>\n{sig['grade']} · {sig['side']} {display_symbol(sig['symbol'])}\nСтратегия: {sig['strategy']}\nВход: {sig['entry']:.8g}\nSL: {sig['sl']:.8g}\n\n{build_stats_text()}"
        else:
            msg = f"✅ <b>Профит / {event}</b>\n{sig['grade']} · {sig['side']} {display_symbol(sig['symbol'])}\nСтратегия: {sig['strategy']}\n\n{build_stats_text()}"
        send_telegram_message(msg)
    save_state()
    return {"active": len(STATE.get("active_signals", {})), "closed": len(closed)}


async def auto_worker() -> None:
    await asyncio.sleep(2)
    startup = (
        f"✅ <b>{APP_NAME}</b> активирован и работает.\n"
        f"Deploy marker: <code>{DEPLOY_MARKER}</code>\n\n"
        "Режим: PROFESSIONAL LEVEL TRADER — качество важнее количества.\n"
        "Логика: уровни 1H/4H → подтверждение 15m/5m → anti-chase → TP1 минимум 10% ROI при x10.\n"
        f"A+ score: {A_PLUS_MIN_SCORE} · B score: {B_MIN_SCORE}.\n"
        f"Лимит: {MAX_SIGNALS_PER_DAY} сигнала/день · active max {MAX_ACTIVE_SIGNALS}.\n"
        "Диагностика и статистика TP/SL: ON."
    )
    send_telegram_message(startup)
    # Force first diagnostic, even if no signal.
    try:
        scan_market(send_to_telegram=True, force_diag=True)
    except Exception as e:
        STATE["auto"]["last_error"] = f"first_scan: {e}"
        send_telegram_message(f"⚠️ Ошибка первого скана V13\n<code>{e}</code>")
    while True:
        try:
            t = now_ts()
            if AUTO_TRACK_ENABLED and t - int(STATE["auto"].get("last_track_time", 0)) >= AUTO_TRACK_SECONDS:
                STATE["auto"]["last_track_time"] = t
                track_active_signals()
            if AUTO_SCAN_ENABLED and t - int(STATE["auto"].get("last_scan_time", 0)) >= AUTO_SCAN_SECONDS:
                scan_market(send_to_telegram=True, force_diag=False)
        except Exception as e:
            STATE["auto"]["last_error"] = f"auto_worker: {e}"
            send_telegram_message(f"⚠️ Ошибка auto-worker V13\n<code>{e}</code>")
        await asyncio.sleep(5)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(auto_worker())


@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME, "marker": DEPLOY_MARKER, "time": now_ts()}


@app.get("/version")
def version():
    return {"app": APP_NAME, "deploy_marker": DEPLOY_MARKER}


@app.get("/auto-status")
def auto_status():
    return JSONResponse({
        "app": APP_NAME,
        "marker": DEPLOY_MARKER,
        "auto": STATE.get("auto", {}),
        "active_signals": list(STATE.get("active_signals", {}).values()),
        "stats": STATE.get("stats", {}),
        "signals_today": STATE.get("signals_today", {}),
    })


@app.get("/scan")
def manual_scan(send: bool = Query(True)):
    return scan_market(send_to_telegram=send, force_diag=True)


@app.get("/stats")
def stats():
    return HTMLResponse("<pre>" + build_stats_text().replace("<", "&lt;").replace(">", "&gt;") + "</pre>")


@app.get("/send-stats")
def send_stats():
    return send_telegram_message(build_stats_text())


@app.get("/test-telegram")
def test_telegram():
    return send_telegram_message(f"✅ Telegram test OK\n{APP_NAME}\n{DEPLOY_MARKER}")


@app.get("/")
def root():
    return HTMLResponse(f"""
    <h2>{APP_NAME}</h2>
    <p><b>Deploy:</b> {DEPLOY_MARKER}</p>
    <ul>
      <li><a href='/health'>/health</a></li>
      <li><a href='/version'>/version</a></li>
      <li><a href='/auto-status'>/auto-status</a></li>
      <li><a href='/scan'>/scan</a></li>
      <li><a href='/stats'>/stats</a></li>
      <li><a href='/send-stats'>/send-stats</a></li>
      <li><a href='/test-telegram'>/test-telegram</a></li>
    </ul>
    """)


if __name__ == "__main__":
    # Background Worker mode for Render: python bot.py
    async def main():
        asyncio.create_task(auto_worker())
        while True:
            await asyncio.sleep(3600)
    asyncio.run(main())
