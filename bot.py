import os
import time
import json
import math
import random
import asyncio
import traceback
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse

# =========================================================
# V12.5 CONFIRMED LEVEL QUALITY MODE
# One core professional strategy:
#   Volatility + BTC context + HTF trend + pullback/reclaim/reject
# Goal:
#   Send A+ / B futures signals where TP1 target gives at least 10% ROI at x10
# =========================================================

APP_NAME = "Professional Adaptive Futures Bot AUTO V12.5 CONFIRMED LEVEL QUALITY MODE"
DEPLOY_MARKER = "V12_5_CONFIRMED_LEVEL_QUALITY_MODE_2026_06_19"

app = FastAPI(title=APP_NAME)

# ---------- ENV ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
BINGX_BASE_URL = os.getenv("BINGX_BASE_URL", "https://open-api.bingx.com")

STATE_FILE = os.getenv("STATE_FILE", "bot_state_v12_5.json")
LEVERAGE = float(os.getenv("LEVERAGE", "10"))

AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "75"))
TRACK_SECONDS = int(os.getenv("TRACK_SECONDS", "20"))
DIAG_SECONDS = int(os.getenv("DIAG_SECONDS", "1800"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "7"))
SCAN_CYCLE_TIMEOUT = int(os.getenv("SCAN_CYCLE_TIMEOUT", "80"))

MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "500"))
MAX_ANALYZE_PER_SCAN = int(os.getenv("MAX_ANALYZE_PER_SCAN", "110"))
QUALITY_FIRST_LIMIT = int(os.getenv("QUALITY_FIRST_LIMIT", "55"))
ACTIVE_MOVER_LIMIT = int(os.getenv("ACTIVE_MOVER_LIMIT", "95"))
RANDOM_ROTATION_LIMIT = int(os.getenv("RANDOM_ROTATION_LIMIT", "35"))

# Signal quality. User requested around 85 / 76 before; V12 keeps that.
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "90"))
B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "85"))

# 10% ROI at x10 means roughly 1% price movement. Add a small buffer for fees/slippage.
MIN_TP1_ROI_X10 = float(os.getenv("MIN_TP1_ROI_X10", "10"))
MIN_TP1_PRICE_MOVE_PCT = float(os.getenv("MIN_TP1_PRICE_MOVE_PCT", "1.05"))

# Volatility filters: we want volatile, but not ultra-risk that can print -20% in two candles.
MIN_1H_MOVE_ABS = float(os.getenv("MIN_1H_MOVE_ABS", "0.65"))
MIN_4H_MOVE_ABS = float(os.getenv("MIN_4H_MOVE_ABS", "1.10"))
MIN_ATR15_PCT = float(os.getenv("MIN_ATR15_PCT", "0.28"))
MAX_ATR15_PCT_NORMAL = float(os.getenv("MAX_ATR15_PCT_NORMAL", "3.20"))
MAX_SINGLE_5M_RANGE_PCT = float(os.getenv("MAX_SINGLE_5M_RANGE_PCT", "7.50"))
MAX_SINGLE_15M_RANGE_PCT = float(os.getenv("MAX_SINGLE_15M_RANGE_PCT", "10.00"))
MIN_DOLLAR_VOLUME_5M = float(os.getenv("MIN_DOLLAR_VOLUME_5M", "15000"))

# Risk/position style. Bot sends signals, not orders. These values are displayed.
A_PLUS_RISK_MULT = float(os.getenv("A_PLUS_RISK_MULT", "1.0"))
B_RISK_MULT = float(os.getenv("B_RISK_MULT", "0.35"))
EXTREME_RISK_MULT = float(os.getenv("EXTREME_RISK_MULT", "0.15"))

# Controls to ensure bot does not stay completely silent in an active market.
# It still respects TP1 10% ROI and ultra-risk block.
OPPORTUNITY_MODE_ENABLED = os.getenv("OPPORTUNITY_MODE_ENABLED", "true").lower() == "true"
OPPORTUNITY_AFTER_HOURS = float(os.getenv("OPPORTUNITY_AFTER_HOURS", "6"))
OPPORTUNITY_MIN_SCORE = int(os.getenv("OPPORTUNITY_MIN_SCORE", "74"))

# Cooldown per symbol/side/strategy
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "7200"))
MAX_ACTIVE_SIGNALS = int(os.getenv("MAX_ACTIVE_SIGNALS", "2"))
MAX_SIGNALS_PER_DAY = int(os.getenv("MAX_SIGNALS_PER_DAY", "5"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "1"))
STRICT_HTF_MODE = os.getenv("STRICT_HTF_MODE", "true").lower() == "true"

# Anti-chase / level reversal filters.
# If a coin already moved too far, the bot must not chase the same direction.
# It waits for bounce/retest/reclaim near levels.
ANTI_CHASE_1H_PCT = float(os.getenv("ANTI_CHASE_1H_PCT", "3.0"))
ANTI_CHASE_4H_PCT = float(os.getenv("ANTI_CHASE_4H_PCT", "6.0"))
MIN_RETRACE_AFTER_EXTREME_PCT = float(os.getenv("MIN_RETRACE_AFTER_EXTREME_PCT", "1.2"))
LEVEL_ZONE_PCT = float(os.getenv("LEVEL_ZONE_PCT", "1.8"))

# V12.5 quality confirmation: fewer trades, stronger entries.
CONFIRM_15M_REQUIRED = os.getenv("CONFIRM_15M_REQUIRED", "true").lower() == "true"
MIN_CONFIRM_5M_CANDLES = int(os.getenv("MIN_CONFIRM_5M_CANDLES", "2"))
MAX_WICK_RATIO = float(os.getenv("MAX_WICK_RATIO", "0.58"))
MIN_BOUNCE_AFTER_DUMP_PCT = float(os.getenv("MIN_BOUNCE_AFTER_DUMP_PCT", "2.0"))
MIN_PULLBACK_AFTER_PUMP_PCT = float(os.getenv("MIN_PULLBACK_AFTER_PUMP_PCT", "2.0"))
REQUIRE_LEVEL_PROXIMITY = os.getenv("REQUIRE_LEVEL_PROXIMITY", "true").lower() == "true"

QUALITY_BASES = set(x.strip().upper() for x in os.getenv(
    "QUALITY_BASES",
    "BTC,ETH,SOL,BNB,XRP,LINK,AVAX,AAVE,SUI,TAO,NEAR,INJ,SEI,OP,ARB,DOGE,DOT,LTC,ADA,APT,UNI,FET,RENDER,RUNE,FIL,ATOM,ETC,TRX,TON,ONDO,PENDLE,JUP,ENA,WLD"
).split(",") if x.strip())

BLOCKED_BASES = set(x.strip().upper() for x in os.getenv(
    "BLOCKED_BASES",
    "1000SATS,HMSTR,GUA,DOGS,CATI,MEME,NOT,PEPE,1000PEPE,BONK,WIF,PNUT,ACT,GOAT,MOODENG,NEIRO,TURBO,BOME,GOBLIN,BLEND,FIGHT,BEAT,MAGMA,VELVET,COLLECT,SPACE"
).split(",") if x.strip())

# ---------- STATE ----------
def now_ts() -> int:
    return int(time.time())


def day_key(ts: Optional[int] = None) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts or now_ts()))


def daily_sent_count() -> int:
    return int(STATE.setdefault("daily_sent", {}).get(day_key(), 0))


def record_daily_signal() -> None:
    d = STATE.setdefault("daily_sent", {})
    k = day_key()
    d[k] = int(d.get(k, 0)) + 1
    # keep only recent daily counters
    for old in list(d.keys())[:-7]:
        d.pop(old, None)


def default_state() -> Dict[str, Any]:
    return {
        "version": DEPLOY_MARKER,
        "started_at": now_ts(),
        "last_scan_at": 0,
        "last_signal_at": 0,
        "last_diag_at": 0,
        "last_error": "",
        "active_signals": [],
        "cooldowns": {},
        "stats": {
            "global": {"positive": 0, "sl": 0},
            "grade": {},
            "side": {},
            "strategy": {},
            "symbol": {},
            "combo": {},
        },
        "disabled_until": {},
        "last_scan_summary": {},
        "near_miss": [],
        "daily_sent": {},
    }


def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            st = json.load(f)
        if not isinstance(st, dict):
            return default_state()
        base = default_state()
        base.update(st)
        base["version"] = DEPLOY_MARKER
        return base
    except Exception:
        return default_state()


STATE = load_state()


def save_state() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ---------- TELEGRAM ----------
def tg_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram(text: str) -> bool:
    """Send Telegram message and record the exact reason if it fails.
    This is intentionally noisy in STATE/console because silent Telegram failures
    were the main reason diagnostics were hard to verify.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        missing = []
        if not TELEGRAM_BOT_TOKEN:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not TELEGRAM_CHAT_ID:
            missing.append("TELEGRAM_CHAT_ID")
        STATE["last_error"] = "telegram disabled: missing " + ", ".join(missing)
        print(STATE["last_error"], flush=True)
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text[:3900],
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        r = requests.post(url, data=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            STATE["last_error"] = f"telegram http {r.status_code}: {r.text[:250]}"
            print(STATE["last_error"], flush=True)
            return False
        print("telegram sent", flush=True)
        return True
    except Exception as e:
        STATE["last_error"] = f"telegram exception: {e}"
        print(STATE["last_error"], flush=True)
        return False

# ---------- MARKET DATA ----------
def normalize_symbol(symbol: str) -> str:
    s = symbol.upper().replace("-", "/")
    if "/" not in s and s.endswith("USDT"):
        s = s[:-4] + "/USDT"
    return s


def api_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("/", "-")


def base_of(symbol: str) -> str:
    return normalize_symbol(symbol).split("/")[0]


def is_usdt_perp(symbol: str) -> bool:
    return normalize_symbol(symbol).endswith("/USDT")


def get_json(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    try:
        url = path if path.startswith("http") else f"{BINGX_BASE_URL}{path}"
        r = requests.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None
        data = r.json()
        if isinstance(data, dict):
            return data
        return None
    except Exception as e:
        STATE["last_error"] = f"get_json {path}: {e}"
        return None


def get_contracts() -> List[str]:
    data = get_json("/openApi/swap/v2/quote/contracts")
    symbols: List[str] = []
    if data and isinstance(data.get("data"), list):
        for item in data["data"]:
            s = item.get("symbol") or item.get("contractId") or ""
            ns = normalize_symbol(str(s))
            if is_usdt_perp(ns):
                symbols.append(ns)
    # Fallback if endpoint fails.
    if not symbols:
        symbols = [f"{b}/USDT" for b in sorted(QUALITY_BASES)]
    # unique
    seen, out = set(), []
    for s in symbols:
        if s not in seen:
            seen.add(s); out.append(s)
    return out[:MAX_CONTRACTS]


def get_klines(symbol: str, interval: str, limit: int = 160) -> Optional[List[Dict[str, float]]]:
    params = {"symbol": api_symbol(symbol), "interval": interval, "limit": limit}
    data = get_json("/openApi/swap/v3/quote/klines", params)
    raw = None
    if data:
        raw = data.get("data")
    if not raw:
        # Some BingX deployments still respond on v2
        data = get_json("/openApi/swap/v2/quote/klines", params)
        raw = data.get("data") if data else None
    if not raw or not isinstance(raw, list):
        return None
    candles = []
    for c in raw:
        try:
            if isinstance(c, dict):
                candles.append({
                    "time": float(c.get("time") or c.get("openTime") or 0),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                    "volume": float(c.get("volume", 0)),
                })
            elif isinstance(c, list) and len(c) >= 6:
                candles.append({
                    "time": float(c[0]),
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                })
        except Exception:
            continue
    candles.sort(key=lambda x: x["time"])
    if len(candles) < 50:
        return None
    return candles

# ---------- INDICATORS ----------
def closes(c: List[Dict[str, float]]) -> List[float]:
    return [x["close"] for x in c]


def ema(vals: List[float], period: int) -> float:
    if not vals:
        return 0.0
    k = 2 / (period + 1)
    e = vals[0]
    for v in vals[1:]:
        e = v * k + e * (1 - k)
    return e


def sma(vals: List[float], period: int) -> float:
    if not vals:
        return 0.0
    subset = vals[-period:] if len(vals) >= period else vals
    return sum(subset) / len(subset)


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100


def atr_pct(c: List[Dict[str, float]], period: int = 14) -> float:
    if len(c) < period + 2:
        return 0.0
    trs = []
    for i in range(-period, 0):
        cur = c[i]
        prev = c[i - 1]
        tr = max(cur["high"] - cur["low"], abs(cur["high"] - prev["close"]), abs(cur["low"] - prev["close"]))
        trs.append(tr)
    atr = sum(trs) / len(trs)
    price = c[-1]["close"]
    return atr / price * 100 if price else 0.0


def vwap(c: List[Dict[str, float]], period: int = 48) -> float:
    part = c[-period:] if len(c) >= period else c
    pv = sum(x["close"] * max(x.get("volume", 0), 0) for x in part)
    vol = sum(max(x.get("volume", 0), 0) for x in part)
    return pv / vol if vol > 0 else sma([x["close"] for x in part], len(part))


def max_candle_range_pct(c: List[Dict[str, float]], lookback: int = 12) -> float:
    part = c[-lookback:] if len(c) >= lookback else c
    mx = 0.0
    for x in part:
        if x["open"]:
            mx = max(mx, (x["high"] - x["low"]) / x["open"] * 100)
    return mx


def avg_dollar_volume(c: List[Dict[str, float]], period: int = 24) -> float:
    part = c[-period:] if len(c) >= period else c
    if not part:
        return 0.0
    return sum(x["close"] * max(x.get("volume", 0), 0) for x in part) / len(part)


def recent_high(c: List[Dict[str, float]], lookback: int = 36) -> float:
    part = c[-lookback:] if len(c) >= lookback else c
    return max(x["high"] for x in part)


def recent_low(c: List[Dict[str, float]], lookback: int = 36) -> float:
    part = c[-lookback:] if len(c) >= lookback else c
    return min(x["low"] for x in part)


def distance_pct(a: float, b: float) -> float:
    return abs(a - b) / a * 100 if a else 999.0

def near_level(price: float, level: float, pct_zone: float = LEVEL_ZONE_PCT) -> bool:
    if price <= 0 or level <= 0:
        return False
    return abs(price - level) / price * 100 <= pct_zone

# ---------- CONTEXT ----------
@dataclass
class BtcContext:
    regime: str
    side_bias: str
    move_15m: float
    move_1h: float
    move_4h: float
    ema1h_fast: float
    ema1h_slow: float
    reason: str


def analyze_btc() -> Optional[BtcContext]:
    c15 = get_klines("BTC/USDT", "15m", 80)
    c1h = get_klines("BTC/USDT", "1h", 120)
    if not c15 or not c1h:
        return None
    cl15, cl1h = closes(c15), closes(c1h)
    price = cl15[-1]
    move_15 = pct(price, cl15[-2]) if len(cl15) >= 2 else 0.0
    move_1h = pct(price, cl15[-5]) if len(cl15) >= 5 else 0.0
    move_4h = pct(price, cl15[-17]) if len(cl15) >= 17 else 0.0
    e20 = ema(cl1h[-80:], 20)
    e50 = ema(cl1h[-100:], 50)
    atr15 = atr_pct(c15, 14)

    if move_1h <= -0.75 or move_4h <= -1.6:
        regime = "IMPULSE_DOWN" if move_1h <= -1.0 else "TREND_DOWN"
        bias = "SHORT"
        reason = f"BTC слабый: 1h {move_1h:.2f}%, 4h {move_4h:.2f}%"
    elif move_1h >= 0.75 or move_4h >= 1.6:
        regime = "IMPULSE_UP" if move_1h >= 1.0 else "TREND_UP"
        bias = "LONG"
        reason = f"BTC сильный: 1h {move_1h:.2f}%, 4h {move_4h:.2f}%"
    elif abs(move_4h) < 1.2 and atr15 < 0.55:
        regime = "RANGE"
        bias = "BOTH"
        reason = f"BTC диапазон/баланс: 4h {move_4h:.2f}%"
    elif e20 > e50:
        regime = "TREND_UP"
        bias = "LONG"
        reason = "BTC 1H выше EMA-тренда"
    elif e20 < e50:
        regime = "TREND_DOWN"
        bias = "SHORT"
        reason = "BTC 1H ниже EMA-тренда"
    else:
        regime = "CHOP"
        bias = "BOTH"
        reason = "BTC без ясного режима"
    return BtcContext(regime, bias, move_15, move_1h, move_4h, e20, e50, reason)

# ---------- SCANNING UNIVERSE ----------
def is_ultra_risk(symbol: str, c5: Optional[List[Dict[str, float]]] = None, c15: Optional[List[Dict[str, float]]] = None) -> bool:
    b = base_of(symbol)
    if b in BLOCKED_BASES:
        return True
    if c5 and max_candle_range_pct(c5, 10) > MAX_SINGLE_5M_RANGE_PCT:
        return True
    if c15 and max_candle_range_pct(c15, 8) > MAX_SINGLE_15M_RANGE_PCT:
        return True
    return False


def choose_symbols(all_symbols: List[str]) -> List[str]:
    quality = [s for s in all_symbols if base_of(s) in QUALITY_BASES]
    rest = [s for s in all_symbols if s not in quality]
    random.shuffle(rest)
    # We cannot know movers without klines, but rotate broad market after quality.
    return (quality[:QUALITY_FIRST_LIMIT] + rest[:MAX_ANALYZE_PER_SCAN])[:MAX_ANALYZE_PER_SCAN]

# ---------- TRADE LOGIC ----------
@dataclass
class Signal:
    symbol: str
    side: str
    grade: str
    strategy: str
    trade_type: str
    entry: float
    tp1: float
    tp2: float
    tp3: float
    sl: float
    score: int
    rr: float
    roi_tp1: float
    risk_mult: float
    expected_time: str
    reason: str
    btc_context: str
    created_at: int


def price_move_roi(entry: float, target: float, side: str) -> float:
    if entry <= 0:
        return 0.0
    if side == "LONG":
        move = (target - entry) / entry * 100
    else:
        move = (entry - target) / entry * 100
    return move * LEVERAGE


def risk_reward(entry: float, tp: float, sl: float, side: str) -> float:
    if side == "LONG":
        reward = tp - entry
        risk = entry - sl
    else:
        reward = entry - tp
        risk = sl - entry
    if risk <= 0:
        return 0.0
    return reward / risk


def is_disabled(strategy: str, side: str, grade: str) -> bool:
    key1 = f"{strategy}:{side}"
    key2 = f"{strategy}:{side}:{grade}"
    t = now_ts()
    return STATE.get("disabled_until", {}).get(key1, 0) > t or STATE.get("disabled_until", {}).get(key2, 0) > t


def in_cooldown(symbol: str, side: str, strategy: str) -> bool:
    key = f"{symbol}:{side}:{strategy}"
    return STATE.get("cooldowns", {}).get(key, 0) > now_ts()


def set_cooldown(symbol: str, side: str, strategy: str) -> None:
    STATE.setdefault("cooldowns", {})[f"{symbol}:{side}:{strategy}"] = now_ts() + SIGNAL_COOLDOWN_SECONDS


def stat_wr(scope: str, key: str) -> Optional[float]:
    st = STATE.get("stats", {}).get(scope, {}).get(key)
    if not st:
        return None
    pos, sl = st.get("positive", 0), st.get("sl", 0)
    total = pos + sl
    if total < 4:
        return None
    return pos / total * 100


def stats_adjust_score(sig: Signal) -> Signal:
    # Use statistics, but do not kill the bot early. Small, bounded adjustment.
    combo = f"{sig.strategy}:{sig.side}:{sig.grade}"
    strategy_wr = stat_wr("strategy", sig.strategy)
    combo_wr = stat_wr("combo", combo)
    adj = 0
    notes = []
    if strategy_wr is not None:
        if strategy_wr >= 58:
            adj += 3; notes.append(f"стратегия WR {strategy_wr:.0f}% +")
        elif strategy_wr <= 35:
            adj -= 6; notes.append(f"стратегия WR {strategy_wr:.0f}% -")
    if combo_wr is not None:
        if combo_wr >= 58:
            adj += 3; notes.append(f"связка WR {combo_wr:.0f}% +")
        elif combo_wr <= 35:
            adj -= 8; notes.append(f"связка WR {combo_wr:.0f}% -")
    sig.score = max(0, min(100, sig.score + adj))
    if notes:
        sig.reason += "\nСтатистика: " + "; ".join(notes)
    return sig


def calc_targets(entry: float, atr15pct: float, recent_hi: float, recent_lo: float, side: str, trade_type: str) -> Tuple[float, float, float, float]:
    # TP1 must be at least 1.05% price move (10%+ ROI at x10). Use volatility/structure if larger.
    min_move = MIN_TP1_PRICE_MOVE_PCT / 100
    if trade_type == "SWING":
        tp1_move = max(min_move, min(max(atr15pct * 2.2 / 100, 0.045), 0.018))
        sl_move = max(tp1_move * 0.75, min(max(atr15pct * 2.6 / 100, 0.060), 0.014))
    elif trade_type == "FAST":
        tp1_move = max(min_move, min(max(atr15pct * 1.5 / 100, 0.028), 0.012))
        sl_move = max(tp1_move * 0.55, min(max(atr15pct * 1.2 / 100, 0.035), 0.007))
    else:
        tp1_move = max(min_move, min(max(atr15pct * 1.9 / 100, 0.038), 0.014))
        sl_move = max(tp1_move * 0.65, min(max(atr15pct * 1.7 / 100, 0.045), 0.009))

    if side == "LONG":
        tp1 = entry * (1 + tp1_move)
        tp2 = entry * (1 + tp1_move * 1.8)
        tp3 = entry * (1 + tp1_move * 3.0)
        # Put SL below local structure if close enough, otherwise volatility stop.
        struct_sl = recent_lo * 0.997
        vol_sl = entry * (1 - sl_move)
        sl = min(vol_sl, struct_sl) if (entry - struct_sl) / entry < 0.05 else vol_sl
    else:
        tp1 = entry * (1 - tp1_move)
        tp2 = entry * (1 - tp1_move * 1.8)
        tp3 = entry * (1 - tp1_move * 3.0)
        struct_sl = recent_hi * 1.003
        vol_sl = entry * (1 + sl_move)
        sl = max(vol_sl, struct_sl) if (struct_sl - entry) / entry < 0.05 else vol_sl
    return tp1, tp2, tp3, sl


def analyze_symbol(symbol: str, btc: BtcContext, diag: Dict[str, int]) -> Optional[Signal]:
    c5 = get_klines(symbol, "5m", 140)
    if not c5:
        diag["no_klines_5m"] += 1
        return None
    c15 = get_klines(symbol, "15m", 140)
    c1h = get_klines(symbol, "1h", 120)
    c4h = get_klines(symbol, "4h", 80)
    if not c15 or not c1h or not c4h:
        diag["no_klines_htf"] += 1
        return None

    if is_ultra_risk(symbol, c5, c15):
        diag["ultra_risk_block"] += 1
        return None

    price = c5[-1]["close"]
    cl5, cl15, cl1h, cl4h = closes(c5), closes(c15), closes(c1h), closes(c4h)
    atr15p = atr_pct(c15, 14)
    if atr15p < MIN_ATR15_PCT:
        diag["low_vol_block"] += 1
        return None
    if atr15p > MAX_ATR15_PCT_NORMAL:
        diag["too_volatile_block"] += 1
        return None

    dollar_vol = avg_dollar_volume(c5, 24)
    if dollar_vol < MIN_DOLLAR_VOLUME_5M and base_of(symbol) not in QUALITY_BASES:
        diag["volume_block"] += 1
        return None

    move_1h = pct(price, cl15[-5]) if len(cl15) >= 5 else 0.0
    move_4h = pct(price, cl15[-17]) if len(cl15) >= 17 else 0.0
    if abs(move_1h) < MIN_1H_MOVE_ABS and abs(move_4h) < MIN_4H_MOVE_ABS:
        diag["not_active_block"] += 1
        return None

    ema5_20 = ema(cl5[-80:], 20)
    ema15_20 = ema(cl15[-80:], 20)
    ema15_50 = ema(cl15[-100:], 50)
    ema1h_20 = ema(cl1h[-80:], 20)
    ema1h_50 = ema(cl1h[-100:], 50)
    ema4h_20 = ema(cl4h[-60:], 20)
    vwap15 = vwap(c15, 48)
    hi36 = recent_high(c15, 36)
    lo36 = recent_low(c15, 36)
    hi96 = recent_high(c15, 96)
    lo96 = recent_low(c15, 96)

    # Anti-chase context: if price already moved far, do not enter late in the same direction.
    extended_down = move_1h <= -ANTI_CHASE_1H_PCT or move_4h <= -ANTI_CHASE_4H_PCT
    extended_up = move_1h >= ANTI_CHASE_1H_PCT or move_4h >= ANTI_CHASE_4H_PCT

    # Direction priority from BTC. If BTC is bearish, search shorts first; bullish -> longs first.
    sides = ["LONG", "SHORT"]
    if btc.side_bias == "SHORT":
        sides = ["SHORT", "LONG"]
    elif btc.side_bias == "LONG":
        sides = ["LONG", "SHORT"]

    best: Optional[Signal] = None

    for side in sides:
        score = 50
        reasons: List[str] = []
        trade_type = "TREND"
        strategy = "VOLATILITY_TREND_PULLBACK"

        # BTC alignment
        if side == btc.side_bias:
            score += 16
            reasons.append(f"BTC совпадает: {btc.reason}")
        elif btc.side_bias == "BOTH":
            score += 4
            reasons.append(f"BTC нейтрален/диапазон: {btc.reason}")
        else:
            # V12.4 level anti-chase mode: no counter-BTC trades. We prefer fewer, cleaner signals.
            diag["btc_block"] += 1
            continue

        # HTF trend and setup.
        if side == "LONG":
            trend_ok = ema1h_20 > ema1h_50 or price > ema1h_20
            htf_ok = price > ema4h_20 or move_4h > 1.2
            pulled_back = (hi36 - price) / hi36 * 100 if hi36 else 0
            reclaim = price > ema5_20 and c5[-1]["close"] > c5[-1]["open"]
            near_value = abs(price - ema15_20) / price * 100 < max(1.8, atr15p * 1.8) or abs(price - vwap15) / price * 100 < max(1.8, atr15p * 1.8)
            near_support = near_level(price, lo36, max(LEVEL_ZONE_PCT, atr15p * 1.4)) or near_level(price, lo96, max(LEVEL_ZONE_PCT, atr15p * 1.4))
            confirm5 = two_candle_confirmation(c5, "LONG")
            confirm15 = htf_candle_confirmation(c15, "LONG")
            wick_ok = candle_wick_ratio(c5[-1], "LONG") <= MAX_WICK_RATIO
            level_ok = near_support or near_value
            # If price already pumped hard, do not chase LONG. LONG is allowed only after a real pullback/reclaim.
            if extended_up and pulled_back < MIN_PULLBACK_AFTER_PUMP_PCT:
                diag["anti_chase_block"] += 1
                add_near_miss(symbol, side, score, "ANTI_CHASE_LONG", f"цена уже выросла: 1h {move_1h:.1f}%, 4h {move_4h:.1f}%; ждём откат/уровень")
                continue
            # If price dumped hard, LONG is not trend-following; it is a level reclaim only. Require support/reclaim.
            if extended_down:
                diag["level_reversal_watch"] += 1
                strategy = "LEVEL_RECLAIM_AFTER_DUMP"
                trade_type = "STRUCTURE"
                if not (near_support and reclaim and c5[-1]["close"] > c5[-2]["close"]):
                    diag["setup_block"] += 1
                    add_near_miss(symbol, side, score, strategy, f"после падения ждём reclaim поддержки; 1h {move_1h:.1f}%, 4h {move_4h:.1f}%")
                    continue
                score += 10
                reasons.append("после сильного падения бот ищет не chase SHORT, а reclaim поддержки для отскока")
            if trend_ok: score += 10; reasons.append("1H тренд/структура поддерживает LONG")
            if htf_ok: score += 6; reasons.append("4H фон не против LONG")
            if pulled_back >= 0.45: score += 7; reasons.append(f"был откат от локального high {pulled_back:.1f}%")
            if reclaim: score += 10; reasons.append("цена вернулась выше 5m EMA / зелёная свеча подтверждения")
            if near_value: score += 7; reasons.append("вход рядом с EMA/VWAP, не в пустоте")
            if move_1h > 0.7: score += 5; reasons.append(f"монета активна вверх: 1h {move_1h:.2f}%")
            if move_4h > 1.2: score += 4; reasons.append(f"4h движение вверх {move_4h:.2f}%")
            if STRICT_HTF_MODE and not (trend_ok and htf_ok):
                diag["htf_block"] += 1
                continue
            if STRICT_HTF_MODE and not (reclaim and near_value):
                diag["setup_block"] += 1
                continue
            if not confirm5 or (CONFIRM_15M_REQUIRED and not confirm15) or not wick_ok:
                diag["setup_block"] += 1
                add_near_miss(symbol, side, score, strategy, "LONG заблокирован: нет 2x5m + 15m подтверждения или плохой фитиль")
                continue
            if REQUIRE_LEVEL_PROXIMITY and not level_ok:
                diag["setup_block"] += 1
                add_near_miss(symbol, side, score, strategy, "LONG заблокирован: вход не рядом с уровнем/EMA/VWAP")
                continue
            if not (trend_ok and reclaim and near_value):
                score -= 12
            score += 6
            reasons.append("подтверждение: 2 свечи 5m + 15m без плохого фитиля")
            if pulled_back > 5.5:
                trade_type = "SWING"
                strategy = "VOLATILITY_SWING_RECLAIM"
        else:
            trend_ok = ema1h_20 < ema1h_50 or price < ema1h_20
            htf_ok = price < ema4h_20 or move_4h < -1.2
            bounced = (price - lo36) / lo36 * 100 if lo36 else 0
            reject = price < ema5_20 and c5[-1]["close"] < c5[-1]["open"]
            near_value = abs(price - ema15_20) / price * 100 < max(1.8, atr15p * 1.8) or abs(price - vwap15) / price * 100 < max(1.8, atr15p * 1.8)
            near_resistance = near_level(price, hi36, max(LEVEL_ZONE_PCT, atr15p * 1.4)) or near_level(price, hi96, max(LEVEL_ZONE_PCT, atr15p * 1.4))
            confirm5 = two_candle_confirmation(c5, "SHORT")
            confirm15 = htf_candle_confirmation(c15, "SHORT")
            wick_ok = candle_wick_ratio(c5[-1], "SHORT") <= MAX_WICK_RATIO
            level_ok = near_resistance or near_value
            # If price already dumped hard, do not chase SHORT. SHORT is allowed only after a bounce into resistance and reject.
            if extended_down and bounced < MIN_BOUNCE_AFTER_DUMP_PCT:
                diag["anti_chase_block"] += 1
                add_near_miss(symbol, side, score, "ANTI_CHASE_SHORT", f"цена уже упала: 1h {move_1h:.1f}%, 4h {move_4h:.1f}%; ждём отскок/retest")
                continue
            if extended_down:
                strategy = "LEVEL_RETEST_SHORT_AFTER_DUMP"
                trade_type = "STRUCTURE"
                if not (near_resistance and reject):
                    diag["setup_block"] += 1
                    add_near_miss(symbol, side, score, strategy, f"после падения SHORT только от сопротивления после отскока; bounce {bounced:.1f}%")
                    continue
                reasons.append("SHORT не на дне: был отскок к сопротивлению и reject")
                score += 8
            # If price pumped hard, SHORT can be level-reversal only if high was not broken/held and reject appeared.
            if extended_up:
                diag["level_reversal_watch"] += 1
                strategy = "LEVEL_REJECT_AFTER_PUMP"
                trade_type = "STRUCTURE"
                failed_breakout = near_resistance or c15[-1]["close"] < hi36 * 0.995
                if not (failed_breakout and reject):
                    diag["setup_block"] += 1
                    add_near_miss(symbol, side, score, strategy, f"после роста ждём failed breakout/reject уровня; 1h {move_1h:.1f}%, 4h {move_4h:.1f}%")
                    continue
                reasons.append("после сильного роста бот ищет failed breakout / reject от сопротивления")
                score += 10
            if trend_ok: score += 10; reasons.append("1H тренд/структура поддерживает SHORT")
            if htf_ok: score += 6; reasons.append("4H фон не против SHORT")
            if bounced >= 0.45: score += 7; reasons.append(f"был отскок от локального low {bounced:.1f}%")
            if reject: score += 10; reasons.append("цена снова ниже 5m EMA / красная свеча подтверждения")
            if near_value: score += 7; reasons.append("вход рядом с EMA/VWAP, не в пустоте")
            if move_1h < -0.7: score += 5; reasons.append(f"монета активна вниз: 1h {move_1h:.2f}%")
            if move_4h < -1.2: score += 4; reasons.append(f"4h движение вниз {move_4h:.2f}%")
            if STRICT_HTF_MODE and not (trend_ok and htf_ok):
                diag["htf_block"] += 1
                continue
            if STRICT_HTF_MODE and not (reject and near_value):
                diag["setup_block"] += 1
                continue
            if not confirm5 or (CONFIRM_15M_REQUIRED and not confirm15) or not wick_ok:
                diag["setup_block"] += 1
                add_near_miss(symbol, side, score, strategy, "SHORT заблокирован: нет 2x5m + 15m подтверждения или плохой фитиль")
                continue
            if REQUIRE_LEVEL_PROXIMITY and not level_ok:
                diag["setup_block"] += 1
                add_near_miss(symbol, side, score, strategy, "SHORT заблокирован: вход не рядом с сопротивлением/EMA/VWAP")
                continue
            if not (trend_ok and reject and near_value):
                score -= 12
            score += 6
            reasons.append("подтверждение: 2 свечи 5m + 15m без плохого фитиля")
            if bounced > 5.5:
                trade_type = "SWING"
                strategy = "VOLATILITY_SWING_REJECT"

        # Quality bonus / active bonus
        if base_of(symbol) in QUALITY_BASES:
            score += 4
            reasons.append("quality coin")
        if abs(move_1h) >= 1.2 or abs(move_4h) >= 2.2:
            score += 5
            trade_type = "FAST" if trade_type == "TREND" and abs(move_1h) >= 1.4 else trade_type
            reasons.append("достаточная волатильность для цели 10% ROI x10")

        tp1, tp2, tp3, sl = calc_targets(price, atr15p, hi36, lo36, side, trade_type)
        roi_tp1 = price_move_roi(price, tp1, side)
        rr = risk_reward(price, tp1, sl, side)
        if roi_tp1 < MIN_TP1_ROI_X10:
            diag["tp1_roi_block"] += 1
            continue
        if rr < 0.75:
            diag["rr_block"] += 1
            continue

        # Score -> grade
        score = int(max(0, min(100, score)))
        grade = "A+" if score >= A_PLUS_MIN_SCORE else ("B" if score >= B_MIN_SCORE else "LOW")
        if grade == "LOW":
            diag["score_block"] += 1
            # store near-miss if close
            if score >= OPPORTUNITY_MIN_SCORE:
                add_near_miss(symbol, side, score, strategy, f"score {score}, нужен {B_MIN_SCORE}; ROI TP1 {roi_tp1:.1f}%")
            continue
        if grade == "B":
            # B is allowed, but only when the higher timeframe is clean and RR is not weak.
            if rr < 0.95:
                diag["rr_block"] += 1
                continue
        if is_disabled(strategy, side, grade):
            diag["adaptive_block"] += 1
            continue
        if in_cooldown(symbol, side, strategy):
            diag["cooldown_block"] += 1
            continue

        risk_mult = A_PLUS_RISK_MULT if grade == "A+" else B_RISK_MULT
        expected_time = "20–90 минут" if trade_type == "FAST" else ("2–8 часов" if trade_type == "SWING" else "45 минут – 3 часа")
        sig = Signal(
            symbol=symbol, side=side, grade=grade, strategy=strategy, trade_type=trade_type,
            entry=price, tp1=tp1, tp2=tp2, tp3=tp3, sl=sl,
            score=score, rr=rr, roi_tp1=roi_tp1, risk_mult=risk_mult,
            expected_time=expected_time,
            reason="\n".join(reasons[:7]),
            btc_context=btc.reason,
            created_at=now_ts(),
        )
        sig = stats_adjust_score(sig)
        # After stat adjustment, grade can improve/degrade.
        sig.grade = "A+" if sig.score >= A_PLUS_MIN_SCORE else ("B" if sig.score >= B_MIN_SCORE else "LOW")
        if sig.grade == "LOW":
            diag["score_block"] += 1
            continue
        if best is None or sig.score > best.score:
            best = sig

    if not best:
        return None
    diag["candidates"] += 1
    return best


def add_near_miss(symbol: str, side: str, score: int, strategy: str, reason: str) -> None:
    nm = STATE.setdefault("near_miss", [])
    nm.append({"time": now_ts(), "symbol": symbol, "side": side, "score": score, "strategy": strategy, "reason": reason})
    STATE["near_miss"] = nm[-10:]

# ---------- MESSAGES ----------
def fmt_price(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.8f}"


def build_signal_message(sig: Signal) -> str:
    emoji = "🟢" if sig.side == "LONG" else "🔴"
    return (
        f"{emoji} <b>{sig.grade} · {sig.side} {sig.symbol}</b>\n"
        f"Стратегия: {sig.strategy}\n"
        f"Тип: {sig.trade_type}\n"
        f"Ожидание: {sig.expected_time}\n\n"
        f"Вход: {fmt_price(sig.entry)}\n"
        f"TP1: {fmt_price(sig.tp1)}  (~{sig.roi_tp1:.1f}% ROI при x{LEVERAGE:g})\n"
        f"TP2: {fmt_price(sig.tp2)}\n"
        f"TP3: {fmt_price(sig.tp3)}\n"
        f"SL: {fmt_price(sig.sl)}\n\n"
        f"Score: {sig.score}/100 · RR к TP1: {sig.rr:.2f}\n"
        f"Риск: x{sig.risk_mult:.2f} от базового\n\n"
        f"<b>Почему вход:</b>\n{sig.reason}\n\n"
        f"<b>BTC:</b> {sig.btc_context}\n"
        f"Сценарий сломан, если цена закрепится за SL."
    )


def build_startup_message() -> str:
    return (
        f"✅ <b>{APP_NAME}</b> активирован и работает.\n"
        f"Deploy marker: <code>{DEPLOY_MARKER}</code>\n\n"
        f"Режим: LEVEL REVERSAL ANTI-CHASE — не шортить дно и не покупать вершину; работать от уровней.\n"
        f"Цель: TP1 минимум {MIN_TP1_ROI_X10:.0f}% ROI при x{LEVERAGE:g}.\n"
        f"A+ score: {A_PLUS_MIN_SCORE} · B score: {B_MIN_SCORE}.\n"
        f"HTF strict: {STRICT_HTF_MODE} · лимит сигналов/день: {MAX_SIGNALS_PER_DAY}.\n"
        f"Скан: каждые {AUTO_SCAN_SECONDS} сек · max analyze {MAX_ANALYZE_PER_SCAN}.\n"
        f"Статистика и TP/SL tracking: ON.\n"
        f"Forced diagnostics: ON — первый отчёт должен прийти сразу после запуска.\n"
        f"Диагностика включает: checked/candidates/blocks/profit/SL."
    )


def get_total_stats_summary() -> Dict[str, Any]:
    stats = STATE.get("stats", {})
    g = stats.get("global", {})
    # Support both old shape {positive, sl} and current shape {all:{positive,sl}}.
    if isinstance(g.get("all"), dict):
        pos = int(g.get("all", {}).get("positive", 0))
        sl = int(g.get("all", {}).get("sl", 0))
    else:
        pos = int(g.get("positive", 0))
        sl = int(g.get("sl", 0))
    total = pos + sl
    wr = (pos / total * 100.0) if total else 0.0
    return {"positive": pos, "sl": sl, "total": total, "wr": wr}


def build_diag_message(summary: Dict[str, Any]) -> str:
    near = STATE.get("near_miss", [])[-5:]
    near_txt = ""
    if near:
        near_txt = "\n\n<b>Почти сигналы:</b>\n" + "\n".join(
            f"• {x['symbol']} {x['side']} score {x['score']} — {x['reason']}" for x in near
        )
    st = get_total_stats_summary()
    return (
        f"🧪 <b>Диагностика V12.4 Level Reversal Anti-Chase</b>\n"
        f"Проверено: {summary.get('checked', 0)}\n"
        f"Кандидатов: {summary.get('candidates', 0)}\n"
        f"Отправлено: {summary.get('sent', 0)} / сегодня {daily_sent_count()}/{MAX_SIGNALS_PER_DAY}\n"
        f"BTC: {summary.get('btc', 'n/a')}\n"
        f"Блоки: btc {summary.get('btc_block', 0)}, htf {summary.get('htf_block', 0)}, setup {summary.get('setup_block', 0)}, "
        f"score {summary.get('score_block', 0)}, tp1 {summary.get('tp1_roi_block', 0)}, rr {summary.get('rr_block', 0)}, "
        f"vol {summary.get('low_vol_block', 0) + summary.get('too_volatile_block', 0)}, "
        f"ultra {summary.get('ultra_risk_block', 0)}, klines {summary.get('no_klines_5m', 0) + summary.get('no_klines_htf', 0)}, "
        f"daily {summary.get('daily_limit_block', 0)}, anti-chase {summary.get('anti_chase_block', 0)}, level-watch {summary.get('level_reversal_watch', 0)}\n\n"
        f"📊 <b>Итог сделок:</b>\n"
        f"Профитных: {st['positive']}\n"
        f"Stop Loss: {st['sl']}\n"
        f"Всего закрыто: {st['total']}\n"
        f"Win Rate: {st['wr']:.1f}%\n\n"
        f"Last error: {STATE.get('last_error') or '-'}"
        f"{near_txt}"
    )

# ---------- STATS & TRACKING ----------
def inc_stat(scope: str, key: str, result: str) -> None:
    st = STATE.setdefault("stats", {}).setdefault(scope, {}).setdefault(key, {"positive": 0, "sl": 0})
    if result == "positive":
        st["positive"] = st.get("positive", 0) + 1
    else:
        st["sl"] = st.get("sl", 0) + 1


def apply_result(sig: Dict[str, Any], result: str) -> None:
    side = sig.get("side", "")
    grade = sig.get("grade", "")
    strategy = sig.get("strategy", "")
    symbol = sig.get("symbol", "")
    combo = f"{strategy}:{side}:{grade}"
    inc_stat("global", "all", result)
    inc_stat("side", side, result)
    inc_stat("grade", grade, result)
    inc_stat("strategy", strategy, result)
    inc_stat("symbol", symbol, result)
    inc_stat("combo", combo, result)

    # Adaptive block after bad sample. Not too aggressive, but protects from repeated SL.
    st = STATE["stats"]["combo"].get(combo, {})
    total = st.get("positive", 0) + st.get("sl", 0)
    if total >= 5:
        wr = st.get("positive", 0) / total * 100
        if wr < 30:
            STATE.setdefault("disabled_until", {})[combo] = now_ts() + 24 * 3600


def build_stats_text() -> str:
    st_total = get_total_stats_summary()
    lines = [
        "📊 <b>Статистика V12.4 Level Reversal</b>",
        f"Итого: {st_total['positive']} профит / {st_total['sl']} SL / всего {st_total['total']} / WR {st_total['wr']:.1f}%",
    ]
    for scope, title in [("side", "Стороны"), ("grade", "Grade"), ("strategy", "Стратегии"), ("symbol", "Монеты")]:
        lines.append(f"\n<b>{title}:</b>")
        items = STATE.get("stats", {}).get(scope, {})
        if not items:
            lines.append("нет данных")
            continue
        # Sort by most trades first so the useful stats are visible.
        rows = []
        for k, v in items.items():
            pos, sl = int(v.get("positive", 0)), int(v.get("sl", 0))
            rows.append((pos + sl, k, pos, sl))
        for total, k, pos, sl in sorted(rows, reverse=True)[:18]:
            wr = pos / total * 100 if total else 0
            lines.append(f"{k}: {pos} профит / {sl} SL / WR {wr:.1f}%")
    return "\n".join(lines)[:3900]


def signal_to_dict(sig: Signal) -> Dict[str, Any]:
    d = asdict(sig)
    d.update({"tp1_hit": False, "tp2_hit": False, "tp3_hit": False})
    return d


def check_signal_result(sig: Dict[str, Any], price: float) -> Optional[str]:
    side = sig["side"]
    if side == "LONG":
        if price <= sig["sl"]:
            return "sl_after_tp1" if sig.get("tp1_hit") else "sl"
        if price >= sig["tp3"]:
            sig["tp3_hit"] = True; return "tp3"
        if price >= sig["tp2"] and not sig.get("tp2_hit"):
            sig["tp2_hit"] = True; return "tp2"
        if price >= sig["tp1"] and not sig.get("tp1_hit"):
            sig["tp1_hit"] = True; return "tp1"
    else:
        if price >= sig["sl"]:
            return "sl_after_tp1" if sig.get("tp1_hit") else "sl"
        if price <= sig["tp3"]:
            sig["tp3_hit"] = True; return "tp3"
        if price <= sig["tp2"] and not sig.get("tp2_hit"):
            sig["tp2_hit"] = True; return "tp2"
        if price <= sig["tp1"] and not sig.get("tp1_hit"):
            sig["tp1_hit"] = True; return "tp1"
    return None


def last_price(symbol: str) -> Optional[float]:
    c = get_klines(symbol, "1m", 60)
    if c:
        return c[-1]["close"]
    return None


async def track_active_signals() -> None:
    while True:
        try:
            active = STATE.get("active_signals", [])
            remaining = []
            changed = False
            for sig in active:
                price = last_price(sig["symbol"])
                if price is None:
                    remaining.append(sig); continue
                res = check_signal_result(sig, price)
                if res == "tp1":
                    send_telegram(f"✅ <b>TP1</b> — {sig['symbol']} {sig['side']}\nЦена: {fmt_price(price)}\nСделка уже дала минимум около 10% ROI при x{LEVERAGE:g}.")
                    changed = True; remaining.append(sig)
                elif res == "tp2":
                    send_telegram(f"✅ <b>TP2</b> — {sig['symbol']} {sig['side']}\nЦена: {fmt_price(price)}")
                    changed = True; remaining.append(sig)
                elif res == "tp3":
                    send_telegram(f"🏁 <b>TP3</b> — {sig['symbol']} {sig['side']}\nЦена: {fmt_price(price)}\nСделка закрыта позитивно.\n\n{build_stats_text()}")
                    apply_result(sig, "positive")
                    changed = True
                elif res == "sl_after_tp1":
                    send_telegram(f"⚠️ <b>SL после TP1</b> — {sig['symbol']} {sig['side']}\nЦена: {fmt_price(price)}\nСделка считалась позитивной, потому что TP1 был достигнут.\n\n{build_stats_text()}")
                    apply_result(sig, "positive")
                    changed = True
                elif res == "sl":
                    send_telegram(f"❌ <b>Stop Loss</b> — {sig['grade']} · {sig['side']} {sig['symbol']}\nСтратегия: {sig['strategy']}\nЦена: {fmt_price(price)}\nSL сработал до TP1.\n\n{build_stats_text()}")
                    apply_result(sig, "sl")
                    changed = True
                else:
                    remaining.append(sig)
            STATE["active_signals"] = remaining
            if changed:
                save_state()
        except Exception as e:
            STATE["last_error"] = f"track: {e}"
        await asyncio.sleep(TRACK_SECONDS)

# ---------- SCAN ----------
def empty_diag() -> Dict[str, int]:
    keys = [
        "checked", "candidates", "sent", "no_klines_5m", "no_klines_htf", "ultra_risk_block",
        "low_vol_block", "too_volatile_block", "volume_block", "not_active_block", "btc_block",
        "tp1_roi_block", "rr_block", "score_block", "adaptive_block", "cooldown_block",
        "htf_block", "setup_block", "anti_chase_block", "level_reversal_watch", "daily_limit_block"
    ]
    return {k: 0 for k in keys}


def run_scan_once(force_diag: bool = False) -> Dict[str, Any]:
    start = time.time()
    diag = empty_diag()
    btc = analyze_btc()
    if not btc:
        STATE["last_error"] = "BTC klines unavailable — scan cannot build market context"
        diag["btc"] = "unavailable"
        STATE["last_scan_at"] = now_ts()
        STATE["last_scan_summary"] = diag
        save_state()
        # Critical fix: previous versions returned here without sending diagnostics.
        if force_diag or now_ts() - STATE.get("last_diag_at", 0) >= DIAG_SECONDS:
            send_telegram(build_diag_message(diag))
            STATE["last_diag_at"] = now_ts()
            save_state()
        return diag
    diag["btc"] = f"{btc.regime} / {btc.reason}"

    symbols = choose_symbols(get_contracts())
    # Slight bias: if BTC bearish, quality+alts that move are enough. Random rotation keeps discovery alive.
    best_signals: List[Signal] = []
    for symbol in symbols:
        if time.time() - start > SCAN_CYCLE_TIMEOUT:
            STATE["last_error"] = "scan timeout"
            break
        if len(STATE.get("active_signals", [])) + len(best_signals) >= MAX_ACTIVE_SIGNALS:
            break
        diag["checked"] += 1
        sig = analyze_symbol(symbol, btc, diag)
        if sig:
            best_signals.append(sig)

    # Sort by score then A+ first.
    best_signals.sort(key=lambda s: (1 if s.grade == "A+" else 0, s.score, s.rr), reverse=True)
    # V12.4 level anti-chase mode: send only the best signal, respect daily cap.
    for sig in best_signals[:MAX_SIGNALS_PER_SCAN]:
        if daily_sent_count() >= MAX_SIGNALS_PER_DAY:
            diag["daily_limit_block"] += 1
            break
        send_telegram(build_signal_message(sig))
        STATE.setdefault("active_signals", []).append(signal_to_dict(sig))
        set_cooldown(sig.symbol, sig.side, sig.strategy)
        record_daily_signal()
        STATE["last_signal_at"] = now_ts()
        diag["sent"] += 1

    STATE["last_scan_at"] = now_ts()
    STATE["last_scan_summary"] = diag
    save_state()

    # Diagnostics if no signals.
    if force_diag or (diag["sent"] == 0 and now_ts() - STATE.get("last_diag_at", 0) >= DIAG_SECONDS):
        send_telegram(build_diag_message(diag))
        STATE["last_diag_at"] = now_ts()
        save_state()
    return diag


async def auto_worker() -> None:
    await asyncio.sleep(2)
    ok = send_telegram(build_startup_message() + "\n\n🧪 Сейчас запускаю обязательный первый диагностический скан.")
    if not ok:
        print("startup telegram was not delivered: " + str(STATE.get("last_error")), flush=True)
    # First diagnostic scan always. It must send a Telegram diagnostic even when BTC/API fails.
    try:
        run_scan_once(force_diag=True)
    except Exception as e:
        STATE["last_error"] = f"first_scan: {e}\n{traceback.format_exc()[-700:]}"
        save_state()
        send_telegram(f"⚠️ <b>Ошибка первого скана V12.4</b>\n<code>{str(e)[:500]}</code>")
    while True:
        try:
            run_scan_once(force_diag=False)
        except Exception as e:
            STATE["last_error"] = f"auto_scan: {e}\n{traceback.format_exc()[-700:]}"
            save_state()
            # Critical fix: do not keep scan errors silent for hours.
            if now_ts() - STATE.get("last_diag_at", 0) >= DIAG_SECONDS:
                send_telegram(f"⚠️ <b>Ошибка auto-scan V12.4</b>\n<code>{str(e)[:700]}</code>")
                STATE["last_diag_at"] = now_ts()
                save_state()
        await asyncio.sleep(AUTO_SCAN_SECONDS)

# ---------- ROUTES ----------
@app.get("/", response_class=HTMLResponse)
def root():
    return f"<h3>{APP_NAME}</h3><pre>{json.dumps(STATE.get('last_scan_summary', {}), ensure_ascii=False, indent=2)}</pre>"

@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME, "deploy_marker": DEPLOY_MARKER, "last_scan_at": STATE.get("last_scan_at"), "last_error": STATE.get("last_error")}

@app.get("/version")
def version():
    return {"app": APP_NAME, "deploy_marker": DEPLOY_MARKER}

@app.get("/auto-status")
def auto_status():
    return {
        "app": APP_NAME,
        "deploy_marker": DEPLOY_MARKER,
        "last_scan_at": STATE.get("last_scan_at"),
        "last_signal_at": STATE.get("last_signal_at"),
        "active_signals": STATE.get("active_signals", []),
        "last_scan_summary": STATE.get("last_scan_summary", {}),
        "near_miss": STATE.get("near_miss", []),
        "last_error": STATE.get("last_error", ""),
    }

@app.get("/scan")
def manual_scan():
    diag = run_scan_once(force_diag=True)
    return diag

@app.get("/stats", response_class=PlainTextResponse)
def stats_route():
    # plain text without HTML tags
    return build_stats_text().replace("<b>", "").replace("</b>", "")

@app.get("/send-analysis")
def send_analysis():
    diag = STATE.get("last_scan_summary", {})
    ok = send_telegram(build_diag_message(diag))
    return {"sent": ok, "summary": diag}

@app.get("/test-telegram")
def test_telegram():
    ok = send_telegram(f"✅ Test Telegram OK\n{APP_NAME}\n{DEPLOY_MARKER}")
    return {"sent": ok}

@app.on_event("startup")
async def on_startup():
    # Works for uvicorn Web Service too.
    asyncio.create_task(auto_worker())
    asyncio.create_task(track_active_signals())

if __name__ == "__main__":
    # Background Worker mode: python bot.py
    async def main():
        asyncio.create_task(auto_worker())
        asyncio.create_task(track_active_signals())
        while True:
            await asyncio.sleep(3600)
    asyncio.run(main())
