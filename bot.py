import os
import time
import json
import math
import asyncio
import random
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any, Tuple

import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

# ============================================================
# V11 Professional Futures Trader
# Signals only. No exchange order placement.
# Philosophy:
# quality universe -> BTC regime -> HTF context -> strategy -> entry near invalidation -> TP1 >= 10% ROI at x10
# ============================================================

APP_NAME = "Professional Adaptive Futures Bot AUTO V11.0 PROFESSIONAL FUTURES TRADER"
DEPLOY_MARKER = "V11_0_PROFESSIONAL_FUTURES_TRADER_2026_06_14"

app = FastAPI(title=APP_NAME)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
BINGX_BASE_URL = "https://open-api.bingx.com"
STATE_FILE = os.getenv("STATE_FILE", "bot_state_v11.json")

TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
LEVERAGE = float(os.getenv("LEVERAGE", "10"))
MIN_TP1_ROI_PERCENT = float(os.getenv("MIN_TP1_ROI_PERCENT", "10"))
MIN_TP1_PRICE_MOVE_PERCENT = MIN_TP1_ROI_PERCENT / max(LEVERAGE, 1.0)

AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "90"))
TRACK_SECONDS = int(os.getenv("TRACK_SECONDS", "35"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "450"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "1800"))
MAX_ACTIVE_SIGNALS = int(os.getenv("MAX_ACTIVE_SIGNALS", "8"))

# Grades. B is not garbage. B = medium quality with reduced risk.
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "90"))
B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "82"))
ALLOW_B_SIGNALS = os.getenv("ALLOW_B_SIGNALS", "true").lower() == "true"
A_PLUS_RISK_MULTIPLIER = float(os.getenv("A_PLUS_RISK_MULTIPLIER", "1.0"))
B_RISK_MULTIPLIER = float(os.getenv("B_RISK_MULTIPLIER", "0.35"))
EXTREME_RISK_MULTIPLIER = float(os.getenv("EXTREME_RISK_MULTIPLIER", "0.12"))

MIN_QUOTE_VOLUME_USDT = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "8000000"))
MIN_ACTIVE_QUOTE_VOLUME_USDT = float(os.getenv("MIN_ACTIVE_QUOTE_VOLUME_USDT", "15000000"))
DYNAMIC_TOP_N = int(os.getenv("DYNAMIC_TOP_N", "180"))
DYNAMIC_MIN_CHANGE_PERCENT = float(os.getenv("DYNAMIC_MIN_CHANGE_PERCENT", "4.0"))
EXTREME_MIN_CHANGE_PERCENT = float(os.getenv("EXTREME_MIN_CHANGE_PERCENT", "18.0"))
ULTRA_RISK_5M_MOVE_BLOCK = float(os.getenv("ULTRA_RISK_5M_MOVE_BLOCK", "8.0"))
ULTRA_RISK_15M_MOVE_BLOCK = float(os.getenv("ULTRA_RISK_15M_MOVE_BLOCK", "12.0"))

# Adaptive disabling.
MIN_TRADES_FOR_DISABLE = int(os.getenv("MIN_TRADES_FOR_DISABLE", "4"))
MIN_WR_FOR_ENABLE = float(os.getenv("MIN_WR_FOR_ENABLE", "42"))
DISABLE_AFTER_CONSECUTIVE_SL = int(os.getenv("DISABLE_AFTER_CONSECUTIVE_SL", "3"))
DISABLE_HOURS = int(os.getenv("DISABLE_HOURS", "72"))

QUALITY_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "LINK", "AVAX", "AAVE", "SUI", "TAO", "NEAR", "INJ", "SEI",
    "ARB", "OP", "APT", "TON", "DOT", "ADA", "DOGE", "LTC", "BCH", "UNI", "ETC", "ATOM", "FIL",
    "TRX", "MATIC", "POL", "ICP", "MKR", "RUNE", "FET", "TIA", "JUP", "WLD", "ENA", "ONDO",
    "ORDI", "STX", "PENDLE", "JTO", "PYTH", "LDO", "CRV", "COMP", "SAND", "MANA", "GALA"
}

# Hard block for normal strategies. These can still be watched only by EXTREME_CONTEXT if enabled.
ULTRA_RISK_BASES = {
    "HMSTR", "GUA", "VELVET", "BEAT", "COLLECT", "SPACE", "GOBLIN", "MAGMA", "FOLKS", "FIGHT", "BLEND",
    "PEPE", "BONK", "WIF", "TURBO", "BOME", "MOODENG", "NEIRO", "GOAT", "PNUT", "ACT", "DOGS", "CATI", "MEME",
    "NOT", "1000SATS", "1000PEPE", "1000BONK", "1000FLOKI", "SHIB", "FLOKI"
}

BLOCKED_BASES = {"USDC", "BUSD", "FDUSD", "TUSD", "DAI"}

STATE: Dict[str, Any] = {
    "active_signals": [],
    "stats": {},
    "disabled_until": {},
    "last_signal_at": {},
    "last_scan_at": 0,
    "last_scan_summary": {},
    "last_error": "",
}

# ----------------------------- utils -----------------------------

def now_ts() -> int:
    return int(time.time())


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.upper().replace("/", "-").replace("_", "-")
    if symbol.endswith("USDT") and "-" not in symbol:
        symbol = symbol[:-4] + "-USDT"
    return symbol


def display_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-", "/")


def base_from_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).split("-")[0]


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def fmt_price(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.8f}"


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def load_state() -> None:
    global STATE
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for k, v in data.items():
                    STATE[k] = v
    except Exception as e:
        STATE["last_error"] = f"load_state: {e}"


def save_state() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
    except Exception as e:
        STATE["last_error"] = f"save_state: {e}"


def stat_key(strategy: str, side: str, grade: str) -> str:
    return f"{strategy}:{side}:{grade}"


def stat_base_key(strategy: str, side: str) -> str:
    return f"{strategy}:{side}"


def get_stat(key: str) -> Dict[str, Any]:
    stats = STATE.setdefault("stats", {})
    if key not in stats:
        stats[key] = {"tp": 0, "sl": 0, "consecutive_sl": 0}
    return stats[key]


def win_rate(s: Dict[str, Any]) -> float:
    total = int(s.get("tp", 0)) + int(s.get("sl", 0))
    if total == 0:
        return 0.0
    return int(s.get("tp", 0)) / total * 100.0


def is_disabled(strategy: str, side: str, grade: Optional[str] = None) -> Tuple[bool, str]:
    disabled = STATE.setdefault("disabled_until", {})
    keys = [stat_base_key(strategy, side)]
    if grade:
        keys.append(stat_key(strategy, side, grade))
    t = now_ts()
    for k in keys:
        until = int(disabled.get(k, 0))
        if until > t:
            return True, f"{k} отключён до {time.strftime('%Y-%m-%d %H:%M', time.localtime(until))}"
    return False, ""


def adaptive_allows(strategy: str, side: str, grade: str) -> Tuple[bool, str]:
    disabled, reason = is_disabled(strategy, side, grade)
    if disabled:
        return False, reason
    # If a strategy side has enough bad data, stop both A+ and B for a cooldown.
    base = get_stat(stat_base_key(strategy, side))
    total = int(base.get("tp", 0)) + int(base.get("sl", 0))
    if total >= MIN_TRADES_FOR_DISABLE and win_rate(base) < MIN_WR_FOR_ENABLE:
        STATE.setdefault("disabled_until", {})[stat_base_key(strategy, side)] = now_ts() + DISABLE_HOURS * 3600
        save_state()
        return False, f"{strategy} {side} заблокирован: WR {win_rate(base):.1f}%"
    return True, ""


def apply_result(signal: Dict[str, Any], positive: bool) -> None:
    strategy = signal.get("strategy", "UNKNOWN")
    side = signal.get("side", "UNKNOWN")
    grade = signal.get("grade", "UNKNOWN")
    for key in [stat_base_key(strategy, side), stat_key(strategy, side, grade), f"GRADE:{grade}", f"SIDE:{side}"]:
        s = get_stat(key)
        if positive:
            s["tp"] = int(s.get("tp", 0)) + 1
            s["consecutive_sl"] = 0
        else:
            s["sl"] = int(s.get("sl", 0)) + 1
            s["consecutive_sl"] = int(s.get("consecutive_sl", 0)) + 1
            if s["consecutive_sl"] >= DISABLE_AFTER_CONSECUTIVE_SL and ":" in key and not key.startswith("GRADE") and not key.startswith("SIDE"):
                STATE.setdefault("disabled_until", {})[key] = now_ts() + DISABLE_HOURS * 3600
    save_state()


def send_telegram(text: str) -> bool:
    if TEST_MODE:
        print(text)
        return True
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram env missing")
        print(text)
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=12)
        return r.status_code == 200
    except Exception as e:
        STATE["last_error"] = f"send_telegram: {e}"
        return False

# ----------------------------- indicators -----------------------------

def closes(c: List[Dict[str, float]]) -> List[float]:
    return [x["close"] for x in c]


def highs(c: List[Dict[str, float]]) -> List[float]:
    return [x["high"] for x in c]


def lows(c: List[Dict[str, float]]) -> List[float]:
    return [x["low"] for x in c]


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(values: List[float], period: int) -> List[float]:
    out = []
    for i in range(len(values)):
        start = max(0, i - period + 1)
        chunk = values[start:i+1]
        out.append(sum(chunk) / len(chunk))
    return out


def atr(c: List[Dict[str, float]], period: int = 14) -> List[float]:
    if not c:
        return []
    trs = []
    prev = c[0]["close"]
    for x in c:
        tr = max(x["high"] - x["low"], abs(x["high"] - prev), abs(x["low"] - prev))
        trs.append(tr)
        prev = x["close"]
    return ema(trs, period)


def rsi(values: List[float], period: int = 14) -> List[float]:
    if len(values) < 2:
        return [50.0] * len(values)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(values)):
        d = values[i] - values[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = ema(gains, period)
    al = ema(losses, period)
    out = []
    for g, l in zip(ag, al):
        if l == 0:
            out.append(100.0)
        else:
            rs = g / l
            out.append(100 - 100 / (1 + rs))
    return out


def vwap(c: List[Dict[str, float]], lookback: int = 48) -> float:
    chunk = c[-lookback:]
    pv, vol = 0.0, 0.0
    for x in chunk:
        typical = (x["high"] + x["low"] + x["close"]) / 3
        v = max(x.get("volume", 0.0), 0.0)
        pv += typical * v
        vol += v
    return pv / vol if vol > 0 else chunk[-1]["close"]


def recent_high(c: List[Dict[str, float]], lookback: int) -> float:
    return max(highs(c[-lookback:]))


def recent_low(c: List[Dict[str, float]], lookback: int) -> float:
    return min(lows(c[-lookback:]))


def avg_volume(c: List[Dict[str, float]], lookback: int = 40) -> float:
    vols = [x.get("volume", 0.0) for x in c[-lookback:]]
    return sum(vols) / len(vols) if vols else 0.0


def volume_ratio(c: List[Dict[str, float]], lookback: int = 40) -> float:
    if len(c) < 5:
        return 1.0
    av = avg_volume(c[:-1], lookback)
    return c[-1].get("volume", 0.0) / av if av > 0 else 1.0


def candle_move_percent(x: Dict[str, float]) -> float:
    if x["open"] == 0:
        return 0.0
    return abs(x["close"] - x["open"]) / x["open"] * 100.0


def wick_exhaustion(c: List[Dict[str, float]], side: str) -> bool:
    x = c[-1]
    rng = max(x["high"] - x["low"], 1e-12)
    body = abs(x["close"] - x["open"])
    upper = x["high"] - max(x["close"], x["open"])
    lower = min(x["close"], x["open"]) - x["low"]
    # LONG after huge upper wick = exhausted. SHORT after huge lower wick = exhausted.
    if side == "LONG" and upper / rng > 0.45 and body / rng < 0.45:
        return True
    if side == "SHORT" and lower / rng > 0.45 and body / rng < 0.45:
        return True
    return False

# ----------------------------- BingX -----------------------------

def get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        STATE["last_error"] = f"get_json: {e}"
        return None


def extract_quote_volume_usdt(item: Dict[str, Any]) -> float:
    for k in ["quoteVolume", "quoteVol", "turnover", "amount", "volumeUsd", "volValue"]:
        if k in item:
            v = safe_float(item.get(k), 0.0)
            if v > 0:
                return v
    price = safe_float(item.get("lastPrice") or item.get("last") or item.get("close") or item.get("price"), 0.0)
    vol = safe_float(item.get("volume") or item.get("baseVolume") or item.get("vol"), 0.0)
    return price * vol


def extract_change_percent(item: Dict[str, Any]) -> Optional[float]:
    for k in ["priceChangePercent", "priceChangeRate", "changePercent", "change", "changeRate", "riseFallRate"]:
        if k in item:
            try:
                v = float(item.get(k))
                if abs(v) <= 2:
                    v *= 100
                return v
            except Exception:
                pass
    return None


def get_tickers() -> List[Dict[str, Any]]:
    for endpoint in ["/openApi/swap/v2/quote/ticker", "/openApi/swap/v2/quote/ticker/24hr"]:
        data = get_json(f"{BINGX_BASE_URL}{endpoint}")
        raw = data.get("data", []) if isinstance(data, dict) else []
        if isinstance(raw, dict):
            raw = list(raw.values()) if not raw.get("symbol") else [raw]
        if raw:
            return [x for x in raw if isinstance(x, dict)]
    return []


def is_good_symbol(symbol: str) -> bool:
    s = normalize_symbol(symbol)
    if not s.endswith("-USDT"):
        return False
    base = base_from_symbol(s)
    if base in BLOCKED_BASES:
        return False
    if any(x in base for x in ["UP", "DOWN", "BULL", "BEAR"]):
        return False
    return True


def get_dynamic_symbols() -> List[str]:
    movers: List[Tuple[float, float, str]] = []
    for item in get_tickers():
        symbol = normalize_symbol(item.get("symbol") or item.get("s") or "")
        if not is_good_symbol(symbol):
            continue
        ch = extract_change_percent(item)
        if ch is None:
            continue
        qv = extract_quote_volume_usdt(item)
        base = base_from_symbol(symbol)
        min_vol = MIN_ACTIVE_QUOTE_VOLUME_USDT if base not in QUALITY_BASES else MIN_QUOTE_VOLUME_USDT
        if qv < min_vol:
            continue
        if abs(ch) >= DYNAMIC_MIN_CHANGE_PERCENT:
            movers.append((abs(ch), qv, symbol))
    movers.sort(reverse=True)
    result = []
    for _, _, s in movers:
        if s not in result:
            result.append(s)
        if len(result) >= DYNAMIC_TOP_N:
            break
    return result


def get_symbols() -> List[str]:
    data = get_json(f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts")
    all_symbols = []
    if isinstance(data, dict):
        for item in data.get("data", []) or []:
            s = normalize_symbol(item.get("symbol", ""))
            if is_good_symbol(s):
                all_symbols.append(s)
    dynamic = get_dynamic_symbols()
    priority = []
    for s in all_symbols:
        if base_from_symbol(s) in QUALITY_BASES:
            priority.append(s)
    random.shuffle(all_symbols)
    result = []
    for s in dynamic + priority + all_symbols:
        if s not in result:
            result.append(s)
        if len(result) >= MAX_SYMBOLS:
            break
    return result


def get_klines(symbol: str, interval: str, limit: int = 260) -> Optional[List[Dict[str, float]]]:
    data = get_json(f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines", params={"symbol": normalize_symbol(symbol), "interval": interval, "limit": limit})
    raw = data.get("data", []) if isinstance(data, dict) else []
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
                "volume": float(c.get("volume", 0.0)),
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["time"])
    return candles if len(candles) >= 60 else None

# ----------------------------- market model -----------------------------

@dataclass
class MarketRegime:
    trend: str
    score: int
    reason: str
    fast_change_15m: float
    fast_change_5m: float

@dataclass
class CandidateSignal:
    symbol: str
    side: str
    grade: str
    strategy: str
    trade_type: str
    expected_time: str
    entry: float
    tp1: float
    tp2: float
    tp3: float
    sl: float
    score: int
    rr: float
    risk_multiplier: float
    reason: str
    invalidation: str
    btc_context: str
    roi_tp1: float
    price_move_tp1: float


def trend_from_candles(c: List[Dict[str, float]]) -> Tuple[str, int, str]:
    cl = closes(c)
    e20, e50, e200 = ema(cl, 20), ema(cl, 50), ema(cl, 200)
    last = cl[-1]
    slope50 = pct(e50[-1], e50[-10]) if len(e50) > 15 else 0.0
    r = rsi(cl, 14)[-1]
    score_up = 0
    score_down = 0
    if last > e20[-1]: score_up += 15
    if last > e50[-1]: score_up += 20
    if e20[-1] > e50[-1]: score_up += 20
    if e50[-1] > e200[-1]: score_up += 25
    if slope50 > 0.15: score_up += 10
    if r > 52: score_up += 10
    if last < e20[-1]: score_down += 15
    if last < e50[-1]: score_down += 20
    if e20[-1] < e50[-1]: score_down += 20
    if e50[-1] < e200[-1]: score_down += 25
    if slope50 < -0.15: score_down += 10
    if r < 48: score_down += 10
    if score_up >= 65 and score_up > score_down + 10:
        return "TREND_UP", score_up, f"цена выше EMA, RSI {r:.0f}, slope50 {slope50:.2f}%"
    if score_down >= 65 and score_down > score_up + 10:
        return "TREND_DOWN", score_down, f"цена ниже EMA, RSI {r:.0f}, slope50 {slope50:.2f}%"
    rh, rl = recent_high(c, 60), recent_low(c, 60)
    width = (rh - rl) / max(last, 1e-12) * 100
    if 1.8 <= width <= 9.0:
        return "RANGE", 55, f"диапазон ~{width:.1f}%"
    return "CHOP", 35, "нет чистого тренда/диапазона"


def get_btc_regime() -> MarketRegime:
    c15 = get_klines("BTC-USDT", "15m", 160)
    c5 = get_klines("BTC-USDT", "5m", 120)
    c1h = get_klines("BTC-USDT", "1h", 220)
    c4h = get_klines("BTC-USDT", "4h", 220)
    if not c15 or not c5 or not c1h or not c4h:
        return MarketRegime("UNKNOWN", 0, "BTC data unavailable", 0, 0)
    ch15 = pct(c15[-1]["close"], c15[-5]["close"])
    ch5 = pct(c5[-1]["close"], c5[-4]["close"])
    t1, s1, r1 = trend_from_candles(c1h)
    t4, s4, r4 = trend_from_candles(c4h)
    if ch15 > 0.55 or ch5 > 0.35:
        return MarketRegime("IMPULSE_UP", 85, f"BTC быстрый импульс вверх: 15m {ch15:.2f}%, 5m {ch5:.2f}%", ch15, ch5)
    if ch15 < -0.55 or ch5 < -0.35:
        return MarketRegime("IMPULSE_DOWN", 85, f"BTC быстрый импульс вниз: 15m {ch15:.2f}%, 5m {ch5:.2f}%", ch15, ch5)
    if t1 == "TREND_UP" and t4 in ["TREND_UP", "RANGE"]:
        return MarketRegime("TREND_UP", min(95, (s1+s4)//2), f"BTC 1H {t1}, 4H {t4}. {r1}", ch15, ch5)
    if t1 == "TREND_DOWN" and t4 in ["TREND_DOWN", "RANGE"]:
        return MarketRegime("TREND_DOWN", min(95, (s1+s4)//2), f"BTC 1H {t1}, 4H {t4}. {r1}", ch15, ch5)
    if t1 == "RANGE" or t4 == "RANGE":
        return MarketRegime("RANGE", 60, f"BTC диапазон/нейтрально: 1H {t1}, 4H {t4}", ch15, ch5)
    return MarketRegime("CHOP", 35, f"BTC без ясного режима: 1H {t1}, 4H {t4}", ch15, ch5)


def btc_allows(side: str, btc: MarketRegime, is_extreme: bool = False) -> Tuple[bool, str, int]:
    # returns allowed, text, score_delta
    if btc.trend == "UNKNOWN":
        return True, "BTC data unavailable, риск ниже", -8
    if side == "LONG":
        if btc.trend == "IMPULSE_DOWN":
            return False, btc.reason, -99
        if btc.trend == "TREND_DOWN" and not is_extreme:
            return False, btc.reason, -99
        if btc.trend in ["TREND_UP", "IMPULSE_UP"]:
            return True, btc.reason, 12
        if btc.trend == "RANGE":
            return True, btc.reason, 2
    if side == "SHORT":
        if btc.trend == "IMPULSE_UP":
            return False, btc.reason, -99
        if btc.trend == "TREND_UP" and not is_extreme:
            return False, btc.reason, -99
        if btc.trend in ["TREND_DOWN", "IMPULSE_DOWN"]:
            return True, btc.reason, 12
        if btc.trend == "RANGE":
            return True, btc.reason, 2
    if btc.trend == "CHOP":
        return True, btc.reason, -8
    return True, btc.reason, 0


def is_ultra_risk(symbol: str, c5: Optional[List[Dict[str, float]]] = None, c15: Optional[List[Dict[str, float]]] = None) -> bool:
    base = base_from_symbol(symbol)
    if base in ULTRA_RISK_BASES:
        return True
    if c5:
        biggest = max(candle_move_percent(x) for x in c5[-12:])
        if biggest >= ULTRA_RISK_5M_MOVE_BLOCK:
            return True
    if c15:
        biggest = max(candle_move_percent(x) for x in c15[-8:])
        if biggest >= ULTRA_RISK_15M_MOVE_BLOCK:
            return True
    return False


def build_signal(symbol: str, side: str, strategy: str, trade_type: str, entry: float, sl: float, tp1: float, score: int,
                 reason: str, invalidation: str, btc_context: str, expected_time: str, risk_mult: float) -> Optional[CandidateSignal]:
    if side == "LONG":
        if not (sl < entry < tp1):
            return None
        price_move = (tp1 - entry) / entry * 100
        risk = entry - sl
        reward = tp1 - entry
        tp2 = entry + max(reward * 1.8, entry * 0.018)
        tp3 = entry + max(reward * 3.0, entry * 0.030)
    else:
        if not (tp1 < entry < sl):
            return None
        price_move = (entry - tp1) / entry * 100
        risk = sl - entry
        reward = entry - tp1
        tp2 = entry - max(reward * 1.8, entry * 0.018)
        tp3 = entry - max(reward * 3.0, entry * 0.030)
    roi = price_move * LEVERAGE
    if roi < MIN_TP1_ROI_PERCENT:
        return None
    rr = reward / max(risk, entry * 0.0001)
    # Need realistic RR. Swing can have slightly lower tp1 RR, because tp2 is structural.
    min_rr = 0.65 if trade_type in ["RANGE EDGE", "STRUCTURE SWING"] else 0.80
    if rr < min_rr:
        return None
    grade = "A+" if score >= A_PLUS_MIN_SCORE and rr >= 0.90 else "B"
    if grade == "B" and (not ALLOW_B_SIGNALS or score < B_MIN_SCORE):
        return None
    allowed, why = adaptive_allows(strategy, side, grade)
    if not allowed:
        return None
    risk_multiplier = risk_mult * (A_PLUS_RISK_MULTIPLIER if grade == "A+" else B_RISK_MULTIPLIER)
    return CandidateSignal(symbol, side, grade, strategy, trade_type, expected_time, entry, tp1, tp2, tp3, sl, score, rr,
                           risk_multiplier, reason, invalidation, btc_context, roi, price_move)

# ----------------------------- strategies -----------------------------

def strategy_trend_pullback(symbol: str, btc: MarketRegime, c5, c15, c1h, c4h) -> List[CandidateSignal]:
    out = []
    base = base_from_symbol(symbol)
    if is_ultra_risk(symbol, c5, c15):
        return out
    t1, s1, r1 = trend_from_candles(c1h)
    t4, s4, r4 = trend_from_candles(c4h)
    cl15, cl5 = closes(c15), closes(c5)
    entry = cl5[-1]
    e20_15, e50_15 = ema(cl15, 20), ema(cl15, 50)
    e20_5, e50_5 = ema(cl5, 20), ema(cl5, 50)
    v15 = vwap(c15, 64)
    a15 = atr(c15, 14)[-1]
    volr = volume_ratio(c15)
    # LONG pullback: HTF trend, pullback to ema/vwap, reclaim on 5m/15m.
    for side in ["LONG", "SHORT"]:
        ok_btc, btc_text, btc_delta = btc_allows(side, btc)
        if not ok_btc:
            continue
        if side == "LONG":
            if not (t1 == "TREND_UP" and t4 in ["TREND_UP", "RANGE"]):
                continue
            near_zone = min(abs(entry - e20_15[-1]), abs(entry - e50_15[-1]), abs(entry - v15)) / entry * 100 <= 1.2
            reclaim = cl5[-1] > e20_5[-1] and cl5[-2] <= e20_5[-2] or (cl5[-1] > cl5[-2] > cl5[-3] and cl5[-1] > e50_5[-1])
            not_late = pct(entry, recent_low(c15, 20)) <= 3.8
            if not (near_zone and reclaim and not_late) or wick_exhaustion(c5, side):
                continue
            swing_low = recent_low(c15, 24)
            sl = min(swing_low - a15 * 0.18, entry * 0.990)
            tp_struct = min(recent_high(c1h, 80), entry * 1.045)
            tp1 = max(entry * (1 + MIN_TP1_PRICE_MOVE_PERCENT/100 + 0.001), min(tp_struct, entry * 1.022))
            score = 58 + btc_delta + int(s1*0.12) + int(s4*0.08) + (10 if volr >= 0.95 else 0) + (8 if base in QUALITY_BASES else 0)
            reason = f"Трендовый откат: 1H {t1}, 4H {t4}. Цена вернулась к EMA/VWAP и 5m показывает возврат покупателя. Volume x{volr:.2f}."
            sig = build_signal(symbol, side, "TREND_PULLBACK", "TREND PULLBACK", entry, sl, tp1, score, reason,
                               "сценарий сломан при закреплении ниже зоны отката/локального swing low", btc_text, "20 минут – 2 часа", 1.0)
            if sig: out.append(sig)
        else:
            if not (t1 == "TREND_DOWN" and t4 in ["TREND_DOWN", "RANGE"]):
                continue
            near_zone = min(abs(entry - e20_15[-1]), abs(entry - e50_15[-1]), abs(entry - v15)) / entry * 100 <= 1.2
            reclaim = cl5[-1] < e20_5[-1] and cl5[-2] >= e20_5[-2] or (cl5[-1] < cl5[-2] < cl5[-3] and cl5[-1] < e50_5[-1])
            not_late = pct(recent_high(c15, 20), entry) <= 3.8
            if not (near_zone and reclaim and not_late) or wick_exhaustion(c5, side):
                continue
            swing_high = recent_high(c15, 24)
            sl = max(swing_high + a15 * 0.18, entry * 1.010)
            tp_struct = max(recent_low(c1h, 80), entry * 0.955)
            tp1 = min(entry * (1 - MIN_TP1_PRICE_MOVE_PERCENT/100 - 0.001), max(tp_struct, entry * 0.978))
            score = 58 + btc_delta + int(s1*0.12) + int(s4*0.08) + (10 if volr >= 0.95 else 0) + (8 if base in QUALITY_BASES else 0)
            reason = f"Трендовый откат: 1H {t1}, 4H {t4}. Цена вернулась к EMA/VWAP и 5m показывает возврат продавца. Volume x{volr:.2f}."
            sig = build_signal(symbol, side, "TREND_PULLBACK", "TREND PULLBACK", entry, sl, tp1, score, reason,
                               "сценарий сломан при закреплении выше зоны отката/локального swing high", btc_text, "20 минут – 2 часа", 1.0)
            if sig: out.append(sig)
    return out


def strategy_range_edge(symbol: str, btc: MarketRegime, c5, c15, c1h, c4h) -> List[CandidateSignal]:
    out = []
    if is_ultra_risk(symbol, c5, c15):
        return out
    t1, s1, r1 = trend_from_candles(c1h)
    # Range edge works if coin or BTC is range, not strong opposite trend.
    if t1 not in ["RANGE", "CHOP"] and btc.trend != "RANGE":
        return out
    entry = c5[-1]["close"]
    hi = recent_high(c1h, 72)
    lo = recent_low(c1h, 72)
    width = (hi - lo) / max(entry, 1e-12) * 100
    if not (2.0 <= width <= 12.0):
        return out
    pos = (entry - lo) / max(hi - lo, 1e-12)
    a15 = atr(c15, 14)[-1]
    volr = volume_ratio(c15)
    # bottom long sweep/reclaim
    if pos <= 0.30:
        side = "LONG"
        ok_btc, btc_text, btc_delta = btc_allows(side, btc)
        if ok_btc:
            swept = recent_low(c5, 10) <= lo * 1.006 or c15[-1]["low"] <= lo * 1.008
            reclaim = entry > lo * 1.006 and c5[-1]["close"] > c5[-1]["open"]
            if swept and reclaim and not wick_exhaustion(c5, side):
                sl = min(recent_low(c5, 16) - a15 * 0.20, lo - a15 * 0.25)
                mid = lo + (hi - lo) * 0.55
                tp1 = max(entry * (1 + MIN_TP1_PRICE_MOVE_PERCENT/100 + 0.001), min(mid, entry * 1.035))
                score = 62 + btc_delta + (12 if volr >= 0.9 else 0) + (10 if base_from_symbol(symbol) in QUALITY_BASES else 0)
                reason = f"Range Edge LONG: цена у нижней части диапазона {fmt_price(lo)}–{fmt_price(hi)}, был локальный вынос снизу и возврат в диапазон. Volume x{volr:.2f}."
                sig = build_signal(symbol, side, "RANGE_EDGE", "RANGE EDGE", entry, sl, tp1, score, reason,
                                   "сценарий сломан при закреплении ниже нижней границы диапазона", btc_text, "1–6 часов", 0.75)
                if sig: out.append(sig)
    # top short sweep/reclaim
    if pos >= 0.70:
        side = "SHORT"
        ok_btc, btc_text, btc_delta = btc_allows(side, btc)
        if ok_btc:
            swept = recent_high(c5, 10) >= hi * 0.994 or c15[-1]["high"] >= hi * 0.992
            reject = entry < hi * 0.994 and c5[-1]["close"] < c5[-1]["open"]
            if swept and reject and not wick_exhaustion(c5, side):
                sl = max(recent_high(c5, 16) + a15 * 0.20, hi + a15 * 0.25)
                mid = lo + (hi - lo) * 0.45
                tp1 = min(entry * (1 - MIN_TP1_PRICE_MOVE_PERCENT/100 - 0.001), max(mid, entry * 0.965))
                score = 62 + btc_delta + (12 if volr >= 0.9 else 0) + (10 if base_from_symbol(symbol) in QUALITY_BASES else 0)
                reason = f"Range Edge SHORT: цена у верхней части диапазона {fmt_price(lo)}–{fmt_price(hi)}, был локальный вынос сверху и возврат продавца. Volume x{volr:.2f}."
                sig = build_signal(symbol, side, "RANGE_EDGE", "RANGE EDGE", entry, sl, tp1, score, reason,
                                   "сценарий сломан при закреплении выше верхней границы диапазона", btc_text, "1–6 часов", 0.75)
                if sig: out.append(sig)
    return out


def strategy_breakout_retest(symbol: str, btc: MarketRegime, c5, c15, c1h, c4h) -> List[CandidateSignal]:
    out = []
    if is_ultra_risk(symbol, c5, c15):
        return out
    entry = c5[-1]["close"]
    level_high = recent_high(c1h[:-2], 72)
    level_low = recent_low(c1h[:-2], 72)
    a15 = atr(c15, 14)[-1]
    volr = volume_ratio(c15)
    cl15 = closes(c15)
    e20_15 = ema(cl15, 20)[-1]
    # Long breakout retest
    side = "LONG"
    ok_btc, btc_text, btc_delta = btc_allows(side, btc)
    if ok_btc:
        broke = recent_high(c15, 12) > level_high * 1.004
        retest = abs(entry - level_high) / entry * 100 <= 0.75 and entry > level_high * 0.998
        hold = c5[-1]["close"] > c5[-1]["open"] and entry > e20_15
        if broke and retest and hold and not wick_exhaustion(c5, side):
            sl = min(level_high - a15 * 0.55, recent_low(c5, 20) - a15 * 0.15)
            tp1 = max(entry * (1 + MIN_TP1_PRICE_MOVE_PERCENT/100 + 0.001), entry + (entry - sl) * 0.9)
            score = 64 + btc_delta + (14 if volr >= 1.0 else 0) + (8 if base_from_symbol(symbol) in QUALITY_BASES else 0)
            reason = f"Breakout Retest LONG: уровень {fmt_price(level_high)} пробит, цена вернулась на ретест и удержалась выше. Volume x{volr:.2f}."
            sig = build_signal(symbol, side, "BREAKOUT_RETEST", "BREAKOUT RETEST", entry, sl, tp1, score, reason,
                               "сценарий сломан при возврате ниже пробитого уровня", btc_text, "30 минут – 3 часа", 0.85)
            if sig: out.append(sig)
    # Short breakout retest
    side = "SHORT"
    ok_btc, btc_text, btc_delta = btc_allows(side, btc)
    if ok_btc:
        broke = recent_low(c15, 12) < level_low * 0.996
        retest = abs(entry - level_low) / entry * 100 <= 0.75 and entry < level_low * 1.002
        hold = c5[-1]["close"] < c5[-1]["open"] and entry < e20_15
        if broke and retest and hold and not wick_exhaustion(c5, side):
            sl = max(level_low + a15 * 0.55, recent_high(c5, 20) + a15 * 0.15)
            tp1 = min(entry * (1 - MIN_TP1_PRICE_MOVE_PERCENT/100 - 0.001), entry - (sl - entry) * 0.9)
            score = 64 + btc_delta + (14 if volr >= 1.0 else 0) + (8 if base_from_symbol(symbol) in QUALITY_BASES else 0)
            reason = f"Breakout Retest SHORT: уровень {fmt_price(level_low)} пробит вниз, цена вернулась на ретест и удержалась ниже. Volume x{volr:.2f}."
            sig = build_signal(symbol, side, "BREAKOUT_RETEST", "BREAKOUT RETEST", entry, sl, tp1, score, reason,
                               "сценарий сломан при возврате выше пробитого уровня", btc_text, "30 минут – 3 часа", 0.85)
            if sig: out.append(sig)
    return out


def strategy_extreme_context(symbol: str, btc: MarketRegime, c5, c15, c1h, c4h) -> List[CandidateSignal]:
    out = []
    # Extreme only if recent move is significant; ultra-risk has tiny risk, normal active also allowed.
    entry = c5[-1]["close"]
    change_24h_proxy = pct(c1h[-1]["close"], c1h[-24]["close"]) if len(c1h) >= 30 else 0.0
    if abs(change_24h_proxy) < EXTREME_MIN_CHANGE_PERCENT:
        return out
    cl15 = closes(c15)
    e20 = ema(cl15, 20)[-1]
    e50 = ema(cl15, 50)[-1]
    vw = vwap(c15, 64)
    a15 = atr(c15, 14)[-1]
    volr = volume_ratio(c15)
    ultra = is_ultra_risk(symbol, c5, c15)
    # If extreme up: prefer reversal short after structure break; continuation long only after real pullback and reclaim, not ultra-risk.
    if change_24h_proxy > EXTREME_MIN_CHANGE_PERCENT:
        # Reversal short after break below EMA/VWAP and weak retest.
        side = "SHORT"
        ok_btc, btc_text, btc_delta = btc_allows(side, btc, is_extreme=True)
        broke = entry < min(e20, vw) and recent_high(c5, 8) < recent_high(c15, 24) * 0.995
        if ok_btc and broke and volr >= 1.05:
            sl = max(recent_high(c5, 16) + a15 * 0.35, entry * 1.018)
            tp1 = min(entry * 0.965, entry * (1 - MIN_TP1_PRICE_MOVE_PERCENT/100 - 0.002))
            score = 66 + btc_delta + (14 if volr >= 1.3 else 6) + (6 if not ultra else -5)
            reason = f"Extreme Reversal SHORT: монета выросла примерно на {change_24h_proxy:.1f}% за 24ч, затем потеряла EMA/VWAP и дала слабый ретест. Volume x{volr:.2f}."
            sig = build_signal(symbol, side, "EXTREME_CONTEXT", "EXTREME REVERSAL", entry, sl, tp1, score, reason,
                               "сценарий сломан при возврате выше локального high/EMA", btc_text, "15 минут – 2 часа", EXTREME_RISK_MULTIPLIER)
            if sig: out.append(sig)
        # Continuation long after deep pullback, not ultra-risk.
        side = "LONG"
        if not ultra:
            ok_btc, btc_text, btc_delta = btc_allows(side, btc, is_extreme=True)
            high24 = recent_high(c1h, 24)
            pullback = (high24 - entry) / max(high24, 1e-12) * 100
            reclaim = 4.0 <= pullback <= 18.0 and entry > max(e20, vw) and c5[-1]["close"] > c5[-1]["open"]
            if ok_btc and reclaim and volr >= 1.0:
                sl = min(recent_low(c15, 20) - a15 * 0.25, entry * 0.972)
                tp1 = max(entry * 1.025, entry * (1 + MIN_TP1_PRICE_MOVE_PERCENT/100 + 0.002))
                score = 64 + btc_delta + (12 if volr >= 1.2 else 5)
                reason = f"Extreme Continuation LONG: активный рост {change_24h_proxy:.1f}%, затем откат {pullback:.1f}% и reclaim EMA/VWAP. Volume x{volr:.2f}."
                sig = build_signal(symbol, side, "EXTREME_CONTEXT", "EXTREME CONTINUATION", entry, sl, tp1, score, reason,
                                   "сценарий сломан при потере зоны reclaim после отката", btc_text, "15 минут – 2 часа", EXTREME_RISK_MULTIPLIER)
                if sig: out.append(sig)
    # Extreme down: reversal long after capitulation reclaim or continuation short after weak bounce.
    if change_24h_proxy < -EXTREME_MIN_CHANGE_PERCENT:
        side = "LONG"
        ok_btc, btc_text, btc_delta = btc_allows(side, btc, is_extreme=True)
        capitulation = recent_low(c15, 16) < recent_low(c1h, 24) * 1.01
        reclaim = entry > max(e20, vw) and c5[-1]["close"] > c5[-1]["open"]
        if ok_btc and capitulation and reclaim and volr >= 1.1 and not ultra:
            sl = min(recent_low(c5, 20) - a15 * 0.30, entry * 0.975)
            tp1 = max(entry * 1.025, entry * (1 + MIN_TP1_PRICE_MOVE_PERCENT/100 + 0.002))
            score = 66 + btc_delta + (12 if volr >= 1.25 else 5)
            reason = f"Extreme Reclaim LONG: монета падала {change_24h_proxy:.1f}%, был capitulation и возврат выше EMA/VWAP. Volume x{volr:.2f}."
            sig = build_signal(symbol, side, "EXTREME_CONTEXT", "EXTREME RECLAIM", entry, sl, tp1, score, reason,
                               "сценарий сломан при новой потере reclaim-зоны", btc_text, "15 минут – 2 часа", EXTREME_RISK_MULTIPLIER)
            if sig: out.append(sig)
        side = "SHORT"
        ok_btc, btc_text, btc_delta = btc_allows(side, btc, is_extreme=True)
        bounce_high = recent_high(c15, 16)
        weak_bounce = entry < min(e20, vw) and pct(bounce_high, recent_low(c1h, 24)) >= 3.0
        if ok_btc and weak_bounce and volr >= 1.0:
            sl = max(recent_high(c5, 18) + a15 * 0.30, entry * 1.020)
            tp1 = min(entry * 0.970, entry * (1 - MIN_TP1_PRICE_MOVE_PERCENT/100 - 0.002))
            score = 64 + btc_delta + (12 if volr >= 1.25 else 5)
            reason = f"Extreme Continuation SHORT: после падения {change_24h_proxy:.1f}% отскок слабый, цена ниже EMA/VWAP. Volume x{volr:.2f}."
            sig = build_signal(symbol, side, "EXTREME_CONTEXT", "EXTREME CONTINUATION", entry, sl, tp1, score, reason,
                               "сценарий сломан при закреплении выше weak-bounce high", btc_text, "15 минут – 2 часа", EXTREME_RISK_MULTIPLIER)
            if sig: out.append(sig)
    return out


def analyze_symbol(symbol: str, btc: MarketRegime) -> Optional[CandidateSignal]:
    try:
        c5 = get_klines(symbol, "5m", 180)
        c15 = get_klines(symbol, "15m", 220)
        c1h = get_klines(symbol, "1h", 260)
        c4h = get_klines(symbol, "4h", 220)
        if not c5 or not c15 or not c1h or not c4h:
            return None
        candidates: List[CandidateSignal] = []
        candidates += strategy_trend_pullback(symbol, btc, c5, c15, c1h, c4h)
        candidates += strategy_range_edge(symbol, btc, c5, c15, c1h, c4h)
        candidates += strategy_breakout_retest(symbol, btc, c5, c15, c1h, c4h)
        candidates += strategy_extreme_context(symbol, btc, c5, c15, c1h, c4h)
        if not candidates:
            return None
        candidates.sort(key=lambda s: (s.grade == "A+", s.score, s.rr, s.roi_tp1), reverse=True)
        return candidates[0]
    except Exception as e:
        STATE["last_error"] = f"analyze_symbol {symbol}: {e}"
        return None

# ----------------------------- messaging -----------------------------

def signal_to_dict(s: CandidateSignal) -> Dict[str, Any]:
    d = asdict(s)
    d["created_at"] = now_ts()
    d["tp1_hit"] = False
    d["tp2_hit"] = False
    d["tp3_hit"] = False
    d["closed"] = False
    return d


def build_signal_message(s: CandidateSignal) -> str:
    arrow = "🟢" if s.side == "LONG" else "🔴"
    grade_icon = "🏆" if s.grade == "A+" else "⚠️"
    return (
        f"{arrow} <b>{grade_icon} {s.grade} · {s.side} {display_symbol(s.symbol)}</b>\n"
        f"Стратегия: <b>{s.strategy}</b>\n"
        f"Тип: <b>{s.trade_type}</b>\n"
        f"Ожидание: <b>{s.expected_time}</b>\n\n"
        f"Вход: <code>{fmt_price(s.entry)}</code>\n"
        f"TP1: <code>{fmt_price(s.tp1)}</code>  (~{s.roi_tp1:.1f}% ROI при x{LEVERAGE:.0f})\n"
        f"TP2: <code>{fmt_price(s.tp2)}</code>\n"
        f"TP3: <code>{fmt_price(s.tp3)}</code>\n"
        f"SL: <code>{fmt_price(s.sl)}</code>\n\n"
        f"Score: <b>{s.score}</b> | RR к TP1: <b>{s.rr:.2f}</b> | Risk x{s.risk_multiplier:.2f}\n"
        f"Движение до TP1: <b>{s.price_move_tp1:.2f}% цены</b>\n\n"
        f"<b>Почему вход:</b>\n{s.reason}\n\n"
        f"<b>BTC:</b> {s.btc_context}\n\n"
        f"<b>Отмена сценария:</b> {s.invalidation}"
    )


def build_result_message(signal: Dict[str, Any], title: str, price: float, positive: bool) -> str:
    icon = "✅" if positive else "❌"
    return (
        f"{icon} <b>{title}</b>\n\n"
        f"{signal.get('grade')} · {signal.get('side')} {display_symbol(signal.get('symbol'))}\n"
        f"Стратегия: {signal.get('strategy')} / {signal.get('trade_type')}\n\n"
        f"Вход: <code>{fmt_price(float(signal.get('entry')))}</code>\n"
        f"Текущая цена: <code>{fmt_price(price)}</code>\n"
        f"TP1: <code>{fmt_price(float(signal.get('tp1')))}</code>\n"
        f"TP2: <code>{fmt_price(float(signal.get('tp2')))}</code>\n"
        f"TP3: <code>{fmt_price(float(signal.get('tp3')))}</code>\n"
        f"SL: <code>{fmt_price(float(signal.get('sl')))}</code>\n\n"
        f"{build_stats_text(short=True)}"
    )


def build_stats_text(short: bool = False) -> str:
    stats = STATE.setdefault("stats", {})
    lines = ["📊 <b>Статистика:</b>"]
    for key in ["SIDE:LONG", "SIDE:SHORT", "GRADE:A+", "GRADE:B"]:
        s = get_stat(key)
        lines.append(f"{key.replace('SIDE:', '').replace('GRADE:', '')}: {s.get('tp',0)} позитив / {s.get('sl',0)} SL / WR {win_rate(s):.1f}%")
    if not short:
        lines.append("\n🧠 <b>Стратегии:</b>")
        base_keys = sorted([k for k in stats if k.count(":") == 1 and not k.startswith("SIDE") and not k.startswith("GRADE")])
        for k in base_keys[:30]:
            s = stats[k]
            disabled_until = int(STATE.setdefault("disabled_until", {}).get(k, 0))
            status = "OFF" if disabled_until > now_ts() else "ON"
            lines.append(f"{k}: {s.get('tp',0)} / {s.get('sl',0)} WR {win_rate(s):.1f}% [{status}]")
    return "\n".join(lines)

# ----------------------------- scanner/tracker -----------------------------

def has_active_or_recent(symbol: str) -> bool:
    t = now_ts()
    for s in STATE.setdefault("active_signals", []):
        if not s.get("closed") and s.get("symbol") == symbol:
            return True
    last = int(STATE.setdefault("last_signal_at", {}).get(symbol, 0))
    return t - last < SIGNAL_COOLDOWN_SECONDS


def add_active_signal(s: CandidateSignal) -> None:
    active = STATE.setdefault("active_signals", [])
    active.append(signal_to_dict(s))
    # keep active list compact
    STATE["active_signals"] = [x for x in active if not x.get("closed")][-50:]
    STATE.setdefault("last_signal_at", {})[s.symbol] = now_ts()
    save_state()


def current_price_from_candles(symbol: str) -> Optional[float]:
    c = get_klines(symbol, "1m", 80)
    if not c:
        return None
    return c[-1]["close"]


def signal_hit(signal: Dict[str, Any], price: float, level: float, side: str, tp: bool) -> bool:
    if side == "LONG":
        return price >= level if tp else price <= level
    return price <= level if tp else price >= level


def track_active_signals() -> None:
    changed = False
    for sig in STATE.setdefault("active_signals", []):
        if sig.get("closed"):
            continue
        price = current_price_from_candles(sig.get("symbol"))
        if price is None:
            continue
        side = sig.get("side")
        # TP hits in order. After TP1, trade is already positive; later SL is not counted negative.
        if not sig.get("tp1_hit") and signal_hit(sig, price, float(sig["tp1"]), side, True):
            sig["tp1_hit"] = True
            changed = True
            apply_result(sig, True)
            send_telegram(build_result_message(sig, "TAKE PROFIT 1 — закрыть 70%, остальное в сопровождение", price, True))
            continue
        if sig.get("tp1_hit") and not sig.get("tp2_hit") and signal_hit(sig, price, float(sig["tp2"]), side, True):
            sig["tp2_hit"] = True
            changed = True
            send_telegram(build_result_message(sig, "TAKE PROFIT 2", price, True))
            continue
        if sig.get("tp2_hit") and not sig.get("tp3_hit") and signal_hit(sig, price, float(sig["tp3"]), side, True):
            sig["tp3_hit"] = True
            sig["closed"] = True
            changed = True
            send_telegram(build_result_message(sig, "TAKE PROFIT 3 — сделка закрыта", price, True))
            continue
        if signal_hit(sig, price, float(sig["sl"]), side, False):
            sig["closed"] = True
            changed = True
            if sig.get("tp1_hit"):
                send_telegram(build_result_message(sig, "Защитный выход после TP1 — сделка уже была положительной", price, True))
            else:
                apply_result(sig, False)
                send_telegram(build_result_message(sig, "Stop Loss", price, False))
    if changed:
        save_state()


def scan_once(manual: bool = False) -> Dict[str, Any]:
    started = now_ts()
    btc = get_btc_regime()
    symbols = get_symbols()
    checked = 0
    candidates: List[CandidateSignal] = []
    skipped_recent = 0
    for symbol in symbols:
        if len([x for x in STATE.setdefault("active_signals", []) if not x.get("closed")]) >= MAX_ACTIVE_SIGNALS:
            break
        if has_active_or_recent(symbol):
            skipped_recent += 1
            continue
        checked += 1
        sig = analyze_symbol(symbol, btc)
        if sig:
            candidates.append(sig)
        # prevent very long scans on worker
        if checked >= MAX_SYMBOLS:
            break
    candidates.sort(key=lambda s: (s.grade == "A+", s.score, s.rr, s.roi_tp1), reverse=True)
    sent = 0
    for sig in candidates[:3]:
        if has_active_or_recent(sig.symbol):
            continue
        add_active_signal(sig)
        send_telegram(build_signal_message(sig))
        sent += 1
    summary = {
        "checked": checked,
        "symbols": len(symbols),
        "candidates": len(candidates),
        "sent": sent,
        "btc": asdict(btc),
        "skipped_recent": skipped_recent,
        "duration_sec": now_ts() - started,
        "time": started,
    }
    STATE["last_scan_at"] = started
    STATE["last_scan_summary"] = summary
    save_state()
    # no no-signal spam. Manual endpoint returns details.
    return summary

async def auto_loop() -> None:
    while True:
        try:
            if AUTO_SCAN_ENABLED:
                scan_once(manual=False)
        except Exception as e:
            STATE["last_error"] = f"auto_loop scan: {e}"
            save_state()
        await asyncio.sleep(AUTO_SCAN_SECONDS)

async def track_loop() -> None:
    while True:
        try:
            track_active_signals()
        except Exception as e:
            STATE["last_error"] = f"track_loop: {e}"
            save_state()
        await asyncio.sleep(TRACK_SECONDS)

# ----------------------------- API endpoints -----------------------------

@app.get("/", response_class=HTMLResponse)
def root():
    return f"<h3>{APP_NAME}</h3><p>{DEPLOY_MARKER}</p><p>Use /health /version /scan /stats /auto-status</p>"

@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME, "deploy_marker": DEPLOY_MARKER, "last_error": STATE.get("last_error", "")}

@app.get("/version")
def version():
    return {"app": APP_NAME, "deploy_marker": DEPLOY_MARKER}

@app.get("/auto-status")
def auto_status():
    return {
        "app": APP_NAME,
        "deploy_marker": DEPLOY_MARKER,
        "auto_scan_enabled": AUTO_SCAN_ENABLED,
        "last_scan_at": STATE.get("last_scan_at"),
        "last_scan_summary": STATE.get("last_scan_summary"),
        "active_signals": [x for x in STATE.setdefault("active_signals", []) if not x.get("closed")],
        "last_error": STATE.get("last_error", ""),
    }

@app.get("/scan")
def scan_endpoint():
    return scan_once(manual=True)

@app.get("/stats")
def stats_endpoint():
    return HTMLResponse("<pre>" + build_stats_text(short=False).replace("<", "&lt;").replace(">", "&gt;") + "</pre>")

@app.get("/test-telegram")
def test_telegram():
    ok = send_telegram(f"✅ {APP_NAME}\nDeploy marker: {DEPLOY_MARKER}\nTelegram test OK")
    return {"sent": ok, "app": APP_NAME, "deploy_marker": DEPLOY_MARKER}

# ----------------------------- startup -----------------------------

STARTUP_TEXT = (
    f"✅ {APP_NAME} запущен.\n"
    f"Deploy marker: {DEPLOY_MARKER}\n\n"
    f"Архитектура: quality universe → BTC → 4H/1H context → 15m confirm → 5m entry.\n"
    f"Стратегии: TREND_PULLBACK / RANGE_EDGE / BREAKOUT_RETEST / EXTREME_CONTEXT.\n"
    f"A+ и B: ON, но B = medium-quality с меньшим риском, не мусор.\n"
    f"TP1 filter: минимум {MIN_TP1_ROI_PERCENT:.0f}% ROI при x{LEVERAGE:.0f}.\n"
    f"Ultra-risk: обычные стратегии заблокированы, только EXTREME_CONTEXT с малым риском.\n"
    f"No-signal spam: OFF. TP/SL и статистика: ON."
)

@app.on_event("startup")
async def fastapi_startup():
    load_state()

async def main():
    load_state()
    send_telegram(STARTUP_TEXT)
    await asyncio.gather(auto_loop(), track_loop())

if __name__ == "__main__":
    asyncio.run(main())
