import os
import time
import json
import math
import html
import asyncio
import logging
import tempfile
import threading
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

# ============================================================
# V14.11 — ANTI-SILENCE LEVEL SCALPER
# Telegram signal bot only. It NEVER opens trades.
#
# Что изменено против V14.10:
# - TP3 больше НЕ блокирует сделку: цели адаптируются под реальную волатильность.
# - Anti-silence режим: после паузы бот разрешает B+ / B компактные скальпы.
# - Pending confirmation имеет таймаут и не висит бесконечно.
# - Scan updates ограничены, чтобы бот не спамил вместо сигналов.
# - Входы остаются через уровень + VWAP/EMA + импульс + объём + BTC-фильтр.
#
# ENV на Render:
# TELEGRAM_BOT_TOKEN=...
# TELEGRAM_CHAT_ID=...
# ADMIN_KEY=любой_секрет_для_/scan_и_/status_если_хочешь
# ============================================================

VERSION = "V14.11_ANTI_SILENCE_LEVEL_SCALPER"

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(VERSION)

# ----------------------------
# Helpers / ENV
# ----------------------------
def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b


def now_ts() -> float:
    return time.time()


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def fmt_price(x: float) -> str:
    if x <= 0:
        return "0"
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 100:
        return f"{x:.3f}"
    if x >= 10:
        return f"{x:.4f}"
    if x >= 1:
        return f"{x:.5f}"
    if x >= 0.1:
        return f"{x:.6f}"
    return f"{x:.8f}"


def roi_text(price_move_pct: float, leverage: int) -> str:
    return f"{price_move_pct * leverage * 100:.1f}% ROI"

# ----------------------------
# Config
# ----------------------------
@dataclass
class Config:
    telegram_token: str = env_str("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = env_str("TELEGRAM_CHAT_ID", "")
    admin_key: str = env_str("ADMIN_KEY", "")

    bingx_base: str = env_str("BINGX_BASE", "https://open-api.bingx.com")

    scan_interval_sec: int = env_int("SCAN_INTERVAL_SEC", 180)
    request_timeout_sec: int = env_int("REQUEST_TIMEOUT_SEC", 10)
    max_symbols_per_scan: int = env_int("MAX_SYMBOLS_PER_SCAN", 180)
    hot_symbols_to_analyze: int = env_int("HOT_SYMBOLS_TO_ANALYZE", 45)
    max_signals_per_scan: int = env_int("MAX_SIGNALS_PER_SCAN", 2)
    max_active_signals: int = env_int("MAX_ACTIVE_SIGNALS", 5)

    leverage: int = env_int("LEVERAGE", 10)
    rm_per_signal_pct: float = env_float("RM_PER_SIGNAL_PCT", 0.5)

    # Anti-silence: делаем мягче, чтобы бот не молчал сутками.
    anti_silence_enabled: bool = env_bool("ANTI_SILENCE_ENABLED", True)
    starvation_soft_minutes: int = env_int("STARVATION_SOFT_MINUTES", 60)
    starvation_hard_minutes: int = env_int("STARVATION_HARD_MINUTES", 180)

    min_quality_a: int = env_int("MIN_QUALITY_A", 82)
    min_quality_b_plus: int = env_int("MIN_QUALITY_B_PLUS", 72)
    min_quality_b: int = env_int("MIN_QUALITY_B", 64)

    # Tolerance around levels.
    level_tolerance_a: float = env_float("LEVEL_TOLERANCE_A", 0.0012)          # 0.12%
    level_tolerance_b_plus: float = env_float("LEVEL_TOLERANCE_B_PLUS", 0.0020)
    level_tolerance_b: float = env_float("LEVEL_TOLERANCE_B", 0.0028)

    vwap_tolerance_b: float = env_float("VWAP_RECLAIM_TOLERANCE_B", 0.0020)
    ema_tolerance_b: float = env_float("EMA_RECLAIM_TOLERANCE_B", 0.0025)

    # SL / RR. Для B сигналов стоп компактнее.
    max_sl_price_pct_a: float = env_float("MAX_SL_PRICE_PCT_A", 0.0065)  # 0.65% price = 6.5% ROI x10
    max_sl_price_pct_b: float = env_float("MAX_SL_PRICE_PCT_B", 0.0055)
    min_rr_a: float = env_float("MIN_RR_A", 1.80)
    min_rr_b: float = env_float("MIN_RR_B", 1.30)

    # ROI targets on leveraged position.
    a_tp1_roi: float = env_float("A_TP1_ROI", 0.10)
    a_tp2_roi: float = env_float("A_TP2_ROI", 0.16)
    a_tp3_roi: float = env_float("A_TP3_ROI", 0.23)
    a_tp4_roi: float = env_float("A_TP4_ROI", 0.32)
    a_tp5_roi: float = env_float("A_TP5_ROI", 0.42)

    bplus_tp1_roi: float = env_float("BPLUS_TP1_ROI", 0.08)
    bplus_tp2_roi: float = env_float("BPLUS_TP2_ROI", 0.13)
    bplus_tp3_roi: float = env_float("BPLUS_TP3_ROI", 0.18)

    b_tp1_roi: float = env_float("B_TP1_ROI", 0.065)
    b_tp2_roi: float = env_float("B_TP2_ROI", 0.105)
    b_tp3_roi: float = env_float("B_TP3_ROI", 0.145)

    # Filters.
    min_5m_volume_ratio_a: float = env_float("MIN_5M_VOLUME_RATIO_A", 0.85)
    min_5m_volume_ratio_b: float = env_float("MIN_5M_VOLUME_RATIO_B", 0.45)
    min_recent_capacity_pct_a: float = env_float("MIN_RECENT_CAPACITY_PCT_A", 0.009)
    min_recent_capacity_pct_b: float = env_float("MIN_RECENT_CAPACITY_PCT_B", 0.0055)
    max_chase_15m_pct: float = env_float("MAX_CHASE_15M_PCT", 0.045)  # avoid entering after huge candle
    max_chase_30m_pct: float = env_float("MAX_CHASE_30M_PCT", 0.075)

    cooldown_minutes_a: int = env_int("COOLDOWN_MINUTES_A", 90)
    cooldown_minutes_b: int = env_int("COOLDOWN_MINUTES_B", 45)
    same_side_cooldown_minutes: int = env_int("SAME_SIDE_COOLDOWN_MINUTES", 120)

    pending_confirmation_timeout_sec: int = env_int("PENDING_CONFIRMATION_TIMEOUT_SEC", 12 * 60)
    pending_enabled: bool = env_bool("PENDING_ENABLED", True)

    signal_expiry_minutes: int = env_int("SIGNAL_EXPIRY_MINUTES", 180)
    track_active_signals: bool = env_bool("TRACK_ACTIVE_SIGNALS", True)

    # Updates. По умолчанию не спамим scan update.
    send_scan_updates: bool = env_bool("SEND_SCAN_UPDATES", False)
    scan_update_min_interval_sec: int = env_int("SCAN_UPDATE_MIN_INTERVAL_SEC", 3 * 60 * 60)

    state_file: str = env_str("STATE_FILE", "/tmp/v14_11_state.json")

CFG = Config()

# ----------------------------
# HTTP Session
# ----------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.35,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": f"{VERSION}/1.0"})
    return s

SESSION = make_session()
STATE_LOCK = threading.RLock()
SCAN_LOCK = asyncio.Lock()

# ----------------------------
# State
# ----------------------------
def default_state() -> Dict[str, Any]:
    return {
        "version": VERSION,
        "bot_start_ts": now_ts(),
        "last_signal_ts": 0,
        "last_scan_ts": 0,
        "last_scan_summary": {},
        "last_scan_update_ts": 0,
        "cooldowns": {},
        "side_cooldowns": {},
        "pending": {},
        "active": {},
        "stats": {"profit": 0, "sl": 0, "expired": 0, "early": 0},
        "sent_today": {},
    }


def load_state() -> Dict[str, Any]:
    with STATE_LOCK:
        try:
            if os.path.exists(CFG.state_file):
                with open(CFG.state_file, "r", encoding="utf-8") as f:
                    st = json.load(f)
            else:
                st = default_state()
        except Exception:
            log.exception("Cannot load state, using default")
            st = default_state()

        base = default_state()
        for k, v in base.items():
            st.setdefault(k, v)
        st["version"] = VERSION
        return st


def save_state_atomic(state: Dict[str, Any]) -> None:
    with STATE_LOCK:
        folder = os.path.dirname(CFG.state_file) or "."
        os.makedirs(folder, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix="v14_11_", suffix=".json", dir=folder)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
            os.replace(tmp_path, CFG.state_file)
        except Exception:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            raise

STATE = load_state()
save_state_atomic(STATE)

# ----------------------------
# Telegram
# ----------------------------
def telegram_send(text: str, disable_preview: bool = True) -> bool:
    if not CFG.telegram_token or not CFG.telegram_chat_id:
        log.warning("Telegram ENV missing. Message not sent:\n%s", text)
        return False
    url = f"https://api.telegram.org/bot{CFG.telegram_token}/sendMessage"
    payload = {
        "chat_id": CFG.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_preview,
    }
    try:
        r = SESSION.post(url, json=payload, timeout=CFG.request_timeout_sec)
        if r.status_code >= 300:
            log.warning("Telegram error %s: %s", r.status_code, r.text[:500])
            return False
        return True
    except Exception:
        log.exception("Telegram send failed")
        return False


def e(s: Any) -> str:
    return html.escape(str(s), quote=False)

# ----------------------------
# BingX API
# ----------------------------
def bingx_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = CFG.bingx_base.rstrip("/") + path
    r = SESSION.get(url, params=params or {}, timeout=CFG.request_timeout_sec)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        # BingX usually: {code:0,data:...}
        if str(data.get("code", "0")) not in ("0", "200") and "data" not in data:
            raise RuntimeError(f"BingX API error: {data}")
        return data.get("data", data)
    return data


def to_bingx_symbol(symbol: str) -> str:
    return symbol.replace("/", "-").upper()


def to_display_symbol(symbol: str) -> str:
    return symbol.replace("-", "/").upper()


def normalize_symbols_from_contracts(data: Any) -> List[str]:
    symbols: List[str] = []
    if isinstance(data, dict):
        data = data.get("data") or data.get("list") or data.get("contracts") or []
    if not isinstance(data, list):
        return []
    for item in data:
        if not isinstance(item, dict):
            continue
        sym = str(item.get("symbol") or item.get("contract") or item.get("pair") or "").upper()
        if not sym:
            continue
        if "USDT" not in sym:
            continue
        status = str(item.get("status", item.get("state", "1"))).lower()
        if status in ("0", "offline", "delisted", "suspend", "suspended"):
            continue
        if "-" not in sym and "/" in sym:
            sym = sym.replace("/", "-")
        if sym.endswith("-USDT") or sym.endswith("USDT"):
            if "-" not in sym:
                sym = sym.replace("USDT", "-USDT")
            symbols.append(sym)
    return sorted(set(symbols))


def fetch_contract_symbols() -> List[str]:
    paths = [
        "/openApi/swap/v2/quote/contracts",
        "/openApi/swap/v2/quote/contract",
    ]
    for p in paths:
        try:
            data = bingx_get(p)
            syms = normalize_symbols_from_contracts(data)
            if syms:
                return syms
        except Exception as ex:
            log.warning("contracts endpoint failed %s: %s", p, ex)
    # Fallback small universe.
    return [
        "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT", "ADA-USDT",
        "DOGE-USDT", "LINK-USDT", "AVAX-USDT", "SUI-USDT", "APT-USDT", "ARB-USDT",
        "OP-USDT", "INJ-USDT", "TIA-USDT", "NEAR-USDT", "SEI-USDT", "LDO-USDT",
        "APE-USDT", "FIL-USDT", "ATOM-USDT", "DYDX-USDT", "JUP-USDT", "WLD-USDT",
    ]


def fetch_tickers() -> Dict[str, Dict[str, Any]]:
    paths = [
        "/openApi/swap/v2/quote/ticker",
        "/openApi/swap/v1/ticker/24hr",
    ]
    for p in paths:
        try:
            data = bingx_get(p)
            if isinstance(data, dict) and "data" in data:
                data = data["data"]
            items = data if isinstance(data, list) else []
            out: Dict[str, Dict[str, Any]] = {}
            for it in items:
                if not isinstance(it, dict):
                    continue
                sym = str(it.get("symbol") or "").upper()
                if not sym or "USDT" not in sym:
                    continue
                sym = to_bingx_symbol(sym)
                out[sym] = it
            if out:
                return out
        except Exception as ex:
            log.warning("ticker endpoint failed %s: %s", p, ex)
    return {}


def parse_candles(raw: Any) -> List[Dict[str, float]]:
    if isinstance(raw, dict):
        raw = raw.get("data") or raw.get("list") or raw.get("klines") or raw.get("candles") or []
    if not isinstance(raw, list):
        return []

    candles: List[Dict[str, float]] = []
    for row in raw:
        try:
            if isinstance(row, dict):
                ts = safe_float(row.get("time") or row.get("openTime") or row.get("timestamp") or row.get("T") or 0)
                o = safe_float(row.get("open") or row.get("o"))
                h = safe_float(row.get("high") or row.get("h"))
                l = safe_float(row.get("low") or row.get("l"))
                c = safe_float(row.get("close") or row.get("c"))
                v = safe_float(row.get("volume") or row.get("vol") or row.get("v") or row.get("quoteVolume") or 0)
            elif isinstance(row, (list, tuple)) and len(row) >= 6:
                # Often: [time, open, high, low, close, volume]
                ts = safe_float(row[0])
                o = safe_float(row[1])
                h = safe_float(row[2])
                l = safe_float(row[3])
                c = safe_float(row[4])
                v = safe_float(row[5])
            else:
                continue
            if o > 0 and h > 0 and l > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": max(v, 0.0)})
        except Exception:
            continue

    # Sort ascending by timestamp if possible.
    candles.sort(key=lambda x: x.get("ts", 0))
    return candles


def fetch_klines(symbol: str, interval: str, limit: int = 120) -> List[Dict[str, float]]:
    sym = to_bingx_symbol(symbol)
    endpoints = [
        ("/openApi/swap/v3/quote/klines", {"symbol": sym, "interval": interval, "limit": limit}),
        ("/openApi/swap/v2/quote/klines", {"symbol": sym, "interval": interval, "limit": limit}),
    ]
    last_error = None
    for path, params in endpoints:
        try:
            raw = bingx_get(path, params=params)
            candles = parse_candles(raw)
            if len(candles) >= 10:
                return candles[-limit:]
        except Exception as ex:
            last_error = ex
    if last_error:
        log.debug("klines failed %s %s: %s", sym, interval, last_error)
    return []

# ----------------------------
# Indicators
# ----------------------------
def closes(c: List[Dict[str, float]]) -> List[float]:
    return [x["close"] for x in c]


def highs(c: List[Dict[str, float]]) -> List[float]:
    return [x["high"] for x in c]


def lows(c: List[Dict[str, float]]) -> List[float]:
    return [x["low"] for x in c]


def volumes(c: List[Dict[str, float]]) -> List[float]:
    return [x["volume"] for x in c]


def ema_values(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def ema_last(values: List[float], period: int) -> float:
    ev = ema_values(values, period)
    return ev[-1] if ev else 0.0


def sma(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    vals = values[-period:]
    return sum(vals) / len(vals)


def rsi_last(values: List[float], period: int = 14) -> float:
    if len(values) < period + 2:
        return 50.0
    gains = []
    losses = []
    recent = values[-(period + 1):]
    for i in range(1, len(recent)):
        d = recent[i] - recent[i - 1]
        if d >= 0:
            gains.append(d)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(d))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr_last(candles: List[Dict[str, float]], period: int = 14) -> float:
    if len(candles) < period + 2:
        return 0.0
    trs: List[float] = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    vals = trs[-period:]
    return sum(vals) / len(vals) if vals else 0.0


def vwap_last(candles: List[Dict[str, float]], period: int = 48) -> float:
    if not candles:
        return 0.0
    part = candles[-period:]
    pv = 0.0
    vv = 0.0
    for x in part:
        typical = (x["high"] + x["low"] + x["close"]) / 3
        v = max(x["volume"], 0.0)
        pv += typical * v
        vv += v
    if vv <= 0:
        return part[-1]["close"]
    return pv / vv


def macd_hist_last(values: List[float]) -> float:
    if len(values) < 35:
        return 0.0
    e12 = ema_values(values, 12)
    e26 = ema_values(values, 26)
    n = min(len(e12), len(e26))
    macd = [e12[-n + i] - e26[-n + i] for i in range(n)]
    sig = ema_values(macd, 9)
    return macd[-1] - sig[-1] if sig else 0.0


def recent_capacity_pct(c5: List[Dict[str, float]], c15: List[Dict[str, float]]) -> float:
    """Recent realistic movement capacity without leverage."""
    vals = []
    for c, n in ((c5, 18), (c15, 12)):
        for x in c[-n:]:
            if x["close"] > 0:
                vals.append((x["high"] - x["low"]) / x["close"])
    if not vals:
        return 0.0
    vals = sorted(vals)
    # 75th percentile, not max, чтобы не целиться в случайный фитиль.
    idx = int(0.75 * (len(vals) - 1))
    return vals[idx]


def vol_ratio(candles: List[Dict[str, float]], lookback: int = 30) -> float:
    if len(candles) < 8:
        return 0.0
    # Берём последнюю закрытую свечу. Если API даёт текущую свечу последней, это всё равно лучше, чем 0.
    last_v = candles[-1]["volume"]
    base = [x["volume"] for x in candles[-(lookback + 1):-1] if x["volume"] > 0]
    if not base:
        return 1.0 if last_v > 0 else 0.0
    avg = sum(base) / len(base)
    if avg <= 0:
        return 0.0
    return last_v / avg


def find_support_resistance(candles: List[Dict[str, float]], lookback: int = 50) -> Tuple[float, float]:
    part = candles[-lookback:] if len(candles) >= lookback else candles[:]
    if not part:
        return 0.0, 0.0
    # Более устойчиво: не абсолютный low/high, а ближние экстремумы.
    lows_sorted = sorted([x["low"] for x in part])
    highs_sorted = sorted([x["high"] for x in part], reverse=True)
    support = lows_sorted[min(2, len(lows_sorted) - 1)]
    resistance = highs_sorted[min(2, len(highs_sorted) - 1)]
    return support, resistance

# ----------------------------
# Market mode / ranking
# ----------------------------
QUALITY_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "LINK", "AVAX", "SUI", "APT", "ARB", "OP",
    "INJ", "TIA", "NEAR", "SEI", "LDO", "APE", "FIL", "ATOM", "DYDX", "JUP", "WLD", "UNI", "AAVE",
    "ORDI", "TON", "TRX", "DOT", "ETC", "LTC", "BCH", "1000PEPE", "1000BONK", "FET", "RENDER",
}

ULTRA_RISK_WORDS = (
    "TEST", "BEER", "PUMP", "PONKE", "WHY", "MOTHER", "TROLL", "CAT", "DOGS", "RATS",
)


def base_asset(symbol: str) -> str:
    s = to_bingx_symbol(symbol)
    return s.split("-")[0]


def is_ultra_risk(symbol: str) -> bool:
    base = base_asset(symbol)
    if base in QUALITY_BASES:
        return False
    # Не режем всё подряд, иначе бот опять будет молчать. Только явный мусор.
    return any(w in base for w in ULTRA_RISK_WORDS)


def get_anti_silence_mode(state: Dict[str, Any]) -> str:
    if not CFG.anti_silence_enabled:
        return "A_ONLY"
    now = now_ts()
    last_signal = safe_float(state.get("last_signal_ts") or 0)
    start = safe_float(state.get("bot_start_ts") or now)
    if last_signal > 0:
        silent_min = (now - last_signal) / 60
    else:
        silent_min = (now - start) / 60
    if silent_min >= CFG.starvation_hard_minutes:
        return "B_FALLBACK"
    if silent_min >= CFG.starvation_soft_minutes:
        return "B_PLUS"
    return "A_ONLY"


def silent_minutes(state: Dict[str, Any]) -> float:
    now = now_ts()
    last_signal = safe_float(state.get("last_signal_ts") or 0)
    start = safe_float(state.get("bot_start_ts") or now)
    ref = last_signal if last_signal > 0 else start
    return max(0.0, (now - ref) / 60)


def ticker_score(symbol: str, t: Dict[str, Any]) -> float:
    # Flexible field names.
    qv = safe_float(t.get("quoteVolume") or t.get("quoteVol") or t.get("turnover") or t.get("volume") or 0)
    chg = safe_float(t.get("priceChangePercent") or t.get("priceChangeRate") or t.get("change") or t.get("changeRate") or 0)
    if abs(chg) > 2:  # maybe percent already, e.g. 5.5
        chg_pct = abs(chg) / 100
    else:
        chg_pct = abs(chg)
    base_bonus = 20 if base_asset(symbol) in QUALITY_BASES else 0
    risk_penalty = -60 if is_ultra_risk(symbol) else 0
    return math.log10(max(qv, 1.0)) * 12 + chg_pct * 120 + base_bonus + risk_penalty


def rank_symbols(symbols: List[str], tickers: Dict[str, Dict[str, Any]]) -> List[str]:
    scored = []
    for s in symbols:
        if not s.endswith("-USDT"):
            continue
        if is_ultra_risk(s) and len(symbols) > 40:
            # Явный мусор оставляем внизу, но не обязательно полностью удаляем.
            pass
        scored.append((ticker_score(s, tickers.get(s, {})), s))
    scored.sort(reverse=True)
    return [s for _, s in scored[:CFG.max_symbols_per_scan]]


def btc_context() -> Dict[str, Any]:
    c1h = fetch_klines("BTC-USDT", "1h", 80)
    c5 = fetch_klines("BTC-USDT", "5m", 80)
    if len(c1h) < 10 or len(c5) < 10:
        return {"mode": "BTC UNKNOWN", "bias_long": True, "bias_short": True, "m1_3": 0.0, "h1": 0.0, "h6": 0.0}
    h1 = pct(c1h[-1]["close"], c1h[-2]["close"])
    h6 = pct(c1h[-1]["close"], c1h[-7]["close"]) if len(c1h) >= 7 else 0.0
    m1_3 = pct(c5[-1]["close"], c5[-4]["close"]) if len(c5) >= 4 else 0.0
    close = c1h[-1]["close"]
    ema50 = ema_last(closes(c1h), 50)
    if abs(h6) < 0.008 and abs(h1) < 0.006:
        mode = "BTC RANGE"
    elif close > ema50 and h6 > 0:
        mode = "BTC BULL"
    elif close < ema50 and h6 < 0:
        mode = "BTC BEAR"
    else:
        mode = "BTC MIXED"
    return {
        "mode": mode,
        "bias_long": not (mode == "BTC BEAR" and h1 < -0.004),
        "bias_short": not (mode == "BTC BULL" and h1 > 0.004),
        "m1_3": m1_3,
        "h1": h1,
        "h6": h6,
    }

# ----------------------------
# Signal analysis
# ----------------------------
def in_cooldown(state: Dict[str, Any], symbol: str, side: str) -> bool:
    now = now_ts()
    cd = state.get("cooldowns", {})
    side_cd = state.get("side_cooldowns", {})
    if safe_float(cd.get(symbol, 0)) > now:
        return True
    if safe_float(side_cd.get(f"{symbol}:{side}", 0)) > now:
        return True
    return False


def set_cooldown(state: Dict[str, Any], symbol: str, side: str, mode: str) -> None:
    now = now_ts()
    minutes = CFG.cooldown_minutes_a if mode == "A" else CFG.cooldown_minutes_b
    state.setdefault("cooldowns", {})[symbol] = now + minutes * 60
    state.setdefault("side_cooldowns", {})[f"{symbol}:{side}"] = now + CFG.same_side_cooldown_minutes * 60


def mode_threshold(anti_mode: str) -> Tuple[int, str]:
    if anti_mode == "B_FALLBACK":
        return CFG.min_quality_b, "B"
    if anti_mode == "B_PLUS":
        return CFG.min_quality_b_plus, "B_PLUS"
    return CFG.min_quality_a, "A"


def classify_signal_mode(quality: float, anti_mode: str) -> Optional[str]:
    if quality >= CFG.min_quality_a:
        return "A"
    if anti_mode in ("B_PLUS", "B_FALLBACK") and quality >= CFG.min_quality_b_plus:
        return "B_PLUS"
    if anti_mode == "B_FALLBACK" and quality >= CFG.min_quality_b:
        return "B"
    return None


def tolerance_for_mode(mode: str) -> float:
    if mode == "A":
        return CFG.level_tolerance_a
    if mode == "B_PLUS":
        return CFG.level_tolerance_b_plus
    return CFG.level_tolerance_b


def build_adaptive_ladder(side: str, entry: float, leverage: int, signal_mode: str, cap_pct: float) -> List[float]:
    if signal_mode == "A":
        roi_targets = [CFG.a_tp1_roi, CFG.a_tp2_roi, CFG.a_tp3_roi, CFG.a_tp4_roi, CFG.a_tp5_roi]
    elif signal_mode == "B_PLUS":
        roi_targets = [CFG.bplus_tp1_roi, CFG.bplus_tp2_roi, CFG.bplus_tp3_roi]
    else:
        roi_targets = [CFG.b_tp1_roi, CFG.b_tp2_roi, CFG.b_tp3_roi]

    raw_moves = [r / max(1, leverage) for r in roi_targets]

    # Вместо tp3_feasibility_block: сжимаем лестницу под недавнюю capacity.
    # Минимум не делаем слишком маленьким, иначе сигнал бессмысленен.
    if signal_mode == "A":
        max_realistic = max(0.010, min(cap_pct * 1.15, 0.035))
    elif signal_mode == "B_PLUS":
        max_realistic = max(0.0075, min(cap_pct * 1.05, 0.024))
    else:
        max_realistic = max(0.0060, min(cap_pct * 0.98, 0.018))

    moves: List[float] = []
    for mv in raw_moves:
        if mv <= max_realistic:
            moves.append(mv)

    # Если рынок не тянет старые цели, строим compact ladder.
    if len(moves) < 2:
        moves = [
            clamp(max_realistic * 0.45, 0.0045, 0.0075),
            clamp(max_realistic * 0.75, 0.0070, 0.0120),
        ]
    if len(moves) == 2 and max_realistic >= 0.0115:
        moves.append(clamp(max_realistic * 0.95, 0.0100, 0.0175))

    tps: List[float] = []
    for mv in moves:
        if side == "LONG":
            tps.append(entry * (1 + mv))
        else:
            tps.append(entry * (1 - mv))
    return tps


def build_sl(side: str, entry: float, support: float, resistance: float, atr5: float, signal_mode: str) -> float:
    max_sl_pct = CFG.max_sl_price_pct_a if signal_mode == "A" else CFG.max_sl_price_pct_b
    atr_pct = atr5 / entry if entry > 0 else 0.0
    sl_dist = clamp(max(atr_pct * 0.85, 0.0035), 0.0035, max_sl_pct)

    if side == "LONG":
        level_sl = support * 0.997 if support > 0 else entry * (1 - sl_dist)
        sl = min(entry * (1 - sl_dist), level_sl)
        # Never wider than max.
        if (entry - sl) / entry > max_sl_pct:
            sl = entry * (1 - max_sl_pct)
    else:
        level_sl = resistance * 1.003 if resistance > 0 else entry * (1 + sl_dist)
        sl = max(entry * (1 + sl_dist), level_sl)
        if (sl - entry) / entry > max_sl_pct:
            sl = entry * (1 + max_sl_pct)
    return sl


def rr_ok(side: str, entry: float, sl: float, tps: List[float], signal_mode: str) -> bool:
    if not tps:
        return False
    risk = abs(entry - sl)
    if risk <= 0:
        return False
    # RR по TP2, если есть, иначе TP1.
    target = tps[1] if len(tps) >= 2 else tps[0]
    reward = abs(target - entry)
    min_rr = CFG.min_rr_a if signal_mode == "A" else CFG.min_rr_b
    return (reward / risk) >= min_rr


def active_count(state: Dict[str, Any]) -> int:
    return len(state.get("active", {}) or {})


def analyze_symbol(symbol: str, anti_mode: str, btc: Dict[str, Any], state: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    if in_cooldown(state, symbol, "LONG") and in_cooldown(state, symbol, "SHORT"):
        return None, "cooldown"
    if active_count(state) >= CFG.max_active_signals:
        return None, "max_active"

    c1h = fetch_klines(symbol, "1h", 100)
    c15 = fetch_klines(symbol, "15m", 120)
    c5 = fetch_klines(symbol, "5m", 120)
    c1 = fetch_klines(symbol, "1m", 40)
    if len(c15) < 60 or len(c5) < 60:
        return None, "no_data"

    # Use last candles. Some APIs include active candle, but for scalping we accept this.
    close = c5[-1]["close"]
    if close <= 0:
        return None, "bad_price"

    cls5 = closes(c5)
    cls15 = closes(c15)
    cls1h = closes(c1h) if len(c1h) >= 30 else cls15

    ema20_5 = ema_last(cls5, 20)
    ema50_5 = ema_last(cls5, 50)
    ema50_15 = ema_last(cls15, 50)
    ema200_15 = ema_last(cls15, 200) if len(cls15) >= 80 else ema_last(cls15, 80)
    ema50_1h = ema_last(cls1h, 50)
    vwap5 = vwap_last(c5, 48)
    rsi5 = rsi_last(cls5, 14)
    rsi15 = rsi_last(cls15, 14)
    macd5 = macd_hist_last(cls5)
    macd15 = macd_hist_last(cls15)
    atr5 = atr_last(c5, 14)
    atr_pct = atr5 / close if close > 0 else 0
    vr5 = vol_ratio(c5, 30)
    vr15 = vol_ratio(c15, 30)
    cap = recent_capacity_pct(c5, c15)

    support15, resistance15 = find_support_resistance(c15, 50)
    support5, resistance5 = find_support_resistance(c5, 50)
    support = max(support15, support5) if support15 and support5 else (support15 or support5)
    resistance = min(resistance15, resistance5) if resistance15 and resistance5 else (resistance15 or resistance5)

    move15 = pct(c15[-1]["close"], c15[-2]["close"]) if len(c15) >= 2 else 0.0
    move30 = pct(c15[-1]["close"], c15[-3]["close"]) if len(c15) >= 3 else 0.0
    move5_3 = pct(c5[-1]["close"], c5[-4]["close"]) if len(c5) >= 4 else 0.0
    move1_3 = pct(c1[-1]["close"], c1[-4]["close"]) if len(c1) >= 4 else 0.0

    # Avoid ultra vertical chase.
    chase_long = move15 > CFG.max_chase_15m_pct or move30 > CFG.max_chase_30m_pct
    chase_short = move15 < -CFG.max_chase_15m_pct or move30 < -CFG.max_chase_30m_pct

    # Score both sides, then pick best.
    candidates: List[Tuple[float, str, List[str]]] = []

    for side in ("LONG", "SHORT"):
        if in_cooldown(state, symbol, side):
            continue
        score = 0.0
        reasons: List[str] = []

        # Determine broad permission.
        if side == "LONG" and not btc.get("bias_long", True):
            score -= 12
            reasons.append("BTC против LONG")
        if side == "SHORT" and not btc.get("bias_short", True):
            score -= 12
            reasons.append("BTC против SHORT")

        # Base quality / liquidity.
        if base_asset(symbol) in QUALITY_BASES:
            score += 7
            reasons.append("качественная база")
        if is_ultra_risk(symbol):
            score -= 18
            reasons.append("ultra-risk penalty")

        # Activity / capacity.
        if cap >= CFG.min_recent_capacity_pct_a:
            score += 10
            reasons.append(f"capacity {cap*100:.2f}%")
        elif cap >= CFG.min_recent_capacity_pct_b:
            score += 6
            reasons.append(f"compact capacity {cap*100:.2f}%")
        else:
            score -= 8

        if atr_pct >= 0.0035:
            score += 5

        # Volume. В fallback не требуем идеальный объём, но полный ноль не любим.
        if vr5 >= CFG.min_5m_volume_ratio_a or vr15 >= 0.85:
            score += 12
            reasons.append(f"volume x{max(vr5, vr15):.2f}")
        elif vr5 >= CFG.min_5m_volume_ratio_b or vr15 >= 0.45:
            score += 7
            reasons.append(f"volume ok x{max(vr5, vr15):.2f}")
        elif max(vr5, vr15) < 0.15:
            score -= 14
            reasons.append("volume weak")

        if side == "LONG":
            tol = CFG.level_tolerance_b  # score stage uses flexible tolerance; final mode will validate.
            near_support = support > 0 and abs(close - support) / close <= tol * 2.2
            reclaimed_vwap = close >= vwap5 * (1 - CFG.vwap_tolerance_b)
            above_ema = close >= ema20_5 * (1 - CFG.ema_tolerance_b) or close >= ema50_5 * (1 - CFG.ema_tolerance_b)
            trend_ok = close >= ema50_15 * 0.996 or close >= ema50_1h * 0.994
            impulse = move5_3 > 0.0015 or move1_3 > 0.0008 or macd5 > 0
            rsi_ok = 38 <= rsi5 <= 68 or rsi5 > 50

            if near_support:
                score += 15
                reasons.append("support bounce")
            if reclaimed_vwap:
                score += 13
                reasons.append("VWAP reclaim")
            if above_ema:
                score += 10
                reasons.append("EMA reclaim")
            if trend_ok:
                score += 10
                reasons.append("15m/1h trend ok")
            if impulse:
                score += 13
                reasons.append("micro impulse LONG")
            if rsi_ok:
                score += 5
            if macd15 > 0:
                score += 5
            if chase_long:
                score -= 18
                reasons.append("anti-chase")
            if rsi5 > 78:
                score -= 10
                reasons.append("RSI overheated")

        else:  # SHORT
            tol = CFG.level_tolerance_b
            near_resistance = resistance > 0 and abs(close - resistance) / close <= tol * 2.2
            rejected_vwap = close <= vwap5 * (1 + CFG.vwap_tolerance_b)
            below_ema = close <= ema20_5 * (1 + CFG.ema_tolerance_b) or close <= ema50_5 * (1 + CFG.ema_tolerance_b)
            trend_ok = close <= ema50_15 * 1.004 or close <= ema50_1h * 1.006
            impulse = move5_3 < -0.0015 or move1_3 < -0.0008 or macd5 < 0
            rsi_ok = 32 <= rsi5 <= 62 or rsi5 < 50

            if near_resistance:
                score += 15
                reasons.append("resistance rejection")
            if rejected_vwap:
                score += 13
                reasons.append("VWAP reject")
            if below_ema:
                score += 10
                reasons.append("EMA reject")
            if trend_ok:
                score += 10
                reasons.append("15m/1h trend ok")
            if impulse:
                score += 13
                reasons.append("micro impulse SHORT")
            if rsi_ok:
                score += 5
            if macd15 < 0:
                score += 5
            if chase_short:
                score -= 18
                reasons.append("anti-chase")
            if rsi5 < 22:
                score -= 10
                reasons.append("RSI oversold")

        # Range / flat penalty.
        if atr_pct < 0.0022 and cap < 0.0045:
            score -= 14
            reasons.append("flat")

        candidates.append((score, side, reasons))

    if not candidates:
        return None, "cooldown"

    candidates.sort(reverse=True, key=lambda x: x[0])
    quality, side, reasons = candidates[0]
    quality = clamp(quality, 0, 96)
    signal_mode = classify_signal_mode(quality, anti_mode)
    if not signal_mode:
        return None, "quality_too_low"

    # Final validations based on selected mode.
    min_vol = CFG.min_5m_volume_ratio_a if signal_mode == "A" else CFG.min_5m_volume_ratio_b
    if max(vr5, vr15) < min_vol and signal_mode != "B":
        return None, "volume_block"
    if max(vr5, vr15) < 0.15:
        return None, "dead_volume_block"

    min_cap = CFG.min_recent_capacity_pct_a if signal_mode == "A" else CFG.min_recent_capacity_pct_b
    if cap < min_cap:
        return None, "capacity_block"

    if side == "LONG" and chase_long and signal_mode == "A":
        return None, "anti_chase_block"
    if side == "SHORT" and chase_short and signal_mode == "A":
        return None, "anti_chase_block"

    entry = close
    sl = build_sl(side, entry, support, resistance, atr5, signal_mode)
    tps = build_adaptive_ladder(side, entry, CFG.leverage, signal_mode, cap)

    if not rr_ok(side, entry, sl, tps, signal_mode):
        # Anti-silence: для B можно один раз сжать SL до лимита и перепроверить.
        if signal_mode in ("B", "B_PLUS"):
            if side == "LONG":
                sl = entry * (1 - CFG.max_sl_price_pct_b * 0.90)
            else:
                sl = entry * (1 + CFG.max_sl_price_pct_b * 0.90)
        if not rr_ok(side, entry, sl, tps, signal_mode):
            return None, "rr_block"

    # Optional pending confirmation: only when signal is not strong enough and not hard fallback.
    needs_pending = CFG.pending_enabled and signal_mode == "B_PLUS" and anti_mode != "B_FALLBACK" and quality < (CFG.min_quality_b_plus + 4)

    candidate = {
        "id": f"{symbol}:{side}:{int(now_ts())}",
        "symbol": symbol,
        "display_symbol": to_display_symbol(symbol),
        "side": side,
        "mode": signal_mode,
        "anti_mode": anti_mode,
        "quality": round(quality, 1),
        "entry": entry,
        "sl": sl,
        "tps": tps,
        "leverage": CFG.leverage,
        "rm_pct": CFG.rm_per_signal_pct,
        "reasons": reasons[:8],
        "metrics": {
            "cap": cap,
            "atr_pct": atr_pct,
            "vr5": vr5,
            "vr15": vr15,
            "rsi5": rsi5,
            "rsi15": rsi15,
            "move1_3": move1_3,
            "move5_3": move5_3,
            "move15": move15,
            "move30": move30,
            "support": support,
            "resistance": resistance,
            "vwap5": vwap5,
            "ema50_5": ema50_5,
            "ema50_15": ema50_15,
            "ema200_15": ema200_15,
        },
        "created_ts": now_ts(),
        "status": "pending" if needs_pending else "ready",
    }

    if needs_pending:
        state.setdefault("pending", {})[symbol] = candidate
        return None, "pending_wait_confirmation"

    return candidate, "ok"


def check_pending_confirmations(state: Dict[str, Any], btc: Dict[str, Any]) -> List[Dict[str, Any]]:
    ready: List[Dict[str, Any]] = []
    pending = dict(state.get("pending", {}) or {})
    now = now_ts()
    for symbol, cand in pending.items():
        created = safe_float(cand.get("created_ts", 0))
        if now - created > CFG.pending_confirmation_timeout_sec:
            state.get("pending", {}).pop(symbol, None)
            continue
        c5 = fetch_klines(symbol, "5m", 20)
        if len(c5) < 5:
            continue
        close = c5[-1]["close"]
        side = cand.get("side")
        entry = safe_float(cand.get("entry"))
        # Confirmation: price moved slightly in our direction and did not hit SL.
        if side == "LONG" and close >= entry * 1.001:
            cand["status"] = "ready"
            cand["entry"] = close
            # rebuild tps from new entry
            cap = safe_float(cand.get("metrics", {}).get("cap", 0.008))
            cand["tps"] = build_adaptive_ladder("LONG", close, CFG.leverage, cand.get("mode", "B_PLUS"), cap)
            ready.append(cand)
            state.get("pending", {}).pop(symbol, None)
        elif side == "SHORT" and close <= entry * 0.999:
            cand["status"] = "ready"
            cand["entry"] = close
            cap = safe_float(cand.get("metrics", {}).get("cap", 0.008))
            cand["tps"] = build_adaptive_ladder("SHORT", close, CFG.leverage, cand.get("mode", "B_PLUS"), cap)
            ready.append(cand)
            state.get("pending", {}).pop(symbol, None)
    return ready

# ----------------------------
# Signal text / tracking
# ----------------------------
def format_signal(c: Dict[str, Any], btc: Dict[str, Any]) -> str:
    side = c["side"]
    emoji = "🟢" if side == "LONG" else "🔴"
    mode = c.get("mode", "A")
    mode_label = "A SETUP" if mode == "A" else ("B+ COMPACT SCALP" if mode == "B_PLUS" else "B ANTI-SILENCE SCALP")
    entry = safe_float(c["entry"])
    sl = safe_float(c["sl"])
    tps = [safe_float(x) for x in c.get("tps", [])]
    risk_pct = abs(entry - sl) / entry if entry > 0 else 0

    lines = []
    lines.append(f"{emoji} <b>{mode_label}</b>")
    lines.append("")
    lines.append(f"<b>{e(c.get('display_symbol', c.get('symbol')))} · {side}</b>")
    lines.append(f"Strategy: {VERSION}")
    lines.append(f"Quality: <b>{c.get('quality')}%</b>")
    lines.append(f"Mode: <b>{e(c.get('anti_mode', 'A_ONLY'))}</b>")
    lines.append("")
    lines.append(f"Entry: <b>{fmt_price(entry)}</b>")
    lines.append(f"Leverage: <b>x{int(c.get('leverage', CFG.leverage))}</b>")
    lines.append(f"RM: ≤<b>{CFG.rm_per_signal_pct:.2f}%</b> от депозита")
    lines.append(f"SL: <b>{fmt_price(sl)}</b> (~{risk_pct * CFG.leverage * 100:.1f}% ROI risk)")
    lines.append("")
    for i, tp in enumerate(tps, 1):
        mv = abs(tp - entry) / entry if entry else 0
        lines.append(f"TP{i}: <b>{fmt_price(tp)}</b> (+{roi_text(mv, CFG.leverage)})")
    lines.append("")
    lines.append(f"BTC: {e(btc.get('mode'))} · 1h {btc.get('h1', 0)*100:+.2f}% · 6h {btc.get('h6', 0)*100:+.2f}%")
    lines.append("Reason:")
    for r in c.get("reasons", [])[:6]:
        lines.append(f"• {e(r)}")

    metrics = c.get("metrics", {})
    lines.append("")
    lines.append(
        f"Capacity {safe_float(metrics.get('cap'))*100:.2f}% · "
        f"Vol5 x{safe_float(metrics.get('vr5')):.2f} · "
        f"RSI5 {safe_float(metrics.get('rsi5')):.1f}"
    )
    lines.append("")
    lines.append("⚠️ Это сигнал, не гарантия прибыли. Бот не открывает сделки сам. Соблюдай риск.")
    return "\n".join(lines)


def register_active_signal(state: Dict[str, Any], c: Dict[str, Any]) -> None:
    sid = c.get("id") or f"{c['symbol']}:{c['side']}:{int(now_ts())}"
    c = dict(c)
    c["id"] = sid
    c["sent_ts"] = now_ts()
    c["hit_tps"] = []
    c["status"] = "active"
    state.setdefault("active", {})[sid] = c


def mark_signal_sent(state: Dict[str, Any], c: Dict[str, Any]) -> None:
    state["last_signal_ts"] = now_ts()
    set_cooldown(state, c["symbol"], c["side"], c.get("mode", "B"))
    if CFG.track_active_signals:
        register_active_signal(state, c)


def current_price(symbol: str) -> float:
    c = fetch_klines(symbol, "1m", 5)
    if c:
        return c[-1]["close"]
    c = fetch_klines(symbol, "5m", 5)
    if c:
        return c[-1]["close"]
    return 0.0


def track_active_signals(state: Dict[str, Any]) -> None:
    if not CFG.track_active_signals:
        return
    active = dict(state.get("active", {}) or {})
    now = now_ts()
    for sid, sig in active.items():
        try:
            symbol = sig.get("symbol")
            side = sig.get("side")
            entry = safe_float(sig.get("entry"))
            sl = safe_float(sig.get("sl"))
            tps = [safe_float(x) for x in sig.get("tps", [])]
            hit = list(sig.get("hit_tps", []) or [])
            sent_ts = safe_float(sig.get("sent_ts", now))
            if now - sent_ts > CFG.signal_expiry_minutes * 60:
                telegram_send(f"⏱️ <b>Expired</b>\n{e(to_display_symbol(symbol))} · {e(side)}\nСделка не реализовалась быстро, сигнал закрыт по времени.")
                state.setdefault("stats", {})["expired"] = state.setdefault("stats", {}).get("expired", 0) + 1
                state.get("active", {}).pop(sid, None)
                continue

            price = current_price(symbol)
            if price <= 0:
                continue

            if side == "LONG":
                if price <= sl:
                    telegram_send(f"❌ <b>Stop Loss</b>\n{e(to_display_symbol(symbol))} · LONG\nEntry {fmt_price(entry)} · SL {fmt_price(sl)} · Last {fmt_price(price)}")
                    state.setdefault("stats", {})["sl"] = state.setdefault("stats", {}).get("sl", 0) + 1
                    state.get("active", {}).pop(sid, None)
                    continue
                for idx, tp in enumerate(tps, 1):
                    if idx not in hit and price >= tp:
                        hit.append(idx)
                        mv = abs(tp - entry) / entry
                        telegram_send(f"✅ <b>TP{idx} HIT</b>\n{e(to_display_symbol(symbol))} · LONG\nTP{idx}: {fmt_price(tp)} (+{roi_text(mv, CFG.leverage)})")
            else:
                if price >= sl:
                    telegram_send(f"❌ <b>Stop Loss</b>\n{e(to_display_symbol(symbol))} · SHORT\nEntry {fmt_price(entry)} · SL {fmt_price(sl)} · Last {fmt_price(price)}")
                    state.setdefault("stats", {})["sl"] = state.setdefault("stats", {}).get("sl", 0) + 1
                    state.get("active", {}).pop(sid, None)
                    continue
                for idx, tp in enumerate(tps, 1):
                    if idx not in hit and price <= tp:
                        hit.append(idx)
                        mv = abs(tp - entry) / entry
                        telegram_send(f"✅ <b>TP{idx} HIT</b>\n{e(to_display_symbol(symbol))} · SHORT\nTP{idx}: {fmt_price(tp)} (+{roi_text(mv, CFG.leverage)})")

            # If all TPs reached, close active as profit.
            if len(hit) >= len(tps) and tps:
                telegram_send(f"🏁 <b>All targets reached</b>\n{e(to_display_symbol(symbol))} · {e(side)}\nВсе цели взяты.")
                state.setdefault("stats", {})["profit"] = state.setdefault("stats", {}).get("profit", 0) + 1
                state.get("active", {}).pop(sid, None)
            else:
                sig["hit_tps"] = sorted(set(hit))
                state.setdefault("active", {})[sid] = sig
        except Exception:
            log.exception("track signal failed: %s", sid)

# ----------------------------
# Scan loop
# ----------------------------
def scan_once_sync(manual: bool = False) -> Dict[str, Any]:
    start = now_ts()
    state = STATE

    # Track existing signals first.
    track_active_signals(state)

    anti_mode = get_anti_silence_mode(state)
    btc = btc_context()
    symbols = fetch_contract_symbols()
    tickers = fetch_tickers()
    ranked = rank_symbols(symbols, tickers)

    checked = 0
    sent = 0
    candidates_count = 0
    block_counts: Dict[str, int] = {}
    hot_preview: List[str] = []
    near_miss: List[str] = []

    # First confirm pending.
    ready_from_pending = check_pending_confirmations(state, btc)
    for c in ready_from_pending[:CFG.max_signals_per_scan]:
        if active_count(state) >= CFG.max_active_signals:
            break
        if telegram_send(format_signal(c, btc)):
            mark_signal_sent(state, c)
            sent += 1

    # Analyze hot symbols.
    for symbol in ranked[:CFG.hot_symbols_to_analyze]:
        if sent >= CFG.max_signals_per_scan:
            break
        checked += 1
        try:
            c15 = fetch_klines(symbol, "15m", 6)
            c5 = fetch_klines(symbol, "5m", 8)
            c1 = fetch_klines(symbol, "1m", 6)
            if len(c15) >= 3 and len(c5) >= 3:
                m15 = pct(c15[-1]["close"], c15[-2]["close"])
                m30 = pct(c15[-1]["close"], c15[-3]["close"])
                m1_3 = pct(c1[-1]["close"], c1[-4]["close"]) if len(c1) >= 4 else 0
                hot_preview.append(f"{to_display_symbol(symbol)}: 1m3 {m1_3*100:+.2f}%, 15m {m15*100:+.2f}%, 30m {m30*100:+.2f}%")
        except Exception:
            pass

        cand, reason = analyze_symbol(symbol, anti_mode, btc, state)
        if cand:
            candidates_count += 1
            if telegram_send(format_signal(cand, btc)):
                mark_signal_sent(state, cand)
                sent += 1
            else:
                block_counts["telegram_failed"] = block_counts.get("telegram_failed", 0) + 1
        else:
            block_counts[reason] = block_counts.get(reason, 0) + 1
            if reason in ("quality_too_low", "rr_block", "capacity_block", "volume_block", "pending_wait_confirmation"):
                near_miss.append(f"{to_display_symbol(symbol)}: {reason}")

    elapsed = now_ts() - start
    summary = {
        "version": VERSION,
        "checked": checked,
        "universe": len(symbols),
        "analyzed_limit": CFG.hot_symbols_to_analyze,
        "candidates": candidates_count,
        "sent": sent,
        "elapsed_sec": round(elapsed, 1),
        "anti_silence_mode": anti_mode,
        "silent_minutes": round(silent_minutes(state), 1),
        "active": len(state.get("active", {}) or {}),
        "pending": len(state.get("pending", {}) or {}),
        "btc": btc,
        "stats": state.get("stats", {}),
        "blocks": dict(sorted(block_counts.items(), key=lambda x: x[1], reverse=True)),
        "hot_symbols": hot_preview[:8],
        "near_miss": near_miss[:8],
    }
    state["last_scan_ts"] = now_ts()
    state["last_scan_summary"] = summary

    # Limited scan update.
    if CFG.send_scan_updates or manual:
        last_upd = safe_float(state.get("last_scan_update_ts", 0))
        if manual or now_ts() - last_upd >= CFG.scan_update_min_interval_sec:
            telegram_send(format_scan_update(summary))
            state["last_scan_update_ts"] = now_ts()

    save_state_atomic(state)
    return summary


def format_scan_update(s: Dict[str, Any]) -> str:
    btc = s.get("btc", {})
    stats = s.get("stats", {})
    blocks = s.get("blocks", {})
    lines = []
    lines.append(f"🧪 <b>V14 scan update</b>")
    lines.append(e(s.get("version", VERSION)))
    lines.append(
        f"Проверено: {s.get('checked')} из {s.get('universe')} · "
        f"кандидатов: {s.get('candidates')} · отправлено: {s.get('sent')} · {s.get('elapsed_sec')}с"
    )
    lines.append(f"Mode: <b>{e(s.get('anti_silence_mode'))}</b> · silence {s.get('silent_minutes')} min")
    lines.append(f"Active: {s.get('active')} · Pending confirmation: {s.get('pending')}")
    lines.append(f"BTC: {e(btc.get('mode'))}: 1h {safe_float(btc.get('h1'))*100:+.2f}%, 6h {safe_float(btc.get('h6'))*100:+.2f}%")
    lines.append(f"Stats: {stats.get('profit',0)} profit / {stats.get('sl',0)} SL / {stats.get('expired',0)} expired / {stats.get('early',0)} early")
    if blocks:
        lines.append("Блокировки: " + ", ".join([f"{e(k)}:{v}" for k, v in list(blocks.items())[:8]]))
    hot = s.get("hot_symbols") or []
    if hot:
        lines.append("")
        lines.append("Hot symbols:")
        for x in hot[:5]:
            lines.append(e(x))
    miss = s.get("near_miss") or []
    if miss:
        lines.append("")
        lines.append("Почти прошли:")
        for x in miss[:5]:
            lines.append(e(x))
    return "\n".join(lines)

async def scan_once(manual: bool = False) -> Dict[str, Any]:
    async with SCAN_LOCK:
        return await asyncio.to_thread(scan_once_sync, manual)

async def background_worker() -> None:
    log.info("Background worker started: %s", VERSION)
    while True:
        try:
            await scan_once(manual=False)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("scan loop error")
        await asyncio.sleep(max(30, CFG.scan_interval_sec))

# ----------------------------
# FastAPI
# ----------------------------
WORKER_TASK: Optional[asyncio.Task] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global WORKER_TASK
    # Startup message once per process.
    log.info("Starting %s", VERSION)
    if CFG.telegram_token and CFG.telegram_chat_id:
        telegram_send(f"🚀 <b>{e(VERSION)} started</b>\nAnti-silence: ON · soft {CFG.starvation_soft_minutes}m · hard {CFG.starvation_hard_minutes}m")
    WORKER_TASK = asyncio.create_task(background_worker())
    try:
        yield
    finally:
        if WORKER_TASK:
            WORKER_TASK.cancel()
            try:
                await WORKER_TASK
            except Exception:
                pass

app = FastAPI(title=VERSION, lifespan=lifespan)


def check_admin(key: Optional[str]) -> bool:
    if not CFG.admin_key:
        return True
    return key == CFG.admin_key

@app.get("/", response_class=HTMLResponse)
async def root():
    st = STATE
    last = st.get("last_scan_summary", {})
    html_body = f"""
    <html>
      <head><title>{e(VERSION)}</title></head>
      <body style="font-family: Arial, sans-serif; max-width: 900px; margin: 40px auto; line-height: 1.5;">
        <h2>{e(VERSION)}</h2>
        <p><b>Status:</b> running</p>
        <p><b>Anti-silence mode:</b> {e(get_anti_silence_mode(st))}</p>
        <p><b>Silent minutes:</b> {silent_minutes(st):.1f}</p>
        <p><b>Last scan:</b> {e(json.dumps(last, ensure_ascii=False, indent=2))}</p>
        <p>Endpoints: <code>/health</code>, <code>/status</code>, <code>/scan</code></p>
      </body>
    </html>
    """
    return HTMLResponse(html_body)

@app.get("/health")
async def health():
    return {"ok": True, "version": VERSION, "time": now_ts()}

@app.get("/status")
async def status(key: Optional[str] = Query(None)):
    if not check_admin(key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    return JSONResponse({
        "ok": True,
        "version": VERSION,
        "anti_silence_mode": get_anti_silence_mode(STATE),
        "silent_minutes": silent_minutes(STATE),
        "state": STATE,
    })

@app.get("/scan")
async def manual_scan(key: Optional[str] = Query(None)):
    if not check_admin(key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    summary = await scan_once(manual=True)
    return JSONResponse({"ok": True, "summary": summary})

@app.post("/reset")
async def reset_state(key: Optional[str] = Query(None)):
    if not check_admin(key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    global STATE
    STATE = default_state()
    save_state_atomic(STATE)
    return {"ok": True, "version": VERSION}

@app.get("/text-status", response_class=PlainTextResponse)
async def text_status():
    last = STATE.get("last_scan_summary", {})
    return PlainTextResponse(json.dumps(last, ensure_ascii=False, indent=2))

# For local run:
# uvicorn app:app --host 0.0.0.0 --port 8000
