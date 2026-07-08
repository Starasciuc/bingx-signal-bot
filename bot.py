# -*- coding: utf-8 -*-
"""
V14 Professional Institutional Scalper
-------------------------------------
Новый движок без старого Instant Edge.

Цель:
- не ловить случайную свечу;
- сначала найти trader-style setup;
- затем дождаться подтверждения tape/price action;
- отправлять только после подтверждения;
- держать стоп локальным и коротким;
- автоматически выключать слабые стратегии по статистике.

Важно:
Это не финансовая рекомендация и не гарантия прибыли. Бот только отправляет сигналы.
"""

import os
import time
import math
import json
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI

# ============================================================
# APP / VERSION
# ============================================================

APP_NAME = "Professional Adaptive Futures Bot AUTO V14 MTF Structure Scalper"
APP_VERSION = "V14.05_MTF_STRUCTURE_SCALPER"
DEPLOY_MARKER = "V14_05_MTF_STRUCTURE_SCALPER_2026"

app = FastAPI(title=APP_NAME)

# ============================================================
# ENV HELPERS
# ============================================================

def env_str(name: str, default: str) -> str:
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
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

# ============================================================
# CONFIG
# ============================================================

BINGX_BASE = env_str("BINGX_BASE", "https://open-api.bingx.com")
TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID", "")

# Telegram service notifications
SEND_STARTUP_TELEGRAM = env_bool("SEND_STARTUP_TELEGRAM", True)
TELEGRAM_SCAN_REPORTS_ENABLED = env_bool("TELEGRAM_SCAN_REPORTS_ENABLED", True)
TELEGRAM_SCAN_REPORT_EVERY_SECONDS = env_int("TELEGRAM_SCAN_REPORT_EVERY_SECONDS", 300)
TELEGRAM_SCAN_REPORT_INCLUDE_HOT = env_bool("TELEGRAM_SCAN_REPORT_INCLUDE_HOT", True)

STATE_FILE = env_str("STATE_FILE", "bot_state_v14_05_mtf_structure_scalper.json")

# Scanning
AUTO_SCAN_ENABLED = env_bool("AUTO_SCAN_ENABLED", True)
AUTO_SCAN_SECONDS = env_int("AUTO_SCAN_SECONDS", 15)
AUTO_TRACK_SECONDS = env_int("AUTO_TRACK_SECONDS", 3)
HTTP_TIMEOUT = env_float("HTTP_TIMEOUT", 7.0)
HOT_WORKERS = env_int("HOT_WORKERS", 18)
MAX_ANALYZE_SYMBOLS = env_int("MAX_ANALYZE_SYMBOLS", 320)
HOT_SYMBOLS_TO_ANALYZE = env_int("HOT_SYMBOLS_TO_ANALYZE", 70)
MIN_HOT_SCORE = env_float("MIN_HOT_SCORE", 35.0)

# Signal limits
MAX_ACTIVE_SIGNALS = env_int("MAX_ACTIVE_SIGNALS", 1)
MAX_SIGNALS_PER_SCAN = env_int("MAX_SIGNALS_PER_SCAN", 1)
PAIR_COOLDOWN_SECONDS = env_int("PAIR_COOLDOWN_SECONDS", 900)
STRATEGY_COOLDOWN_SECONDS = env_int("STRATEGY_COOLDOWN_SECONDS", 180)

# Sides
ALLOW_LONG = env_bool("ALLOW_LONG", True)
ALLOW_SHORT = env_bool("ALLOW_SHORT", True)

# Old bad mode hard disabled
INSTANT_EDGE_ENABLED = False
INSTANT_EDGE_HARD_DISABLED = True

# New strategies
BEAT_STYLE_SHORT_ENABLED = env_bool("BEAT_STYLE_SHORT_ENABLED", True)
AERO_STYLE_ENABLED = env_bool("AERO_STYLE_ENABLED", True)
CONFIRMED_TAPE_ENABLED = env_bool("CONFIRMED_TAPE_ENABLED", True)
MARKET_DUMP_SHORT_ENABLED = env_bool("MARKET_DUMP_SHORT_ENABLED", True)
RECOVERY_RECLAIM_LONG_ENABLED = env_bool("RECOVERY_RECLAIM_LONG_ENABLED", True)

# Bounce / retest rejection setup. This is designed for examples like GRASS/AERO/BEAT SHORT:
# selloff -> bounce/retest upward -> rejection -> short continuation.
BOUNCE_REJECT_SHORT_ENABLED = env_bool("BOUNCE_REJECT_SHORT_ENABLED", True)

# PUMP trap setup. Designed for examples like PUMP SHORT:
# fast upside expansion / micro-pump -> rejection from local high -> confirmed downside tape.
# This is NOT the old Instant Edge; it still waits for confirmation before sending.
PUMP_TRAP_SHORT_ENABLED = env_bool("PUMP_TRAP_SHORT_ENABLED", True)

# Confirmation engine: important difference from V13
CONFIRMATION_ENGINE_ENABLED = env_bool("CONFIRMATION_ENGINE_ENABLED", True)
CONFIRM_MIN_SECONDS = env_int("CONFIRM_MIN_SECONDS", 35)
CONFIRM_MAX_SECONDS = env_int("CONFIRM_MAX_SECONDS", 150)
CONFIRM_MIN_MOVE = env_float("CONFIRM_MIN_MOVE", 0.0014)       # price must move 0.14% in our direction before sending
CONFIRM_MAX_CHASE = env_float("CONFIRM_MAX_CHASE", 0.0048)     # do not send if it already moved too far
IMMEDIATE_A_PLUS_SCORE = env_float("IMMEDIATE_A_PLUS_SCORE", 97.0)

# Professional anti-SL protection.
# V14.02 does not send a signal only because a setup exists.
# It waits for real confirmation and blocks snapback/reversal candles before send.
ALLOW_IMMEDIATE_SEND = env_bool("ALLOW_IMMEDIATE_SEND", False)
PRE_SEND_TAPE_PROTECTION_ENABLED = env_bool("PRE_SEND_TAPE_PROTECTION_ENABLED", True)
CONFIRM_MIN_TP1_PROGRESS = env_float("CONFIRM_MIN_TP1_PROGRESS", 0.22)
CONFIRM_MAX_TP1_PROGRESS = env_float("CONFIRM_MAX_TP1_PROGRESS", 0.78)
CONFIRM_MIN_VOL1 = env_float("CONFIRM_MIN_VOL1", 0.58)
CONFIRM_MIN_RANGE1 = env_float("CONFIRM_MIN_RANGE1", 0.72)
CONFIRM_SHORT_LOC_MAX = env_float("CONFIRM_SHORT_LOC_MAX", 0.42)
CONFIRM_LONG_LOC_MIN = env_float("CONFIRM_LONG_LOC_MIN", 0.58)
CONFIRM_MAX_OPPOSITE_1M = env_float("CONFIRM_MAX_OPPOSITE_1M", 0.0010)
CONFIRM_REQUIRE_MICRO_BREAK = env_bool("CONFIRM_REQUIRE_MICRO_BREAK", False)

# Early invalidation: this is not a Stop Loss.
# If the signal starts moving against us and never showed progress, bot exits the idea before full SL.
EARLY_INVALIDATION_ENABLED = env_bool("EARLY_INVALIDATION_ENABLED", True)
EARLY_INVALIDATION_SECONDS = env_int("EARLY_INVALIDATION_SECONDS", 90)
EARLY_INVALIDATION_ADVERSE_SL_FRACTION = env_float("EARLY_INVALIDATION_ADVERSE_SL_FRACTION", 0.55)
EARLY_INVALIDATION_MAX_PROGRESS = env_float("EARLY_INVALIDATION_MAX_PROGRESS", 0.12)

# TP ladder similar to BEAT/AERO examples, but not too silent
TP1_MOVE = env_float("TP1_MOVE", 0.0085)
TP2_MOVE = env_float("TP2_MOVE", 0.0145)
TP3_MOVE = env_float("TP3_MOVE", 0.0205)
TP4_MOVE = env_float("TP4_MOVE", 0.0305)
TP5_MOVE = env_float("TP5_MOVE", 0.0410)
TP_MOVES = [TP1_MOVE, TP2_MOVE, TP3_MOVE, TP4_MOVE, TP5_MOVE]

# Local scalp stop, tighter than previous versions
LEVERAGE_DISPLAY = env_int("LEVERAGE_DISPLAY", 10)
LOCAL_SCALP_STOP_ENABLED = env_bool("LOCAL_SCALP_STOP_ENABLED", True)
LOCAL_SCALP_MIN_SL_MOVE = env_float("LOCAL_SCALP_MIN_SL_MOVE", 0.0038)
LOCAL_SCALP_MAX_SL_MOVE = env_float("LOCAL_SCALP_MAX_SL_MOVE", 0.0070)
STRUCTURE_BUFFER = env_float("STRUCTURE_BUFFER", 0.0012)

# Quality filters
MIN_TP1_RR = env_float("MIN_TP1_RR", 0.65)
MIN_LADDER_RR = env_float("MIN_LADDER_RR_HARD", 1.25)
MIN_FINAL_RR = env_float("MIN_FINAL_RR_HARD", 3.00)
MAX_SCALP_SL_ROI = env_float("MAX_SCALP_SL_ROI", 14.5)
MIN_SCORE_TO_SEND = env_float("MIN_SCORE_TO_SEND", 80.0)

# Strategy statistic protection
STRATEGY_STATS_PROTECTION = env_bool("STRATEGY_STATS_PROTECTION", True)
STRATEGY_MIN_CLOSED_FOR_BLOCK = env_int("STRATEGY_MIN_CLOSED_FOR_BLOCK", 18)
STRATEGY_MIN_WR = env_float("STRATEGY_MIN_WR", 32.0)
STRATEGY_MAX_SL_RATE = env_float("STRATEGY_MAX_SL_RATE", 48.0)

# BEAT/AERO style requirements
BEAT_MIN_1M3 = env_float("BEAT_MIN_1M3", 0.0038)
BEAT_MIN_PULLBACK = env_float("BEAT_MIN_PULLBACK", 0.0045)
BEAT_MIN_RECENT_RANGE = env_float("BEAT_MIN_RECENT_RANGE", 0.0120)
BEAT_MIN_VOL1 = env_float("BEAT_MIN_VOL1", 0.38)
BEAT_MIN_VOL5 = env_float("BEAT_MIN_VOL5", 0.45)
BEAT_MIN_RANGE1 = env_float("BEAT_MIN_RANGE1", 0.60)
BEAT_MIN_RANGE5 = env_float("BEAT_MIN_RANGE5", 0.65)

AERO_MIN_1M3 = env_float("AERO_MIN_1M3", 0.0036)
AERO_MIN_PULLBACK = env_float("AERO_MIN_PULLBACK", 0.0040)
AERO_MIN_RECENT_RANGE = env_float("AERO_MIN_RECENT_RANGE", 0.0110)

# GRASS-style bounce reject SHORT requirements.
# These are intentionally not too strict so the bot does not go silent, but they force a real bounce/rejection
# instead of a random late market-order short.
BOUNCE_MIN_FROM_LOW = env_float("BOUNCE_MIN_FROM_LOW", 0.0060)       # price bounced at least 0.60% from local low
BOUNCE_MAX_FROM_LOW = env_float("BOUNCE_MAX_FROM_LOW", 0.0450)       # avoid shorting too high after full reversal
BOUNCE_MIN_REJECT_FROM_HIGH = env_float("BOUNCE_MIN_REJECT_FROM_HIGH", 0.0028)
BOUNCE_MIN_1M3 = env_float("BOUNCE_MIN_1M3", 0.0028)
BOUNCE_MAX_1M3_CHASE = env_float("BOUNCE_MAX_1M3_CHASE", 0.0140)
BOUNCE_MIN_RECENT_RANGE = env_float("BOUNCE_MIN_RECENT_RANGE", 0.0100)
BOUNCE_MIN_VOL1 = env_float("BOUNCE_MIN_VOL1", 0.42)
BOUNCE_MIN_VOL5 = env_float("BOUNCE_MIN_VOL5", 0.45)
BOUNCE_MIN_RANGE1 = env_float("BOUNCE_MIN_RANGE1", 0.62)
BOUNCE_MIN_RANGE5 = env_float("BOUNCE_MIN_RANGE5", 0.65)
BOUNCE_SHORT_LOC_MAX = env_float("BOUNCE_SHORT_LOC_MAX", 0.48)
BOUNCE_REQUIRE_CLOSE_UNDER_EMA_OR_VWAP = env_bool("BOUNCE_REQUIRE_CLOSE_UNDER_EMA_OR_VWAP", True)
BOUNCE_NEAR_LOW_BLOCK = env_bool("BOUNCE_NEAR_LOW_BLOCK", True)
BOUNCE_ALLOW_B_PLUS = env_bool("BOUNCE_ALLOW_B_PLUS", True)

# PUMP / squeeze trap SHORT requirements. This catches setups like PUMP SHORT:
# strong upward expansion -> failed continuation -> rejection from recent high -> short.
PUMP_MIN_15M_UP = env_float("PUMP_MIN_15M_UP", 0.0045)
PUMP_MIN_30M_UP = env_float("PUMP_MIN_30M_UP", 0.0060)
PUMP_MAX_30M_UP = env_float("PUMP_MAX_30M_UP", 0.0750)
PUMP_MIN_REJECT_FROM_HIGH = env_float("PUMP_MIN_REJECT_FROM_HIGH", 0.0022)
PUMP_MIN_1M3_DOWN = env_float("PUMP_MIN_1M3_DOWN", 0.0022)
PUMP_MAX_1M3_CHASE = env_float("PUMP_MAX_1M3_CHASE", 0.0130)
PUMP_MIN_RECENT_RANGE = env_float("PUMP_MIN_RECENT_RANGE", 0.0100)
PUMP_MIN_VOL1 = env_float("PUMP_MIN_VOL1", 0.34)
PUMP_MIN_VOL5 = env_float("PUMP_MIN_VOL5", 0.42)
PUMP_MIN_RANGE1 = env_float("PUMP_MIN_RANGE1", 0.58)
PUMP_MIN_RANGE5 = env_float("PUMP_MIN_RANGE5", 0.62)
PUMP_SHORT_LOC_MAX = env_float("PUMP_SHORT_LOC_MAX", 0.52)
PUMP_CONFIRM_MIN_TP1_PROGRESS = env_float("PUMP_CONFIRM_MIN_TP1_PROGRESS", 0.26)
PUMP_CONFIRM_SHORT_LOC_MAX = env_float("PUMP_CONFIRM_SHORT_LOC_MAX", 0.40)
PUMP_AVOID_NEAR_LOW = env_bool("PUMP_AVOID_NEAR_LOW", True)
PUMP_ALLOW_B_PLUS = env_bool("PUMP_ALLOW_B_PLUS", True)

# Professional multi-timeframe structure filter inspired by trader dashboards:
# 4h/1h/15m context + MACD/OBV/volume-delta proxy + confirmed structure.
# It is a final quality gate before a setup is allowed to wait for confirmation.
PRO_MTF_FILTER_ENABLED = env_bool("PRO_MTF_FILTER_ENABLED", True)
PRO_MTF_MIN_CONFIRMATIONS = env_float("PRO_MTF_MIN_CONFIRMATIONS", 3.0)
PRO_MTF_A_PLUS_MIN_CONFIRMATIONS = env_float("PRO_MTF_A_PLUS_MIN_CONFIRMATIONS", 3.5)
PRO_MTF_STRONG_SCORE_ESCAPE = env_float("PRO_MTF_STRONG_SCORE_ESCAPE", 93.0)
PRO_MTF_BLOCK_BAD_DELTA = env_bool("PRO_MTF_BLOCK_BAD_DELTA", True)
COINS_IN_ANALYSIS_TARGET = env_int("COINS_IN_ANALYSIS_TARGET", 12)

# Market dump, but not old bad dump. Requires tape confirmation/pending unless A+.
DUMP_MIN_1M3 = env_float("DUMP_MIN_1M3", 0.0048)
DUMP_MIN_15M = env_float("DUMP_MIN_15M", 0.0035)
DUMP_MIN_VOL1 = env_float("DUMP_MIN_VOL1", 0.55)
DUMP_MIN_VOL5 = env_float("DUMP_MIN_VOL5", 0.55)
DUMP_MIN_RANGE1 = env_float("DUMP_MIN_RANGE1", 0.70)
DUMP_MIN_RANGE5 = env_float("DUMP_MIN_RANGE5", 0.70)
DUMP_CLOSE_SHORT = env_float("DUMP_CLOSE_SHORT", 0.42)

# Recovery long: allowed, but must be stronger than shorts.
LONG_MIN_1M3 = env_float("LONG_MIN_1M3", 0.0052)
LONG_MIN_VOL1 = env_float("LONG_MIN_VOL1", 0.70)
LONG_MIN_RANGE1 = env_float("LONG_MIN_RANGE1", 0.85)
LONG_CLOSE_LONG = env_float("LONG_CLOSE_LONG", 0.62)

# Expiration after send
FAST_MAX_MINUTES_TO_TP1 = env_int("FAST_MAX_MINUTES_TO_TP1", 6)
FAST_HARD_EXPIRE_MINUTES = env_int("FAST_HARD_EXPIRE_MINUTES", 11)
FAST_MIN_PROGRESS_TO_KEEP = env_float("FAST_MIN_PROGRESS_TO_KEEP", 0.20)

# Symbol filters
ULTRA_RISK_KEYWORDS = [x.strip().upper() for x in env_str(
    "ULTRA_RISK_KEYWORDS",
    "UP,DOWN,BULL,BEAR,3L,3S,5L,5S"
).split(",") if x.strip()]

HEAVY_BASES = set(x.strip().upper() for x in env_str(
    "HEAVY_BASES",
    "BTC,ETH,BNB,SOL,XRP,DOGE,ADA,TRX,LINK,AVAX,DOT,LTC,BCH,XMR,GMX,AAVE,UNI,ATOM"
).split(",") if x.strip())

# ============================================================
# STATE
# ============================================================

STATE_LOCK = threading.RLock()


def empty_stat() -> Dict[str, int]:
    return {"profit": 0, "sl": 0, "expired": 0, "early_exit": 0}


def default_state() -> Dict[str, Any]:
    return {
        "version": APP_VERSION,
        "deploy_marker": DEPLOY_MARKER,
        "created_at": time.time(),
        "last_scan_at": 0,
        "last_scan_report_at": 0,
        "last_track_at": 0,
        "last_error": "",
        "active_signals": [],
        "pending_setups": [],
        "cooldowns": {},
        "stats": {
            "total": empty_stat(),
            "side": {},
            "class": {},
            "strategy": {},
            "type": {},
        },
        "last_scan": {},
    }


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            s = json.load(f)
        base = default_state()
        for k, v in base.items():
            if k not in s:
                s[k] = v
        s["version"] = APP_VERSION
        s["deploy_marker"] = DEPLOY_MARKER
        return s
    except Exception:
        return default_state()


STATE = load_state()


def save_state() -> None:
    with STATE_LOCK:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)

# ============================================================
# UTILS
# ============================================================

def now_ts() -> float:
    return time.time()


def fmt_price(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}".rstrip("0").rstrip(".")
    if x >= 0.01:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def display_symbol(symbol: str) -> str:
    return symbol.replace("-", "/")


def base_asset(symbol: str) -> str:
    return symbol.replace("-USDT", "").replace("/USDT", "").upper()


def is_ultra_risk(symbol: str) -> bool:
    b = base_asset(symbol)
    return any(k in b for k in ULTRA_RISK_KEYWORDS)


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        if b == 0:
            return default
        return a / b
    except Exception:
        return default


def item_total(item: Dict[str, int]) -> int:
    return int(item.get("profit", 0)) + int(item.get("sl", 0)) + int(item.get("expired", 0)) + int(item.get("early_exit", 0))


def item_wr(item: Dict[str, int]) -> Tuple[int, float]:
    total = item_total(item)
    if total <= 0:
        return 0, 0.0
    return total, 100.0 * float(item.get("profit", 0)) / total


def item_sl_rate(item: Dict[str, int]) -> float:
    total = item_total(item)
    if total <= 0:
        return 0.0
    return 100.0 * float(item.get("sl", 0)) / total


def format_stat_line(name: str, item: Dict[str, int]) -> str:
    total, wr = item_wr(item)
    return (
        f"{name}: {int(item.get('profit', 0))} profit / "
        f"{int(item.get('sl', 0))} SL / "
        f"{int(item.get('expired', 0))} expired / "
        f"{int(item.get('early_exit', 0))} early-exit / "
        f"WR {wr:.1f}%"
    )


def stats_summary_text(limit: int = 6) -> str:
    st = STATE.get("stats", {})
    lines = ["", "📊 <b>Статистика V14</b>"]
    lines.append(format_stat_line("Итого", st.get("total", empty_stat())))
    for title, key in [("Стороны", "side"), ("Стратегии", "strategy"), ("Типы", "type")]:
        bucket = st.get(key, {}) or {}
        if not bucket:
            continue
        lines.append("")
        lines.append(f"<b>{title}:</b>")
        items = sorted(bucket.items(), key=lambda kv: item_total(kv[1]), reverse=True)[:limit]
        for name, item in items:
            lines.append(format_stat_line(str(name), item))
    return "\n".join(lines)

# ============================================================
# HTTP / BINGX
# ============================================================

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "V14ProfessionalScalper/1.0"})


def http_get(path: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
    url = BINGX_BASE.rstrip("/") + path
    last = None
    for _ in range(2):
        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout or HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.15)
    raise last  # type: ignore


def bingx_symbols() -> List[str]:
    try:
        data = http_get("/openApi/swap/v2/quote/contracts")
        arr = data.get("data", []) if isinstance(data, dict) else []
        out = []
        for it in arr:
            sym = str(it.get("symbol", ""))
            if not sym or "USDT" not in sym:
                continue
            if not sym.endswith("USDT") and not sym.endswith("-USDT"):
                continue
            if "-" not in sym and sym.endswith("USDT"):
                sym = sym[:-4] + "-USDT"
            status = str(it.get("status", it.get("state", ""))).lower()
            if status and any(x in status for x in ["offline", "delist", "suspend"]):
                continue
            out.append(sym)
        out = sorted(list(dict.fromkeys(out)))
        return out
    except Exception as e:
        STATE["last_error"] = f"symbols: {repr(e)}"
        return []


def parse_klines_payload(data: Any) -> List[Dict[str, float]]:
    raw = data.get("data", []) if isinstance(data, dict) else data
    if raw is None:
        raw = []
    out = []
    for row in raw:
        try:
            if isinstance(row, dict):
                t = float(row.get("time", row.get("openTime", row.get("t", 0))))
                o = float(row.get("open", row.get("o")))
                h = float(row.get("high", row.get("h")))
                l = float(row.get("low", row.get("l")))
                c = float(row.get("close", row.get("c")))
                v = float(row.get("volume", row.get("v", row.get("quoteVolume", 0))))
            else:
                # BingX usually returns [time, open, high, low, close, volume]
                t = float(row[0])
                o = float(row[1])
                h = float(row[2])
                l = float(row[3])
                c = float(row[4])
                v = float(row[5]) if len(row) > 5 else 0.0
            if o > 0 and h > 0 and l > 0 and c > 0:
                out.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": max(v, 0.0)})
        except Exception:
            continue
    out.sort(key=lambda x: x["t"])
    return out


def klines(symbol: str, interval: str, limit: int = 60) -> List[Dict[str, float]]:
    data = http_get("/openApi/swap/v3/quote/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    out = parse_klines_payload(data)
    if not out:
        # fallback v2
        data = http_get("/openApi/swap/v2/quote/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        out = parse_klines_payload(data)
    return out[-limit:]

# ============================================================
# INDICATORS / PRICE ACTION
# ============================================================

def closes(c: List[Dict[str, float]]) -> List[float]:
    return [x["c"] for x in c]


def highs(c: List[Dict[str, float]]) -> List[float]:
    return [x["h"] for x in c]


def lows(c: List[Dict[str, float]]) -> List[float]:
    return [x["l"] for x in c]


def volumes(c: List[Dict[str, float]]) -> List[float]:
    return [x["v"] for x in c]


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b


def percent_change(c: List[Dict[str, float]], bars: int) -> float:
    if len(c) < bars + 1:
        return 0.0
    return pct(c[-1]["c"], c[-1 - bars]["c"])


def avg(vals: List[float], default: float = 0.0) -> float:
    vals = [x for x in vals if math.isfinite(x)]
    if not vals:
        return default
    return sum(vals) / len(vals)


def ema(values: List[float], length: int) -> float:
    if not values:
        return 0.0
    if len(values) < length:
        return avg(values, values[-1])
    k = 2.0 / (length + 1.0)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1.0 - k)
    return e


def vwap(c: List[Dict[str, float]], length: int = 20) -> float:
    arr = c[-length:] if len(c) >= length else c
    pv = 0.0
    vv = 0.0
    for x in arr:
        typical = (x["h"] + x["l"] + x["c"]) / 3.0
        pv += typical * x["v"]
        vv += x["v"]
    if vv <= 0:
        return arr[-1]["c"] if arr else 0.0
    return pv / vv


def atr(c: List[Dict[str, float]], length: int = 14) -> float:
    if len(c) < 2:
        return 0.0
    trs = []
    for i in range(1, len(c)):
        h = c[i]["h"]
        l = c[i]["l"]
        pc = c[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return 0.0
    return avg(trs[-length:])


def candle_range(x: Dict[str, float]) -> float:
    return max(x["h"] - x["l"], 0.0)


def close_location(x: Dict[str, float]) -> float:
    r = candle_range(x)
    if r <= 0:
        return 0.5
    return (x["c"] - x["l"]) / r


def volume_ratio(c: List[Dict[str, float]], lookback: int = 20) -> float:
    if len(c) < 3:
        return 1.0
    cur = c[-1]["v"]
    hist = [x["v"] for x in c[-lookback - 1:-1] if x["v"] >= 0]
    base = avg(hist, cur or 1.0)
    if base <= 0:
        return 1.0 if cur <= 0 else 10.0
    return min(cur / base, 20.0)


def range_ratio(c: List[Dict[str, float]], lookback: int = 20) -> float:
    if len(c) < 3:
        return 1.0
    cur = candle_range(c[-1])
    hist = [candle_range(x) for x in c[-lookback - 1:-1]]
    base = avg(hist, cur or 1.0)
    if base <= 0:
        return 1.0 if cur <= 0 else 10.0
    return min(cur / base, 10.0)


def recent_range_pct(c: List[Dict[str, float]], bars: int = 12) -> float:
    if len(c) < 2:
        return 0.0
    arr = c[-bars:] if len(c) >= bars else c
    hi = max(highs(arr))
    lo = min(lows(arr))
    last = arr[-1]["c"]
    return safe_div(hi - lo, last, 0.0)


def two_last_same_color(c: List[Dict[str, float]], side: str) -> bool:
    if len(c) < 2:
        return False
    a, b = c[-2], c[-1]
    if side == "SHORT":
        return a["c"] < a["o"] and b["c"] < b["o"]
    return a["c"] > a["o"] and b["c"] > b["o"]


def micro_break(c1: List[Dict[str, float]], side: str, bars: int = 4) -> bool:
    if len(c1) < bars + 2:
        return False
    last = c1[-1]
    prev = c1[-bars - 1:-1]
    if side == "SHORT":
        return last["c"] <= min(lows(prev)) or last["l"] <= min(lows(prev))
    return last["c"] >= max(highs(prev)) or last["h"] >= max(highs(prev))


def macd_hist(c: List[Dict[str, float]], fast: int = 12, slow: int = 26, signal: int = 9) -> float:
    """MACD histogram approximation from candle closes. Positive = bullish momentum, negative = bearish."""
    cl = closes(c)
    if len(cl) < slow + signal:
        return 0.0
    macd_line = []
    # Use rolling EMA approximations over growing windows; enough for a filter, not for charting.
    for i in range(slow, len(cl) + 1):
        sub = cl[:i]
        macd_line.append(ema(sub[-max(slow * 3, slow):], fast) - ema(sub[-max(slow * 3, slow):], slow))
    if len(macd_line) < signal:
        return macd_line[-1] if macd_line else 0.0
    sig = ema(macd_line, signal)
    return macd_line[-1] - sig


def obv_slope(c: List[Dict[str, float]], length: int = 10) -> float:
    """Normalized OBV slope. Positive = accumulation, negative = distribution."""
    if len(c) < length + 2:
        return 0.0
    vals = [0.0]
    for i in range(1, len(c)):
        if c[i]["c"] > c[i - 1]["c"]:
            vals.append(vals[-1] + c[i]["v"])
        elif c[i]["c"] < c[i - 1]["c"]:
            vals.append(vals[-1] - c[i]["v"])
        else:
            vals.append(vals[-1])
    recent = vals[-length:]
    denom = sum(abs(x) for x in recent) / max(len(recent), 1)
    if denom <= 0:
        denom = avg([x["v"] for x in c[-length:]], 1.0)
    return safe_div(recent[-1] - recent[0], denom * max(length, 1), 0.0)


def volume_delta_proxy(c: List[Dict[str, float]], length: int = 8) -> float:
    """Candle-based volume delta proxy in [-1, 1]. Green volume positive, red volume negative."""
    if len(c) < 2:
        return 0.0
    arr = c[-length:] if len(c) >= length else c
    signed = 0.0
    total = 0.0
    for x in arr:
        rng = candle_range(x)
        body = x["c"] - x["o"]
        # Weight by body dominance; wicks reduce confidence.
        dominance = abs(body) / rng if rng > 0 else 0.0
        signed += (1.0 if body > 0 else -1.0 if body < 0 else 0.0) * x["v"] * max(0.25, dominance)
        total += x["v"]
    return max(-1.0, min(1.0, safe_div(signed, total, 0.0)))


def pro_mtf_confirmation_score(tr: Dict[str, Any]) -> Tuple[float, str, List[str]]:
    """Return professional MTF confirmation score and explanation."""
    ctx = tr.get("ctx", {}) or {}
    side = tr.get("side", "")
    score = 0.0
    passed: List[str] = []
    failed: List[str] = []

    ch1h = float(ctx.get("ch1h_mtf", 0.0))
    ch4h = float(ctx.get("ch4h_mtf", 0.0))
    macd15 = float(ctx.get("macd15", 0.0))
    macd15_prev = float(ctx.get("macd15_prev", 0.0))
    obv5 = float(ctx.get("obv5_slope", 0.0))
    obv15 = float(ctx.get("obv15_slope", 0.0))
    vd1 = float(ctx.get("vdelta1", 0.0))
    vd5 = float(ctx.get("vdelta5", 0.0))

    if side == "SHORT":
        # Context should not be violently against the short unless this is a trap/rejection setup.
        trap = tr.get("strategy") in {"PRO_PUMP_TRAP_SHORT", "PRO_BOUNCE_REJECT_SHORT", "PRO_BEAT_STYLE_SHORT", "PRO_AERO_STYLE_SHORT"}
        if ch1h <= 0.006 or trap:
            score += 0.6; passed.append("1h/4h context ok")
        else:
            failed.append("1h контекст против SHORT")
        if macd15 <= 0 or macd15 < macd15_prev:
            score += 0.8; passed.append("15m MACD вниз/слабеет")
        else:
            failed.append("15m MACD не подтверждает")
        if obv5 < -0.02 or obv15 < -0.02:
            score += 0.8; passed.append("OBV distribution")
        else:
            failed.append("OBV без распределения")
        if vd1 < -0.05 or vd5 < -0.05:
            score += 0.8; passed.append("Vol Δ sell pressure")
        else:
            failed.append("Vol Δ слабый")
        if ctx.get("break_short") or ctx.get("two_red") or ctx.get("under_ema_or_vwap") or float(ctx.get("reject_from_high_10m", 0.0)) >= 0.002:
            score += 1.0; passed.append("структура SHORT подтверждена")
        else:
            failed.append("структура SHORT не подтверждена")
        if float(ctx.get("loc1", 0.5)) <= 0.48:
            score += 0.5; passed.append("1m close near low")
        else:
            failed.append("закрытие 1m не у low")
    else:
        if ch1h >= -0.006 or float(ctx.get("ch15m", 0.0)) >= 0.004:
            score += 0.6; passed.append("1h/RS context ok")
        else:
            failed.append("1h контекст против LONG")
        if macd15 >= 0 or macd15 > macd15_prev:
            score += 0.8; passed.append("15m MACD вверх/усиливается")
        else:
            failed.append("15m MACD не подтверждает")
        if obv5 > 0.02 or obv15 > 0.02:
            score += 0.8; passed.append("OBV accumulation")
        else:
            failed.append("OBV без накопления")
        if vd1 > 0.05 or vd5 > 0.05:
            score += 0.8; passed.append("Vol Δ buy pressure")
        else:
            failed.append("Vol Δ слабый")
        if ctx.get("break_long") or ctx.get("two_green") or float(ctx.get("bounce_from_low", 0.0)) >= 0.0035:
            score += 1.0; passed.append("структура LONG подтверждена")
        else:
            failed.append("структура LONG не подтверждена")
        if float(ctx.get("loc1", 0.5)) >= 0.52:
            score += 0.5; passed.append("1m close near high")
        else:
            failed.append("закрытие 1m не у high")

    note = "; ".join(passed[:4])
    return score, note, failed

# ============================================================
# MARKET CONTEXT / HOT SCANNER
# ============================================================

def market_context() -> Dict[str, Any]:
    try:
        btc1 = klines("BTC-USDT", "1m", 80)
        btc5 = klines("BTC-USDT", "5m", 80)
        eth5 = klines("ETH-USDT", "5m", 80)
        btc_1h = percent_change(btc5, 12)
        btc_6h = percent_change(btc5, 72) if len(btc5) >= 73 else percent_change(btc5, min(20, len(btc5)-1))
        eth_1h = percent_change(eth5, 12)
        btc_1m3 = percent_change(btc1, 3)
        if btc_6h <= -0.025 or btc_1h <= -0.008:
            regime = "BTC BEAR"
        elif btc_6h >= 0.025 or btc_1h >= 0.008:
            regime = "BTC BULL"
        else:
            regime = "BTC RANGE"
        panic = btc_1h <= -0.012 or btc_1m3 <= -0.004
        return {
            "regime": regime,
            "btc_1h": btc_1h,
            "btc_6h": btc_6h,
            "btc_1m3": btc_1m3,
            "eth_1h": eth_1h,
            "panic": panic,
            "text": f"{regime}: 1h {btc_1h*100:+.2f}%, 6h {btc_6h*100:+.2f}%, BTC 1m3 {btc_1m3*100:+.2f}%",
        }
    except Exception as e:
        return {"regime": "UNKNOWN", "btc_1h": 0.0, "btc_6h": 0.0, "btc_1m3": 0.0, "eth_1h": 0.0, "panic": False, "text": f"BTC UNKNOWN: {repr(e)}"}


def hot_score(symbol: str) -> Optional[Tuple[float, str, Dict[str, float]]]:
    if is_ultra_risk(symbol):
        return None
    try:
        c1 = klines(symbol, "1m", 35)
        c5 = klines(symbol, "5m", 35)
        if len(c1) < 15 or len(c5) < 8:
            return None
        ch1m3 = percent_change(c1, 3)
        ch15m = percent_change(c5, 3)
        ch30m = percent_change(c5, 6)
        vr1 = volume_ratio(c1, 20)
        vr5 = volume_ratio(c5, 20)
        rr1 = range_ratio(c1, 20)
        rr5 = range_ratio(c5, 20)
        r15 = recent_range_pct(c1, 15)
        live = abs(ch1m3) * 10500 + min(vr1, 4.0) * 10 + min(rr1, 4.0) * 12
        recent = abs(ch15m) * 900 + abs(ch30m) * 380 + min(vr5, 4.0) * 5 + min(rr5, 4.0) * 5
        score = live + recent + min(r15 * 600, 30)
        dead = abs(ch1m3) < 0.0012 and rr1 < 0.55 and vr1 < 0.55
        if dead:
            score *= 0.20
        tag = "LIVE/MOM" if (abs(ch1m3) >= 0.003 or rr1 >= 1.0) else "WATCH"
        note = f"{tag}: 1m3 {ch1m3*100:+.2f}%, 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, vol1 x{vr1:.2f}, vol5 x{vr5:.2f}, range1 x{rr1:.2f}, range5 x{rr5:.2f}"
        return score, note, {"ch1m3": ch1m3, "ch15m": ch15m, "ch30m": ch30m, "vol1": vr1, "vol5": vr5, "range1": rr1, "range5": rr5}
    except Exception:
        return None


def select_hot_symbols(symbols: List[str]) -> Tuple[List[str], List[str]]:
    universe = [s for s in symbols if not is_ultra_risk(s)][:MAX_ANALYZE_SYMBOLS]
    scores: List[Tuple[float, str, str]] = []
    notes: List[str] = []
    with ThreadPoolExecutor(max_workers=max(2, HOT_WORKERS)) as ex:
        futs = {ex.submit(hot_score, s): s for s in universe}
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                res = fut.result()
                if not res:
                    continue
                sc, note, _ = res
                scores.append((sc, s, note))
            except Exception:
                continue
    scores.sort(reverse=True, key=lambda x: x[0])
    for sc, s, note in scores[:8]:
        notes.append(f"{display_symbol(s)} hot {sc:.1f}: {note}")
    selected = [s for sc, s, _ in scores if sc >= MIN_HOT_SCORE][:HOT_SYMBOLS_TO_ANALYZE]
    if len(selected) < min(30, HOT_SYMBOLS_TO_ANALYZE):
        for _, s, _ in scores:
            if s not in selected:
                selected.append(s)
            if len(selected) >= min(30, HOT_SYMBOLS_TO_ANALYZE):
                break
    return selected, notes

# ============================================================
# SYMBOL CONTEXT
# ============================================================

def symbol_context(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        c1 = klines(symbol, "1m", 80)
        c5 = klines(symbol, "5m", 80)
        c15 = klines(symbol, "15m", 60)
        if len(c1) < 30 or len(c5) < 20:
            return None
        price = c1[-1]["c"]
        cl1 = closes(c1)
        cl5 = closes(c5)
        ctx = {
            "symbol": symbol,
            "price": price,
            "c1": c1,
            "c5": c5,
            "c15": c15,
            "ch1m1": percent_change(c1, 1),
            "ch1m3": percent_change(c1, 3),
            "ch1m5": percent_change(c1, 5),
            "ch15m": percent_change(c5, 3),
            "ch30m": percent_change(c5, 6),
            "ch1h": percent_change(c5, 12),
            "vol1": volume_ratio(c1, 20),
            "vol5": volume_ratio(c5, 20),
            "range1": range_ratio(c1, 20),
            "range5": range_ratio(c5, 20),
            "loc1": close_location(c1[-1]),
            "loc5": close_location(c5[-1]),
            "ema1_9": ema(cl1[-30:], 9),
            "ema1_21": ema(cl1[-50:], 21),
            "ema5_9": ema(cl5[-30:], 9),
            "ema5_21": ema(cl5[-50:], 21),
            "vwap1": vwap(c1, 20),
            "vwap5": vwap(c5, 20),
            "atr1": atr(c1, 14),
            "recent_range_15m": recent_range_pct(c1, 15),
            "recent_range_30m": recent_range_pct(c1, 30),
            "recent_high_10m": max(highs(c1[-10:])),
            "recent_low_10m": min(lows(c1[-10:])),
            "recent_high_20m": max(highs(c1[-20:])),
            "recent_low_20m": min(lows(c1[-20:])),
            "last_red": c1[-1]["c"] < c1[-1]["o"],
            "last_green": c1[-1]["c"] > c1[-1]["o"],
            "two_red": two_last_same_color(c1, "SHORT"),
            "two_green": two_last_same_color(c1, "LONG"),
            "break_short": micro_break(c1, "SHORT", 4),
            "break_long": micro_break(c1, "LONG", 4),
        }
        ctx["pullback_from_high"] = max(0.0, safe_div(ctx["recent_high_20m"] - price, price))
        ctx["bounce_from_low"] = max(0.0, safe_div(price - ctx["recent_low_20m"], price))
        ctx["bounce_from_low_10m"] = max(0.0, safe_div(price - ctx["recent_low_10m"], price))
        ctx["reject_from_high_10m"] = max(0.0, safe_div(ctx["recent_high_10m"] - price, price))
        ctx["selloff_15m"] = max(0.0, safe_div(max(highs(c1[-15:])) - min(lows(c1[-15:])), price, 0.0))
        # MTF / pro-dashboard style filters. 1h/4h are approximated from 15m candles to keep scanning fast.
        ctx["ch1h_mtf"] = percent_change(c15, 4) if len(c15) >= 5 else ctx["ch1h"]
        ctx["ch4h_mtf"] = percent_change(c15, 16) if len(c15) >= 17 else ctx["ch1h"]
        ctx["macd15"] = macd_hist(c15) if len(c15) >= 35 else 0.0
        ctx["macd15_prev"] = macd_hist(c15[:-1]) if len(c15) >= 36 else 0.0
        ctx["obv5_slope"] = obv_slope(c5, 10)
        ctx["obv15_slope"] = obv_slope(c15, 8) if len(c15) >= 10 else 0.0
        ctx["vdelta1"] = volume_delta_proxy(c1, 8)
        ctx["vdelta5"] = volume_delta_proxy(c5, 6)
        ctx["under_ema_or_vwap"] = price < ctx["ema1_9"] or price < ctx["vwap1"]
        ctx["under_both_ema_vwap"] = price < ctx["ema1_9"] and price < ctx["vwap1"]
        return ctx
    except Exception:
        return None

# ============================================================
# TRADE / QUALITY
# ============================================================

def build_trade(ctx: Dict[str, Any], side: str, strategy: str, setup_type: str, score: float, reason: str) -> Dict[str, Any]:
    entry = float(ctx["price"])
    c1 = ctx["c1"]
    if side == "SHORT":
        tps = [entry * (1.0 - m) for m in TP_MOVES]
        structural_move = safe_div(max(ctx.get("recent_high_10m", entry), entry) - entry, entry, 0.0) + STRUCTURE_BUFFER
        sl_move = max(structural_move, LOCAL_SCALP_MIN_SL_MOVE)
        if LOCAL_SCALP_STOP_ENABLED:
            sl_move = min(sl_move, LOCAL_SCALP_MAX_SL_MOVE)
        sl = entry * (1.0 + sl_move)
        tp1_reward = safe_div(entry - tps[0], entry, 0.0)
        final_reward = safe_div(entry - tps[-1], entry, 0.0)
        ladder_reward = avg([safe_div(entry - x, entry, 0.0) for x in tps])
    else:
        tps = [entry * (1.0 + m) for m in TP_MOVES]
        structural_move = safe_div(entry - min(ctx.get("recent_low_10m", entry), entry), entry, 0.0) + STRUCTURE_BUFFER
        sl_move = max(structural_move, LOCAL_SCALP_MIN_SL_MOVE)
        if LOCAL_SCALP_STOP_ENABLED:
            sl_move = min(sl_move, LOCAL_SCALP_MAX_SL_MOVE)
        sl = entry * (1.0 - sl_move)
        tp1_reward = safe_div(tps[0] - entry, entry, 0.0)
        final_reward = safe_div(tps[-1] - entry, entry, 0.0)
        ladder_reward = avg([safe_div(x - entry, entry, 0.0) for x in tps])

    tp1_rr = safe_div(tp1_reward, sl_move, 0.0)
    ladder_rr = safe_div(ladder_reward, sl_move, 0.0)
    final_rr = safe_div(final_reward, sl_move, 0.0)
    roi_sl = sl_move * LEVERAGE_DISPLAY * 100.0
    roi_tp1 = tp1_reward * LEVERAGE_DISPLAY * 100.0
    return {
        "id": f"{symbol_key(ctx['symbol'])}_{int(time.time()*1000)}_{side}_{strategy}",
        "symbol": ctx["symbol"],
        "side": side,
        "strategy": strategy,
        "type": setup_type,
        "class": "A+" if score >= 88 else "B",
        "score": round(score, 1),
        "entry": entry,
        "sl": sl,
        "tps": tps,
        "tp1": tps[0],
        "created_at": now_ts(),
        "reason": reason,
        "ctx": {
            "ch1m3": ctx.get("ch1m3", 0),
            "ch15m": ctx.get("ch15m", 0),
            "ch30m": ctx.get("ch30m", 0),
            "vol1": ctx.get("vol1", 0),
            "vol5": ctx.get("vol5", 0),
            "range1": ctx.get("range1", 0),
            "range5": ctx.get("range5", 0),
            "loc1": ctx.get("loc1", 0.5),
            "pullback_from_high": ctx.get("pullback_from_high", 0),
            "bounce_from_low": ctx.get("bounce_from_low", 0),
            "reject_from_high_10m": ctx.get("reject_from_high_10m", 0),
            "bounce_from_low_10m": ctx.get("bounce_from_low_10m", 0),
            "break_short": ctx.get("break_short", False),
            "break_long": ctx.get("break_long", False),
            "two_red": ctx.get("two_red", False),
            "two_green": ctx.get("two_green", False),
            "last_red": ctx.get("last_red", False),
            "last_green": ctx.get("last_green", False),
            "under_ema_or_vwap": ctx.get("under_ema_or_vwap", False),
            "under_both_ema_vwap": ctx.get("under_both_ema_vwap", False),
            "ch1h_mtf": ctx.get("ch1h_mtf", 0),
            "ch4h_mtf": ctx.get("ch4h_mtf", 0),
            "macd15": ctx.get("macd15", 0),
            "macd15_prev": ctx.get("macd15_prev", 0),
            "obv5_slope": ctx.get("obv5_slope", 0),
            "obv15_slope": ctx.get("obv15_slope", 0),
            "vdelta1": ctx.get("vdelta1", 0),
            "vdelta5": ctx.get("vdelta5", 0),
        },
        "risk": {
            "sl_move": sl_move,
            "tp1_rr": tp1_rr,
            "ladder_rr": ladder_rr,
            "final_rr": final_rr,
            "roi_sl": roi_sl,
            "roi_tp1": roi_tp1,
        },
        "max_progress": 0.0,
    }


def symbol_key(symbol: str) -> str:
    return symbol.replace("-", "_").replace("/", "_")


def pro_mtf_filter_ok(tr: Dict[str, Any], blocks: Dict[str, int], near: List[str]) -> bool:
    if not PRO_MTF_FILTER_ENABLED:
        return True
    score, note, failed = pro_mtf_confirmation_score(tr)
    required = PRO_MTF_A_PLUS_MIN_CONFIRMATIONS if tr.get("class") == "A+" else PRO_MTF_MIN_CONFIRMATIONS
    # Very strong setups may pass with a small deficit, but only if delta is not directly against the trade.
    ctx = tr.get("ctx", {}) or {}
    side = tr.get("side", "")
    bad_delta = False
    if side == "SHORT":
        bad_delta = float(ctx.get("vdelta1", 0.0)) > 0.25 and float(ctx.get("vdelta5", 0.0)) > 0.15
    elif side == "LONG":
        bad_delta = float(ctx.get("vdelta1", 0.0)) < -0.25 and float(ctx.get("vdelta5", 0.0)) < -0.15

    escape = tr.get("score", 0) >= PRO_MTF_STRONG_SCORE_ESCAPE and score >= required - 0.7 and not bad_delta
    if (score >= required and not (PRO_MTF_BLOCK_BAD_DELTA and bad_delta)) or escape:
        tr["reason"] = tr.get("reason", "") + f" MTF-фильтр: {score:.1f}/{required:.1f}; {note}."
        tr.setdefault("ctx", {})["mtf_score"] = score
        return True

    blocks["mtf_structure_block"] = blocks.get("mtf_structure_block", 0) + 1
    if bad_delta:
        blocks["mtf_bad_delta_block"] = blocks.get("mtf_bad_delta_block", 0) + 1
    if len(near) < 10:
        near.append(f"{display_symbol(tr['symbol'])} {side}: MTF {score:.1f}/{required:.1f} блок · " + "; ".join(failed[:3]))
    return False


def trade_quality_ok(tr: Dict[str, Any], blocks: Dict[str, int], near: List[str]) -> bool:
    sym = tr["symbol"]
    risk = tr["risk"]
    if tr["score"] < MIN_SCORE_TO_SEND:
        blocks["score_too_low"] = blocks.get("score_too_low", 0) + 1
        return False
    if risk["roi_sl"] > MAX_SCALP_SL_ROI:
        blocks["sl_roi_too_high_block"] = blocks.get("sl_roi_too_high_block", 0) + 1
        if len(near) < 10:
            near.append(f"{display_symbol(sym)} {tr['side']}: SL risk too high {risk['roi_sl']:.1f}% ROI")
        return False
    if risk["tp1_rr"] < MIN_TP1_RR:
        blocks["tp1_rr_block"] = blocks.get("tp1_rr_block", 0) + 1
        return False
    if risk["ladder_rr"] < MIN_LADDER_RR:
        blocks["ladder_rr_block"] = blocks.get("ladder_rr_block", 0) + 1
        return False
    if risk["final_rr"] < MIN_FINAL_RR:
        blocks["final_rr_block"] = blocks.get("final_rr_block", 0) + 1
        return False
    # Heavy coins require A+ only, because their fast ladder is often less clean.
    if base_asset(sym) in HEAVY_BASES and tr["score"] < 90:
        blocks["heavy_coin_score_block"] = blocks.get("heavy_coin_score_block", 0) + 1
        return False
    if not pro_mtf_filter_ok(tr, blocks, near):
        return False
    return True

# ============================================================
# STATS / COOLDOWNS / KILL SWITCH
# ============================================================

def update_stat_bucket(bucket: Dict[str, Dict[str, int]], key: str, outcome: str) -> None:
    item = bucket.setdefault(key, empty_stat())
    item[outcome] = int(item.get(outcome, 0)) + 1


def record_outcome(tr: Dict[str, Any], outcome: str) -> None:
    with STATE_LOCK:
        STATE["stats"].setdefault("total", empty_stat())[outcome] += 1
        update_stat_bucket(STATE["stats"].setdefault("side", {}), tr["side"], outcome)
        update_stat_bucket(STATE["stats"].setdefault("class", {}), tr.get("class", "?"), outcome)
        update_stat_bucket(STATE["stats"].setdefault("strategy", {}), tr["strategy"], outcome)
        update_stat_bucket(STATE["stats"].setdefault("type", {}), tr["type"], outcome)
        save_state()


def strategy_allowed(strategy: str) -> Tuple[bool, str]:
    if not STRATEGY_STATS_PROTECTION:
        return True, "ok"
    item = STATE.get("stats", {}).get("strategy", {}).get(strategy, empty_stat())
    total, wr = item_wr(item)
    slr = item_sl_rate(item)
    if total >= STRATEGY_MIN_CLOSED_FOR_BLOCK and (wr < STRATEGY_MIN_WR or slr > STRATEGY_MAX_SL_RATE):
        return False, f"strategy blocked by stats: WR {wr:.1f}%, SL-rate {slr:.1f}% after {total}"
    return True, "ok"


def cooldown_ok(symbol: str, strategy: str) -> Tuple[bool, str]:
    cd = STATE.setdefault("cooldowns", {})
    now = now_ts()
    s_key = f"pair:{symbol}"
    st_key = f"strategy:{strategy}"
    if now - float(cd.get(s_key, 0)) < PAIR_COOLDOWN_SECONDS:
        return False, "pair cooldown"
    if now - float(cd.get(st_key, 0)) < STRATEGY_COOLDOWN_SECONDS:
        return False, "strategy cooldown"
    return True, "ok"


def set_cooldown(symbol: str, strategy: str) -> None:
    with STATE_LOCK:
        cd = STATE.setdefault("cooldowns", {})
        cd[f"pair:{symbol}"] = now_ts()
        cd[f"strategy:{strategy}"] = now_ts()
        save_state()

# ============================================================
# STRATEGIES
# ============================================================

def score_common(ctx: Dict[str, Any], side: str, base: float = 70.0) -> float:
    ch = ctx["ch1m3"] if side == "LONG" else -ctx["ch1m3"]
    ch15 = ctx["ch15m"] if side == "LONG" else -ctx["ch15m"]
    score = base
    score += min(max(ch, 0) * 4200, 18)
    score += min(max(ch15, 0) * 900, 8)
    score += min(ctx["vol1"], 2.2) * 3.5
    score += min(ctx["vol5"], 2.2) * 2.5
    score += min(ctx["range1"], 3.0) * 3.0
    score += min(ctx["range5"], 3.0) * 2.5
    if side == "SHORT":
        score += max(0, 0.5 - ctx["loc1"]) * 12
        if ctx["two_red"]:
            score += 5
        if ctx["break_short"]:
            score += 5
    else:
        score += max(0, ctx["loc1"] - 0.5) * 12
        if ctx["two_green"]:
            score += 5
        if ctx["break_long"]:
            score += 5
    return min(score, 99.0)


def detect_pump_trap_short(ctx: Dict[str, Any], market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    PUMP-style trap short.
    Pattern: fast upside expansion / pump -> rejection from local high -> confirmed downside tape.
    This is the type of trade from the PUMP example: entry after the pump fails,
    close to the rejection zone, with TP1 around 0.8-1.0% and TP5 around 4%.
    """
    if not (ALLOW_SHORT and PUMP_TRAP_SHORT_ENABLED):
        return None

    # There must be a real upside expansion first; otherwise this is just a normal dump.
    had_pump = ctx["ch15m"] >= PUMP_MIN_15M_UP or ctx["ch30m"] >= PUMP_MIN_30M_UP
    if not had_pump:
        return None

    # Avoid extreme late-parabolic tops unless tape is very strong.
    if ctx["ch30m"] > PUMP_MAX_30M_UP and not (ctx["break_short"] and ctx["vol1"] >= 0.80 and ctx["range1"] >= 1.20):
        return None

    # Must reject from recent high. This prevents shorting random weakness.
    reject = max(ctx.get("reject_from_high_10m", 0.0), ctx.get("pullback_from_high", 0.0))
    if reject < PUMP_MIN_REJECT_FROM_HIGH:
        return None

    # Fresh downside pressure, but not too late/chased.
    if ctx["ch1m3"] > -PUMP_MIN_1M3_DOWN:
        return None
    if abs(ctx["ch1m3"]) > PUMP_MAX_1M3_CHASE:
        return None

    # Enough recent range for a 5-target ladder like the examples.
    if ctx["recent_range_30m"] < PUMP_MIN_RECENT_RANGE:
        return None

    # Tape must be alive, but we keep it realistic so the bot does not go silent.
    if ctx["vol1"] < PUMP_MIN_VOL1 or ctx["vol5"] < PUMP_MIN_VOL5:
        return None
    if ctx["range1"] < PUMP_MIN_RANGE1 or ctx["range5"] < PUMP_MIN_RANGE5:
        return None

    # Candle must close under control of sellers.
    if ctx["loc1"] > PUMP_SHORT_LOC_MAX:
        return None
    if not (ctx["last_red"] or ctx["two_red"] or ctx["break_short"]):
        return None

    # Price should lose momentum area: EMA/VWAP or have a clean micro-break.
    if not (ctx.get("under_ema_or_vwap") or ctx["break_short"] or ctx["two_red"]):
        return None

    # Do not send if already sitting on the local low; the example trades enter before the whole move is gone.
    if PUMP_AVOID_NEAR_LOW and ctx["bounce_from_low"] < max(TP1_MOVE * 0.45, 0.0035):
        return None

    score = score_common(ctx, "SHORT", 78.0)
    score += min(max(ctx["ch15m"], 0.0) * 850, 7)
    score += min(max(ctx["ch30m"], 0.0) * 450, 7)
    score += min(reject * 1400, 8)
    if ctx.get("under_both_ema_vwap"):
        score += 4
    if ctx["break_short"]:
        score += 5

    if not PUMP_ALLOW_B_PLUS and score < 88:
        return None

    reason = (
        "PUMP TRAP SHORT: быстрый памп/расширение вверх → failure/reject от локального high → tape развернулся вниз. "
        f"pump15 {ctx['ch15m']*100:+.2f}%, pump30 {ctx['ch30m']*100:+.2f}%, "
        f"reject {reject*100:.2f}%, 1m3 {ctx['ch1m3']*100:.2f}%, "
        f"vol1 x{ctx['vol1']:.2f}, vol5 x{ctx['vol5']:.2f}, "
        f"range1 x{ctx['range1']:.2f}, range5 x{ctx['range5']:.2f}."
    )
    return build_trade(ctx, "SHORT", "PRO_PUMP_TRAP_SHORT", "PUMP TRAP SHORT", score, reason)


def detect_beat_style_short(ctx: Dict[str, Any], market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not (ALLOW_SHORT and BEAT_STYLE_SHORT_ENABLED):
        return None
    if ctx["ch1m3"] > -BEAT_MIN_1M3:
        return None
    if ctx["pullback_from_high"] < BEAT_MIN_PULLBACK:
        return None
    if ctx["recent_range_30m"] < BEAT_MIN_RECENT_RANGE:
        return None
    if ctx["vol1"] < BEAT_MIN_VOL1 or ctx["vol5"] < BEAT_MIN_VOL5:
        return None
    if ctx["range1"] < BEAT_MIN_RANGE1 or ctx["range5"] < BEAT_MIN_RANGE5:
        return None
    if ctx["loc1"] > 0.55:
        return None
    if not (ctx["last_red"] or ctx["two_red"] or ctx["break_short"]):
        return None
    if ctx["price"] > max(ctx["ema1_9"], ctx["vwap1"]) and not ctx["break_short"]:
        return None
    score = score_common(ctx, "SHORT", 73.0)
    score += min(ctx["pullback_from_high"] * 1000, 8)
    reason = (
        "BEAT STYLE SHORT: откат/вынос вверх → reject → live downside pressure. "
        f"pullback {ctx['pullback_from_high']*100:.2f}%, 1m3 {ctx['ch1m3']*100:.2f}%, "
        f"15m {ctx['ch15m']*100:.2f}%, vol1 x{ctx['vol1']:.2f}, range1 x{ctx['range1']:.2f}."
    )
    return build_trade(ctx, "SHORT", "PRO_BEAT_STYLE_SHORT", "BEAT STYLE SHORT", score, reason)


def detect_aero_style_short(ctx: Dict[str, Any], market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not (ALLOW_SHORT and AERO_STYLE_ENABLED):
        return None
    if ctx["ch1m3"] > -AERO_MIN_1M3:
        return None
    if ctx["pullback_from_high"] < AERO_MIN_PULLBACK:
        return None
    if ctx["recent_range_30m"] < AERO_MIN_RECENT_RANGE:
        return None
    if ctx["loc1"] > 0.50:
        return None
    if ctx["vol1"] < 0.42 or ctx["range1"] < 0.75:
        return None
    if not (ctx["break_short"] or ctx["two_red"]):
        return None
    score = score_common(ctx, "SHORT", 75.0)
    reason = (
        "AERO STYLE SHORT: перехват после отката, пробой микро-структуры вниз, "
        f"1m3 {ctx['ch1m3']*100:.2f}%, range5 x{ctx['range5']:.2f}."
    )
    return build_trade(ctx, "SHORT", "PRO_AERO_STYLE_SHORT", "AERO STYLE SHORT", score, reason)


def detect_bounce_reject_short(ctx: Dict[str, Any], market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    GRASS/AERO/BEAT-style bounce rejection short.
    Pattern: prior weakness/selloff -> bounce upward/retest -> rejection -> short continuation.
    This is not a late dump short: it requires the current price to be above the local low,
    then to start rolling over from the local high/retest zone.
    """
    if not (ALLOW_SHORT and BOUNCE_REJECT_SHORT_ENABLED):
        return None

    # There must be a real bounce from a local low; otherwise we are shorting the bottom.
    if ctx["bounce_from_low"] < BOUNCE_MIN_FROM_LOW:
        return None
    if ctx["bounce_from_low"] > BOUNCE_MAX_FROM_LOW:
        return None

    # There must also be rejection from the recent bounce high.
    reject = max(ctx.get("reject_from_high_10m", 0.0), ctx.get("pullback_from_high", 0.0))
    if reject < BOUNCE_MIN_REJECT_FROM_HIGH:
        return None

    # Fresh downside pressure, but not already too chased.
    if ctx["ch1m3"] > -BOUNCE_MIN_1M3:
        return None
    if abs(ctx["ch1m3"]) > BOUNCE_MAX_1M3_CHASE:
        return None

    # Recent volatility must be enough for a 5-target ladder.
    if ctx["recent_range_30m"] < BOUNCE_MIN_RECENT_RANGE:
        return None

    # Tape must be alive.
    if ctx["vol1"] < BOUNCE_MIN_VOL1 or ctx["vol5"] < BOUNCE_MIN_VOL5:
        return None
    if ctx["range1"] < BOUNCE_MIN_RANGE1 or ctx["range5"] < BOUNCE_MIN_RANGE5:
        return None

    # The 1m candle must show rejection/downside control.
    if ctx["loc1"] > BOUNCE_SHORT_LOC_MAX:
        return None
    if not (ctx["last_red"] or ctx["two_red"] or ctx["break_short"]):
        return None

    # After bounce/retest, price should lose 1m EMA or VWAP, unless we have a confirmed micro-break.
    if BOUNCE_REQUIRE_CLOSE_UNDER_EMA_OR_VWAP and not (ctx.get("under_ema_or_vwap") or ctx["break_short"] or ctx["two_red"]):
        return None

    # Do not short if price is already sitting on the recent low; there must be room for TP1/TP2.
    if BOUNCE_NEAR_LOW_BLOCK and ctx["bounce_from_low"] < max(TP1_MOVE * 0.75, 0.0045):
        return None

    score = score_common(ctx, "SHORT", 76.0)
    score += min(ctx["bounce_from_low"] * 800, 7)
    score += min(reject * 1200, 8)
    if ctx.get("under_both_ema_vwap"):
        score += 4
    if ctx["break_short"]:
        score += 4
    if market.get("regime") == "BTC BEAR" or market.get("panic"):
        score += 2

    # Allow good B+ setups, but weak B setups are filtered.
    if not BOUNCE_ALLOW_B_PLUS and score < 88:
        return None

    reason = (
        "BOUNCE REJECT SHORT: отскок вверх после слабости → retest/reject → перехват вниз. "
        f"bounce from low {ctx['bounce_from_low']*100:.2f}%, reject {reject*100:.2f}%, "
        f"1m3 {ctx['ch1m3']*100:.2f}%, 15m {ctx['ch15m']*100:.2f}%, "
        f"vol1 x{ctx['vol1']:.2f}, vol5 x{ctx['vol5']:.2f}, "
        f"range1 x{ctx['range1']:.2f}, range5 x{ctx['range5']:.2f}."
    )
    return build_trade(ctx, "SHORT", "PRO_BOUNCE_REJECT_SHORT", "BOUNCE REJECT SHORT", score, reason)

def detect_market_dump_short(ctx: Dict[str, Any], market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not (ALLOW_SHORT and MARKET_DUMP_SHORT_ENABLED):
        return None
    # Not old broad dump. Requires the coin itself to be weak and live tape to confirm.
    if ctx["ch1m3"] > -DUMP_MIN_1M3:
        return None
    if ctx["ch15m"] > -DUMP_MIN_15M and ctx["ch30m"] > -DUMP_MIN_15M:
        return None
    if ctx["vol1"] < DUMP_MIN_VOL1 or ctx["vol5"] < DUMP_MIN_VOL5:
        return None
    if ctx["range1"] < DUMP_MIN_RANGE1 or ctx["range5"] < DUMP_MIN_RANGE5:
        return None
    if ctx["loc1"] > DUMP_CLOSE_SHORT:
        return None
    if not (ctx["two_red"] or ctx["break_short"]):
        return None
    score = score_common(ctx, "SHORT", 71.0)
    if market.get("panic") or market.get("regime") == "BTC BEAR":
        score += 4
    reason = (
        "MARKET DUMP SHORT V14: рыночный слив + альт слабый + подтверждённый tape вниз. "
        f"BTC {market.get('regime')}; 1m3 {ctx['ch1m3']*100:.2f}%, 15m {ctx['ch15m']*100:.2f}%, "
        f"vol1 x{ctx['vol1']:.2f}, range1 x{ctx['range1']:.2f}."
    )
    return build_trade(ctx, "SHORT", "PRO_MARKET_DUMP_SHORT_V14", "MARKET DUMP SHORT V14", score, reason)


def detect_confirmed_tape_long(ctx: Dict[str, Any], market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not (ALLOW_LONG and CONFIRMED_TAPE_ENABLED and RECOVERY_RECLAIM_LONG_ENABLED):
        return None
    # LONG must be much cleaner than short because previous long stats were weak.
    if ctx["ch1m3"] < LONG_MIN_1M3:
        return None
    if ctx["vol1"] < LONG_MIN_VOL1 or ctx["range1"] < LONG_MIN_RANGE1:
        return None
    if ctx["loc1"] < LONG_CLOSE_LONG:
        return None
    if not (ctx["break_long"] or ctx["two_green"]):
        return None
    # Must reclaim 1m EMA/VWAP; avoid buying under local trend.
    if ctx["price"] < max(ctx["ema1_9"], ctx["vwap1"]):
        return None
    # If BTC is bear, require relative strength.
    if market.get("regime") == "BTC BEAR" and ctx["ch15m"] < 0.004:
        return None
    if ctx["bounce_from_low"] < 0.0035:
        return None
    score = score_common(ctx, "LONG", 74.0)
    if ctx["ch15m"] > 0:
        score += min(ctx["ch15m"] * 900, 8)
    reason = (
        "CONFIRMED TAPE LONG: sweep/reclaim + сила против рынка + live pressure вверх. "
        f"bounce {ctx['bounce_from_low']*100:.2f}%, 1m3 {ctx['ch1m3']*100:.2f}%, vol1 x{ctx['vol1']:.2f}."
    )
    return build_trade(ctx, "LONG", "PRO_CONFIRMED_TAPE_LONG", "CONFIRMED TAPE LONG", score, reason)


def detect_aero_style_long(ctx: Dict[str, Any], market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not (ALLOW_LONG and AERO_STYLE_ENABLED):
        return None
    if ctx["ch1m3"] < LONG_MIN_1M3:
        return None
    if ctx["bounce_from_low"] < AERO_MIN_PULLBACK:
        return None
    if ctx["recent_range_30m"] < AERO_MIN_RECENT_RANGE:
        return None
    if ctx["loc1"] < LONG_CLOSE_LONG:
        return None
    if ctx["vol1"] < LONG_MIN_VOL1 or ctx["range1"] < LONG_MIN_RANGE1:
        return None
    if not (ctx["break_long"] or ctx["two_green"]):
        return None
    score = score_common(ctx, "LONG", 76.0)
    reason = (
        "AERO STYLE LONG: вынос вниз/reclaim, сильное закрытие 1m, пробой микро-структуры вверх. "
        f"1m3 {ctx['ch1m3']*100:.2f}%, 15m {ctx['ch15m']*100:.2f}%."
    )
    return build_trade(ctx, "LONG", "PRO_AERO_STYLE_LONG", "AERO STYLE LONG", score, reason)


def analyze_symbol(symbol: str, market: Dict[str, Any], blocks: Dict[str, int], near: List[str]) -> List[Dict[str, Any]]:
    if is_ultra_risk(symbol):
        blocks["ultra_risk_block"] = blocks.get("ultra_risk_block", 0) + 1
        return []
    ctx = symbol_context(symbol)
    if not ctx:
        blocks["no_candles"] = blocks.get("no_candles", 0) + 1
        return []
    candidates = []
    detectors = [
        detect_pump_trap_short,
        detect_bounce_reject_short,
        detect_beat_style_short,
        detect_aero_style_short,
        detect_market_dump_short,
        detect_confirmed_tape_long,
        detect_aero_style_long,
    ]
    for det in detectors:
        tr = det(ctx, market)
        if not tr:
            continue
        ok, why = strategy_allowed(tr["strategy"])
        if not ok:
            blocks["strategy_stats_block"] = blocks.get("strategy_stats_block", 0) + 1
            if len(near) < 10:
                near.append(f"{display_symbol(symbol)} {tr['side']}: {why}")
            continue
        ok, why = cooldown_ok(symbol, tr["strategy"])
        if not ok:
            blocks["cooldown_block"] = blocks.get("cooldown_block", 0) + 1
            continue
        if not trade_quality_ok(tr, blocks, near):
            continue
        candidates.append(tr)
    if not candidates:
        blocks["no_v14_setup"] = blocks.get("no_v14_setup", 0) + 1
    return candidates

# ============================================================
# PENDING CONFIRMATION ENGINE
# ============================================================

def setup_signature(tr: Dict[str, Any]) -> str:
    return f"{tr['symbol']}|{tr['side']}|{tr['strategy']}"


def add_pending(tr: Dict[str, Any]) -> None:
    sig = setup_signature(tr)
    with STATE_LOCK:
        arr = STATE.setdefault("pending_setups", [])
        for x in arr:
            if x.get("sig") == sig:
                x["updated_at"] = now_ts()
                x["seed"] = tr
                save_state()
                return
        arr.append({"sig": sig, "created_at": now_ts(), "updated_at": now_ts(), "seed": tr})
        # keep small
        STATE["pending_setups"] = arr[-80:]
        save_state()


def confirm_pending_setups(market: Dict[str, Any], blocks: Dict[str, int], near: List[str]) -> List[Dict[str, Any]]:
    if not CONFIRMATION_ENGINE_ENABLED:
        return []
    ready: List[Dict[str, Any]] = []
    keep: List[Dict[str, Any]] = []
    now = now_ts()
    arr = list(STATE.get("pending_setups", []))
    for item in arr:
        seed = item.get("seed", {})
        created = float(item.get("created_at", now))
        age = now - created
        if age > CONFIRM_MAX_SECONDS:
            blocks["pending_expired"] = blocks.get("pending_expired", 0) + 1
            continue
        if age < CONFIRM_MIN_SECONDS:
            keep.append(item)
            continue
        sym = seed.get("symbol")
        side = seed.get("side")
        if not sym or not side:
            continue
        ctx = symbol_context(sym)
        if not ctx:
            keep.append(item)
            continue
        entry0 = float(seed.get("entry", ctx["price"]))
        current = float(ctx["price"])
        if side == "SHORT":
            progress = safe_div(entry0 - current, entry0, 0.0)
            tape_ok = ctx["loc1"] <= CONFIRM_SHORT_LOC_MAX and (ctx["last_red"] or ctx["break_short"] or ctx["two_red"])
            not_snapback = ctx.get("ch1m1", 0.0) <= CONFIRM_MAX_OPPOSITE_1M
            micro_ok = (ctx["break_short"] or ctx["two_red"]) if CONFIRM_REQUIRE_MICRO_BREAK else True
        else:
            progress = safe_div(current - entry0, entry0, 0.0)
            tape_ok = ctx["loc1"] >= CONFIRM_LONG_LOC_MIN and (ctx["last_green"] or ctx["break_long"] or ctx["two_green"])
            not_snapback = ctx.get("ch1m1", 0.0) >= -CONFIRM_MAX_OPPOSITE_1M
            micro_ok = (ctx["break_long"] or ctx["two_green"]) if CONFIRM_REQUIRE_MICRO_BREAK else True

        # Progress is measured both as raw price move and as share of TP1.
        # This prevents sending a signal before the tape really starts moving.
        tp1_move = abs(float(seed.get("tp1", current)) - entry0) / entry0 if entry0 else 0.0
        tp1_progress = safe_div(progress, tp1_move, 0.0)
        live_ok = ctx.get("vol1", 0.0) >= CONFIRM_MIN_VOL1 and ctx.get("range1", 0.0) >= CONFIRM_MIN_RANGE1
        chase_ok = progress <= CONFIRM_MAX_CHASE and tp1_progress <= CONFIRM_MAX_TP1_PROGRESS
        confirm_ok = (
            progress >= CONFIRM_MIN_MOVE
            and tp1_progress >= CONFIRM_MIN_TP1_PROGRESS
            and chase_ok
            and tape_ok
            and live_ok
            and not_snapback
            and micro_ok
        )

        if seed.get("strategy") == "PRO_PUMP_TRAP_SHORT" and side == "SHORT":
            pump_tape_ok = (
                tp1_progress >= PUMP_CONFIRM_MIN_TP1_PROGRESS
                and ctx.get("loc1", 0.5) <= PUMP_CONFIRM_SHORT_LOC_MAX
                and (ctx.get("last_red") or ctx.get("two_red") or ctx.get("break_short"))
                and ctx.get("vol1", 0.0) >= max(CONFIRM_MIN_VOL1, PUMP_MIN_VOL1)
                and ctx.get("range1", 0.0) >= max(CONFIRM_MIN_RANGE1, PUMP_MIN_RANGE1 * 0.90)
            )
            confirm_ok = confirm_ok and pump_tape_ok

        if confirm_ok:
            # rebuild at current price, not stale entry
            rebuilt = build_trade(ctx, side, seed["strategy"], seed["type"], max(float(seed.get("score", 80)), 86.0), seed.get("reason", "") + f" Подтверждение V14.05: цена прошла {tp1_progress*100:.0f}% пути к TP1 до отправки, tape не разворачивается против входа.")
            ok, why = strategy_allowed(rebuilt["strategy"])
            if ok and trade_quality_ok(rebuilt, blocks, near):
                ready.append(rebuilt)
            else:
                blocks["pending_quality_block"] = blocks.get("pending_quality_block", 0) + 1
        else:
            if not live_ok:
                blocks["pending_live_weak"] = blocks.get("pending_live_weak", 0) + 1
            if not tape_ok or not not_snapback:
                blocks["pending_tape_reject"] = blocks.get("pending_tape_reject", 0) + 1
            if progress > CONFIRM_MAX_CHASE or tp1_progress > CONFIRM_MAX_TP1_PROGRESS:
                blocks["pending_chase_block"] = blocks.get("pending_chase_block", 0) + 1
            keep.append(item)
    with STATE_LOCK:
        STATE["pending_setups"] = keep[-80:]
        save_state()
    return ready


def should_send_immediately(tr: Dict[str, Any]) -> bool:
    # V14.02: by default every setup must pass pending confirmation.
    # This reduces "entered and instantly stopped" trades.
    if not ALLOW_IMMEDIATE_SEND:
        return False
    if tr["score"] < IMMEDIATE_A_PLUS_SCORE:
        return False
    c = tr.get("ctx", {})
    if tr["side"] == "SHORT":
        return c.get("loc1", 0.5) <= 0.25 and c.get("range1", 0) >= 1.45 and c.get("vol1", 0) >= 0.95
    return c.get("loc1", 0.5) >= 0.75 and c.get("range1", 0) >= 1.45 and c.get("vol1", 0) >= 1.05

# ============================================================
# TELEGRAM / SIGNALS
# ============================================================

def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        STATE["last_error"] = "Telegram env missing"
        save_state()
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        r = SESSION.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=HTTP_TIMEOUT)
        if r.status_code >= 400:
            STATE["last_error"] = f"telegram {r.status_code}: {r.text[:200]}"
            save_state()
            return False
        return True
    except Exception as e:
        STATE["last_error"] = f"telegram: {repr(e)}"
        save_state()
        return False


def build_signal_message(tr: Dict[str, Any], market: Dict[str, Any]) -> str:
    emoji = "🔴" if tr["side"] == "SHORT" else "🟢"
    risk = tr["risk"]
    tps = tr["tps"]
    c = tr.get("ctx", {})
    return (
        f"{emoji} <b>{tr['side']} {display_symbol(tr['symbol'])}</b>\n"
        f"Класс: <b>{tr['class']}</b> · Score {tr['score']:.1f} · {tr['type']}\n"
        f"Стратегия: <b>{tr['strategy']}</b>\n\n"
        f"Вход: <b>{fmt_price(tr['entry'])}</b>\n"
        f"TP1: {fmt_price(tps[0])} · ≈ {risk['roi_tp1']:.1f}% ROI x{LEVERAGE_DISPLAY}\n"
        f"TP2: {fmt_price(tps[1])}\n"
        f"TP3: {fmt_price(tps[2])}\n"
        f"TP4: {fmt_price(tps[3])}\n"
        f"TP5: {fmt_price(tps[4])}\n"
        f"SL: <b>{fmt_price(tr['sl'])}</b> · риск ≈ {risk['roi_sl']:.1f}% ROI x{LEVERAGE_DISPLAY}\n"
        f"RR TP1: {risk['tp1_rr']:.2f} · Ladder RR: {risk['ladder_rr']:.2f} · Final RR: {risk['final_rr']:.2f}\n\n"
        f"📌 <b>Логика:</b>\n{tr['reason']}\n"
        f"1m3: {c.get('ch1m3', 0)*100:+.2f}% · 15m: {c.get('ch15m', 0)*100:+.2f}% · 30m: {c.get('ch30m', 0)*100:+.2f}%\n"
        f"Vol1 x{c.get('vol1', 0):.2f} · Vol5 x{c.get('vol5', 0):.2f} · Range1 x{c.get('range1', 0):.2f} · Range5 x{c.get('range5', 0):.2f}\n"
        f"BTC: {market.get('text', 'unknown')}\n\n"
        f"⏱ Правило V14: если за {FAST_MAX_MINUTES_TO_TP1} минут нет движения к TP1 — сигнал expired. "
        f"Старый Instant Edge отключён."
    )


def add_active_signal(tr: Dict[str, Any]) -> None:
    with STATE_LOCK:
        arr = STATE.setdefault("active_signals", [])
        arr.append(tr)
        STATE["active_signals"] = arr[-20:]
        set_cooldown(tr["symbol"], tr["strategy"])
        save_state()

# ============================================================
# TRACKING
# ============================================================

def current_price(symbol: str) -> Optional[float]:
    try:
        c1 = klines(symbol, "1m", 3)
        if c1:
            return float(c1[-1]["c"])
    except Exception:
        return None
    return None


def progress_to_tp1(tr: Dict[str, Any], price: float) -> float:
    entry = tr["entry"]
    tp1 = tr["tp1"]
    if tr["side"] == "SHORT":
        return safe_div(entry - price, entry - tp1, 0.0)
    return safe_div(price - entry, tp1 - entry, 0.0)


def adverse_sl_fraction(tr: Dict[str, Any], price: float) -> float:
    entry = float(tr["entry"])
    sl = float(tr["sl"])
    if tr["side"] == "SHORT":
        return max(0.0, safe_div(price - entry, sl - entry, 0.0))
    return max(0.0, safe_div(entry - price, entry - sl, 0.0))


def early_invalidation_reason(tr: Dict[str, Any], price: float, age_min: float) -> Optional[str]:
    if not EARLY_INVALIDATION_ENABLED:
        return None
    if age_min * 60.0 < EARLY_INVALIDATION_SECONDS:
        return None
    if float(tr.get("max_progress", 0.0)) > EARLY_INVALIDATION_MAX_PROGRESS:
        return None
    frac = adverse_sl_fraction(tr, price)
    if frac >= EARLY_INVALIDATION_ADVERSE_SL_FRACTION and frac < 1.0:
        return f"движение против входа {frac*100:.0f}% пути до SL, прогресс к TP1 слабый"
    return None


def track_active_signals() -> None:
    now = now_ts()
    with STATE_LOCK:
        active = list(STATE.get("active_signals", []))
    keep = []
    for tr in active:
        sym = tr["symbol"]
        px = current_price(sym)
        if px is None:
            keep.append(tr)
            continue
        age_min = (now - float(tr.get("created_at", now))) / 60.0
        side = tr["side"]
        hit_tp1 = px <= tr["tp1"] if side == "SHORT" else px >= tr["tp1"]
        hit_sl = px >= tr["sl"] if side == "SHORT" else px <= tr["sl"]
        prog = max(0.0, min(progress_to_tp1(tr, px), 2.0))
        tr["max_progress"] = max(float(tr.get("max_progress", 0.0)), prog)
        if hit_tp1:
            record_outcome(tr, "profit")
            send_telegram(
                f"✅ <b>TAKE PROFIT / TP1</b>\n{tr['class']} · {tr['side']} {display_symbol(sym)}\n"
                f"Стратегия: {tr['strategy']}\nВход: {fmt_price(tr['entry'])}\nTP1: {fmt_price(tr['tp1'])}\nТекущая цена: {fmt_price(px)}"
                + stats_summary_text()
            )
            continue
        if hit_sl:
            record_outcome(tr, "sl")
            send_telegram(
                f"❌ <b>Stop Loss</b>\n{tr['class']} · {tr['side']} {display_symbol(sym)}\n"
                f"Стратегия: {tr['strategy']}\nВход: {fmt_price(tr['entry'])}\nSL: {fmt_price(tr['sl'])}\nТекущая цена: {fmt_price(px)}"
                + stats_summary_text()
            )
            continue
        early_reason = early_invalidation_reason(tr, px, age_min)
        if early_reason:
            record_outcome(tr, "early_exit")
            send_telegram(
                f"🟠 <b>EARLY INVALIDATION / выйти раньше SL</b>\n{tr['class']} · {tr['side']} {display_symbol(sym)}\n"
                f"Стратегия: {tr['strategy']}\nПричина: {early_reason}\n"
                f"Вход: {fmt_price(tr['entry'])}\nТекущая цена: {fmt_price(px)}\nSL: {fmt_price(tr['sl'])}"
                + stats_summary_text()
            )
            continue
        if age_min >= FAST_MAX_MINUTES_TO_TP1 and tr.get("max_progress", 0.0) < FAST_MIN_PROGRESS_TO_KEEP:
            record_outcome(tr, "expired")
            send_telegram(
                f"⏱ <b>FAST TRADE EXPIRED</b>\n{tr['class']} · {tr['side']} {display_symbol(sym)}\n"
                f"Стратегия: {tr['strategy']}\nЦена не реализовалась за {FAST_MAX_MINUTES_TO_TP1} минут.\n"
                f"Вход: {fmt_price(tr['entry'])}\nТекущая цена: {fmt_price(px)}\nTP1: {fmt_price(tr['tp1'])}\n"
                f"Прогресс к TP1: {tr.get('max_progress', 0.0)*100:.1f}%"
                + stats_summary_text()
            )
            continue
        if age_min >= FAST_HARD_EXPIRE_MINUTES:
            record_outcome(tr, "expired")
            send_telegram(
                f"⏱ <b>HARD EXPIRED</b>\n{tr['class']} · {tr['side']} {display_symbol(sym)}\n"
                f"Стратегия: {tr['strategy']}\nВход: {fmt_price(tr['entry'])}\nТекущая цена: {fmt_price(px)}"
                + stats_summary_text()
            )
            continue
        keep.append(tr)
    with STATE_LOCK:
        STATE["active_signals"] = keep
        STATE["last_track_at"] = now
        save_state()

# ============================================================
# SCAN
# ============================================================

def run_scan() -> Dict[str, Any]:
    start = now_ts()
    blocks: Dict[str, int] = {}
    near: List[str] = []
    sent = 0
    candidates: List[Dict[str, Any]] = []
    hot_notes: List[str] = []

    try:
        track_active_signals()
        market = market_context()
        # Confirm existing pending setups first. This is the professional layer.
        confirmed = confirm_pending_setups(market, blocks, near)
        candidates.extend(confirmed)

        symbols = bingx_symbols()
        if not symbols:
            raise RuntimeError("empty symbols")

        selected, hot_notes = select_hot_symbols(symbols)
        checked = 0
        for sym in selected:
            checked += 1
            # Even if active slot full, continue diagnostics only lightly.
            found = analyze_symbol(sym, market, blocks, near)
            for tr in found:
                if should_send_immediately(tr):
                    candidates.append(tr)
                elif CONFIRMATION_ENGINE_ENABLED:
                    add_pending(tr)
                    blocks["pending_wait_confirmation"] = blocks.get("pending_wait_confirmation", 0) + 1
                else:
                    candidates.append(tr)

        # Sort by score and quality.
        candidates.sort(key=lambda x: (x.get("score", 0), x.get("risk", {}).get("final_rr", 0)), reverse=True)

        with STATE_LOCK:
            active_count = len(STATE.get("active_signals", []))
        free_slots = max(0, MAX_ACTIVE_SIGNALS - active_count)
        send_limit = min(MAX_SIGNALS_PER_SCAN, free_slots, len(candidates))
        if send_limit <= 0 and candidates:
            blocks["active_slots_full_send_block"] = blocks.get("active_slots_full_send_block", 0) + 1
        for tr in candidates[:send_limit]:
            if send_telegram(build_signal_message(tr, market)):
                add_active_signal(tr)
                sent += 1

        diag = {
            "title": "Диагностика V14.05 MTF Structure Scalper",
            "checked": checked,
            "universe": len(symbols),
            "coins_in_analysis": min(COINS_IN_ANALYSIS_TARGET, len(selected)),
            "pending_setups": len(STATE.get("pending_setups", [])),
            "active_signals": len(STATE.get("active_signals", [])),
            "candidates": len(candidates),
            "sent": sent,
            "seconds": round(now_ts() - start, 1),
            "btc": market.get("text", "unknown"),
            "hot_symbols": hot_notes,
            "blocks": dict(sorted(blocks.items(), key=lambda x: x[1], reverse=True)),
            "near_miss": near[:10],
            "last_error": STATE.get("last_error", ""),
        }
        with STATE_LOCK:
            STATE["last_scan"] = diag
            STATE["last_scan_at"] = now_ts()
            save_state()
        return diag
    except Exception as e:
        STATE["last_error"] = f"scan: {repr(e)}"
        save_state()
        return {"error": repr(e), "last_error": STATE.get("last_error", "")}


def format_diag(diag: Dict[str, Any]) -> str:
    if "error" in diag:
        return f"❌ Ошибка scan: {diag['error']}"
    lines = []
    lines.append(f"🧪 {diag.get('title', 'Диагностика')}" )
    lines.append(f"Проверено: {diag.get('checked', 0)} из universe {diag.get('universe', 0)}")
    lines.append(f"Монет в анализе: {diag.get('coins_in_analysis', 0)} · pending: {diag.get('pending_setups', 0)} · active: {diag.get('active_signals', 0)}")
    lines.append(f"Кандидатов: {diag.get('candidates', 0)} · отправлено: {diag.get('sent', 0)} · время: {diag.get('seconds', 0)}с")
    lines.append(f"BTC: {diag.get('btc', '')}")
    lines.append("")
    lines.append("Hot symbols:")
    for x in diag.get("hot_symbols", []):
        lines.append(x)
    lines.append("")
    lines.append("Главные блокировки:")
    blocks = diag.get("blocks", {})
    if blocks:
        for k, v in blocks.items():
            lines.append(f"{k}: {v}")
    else:
        lines.append("—")
    near = diag.get("near_miss", [])
    if near:
        lines.append("")
        lines.append("Почти прошли:")
        for x in near:
            lines.append(x)
    lines.append("")
    lines.append(f"Last error: {diag.get('last_error', '')}")
    return "\n".join(lines)

# ============================================================
# LOOPS
# ============================================================

_STOP = False



def format_telegram_scan_report(diag: Dict[str, Any]) -> str:
    """Compact Telegram status update: shows scan activity without spamming full diagnostics."""
    if "error" in diag:
        return f"⚠️ V14 scan error\n{diag.get('error')}\nLast error: {diag.get('last_error', '')}"

    blocks = diag.get("blocks", {}) or {}
    top_blocks = ", ".join([f"{k}:{v}" for k, v in list(blocks.items())[:5]]) or "нет"

    total_item = STATE.get("stats", {}).get("total", empty_stat())
    _, total_wr = item_wr(total_item)
    lines = [
        f"🧪 V14 scan update",
        f"{APP_VERSION}",
        f"Проверено: {diag.get('checked', 0)} из {diag.get('universe', 0)} · монет в анализе: {diag.get('coins_in_analysis', 0)} · кандидатов: {diag.get('candidates', 0)} · отправлено: {diag.get('sent', 0)} · {diag.get('seconds', 0)}с",
        f"Active: {diag.get('active_signals', 0)} · Pending confirmation: {diag.get('pending_setups', 0)}",
        f"BTC: {diag.get('btc', '')}",
        f"Stats: {int(total_item.get('profit',0))} profit / {int(total_item.get('sl',0))} SL / {int(total_item.get('expired',0))} expired / {int(total_item.get('early_exit',0))} early · WR {total_wr:.1f}%",
        f"Блокировки: {top_blocks}",
    ]
    if TELEGRAM_SCAN_REPORT_INCLUDE_HOT:
        hot = diag.get("hot_symbols", [])[:5]
        if hot:
            lines.append("")
            lines.append("Hot symbols:")
            lines.extend(hot)
    near = diag.get("near_miss", [])[:4]
    if near:
        lines.append("")
        lines.append("Почти прошли:")
        lines.extend(near)
    return "\n".join(lines)


def maybe_send_scan_report(diag: Dict[str, Any]) -> None:
    if not TELEGRAM_SCAN_REPORTS_ENABLED:
        return
    now = now_ts()
    with STATE_LOCK:
        last = float(STATE.get("last_scan_report_at", 0) or 0)
    if now - last < max(60, TELEGRAM_SCAN_REPORT_EVERY_SECONDS):
        return
    if send_telegram(format_telegram_scan_report(diag)):
        with STATE_LOCK:
            STATE["last_scan_report_at"] = now
            save_state()


def scan_loop() -> None:
    while not _STOP:
        try:
            if AUTO_SCAN_ENABLED:
                diag = run_scan()
                maybe_send_scan_report(diag)
        except Exception as e:
            STATE["last_error"] = f"scan_loop: {repr(e)}"
            save_state()
        time.sleep(max(5, AUTO_SCAN_SECONDS))


def track_loop() -> None:
    while not _STOP:
        try:
            track_active_signals()
        except Exception as e:
            STATE["last_error"] = f"track_loop: {repr(e)}"
            save_state()
        time.sleep(max(2, AUTO_TRACK_SECONDS))

@app.on_event("startup")
def startup_event() -> None:
    if SEND_STARTUP_TELEGRAM:
        send_telegram(
            "✅ Bot activated\n"
            f"{APP_VERSION}\n"
            f"{DEPLOY_MARKER}\n"
            f"Scan: every {AUTO_SCAN_SECONDS}s · Track: every {AUTO_TRACK_SECONDS}s\n"
            f"Max active: {MAX_ACTIVE_SIGNALS} · Max per scan: {MAX_SIGNALS_PER_SCAN}\n"
            f"Confirmation engine: {CONFIRMATION_ENGINE_ENABLED}\n"
            f"Instant Edge disabled: {INSTANT_EDGE_HARD_DISABLED}\n"
            f"Bounce Reject SHORT: {BOUNCE_REJECT_SHORT_ENABLED}\n"
            f"PUMP Trap SHORT: {PUMP_TRAP_SHORT_ENABLED}\n"
            f"Immediate send: {ALLOW_IMMEDIATE_SEND} · Early invalidation: {EARLY_INVALIDATION_ENABLED}"
        )
    threading.Thread(target=scan_loop, daemon=True).start()
    threading.Thread(target=track_loop, daemon=True).start()

# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/")
def root() -> Dict[str, Any]:
    return {"app": APP_NAME, "version": APP_VERSION, "deploy_marker": DEPLOY_MARKER}


@app.get("/version")
def version() -> Dict[str, Any]:
    return {
        "app": APP_NAME,
        "version": APP_VERSION,
        "deploy_marker": DEPLOY_MARKER,
        "instant_edge_enabled": INSTANT_EDGE_ENABLED,
        "confirmation_engine": CONFIRMATION_ENGINE_ENABLED,
        "state_file": STATE_FILE,
        "send_startup_telegram": SEND_STARTUP_TELEGRAM,
        "telegram_scan_reports_enabled": TELEGRAM_SCAN_REPORTS_ENABLED,
        "telegram_scan_report_every_seconds": TELEGRAM_SCAN_REPORT_EVERY_SECONDS,
        "allow_immediate_send": ALLOW_IMMEDIATE_SEND,
        "early_invalidation_enabled": EARLY_INVALIDATION_ENABLED,
        "confirm_min_tp1_progress": CONFIRM_MIN_TP1_PROGRESS,
        "pump_trap_short_enabled": PUMP_TRAP_SHORT_ENABLED,
        "pro_mtf_filter_enabled": PRO_MTF_FILTER_ENABLED,
        "pro_mtf_min_confirmations": PRO_MTF_MIN_CONFIRMATIONS,
        "coins_in_analysis_target": COINS_IN_ANALYSIS_TARGET,
        "tp_moves": TP_MOVES,
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "version": APP_VERSION,
        "active_signals": len(STATE.get("active_signals", [])),
        "pending_setups": len(STATE.get("pending_setups", [])),
        "last_error": STATE.get("last_error", ""),
        "telegram_scan_reports_enabled": TELEGRAM_SCAN_REPORTS_ENABLED,
        "telegram_scan_report_every_seconds": TELEGRAM_SCAN_REPORT_EVERY_SECONDS,
        "last_scan_report_at": STATE.get("last_scan_report_at", 0),
    }


@app.get("/scan")
def scan_endpoint() -> str:
    diag = run_scan()
    return format_diag(diag)


@app.get("/auto-status")
def auto_status() -> Dict[str, Any]:
    return {
        "version": APP_VERSION,
        "auto_scan_enabled": AUTO_SCAN_ENABLED,
        "auto_scan_seconds": AUTO_SCAN_SECONDS,
        "auto_track_seconds": AUTO_TRACK_SECONDS,
        "active_signals": STATE.get("active_signals", []),
        "pending_setups_count": len(STATE.get("pending_setups", [])),
        "last_scan": STATE.get("last_scan", {}),
        "last_error": STATE.get("last_error", ""),
    }


@app.get("/stats")
def stats_endpoint() -> Dict[str, Any]:
    return STATE.get("stats", {})


@app.get("/stats-text")
def stats_text_endpoint() -> str:
    return stats_summary_text()


@app.get("/test-telegram")
def test_telegram() -> Dict[str, Any]:
    ok = send_telegram(f"✅ Test Telegram OK\n{APP_VERSION}\n{DEPLOY_MARKER}")
    return {"ok": ok, "last_error": STATE.get("last_error", "")}


@app.get("/reset-state")
def reset_state() -> Dict[str, Any]:
    # Simple endpoint for tests. Remove if you do not want remote reset.
    global STATE
    STATE = default_state()
    save_state()
    return {"ok": True, "version": APP_VERSION}

# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":
    import uvicorn
    port = env_int("PORT", 8000)
    uvicorn.run(app, host="0.0.0.0", port=port)
