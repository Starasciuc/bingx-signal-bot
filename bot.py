"""
BingX Extreme Bounce Signal Bot for Render
Start command: uvicorn bot:app --host 0.0.0.0 --port $PORT

Signal bot only. It never opens trades.
Core logic:
- Only extreme moves: strong pump exhaustion SHORT or strong dump capitulation bounce LONG.
- Waits for confirmation before Telegram signal.
- Tracks TP/average/stop/expired and keeps statistics.
"""

import asyncio
import json
import logging
import math
import os
import tempfile
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse

APP_NAME = "BingX Extreme Bounce Signal Bot"
BOT_VERSION = "V1.0_EXTREME_BOUNCE_ONLY_WORKING_2026_07_09"
BINGX_BASE_URL = os.getenv("BINGX_BASE_URL", "https://open-api.bingx.com").rstrip("/")
STATE_FILE = os.getenv("STATE_FILE", "bot_state_extreme_bounce.json")

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(APP_NAME)

# -------------------------
# ENV helpers
# -------------------------

def first_env(names: List[str], default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return default


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
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_list(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


TELEGRAM_BOT_TOKEN = first_env(["TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "BOT_TOKEN", "TG_BOT_TOKEN"])
TELEGRAM_CHAT_ID = first_env(["TELEGRAM_CHAT_ID", "TELEGRAM_CHANNEL_ID", "TG_CHAT_ID", "CHAT_ID"])

# Scan settings
SCAN_INTERVAL_SECONDS = max(20, env_int("SCAN_INTERVAL_SECONDS", 45))
TRACK_INTERVAL_SECONDS = max(2, env_int("TRACK_INTERVAL_SECONDS", 4))
CONTRACTS_CACHE_SECONDS = max(60, env_int("CONTRACTS_CACHE_SECONDS", 600))
KLINE_CACHE_SECONDS = max(3, env_int("KLINE_CACHE_SECONDS", 10))
MAX_CONTRACTS = max(50, env_int("MAX_CONTRACTS", 350))
HOT_SCAN_SYMBOLS = max(30, env_int("HOT_SCAN_SYMBOLS", 180))
HOT_SYMBOLS_TO_ANALYZE = max(10, env_int("HOT_SYMBOLS_TO_ANALYZE", 45))
CONCURRENCY = max(2, env_int("CONCURRENCY", 8))
REQUEST_TIMEOUT = max(5, env_int("REQUEST_TIMEOUT", 15))

# Signal limits
MIN_SCORE = env_int("MIN_SCORE", 72)
FALLBACK_SCORE = env_int("FALLBACK_SCORE", 66)
DAILY_MIN_SIGNALS = max(0, env_int("DAILY_MIN_SIGNALS", 1))
MAX_SIGNALS_PER_DAY = max(1, env_int("MAX_SIGNALS_PER_DAY", 5))
MAX_ACTIVE_SIGNALS = max(1, env_int("MAX_ACTIVE_SIGNALS", 1))
MAX_SIGNALS_PER_SCAN = max(1, env_int("MAX_SIGNALS_PER_SCAN", 1))
COOLDOWN_MINUTES = max(10, env_int("COOLDOWN_MINUTES", 180))

# Telegram/reporting
SEND_STARTUP_MESSAGE = env_bool("SEND_STARTUP_MESSAGE", True)
SEND_STATS_AFTER_CLOSE = env_bool("SEND_STATS_AFTER_CLOSE", True)
SEND_SCAN_ERRORS_TO_TELEGRAM = env_bool("SEND_SCAN_ERRORS_TO_TELEGRAM", False)
SEND_EMPTY_SCAN = env_bool("SEND_EMPTY_SCAN", True)
EMPTY_SCAN_EVERY_SECONDS = max(60, env_int("EMPTY_SCAN_EVERY_SECONDS", 300))

# Trade plan
LEVERAGE = max(1, env_int("LEVERAGE", 10))
TAKE_PCTS = [float(x.strip()) / 100.0 for x in env_str("TAKE_PCTS", "0.85,1.45,2.05,3.05,4.10").split(",") if x.strip()]
if len(TAKE_PCTS) < 5:
    TAKE_PCTS = [0.0085, 0.0145, 0.0205, 0.0305, 0.0410]
AVERAGE_PCT = env_float("AVERAGE_PCT", 7.7) / 100.0
STOP_AFTER_AVERAGE_PCT = env_float("STOP_AFTER_AVERAGE_PCT", 10.5) / 100.0
MIN_PROFIT_TARGET_INDEX = min(5, max(1, env_int("MIN_PROFIT_TARGET_INDEX", 3)))

# Extreme bounce logic thresholds, in percent points (not decimals)
# SHORT: strong pump -> high failure -> rejection down.
SHORT_MIN_PUMP_1H = env_float("SHORT_MIN_PUMP_1H", 12.0)
SHORT_MIN_PUMP_3H = env_float("SHORT_MIN_PUMP_3H", 18.0)
SHORT_MIN_PUMP_6H = env_float("SHORT_MIN_PUMP_6H", 24.0)
SHORT_MIN_REJECT_15M = env_float("SHORT_MIN_REJECT_15M", -0.35)
SHORT_MIN_REJECT_5M = env_float("SHORT_MIN_REJECT_5M", -0.20)
SHORT_MAX_DIST_FROM_HIGH = env_float("SHORT_MAX_DIST_FROM_HIGH", 4.5)
SHORT_RSI_MIN = env_float("SHORT_RSI_MIN", 58.0)

# LONG: strong dump -> low failure -> bounce up.
LONG_MIN_DUMP_1H = env_float("LONG_MIN_DUMP_1H", -12.0)
LONG_MIN_DUMP_3H = env_float("LONG_MIN_DUMP_3H", -22.0)
LONG_MIN_DUMP_6H = env_float("LONG_MIN_DUMP_6H", -30.0)
LONG_MIN_BOUNCE_15M = env_float("LONG_MIN_BOUNCE_15M", 0.45)
LONG_MIN_BOUNCE_5M = env_float("LONG_MIN_BOUNCE_5M", 0.20)
LONG_MAX_DIST_FROM_LOW = env_float("LONG_MAX_DIST_FROM_LOW", 5.0)
LONG_RSI_MAX = env_float("LONG_RSI_MAX", 42.0)

# Participation / MTF filters
MIN_VOLR_5M = env_float("MIN_VOLR_5M", 0.55)
MIN_VOLR_15M = env_float("MIN_VOLR_15M", 0.60)
MIN_RANGE_5M = env_float("MIN_RANGE_5M", 0.45)
PHOTO_MTF_FILTER_ENABLED = env_bool("PHOTO_MTF_FILTER_ENABLED", True)
PHOTO_MTF_MIN_CONFIRMATIONS = env_float("PHOTO_MTF_MIN_CONFIRMATIONS", 2.0)
PHOTO_MTF_BLOCK_BAD_DELTA = env_bool("PHOTO_MTF_BLOCK_BAD_DELTA", False)

# Confirmation before send
CONFIRM_BEFORE_SEND_ENABLED = env_bool("CONFIRM_BEFORE_SEND_ENABLED", True)
CONFIRM_MIN_SECONDS = max(5, env_int("CONFIRM_MIN_SECONDS", 18))
CONFIRM_MAX_SECONDS = max(CONFIRM_MIN_SECONDS + 5, env_int("CONFIRM_MAX_SECONDS", 150))
CONFIRM_MIN_TP1_PROGRESS = env_float("CONFIRM_MIN_TP1_PROGRESS", 0.08)
CONFIRM_MAX_TP1_PROGRESS = env_float("CONFIRM_MAX_TP1_PROGRESS", 0.88)
CONFIRM_MAX_ADVERSE_SL_FRACTION = env_float("CONFIRM_MAX_ADVERSE_SL_FRACTION", 0.28)
CONFIRM_REQUIRE_CANDLE_DIRECTION = env_bool("CONFIRM_REQUIRE_CANDLE_DIRECTION", True)
POST_CONFIRM_HOLD_ENABLED = env_bool("POST_CONFIRM_HOLD_ENABLED", True)
POST_CONFIRM_HOLD_SECONDS = max(0, env_int("POST_CONFIRM_HOLD_SECONDS", 20))
POST_CONFIRM_MAX_SNAPBACK = env_float("POST_CONFIRM_MAX_SNAPBACK", 0.25)

# Active trade management
MAX_MINUTES_TO_TP1 = max(3, env_int("MAX_MINUTES_TO_TP1", 18))
HARD_EXPIRE_MINUTES = max(MAX_MINUTES_TO_TP1 + 2, env_int("HARD_EXPIRE_MINUTES", 35))
MIN_PROGRESS_TO_KEEP = env_float("MIN_PROGRESS_TO_KEEP", 0.12)
EARLY_INVALIDATION_ENABLED = env_bool("EARLY_INVALIDATION_ENABLED", True)
EARLY_INVALIDATION_SECONDS = max(20, env_int("EARLY_INVALIDATION_SECONDS", 90))
EARLY_INVALIDATION_ADVERSE_SL_FRACTION = env_float("EARLY_INVALIDATION_ADVERSE_SL_FRACTION", 0.55)
EARLY_INVALIDATION_MAX_PROGRESS = env_float("EARLY_INVALIDATION_MAX_PROGRESS", 0.12)

# Risk control
CIRCUIT_BREAKER_ENABLED = env_bool("CIRCUIT_BREAKER_ENABLED", True)
MAX_CONSECUTIVE_LOSSES = max(1, env_int("MAX_CONSECUTIVE_LOSSES", 2))
MAX_DAILY_LOSSES = max(1, env_int("MAX_DAILY_LOSSES", 4))
CIRCUIT_PAUSE_MINUTES = max(15, env_int("CIRCUIT_PAUSE_MINUTES", 120))

EXCLUDED_BASES = set(env_list("EXCLUDED_BASES", "BTC,ETH,USDC,FDUSD,TUSD,DAI,USDE,USDP,USTC,BUSD"))
ULTRA_RISK_KEYWORDS = set(env_list("ULTRA_RISK_KEYWORDS", "1000,PEPE,BONK,WIF,MEME,DOGS,CATI,HMSTR,GOBLIN,MOG,TURBO,BOME,NEIRO,PNUT,MOODENG,ACT,GOAT,FIGHT,BLEND,MAGMA"))

# -------------------------
# Global app state
# -------------------------
app = FastAPI(title=APP_NAME)
started_at = time.time()
http_session: Optional[aiohttp.ClientSession] = None
scanner_task: Optional[asyncio.Task] = None
tracker_task: Optional[asyncio.Task] = None
last_scan_summary: Dict[str, Any] = {}
last_telegram_status: Dict[str, Any] = {"ok": None, "time_utc": None, "http_status": None, "error": None, "response": None}
contracts_cache: Tuple[float, List[str]] = (0.0, [])
kline_cache: Dict[str, Tuple[float, List[List[float]]]] = {}
scan_lock = asyncio.Lock()
state_lock = asyncio.Lock()
last_empty_scan_sent_ts = 0.0

# -------------------------
# Dataclasses / State
# -------------------------
@dataclass
class Signal:
    symbol: str
    display_symbol: str
    base: str
    side: str
    setup: str
    quality: int
    entry: float
    targets: List[float]
    average: float
    stop: float
    leverage: int
    score_reasons: List[str]
    metrics: Dict[str, Any]
    created_ts: float = field(default_factory=time.time)
    hit_targets: List[int] = field(default_factory=list)
    average_hit: bool = False
    closed: bool = False
    realized_profit: bool = False


@dataclass
class PendingSignal:
    signal: Dict[str, Any]
    first_seen_ts: float = field(default_factory=time.time)
    confirmed_ts: float = 0.0
    best_progress: float = 0.0


def default_stats() -> Dict[str, Any]:
    return {
        "total_signals_sent": 0,
        "pending_created": 0,
        "pending_confirmed": 0,
        "pending_expired": 0,
        "pending_rejected": 0,
        "closed_total": 0,
        "closed_profit": 0,
        "closed_loss": 0,
        "expired": 0,
        "early_exit": 0,
        "tp1_hits": 0,
        "tp2_hits": 0,
        "tp3_hits": 0,
        "tp4_hits": 0,
        "tp5_hits": 0,
        "all_targets": 0,
        "stop_losses": 0,
        "average_hits": 0,
        "total_closed_roi": 0.0,
        "best_roi": None,
        "worst_roi": None,
        "consecutive_losses": 0,
        "daily_losses": 0,
        "daily_key": "",
        "pause_until": 0.0,
        "strategy": {},
        "side": {},
        "last_results": [],
    }


@dataclass
class BotState:
    day: str = ""
    signals_today: int = 0
    last_signal_ts: float = 0.0
    cooldowns: Dict[str, float] = field(default_factory=dict)
    active: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    pending: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    stats: Dict[str, Any] = field(default_factory=default_stats)


state = BotState()

# -------------------------
# State helpers
# -------------------------

def today_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def ensure_stats() -> None:
    base = default_stats()
    if not isinstance(state.stats, dict):
        state.stats = base
    for key, value in base.items():
        state.stats.setdefault(key, value)
    if not isinstance(state.stats.get("last_results"), list):
        state.stats["last_results"] = []
    state.stats["last_results"] = state.stats["last_results"][-40:]
    if state.stats.get("daily_key") != today_key():
        state.stats["daily_key"] = today_key()
        state.stats["daily_losses"] = 0


def reset_daily_if_needed() -> None:
    today = today_key()
    if state.day != today:
        state.day = today
        state.signals_today = 0
    ensure_stats()


def load_state() -> None:
    global state
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            state = BotState(
                day=raw.get("day", ""),
                signals_today=int(raw.get("signals_today", 0) or 0),
                last_signal_ts=float(raw.get("last_signal_ts", 0.0) or 0.0),
                cooldowns=raw.get("cooldowns", {}) if isinstance(raw.get("cooldowns", {}), dict) else {},
                active=raw.get("active", {}) if isinstance(raw.get("active", {}), dict) else {},
                pending=raw.get("pending", {}) if isinstance(raw.get("pending", {}), dict) else {},
                stats=raw.get("stats", default_stats()) if isinstance(raw.get("stats", {}), dict) else default_stats(),
            )
            ensure_stats()
            reset_daily_if_needed()
            logger.info("State loaded")
        else:
            state = BotState(day=today_key())
            ensure_stats()
    except Exception as e:
        logger.warning("Could not load state: %s", e)
        state = BotState(day=today_key())
        ensure_stats()


def save_state() -> None:
    try:
        payload = json.dumps(asdict(state), ensure_ascii=False, indent=2)
        directory = os.path.dirname(os.path.abspath(STATE_FILE)) or "."
        fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".state_", suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp_path, STATE_FILE)
    except Exception as e:
        logger.warning("Could not save state: %s", e)


def stat_inc(key: str, amount: int = 1) -> None:
    ensure_stats()
    state.stats[key] = int(state.stats.get(key, 0) or 0) + amount


def nested_stat(bucket: str, key: str, result: str) -> None:
    ensure_stats()
    d = state.stats.setdefault(bucket, {})
    item = d.setdefault(key, {"profit": 0, "loss": 0, "expired": 0, "early": 0})
    item[result] = int(item.get(result, 0) or 0) + 1


# -------------------------
# Numeric / formatting helpers
# -------------------------

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


def pct_change(new: float, old: float) -> float:
    old = safe_float(old)
    new = safe_float(new)
    if old == 0:
        return 0.0
    return (new / old - 1.0) * 100.0


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def fmt_price(price: float) -> str:
    price = safe_float(price)
    if price <= 0:
        return "-"
    if price >= 1000:
        return f"{price:.2f}"
    if price >= 100:
        return f"{price:.3f}"
    if price >= 10:
        return f"{price:.4f}"
    if price >= 1:
        return f"{price:.5f}".rstrip("0").rstrip(".")
    if price >= 0.1:
        return f"{price:.5f}".rstrip("0").rstrip(".")
    if price >= 0.01:
        return f"{price:.6f}".rstrip("0").rstrip(".")
    if price >= 0.001:
        return f"{price:.7f}".rstrip("0").rstrip(".")
    return f"{price:.10f}".rstrip("0").rstrip(".")


def fmt_pct(value: float) -> str:
    value = safe_float(value)
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def display_symbol(symbol: str) -> str:
    return symbol.replace("-", "/").replace(":USDT", "")


def base_from_symbol(symbol: str) -> str:
    return symbol.replace(":USDT", "").replace("/", "-").split("-")[0].upper()


# -------------------------
# Telegram
# -------------------------
async def get_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None or http_session.closed:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        http_session = aiohttp.ClientSession(timeout=timeout)
    return http_session


async def send_telegram(text: str, silent: bool = False) -> bool:
    global last_telegram_status
    last_telegram_status = {"ok": None, "time_utc": datetime.now(timezone.utc).isoformat(), "http_status": None, "error": None, "response": None}
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        msg = "Telegram env missing: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID"
        last_telegram_status.update({"ok": False, "error": msg})
        logger.warning(msg)
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks: List[str] = []
    text = str(text)
    while len(text) > 3900:
        cut = text.rfind("\n", 0, 3900)
        cut = cut if cut > 500 else 3900
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")
    chunks.append(text)

    ok_all = True
    session = await get_session()
    for chunk in chunks:
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "HTML", "disable_web_page_preview": True, "disable_notification": silent}
        try:
            async with session.post(url, json=payload) as resp:
                body = await resp.text()
                ok = 200 <= resp.status < 300
                last_telegram_status.update({"ok": ok, "http_status": resp.status, "response": body[:500], "error": None if ok else body[:500]})
                if not ok:
                    ok_all = False
                    logger.error("Telegram error %s: %s", resp.status, body[:300])
        except Exception as e:
            ok_all = False
            last_telegram_status.update({"ok": False, "error": repr(e)})
            logger.exception("Telegram send failed")
    return ok_all


def startup_message() -> str:
    return (
        f"🚀 <b>{APP_NAME} запущен</b>\n"
        f"Версия: <b>{BOT_VERSION}</b>\n\n"
        f"Режим: только экстремальные отскоки\n"
        f"SHORT: сильный памп → high failure → отбой вниз\n"
        f"LONG: сильный пролив → low reclaim → отскок вверх\n\n"
        f"Скан: каждые {SCAN_INTERVAL_SECONDS} сек\n"
        f"Hot scan: {HOT_SCAN_SYMBOLS}, анализ: {HOT_SYMBOLS_TO_ANALYZE}\n"
        f"MIN_SCORE: {MIN_SCORE}, fallback: {FALLBACK_SCORE}\n"
        f"Плечо в сообщении: x{LEVERAGE}\n"
        f"Profit считается от TP{MIN_PROFIT_TARGET_INDEX}\n\n"
        f"Бот только отправляет сигналы, сделки не открывает."
    )

# -------------------------
# BingX public API
# -------------------------
async def bingx_get(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
    session = await get_session()
    url = BINGX_BASE_URL + path
    for attempt in range(3):
        try:
            async with session.get(url, params=params) as resp:
                text = await resp.text()
                if resp.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(0.4 * (attempt + 1))
                    continue
                if not (200 <= resp.status < 300):
                    logger.debug("BingX HTTP %s %s", resp.status, text[:200])
                    return None
                data = json.loads(text)
                if isinstance(data, dict):
                    return data.get("data", data)
                return data
        except Exception as e:
            logger.debug("BingX get failed %s: %s", path, e)
            await asyncio.sleep(0.35 * (attempt + 1))
    return None


def parse_klines(raw: Any) -> List[List[float]]:
    out: List[List[float]] = []
    if not raw:
        return out
    for c in raw:
        try:
            if isinstance(c, dict):
                ts = int(c.get("time") or c.get("openTime") or c.get("T") or 0)
                o = safe_float(c.get("open")); h = safe_float(c.get("high")); l = safe_float(c.get("low")); cl = safe_float(c.get("close")); v = safe_float(c.get("volume") or c.get("vol") or 0)
            elif isinstance(c, (list, tuple)) and len(c) >= 6:
                ts = int(c[0]); o = safe_float(c[1]); h = safe_float(c[2]); l = safe_float(c[3]); cl = safe_float(c[4]); v = safe_float(c[5])
            else:
                continue
            if o > 0 and h > 0 and l > 0 and cl > 0:
                out.append([ts, o, h, l, cl, v])
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out


async def fetch_ohlcv_safe(symbol: str, timeframe: str, limit: int) -> List[List[float]]:
    cache_key = f"{symbol}:{timeframe}:{limit}"
    now = time.time()
    cached = kline_cache.get(cache_key)
    if cached and now - cached[0] < KLINE_CACHE_SECONDS:
        return cached[1]
    for path in ("/openApi/swap/v3/quote/klines", "/openApi/swap/v2/quote/klines"):
        raw = await bingx_get(path, {"symbol": symbol, "interval": timeframe, "limit": limit})
        candles = parse_klines(raw)
        if len(candles) >= 20:
            kline_cache[cache_key] = (now, candles)
            return candles
    kline_cache[cache_key] = (now, [])
    return []


async def get_contract_symbols() -> List[str]:
    global contracts_cache
    now = time.time()
    if contracts_cache[1] and now - contracts_cache[0] < CONTRACTS_CACHE_SECONDS:
        return contracts_cache[1]
    raw = await bingx_get("/openApi/swap/v2/quote/contracts")
    symbols: List[str] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            sym = str(item.get("symbol") or "").upper().replace("/", "-")
            if not sym.endswith("-USDT"):
                continue
            base = base_from_symbol(sym)
            if base in EXCLUDED_BASES:
                continue
            if any(k in base for k in ULTRA_RISK_KEYWORDS):
                continue
            if item.get("status") not in (None, 1, "1", "TRADING", "ONLINE"):
                # Some BingX responses do not include a universal status field, so only soft-filter.
                pass
            symbols.append(sym)
    if not symbols:
        symbols = ["SOL-USDT", "XRP-USDT", "DOGE-USDT", "WLD-USDT", "PUMP-USDT", "GRASS-USDT", "AERO-USDT", "BEAT-USDT", "LAB-USDT", "PORTAL-USDT"]
    random_part = symbols[:]
    random.shuffle(random_part)
    # Keep examples and liquid names near the front, then randomize the rest.
    priority = [s for s in symbols if base_from_symbol(s) in {"SOL", "XRP", "DOGE", "WLD", "PUMP", "GRASS", "AERO", "BEAT", "LAB", "PORTAL", "HOME", "TAC", "VELVET", "BLESS"}]
    seen = set(priority)
    result = priority + [s for s in random_part if s not in seen]
    contracts_cache = (now, result[:MAX_CONTRACTS])
    return contracts_cache[1]

# -------------------------
# Indicators
# -------------------------
def closes(ohlcv: List[List[float]]) -> List[float]:
    return [safe_float(x[4]) for x in ohlcv if len(x) >= 5]


def highs(ohlcv: List[List[float]]) -> List[float]:
    return [safe_float(x[2]) for x in ohlcv if len(x) >= 3]


def lows(ohlcv: List[List[float]]) -> List[float]:
    return [safe_float(x[3]) for x in ohlcv if len(x) >= 4]


def volumes(ohlcv: List[List[float]]) -> List[float]:
    return [safe_float(x[5]) for x in ohlcv if len(x) >= 6]


def ema_series(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    if len(values) < period:
        return [sum(values) / len(values)] * len(values)
    k = 2 / (period + 1)
    out: List[float] = []
    e = sum(values[:period]) / period
    for i, v in enumerate(values):
        if i < period:
            out.append(e)
        else:
            e = v * k + e * (1 - k)
            out.append(e)
    return out


def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < max(2, period):
        return None
    return ema_series(values, period)[-1]


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) <= period:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for g, l in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(ohlcv: List[List[float]], period: int = 14) -> Optional[float]:
    if len(ohlcv) <= period:
        return None
    trs = []
    for i in range(1, len(ohlcv)):
        h = safe_float(ohlcv[i][2]); l = safe_float(ohlcv[i][3]); pc = safe_float(ohlcv[i - 1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def volume_ratio(ohlcv: List[List[float]], lookback: int = 20) -> float:
    vols = volumes(ohlcv)
    if len(vols) < lookback + 1:
        return 1.0
    avg = sum(vols[-lookback - 1:-1]) / lookback
    return vols[-1] / avg if avg > 0 else 1.0


def range_ratio(ohlcv: List[List[float]], lookback: int = 20) -> float:
    if len(ohlcv) < lookback + 1:
        return 1.0
    ranges = [max(c[2] - c[3], 0.0) for c in ohlcv]
    avg = sum(ranges[-lookback - 1:-1]) / lookback
    return ranges[-1] / avg if avg > 0 else 1.0


def vwap(ohlcv: List[List[float]], lookback: int = 48) -> Optional[float]:
    chunk = ohlcv[-lookback:]
    pv, vv = 0.0, 0.0
    for c in chunk:
        if len(c) < 6:
            continue
        typical = (safe_float(c[2]) + safe_float(c[3]) + safe_float(c[4])) / 3
        vol = safe_float(c[5])
        pv += typical * vol
        vv += vol
    return pv / vv if vv > 0 else None


def candle_direction(ohlcv: List[List[float]]) -> str:
    if not ohlcv:
        return "flat"
    o, c = safe_float(ohlcv[-1][1]), safe_float(ohlcv[-1][4])
    if c > o:
        return "bull"
    if c < o:
        return "bear"
    return "flat"


def close_location(ohlcv: List[List[float]]) -> float:
    if not ohlcv:
        return 0.5
    c = ohlcv[-1]
    h, l, cl = safe_float(c[2]), safe_float(c[3]), safe_float(c[4])
    rng = max(h - l, 1e-12)
    return (cl - l) / rng


def wick_ratios(ohlcv: List[List[float]]) -> Tuple[float, float]:
    if not ohlcv:
        return 0.0, 0.0
    o, h, l, cl = safe_float(ohlcv[-1][1]), safe_float(ohlcv[-1][2]), safe_float(ohlcv[-1][3]), safe_float(ohlcv[-1][4])
    rng = max(h - l, 1e-12)
    upper = (h - max(o, cl)) / rng
    lower = (min(o, cl) - l) / rng
    return upper, lower


def macd_hist(values: List[float]) -> float:
    if len(values) < 35:
        return 0.0
    e12 = ema_series(values, 12)
    e26 = ema_series(values, 26)
    macd_line = [a - b for a, b in zip(e12[-len(e26):], e26)] if len(e12) != len(e26) else [a - b for a, b in zip(e12, e26)]
    signal = ema_series(macd_line, 9)
    if not signal:
        return 0.0
    return macd_line[-1] - signal[-1]


def obv_slope(ohlcv: List[List[float]], bars: int = 12) -> float:
    if len(ohlcv) < bars + 2:
        return 0.0
    obv = 0.0
    values = []
    for i in range(1, len(ohlcv)):
        c, pc, vol = safe_float(ohlcv[i][4]), safe_float(ohlcv[i - 1][4]), safe_float(ohlcv[i][5])
        if c > pc:
            obv += vol
        elif c < pc:
            obv -= vol
        values.append(obv)
    if len(values) < bars + 1:
        return 0.0
    base = max(abs(values[-bars - 1]), 1.0)
    return (values[-1] - values[-bars - 1]) / base


def volume_delta_proxy(ohlcv: List[List[float]], bars: int = 8) -> float:
    if len(ohlcv) < bars:
        return 0.0
    buy, sell = 0.0, 0.0
    for c in ohlcv[-bars:]:
        o, cl, vol = safe_float(c[1]), safe_float(c[4]), safe_float(c[5])
        if cl >= o:
            buy += vol
        else:
            sell += vol
    total = buy + sell
    return (buy - sell) / total if total > 0 else 0.0

# -------------------------
# Metrics / scoring
# -------------------------
def calc_metrics(c1: List[List[float]], c5: List[List[float]], c15: List[List[float]], c1h: List[List[float]]) -> Optional[Dict[str, Any]]:
    if len(c1) < 40 or len(c5) < 50 or len(c15) < 60:
        return None
    cl1, cl5, cl15, cl1h = closes(c1), closes(c5), closes(c15), closes(c1h)
    h15, l15 = highs(c15), lows(c15)
    last = cl1[-1]
    if last <= 0:
        return None
    recent_high_48 = max(h15[-49:-1]) if len(h15) >= 49 else max(h15[:-1])
    recent_low_48 = min(l15[-49:-1]) if len(l15) >= 49 else min(l15[:-1])
    recent_high_24 = max(h15[-25:-1]) if len(h15) >= 25 else max(h15[:-1])
    recent_low_24 = min(l15[-25:-1]) if len(l15) >= 25 else min(l15[:-1])
    atr15 = atr(c15, 14) or 0.0
    vwap15 = vwap(c15, 48) or last
    ema20_15 = ema(cl15, 20) or last
    ema50_15 = ema(cl15, 50) or last
    upper5, lower5 = wick_ratios(c5)
    upper1, lower1 = wick_ratios(c1)
    metrics = {
        "last": last,
        "move_1m_3": pct_change(cl1[-1], cl1[-4]) if len(cl1) >= 4 else 0.0,
        "move_5m_3": pct_change(cl5[-1], cl5[-4]) if len(cl5) >= 4 else 0.0,
        "move_5m_6": pct_change(cl5[-1], cl5[-7]) if len(cl5) >= 7 else 0.0,
        "move_15m_4": pct_change(cl15[-1], cl15[-5]) if len(cl15) >= 5 else 0.0,
        "move_15m_12": pct_change(cl15[-1], cl15[-13]) if len(cl15) >= 13 else 0.0,
        "move_15m_24": pct_change(cl15[-1], cl15[-25]) if len(cl15) >= 25 else 0.0,
        "move_1h_6": pct_change(cl1h[-1], cl1h[-7]) if len(cl1h) >= 7 else 0.0,
        "rsi15": rsi(cl15, 14) or 50.0,
        "rsi5": rsi(cl5, 14) or 50.0,
        "ema20_15": ema20_15,
        "ema50_15": ema50_15,
        "vwap15": vwap15,
        "atr15": atr15,
        "atr_pct": atr15 / last * 100.0 if last > 0 else 0.0,
        "volr1": volume_ratio(c1, 20),
        "volr5": volume_ratio(c5, 20),
        "volr15": volume_ratio(c15, 20),
        "range1": range_ratio(c1, 20),
        "range5": range_ratio(c5, 20),
        "recent_high_48": recent_high_48,
        "recent_low_48": recent_low_48,
        "recent_high_24": recent_high_24,
        "recent_low_24": recent_low_24,
        "dist_to_high_48": abs(pct_change(last, recent_high_48)),
        "dist_from_low_48": abs(pct_change(last, recent_low_48)),
        "dist_to_high_24": abs(pct_change(last, recent_high_24)),
        "dist_from_low_24": abs(pct_change(last, recent_low_24)),
        "candle1": candle_direction(c1),
        "candle5": candle_direction(c5),
        "candle15": candle_direction(c15),
        "close_loc1": close_location(c1),
        "close_loc5": close_location(c5),
        "upper_wick5": upper5,
        "lower_wick5": lower5,
        "upper_wick1": upper1,
        "lower_wick1": lower1,
        "macd15_hist": macd_hist(cl15),
        "obv5_slope": obv_slope(c5, 12),
        "obv15_slope": obv_slope(c15, 10),
        "vol_delta1": volume_delta_proxy(c1, 8),
        "vol_delta5": volume_delta_proxy(c5, 8),
    }
    return metrics


def mtf_confirmations(metrics: Dict[str, Any], side: str) -> Tuple[float, List[str], bool]:
    score = 0.0
    reasons: List[str] = []
    bad_delta = False
    if side == "SHORT":
        if metrics["macd15_hist"] < 0:
            score += 1.0; reasons.append("MACD15 вниз")
        if metrics["obv5_slope"] < 0 or metrics["obv15_slope"] < 0:
            score += 1.0; reasons.append("OBV distribution")
        if metrics["vol_delta1"] < -0.05 or metrics["vol_delta5"] < -0.05:
            score += 1.0; reasons.append("VolΔ sell pressure")
        elif metrics["vol_delta1"] > 0.25 and metrics["vol_delta5"] > 0.15:
            bad_delta = True
        if metrics["candle1"] == "bear" and metrics["close_loc1"] <= 0.45:
            score += 0.75; reasons.append("1m close вниз")
        if metrics["candle5"] == "bear" or metrics["upper_wick5"] >= 0.35:
            score += 0.75; reasons.append("5m rejection")
    else:
        if metrics["macd15_hist"] > 0:
            score += 1.0; reasons.append("MACD15 вверх")
        if metrics["obv5_slope"] > 0 or metrics["obv15_slope"] > 0:
            score += 1.0; reasons.append("OBV accumulation")
        if metrics["vol_delta1"] > 0.05 or metrics["vol_delta5"] > 0.05:
            score += 1.0; reasons.append("VolΔ buy pressure")
        elif metrics["vol_delta1"] < -0.25 and metrics["vol_delta5"] < -0.15:
            bad_delta = True
        if metrics["candle1"] == "bull" and metrics["close_loc1"] >= 0.55:
            score += 0.75; reasons.append("1m close вверх")
        if metrics["candle5"] == "bull" or metrics["lower_wick5"] >= 0.35:
            score += 0.75; reasons.append("5m reclaim")
    return score, reasons, bad_delta


def score_candidate(metrics: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    # SHORT: extreme pump exhaustion
    score = 0
    reasons: List[str] = []
    extreme_short = False
    if metrics["move_15m_24"] >= SHORT_MIN_PUMP_6H:
        score += 30; reasons.append(f"экстремальный памп 6ч {fmt_pct(metrics['move_15m_24'])}"); extreme_short = True
    elif metrics["move_15m_12"] >= SHORT_MIN_PUMP_3H:
        score += 26; reasons.append(f"сильный памп 3ч {fmt_pct(metrics['move_15m_12'])}"); extreme_short = True
    elif metrics["move_15m_4"] >= SHORT_MIN_PUMP_1H:
        score += 22; reasons.append(f"быстрый памп 1ч {fmt_pct(metrics['move_15m_4'])}"); extreme_short = True
    if metrics["dist_to_high_48"] <= SHORT_MAX_DIST_FROM_HIGH or metrics["dist_to_high_24"] <= SHORT_MAX_DIST_FROM_HIGH:
        score += 14; reasons.append("рядом с high/сопротивлением")
    if metrics["move_1m_3"] <= SHORT_MIN_REJECT_15M or metrics["move_5m_3"] <= SHORT_MIN_REJECT_5M:
        score += 18; reasons.append("отбой вниз уже начался")
    if metrics["candle1"] == "bear" and metrics["candle5"] == "bear":
        score += 12; reasons.append("1m/5m медвежьи")
    elif metrics["candle1"] == "bear":
        score += 7; reasons.append("1m медвежья")
    if metrics["upper_wick5"] >= 0.35 or metrics["upper_wick1"] >= 0.35:
        score += 10; reasons.append("верхняя тень / rejection")
    if metrics["rsi15"] >= SHORT_RSI_MIN:
        score += 9; reasons.append(f"RSI15 перегрет {metrics['rsi15']:.1f}")
    if metrics["volr5"] >= MIN_VOLR_5M or metrics["volr15"] >= MIN_VOLR_15M:
        score += 10; reasons.append("объём подтверждает")
    if metrics["range5"] >= MIN_RANGE_5M:
        score += 5; reasons.append("range 5m живой")
    if metrics["last"] <= metrics["vwap15"] or metrics["last"] <= metrics["ema20_15"]:
        score += 7; reasons.append("потеря VWAP/EMA20")
    if metrics["move_5m_6"] <= -6.0:
        score -= 10; reasons.append("анти-чейз: вниз уже слишком далеко")
    mtf_score, mtf_reasons, bad_delta = mtf_confirmations(metrics, "SHORT")
    if PHOTO_MTF_FILTER_ENABLED:
        score += int(mtf_score * 5)
        reasons += mtf_reasons[:3]
        if bad_delta and PHOTO_MTF_BLOCK_BAD_DELTA:
            score -= 30; reasons.append("bad VolΔ против SHORT")
    if extreme_short:
        candidates.append({"side": "SHORT", "setup": "EXTREME PUMP EXHAUSTION SHORT", "score": score, "reasons": reasons, "mtf_score": mtf_score, "bad_delta": bad_delta})

    # LONG: extreme dump capitulation bounce
    score = 0
    reasons = []
    extreme_long = False
    if metrics["move_15m_24"] <= LONG_MIN_DUMP_6H:
        score += 30; reasons.append(f"экстремальный пролив 6ч {fmt_pct(metrics['move_15m_24'])}"); extreme_long = True
    elif metrics["move_15m_12"] <= LONG_MIN_DUMP_3H:
        score += 26; reasons.append(f"сильный пролив 3ч {fmt_pct(metrics['move_15m_12'])}"); extreme_long = True
    elif metrics["move_15m_4"] <= LONG_MIN_DUMP_1H:
        score += 22; reasons.append(f"быстрый пролив 1ч {fmt_pct(metrics['move_15m_4'])}"); extreme_long = True
    if metrics["dist_from_low_48"] <= LONG_MAX_DIST_FROM_LOW or metrics["dist_from_low_24"] <= LONG_MAX_DIST_FROM_LOW:
        score += 14; reasons.append("рядом с low/поддержкой")
    if metrics["move_1m_3"] >= LONG_MIN_BOUNCE_15M or metrics["move_5m_3"] >= LONG_MIN_BOUNCE_5M:
        score += 18; reasons.append("отскок вверх уже начался")
    if metrics["candle1"] == "bull" and metrics["candle5"] == "bull":
        score += 12; reasons.append("1m/5m бычьи")
    elif metrics["candle1"] == "bull":
        score += 7; reasons.append("1m бычья")
    if metrics["lower_wick5"] >= 0.35 or metrics["lower_wick1"] >= 0.35:
        score += 10; reasons.append("нижняя тень / reclaim")
    if metrics["rsi15"] <= LONG_RSI_MAX:
        score += 9; reasons.append(f"RSI15 перепродан {metrics['rsi15']:.1f}")
    if metrics["volr5"] >= MIN_VOLR_5M or metrics["volr15"] >= MIN_VOLR_15M:
        score += 10; reasons.append("объём подтверждает")
    if metrics["range5"] >= MIN_RANGE_5M:
        score += 5; reasons.append("range 5m живой")
    if metrics["last"] >= metrics["vwap15"] or metrics["last"] >= metrics["ema20_15"]:
        score += 7; reasons.append("reclaim VWAP/EMA20")
    if metrics["move_5m_6"] >= 7.0:
        score -= 10; reasons.append("анти-чейз: отскок уже слишком далеко")
    mtf_score, mtf_reasons, bad_delta = mtf_confirmations(metrics, "LONG")
    if PHOTO_MTF_FILTER_ENABLED:
        score += int(mtf_score * 5)
        reasons += mtf_reasons[:3]
        if bad_delta and PHOTO_MTF_BLOCK_BAD_DELTA:
            score -= 30; reasons.append("bad VolΔ против LONG")
    if extreme_long:
        candidates.append({"side": "LONG", "setup": "EXTREME CAPITULATION BOUNCE LONG", "score": score, "reasons": reasons, "mtf_score": mtf_score, "bad_delta": bad_delta})

    if not candidates:
        return None
    best = max(candidates, key=lambda x: x["score"])
    if PHOTO_MTF_FILTER_ENABLED and best.get("mtf_score", 0.0) < PHOTO_MTF_MIN_CONFIRMATIONS and best["score"] < MIN_SCORE + 12:
        # Soft block: only very strong level reactions can bypass weak MTF.
        return None
    quality = int(clamp(best["score"], 0, 99))
    if quality <= 0:
        return None
    best["quality"] = quality
    return best


def build_signal(symbol: str, metrics: Dict[str, Any], scored: Dict[str, Any]) -> Signal:
    entry = safe_float(metrics["last"])
    side = scored["side"]
    base = base_from_symbol(symbol)
    if side == "LONG":
        targets = [entry * (1 + p) for p in TAKE_PCTS[:5]]
        average = entry * (1 - AVERAGE_PCT)
        stop = entry * (1 - STOP_AFTER_AVERAGE_PCT)
    else:
        targets = [entry * (1 - p) for p in TAKE_PCTS[:5]]
        average = entry * (1 + AVERAGE_PCT)
        stop = entry * (1 + STOP_AFTER_AVERAGE_PCT)
    return Signal(symbol=symbol, display_symbol=display_symbol(symbol), base=base, side=side, setup=scored["setup"], quality=int(scored["quality"]), entry=entry, targets=targets, average=average, stop=stop, leverage=LEVERAGE, score_reasons=list(scored.get("reasons", []))[:9], metrics=metrics)

# -------------------------
# Signal / Stats formatting
# -------------------------
def signal_message(sig: Signal) -> str:
    lines = [
        f"🔥 <b>Скальп-позиция - {sig.base} {sig.side}</b>",
        "",
        "Биржа: BingX USDT-M Futures",
        f"Сетап: <b>{sig.setup}</b>",
        f"Качество: <b>{sig.quality}/100</b>",
        f"Версия: {BOT_VERSION}",
        "",
        f"Моя точка входа - <b>{fmt_price(sig.entry)}</b>",
        "",
        "Пока заходите, следующим блоком параметры сделки!",
        "",
        "Лимитные ордера на фиксацию выставить на значениях:",
        "",
    ]
    for target in sig.targets:
        lines.append(fmt_price(target))
    lines += [
        "",
        f"Лимитный ордер на усреднение: <b>{fmt_price(sig.average)}</b>",
        f"Защитный стоп: <b>{fmt_price(sig.stop)}</b>",
        "",
        f"Плечо: до x{sig.leverage}",
        "Риск-менеджмент: RM ≤ 0.5% депозита",
        f"Статистика считает profit от TP{MIN_PROFIT_TARGET_INDEX}",
        "",
        "Подтверждения:",
    ]
    for reason in sig.score_reasons:
        lines.append(f"• {reason}")
    lines += [
        "",
        f"Движение: 1h {fmt_pct(sig.metrics.get('move_15m_4', 0))} · 3h {fmt_pct(sig.metrics.get('move_15m_12', 0))} · 6h {fmt_pct(sig.metrics.get('move_15m_24', 0))}",
        f"Vol5 x{sig.metrics.get('volr5', 1):.2f} · Vol15 x{sig.metrics.get('volr15', 1):.2f} · RSI15 {sig.metrics.get('rsi15', 50):.1f}",
        "",
        "О любых действиях по открытой сделке буду сообщать в канале.",
        "⚠️ Это сигнал, а не гарантия прибыли. Соблюдай риск."
    ]
    return "\n".join(lines)


def stats_text() -> str:
    ensure_stats()
    st = state.stats
    closed = int(st.get("closed_total", 0) or 0)
    profit = int(st.get("closed_profit", 0) or 0)
    loss = int(st.get("closed_loss", 0) or 0)
    expired = int(st.get("expired", 0) or 0)
    early = int(st.get("early_exit", 0) or 0)
    wr = (profit / closed * 100.0) if closed else 0.0
    avg_roi = (float(st.get("total_closed_roi", 0.0) or 0.0) / closed) if closed else 0.0
    lines = [
        "📊 <b>Статистика бота</b>",
        f"Версия: {BOT_VERSION}",
        "",
        f"Всего сигналов: <b>{int(st.get('total_signals_sent', 0) or 0)}</b>",
        f"Pending: <b>{len(state.pending)}</b>",
        f"Активные сделки: <b>{len(state.active)}</b>",
        f"Закрытые сделки: <b>{closed}</b>",
        f"✅ Прибыльные: <b>{profit}</b>",
        f"🛑 Убыточные: <b>{loss}</b>",
        f"⏱ Expired: <b>{expired}</b>",
        f"⚠️ Early-exit: <b>{early}</b>",
        f"Win rate: <b>{wr:.1f}%</b>",
        "",
        f"Pending created/confirmed/expired/rejected: {st.get('pending_created', 0)}/{st.get('pending_confirmed', 0)}/{st.get('pending_expired', 0)}/{st.get('pending_rejected', 0)}",
        f"TP1/TP2/TP3/TP4/TP5: {st.get('tp1_hits', 0)}/{st.get('tp2_hits', 0)}/{st.get('tp3_hits', 0)}/{st.get('tp4_hits', 0)}/{st.get('tp5_hits', 0)}",
        f"Усреднений: {st.get('average_hits', 0)} · Стопов: {st.get('stop_losses', 0)}",
        "",
        f"Средний ROI по закрытым: {fmt_pct(avg_roi)}",
        f"Лучший ROI: {fmt_pct(st.get('best_roi')) if st.get('best_roi') is not None else '—'}",
        f"Худший ROI: {fmt_pct(st.get('worst_roi')) if st.get('worst_roi') is not None else '—'}",
    ]
    if st.get("strategy"):
        lines.append("\nСтратегии:")
        for k, v in sorted(st["strategy"].items(), key=lambda kv: -(sum(kv[1].values()))):
            total = sum(v.values())
            w = v.get("profit", 0) / total * 100 if total else 0
            lines.append(f"{k}: {v} · WR {w:.1f}%")
    recent = st.get("last_results", [])[-5:]
    if recent:
        lines.append("\nПоследние закрытые:")
        for r in reversed(recent):
            icon = "✅" if r.get("result") == "profit" else "🛑" if r.get("result") == "loss" else "⏱"
            lines.append(f"{icon} {r.get('symbol')} {r.get('side')} · {r.get('reason')} · {fmt_pct(safe_float(r.get('roi')))}")
    return "\n".join(lines)

# -------------------------
# Cooldown / circuit
# -------------------------
def is_on_cooldown(symbol: str) -> bool:
    return safe_float(state.cooldowns.get(symbol), 0.0) > time.time()


def put_cooldown(symbol: str) -> None:
    state.cooldowns[symbol] = time.time() + COOLDOWN_MINUTES * 60


def cleanup_cooldowns() -> None:
    now = time.time()
    state.cooldowns = {s: ts for s, ts in state.cooldowns.items() if safe_float(ts) > now}


def circuit_paused() -> bool:
    ensure_stats()
    return time.time() < safe_float(state.stats.get("pause_until"), 0.0)


def update_circuit(result: str) -> None:
    ensure_stats()
    if result == "profit":
        state.stats["consecutive_losses"] = 0
    elif result in {"loss", "early", "expired"}:
        if result == "loss":
            state.stats["consecutive_losses"] = int(state.stats.get("consecutive_losses", 0) or 0) + 1
            state.stats["daily_losses"] = int(state.stats.get("daily_losses", 0) or 0) + 1
        if CIRCUIT_BREAKER_ENABLED and (int(state.stats.get("consecutive_losses", 0)) >= MAX_CONSECUTIVE_LOSSES or int(state.stats.get("daily_losses", 0)) >= MAX_DAILY_LOSSES):
            state.stats["pause_until"] = time.time() + CIRCUIT_PAUSE_MINUTES * 60
            state.stats["consecutive_losses"] = 0

# -------------------------
# Scanning / confirmation
# -------------------------
async def hot_score_symbol(symbol: str, sem: asyncio.Semaphore) -> Tuple[float, str, Dict[str, Any]]:
    async with sem:
        c5 = await fetch_ohlcv_safe(symbol, "5m", 48)
    if len(c5) < 25:
        return 0.0, symbol, {}
    cl5 = closes(c5)
    move3 = abs(pct_change(cl5[-1], cl5[-4])) if len(cl5) >= 4 else 0.0
    move6 = abs(pct_change(cl5[-1], cl5[-7])) if len(cl5) >= 7 else 0.0
    vr = volume_ratio(c5, 20)
    rr = range_ratio(c5, 20)
    score = move3 * 18 + move6 * 9 + min(vr, 5) * 8 + min(rr, 5) * 8
    return score, symbol, {"move5m3": move3, "move5m6": move6, "volr5": vr, "range5": rr}


async def get_hot_symbols() -> Tuple[List[str], List[str]]:
    symbols = await get_contract_symbols()
    symbols = symbols[:HOT_SCAN_SYMBOLS]
    sem = asyncio.Semaphore(CONCURRENCY)
    rows = await asyncio.gather(*(hot_score_symbol(s, sem) for s in symbols), return_exceptions=True)
    scored: List[Tuple[float, str, Dict[str, Any]]] = []
    for row in rows:
        if isinstance(row, Exception):
            continue
        sc, sym, m = row
        if sc > 0:
            scored.append((sc, sym, m))
    scored.sort(key=lambda x: x[0], reverse=True)
    notes = [f"{display_symbol(sym)} hot {sc:.1f}: 5m3 {m.get('move5m3', 0):.2f}%, 5m6 {m.get('move5m6', 0):.2f}%, vol5 x{m.get('volr5', 1):.2f}, range5 x{m.get('range5', 1):.2f}" for sc, sym, m in scored[:8]]
    return [sym for _, sym, _ in scored[:HOT_SYMBOLS_TO_ANALYZE]], notes


async def analyze_symbol(symbol: str) -> Optional[Signal]:
    c1, c5, c15, c1h = await asyncio.gather(
        fetch_ohlcv_safe(symbol, "1m", 80),
        fetch_ohlcv_safe(symbol, "5m", 100),
        fetch_ohlcv_safe(symbol, "15m", 130),
        fetch_ohlcv_safe(symbol, "1h", 80),
    )
    metrics = calc_metrics(c1, c5, c15, c1h)
    if not metrics:
        return None
    scored = score_candidate(metrics)
    if not scored:
        return None
    return build_signal(symbol, metrics, scored)


def signal_from_dict(data: Dict[str, Any]) -> Signal:
    return Signal(
        symbol=data.get("symbol", ""), display_symbol=data.get("display_symbol") or display_symbol(data.get("symbol", "")), base=data.get("base", ""),
        side=data.get("side", "LONG"), setup=data.get("setup", ""), quality=int(data.get("quality", 0) or 0),
        entry=safe_float(data.get("entry")), targets=[safe_float(x) for x in data.get("targets", [])],
        average=safe_float(data.get("average")), stop=safe_float(data.get("stop")), leverage=int(data.get("leverage", LEVERAGE) or LEVERAGE),
        score_reasons=list(data.get("score_reasons", [])), metrics=dict(data.get("metrics", {})), created_ts=safe_float(data.get("created_ts"), time.time()),
        hit_targets=[int(x) for x in data.get("hit_targets", [])], average_hit=bool(data.get("average_hit", False)),
        closed=bool(data.get("closed", False)), realized_profit=bool(data.get("realized_profit", False)),
    )


async def current_price(symbol: str) -> Optional[float]:
    c = await fetch_ohlcv_safe(symbol, "1m", 30)
    if c:
        return safe_float(c[-1][4])
    return None


def progress_to_tp1(sig: Signal, price: float) -> Tuple[bool, float, float]:
    full = abs(sig.targets[0] - sig.entry)
    risk = abs(sig.stop - sig.entry)
    if full <= 0:
        return False, 0.0, 0.0
    if sig.side == "LONG":
        progress = max(0.0, price - sig.entry) / full
        adverse = max(0.0, sig.entry - price) / max(risk, 1e-12)
        direction = price > sig.entry
    else:
        progress = max(0.0, sig.entry - price) / full
        adverse = max(0.0, price - sig.entry) / max(risk, 1e-12)
        direction = price < sig.entry
    return direction, progress, adverse


async def confirm_pending_signal(key: str, raw: Dict[str, Any]) -> Tuple[str, Optional[Signal], str]:
    p = PendingSignal(signal=raw.get("signal", {}), first_seen_ts=safe_float(raw.get("first_seen_ts"), time.time()), confirmed_ts=safe_float(raw.get("confirmed_ts"), 0.0), best_progress=safe_float(raw.get("best_progress"), 0.0))
    sig = signal_from_dict(p.signal)
    price = await current_price(sig.symbol)
    if price is None:
        return "wait", None, "no price"
    age = time.time() - p.first_seen_ts
    if age > CONFIRM_MAX_SECONDS:
        return "expired", None, f"pending expired age {age:.0f}s"
    direction, progress, adverse = progress_to_tp1(sig, price)
    p.best_progress = max(p.best_progress, progress)
    state.pending[key] = asdict(p)
    if adverse > CONFIRM_MAX_ADVERSE_SL_FRACTION:
        return "rejected", None, f"adverse {adverse:.2f} SL fraction"
    if age < CONFIRM_MIN_SECONDS:
        return "wait", None, f"wait confirmation {age:.0f}s"
    if not direction or progress < CONFIRM_MIN_TP1_PROGRESS:
        return "wait", None, f"progress weak {progress:.2f}"
    if progress > CONFIRM_MAX_TP1_PROGRESS:
        return "rejected", None, f"too late/chase progress {progress:.2f}"
    if CONFIRM_REQUIRE_CANDLE_DIRECTION:
        c1 = await fetch_ohlcv_safe(sig.symbol, "1m", 25)
        dir1 = candle_direction(c1)
        if sig.side == "LONG" and dir1 != "bull":
            return "wait", None, "1m candle not bull"
        if sig.side == "SHORT" and dir1 != "bear":
            return "wait", None, "1m candle not bear"
    if POST_CONFIRM_HOLD_ENABLED:
        if p.confirmed_ts <= 0:
            p.confirmed_ts = time.time()
            state.pending[key] = asdict(p)
            return "wait", None, "post-confirm hold started"
        if time.time() - p.confirmed_ts < POST_CONFIRM_HOLD_SECONDS:
            # Avoid snapback after first confirmation.
            if p.best_progress - progress > POST_CONFIRM_MAX_SNAPBACK:
                return "rejected", None, f"snapback {p.best_progress:.2f}->{progress:.2f}"
            return "wait", None, "post-confirm hold"
    return "confirmed", sig, f"confirmed progress {progress:.2f}"


async def process_pending() -> Tuple[int, int, int]:
    confirmed = expired = rejected = 0
    for key, raw in list(state.pending.items()):
        status, sig, reason = await confirm_pending_signal(key, raw)
        if status == "confirmed" and sig:
            if len(state.active) < MAX_ACTIVE_SIGNALS and state.signals_today < MAX_SIGNALS_PER_DAY and sig.symbol not in state.active:
                ok = await send_telegram(signal_message(sig))
                if ok:
                    state.active[sig.symbol] = asdict(sig)
                    state.signals_today += 1
                    state.last_signal_ts = time.time()
                    stat_inc("total_signals_sent")
                    stat_inc("pending_confirmed")
                    put_cooldown(sig.symbol)
                    confirmed += 1
                state.pending.pop(key, None)
            else:
                state.pending.pop(key, None)
                rejected += 1
                stat_inc("pending_rejected")
        elif status == "expired":
            state.pending.pop(key, None)
            expired += 1
            stat_inc("pending_expired")
        elif status == "rejected":
            state.pending.pop(key, None)
            rejected += 1
            stat_inc("pending_rejected")
        await asyncio.sleep(0.05)
    if confirmed or expired or rejected:
        save_state()
    return confirmed, expired, rejected


async def scan_once(send: bool = False) -> Dict[str, Any]:
    global last_scan_summary, last_empty_scan_sent_ts
    async with scan_lock:
        reset_daily_if_needed()
        cleanup_cooldowns()
        await update_active_signals()
        confirmed, pexpired, prejected = await process_pending()
        if circuit_paused():
            last_scan_summary = {"time_utc": datetime.now(timezone.utc).isoformat(), "paused": True, "active": len(state.active), "pending": len(state.pending)}
            return last_scan_summary
        hot, hot_notes = await get_hot_symbols()
        checked = 0
        candidates: List[Signal] = []
        threshold = MIN_SCORE if state.signals_today >= DAILY_MIN_SIGNALS else min(MIN_SCORE, FALLBACK_SCORE)
        for symbol in hot:
            checked += 1
            if state.signals_today >= MAX_SIGNALS_PER_DAY:
                break
            if len(state.active) >= MAX_ACTIVE_SIGNALS:
                break
            if symbol in state.active or symbol in state.pending or is_on_cooldown(symbol):
                continue
            try:
                sig = await analyze_symbol(symbol)
                if sig:
                    candidates.append(sig)
            except Exception as e:
                logger.debug("Analyze failed %s: %s", symbol, e)
            await asyncio.sleep(0.03)
        candidates.sort(key=lambda s: s.quality, reverse=True)
        added_pending = 0
        for sig in candidates:
            if added_pending >= MAX_SIGNALS_PER_SCAN:
                break
            if sig.quality < threshold:
                continue
            if sig.symbol in state.active or sig.symbol in state.pending or is_on_cooldown(sig.symbol):
                continue
            if CONFIRM_BEFORE_SEND_ENABLED:
                state.pending[sig.symbol] = asdict(PendingSignal(signal=asdict(sig)))
                stat_inc("pending_created")
                added_pending += 1
            else:
                ok = await send_telegram(signal_message(sig))
                if ok:
                    state.active[sig.symbol] = asdict(sig)
                    state.signals_today += 1
                    stat_inc("total_signals_sent")
                    put_cooldown(sig.symbol)
                    added_pending += 1
            await asyncio.sleep(0.2)
        save_state()
        last_scan_summary = {
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "version": BOT_VERSION,
            "checked": checked,
            "hot_count": len(hot),
            "candidates": len(candidates),
            "added_pending_or_sent": added_pending,
            "pending_confirmed": confirmed,
            "pending_expired": pexpired,
            "pending_rejected": prejected,
            "signals_today": state.signals_today,
            "active": len(state.active),
            "pending": len(state.pending),
            "threshold_used": threshold,
            "hot_notes": hot_notes,
            "best_candidates": [{"symbol": s.display_symbol, "side": s.side, "setup": s.setup, "quality": s.quality, "entry": s.entry, "reasons": s.score_reasons[:4]} for s in candidates[:10]],
        }
        logger.info("Scan: checked=%s candidates=%s pending/sent=%s active=%s pending=%s", checked, len(candidates), added_pending, len(state.active), len(state.pending))
        if SEND_EMPTY_SCAN and (send or (time.time() - last_empty_scan_sent_ts >= EMPTY_SCAN_EVERY_SECONDS)):
            best = candidates[0] if candidates else None
            msg = (
                f"🧪 <b>Scan update</b>\n{BOT_VERSION}\n"
                f"Проверено: {checked} · hot: {len(hot)}\n"
                f"Кандидатов: {len(candidates)} · pending added/sent: {added_pending}\n"
                f"Active: {len(state.active)} · Pending: {len(state.pending)}\n"
                f"Pending confirmed/expired/rejected: {confirmed}/{pexpired}/{prejected}\n"
                f"Best: {best.display_symbol + ' ' + best.side + ' ' + str(best.quality) if best else 'нет'}\n\n"
                f"Hot symbols:\n" + ("\n".join(hot_notes[:6]) if hot_notes else "нет")
            )
            await send_telegram(msg, silent=True)
            last_empty_scan_sent_ts = time.time()
        return last_scan_summary

# -------------------------
# Active trade tracking
# -------------------------
def trade_roi(sig: Signal, exit_price: float) -> float:
    if sig.entry <= 0:
        return 0.0
    if sig.side == "LONG":
        return pct_change(exit_price, sig.entry) * sig.leverage
    return pct_change(sig.entry, exit_price) * sig.leverage


def close_signal(symbol: str, sig: Signal, reason: str, result: str, exit_price: float) -> None:
    roi = trade_roi(sig, exit_price)
    ensure_stats()
    if result in {"profit", "loss"}:
        stat_inc("closed_total")
        stat_inc("closed_profit" if result == "profit" else "closed_loss")
    elif result == "expired":
        stat_inc("expired")
    elif result == "early":
        stat_inc("early_exit")
    if result == "loss":
        stat_inc("stop_losses")
    if len(sig.hit_targets) >= 5:
        stat_inc("all_targets")
    state.stats["total_closed_roi"] = float(state.stats.get("total_closed_roi", 0.0) or 0.0) + roi
    best = state.stats.get("best_roi"); worst = state.stats.get("worst_roi")
    state.stats["best_roi"] = roi if best is None else max(float(best), roi)
    state.stats["worst_roi"] = roi if worst is None else min(float(worst), roi)
    state.stats["last_results"].append({"time_utc": datetime.now(timezone.utc).isoformat(), "symbol": sig.display_symbol, "side": sig.side, "setup": sig.setup, "reason": reason, "result": result, "roi": roi, "entry": sig.entry, "exit": exit_price})
    state.stats["last_results"] = state.stats["last_results"][-40:]
    nested_stat("strategy", sig.setup, "profit" if result == "profit" else "loss" if result == "loss" else "expired" if result == "expired" else "early")
    nested_stat("side", sig.side, "profit" if result == "profit" else "loss" if result == "loss" else "expired" if result == "expired" else "early")
    update_circuit(result)
    state.active.pop(symbol, None)
    save_state()


async def update_active_signals() -> None:
    if not state.active:
        return
    changed = False
    for symbol, raw in list(state.active.items()):
        sig = signal_from_dict(raw)
        if sig.closed:
            continue
        price = await current_price(symbol)
        if price is None:
            continue
        age_minutes = (time.time() - sig.created_ts) / 60.0
        direction, progress, adverse = progress_to_tp1(sig, price)

        # Average hit.
        if not sig.average_hit:
            avg_hit = (sig.side == "LONG" and price <= sig.average) or (sig.side == "SHORT" and price >= sig.average)
            if avg_hit:
                sig.average_hit = True
                stat_inc("average_hits")
                changed = True
                await send_telegram(f"⚠️ <b>Усреднение активировано</b>\n{sig.display_symbol} {sig.side}\nЦена дошла до: <b>{fmt_price(sig.average)}</b>\nТекущая цена: {fmt_price(price)}\nРиск держи строго по плану.")

        # Early invalidation before full stop.
        if EARLY_INVALIDATION_ENABLED and not sig.realized_profit and age_minutes * 60 >= EARLY_INVALIDATION_SECONDS:
            if adverse >= EARLY_INVALIDATION_ADVERSE_SL_FRACTION and progress <= EARLY_INVALIDATION_MAX_PROGRESS:
                close_signal(symbol, sig, "EARLY INVALIDATION", "early", price)
                await send_telegram(f"⚠️ <b>EARLY INVALIDATION</b>\n{sig.display_symbol} {sig.side}\nЦена идёт против входа, прогресс слабый.\nEntry: {fmt_price(sig.entry)}\nCurrent: {fmt_price(price)}\nROI x{sig.leverage}: <b>{fmt_pct(trade_roi(sig, price))}</b>\n\n{stats_text()}")
                continue

        # Target hits.
        for i, target in enumerate(sig.targets, start=1):
            if i in sig.hit_targets:
                continue
            hit = (sig.side == "LONG" and price >= target) or (sig.side == "SHORT" and price <= target)
            if hit:
                sig.hit_targets.append(i)
                changed = True
                stat_inc(f"tp{i}_hits")
                roi = trade_roi(sig, target)
                await send_telegram(f"✅ <b>TP{i} достигнут</b>\n{sig.display_symbol} {sig.side}\nЦель: <b>{fmt_price(target)}</b>\nROI x{sig.leverage}: <b>{fmt_pct(roi)}</b>")
                if i >= MIN_PROFIT_TARGET_INDEX and not sig.realized_profit:
                    sig.realized_profit = True
                    close_signal(symbol, sig, f"TP{i} REALIZED", "profit", target)
                    await send_telegram(f"🏆 <b>Сделка реализована от TP{i}</b>\n{sig.display_symbol} {sig.side}\nEntry: {fmt_price(sig.entry)}\nTP{i}: {fmt_price(target)}\nИтог ROI x{sig.leverage}: <b>{fmt_pct(roi)}</b>\n\n{stats_text() if SEND_STATS_AFTER_CLOSE else ''}")
                    break
        if symbol not in state.active:
            continue

        # Stop.
        stop_hit = (sig.side == "LONG" and price <= sig.stop) or (sig.side == "SHORT" and price >= sig.stop)
        if stop_hit:
            result = "profit" if sig.realized_profit or len(sig.hit_targets) >= MIN_PROFIT_TARGET_INDEX else "loss"
            reason = "STOP after realized" if result == "profit" else "STOP before TP3"
            close_signal(symbol, sig, reason, result, price)
            icon = "✅" if result == "profit" else "🛑"
            await send_telegram(f"{icon} <b>{'Сделка закрыта в плюс' if result == 'profit' else 'Stop Loss'}</b>\n{sig.display_symbol} {sig.side}\nEntry: {fmt_price(sig.entry)}\nExit: {fmt_price(price)}\nROI x{sig.leverage}: <b>{fmt_pct(trade_roi(sig, price))}</b>\n\n{stats_text() if SEND_STATS_AFTER_CLOSE else ''}")
            continue

        # Expire if no movement.
        if age_minutes >= MAX_MINUTES_TO_TP1 and not sig.hit_targets:
            if (not direction) or progress < MIN_PROGRESS_TO_KEEP:
                close_signal(symbol, sig, "NO TP1 PROGRESS", "expired", price)
                await send_telegram(f"⏱ <b>FAST TRADE EXPIRED</b>\n{sig.display_symbol} {sig.side}\nЗа {MAX_MINUTES_TO_TP1} мин нет движения к TP1.\nEntry: {fmt_price(sig.entry)}\nCurrent: {fmt_price(price)}\nProgress to TP1: {progress*100:.1f}%\n\n{stats_text()}")
                continue
        if age_minutes >= HARD_EXPIRE_MINUTES and not sig.realized_profit:
            close_signal(symbol, sig, "HARD EXPIRE BEFORE TP3", "expired", price)
            await send_telegram(f"⏱ <b>HARD EXPIRE</b>\n{sig.display_symbol} {sig.side}\nTP{MIN_PROFIT_TARGET_INDEX} не достигнут за {HARD_EXPIRE_MINUTES} мин.\nCurrent: {fmt_price(price)}\n\n{stats_text()}")
            continue
        state.active[symbol] = asdict(sig)
    if changed:
        save_state()

# -------------------------
# Loops and endpoints
# -------------------------
async def scanner_loop() -> None:
    while True:
        try:
            await scan_once(send=False)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            err = "".join(traceback.format_exception_only(type(e), e)).strip()
            logger.error("Scanner loop error: %s\n%s", err, traceback.format_exc())
            if SEND_SCAN_ERRORS_TO_TELEGRAM:
                await send_telegram(f"⚠️ Ошибка сканера:\n<code>{err}</code>", silent=True)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


async def tracker_loop() -> None:
    while True:
        try:
            reset_daily_if_needed()
            await update_active_signals()
            await process_pending()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug("Tracker loop error: %s", e)
        await asyncio.sleep(TRACK_INTERVAL_SECONDS)


@app.on_event("startup")
async def on_startup() -> None:
    global scanner_task, tracker_task
    load_state()
    logger.info("Starting %s %s", APP_NAME, BOT_VERSION)
    if SEND_STARTUP_MESSAGE:
        await send_telegram(startup_message())
    scanner_task = asyncio.create_task(scanner_loop())
    tracker_task = asyncio.create_task(tracker_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global http_session, scanner_task, tracker_task
    save_state()
    for task in (scanner_task, tracker_task):
        if task:
            task.cancel()
    if http_session is not None and not http_session.closed:
        await http_session.close()


@app.get("/")
async def root() -> JSONResponse:
    return JSONResponse({"ok": True, "app": APP_NAME, "version": BOT_VERSION, "mode": "uvicorn bot:app", "uptime_sec": int(time.time() - started_at), "active": len(state.active), "pending": len(state.pending), "signals_today": state.signals_today, "last_scan": last_scan_summary})


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"ok": True, "version": BOT_VERSION, "active": len(state.active), "pending": len(state.pending), "paused": circuit_paused(), "telegram": last_telegram_status})


@app.get("/version")
async def version() -> JSONResponse:
    return JSONResponse({"app": APP_NAME, "version": BOT_VERSION})


@app.get("/scan")
async def manual_scan(send: bool = Query(True)) -> JSONResponse:
    scan = await scan_once(send=send)
    return JSONResponse(scan)


@app.get("/stats")
async def stats() -> JSONResponse:
    ensure_stats()
    return JSONResponse({"ok": True, "version": BOT_VERSION, "stats": state.stats, "active": state.active, "pending": state.pending})


@app.get("/stats-text")
async def stats_text_endpoint() -> PlainTextResponse:
    return PlainTextResponse(stats_text())


@app.get("/stats/send")
async def send_stats() -> JSONResponse:
    ok = await send_telegram(stats_text())
    return JSONResponse({"ok": ok, "telegram": last_telegram_status})


@app.get("/telegram/test")
async def telegram_test() -> JSONResponse:
    ok = await send_telegram(f"✅ Telegram test OK\n{APP_NAME}\n{BOT_VERSION}")
    return JSONResponse({"ok": ok, "telegram": last_telegram_status})


@app.get("/debug")
async def debug() -> JSONResponse:
    return JSONResponse({"ok": True, "env": {"TELEGRAM_BOT_TOKEN_present": bool(TELEGRAM_BOT_TOKEN), "TELEGRAM_CHAT_ID_present": bool(TELEGRAM_CHAT_ID), "SCAN_INTERVAL_SECONDS": SCAN_INTERVAL_SECONDS, "TRACK_INTERVAL_SECONDS": TRACK_INTERVAL_SECONDS, "HOT_SCAN_SYMBOLS": HOT_SCAN_SYMBOLS, "HOT_SYMBOLS_TO_ANALYZE": HOT_SYMBOLS_TO_ANALYZE, "MIN_SCORE": MIN_SCORE, "FALLBACK_SCORE": FALLBACK_SCORE, "DAILY_MIN_SIGNALS": DAILY_MIN_SIGNALS, "MAX_SIGNALS_PER_DAY": MAX_SIGNALS_PER_DAY, "LEVERAGE": LEVERAGE, "MIN_PROFIT_TARGET_INDEX": MIN_PROFIT_TARGET_INDEX}, "last_telegram_status": last_telegram_status, "last_scan_summary": last_scan_summary})


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000") or "8000")
    uvicorn.run("bot:app", host="0.0.0.0", port=port, reload=False)
