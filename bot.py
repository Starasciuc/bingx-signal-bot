"""
BingX Signal Bot for existing Render Background Worker
Start command supported: uvicorn bot:app --host 0.0.0.0 --port $PORT

What it does:
- Sends Telegram startup notification.
- Scans BingX USDT-M swap/futures public markets.
- Sends scalp setups in the style: ENTRY, 5 TPs, averaging order, protective stop.
- Tracks TP/averaging/stop and keeps win/loss statistics.

This is a signal bot only. It never opens trades.
"""

import asyncio
import json
import logging
import math
import os
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import ccxt.async_support as ccxt
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

APP_NAME = "BingX Scalp Signal Bot"
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")

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


TELEGRAM_BOT_TOKEN = first_env([
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "BOT_TOKEN", "TG_BOT_TOKEN"
])
TELEGRAM_CHAT_ID = first_env([
    "TELEGRAM_CHAT_ID", "TELEGRAM_CHANNEL_ID", "TG_CHAT_ID", "CHAT_ID"
])

# Scan settings. If the bot is too quiet, lower MIN_SCORE/FALLBACK_SCORE.
SCAN_INTERVAL_SECONDS = max(45, env_int("SCAN_INTERVAL_SECONDS", 180))
TOP_SYMBOLS_LIMIT = max(20, env_int("TOP_SYMBOLS_LIMIT", 120))
MIN_24H_QUOTE_VOLUME_USDT = env_float("MIN_24H_QUOTE_VOLUME_USDT", 700_000)

MIN_SCORE = env_int("MIN_SCORE", 74)
FALLBACK_SCORE = env_int("FALLBACK_SCORE", 68)
DAILY_MIN_SIGNALS = max(0, env_int("DAILY_MIN_SIGNALS", 1))
MAX_SIGNALS_PER_DAY = max(1, env_int("MAX_SIGNALS_PER_DAY", 6))
COOLDOWN_MINUTES = max(15, env_int("COOLDOWN_MINUTES", 180))

LEVERAGE = max(1, env_int("LEVERAGE", 20))
TAKE_PCTS = [
    float(x.strip()) / 100.0
    for x in env_str("TAKE_PCTS", "0.80,1.40,2.00,3.00,4.00").split(",")
    if x.strip()
]
if len(TAKE_PCTS) < 5:
    TAKE_PCTS = [0.008, 0.014, 0.020, 0.030, 0.040]
AVERAGE_PCT = env_float("AVERAGE_PCT", 7.7) / 100.0
STOP_AFTER_AVERAGE_PCT = env_float("STOP_AFTER_AVERAGE_PCT", 10.5) / 100.0

SEND_STARTUP_MESSAGE = env_bool("SEND_STARTUP_MESSAGE", True)
SEND_STATS_AFTER_CLOSE = env_bool("SEND_STATS_AFTER_CLOSE", True)
SEND_SCAN_ERRORS_TO_TELEGRAM = env_bool("SEND_SCAN_ERRORS_TO_TELEGRAM", False)
SEND_EMPTY_SCAN = env_bool("SEND_EMPTY_SCAN", False)

EXCLUDED_BASES = set(env_list(
    "EXCLUDED_BASES",
    "BTC,ETH,USDC,FDUSD,TUSD,DAI,USDE,USDP,USTC,BUSD"
))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(APP_NAME)

app = FastAPI(title=APP_NAME)
exchange = None
scanner_task: Optional[asyncio.Task] = None
started_at = time.time()
last_scan_summary: Dict[str, Any] = {}
last_telegram_status: Dict[str, Any] = {
    "ok": None,
    "time_utc": None,
    "http_status": None,
    "error": None,
    "response": None,
}

# -------------------------
# State / stats
# -------------------------

@dataclass
class Signal:
    symbol: str
    display_symbol: str
    base: str
    side: str  # LONG / SHORT
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


def default_stats() -> Dict[str, Any]:
    return {
        "total_signals_sent": 0,
        "closed_total": 0,
        "closed_profit": 0,
        "closed_loss": 0,
        "tp1_hits": 0,
        "all_targets": 0,
        "stop_losses": 0,
        "average_hits": 0,
        "total_closed_roi": 0.0,
        "best_roi": None,
        "worst_roi": None,
        "last_results": [],
    }


@dataclass
class BotState:
    day: str = ""
    signals_today: int = 0
    last_signal_ts: float = 0.0
    cooldowns: Dict[str, float] = field(default_factory=dict)
    active: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    stats: Dict[str, Any] = field(default_factory=default_stats)


state = BotState()


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
    state.stats["last_results"] = state.stats["last_results"][-30:]


def reset_daily_if_needed() -> None:
    today = today_key()
    if state.day != today:
        state.day = today
        state.signals_today = 0


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
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(asdict(state), f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Could not save state: %s", e)


def stat_inc(key: str, amount: int = 1) -> None:
    ensure_stats()
    state.stats[key] = int(state.stats.get(key, 0) or 0) + amount

# -------------------------
# Formatting helpers
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
    return f"{price:.9f}".rstrip("0").rstrip(".")


def fmt_pct(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def display_symbol_from_market(market: Dict[str, Any]) -> str:
    base = str(market.get("base") or "").upper()
    quote = str(market.get("quote") or "USDT").upper()
    if base:
        return f"{base}/{quote}"
    symbol = str(market.get("symbol") or "")
    return symbol.replace(":USDT", "")


def compact_symbol(display_symbol: str) -> str:
    return display_symbol.replace("/", "").replace(":", "")

# -------------------------
# Telegram
# -------------------------

async def send_telegram(text: str, silent: bool = False) -> bool:
    global last_telegram_status
    last_telegram_status = {
        "ok": None,
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "http_status": None,
        "error": None,
        "response": None,
    }

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        msg = "Telegram env missing: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID"
        last_telegram_status.update({"ok": False, "error": msg})
        logger.warning(msg)
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = []
    text = str(text)
    while len(text) > 3900:
        chunks.append(text[:3900])
        text = text[3900:]
    chunks.append(text)

    ok_all = True
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        for chunk in chunks:
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "disable_notification": silent,
            }
            try:
                async with session.post(url, json=payload) as resp:
                    body = await resp.text()
                    last_telegram_status.update({
                        "ok": 200 <= resp.status < 300,
                        "http_status": resp.status,
                        "response": body[:500],
                        "error": None if 200 <= resp.status < 300 else body[:500],
                    })
                    if not (200 <= resp.status < 300):
                        ok_all = False
                        logger.error("Telegram error %s: %s", resp.status, body[:300])
            except Exception as e:
                ok_all = False
                last_telegram_status.update({"ok": False, "error": repr(e)})
                logger.exception("Telegram send failed")
    return ok_all


def startup_message() -> str:
    token_ok = "✅" if TELEGRAM_BOT_TOKEN else "❌"
    chat_ok = "✅" if TELEGRAM_CHAT_ID else "❌"
    return (
        "🚀 <b>BingX scalp bot запущен</b>\n\n"
        f"Режим: Background Worker / uvicorn bot:app\n"
        f"Скан: каждые {SCAN_INTERVAL_SECONDS} сек\n"
        f"Монет в топе: {TOP_SYMBOLS_LIMIT}\n"
        f"MIN_SCORE: {MIN_SCORE} / fallback: {FALLBACK_SCORE}\n"
        f"Лимит сигналов/день: {MAX_SIGNALS_PER_DAY}\n"
        f"Плечо в сообщении: x{LEVERAGE}\n\n"
        f"Telegram token: {token_ok}\n"
        f"Telegram chat_id: {chat_ok}\n\n"
        "Бот только отправляет сигналы, сделки не открывает."
    )

# -------------------------
# Indicator functions without pandas/numpy
# -------------------------


def closes(ohlcv: List[List[float]]) -> List[float]:
    return [safe_float(x[4]) for x in ohlcv if len(x) >= 5]


def highs(ohlcv: List[List[float]]) -> List[float]:
    return [safe_float(x[2]) for x in ohlcv if len(x) >= 3]


def lows(ohlcv: List[List[float]]) -> List[float]:
    return [safe_float(x[3]) for x in ohlcv if len(x) >= 4]


def volumes(ohlcv: List[List[float]]) -> List[float]:
    return [safe_float(x[5]) for x in ohlcv if len(x) >= 6]


def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period or period <= 0:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) <= period:
        return None
    gains = []
    losses = []
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
        h = safe_float(ohlcv[i][2])
        l = safe_float(ohlcv[i][3])
        pc = safe_float(ohlcv[i - 1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def volume_ratio(ohlcv: List[List[float]], lookback: int = 20) -> float:
    vols = volumes(ohlcv)
    if len(vols) < lookback + 1:
        return 1.0
    avg = sum(vols[-lookback - 1:-1]) / lookback
    if avg <= 0:
        return 1.0
    return vols[-1] / avg


def vwap(ohlcv: List[List[float]], lookback: int = 48) -> Optional[float]:
    chunk = ohlcv[-lookback:]
    pv = 0.0
    vv = 0.0
    for c in chunk:
        if len(c) < 6:
            continue
        high = safe_float(c[2])
        low = safe_float(c[3])
        close = safe_float(c[4])
        vol = safe_float(c[5])
        typical = (high + low + close) / 3
        pv += typical * vol
        vv += vol
    if vv <= 0:
        return None
    return pv / vv


def last_candle_direction(ohlcv: List[List[float]]) -> str:
    if not ohlcv:
        return "flat"
    o = safe_float(ohlcv[-1][1])
    c = safe_float(ohlcv[-1][4])
    if c > o:
        return "bull"
    if c < o:
        return "bear"
    return "flat"

# -------------------------
# Exchange / scanning
# -------------------------

async def init_exchange() -> None:
    global exchange
    if exchange is not None:
        return
    exchange_class = getattr(ccxt, "bingx", None)
    if exchange_class is None:
        raise RuntimeError("Your ccxt version does not support bingx. Update requirements.txt ccxt.")
    exchange = exchange_class({
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "defaultSubType": "linear",
        },
        "timeout": 20000,
    })
    await exchange.load_markets()
    logger.info("BingX markets loaded: %s", len(exchange.markets))


def market_is_candidate(market: Dict[str, Any]) -> bool:
    if not market:
        return False
    base = str(market.get("base") or "").upper()
    quote = str(market.get("quote") or "").upper()
    active = market.get("active", True)
    is_swap = bool(market.get("swap") or market.get("type") == "swap")
    linear = market.get("linear", True)
    if not active or not is_swap or linear is False:
        return False
    if quote != "USDT":
        return False
    if base in EXCLUDED_BASES:
        return False
    return True


def ticker_quote_volume(ticker: Dict[str, Any]) -> float:
    for key in ("quoteVolume", "quoteVolume24h"):
        v = safe_float(ticker.get(key), 0.0)
        if v > 0:
            return v
    info = ticker.get("info", {}) if isinstance(ticker.get("info"), dict) else {}
    for key in ("quoteVolume", "quoteVolume24h", "turnover", "turnover24h", "amount"):
        v = safe_float(info.get(key), 0.0)
        if v > 0:
            return v
    last = safe_float(ticker.get("last"), 0.0)
    base_vol = safe_float(ticker.get("baseVolume"), 0.0)
    return last * base_vol


async def get_top_symbols() -> List[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
    await init_exchange()
    assert exchange is not None
    tickers = await exchange.fetch_tickers()
    rows: List[Tuple[str, Dict[str, Any], Dict[str, Any], float]] = []
    for symbol, ticker in tickers.items():
        market = exchange.markets.get(symbol)
        if not market_is_candidate(market):
            continue
        qv = ticker_quote_volume(ticker)
        if qv < MIN_24H_QUOTE_VOLUME_USDT:
            continue
        last = safe_float(ticker.get("last"), 0.0)
        if last <= 0:
            continue
        rows.append((symbol, market, ticker, qv))
    rows.sort(key=lambda x: x[3], reverse=True)
    return [(s, m, t) for s, m, t, _ in rows[:TOP_SYMBOLS_LIMIT]]


async def fetch_ohlcv_safe(symbol: str, timeframe: str, limit: int) -> List[List[float]]:
    try:
        assert exchange is not None
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        logger.debug("fetch_ohlcv failed %s %s: %s", symbol, timeframe, e)
        return []


def calc_metrics(ohlcv_5m: List[List[float]], ohlcv_15m: List[List[float]], ohlcv_1h: List[List[float]]) -> Optional[Dict[str, Any]]:
    if len(ohlcv_5m) < 40 or len(ohlcv_15m) < 60:
        return None

    c5 = closes(ohlcv_5m)
    c15 = closes(ohlcv_15m)
    c1h = closes(ohlcv_1h) if ohlcv_1h else []
    h15 = highs(ohlcv_15m)
    l15 = lows(ohlcv_15m)

    last = c15[-1]
    if last <= 0:
        return None

    recent_high_20 = max(h15[-21:-1]) if len(h15) >= 21 else max(h15[:-1])
    recent_low_20 = min(l15[-21:-1]) if len(l15) >= 21 else min(l15[:-1])
    recent_high_48 = max(h15[-49:-1]) if len(h15) >= 49 else max(h15[:-1])
    recent_low_48 = min(l15[-49:-1]) if len(l15) >= 49 else min(l15[:-1])

    rsi15 = rsi(c15, 14) or 50.0
    rsi5 = rsi(c5, 14) or 50.0
    ema20_15 = ema(c15, 20) or last
    ema50_15 = ema(c15, 50) or last
    ema50_1h = ema(c1h, 50) or (c1h[-1] if c1h else last)
    ema100_1h = ema(c1h, 100) or ema50_1h
    atr15 = atr(ohlcv_15m, 14) or 0.0
    vwap15 = vwap(ohlcv_15m, 48) or last
    volr15 = volume_ratio(ohlcv_15m, 20)
    volr5 = volume_ratio(ohlcv_5m, 20)

    return {
        "last": last,
        "move_5m_3": pct_change(c5[-1], c5[-4]) if len(c5) >= 4 else 0.0,
        "move_5m_6": pct_change(c5[-1], c5[-7]) if len(c5) >= 7 else 0.0,
        "move_15m_4": pct_change(c15[-1], c15[-5]) if len(c15) >= 5 else 0.0,
        "move_15m_12": pct_change(c15[-1], c15[-13]) if len(c15) >= 13 else 0.0,
        "move_15m_24": pct_change(c15[-1], c15[-25]) if len(c15) >= 25 else 0.0,
        "rsi15": rsi15,
        "rsi5": rsi5,
        "ema20_15": ema20_15,
        "ema50_15": ema50_15,
        "ema50_1h": ema50_1h,
        "ema100_1h": ema100_1h,
        "vwap15": vwap15,
        "atr15": atr15,
        "atr_pct": (atr15 / last * 100.0) if last else 0.0,
        "volr15": volr15,
        "volr5": volr5,
        "recent_high_20": recent_high_20,
        "recent_low_20": recent_low_20,
        "recent_high_48": recent_high_48,
        "recent_low_48": recent_low_48,
        "dist_to_high_20": pct_change(last, recent_high_20),
        "dist_from_low_20": pct_change(last, recent_low_20),
        "break_high_pct": pct_change(last, recent_high_20),
        "break_low_pct": pct_change(last, recent_low_20),
        "candle15": last_candle_direction(ohlcv_15m),
        "candle5": last_candle_direction(ohlcv_5m),
    }


def score_candidate(metrics: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    last = metrics["last"]
    if last <= 0:
        return None

    candidates: List[Dict[str, Any]] = []

    # LONG: strong dump -> first confirmed bounce.
    score = 0
    reasons = []
    if metrics["move_15m_24"] <= -7.0:
        score += 24; reasons.append("сильный пролив 6ч")
    elif metrics["move_15m_12"] <= -4.5:
        score += 18; reasons.append("быстрый пролив 3ч")
    if metrics["rsi15"] <= 39:
        score += 14; reasons.append("RSI15 перепродан")
    if metrics["candle15"] == "bull" and metrics["candle5"] == "bull":
        score += 14; reasons.append("бычья 15m/5m свеча")
    if metrics["move_5m_3"] >= 0.25:
        score += 14; reasons.append("отскок уже начался")
    if metrics["volr15"] >= 1.25 or metrics["volr5"] >= 1.35:
        score += 16; reasons.append("объем выше среднего")
    if metrics["dist_from_low_20"] <= 3.2:
        score += 10; reasons.append("цена рядом с локальной поддержкой")
    if last >= metrics["vwap15"] or last >= metrics["ema20_15"]:
        score += 8; reasons.append("reclaim VWAP/EMA20")
    # anti-chase
    if metrics["move_5m_6"] > 5.5:
        score -= 12; reasons.append("анти-чейз: слишком резкий отскок")
    candidates.append({"side": "LONG", "setup": "SUPPORT BOUNCE / RECLAIM", "score": score, "reasons": reasons})

    # SHORT: strong pump -> rejection.
    score = 0
    reasons = []
    if metrics["move_15m_24"] >= 8.0:
        score += 24; reasons.append("сильный памп 6ч")
    elif metrics["move_15m_12"] >= 5.0:
        score += 18; reasons.append("быстрый памп 3ч")
    if metrics["rsi15"] >= 61:
        score += 14; reasons.append("RSI15 перегрет")
    if metrics["candle15"] == "bear" and metrics["candle5"] == "bear":
        score += 14; reasons.append("медвежья 15m/5m свеча")
    if metrics["move_5m_3"] <= -0.25:
        score += 14; reasons.append("отбой уже начался")
    if metrics["volr15"] >= 1.25 or metrics["volr5"] >= 1.35:
        score += 16; reasons.append("объем выше среднего")
    if abs(metrics["dist_to_high_20"]) <= 3.2:
        score += 10; reasons.append("цена рядом с сопротивлением")
    if last <= metrics["vwap15"] or last <= metrics["ema20_15"]:
        score += 8; reasons.append("потеря VWAP/EMA20")
    if metrics["move_5m_6"] < -5.5:
        score -= 12; reasons.append("анти-чейз: падение уже большое")
    candidates.append({"side": "SHORT", "setup": "RESISTANCE REJECTION", "score": score, "reasons": reasons})

    # LONG breakout.
    score = 0
    reasons = []
    if metrics["break_high_pct"] >= 0.15:
        score += 23; reasons.append("пробой сопротивления 15m")
    if metrics["volr15"] >= 1.45:
        score += 18; reasons.append("пробой на объеме")
    if metrics["move_5m_3"] >= 0.25:
        score += 12; reasons.append("импульс после пробоя")
    if last > metrics["ema20_15"] > 0 and last > metrics["vwap15"]:
        score += 13; reasons.append("цена выше EMA20/VWAP")
    if metrics["ema50_1h"] >= metrics["ema100_1h"] * 0.985:
        score += 10; reasons.append("1h тренд не против")
    if metrics["move_15m_12"] > 12.0:
        score -= 16; reasons.append("анти-чейз после вертикали")
    candidates.append({"side": "LONG", "setup": "RESISTANCE BREAKOUT", "score": score, "reasons": reasons})

    # SHORT breakdown.
    score = 0
    reasons = []
    if metrics["break_low_pct"] <= -0.15:
        score += 23; reasons.append("пробой поддержки 15m")
    if metrics["volr15"] >= 1.45:
        score += 18; reasons.append("пролом на объеме")
    if metrics["move_5m_3"] <= -0.25:
        score += 12; reasons.append("импульс после пролома")
    if last < metrics["ema20_15"] and last < metrics["vwap15"]:
        score += 13; reasons.append("цена ниже EMA20/VWAP")
    if metrics["ema50_1h"] <= metrics["ema100_1h"] * 1.015:
        score += 10; reasons.append("1h тренд не против")
    if metrics["move_15m_12"] < -12.0:
        score -= 16; reasons.append("анти-чейз после вертикали")
    candidates.append({"side": "SHORT", "setup": "SUPPORT BREAKDOWN", "score": score, "reasons": reasons})

    best = max(candidates, key=lambda x: x["score"])
    quality = int(clamp(best["score"], 0, 99))
    if quality <= 0:
        return None
    best["quality"] = quality
    return best


def build_signal(symbol: str, market: Dict[str, Any], metrics: Dict[str, Any], scored: Dict[str, Any]) -> Signal:
    entry = safe_float(metrics["last"])
    side = scored["side"]
    display_symbol = display_symbol_from_market(market)
    base = str(market.get("base") or display_symbol.split("/")[0]).upper()

    if side == "LONG":
        targets = [entry * (1 + p) for p in TAKE_PCTS[:5]]
        average = entry * (1 - AVERAGE_PCT)
        stop = entry * (1 - STOP_AFTER_AVERAGE_PCT)
    else:
        targets = [entry * (1 - p) for p in TAKE_PCTS[:5]]
        average = entry * (1 + AVERAGE_PCT)
        stop = entry * (1 + STOP_AFTER_AVERAGE_PCT)

    return Signal(
        symbol=symbol,
        display_symbol=display_symbol,
        base=base,
        side=side,
        setup=scored["setup"],
        quality=int(scored["quality"]),
        entry=entry,
        targets=targets,
        average=average,
        stop=stop,
        leverage=LEVERAGE,
        score_reasons=list(scored.get("reasons", []))[:7],
        metrics=metrics,
    )


def signal_message(sig: Signal) -> str:
    compact = compact_symbol(sig.display_symbol)
    lines = [
        f"🔥 <b>Скальп-позиция - {sig.base} {sig.side}</b>",
        "",
        f"Биржа: BingX USDT-M Futures",
        f"Сетап: {sig.setup}",
        f"Качество: <b>{sig.quality}/100</b>",
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
        "",
        "Подтверждения: " + ", ".join(sig.score_reasons),
        "",
        f"Монета: {compact}",
        "О любых действиях по открытой сделке буду сообщать в канале.",
        "",
        "⚠️ Это сигнал, а не гарантия прибыли. Соблюдай риск."
    ]
    return "\n".join(lines)


def stats_text() -> str:
    ensure_stats()
    st = state.stats
    closed = int(st.get("closed_total", 0) or 0)
    profit = int(st.get("closed_profit", 0) or 0)
    loss = int(st.get("closed_loss", 0) or 0)
    wr = (profit / closed * 100.0) if closed else 0.0
    avg_roi = (float(st.get("total_closed_roi", 0.0) or 0.0) / closed) if closed else 0.0
    best = st.get("best_roi")
    worst = st.get("worst_roi")

    lines = [
        "📊 <b>Статистика бота</b>",
        "",
        f"Всего сигналов: <b>{int(st.get('total_signals_sent', 0) or 0)}</b>",
        f"Активные сделки: <b>{len(state.active)}</b>",
        f"Закрытые сделки: <b>{closed}</b>",
        f"✅ Прибыльные: <b>{profit}</b>",
        f"🛑 Убыточные: <b>{loss}</b>",
        f"Win rate: <b>{wr:.1f}%</b>",
        "",
        f"TP1 достигнут: {int(st.get('tp1_hits', 0) or 0)}",
        f"Все 5 целей: {int(st.get('all_targets', 0) or 0)}",
        f"Усреднений: {int(st.get('average_hits', 0) or 0)}",
        f"Стопов: {int(st.get('stop_losses', 0) or 0)}",
        "",
        f"Средний ROI по закрытым: {fmt_pct(avg_roi)}",
        f"Лучший ROI: {fmt_pct(best) if best is not None else '—'}",
        f"Худший ROI: {fmt_pct(worst) if worst is not None else '—'}",
    ]
    recent = st.get("last_results", [])[-5:]
    if recent:
        lines.append("")
        lines.append("Последние закрытые:")
        for r in reversed(recent):
            icon = "✅" if r.get("result") == "profit" else "🛑"
            lines.append(f"{icon} {r.get('symbol')} {r.get('side')} · {r.get('reason')} · {fmt_pct(safe_float(r.get('roi')))}")
    return "\n".join(lines)


def is_on_cooldown(symbol: str) -> bool:
    until = safe_float(state.cooldowns.get(symbol), 0.0)
    return until > time.time()


def put_cooldown(symbol: str) -> None:
    state.cooldowns[symbol] = time.time() + COOLDOWN_MINUTES * 60


def cleanup_cooldowns() -> None:
    now = time.time()
    state.cooldowns = {s: ts for s, ts in state.cooldowns.items() if safe_float(ts) > now}


async def analyze_symbol(symbol: str, market: Dict[str, Any]) -> Optional[Signal]:
    ohlcv_5m, ohlcv_15m, ohlcv_1h = await asyncio.gather(
        fetch_ohlcv_safe(symbol, "5m", 90),
        fetch_ohlcv_safe(symbol, "15m", 120),
        fetch_ohlcv_safe(symbol, "1h", 140),
    )
    metrics = calc_metrics(ohlcv_5m, ohlcv_15m, ohlcv_1h)
    if not metrics:
        return None
    scored = score_candidate(metrics)
    if not scored:
        return None
    return build_signal(symbol, market, metrics, scored)


async def scan_once() -> Dict[str, Any]:
    global last_scan_summary
    reset_daily_if_needed()
    cleanup_cooldowns()

    await init_exchange()
    assert exchange is not None

    # First update active positions.
    await update_active_signals()

    top = await get_top_symbols()
    candidates: List[Signal] = []
    checked = 0

    # Scan sequentially with a little delay to respect rate limits.
    for symbol, market, _ticker in top:
        checked += 1
        if state.signals_today >= MAX_SIGNALS_PER_DAY:
            break
        if symbol in state.active or is_on_cooldown(symbol):
            continue
        try:
            sig = await analyze_symbol(symbol, market)
            if sig:
                candidates.append(sig)
        except Exception as e:
            logger.debug("Analyze failed %s: %s", symbol, e)
        await asyncio.sleep(0.05)

    candidates.sort(key=lambda s: s.quality, reverse=True)
    threshold = MIN_SCORE
    if state.signals_today < DAILY_MIN_SIGNALS:
        threshold = min(MIN_SCORE, FALLBACK_SCORE)

    sent = 0
    for sig in candidates:
        if sig.quality < threshold:
            continue
        if state.signals_today >= MAX_SIGNALS_PER_DAY:
            break
        if sig.symbol in state.active or is_on_cooldown(sig.symbol):
            continue
        ok = await send_telegram(signal_message(sig))
        if ok:
            state.active[sig.symbol] = asdict(sig)
            state.signals_today += 1
            state.last_signal_ts = time.time()
            stat_inc("total_signals_sent")
            put_cooldown(sig.symbol)
            save_state()
            sent += 1
            logger.info("Signal sent: %s %s quality=%s", sig.display_symbol, sig.side, sig.quality)
        await asyncio.sleep(1.0)

    last_scan_summary = {
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "checked": checked,
        "candidates": len(candidates),
        "sent": sent,
        "signals_today": state.signals_today,
        "active": len(state.active),
        "threshold_used": threshold,
        "best_candidates": [
            {
                "symbol": s.display_symbol,
                "side": s.side,
                "setup": s.setup,
                "quality": s.quality,
                "entry": s.entry,
            }
            for s in candidates[:10]
        ],
    }
    logger.info("Scan: checked=%s candidates=%s sent=%s active=%s", checked, len(candidates), sent, len(state.active))

    if SEND_EMPTY_SCAN and sent == 0:
        best = candidates[0] if candidates else None
        msg = (
            "🧪 <b>Scan update</b>\n"
            f"Проверено: {checked}\n"
            f"Кандидатов: {len(candidates)}\n"
            f"Отправлено: {sent}\n"
            f"Active: {len(state.active)}\n"
            f"Best: {best.display_symbol + ' ' + best.side + ' ' + str(best.quality) if best else 'нет'}"
        )
        await send_telegram(msg, silent=True)
    return last_scan_summary


async def current_price(symbol: str) -> Optional[float]:
    try:
        assert exchange is not None
        ticker = await exchange.fetch_ticker(symbol)
        price = safe_float(ticker.get("last"), 0.0)
        return price if price > 0 else None
    except Exception as e:
        logger.debug("fetch_ticker active failed %s: %s", symbol, e)
        return None


def signal_from_dict(data: Dict[str, Any]) -> Signal:
    return Signal(
        symbol=data.get("symbol", ""),
        display_symbol=data.get("display_symbol") or data.get("symbol", ""),
        base=data.get("base", ""),
        side=data.get("side", "LONG"),
        setup=data.get("setup", ""),
        quality=int(data.get("quality", 0) or 0),
        entry=safe_float(data.get("entry")),
        targets=[safe_float(x) for x in data.get("targets", [])],
        average=safe_float(data.get("average")),
        stop=safe_float(data.get("stop")),
        leverage=int(data.get("leverage", LEVERAGE) or LEVERAGE),
        score_reasons=list(data.get("score_reasons", [])),
        metrics=dict(data.get("metrics", {})),
        created_ts=safe_float(data.get("created_ts"), time.time()),
        hit_targets=[int(x) for x in data.get("hit_targets", [])],
        average_hit=bool(data.get("average_hit", False)),
        closed=bool(data.get("closed", False)),
    )


def trade_roi(sig: Signal, exit_price: float) -> float:
    if sig.entry <= 0:
        return 0.0
    if sig.side == "LONG":
        return pct_change(exit_price, sig.entry) * sig.leverage
    return pct_change(sig.entry, exit_price) * sig.leverage


def close_signal(symbol: str, sig: Signal, reason: str, result: str, exit_price: float) -> None:
    roi = trade_roi(sig, exit_price)
    ensure_stats()
    stat_inc("closed_total")
    if result == "profit":
        stat_inc("closed_profit")
    else:
        stat_inc("closed_loss")
    if "STOP" in reason.upper():
        stat_inc("stop_losses")
    if len(sig.hit_targets) >= 5:
        stat_inc("all_targets")
    state.stats["total_closed_roi"] = float(state.stats.get("total_closed_roi", 0.0) or 0.0) + roi
    best = state.stats.get("best_roi")
    worst = state.stats.get("worst_roi")
    state.stats["best_roi"] = roi if best is None else max(float(best), roi)
    state.stats["worst_roi"] = roi if worst is None else min(float(worst), roi)
    state.stats["last_results"].append({
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": sig.display_symbol,
        "side": sig.side,
        "reason": reason,
        "result": result,
        "roi": roi,
        "entry": sig.entry,
        "exit": exit_price,
    })
    state.stats["last_results"] = state.stats["last_results"][-30:]
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

        # Average hit.
        if not sig.average_hit:
            if (sig.side == "LONG" and price <= sig.average) or (sig.side == "SHORT" and price >= sig.average):
                sig.average_hit = True
                stat_inc("average_hits")
                changed = True
                await send_telegram(
                    f"⚠️ <b>Усреднение активировано</b>\n"
                    f"{sig.display_symbol} {sig.side}\n"
                    f"Цена дошла до: <b>{fmt_price(sig.average)}</b>\n"
                    f"Текущая цена: {fmt_price(price)}\n"
                    "Риск держи строго по плану."
                )

        # Target hits.
        for i, target in enumerate(sig.targets, start=1):
            if i in sig.hit_targets:
                continue
            hit = (sig.side == "LONG" and price >= target) or (sig.side == "SHORT" and price <= target)
            if hit:
                sig.hit_targets.append(i)
                changed = True
                if i == 1:
                    stat_inc("tp1_hits")
                roi = trade_roi(sig, target)
                await send_telegram(
                    f"✅ <b>TP{i} достигнут</b>\n"
                    f"{sig.display_symbol} {sig.side}\n"
                    f"Цель: <b>{fmt_price(target)}</b>\n"
                    f"ROI по плечу x{sig.leverage}: <b>{fmt_pct(roi)}</b>"
                )

        # Full close on TP5.
        if len(sig.hit_targets) >= 5:
            close_signal(symbol, sig, "TP5", "profit", sig.targets[-1])
            await send_telegram(
                f"🏆 <b>Все 5 целей взяты</b>\n"
                f"{sig.display_symbol} {sig.side}\n"
                f"Entry: {fmt_price(sig.entry)}\n"
                f"TP5: {fmt_price(sig.targets[-1])}\n"
                f"Итог ROI x{sig.leverage}: <b>{fmt_pct(trade_roi(sig, sig.targets[-1]))}</b>"
            )
            if SEND_STATS_AFTER_CLOSE:
                await send_telegram(stats_text(), silent=True)
            continue

        # Stop.
        stop_hit = (sig.side == "LONG" and price <= sig.stop) or (sig.side == "SHORT" and price >= sig.stop)
        if stop_hit:
            result = "profit" if 1 in sig.hit_targets else "loss"
            reason = "STOP after TP" if result == "profit" else "STOP before TP1"
            close_signal(symbol, sig, reason, result, price)
            icon = "✅" if result == "profit" else "🛑"
            await send_telegram(
                f"{icon} <b>{'Сделка закрыта в плюс' if result == 'profit' else 'Stop Loss'}</b>\n"
                f"{sig.display_symbol} {sig.side}\n"
                f"Entry: {fmt_price(sig.entry)}\n"
                f"Exit: {fmt_price(price)}\n"
                f"ROI x{sig.leverage}: <b>{fmt_pct(trade_roi(sig, price))}</b>"
            )
            if SEND_STATS_AFTER_CLOSE:
                await send_telegram(stats_text(), silent=True)
            continue

        state.active[symbol] = asdict(sig)
    if changed:
        save_state()


async def scanner_loop() -> None:
    # Never let the worker die on one scan error.
    while True:
        try:
            await scan_once()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            err = "".join(traceback.format_exception_only(type(e), e)).strip()
            logger.error("Scanner loop error: %s\n%s", err, traceback.format_exc())
            if SEND_SCAN_ERRORS_TO_TELEGRAM:
                await send_telegram(f"⚠️ Ошибка сканера:\n<code>{err}</code>", silent=True)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


@app.on_event("startup")
async def on_startup() -> None:
    global scanner_task
    load_state()
    logger.info("Starting %s", APP_NAME)
    if SEND_STARTUP_MESSAGE:
        await send_telegram(startup_message())
    # Start scanner after startup; do not crash Render if first exchange connection fails.
    scanner_task = asyncio.create_task(scanner_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global exchange, scanner_task
    save_state()
    if scanner_task:
        scanner_task.cancel()
    if exchange is not None:
        try:
            await exchange.close()
        except Exception:
            pass
        exchange = None


@app.get("/")
async def root() -> JSONResponse:
    return JSONResponse({
        "ok": True,
        "app": APP_NAME,
        "mode": "uvicorn bot:app",
        "uptime_sec": int(time.time() - started_at),
        "active": len(state.active),
        "signals_today": state.signals_today,
        "last_scan": last_scan_summary,
    })


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"ok": True, "active": len(state.active), "telegram": last_telegram_status})


@app.get("/stats")
async def stats() -> JSONResponse:
    ensure_stats()
    return JSONResponse({"ok": True, "stats": state.stats, "active": state.active})


@app.get("/stats/send")
async def send_stats() -> JSONResponse:
    ok = await send_telegram(stats_text())
    return JSONResponse({"ok": ok, "telegram": last_telegram_status})


@app.get("/telegram/test")
async def telegram_test() -> JSONResponse:
    ok = await send_telegram("✅ Telegram test: бот может отправлять сообщения.")
    return JSONResponse({"ok": ok, "telegram": last_telegram_status})


@app.get("/debug")
async def debug() -> JSONResponse:
    return JSONResponse({
        "ok": True,
        "env": {
            "TELEGRAM_BOT_TOKEN_present": bool(TELEGRAM_BOT_TOKEN),
            "TELEGRAM_CHAT_ID_present": bool(TELEGRAM_CHAT_ID),
            "SCAN_INTERVAL_SECONDS": SCAN_INTERVAL_SECONDS,
            "TOP_SYMBOLS_LIMIT": TOP_SYMBOLS_LIMIT,
            "MIN_SCORE": MIN_SCORE,
            "FALLBACK_SCORE": FALLBACK_SCORE,
            "DAILY_MIN_SIGNALS": DAILY_MIN_SIGNALS,
            "MAX_SIGNALS_PER_DAY": MAX_SIGNALS_PER_DAY,
            "LEVERAGE": LEVERAGE,
        },
        "last_telegram_status": last_telegram_status,
        "last_scan_summary": last_scan_summary,
    })


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000") or "8000")
    uvicorn.run("bot:app", host="0.0.0.0", port=port, reload=False)
