"""
BingX Scalp Signal Bot for the user's EXISTING Render Background Worker.

Render Start Command that this file supports:
uvicorn bot:app --host 0.0.0.0 --port 1000

Important:
- This version DOES NOT use ccxt. It uses BingX public REST with aiohttp.
- It sends Telegram startup notification.
- It scans BingX USDT-M swap/futures markets.
- It sends scalp setups in the style of the examples:
  entry, 5 take-profit limit levels, averaging level, protective stop.
- It tracks TP / averaging / SL and keeps profit/loss statistics.

This is a signal bot only. It never opens trades.
"""

import asyncio
import json
import logging
import math
import os
import random
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

# =============================================================================
# CONFIG
# =============================================================================

APP_NAME = "BingX Scalp Signal Bot"
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")

TELEGRAM_BOT_TOKEN = (
    os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
    or os.getenv("TG_BOT_TOKEN")
    or ""
).strip()

TELEGRAM_CHAT_ID = (
    os.getenv("TELEGRAM_CHAT_ID")
    or os.getenv("CHAT_ID")
    or os.getenv("TG_CHAT_ID")
    or ""
).strip()

SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() in ("1", "true", "yes", "y", "on")
SEND_STATS_AFTER_CLOSE = os.getenv("SEND_STATS_AFTER_CLOSE", "true").lower() in ("1", "true", "yes", "y", "on")
SEND_SCANNER_ERRORS_TO_TELEGRAM = os.getenv("SEND_SCANNER_ERRORS_TO_TELEGRAM", "true").lower() in ("1", "true", "yes", "y", "on")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "180"))
MONITOR_INTERVAL_SECONDS = int(os.getenv("MONITOR_INTERVAL_SECONDS", "60"))
TOP_SYMBOLS_LIMIT = int(os.getenv("TOP_SYMBOLS_LIMIT", "120"))
MIN_SCORE = float(os.getenv("MIN_SCORE", "78"))
FALLBACK_SCORE = float(os.getenv("FALLBACK_SCORE", "74"))
DAILY_MIN_SIGNALS = int(os.getenv("DAILY_MIN_SIGNALS", "1"))
MAX_SIGNALS_PER_DAY = int(os.getenv("MAX_SIGNALS_PER_DAY", "6"))
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "180"))
SYMBOL_COOLDOWN_MINUTES = int(os.getenv("SYMBOL_COOLDOWN_MINUTES", "360"))
MAX_ACTIVE_TRADES = int(os.getenv("MAX_ACTIVE_TRADES", "8"))
LEVERAGE = float(os.getenv("LEVERAGE", "20"))

# Precise mode: the bot does NOT send a signal immediately after one scan.
# It first finds a structural setup on a CLOSED candle, stores it as pending,
# then sends only if the same symbol/side is confirmed again on the next scan.
STRICT_CONFIRMATION = os.getenv("STRICT_CONFIRMATION", "true").lower() in ("1", "true", "yes", "y", "on")
CONFIRMATION_SCANS = int(os.getenv("CONFIRMATION_SCANS", "2"))
PENDING_EXPIRE_SECONDS = int(os.getenv("PENDING_EXPIRE_SECONDS", "1200"))
MIN_VOLUME_RATIO_CONFIRM = float(os.getenv("MIN_VOLUME_RATIO_CONFIRM", "1.10"))
MIN_ATR_PCT = float(os.getenv("MIN_ATR_PCT", "0.20"))
MAX_ATR_PCT = float(os.getenv("MAX_ATR_PCT", "5.00"))
MAX_ENTRY_DISTANCE_ATR = float(os.getenv("MAX_ENTRY_DISTANCE_ATR", "1.35"))
MIN_REJECTION_WICK_RATIO = float(os.getenv("MIN_REJECTION_WICK_RATIO", "0.25"))
MIN_CLOSE_LOCATION = float(os.getenv("MIN_CLOSE_LOCATION", "0.58"))
ANTI_CHASE_8_CANDLE_MOVE = float(os.getenv("ANTI_CHASE_8_CANDLE_MOVE", "9.0"))
SEND_PENDING_LOGS_TO_TELEGRAM = os.getenv("SEND_PENDING_LOGS_TO_TELEGRAM", "false").lower() in ("1", "true", "yes", "y", "on")

# Example-like ladder: around 0.8% to 4.0% price movement.
TP_PCTS = [0.008, 0.014, 0.020, 0.030, 0.040]
AVERAGING_PCT = float(os.getenv("AVERAGING_PCT", "0.077"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.105"))

MIN_QUOTE_VOLUME_USDT = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "500000"))
MAX_24H_MOVE_ABS = float(os.getenv("MAX_24H_MOVE_ABS", "45"))

BINGX_BASES = [
    "https://open-api.bingx.com",
]

BINGX_CONTRACTS_ENDPOINTS = [
    "/openApi/swap/v2/quote/contracts",
]

BINGX_TICKER_ENDPOINTS = [
    "/openApi/swap/v2/quote/ticker",
]

BINGX_KLINE_ENDPOINTS = [
    "/openApi/swap/v3/quote/klines",
    "/openApi/swap/v2/quote/klines",
]

DEFAULT_SYMBOLS = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT", "DOGE-USDT",
    "ADA-USDT", "AVAX-USDT", "LINK-USDT", "SUI-USDT", "APT-USDT", "ARB-USDT",
    "OP-USDT", "NEAR-USDT", "INJ-USDT", "TIA-USDT", "SEI-USDT", "WIF-USDT",
    "PEPE-USDT", "FET-USDT", "RNDR-USDT", "JUP-USDT", "ORDI-USDT", "UNI-USDT",
    "AERO-USDT", "GRASS-USDT", "PUMP-USDT", "BEAT-USDT", "BLESS-USDT", "LAB-USDT",
]

EXCLUDED_WORDS = (
    "USDC", "FDUSD", "TUSD", "DAI", "UST", "BUSD", "EUR", "TRY",
)

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("bingx_bot")

# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class Trade:
    id: str
    symbol: str
    side: str  # LONG / SHORT
    entry: float
    tps: List[float]
    averaging: float
    stop: float
    score: float
    strategy: str
    created_ts: float
    status: str = "active"
    reached_tps: List[int] = field(default_factory=list)
    averaged: bool = False
    closed_ts: Optional[float] = None
    close_reason: Optional[str] = None
    best_roi_pct: float = 0.0
    worst_roi_pct: float = 0.0
    telegram_message_id: Optional[int] = None

@dataclass
class Stats:
    total_signals: int = 0
    closed: int = 0
    profitable: int = 0
    losing: int = 0
    tp1_hits: int = 0
    tp5_hits: int = 0
    averaged_count: int = 0
    stop_count: int = 0
    best_roi_pct: float = 0.0
    worst_roi_pct: float = 0.0
    last_closed: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class State:
    active_trades: Dict[str, Trade] = field(default_factory=dict)
    stats: Stats = field(default_factory=Stats)
    last_signal_ts_by_symbol: Dict[str, float] = field(default_factory=dict)
    sent_today: int = 0
    day_key: str = ""

state = State()
state_lock = asyncio.Lock()
started_at = time.time()
scanner_task: Optional[asyncio.Task] = None
monitor_task: Optional[asyncio.Task] = None
last_scanner_error_ts = 0.0
last_scan_info: Dict[str, Any] = {}
pending_candidates: Dict[str, Dict[str, Any]] = {}
pending_lock = asyncio.Lock()

# =============================================================================
# FASTAPI APP
# =============================================================================

app = FastAPI(title=APP_NAME)

# =============================================================================
# UTILS
# =============================================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def today_key() -> str:
    return now_utc().strftime("%Y-%m-%d")


def fmt_price(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 10:
        return f"{x:.3f}"
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.1:
        return f"{x:.5f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.8f}".rstrip("0").rstrip(".")


def display_symbol(symbol: str) -> str:
    return symbol.replace("-", "").replace("/", "")


def percent_change(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return (b - a) / a * 100.0


def side_profit_pct(side: str, entry: float, price: float) -> float:
    if entry <= 0:
        return 0.0
    if side == "LONG":
        return (price - entry) / entry * 100.0
    return (entry - price) / entry * 100.0


def roi_pct(side: str, entry: float, price: float) -> float:
    return side_profit_pct(side, entry, price) * LEVERAGE


def is_usdt_symbol(symbol: str) -> bool:
    s = symbol.upper()
    if not (s.endswith("-USDT") or s.endswith("/USDT") or s.endswith("USDT")):
        return False
    return not any(w in s.replace("USDT", "") for w in EXCLUDED_WORDS)


def normalize_symbol(s: str) -> str:
    s = str(s).upper().strip()
    s = s.replace("/", "-")
    if s.endswith("USDT") and "-" not in s:
        s = s[:-4] + "-USDT"
    return s


def interval_to_ms(interval: str) -> int:
    table = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
        "4h": 14_400_000,
        "1d": 86_400_000,
    }
    return table.get(interval, 60_000)

# =============================================================================
# STATE
# =============================================================================

def state_to_json() -> Dict[str, Any]:
    return {
        "active_trades": {k: asdict(v) for k, v in state.active_trades.items()},
        "stats": asdict(state.stats),
        "last_signal_ts_by_symbol": state.last_signal_ts_by_symbol,
        "sent_today": state.sent_today,
        "day_key": state.day_key,
    }


def load_state() -> None:
    global state
    p = Path(STATE_FILE)
    if not p.exists():
        state = State(day_key=today_key())
        return
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        active = {
            k: Trade(**v) for k, v in raw.get("active_trades", {}).items()
        }
        stats = Stats(**raw.get("stats", {}))
        state = State(
            active_trades=active,
            stats=stats,
            last_signal_ts_by_symbol=raw.get("last_signal_ts_by_symbol", {}),
            sent_today=int(raw.get("sent_today", 0)),
            day_key=raw.get("day_key") or today_key(),
        )
        if state.day_key != today_key():
            state.day_key = today_key()
            state.sent_today = 0
        logger.info("State loaded: active=%s closed=%s", len(state.active_trades), state.stats.closed)
    except Exception as e:
        logger.exception("Failed to load state: %s", e)
        state = State(day_key=today_key())


def save_state() -> None:
    try:
        Path(STATE_FILE).write_text(json.dumps(state_to_json(), ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.exception("Failed to save state: %s", e)

# =============================================================================
# TELEGRAM
# =============================================================================

async def telegram_request(method: str, payload: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is empty")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            text = await resp.text()
            try:
                data = json.loads(text)
            except Exception:
                data = {"ok": False, "raw": text}
            if resp.status >= 400 or not data.get("ok", False):
                description = data.get("description") or text
                raise RuntimeError(f"Telegram {method} failed: HTTP {resp.status}: {description}")
            return data


async def send_telegram(text: str, disable_web_page_preview: bool = True) -> Optional[int]:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error(
            "Telegram is not configured: token_present=%s chat_present=%s",
            bool(TELEGRAM_BOT_TOKEN),
            bool(TELEGRAM_CHAT_ID),
        )
        return None
    try:
        data = await telegram_request(
            "sendMessage",
            {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": disable_web_page_preview,
            },
        )
        message_id = data.get("result", {}).get("message_id")
        logger.info("Telegram message sent successfully: message_id=%s", message_id)
        return message_id
    except Exception as e:
        logger.error("Telegram send failed: %s", e)
        return None


async def telegram_get_me() -> str:
    try:
        data = await telegram_request("getMe", {})
        user = data.get("result", {})
        return f"OK @{user.get('username')} id={user.get('id')}"
    except Exception as e:
        return f"ERROR {e}"


async def send_startup_message() -> None:
    if not SEND_STARTUP_MESSAGE:
        return
    get_me = await telegram_get_me()
    msg = (
        "🚀 <b>BingX scalp signal bot запущен на Render</b>\n\n"
        "Service: <b>Background Worker</b>\n"
        "Start command: <code>uvicorn bot:app --host 0.0.0.0 --port 1000</code>\n"
        f"Scanner: каждые <b>{SCAN_INTERVAL_SECONDS}</b> сек\n"
        f"Score: <b>{MIN_SCORE:.0f}</b> / fallback <b>{FALLBACK_SCORE:.0f}</b>\n"
        f"Плечо в расчетах: <b>x{LEVERAGE:.0f}</b>\n"
        f"Режим: <b>точные отскоки/отбои + {CONFIRMATION_SCANS} проверки</b>\n"
        "Data: BingX public REST, <b>без ccxt</b>\n"
        f"Telegram getMe: <code>{get_me}</code>\n\n"
        "Если ты видишь это сообщение — Telegram подключен правильно."
    )
    await send_telegram(msg)

# =============================================================================
# BINGX HTTP
# =============================================================================

async def http_get_json(session: aiohttp.ClientSession, url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 12) -> Dict[str, Any]:
    async with session.get(url, params=params or {}, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
        text = await resp.text()
        try:
            data = json.loads(text)
        except Exception as e:
            raise RuntimeError(f"JSON parse failed: status={resp.status}, text={text[:300]}") from e
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status}: {text[:300]}")
        return data


async def bingx_get(session: aiohttp.ClientSession, endpoints: List[str], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    last_error = None
    for base in BINGX_BASES:
        for endpoint in endpoints:
            url = base + endpoint
            try:
                data = await http_get_json(session, url, params=params)
                # BingX normally returns code=0. Some endpoints may return data directly.
                code = data.get("code") if isinstance(data, dict) else None
                if code in (0, "0", None):
                    return data
                last_error = RuntimeError(f"BingX code={code}: {data}")
            except Exception as e:
                last_error = e
                continue
    raise RuntimeError(f"All BingX endpoints failed. Last error: {last_error}")


def extract_data(payload: Any) -> Any:
    if isinstance(payload, dict):
        if "data" in payload:
            return payload["data"]
        return payload
    return payload


async def fetch_contract_symbols(session: aiohttp.ClientSession) -> List[str]:
    try:
        payload = await bingx_get(session, BINGX_CONTRACTS_ENDPOINTS)
        data = extract_data(payload)
        symbols: List[str] = []
        if isinstance(data, dict):
            data = data.get("contracts") or data.get("symbols") or data.get("list") or []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    raw = item.get("symbol") or item.get("contractId") or item.get("name")
                    if not raw:
                        continue
                    sym = normalize_symbol(raw)
                    # Prefer currently tradeable contracts if status field exists.
                    status = str(item.get("status", item.get("state", ""))).lower()
                    if status and any(bad in status for bad in ("offline", "suspend", "delist")):
                        continue
                    if is_usdt_symbol(sym):
                        symbols.append(sym)
                elif isinstance(item, str):
                    sym = normalize_symbol(item)
                    if is_usdt_symbol(sym):
                        symbols.append(sym)
        symbols = sorted(set(symbols))
        return symbols or DEFAULT_SYMBOLS
    except Exception as e:
        logger.warning("fetch_contract_symbols failed, using DEFAULT_SYMBOLS: %s", e)
        return DEFAULT_SYMBOLS


async def fetch_tickers(session: aiohttp.ClientSession) -> Dict[str, Dict[str, Any]]:
    try:
        payload = await bingx_get(session, BINGX_TICKER_ENDPOINTS)
        data = extract_data(payload)
        if isinstance(data, dict):
            data = data.get("tickers") or data.get("ticker") or data.get("list") or data.get("data") or []
        result: Dict[str, Dict[str, Any]] = {}
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                raw_symbol = item.get("symbol") or item.get("s")
                if not raw_symbol:
                    continue
                sym = normalize_symbol(raw_symbol)
                if not is_usdt_symbol(sym):
                    continue
                result[sym] = item
        return result
    except Exception as e:
        logger.warning("fetch_tickers failed: %s", e)
        return {}


def parse_quote_volume(t: Dict[str, Any]) -> float:
    keys = [
        "quoteVolume", "quoteVol", "amount", "turnover", "volumeUsd", "volumeUSDT",
        "quoteVolume24h", "quoteVolume24H", "volUsd", "q",
    ]
    for k in keys:
        try:
            if k in t and t[k] not in (None, ""):
                return float(t[k])
        except Exception:
            pass
    # If only base volume and last price are available.
    vol = 0.0
    price = 0.0
    for k in ("volume", "baseVolume", "vol", "v"):
        try:
            if k in t and t[k] not in (None, ""):
                vol = float(t[k])
                break
        except Exception:
            pass
    for k in ("lastPrice", "last", "price", "close", "c"):
        try:
            if k in t and t[k] not in (None, ""):
                price = float(t[k])
                break
        except Exception:
            pass
    return vol * price


def parse_price_change_24h(t: Dict[str, Any]) -> float:
    for k in ("priceChangePercent", "priceChangeRate", "change", "changePercent", "priceChangePcnt"):
        try:
            if k in t and t[k] not in (None, ""):
                x = float(t[k])
                # Some APIs return rate 0.05 instead of 5%.
                if abs(x) < 1.0:
                    return x * 100.0
                return x
        except Exception:
            pass
    return 0.0


async def get_top_symbols(session: aiohttp.ClientSession) -> List[str]:
    symbols = await fetch_contract_symbols(session)
    tickers = await fetch_tickers(session)
    ranked: List[Tuple[str, float, float]] = []
    for sym in symbols:
        t = tickers.get(sym, {})
        qv = parse_quote_volume(t) if t else 0.0
        ch = parse_price_change_24h(t) if t else 0.0
        if qv and qv < MIN_QUOTE_VOLUME_USDT:
            continue
        if abs(ch) > MAX_24H_MOVE_ABS:
            continue
        ranked.append((sym, qv, abs(ch)))
    if not ranked:
        ranked = [(s, 0.0, 0.0) for s in symbols]
    ranked.sort(key=lambda x: (x[1], x[2]), reverse=True)
    selected = [x[0] for x in ranked[:TOP_SYMBOLS_LIMIT]]
    # Always include example-like / hot symbols if available.
    for s in DEFAULT_SYMBOLS:
        if s not in selected and s in symbols and len(selected) < TOP_SYMBOLS_LIMIT:
            selected.append(s)
    return selected[:TOP_SYMBOLS_LIMIT]


def parse_kline_item(item: Any) -> Optional[Dict[str, float]]:
    try:
        if isinstance(item, dict):
            ts = item.get("time") or item.get("openTime") or item.get("t") or item.get("T") or item.get("id") or 0
            o = item.get("open") or item.get("o")
            h = item.get("high") or item.get("h")
            l = item.get("low") or item.get("l")
            c = item.get("close") or item.get("c")
            v = item.get("volume") or item.get("vol") or item.get("v") or item.get("amount") or 0
            if o is None or h is None or l is None or c is None:
                return None
            return {"ts": float(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": float(v)}
        if isinstance(item, (list, tuple)) and len(item) >= 5:
            # Common order: [time, open, high, low, close, volume]
            # Some APIs use [open, close, high, low, volume, time]. Try to detect timestamp.
            vals = list(item)
            first = float(vals[0])
            if first > 1_000_000_000:  # timestamp first
                ts = first
                o, h, l, c = float(vals[1]), float(vals[2]), float(vals[3]), float(vals[4])
                v = float(vals[5]) if len(vals) > 5 else 0.0
            else:
                # Fallback guess.
                ts = float(vals[5]) if len(vals) > 5 and float(vals[5]) > 1_000_000_000 else 0.0
                o, h, l, c = float(vals[0]), float(vals[2]), float(vals[3]), float(vals[1])
                v = float(vals[4]) if len(vals) > 4 else 0.0
            return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}
    except Exception:
        return None
    return None


def parse_klines(payload: Any) -> List[Dict[str, float]]:
    data = extract_data(payload)
    if isinstance(data, dict):
        data = data.get("klines") or data.get("kline") or data.get("list") or data.get("candles") or []
    out: List[Dict[str, float]] = []
    if isinstance(data, list):
        for item in data:
            k = parse_kline_item(item)
            if k and k["open"] > 0 and k["high"] > 0 and k["low"] > 0 and k["close"] > 0:
                out.append(k)
    # Remove duplicates and sort by ts when possible.
    if out and any(k["ts"] for k in out):
        out.sort(key=lambda x: x["ts"])
    return out


async def fetch_klines(session: aiohttp.ClientSession, symbol: str, interval: str = "15m", limit: int = 120) -> List[Dict[str, float]]:
    sym = normalize_symbol(symbol)
    params_variants = [
        {"symbol": sym, "interval": interval, "limit": limit},
        {"symbol": sym.replace("-", ""), "interval": interval, "limit": limit},
    ]
    last_error = None
    for params in params_variants:
        try:
            payload = await bingx_get(session, BINGX_KLINE_ENDPOINTS, params=params)
            klines = parse_klines(payload)
            if len(klines) >= 20:
                return klines[-limit:]
            last_error = RuntimeError(f"Not enough klines: {len(klines)}")
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(f"fetch_klines failed for {symbol} {interval}: {last_error}")

# =============================================================================
# INDICATORS
# =============================================================================

def sma(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    return sum(values[-period:]) / period


def ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2 / (period + 1)
    e = values[0]
    for x in values[1:]:
        e = x * k + e * (1 - k)
    return e


def rsi(values: List[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0
    gains = []
    losses = []
    for i in range(-period, 0):
        delta = values[i] - values[i - 1]
        if delta >= 0:
            gains.append(delta)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(delta))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(klines: List[Dict[str, float]], period: int = 14) -> float:
    if len(klines) < 2:
        return 0.0
    trs = []
    for i in range(1, len(klines)):
        h = klines[i]["high"]
        l = klines[i]["low"]
        pc = klines[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return 0.0
    if len(trs) < period:
        return sum(trs) / len(trs)
    return sum(trs[-period:]) / period


def volume_ratio(klines: List[Dict[str, float]], period: int = 20) -> float:
    vols = [k["volume"] for k in klines if k.get("volume", 0) >= 0]
    if len(vols) < 3:
        return 1.0
    base = vols[-period - 1:-1] if len(vols) > period + 1 else vols[:-1]
    avg = sum(base) / len(base) if base else 0.0
    if avg <= 0:
        return 1.0
    return vols[-1] / avg


def candle_body_pct(k: Dict[str, float]) -> float:
    o, c = k["open"], k["close"]
    if o <= 0:
        return 0.0
    return abs(c - o) / o * 100.0



def closed_klines(klines: List[Dict[str, float]], interval: str) -> List[Dict[str, float]]:
    """Return only closed candles when timestamps allow detection.

    BingX may include the currently forming candle as the last kline. For precise
    scalp entries we do not want to judge a setup by an unfinished candle.
    If timestamps are missing, the function returns the input unchanged.
    """
    if not klines:
        return klines
    last_ts = klines[-1].get("ts", 0) or 0
    if last_ts <= 0:
        return klines
    if last_ts < 10_000_000_000:
        last_ts *= 1000
    now_ms = time.time() * 1000
    ms = interval_to_ms(interval)
    if now_ms - last_ts < ms * 0.92 and len(klines) > 1:
        return klines[:-1]
    return klines


def candle_position(k: Dict[str, float]) -> Tuple[float, float, float, float]:
    """Return lower wick ratio, upper wick ratio, close-location-high, close-location-low."""
    o, h, l, c = k["open"], k["high"], k["low"], k["close"]
    rng = max(h - l, c * 0.0001)
    lower_wick = (min(o, c) - l) / rng
    upper_wick = (h - max(o, c)) / rng
    close_near_high = (c - l) / rng
    close_near_low = (h - c) / rng
    return lower_wick, upper_wick, close_near_high, close_near_low


def local_levels(k15: List[Dict[str, float]]) -> Dict[str, float]:
    """Compute recent support/resistance from closed 15m candles excluding signal candles."""
    lookback = k15[-54:-4] if len(k15) >= 58 else k15[:-4]
    if len(lookback) < 20:
        lookback = k15[:-2]
    highs = sorted([k["high"] for k in lookback])
    lows = sorted([k["low"] for k in lookback])
    if not highs or not lows:
        price = k15[-1]["close"]
        return {"support": price, "resistance": price, "hard_support": price, "hard_resistance": price}
    support = lows[max(0, int(len(lows) * 0.10) - 1)]
    resistance = highs[min(len(highs) - 1, int(len(highs) * 0.90))]
    hard_support = min(lows)
    hard_resistance = max(highs)
    return {
        "support": support,
        "resistance": resistance,
        "hard_support": hard_support,
        "hard_resistance": hard_resistance,
    }


def is_5m_confirmed(side: str, k5: List[Dict[str, float]]) -> Tuple[bool, List[str]]:
    if len(k5) < 12:
        return False, ["5m мало свечей"]
    closes = [k["close"] for k in k5]
    e9 = ema(closes[-30:], 9)
    last = k5[-1]
    if side == "LONG":
        ok = closes[-1] > closes[-2] >= closes[-3] and closes[-1] >= e9 and last["close"] > last["open"]
        return ok, ["5m подтверждение вверх"] if ok else []
    ok = closes[-1] < closes[-2] <= closes[-3] and closes[-1] <= e9 and last["close"] < last["open"]
    return ok, ["5m подтверждение вниз"] if ok else []

# =============================================================================
# SIGNAL ANALYSIS
# =============================================================================

@dataclass
class Candidate:
    symbol: str
    side: str
    score: float
    strategy: str
    entry: float
    reasons: List[str]
    debug: Dict[str, Any] = field(default_factory=dict)


def analyze_symbol(symbol: str, k15: List[Dict[str, float]], k5: List[Dict[str, float]], k1h: List[Dict[str, float]]) -> Optional[Candidate]:
    """Strict structural scanner.

    Sends only setups that already touched a local support/resistance zone and
    then CLOSED with rejection/confirmation. It avoids chasing candles that are
    already too far from the level.
    """
    k15 = closed_klines(k15, "15m")
    k5 = closed_klines(k5, "5m")
    k1h = closed_klines(k1h, "1h")
    if len(k15) < 58 or len(k5) < 20:
        return None

    closes15 = [k["close"] for k in k15]
    closes5 = [k["close"] for k in k5]
    closes1h = [k["close"] for k in k1h] if len(k1h) >= 20 else closes15

    last = k15[-1]       # last CLOSED 15m candle
    prev = k15[-2]
    entry = last["close"]
    if entry <= 0:
        return None

    rsi15 = rsi(closes15, 14)
    ema9 = ema(closes15[-40:], 9)
    ema21 = ema(closes15[-60:], 21)
    ema1h21 = ema(closes1h[-60:], 21)
    ema1h50 = ema(closes1h[-80:], 50)
    volr = volume_ratio(k15, 20)
    atr_val = atr(k15, 14)
    atr_pct = atr_val / entry * 100 if entry else 0.0

    if atr_pct < MIN_ATR_PCT or atr_pct > MAX_ATR_PCT:
        return None

    chg_3 = percent_change(closes15[-4], closes15[-1]) if len(closes15) >= 4 else 0.0
    chg_8 = percent_change(closes15[-9], closes15[-1]) if len(closes15) >= 9 else 0.0
    chg_16 = percent_change(closes15[-17], closes15[-1]) if len(closes15) >= 17 else 0.0
    chg_5m3 = percent_change(closes5[-4], closes5[-1]) if len(closes5) >= 4 else 0.0

    # Anti-chase: no signals after vertical move away from the level.
    if abs(chg_8) > ANTI_CHASE_8_CANDLE_MOVE:
        return None
    if candle_body_pct(last) > 5.0:
        return None

    levels = local_levels(k15)
    support = levels["support"]
    resistance = levels["resistance"]
    hard_support = levels.get("hard_support", support)
    hard_resistance = levels.get("hard_resistance", resistance)

    lower_wick, upper_wick, close_near_high, close_near_low = candle_position(last)
    candle_green = last["close"] > last["open"]
    candle_red = last["close"] < last["open"]

    tol_pct = max(0.0015, min(0.008, (atr_val / entry) * 0.55))
    dist_from_support_atr = (entry - support) / max(atr_val, entry * 0.0001)
    dist_from_resistance_atr = (resistance - entry) / max(atr_val, entry * 0.0001)

    candidates: List[Candidate] = []

    # LONG: support touch / false breakdown / reclaim, then closed upward.
    support_touched = (
        last["low"] <= support * (1 + tol_pct)
        or prev["low"] <= support * (1 + tol_pct)
        or last["low"] <= hard_support * (1 + tol_pct)
    )
    support_reclaimed = last["close"] > support and last["close"] > prev["close"]
    false_break_support = last["low"] < support * (1 - tol_pct * 0.35) and last["close"] > support
    long_5m_ok, long_5m_reasons = is_5m_confirmed("LONG", k5)
    one_hour_not_bearish = closes1h[-1] >= ema1h50 * 0.975 or ema1h21 >= ema1h50 * 0.985

    long_ok = all([
        support_touched,
        support_reclaimed,
        candle_green or close_near_high >= MIN_CLOSE_LOCATION,
        lower_wick >= MIN_REJECTION_WICK_RATIO or false_break_support,
        close_near_high >= MIN_CLOSE_LOCATION,
        dist_from_support_atr <= MAX_ENTRY_DISTANCE_ATR,
        30 <= rsi15 <= 64,
        volr >= MIN_VOLUME_RATIO_CONFIRM,
        long_5m_ok,
        one_hour_not_bearish,
        chg_5m3 > 0,
    ])

    if long_ok:
        score = 78.0
        reasons = [
            "15m закрылась после отскока от поддержки",
            f"поддержка ~{fmt_price(support)}",
            f"вход недалеко от уровня: {dist_from_support_atr:.2f} ATR",
            f"нижняя тень {lower_wick:.2f}",
            f"объем x{volr:.2f}",
            f"RSI {rsi15:.1f}",
        ] + long_5m_reasons
        if false_break_support:
            score += 6; reasons.append("ложный прокол поддержки и возврат выше")
        if ema9 > ema21 or entry > ema21:
            score += 4; reasons.append("15m EMA не против")
        if ema1h21 >= ema1h50 * 0.995:
            score += 3; reasons.append("1h тренд нейтральный/выше")
        if chg_3 > 0.20:
            score += min(5, chg_3 * 3); reasons.append(f"локальный импульс +{chg_3:.2f}%")
        if -10 <= chg_16 <= -0.35:
            score += 3; reasons.append(f"был откат {chg_16:.2f}%")
        candidates.append(Candidate(symbol, "LONG", round(min(score, 96), 1), "confirmed support bounce", entry, reasons, {
            "support": support, "resistance": resistance, "rsi15": rsi15, "volr": volr,
            "atr_pct": atr_pct, "dist_from_support_atr": dist_from_support_atr,
            "close_near_high": close_near_high, "lower_wick": lower_wick,
        }))

    # SHORT: resistance touch / false breakout / rejection, then closed down.
    resistance_touched = (
        last["high"] >= resistance * (1 - tol_pct)
        or prev["high"] >= resistance * (1 - tol_pct)
        or last["high"] >= hard_resistance * (1 - tol_pct)
    )
    resistance_rejected = last["close"] < resistance and last["close"] < prev["close"]
    false_break_resistance = last["high"] > resistance * (1 + tol_pct * 0.35) and last["close"] < resistance
    short_5m_ok, short_5m_reasons = is_5m_confirmed("SHORT", k5)
    one_hour_not_bullish = closes1h[-1] <= ema1h50 * 1.025 or ema1h21 <= ema1h50 * 1.015

    short_ok = all([
        resistance_touched,
        resistance_rejected,
        candle_red or close_near_low >= MIN_CLOSE_LOCATION,
        upper_wick >= MIN_REJECTION_WICK_RATIO or false_break_resistance,
        close_near_low >= MIN_CLOSE_LOCATION,
        dist_from_resistance_atr <= MAX_ENTRY_DISTANCE_ATR,
        36 <= rsi15 <= 76,
        volr >= MIN_VOLUME_RATIO_CONFIRM,
        short_5m_ok,
        one_hour_not_bullish,
        chg_5m3 < 0,
    ])

    if short_ok:
        score = 78.0
        reasons = [
            "15m закрылась после отбоя от сопротивления",
            f"сопротивление ~{fmt_price(resistance)}",
            f"вход недалеко от уровня: {dist_from_resistance_atr:.2f} ATR",
            f"верхняя тень {upper_wick:.2f}",
            f"объем x{volr:.2f}",
            f"RSI {rsi15:.1f}",
        ] + short_5m_reasons
        if false_break_resistance:
            score += 7; reasons.append("ложный пробой сопротивления и возврат ниже")
        if ema9 < ema21 or entry < ema21:
            score += 4; reasons.append("15m EMA не против")
        if ema1h21 <= ema1h50 * 1.005:
            score += 3; reasons.append("1h тренд нейтральный/ниже")
        if chg_3 < -0.20:
            score += min(5, abs(chg_3) * 3); reasons.append(f"локальный разворот {chg_3:.2f}%")
        if 0.35 <= chg_16 <= 14:
            score += 3; reasons.append(f"был памп +{chg_16:.2f}%")
        candidates.append(Candidate(symbol, "SHORT", round(min(score, 97), 1), "confirmed resistance rejection", entry, reasons, {
            "support": support, "resistance": resistance, "rsi15": rsi15, "volr": volr,
            "atr_pct": atr_pct, "dist_from_resistance_atr": dist_from_resistance_atr,
            "close_near_low": close_near_low, "upper_wick": upper_wick,
        }))

    if not candidates:
        return None
    best = max(candidates, key=lambda c: c.score)
    if best.score < FALLBACK_SCORE:
        return None
    return best


def build_trade_from_candidate(c: Candidate) -> Trade:
    entry = c.entry
    if c.side == "LONG":
        tps = [entry * (1 + p) for p in TP_PCTS]
        averaging = entry * (1 - AVERAGING_PCT)
        stop = entry * (1 - STOP_PCT)
    else:
        tps = [entry * (1 - p) for p in TP_PCTS]
        averaging = entry * (1 + AVERAGING_PCT)
        stop = entry * (1 + STOP_PCT)
    tid = f"{display_symbol(c.symbol)}-{c.side}-{int(time.time())}-{random.randint(100,999)}"
    return Trade(
        id=tid,
        symbol=c.symbol,
        side=c.side,
        entry=entry,
        tps=tps,
        averaging=averaging,
        stop=stop,
        score=c.score,
        strategy=c.strategy,
        created_ts=time.time(),
    )


def format_signal_message(trade: Trade, reasons: Optional[List[str]] = None) -> str:
    coin = display_symbol(trade.symbol).replace("USDT", "")
    head = f"Скальп-позиция - <b>{coin} {trade.side}</b>"
    tp_lines = "\n".join(fmt_price(x) for x in trade.tps)
    roi_lines = []
    for i, tp in enumerate(trade.tps, 1):
        roi_lines.append(f"TP{i}: ~{roi_pct(trade.side, trade.entry, tp):.1f}% ROI x{LEVERAGE:.0f}")
    reason_text = ""
    if reasons:
        reason_text = "\n\nФильтры: " + "; ".join(reasons[:5])
    return (
        f"{head}\n\n"
        f"Моя точка входа - <code>{fmt_price(trade.entry)}</code>\n\n"
        "Пока заходите, следующим постом пришлю параметры сделки!\n\n"
        "Лимитные ордера на фиксацию выставил на значениях:\n\n"
        f"<code>{tp_lines}</code>\n\n"
        f"Лимитный ордер на усреднение: <code>{fmt_price(trade.averaging)}</code>\n"
        f"Защитный стоп: <code>{fmt_price(trade.stop)}</code>\n\n"
        f"Score сетапа: <b>{trade.score:.0f}/100</b> · Подтверждение: <b>15m закрытая + 5m</b> · Плечо: <b>x{LEVERAGE:.0f}</b>\n"
        + reason_text +
        "\n\nО любых действиях по открытой сделке буду сообщать в канале\n\n"
        "⚠️ Это сигнал, не финансовая рекомендация. Риск контролируй сам."
    )


def format_stats() -> str:
    s = state.stats
    wr = (s.profitable / s.closed * 100.0) if s.closed else 0.0
    active = len(state.active_trades)
    lines = [
        "📊 <b>Обновление статистики</b>",
        "",
        f"Всего сигналов: <b>{s.total_signals}</b>",
        f"Активные сделки: <b>{active}</b>",
        f"Закрытые сделки: <b>{s.closed}</b>",
        f"✅ Прибыльные: <b>{s.profitable}</b>",
        f"🛑 Убыточные: <b>{s.losing}</b>",
        f"Win rate: <b>{wr:.1f}%</b>",
        "",
        f"TP1 достигнут: <b>{s.tp1_hits}</b>",
        f"Все 5 целей: <b>{s.tp5_hits}</b>",
        f"Усреднений: <b>{s.averaged_count}</b>",
        f"Стопов: <b>{s.stop_count}</b>",
        f"Лучший ROI: <b>{s.best_roi_pct:.2f}%</b>",
        f"Худший ROI: <b>{s.worst_roi_pct:.2f}%</b>",
    ]
    if s.last_closed:
        lines.append("\nПоследние закрытые:")
        for item in s.last_closed[-8:][::-1]:
            lines.append(
                f"{item.get('symbol')} {item.get('side')} — {item.get('reason')} · ROI {item.get('roi_pct', 0):.2f}%"
            )
    return "\n".join(lines)

# =============================================================================
# SCANNER AND MONITOR
# =============================================================================

async def scan_one_symbol(session: aiohttp.ClientSession, symbol: str, semaphore: asyncio.Semaphore) -> Optional[Candidate]:
    async with semaphore:
        try:
            k15, k5, k1h = await asyncio.gather(
                fetch_klines(session, symbol, "15m", 100),
                fetch_klines(session, symbol, "5m", 80),
                fetch_klines(session, symbol, "1h", 80),
            )
            return analyze_symbol(symbol, k15, k5, k1h)
        except Exception as e:
            logger.debug("scan_one_symbol failed %s: %s", symbol, e)
            return None


async def confirmation_ready(candidate: Candidate) -> bool:
    """Two-step confirmation: first scan stores setup, next scan must confirm it again."""
    if not STRICT_CONFIRMATION or CONFIRMATION_SCANS <= 1:
        return True
    key = candidate.symbol
    now_ts = time.time()
    async with pending_lock:
        expired = [k for k, v in pending_candidates.items() if now_ts - float(v.get("ts", 0)) > PENDING_EXPIRE_SECONDS]
        for k in expired:
            pending_candidates.pop(k, None)

        old = pending_candidates.get(key)
        if not old or old.get("side") != candidate.side:
            pending_candidates[key] = {
                "side": candidate.side,
                "score": candidate.score,
                "entry": candidate.entry,
                "strategy": candidate.strategy,
                "reasons": candidate.reasons,
                "count": 1,
                "ts": now_ts,
            }
            logger.info("pending setup stored: %s %s score=%s", candidate.symbol, candidate.side, candidate.score)
            if SEND_PENDING_LOGS_TO_TELEGRAM:
                await send_telegram(
                    f"👀 <b>Сетап найден, жду подтверждение</b>\n"
                    f"{display_symbol(candidate.symbol)} {candidate.side}\n"
                    f"Score: <b>{candidate.score:.0f}</b>\n"
                    f"Причина: {candidate.strategy}\n"
                    f"Сигнал не отправлен, нужна повторная проверка."
                )
            return False

        first_entry = float(old.get("entry", candidate.entry) or candidate.entry)
        move_from_first = abs(candidate.entry - first_entry) / max(first_entry, 1e-12) * 100
        if move_from_first > 1.20:
            pending_candidates[key] = {
                "side": candidate.side,
                "score": candidate.score,
                "entry": candidate.entry,
                "strategy": candidate.strategy,
                "reasons": candidate.reasons,
                "count": 1,
                "ts": now_ts,
            }
            logger.info("pending reset because price moved too far: %s %.2f%%", candidate.symbol, move_from_first)
            return False

        old["count"] = int(old.get("count", 1)) + 1
        old["score"] = max(float(old.get("score", 0)), candidate.score)
        old["entry"] = candidate.entry
        old["ts"] = now_ts
        if old["count"] >= CONFIRMATION_SCANS:
            pending_candidates.pop(key, None)
            logger.info("confirmed setup ready: %s %s score=%s checks=%s", candidate.symbol, candidate.side, candidate.score, old["count"])
            return True
        return False


async def send_signal(candidate: Candidate) -> bool:
    async with state_lock:
        if state.day_key != today_key():
            state.day_key = today_key()
            state.sent_today = 0

        if state.sent_today >= MAX_SIGNALS_PER_DAY:
            return False
        if len(state.active_trades) >= MAX_ACTIVE_TRADES:
            return False

        now_ts = time.time()
        last_ts = state.last_signal_ts_by_symbol.get(candidate.symbol, 0)
        if now_ts - last_ts < SYMBOL_COOLDOWN_MINUTES * 60:
            return False

        # Avoid two trades on same symbol.
        for t in state.active_trades.values():
            if t.symbol == candidate.symbol and t.status == "active":
                return False

        trade = build_trade_from_candidate(candidate)
        msg = format_signal_message(trade, candidate.reasons)
        message_id = await send_telegram(msg)
        trade.telegram_message_id = message_id

        state.active_trades[trade.id] = trade
        state.last_signal_ts_by_symbol[candidate.symbol] = now_ts
        state.sent_today += 1
        state.stats.total_signals += 1
        save_state()
        logger.info("Signal sent: %s %s score=%s", trade.symbol, trade.side, trade.score)
        return True


async def scanner_loop() -> None:
    global last_scanner_error_ts, last_scan_info
    await asyncio.sleep(3)
    while True:
        started = time.time()
        try:
            async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 BingXSignalBot/1.0"}) as session:
                symbols = await get_top_symbols(session)
                semaphore = asyncio.Semaphore(8)
                tasks = [scan_one_symbol(session, sym, semaphore) for sym in symbols]
                raw_candidates = await asyncio.gather(*tasks)
                candidates = [c for c in raw_candidates if c is not None]
                candidates.sort(key=lambda x: x.score, reverse=True)

                threshold = MIN_SCORE
                async with state_lock:
                    if state.day_key != today_key():
                        state.day_key = today_key()
                        state.sent_today = 0
                        save_state()
                    # If bot has sent nothing today, allow fallback threshold.
                    if state.sent_today < DAILY_MIN_SIGNALS:
                        threshold = FALLBACK_SCORE

                sent = 0
                pending_seen = 0
                for c in candidates:
                    if c.score < threshold:
                        break
                    ready = await confirmation_ready(c)
                    if not ready:
                        pending_seen += 1
                        continue
                    ok = await send_signal(c)
                    if ok:
                        sent += 1
                        # Usually one confirmed signal per cycle is enough.
                        if sent >= 1:
                            break

                last_scan_info = {
                    "symbols_checked": len(symbols),
                    "candidates": len(candidates),
                    "best": asdict(candidates[0]) if candidates else None,
                    "sent": sent,
                    "pending_seen": pending_seen,
                    "pending_total": len(pending_candidates),
                    "threshold": threshold,
                    "duration_sec": round(time.time() - started, 2),
                    "time": now_utc().isoformat(),
                }
                logger.info(
                    "scan done: checked=%s candidates=%s pending=%s sent=%s threshold=%s duration=%.1fs",
                    len(symbols), len(candidates), len(pending_candidates), sent, threshold, time.time() - started,
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            logger.error("Scanner error: %s\n%s", err, traceback.format_exc())
            now_ts = time.time()
            # Send not more than once every 10 minutes.
            if SEND_SCANNER_ERRORS_TO_TELEGRAM and now_ts - last_scanner_error_ts > 600:
                last_scanner_error_ts = now_ts
                await send_telegram(f"⚠️ <b>Ошибка сканера Render:</b>\n<code>{err}</code>\n\nБот не остановлен, следующая попытка через {SCAN_INTERVAL_SECONDS} сек.")

        await asyncio.sleep(max(10, SCAN_INTERVAL_SECONDS))


async def monitor_loop() -> None:
    await asyncio.sleep(8)
    while True:
        try:
            async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 BingXSignalBot/1.0"}) as session:
                async with state_lock:
                    trades_snapshot = list(state.active_trades.values())
                for trade in trades_snapshot:
                    if trade.status != "active":
                        continue
                    try:
                        klines = await fetch_klines(session, trade.symbol, "1m", 5)
                        if not klines:
                            continue
                        price = klines[-1]["close"]
                        await update_trade_by_price(trade.id, price)
                    except Exception as e:
                        logger.debug("monitor trade failed %s: %s", trade.symbol, e)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Monitor loop error: %s", e)
        await asyncio.sleep(max(10, MONITOR_INTERVAL_SECONDS))


async def update_trade_by_price(trade_id: str, price: float) -> None:
    messages: List[str] = []
    stats_to_send = False
    async with state_lock:
        trade = state.active_trades.get(trade_id)
        if not trade or trade.status != "active":
            return

        current_roi = roi_pct(trade.side, trade.entry, price)
        trade.best_roi_pct = max(trade.best_roi_pct, current_roi)
        trade.worst_roi_pct = min(trade.worst_roi_pct, current_roi)

        # Averaging touch.
        if not trade.averaged:
            if (trade.side == "LONG" and price <= trade.averaging) or (trade.side == "SHORT" and price >= trade.averaging):
                trade.averaged = True
                state.stats.averaged_count += 1
                messages.append(
                    f"🔄 <b>Усреднение активировано</b>\n{display_symbol(trade.symbol)} {trade.side}\nЦена: <code>{fmt_price(price)}</code>\nУровень: <code>{fmt_price(trade.averaging)}</code>"
                )

        # TP hits.
        for idx, tp in enumerate(trade.tps, 1):
            if idx in trade.reached_tps:
                continue
            hit = (trade.side == "LONG" and price >= tp) or (trade.side == "SHORT" and price <= tp)
            if hit:
                trade.reached_tps.append(idx)
                r = roi_pct(trade.side, trade.entry, tp)
                if idx == 1:
                    state.stats.tp1_hits += 1
                messages.append(
                    f"✅ <b>TP{idx} достигнут</b>\n{display_symbol(trade.symbol)} {trade.side}\nЦена: <code>{fmt_price(tp)}</code>\nROI примерно: <b>+{r:.2f}%</b> x{LEVERAGE:.0f}"
                )

        # Close at TP5.
        if 5 in trade.reached_tps:
            close_trade_locked(trade, "TP5", trade.tps[-1])
            state.stats.tp5_hits += 1
            stats_to_send = True
            messages.append(
                f"🔥 <b>Все 5 целей были достигнуты</b>\n{display_symbol(trade.symbol)} {trade.side}\nИтог ROI примерно: <b>+{roi_pct(trade.side, trade.entry, trade.tps[-1]):.2f}%</b>"
            )

        # Stop.
        if trade.status == "active":
            stopped = (trade.side == "LONG" and price <= trade.stop) or (trade.side == "SHORT" and price >= trade.stop)
            if stopped:
                had_tp = len(trade.reached_tps) > 0
                close_trade_locked(trade, "SL_AFTER_TP" if had_tp else "SL", price)
                state.stats.stop_count += 1
                stats_to_send = True
                if had_tp:
                    messages.append(
                        f"🟡 <b>Стоп после фиксации части целей</b>\n{display_symbol(trade.symbol)} {trade.side}\nЦена: <code>{fmt_price(price)}</code>\nСделка учтена как прибыльная, потому что был TP1."
                    )
                else:
                    messages.append(
                        f"🛑 <b>Stop Loss</b>\n{display_symbol(trade.symbol)} {trade.side}\nЦена: <code>{fmt_price(price)}</code>\nROI примерно: <b>{roi_pct(trade.side, trade.entry, price):.2f}%</b>"
                    )

        save_state()

    for m in messages:
        await send_telegram(m)
    if stats_to_send and SEND_STATS_AFTER_CLOSE:
        async with state_lock:
            msg = format_stats()
        await send_telegram(msg)


def close_trade_locked(trade: Trade, reason: str, close_price: float) -> None:
    trade.status = "closed"
    trade.closed_ts = time.time()
    trade.close_reason = reason
    final_roi = roi_pct(trade.side, trade.entry, close_price)
    trade.best_roi_pct = max(trade.best_roi_pct, final_roi)
    trade.worst_roi_pct = min(trade.worst_roi_pct, final_roi)

    state.stats.closed += 1
    if len(trade.reached_tps) > 0:
        state.stats.profitable += 1
    else:
        state.stats.losing += 1

    state.stats.best_roi_pct = max(state.stats.best_roi_pct, trade.best_roi_pct)
    state.stats.worst_roi_pct = min(state.stats.worst_roi_pct, trade.worst_roi_pct)
    state.stats.last_closed.append({
        "symbol": display_symbol(trade.symbol),
        "side": trade.side,
        "reason": reason,
        "roi_pct": final_roi,
        "closed_at": now_utc().isoformat(),
        "tp_count": len(trade.reached_tps),
    })
    state.stats.last_closed = state.stats.last_closed[-30:]
    state.active_trades.pop(trade.id, None)

# =============================================================================
# APP ROUTES
# =============================================================================

@app.on_event("startup")
async def on_startup() -> None:
    global scanner_task, monitor_task
    load_state()
    logger.info("%s starting. No ccxt dependency. Start command expected: uvicorn bot:app --host 0.0.0.0 --port 1000", APP_NAME)
    logger.info("Telegram config: token_present=%s chat_id_present=%s chat_id=%s", bool(TELEGRAM_BOT_TOKEN), bool(TELEGRAM_CHAT_ID), TELEGRAM_CHAT_ID[:6] + "..." if TELEGRAM_CHAT_ID else "")
    await send_startup_message()
    scanner_task = asyncio.create_task(scanner_loop())
    monitor_task = asyncio.create_task(monitor_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    for task in (scanner_task, monitor_task):
        if task:
            task.cancel()
    save_state()


@app.get("/")
async def root() -> JSONResponse:
    return JSONResponse({
        "ok": True,
        "app": APP_NAME,
        "no_ccxt": True,
        "strict_confirmation": STRICT_CONFIRMATION,
        "pending_candidates": len(pending_candidates),
        "uptime_sec": round(time.time() - started_at, 1),
        "active_trades": len(state.active_trades),
        "stats": asdict(state.stats),
        "last_scan": last_scan_info,
    })


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"ok": True, "status": "running", "no_ccxt": True})


@app.get("/stats")
async def stats_route() -> PlainTextResponse:
    async with state_lock:
        return PlainTextResponse(format_stats())


@app.get("/stats/send")
async def stats_send_route() -> JSONResponse:
    async with state_lock:
        msg = format_stats()
    mid = await send_telegram(msg)
    return JSONResponse({"ok": bool(mid), "message_id": mid})


@app.get("/telegram/test")
async def telegram_test() -> JSONResponse:
    get_me = await telegram_get_me()
    mid = await send_telegram(
        "✅ <b>Тест Telegram успешный</b>\n"
        "Бот подключен к каналу/чату.\n"
        "Версия: <b>точные отскоки/отбои без ccxt</b>."
    )
    return JSONResponse({"ok": bool(mid), "message_id": mid, "getMe": get_me})


@app.get("/debug")
async def debug_route() -> JSONResponse:
    return JSONResponse({
        "ok": True,
        "token_present": bool(TELEGRAM_BOT_TOKEN),
        "chat_id_present": bool(TELEGRAM_CHAT_ID),
        "chat_id_preview": TELEGRAM_CHAT_ID[:8] + "..." if TELEGRAM_CHAT_ID else "",
        "scan_interval": SCAN_INTERVAL_SECONDS,
        "min_score": MIN_SCORE,
        "fallback_score": FALLBACK_SCORE,
        "leverage": LEVERAGE,
        "last_scan": last_scan_info,
        "pending_candidates": pending_candidates,
        "strict_confirmation": STRICT_CONFIRMATION,
        "confirmation_scans": CONFIRMATION_SCANS,
        "state": state_to_json(),
    })


@app.get("/scan/once")
async def scan_once_route() -> JSONResponse:
    """Manual debug scan. Useful only if Render service URL is available."""
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 BingXSignalBot/1.0"}) as session:
        symbols = await get_top_symbols(session)
        semaphore = asyncio.Semaphore(8)
        tasks = [scan_one_symbol(session, sym, semaphore) for sym in symbols[:30]]
        raw_candidates = await asyncio.gather(*tasks)
        candidates = [c for c in raw_candidates if c is not None]
        candidates.sort(key=lambda x: x.score, reverse=True)
    return JSONResponse({
        "checked": min(30, len(symbols)),
        "candidates": [asdict(c) for c in candidates[:10]],
    })


if __name__ == "__main__":
    # Local run fallback. Render should use uvicorn bot:app --host 0.0.0.0 --port 1000
    import uvicorn
    uvicorn.run("bot:app", host="0.0.0.0", port=int(os.getenv("PORT", "1000")))
