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
import numpy as np
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

# ============================================================
# BingX Scalp Signal Bot
# ------------------------------------------------------------
# - Сканирует публичные USDT-M futures/swap рынки BingX через CCXT.
# - НЕ открывает сделки. Только Telegram-сигналы.
# - Ищет сетапы в стиле: сильный импульс/перегрев -> отскок/отбой,
#   либо пробой/пролом уровня с объемом.
# - Формат сигнала похож на примеры: entry, 5 take-profit, averaging order.
# ============================================================

APP_NAME = "BingX Scalp Signal Bot"
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")


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
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_list(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID")

SCAN_INTERVAL_SECONDS = max(45, env_int("SCAN_INTERVAL_SECONDS", 180))
TOP_SYMBOLS_LIMIT = max(20, env_int("TOP_SYMBOLS_LIMIT", 100))
MIN_24H_QUOTE_VOLUME_USDT = env_float("MIN_24H_QUOTE_VOLUME_USDT", 1_000_000)

MIN_SCORE = env_int("MIN_SCORE", 76)
FALLBACK_SCORE = env_int("FALLBACK_SCORE", 70)
DAILY_MIN_SIGNALS = env_int("DAILY_MIN_SIGNALS", 1)
MAX_SIGNALS_PER_DAY = env_int("MAX_SIGNALS_PER_DAY", 6)
COOLDOWN_MINUTES = env_int("COOLDOWN_MINUTES", 180)

LEVERAGE = max(1, env_int("LEVERAGE", 20))
TAKE_PCTS = [float(x.strip()) / 100 for x in env_str("TAKE_PCTS", "0.77,1.37,1.97,2.98,3.98").split(",") if x.strip()]
AVERAGE_PCT = env_float("AVERAGE_PCT", 7.7) / 100
STOP_AFTER_AVERAGE_PCT = env_float("STOP_AFTER_AVERAGE_PCT", 10.5) / 100

SEND_STARTUP_MESSAGE = env_bool("SEND_STARTUP_MESSAGE", True)
SEND_EMPTY_SCAN = env_bool("SEND_EMPTY_SCAN", False)
EXCLUDED_BASES = set(env_list("EXCLUDED_BASES", "BTC,ETH,USDC,FDUSD,TUSD,DAI,USDE,USDP,USTC"))

# Если сигналы слишком частые — увеличь MIN_SCORE до 80-84.
# Если бот слишком молчит — снизь MIN_SCORE до 72-74 или TOP_SYMBOLS_LIMIT увеличь до 150.

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


@dataclass
class Signal:
    symbol: str
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


@dataclass
class BotState:
    day: str = ""
    signals_today: int = 0
    last_signal_ts: float = 0.0
    cooldowns: Dict[str, float] = field(default_factory=dict)
    active: Dict[str, Dict[str, Any]] = field(default_factory=dict)


state = BotState()


def today_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def reset_daily_if_needed() -> None:
    global state
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
            state = BotState(**raw)
            reset_daily_if_needed()
            logger.info("State loaded")
    except Exception as e:
        logger.warning("Could not load state: %s", e)
        state = BotState(day=today_key())


def save_state() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(asdict(state), f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Could not save state: %s", e)


def now_ms() -> int:
    return int(time.time() * 1000)


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return default
        return float(x)
    except Exception:
        return default


def pct_change(new: float, old: float) -> float:
    if old == 0:
        return 0.0
    return (new / old - 1.0) * 100.0


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def fmt_price(price: float) -> str:
    """Dynamic price formatting for crypto scalps."""
    price = safe_float(price)
    if price >= 1000:
        return f"{price:.2f}"
    if price >= 100:
        return f"{price:.3f}"
    if price >= 10:
        return f"{price:.4f}"
    if price >= 1:
        return f"{price:.5f}"
    if price >= 0.1:
        return f"{price:.5f}"
    if price >= 0.01:
        return f"{price:.6f}"
    if price >= 0.001:
        return f"{price:.7f}"
    return f"{price:.9f}"


def base_from_symbol(symbol: str) -> str:
    # CCXT swap symbol usually looks like GRASS/USDT:USDT
    return symbol.split("/")[0].replace("1000", "1000")


def display_pair(symbol: str) -> str:
    return f"{base_from_symbol(symbol)}USDT"


def make_levels(entry: float, side: str) -> Tuple[List[float], float, float]:
    if side == "LONG":
        targets = [entry * (1 + p) for p in TAKE_PCTS]
        average = entry * (1 - AVERAGE_PCT)
        stop = entry * (1 - STOP_AFTER_AVERAGE_PCT)
    else:
        targets = [entry * (1 - p) for p in TAKE_PCTS]
        average = entry * (1 + AVERAGE_PCT)
        stop = entry * (1 + STOP_AFTER_AVERAGE_PCT)
    return targets, average, stop


def roi_pct(entry: float, price: float, side: str, leverage: int = LEVERAGE) -> float:
    if entry <= 0:
        return 0.0
    if side == "LONG":
        return ((price - entry) / entry) * 100.0 * leverage
    return ((entry - price) / entry) * 100.0 * leverage


async def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram variables are missing. Message not sent: %s", text[:80])
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("Telegram error %s: %s", resp.status, body[:300])
                    return False
                return True
    except Exception as e:
        logger.error("Telegram send failed: %s", e)
        return False


def ohlcv_to_df(ohlcv: List[List[float]]) -> pd.DataFrame:
    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna().reset_index(drop=True)
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close = df["close"]
    high = df["high"]
    low = df["low"]
    vol = df["volume"].replace(0, np.nan)

    df["ema9"] = close.ewm(span=9, adjust=False).mean()
    df["ema20"] = close.ewm(span=20, adjust=False).mean()
    df["ema50"] = close.ewm(span=50, adjust=False).mean()
    df["ema200"] = close.ewm(span=200, adjust=False).mean()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))
    df["rsi"] = df["rsi"].fillna(50)

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr"] = tr.ewm(span=14, adjust=False).mean()
    df["atr_pct"] = (df["atr"] / close.replace(0, np.nan)) * 100

    typical = (high + low + close) / 3
    vwap_vol = vol.rolling(50, min_periods=10).sum()
    df["vwap50"] = (typical * vol).rolling(50, min_periods=10).sum() / vwap_vol
    df["vwap50"] = df["vwap50"].fillna(df["ema20"])

    basis = close.rolling(20, min_periods=20).mean()
    sd = close.rolling(20, min_periods=20).std()
    df["bb_mid"] = basis
    df["bb_up"] = basis + 2 * sd
    df["bb_low"] = basis - 2 * sd

    df["vol_avg20"] = df["volume"].rolling(20, min_periods=5).mean()
    df["vol_ratio"] = df["volume"] / df["vol_avg20"].replace(0, np.nan)
    df["vol_ratio"] = df["vol_ratio"].replace([np.inf, -np.inf], np.nan).fillna(1.0)
    return df


def candle_parts(row: pd.Series) -> Dict[str, float]:
    o, h, l, c = map(safe_float, [row["open"], row["high"], row["low"], row["close"]])
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    return {
        "range_pct": (rng / c) * 100 if c else 0,
        "body_pct_of_range": body / rng,
        "upper_wick": upper / rng,
        "lower_wick": lower / rng,
        "is_bull": 1.0 if c > o else 0.0,
        "is_bear": 1.0 if c < o else 0.0,
        "candle_change_pct": pct_change(c, o),
    }


def add_reason(reasons: List[str], text: str) -> None:
    if len(reasons) < 7:
        reasons.append(text)


def score_long_bounce(df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame, df4h: pd.DataFrame) -> Tuple[int, List[str], Dict[str, Any]]:
    c5, p5 = df5.iloc[-1], df5.iloc[-2]
    c15 = df15.iloc[-1]
    c1h = df1h.iloc[-1]
    c4h = df4h.iloc[-1]
    parts = candle_parts(c15)
    price = safe_float(c15["close"])

    change_1h = pct_change(price, safe_float(df15.iloc[-5]["close"])) if len(df15) >= 6 else 0
    change_4h = pct_change(price, safe_float(df15.iloc[-17]["close"])) if len(df15) >= 18 else 0
    change_24h = pct_change(safe_float(c1h["close"]), safe_float(df1h.iloc[-25]["close"])) if len(df1h) >= 26 else 0
    vol_ratio = safe_float(c15["vol_ratio"], 1.0)
    rsi15 = safe_float(c15["rsi"], 50)
    rsi5 = safe_float(c5["rsi"], 50)
    rsi5_prev = safe_float(p5["rsi"], 50)
    atr_pct = safe_float(c15["atr_pct"], 0)

    score = 0
    reasons: List[str] = []

    if change_1h <= -3.0:
        score += 14
        add_reason(reasons, f"1h сильное падение {change_1h:.2f}%")
    if change_4h <= -6.0:
        score += 14
        add_reason(reasons, f"4h перепроданность {change_4h:.2f}%")
    if change_24h <= -10.0:
        score += 6
        add_reason(reasons, f"24h давление {change_24h:.2f}%")

    if parts["is_bull"]:
        score += 8
        add_reason(reasons, "15m свеча закрывается бычьей")
    if parts["lower_wick"] >= 0.30:
        score += 10
        add_reason(reasons, "нижняя тень показывает выкуп")
    if price > safe_float(c15["ema9"]):
        score += 8
        add_reason(reasons, "возврат выше EMA9")
    if price > safe_float(c5["vwap50"]):
        score += 7
        add_reason(reasons, "5m выше VWAP")
    if vol_ratio >= 1.35:
        score += min(16, int(8 + (vol_ratio - 1.35) * 6))
        add_reason(reasons, f"объем x{vol_ratio:.2f}")
    if 24 <= rsi15 <= 54:
        score += 8
        add_reason(reasons, f"RSI15 {rsi15:.1f}, зона отскока")
    if rsi5 - rsi5_prev >= 2.0:
        score += 7
        add_reason(reasons, "RSI5 разворачивается вверх")
    if 0.45 <= atr_pct <= 7.0:
        score += 7
        add_reason(reasons, f"ATR15 {atr_pct:.2f}% подходит для скальпа")

    # Anti-chase: if already huge green candle, reduce score.
    if parts["candle_change_pct"] > 5.5:
        score -= 12
        add_reason(reasons, "анти-чейз: свеча уже слишком большая")
    elif parts["candle_change_pct"] >= 0.25:
        score += 5

    # Bigger market structure: avoid catching a knife under all EMAs unless bounce is strong.
    if safe_float(c1h["close"]) > safe_float(c1h["ema20"]):
        score += 4
    if safe_float(c4h["close"]) > safe_float(c4h["ema50"]):
        score += 3

    metrics = {
        "change_1h": round(change_1h, 2),
        "change_4h": round(change_4h, 2),
        "change_24h": round(change_24h, 2),
        "vol_ratio": round(vol_ratio, 2),
        "rsi15": round(rsi15, 1),
        "rsi5": round(rsi5, 1),
        "atr_pct": round(atr_pct, 2),
        "lower_wick": round(parts["lower_wick"], 2),
    }
    return int(clamp(score, 0, 99)), reasons, metrics


def score_short_rejection(df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame, df4h: pd.DataFrame) -> Tuple[int, List[str], Dict[str, Any]]:
    c5, p5 = df5.iloc[-1], df5.iloc[-2]
    c15 = df15.iloc[-1]
    c1h = df1h.iloc[-1]
    c4h = df4h.iloc[-1]
    parts = candle_parts(c15)
    price = safe_float(c15["close"])

    change_1h = pct_change(price, safe_float(df15.iloc[-5]["close"])) if len(df15) >= 6 else 0
    change_4h = pct_change(price, safe_float(df15.iloc[-17]["close"])) if len(df15) >= 18 else 0
    change_24h = pct_change(safe_float(c1h["close"]), safe_float(df1h.iloc[-25]["close"])) if len(df1h) >= 26 else 0
    vol_ratio = safe_float(c15["vol_ratio"], 1.0)
    rsi15 = safe_float(c15["rsi"], 50)
    rsi5 = safe_float(c5["rsi"], 50)
    rsi5_prev = safe_float(p5["rsi"], 50)
    atr_pct = safe_float(c15["atr_pct"], 0)

    score = 0
    reasons: List[str] = []

    if change_1h >= 3.0:
        score += 14
        add_reason(reasons, f"1h сильный рост {change_1h:.2f}%")
    if change_4h >= 6.0:
        score += 14
        add_reason(reasons, f"4h перегрев {change_4h:.2f}%")
    if change_24h >= 10.0:
        score += 6
        add_reason(reasons, f"24h перегрев {change_24h:.2f}%")

    if parts["is_bear"]:
        score += 8
        add_reason(reasons, "15m свеча закрывается медвежьей")
    if parts["upper_wick"] >= 0.30:
        score += 10
        add_reason(reasons, "верхняя тень показывает продавца")
    if price < safe_float(c15["ema9"]):
        score += 8
        add_reason(reasons, "возврат ниже EMA9")
    if price < safe_float(c5["vwap50"]):
        score += 7
        add_reason(reasons, "5m ниже VWAP")
    if vol_ratio >= 1.35:
        score += min(16, int(8 + (vol_ratio - 1.35) * 6))
        add_reason(reasons, f"объем x{vol_ratio:.2f}")
    if 46 <= rsi15 <= 82:
        score += 8
        add_reason(reasons, f"RSI15 {rsi15:.1f}, зона отбоя")
    if rsi5_prev - rsi5 >= 2.0:
        score += 7
        add_reason(reasons, "RSI5 разворачивается вниз")
    if 0.45 <= atr_pct <= 7.0:
        score += 7
        add_reason(reasons, f"ATR15 {atr_pct:.2f}% подходит для скальпа")

    if parts["candle_change_pct"] < -5.5:
        score -= 12
        add_reason(reasons, "анти-чейз: свеча уже слишком большая")
    elif parts["candle_change_pct"] <= -0.25:
        score += 5

    if safe_float(c1h["close"]) < safe_float(c1h["ema20"]):
        score += 4
    if safe_float(c4h["close"]) < safe_float(c4h["ema50"]):
        score += 3

    metrics = {
        "change_1h": round(change_1h, 2),
        "change_4h": round(change_4h, 2),
        "change_24h": round(change_24h, 2),
        "vol_ratio": round(vol_ratio, 2),
        "rsi15": round(rsi15, 1),
        "rsi5": round(rsi5, 1),
        "atr_pct": round(atr_pct, 2),
        "upper_wick": round(parts["upper_wick"], 2),
    }
    return int(clamp(score, 0, 99)), reasons, metrics


def score_breakout_long(df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame, df4h: pd.DataFrame) -> Tuple[int, List[str], Dict[str, Any]]:
    c15 = df15.iloc[-1]
    c5 = df5.iloc[-1]
    c1h = df1h.iloc[-1]
    parts = candle_parts(c15)
    price = safe_float(c15["close"])
    prev_high = safe_float(df15.iloc[-36:-1]["high"].max()) if len(df15) >= 40 else safe_float(df15.iloc[:-1]["high"].max())
    change_1h = pct_change(price, safe_float(df15.iloc[-5]["close"])) if len(df15) >= 6 else 0
    vol_ratio = safe_float(c15["vol_ratio"], 1.0)
    rsi15 = safe_float(c15["rsi"], 50)
    atr_pct = safe_float(c15["atr_pct"], 0)

    score = 0
    reasons: List[str] = []
    breakout = price > prev_high * 1.001

    if breakout:
        score += 24
        add_reason(reasons, "пробой 15m сопротивления")
    if vol_ratio >= 1.5:
        score += min(18, int(10 + (vol_ratio - 1.5) * 6))
        add_reason(reasons, f"пробой на объеме x{vol_ratio:.2f}")
    if price > safe_float(c15["vwap50"]):
        score += 8
        add_reason(reasons, "цена выше VWAP15")
    if price > safe_float(c15["ema20"]):
        score += 7
        add_reason(reasons, "цена выше EMA20")
    if safe_float(c1h["close"]) > safe_float(c1h["ema50"]):
        score += 8
        add_reason(reasons, "1h тренд поддерживает LONG")
    if safe_float(df4h.iloc[-1]["close"]) > safe_float(df4h.iloc[-1]["ema20"]):
        score += 5
    if parts["is_bull"] and parts["body_pct_of_range"] >= 0.45:
        score += 8
        add_reason(reasons, "тело свечи подтверждает импульс")
    if 52 <= rsi15 <= 76:
        score += 7
        add_reason(reasons, f"RSI15 {rsi15:.1f}, импульс без экстремума")
    if 0.45 <= atr_pct <= 6.5:
        score += 6
    if change_1h > 12:
        score -= 12
        add_reason(reasons, "анти-чейз: 1h уже слишком разогналась")

    metrics = {
        "prev_resistance": fmt_price(prev_high),
        "change_1h": round(change_1h, 2),
        "vol_ratio": round(vol_ratio, 2),
        "rsi15": round(rsi15, 1),
        "atr_pct": round(atr_pct, 2),
    }
    return int(clamp(score, 0, 99)), reasons, metrics


def score_breakdown_short(df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame, df4h: pd.DataFrame) -> Tuple[int, List[str], Dict[str, Any]]:
    c15 = df15.iloc[-1]
    c1h = df1h.iloc[-1]
    parts = candle_parts(c15)
    price = safe_float(c15["close"])
    prev_low = safe_float(df15.iloc[-36:-1]["low"].min()) if len(df15) >= 40 else safe_float(df15.iloc[:-1]["low"].min())
    change_1h = pct_change(price, safe_float(df15.iloc[-5]["close"])) if len(df15) >= 6 else 0
    vol_ratio = safe_float(c15["vol_ratio"], 1.0)
    rsi15 = safe_float(c15["rsi"], 50)
    atr_pct = safe_float(c15["atr_pct"], 0)

    score = 0
    reasons: List[str] = []
    breakdown = price < prev_low * 0.999

    if breakdown:
        score += 24
        add_reason(reasons, "пробой 15m поддержки вниз")
    if vol_ratio >= 1.5:
        score += min(18, int(10 + (vol_ratio - 1.5) * 6))
        add_reason(reasons, f"пролом на объеме x{vol_ratio:.2f}")
    if price < safe_float(c15["vwap50"]):
        score += 8
        add_reason(reasons, "цена ниже VWAP15")
    if price < safe_float(c15["ema20"]):
        score += 7
        add_reason(reasons, "цена ниже EMA20")
    if safe_float(c1h["close"]) < safe_float(c1h["ema50"]):
        score += 8
        add_reason(reasons, "1h тренд поддерживает SHORT")
    if safe_float(df4h.iloc[-1]["close"]) < safe_float(df4h.iloc[-1]["ema20"]):
        score += 5
    if parts["is_bear"] and parts["body_pct_of_range"] >= 0.45:
        score += 8
        add_reason(reasons, "тело свечи подтверждает импульс вниз")
    if 24 <= rsi15 <= 48:
        score += 7
        add_reason(reasons, f"RSI15 {rsi15:.1f}, продавец активен")
    if 0.45 <= atr_pct <= 6.5:
        score += 6
    if change_1h < -12:
        score -= 12
        add_reason(reasons, "анти-чейз: 1h уже слишком упала")

    metrics = {
        "prev_support": fmt_price(prev_low),
        "change_1h": round(change_1h, 2),
        "vol_ratio": round(vol_ratio, 2),
        "rsi15": round(rsi15, 1),
        "atr_pct": round(atr_pct, 2),
    }
    return int(clamp(score, 0, 99)), reasons, metrics


def is_hot_enough(df15: pd.DataFrame) -> bool:
    if len(df15) < 60:
        return False
    df15 = add_indicators(df15)
    last = df15.iloc[-1]
    price = safe_float(last["close"])
    change_1h = abs(pct_change(price, safe_float(df15.iloc[-5]["close"]))) if len(df15) >= 6 else 0
    change_4h = abs(pct_change(price, safe_float(df15.iloc[-17]["close"]))) if len(df15) >= 18 else 0
    vol_ratio = safe_float(last["vol_ratio"], 1.0)
    atr_pct = safe_float(last["atr_pct"], 0)

    # Нужно движение. Иначе бот будет ловить мертвый флэт.
    return (
        change_1h >= 1.4
        or change_4h >= 3.0
        or vol_ratio >= 1.45
        or atr_pct >= 0.75
    )


def choose_best_signal(symbol: str, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame, df4h: pd.DataFrame) -> Optional[Signal]:
    if min(len(df5), len(df15), len(df1h), len(df4h)) < 50:
        return None

    df5 = add_indicators(df5)
    df15 = add_indicators(df15)
    df1h = add_indicators(df1h)
    df4h = add_indicators(df4h)
    entry = safe_float(df15.iloc[-1]["close"])
    base = base_from_symbol(symbol)

    scored = []
    s, r, m = score_long_bounce(df5, df15, df1h, df4h)
    scored.append((s, "LONG", "ОТСКОК ПОСЛЕ ПРОЛИВА", r, m))
    s, r, m = score_short_rejection(df5, df15, df1h, df4h)
    scored.append((s, "SHORT", "ОТБОЙ ПОСЛЕ ПАМПА", r, m))
    s, r, m = score_breakout_long(df5, df15, df1h, df4h)
    scored.append((s, "LONG", "ПРОБОЙ СОПРОТИВЛЕНИЯ", r, m))
    s, r, m = score_breakdown_short(df5, df15, df1h, df4h)
    scored.append((s, "SHORT", "ПРОБОЙ ПОДДЕРЖКИ", r, m))

    best_score, side, setup, reasons, metrics = max(scored, key=lambda x: x[0])
    if best_score <= 0:
        return None

    targets, average, stop = make_levels(entry, side)
    return Signal(
        symbol=symbol,
        base=base,
        side=side,
        setup=setup,
        quality=best_score,
        entry=entry,
        targets=targets,
        average=average,
        stop=stop,
        leverage=LEVERAGE,
        score_reasons=reasons,
        metrics=metrics,
    )


def signal_to_text(sig: Signal) -> str:
    q = "A" if sig.quality >= 85 else "B" if sig.quality >= MIN_SCORE else "B-"
    lines = []
    lines.append(f"Скальп-позиция - {sig.base} {sig.side}")
    lines.append(f"Сетап: {sig.setup} · качество {q} {sig.quality}%")
    lines.append(f"Плечо: x{sig.leverage} · биржа: BingX USDT-M")
    lines.append("")
    lines.append(f"Моя точка входа - {fmt_price(sig.entry)}")
    lines.append("")
    lines.append("Лимитные ордера на фиксацию выставить на значениях:")
    lines.append("")
    for i, target in enumerate(sig.targets, start=1):
        roi = roi_pct(sig.entry, target, sig.side, sig.leverage)
        lines.append(f"TP{i}: {fmt_price(target)}  (+{roi:.1f}% ROI x{sig.leverage})")
    lines.append("")
    lines.append(f"Лимитный ордер на усреднение: {fmt_price(sig.average)}")
    lines.append(f"Защитный стоп после усреднения: {fmt_price(sig.stop)}")
    lines.append("")
    if sig.score_reasons:
        lines.append("Почему бот дал сетап:")
        for reason in sig.score_reasons[:6]:
            lines.append(f"• {reason}")
        lines.append("")
    lines.append("Риск: бот не открывает сделку сам. Не заходи всем депозитом; стоп обязателен.")
    lines.append("О любых действиях по открытой сделке бот будет сообщать в канале.")
    return "\n".join(lines)


def update_to_text(sig: Signal, event: str, price: float, extra: str = "") -> str:
    roi = roi_pct(sig.entry, price, sig.side, sig.leverage)
    lines = [f"{event} — {sig.base} {sig.side}"]
    lines.append(f"Цена: {fmt_price(price)} · результат: {roi:+.2f}% ROI x{sig.leverage}")
    if extra:
        lines.append(extra)
    return "\n".join(lines)


def is_excluded_symbol(symbol: str, market: Dict[str, Any]) -> bool:
    base = str(market.get("base") or base_from_symbol(symbol)).upper()
    if base in EXCLUDED_BASES:
        return True
    bad_parts = ["UP/", "DOWN/", "BULL/", "BEAR/", "3L/", "3S/", "5L/", "5S/"]
    up = symbol.upper()
    if any(part in up for part in bad_parts):
        return True
    return False


def ticker_quote_volume(ticker: Dict[str, Any]) -> float:
    qv = safe_float(ticker.get("quoteVolume"), 0)
    if qv > 0:
        return qv
    last = safe_float(ticker.get("last"), 0)
    bv = safe_float(ticker.get("baseVolume"), 0)
    return last * bv


async def make_exchange():
    ex = ccxt.bingx({
        "enableRateLimit": True,
        "timeout": 20_000,
        "options": {
            "defaultType": "swap",
        },
    })
    return ex


async def fetch_ohlcv_safe(symbol: str, timeframe: str, limit: int = 120) -> Optional[pd.DataFrame]:
    try:
        raw = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not raw or len(raw) < 40:
            return None
        return ohlcv_to_df(raw)
    except Exception as e:
        logger.debug("OHLCV failed %s %s: %s", symbol, timeframe, e)
        return None


async def get_tradeable_symbols() -> List[Tuple[str, Dict[str, Any], float]]:
    try:
        markets = await exchange.load_markets()
        tickers = await exchange.fetch_tickers()
    except Exception as e:
        logger.error("Could not load markets/tickers: %s", e)
        return []

    result: List[Tuple[str, Dict[str, Any], float]] = []
    for symbol, market in markets.items():
        try:
            if not market.get("active", True):
                continue
            if not market.get("swap"):
                continue
            if str(market.get("quote", "")).upper() != "USDT":
                continue
            if is_excluded_symbol(symbol, market):
                continue
            ticker = tickers.get(symbol) or {}
            qv = ticker_quote_volume(ticker)
            if qv < MIN_24H_QUOTE_VOLUME_USDT:
                continue
            result.append((symbol, market, qv))
        except Exception:
            continue

    result.sort(key=lambda x: x[2], reverse=True)
    return result[:TOP_SYMBOLS_LIMIT]


def can_send_symbol(symbol: str) -> bool:
    reset_daily_if_needed()
    if state.signals_today >= MAX_SIGNALS_PER_DAY:
        return False
    cooldown_until = state.cooldowns.get(symbol, 0)
    return time.time() >= cooldown_until


def current_threshold() -> int:
    """Adaptive mode: if bot is completely silent, allow one weaker B- setup."""
    reset_daily_if_needed()
    if state.signals_today >= DAILY_MIN_SIGNALS:
        return MIN_SCORE
    hours_since_signal = (time.time() - state.last_signal_ts) / 3600 if state.last_signal_ts else 999
    if hours_since_signal >= 10:
        return min(MIN_SCORE, FALLBACK_SCORE)
    return MIN_SCORE


async def analyze_symbol(symbol: str) -> Optional[Signal]:
    df15 = await fetch_ohlcv_safe(symbol, "15m", 140)
    if df15 is None or not is_hot_enough(df15):
        return None

    # Загружаем тяжелые таймфреймы только если монета уже горячая.
    df5, df1h, df4h = await asyncio.gather(
        fetch_ohlcv_safe(symbol, "5m", 140),
        fetch_ohlcv_safe(symbol, "1h", 120),
        fetch_ohlcv_safe(symbol, "4h", 120),
    )
    if df5 is None or df1h is None or df4h is None:
        return None
    return choose_best_signal(symbol, df5, df15, df1h, df4h)


async def send_new_signal(sig: Signal) -> None:
    text = signal_to_text(sig)
    ok = await send_telegram(text)
    if ok:
        state.signals_today += 1
        state.last_signal_ts = time.time()
        state.cooldowns[sig.symbol] = time.time() + COOLDOWN_MINUTES * 60
        state.active[sig.symbol] = asdict(sig)
        save_state()
        logger.info("Signal sent: %s %s quality=%s", sig.symbol, sig.side, sig.quality)


async def track_active_signals() -> None:
    if not state.active:
        return
    try:
        tickers = await exchange.fetch_tickers(list(state.active.keys()))
    except Exception:
        tickers = {}

    changed = False
    to_remove: List[str] = []
    for symbol, raw_sig in list(state.active.items()):
        try:
            sig = Signal(**raw_sig)
            ticker = tickers.get(symbol) or {}
            price = safe_float(ticker.get("last"), 0)
            if price <= 0:
                continue

            # Average order hit
            if not sig.average_hit:
                avg_hit = (price <= sig.average) if sig.side == "LONG" else (price >= sig.average)
                if avg_hit:
                    sig.average_hit = True
                    changed = True
                    await send_telegram(update_to_text(sig, "⚠️ Усреднение достигнуто", price, f"Уровень усреднения: {fmt_price(sig.average)}"))

            # Stop hit
            stop_hit = (price <= sig.stop) if sig.side == "LONG" else (price >= sig.stop)
            if stop_hit:
                sig.closed = True
                changed = True
                await send_telegram(update_to_text(sig, "🛑 Защитный стоп", price, "Сетап закрыт ботом в мониторинге."))
                to_remove.append(symbol)
                continue

            # Targets hit
            for idx, target in enumerate(sig.targets, start=1):
                if idx in sig.hit_targets:
                    continue
                hit = (price >= target) if sig.side == "LONG" else (price <= target)
                if hit:
                    sig.hit_targets.append(idx)
                    changed = True
                    await send_telegram(update_to_text(sig, f"✅ TP{idx} взят", price, f"Цель: {fmt_price(target)}"))

            if len(sig.hit_targets) >= len(sig.targets):
                sig.closed = True
                await send_telegram(f"🏁 {sig.base} {sig.side}: все 5 целей достигнуты. Сделка закрыта в мониторинге.")
                to_remove.append(symbol)
            else:
                state.active[symbol] = asdict(sig)
        except Exception as e:
            logger.warning("Track active failed for %s: %s", symbol, e)

    for symbol in to_remove:
        state.active.pop(symbol, None)
    if changed or to_remove:
        save_state()


async def scanner_loop() -> None:
    global last_scan_summary
    await asyncio.sleep(3)
    while True:
        reset_daily_if_needed()
        scan_started = time.time()
        checked = 0
        hot = 0
        best_seen: List[Tuple[int, str, str, str]] = []
        sent = 0
        err = None

        try:
            await track_active_signals()
            symbols = await get_tradeable_symbols()
            threshold = current_threshold()
            candidates: List[Signal] = []

            for symbol, market, qv in symbols:
                checked += 1
                if not can_send_symbol(symbol):
                    continue
                sig = await analyze_symbol(symbol)
                if sig is None:
                    continue
                hot += 1
                best_seen.append((sig.quality, display_pair(sig.symbol), sig.side, sig.setup))
                if sig.quality >= threshold:
                    candidates.append(sig)

                # Do not scan forever when daily limit is already nearly reached.
                await asyncio.sleep(0.05)

            candidates.sort(key=lambda s: s.quality, reverse=True)
            for sig in candidates:
                if state.signals_today >= MAX_SIGNALS_PER_DAY:
                    break
                if not can_send_symbol(sig.symbol):
                    continue
                await send_new_signal(sig)
                sent += 1
                # чтобы не закинуть 5 сигналов сразу одной пачкой
                await asyncio.sleep(2)

            best_seen.sort(reverse=True, key=lambda x: x[0])
            last_scan_summary = {
                "time_utc": datetime.now(timezone.utc).isoformat(),
                "checked": checked,
                "analyzed_hot": hot,
                "sent": sent,
                "threshold": threshold,
                "signals_today": state.signals_today,
                "active_signals": len(state.active),
                "best_seen": best_seen[:10],
                "duration_sec": round(time.time() - scan_started, 1),
            }
            logger.info("Scan: %s", last_scan_summary)

            if SEND_EMPTY_SCAN and sent == 0:
                top = best_seen[:3]
                txt = f"Скан завершен: сигналов нет. Проверено {checked}, threshold {threshold}."
                if top:
                    txt += "\nЛучшие кандидаты:\n" + "\n".join([f"{p} {side} {score}% — {setup}" for score, p, side, setup in top])
                await send_telegram(txt)

        except Exception as e:
            err = str(e)
            logger.error("Scanner loop error: %s\n%s", e, traceback.format_exc())
            await send_telegram(f"⚠️ {APP_NAME}: ошибка сканера\n{e}")

        if err:
            await asyncio.sleep(max(60, SCAN_INTERVAL_SECONDS))
        else:
            elapsed = time.time() - scan_started
            await asyncio.sleep(max(15, SCAN_INTERVAL_SECONDS - elapsed))


@app.on_event("startup")
async def on_startup():
    global exchange, scanner_task
    load_state()
    reset_daily_if_needed()
    exchange = await make_exchange()
    if SEND_STARTUP_MESSAGE:
        await send_telegram(
            "✅ BingX scalp bot запущен\n"
            f"Сканер активен: каждые {SCAN_INTERVAL_SECONDS} сек.\n"
            f"Рынок: BingX USDT-M futures/swap\n"
            f"Формат: entry + 5 TP + усреднение + стоп\n"
            f"Плечо в расчетах ROI: x{LEVERAGE}\n"
            "Бот НЕ открывает сделки, только отправляет сигналы."
        )
    scanner_task = asyncio.create_task(scanner_loop())
    logger.info("Startup complete")


@app.on_event("shutdown")
async def on_shutdown():
    global exchange, scanner_task
    if scanner_task:
        scanner_task.cancel()
    save_state()
    if exchange:
        await exchange.close()


@app.get("/", response_class=PlainTextResponse)
async def root():
    return f"{APP_NAME} is running. Open /status for scanner state."


@app.get("/health")
async def health():
    return {"ok": True, "uptime_sec": int(time.time() - started_at)}


@app.get("/status")
async def status():
    reset_daily_if_needed()
    return JSONResponse(
        {
            "app": APP_NAME,
            "uptime_sec": int(time.time() - started_at),
            "config": {
                "scan_interval_seconds": SCAN_INTERVAL_SECONDS,
                "top_symbols_limit": TOP_SYMBOLS_LIMIT,
                "min_24h_quote_volume_usdt": MIN_24H_QUOTE_VOLUME_USDT,
                "min_score": MIN_SCORE,
                "fallback_score": FALLBACK_SCORE,
                "daily_min_signals": DAILY_MIN_SIGNALS,
                "max_signals_per_day": MAX_SIGNALS_PER_DAY,
                "cooldown_minutes": COOLDOWN_MINUTES,
                "leverage": LEVERAGE,
                "take_pcts": TAKE_PCTS,
                "average_pct": AVERAGE_PCT,
                "stop_after_average_pct": STOP_AFTER_AVERAGE_PCT,
            },
            "state": asdict(state),
            "last_scan_summary": last_scan_summary,
        }
    )
