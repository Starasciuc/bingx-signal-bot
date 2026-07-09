"""
BingX scalp signal bot for Render Background Worker.

Designed for existing Render settings:
Build Command: pip install -r requirements.txt
Start Command: uvicorn bot:app --host 0.0.0.0 --port $PORT

The app never crashes because of Telegram/BingX errors; errors are logged in Render Logs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

try:
    import ccxt  # type: ignore
except Exception as exc:  # keep app alive if dependency/import fails
    ccxt = None  # type: ignore
    CCXT_IMPORT_ERROR = repr(exc)
else:
    CCXT_IMPORT_ERROR = ""

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("bingx-scalp-bot")

# -----------------------------------------------------------------------------
# Environment helpers
# -----------------------------------------------------------------------------

def _clean_env(value: Optional[str], default: str = "") -> str:
    if value is None:
        return default
    value = str(value).strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1].strip()
    return value


def env_str(*names: str, default: str = "") -> str:
    for name in names:
        val = _clean_env(os.getenv(name))
        if val:
            return val
    return default


def env_int(name: str, default: int) -> int:
    try:
        return int(_clean_env(os.getenv(name), str(default)))
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(_clean_env(os.getenv(name), str(default)))
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    val = _clean_env(os.getenv(name))
    if not val:
        return default
    return val.lower() in {"1", "true", "yes", "y", "on", "да"}


def normalize_chat_id(raw: str) -> str:
    """Accept @channel, numeric ids, or simple t.me links and normalize for Telegram API."""
    chat = _clean_env(raw)
    if not chat:
        return ""
    chat = chat.replace(" ", "")
    # Convert https://t.me/channel_username -> @channel_username
    m = re.match(r"^(?:https?://)?t\.me/([A-Za-z0-9_]{5,})/?$", chat)
    if m:
        return "@" + m.group(1)
    # t.me/c/private links are not valid for Bot API without -100 id
    if "t.me/c/" in chat:
        return chat
    return chat

# Telegram env supports several common names, so old Render env can still work.
TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN", "BOT_TOKEN", "TG_BOT_TOKEN", default="")
TELEGRAM_CHAT_ID = normalize_chat_id(env_str("TELEGRAM_CHAT_ID", "CHAT_ID", "TG_CHAT_ID", default=""))
SEND_STARTUP_MESSAGE = env_bool("SEND_STARTUP_MESSAGE", True)

# Scanner settings. Defaults are intentionally not too strict, so the bot does not stay silent forever.
SCAN_INTERVAL_SECONDS = env_int("SCAN_INTERVAL_SECONDS", 180)
TOP_SYMBOLS_LIMIT = env_int("TOP_SYMBOLS_LIMIT", 100)
MIN_SCORE = env_int("MIN_SCORE", 72)
FALLBACK_SCORE = env_int("FALLBACK_SCORE", 66)
DAILY_MIN_SIGNALS = env_int("DAILY_MIN_SIGNALS", 1)
MAX_SIGNALS_PER_DAY = env_int("MAX_SIGNALS_PER_DAY", 6)
COOLDOWN_MINUTES = env_int("COOLDOWN_MINUTES", 180)
LEVERAGE = env_int("LEVERAGE", 20)
TRACK_INTERVAL_SECONDS = env_int("TRACK_INTERVAL_SECONDS", 45)
SEND_SCAN_UPDATES = env_bool("SEND_SCAN_UPDATES", False)
EXCHANGE_TIMEOUT_MS = env_int("EXCHANGE_TIMEOUT_MS", 15000)

STATE_FILE = Path(env_str("STATE_FILE", default="bot_state.json"))

# -----------------------------------------------------------------------------
# Data models/state
# -----------------------------------------------------------------------------
@dataclass
class Trade:
    id: str
    symbol: str
    side: str  # LONG/SHORT
    strategy: str
    entry: float
    take_profits: List[float]
    average: float
    stop: float
    score: int
    created_at: float
    status: str = "OPEN"
    hit_tps: int = 0
    averaged: bool = False
    closed_at: Optional[float] = None
    close_reason: str = ""
    best_roi: float = 0.0
    worst_roi: float = 0.0


state: Dict[str, Any] = {
    "started_at": time.time(),
    "signals_today_date": time.strftime("%Y-%m-%d"),
    "signals_today": 0,
    "last_signal_ts_by_symbol": {},
    "active_trades": {},
    "closed_trades": [],
    "stats": {
        "total_signals": 0,
        "closed": 0,
        "profit": 0,
        "loss": 0,
        "tp1": 0,
        "tp5": 0,
        "sl": 0,
        "averages": 0,
        "best_roi": 0.0,
        "worst_roi": 0.0,
    },
}
state_lock = asyncio.Lock()

# -----------------------------------------------------------------------------
# Utility calculations
# -----------------------------------------------------------------------------

def fmt_price(x: float) -> str:
    if x == 0 or not math.isfinite(x):
        return "0"
    ax = abs(x)
    if ax >= 100:
        return f"{x:.3f}".rstrip("0").rstrip(".")
    if ax >= 1:
        return f"{x:.4f}".rstrip("0").rstrip(".")
    if ax >= 0.01:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def pct_change(old: float, new: float) -> float:
    if not old:
        return 0.0
    return (new / old - 1.0) * 100.0


def ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    k = 2.0 / (period + 1.0)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1.0 - k)
    return e


def rsi(values: List[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr(ohlcv: List[List[float]], period: int = 14) -> float:
    if len(ohlcv) < 2:
        return 0.0
    trs = []
    for i in range(1, len(ohlcv)):
        high = float(ohlcv[i][2])
        low = float(ohlcv[i][3])
        prev_close = float(ohlcv[i - 1][4])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if not trs:
        return 0.0
    return sum(trs[-period:]) / min(period, len(trs))


def volume_ratio(ohlcv: List[List[float]], period: int = 20) -> float:
    if len(ohlcv) < 3:
        return 1.0
    last_vol = float(ohlcv[-1][5])
    vols = [float(c[5]) for c in ohlcv[-period - 1:-1] if float(c[5]) > 0]
    if not vols:
        return 1.0
    avg = sum(vols) / len(vols)
    return last_vol / avg if avg else 1.0


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        x = float(v)
        if not math.isfinite(x):
            return default
        return x
    except Exception:
        return default

# -----------------------------------------------------------------------------
# Persistence
# -----------------------------------------------------------------------------

def save_state() -> None:
    try:
        serializable = dict(state)
        serializable["active_trades"] = {
            k: asdict(v) if isinstance(v, Trade) else v
            for k, v in state.get("active_trades", {}).items()
        }
        STATE_FILE.write_text(json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("State save failed: %s", exc)


def load_state() -> None:
    if not STATE_FILE.exists():
        return
    try:
        loaded = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        state.update(loaded)
        active = {}
        for k, v in loaded.get("active_trades", {}).items():
            try:
                active[k] = Trade(**v)
            except Exception:
                pass
        state["active_trades"] = active
        logger.info("State loaded: active=%s closed=%s", len(active), len(state.get("closed_trades", [])))
    except Exception as exc:
        logger.warning("State load failed: %s", exc)

# -----------------------------------------------------------------------------
# Telegram
# -----------------------------------------------------------------------------

def telegram_config_status() -> Dict[str, Any]:
    token = TELEGRAM_BOT_TOKEN
    chat = TELEGRAM_CHAT_ID
    return {
        "token_present": bool(token),
        "token_prefix": token[:8] + "..." if token and len(token) > 8 else "",
        "chat_id_present": bool(chat),
        "chat_id": chat if chat else "",
        "chat_warning": "t.me/c links do not work with Bot API; use -100... numeric id" if "t.me/c/" in chat else "",
    }


def telegram_api(method: str, payload: Optional[Dict[str, Any]] = None, timeout: int = 12) -> Tuple[bool, Dict[str, Any]]:
    if not TELEGRAM_BOT_TOKEN:
        return False, {"description": "TELEGRAM_BOT_TOKEN is empty"}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        if payload is None:
            resp = requests.get(url, timeout=timeout)
        else:
            resp = requests.post(url, json=payload, timeout=timeout)
        try:
            data = resp.json()
        except Exception:
            data = {"http_status": resp.status_code, "text": resp.text[:500]}
        ok = bool(resp.ok and data.get("ok", False))
        if not ok:
            logger.error("Telegram API error method=%s status=%s response=%s", method, resp.status_code, data)
        return ok, data
    except Exception as exc:
        logger.error("Telegram request failed method=%s error=%r", method, exc)
        return False, {"description": repr(exc)}


def send_telegram(text: str, disable_notification: bool = False) -> bool:
    if not TELEGRAM_CHAT_ID:
        logger.error("Telegram send skipped: TELEGRAM_CHAT_ID is empty")
        return False
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "disable_notification": disable_notification,
    }
    ok, data = telegram_api("sendMessage", payload)
    if ok:
        logger.info("Telegram message sent successfully")
    else:
        logger.error("Telegram message was NOT sent: %s", data)
    return ok


def escape_html(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def stats_text() -> str:
    st = state.get("stats", {})
    closed = int(st.get("closed", 0))
    profit = int(st.get("profit", 0))
    loss = int(st.get("loss", 0))
    wr = (profit / closed * 100.0) if closed else 0.0
    active = len(state.get("active_trades", {}))
    return (
        "📊 <b>Статистика бота</b>\n\n"
        f"Всего сигналов: <b>{int(st.get('total_signals', 0))}</b>\n"
        f"Активные сделки: <b>{active}</b>\n"
        f"Закрытые сделки: <b>{closed}</b>\n"
        f"✅ Прибыльные: <b>{profit}</b>\n"
        f"🛑 Убыточные: <b>{loss}</b>\n"
        f"Win rate: <b>{wr:.1f}%</b>\n"
        f"TP1: <b>{int(st.get('tp1', 0))}</b> · TP5: <b>{int(st.get('tp5', 0))}</b>\n"
        f"Усреднений: <b>{int(st.get('averages', 0))}</b> · SL: <b>{int(st.get('sl', 0))}</b>\n"
        f"Лучший ROI: <b>{float(st.get('best_roi', 0.0)):.2f}%</b>\n"
        f"Худший ROI: <b>{float(st.get('worst_roi', 0.0)):.2f}%</b>"
    )

# -----------------------------------------------------------------------------
# Exchange/scanner
# -----------------------------------------------------------------------------
_exchange = None


def get_exchange():
    global _exchange
    if ccxt is None:
        raise RuntimeError(f"ccxt import failed: {CCXT_IMPORT_ERROR}")
    if _exchange is None:
        _exchange = ccxt.bingx({
            "enableRateLimit": True,
            "timeout": EXCHANGE_TIMEOUT_MS,
            "options": {"defaultType": "swap"},
        })
        logger.info("Loading BingX markets...")
        _exchange.load_markets()
        logger.info("BingX markets loaded: %s", len(getattr(_exchange, "markets", {}) or {}))
    return _exchange


def fetch_top_symbols(limit: int) -> List[str]:
    ex = get_exchange()
    markets = getattr(ex, "markets", {}) or {}
    candidates = []
    for symbol, market in markets.items():
        try:
            if not market.get("active", True):
                continue
            quote = market.get("quote")
            base = str(market.get("base") or "")
            if quote != "USDT":
                continue
            # prefer perpetual swaps/futures; accept if info is incomplete
            is_contract = bool(market.get("contract")) or bool(market.get("swap")) or ":USDT" in symbol
            if not is_contract:
                continue
            if any(x in base.upper() for x in ["UP", "DOWN", "BULL", "BEAR"]):
                continue
            candidates.append(symbol)
        except Exception:
            continue

    if not candidates:
        # fallback: any USDT market
        candidates = [s for s in markets.keys() if "/USDT" in s]

    # Try ranking by quoteVolume, but do not fail if BingX/ccxt blocks it.
    try:
        tickers = ex.fetch_tickers(candidates[: min(len(candidates), 300)])
        ranked = []
        for sym in candidates:
            t = tickers.get(sym, {}) if isinstance(tickers, dict) else {}
            qv = safe_float(t.get("quoteVolume") or t.get("baseVolume") or 0.0)
            ranked.append((qv, sym))
        ranked.sort(reverse=True)
        result = [s for _, s in ranked if s][:limit]
        if result:
            return result
    except Exception as exc:
        logger.warning("fetch_tickers ranking failed, using market list: %r", exc)

    random.shuffle(candidates)
    return candidates[:limit]


def fetch_ohlcv_safe(symbol: str, timeframe: str, limit: int) -> List[List[float]]:
    try:
        data = get_exchange().fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return data or []
    except Exception as exc:
        logger.debug("OHLCV failed %s %s: %r", symbol, timeframe, exc)
        return []


def analyze_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    o15 = fetch_ohlcv_safe(symbol, "15m", 80)
    if len(o15) < 35:
        return None
    o5 = fetch_ohlcv_safe(symbol, "5m", 80)
    o1h = fetch_ohlcv_safe(symbol, "1h", 60)
    closes15 = [float(x[4]) for x in o15]
    highs15 = [float(x[2]) for x in o15]
    lows15 = [float(x[3]) for x in o15]
    last = o15[-1]
    prev = o15[-2]
    price = float(last[4])
    if price <= 0:
        return None

    last_open = float(last[1])
    last_high = float(last[2])
    last_low = float(last[3])
    prev_close = float(prev[4])
    r = rsi(closes15, 14)
    e20 = ema(closes15, 20)
    e50 = ema(closes15, 50)
    a = atr(o15, 14)
    atr_pct = (a / price * 100.0) if price else 0.0
    vr = volume_ratio(o15, 20)

    lookback = o15[-24:]
    recent_high = max(float(c[2]) for c in lookback)
    recent_low = min(float(c[3]) for c in lookback)
    change_6h = pct_change(float(lookback[0][4]), price)
    candle_pct = pct_change(last_open, price)
    prev_12_change = pct_change(float(o15[-13][4]), float(o15[-2][4])) if len(o15) >= 14 else 0.0

    # 5m confirmation
    confirm_5m_long = False
    confirm_5m_short = False
    if len(o5) >= 25:
        c5 = [float(x[4]) for x in o5]
        confirm_5m_long = c5[-1] > ema(c5, 9) and c5[-1] > c5[-2]
        confirm_5m_short = c5[-1] < ema(c5, 9) and c5[-1] < c5[-2]

    # 1h directional context
    h1_change = 0.0
    if len(o1h) >= 7:
        h1_change = pct_change(float(o1h[-7][4]), float(o1h[-1][4]))

    setups: List[Dict[str, Any]] = []

    # LONG: strong fall, then bounce/reclaim. Similar to "упала сильно и отскочила".
    long_score = 45
    if change_6h <= -4.0 or prev_12_change <= -3.0:
        long_score += 10
    if price > last_open and candle_pct > 0.15:
        long_score += 8
    if price > prev_close:
        long_score += 5
    if 28 <= r <= 58:
        long_score += 8
    if price > e20 or price > ((recent_low + recent_high) / 2.0):
        long_score += 6
    if vr >= 1.20:
        long_score += min(12, int((vr - 1.0) * 8))
    if confirm_5m_long:
        long_score += 8
    if h1_change > -10:
        long_score += 3
    if atr_pct >= 0.35:
        long_score += 4
    if (price / recent_low - 1.0) * 100.0 > 8.0:  # avoid chasing after huge rebound
        long_score -= 8
    setups.append({"side": "LONG", "score": long_score, "strategy": "REB / отскок после пролива"})

    # SHORT: strong pump, then rejection. Similar to "выросла 20-30% и отскочила вниз".
    short_score = 45
    if change_6h >= 4.0 or prev_12_change >= 3.0:
        short_score += 10
    if price < last_open and candle_pct < -0.15:
        short_score += 8
    if price < prev_close:
        short_score += 5
    if 42 <= r <= 78:
        short_score += 8
    if price < e20 or price < ((recent_low + recent_high) / 2.0):
        short_score += 6
    if vr >= 1.20:
        short_score += min(12, int((vr - 1.0) * 8))
    if confirm_5m_short:
        short_score += 8
    if h1_change < 10:
        short_score += 3
    if atr_pct >= 0.35:
        short_score += 4
    if (recent_high / price - 1.0) * 100.0 > 8.0:  # avoid chasing too low after dump
        short_score -= 8
    setups.append({"side": "SHORT", "score": short_score, "strategy": "REB / отбой после пампа"})

    # Breakout / breakdown confirmations add extra optional setup when levels are broken.
    prev_high = max(highs15[-25:-2])
    prev_low = min(lows15[-25:-2])
    if price > prev_high and vr >= 1.15 and price > e20:
        setups.append({"side": "LONG", "score": 68 + min(16, int((vr - 1.0) * 10)), "strategy": "BREAKOUT / пробой сопротивления"})
    if price < prev_low and vr >= 1.15 and price < e20:
        setups.append({"side": "SHORT", "score": 68 + min(16, int((vr - 1.0) * 10)), "strategy": "BREAKDOWN / пробой поддержки"})

    best = max(setups, key=lambda x: x["score"])
    best.update({
        "symbol": symbol,
        "price": price,
        "rsi": r,
        "vol_ratio": vr,
        "change_6h": change_6h,
        "atr_pct": atr_pct,
        "ema20": e20,
        "ema50": e50,
    })
    return best


def build_trade(candidate: Dict[str, Any]) -> Trade:
    symbol = candidate["symbol"]
    side = candidate["side"]
    entry = float(candidate["price"])
    # Similar Telegram scalper ladder: 0.8%, 1.4%, 2.0%, 3.0%, 4.0% spot move.
    tp_steps = [0.008, 0.014, 0.020, 0.030, 0.040]
    avg_step = 0.077
    stop_step = 0.105
    if side == "LONG":
        tps = [entry * (1 + s) for s in tp_steps]
        avg = entry * (1 - avg_step)
        stop = entry * (1 - stop_step)
    else:
        tps = [entry * (1 - s) for s in tp_steps]
        avg = entry * (1 + avg_step)
        stop = entry * (1 + stop_step)
    tid = f"{symbol}-{side}-{int(time.time())}"
    return Trade(
        id=tid,
        symbol=symbol,
        side=side,
        strategy=str(candidate.get("strategy", "SCALP")),
        entry=entry,
        take_profits=tps,
        average=avg,
        stop=stop,
        score=int(candidate.get("score", 0)),
        created_at=time.time(),
    )


def signal_text(trade: Trade, candidate: Dict[str, Any]) -> str:
    clean_symbol = trade.symbol.replace(":USDT", "").replace("/", "")
    tp_lines = "\n".join(fmt_price(x) for x in trade.take_profits)
    side_emoji = "🟢" if trade.side == "LONG" else "🔴"
    roi_tp1 = abs((trade.take_profits[0] / trade.entry - 1.0) * 100.0) * LEVERAGE
    roi_tp5 = abs((trade.take_profits[-1] / trade.entry - 1.0) * 100.0) * LEVERAGE
    return (
        f"{side_emoji} <b>Скальп-позиция - {escape_html(clean_symbol)} {trade.side}</b>\n\n"
        f"Стратегия: <b>{escape_html(trade.strategy)}</b>\n"
        f"Качество сетапа: <b>{trade.score}%</b>\n"
        f"Моя точка входа - <b>{fmt_price(trade.entry)}</b>\n\n"
        "Пока заходите, следующим постом пришлю параметры сделки!\n\n"
        "Лимитные ордера на фиксацию выставил на значениях:\n\n"
        f"<code>{tp_lines}</code>\n\n"
        f"Лимитный ордер на усреднение: <b>{fmt_price(trade.average)}</b>\n"
        f"Защитный стоп: <b>{fmt_price(trade.stop)}</b>\n\n"
        f"Плечо: до <b>x{LEVERAGE}</b> · TP1 ≈ <b>{roi_tp1:.1f}% ROI</b> · TP5 ≈ <b>{roi_tp5:.1f}% ROI</b>\n"
        f"RSI: <b>{float(candidate.get('rsi', 0)):.1f}</b> · Vol: <b>x{float(candidate.get('vol_ratio', 0)):.2f}</b> · 6h: <b>{float(candidate.get('change_6h', 0)):+.2f}%</b>\n\n"
        "О любых действиях по открытой сделке буду сообщать в канале\n\n"
        "⚠️ Не финсовет. Риск на сделку лучше держать ≤0.5–1% депозита."
    )


def trade_roi(trade: Trade, price: float) -> float:
    if trade.side == "LONG":
        return (price / trade.entry - 1.0) * 100.0 * LEVERAGE
    return (trade.entry / price - 1.0) * 100.0 * LEVERAGE


def price_hit_tp(trade: Trade, price: float, index: int) -> bool:
    tp = trade.take_profits[index]
    return price >= tp if trade.side == "LONG" else price <= tp


def price_hit_average(trade: Trade, price: float) -> bool:
    return price <= trade.average if trade.side == "LONG" else price >= trade.average


def price_hit_stop(trade: Trade, price: float) -> bool:
    return price <= trade.stop if trade.side == "LONG" else price >= trade.stop


def close_trade(trade: Trade, reason: str, price: float) -> None:
    trade.status = "CLOSED"
    trade.closed_at = time.time()
    trade.close_reason = reason
    roi = trade_roi(trade, price)
    trade.best_roi = max(trade.best_roi, roi)
    trade.worst_roi = min(trade.worst_roi, roi)
    st = state["stats"]
    st["closed"] += 1
    if trade.hit_tps >= 1:
        st["profit"] += 1
    else:
        st["loss"] += 1
    if reason == "SL":
        st["sl"] += 1
    st["best_roi"] = max(float(st.get("best_roi", 0.0)), trade.best_roi)
    st["worst_roi"] = min(float(st.get("worst_roi", 0.0)), trade.worst_roi)
    state["closed_trades"].append(asdict(trade))
    state["active_trades"].pop(trade.id, None)


def reset_daily_if_needed() -> None:
    today = time.strftime("%Y-%m-%d")
    if state.get("signals_today_date") != today:
        state["signals_today_date"] = today
        state["signals_today"] = 0


def can_signal_symbol(symbol: str) -> bool:
    reset_daily_if_needed()
    if int(state.get("signals_today", 0)) >= MAX_SIGNALS_PER_DAY:
        return False
    last_map = state.get("last_signal_ts_by_symbol", {})
    last_ts = float(last_map.get(symbol, 0) or 0)
    return (time.time() - last_ts) >= COOLDOWN_MINUTES * 60


async def scan_once(send_best_fallback: bool = False) -> Dict[str, Any]:
    symbols = await asyncio.to_thread(fetch_top_symbols, TOP_SYMBOLS_LIMIT)
    logger.info("Scanning %s symbols...", len(symbols))
    best: Optional[Dict[str, Any]] = None
    sent = 0
    checked = 0
    for symbol in symbols:
        checked += 1
        if not can_signal_symbol(symbol):
            continue
        try:
            candidate = await asyncio.to_thread(analyze_symbol, symbol)
        except Exception as exc:
            logger.debug("Analyze failed %s: %r", symbol, exc)
            continue
        if not candidate:
            continue
        if best is None or int(candidate.get("score", 0)) > int(best.get("score", 0)):
            best = candidate
        score = int(candidate.get("score", 0))
        if score >= MIN_SCORE:
            trade = build_trade(candidate)
            async with state_lock:
                state["active_trades"][trade.id] = trade
                state["last_signal_ts_by_symbol"][symbol] = time.time()
                state["signals_today"] = int(state.get("signals_today", 0)) + 1
                state["stats"]["total_signals"] += 1
                save_state()
            send_telegram(signal_text(trade, candidate))
            sent += 1
            logger.info("Signal sent: %s %s score=%s", symbol, trade.side, score)
            if int(state.get("signals_today", 0)) >= MAX_SIGNALS_PER_DAY:
                break
            await asyncio.sleep(1)

    # If user wants at least 1/day, allow fallback once daily with lower score.
    reset_daily_if_needed()
    if sent == 0 and best and send_best_fallback and int(state.get("signals_today", 0)) < DAILY_MIN_SIGNALS:
        if int(best.get("score", 0)) >= FALLBACK_SCORE and can_signal_symbol(best["symbol"]):
            trade = build_trade(best)
            async with state_lock:
                state["active_trades"][trade.id] = trade
                state["last_signal_ts_by_symbol"][best["symbol"]] = time.time()
                state["signals_today"] = int(state.get("signals_today", 0)) + 1
                state["stats"]["total_signals"] += 1
                save_state()
            msg = "⚠️ <b>Fallback-сигнал дня</b>\nБот не нашел идеальный сетап, но нашел лучший доступный вариант.\n\n" + signal_text(trade, best)
            send_telegram(msg)
            sent += 1

    return {"checked": checked, "sent": sent, "best": best}


async def scanner_loop() -> None:
    # Wait a little so uvicorn fully starts.
    await asyncio.sleep(2)
    while True:
        try:
            now_hour = int(time.strftime("%H"))
            # Fallback mostly during active daytime/evening UTC; still scans always.
            send_fallback = DAILY_MIN_SIGNALS > 0 and 8 <= now_hour <= 23
            result = await scan_once(send_best_fallback=send_fallback)
            if SEND_SCAN_UPDATES:
                best = result.get("best") or {}
                send_telegram(
                    f"🧪 Scan update\nПроверено: {result.get('checked')} · отправлено: {result.get('sent')}\n"
                    f"Best: {escape_html(best.get('symbol', '-'))} {escape_html(best.get('side', '-'))} score={best.get('score', '-')}",
                    disable_notification=True,
                )
        except Exception as exc:
            logger.exception("scanner_loop error: %r", exc)
            send_telegram(f"⚠️ Ошибка сканера Render:\n<code>{escape_html(repr(exc))}</code>", disable_notification=True)
        await asyncio.sleep(max(30, SCAN_INTERVAL_SECONDS))


async def tracker_loop() -> None:
    await asyncio.sleep(5)
    while True:
        try:
            active_ids = list(state.get("active_trades", {}).keys())
            for tid in active_ids:
                trade = state["active_trades"].get(tid)
                if not isinstance(trade, Trade):
                    continue
                try:
                    ticker = await asyncio.to_thread(get_exchange().fetch_ticker, trade.symbol)
                    price = safe_float(ticker.get("last") or ticker.get("close"))
                    if price <= 0:
                        continue
                except Exception as exc:
                    logger.debug("Ticker failed %s: %r", trade.symbol, exc)
                    continue

                roi = trade_roi(trade, price)
                trade.best_roi = max(trade.best_roi, roi)
                trade.worst_roi = min(trade.worst_roi, roi)

                # Average alert only once
                if not trade.averaged and price_hit_average(trade, price):
                    trade.averaged = True
                    state["stats"]["averages"] += 1
                    send_telegram(
                        f"⚠️ <b>Усреднение активировано</b>\n{escape_html(trade.symbol)} {trade.side}\n"
                        f"Цена: <b>{fmt_price(price)}</b> · уровень: <b>{fmt_price(trade.average)}</b>"
                    )

                # TPs sequentially
                while trade.hit_tps < len(trade.take_profits) and price_hit_tp(trade, price, trade.hit_tps):
                    trade.hit_tps += 1
                    if trade.hit_tps == 1:
                        state["stats"]["tp1"] += 1
                    if trade.hit_tps == 5:
                        state["stats"]["tp5"] += 1
                    send_telegram(
                        f"✅ <b>TP{trade.hit_tps} достигнут</b>\n{escape_html(trade.symbol)} {trade.side}\n"
                        f"Цена: <b>{fmt_price(price)}</b> · ROI примерно: <b>{roi:.2f}%</b>"
                    )

                # Close after TP5 or SL. If SL after TP1, still counts as profitable by channel-style stats.
                if trade.hit_tps >= 5:
                    close_trade(trade, "TP5", price)
                    send_telegram(
                        f"🔥 <b>Сделка закрыта по TP5</b>\n{escape_html(trade.symbol)} {trade.side}\n"
                        f"Лучший ROI: <b>{trade.best_roi:.2f}%</b>\n\n{stats_text()}"
                    )
                elif price_hit_stop(trade, price):
                    close_trade(trade, "SL", price)
                    icon = "✅" if trade.hit_tps >= 1 else "🛑"
                    send_telegram(
                        f"{icon} <b>Сделка закрыта по SL</b>\n{escape_html(trade.symbol)} {trade.side}\n"
                        f"TP взято: <b>{trade.hit_tps}</b> · ROI: <b>{roi:.2f}%</b>\n\n{stats_text()}"
                    )
                save_state()
        except Exception as exc:
            logger.exception("tracker_loop error: %r", exc)
        await asyncio.sleep(max(15, TRACK_INTERVAL_SECONDS))

# -----------------------------------------------------------------------------
# FastAPI app for Render uvicorn bot:app
# -----------------------------------------------------------------------------
app = FastAPI(title="BingX Scalp Signal Bot", version="2026.07.render-safe")
_started = False


@app.on_event("startup")
async def on_startup() -> None:
    global _started
    if _started:
        return
    _started = True
    load_state()
    logger.info("Bot startup. Telegram config: %s", telegram_config_status())
    logger.info("ccxt status: present=%s error=%s", ccxt is not None, CCXT_IMPORT_ERROR)
    logger.info("Settings: scan=%ss top=%s min_score=%s fallback=%s daily_min=%s max_day=%s leverage=x%s", SCAN_INTERVAL_SECONDS, TOP_SYMBOLS_LIMIT, MIN_SCORE, FALLBACK_SCORE, DAILY_MIN_SIGNALS, MAX_SIGNALS_PER_DAY, LEVERAGE)

    # Telegram diagnostics. Never crash startup if Telegram fails.
    ok_me, me_data = await asyncio.to_thread(telegram_api, "getMe", None)
    if ok_me:
        logger.info("Telegram getMe OK: %s", me_data.get("result", {}))
    else:
        logger.error("Telegram getMe FAILED: %s", me_data)

    if SEND_STARTUP_MESSAGE:
        startup_msg = (
            "🚀 <b>BingX scalp signal bot запущен на Render</b>\n\n"
            f"Service: <b>Background Worker</b>\n"
            f"Start command: <code>uvicorn bot:app --host 0.0.0.0 --port $PORT</code>\n"
            f"Scanner: каждые <b>{SCAN_INTERVAL_SECONDS}</b> сек\n"
            f"Score: <b>{MIN_SCORE}</b> / fallback <b>{FALLBACK_SCORE}</b>\n"
            f"Плечо в расчетах: <b>x{LEVERAGE}</b>\n\n"
            "Если ты видишь это сообщение — Telegram подключен правильно."
        )
        for attempt in range(1, 6):
            delivered = await asyncio.to_thread(send_telegram, startup_msg if attempt == 1 else f"✅ Telegram test retry {attempt}: бот на Render работает")
            logger.info("Startup Telegram attempt %s delivered=%s", attempt, delivered)
            if delivered:
                break
            await asyncio.sleep(3)

    asyncio.create_task(scanner_loop())
    asyncio.create_task(tracker_loop())


@app.get("/")
async def root() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "bingx-scalp-signal-bot",
        "uptime_sec": int(time.time() - float(state.get("started_at", time.time()))),
        "telegram": telegram_config_status(),
        "active_trades": len(state.get("active_trades", {})),
        "stats": state.get("stats", {}),
    }


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"ok": True, "time": time.time(), "ccxt_present": ccxt is not None, "ccxt_error": CCXT_IMPORT_ERROR}


@app.get("/telegram/test")
async def telegram_test() -> JSONResponse:
    ok_me, me = await asyncio.to_thread(telegram_api, "getMe", None)
    ok_send = await asyncio.to_thread(send_telegram, "✅ Тест Telegram: бот работает и может отправлять сообщения.")
    return JSONResponse({"getMe_ok": ok_me, "getMe": me, "send_ok": ok_send, "config": telegram_config_status()})


@app.get("/telegram/debug")
async def telegram_debug() -> JSONResponse:
    ok_me, me = await asyncio.to_thread(telegram_api, "getMe", None)
    return JSONResponse({"getMe_ok": ok_me, "getMe": me, "config": telegram_config_status()})


@app.get("/stats")
async def stats() -> JSONResponse:
    return JSONResponse({"stats": state.get("stats", {}), "active": [asdict(t) for t in state.get("active_trades", {}).values() if isinstance(t, Trade)]})


@app.get("/stats/send")
async def send_stats() -> Dict[str, Any]:
    ok = await asyncio.to_thread(send_telegram, stats_text())
    return {"sent": ok}


@app.get("/scan/once")
async def scan_once_endpoint() -> JSONResponse:
    result = await scan_once(send_best_fallback=True)
    return JSONResponse(result)


@app.get("/logs/help", response_class=PlainTextResponse)
async def logs_help() -> str:
    return (
        "Open Render -> your service -> Logs.\n"
        "Look for: Telegram config, Telegram getMe OK/FAILED, Telegram API error.\n"
        "Common Telegram errors:\n"
        "401 Unauthorized = wrong TELEGRAM_BOT_TOKEN\n"
        "400 chat not found = wrong TELEGRAM_CHAT_ID\n"
        "403 Forbidden = press /start in bot chat or add bot as admin in channel\n"
    )


if __name__ == "__main__":
    # Allows local/alternative Render start command: python bot.py
    import uvicorn

    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("bot:app", host="0.0.0.0", port=port, log_level="info")
