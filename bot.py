#!/usr/bin/env python3
"""
Professional Adaptive Futures Signal Bot V20.4.2 RENDER-SAFE PRO CONTINUATION.

Design goals:
* Detect real volatility compression releases and strong active-mover impulses.
* Never treat RSI overbought/oversold as a reversal signal by itself.
* Wait for pullback -> reclaim -> re-acceleration before a continuation entry.
* Keep exhaustion/fade setups in research only until forward evidence is positive.
* Count TP3 as success, use the H-style 0.8/1.4/2.0/3.0/4.4% ladder.
* Never average down, never open opposite signals on the same symbol/event.

This program emits PAPER Telegram signals. It intentionally contains no real-order
endpoint. Real execution should only be added after an independent forward test.
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import math
import os
import re
import signal as os_signal
import statistics
import tempfile
import threading
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

try:
    import aiohttp
except (
    ModuleNotFoundError
):  # Indicator-only unit tests can run before dependencies are installed.
    aiohttp = None  # type: ignore[assignment]


APP_NAME = "Professional Adaptive Futures Bot V20.4.2 RENDER-SAFE PRO CONTINUATION"
DEPLOY_MARKER = "V20_4_2_RENDER_SAFE_ZERO_DEPENDENCY_START_2026_09_03"
FEATURE_SCHEMA = "v20_4_continuation_1"
STATE_SCHEMA = 1


def clean_env(value: Optional[str], default: str = "") -> str:
    """Normalize values pasted into Render, including accidental quote marks."""
    if value is None:
        return default
    cleaned = str(value).strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        cleaned = cleaned[1:-1].strip()
    return cleaned


def first_env(*names: str, default: str = "") -> str:
    for name in names:
        value = clean_env(os.getenv(name))
        if value:
            return value
    return default


def normalize_telegram_chat(raw: str) -> str:
    chat = clean_env(raw).replace(" ", "")
    match = re.match(r"^(?:https?://)?t\.me/([A-Za-z0-9_]{5,})/?$", chat)
    if match:
        return f"@{match.group(1)}"
    return chat


def env_int(name: str, default: int, low: Optional[int] = None, high: Optional[int] = None) -> int:
    """Render-safe integer env parsing: bad pasted values fall back instead of crashing import."""
    raw = clean_env(os.getenv(name), str(default))
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        LOG.warning("Invalid integer env %s=%r; using %s", name, raw, default) if "LOG" in globals() else None
        value = int(default)
    if low is not None:
        value = max(low, value)
    if high is not None:
        value = min(high, value)
    return value


def env_float(name: str, default: float, low: Optional[float] = None, high: Optional[float] = None) -> float:
    """Render-safe float env parsing."""
    raw = clean_env(os.getenv(name), str(default))
    try:
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError(raw)
    except (TypeError, ValueError):
        LOG.warning("Invalid float env %s=%r; using %s", name, raw, default) if "LOG" in globals() else None
        value = float(default)
    if low is not None:
        value = max(low, value)
    if high is not None:
        value = min(high, value)
    return value


def env_bool(name: str, default: bool = False) -> bool:
    raw = clean_env(os.getenv(name), "true" if default else "false").lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


BINGX_BASE_URL = os.getenv("BINGX_BASE_URL", "https://open-api.bingx.com").rstrip("/")
TELEGRAM_BOT_TOKEN = first_env(
    "TELEGRAM_BOT_TOKEN", "BOT_TOKEN", "TG_BOT_TOKEN", default=""
)
TELEGRAM_CHAT_ID = normalize_telegram_chat(
    first_env(
        "TELEGRAM_CHAT_ID",
        "CHAT_ID",
        "TG_CHAT_ID",
        "TELEGRAM_CHANNEL_ID",
        default="",
    )
)
TELEGRAM_SEND_ATTEMPTS = env_int("TELEGRAM_SEND_ATTEMPTS", 3, 1, 8)
TELEGRAM_STARTUP_ATTEMPTS = env_int("TELEGRAM_STARTUP_ATTEMPTS", 5, 1, 10)
TELEGRAM_RETRY_BASE_SECONDS = env_float("TELEGRAM_RETRY_BASE_SECONDS", 0.8, 0.2, 30.0)

STATE_FILE = Path(clean_env(os.getenv("STATE_FILE"), "bot_state_v20_4.json"))

SCAN_SECONDS = env_int("SCAN_SECONDS", 30, 15)
DIAGNOSTIC_SECONDS = env_int("DIAGNOSTIC_SECONDS", 14400, 1800)
UNIVERSE_SIZE = env_int("UNIVERSE_SIZE", 80, 20, 160)
DEEP_CANDIDATE_LIMIT = env_int("DEEP_CANDIDATE_LIMIT", 14, 4, 30)
MAX_ACTIVE_SIGNALS = env_int("MAX_ACTIVE_SIGNALS", 2, 1, 4)
HTTP_CONCURRENCY = env_int("HTTP_CONCURRENCY", 8, 2, 20)
HTTP_TIMEOUT_SECONDS = env_int("HTTP_TIMEOUT_SECONDS", 12, 5, 60)
WATCH_MAX_MINUTES = env_int("WATCH_MAX_MINUTES", 90, 30)

MIN_QUOTE_TURNOVER_USDT = env_float("MIN_QUOTE_TURNOVER_USDT", 3000000.0, 0.0)
MAX_BOOK_SPREAD_BPS = env_float("MAX_BOOK_SPREAD_BPS", 8.0, 0.1)
MIN_BOOK_DEPTH_USDT = env_float("MIN_BOOK_DEPTH_USDT", 20000.0, 0.0)
MAX_EXPECTED_SLIPPAGE = env_float("MAX_EXPECTED_SLIPPAGE", 0.0008, 0.0)

MIN_SIGNAL_SCORE = env_int("MIN_SIGNAL_SCORE", 82, 0, 100)
CONT_SCORE_MIN = env_int("CONT_SCORE_MIN", 7, 0, 10)
FADE_SCORE_MAX = env_int("FADE_SCORE_MAX", 2, 0, 10)
TF_AGREEMENT_MIN = env_int("TF_AGREEMENT_MIN", 2, 1, 4)
PULLBACK_FRACTION_MIN = env_float("PULLBACK_FRACTION_MIN", 0.18, 0.0, 1.0)
PULLBACK_FRACTION_MAX = env_float("PULLBACK_FRACTION_MAX", 0.45, 0.0, 1.0)
PULLBACK_INVALIDATION = env_float("PULLBACK_INVALIDATION", 0.60, 0.0, 2.0)
ANTI_CHASE_MAX = env_float("ANTI_CHASE_MAX", 0.004, 0.0, 0.10)

TP_MOVES = (0.0080, 0.0140, 0.0200, 0.0300, 0.0440)
TP_SHARES = (0.15, 0.20, 0.30, 0.20, 0.15)
STOP_MOVE_MIN = env_float("STOP_MOVE_MIN", 0.0065, 0.001)
STOP_MOVE_MAX = env_float("STOP_MOVE_MAX", 0.0115, STOP_MOVE_MIN)
MIN_RR_TO_TP3 = env_float("MIN_RR_TO_TP3", 1.70, 0.1)
ESTIMATED_TAKER_FEE = env_float("ESTIMATED_TAKER_FEE", 0.0005, 0.0)
ESTIMATED_MAKER_FEE = env_float("ESTIMATED_MAKER_FEE", 0.0002, 0.0)
ACCOUNT_RISK_FRACTION = env_float("ACCOUNT_RISK_FRACTION", 0.0025, 0.0, 0.02)
MAX_LEVERAGE = env_int("MAX_LEVERAGE", 10, 1, 10)

NO_PROGRESS_MINUTES = env_int("NO_PROGRESS_MINUTES", 6, 1)
NO_PROGRESS_MFE_R = env_float("NO_PROGRESS_MFE_R", 0.25, 0.0)
PRE_TP1_STALE_MINUTES = env_int("PRE_TP1_STALE_MINUTES", 12, 1)
HARD_EXPIRE_MINUTES = env_int("HARD_EXPIRE_MINUTES", 45, PRE_TP1_STALE_MINUTES)
SYMBOL_QUARANTINE_HOURS = env_int("SYMBOL_QUARANTINE_HOURS", 24, 1)
SEND_WATCH_ALERTS = env_bool("SEND_WATCH_ALERTS", False)
FORCE_STDLIB_HTTP = env_bool("FORCE_STDLIB_HTTP", False)

PAPER_MODE = True  # hard-coded; no real-order implementation in this version.


EXCLUDED_BASES = {
    item.strip().upper()
    for item in os.getenv("EXCLUDED_BASES", "USDC,FDUSD,TUSD,USDE,DAI").split(",")
    if item.strip()
}

INTERVAL_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOG = logging.getLogger("v20_4")

TELEGRAM_STATUS: dict[str, Any] = {
    "token_present": bool(TELEGRAM_BOT_TOKEN),
    "chat_id_present": bool(TELEGRAM_CHAT_ID),
    "chat_id_kind": (
        "channel_username"
        if TELEGRAM_CHAT_ID.startswith("@")
        else "numeric"
        if TELEGRAM_CHAT_ID.lstrip("-").isdigit()
        else "invalid_or_unknown"
        if TELEGRAM_CHAT_ID
        else "missing"
    ),
    "get_me_ok": None,
    "get_chat_ok": None,
    "last_send_ok": None,
    "last_http_status": None,
    "last_error": None,
    "last_checked_at": None,
    "consecutive_failures": 0,
}


def now_ts() -> int:
    return int(time.time())


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_text(ts: Optional[int] = None) -> str:
    return datetime.fromtimestamp(ts or now_ts(), timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except (TypeError, ValueError):
        return default


def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    return numerator / denominator if abs(denominator) > 1e-15 else default


def median(values: Iterable[float], default: float = 0.0) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    return statistics.median(clean) if clean else default


def percentile(values: Sequence[float], quantile: float) -> float:
    clean = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not clean:
        return 0.0
    position = clamp(quantile, 0.0, 1.0) * (len(clean) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return clean[lower]
    weight = position - lower
    return clean[lower] * (1.0 - weight) + clean[upper] * weight


def percentile_rank(history: Sequence[float], value: float) -> float:
    clean = [float(item) for item in history if math.isfinite(float(item))]
    if not clean:
        return 1.0
    return sum(item <= value for item in clean) / len(clean)


@dataclass(frozen=True)
class Candle:
    time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def range(self) -> float:
        return max(0.0, self.high - self.low)

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def body_fraction(self) -> float:
        return safe_div(self.body, self.range)

    @property
    def close_location(self) -> float:
        return safe_div(self.close - self.low, self.range, 0.5)

    @property
    def upper_wick_fraction(self) -> float:
        return safe_div(self.high - max(self.open, self.close), self.range)

    @property
    def lower_wick_fraction(self) -> float:
        return safe_div(min(self.open, self.close) - self.low, self.range)

    def true_range(self, previous_close: Optional[float] = None) -> float:
        if previous_close is None:
            return self.range
        return max(
            self.range, abs(self.high - previous_close), abs(self.low - previous_close)
        )


@dataclass(frozen=True)
class Ticker:
    symbol: str
    last_price: float
    quote_turnover: float
    change_24h: float


@dataclass(frozen=True)
class BookQuality:
    spread_bps: float
    bid_depth_usdt: float
    ask_depth_usdt: float
    imbalance: float
    expected_slippage: float

    @property
    def min_depth_usdt(self) -> float:
        return min(self.bid_depth_usdt, self.ask_depth_usdt)

    @property
    def passed(self) -> bool:
        return (
            self.spread_bps <= MAX_BOOK_SPREAD_BPS
            and self.min_depth_usdt >= MIN_BOOK_DEPTH_USDT
            and self.expected_slippage <= MAX_EXPECTED_SLIPPAGE
        )


@dataclass(frozen=True)
class ScreenCandidate:
    symbol: str
    direction: int
    candle_time_ms: int
    base_price: float
    extreme_price: float
    impulse_move: float
    atr_mult: float
    volume_pace: float
    range_acceleration: float
    compression_release: bool
    compression_rank: float
    preliminary_score: int


@dataclass(frozen=True)
class MarketContext:
    symbol: str
    direction: int
    tf_agreement: int
    trend_aligned: bool
    adx_15m: float
    rsi_15m: float
    ema_distance_atr: float
    divergence: bool
    wick_rejection: bool
    volume_climax: bool
    structure_broken: bool
    failed_retest: bool
    overextended: bool
    heat_state: str
    fade_score: int
    seed_continuation_score: int
    btc_regime: str


@dataclass
class WatchState:
    event_id: str
    symbol: str
    direction: int
    created_at: int
    candle_time_ms: int
    base_price: float
    extreme_price: float
    impulse_move: float
    atr_mult: float
    volume_pace: float
    range_acceleration: float
    compression_release: bool
    compression_rank: float
    preliminary_score: int
    tf_agreement: int
    trend_aligned: bool
    adx_15m: float
    rsi_15m: float
    initial_ema_distance_atr: float
    fade_score: int
    btc_regime: str
    spread_bps: float
    depth_usdt: float
    book_imbalance: float
    heat_state: str = "UNKNOWN"
    max_retrace_fraction: float = 0.0
    pullback_seen: bool = False
    stage: str = "WAIT_PULLBACK"
    last_updated_at: int = 0
    rejection_reason: str = ""


@dataclass
class TradeSignal:
    signal_id: str
    event_id: str
    feature_schema: str
    symbol: str
    side: str
    strategy: str
    created_at: int
    entry_price: float
    entry_zone_low: float
    entry_zone_high: float
    stop_price: float
    targets: list[float]
    stop_move: float
    quality_score: int
    grade: str
    tf_agreement: int
    continuation_score: int
    fade_score: int
    impulse_move: float
    pullback_fraction: float
    reclaim_volume_ratio: float
    btc_regime: str
    spread_bps: float
    depth_usdt: float
    status: str = "ACTIVE"
    tp_hit: int = 0
    mfe_r: float = 0.0
    mae_r: float = 0.0
    max_price: float = 0.0
    min_price: float = 0.0
    last_monitor_ms: int = 0
    heat_state: str = "UNKNOWN"


def ema_series(values: Sequence[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    output = [float(values[0])]
    for value in values[1:]:
        output.append(alpha * float(value) + (1.0 - alpha) * output[-1])
    return output


def ema_value(values: Sequence[float], period: int) -> float:
    series = ema_series(values, period)
    return series[-1] if series else 0.0


def atr_value(
    candles: Sequence[Candle], period: int = 14, exclude_last: bool = False
) -> float:
    selected = list(candles[:-1] if exclude_last else candles)
    if len(selected) < 2:
        return 0.0
    true_ranges = [
        selected[index].true_range(selected[index - 1].close)
        for index in range(1, len(selected))
    ]
    return statistics.fmean(true_ranges[-period:]) if true_ranges else 0.0


def rsi_series(values: Sequence[float], period: int = 14) -> list[Optional[float]]:
    if len(values) <= period:
        return [None] * len(values)
    changes = [
        float(values[index]) - float(values[index - 1])
        for index in range(1, len(values))
    ]
    gains = [max(change, 0.0) for change in changes]
    losses = [max(-change, 0.0) for change in changes]
    avg_gain = statistics.fmean(gains[:period])
    avg_loss = statistics.fmean(losses[:period])
    output: list[Optional[float]] = [None] * period

    def calculate(gain: float, loss: float) -> float:
        if loss <= 1e-15:
            return 100.0 if gain > 0 else 50.0
        relative_strength = gain / loss
        return 100.0 - 100.0 / (1.0 + relative_strength)

    output.append(calculate(avg_gain, avg_loss))
    for index in range(period, len(changes)):
        avg_gain = (avg_gain * (period - 1) + gains[index]) / period
        avg_loss = (avg_loss * (period - 1) + losses[index]) / period
        output.append(calculate(avg_gain, avg_loss))
    return output


def rsi_value(values: Sequence[float], period: int = 14) -> float:
    series = rsi_series(values, period)
    for value in reversed(series):
        if value is not None:
            return value
    return 50.0


def adx_value(candles: Sequence[Candle], period: int = 14) -> float:
    if len(candles) < period * 2 + 2:
        return 0.0
    true_ranges: list[float] = []
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    for previous, current in zip(candles, candles[1:]):
        up_move = current.high - previous.high
        down_move = previous.low - current.low
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
        true_ranges.append(current.true_range(previous.close))
    dx_values: list[float] = []
    for end in range(period, len(true_ranges) + 1):
        start = end - period
        tr_sum = sum(true_ranges[start:end])
        plus_di = 100.0 * safe_div(sum(plus_dm[start:end]), tr_sum)
        minus_di = 100.0 * safe_div(sum(minus_dm[start:end]), tr_sum)
        dx_values.append(100.0 * safe_div(abs(plus_di - minus_di), plus_di + minus_di))
    return statistics.fmean(dx_values[-period:]) if len(dx_values) >= period else 0.0


def rolling_vwap(candles: Sequence[Candle], period: int = 20) -> float:
    selected = list(candles[-period:])
    total_volume = sum(candle.volume for candle in selected)
    if total_volume <= 0:
        return selected[-1].close if selected else 0.0
    value = sum(
        ((candle.high + candle.low + candle.close) / 3.0) * candle.volume
        for candle in selected
    )
    return value / total_volume


def bollinger_widths(values: Sequence[float], period: int = 20) -> list[float]:
    output: list[float] = []
    for end in range(period, len(values) + 1):
        window = [float(value) for value in values[end - period : end]]
        mean = statistics.fmean(window)
        deviation = statistics.pstdev(window)
        output.append(safe_div(4.0 * deviation, mean))
    return output


def signed_return(candles: Sequence[Candle], bars: int) -> float:
    if len(candles) <= bars:
        return 0.0
    return safe_div(candles[-1].close, candles[-bars - 1].close, 1.0) - 1.0


def direction_sign(value: float, neutral: float = 0.0) -> int:
    if value > neutral:
        return 1
    if value < -neutral:
        return -1
    return 0


def has_rsi_divergence(
    candles: Sequence[Candle], direction: int, lookback: int = 24
) -> bool:
    if len(candles) < lookback + 15:
        return False
    selected = list(candles[-lookback:])
    rsi_values = rsi_series([candle.close for candle in candles], 14)[-lookback:]
    midpoint = lookback // 2
    first_candles, second_candles = selected[:midpoint], selected[midpoint:]
    first_rsi = [value for value in rsi_values[:midpoint] if value is not None]
    second_rsi = [value for value in rsi_values[midpoint:] if value is not None]
    if not first_rsi or not second_rsi:
        return False
    if direction > 0:
        higher_high = (
            max(c.high for c in second_candles)
            > max(c.high for c in first_candles) * 1.002
        )
        weaker_rsi = max(second_rsi) < max(first_rsi) - 3.0
        return higher_high and weaker_rsi
    lower_low = (
        min(c.low for c in second_candles) < min(c.low for c in first_candles) * 0.998
    )
    stronger_rsi = min(second_rsi) > min(first_rsi) + 3.0
    return lower_low and stronger_rsi


def structure_break_against_impulse(candles: Sequence[Candle], direction: int) -> bool:
    if len(candles) < 8:
        return False
    last = candles[-1]
    reference = candles[-6:-2]
    ema9 = ema_series([candle.close for candle in candles[-20:]], 9)
    ema_falling = len(ema9) >= 3 and ema9[-1] < ema9[-2] < ema9[-3]
    ema_rising = len(ema9) >= 3 and ema9[-1] > ema9[-2] > ema9[-3]
    if direction > 0:
        return last.close < min(candle.low for candle in reference) and ema_falling
    return last.close > max(candle.high for candle in reference) and ema_rising


def failed_retest_after_break(candles: Sequence[Candle], direction: int) -> bool:
    if len(candles) < 7:
        return False
    older = candles[-7:-3]
    retest, confirmation = candles[-2], candles[-1]
    if direction > 0:
        broken_level = min(candle.low for candle in older)
        return (
            retest.high >= broken_level
            and confirmation.close < broken_level
            and confirmation.close < confirmation.open
        )
    broken_level = max(candle.high for candle in older)
    return (
        retest.low <= broken_level
        and confirmation.close > broken_level
        and confirmation.close > confirmation.open
    )


def detect_compression_release(candles: Sequence[Candle]) -> tuple[bool, int, float]:
    if len(candles) < 70:
        return False, 0, 1.0
    closes = [candle.close for candle in candles]
    widths = bollinger_widths(closes, 20)
    if len(widths) < 45:
        return False, 0, 1.0
    pre_release_width = widths[-2]
    compression_rank = percentile_rank(widths[-62:-2], pre_release_width)
    last = candles[-1]
    previous = candles[-22:-1]
    previous_high = max(candle.high for candle in previous)
    previous_low = min(candle.low for candle in previous)
    previous_atr = atr_value(candles[:-1], 14)
    tr_multiple = safe_div(last.true_range(candles[-2].close), previous_atr)
    volume_multiple = safe_div(last.volume, median(c.volume for c in candles[-22:-2]))
    long_break = (
        last.close > previous_high
        and last.close > last.open
        and last.close_location >= 0.72
    )
    short_break = (
        last.close < previous_low
        and last.close < last.open
        and last.close_location <= 0.28
    )
    direction = 1 if long_break else -1 if short_break else 0
    released = (
        compression_rank <= 0.25
        and direction != 0
        and last.body_fraction >= 0.58
        and tr_multiple >= 1.30
        and volume_multiple >= 1.60
    )
    return released, direction, compression_rank


def screen_impulse(
    symbol: str, candles_15m: Sequence[Candle]
) -> Optional[ScreenCandidate]:
    if len(candles_15m) < 70:
        return None
    last = candles_15m[-1]
    move_1 = safe_div(last.close, last.open, 1.0) - 1.0
    move_3 = signed_return(candles_15m, 3)
    signed_move = move_3 if abs(move_3) >= abs(move_1) else move_1
    direction = direction_sign(signed_move)
    if direction == 0:
        return None

    previous_volumes = [candle.volume for candle in candles_15m[-24:-3]]
    previous_ranges = [candle.range for candle in candles_15m[-24:-3]]
    volume_pace = safe_div(
        statistics.fmean(c.volume for c in candles_15m[-2:]), median(previous_volumes)
    )
    range_acceleration = safe_div(
        statistics.fmean(c.range for c in candles_15m[-2:]), median(previous_ranges)
    )
    previous_atr = atr_value(candles_15m[:-1], 14)
    atr_mult = safe_div(last.true_range(candles_15m[-2].close), previous_atr)
    compression_release, release_direction, compression_rank = (
        detect_compression_release(candles_15m)
    )
    if compression_release:
        direction = release_direction

    regular_impulse = (
        abs(signed_move) >= 0.025
        and volume_pace >= 1.40
        and range_acceleration >= 1.25
        and atr_mult >= 1.20
    )
    strong_multibar_impulse = (
        abs(move_3) >= 0.040 and volume_pace >= 1.25 and range_acceleration >= 1.20
    )
    if not (compression_release or regular_impulse or strong_multibar_impulse):
        return None

    if abs(signed_move) > 0.30:
        return None
    base = candles_15m[-4].close if abs(move_3) >= abs(move_1) else last.open
    extreme = last.high if direction > 0 else last.low
    score = 45
    score += min(15, int(max(0.0, abs(signed_move) - 0.02) * 300))
    score += min(15, int(max(0.0, volume_pace - 1.0) * 5))
    score += min(10, int(max(0.0, atr_mult - 1.0) * 5))
    score += 10 if compression_release else 0
    score += 5 if range_acceleration >= 1.8 else 0
    return ScreenCandidate(
        symbol=symbol,
        direction=direction,
        candle_time_ms=last.time_ms,
        base_price=base,
        extreme_price=extreme,
        impulse_move=abs(signed_move),
        atr_mult=atr_mult,
        volume_pace=volume_pace,
        range_acceleration=range_acceleration,
        compression_release=compression_release,
        compression_rank=compression_rank,
        preliminary_score=min(100, score),
    )


def timeframe_agreement(direction: int, frames: dict[str, Sequence[Candle]]) -> int:
    definitions = {
        "5m": (3, 0.0020),
        "15m": (3, 0.0040),
        "1h": (2, 0.0030),
        "4h": (1, 0.0040),
    }
    agreement = 0
    for interval, (bars, threshold) in definitions.items():
        candles = frames.get(interval, [])
        if len(candles) <= bars + 50:
            continue
        move = signed_return(candles, bars)
        closes = [candle.close for candle in candles]
        ema20, ema50 = ema_value(closes, 20), ema_value(closes, 50)
        trend_direction = direction_sign(ema20 - ema50)
        if move * direction >= threshold or (
            move * direction > 0 and trend_direction == direction
        ):
            agreement += 1
    return agreement


def classify_heat_state(
    *,
    overextended: bool,
    trend_aligned: bool,
    structure_broken: bool,
    failed_retest: bool,
    fade_score: int,
) -> str:
    """Separate trend heat from a confirmed exhaustion reversal.

    An oscillator extreme or large EMA distance can make a trend hot, but only a
    structure break plus a failed retest upgrades it to confirmed exhaustion.
    """
    if structure_broken and failed_retest and fade_score >= 6:
        return "EXHAUSTION_CONFIRMED"
    if overextended and trend_aligned and not structure_broken:
        return "HOT_TREND"
    if overextended:
        return "OVEREXTENDED_UNCONFIRMED"
    return "NORMAL"


def classify_market_context(
    candidate: ScreenCandidate,
    frames: dict[str, Sequence[Candle]],
    btc_regime: str,
) -> MarketContext:
    candles_15m = list(frames["15m"])
    closes_15m = [candle.close for candle in candles_15m]
    atr15 = atr_value(candles_15m, 14)
    ema20_15 = ema_value(closes_15m, 20)
    rsi15 = rsi_value(closes_15m, 14)
    adx15 = adx_value(candles_15m, 14)
    signed_distance = safe_div(candles_15m[-1].close - ema20_15, atr15)
    distance_in_direction = signed_distance * candidate.direction

    candles_1h = list(frames.get("1h", []))
    trend_15m = direction_sign(ema_value(closes_15m, 20) - ema_value(closes_15m, 50))
    trend_1h = 0
    if len(candles_1h) >= 50:
        closes_1h = [candle.close for candle in candles_1h]
        trend_1h = direction_sign(ema_value(closes_1h, 20) - ema_value(closes_1h, 50))
    trend_aligned = trend_15m == candidate.direction and trend_1h in {
        0,
        candidate.direction,
    }
    agreement = timeframe_agreement(candidate.direction, frames)

    last = candles_15m[-1]
    wick_rejection = (
        last.upper_wick_fraction >= 0.35 and last.close_location < 0.65
        if candidate.direction > 0
        else last.lower_wick_fraction >= 0.35 and last.close_location > 0.35
    )
    divergence = has_rsi_divergence(candles_15m, candidate.direction)
    volume_climax = candidate.volume_pace >= 4.0
    structure_broken = structure_break_against_impulse(candles_15m, candidate.direction)
    failed_retest = failed_retest_after_break(candles_15m, candidate.direction)
    rsi_extreme = rsi15 >= 80.0 if candidate.direction > 0 else rsi15 <= 20.0
    overextended = (
        distance_in_direction >= 2.50 or rsi_extreme or candidate.impulse_move >= 0.15
    )

    # RSI extreme alone contributes only one point: it is a condition, not a reversal signal.
    fade_score = 0
    fade_score += 1 if rsi_extreme else 0
    fade_score += 2 if wick_rejection else 0
    fade_score += 1 if volume_climax else 0
    fade_score += 1 if divergence else 0
    fade_score += 2 if structure_broken else 0
    fade_score += 2 if failed_retest else 0
    fade_score += 1 if distance_in_direction >= 3.5 else 0

    # Market heat is a state machine, not a single oscillator threshold.
    # HOT_TREND means “do not chase, wait for a pullback”; it does not mean short/long reversal.
    heat_state = classify_heat_state(
        overextended=overextended,
        trend_aligned=trend_aligned,
        structure_broken=structure_broken,
        failed_retest=failed_retest,
        fade_score=fade_score,
    )

    continuation_score = 0
    continuation_score += 2 if agreement >= TF_AGREEMENT_MIN else 0
    continuation_score += 1 if trend_aligned else 0
    continuation_score += 1 if adx15 >= 18.0 else 0
    continuation_score += 1 if candidate.volume_pace >= 1.5 else 0
    continuation_score += 1 if candidate.atr_mult >= 1.5 else 0
    continuation_score += 1 if not structure_broken else 0
    continuation_score += 1 if not failed_retest else 0

    return MarketContext(
        symbol=candidate.symbol,
        direction=candidate.direction,
        tf_agreement=agreement,
        trend_aligned=trend_aligned,
        adx_15m=adx15,
        rsi_15m=rsi15,
        ema_distance_atr=distance_in_direction,
        divergence=divergence,
        wick_rejection=wick_rejection,
        volume_climax=volume_climax,
        structure_broken=structure_broken,
        failed_retest=failed_retest,
        overextended=overextended,
        heat_state=heat_state,
        fade_score=fade_score,
        seed_continuation_score=continuation_score,
        btc_regime=btc_regime,
    )


def parse_kline_item(item: Any) -> Optional[Candle]:
    try:
        if isinstance(item, dict):
            timestamp = item.get("time", item.get("timestamp", item.get("openTime", 0)))
            candle = Candle(
                time_ms=int(timestamp),
                open=safe_float(item.get("open")),
                high=safe_float(item.get("high")),
                low=safe_float(item.get("low")),
                close=safe_float(item.get("close")),
                volume=safe_float(item.get("volume", item.get("vol", 0.0))),
            )
        elif isinstance(item, (list, tuple)) and len(item) >= 6:
            values = list(item)
            if safe_float(values[0]) > 10_000_000_000:
                candle = Candle(
                    time_ms=int(safe_float(values[0])),
                    open=safe_float(values[1]),
                    high=safe_float(values[2]),
                    low=safe_float(values[3]),
                    close=safe_float(values[4]),
                    volume=safe_float(values[5]),
                )
            elif safe_float(values[-1]) > 10_000_000_000:
                # Legacy BingX layout: open, close, high, low, volume, time.
                candle = Candle(
                    time_ms=int(safe_float(values[-1])),
                    open=safe_float(values[0]),
                    high=safe_float(values[2]),
                    low=safe_float(values[3]),
                    close=safe_float(values[1]),
                    volume=safe_float(values[4]),
                )
            else:
                return None
        else:
            return None
        if (
            candle.time_ms <= 0
            or candle.open <= 0
            or candle.high <= 0
            or candle.low <= 0
            or candle.close <= 0
            or candle.high < max(candle.open, candle.close)
            or candle.low > min(candle.open, candle.close)
        ):
            return None
        return candle
    except (TypeError, ValueError, IndexError):
        return None


def parse_closed_klines(
    payload: Any, interval: str, current_ms: Optional[int] = None
) -> list[Candle]:
    current_ms = current_ms or now_ms()
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if isinstance(data, dict):
        data = data.get("list", data.get("klines", data.get("data", [])))
    if not isinstance(data, list):
        return []
    parsed = [candle for item in data if (candle := parse_kline_item(item)) is not None]
    unique = {candle.time_ms: candle for candle in parsed}
    ordered = sorted(unique.values(), key=lambda candle: candle.time_ms)
    interval_ms = INTERVAL_MS[interval]
    # Never use the still-forming candle for a setup decision.
    closed = [
        candle
        for candle in ordered
        if candle.time_ms + interval_ms <= current_ms - 1_500
    ]
    return closed


def parse_tickers(payload: Any) -> list[Ticker]:
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if isinstance(data, dict):
        data = data.get("tickers", data.get("list", [data]))
    if not isinstance(data, list):
        return []
    output: list[Ticker] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).upper()
        if not symbol.endswith("-USDT"):
            continue
        last = safe_float(
            item.get("lastPrice", item.get("price", item.get("last", 0.0)))
        )
        quote_volume = safe_float(
            item.get(
                "quoteVolume",
                item.get(
                    "quoteVolume24h", item.get("turnover", item.get("amount", 0.0))
                ),
            )
        )
        if quote_volume <= 0:
            quote_volume = (
                safe_float(item.get("volume", item.get("baseVolume", 0.0))) * last
            )
        open_price = safe_float(item.get("openPrice", item.get("open", 0.0)))
        if open_price > 0:
            change = last / open_price - 1.0
        else:
            change = safe_float(
                item.get(
                    "priceChangePercent",
                    item.get("changePercent", item.get("change", 0.0)),
                )
            )
            if abs(change) > 2.0:
                change /= 100.0
        if last > 0:
            output.append(Ticker(symbol, last, quote_volume, change))
    return output


def parse_book(payload: Any) -> BookQuality:
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not isinstance(data, dict):
        return BookQuality(9999.0, 0.0, 0.0, 0.0, 1.0)
    bids = data.get("bids", [])
    asks = data.get("asks", [])

    def levels(raw: Any, reverse: bool) -> list[tuple[float, float]]:
        output: list[tuple[float, float]] = []
        if not isinstance(raw, list):
            return output
        for item in raw:
            if isinstance(item, dict):
                price = safe_float(item.get("price"))
                quantity = safe_float(
                    item.get("quantity", item.get("qty", item.get("volume", 0.0)))
                )
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                price, quantity = safe_float(item[0]), safe_float(item[1])
            else:
                continue
            if price > 0 and quantity > 0:
                output.append((price, quantity))
        return sorted(output, key=lambda level: level[0], reverse=reverse)

    bid_levels = levels(bids, True)
    ask_levels = levels(asks, False)
    if not bid_levels or not ask_levels:
        return BookQuality(9999.0, 0.0, 0.0, 0.0, 1.0)
    best_bid, best_ask = bid_levels[0][0], ask_levels[0][0]
    midpoint = (best_bid + best_ask) / 2.0
    spread_bps = safe_div(best_ask - best_bid, midpoint) * 10_000.0
    bid_depth = sum(price * quantity for price, quantity in bid_levels[:10])
    ask_depth = sum(price * quantity for price, quantity in ask_levels[:10])
    imbalance = safe_div(bid_depth - ask_depth, bid_depth + ask_depth)

    # Estimate the relative move needed to consume MIN_BOOK_DEPTH_USDT / 20.
    test_notional = max(100.0, MIN_BOOK_DEPTH_USDT / 20.0)

    def slippage(level_data: Sequence[tuple[float, float]], reference: float) -> float:
        remaining = test_notional
        quantity_sum = 0.0
        cost_sum = 0.0
        for price, quantity in level_data:
            available_notional = price * quantity
            used_notional = min(remaining, available_notional)
            used_quantity = safe_div(used_notional, price)
            quantity_sum += used_quantity
            cost_sum += used_quantity * price
            remaining -= used_notional
            if remaining <= 1e-9:
                break
        if remaining > 1e-9 or quantity_sum <= 0:
            return 1.0
        average_price = cost_sum / quantity_sum
        return abs(average_price - reference) / reference

    expected_slippage = max(
        slippage(ask_levels, best_ask), slippage(bid_levels, best_bid)
    )
    return BookQuality(spread_bps, bid_depth, ask_depth, imbalance, expected_slippage)


class _StdlibResponse:
    def __init__(self, status: int, raw: bytes, headers: Any = None) -> None:
        self.status = int(status)
        self._raw = raw
        self.headers = headers or {}

    async def json(self, content_type: Any = None) -> Any:
        del content_type
        return json.loads(self._raw.decode("utf-8", errors="replace"))

    async def text(self) -> str:
        return self._raw.decode("utf-8", errors="replace")


class _StdlibRequestContext:
    """aiohttp-like async context manager backed by urllib in a worker thread."""

    def __init__(
        self,
        method: str,
        url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json_payload: Optional[dict[str, Any]] = None,
        timeout: float = 12.0,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.method = method.upper()
        self.url = url
        self.params = params or {}
        self.json_payload = json_payload
        self.timeout = timeout
        self.headers = headers or {}
        self.response: Optional[_StdlibResponse] = None

    def _perform(self) -> _StdlibResponse:
        url = self.url
        if self.params:
            query = urllib.parse.urlencode(
                {key: str(value) for key, value in self.params.items()}
            )
            url = f"{url}{'&' if '?' in url else '?'}{query}"

        body: Optional[bytes] = None
        headers = dict(self.headers)
        if self.json_payload is not None:
            body = json.dumps(self.json_payload).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")

        request = urllib.request.Request(
            url=url,
            data=body,
            headers=headers,
            method=self.method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return _StdlibResponse(
                    int(getattr(response, "status", 200)),
                    response.read(),
                    getattr(response, "headers", {}),
                )
        except urllib.error.HTTPError as error:
            return _StdlibResponse(
                int(error.code),
                error.read(),
                getattr(error, "headers", {}),
            )

    async def __aenter__(self) -> _StdlibResponse:
        self.response = await asyncio.to_thread(self._perform)
        return self.response

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False


class StdlibHTTPSession:
    """Minimal aiohttp-compatible session used when aiohttp is not installed.

    This makes `python bot.py` work on a clean Render Python runtime without
    installing any HTTP package. Network calls run in worker threads so the
    trading event loop stays responsive.
    """

    def __init__(self, timeout: float, headers: Optional[dict[str, str]] = None) -> None:
        self.timeout = float(timeout)
        self.headers = dict(headers or {})
        self.closed = False

    def get(
        self,
        url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> _StdlibRequestContext:
        return _StdlibRequestContext(
            "GET",
            url,
            params=params,
            timeout=self.timeout,
            headers=self.headers,
        )

    def post(
        self,
        url: str,
        *,
        json: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> _StdlibRequestContext:
        return _StdlibRequestContext(
            "POST",
            url,
            json_payload=json,
            timeout=self.timeout,
            headers=self.headers,
        )

    async def close(self) -> None:
        self.closed = True


class BingXPublicClient:
    def __init__(self) -> None:
        headers = {"User-Agent": f"{APP_NAME}/1.0"}
        if aiohttp is not None and not FORCE_STDLIB_HTTP:
            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
            self.session = aiohttp.ClientSession(timeout=timeout, headers=headers)
            self.http_backend = "aiohttp"
        else:
            self.session = StdlibHTTPSession(HTTP_TIMEOUT_SECONDS, headers=headers)
            self.http_backend = "stdlib"
            LOG.warning(
                "aiohttp is unavailable/disabled; using zero-dependency stdlib HTTP backend"
            )
        self.semaphore = asyncio.Semaphore(HTTP_CONCURRENCY)

    async def close(self) -> None:
        await self.session.close()

    async def _get(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(3):
            try:
                async with self.semaphore:
                    async with self.session.get(
                        f"{BINGX_BASE_URL}{path}", params=params
                    ) as response:
                        payload = await response.json(content_type=None)
                        if response.status >= 400:
                            raise RuntimeError(
                                f"HTTP {response.status}: {str(payload)[:240]}"
                            )
                        if isinstance(payload, dict) and payload.get("code") not in {
                            None,
                            0,
                            "0",
                        }:
                            raise RuntimeError(
                                f"BingX {payload.get('code')}: {payload.get('msg')}"
                            )
                        return payload
            except Exception as error:
                # Includes aiohttp errors, urllib errors, timeout and malformed payloads.
                last_error = error
                await asyncio.sleep(0.4 * (2**attempt))
        raise RuntimeError(f"BingX request failed {path}: {last_error}")

    async def tickers(self) -> list[Ticker]:
        return parse_tickers(await self._get("/openApi/swap/v2/quote/ticker"))

    async def klines(
        self, symbol: str, interval: str, limit: int = 120
    ) -> list[Candle]:
        payload = await self._get(
            "/openApi/swap/v3/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": str(limit)},
        )
        return parse_closed_klines(payload, interval)

    async def depth(self, symbol: str, limit: int = 20) -> BookQuality:
        payload = await self._get(
            "/openApi/swap/v2/quote/depth",
            {"symbol": symbol, "limit": str(limit)},
        )
        return parse_book(payload)


class TelegramClient:
    def __init__(self, session: Any) -> None:
        self.session = session

    @property
    def enabled(self) -> bool:
        return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

    @staticmethod
    def public_status() -> dict[str, Any]:
        """Health-safe status: confirms configuration without exposing credentials."""
        return dict(TELEGRAM_STATUS)

    @staticmethod
    def _set_failure(description: str, http_status: Optional[int] = None) -> None:
        TELEGRAM_STATUS.update(
            {
                "last_send_ok": False,
                "last_http_status": http_status,
                "last_error": description[:500],
                "last_checked_at": utc_text(),
                "consecutive_failures": int(
                    TELEGRAM_STATUS.get("consecutive_failures", 0)
                )
                + 1,
            }
        )

    async def _request(
        self,
        method: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> tuple[bool, dict[str, Any], int]:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
        try:
            request = (
                self.session.get(url)
                if payload is None
                else self.session.post(url, json=payload)
            )
            async with request as response:
                try:
                    body = await response.json(content_type=None)
                except Exception:
                    raw = await response.text()
                    body = {"ok": False, "description": raw[:500]}
                if not isinstance(body, dict):
                    body = {"ok": False, "description": str(body)[:500]}
                ok = response.status < 300 and bool(body.get("ok", False))
                return ok, body, int(response.status)
        except Exception as error:
            return False, {"ok": False, "description": repr(error)}, 0

    async def diagnose(self) -> bool:
        if not TELEGRAM_BOT_TOKEN:
            self._set_failure("TELEGRAM_BOT_TOKEN is missing")
            LOG.error("Telegram check failed: TELEGRAM_BOT_TOKEN is missing")
            return False
        if not TELEGRAM_CHAT_ID:
            self._set_failure("TELEGRAM_CHAT_ID is missing")
            LOG.error("Telegram check failed: TELEGRAM_CHAT_ID is missing")
            return False
        if "t.me/c/" in TELEGRAM_CHAT_ID:
            self._set_failure(
                "A t.me/c link is not a Bot API chat_id; use the numeric -100... id"
            )
            LOG.error("Telegram check failed: t.me/c link cannot be used as chat_id")
            return False

        get_me_ok, get_me, get_me_status = await self._request("getMe")
        TELEGRAM_STATUS["get_me_ok"] = get_me_ok
        TELEGRAM_STATUS["last_http_status"] = get_me_status
        TELEGRAM_STATUS["last_checked_at"] = utc_text()
        if not get_me_ok:
            description = str(get_me.get("description", get_me))[:500]
            self._set_failure(f"getMe failed: {description}", get_me_status)
            LOG.error("Telegram getMe failed HTTP %s: %s", get_me_status, description)
            return False

        get_chat_ok, get_chat, get_chat_status = await self._request(
            "getChat", {"chat_id": TELEGRAM_CHAT_ID}
        )
        TELEGRAM_STATUS["get_chat_ok"] = get_chat_ok
        TELEGRAM_STATUS["last_http_status"] = get_chat_status
        if not get_chat_ok:
            description = str(get_chat.get("description", get_chat))[:500]
            self._set_failure(f"getChat failed: {description}", get_chat_status)
            LOG.error(
                "Telegram getChat failed HTTP %s: %s", get_chat_status, description
            )
            return False

        bot_name = str((get_me.get("result") or {}).get("username", "unknown"))
        chat_type = str((get_chat.get("result") or {}).get("type", "unknown"))
        TELEGRAM_STATUS.update(
            {
                "get_me_ok": True,
                "get_chat_ok": True,
                "last_error": None,
                "last_checked_at": utc_text(),
                "bot_username": bot_name,
                "chat_type": chat_type,
            }
        )
        LOG.info("Telegram diagnostics OK: bot=@%s chat_type=%s", bot_name, chat_type)
        return True

    @staticmethod
    def _chunks(text: str) -> list[str]:
        remaining = str(text)
        chunks: list[str] = []
        while len(remaining) > 3900:
            cut = remaining.rfind("\n", 0, 3900)
            cut = cut if cut >= 500 else 3900
            chunks.append(remaining[:cut])
            remaining = remaining[cut:].lstrip("\n")
        chunks.append(remaining)
        return chunks

    async def send(
        self,
        text: str,
        *,
        attempts: Optional[int] = None,
        silent: bool = False,
    ) -> bool:
        if not self.enabled:
            missing = []
            if not TELEGRAM_BOT_TOKEN:
                missing.append("TELEGRAM_BOT_TOKEN")
            if not TELEGRAM_CHAT_ID:
                missing.append("TELEGRAM_CHAT_ID")
            description = f"Telegram env missing: {', '.join(missing)}"
            self._set_failure(description)
            LOG.error("%s; unsent message:\n%s", description, text)
            return False

        max_attempts = max(1, attempts or TELEGRAM_SEND_ATTEMPTS)
        for chunk in self._chunks(text):
            delivered = False
            last_description = "unknown Telegram error"
            for attempt in range(1, max_attempts + 1):
                ok, body, status = await self._request(
                    "sendMessage",
                    {
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": chunk,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                        "disable_notification": silent,
                    },
                )
                if ok:
                    TELEGRAM_STATUS.update(
                        {
                            "last_send_ok": True,
                            "last_http_status": status,
                            "last_error": None,
                            "last_checked_at": utc_text(),
                            "consecutive_failures": 0,
                        }
                    )
                    LOG.info("Telegram message delivered on attempt %d", attempt)
                    delivered = True
                    break

                last_description = str(body.get("description", body))[:500]
                self._set_failure(last_description, status)
                LOG.error(
                    "Telegram send failed attempt=%d/%d HTTP=%s: %s",
                    attempt,
                    max_attempts,
                    status,
                    last_description,
                )
                if attempt < max_attempts:
                    retry_after = safe_float(
                        (body.get("parameters") or {}).get("retry_after"), 0.0
                    )
                    delay = retry_after or TELEGRAM_RETRY_BASE_SECONDS * (
                        2 ** (attempt - 1)
                    )
                    await asyncio.sleep(min(30.0, delay))
            if not delivered:
                LOG.error("Telegram message permanently failed: %s", last_description)
                return False
        return True


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def empty(self) -> dict[str, Any]:
        return {
            "schema": STATE_SCHEMA,
            "app": APP_NAME,
            "deploy_marker": DEPLOY_MARKER,
            "feature_schema": FEATURE_SCHEMA,
            "watches": {},
            "signals": {},
            "events": {},
            "outcomes": [],
            "symbol_stats": {},
            "adaptive_buckets": {},
            "counters": {},
            "last_diagnostic_at": 0,
            "last_checkpoint_count": 0,
            "saved_at": now_ts(),
        }

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return self.empty()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("state root must be a JSON object")
            if data.get("schema") != STATE_SCHEMA:
                raise ValueError(f"unsupported state schema {data.get('schema')}")
            base = self.empty()
            base.update(data)
            base["app"] = APP_NAME
            base["deploy_marker"] = DEPLOY_MARKER
            base["feature_schema"] = FEATURE_SCHEMA
            return base
        except (OSError, ValueError, TypeError, AttributeError, json.JSONDecodeError) as error:
            backup = self.path.with_suffix(self.path.suffix + f".invalid-{now_ts()}")
            LOG.error(
                "State is invalid (%s); preserving as %s and starting clean",
                error,
                backup,
            )
            try:
                self.path.replace(backup)
            except OSError:
                pass
            return self.empty()

    def save(self, state: dict[str, Any]) -> None:
        """Atomic save with /tmp fallback so a read-only Render workdir cannot kill the bot."""
        state["saved_at"] = now_ts()
        serialized = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True)

        targets = [self.path]
        fallback = Path("/tmp") / self.path.name
        if fallback != self.path:
            targets.append(fallback)

        last_error: Optional[OSError] = None
        for target in targets:
            temporary_name: Optional[str] = None
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
                )
                with os.fdopen(descriptor, "w", encoding="utf-8") as temporary:
                    temporary.write(serialized)
                    temporary.flush()
                    os.fsync(temporary.fileno())
                os.replace(temporary_name, target)
                self.path = target
                return
            except OSError as error:
                last_error = error
                LOG.error("State save failed at %s: %s", target, error)
            finally:
                if temporary_name and os.path.exists(temporary_name):
                    try:
                        os.unlink(temporary_name)
                    except OSError:
                        pass

        if last_error is not None:
            # State persistence is important, but must never kill the Render process.
            LOG.error("State persistence unavailable; continuing in-memory: %s", last_error)



def watch_from_dict(data: dict[str, Any]) -> WatchState:
    return WatchState(**data)


def signal_from_dict(data: dict[str, Any]) -> TradeSignal:
    return TradeSignal(**data)


def select_universe(tickers: Sequence[Ticker]) -> list[Ticker]:
    eligible = []
    for ticker in tickers:
        base = ticker.symbol.removesuffix("-USDT")
        if base in EXCLUDED_BASES or ticker.quote_turnover < MIN_QUOTE_TURNOVER_USDT:
            continue
        if any(token in base for token in ("3L", "3S", "5L", "5S", "BULL", "BEAR")):
            continue
        eligible.append(ticker)
    liquid_count = max(10, UNIVERSE_SIZE // 2)
    mover_count = UNIVERSE_SIZE - liquid_count
    most_liquid = sorted(
        eligible, key=lambda ticker: ticker.quote_turnover, reverse=True
    )[:liquid_count]
    most_active = sorted(
        eligible,
        key=lambda ticker: (
            abs(ticker.change_24h),
            math.log10(max(ticker.quote_turnover, 1.0)),
        ),
        reverse=True,
    )[: max(mover_count * 2, mover_count)]
    selected: dict[str, Ticker] = {ticker.symbol: ticker for ticker in most_liquid}
    for ticker in most_active:
        if len(selected) >= UNIVERSE_SIZE:
            break
        selected[ticker.symbol] = ticker
    return list(selected.values())


def classify_btc_regime(
    candles_15m: Sequence[Candle], candles_1h: Sequence[Candle]
) -> str:
    if len(candles_15m) < 50 or len(candles_1h) < 50:
        return "UNKNOWN"
    close15 = [candle.close for candle in candles_15m]
    close1h = [candle.close for candle in candles_1h]
    trend15 = direction_sign(ema_value(close15, 20) - ema_value(close15, 50))
    trend1h = direction_sign(ema_value(close1h, 20) - ema_value(close1h, 50))
    move1h = signed_return(candles_1h, 1)
    move6h = signed_return(candles_1h, 6)
    if trend15 > 0 and trend1h > 0 and move6h > 0:
        return "BULL"
    if trend15 < 0 and trend1h < 0 and move6h < 0:
        return "BEAR"
    if abs(move1h) >= 0.02:
        return "SHOCK_UP" if move1h > 0 else "SHOCK_DOWN"
    return "RANGE"


@dataclass(frozen=True)
class ReclaimMetrics:
    confirmed: bool
    trigger_price: float
    current_price: float
    momentum_3m: float
    momentum_5m: float
    volume_ratio: float
    pullback_volume_ratio: float
    anti_chase: float
    lost_reclaim: bool


def calculate_retrace(
    direction: int, base: float, extreme: float, price: float
) -> float:
    impulse_size = abs(extreme - base)
    if impulse_size <= 1e-15:
        return 0.0
    if direction > 0:
        return max(0.0, (extreme - price) / impulse_size)
    return max(0.0, (price - extreme) / impulse_size)


def evaluate_reclaim(candles_1m: Sequence[Candle], direction: int) -> ReclaimMetrics:
    if len(candles_1m) < 30:
        return ReclaimMetrics(False, 0.0, 0.0, 0.0, 0.0, 0.0, 9.0, 1.0, True)
    last = candles_1m[-1]
    previous = candles_1m[-5:-1]
    trigger = (
        max(candle.high for candle in previous)
        if direction > 0
        else min(candle.low for candle in previous)
    )
    ema9 = ema_value([candle.close for candle in candles_1m], 9)
    vwap20 = rolling_vwap(candles_1m, 20)
    momentum_3m = signed_return(candles_1m, 3)
    momentum_5m = signed_return(candles_1m, 5)
    volume_ratio = safe_div(
        last.volume, median(candle.volume for candle in candles_1m[-22:-2])
    )
    pullback_volume = statistics.fmean(candle.volume for candle in candles_1m[-5:-1])
    pullback_volume_ratio = safe_div(
        pullback_volume, median(candle.volume for candle in candles_1m[-26:-6])
    )
    anti_chase = abs(last.close - trigger) / trigger if trigger > 0 else 1.0

    if direction > 0:
        confirmed = (
            last.close > trigger
            and last.close > ema9
            and last.close > vwap20
            and last.close > last.open
            and last.body_fraction >= 0.45
            and last.close_location >= 0.68
            and momentum_3m >= 0.0010
            and momentum_5m > 0
            and volume_ratio >= 1.05
            and anti_chase <= ANTI_CHASE_MAX
        )
        lost_reclaim = last.close < min(ema9, vwap20) and momentum_3m < 0
    else:
        confirmed = (
            last.close < trigger
            and last.close < ema9
            and last.close < vwap20
            and last.close < last.open
            and last.body_fraction >= 0.45
            and last.close_location <= 0.32
            and momentum_3m <= -0.0010
            and momentum_5m < 0
            and volume_ratio >= 1.05
            and anti_chase <= ANTI_CHASE_MAX
        )
        lost_reclaim = last.close > max(ema9, vwap20) and momentum_3m > 0
    return ReclaimMetrics(
        confirmed=confirmed,
        trigger_price=trigger,
        current_price=last.close,
        momentum_3m=momentum_3m,
        momentum_5m=momentum_5m,
        volume_ratio=volume_ratio,
        pullback_volume_ratio=pullback_volume_ratio,
        anti_chase=anti_chase,
        lost_reclaim=lost_reclaim,
    )


def entry_fade_score(watch: WatchState, candles_5m: Sequence[Candle]) -> int:
    if len(candles_5m) < 55:
        return 99
    direction = watch.direction
    last = candles_5m[-1]
    closes = [candle.close for candle in candles_5m]
    rsi = rsi_value(closes, 14)
    atr5 = atr_value(candles_5m, 14)
    ema20 = ema_value(closes, 20)
    distance = safe_div((last.close - ema20) * direction, atr5)
    wick_rejection = (
        last.upper_wick_fraction >= 0.38 and last.close_location < 0.62
        if direction > 0
        else last.lower_wick_fraction >= 0.38 and last.close_location > 0.38
    )
    rsi_extreme = rsi >= 82.0 if direction > 0 else rsi <= 18.0
    score = 0
    score += 1 if rsi_extreme else 0
    score += 2 if wick_rejection else 0
    score += 1 if has_rsi_divergence(candles_5m, direction) else 0
    score += 2 if structure_break_against_impulse(candles_5m, direction) else 0
    score += 2 if failed_retest_after_break(candles_5m, direction) else 0
    score += 1 if distance >= 3.0 else 0
    return score


def continuation_score(
    watch: WatchState, reclaim: ReclaimMetrics, fade_score: int
) -> int:
    score = 0
    score += 2 if watch.tf_agreement >= TF_AGREEMENT_MIN else 0
    score += 1 if watch.trend_aligned else 0
    score += 1 if watch.adx_15m >= 18.0 else 0
    score += 1 if reclaim.pullback_volume_ratio <= 1.10 else 0
    score += 2 if reclaim.confirmed else 0
    score += 1 if reclaim.momentum_3m * watch.direction > 0 else 0
    score += (
        1
        if PULLBACK_FRACTION_MIN <= watch.max_retrace_fraction <= PULLBACK_FRACTION_MAX
        else 0
    )
    score += 1 if fade_score <= FADE_SCORE_MAX else 0
    return score


def calculate_quality_score(
    watch: WatchState,
    reclaim: ReclaimMetrics,
    continuation: int,
    fade_score: int,
) -> int:
    score = 44
    score += min(12, watch.tf_agreement * 4)
    score += 7 if watch.trend_aligned else 0
    score += min(8, max(0, continuation - 5) * 3)
    score += 9 if 0.22 <= watch.max_retrace_fraction <= 0.38 else 5
    score += (
        7
        if reclaim.pullback_volume_ratio <= 0.95
        else 3
        if reclaim.pullback_volume_ratio <= 1.10
        else 0
    )
    score += 8 if reclaim.confirmed else 0
    score += 5 if watch.compression_release else 0
    score += 4 if watch.spread_bps <= MAX_BOOK_SPREAD_BPS / 2 else 2
    if watch.btc_regime == "BULL" and watch.direction > 0:
        score += 3
    elif watch.btc_regime == "BEAR" and watch.direction < 0:
        score += 3
    elif watch.btc_regime == "SHOCK_UP" and watch.direction < 0:
        score -= 10
    elif watch.btc_regime == "SHOCK_DOWN" and watch.direction > 0:
        score -= 10
    score -= max(0, fade_score - FADE_SCORE_MAX) * 5
    score -= 6 if reclaim.anti_chase > ANTI_CHASE_MAX else 0
    return int(clamp(score, 0, 100))


def build_trade_signal(
    watch: WatchState,
    candles_1m: Sequence[Candle],
    reclaim: ReclaimMetrics,
    continuation: int,
    fade_score: int,
    quality: int,
) -> tuple[Optional[TradeSignal], str]:
    if not candles_1m or not reclaim.confirmed:
        return None, "reclaim_not_confirmed"
    entry = reclaim.current_price
    atr1 = atr_value(candles_1m, 14)
    recent = candles_1m[-8:]
    if watch.direction > 0:
        structural_stop = min(candle.low for candle in recent) - 0.15 * atr1
        raw_stop_move = safe_div(entry - structural_stop, entry)
    else:
        structural_stop = max(candle.high for candle in recent) + 0.15 * atr1
        raw_stop_move = safe_div(structural_stop - entry, entry)
    if raw_stop_move <= 0:
        return None, "invalid_structure_stop"
    if raw_stop_move > STOP_MOVE_MAX:
        return None, f"structure_stop_too_wide:{raw_stop_move:.4f}"
    stop_move = max(STOP_MOVE_MIN, raw_stop_move)
    if TP_MOVES[2] / stop_move < MIN_RR_TO_TP3:
        return None, "rr_to_tp3_too_low"

    side = "LONG" if watch.direction > 0 else "SHORT"
    stop = (
        entry * (1.0 - stop_move) if watch.direction > 0 else entry * (1.0 + stop_move)
    )
    targets = [
        entry * (1.0 + move) if watch.direction > 0 else entry * (1.0 - move)
        for move in TP_MOVES
    ]
    if watch.direction > 0:
        zone_low, zone_high = entry * 0.9985, entry * 1.0005
    else:
        zone_low, zone_high = entry * 0.9995, entry * 1.0015
    created = now_ts()
    signal_id = f"v20.4:{watch.symbol}:{side}:{created}:{uuid.uuid4().hex[:8]}"
    signal = TradeSignal(
        signal_id=signal_id,
        event_id=watch.event_id,
        feature_schema=FEATURE_SCHEMA,
        symbol=watch.symbol,
        side=side,
        strategy=f"PRO_SPIKE_CONTINUATION_{side}",
        created_at=created,
        entry_price=entry,
        entry_zone_low=zone_low,
        entry_zone_high=zone_high,
        stop_price=stop,
        targets=targets,
        stop_move=stop_move,
        quality_score=quality,
        grade="A+" if quality >= 90 else "A",
        tf_agreement=watch.tf_agreement,
        continuation_score=continuation,
        fade_score=fade_score,
        impulse_move=watch.impulse_move,
        pullback_fraction=watch.max_retrace_fraction,
        reclaim_volume_ratio=reclaim.volume_ratio,
        btc_regime=watch.btc_regime,
        spread_bps=watch.spread_bps,
        depth_usdt=watch.depth_usdt,
        heat_state=watch.heat_state,
        max_price=entry,
        min_price=entry,
        last_monitor_ms=((created * 1000) // 60_000 + 1) * 60_000,
    )
    return signal, "accepted"


def price_digits(price: float) -> int:
    if price >= 1000:
        return 2
    if price >= 100:
        return 3
    if price >= 1:
        return 4
    if price >= 0.1:
        return 5
    if price >= 0.01:
        return 6
    return 8


def format_price(price: float) -> str:
    return f"{price:.{price_digits(price)}f}"


def format_signal(signal: TradeSignal) -> str:
    emoji = "🟢" if signal.side == "LONG" else "🔴"
    targets = "\n".join(
        f"TP{index}: <code>{format_price(price)}</code> ({TP_MOVES[index - 1] * 100:.1f}%)"
        for index, price in enumerate(signal.targets, 1)
    )
    return (
        f"{emoji} <b>V20.4 PRO CONTINUATION · {signal.side}</b>\n"
        f"<b>{signal.symbol}</b> · качество {signal.quality_score}% ({signal.grade})\n\n"
        f"Зона входа: <code>{format_price(signal.entry_zone_low)} — {format_price(signal.entry_zone_high)}</code>\n"
        f"Контрольная цена: <code>{format_price(signal.entry_price)}</code>\n"
        f"SL: <code>{format_price(signal.stop_price)}</code> ({signal.stop_move * 100:.2f}%)\n\n"
        f"{targets}\n\n"
        f"Подтверждения: continuation {signal.continuation_score}/10 · fade {signal.fade_score} · "
        f"TF {signal.tf_agreement}/4 · pullback {signal.pullback_fraction * 100:.1f}% импульса · "
        f"reclaim Vx{signal.reclaim_volume_ratio:.2f}\n"
        f"Рынок: {signal.heat_state} · BTC: {signal.btc_regime} · "
        f"spread {signal.spread_bps:.2f} bps\n\n"
        f"Главная цель: <b>TP3 (+2.0%)</b>. Усреднение запрещено. "
        f"Плечо ≤ {MAX_LEVERAGE}x · риск ≤ {ACCOUNT_RISK_FRACTION * 100:.2f}% счёта.\n"
        f"Режим: <b>PAPER</b> · сигнал {signal.signal_id}"
    )


def format_watch(watch: WatchState) -> str:
    direction = "UP/LONG" if watch.direction > 0 else "DOWN/SHORT"
    kind = "compression release" if watch.compression_release else "active impulse"
    return (
        f"👀 <b>V20.4 WATCH {direction}</b> · {watch.symbol}\n"
        f"{kind}: impulse {watch.impulse_move * 100:.1f}% · ATR x{watch.atr_mult:.2f} · "
        f"volume x{watch.volume_pace:.2f} · TF {watch.tf_agreement}/4.\n"
        f"Состояние рынка: {watch.heat_state}.\n"
        f"Жду откат 18–45% → закрытый reclaim → повторное ускорение. Входа пока нет."
    )


class TradingEngine:
    def __init__(
        self, client: BingXPublicClient, telegram: TelegramClient, store: StateStore
    ) -> None:
        self.client = client
        self.telegram = telegram
        self.store = store
        self.state = store.load()
        self.cache: dict[tuple[str, str], list[Candle]] = {}
        self.stop_event = asyncio.Event()

    def count(self, name: str, amount: int = 1) -> None:
        counters = self.state.setdefault("counters", {})
        counters[name] = int(counters.get(name, 0)) + amount

    def reject(self, reason: str) -> None:
        self.count(f"reject:{reason}")

    async def frame(self, symbol: str, interval: str, limit: int = 120) -> list[Candle]:
        key = (symbol, interval)
        if key not in self.cache:
            self.cache[key] = await self.client.klines(symbol, interval, limit)
        return self.cache[key]

    def active_signal_for_symbol(self, symbol: str) -> bool:
        return any(
            item.get("symbol") == symbol and item.get("status") == "ACTIVE"
            for item in self.state.get("signals", {}).values()
        )

    def symbol_is_quarantined(self, symbol: str) -> bool:
        stats = self.state.get("symbol_stats", {}).get(symbol, {})
        return int(stats.get("quarantine_until", 0)) > now_ts()

    def strategy_is_allowed(self, strategy: str) -> tuple[bool, str]:
        outcomes = [
            item
            for item in self.state.get("outcomes", [])
            if item.get("strategy") == strategy
            and item.get("feature_schema") == FEATURE_SCHEMA
        ][-25:]
        if len(outcomes) < 25:
            return True, "collecting_forward_evidence"
        tp3 = sum(bool(item.get("tp3_success")) for item in outcomes)
        sl = sum(item.get("result") == "sl" for item in outcomes)
        net_r = sum(safe_float(item.get("net_pnl_r")) for item in outcomes)
        if net_r < 0 or tp3 / len(outcomes) < 0.20 or sl / len(outcomes) > 0.40:
            return False, f"rolling25_failed:tp3={tp3},sl={sl},net={net_r:+.2f}R"
        return True, f"rolling25_passed:tp3={tp3},sl={sl},net={net_r:+.2f}R"

    def bucket_key(self, signal: TradeSignal) -> str:
        tf_bin = "tf3plus" if signal.tf_agreement >= 3 else "tf2"
        pullback_bin = (
            "pb22_38" if 0.22 <= signal.pullback_fraction <= 0.38 else "pb_edge"
        )
        quality_bin = "q90" if signal.quality_score >= 90 else "q82_89"
        return f"{signal.side}|{tf_bin}|{pullback_bin}|{quality_bin}"

    def adaptive_bucket_allows(self, signal: TradeSignal) -> tuple[bool, str]:
        key = self.bucket_key(signal)
        bucket = self.state.get("adaptive_buckets", {}).get(key, {})
        n = int(bucket.get("n", 0))
        if n < 20:
            return True, "bucket_learning"
        avg_r = safe_div(safe_float(bucket.get("net_r")), n)
        tp_rate = safe_div(int(bucket.get("tp3", 0)), n)
        if avg_r <= -0.15 and tp_rate < 0.20:
            return False, f"negative_bucket:{key}:n={n}:avg={avg_r:+.2f}R"
        return True, f"bucket_ok:{key}:n={n}:avg={avg_r:+.2f}R"

    def correlated_entry_allowed(self, side: str) -> bool:
        cutoff = now_ts() - 300
        recent = [
            item
            for item in self.state.get("outcomes", [])
            if item.get("side") == side and int(item.get("created_at", 0)) >= cutoff
        ]
        active = [
            item
            for item in self.state.get("signals", {}).values()
            if item.get("side") == side and int(item.get("created_at", 0)) >= cutoff
        ]
        return len(recent) + len(active) < 2

    def can_open(self, signal: TradeSignal) -> tuple[bool, str]:
        active = [
            item
            for item in self.state.get("signals", {}).values()
            if item.get("status") == "ACTIVE"
        ]
        if len(active) >= MAX_ACTIVE_SIGNALS:
            return False, "max_active_signals"
        if any(item.get("symbol") == signal.symbol for item in active):
            return False, "symbol_already_active"
        if self.symbol_is_quarantined(signal.symbol):
            return False, "symbol_quarantine"
        breaker_until = int(self.state.get("circuit_breaker_until", 0))
        if breaker_until > now_ts():
            return False, "circuit_breaker"
        if not self.correlated_entry_allowed(signal.side):
            return False, "correlated_entry_limit"
        strategy_allowed, reason = self.strategy_is_allowed(signal.strategy)
        if not strategy_allowed:
            return False, reason
        bucket_allowed, reason = self.adaptive_bucket_allows(signal)
        if not bucket_allowed:
            return False, reason
        return True, "allowed"

    async def screen_symbol(self, ticker: Ticker) -> Optional[ScreenCandidate]:
        try:
            candles = await self.frame(ticker.symbol, "15m", 120)
            candidate = screen_impulse(ticker.symbol, candles)
            if candidate:
                self.count("screen_candidates")
            return candidate
        except Exception as error:  # one pair must never kill the whole scan
            LOG.debug("Screen failed for %s: %s", ticker.symbol, error)
            self.count("screen_errors")
            return None

    async def create_watch(
        self, candidate: ScreenCandidate, btc_regime: str
    ) -> Optional[WatchState]:
        event_id = (
            f"{candidate.symbol}:{candidate.direction}:{candidate.candle_time_ms}"
        )
        if event_id in self.state.get("events", {}):
            self.reject("event_already_processed")
            return None
        if self.active_signal_for_symbol(candidate.symbol):
            self.reject("symbol_already_active")
            return None
        if candidate.symbol in self.state.get("watches", {}):
            self.reject("watch_already_exists")
            return None
        if self.symbol_is_quarantined(candidate.symbol):
            self.reject("symbol_quarantine")
            return None
        minimum_impulse = 0.012 if candidate.compression_release else 0.030
        if not minimum_impulse <= candidate.impulse_move <= 0.15:
            self.reject("impulse_outside_primary_range")
            return None
        if candidate.volume_pace < 1.50 and not candidate.compression_release:
            self.reject("volume_pace")
            return None
        if candidate.atr_mult < 1.50 and not candidate.compression_release:
            self.reject("atr_expansion")
            return None
        if candidate.volume_pace > 8.0:
            self.reject("runaway_volume_requires_extended_mode")
            return None
        try:
            candles_5m, candles_1h, candles_4h, book = await asyncio.gather(
                self.frame(candidate.symbol, "5m", 120),
                self.frame(candidate.symbol, "1h", 100),
                self.frame(candidate.symbol, "4h", 100),
                self.client.depth(candidate.symbol, 20),
            )
        except Exception as error:
            LOG.debug("Deep data failed for %s: %s", candidate.symbol, error)
            self.reject("deep_data_error")
            return None
        frames = {
            "5m": candles_5m,
            "15m": await self.frame(candidate.symbol, "15m", 120),
            "1h": candles_1h,
            "4h": candles_4h,
        }
        if any(len(value) < 55 for value in frames.values()):
            self.reject("insufficient_closed_candles")
            return None
        context = classify_market_context(candidate, frames, btc_regime)
        if context.tf_agreement < TF_AGREEMENT_MIN:
            self.reject("tf_disagreement")
            return None
        if context.structure_broken and context.failed_retest:
            # A confirmed exhaustion is recorded for research, never converted into a live fade.
            self.reject("confirmed_exhaustion_research_only")
            self.count("research_exhaustion")
            return None
        if not book.passed:
            if book.spread_bps > MAX_BOOK_SPREAD_BPS:
                self.reject("book_spread")
            elif book.min_depth_usdt < MIN_BOOK_DEPTH_USDT:
                self.reject("book_depth")
            else:
                self.reject("book_slippage")
            return None
        if (
            btc_regime == "SHOCK_DOWN"
            and candidate.direction > 0
            and context.tf_agreement < 3
        ):
            self.reject("btc_shock_conflict")
            return None
        if (
            btc_regime == "SHOCK_UP"
            and candidate.direction < 0
            and context.tf_agreement < 3
        ):
            self.reject("btc_shock_conflict")
            return None
        watch = WatchState(
            event_id=event_id,
            symbol=candidate.symbol,
            direction=candidate.direction,
            created_at=now_ts(),
            candle_time_ms=candidate.candle_time_ms,
            base_price=candidate.base_price,
            extreme_price=candidate.extreme_price,
            impulse_move=candidate.impulse_move,
            atr_mult=candidate.atr_mult,
            volume_pace=candidate.volume_pace,
            range_acceleration=candidate.range_acceleration,
            compression_release=candidate.compression_release,
            compression_rank=candidate.compression_rank,
            preliminary_score=candidate.preliminary_score,
            tf_agreement=context.tf_agreement,
            trend_aligned=context.trend_aligned,
            adx_15m=context.adx_15m,
            rsi_15m=context.rsi_15m,
            initial_ema_distance_atr=context.ema_distance_atr,
            fade_score=context.fade_score,
            btc_regime=btc_regime,
            spread_bps=book.spread_bps,
            depth_usdt=book.min_depth_usdt,
            book_imbalance=book.imbalance,
            heat_state=context.heat_state,
            last_updated_at=now_ts(),
        )
        self.state.setdefault("watches", {})[watch.symbol] = asdict(watch)
        self.state.setdefault("events", {})[watch.event_id] = {
            "symbol": watch.symbol,
            "direction": watch.direction,
            "seen_at": now_ts(),
            "status": "WATCH",
        }
        self.count("watches_started")
        LOG.info("Watch started %s direction=%s", watch.symbol, watch.direction)
        if SEND_WATCH_ALERTS:
            await self.telegram.send(format_watch(watch))
        return watch

    async def update_watch(self, watch: WatchState) -> None:
        age_minutes = (now_ts() - watch.created_at) / 60.0
        if age_minutes > WATCH_MAX_MINUTES:
            self.reject("watch_expired")
            self.state["watches"].pop(watch.symbol, None)
            return
        try:
            candles_1m, candles_5m = await asyncio.gather(
                self.frame(watch.symbol, "1m", 120),
                self.frame(watch.symbol, "5m", 120),
            )
        except Exception as error:
            LOG.debug("Watch update failed %s: %s", watch.symbol, error)
            self.reject("watch_data_error")
            return
        if len(candles_1m) < 30 or len(candles_5m) < 55:
            self.reject("watch_insufficient_candles")
            return
        recent = [
            candle
            for candle in candles_1m
            if candle.time_ms >= watch.created_at * 1000 - 60_000
        ]
        if not recent:
            recent = candles_1m[-5:]
        if watch.direction > 0:
            watch.extreme_price = max(
                watch.extreme_price, max(candle.high for candle in recent)
            )
            retrace_price = min(candle.low for candle in recent[-4:])
        else:
            watch.extreme_price = min(
                watch.extreme_price, min(candle.low for candle in recent)
            )
            retrace_price = max(candle.high for candle in recent[-4:])
        current_retrace = calculate_retrace(
            watch.direction, watch.base_price, watch.extreme_price, retrace_price
        )
        watch.max_retrace_fraction = max(watch.max_retrace_fraction, current_retrace)
        watch.last_updated_at = now_ts()
        if (
            current_retrace >= PULLBACK_INVALIDATION
            or watch.max_retrace_fraction > 0.50
        ):
            self.reject("pullback_structure_broken")
            self.state["watches"].pop(watch.symbol, None)
            return
        if PULLBACK_FRACTION_MIN <= watch.max_retrace_fraction <= PULLBACK_FRACTION_MAX:
            watch.pullback_seen = True
            watch.stage = "WAIT_RECLAIM"
        self.state["watches"][watch.symbol] = asdict(watch)
        if not watch.pullback_seen:
            return

        reclaim = evaluate_reclaim(candles_1m, watch.direction)
        if not reclaim.confirmed:
            return
        fade = entry_fade_score(watch, candles_5m)
        continuation = continuation_score(watch, reclaim, fade)
        quality = calculate_quality_score(watch, reclaim, continuation, fade)
        if fade > FADE_SCORE_MAX:
            self.reject("fade_risk_at_entry")
            return
        if continuation < CONT_SCORE_MIN:
            self.reject("continuation_score")
            return
        if quality < MIN_SIGNAL_SCORE:
            self.reject("quality_score")
            return
        signal, reason = build_trade_signal(
            watch, candles_1m, reclaim, continuation, fade, quality
        )
        if signal is None:
            self.reject(reason.split(":", 1)[0])
            return
        allowed, reason = self.can_open(signal)
        if not allowed:
            self.reject(reason.split(":", 1)[0])
            return
        self.state.setdefault("signals", {})[signal.signal_id] = asdict(signal)
        if signal.event_id in self.state.get("events", {}):
            self.state["events"][signal.event_id]["status"] = "SIGNAL"
        self.state["watches"].pop(watch.symbol, None)
        self.count("signals_created")
        await self.telegram.send(format_signal(signal))
        LOG.info(
            "Signal created %s %s score=%s",
            signal.symbol,
            signal.side,
            signal.quality_score,
        )

    async def update_all_watches(self) -> None:
        watches = [
            watch_from_dict(item)
            for item in list(self.state.get("watches", {}).values())
        ]
        if watches:
            await asyncio.gather(*(self.update_watch(watch) for watch in watches))

    @staticmethod
    def current_r(signal: TradeSignal, price: float) -> float:
        direction = 1.0 if signal.side == "LONG" else -1.0
        return safe_div(
            (price / signal.entry_price - 1.0) * direction, signal.stop_move
        )

    @staticmethod
    def update_excursions(signal: TradeSignal, high: float, low: float) -> None:
        signal.max_price = max(signal.max_price or signal.entry_price, high)
        signal.min_price = min(signal.min_price or signal.entry_price, low)
        if signal.side == "LONG":
            favourable = safe_div(
                signal.max_price / signal.entry_price - 1.0, signal.stop_move
            )
            adverse = safe_div(
                signal.min_price / signal.entry_price - 1.0, signal.stop_move
            )
        else:
            favourable = safe_div(
                1.0 - signal.min_price / signal.entry_price, signal.stop_move
            )
            adverse = safe_div(
                1.0 - signal.max_price / signal.entry_price, signal.stop_move
            )
        signal.mfe_r = max(signal.mfe_r, favourable)
        signal.mae_r = min(signal.mae_r, adverse)

    @staticmethod
    def stop_touched(signal: TradeSignal, high: float, low: float) -> bool:
        return (
            low <= signal.stop_price
            if signal.side == "LONG"
            else high >= signal.stop_price
        )

    @staticmethod
    def target_touched(
        signal: TradeSignal, target: float, high: float, low: float
    ) -> bool:
        return high >= target if signal.side == "LONG" else low <= target

    async def announce_target(self, signal: TradeSignal, target_number: int) -> None:
        label = (
            "✅ Главная цель TP3 достигнута"
            if target_number == 3
            else f"🎯 TP{target_number} достигнута"
        )
        await self.telegram.send(
            f"{label}\n<b>{signal.symbol} {signal.side}</b> · "
            f"цена <code>{format_price(signal.targets[target_number - 1])}</code>"
        )

    def calculate_realized_r(
        self, signal: TradeSignal, exit_price: float, exit_kind: str
    ) -> tuple[float, float, float]:
        hit_count = min(signal.tp_hit, len(TP_MOVES))
        gross_r = sum(
            TP_SHARES[index] * TP_MOVES[index] / signal.stop_move
            for index in range(hit_count)
        )
        closed_share = sum(TP_SHARES[:hit_count])
        remaining_share = max(0.0, 1.0 - closed_share)
        gross_r += remaining_share * self.current_r(signal, exit_price)
        exit_fee = ESTIMATED_MAKER_FEE if exit_kind == "tp3" else ESTIMATED_TAKER_FEE
        fee_fraction = ESTIMATED_TAKER_FEE
        fee_fraction += sum(TP_SHARES[:hit_count]) * ESTIMATED_MAKER_FEE
        fee_fraction += remaining_share * exit_fee
        fee_r = fee_fraction / signal.stop_move
        return gross_r, fee_r, gross_r - fee_r

    async def finalize_signal(
        self,
        signal: TradeSignal,
        result: str,
        exit_price: float,
        exit_kind: str,
        reason: str,
    ) -> None:
        gross_r, fee_r, net_r = self.calculate_realized_r(signal, exit_price, exit_kind)
        outcome = {
            "signal_id": signal.signal_id,
            "event_id": signal.event_id,
            "feature_schema": signal.feature_schema,
            "strategy": signal.strategy,
            "symbol": signal.symbol,
            "side": signal.side,
            "created_at": signal.created_at,
            "closed_at": now_ts(),
            "duration_minutes": (now_ts() - signal.created_at) / 60.0,
            "entry_price": signal.entry_price,
            "exit_price": exit_price,
            "stop_price": signal.stop_price,
            "targets": signal.targets,
            "tp_hit": signal.tp_hit,
            "tp3_success": signal.tp_hit >= 3,
            "result": result,
            "reason": reason,
            "gross_pnl_r": gross_r,
            "fees_r": fee_r,
            "net_pnl_r": net_r,
            "mfe_r": signal.mfe_r,
            "mae_r": signal.mae_r,
            "quality_score": signal.quality_score,
            "tf_agreement": signal.tf_agreement,
            "continuation_score": signal.continuation_score,
            "fade_score": signal.fade_score,
            "impulse_move": signal.impulse_move,
            "pullback_fraction": signal.pullback_fraction,
            "reclaim_volume_ratio": signal.reclaim_volume_ratio,
            "btc_regime": signal.btc_regime,
            "spread_bps": signal.spread_bps,
            "paper": True,
        }
        outcomes = self.state.setdefault("outcomes", [])
        outcomes.append(outcome)
        del outcomes[:-2000]
        self.state.get("signals", {}).pop(signal.signal_id, None)
        self.update_symbol_and_bucket(signal, outcome)
        self.update_circuit_breaker()
        emoji = "✅" if signal.tp_hit >= 3 else "🛑" if result == "sl" else "⌛"
        await self.telegram.send(
            f"{emoji} <b>{signal.symbol} {signal.side} · {result.upper()}</b>\n"
            f"TP достигнуто: {signal.tp_hit} · net {net_r:+.2f}R · MFE {signal.mfe_r:.2f}R · "
            f"MAE {signal.mae_r:.2f}R\nПричина: {reason}"
        )
        await self.maybe_send_checkpoint()

    def update_symbol_and_bucket(
        self, signal: TradeSignal, outcome: dict[str, Any]
    ) -> None:
        symbol_stats = self.state.setdefault("symbol_stats", {}).setdefault(
            signal.symbol,
            {
                "fail_streak": 0,
                "quarantine_until": 0,
                "sl_timestamps": [],
                "observations": 0,
            },
        )
        symbol_stats["observations"] = int(symbol_stats.get("observations", 0)) + 1
        symbol_stats["last_result"] = outcome["result"]
        symbol_stats["last_net_r"] = outcome["net_pnl_r"]
        symbol_stats["updated_at"] = now_ts()
        if outcome["tp3_success"]:
            symbol_stats["fail_streak"] = 0
        else:
            symbol_stats["fail_streak"] = int(symbol_stats.get("fail_streak", 0)) + 1
        sl_timestamps = [
            int(timestamp)
            for timestamp in symbol_stats.get("sl_timestamps", [])
            if int(timestamp) >= now_ts() - 86_400
        ]
        if outcome["result"] == "sl":
            sl_timestamps.append(now_ts())
        symbol_stats["sl_timestamps"] = sl_timestamps
        if len(sl_timestamps) >= 2:
            symbol_stats["quarantine_until"] = now_ts() + SYMBOL_QUARANTINE_HOURS * 3600

        key = self.bucket_key(signal)
        bucket = self.state.setdefault("adaptive_buckets", {}).setdefault(
            key, {"n": 0, "tp3": 0, "sl": 0, "expired": 0, "net_r": 0.0}
        )
        bucket["n"] += 1
        bucket["tp3"] += int(outcome["tp3_success"])
        bucket["sl"] += int(outcome["result"] == "sl")
        bucket["expired"] += int(outcome["result"] == "expired")
        bucket["net_r"] += safe_float(outcome["net_pnl_r"])

    def update_circuit_breaker(self) -> None:
        recent = [
            item
            for item in self.state.get("outcomes", [])
            if item.get("feature_schema") == FEATURE_SCHEMA
        ][-10:]
        if len(recent) < 3:
            return
        consecutive_sl = 0
        for item in reversed(recent):
            if item.get("result") == "sl":
                consecutive_sl += 1
            else:
                break
        rolling_r = sum(safe_float(item.get("net_pnl_r")) for item in recent)
        if consecutive_sl >= 3 or (len(recent) == 10 and rolling_r <= -2.0):
            self.state["circuit_breaker_until"] = now_ts() + 2 * 3600
            self.state["circuit_breaker_reason"] = (
                f"consecutive_sl={consecutive_sl}, rolling10={rolling_r:+.2f}R"
            )

    async def maybe_send_checkpoint(self) -> None:
        outcomes = [
            item
            for item in self.state.get("outcomes", [])
            if item.get("feature_schema") == FEATURE_SCHEMA
        ]
        count = len(outcomes)
        if (
            count == 0
            or count % 25 != 0
            or self.state.get("last_checkpoint_count") == count
        ):
            return
        cohort = outcomes[-25:]
        tp3 = sum(bool(item.get("tp3_success")) for item in cohort)
        sl = sum(item.get("result") == "sl" for item in cohort)
        expired = sum(item.get("result") == "expired" for item in cohort)
        net_r = sum(safe_float(item.get("net_pnl_r")) for item in cohort)
        self.state["last_checkpoint_count"] = count
        await self.telegram.send(
            f"🧠 <b>V20.4 forward checkpoint {count}</b>\n"
            f"Последние 25: {tp3} TP3+ / {sl} SL / {expired} expired · net {net_r:+.2f}R.\n"
            f"Автоматический переход на реальные деньги отключён."
        )

    async def process_market_path(
        self, signal: TradeSignal, candles: Sequence[Candle], last_price: float
    ) -> None:
        for candle in candles:
            if candle.time_ms < signal.last_monitor_ms:
                continue
            self.update_excursions(signal, candle.high, candle.low)
            # Conservative same-candle rule: if target and stop are both touched, count stop first.
            if self.stop_touched(signal, candle.high, candle.low):
                await self.finalize_signal(
                    signal, "sl", signal.stop_price, "sl", "protective_stop"
                )
                return
            while signal.tp_hit < len(signal.targets) and self.target_touched(
                signal, signal.targets[signal.tp_hit], candle.high, candle.low
            ):
                signal.tp_hit += 1
                await self.announce_target(signal, signal.tp_hit)
                if signal.tp_hit >= 3:
                    await self.finalize_signal(
                        signal,
                        "profit",
                        signal.targets[2],
                        "tp3",
                        "tp3_primary_success",
                    )
                    return
            signal.last_monitor_ms = candle.time_ms + INTERVAL_MS["1m"]

        self.update_excursions(signal, last_price, last_price)
        if self.stop_touched(signal, last_price, last_price):
            await self.finalize_signal(
                signal, "sl", signal.stop_price, "sl", "protective_stop"
            )
            return
        while signal.tp_hit < len(signal.targets) and self.target_touched(
            signal, signal.targets[signal.tp_hit], last_price, last_price
        ):
            signal.tp_hit += 1
            await self.announce_target(signal, signal.tp_hit)
            if signal.tp_hit >= 3:
                await self.finalize_signal(
                    signal, "profit", signal.targets[2], "tp3", "tp3_primary_success"
                )
                return

        age_minutes = (now_ts() - signal.created_at) / 60.0
        reclaim = evaluate_reclaim(candles, 1 if signal.side == "LONG" else -1)
        current_r = self.current_r(signal, last_price)
        if (
            signal.tp_hit == 0
            and age_minutes >= NO_PROGRESS_MINUTES
            and signal.mfe_r < NO_PROGRESS_MFE_R
            and reclaim.lost_reclaim
        ):
            await self.finalize_signal(
                signal, "expired", last_price, "expired", "no_progress_and_reclaim_lost"
            )
            return
        if (
            signal.tp_hit == 0
            and age_minutes >= PRE_TP1_STALE_MINUTES
            and current_r <= 0
            and reclaim.lost_reclaim
        ):
            await self.finalize_signal(
                signal, "expired", last_price, "expired", "pre_tp1_stale"
            )
            return
        if age_minutes >= HARD_EXPIRE_MINUTES:
            await self.finalize_signal(
                signal, "expired", last_price, "expired", "hard_expire"
            )
            return
        self.state["signals"][signal.signal_id] = asdict(signal)

    async def monitor_signals(self, price_map: dict[str, float]) -> None:
        signals = [
            signal_from_dict(item)
            for item in list(self.state.get("signals", {}).values())
            if item.get("status") == "ACTIVE"
        ]
        for signal in signals:
            price = price_map.get(signal.symbol)
            if not price:
                self.count("monitor_missing_price")
                continue
            try:
                candles = await self.frame(signal.symbol, "1m", 120)
                await self.process_market_path(signal, candles, price)
            except Exception as error:
                LOG.error("Monitor failed %s: %s", signal.signal_id, error)
                self.count("monitor_errors")

    def prune_state(self) -> None:
        event_cutoff = now_ts() - 36 * 3600
        events = self.state.setdefault("events", {})
        self.state["events"] = {
            event_id: item
            for event_id, item in events.items()
            if int(item.get("seen_at", 0)) >= event_cutoff
        }
        for stats in self.state.get("symbol_stats", {}).values():
            stats["sl_timestamps"] = [
                int(timestamp)
                for timestamp in stats.get("sl_timestamps", [])
                if int(timestamp) >= now_ts() - 86_400
            ]
            if int(stats.get("quarantine_until", 0)) <= now_ts():
                stats["quarantine_until"] = 0

    def diagnostic_text(self, cycle_seconds: float = 0.0) -> str:
        outcomes = [
            item
            for item in self.state.get("outcomes", [])
            if item.get("feature_schema") == FEATURE_SCHEMA
        ]
        cohort = outcomes[-25:]
        tp3 = sum(bool(item.get("tp3_success")) for item in cohort)
        sl = sum(item.get("result") == "sl" for item in cohort)
        expired = sum(item.get("result") == "expired" for item in cohort)
        net_r = sum(safe_float(item.get("net_pnl_r")) for item in cohort)
        gross_wins = sum(max(0.0, safe_float(item.get("net_pnl_r"))) for item in cohort)
        gross_losses = abs(
            sum(min(0.0, safe_float(item.get("net_pnl_r"))) for item in cohort)
        )
        profit_factor = safe_div(
            gross_wins, gross_losses, 99.0 if gross_wins > 0 else 0.0
        )
        counters = self.state.get("counters", {})
        rejects = sorted(
            (
                (name.removeprefix("reject:"), int(value))
                for name, value in counters.items()
                if name.startswith("reject:")
            ),
            key=lambda pair: pair[1],
            reverse=True,
        )[:5]
        reject_text = ", ".join(f"{name}={value}" for name, value in rejects) or "нет"
        breaker_until = int(self.state.get("circuit_breaker_until", 0))
        breaker = (
            f"до {utc_text(breaker_until)}" if breaker_until > now_ts() else "выключен"
        )
        return (
            f"📊 <b>V20.4.2 PRO diagnostics</b>\n"
            f"Сделки нового алгоритма: {len(outcomes)} · последние {len(cohort)}: "
            f"TP3+ {tp3} / SL {sl} / expired {expired}\n"
            f"Net {net_r:+.2f}R · PF {profit_factor:.2f}\n"
            f"Активные сигналы: {len(self.state.get('signals', {}))} · "
            f"наблюдения: {len(self.state.get('watches', {}))}\n"
            f"Circuit breaker: {breaker}\n"
            f"Частые отказы: {reject_text}\n"
            f"Последний цикл: {cycle_seconds:.1f}s · {utc_text()}"
        )

    async def maybe_send_diagnostic(
        self, cycle_seconds: float, force: bool = False
    ) -> None:
        last_sent = int(self.state.get("last_diagnostic_at", 0))
        if not force and now_ts() - last_sent < DIAGNOSTIC_SECONDS:
            return
        self.state["last_diagnostic_at"] = now_ts()
        await self.telegram.send(self.diagnostic_text(cycle_seconds))

    async def run_cycle(self) -> None:
        cycle_started = time.monotonic()
        self.cache = {}
        tickers = await self.client.tickers()
        if not tickers:
            raise RuntimeError("BingX returned no valid USDT perpetual tickers")
        price_map = {ticker.symbol: ticker.last_price for ticker in tickers}

        # Existing positions are always monitored before looking for new risk.
        await self.monitor_signals(price_map)
        btc_15m, btc_1h = await asyncio.gather(
            self.frame("BTC-USDT", "15m", 120),
            self.frame("BTC-USDT", "1h", 100),
        )
        btc_regime = classify_btc_regime(btc_15m, btc_1h)
        universe = select_universe(tickers)
        self.count("cycles")
        self.count("symbols_screened", len(universe))

        screened = await asyncio.gather(
            *(self.screen_symbol(ticker) for ticker in universe)
        )
        candidates = sorted(
            (candidate for candidate in screened if candidate is not None),
            key=lambda candidate: (
                candidate.compression_release,
                candidate.preliminary_score,
                candidate.volume_pace,
            ),
            reverse=True,
        )[:DEEP_CANDIDATE_LIMIT]
        if candidates:
            await asyncio.gather(
                *(self.create_watch(candidate, btc_regime) for candidate in candidates)
            )
        await self.update_all_watches()
        self.prune_state()
        elapsed = time.monotonic() - cycle_started
        await self.maybe_send_diagnostic(elapsed)
        self.store.save(self.state)
        LOG.info(
            "Cycle complete universe=%d candidates=%d watches=%d signals=%d btc=%s elapsed=%.1fs",
            len(universe),
            len(candidates),
            len(self.state.get("watches", {})),
            len(self.state.get("signals", {})),
            btc_regime,
            elapsed,
        )

    async def run(self) -> None:
        telegram_status = "ON" if self.telegram.enabled else "OFF (console only)"
        diagnosed = await self.telegram.diagnose()
        delivered = await self.telegram.send(
            f"🚀 <b>{APP_NAME}</b>\n"
            f"Маркер: <code>{DEPLOY_MARKER}</code>\n"
            f"Режим: PAPER · Telegram: {telegram_status}\n"
            f"Проверка Telegram: {'OK' if diagnosed else 'FAILED — см. Render Logs / health'}\n"
            f"Логика: impulse/squeeze → pullback 18–45% → closed reclaim → continuation.\n"
            f"RSI не используется как самостоятельная команда на разворот.\n\n"
            f"Если это сообщение пришло — V20.4.2 RENDER-SAFE запущен правильно.",
            attempts=TELEGRAM_STARTUP_ATTEMPTS,
        )
        if not delivered:
            LOG.critical(
                "STARTUP TELEGRAM MESSAGE WAS NOT DELIVERED. Open /health and Render Logs."
            )
        consecutive_errors = 0
        while not self.stop_event.is_set():
            loop_started = time.monotonic()
            try:
                await self.run_cycle()
                consecutive_errors = 0
            except asyncio.CancelledError:
                raise
            except Exception as error:
                consecutive_errors += 1
                self.count("cycle_errors")
                LOG.exception("Cycle failed: %s", error)
                if consecutive_errors in {1, 3, 10}:
                    await self.telegram.send(
                        f"⚠️ <b>V20.4.2 cycle error</b> · подряд {consecutive_errors}\n"
                        f"<code>{html.escape(str(error)[:600])}</code>"
                    )
            finally:
                try:
                    self.store.save(self.state)
                except OSError as error:
                    LOG.error("State save failed: %s", error)
            delay = max(1.0, SCAN_SECONDS - (time.monotonic() - loop_started))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
        await self.telegram.send("⏹ <b>V20.4.2 PRO stopped cleanly</b>")


HEALTH = {
    "app": APP_NAME,
    "marker": DEPLOY_MARKER,
    "started_at": utc_text(),
    "paper": True,
    "runtime": "not_started",
    "engine_state": "not_started",
    "engine_error": None,
}


def health_payload() -> dict[str, Any]:
    engine_state = str(HEALTH.get("engine_state", "unknown"))
    telegram = TelegramClient.public_status()
    if engine_state == "failed":
        status = "failed"
    elif telegram.get("last_send_ok") is False or not (
        telegram.get("token_present") and telegram.get("chat_id_present")
    ):
        status = "degraded"
    else:
        status = "ok"
    return {
        **HEALTH,
        "status": status,
        "time": utc_text(),
        "telegram": telegram,
        "start_commands_supported": [
            "python bot.py",
            "uvicorn bot:app --host 0.0.0.0 --port $PORT",
        ],
    }


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        if self.path not in {"/", "/health"}:
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(health_payload(), ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format_string: str, *args: Any) -> None:
        LOG.debug("Health server: " + format_string, *args)


def start_health_server() -> Optional[ThreadingHTTPServer]:
    try:
        port = int(clean_env(os.getenv("PORT"), "0"))
    except ValueError:
        LOG.error("Invalid PORT value; health server is disabled")
        return None
    if port <= 0:
        return None
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(
        target=server.serve_forever, name="health-server", daemon=True
    )
    thread.start()
    LOG.info("Health server listening on port %d", port)
    return server


class BotASGI:
    """Render-safe ASGI wrapper.

    The HTTP service completes its startup handshake immediately. The trading
    engine is supervised in the background and automatically retries after
    recoverable runtime/dependency/network errors. This prevents Uvicorn/Render
    exit status 3 from a bot-side startup exception.
    """

    def __init__(self) -> None:
        self.client: Optional[BingXPublicClient] = None
        self.engine: Optional[TradingEngine] = None
        self.engine_task: Optional[asyncio.Task[None]] = None
        self.stop_event: Optional[asyncio.Event] = None

    async def start(self) -> None:
        if self.engine_task is not None and not self.engine_task.done():
            return
        self.stop_event = asyncio.Event()
        HEALTH.update(
            {
                "runtime": "uvicorn_asgi",
                "engine_state": "starting",
                "engine_error": None,
                "started_at": utc_text(),
            }
        )
        self.engine_task = asyncio.create_task(
            self._supervise_engine(), name="v20-4-2-render-safe-supervisor"
        )
        LOG.info("ASGI startup complete; supervised trading engine scheduled")

    async def _supervise_engine(self) -> None:
        attempt = 0
        while self.stop_event is not None and not self.stop_event.is_set():
            attempt += 1
            try:
                self.client = BingXPublicClient()
                telegram = TelegramClient(self.client.session)
                self.engine = TradingEngine(
                    self.client, telegram, StateStore(STATE_FILE)
                )
                HEALTH.update(
                    {
                        "engine_state": "running",
                        "engine_error": None,
                        "engine_restart_attempt": attempt,
                        "http_backend": getattr(self.client, "http_backend", "unknown"),
                    }
                )
                await self.engine.run()
                if self.stop_event.is_set():
                    break
                raise RuntimeError("trading engine returned unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                HEALTH.update(
                    {
                        "engine_state": "degraded",
                        "engine_error": repr(error)[:500],
                        "engine_restart_attempt": attempt,
                    }
                )
                LOG.exception(
                    "Trading engine startup/runtime failed; retrying in 10s"
                )
            finally:
                if self.client is not None and not self.client.session.closed:
                    try:
                        await self.client.close()
                    except Exception:
                        LOG.exception("HTTP client close failed")
                self.client = None
                self.engine = None

            if self.stop_event is None or self.stop_event.is_set():
                break
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pass

    async def stop(self) -> None:
        if self.stop_event is not None:
            self.stop_event.set()
        if self.engine is not None:
            self.engine.stop_event.set()
        if self.engine_task is not None:
            try:
                await asyncio.wait_for(self.engine_task, timeout=8)
            except asyncio.TimeoutError:
                self.engine_task.cancel()
                await asyncio.gather(self.engine_task, return_exceptions=True)
        if self.engine is not None:
            try:
                self.engine.store.save(self.engine.state)
            except Exception:
                LOG.exception("Final state save failed")
        if self.client is not None and not self.client.session.closed:
            try:
                await self.client.close()
            except Exception:
                LOG.exception("Final HTTP client close failed")
        HEALTH["engine_state"] = "stopped"
        LOG.info("ASGI supervisor stopped cleanly")

    async def _lifespan(self, receive: Any, send: Any) -> None:
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                # Never fail the ASGI startup handshake for a recoverable bot error.
                try:
                    await self.start()
                except Exception as error:
                    HEALTH.update(
                        {
                            "engine_state": "degraded",
                            "engine_error": repr(error)[:500],
                        }
                    )
                    LOG.exception("ASGI bot scheduling failed; HTTP service stays alive")
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await self.stop()
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _http(self, scope: dict[str, Any], send: Any) -> None:
        path = str(scope.get("path", "/"))
        if path not in {"/", "/health"}:
            status_code = 404
            body = json.dumps({"status": "not_found"}).encode("utf-8")
        else:
            data = health_payload()
            # Liveness stays HTTP 200 even if the trading engine is temporarily
            # degraded; the JSON body exposes the real engine state.
            status_code = 200
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": [
                    (b"content-type", b"application/json; charset=utf-8"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            await self._lifespan(receive, send)
            return
        if scope["type"] == "http":
            await self._http(scope, send)
            return
        raise RuntimeError(f"Unsupported ASGI scope: {scope['type']}")


# Backward compatibility: existing Render services can keep
# `uvicorn bot:app --host 0.0.0.0 --port $PORT` unchanged.
app = BotASGI()


async def async_main() -> None:
    """Zero-dependency direct Render mode: `python bot.py`.

    The built-in health server binds $PORT. Trading runtime errors are retried
    without terminating the process.
    """
    health_server = start_health_server()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_name in (os_signal.SIGINT, os_signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_name, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass

    HEALTH.update(
        {
            "runtime": "python_direct",
            "engine_state": "starting",
            "engine_error": None,
            "started_at": utc_text(),
        }
    )

    attempt = 0
    try:
        while not stop_event.is_set():
            attempt += 1
            client: Optional[BingXPublicClient] = None
            try:
                client = BingXPublicClient()
                telegram = TelegramClient(client.session)
                engine = TradingEngine(client, telegram, StateStore(STATE_FILE))
                HEALTH.update(
                    {
                        "engine_state": "running",
                        "engine_error": None,
                        "engine_restart_attempt": attempt,
                        "http_backend": getattr(client, "http_backend", "unknown"),
                    }
                )

                async def bridge_stop() -> None:
                    await stop_event.wait()
                    engine.stop_event.set()

                bridge = asyncio.create_task(bridge_stop())
                try:
                    await engine.run()
                finally:
                    bridge.cancel()
                    await asyncio.gather(bridge, return_exceptions=True)

                if stop_event.is_set():
                    break
                raise RuntimeError("trading engine returned unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                HEALTH.update(
                    {
                        "engine_state": "degraded",
                        "engine_error": repr(error)[:500],
                        "engine_restart_attempt": attempt,
                    }
                )
                LOG.exception("Direct trading engine failed; retrying in 10s")
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    pass
            finally:
                if client is not None and not client.session.closed:
                    try:
                        await client.close()
                    except Exception:
                        LOG.exception("HTTP client close failed")
    finally:
        HEALTH["engine_state"] = "stopped"
        if health_server is not None:
            await asyncio.to_thread(health_server.shutdown)
            health_server.server_close()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
