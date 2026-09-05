from __future__ import annotations

# BingX Impulse Bot — single-file Render build.
# Start command: uvicorn bot:app --host 0.0.0.0 --port $PORT
# Default mode is PAPER. Real execution remains protected by the safety locks.

SINGLE_FILE_BUILD_VERSION = "0.1.1-relative-import-fix"


# ==================== config.py ====================
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

import yaml


SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z]+$")


@dataclass(slots=True, frozen=True)
class ApiSettings:
    ws_url: str = "wss://open-api-swap.bingx.com/swap-market"
    live_base_urls: tuple[str, ...] = (
        "https://open-api.bingx.com",
        "https://open-api.bingx.pro",
    )
    vst_base_urls: tuple[str, ...] = (
        "https://open-api-vst.bingx.com",
        "https://open-api-vst.bingx.pro",
    )
    recv_window_ms: int = 5000
    request_timeout_sec: int = 10
    source_key: str = "BX-AI-SKILL"


@dataclass(slots=True, frozen=True)
class UniverseSettings:
    symbols: tuple[str, ...] = (
        "BTC-USDT",
        "ETH-USDT",
        "SOL-USDT",
        "XRP-USDT",
    )
    dynamic: bool = True
    max_symbols: int = 12
    min_quote_volume_24h: float = 25_000_000.0
    min_listing_age_days: int = 30
    exclude_assets: tuple[str, ...] = ("USDC", "FDUSD")


@dataclass(slots=True, frozen=True)
class MarketSettings:
    depth_levels: int = 20
    depth_interval: str = "500ms"
    stale_after_ms: int = 1800
    trade_retention_sec: int = 120
    max_spread_bps: float = 6.0
    warmup_candles: int = 100
    day_history_candles: int = 1440


@dataclass(slots=True, frozen=True)
class StrategySettings:
    breakout_lookback: int = 20
    breakout_buffer_bps: float = 1.0
    ema_fast: int = 20
    ema_slow: int = 50
    atr_period: int = 14
    rsi_period: int = 14
    impulse_window_sec: int = 20
    flow_window_sec: int = 10
    baseline_window_sec: int = 60
    min_impulse_atr: float = 0.22
    max_impulse_atr: float = 1.35
    min_trend_atr: float = 0.04
    min_volume_ratio: float = 1.6
    min_flow_imbalance: float = 0.20
    min_book_imbalance: float = 0.08
    min_trade_count: int = 8
    max_opposing_wall_ratio: float = 10.0
    min_pullback_atr: float = 0.06
    max_pullback_atr: float = 0.38
    reacceleration_bps: float = 1.5
    reentry_flow_imbalance: float = 0.10
    reentry_book_imbalance: float = 0.03
    arm_lifetime_sec: int = 45
    invalidation_buffer_atr: float = 0.12
    stop_buffer_atr: float = 0.10
    min_stop_atr: float = 0.30
    max_stop_atr: float = 0.90
    target_rr: float = 1.8
    max_extension_atr: float = 1.6
    max_rsi_long: float = 86.0
    min_rsi_short: float = 14.0
    leader_symbol: str = "BTC-USDT"
    min_leader_correlation: float = 0.35
    max_adverse_leader_move_bps: float = 8.0
    cooldown_sec: int = 300
    min_atr_pct: float = 0.04
    high_atr_pct: float = 0.50
    max_atr_pct: float = 1.20
    high_volatility_risk_multiplier: float = 0.50
    mtf_ema_fast: int = 20
    mtf_ema_slow: int = 50
    min_mtf_trend_atr: float = 0.02
    min_oi_change_pct: float = 0.02
    max_abs_funding_rate: float = 0.0010
    max_spoof_score: float = 0.65
    min_wall_age_ms: int = 3000
    min_wall_confidence: float = 0.60
    cvd_window_sec: int = 60
    cvd_divergence_min_bps: float = 2.0
    footprint_window_sec: int = 20
    footprint_imbalance_ratio: float = 3.0
    footprint_min_level_share: float = 0.03
    footprint_zone_tolerance_atr: float = 0.18
    tape_fast_window_sec: int = 2
    tape_baseline_window_sec: int = 30
    min_tape_ticks_per_sec: float = 3.0
    min_tape_velocity_ratio: float = 1.8
    level_touch_tolerance_atr: float = 0.08
    max_reversal_touches: int = 2
    spread_baseline_window_sec: int = 30
    max_spread_expansion_ratio: float = 2.0
    min_spread_samples: int = 10
    sweep_min_bps: float = 2.0
    sweep_max_bps: float = 18.0
    sweep_return_window_sec: int = 30
    choch_buffer_bps: float = 1.0


@dataclass(slots=True, frozen=True)
class MLSettings:
    enabled: bool = True
    model_path: str = "data/models/setup_validator.joblib"
    min_probability: float = 0.70
    min_training_samples: int = 500
    min_validation_samples: int = 100
    min_validation_auc: float = 0.55
    max_model_age_days: int = 30
    required_in_live: bool = True
    training_modes: tuple[str, ...] = ("vst",)


@dataclass(slots=True, frozen=True)
class DerivativesSettings:
    enabled: bool = True
    refresh_interval_sec: int = 60
    history_window_sec: int = 600
    max_staleness_sec: int = 180


@dataclass(slots=True, frozen=True)
class NewsSettings:
    enabled: bool = True
    provider: str = "tradingeconomics"
    countries: tuple[str, ...] = ("United States", "Euro Area")
    min_importance: int = 3
    blackout_before_min: int = 15
    blackout_after_min: int = 15
    refresh_interval_sec: int = 300
    max_staleness_sec: int = 900
    fail_closed_live: bool = True


@dataclass(slots=True, frozen=True)
class RiskSettings:
    paper_initial_equity: float = 1000.0
    risk_per_trade_pct: float = 0.25
    hard_max_risk_per_trade_pct: float = 0.50
    daily_max_loss_pct: float = 1.50
    max_consecutive_losses: int = 4
    max_open_positions: int = 2
    max_total_notional_to_equity: float = 2.0
    max_single_notional_usdt: float = 1000.0


@dataclass(slots=True, frozen=True)
class ExecutionSettings:
    leverage: int = 5
    taker_fee_rate: float = 0.0005
    maker_fee_rate: float = 0.0002
    assumed_slippage_bps: float = 2.0
    max_signal_age_ms: int = 1800
    no_progress_exit_sec: int = 300
    no_progress_min_mfe_r: float = 0.45
    max_hold_sec: int = 900
    trailing_activation_r: float = 1.0
    trailing_distance_r: float = 0.55
    position_side: str = "BOTH"


@dataclass(slots=True, frozen=True)
class TelegramSettings:
    enabled: bool = True
    stats_times_utc: tuple[str, ...] = ("08:00", "20:00")
    heartbeat_interval_minutes: int = 120
    report_every_closed_trades: int = 25
    reports_dir: str = "data/reports"
    send_csv: bool = True
    request_timeout_sec: int = 10


@dataclass(slots=True, frozen=True)
class AppSettings:
    mode: str = "paper"
    log_level: str = "INFO"
    database_path: str = "data/impulse_bot.sqlite3"
    api: ApiSettings = field(default_factory=ApiSettings)
    universe: UniverseSettings = field(default_factory=UniverseSettings)
    market: MarketSettings = field(default_factory=MarketSettings)
    strategy: StrategySettings = field(default_factory=StrategySettings)
    risk: RiskSettings = field(default_factory=RiskSettings)
    execution: ExecutionSettings = field(default_factory=ExecutionSettings)
    telegram: TelegramSettings = field(default_factory=TelegramSettings)
    derivatives: DerivativesSettings = field(default_factory=DerivativesSettings)
    news: NewsSettings = field(default_factory=NewsSettings)
    ml: MLSettings = field(default_factory=MLSettings)


@dataclass(slots=True, frozen=True)
class Credentials:
    api_key: str
    secret_key: str

    @classmethod
    def from_env(cls) -> Credentials | None:
        key = os.getenv("BINGX_API_KEY", "").strip()
        secret = os.getenv("BINGX_SECRET_KEY", "").strip()
        if not key and not secret:
            return None
        if not key or not secret:
            raise ValueError("Both BINGX_API_KEY and BINGX_SECRET_KEY are required")
        return cls(api_key=key, secret_key=secret)


T = TypeVar("T")


def load_dotenv(path: str | Path = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _build(cls: type[T], values: dict[str, Any] | None, tuple_fields: tuple[str, ...] = ()) -> T:
    clean = dict(values or {})
    for name in tuple_fields:
        if name in clean:
            clean[name] = tuple(clean[name])
    return cls(**clean)


def load_settings(path: str | Path | None = None) -> AppSettings:
    load_dotenv()
    config_path = Path(path or os.getenv("BOT_CONFIG", "config.yaml"))
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found: {config_path}. Copy config.example.yaml to config.yaml."
        )
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    mode = os.getenv("BOT_MODE", str(raw.get("mode", "paper"))).strip().lower()
    db_path = Path(str(raw.get("database_path", "data/impulse_bot.sqlite3")))
    if not db_path.is_absolute():
        db_path = config_path.resolve().parent / db_path
    telegram_raw = dict(raw.get("telegram") or {})
    reports_path = Path(str(telegram_raw.get("reports_dir", "data/reports")))
    if not reports_path.is_absolute():
        reports_path = config_path.resolve().parent / reports_path
    telegram_raw["reports_dir"] = str(reports_path)
    ml_raw = dict(raw.get("ml") or {})
    model_path = Path(str(ml_raw.get("model_path", "data/models/setup_validator.joblib")))
    if not model_path.is_absolute():
        model_path = config_path.resolve().parent / model_path
    ml_raw["model_path"] = str(model_path)

    settings = AppSettings(
        mode=mode,
        log_level=str(raw.get("log_level", "INFO")).upper(),
        database_path=str(db_path),
        api=_build(
            ApiSettings,
            raw.get("api"),
            tuple_fields=("live_base_urls", "vst_base_urls"),
        ),
        universe=_build(
            UniverseSettings,
            raw.get("universe"),
            tuple_fields=("symbols", "exclude_assets"),
        ),
        market=_build(MarketSettings, raw.get("market")),
        strategy=_build(StrategySettings, raw.get("strategy")),
        risk=_build(RiskSettings, raw.get("risk")),
        execution=_build(ExecutionSettings, raw.get("execution")),
        telegram=_build(
            TelegramSettings,
            telegram_raw,
            tuple_fields=("stats_times_utc",),
        ),
        derivatives=_build(DerivativesSettings, raw.get("derivatives")),
        news=_build(NewsSettings, raw.get("news"), tuple_fields=("countries",)),
        ml=_build(MLSettings, ml_raw, tuple_fields=("training_modes",)),
    )
    validate_settings(settings)
    return settings


def validate_settings(cfg: AppSettings) -> None:
    if cfg.mode not in {"paper", "vst", "live"}:
        raise ValueError("mode must be paper, vst, or live")
    if not cfg.universe.symbols:
        raise ValueError("universe.symbols cannot be empty")
    for symbol in (*cfg.universe.symbols, cfg.strategy.leader_symbol):
        if len(symbol) > 20 or not SYMBOL_PATTERN.fullmatch(symbol):
            raise ValueError(f"Invalid BingX symbol: {symbol!r}")
    if not 1 <= cfg.api.recv_window_ms <= 5000:
        raise ValueError("api.recv_window_ms must be between 1 and 5000")
    if cfg.market.depth_levels not in {5, 10, 20, 50, 100}:
        raise ValueError("market.depth_levels must be 5, 10, 20, 50, or 100")
    if cfg.market.depth_interval not in {"200ms", "500ms"}:
        raise ValueError("market.depth_interval must be 200ms or 500ms")
    if not 100 <= cfg.market.day_history_candles <= 1440:
        raise ValueError("market.day_history_candles must be between 100 and 1440")
    if not 0 < cfg.risk.risk_per_trade_pct <= cfg.risk.hard_max_risk_per_trade_pct:
        raise ValueError("risk_per_trade_pct exceeds the configured hard limit")
    if cfg.risk.hard_max_risk_per_trade_pct > 0.50:
        raise ValueError("hard_max_risk_per_trade_pct cannot exceed 0.50%")
    if cfg.strategy.ema_fast >= cfg.strategy.ema_slow:
        raise ValueError("strategy.ema_fast must be less than ema_slow")
    if cfg.strategy.target_rr < 1.5:
        raise ValueError("strategy.target_rr must be at least 1.5")
    if not 0 < cfg.strategy.high_volatility_risk_multiplier <= 1:
        raise ValueError("strategy.high_volatility_risk_multiplier must be in (0, 1]")
    if not 0 < cfg.strategy.min_atr_pct < cfg.strategy.high_atr_pct < cfg.strategy.max_atr_pct:
        raise ValueError("ATR volatility thresholds must be strictly increasing")
    if cfg.strategy.footprint_imbalance_ratio < 3.0:
        raise ValueError("footprint_imbalance_ratio must be at least 3.0")
    if cfg.strategy.tape_fast_window_sec >= cfg.strategy.tape_baseline_window_sec:
        raise ValueError("tape_fast_window_sec must be below tape_baseline_window_sec")
    if cfg.strategy.min_spread_samples < 5:
        raise ValueError("min_spread_samples must be at least 5")
    if cfg.execution.position_side != "BOTH":
        raise ValueError("This version supports BingX one-way mode only (position_side=BOTH)")
    if cfg.execution.leverage < 1 or cfg.execution.leverage > 20:
        raise ValueError("execution.leverage must be between 1 and 20")
    if cfg.universe.max_symbols < 1 or cfg.universe.max_symbols > 30:
        raise ValueError("universe.max_symbols must be between 1 and 30")
    if cfg.telegram.heartbeat_interval_minutes < 15:
        raise ValueError("telegram.heartbeat_interval_minutes must be at least 15")
    if cfg.telegram.report_every_closed_trades < 1:
        raise ValueError("telegram.report_every_closed_trades must be positive")
    if len(cfg.telegram.stats_times_utc) != 2:
        raise ValueError("telegram.stats_times_utc must contain exactly two UTC times")
    for item in cfg.telegram.stats_times_utc:
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", item):
            raise ValueError(f"Invalid UTC report time: {item!r}")
    if cfg.derivatives.refresh_interval_sec < 30:
        raise ValueError("derivatives.refresh_interval_sec must be at least 30")
    if cfg.news.provider not in {"tradingeconomics"}:
        raise ValueError("news.provider must be tradingeconomics")
    if cfg.news.min_importance not in {1, 2, 3}:
        raise ValueError("news.min_importance must be 1, 2, or 3")
    if not 0.5 <= cfg.ml.min_probability < 1:
        raise ValueError("ml.min_probability must be in [0.5, 1)")
    if cfg.ml.min_training_samples < 100:
        raise ValueError("ml.min_training_samples must be at least 100")
    if not cfg.ml.training_modes or not set(cfg.ml.training_modes) <= {"paper", "vst", "live"}:
        raise ValueError("ml.training_modes contains an unsupported mode")

# ==================== models.py ====================
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def direction(self) -> int:
        return 1 if self is Side.LONG else -1

    @property
    def entry_order_side(self) -> str:
        return "BUY" if self is Side.LONG else "SELL"

    @property
    def exit_order_side(self) -> str:
        return "SELL" if self is Side.LONG else "BUY"


@dataclass(slots=True, frozen=True)
class Candle:
    open_time_ms: int
    close_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float = 0.0
    trade_count: int = 0
    taker_buy_quote_volume: float = 0.0

    def is_closed(self, now_ms: int) -> bool:
        return self.close_time_ms <= now_ms


@dataclass(slots=True, frozen=True)
class TradeTick:
    timestamp_ms: int
    price: float
    quantity: float
    buyer_is_maker: bool

    @property
    def quote_notional(self) -> float:
        return self.price * self.quantity

    @property
    def aggressor_direction(self) -> int:
        # buyer_is_maker=True means the seller crossed the spread.
        return -1 if self.buyer_is_maker else 1


@dataclass(slots=True, frozen=True)
class OrderBook:
    timestamp_ms: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    @property
    def mid(self) -> float:
        if self.best_bid <= 0 or self.best_ask <= 0:
            return 0.0
        return (self.best_bid + self.best_ask) / 2.0


@dataclass(slots=True, frozen=True)
class ContractSpec:
    symbol: str
    quantity_precision: int = 6
    price_precision: int = 8
    min_quantity: float = 0.0
    min_notional_usdt: float = 2.0
    maker_fee_rate: float = 0.0002
    taker_fee_rate: float = 0.0005
    max_long_leverage: int = 125
    max_short_leverage: int = 125


@dataclass(slots=True, frozen=True)
class BookMetrics:
    spread_bps: float
    imbalance: float
    microprice_bias_bps: float
    bid_wall_ratio: float
    ask_wall_ratio: float
    bid_wall_age_ms: int = 0
    ask_wall_age_ms: int = 0
    bid_wall_confidence: float = 0.0
    ask_wall_confidence: float = 0.0
    spoof_score: float = 0.0


@dataclass(slots=True, frozen=True)
class FlowMetrics:
    impulse_return: float
    flow_imbalance: float
    volume_ratio: float
    recent_trade_count: int
    recent_quote_volume: float


@dataclass(slots=True, frozen=True)
class FeatureVector:
    symbol: str
    timestamp_ms: int
    ready: bool
    bid: float = 0.0
    ask: float = 0.0
    mid: float = 0.0
    data_age_ms: int = 0
    spread_bps: float = 0.0
    atr: float = 0.0
    atr_pct: float = 0.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    trend_atr: float = 0.0
    breakout_high: float = 0.0
    breakout_low: float = 0.0
    impulse_atr: float = 0.0
    flow_imbalance: float = 0.0
    volume_ratio: float = 0.0
    recent_trade_count: int = 0
    recent_quote_volume: float = 0.0
    book_imbalance: float = 0.0
    microprice_bias_bps: float = 0.0
    bid_wall_ratio: float = 0.0
    ask_wall_ratio: float = 0.0
    rsi: float = 50.0
    extension_atr: float = 0.0
    mtf_trend_15m_atr: float = 0.0
    mtf_trend_1h_atr: float = 0.0
    mtf_ready: bool = False
    leader_return_bps: float = 0.0
    leader_correlation: float = 0.0
    open_interest_change_pct: float = 0.0
    funding_rate: float = 0.0
    derivatives_ready: bool = False
    derivatives_age_ms: int = 0
    bid_wall_age_ms: int = 0
    ask_wall_age_ms: int = 0
    bid_wall_confidence: float = 0.0
    ask_wall_confidence: float = 0.0
    spoof_score: float = 0.0
    cvd_quote: float = 0.0
    cvd_divergence: float = 0.0
    absorption_side: float = 0.0
    footprint_buy_ratio: float = 0.0
    footprint_sell_ratio: float = 0.0
    initiative_buy_zone: float = 0.0
    initiative_sell_zone: float = 0.0
    tape_ticks_per_sec: float = 0.0
    tape_velocity_ratio: float = 0.0
    micro_swing_high: float = 0.0
    micro_swing_low: float = 0.0
    day_high: float = 0.0
    day_low: float = 0.0
    hourly_swing_high: float = 0.0
    hourly_swing_low: float = 0.0
    day_high_touch_count: int = 0
    day_low_touch_count: int = 0
    hourly_high_touch_count: int = 0
    hourly_low_touch_count: int = 0
    high_touch_count: int = 0
    low_touch_count: int = 0
    spread_baseline_bps: float = 0.0
    spread_expansion_ratio: float = 1.0
    spread_baseline_ready: bool = False
    news_blocked: bool = False
    news_reason: str = ""
    reject_reason: str = ""


@dataclass(slots=True, frozen=True)
class Signal:
    signal_id: str
    symbol: str
    side: Side
    created_at_ms: int
    expires_at_ms: int
    entry_price: float
    stop_price: float
    target_price: float
    target_rr: float
    strength: float
    breakout_level: float
    features: FeatureVector
    reasons: tuple[str, ...] = ()
    setup_type: str = "impulse_continuation"

    @property
    def price_risk(self) -> float:
        return abs(self.entry_price - self.stop_price)


@dataclass(slots=True, frozen=True)
class SizingResult:
    accepted: bool
    quantity: float = 0.0
    notional_usdt: float = 0.0
    risk_cash: float = 0.0
    estimated_worst_loss: float = 0.0
    reason: str = ""


@dataclass(slots=True)
class Position:
    position_id: str
    signal_id: str
    symbol: str
    side: Side
    mode: str
    quantity: float
    entry_price: float
    stop_price: float
    target_price: float
    opened_at_ms: int
    initial_risk_cash: float
    entry_fee: float
    exchange_order_id: str = ""
    trailing_stop: float | None = None
    highest_price: float = 0.0
    lowest_price: float = 0.0
    mfe_r: float = 0.0
    mae_r: float = 0.0
    realized_pnl: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def direction(self) -> int:
        return self.side.direction

    @property
    def unit_risk(self) -> float:
        return abs(self.entry_price - self.stop_price)


@dataclass(slots=True, frozen=True)
class BrokerEvent:
    kind: str
    timestamp_ms: int
    symbol: str
    position_id: str
    pnl: float = 0.0
    r_multiple: float = 0.0
    price: float = 0.0
    reason: str = ""
    payload: dict[str, Any] = field(default_factory=dict)

# ==================== indicators.py ====================
import math
from collections.abc import Sequence



def ema(values: Sequence[float], period: int) -> float:
    if period <= 0 or len(values) < period:
        return math.nan
    alpha = 2.0 / (period + 1.0)
    value = sum(values[:period]) / period
    for item in values[period:]:
        value = alpha * item + (1.0 - alpha) * value
    return value


def atr(candles: Sequence[Candle], period: int) -> float:
    if period <= 0 or len(candles) < period + 1:
        return math.nan
    true_ranges: list[float] = []
    for previous, current in zip(candles, candles[1:]):
        true_ranges.append(
            max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )
        )
    seed = true_ranges[:period]
    if len(seed) < period:
        return math.nan
    value = sum(seed) / period
    for tr in true_ranges[period:]:
        value = ((period - 1) * value + tr) / period
    return value


def rsi(values: Sequence[float], period: int) -> float:
    if period <= 0 or len(values) < period + 1:
        return math.nan
    changes = [current - previous for previous, current in zip(values, values[1:])]
    gains = [max(change, 0.0) for change in changes]
    losses = [max(-change, 0.0) for change in changes]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = ((period - 1) * avg_gain + gain) / period
        avg_loss = ((period - 1) * avg_loss + loss) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    relative_strength = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + relative_strength)


def pearson_correlation(left: Sequence[float], right: Sequence[float]) -> float:
    size = min(len(left), len(right))
    if size < 5:
        return 0.0
    x = left[-size:]
    y = right[-size:]
    mean_x = sum(x) / size
    mean_y = sum(y) / size
    covariance = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y))
    variance_x = sum((a - mean_x) ** 2 for a in x)
    variance_y = sum((b - mean_y) ** 2 for b in y)
    denominator = math.sqrt(variance_x * variance_y)
    return covariance / denominator if denominator > 0 else 0.0


def returns(values: Sequence[float]) -> list[float]:
    result: list[float] = []
    for previous, current in zip(values, values[1:]):
        if previous > 0:
            result.append(current / previous - 1.0)
    return result


def finite(value: float) -> bool:
    return math.isfinite(value)

# ==================== microstructure.py ====================
import math
from dataclasses import dataclass
from typing import Sequence



@dataclass(slots=True, frozen=True)
class MicrostructureMetrics:
    cvd_quote: float = 0.0
    cvd_divergence: float = 0.0
    absorption_side: float = 0.0
    footprint_buy_ratio: float = 0.0
    footprint_sell_ratio: float = 0.0
    initiative_buy_zone: float = 0.0
    initiative_sell_zone: float = 0.0
    tape_ticks_per_sec: float = 0.0
    tape_velocity_ratio: float = 0.0
    micro_swing_high: float = 0.0
    micro_swing_low: float = 0.0


def analyze_microstructure(
    trades: Sequence[TradeTick],
    *,
    now_ms: int,
    price: float,
    atr: float,
    cfg: StrategySettings,
) -> MicrostructureMetrics:
    if not trades or price <= 0:
        return MicrostructureMetrics()
    cvd_cutoff = now_ms - cfg.cvd_window_sec * 1000
    cvd_ticks = [tick for tick in trades if tick.timestamp_ms >= cvd_cutoff]
    cvd = sum(tick.quote_notional * tick.aggressor_direction for tick in cvd_ticks)
    divergence = _cvd_divergence(cvd_ticks, cfg.cvd_divergence_min_bps)
    absorption = _absorption(cvd_ticks, cfg.cvd_divergence_min_bps)

    footprint_cutoff = now_ms - cfg.footprint_window_sec * 1000
    footprint_ticks = [tick for tick in trades if tick.timestamp_ms >= footprint_cutoff]
    buy_ratio, sell_ratio, buy_zone, sell_zone = _footprint(
        footprint_ticks,
        price=price,
        atr=atr,
        imbalance_ratio=cfg.footprint_imbalance_ratio,
        min_share=cfg.footprint_min_level_share,
    )

    fast_cutoff = now_ms - cfg.tape_fast_window_sec * 1000
    base_cutoff = now_ms - cfg.tape_baseline_window_sec * 1000
    fast_count = sum(tick.timestamp_ms >= fast_cutoff for tick in trades)
    baseline_count = sum(base_cutoff <= tick.timestamp_ms < fast_cutoff for tick in trades)
    fast_tps = fast_count / max(cfg.tape_fast_window_sec, 1)
    baseline_seconds = max(cfg.tape_baseline_window_sec - cfg.tape_fast_window_sec, 1)
    baseline_tps = baseline_count / baseline_seconds
    velocity_ratio = fast_tps / baseline_tps if baseline_tps > 0 else 0.0

    # The level is frozen from trades that precede the current one-second trigger.
    structure_ticks = [
        tick
        for tick in trades
        if now_ms - 12_000 <= tick.timestamp_ms <= now_ms - 1_000
    ]
    return MicrostructureMetrics(
        cvd_quote=cvd,
        cvd_divergence=divergence,
        absorption_side=absorption,
        footprint_buy_ratio=buy_ratio,
        footprint_sell_ratio=sell_ratio,
        initiative_buy_zone=buy_zone,
        initiative_sell_zone=sell_zone,
        tape_ticks_per_sec=fast_tps,
        tape_velocity_ratio=velocity_ratio,
        micro_swing_high=max((tick.price for tick in structure_ticks), default=price),
        micro_swing_low=min((tick.price for tick in structure_ticks), default=price),
    )


def _cvd_divergence(trades: Sequence[TradeTick], min_price_bps: float) -> float:
    if len(trades) < 10:
        return 0.0
    midpoint = len(trades) // 2
    first, second = trades[:midpoint], trades[midpoint:]
    first_delta = sum(t.quote_notional * t.aggressor_direction for t in first)
    second_delta = sum(t.quote_notional * t.aggressor_direction for t in second)
    first_low = min(t.price for t in first)
    second_low = min(t.price for t in second)
    first_high = max(t.price for t in first)
    second_high = max(t.price for t in second)
    down_break_bps = (first_low - second_low) / first_low * 10_000.0 if first_low > 0 else 0.0
    up_break_bps = (second_high - first_high) / first_high * 10_000.0 if first_high > 0 else 0.0
    scale = max(sum(t.quote_notional for t in trades), 1e-9)
    delta_improvement = (second_delta - first_delta) / scale
    if down_break_bps >= min_price_bps and delta_improvement > 0.03:
        return min(delta_improvement * 5.0, 1.0)  # Bullish divergence.
    if up_break_bps >= min_price_bps and delta_improvement < -0.03:
        return max(delta_improvement * 5.0, -1.0)  # Bearish divergence.
    return 0.0


def _absorption(trades: Sequence[TradeTick], min_price_bps: float) -> float:
    if len(trades) < 8 or trades[0].price <= 0:
        return 0.0
    total = sum(t.quote_notional for t in trades)
    delta = sum(t.quote_notional * t.aggressor_direction for t in trades)
    normalized_delta = delta / max(total, 1e-9)
    price_bps = (trades[-1].price / trades[0].price - 1.0) * 10_000.0
    # Strong aggressive sells without downside progress imply passive bid absorption.
    if normalized_delta <= -0.25 and price_bps > -min_price_bps:
        return min(abs(normalized_delta), 1.0)
    # Strong aggressive buys without upside progress imply passive ask absorption.
    if normalized_delta >= 0.25 and price_bps < min_price_bps:
        return -min(normalized_delta, 1.0)
    return 0.0


def _footprint(
    trades: Sequence[TradeTick],
    *,
    price: float,
    atr: float,
    imbalance_ratio: float,
    min_share: float,
) -> tuple[float, float, float, float]:
    if not trades:
        return 0.0, 0.0, 0.0, 0.0
    bin_size = max(atr * 0.02 if math.isfinite(atr) else 0.0, price * 0.00001, 1e-12)
    levels: dict[int, list[float]] = {}
    for tick in trades:
        key = round(tick.price / bin_size)
        side = 0 if tick.aggressor_direction > 0 else 1
        levels.setdefault(key, [0.0, 0.0])[side] += tick.quote_notional
    total = sum(sum(values) for values in levels.values())
    minimum = total * min_share
    strongest_buy = (0.0, 0.0)
    strongest_sell = (0.0, 0.0)
    for key, (buy, sell) in levels.items():
        if buy + sell < minimum:
            continue
        buy_ratio = buy / max(sell, 1e-9)
        sell_ratio = sell / max(buy, 1e-9)
        if buy_ratio >= imbalance_ratio and buy_ratio > strongest_buy[0]:
            strongest_buy = (min(buy_ratio, 99.0), key * bin_size)
        if sell_ratio >= imbalance_ratio and sell_ratio > strongest_sell[0]:
            strongest_sell = (min(sell_ratio, 99.0), key * bin_size)
    return strongest_buy[0], strongest_sell[0], strongest_buy[1], strongest_sell[1]

# ==================== bingx.py ====================
import asyncio
import hashlib
import hmac
import json
import math
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from typing import Any



FORBIDDEN_PARAM_CHARS = re.compile(r"[&=?#\r\n]")


class BingXApiError(RuntimeError):
    def __init__(self, code: int | str, message: str, payload: Any = None) -> None:
        super().__init__(f"BingX error {code}: {message}")
        self.code = code
        self.message = message
        self.payload = payload


class BingXNetworkError(RuntimeError):
    pass


def _serialize(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("API parameters cannot contain NaN or infinity")
        fixed = format(value, ".15f").rstrip("0").rstrip(".")
        return "0" if fixed in {"", "-0"} else fixed
    return str(value)


def _canonical(params: Mapping[str, Any]) -> tuple[str, dict[str, str]]:
    serialized: dict[str, str] = {}
    for key, value in params.items():
        text = _serialize(value)
        if not isinstance(value, (dict, list, tuple)) and FORBIDDEN_PARAM_CHARS.search(text):
            raise ValueError(f"Unsafe character in API parameter {key!r}")
        serialized[key] = text
    raw = "&".join(f"{key}={serialized[key]}" for key in sorted(serialized))
    return raw, serialized


def _transmitted_query(serialized: Mapping[str, str], signature: str) -> str:
    parts: list[str] = []
    for key in sorted(serialized):
        value = serialized[key]
        if "[" in value or "{" in value:
            value = urllib.parse.quote(value, safe="")
        parts.append(f"{key}={value}")
    parts.append(f"signature={signature}")
    return "&".join(parts)


class BingXRestClient:
    """BingX REST client: public calls work keyless; private calls are signed."""

    def __init__(
        self,
        api: ApiSettings,
        credentials: Credentials | None,
        base_urls: Sequence[str],
    ) -> None:
        self.api = api
        self.credentials = credentials
        self.base_urls = tuple(base_urls)
        self.clock_offset_ms = 0

    async def request(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        authenticated: bool = False,
    ) -> Any:
        if authenticated and self.credentials is None:
            raise BingXApiError("AUTH", "API credentials are required for this endpoint")
        return await asyncio.to_thread(
            self._request_sync, method, path, params or {}, authenticated
        )

    def _request_sync(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any],
        authenticated: bool,
    ) -> Any:
        method = method.upper()
        all_params = dict(params)
        all_params.setdefault("recvWindow", self.api.recv_window_ms)
        all_params["timestamp"] = int(time.time() * 1000) + self.clock_offset_ms
        raw, serialized = _canonical(all_params)
        signature = ""
        if authenticated:
            assert self.credentials is not None
            signature = hmac.new(
                self.credentials.secret_key.encode("utf-8"),
                raw.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
        query = (
            _transmitted_query(serialized, signature)
            if authenticated
            else urllib.parse.urlencode(serialized)
        )
        headers = {
            "X-SOURCE-KEY": self.api.source_key,
            "Accept": "application/json",
            "User-Agent": "bingx-impulse-bot/0.1",
        }
        if authenticated:
            assert self.credentials is not None
            headers["X-BX-APIKEY"] = self.credentials.api_key
        last_error: BaseException | None = None

        for base_url in self.base_urls:
            url = f"{base_url.rstrip('/')}{path}"
            body: bytes | None = None
            if method == "POST":
                body = query.encode("utf-8")
                headers["Content-Type"] = "application/x-www-form-urlencoded"
            else:
                url = f"{url}?{query}"
            request = urllib.request.Request(url, data=body, method=method, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=self.api.request_timeout_sec) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                raw_error = exc.read().decode("utf-8", errors="replace")
                try:
                    payload = json.loads(raw_error)
                except json.JSONDecodeError:
                    payload = {"code": exc.code, "msg": raw_error[:500]}
                raise BingXApiError(payload.get("code", exc.code), payload.get("msg", raw_error), payload) from exc
            except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
                last_error = exc
                continue

            code = payload.get("code", -1)
            if str(code) != "0":
                raise BingXApiError(code, str(payload.get("msg", "unknown error")), payload)
            return payload.get("data")

        raise BingXNetworkError(f"All BingX API domains failed: {last_error}")

    async def get_contracts(self) -> list[dict[str, Any]]:
        data = await self.request("GET", "/openApi/swap/v2/quote/contracts")
        if isinstance(data, dict):
            data = data.get("contracts", data.get("data", []))
        return list(data or [])

    async def get_tickers(self) -> list[dict[str, Any]]:
        data = await self.request("GET", "/openApi/swap/v2/quote/ticker")
        return list(data if isinstance(data, list) else [data]) if data else []

    async def get_klines(self, symbol: str, interval: str, limit: int = 100) -> list[Any]:
        data = await self.request(
            "GET",
            "/openApi/swap/v3/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        return list(data or [])

    async def get_open_interest(self, symbol: str) -> dict[str, Any]:
        data = await self.request(
            "GET",
            "/openApi/swap/v2/quote/openInterest",
            {"symbol": symbol},
        )
        return dict(data or {})

    async def get_premium_index(self, symbol: str) -> dict[str, Any]:
        data = await self.request(
            "GET",
            "/openApi/swap/v2/quote/premiumIndex",
            {"symbol": symbol},
        )
        if isinstance(data, list):
            return dict(data[0]) if data else {}
        return dict(data or {})

    async def get_balance(self) -> list[dict[str, Any]]:
        data = await self.request(
            "GET", "/openApi/swap/v3/user/balance", authenticated=True
        )
        if isinstance(data, dict):
            data = data.get("balance", data.get("balances", [data]))
        return list(data or [])

    async def get_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol} if symbol else {}
        data = await self.request(
            "GET", "/openApi/swap/v2/user/positions", params, authenticated=True
        )
        if isinstance(data, dict):
            data = data.get("positions", [data])
        return list(data or [])

    async def get_commission(self) -> dict[str, Any]:
        data = await self.request(
            "GET", "/openApi/swap/v2/user/commissionRate", authenticated=True
        )
        if isinstance(data, dict) and isinstance(data.get("commission"), dict):
            return dict(data["commission"])
        return dict(data or {})

    async def get_income(
        self,
        *,
        symbol: str | None = None,
        income_type: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        if income_type:
            params["incomeType"] = income_type
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        data = await self.request(
            "GET", "/openApi/swap/v2/user/income", params, authenticated=True
        )
        return list(data or [])

    async def get_fill_history(
        self, symbol: str, start_time: int, end_time: int
    ) -> list[dict[str, Any]]:
        data = await self.request(
            "GET",
            "/openApi/swap/v2/trade/allFillOrders",
            {
                "tradingUnit": "COIN",
                "currency": "USDT",
                "startTs": start_time,
                "endTs": end_time,
            },
            authenticated=True,
        )
        if isinstance(data, dict):
            data = data.get("fill_orders", data.get("fills", []))
        return [dict(row) for row in (data or []) if str(row.get("symbol", symbol)) == symbol]

    async def place_order(self, params: Mapping[str, Any]) -> dict[str, Any]:
        data = await self.request(
            "POST", "/openApi/swap/v2/trade/order", params, authenticated=True
        )
        if isinstance(data, dict) and isinstance(data.get("order"), dict):
            return dict(data["order"])
        return dict(data or {})

    async def query_order(
        self, symbol: str, *, order_id: str | None = None, client_order_id: str | None = None
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["clientOrderId"] = client_order_id
        else:
            raise ValueError("order_id or client_order_id is required")
        data = await self.request(
            "GET", "/openApi/swap/v2/trade/order", params, authenticated=True
        )
        if isinstance(data, dict) and isinstance(data.get("order"), dict):
            return dict(data["order"])
        return dict(data or {})

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        await self.request(
            "POST",
            "/openApi/swap/v2/trade/leverage",
            {"symbol": symbol, "side": "BOTH", "leverage": leverage},
            authenticated=True,
        )

    async def get_margin_type(self, symbol: str) -> str:
        data = await self.request(
            "GET",
            "/openApi/swap/v2/trade/marginType",
            {"symbol": symbol},
            authenticated=True,
        )
        return str((data or {}).get("marginType", ""))

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> None:
        await self.request(
            "POST",
            "/openApi/swap/v2/trade/marginType",
            {"symbol": symbol, "marginType": margin_type},
            authenticated=True,
        )

    async def get_position_mode(self) -> bool:
        data = await self.request(
            "GET", "/openApi/swap/v1/positionSide/dual", authenticated=True
        )
        value = (data or {}).get("dualSidePosition", False)
        return value if isinstance(value, bool) else str(value).lower() == "true"

    async def get_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        data = await self.request(
            "GET",
            "/openApi/swap/v2/trade/openOrders",
            {"symbol": symbol},
            authenticated=True,
        )
        if isinstance(data, dict):
            data = data.get("orders", data.get("openOrders", []))
        return list(data or [])

    async def cancel_all_after(self, timeout_sec: int) -> dict[str, Any]:
        return dict(
            await self.request(
                "POST",
                "/openApi/swap/v2/trade/cancelAllAfter",
                {"type": "ACTIVATE", "timeOut": timeout_sec},
                authenticated=True,
            )
            or {}
        )

    async def cancel_all_open_orders(self, symbol: str) -> dict[str, Any]:
        return dict(
            await self.request(
                "DELETE",
                "/openApi/swap/v2/trade/allOpenOrders",
                {"symbol": symbol},
                authenticated=True,
            )
            or {}
        )

    async def close_market(self, symbol: str, side: str, quantity: float) -> dict[str, Any]:
        return await self.place_order(
            {
                "symbol": symbol,
                "side": side,
                "positionSide": "BOTH",
                "type": "MARKET",
                "quantity": quantity,
                "reduceOnly": True,
            }
        )

# ==================== universe.py ====================
import asyncio
import math
import time
from dataclasses import dataclass
from typing import Any



LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR", "2L", "2S", "3L", "3S", "5L", "5S")


@dataclass(slots=True, frozen=True)
class UniverseSelection:
    symbols: tuple[str, ...]
    specs: dict[str, ContractSpec]
    source: str


class UniverseSelector:
    def __init__(self, settings: UniverseSettings, client: BingXRestClient | None) -> None:
        self.settings = settings
        self.client = client

    async def select(self) -> UniverseSelection:
        if not self.settings.dynamic or self.client is None:
            return self._static("configured")
        try:
            contracts = await self.client.get_contracts()
            await asyncio.sleep(1.05)
            tickers = await self.client.get_tickers()
            return self._dynamic(contracts, tickers)
        except Exception:
            return self._static("configured-fallback")

    def _static(self, source: str) -> UniverseSelection:
        symbols = tuple(dict.fromkeys(self.settings.symbols))[: self.settings.max_symbols]
        specs = {symbol: ContractSpec(symbol=symbol) for symbol in symbols}
        return UniverseSelection(symbols=symbols, specs=specs, source=source)

    def _dynamic(
        self, contracts: list[dict[str, Any]], tickers: list[dict[str, Any]]
    ) -> UniverseSelection:
        now_ms = int(time.time() * 1000)
        min_age_ms = self.settings.min_listing_age_days * 86_400_000
        tickers_by_symbol = {str(item.get("symbol", "")): item for item in tickers}
        specs: dict[str, ContractSpec] = {}
        ranked: list[tuple[float, str]] = []

        for item in contracts:
            symbol = str(item.get("symbol", ""))
            if not symbol.endswith("-USDT") or symbol not in tickers_by_symbol:
                continue
            asset = str(item.get("asset") or symbol.split("-", 1)[0]).upper()
            if asset in self.settings.exclude_assets or asset.endswith(LEVERAGED_SUFFIXES):
                continue
            if int(item.get("status", 0) or 0) != 1:
                continue
            if str(item.get("apiStateOpen", "true")).lower() != "true":
                continue
            launch_time = int(item.get("launchTime", 0) or 0)
            if launch_time and now_ms - launch_time < min_age_ms:
                continue

            ticker = tickers_by_symbol[symbol]
            quote_volume = float(ticker.get("quoteVolume", 0.0) or 0.0)
            if quote_volume < self.settings.min_quote_volume_24h:
                continue
            bid = float(ticker.get("bidPrice", 0.0) or 0.0)
            ask = float(ticker.get("askPrice", 0.0) or 0.0)
            if bid <= 0 or ask <= bid:
                continue
            spread_bps = (ask - bid) / ((ask + bid) / 2.0) * 10_000.0
            if spread_bps > 8.0:
                continue
            change_pct = abs(float(ticker.get("priceChangePercent", 0.0) or 0.0))
            activity_score = math.log10(max(quote_volume, 1.0)) + min(change_pct, 20.0) / 20.0
            ranked.append((activity_score, symbol))
            specs[symbol] = ContractSpec(
                symbol=symbol,
                quantity_precision=int(item.get("quantityPrecision", 6) or 6),
                price_precision=int(item.get("pricePrecision", 8) or 8),
                min_quantity=float(item.get("tradeMinQuantity", 0.0) or 0.0),
                min_notional_usdt=float(item.get("tradeMinUSDT", 2.0) or 2.0),
                maker_fee_rate=float(item.get("makerFeeRate", 0.0002) or 0.0002),
                taker_fee_rate=float(item.get("takerFeeRate", 0.0005) or 0.0005),
                max_long_leverage=int(item.get("maxLongLeverage", 1) or 1),
                max_short_leverage=int(item.get("maxShortLeverage", 1) or 1),
            )

        ranked.sort(reverse=True)
        chosen = [symbol for _, symbol in ranked[: self.settings.max_symbols]]
        for core in self.settings.symbols[:2]:
            if core in specs and core not in chosen:
                if len(chosen) >= self.settings.max_symbols:
                    chosen.pop()
                chosen.append(core)
        if not chosen:
            return self._static("configured-empty-dynamic-fallback")
        return UniverseSelection(
            symbols=tuple(chosen),
            specs={symbol: specs[symbol] for symbol in chosen},
            source="dynamic-bingx",
        )

# ==================== journal.py ====================
import csv
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any



@dataclass(slots=True, frozen=True)
class PerformanceStats:
    period: str
    trades: int
    wins: int
    losses: int
    breakeven: int
    win_rate_pct: float
    net_pnl: float
    gross_profit: float
    gross_loss: float
    profit_factor: float | None
    average_r: float
    max_drawdown: float


class TradeJournal:
    def __init__(self, database_path: str) -> None:
        path = Path(database_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self._create_schema()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;

            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY,
                created_at_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_price REAL NOT NULL,
                target_price REAL NOT NULL,
                target_rr REAL NOT NULL,
                strength REAL NOT NULL,
                setup_type TEXT NOT NULL DEFAULT 'impulse_continuation',
                status TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                features_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id TEXT NOT NULL UNIQUE,
                signal_id TEXT NOT NULL,
                mode TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                entry_price REAL NOT NULL,
                stop_price REAL NOT NULL,
                target_price REAL NOT NULL,
                opened_at_ms INTEGER NOT NULL,
                initial_risk_cash REAL NOT NULL DEFAULT 0,
                closed_at_ms INTEGER,
                exit_price REAL,
                exit_reason TEXT NOT NULL DEFAULT '',
                gross_pnl REAL NOT NULL DEFAULT 0,
                fees REAL NOT NULL DEFAULT 0,
                net_pnl REAL NOT NULL DEFAULT 0,
                r_multiple REAL NOT NULL DEFAULT 0,
                entry_order_id TEXT NOT NULL DEFAULT '',
                exit_order_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_trades_closed_at ON trades(closed_at_ms);
            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);

            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self._ensure_column("signals", "setup_type", "TEXT NOT NULL DEFAULT 'impulse_continuation'")
        self._ensure_column("trades", "initial_risk_cash", "REAL NOT NULL DEFAULT 0")
        self.connection.commit()

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        columns = {str(row["name"]) for row in self.connection.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def record_signal(self, signal: Signal, status: str = "accepted", reason: str = "") -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO signals(
                signal_id, created_at_ms, symbol, side, entry_price, stop_price,
                target_price, target_rr, strength, setup_type, status, reason, features_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal.signal_id,
                signal.created_at_ms,
                signal.symbol,
                signal.side.value,
                signal.entry_price,
                signal.stop_price,
                signal.target_price,
                signal.target_rr,
                signal.strength,
                signal.setup_type,
                status,
                reason,
                json.dumps(asdict(signal.features), ensure_ascii=False, separators=(",", ":")),
            ),
        )
        self.connection.commit()

    def update_signal(self, signal_id: str, status: str, reason: str = "") -> None:
        self.connection.execute(
            "UPDATE signals SET status = ?, reason = ? WHERE signal_id = ?",
            (status, reason, signal_id),
        )
        self.connection.commit()

    def record_open(self, position: Position) -> None:
        self.connection.execute(
            """
            INSERT INTO trades(
                position_id, signal_id, mode, symbol, side, quantity, entry_price,
                stop_price, target_price, opened_at_ms, fees, entry_order_id,
                initial_risk_cash, status, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
            """,
            (
                position.position_id,
                position.signal_id,
                position.mode,
                position.symbol,
                position.side.value,
                position.quantity,
                position.entry_price,
                position.stop_price,
                position.target_price,
                position.opened_at_ms,
                position.entry_fee,
                position.exchange_order_id,
                position.initial_risk_cash,
                json.dumps(position.metadata, ensure_ascii=False, separators=(",", ":")),
            ),
        )
        self.connection.commit()

    def record_close(self, event: BrokerEvent) -> None:
        gross = float(event.payload.get("gross_pnl", event.pnl))
        fees = float(event.payload.get("total_fees", max(gross - event.pnl, 0.0)))
        exit_order_id = str(event.payload.get("exit_order_id", ""))
        metadata = json.dumps(event.payload, ensure_ascii=False, separators=(",", ":"))
        self.connection.execute(
            """
            UPDATE trades
            SET closed_at_ms = ?, exit_price = ?, exit_reason = ?, gross_pnl = ?,
                fees = ?, net_pnl = ?, r_multiple = ?, exit_order_id = ?,
                status = 'CLOSED', metadata_json = ?
            WHERE position_id = ?
            """,
            (
                event.timestamp_ms,
                event.price,
                event.reason,
                gross,
                fees,
                event.pnl,
                event.r_multiple,
                exit_order_id,
                metadata,
                event.position_id,
            ),
        )
        self.connection.commit()

    def open_rows(self) -> list[sqlite3.Row]:
        return list(self.connection.execute("SELECT * FROM trades WHERE status = 'OPEN' ORDER BY id"))

    def restore_positions(self) -> list[Position]:

        restored: list[Position] = []
        for row in self.open_rows():
            restored.append(
                Position(
                    position_id=str(row["position_id"]),
                    signal_id=str(row["signal_id"]),
                    symbol=str(row["symbol"]),
                    side=Side(str(row["side"])),
                    mode=str(row["mode"]),
                    quantity=float(row["quantity"]),
                    entry_price=float(row["entry_price"]),
                    stop_price=float(row["stop_price"]),
                    target_price=float(row["target_price"]),
                    opened_at_ms=int(row["opened_at_ms"]),
                    initial_risk_cash=float(row["initial_risk_cash"]),
                    entry_fee=float(row["fees"]),
                    exchange_order_id=str(row["entry_order_id"]),
                    highest_price=float(row["entry_price"]),
                    lowest_price=float(row["entry_price"]),
                    metadata=json.loads(str(row["metadata_json"] or "{}")),
                )
            )
        return restored

    def closed_count(self, mode: str | None = None) -> int:
        sql = "SELECT COUNT(*) AS count FROM trades WHERE status = 'CLOSED'"
        params: tuple[Any, ...] = ()
        if mode:
            sql += " AND mode = ?"
            params = (mode,)
        row = self.connection.execute(sql, params).fetchone()
        return int(row["count"] if row else 0)

    def export_due(self, every: int, reports_dir: str, mode: str | None = None) -> list[Path]:
        report_path = Path(reports_dir)
        report_path.mkdir(parents=True, exist_ok=True)
        completed_batches = self.closed_count(mode) // every
        meta_key = f"last_csv_batch:{mode or 'all'}"
        last_batch = int(self.get_meta(meta_key, "0"))
        files: list[Path] = []
        for batch in range(last_batch + 1, completed_batches + 1):
            start = (batch - 1) * every
            mode_filter = " AND mode = ?" if mode else ""
            params: tuple[Any, ...] = (mode, every, start) if mode else (every, start)
            rows = list(
                self.connection.execute(
                    f"""
                    SELECT id, position_id, signal_id, mode, symbol, side, quantity,
                           entry_price, stop_price, target_price, opened_at_ms,
                           closed_at_ms, exit_price, exit_reason, gross_pnl, fees,
                           net_pnl, r_multiple, entry_order_id, exit_order_id
                    FROM trades WHERE status = 'CLOSED'{mode_filter}
                    ORDER BY closed_at_ms, id LIMIT ? OFFSET ?
                    """,
                    params,
                )
            )
            if len(rows) != every:
                break
            first_number, last_number = start + 1, start + every
            prefix = f"trades_{mode}_" if mode else "trades_"
            output = report_path / f"{prefix}{first_number:05d}-{last_number:05d}.csv"
            temp = output.with_suffix(".csv.tmp")
            with temp.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow((*rows[0].keys(), "opened_at_utc", "closed_at_utc"))
                for row in rows:
                    values = list(row)
                    opened = self._iso_utc(int(row["opened_at_ms"]))
                    closed = self._iso_utc(int(row["closed_at_ms"]))
                    writer.writerow((*values, opened, closed))
            temp.replace(output)
            self.set_meta(meta_key, str(batch))
            files.append(output)
        return files

    @staticmethod
    def _iso_utc(timestamp_ms: int) -> str:
        return datetime.fromtimestamp(timestamp_ms / 1000.0, UTC).isoformat()

    def stats(
        self, *, since_ms: int | None = None, period: str = "all", mode: str | None = None
    ) -> PerformanceStats:
        sql = "SELECT net_pnl, r_multiple FROM trades WHERE status = 'CLOSED'"
        values: list[Any] = []
        if mode:
            sql += " AND mode = ?"
            values.append(mode)
        if since_ms is not None:
            sql += " AND closed_at_ms >= ?"
            values.append(since_ms)
        sql += " ORDER BY closed_at_ms, id"
        rows = list(self.connection.execute(sql, tuple(values)))
        pnls = [float(row["net_pnl"]) for row in rows]
        multiples = [float(row["r_multiple"]) for row in rows]
        wins = sum(item > 0 for item in pnls)
        losses = sum(item < 0 for item in pnls)
        gross_profit = sum(item for item in pnls if item > 0)
        gross_loss = -sum(item for item in pnls if item < 0)
        equity_curve = 0.0
        peak = 0.0
        max_drawdown = 0.0
        for pnl in pnls:
            equity_curve += pnl
            peak = max(peak, equity_curve)
            max_drawdown = max(max_drawdown, peak - equity_curve)
        return PerformanceStats(
            period=period,
            trades=len(rows),
            wins=wins,
            losses=losses,
            breakeven=len(rows) - wins - losses,
            win_rate_pct=wins / len(rows) * 100.0 if rows else 0.0,
            net_pnl=sum(pnls),
            gross_profit=gross_profit,
            gross_loss=gross_loss,
            profit_factor=gross_profit / gross_loss if gross_loss > 0 else None,
            average_r=sum(multiples) / len(multiples) if multiples else 0.0,
            max_drawdown=max_drawdown,
        )

    def today_stats(self, now_ms: int | None = None, mode: str | None = None) -> PerformanceStats:
        now = datetime.fromtimestamp(now_ms / 1000.0, UTC) if now_ms else datetime.now(UTC)
        start = datetime(now.year, now.month, now.day, tzinfo=UTC)
        return self.stats(
            since_ms=int(start.timestamp() * 1000),
            period=now.strftime("%Y-%m-%d UTC"),
            mode=mode,
        )

    def recent_consecutive_losses(self, now_ms: int | None = None, mode: str | None = None) -> int:
        now = datetime.fromtimestamp(now_ms / 1000.0, UTC) if now_ms else datetime.now(UTC)
        start = int(datetime(now.year, now.month, now.day, tzinfo=UTC).timestamp() * 1000)
        mode_filter = " AND mode = ?" if mode else ""
        params: tuple[Any, ...] = (start, mode) if mode else (start,)
        rows = self.connection.execute(
            f"""SELECT net_pnl FROM trades
               WHERE status = 'CLOSED' AND closed_at_ms >= ?{mode_filter}
               ORDER BY closed_at_ms DESC, id DESC""",
            params,
        )
        count = 0
        for row in rows:
            if float(row["net_pnl"]) < 0:
                count += 1
            else:
                break
        return count

    def get_meta(self, key: str, default: str = "") -> str:
        row = self.connection.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row else default

    def set_meta(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT INTO metadata(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

# ==================== ml.py ====================
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import brier_score_loss, roc_auc_score



FEATURE_NAMES = (
    "side",
    "setup_sweep",
    "hour_sin",
    "hour_cos",
    "atr_pct",
    "spread_bps",
    "spread_expansion_ratio",
    "impulse_aligned",
    "trend_aligned",
    "mtf_15m_aligned",
    "mtf_1h_aligned",
    "volume_ratio",
    "flow_aligned",
    "book_aligned",
    "microprice_aligned",
    "rsi_aligned",
    "extension_aligned",
    "leader_return_aligned",
    "leader_correlation",
    "open_interest_change_pct",
    "funding_aligned",
    "spoof_score",
    "cvd_divergence_aligned",
    "absorption_aligned",
    "footprint_aligned",
    "footprint_opposing",
    "tape_ticks_per_sec",
    "tape_velocity_ratio",
    "touch_count",
)
FEATURE_SCHEMA_VERSION = 2


@dataclass(slots=True, frozen=True)
class MLDecision:
    allowed: bool
    valid_model: bool
    probability: float | None = None
    reason: str = ""


def feature_row(feature: FeatureVector, side: Side, setup_type: str) -> list[float]:
    direction = side.direction
    moment = datetime.fromtimestamp(feature.timestamp_ms / 1000.0, UTC)
    hour = moment.hour + moment.minute / 60.0
    phase = 2.0 * math.pi * hour / 24.0
    footprint = feature.footprint_buy_ratio if side is Side.LONG else feature.footprint_sell_ratio
    opposing_footprint = (
        feature.footprint_sell_ratio if side is Side.LONG else feature.footprint_buy_ratio
    )
    if setup_type.startswith("liquidity"):
        touch_count = (
            max(feature.day_low_touch_count, feature.hourly_low_touch_count)
            if side is Side.LONG
            else max(feature.day_high_touch_count, feature.hourly_high_touch_count)
        )
    else:
        touch_count = max(feature.high_touch_count, feature.low_touch_count)
    return [
        float(direction),
        float(setup_type.startswith("liquidity")),
        math.sin(phase),
        math.cos(phase),
        feature.atr_pct,
        feature.spread_bps,
        feature.spread_expansion_ratio,
        direction * feature.impulse_atr,
        direction * feature.trend_atr,
        direction * feature.mtf_trend_15m_atr,
        direction * feature.mtf_trend_1h_atr,
        feature.volume_ratio,
        direction * feature.flow_imbalance,
        direction * feature.book_imbalance,
        direction * feature.microprice_bias_bps,
        direction * (feature.rsi - 50.0),
        direction * feature.extension_atr,
        direction * feature.leader_return_bps,
        feature.leader_correlation,
        feature.open_interest_change_pct,
        direction * feature.funding_rate,
        feature.spoof_score,
        direction * feature.cvd_divergence,
        direction * feature.absorption_side,
        footprint,
        opposing_footprint,
        feature.tape_ticks_per_sec,
        feature.tape_velocity_ratio,
        float(touch_count),
    ]


class MLValidator:
    def __init__(self, settings: MLSettings, mode: str) -> None:
        self.settings = settings
        self.mode = mode
        self.payload: dict[str, Any] | None = None
        self.load_error = ""
        self.reload()

    def reload(self) -> None:
        self.payload = None
        path = Path(self.settings.model_path)
        if not path.exists():
            self.load_error = "ML-модель ещё не обучена"
            return
        try:
            payload = joblib.load(path)
            if tuple(payload.get("feature_names", ())) != FEATURE_NAMES:
                raise ValueError("feature schema mismatch")
            if int(payload.get("schema_version", 0)) != FEATURE_SCHEMA_VERSION:
                raise ValueError("feature schema version mismatch")
            self.payload = payload
            self.load_error = ""
        except Exception as exc:
            self.load_error = f"ML-модель не загружена: {type(exc).__name__}"

    def _approved(self) -> tuple[bool, str]:
        if not self.payload:
            return False, self.load_error or "нет ML-модели"
        trained_at = datetime.fromisoformat(str(self.payload["trained_at_utc"]))
        if trained_at.tzinfo is None:
            trained_at = trained_at.replace(tzinfo=UTC)
        age_days = (datetime.now(UTC) - trained_at).total_seconds() / 86_400.0
        metrics = self.payload.get("validation", {})
        if abs(float(metrics.get("threshold", -1.0)) - self.settings.min_probability) > 1e-9:
            return False, "ML-порог изменён; требуется переобучение"
        trained_modes = tuple(self.payload.get("training_modes", ()))
        if trained_modes != self.settings.training_modes:
            return False, "режимы обучающей выборки изменены; требуется переобучение"
        if age_days > self.settings.max_model_age_days:
            return False, f"ML-модель устарела: {age_days:.0f} дней"
        if int(metrics.get("samples", 0)) < self.settings.min_validation_samples:
            return False, "недостаточно out-of-sample наблюдений"
        if float(metrics.get("roc_auc", 0.0)) < self.settings.min_validation_auc:
            return False, f"низкий out-of-sample AUC {float(metrics.get('roc_auc', 0.0)):.2f}"
        return bool(self.payload.get("approved", False)), str(self.payload.get("approval_reason", ""))

    def decide(self, signal: Signal) -> MLDecision:
        if not self.settings.enabled:
            return MLDecision(True, False, reason="ML disabled")
        approved, reason = self._approved()
        if not approved:
            block = self.mode == "live" and self.settings.required_in_live
            return MLDecision(not block, False, reason=reason or "ML работает в shadow-режиме")
        model = self.payload["model"]
        probability = float(
            model.predict_proba([feature_row(signal.features, signal.side, signal.setup_type)])[0][1]
        )
        allowed = probability >= self.settings.min_probability
        return MLDecision(
            allowed,
            True,
            probability,
            "ML ниже порога" if not allowed else "ML подтвердил сетап",
        )

    def live_block_reason(self) -> str:
        if not self.settings.enabled or self.mode != "live" or not self.settings.required_in_live:
            return ""
        approved, reason = self._approved()
        return "" if approved else (reason or "ML-модель не прошла проверку")


def train_from_journal(settings: MLSettings, journal: TradeJournal) -> dict[str, Any]:
    placeholders = ",".join("?" for _ in settings.training_modes)
    rows = list(
        journal.connection.execute(
            f"""
            SELECT s.features_json, s.side, s.setup_type, t.net_pnl, t.closed_at_ms
            FROM trades t JOIN signals s ON s.signal_id = t.signal_id
            WHERE t.status = 'CLOSED' AND t.mode IN ({placeholders})
            ORDER BY t.closed_at_ms, t.id
            """,
            settings.training_modes,
        )
    )
    if len(rows) < settings.min_training_samples:
        raise ValueError(
            f"Need at least {settings.min_training_samples} closed trades; found {len(rows)}"
        )
    features: list[list[float]] = []
    labels: list[int] = []
    for row in rows:
        raw = json.loads(row["features_json"])
        feature = FeatureVector(**raw)
        side = Side(str(row["side"]))
        features.append(feature_row(feature, side, str(row["setup_type"])))
        labels.append(int(float(row["net_pnl"]) > 0))
    holdout = max(settings.min_validation_samples, len(rows) // 5)
    remaining = len(rows) - holdout
    calibration_size = max(75, remaining // 4)
    training_end = remaining - calibration_size
    if training_end < 100:
        raise ValueError("Training partition is too small after calibration and holdout splits")
    x_train = np.asarray(features[:training_end], dtype=float)
    y_train = np.asarray(labels[:training_end], dtype=int)
    x_calibration = np.asarray(features[training_end:remaining], dtype=float)
    y_calibration = np.asarray(labels[training_end:remaining], dtype=int)
    x_test = np.asarray(features[-holdout:], dtype=float)
    y_test = np.asarray(labels[-holdout:], dtype=int)
    if any(len(set(part)) < 2 for part in (y_train, y_calibration, y_test)):
        raise ValueError("Train, calibration, and holdout sets must each contain wins and losses")
    base = RandomForestClassifier(
        n_estimators=400,
        max_depth=6,
        min_samples_leaf=12,
        max_features="sqrt",
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1,
    )
    base.fit(x_train, y_train)
    try:
        from sklearn.frozen import FrozenEstimator

        model = CalibratedClassifierCV(FrozenEstimator(base), method="sigmoid")
    except ImportError:  # scikit-learn 1.5 compatibility
        model = CalibratedClassifierCV(base, method="sigmoid", cv="prefit")
    model.fit(x_calibration, y_calibration)
    probabilities = model.predict_proba(x_test)[:, 1]
    auc = float(roc_auc_score(y_test, probabilities))
    brier = float(brier_score_loss(y_test, probabilities))
    selected = probabilities >= settings.min_probability
    selected_count = int(selected.sum())
    selected_precision = float(y_test[selected].mean()) if selected_count else 0.0
    approved = bool(
        len(x_test) >= settings.min_validation_samples
        and auc >= settings.min_validation_auc
        and selected_count >= 20
        and selected_precision >= settings.min_probability
    )
    reason = "approved" if approved else "holdout quality gate failed"
    payload = {
        "schema_version": FEATURE_SCHEMA_VERSION,
        "feature_names": FEATURE_NAMES,
        "trained_at_utc": datetime.now(UTC).isoformat(),
        "training_samples": len(x_train),
        "calibration_samples": len(x_calibration),
        "training_modes": settings.training_modes,
        "model": model,
        "approved": approved,
        "approval_reason": reason,
        "validation": {
            "samples": len(x_test),
            "roc_auc": auc,
            "brier": brier,
            "threshold": settings.min_probability,
            "selected_count": selected_count,
            "selected_precision": selected_precision,
        },
    }
    output = Path(settings.model_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    joblib.dump(payload, temp)
    temp.replace(output)
    return {key: value for key, value in payload.items() if key != "model"}

# ==================== strategy.py ====================
import time
import uuid
from dataclasses import dataclass



@dataclass(slots=True)
class ArmedSetup:
    symbol: str
    side: Side
    created_at_ms: int
    breakout_level: float
    impulse_extreme: float
    pullback_extreme: float
    pullback_seen: bool = False
    initiative_zone: float = 0.0
    zone_tested: bool = False


@dataclass(slots=True, frozen=True)
class ScanDiagnostic:
    symbol: str
    score: float
    state: str
    reason: str
    side: Side | None = None


class ImpulseStrategy:
    """Impulse -> breakout -> shallow pullback -> reacceleration state machine."""

    def __init__(self, strategy: StrategySettings, market: MarketSettings) -> None:
        self.cfg = strategy
        self.market = market
        self.arms: dict[str, ArmedSetup] = {}
        self.cooldown_until: dict[str, int] = {}
        self.diagnostics: dict[str, ScanDiagnostic] = {}

    def evaluate(self, feature: FeatureVector) -> Signal | None:
        now_ms = feature.timestamp_ms
        arm = self.arms.get(feature.symbol)
        if arm:
            signal = self._advance_arm(arm, feature)
            if signal:
                self.arms.pop(feature.symbol, None)
                self.cooldown_until[feature.symbol] = now_ms + self.cfg.cooldown_sec * 1000
            return signal

        if now_ms < self.cooldown_until.get(feature.symbol, 0):
            left = max((self.cooldown_until[feature.symbol] - now_ms) // 1000, 0)
            self._diag(feature, 0.0, "cooldown", f"пауза после сигнала: {left} сек")
            return None

        side, score, reason = self._qualify_impulse(feature)
        if side is None:
            self._diag(feature, score, "scan", reason)
            return None
        price = feature.ask if side is Side.LONG else feature.bid
        breakout = feature.breakout_high if side is Side.LONG else feature.breakout_low
        self.arms[feature.symbol] = ArmedSetup(
            symbol=feature.symbol,
            side=side,
            created_at_ms=now_ms,
            breakout_level=breakout,
            impulse_extreme=price,
            pullback_extreme=price,
            initiative_zone=(
                feature.initiative_buy_zone if side is Side.LONG else feature.initiative_sell_zone
            ),
        )
        self._diag(feature, score, "armed", "импульс подтверждён; ждём неглубокий откат", side)
        return None

    def _qualify_impulse(self, f: FeatureVector) -> tuple[Side | None, float, str]:
        if not f.ready:
            return None, 0.0, f.reject_reason or "данные не готовы"
        direction = 1 if f.impulse_atr >= 0 else -1
        side = Side.LONG if direction > 0 else Side.SHORT
        impulse = abs(f.impulse_atr)
        aligned_trend = direction * f.trend_atr
        aligned_flow = direction * f.flow_imbalance
        aligned_book = direction * f.book_imbalance
        breakout_ok = (
            f.ask >= f.breakout_high * (1.0 + self.cfg.breakout_buffer_bps / 10_000.0)
            if side is Side.LONG
            else f.bid <= f.breakout_low * (1.0 - self.cfg.breakout_buffer_bps / 10_000.0)
        )
        rsi_ok = f.rsi <= self.cfg.max_rsi_long if side is Side.LONG else f.rsi >= self.cfg.min_rsi_short
        extension_ok = abs(f.extension_atr) <= self.cfg.max_extension_atr
        opposing_wall = f.ask_wall_ratio if side is Side.LONG else f.bid_wall_ratio
        wall_age = f.ask_wall_age_ms if side is Side.LONG else f.bid_wall_age_ms
        wall_confidence = f.ask_wall_confidence if side is Side.LONG else f.bid_wall_confidence
        opposing_wall_is_real = (
            wall_age >= self.cfg.min_wall_age_ms
            and wall_confidence >= self.cfg.min_wall_confidence
        )
        footprint_ratio = f.footprint_buy_ratio if side is Side.LONG else f.footprint_sell_ratio
        correlation_sign = 1 if f.leader_correlation >= 0 else -1
        leader_aligned_move = direction * correlation_sign * f.leader_return_bps
        leader_ok = not (
            abs(f.leader_correlation) >= self.cfg.min_leader_correlation
            and leader_aligned_move < -self.cfg.max_adverse_leader_move_bps
        )
        ratios = (
            min(impulse / max(self.cfg.min_impulse_atr, 1e-9), 1.0),
            min(aligned_trend / max(self.cfg.min_trend_atr, 1e-9), 1.0),
            min(f.volume_ratio / max(self.cfg.min_volume_ratio, 1e-9), 1.0),
            min(aligned_flow / max(self.cfg.min_flow_imbalance, 1e-9), 1.0),
            min(aligned_book / max(self.cfg.min_book_imbalance, 1e-9), 1.0),
            min(f.recent_trade_count / max(self.cfg.min_trade_count, 1), 1.0),
        )
        score = max(0.0, min(100.0, sum(max(item, 0.0) for item in ratios) / len(ratios) * 100.0))
        checks = (
            (f.spread_bps <= self.market.max_spread_bps, f"спред {f.spread_bps:.2f} bps"),
            (f.spread_baseline_ready, "история спреда не прогрета"),
            (
                f.spread_expansion_ratio <= self.cfg.max_spread_expansion_ratio,
                f"спред расширился в {f.spread_expansion_ratio:.1f}x",
            ),
            (not f.news_blocked, f.news_reason or "новостное окно"),
            (
                self.cfg.min_atr_pct <= f.atr_pct <= self.cfg.max_atr_pct,
                f"ATR {f.atr_pct:.3f}% вне рабочего режима",
            ),
            (f.mtf_ready, "старшие таймфреймы не прогреты"),
            (
                direction * f.mtf_trend_15m_atr >= self.cfg.min_mtf_trend_atr
                and direction * f.mtf_trend_1h_atr >= self.cfg.min_mtf_trend_atr,
                "нет согласования тренда 15m/1h",
            ),
            (f.derivatives_ready, "OI/funding не готовы"),
            (
                f.open_interest_change_pct >= self.cfg.min_oi_change_pct,
                f"OI {f.open_interest_change_pct:+.3f}% не растёт",
            ),
            (
                abs(f.funding_rate) <= self.cfg.max_abs_funding_rate,
                f"экстремальный funding {f.funding_rate:.4%}",
            ),
            (f.spoof_score <= self.cfg.max_spoof_score, f"spoof-score {f.spoof_score:.2f}"),
            (self.cfg.min_impulse_atr <= impulse <= self.cfg.max_impulse_atr, f"импульс {impulse:.2f} ATR"),
            (aligned_trend >= self.cfg.min_trend_atr, f"тренд {aligned_trend:.2f} ATR"),
            (breakout_ok, "локальный уровень ещё не пробит"),
            (f.volume_ratio >= self.cfg.min_volume_ratio, f"темп объёма {f.volume_ratio:.2f}x"),
            (aligned_flow >= self.cfg.min_flow_imbalance, f"дисбаланс ленты {aligned_flow:+.2f}"),
            (aligned_book >= self.cfg.min_book_imbalance, f"дисбаланс стакана {aligned_book:+.2f}"),
            (f.recent_trade_count >= self.cfg.min_trade_count, f"сделок в окне {f.recent_trade_count}"),
            (
                not opposing_wall_is_real or opposing_wall <= self.cfg.max_opposing_wall_ratio,
                f"устойчивая встречная плотность {opposing_wall:.1f}x",
            ),
            (
                footprint_ratio >= self.cfg.footprint_imbalance_ratio,
                f"нет footprint {self.cfg.footprint_imbalance_ratio:.0f}:1",
            ),
            (
                f.tape_ticks_per_sec >= self.cfg.min_tape_ticks_per_sec
                and f.tape_velocity_ratio >= self.cfg.min_tape_velocity_ratio,
                f"лента {f.tape_ticks_per_sec:.1f} tps / {f.tape_velocity_ratio:.1f}x",
            ),
            (
                direction * f.cvd_divergence >= -0.25,
                "CVD-дивергенция против импульса",
            ),
            (rsi_ok, f"RSI {f.rsi:.1f}: поздний вход"),
            (extension_ok, f"растяжение {abs(f.extension_atr):.2f} ATR"),
            (leader_ok, f"BTC против входа {f.leader_return_bps:+.1f} bps"),
        )
        for passed, reason in checks:
            if not passed:
                return None, score, reason
        return side, max(score, 80.0), "сильный импульс подтверждён"

    def _advance_arm(self, arm: ArmedSetup, f: FeatureVector) -> Signal | None:
        if not f.ready:
            self._diag(f, 50.0, "armed", f.reject_reason or "данные не готовы", arm.side)
            return None
        age_ms = f.timestamp_ms - arm.created_at_ms
        if age_ms > self.cfg.arm_lifetime_sec * 1000:
            self.arms.pop(f.symbol, None)
            self._diag(f, 15.0, "expired", "сетап не дал повторного ускорения", arm.side)
            return None
        direction = arm.side.direction
        price = f.ask if arm.side is Side.LONG else f.bid
        live_checks = (
            (not f.news_blocked, f.news_reason or "новостное окно"),
            (f.spread_bps <= self.market.max_spread_bps, f"спред {f.spread_bps:.2f} bps"),
            (f.spread_baseline_ready, "история спреда не прогрета"),
            (
                f.spread_expansion_ratio <= self.cfg.max_spread_expansion_ratio,
                f"спред расширился в {f.spread_expansion_ratio:.1f}x",
            ),
            (self.cfg.min_atr_pct <= f.atr_pct <= self.cfg.max_atr_pct, "сменился режим ATR"),
            (
                f.mtf_ready
                and direction * f.mtf_trend_15m_atr >= self.cfg.min_mtf_trend_atr
                and direction * f.mtf_trend_1h_atr >= self.cfg.min_mtf_trend_atr,
                "пропало согласование 15m/1h",
            ),
            (
                f.derivatives_ready
                and f.open_interest_change_pct >= self.cfg.min_oi_change_pct
                and abs(f.funding_rate) <= self.cfg.max_abs_funding_rate,
                "OI/funding больше не подтверждают",
            ),
            (f.spoof_score <= self.cfg.max_spoof_score, f"spoof-score {f.spoof_score:.2f}"),
            (direction * f.cvd_divergence >= -0.25, "CVD развернулся против сетапа"),
        )
        for passed, reason in live_checks:
            if not passed:
                return self._invalidate(f, arm, reason)

        if arm.side is Side.LONG:
            if price > arm.impulse_extreme and not arm.pullback_seen:
                arm.impulse_extreme = price
                arm.pullback_extreme = price
            pullback_atr = (arm.impulse_extreme - price) / f.atr
            invalidated = price < arm.breakout_level - self.cfg.invalidation_buffer_atr * f.atr
            if pullback_atr > self.cfg.max_pullback_atr or invalidated:
                return self._invalidate(f, arm, "слишком глубокий откат")
            if pullback_atr >= self.cfg.min_pullback_atr:
                arm.pullback_seen = True
                arm.pullback_extreme = min(arm.pullback_extreme, price)
                arm.zone_tested = arm.zone_tested or abs(price - arm.initiative_zone) <= (
                    self.cfg.footprint_zone_tolerance_atr * f.atr
                )
            accelerated = arm.pullback_seen and price >= arm.pullback_extreme * (
                1.0 + self.cfg.reacceleration_bps / 10_000.0
            )
        else:
            if price < arm.impulse_extreme and not arm.pullback_seen:
                arm.impulse_extreme = price
                arm.pullback_extreme = price
            pullback_atr = (price - arm.impulse_extreme) / f.atr
            invalidated = price > arm.breakout_level + self.cfg.invalidation_buffer_atr * f.atr
            if pullback_atr > self.cfg.max_pullback_atr or invalidated:
                return self._invalidate(f, arm, "слишком глубокий откат")
            if pullback_atr >= self.cfg.min_pullback_atr:
                arm.pullback_seen = True
                arm.pullback_extreme = max(arm.pullback_extreme, price)
                arm.zone_tested = arm.zone_tested or abs(price - arm.initiative_zone) <= (
                    self.cfg.footprint_zone_tolerance_atr * f.atr
                )
            accelerated = arm.pullback_seen and price <= arm.pullback_extreme * (
                1.0 - self.cfg.reacceleration_bps / 10_000.0
            )

        flow_ok = direction * f.flow_imbalance >= self.cfg.reentry_flow_imbalance
        book_ok = direction * f.book_imbalance >= self.cfg.reentry_book_imbalance
        tape_ok = f.tape_velocity_ratio >= self.cfg.min_tape_velocity_ratio
        if not arm.pullback_seen:
            self._diag(f, 65.0, "armed", f"ждём откат {self.cfg.min_pullback_atr:.2f} ATR", arm.side)
            return None
        if not accelerated or not flow_ok or not book_ok or not tape_ok or not arm.zone_tested:
            parts = []
            if not accelerated:
                parts.append("нет повторного ускорения")
            if not flow_ok:
                parts.append("лента не подтвердила")
            if not book_ok:
                parts.append("стакан не подтвердил")
            if not tape_ok:
                parts.append("лента не ускорилась")
            if not arm.zone_tested:
                parts.append("зона footprint не протестирована")
            self._diag(f, 72.0, "armed", ", ".join(parts), arm.side)
            return None

        anchor = (
            min(arm.breakout_level, arm.pullback_extreme)
            if arm.side is Side.LONG
            else max(arm.breakout_level, arm.pullback_extreme)
        )
        stop = anchor - direction * self.cfg.stop_buffer_atr * f.atr
        raw_risk = direction * (price - stop)
        if raw_risk <= 0 or raw_risk > self.cfg.max_stop_atr * f.atr:
            return self._invalidate(f, arm, f"стоп слишком широк: {raw_risk / f.atr:.2f} ATR")
        if raw_risk < self.cfg.min_stop_atr * f.atr:
            stop = price - direction * self.cfg.min_stop_atr * f.atr
            raw_risk = abs(price - stop)
        target = price + direction * raw_risk * self.cfg.target_rr
        strength = min(
            100.0,
            78.0
            + min(abs(f.flow_imbalance), 0.5) * 20.0
            + min(abs(f.book_imbalance), 0.3) * 15.0,
        )
        self._diag(f, strength, "signal", "откат и повторное ускорение подтверждены", arm.side)
        return Signal(
            signal_id=uuid.uuid4().hex,
            symbol=f.symbol,
            side=arm.side,
            created_at_ms=f.timestamp_ms,
            expires_at_ms=f.timestamp_ms + self.market.stale_after_ms,
            entry_price=price,
            stop_price=stop,
            target_price=target,
            target_rr=self.cfg.target_rr,
            strength=strength,
            breakout_level=arm.breakout_level,
            features=f,
            reasons=(
                "импульс + пробой",
                "неглубокий откат",
                "повторное ускорение",
                "лента и стакан согласованы",
            ),
            setup_type="impulse_continuation",
        )

    def _invalidate(self, f: FeatureVector, arm: ArmedSetup, reason: str) -> None:
        self.arms.pop(f.symbol, None)
        self._diag(f, 20.0, "invalidated", reason, arm.side)
        return None

    def _diag(
        self,
        feature: FeatureVector,
        score: float,
        state: str,
        reason: str,
        side: Side | None = None,
    ) -> None:
        self.diagnostics[feature.symbol] = ScanDiagnostic(
            symbol=feature.symbol,
            score=score,
            state=state,
            reason=reason,
            side=side,
        )

    def best_diagnostic(self) -> ScanDiagnostic | None:
        return max(self.diagnostics.values(), key=lambda item: item.score, default=None)

    def clear_symbol(self, symbol: str) -> None:
        self.arms.pop(symbol, None)


def now_ms() -> int:
    return int(time.time() * 1000)

# ==================== structure.py ====================
import uuid
from dataclasses import dataclass



@dataclass(slots=True)
class SweepCandidate:
    symbol: str
    side: Side
    level: float
    extreme: float
    choch_level: float
    created_at_ms: int
    touch_count: int
    returned: bool = False


class LiquiditySweepStrategy:
    """Objective liquidity sweep and micro-CHoCH detector; no discretionary SMC labels."""

    def __init__(self, strategy: StrategySettings, market: MarketSettings) -> None:
        self.cfg = strategy
        self.market = market
        self.previous: dict[str, FeatureVector] = {}
        self.candidates: dict[str, SweepCandidate] = {}
        self.diagnostics: dict[str, ScanDiagnostic] = {}
        self.cooldown_until: dict[str, int] = {}

    def evaluate(self, feature: FeatureVector) -> Signal | None:
        previous = self.previous.get(feature.symbol)
        self.previous[feature.symbol] = feature
        candidate = self.candidates.get(feature.symbol)
        if candidate:
            return self._advance(candidate, feature)
        if previous is None or not feature.ready:
            return None
        if feature.timestamp_ms < self.cooldown_until.get(feature.symbol, 0):
            return None
        created = self._detect_cross(previous, feature)
        if created:
            self.candidates[feature.symbol] = created
            self.diagnostics[feature.symbol] = ScanDiagnostic(
                feature.symbol,
                68.0,
                "sweep",
                "ликвидность снята; ждём возврат и CHoCH",
                created.side,
            )
        return None

    def _detect_cross(self, previous: FeatureVector, current: FeatureVector) -> SweepCandidate | None:
        high_levels = [
            (level, touches)
            for level, touches in (
                (previous.day_high, previous.day_high_touch_count),
                (previous.hourly_swing_high, previous.hourly_high_touch_count),
            )
            if level > 0
        ]
        low_levels = [
            (level, touches)
            for level, touches in (
                (previous.day_low, previous.day_low_touch_count),
                (previous.hourly_swing_low, previous.hourly_low_touch_count),
            )
            if level > 0
        ]
        crossed_high = [
            (level, touches)
            for level, touches in high_levels
            if previous.mid <= level
            and current.mid >= level * (1.0 + self.cfg.sweep_min_bps / 10_000.0)
        ]
        crossed_low = [
            (level, touches)
            for level, touches in low_levels
            if previous.mid >= level
            and current.mid <= level * (1.0 - self.cfg.sweep_min_bps / 10_000.0)
        ]
        if crossed_high:
            level, touches = max(crossed_high, key=lambda item: item[0])
            excursion = (current.mid / level - 1.0) * 10_000.0
            if excursion <= self.cfg.sweep_max_bps:
                return SweepCandidate(
                    current.symbol,
                    Side.SHORT,
                    level,
                    current.ask,
                    previous.micro_swing_low,
                    current.timestamp_ms,
                    touches,
                )
        if crossed_low:
            level, touches = min(crossed_low, key=lambda item: item[0])
            excursion = (level / current.mid - 1.0) * 10_000.0
            if excursion <= self.cfg.sweep_max_bps:
                return SweepCandidate(
                    current.symbol,
                    Side.LONG,
                    level,
                    current.bid,
                    previous.micro_swing_high,
                    current.timestamp_ms,
                    touches,
                )
        return None

    def _advance(self, candidate: SweepCandidate, f: FeatureVector) -> Signal | None:
        age = f.timestamp_ms - candidate.created_at_ms
        direction = candidate.side.direction
        if age > self.cfg.sweep_return_window_sec * 1000:
            return self._invalidate(f, "возврат после снятия ликвидности не состоялся")
        if candidate.side is Side.SHORT:
            candidate.extreme = max(candidate.extreme, f.ask)
            excursion = (candidate.extreme / candidate.level - 1.0) * 10_000.0
            candidate.returned = candidate.returned or f.bid < candidate.level
            choch = f.bid <= candidate.choch_level * (1.0 - self.cfg.choch_buffer_bps / 10_000.0)
        else:
            candidate.extreme = min(candidate.extreme, f.bid)
            excursion = (candidate.level / candidate.extreme - 1.0) * 10_000.0
            candidate.returned = candidate.returned or f.ask > candidate.level
            choch = f.ask >= candidate.choch_level * (1.0 + self.cfg.choch_buffer_bps / 10_000.0)
        if excursion > self.cfg.sweep_max_bps:
            return self._invalidate(f, "прокол превратился в пробой")

        reversal_flow = direction * f.flow_imbalance >= self.cfg.reentry_flow_imbalance
        reversal_book = direction * f.book_imbalance >= self.cfg.reentry_book_imbalance
        footprint_ratio = f.footprint_buy_ratio if candidate.side is Side.LONG else f.footprint_sell_ratio
        tape_ok = (
            f.tape_ticks_per_sec >= self.cfg.min_tape_ticks_per_sec
            and f.tape_velocity_ratio >= self.cfg.min_tape_velocity_ratio
        )
        quality_checks = (
            (f.ready, f.reject_reason or "данные не готовы"),
            (not f.news_blocked, f.news_reason or "новостное окно"),
            (f.mtf_ready, "старшие таймфреймы не прогреты"),
            (
                direction * f.mtf_trend_15m_atr >= self.cfg.min_mtf_trend_atr
                and direction * f.mtf_trend_1h_atr >= self.cfg.min_mtf_trend_atr,
                "разворот против тренда 15m/1h",
            ),
            (
                1 <= candidate.touch_count <= self.cfg.max_reversal_touches,
                f"неподходящее число касаний уровня: {candidate.touch_count}",
            ),
            (f.spread_bps <= self.market.max_spread_bps, f"спред {f.spread_bps:.2f} bps"),
            (f.spread_baseline_ready, "история спреда не прогрета"),
            (
                f.spread_expansion_ratio <= self.cfg.max_spread_expansion_ratio,
                f"спред расширился в {f.spread_expansion_ratio:.1f}x",
            ),
            (f.spoof_score <= self.cfg.max_spoof_score, f"spoof-score {f.spoof_score:.2f}"),
            (self.cfg.min_atr_pct <= f.atr_pct <= self.cfg.max_atr_pct, f"ATR {f.atr_pct:.3f}% вне режима"),
            (f.derivatives_ready, "OI/funding не готовы"),
            (abs(f.open_interest_change_pct) >= self.cfg.min_oi_change_pct, "OI не подтвердил участие"),
            (abs(f.funding_rate) <= self.cfg.max_abs_funding_rate, f"экстремальный funding {f.funding_rate:.4%}"),
        )
        for passed, reason in quality_checks:
            if not passed:
                return self._invalidate(f, reason)
        footprint_ok = footprint_ratio >= self.cfg.footprint_imbalance_ratio
        if (
            not candidate.returned
            or not choch
            or not reversal_flow
            or not reversal_book
            or not tape_ok
            or not footprint_ok
        ):
            missing = []
            if not candidate.returned:
                missing.append("возврат под/над уровень")
            if not choch:
                missing.append("micro-CHoCH")
            if not reversal_flow or not reversal_book:
                missing.append("разворот order flow")
            if not footprint_ok:
                missing.append("footprint 3:1")
            if not tape_ok:
                missing.append("ускорение ленты")
            self.diagnostics[f.symbol] = ScanDiagnostic(
                f.symbol, 74.0, "sweep", "ждём: " + ", ".join(missing), candidate.side
            )
            return None
        entry = f.ask if candidate.side is Side.LONG else f.bid
        stop = candidate.extreme - direction * self.cfg.stop_buffer_atr * f.atr
        unit_risk = direction * (entry - stop)
        if unit_risk <= 0 or unit_risk > self.cfg.max_stop_atr * f.atr:
            return self._invalidate(f, "стоп sweep-сетапа слишком широк")
        if unit_risk < self.cfg.min_stop_atr * f.atr:
            stop = entry - direction * self.cfg.min_stop_atr * f.atr
            unit_risk = abs(entry - stop)
        target = entry + direction * unit_risk * self.cfg.target_rr
        self.candidates.pop(f.symbol, None)
        self.cooldown_until[f.symbol] = f.timestamp_ms + self.cfg.cooldown_sec * 1000
        self.diagnostics[f.symbol] = ScanDiagnostic(
            f.symbol, 90.0, "signal", "sweep + возврат + CHoCH подтверждены", candidate.side
        )
        return Signal(
            signal_id=uuid.uuid4().hex,
            symbol=f.symbol,
            side=candidate.side,
            created_at_ms=f.timestamp_ms,
            expires_at_ms=f.timestamp_ms + self.market.stale_after_ms,
            entry_price=entry,
            stop_price=stop,
            target_price=target,
            target_rr=self.cfg.target_rr,
            strength=90.0,
            breakout_level=candidate.level,
            features=f,
            reasons=("liquidity sweep", "возврат за уровень", "micro-CHoCH", "order flow"),
            setup_type="liquidity_sweep_choch",
        )

    def _invalidate(self, feature: FeatureVector, reason: str) -> None:
        candidate = self.candidates.pop(feature.symbol, None)
        self.diagnostics[feature.symbol] = ScanDiagnostic(
            feature.symbol,
            20.0,
            "invalidated",
            reason,
            candidate.side if candidate else None,
        )
        return None

    def best_diagnostic(self) -> ScanDiagnostic | None:
        return max(self.diagnostics.values(), key=lambda item: item.score, default=None)

    def clear_symbol(self, symbol: str) -> None:
        self.candidates.pop(symbol, None)

# ==================== risk.py ====================
import math
from dataclasses import dataclass
from datetime import UTC, datetime



@dataclass(slots=True, frozen=True)
class RiskStatus:
    allowed: bool
    reason: str = ""


class RiskManager:
    def __init__(self, risk: RiskSettings, execution: ExecutionSettings, equity: float) -> None:
        self.cfg = risk
        self.execution = execution
        self.equity = equity
        self.day_key = self._day_key()
        self.day_start_equity = equity
        self.daily_realized_pnl = 0.0
        self.consecutive_losses = 0

    @staticmethod
    def _day_key(timestamp_ms: int | None = None) -> str:
        moment = datetime.fromtimestamp(timestamp_ms / 1000.0, UTC) if timestamp_ms else datetime.now(UTC)
        return moment.strftime("%Y-%m-%d")

    def _roll_day(self, timestamp_ms: int | None = None) -> None:
        key = self._day_key(timestamp_ms)
        if key != self.day_key:
            self.day_key = key
            self.day_start_equity = self.equity
            self.daily_realized_pnl = 0.0
            self.consecutive_losses = 0

    def can_open(self, open_positions: int, timestamp_ms: int | None = None) -> RiskStatus:
        self._roll_day(timestamp_ms)
        if self.equity <= 0:
            return RiskStatus(False, "equity is not positive")
        max_loss = self.day_start_equity * self.cfg.daily_max_loss_pct / 100.0
        if self.daily_realized_pnl <= -max_loss:
            return RiskStatus(False, "достигнут дневной лимит убытка")
        if self.consecutive_losses >= self.cfg.max_consecutive_losses:
            return RiskStatus(False, "достигнут лимит последовательных убытков")
        if open_positions >= self.cfg.max_open_positions:
            return RiskStatus(False, "достигнут лимит открытых позиций")
        return RiskStatus(True)

    def size(
        self,
        signal: Signal,
        spec: ContractSpec,
        *,
        open_positions: int,
        total_open_notional: float,
        risk_multiplier: float = 1.0,
    ) -> SizingResult:
        status = self.can_open(open_positions, signal.created_at_ms)
        if not status.allowed:
            return SizingResult(False, reason=status.reason)
        if signal.price_risk <= 0 or signal.entry_price <= 0:
            return SizingResult(False, reason="invalid entry or stop")
        max_leverage = spec.max_long_leverage if signal.side.value == "LONG" else spec.max_short_leverage
        if self.execution.leverage > max_leverage:
            return SizingResult(False, reason=f"leverage exceeds contract maximum {max_leverage}x")

        risk_multiplier = max(0.0, min(risk_multiplier, 1.0))
        risk_cash = self.equity * self.cfg.risk_per_trade_pct / 100.0 * risk_multiplier
        taker_fee = max(spec.taker_fee_rate, self.execution.taker_fee_rate)
        slip_rate = self.execution.assumed_slippage_bps / 10_000.0
        # Entry and stop exit are both conservatively treated as taker fills.
        round_trip_cost_per_unit = (signal.entry_price + signal.stop_price) * (taker_fee + slip_rate)
        worst_loss_per_unit = signal.price_risk + round_trip_cost_per_unit
        raw_quantity = risk_cash / worst_loss_per_unit

        max_total_notional = self.equity * self.cfg.max_total_notional_to_equity
        remaining_notional = max(max_total_notional - total_open_notional, 0.0)
        allowed_notional = min(self.cfg.max_single_notional_usdt, remaining_notional)
        raw_quantity = min(raw_quantity, allowed_notional / signal.entry_price)
        scale = 10**spec.quantity_precision
        quantity = math.floor(raw_quantity * scale) / scale
        notional = quantity * signal.entry_price
        estimated_loss = quantity * worst_loss_per_unit
        if quantity <= 0:
            return SizingResult(False, reason="quantity rounded to zero")
        if quantity < spec.min_quantity:
            return SizingResult(False, reason=f"quantity below minimum {spec.min_quantity:g}")
        if notional < spec.min_notional_usdt:
            return SizingResult(False, reason=f"notional below minimum {spec.min_notional_usdt:g} USDT")
        if estimated_loss > self.equity * self.cfg.hard_max_risk_per_trade_pct / 100.0 + 1e-9:
            return SizingResult(False, reason="estimated loss exceeds hard per-trade limit")
        return SizingResult(
            accepted=True,
            quantity=quantity,
            notional_usdt=notional,
            risk_cash=risk_cash,
            estimated_worst_loss=estimated_loss,
        )

    def register_close(self, pnl: float, timestamp_ms: int) -> None:
        self._roll_day(timestamp_ms)
        self.equity += pnl
        self.daily_realized_pnl += pnl
        self.consecutive_losses = self.consecutive_losses + 1 if pnl < 0 else 0

    def sync_equity(self, equity: float) -> None:
        if equity > 0:
            delta = equity - self.equity
            self.equity = equity
            # Preserve the day's already realized PnL while updating the balance baseline.
            self.day_start_equity += delta

# ==================== market.py ====================
import asyncio
import calendar
import gzip
import json
import logging
import math
import statistics
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import websockets



LOGGER = logging.getLogger(__name__)


def utc_ms() -> int:
    return int(time.time() * 1000)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _bool(value: Any) -> bool:
    return value if isinstance(value, bool) else str(value).strip().lower() == "true"


def decode_ws_message(raw: str | bytes) -> str:
    if isinstance(raw, bytes):
        try:
            return gzip.decompress(raw).decode("utf-8")
        except (OSError, UnicodeDecodeError):
            return raw.decode("utf-8", errors="replace")
    return raw


@dataclass(slots=True)
class SymbolState:
    symbol: str
    trades: deque[TradeTick] = field(default_factory=lambda: deque(maxlen=25_000))
    candles_1m: deque[Candle] = field(default_factory=lambda: deque(maxlen=1600))
    candles_15m: deque[Candle] = field(default_factory=lambda: deque(maxlen=400))
    candles_1h: deque[Candle] = field(default_factory=lambda: deque(maxlen=300))
    spread_history: deque[tuple[int, float]] = field(default_factory=lambda: deque(maxlen=1000))
    wall_history: deque[tuple[int, float, float]] = field(default_factory=lambda: deque(maxlen=1000))
    order_book: OrderBook | None = None
    mark_price: float = 0.0
    last_event_ms: int = 0

    def update_candle(self, candle: Candle, interval: str = "1m") -> None:
        target = {"1m": self.candles_1m, "15m": self.candles_15m, "1h": self.candles_1h}[interval]
        if target and target[-1].open_time_ms == candle.open_time_ms:
            target[-1] = candle
            return
        if not target or candle.open_time_ms > target[-1].open_time_ms:
            target.append(candle)


class MarketDataStore:
    def __init__(self, cfg: AppSettings, symbols: Iterable[str]) -> None:
        self.cfg = cfg
        self.states = {symbol: SymbolState(symbol=symbol) for symbol in symbols}
        self.events_seen = 0
        self.derivatives: dict[str, tuple[float, float, int, bool]] = {}
        self.news_blocked = False
        self.news_reason = ""

    def state(self, symbol: str) -> SymbolState:
        return self.states[symbol]

    def ingest(self, payload: dict[str, Any], now_ms: int | None = None) -> str | None:
        data_type = str(payload.get("dataType") or payload.get("data_type") or "")
        data = payload.get("data")
        if not isinstance(data, dict) or "@" not in data_type:
            return None
        symbol, channel = data_type.split("@", 1)
        state = self.states.get(symbol)
        if state is None:
            return None
        event_ms = _int(data.get("E") or data.get("T") or payload.get("ts"), now_ms or utc_ms())
        state.last_event_ms = max(state.last_event_ms, event_ms)

        if channel.startswith("trade"):
            self._ingest_trade(state, data, event_ms)
        elif channel.startswith("depth"):
            self._ingest_depth(state, data, event_ms)
        elif channel.startswith("kline"):
            interval = channel.split("_", 1)[1].split("@", 1)[0] if "_" in channel else "1m"
            if interval in {"1m", "15m", "1h"}:
                self._ingest_kline(state, data, event_ms, interval)
        elif channel.startswith("markPrice"):
            state.mark_price = _float(data.get("p") or data.get("markPrice"))
        elif channel.startswith("bookTicker"):
            self._ingest_book_ticker(state, data, event_ms)
        else:
            return None
        self.events_seen += 1
        self._prune_trades(state, now_ms or utc_ms())
        return symbol

    def _ingest_trade(self, state: SymbolState, data: dict[str, Any], event_ms: int) -> None:
        rows = data.get("trades") if isinstance(data.get("trades"), list) else [data]
        for row in rows:
            if not isinstance(row, dict):
                continue
            price = _float(row.get("p") or row.get("price"))
            quantity = _float(row.get("q") or row.get("qty") or row.get("quantity"))
            if price <= 0 or quantity <= 0:
                continue
            maker = row.get("m", row.get("buyerMaker", row.get("isBuyerMaker", False)))
            state.trades.append(
                TradeTick(
                    timestamp_ms=_int(row.get("T") or row.get("time"), event_ms),
                    price=price,
                    quantity=quantity,
                    buyer_is_maker=_bool(maker),
                )
            )

    @staticmethod
    def _levels(value: Any, *, reverse: bool) -> tuple[tuple[float, float], ...]:
        levels: list[tuple[float, float]] = []
        for row in value if isinstance(value, list) else []:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue
            price, quantity = _float(row[0]), _float(row[1])
            if price > 0 and quantity > 0:
                levels.append((price, quantity))
        return tuple(sorted(levels, key=lambda item: item[0], reverse=reverse))

    def _ingest_depth(self, state: SymbolState, data: dict[str, Any], event_ms: int) -> None:
        bids = self._levels(data.get("bids") or data.get("b"), reverse=True)
        asks = self._levels(data.get("asks") or data.get("a"), reverse=False)
        if bids and asks and bids[0][0] < asks[0][0]:
            state.order_book = OrderBook(event_ms, bids, asks)
            self._track_book_state(state)

    def _ingest_book_ticker(self, state: SymbolState, data: dict[str, Any], event_ms: int) -> None:
        bid = _float(data.get("b") or data.get("bidPrice"))
        ask = _float(data.get("a") or data.get("askPrice"))
        bid_qty = _float(data.get("B") or data.get("bidQty"))
        ask_qty = _float(data.get("A") or data.get("askQty"))
        if bid > 0 and ask > bid:
            state.order_book = OrderBook(event_ms, ((bid, max(bid_qty, 0.0)),), ((ask, max(ask_qty, 0.0)),))
            self._track_book_state(state)

    def _ingest_kline(
        self, state: SymbolState, data: dict[str, Any], event_ms: int, interval: str
    ) -> None:
        row = data.get("K") or data.get("k") or data
        if not isinstance(row, dict):
            return
        open_time = _int(row.get("t") or row.get("openTime") or row.get("time"))
        if not open_time:
            open_time = event_ms - event_ms % 60_000
        close_time = _int(row.get("T") or row.get("closeTime"), open_time + 59_999)
        candle = Candle(
            open_time_ms=open_time,
            close_time_ms=close_time,
            open=_float(row.get("o") or row.get("open")),
            high=_float(row.get("h") or row.get("high")),
            low=_float(row.get("l") or row.get("low")),
            close=_float(row.get("c") or row.get("close")),
            volume=_float(row.get("v") or row.get("volume")),
            quote_volume=_float(row.get("q") or row.get("quoteVolume")),
            trade_count=_int(row.get("n") or row.get("tradeCount")),
            taker_buy_quote_volume=_float(row.get("Q") or row.get("takerBuyQuoteVolume")),
        )
        if min(candle.open, candle.high, candle.low, candle.close) > 0:
            state.update_candle(candle, interval)

    def _prune_trades(self, state: SymbolState, now_ms: int) -> None:
        cutoff = now_ms - self.cfg.market.trade_retention_sec * 1000
        while state.trades and state.trades[0].timestamp_ms < cutoff:
            state.trades.popleft()

    def load_history(self, symbol: str, rows: list[Any], interval: str = "1m") -> None:
        state = self.states[symbol]
        parsed: list[Candle] = []
        for row in rows:
            if isinstance(row, (list, tuple)) and len(row) >= 7:
                candle = Candle(
                    open_time_ms=_int(row[0]),
                    close_time_ms=_int(row[6]),
                    open=_float(row[1]),
                    high=_float(row[2]),
                    low=_float(row[3]),
                    close=_float(row[4]),
                    volume=_float(row[5]),
                    quote_volume=_float(row[7]) if len(row) > 7 else 0.0,
                    trade_count=_int(row[8]) if len(row) > 8 else 0,
                    taker_buy_quote_volume=_float(row[10]) if len(row) > 10 else 0.0,
                )
            elif isinstance(row, dict):
                candle = Candle(
                    open_time_ms=_int(row.get("time") or row.get("openTime")),
                    close_time_ms=_int(row.get("closeTime"), _int(row.get("time")) + 59_999),
                    open=_float(row.get("open")),
                    high=_float(row.get("high")),
                    low=_float(row.get("low")),
                    close=_float(row.get("close")),
                    volume=_float(row.get("volume")),
                )
            else:
                continue
            if candle.open_time_ms and min(candle.open, candle.high, candle.low, candle.close) > 0:
                parsed.append(candle)
        for candle in sorted(parsed, key=lambda item: item.open_time_ms):
            state.update_candle(candle, interval)

    def update_derivatives(
        self,
        symbol: str,
        *,
        open_interest_change_pct: float,
        funding_rate: float,
        timestamp_ms: int,
        ready: bool,
    ) -> None:
        self.derivatives[symbol] = (
            open_interest_change_pct,
            funding_rate,
            timestamp_ms,
            ready,
        )

    def update_news(self, blocked: bool, reason: str) -> None:
        self.news_blocked = blocked
        self.news_reason = reason

    def _track_book_state(self, state: SymbolState) -> None:
        book = state.order_book
        if book is None or book.mid <= 0:
            return
        spread = (book.best_ask - book.best_bid) / book.mid * 10_000.0
        bid_price, bid_ratio = self._dominant_wall(book.bids)
        ask_price, ask_ratio = self._dominant_wall(book.asks)
        state.spread_history.append((book.timestamp_ms, spread))
        state.wall_history.append(
            (
                book.timestamp_ms,
                bid_price if bid_ratio >= 3.0 else 0.0,
                ask_price if ask_ratio >= 3.0 else 0.0,
            )
        )
        cutoff = book.timestamp_ms - max(self.cfg.strategy.spread_baseline_window_sec, 15) * 2000
        while state.spread_history and state.spread_history[0][0] < cutoff:
            state.spread_history.popleft()
        while state.wall_history and state.wall_history[0][0] < cutoff:
            state.wall_history.popleft()

    @staticmethod
    def _dominant_wall(levels: tuple[tuple[float, float], ...]) -> tuple[float, float]:
        notionals = [(price, price * quantity) for price, quantity in levels]
        if not notionals:
            return 0.0, 0.0
        median = statistics.median(value for _, value in notionals)
        price, value = max(notionals, key=lambda item: item[1])
        return price, value / max(median, 1e-12)

    def _book_metrics(self, state: SymbolState) -> BookMetrics:
        book = state.order_book
        if book is None or book.mid <= 0:
            return BookMetrics(99_999.0, 0.0, 0.0, 0.0, 0.0)
        bid_notional = [price * qty for price, qty in book.bids]
        ask_notional = [price * qty for price, qty in book.asks]
        bid_sum, ask_sum = sum(bid_notional), sum(ask_notional)
        total = bid_sum + ask_sum
        imbalance = (bid_sum - ask_sum) / total if total > 0 else 0.0
        spread_bps = (book.best_ask - book.best_bid) / book.mid * 10_000.0
        bid_qty = book.bids[0][1]
        ask_qty = book.asks[0][1]
        microprice = (
            (book.best_ask * bid_qty + book.best_bid * ask_qty) / (bid_qty + ask_qty)
            if bid_qty + ask_qty > 0
            else book.mid
        )
        microprice_bias = (microprice / book.mid - 1.0) * 10_000.0
        bid_median = sorted(bid_notional)[len(bid_notional) // 2] if bid_notional else 0.0
        ask_median = sorted(ask_notional)[len(ask_notional) // 2] if ask_notional else 0.0
        bid_wall = max(bid_notional, default=0.0) / max(bid_median, 1e-12)
        ask_wall = max(ask_notional, default=0.0) / max(ask_median, 1e-12)
        bid_price, _ = self._dominant_wall(book.bids)
        ask_price, _ = self._dominant_wall(book.asks)
        bid_age, bid_confidence = self._wall_quality(state, bid_price, 1)
        ask_age, ask_confidence = self._wall_quality(state, ask_price, 2)
        history = list(state.wall_history)
        changes = 0
        comparisons = 0
        for previous, current in zip(history, history[1:]):
            for index in (1, 2):
                if previous[index] or current[index]:
                    comparisons += 1
                    if previous[index] != current[index]:
                        changes += 1
        spoof_score = changes / comparisons if comparisons else 0.0
        return BookMetrics(
            spread_bps,
            imbalance,
            microprice_bias,
            bid_wall,
            ask_wall,
            bid_age,
            ask_age,
            bid_confidence,
            ask_confidence,
            spoof_score,
        )

    @staticmethod
    def _wall_quality(state: SymbolState, price: float, index: int) -> tuple[int, float]:
        if price <= 0 or not state.wall_history:
            return 0, 0.0
        now_ms = state.wall_history[-1][0]
        recent = [row for row in state.wall_history if row[0] >= now_ms - 5_000]
        matches = [row for row in recent if row[index] == price]
        confidence = len(matches) / len(recent) if recent else 0.0
        age = now_ms - matches[0][0] if matches else 0
        return age, confidence

    def _flow_metrics(self, state: SymbolState, now_ms: int, price: float) -> FlowMetrics:
        cfg = self.cfg.strategy
        recent_cutoff = now_ms - cfg.flow_window_sec * 1000
        impulse_cutoff = now_ms - cfg.impulse_window_sec * 1000
        baseline_cutoff = now_ms - cfg.baseline_window_sec * 1000
        recent = [trade for trade in state.trades if trade.timestamp_ms >= recent_cutoff]
        impulse = [trade for trade in state.trades if trade.timestamp_ms >= impulse_cutoff]
        baseline = [trade for trade in state.trades if baseline_cutoff <= trade.timestamp_ms < recent_cutoff]
        buy = sum(t.quote_notional for t in recent if t.aggressor_direction > 0)
        sell = sum(t.quote_notional for t in recent if t.aggressor_direction < 0)
        total = buy + sell
        imbalance = (buy - sell) / total if total > 0 else 0.0
        recent_quote = sum(t.quote_notional for t in recent)
        baseline_quote = sum(t.quote_notional for t in baseline)
        recent_rate = recent_quote / max(cfg.flow_window_sec, 1)
        baseline_seconds = max(cfg.baseline_window_sec - cfg.flow_window_sec, 1)
        baseline_rate = baseline_quote / baseline_seconds
        volume_ratio = recent_rate / baseline_rate if baseline_rate > 0 else 0.0
        start_price = impulse[0].price if impulse else price
        impulse_return = price / start_price - 1.0 if start_price > 0 else 0.0
        return FlowMetrics(impulse_return, imbalance, volume_ratio, len(recent), recent_quote)

    def feature(self, symbol: str, now_ms: int | None = None) -> FeatureVector:
        now_ms = now_ms or utc_ms()
        state = self.states[symbol]
        book = state.order_book
        book_metrics = self._book_metrics(state)
        price = book.mid if book and book.mid > 0 else (state.trades[-1].price if state.trades else 0.0)
        flow = self._flow_metrics(state, now_ms, price)
        closed = [item for item in state.candles_1m if item.is_closed(now_ms)]
        closes = [item.close for item in closed]
        atr_value = atr(closed, self.cfg.strategy.atr_period)
        fast = ema(closes, self.cfg.strategy.ema_fast)
        slow = ema(closes, self.cfg.strategy.ema_slow)
        rsi_value = rsi(closes, self.cfg.strategy.rsi_period)
        lookback = closed[-self.cfg.strategy.breakout_lookback :]
        breakout_high = max((item.high for item in lookback), default=0.0)
        breakout_low = min((item.low for item in lookback), default=0.0)
        atr_ok = finite(atr_value) and atr_value > 0 and price > 0
        impulse_atr = flow.impulse_return * price / atr_value if atr_ok else 0.0
        trend_atr = (fast - slow) / atr_value if atr_ok and finite(fast) and finite(slow) else 0.0
        extension_atr = (price - fast) / atr_value if atr_ok and finite(fast) else 0.0
        trend_15m, ready_15m = self._higher_trend(state.candles_15m, now_ms)
        trend_1h, ready_1h = self._higher_trend(state.candles_1h, now_ms)
        micro = analyze_microstructure(
            list(state.trades),
            now_ms=now_ms,
            price=price,
            atr=atr_value if atr_ok else 0.0,
            cfg=self.cfg.strategy,
        )
        day_high, day_low = self._day_levels(closed, now_ms)
        hourly_high, hourly_low = self._hourly_swings(state.candles_1h, now_ms)
        high_touches = self._touch_count(
            closed[-60:], breakout_high, atr_value, high=True
        ) if atr_ok else 0
        low_touches = self._touch_count(
            closed[-60:], breakout_low, atr_value, high=False
        ) if atr_ok else 0
        day_high_touches = self._touch_count(closed[-1440:], day_high, atr_value, high=True) if atr_ok else 0
        day_low_touches = self._touch_count(closed[-1440:], day_low, atr_value, high=False) if atr_ok else 0
        hourly_high_touches = self._touch_count(closed[-240:], hourly_high, atr_value, high=True) if atr_ok else 0
        hourly_low_touches = self._touch_count(closed[-240:], hourly_low, atr_value, high=False) if atr_ok else 0
        recent_spreads = [
            value
            for timestamp, value in state.spread_history
            if timestamp >= now_ms - self.cfg.strategy.spread_baseline_window_sec * 1000
        ]
        spread_baseline = statistics.median(recent_spreads) if recent_spreads else book_metrics.spread_bps
        spread_expansion = book_metrics.spread_bps / max(spread_baseline, 1e-9)
        derivative = self.derivatives.get(symbol, (0.0, 0.0, 0, False))
        derivative_age = now_ms - derivative[2] if derivative[2] else 2**31 - 1
        leader_return, correlation = self._leader_context(symbol, now_ms)
        data_age = max(
            now_ms - (book.timestamp_ms if book else 0),
            now_ms - (state.trades[-1].timestamp_ms if state.trades else 0),
        )
        ready = bool(
            book
            and state.trades
            and len(closed) >= self.cfg.strategy.ema_slow + 1
            and atr_ok
            and finite(rsi_value)
            and data_age <= self.cfg.market.stale_after_ms
        )
        reject = ""
        if not book:
            reject = "нет стакана"
        elif not state.trades:
            reject = "нет ленты сделок"
        elif len(closed) < self.cfg.strategy.ema_slow + 1:
            reject = f"прогрев свечей {len(closed)}/{self.cfg.strategy.ema_slow + 1}"
        elif data_age > self.cfg.market.stale_after_ms:
            reject = f"устаревшие данные {data_age} мс"
        elif not atr_ok:
            reject = "ATR не готов"
        return FeatureVector(
            symbol=symbol,
            timestamp_ms=now_ms,
            ready=ready,
            bid=book.best_bid if book else price,
            ask=book.best_ask if book else price,
            mid=price,
            data_age_ms=max(data_age, 0),
            spread_bps=book_metrics.spread_bps,
            atr=atr_value if atr_ok else 0.0,
            atr_pct=atr_value / price * 100.0 if atr_ok else 0.0,
            ema_fast=fast if finite(fast) else 0.0,
            ema_slow=slow if finite(slow) else 0.0,
            trend_atr=trend_atr,
            breakout_high=breakout_high,
            breakout_low=breakout_low,
            impulse_atr=impulse_atr,
            flow_imbalance=flow.flow_imbalance,
            volume_ratio=flow.volume_ratio,
            recent_trade_count=flow.recent_trade_count,
            recent_quote_volume=flow.recent_quote_volume,
            book_imbalance=book_metrics.imbalance,
            microprice_bias_bps=book_metrics.microprice_bias_bps,
            bid_wall_ratio=book_metrics.bid_wall_ratio,
            ask_wall_ratio=book_metrics.ask_wall_ratio,
            rsi=rsi_value if finite(rsi_value) else 50.0,
            extension_atr=extension_atr,
            mtf_trend_15m_atr=trend_15m,
            mtf_trend_1h_atr=trend_1h,
            mtf_ready=ready_15m and ready_1h,
            leader_return_bps=leader_return,
            leader_correlation=correlation,
            open_interest_change_pct=derivative[0],
            funding_rate=derivative[1],
            derivatives_ready=bool(
                not self.cfg.derivatives.enabled
                or (
                    derivative[3]
                    and derivative_age <= self.cfg.derivatives.max_staleness_sec * 1000
                )
            ),
            derivatives_age_ms=max(derivative_age, 0),
            bid_wall_age_ms=book_metrics.bid_wall_age_ms,
            ask_wall_age_ms=book_metrics.ask_wall_age_ms,
            bid_wall_confidence=book_metrics.bid_wall_confidence,
            ask_wall_confidence=book_metrics.ask_wall_confidence,
            spoof_score=book_metrics.spoof_score,
            cvd_quote=micro.cvd_quote,
            cvd_divergence=micro.cvd_divergence,
            absorption_side=micro.absorption_side,
            footprint_buy_ratio=micro.footprint_buy_ratio,
            footprint_sell_ratio=micro.footprint_sell_ratio,
            initiative_buy_zone=micro.initiative_buy_zone,
            initiative_sell_zone=micro.initiative_sell_zone,
            tape_ticks_per_sec=micro.tape_ticks_per_sec,
            tape_velocity_ratio=micro.tape_velocity_ratio,
            micro_swing_high=micro.micro_swing_high,
            micro_swing_low=micro.micro_swing_low,
            day_high=day_high,
            day_low=day_low,
            hourly_swing_high=hourly_high,
            hourly_swing_low=hourly_low,
            day_high_touch_count=day_high_touches,
            day_low_touch_count=day_low_touches,
            hourly_high_touch_count=hourly_high_touches,
            hourly_low_touch_count=hourly_low_touches,
            high_touch_count=high_touches,
            low_touch_count=low_touches,
            spread_baseline_bps=spread_baseline,
            spread_expansion_ratio=spread_expansion,
            spread_baseline_ready=len(recent_spreads) >= self.cfg.strategy.min_spread_samples,
            news_blocked=self.news_blocked,
            news_reason=self.news_reason,
            reject_reason=reject,
        )

    def _higher_trend(self, candles: deque[Candle], now_ms: int) -> tuple[float, bool]:
        closed = [item for item in candles if item.is_closed(now_ms)]
        closes = [item.close for item in closed]
        value_atr = atr(closed, self.cfg.strategy.atr_period)
        fast = ema(closes, self.cfg.strategy.mtf_ema_fast)
        slow = ema(closes, self.cfg.strategy.mtf_ema_slow)
        ready = finite(value_atr) and value_atr > 0 and finite(fast) and finite(slow)
        return ((fast - slow) / value_atr if ready else 0.0), ready

    @staticmethod
    def _day_levels(candles: list[Candle], now_ms: int) -> tuple[float, float]:
        moment = time.gmtime(now_ms / 1000.0)
        day_start_ms = calendar.timegm(
            (moment.tm_year, moment.tm_mon, moment.tm_mday, 0, 0, 0, 0, 0, 0)
        ) * 1000
        today = [item for item in candles if item.open_time_ms >= day_start_ms]
        return (
            max((item.high for item in today), default=0.0),
            min((item.low for item in today), default=0.0),
        )

    @staticmethod
    def _hourly_swings(candles: deque[Candle], now_ms: int) -> tuple[float, float]:
        values = [item for item in candles if item.is_closed(now_ms)]
        swing_high = 0.0
        swing_low = 0.0
        for index in range(2, len(values) - 2):
            center = values[index]
            nearby = values[index - 2 : index] + values[index + 1 : index + 3]
            if center.high > max(item.high for item in nearby):
                swing_high = center.high
            if center.low < min(item.low for item in nearby):
                swing_low = center.low
        if not swing_high and values:
            swing_high = max(item.high for item in values[-24:])
        if not swing_low and values:
            swing_low = min(item.low for item in values[-24:])
        return swing_high, swing_low

    def _touch_count(
        self, candles: list[Candle], level: float, atr_value: float, *, high: bool
    ) -> int:
        if level <= 0 or atr_value <= 0:
            return 0
        tolerance = self.cfg.strategy.level_touch_tolerance_atr * atr_value
        touches = 0
        last_index = -3
        for index, candle in enumerate(candles):
            tested = abs((candle.high if high else candle.low) - level) <= tolerance
            if tested and index - last_index >= 2:
                touches += 1
                last_index = index
        return touches

    def _leader_context(self, symbol: str, now_ms: int) -> tuple[float, float]:
        leader = self.states.get(self.cfg.strategy.leader_symbol)
        current = self.states.get(symbol)
        if leader is None or current is None:
            return 0.0, 0.0
        cutoff = now_ms - self.cfg.strategy.impulse_window_sec * 1000
        leader_ticks = [tick for tick in leader.trades if tick.timestamp_ms >= cutoff]
        leader_return = (
            (leader_ticks[-1].price / leader_ticks[0].price - 1.0) * 10_000.0
            if len(leader_ticks) >= 2 and leader_ticks[0].price > 0
            else 0.0
        )
        if symbol == leader.symbol:
            return leader_return, 1.0
        current_map = {
            item.open_time_ms: item.close
            for item in current.candles_1m
            if item.is_closed(now_ms)
        }
        leader_map = {
            item.open_time_ms: item.close
            for item in leader.candles_1m
            if item.is_closed(now_ms)
        }
        shared = sorted(set(current_map) & set(leader_map))[-31:]
        now_current = [current_map[key] for key in shared]
        now_leader = [leader_map[key] for key in shared]
        return leader_return, pearson_correlation(returns(now_current), returns(now_leader))


MarketCallback = Callable[[str], Awaitable[None]]


class BingXMarketStream:
    def __init__(
        self,
        cfg: AppSettings,
        symbols: tuple[str, ...],
        store: MarketDataStore,
        on_update: MarketCallback,
    ) -> None:
        self.cfg = cfg
        self.symbols = symbols
        self.store = store
        self.on_update = on_update
        self.connected = False
        self.last_message_ms = 0
        self._updates: asyncio.Queue[str] = asyncio.Queue(maxsize=max(len(symbols), 1))
        self._pending: set[str] = set()
        self._dirty: set[str] = set()

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    self.cfg.api.ws_url,
                    ping_interval=None,
                    open_timeout=10,
                    close_timeout=5,
                    max_size=4 * 1024 * 1024,
                ) as socket:
                    self.connected = True
                    backoff = 1.0
                    await self._subscribe(socket)
                    LOGGER.info("BingX market WebSocket connected for %d symbols", len(self.symbols))
                    worker = asyncio.create_task(self._update_worker(), name="market-evaluator")
                    try:
                        async for raw in socket:
                            message = decode_ws_message(raw)
                            self.last_message_ms = utc_ms()
                            if message == "Ping":
                                await socket.send("Pong")
                                continue
                            try:
                                payload = json.loads(message)
                            except json.JSONDecodeError:
                                LOGGER.debug("Ignored non-JSON WebSocket payload: %.120s", message)
                                continue
                            if str(payload.get("ping", "")):
                                await socket.send(json.dumps({"pong": payload["ping"]}))
                                continue
                            symbol = self.store.ingest(payload, self.last_message_ms)
                            if symbol:
                                self._schedule_update(symbol)
                    finally:
                        worker.cancel()
                        await asyncio.gather(worker, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                LOGGER.warning("BingX WebSocket disconnected: %s; retry in %.1fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    def _schedule_update(self, symbol: str) -> None:
        # Coalesce bursts by symbol. WebSocket ingestion never waits for REST
        # order handling, while one worker serializes position/risk decisions.
        if symbol in self._pending:
            self._dirty.add(symbol)
            return
        self._pending.add(symbol)
        self._updates.put_nowait(symbol)

    async def _update_worker(self) -> None:
        while True:
            symbol = await self._updates.get()
            try:
                await self.on_update(symbol)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("Market evaluation failed for %s", symbol)
            finally:
                if symbol in self._dirty:
                    self._dirty.discard(symbol)
                    self._updates.put_nowait(symbol)
                else:
                    self._pending.discard(symbol)
                self._updates.task_done()

    async def _subscribe(self, socket: Any) -> None:
        channels: list[str] = []
        for symbol in self.symbols:
            depth_interval = (
                self.cfg.market.depth_interval
                if symbol in {"BTC-USDT", "ETH-USDT"}
                else "500ms"
            )
            channels.extend(
                (
                    f"{symbol}@trade",
                    f"{symbol}@depth{self.cfg.market.depth_levels}@{depth_interval}",
                    f"{symbol}@kline_1m",
                    f"{symbol}@kline_15m",
                    f"{symbol}@kline_1h",
                    f"{symbol}@markPrice",
                )
            )
        # BingX expects one dataType string per subscription request.
        for channel in channels:
            request = {
                "id": str(uuid.uuid4()),
                "reqType": "sub",
                "dataType": channel,
            }
            await socket.send(json.dumps(request, separators=(",", ":")))
            await asyncio.sleep(0.03)


async def warmup_market(
    client: BingXRestClient | None,
    store: MarketDataStore,
    symbols: tuple[str, ...],
) -> None:
    if client is None:
        LOGGER.warning("REST warm-up skipped: API credentials are absent")
        return
    requests = [(symbol, interval) for symbol in symbols for interval in ("1m", "15m", "1h")]
    for index, (symbol, interval) in enumerate(requests):
        try:
            limit = (
                store.cfg.market.day_history_candles
                if interval == "1m"
                else store.cfg.market.warmup_candles
            )
            rows = await client.get_klines(symbol, interval, limit)
            store.load_history(symbol, rows, interval)
        except Exception as exc:
            LOGGER.warning("Kline warm-up failed for %s %s: %s", symbol, interval, exc)
        if index + 1 < len(requests):
            await asyncio.sleep(1.05)  # Official kline limit: one request/sec per IP.
    if requests:
        await asyncio.sleep(1.05)

# ==================== news.py ====================
import asyncio
import json
import logging
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta



LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class EconomicEvent:
    timestamp_ms: int
    country: str
    event: str
    importance: int


class EconomicCalendarService:
    def __init__(self, settings: NewsSettings, store: MarketDataStore, mode: str) -> None:
        self.settings = settings
        self.store = store
        self.mode = mode
        self.api_key = os.getenv("ECONOMIC_CALENDAR_API_KEY", "").strip()
        self.events: list[EconomicEvent] = []
        self.last_success_ms = 0
        self.last_error = ""

    async def run(self) -> None:
        if not self.settings.enabled:
            self.store.update_news(False, "news filter disabled")
            return
        while True:
            try:
                if not self.api_key:
                    self.last_error = "не задан ECONOMIC_CALENDAR_API_KEY"
                else:
                    self.events = await asyncio.to_thread(self._fetch)
                    self.last_success_ms = int(time.time() * 1000)
                    self.last_error = ""
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"календарь недоступен: {type(exc).__name__}"
                LOGGER.warning("Economic calendar refresh failed (%s)", type(exc).__name__)
            self._publish_status()
            await asyncio.sleep(self.settings.refresh_interval_sec)

    def _fetch(self) -> list[EconomicEvent]:
        today = datetime.now(UTC).date()
        start = today - timedelta(days=1)
        end = today + timedelta(days=1)
        key = urllib.parse.quote(self.api_key, safe=":")
        url = (
            "https://api.tradingeconomics.com/calendar/country/All/"
            f"{start.isoformat()}/{end.isoformat()}?c={key}"
            f"&importance={self.settings.min_importance}&f=json"
        )
        request = urllib.request.Request(url, headers={"User-Agent": "bingx-impulse-bot/0.1"})
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, socket.timeout, TimeoutError) as exc:
            raise RuntimeError("calendar request failed") from exc
        allowed = {item.casefold() for item in self.settings.countries}
        events: list[EconomicEvent] = []
        for row in payload if isinstance(payload, list) else []:
            country = str(row.get("Country", ""))
            importance = int(row.get("Importance", 0) or 0)
            if country.casefold() not in allowed or importance < self.settings.min_importance:
                continue
            timestamp = self._parse_timestamp(str(row.get("Date", "")))
            if timestamp is None:
                continue
            events.append(
                EconomicEvent(
                    timestamp_ms=int(timestamp.timestamp() * 1000),
                    country=country,
                    event=str(row.get("Event") or row.get("Category") or "macro event"),
                    importance=importance,
                )
            )
        return sorted(events, key=lambda item: item.timestamp_ms)

    @staticmethod
    def _parse_timestamp(value: str) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)

    def block_status(self, now_ms: int | None = None) -> tuple[bool, str]:
        now_ms = now_ms or int(time.time() * 1000)
        if not self.settings.enabled:
            return False, ""
        unavailable = not self.api_key or not self.last_success_ms
        stale = self.last_success_ms and (
            now_ms - self.last_success_ms > self.settings.max_staleness_sec * 1000
        )
        if unavailable or stale:
            reason = self.last_error or "экономический календарь устарел"
            fail_closed = self.settings.fail_closed_live and self.mode in {"vst", "live"}
            return fail_closed, reason
        before_ms = self.settings.blackout_before_min * 60_000
        after_ms = self.settings.blackout_after_min * 60_000
        for event in self.events:
            if event.timestamp_ms - before_ms <= now_ms <= event.timestamp_ms + after_ms:
                moment = datetime.fromtimestamp(event.timestamp_ms / 1000.0, UTC).strftime("%H:%M UTC")
                return True, f"новости: {event.country} · {event.event} · {moment}"
        return False, ""

    def _publish_status(self) -> None:
        blocked, reason = self.block_status()
        self.store.update_news(blocked, reason)

# ==================== derivatives.py ====================
import asyncio
import logging
import time
from collections import defaultdict, deque



LOGGER = logging.getLogger(__name__)


class DerivativesContextService:
    def __init__(
        self,
        settings: DerivativesSettings,
        client: BingXRestClient | None,
        store: MarketDataStore,
        symbols: tuple[str, ...],
    ) -> None:
        self.settings = settings
        self.client = client
        self.store = store
        self.symbols = symbols
        self.history: dict[str, deque[tuple[int, float]]] = defaultdict(
            lambda: deque(maxlen=100)
        )
        self.last_error = ""

    async def run(self) -> None:
        if not self.settings.enabled:
            return
        if self.client is None:
            self.last_error = "нет REST-доступа для OI/фандинга"
            LOGGER.warning("Derivatives context disabled: API credentials are absent")
            return
        while True:
            cycle_started = time.monotonic()
            for symbol in self.symbols:
                try:
                    await self._refresh_symbol(symbol)
                    self.last_error = ""
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.last_error = f"ошибка OI/funding: {type(exc).__name__}"
                    LOGGER.warning("Derivatives refresh failed for %s: %s", symbol, exc)
                await asyncio.sleep(1.05)
            elapsed = time.monotonic() - cycle_started
            await asyncio.sleep(max(self.settings.refresh_interval_sec - elapsed, 1.0))

    async def _refresh_symbol(self, symbol: str) -> None:
        assert self.client is not None
        oi_data = await self.client.get_open_interest(symbol)
        await asyncio.sleep(1.05)
        premium = await self.client.get_premium_index(symbol)
        now_ms = int(time.time() * 1000)
        oi = float(oi_data.get("openInterest", 0.0) or 0.0)
        funding = float(
            premium.get("lastFundingRate", premium.get("fundingRate", 0.0)) or 0.0
        )
        history = self.history[symbol]
        cutoff = now_ms - self.settings.history_window_sec * 1000
        while history and history[0][0] < cutoff:
            history.popleft()
        if oi > 0:
            history.append((now_ms, oi))
        change = 0.0
        ready = len(history) >= 2 and history[0][1] > 0
        if ready:
            change = (history[-1][1] / history[0][1] - 1.0) * 100.0
        source_times = [
            int(value)
            for value in (oi_data.get("time"), premium.get("time"))
            if value is not None and str(value).isdigit()
        ]
        source_time = min(source_times) if source_times else now_ms
        ready = ready and now_ms - source_time <= self.settings.max_staleness_sec * 1000
        self.store.update_derivatives(
            symbol,
            open_interest_change_pct=change,
            funding_rate=funding,
            timestamp_ms=source_time,
            ready=ready,
        )

# ==================== telegram.py ====================
import asyncio
import json
import logging
import mimetypes
import os
import socket
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path



LOGGER = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, settings: TelegramSettings) -> None:
        self.settings = settings
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.enabled = bool(settings.enabled and self.token and self.chat_id)
        if settings.enabled and not self.enabled:
            LOGGER.warning("Telegram disabled: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing")

    async def send_message(self, text: str) -> bool:
        if not self.enabled:
            return False
        return await asyncio.to_thread(self._send_message_sync, text[:4096])

    async def send_document(self, path: Path, caption: str) -> bool:
        if not self.enabled:
            return False
        return await asyncio.to_thread(self._send_document_sync, path, caption[:1024])

    def _send_message_sync(self, text: str) -> bool:
        data = urllib.parse.urlencode({"chat_id": self.chat_id, "text": text}).encode("utf-8")
        return self._request("sendMessage", data, "application/x-www-form-urlencoded")

    def _send_document_sync(self, path: Path, caption: str) -> bool:
        boundary = f"----ImpulseBot{uuid.uuid4().hex}"
        filename = path.name.replace('"', "")
        mime = mimetypes.guess_type(filename)[0] or "text/csv"
        chunks = [
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n{self.chat_id}\r\n".encode(),
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"caption\"\r\n\r\n{caption}\r\n".encode("utf-8"),
            (
                f"--{boundary}\r\nContent-Disposition: form-data; name=\"document\"; "
                f"filename=\"{filename}\"\r\nContent-Type: {mime}\r\n\r\n"
            ).encode("utf-8"),
            path.read_bytes(),
            f"\r\n--{boundary}--\r\n".encode(),
        ]
        return self._request("sendDocument", b"".join(chunks), f"multipart/form-data; boundary={boundary}")

    def _request(self, method: str, body: bytes, content_type: str) -> bool:
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{self.token}/{method}",
            data=body,
            method="POST",
            headers={"Content-Type": content_type, "User-Agent": "bingx-impulse-bot/0.1"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.settings.request_timeout_sec) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not payload.get("ok"):
                LOGGER.warning("Telegram %s returned ok=false", method)
                return False
            return True
        except urllib.error.HTTPError as exc:
            LOGGER.warning("Telegram %s failed with HTTP %s", method, exc.code)
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError):
            LOGGER.warning("Telegram %s network request failed", method)
        return False


def open_message(position: Position, signal: Signal, sizing: SizingResult, equity: float) -> str:
    risk_pct = sizing.estimated_worst_loss / equity * 100.0 if equity > 0 else 0.0
    return (
        f"ОТКРЫТА СДЕЛКА · {position.mode.upper()}\n"
        f"{position.symbol} · {position.side.value}\n"
        f"Вход: {position.entry_price:.8g}\n"
        f"Stop Loss: {position.stop_price:.8g}\n"
        f"Take Profit: {position.target_price:.8g} · RR {signal.target_rr:.2f}\n"
        f"Количество: {position.quantity:.8g} · Номинал: {sizing.notional_usdt:.2f} USDT\n"
        f"Макс. расчётный риск: {sizing.estimated_worst_loss:.2f} USDT ({risk_pct:.2f}%)\n"
        f"Сила сетапа: {signal.strength:.0f}/100"
    )


def close_message(event: BrokerEvent, mode: str, equity: float) -> str:
    outcome = "ПРИБЫЛЬ" if event.pnl > 0 else "УБЫТОК" if event.pnl < 0 else "БЕЗУБЫТОК"
    return (
        f"ЗАКРЫТА СДЕЛКА · {mode.upper()} · {outcome}\n"
        f"{event.symbol}\n"
        f"Выход: {event.price:.8g} · причина: {event.reason}\n"
        f"Чистый PnL: {event.pnl:+.2f} USDT · {event.r_multiple:+.2f}R\n"
        f"Текущий equity: {equity:.2f} USDT"
    )


def stats_message(
    stats: PerformanceStats,
    *,
    mode: str,
    equity: float,
    open_positions: int,
    daily_loss_limit: float,
) -> str:
    factor = "∞" if stats.profit_factor is None and stats.gross_profit > 0 else (
        "—" if stats.profit_factor is None else f"{stats.profit_factor:.2f}"
    )
    return (
        f"СТАТИСТИКА · {mode.upper()} · {stats.period}\n"
        f"Закрыто: {stats.trades} · W/L/BE: {stats.wins}/{stats.losses}/{stats.breakeven}\n"
        f"Win rate: {stats.win_rate_pct:.1f}% · Profit factor: {factor}\n"
        f"Чистый PnL: {stats.net_pnl:+.2f} USDT · средний результат: {stats.average_r:+.2f}R\n"
        f"Макс. просадка периода: {stats.max_drawdown:.2f} USDT\n"
        f"Equity: {equity:.2f} USDT · открыто: {open_positions}\n"
        f"Дневной стоп: {daily_loss_limit:.2f} USDT"
    )


def heartbeat_message(
    *,
    mode: str,
    connected: bool,
    symbols: int,
    open_positions: int,
    best: ScanDiagnostic | None,
    risk_block: str,
) -> str:
    market = "подключён" if connected else "переподключение"
    candidate = "кандидатов пока нет"
    if best:
        direction = f" {best.side.value}" if best.side else ""
        candidate = f"лучший: {best.symbol}{direction}, {best.score:.0f}/100 — {best.reason}"
    risk = f" · торговля заблокирована: {risk_block}" if risk_block else ""
    return (
        f"БОТ ОНЛАЙН · {mode.upper()}\n"
        f"Рынок: {market} · сканируется: {symbols} · открыто: {open_positions}\n"
        f"{candidate}{risk}\n"
        "Сделка не открывается без полного подтверждения сетапа."
    )


class TelegramSchedule:
    def __init__(self, settings: TelegramSettings, notifier: TelegramNotifier, journal: TradeJournal) -> None:
        self.settings = settings
        self.notifier = notifier
        self.journal = journal

    def due_stats_keys(self, now: datetime | None = None) -> list[str]:
        now = now or datetime.now(UTC)
        current_minutes = now.hour * 60 + now.minute
        due: list[str] = []
        for report_time in self.settings.stats_times_utc:
            hour, minute = (int(part) for part in report_time.split(":"))
            key = f"stats:{now.date().isoformat()}:{report_time}"
            if current_minutes >= hour * 60 + minute and self.journal.get_meta(key) != "sent":
                due.append(key)
        return due

    def mark_stats_sent(self, key: str) -> None:
        self.journal.set_meta(key, "sent")

    def heartbeat_due(self, now_ms: int) -> bool:
        previous = int(self.journal.get_meta("last_heartbeat_ms", "0"))
        return now_ms - previous >= self.settings.heartbeat_interval_minutes * 60_000

    def mark_heartbeat_sent(self, now_ms: int) -> None:
        self.journal.set_meta("last_heartbeat_ms", str(now_ms))

# ==================== broker.py ====================
import asyncio
import json
import logging
import os
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any



LOGGER = logging.getLogger(__name__)


class SafetyLockError(RuntimeError):
    pass


class ExecutionSafetyError(RuntimeError):
    def __init__(self, message: str, position: Position) -> None:
        super().__init__(message)
        self.position = position


class Broker(ABC):
    mode: str
    positions: dict[str, Position]
    equity: float

    @abstractmethod
    async def open(self, signal: Signal, sizing: SizingResult) -> Position:
        raise NotImplementedError

    @abstractmethod
    async def on_market(
        self, symbol: str, bid: float, ask: float, timestamp_ms: int
    ) -> list[BrokerEvent]:
        raise NotImplementedError

    async def reconcile(self, prices: dict[str, float], timestamp_ms: int) -> list[BrokerEvent]:
        return []

    async def initialize(self, restored: list[Position]) -> None:
        self.positions = {position.symbol: position for position in restored}

    @property
    def total_notional(self) -> float:
        return sum(position.quantity * position.entry_price for position in self.positions.values())


class PaperBroker(Broker):
    def __init__(
        self,
        settings: ExecutionSettings,
        initial_equity: float,
        specs: dict[str, ContractSpec] | None = None,
    ) -> None:
        self.settings = settings
        self.specs = specs or {}
        self.mode = "paper"
        self.equity = initial_equity
        self.positions: dict[str, Position] = {}

    async def open(self, signal: Signal, sizing: SizingResult) -> Position:
        if signal.symbol in self.positions:
            raise ValueError(f"position already open for {signal.symbol}")
        slip = self.settings.assumed_slippage_bps / 10_000.0
        entry = signal.entry_price * (1.0 + signal.side.direction * slip)
        entry_fee = entry * sizing.quantity * self._taker_fee(signal.symbol)
        position = Position(
            position_id=uuid.uuid4().hex,
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            side=signal.side,
            mode=self.mode,
            quantity=sizing.quantity,
            entry_price=entry,
            stop_price=signal.stop_price,
            target_price=signal.target_price,
            opened_at_ms=signal.created_at_ms,
            initial_risk_cash=sizing.estimated_worst_loss,
            entry_fee=entry_fee,
            highest_price=entry,
            lowest_price=entry,
            metadata={"setup_type": signal.setup_type, "strength": signal.strength},
        )
        self.positions[signal.symbol] = position
        return position

    async def on_market(
        self, symbol: str, bid: float, ask: float, timestamp_ms: int
    ) -> list[BrokerEvent]:
        position = self.positions.get(symbol)
        if position is None or bid <= 0 or ask <= 0:
            return []
        executable = bid if position.side.direction > 0 else ask
        reason = self._update_and_exit_reason(position, executable, timestamp_ms)
        if not reason:
            return []
        return [self._close(position, executable, timestamp_ms, reason)]

    def _update_and_exit_reason(
        self, position: Position, price: float, timestamp_ms: int
    ) -> str:
        direction = position.direction
        position.highest_price = max(position.highest_price, price)
        position.lowest_price = min(position.lowest_price, price)
        favorable = (
            position.highest_price - position.entry_price
            if direction > 0
            else position.entry_price - position.lowest_price
        )
        adverse = (
            position.entry_price - position.lowest_price
            if direction > 0
            else position.highest_price - position.entry_price
        )
        risk = max(position.unit_risk, 1e-12)
        position.mfe_r = max(position.mfe_r, favorable / risk)
        position.mae_r = max(position.mae_r, adverse / risk)
        if position.mfe_r >= self.settings.trailing_activation_r:
            candidate = price - direction * self.settings.trailing_distance_r * risk
            if position.trailing_stop is None:
                position.trailing_stop = candidate
            elif direction > 0:
                position.trailing_stop = max(position.trailing_stop, candidate)
            else:
                position.trailing_stop = min(position.trailing_stop, candidate)
        effective_stop = position.stop_price
        if position.trailing_stop is not None:
            effective_stop = (
                max(effective_stop, position.trailing_stop)
                if direction > 0
                else min(effective_stop, position.trailing_stop)
            )
        if direction * (price - effective_stop) <= 0:
            return "trailing_stop" if position.trailing_stop is not None else "stop_loss"
        if direction * (price - position.target_price) >= 0:
            return "take_profit"
        held_sec = (timestamp_ms - position.opened_at_ms) / 1000.0
        if held_sec >= self.settings.max_hold_sec:
            return "max_hold_time"
        if (
            held_sec >= self.settings.no_progress_exit_sec
            and position.mfe_r < self.settings.no_progress_min_mfe_r
        ):
            return "no_progress"
        return ""

    def _close(
        self, position: Position, market_price: float, timestamp_ms: int, reason: str
    ) -> BrokerEvent:
        slip = self.settings.assumed_slippage_bps / 10_000.0
        exit_price = market_price * (1.0 - position.direction * slip)
        gross = position.direction * (exit_price - position.entry_price) * position.quantity
        exit_fee = exit_price * position.quantity * self._taker_fee(position.symbol)
        total_fees = position.entry_fee + exit_fee
        net = gross - total_fees
        position.realized_pnl = net
        self.equity += net
        self.positions.pop(position.symbol, None)
        return BrokerEvent(
            kind="closed",
            timestamp_ms=timestamp_ms,
            symbol=position.symbol,
            position_id=position.position_id,
            pnl=net,
            r_multiple=net / max(position.initial_risk_cash, 1e-12),
            price=exit_price,
            reason=reason,
            payload={"gross_pnl": gross, "total_fees": total_fees},
        )

    def _taker_fee(self, symbol: str) -> float:
        spec = self.specs.get(symbol)
        return max(spec.taker_fee_rate, self.settings.taker_fee_rate) if spec else self.settings.taker_fee_rate


class BingXBroker(PaperBroker):
    """VST/LIVE broker: server-side SL/TP plus local trailing/time exits."""

    def __init__(
        self,
        mode: str,
        settings: ExecutionSettings,
        client: BingXRestClient,
        initial_equity: float,
        specs: dict[str, ContractSpec] | None = None,
    ) -> None:
        self._enforce_locks(mode)
        super().__init__(settings, initial_equity, specs)
        self.mode = mode
        self.client = client
        self.close_requested: set[str] = set()
        self.configured_symbols: set[str] = set()

    @staticmethod
    def _enforce_locks(mode: str) -> None:
        if os.getenv("ALLOW_EXPERIMENTAL_EXCHANGE_EXECUTION", "") != "YES":
            raise SafetyLockError("Set ALLOW_EXPERIMENTAL_EXCHANGE_EXECUTION=YES")
        if mode == "live" and os.getenv("LIVE_TRADING_ACK", "") != "I_ACCEPT_REAL_MONEY_RISK":
            raise SafetyLockError("LIVE_TRADING_ACK is missing or incorrect")

    async def refresh_equity(self) -> float:
        balances = await self.client.get_balance()
        for row in balances:
            if str(row.get("asset", "")).upper() == "USDT":
                value = float(row.get("equity", row.get("balance", 0.0)) or 0.0)
                if value > 0:
                    self.equity = value
                    return value
        raise RuntimeError("USDT futures equity not found")

    async def initialize(self, restored: list[Position]) -> None:
        await self.refresh_equity()
        if await self.client.get_position_mode():
            raise SafetyLockError("BingX account must be in one-way position mode")
        journal_by_symbol = {
            position.symbol: position for position in restored if position.mode == self.mode
        }
        exchange_rows = await self.client.get_positions()
        active_rows = {
            str(row.get("symbol", "")): row
            for row in exchange_rows
            if abs(float(row.get("positionAmt", 0.0) or 0.0)) > 0
        }
        unexpected = sorted(set(active_rows) - set(journal_by_symbol))
        if unexpected:
            raise SafetyLockError(
                "Untracked exchange positions exist: " + ", ".join(unexpected)
            )
        for symbol in set(active_rows) & set(journal_by_symbol):
            expected = journal_by_symbol[symbol].quantity
            actual = abs(float(active_rows[symbol].get("positionAmt", 0.0) or 0.0))
            if abs(actual - expected) > max(expected * 0.01, 1e-12):
                raise SafetyLockError(f"Position quantity mismatch for {symbol}")
            actual_side = str(active_rows[symbol].get("positionSide", "")).upper()
            if actual_side in {"LONG", "SHORT"} and actual_side != journal_by_symbol[symbol].side.value:
                raise SafetyLockError(f"Position side mismatch for {symbol}")
        self.positions = journal_by_symbol

    async def open(self, signal: Signal, sizing: SizingResult) -> Position:
        if signal.symbol in self.positions:
            raise ValueError(f"position already open for {signal.symbol}")
        if signal.symbol not in self.configured_symbols:
            if await self.client.get_margin_type(signal.symbol) != "ISOLATED":
                await self.client.set_margin_type(signal.symbol, "ISOLATED")
                await asyncio.sleep(0.55)
            await self.client.set_leverage(signal.symbol, self.settings.leverage)
            self.configured_symbols.add(signal.symbol)
        client_id = f"ib{signal.signal_id[:30]}"
        stop_loss = {
            "type": "STOP_MARKET",
            "stopPrice": signal.stop_price,
            "workingType": "MARK_PRICE",
            "stopGuaranteed": False,
        }
        take_profit = {
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": signal.target_price,
            "workingType": "MARK_PRICE",
            "stopGuaranteed": False,
        }
        order = await self.client.place_order(
            {
                "symbol": signal.symbol,
                "side": signal.side.entry_order_side,
                "positionSide": "BOTH",
                "type": "MARKET",
                "quantity": sizing.quantity,
                "clientOrderId": client_id,
                "stopLoss": stop_loss,
                "takeProfit": take_profit,
            }
        )
        order_id = str(order.get("orderID") or order.get("orderId") or "")
        detail: dict[str, Any] = {}
        status = str(order.get("status", ""))
        for delay in (0.20, 0.35, 0.60, 1.0):
            await asyncio.sleep(delay)
            try:
                detail = await self.client.query_order(
                    signal.symbol,
                    order_id=order_id or None,
                    client_order_id=None if order_id else client_id,
                )
            except Exception:
                continue
            status = str(detail.get("status", status))
            if status == "FILLED":
                break
            if status in {"CANCELED", "EXPIRED"}:
                raise RuntimeError(f"entry order ended with {status}")
        filled_qty = float(detail.get("executedQty", detail.get("origQty", 0.0)) or 0.0)
        entry = float(detail.get("avgPrice", 0.0) or 0.0)
        if status != "FILLED" or filled_qty <= 0:
            rows = await self.client.get_positions(signal.symbol)
            active = next(
                (
                    row
                    for row in rows
                    if str(row.get("symbol", signal.symbol)) == signal.symbol
                    and abs(float(row.get("positionAmt", 0.0) or 0.0)) > 0
                ),
                None,
            )
            if active is None:
                raise RuntimeError(f"entry fill could not be confirmed (status={status or 'unknown'})")
            filled_qty = abs(float(active.get("positionAmt", 0.0) or 0.0))
            entry = float(active.get("avgPrice", signal.entry_price) or signal.entry_price)
        if entry <= 0:
            entry = signal.entry_price
        entry_fee = entry * filled_qty * self._taker_fee(signal.symbol)
        actual_unit_risk = signal.side.direction * (entry - signal.stop_price)
        actual_reward = signal.side.direction * (signal.target_price - entry)
        actual_rr = actual_reward / actual_unit_risk if actual_unit_risk > 0 else 0.0
        actual_risk_cash = filled_qty * max(actual_unit_risk, 0.0) + entry_fee
        position = Position(
            position_id=uuid.uuid4().hex,
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            side=signal.side,
            mode=self.mode,
            quantity=filled_qty,
            entry_price=entry,
            stop_price=signal.stop_price,
            target_price=signal.target_price,
            opened_at_ms=int(detail.get("updateTime", signal.created_at_ms) or signal.created_at_ms),
            initial_risk_cash=max(sizing.estimated_worst_loss, actual_risk_cash),
            entry_fee=entry_fee,
            exchange_order_id=order_id,
            highest_price=entry,
            lowest_price=entry,
            metadata={
                "client_order_id": client_id,
                "server_side_stop": True,
                "server_side_take_profit": True,
                "setup_type": signal.setup_type,
            },
        )
        self.positions[signal.symbol] = position
        if actual_unit_risk <= 0 or actual_rr < 1.5:
            await self._emergency_close(position, "fill_invalidated_rr")
            raise ExecutionSafetyError(
                f"actual fill invalidated RR ({actual_rr:.2f}); emergency close requested",
                position,
            )
        try:
            protected = await self._protective_orders_present(signal.symbol)
        except Exception:
            protected = False
        if not protected:
            await self._emergency_close(position, "protective_orders_missing")
            raise ExecutionSafetyError(
                "attached Stop Loss / Take Profit could not be verified; emergency close requested",
                position,
            )
        return position

    async def _emergency_close(self, position: Position, reason: str) -> None:
        try:
            response = await self.client.close_market(
                position.symbol, position.side.exit_order_side, position.quantity
            )
            position.metadata["exit_order_id"] = str(
                response.get("orderID") or response.get("orderId") or ""
            )
        except Exception as exc:
            position.metadata["emergency_close_error"] = type(exc).__name__
        finally:
            position.metadata["requested_exit_reason"] = reason
            position.metadata["exit_requested_at_ms"] = int(time.time() * 1000)
            self.close_requested.add(position.symbol)

    async def _protective_orders_present(self, symbol: str) -> bool:
        seen: set[str] = set()
        for delay in (0.20, 0.35, 0.60):
            await asyncio.sleep(delay)
            orders = await self.client.get_open_orders(symbol)
            seen = {str(row.get("type", "")) for row in orders}
            if "STOP_MARKET" in seen and "TAKE_PROFIT_MARKET" in seen:
                return True
        return False

    async def on_market(
        self, symbol: str, bid: float, ask: float, timestamp_ms: int
    ) -> list[BrokerEvent]:
        position = self.positions.get(symbol)
        if position is None or symbol in self.close_requested:
            return []
        executable = bid if position.direction > 0 else ask
        reason = self._local_exit_reason(position, executable, timestamp_ms)
        if reason:
            position.metadata["requested_exit_reason"] = reason
            position.metadata["exit_requested_at_ms"] = timestamp_ms
            self.close_requested.add(symbol)
            try:
                response = await self.client.close_market(
                    position.symbol, position.side.exit_order_side, position.quantity
                )
                position.metadata["exit_order_id"] = str(
                    response.get("orderID") or response.get("orderId") or ""
                )
            except Exception as exc:
                position.metadata["emergency_close_error"] = type(exc).__name__
                raise
        return []

    def _local_exit_reason(self, position: Position, price: float, timestamp_ms: int) -> str:
        # Initial SL/TP are exchange-side. Locally manage only trailing and time exits.
        direction = position.direction
        position.highest_price = max(position.highest_price, price)
        position.lowest_price = min(position.lowest_price, price)
        favorable = (
            position.highest_price - position.entry_price
            if direction > 0
            else position.entry_price - position.lowest_price
        )
        risk = max(position.unit_risk, 1e-12)
        position.mfe_r = max(position.mfe_r, favorable / risk)
        if position.mfe_r >= self.settings.trailing_activation_r:
            candidate = price - direction * self.settings.trailing_distance_r * risk
            if position.trailing_stop is None:
                position.trailing_stop = candidate
            elif direction > 0:
                position.trailing_stop = max(position.trailing_stop, candidate)
            else:
                position.trailing_stop = min(position.trailing_stop, candidate)
            if direction * (price - position.trailing_stop) <= 0:
                return "trailing_stop"
        held_sec = (timestamp_ms - position.opened_at_ms) / 1000.0
        if held_sec >= self.settings.max_hold_sec:
            return "max_hold_time"
        if held_sec >= self.settings.no_progress_exit_sec and position.mfe_r < self.settings.no_progress_min_mfe_r:
            return "no_progress"
        return ""

    async def reconcile(self, prices: dict[str, float], timestamp_ms: int) -> list[BrokerEvent]:
        events: list[BrokerEvent] = []
        for symbol, position in list(self.positions.items()):
            exchange_positions = await self.client.get_positions(symbol)
            active = any(
                abs(float(row.get("positionAmt", 0.0) or 0.0)) > 0
                for row in exchange_positions
                if str(row.get("symbol", symbol)) == symbol
            )
            if active:
                if (
                    position.metadata.get("requested_exit_reason")
                    and (
                        not position.metadata.get("exit_order_id")
                        or timestamp_ms
                        - int(position.metadata.get("exit_requested_at_ms", 0) or 0)
                        >= 10_000
                    )
                ):
                    try:
                        response = await self.client.close_market(
                            position.symbol,
                            position.side.exit_order_side,
                            position.quantity,
                        )
                        position.metadata["exit_order_id"] = str(
                            response.get("orderID") or response.get("orderId") or ""
                        )
                        position.metadata["exit_requested_at_ms"] = timestamp_ms
                        position.metadata.pop("emergency_close_error", None)
                    except Exception as exc:
                        position.metadata["emergency_close_error"] = type(exc).__name__
                await asyncio.sleep(0.4)
                continue
            event = await self._closed_event(position, prices.get(symbol, position.entry_price), timestamp_ms)
            # Attached TP/SL should be linked, but explicitly remove any orphaned
            # protective order after the exchange position is confirmed flat. A
            # failure propagates so the engine blocks new entries and retries.
            cancellation = await self.client.cancel_all_open_orders(symbol)
            if cancellation.get("failed"):
                raise RuntimeError(f"residual order cancellation failed for {symbol}")
            self.positions.pop(symbol, None)
            self.close_requested.discard(symbol)
            events.append(event)
            await asyncio.sleep(0.55)
        if events:
            await self.refresh_equity()
        return events

    async def _closed_event(
        self, position: Position, fallback_price: float, timestamp_ms: int
    ) -> BrokerEvent:
        start = max(position.opened_at_ms - 2_000, timestamp_ms - 24 * 3600 * 1000)
        fills = await self.client.get_fill_history(position.symbol, start, timestamp_ms)
        gross = sum(float(row.get("realizedPnl", 0.0) or 0.0) for row in fills)
        fee_cashflow = sum(float(row.get("fee", 0.0) or 0.0) for row in fills)
        net = gross + fee_cashflow
        exit_fills = [
            row
            for row in fills
            if str(row.get("side", "")).upper() == position.side.exit_order_side
        ]
        exit_quantity = sum(float(row.get("qty", 0.0) or 0.0) for row in exit_fills)
        exit_price = (
            sum(
                float(row.get("price", 0.0) or 0.0) * float(row.get("qty", 0.0) or 0.0)
                for row in exit_fills
            )
            / exit_quantity
            if exit_quantity > 0
            else fallback_price
        )
        reason = str(position.metadata.get("requested_exit_reason", "exchange_sl_or_tp"))
        return BrokerEvent(
            kind="closed",
            timestamp_ms=timestamp_ms,
            symbol=position.symbol,
            position_id=position.position_id,
            pnl=net,
            r_multiple=net / max(position.initial_risk_cash, 1e-12),
            price=exit_price,
            reason=reason,
            payload={
                "gross_pnl": gross,
                "total_fees": -fee_cashflow,
                "exit_order_id": position.metadata.get("exit_order_id", ""),
                "exchange_reconciled": True,
            },
        )

# ==================== engine.py ====================
import asyncio
import logging
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path



LOGGER = logging.getLogger(__name__)


class TradingEngine:
    def __init__(self, cfg: AppSettings) -> None:
        self.cfg = cfg
        self.credentials = Credentials.from_env()
        if cfg.mode in {"vst", "live"} and self.credentials is None:
            raise SafetyLockError("BINGX_API_KEY and BINGX_SECRET_KEY are required")
        base_urls = cfg.api.vst_base_urls if cfg.mode == "vst" else cfg.api.live_base_urls
        # Public market REST is usable without a trading key; private methods enforce auth.
        self.client = BingXRestClient(cfg.api, self.credentials, base_urls)
        self.journal = TradeJournal(cfg.database_path)
        self.notifier = TelegramNotifier(cfg.telegram)
        self.schedule = TelegramSchedule(cfg.telegram, self.notifier, self.journal)
        self.universe_symbols: tuple[str, ...] = ()
        self.specs: dict[str, ContractSpec] = {}
        self.store: MarketDataStore | None = None
        self.stream: BingXMarketStream | None = None
        self.derivatives: DerivativesContextService | None = None
        self.calendar: EconomicCalendarService | None = None
        self.impulse = ImpulseStrategy(cfg.strategy, cfg.market)
        self.sweep = LiquiditySweepStrategy(cfg.strategy, cfg.market)
        self.ml = MLValidator(cfg.ml, cfg.mode)
        self.broker: Broker | None = None
        self.risk: RiskManager | None = None
        self.last_eval_ms: dict[str, int] = {}
        self.last_prices: dict[str, float] = {}
        self.execution_block = ""

    async def run(self) -> None:
        selection = await UniverseSelector(self.cfg.universe, self.client).select()
        self.universe_symbols = selection.symbols
        self.specs = selection.specs
        await self._load_account_fees()
        self.store = MarketDataStore(self.cfg, selection.symbols)
        self.calendar = EconomicCalendarService(self.cfg.news, self.store, self.cfg.mode)
        initial_news_block, news_reason = self.calendar.block_status()
        self.store.update_news(initial_news_block, news_reason)
        build_version = globals().get("SINGLE_FILE_BUILD_VERSION", "")
        build_line = f"Сборка: {build_version}\n" if build_version else ""
        await self.notifier.send_message(
            f"ЗАПУСК · {self.cfg.mode.upper()}\n"
            f"{build_line}"
            f"Выбрано рынков: {len(selection.symbols)} ({selection.source})\n"
            "Прогреваются 1m/15m/1h и защитные фильтры. Новые входы до готовности запрещены."
        )
        await warmup_market(self.client, self.store, selection.symbols)
        await self._initialize_broker()
        assert self.broker is not None
        assert self.risk is not None
        self.derivatives = DerivativesContextService(
            self.cfg.derivatives, self.client, self.store, selection.symbols
        )
        self.stream = BingXMarketStream(
            self.cfg, selection.symbols, self.store, self._on_market_update
        )
        tasks = [
            asyncio.create_task(self.stream.run(), name="bingx-market"),
            asyncio.create_task(self._maintenance(), name="maintenance"),
            asyncio.create_task(self.calendar.run(), name="economic-calendar"),
        ]
        if self.cfg.derivatives.enabled:
            tasks.append(asyncio.create_task(self.derivatives.run(), name="derivatives-context"))
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.journal.close()

    async def _initialize_broker(self) -> None:
        all_stats = self.journal.stats(mode=self.cfg.mode)
        if self.cfg.mode == "paper":
            initial_equity = self.cfg.risk.paper_initial_equity + all_stats.net_pnl
            broker: Broker = PaperBroker(self.cfg.execution, initial_equity, self.specs)
        else:
            assert self.client is not None
            broker = BingXBroker(
                self.cfg.mode, self.cfg.execution, self.client, 0.0, self.specs
            )
        restored = self.journal.restore_positions()
        wrong_mode = [position.symbol for position in restored if position.mode != self.cfg.mode]
        if wrong_mode:
            raise SafetyLockError(
                "Open journal positions belong to another mode: " + ", ".join(wrong_mode)
            )
        await broker.initialize(restored)
        self.broker = broker
        self.risk = RiskManager(self.cfg.risk, self.cfg.execution, broker.equity)
        today = self.journal.today_stats(mode=self.cfg.mode)
        self.risk.daily_realized_pnl = today.net_pnl
        self.risk.day_start_equity = broker.equity - today.net_pnl
        self.risk.consecutive_losses = self.journal.recent_consecutive_losses(mode=self.cfg.mode)

    async def _load_account_fees(self) -> None:
        if self.credentials is None:
            return
        try:
            commission = await self.client.get_commission()
            taker = float(commission.get("takerCommissionRate", 0.0) or 0.0)
            maker = float(commission.get("makerCommissionRate", 0.0) or 0.0)
            if taker <= 0 or maker < 0:
                raise ValueError("invalid commission response")
            self.specs = {
                symbol: replace(spec, taker_fee_rate=taker, maker_fee_rate=maker)
                for symbol, spec in self.specs.items()
            }
        except Exception as exc:
            LOGGER.warning("Could not load account commission; conservative defaults remain: %s", exc)

    async def _on_market_update(self, symbol: str) -> None:
        assert self.store is not None and self.broker is not None and self.risk is not None
        state = self.store.state(symbol)
        book = state.order_book
        if book is None or book.mid <= 0:
            return
        now_ms = int(time.time() * 1000)
        self.last_prices[symbol] = book.mid
        try:
            events = await self.broker.on_market(
                symbol, book.best_bid, book.best_ask, now_ms
            )
        except Exception as exc:
            LOGGER.exception("Position exit request failed for %s", symbol)
            if self.cfg.mode in {"vst", "live"} and not self.execution_block:
                self.execution_block = "ошибка защитного выхода; требуется сверка"
                await self.notifier.send_message(
                    f"КРИТИЧЕСКАЯ ОШИБКА ВЫХОДА · {self.cfg.mode.upper()}\n"
                    f"{symbol}: {type(exc).__name__}\n"
                    "Новые входы заблокированы; проверьте позицию на BingX."
                )
            return
        for event in events:
            await self._handle_close(event)
        if symbol in self.broker.positions or self.execution_block:
            self.impulse.clear_symbol(symbol)
            self.sweep.clear_symbol(symbol)
            return
        if now_ms - self.last_eval_ms.get(symbol, 0) < 200:
            return
        self.last_eval_ms[symbol] = now_ms
        feature = self.store.feature(symbol, now_ms)
        candidates = [
            item
            for item in (self.impulse.evaluate(feature), self.sweep.evaluate(feature))
            if item is not None
        ]
        if not candidates:
            return
        signal = max(candidates, key=lambda item: item.strength)
        await self._handle_signal(signal)

    async def _handle_signal(self, signal: Signal) -> None:
        assert self.broker is not None and self.risk is not None
        spec = self.specs.get(signal.symbol, ContractSpec(symbol=signal.symbol))
        signal = replace(
            signal,
            stop_price=round(signal.stop_price, spec.price_precision),
            target_price=round(signal.target_price, spec.price_precision),
        )
        rounded_risk = signal.side.direction * (signal.entry_price - signal.stop_price)
        rounded_reward = signal.side.direction * (signal.target_price - signal.entry_price)
        actual_rr = rounded_reward / rounded_risk if rounded_risk > 0 else 0.0
        signal = replace(signal, target_rr=actual_rr)
        self.journal.record_signal(signal, "candidate")
        if rounded_risk <= 0 or actual_rr < 1.5:
            self.journal.update_signal(
                signal.signal_id, "rejected_precision", "price rounding invalidated stop/target"
            )
            return
        now_ms = int(time.time() * 1000)
        if (
            now_ms > signal.expires_at_ms
            or now_ms - signal.created_at_ms > self.cfg.execution.max_signal_age_ms
        ):
            self.journal.update_signal(signal.signal_id, "rejected", "signal expired")
            return
        ml_decision = self.ml.decide(signal)
        if not ml_decision.allowed:
            reason = ml_decision.reason
            if ml_decision.probability is not None:
                reason += f" ({ml_decision.probability:.1%})"
            self.journal.update_signal(signal.signal_id, "rejected_ml", reason)
            return
        risk_multiplier = (
            self.cfg.strategy.high_volatility_risk_multiplier
            if signal.features.atr_pct >= self.cfg.strategy.high_atr_pct
            else 1.0
        )
        sizing = self.risk.size(
            signal,
            spec,
            open_positions=len(self.broker.positions),
            total_open_notional=self.broker.total_notional,
            risk_multiplier=risk_multiplier,
        )
        if not sizing.accepted:
            self.journal.update_signal(signal.signal_id, "rejected_risk", sizing.reason)
            return
        try:
            position = await self.broker.open(signal, sizing)
        except Exception as exc:
            reason = f"execution error: {type(exc).__name__}: {exc}"
            self.journal.update_signal(signal.signal_id, "execution_error", reason)
            LOGGER.exception("Order execution failed for %s", signal.symbol)
            if isinstance(exc, ExecutionSafetyError):
                self.journal.record_open(exc.position)
            if self.cfg.mode in {"vst", "live"}:
                self.execution_block = "ошибка исполнения; требуется ручная сверка биржи"
                await self.notifier.send_message(
                    f"КРИТИЧЕСКАЯ ОШИБКА · {self.cfg.mode.upper()}\n"
                    f"{signal.symbol}: {type(exc).__name__}\n"
                    "Новые входы заблокированы. Сверьте позиции и защитные ордера на BingX."
                )
            return
        self.journal.record_open(position)
        probability = (
            f"; ML={ml_decision.probability:.1%}" if ml_decision.probability is not None else ""
        )
        self.journal.update_signal(signal.signal_id, "opened", f"opened{probability}")
        await self.notifier.send_message(open_message(position, signal, sizing, self.risk.equity))

    async def _handle_close(self, event: BrokerEvent) -> None:
        assert self.broker is not None and self.risk is not None
        self.journal.record_close(event)
        self.risk.register_close(event.pnl, event.timestamp_ms)
        if self.cfg.mode in {"vst", "live"}:
            self.risk.sync_equity(self.broker.equity)
        await self.notifier.send_message(
            close_message(event, self.cfg.mode, self.risk.equity)
        )
        self.journal.export_due(
            self.cfg.telegram.report_every_closed_trades,
            self.cfg.telegram.reports_dir,
            self.cfg.mode,
        )
        await self._send_pending_csv()
        status = self.risk.can_open(len(self.broker.positions), event.timestamp_ms)
        if not status.allowed:
            await self.notifier.send_message(
                f"ТОРГОВЛЯ ПРИОСТАНОВЛЕНА · {self.cfg.mode.upper()}\n{status.reason}\n"
                "Новые входы не создаются; мониторинг и отчёты продолжаются."
            )

    async def _maintenance(self) -> None:
        while True:
            await asyncio.sleep(5)
            assert self.broker is not None and self.risk is not None
            now_ms = int(time.time() * 1000)
            try:
                events = await self.broker.reconcile(self.last_prices, now_ms)
                for event in events:
                    await self._handle_close(event)
            except Exception as exc:
                LOGGER.warning("Exchange reconciliation failed: %s", exc)
                if self.cfg.mode in {"vst", "live"}:
                    self.execution_block = "ошибка сверки позиций"
            self.journal.export_due(
                self.cfg.telegram.report_every_closed_trades,
                self.cfg.telegram.reports_dir,
                self.cfg.mode,
            )
            await self._send_pending_csv()
            await self._send_scheduled_stats()
            await self._send_heartbeat(now_ms)

    async def _send_scheduled_stats(self) -> None:
        assert self.broker is not None and self.risk is not None
        for key in self.schedule.due_stats_keys(datetime.now(UTC)):
            stats = self.journal.today_stats(mode=self.cfg.mode)
            max_loss = self.risk.day_start_equity * self.cfg.risk.daily_max_loss_pct / 100.0
            sent = await self.notifier.send_message(
                stats_message(
                    stats,
                    mode=self.cfg.mode,
                    equity=self.risk.equity,
                    open_positions=len(self.broker.positions),
                    daily_loss_limit=max_loss,
                )
            )
            if sent:
                self.schedule.mark_stats_sent(key)

    async def _send_heartbeat(self, now_ms: int) -> None:
        if not self.schedule.heartbeat_due(now_ms):
            return
        assert self.broker is not None and self.risk is not None
        risk_status = self.risk.can_open(len(self.broker.positions), now_ms)
        risk_block = self.execution_block or ("" if risk_status.allowed else risk_status.reason)
        if not risk_block and self.store and self.store.news_blocked:
            risk_block = self.store.news_reason
        if not risk_block:
            risk_block = self.ml.live_block_reason()
        best = self._best_diagnostic()
        sent = await self.notifier.send_message(
            heartbeat_message(
                mode=self.cfg.mode,
                connected=bool(self.stream and self.stream.connected),
                symbols=len(self.universe_symbols),
                open_positions=len(self.broker.positions),
                best=best,
                risk_block=risk_block,
            )
        )
        if sent:
            self.schedule.mark_heartbeat_sent(now_ms)

    def _best_diagnostic(self) -> ScanDiagnostic | None:
        items = [self.impulse.best_diagnostic(), self.sweep.best_diagnostic()]
        return max((item for item in items if item is not None), key=lambda item: item.score, default=None)

    async def _send_pending_csv(self) -> None:
        if not self.cfg.telegram.send_csv:
            return
        now_ms = int(time.time() * 1000)
        directory = Path(self.cfg.telegram.reports_dir)
        for path in sorted(directory.glob(f"trades_{self.cfg.mode}_*.csv")):
            sent_key = f"csv_sent:{path.name}"
            if self.journal.get_meta(sent_key) == "sent":
                continue
            attempt_key = f"csv_attempt_ms:{path.name}"
            last_attempt = int(self.journal.get_meta(attempt_key, "0"))
            if now_ms - last_attempt < 300_000:
                continue
            self.journal.set_meta(attempt_key, str(now_ms))
            sent = await self.notifier.send_document(
                path,
                f"Пакет из {self.cfg.telegram.report_every_closed_trades} закрытых сделок",
            )
            if sent:
                self.journal.set_meta(sent_key, "sent")


# ============================================================
# SINGLE-FILE RENDER WEB WRAPPER
# ============================================================

from fastapi import FastAPI
from fastapi.responses import JSONResponse


APP_NAME = "BingX Impulse Bot Single File"
app = FastAPI(title=APP_NAME)
_engine_task: asyncio.Task[None] | None = None
_runtime_state: dict[str, Any] = {
    "status": "starting",
    "mode": os.getenv("BOT_MODE", "paper").lower(),
    "last_error": "",
    "started_at": int(time.time()),
    "restarts": 0,
}


def _single_file_settings() -> AppSettings:
    mode = os.getenv("BOT_MODE", "paper").strip().lower()
    configured = os.getenv("BOT_CONFIG", "").strip()
    if configured and Path(configured).exists():
        return load_settings(configured)

    preferred = Path(os.getenv("BOT_DATA_DIR", "/var/data"))
    try:
        preferred.mkdir(parents=True, exist_ok=True)
        probe = preferred / ".write-test"
        probe.touch(exist_ok=True)
        probe.unlink(missing_ok=True)
        data_dir = preferred
    except OSError:
        data_dir = Path("data").resolve()
        data_dir.mkdir(parents=True, exist_ok=True)

    reports = data_dir / "reports"
    models = data_dir / "models"
    reports.mkdir(parents=True, exist_ok=True)
    models.mkdir(parents=True, exist_ok=True)
    cfg = AppSettings(
        mode=mode,
        database_path=str(data_dir / "impulse_bot.sqlite3"),
        telegram=replace(TelegramSettings(), reports_dir=str(reports)),
        ml=replace(MLSettings(), model_path=str(models / "setup_validator.joblib")),
    )
    validate_settings(cfg)
    return cfg


async def _run_engine_forever() -> None:
    while True:
        engine: TradingEngine | None = None
        try:
            cfg = _single_file_settings()
            _runtime_state.update(status="running", mode=cfg.mode, last_error="")
            engine = TradingEngine(cfg)
            await engine.run()
            raise RuntimeError("trading engine stopped unexpectedly")
        except asyncio.CancelledError:
            _runtime_state["status"] = "stopped"
            raise
        except Exception as exc:
            LOGGER.exception("Trading engine crashed; retrying")
            _runtime_state.update(
                status="retrying",
                last_error=f"{type(exc).__name__}: {exc}"[:500],
                restarts=int(_runtime_state["restarts"]) + 1,
            )
            try:
                notifier = TelegramNotifier(TelegramSettings())
                await notifier.send_message(
                    "ОШИБКА ЗАПУСКА · бот повторит попытку через 30 секунд\n"
                    + _runtime_state["last_error"]
                )
            except Exception:
                pass
            await asyncio.sleep(30)


@app.on_event("startup")
async def _startup() -> None:
    global _engine_task
    if _engine_task is None or _engine_task.done():
        _engine_task = asyncio.create_task(_run_engine_forever(), name="trading-engine")


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _engine_task
    if _engine_task is not None:
        _engine_task.cancel()
        await asyncio.gather(_engine_task, return_exceptions=True)
        _engine_task = None


@app.get("/")
def root() -> dict[str, Any]:
    return {"name": APP_NAME, **_runtime_state}


@app.get("/health")
def health() -> JSONResponse:
    healthy = _engine_task is not None and not _engine_task.done()
    return JSONResponse(
        {"ok": healthy, **_runtime_state},
        status_code=200 if healthy else 503,
    )


@app.get("/version")
def version() -> dict[str, str]:
    return {"version": SINGLE_FILE_BUILD_VERSION, "default_mode": "paper"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
