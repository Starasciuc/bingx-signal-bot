from __future__ import annotations

"""
V18.4 Visible Trades + Bounded Adaptive PAPER Research Recorder
BingX USDT-M perpetual markets.

RESEARCH / PAPER ONLY:
- no authenticated exchange endpoints
- cannot place, modify, or cancel orders
- every executable PAPER entry, primary result and full path is visible in Telegram
- PAPER trades have a stable visible number; there are no hidden SHADOW trades
- the unchanged V18.1 liquid-momentum lane is the CHAMPION
- a fixed trend/pullback/reclaim lane is the CHALLENGER
- OBSERVER is diagnostic only and never trains or enables a model
- only TP3+ is a profitable outcome
- primary legacy result remains 6 minutes
- path is sampled from executable BBO every ~2 seconds
- missed horizons are marked DATA_GAP instead of inventing historical prices
- TP3/SL hits in the same sample are DATA_GAP, never an invented winner
- real-money readiness uses cost-adjusted payoff, regime/day diversity,
  cluster-robust confidence and three non-overlapping 50-trade blocks

Every 25 closed visible PAPER outcomes trigger an audit, a prospective bounded
policy update and a JSON file sent to Telegram.  A weak block can only move its
lane one step up a pre-registered selectivity ladder (higher score floor and a
longer cooldown).  The bot never loosens rules automatically and never rewrites
its Python source.  Promotion statistics use only independent outcomes produced
by the current policy revision.  No audit can change targets, place an exchange
order, or enable real money.  A lane can be recommended for a separately
reviewed micro-LIVE build only after a much larger independent forward sample
under one unchanged policy revision passes all pre-registered checks.
"""

import asyncio
import hashlib
import io
import json
import math
import os
import secrets
import sqlite3
import statistics
import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, Response


# ---------------------------------------------------------------------------
# Immutable V18.4 base protocol
# ---------------------------------------------------------------------------

APP_NAME = "Professional Futures Research Bot V18.4 VISIBLE BOUNDED ADAPTIVE"
DEPLOY_MARKER = "V18_4_VISIBLE_BOUNDED_ADAPTIVE_2026_08_16"
EXPORT_SCHEMA = "v18_4_visible_adaptive_export_v1"
COHORT_ID = "V18_4_VISIBLE_BOUNDED_ADAPTIVE_V1"

STRATEGY_MOMENTUM = "LIQUID_MOMENTUM_CHAMPION"
STRATEGY_PULLBACK = "TREND_PULLBACK_RECLAIM_CHALLENGER"
STRATEGIES: Tuple[str, ...] = (STRATEGY_MOMENTUM, STRATEGY_PULLBACK)

TARGET_MOVES: Tuple[float, ...] = (0.0065, 0.0120, 0.0185, 0.0260, 0.0350)
TP1_MOVE, TP2_MOVE, TP3_MOVE, TP4_MOVE, TP5_MOVE = TARGET_MOVES

HORIZONS_SECONDS: Tuple[int, ...] = (60, 180, 360, 600, 900, 1800)
PRIMARY_HORIZON_SECONDS = 360
FINAL_HORIZON_SECONDS = 1800
MAX_HORIZON_LATENESS_SECONDS = 8

# Broad recorder: deliberately permissive for diagnostics. Only executable
# PAPER episodes are visible in Telegram; OBSERVER cannot train or promote.
BROAD_MIN_DIRECTIONAL_3M = 0.0035
BROAD_MAX_DIRECTIONAL_3M = 0.0500
BROAD_MIN_DIRECTIONAL_15M = 0.0050
BROAD_MAX_DIRECTIONAL_15M = 0.0800

# Stronger PAPER continuation gate. Designed to attack the old failure mode:
# weak/late impulses that quickly become expired or reverse into SL.
PAPER_MIN_DIRECTIONAL_1M = 0.0012
PAPER_MAX_DIRECTIONAL_1M = 0.0090
PAPER_MIN_DIRECTIONAL_3M = 0.0065
PAPER_MAX_DIRECTIONAL_3M = 0.0280
PAPER_MIN_DIRECTIONAL_15M = 0.0090
PAPER_MAX_DIRECTIONAL_15M = 0.0500
PAPER_MIN_ACCELERATION = 0.22        # d3 must be >= 22% of d15
PAPER_MAX_D3_SHARE_OF_D15 = 0.78    # avoids one-bar exhaustion
PAPER_MIN_VOL1 = 0.95
PAPER_MAX_VOL1 = 2.80
PAPER_MIN_RANGE1 = 1.15
PAPER_MAX_RANGE1 = 4.00
PAPER_MIN_BODY_FRACTION = 0.38
PAPER_LONG_MIN_CLOSE_LOCATION = 0.65
PAPER_SHORT_MAX_CLOSE_LOCATION = 0.35
PAPER_MAX_VWAP_DISTANCE = 0.0180
PAPER_MAX_SPREAD_BPS = 10.0
PAPER_MIN_DEPTH_USDT = 5_000.0
PAPER_MIN_24H_QUOTE_VOLUME_USDT = 10_000_000.0
# This is the actual net winner / actual net loser ratio after two taker fees
# and the fixed slippage allowance below.  The previous build checked gross
# TP3/stop and could accept a trade whose real payoff was materially weaker.
PAPER_MIN_NET_PAYOFF_RATIO = 1.45

# Pre-registered CHALLENGER.  This is a mechanical interpretation of the
# public trend -> controlled pullback -> reclaim idea, not a claim that these
# are any named trader's proprietary parameters.
PULLBACK_MIN_ADX14 = 20.0
PULLBACK_MAX_ADX14 = 60.0
PULLBACK_MIN_DIRECTIONAL_15M = 0.0060
PULLBACK_MAX_DIRECTIONAL_15M = 0.0500
PULLBACK_MIN_DIRECTIONAL_30M = 0.0080
PULLBACK_MAX_DIRECTIONAL_30M = 0.0800
PULLBACK_MIN_RETRACE = 0.12
PULLBACK_MAX_RETRACE = 0.50
PULLBACK_EMA20_TOLERANCE_ATR = 0.35
PULLBACK_MIN_TRIGGER_BODY = 0.30
PULLBACK_LONG_MIN_CLOSE_LOCATION = 0.62
PULLBACK_SHORT_MAX_CLOSE_LOCATION = 0.38
PULLBACK_MIN_VOL1 = 0.70
PULLBACK_MAX_VOL1 = 3.50
PULLBACK_MIN_RANGE1 = 0.80
PULLBACK_MAX_RANGE1 = 4.00
PULLBACK_MAX_ENTRY_CHASE_ATR = 0.30

MIN_24H_QUOTE_VOLUME_USDT = 3_000_000.0
MIN_ACTIVE_CANDLE_FRACTION = 0.82
MIN_UNIQUE_CLOSE_FRACTION = 0.35
MAX_UNIVERSE = 100
SCAN_WORKERS = 8
MAX_OPEN_EPISODES = 180

# We still avoid sending the identical symbol/side/strategy every scan.
OBSERVER_SYMBOL_COOLDOWN_SECONDS = 30 * 60
PAPER_SYMBOL_COOLDOWN_SECONDS = 3 * 60 * 60
INDEPENDENT_CLUSTER_SECONDS = 15 * 60

MIN_STOP_MOVE = 0.0080
MAX_STOP_MOVE = 0.0127
ATR_STOP_MULTIPLIER = 2.00

# Conservative execution costs for readiness accounting.
TAKER_FEE_PER_SIDE = 0.0005
ROUND_TRIP_FEE_MOVE = TAKER_FEE_PER_SIDE * 2.0
ASSUMED_ENTRY_SLIPPAGE_BPS = 4.0
ASSUMED_EXIT_SLIPPAGE_BPS = 4.0
ASSUMED_SLIPPAGE_MOVE = (
    ASSUMED_ENTRY_SLIPPAGE_BPS + ASSUMED_EXIT_SLIPPAGE_BPS
) / 10_000.0
ROUND_TRIP_COST_MOVE = ROUND_TRIP_FEE_MOVE + ASSUMED_SLIPPAGE_MOVE

# Research gate. Passing NEVER enables money in this program.
REVIEW_MIN_INDEPENDENT_PAPER = 150
REVIEW_MIN_UNIQUE_SYMBOLS = 30
REVIEW_MIN_UNIQUE_CLUSTERS = 75
REVIEW_MIN_ACTIVE_DAYS = 10
REVIEW_MIN_MARKET_REGIMES = 2
REVIEW_MIN_PER_SIDE = 30
REVIEW_MIN_TP3_RATE = 0.52
REVIEW_MIN_EXPECTANCY_R = 0.15
REVIEW_MIN_PROFIT_FACTOR = 1.30
REVIEW_MAX_DRAWDOWN_R = 8.0
REVIEW_MAX_DATA_GAP_RATE = 0.02
REVIEW_RECENT_WINDOW = 50
REVIEW_STABILITY_BLOCKS = 3
AUDIT_BLOCK_SIZE = 25
MIN_LANE_COMPARISON_SAMPLE = 100
MIN_LCB_ADVANTAGE_R = 0.05

# Pre-registered, one-way adaptive ladder.  A completed 25-trade block may
# tighten a lane by at most one level.  Automatic loosening is intentionally
# impossible: a new policy revision must prove itself prospectively.
ADAPTIVE_POLICY_SCHEMA = "v18_4_bounded_selectivity_v1"
ADAPTIVE_MIN_INDEPENDENT_PER_LANE_BLOCK = 8
ADAPTIVE_MAX_DATA_GAP_RATE = 0.05
ADAPTIVE_LEVELS: Tuple[Dict[str, float], ...] = (
    {"min_quality_score": 0.0, "cooldown_multiplier": 1.0},
    {"min_quality_score": 60.0, "cooldown_multiplier": 1.5},
    {"min_quality_score": 65.0, "cooldown_multiplier": 2.0},
    {"min_quality_score": 70.0, "cooldown_multiplier": 3.0},
    {"min_quality_score": 75.0, "cooldown_multiplier": 4.0},
)

PROTOCOL_MANIFEST = {
    "cohort_id": COHORT_ID,
    "targets": TARGET_MOVES,
    "horizons": HORIZONS_SECONDS,
    "primary_horizon": PRIMARY_HORIZON_SECONDS,
    "max_horizon_lateness": MAX_HORIZON_LATENESS_SECONDS,
    "broad_3m": [BROAD_MIN_DIRECTIONAL_3M, BROAD_MAX_DIRECTIONAL_3M],
    "broad_15m": [BROAD_MIN_DIRECTIONAL_15M, BROAD_MAX_DIRECTIONAL_15M],
    "paper_1m": [PAPER_MIN_DIRECTIONAL_1M, PAPER_MAX_DIRECTIONAL_1M],
    "paper_3m": [PAPER_MIN_DIRECTIONAL_3M, PAPER_MAX_DIRECTIONAL_3M],
    "paper_15m": [PAPER_MIN_DIRECTIONAL_15M, PAPER_MAX_DIRECTIONAL_15M],
    "paper_acceleration": [PAPER_MIN_ACCELERATION, PAPER_MAX_D3_SHARE_OF_D15],
    "paper_vol1": [PAPER_MIN_VOL1, PAPER_MAX_VOL1],
    "paper_range1": [PAPER_MIN_RANGE1, PAPER_MAX_RANGE1],
    "paper_body": PAPER_MIN_BODY_FRACTION,
    "paper_vwap_distance": PAPER_MAX_VWAP_DISTANCE,
    "spread_bps": PAPER_MAX_SPREAD_BPS,
    "depth_usdt": PAPER_MIN_DEPTH_USDT,
    "paper_min_24h_quote_volume": PAPER_MIN_24H_QUOTE_VOLUME_USDT,
    "paper_min_net_payoff_ratio": PAPER_MIN_NET_PAYOFF_RATIO,
    "strategies": STRATEGIES,
    "pullback": {
        "adx14": [PULLBACK_MIN_ADX14, PULLBACK_MAX_ADX14],
        "directional_15m": [PULLBACK_MIN_DIRECTIONAL_15M, PULLBACK_MAX_DIRECTIONAL_15M],
        "directional_30m": [PULLBACK_MIN_DIRECTIONAL_30M, PULLBACK_MAX_DIRECTIONAL_30M],
        "retrace": [PULLBACK_MIN_RETRACE, PULLBACK_MAX_RETRACE],
        "ema20_tolerance_atr": PULLBACK_EMA20_TOLERANCE_ATR,
        "trigger_body": PULLBACK_MIN_TRIGGER_BODY,
        "vol1": [PULLBACK_MIN_VOL1, PULLBACK_MAX_VOL1],
        "range1": [PULLBACK_MIN_RANGE1, PULLBACK_MAX_RANGE1],
        "entry_chase_atr": PULLBACK_MAX_ENTRY_CHASE_ATR,
    },
    "fees_round_trip": ROUND_TRIP_FEE_MOVE,
    "assumed_slippage_round_trip": ASSUMED_SLIPPAGE_MOVE,
    "cost_round_trip": ROUND_TRIP_COST_MOVE,
    "stop_bounds": [MIN_STOP_MOVE, MAX_STOP_MOVE],
    "review": {
        "independent_paper": REVIEW_MIN_INDEPENDENT_PAPER,
        "unique_symbols": REVIEW_MIN_UNIQUE_SYMBOLS,
        "unique_clusters": REVIEW_MIN_UNIQUE_CLUSTERS,
        "active_days": REVIEW_MIN_ACTIVE_DAYS,
        "market_regimes": REVIEW_MIN_MARKET_REGIMES,
        "per_side": REVIEW_MIN_PER_SIDE,
        "data_gap_rate_max": REVIEW_MAX_DATA_GAP_RATE,
        "stability_blocks": REVIEW_STABILITY_BLOCKS,
        "stability_block_size": REVIEW_RECENT_WINDOW,
    },
    "bounded_adaptation": {
        "schema": ADAPTIVE_POLICY_SCHEMA,
        "audit_block_size": AUDIT_BLOCK_SIZE,
        "min_independent_per_lane_block": ADAPTIVE_MIN_INDEPENDENT_PER_LANE_BLOCK,
        "max_data_gap_rate": ADAPTIVE_MAX_DATA_GAP_RATE,
        "levels": ADAPTIVE_LEVELS,
        "automatic_loosening": False,
        "source_rewriting": False,
    },
}
PROTOCOL_HASH = hashlib.sha256(
    json.dumps(PROTOCOL_MANIFEST, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Environment/runtime
# ---------------------------------------------------------------------------

def _env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except Exception:
        return max(minimum, default)

def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(os.getenv(name, str(default))))
    except Exception:
        return max(minimum, default)

BINGX_BASE_URL = os.getenv("BINGX_BASE_URL", "https://open-api.bingx.com").rstrip("/")
DB_PATH = os.getenv("RESEARCH_DB_PATH", "research_v18_4.sqlite3")
SEED_PATH = os.getenv("RESEARCH_SEED_PATH", "adaptive_seed.json")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
ADMIN_KEY = os.getenv("ADMIN_KEY", "").strip()
PORT = _env_int("PORT", 10000, 1)

SCAN_INTERVAL_SECONDS = _env_int("SCAN_INTERVAL_SECONDS", 60, 30)
# Critical change: execution path from 15s -> 2s.
TRACK_INTERVAL_SECONDS = _env_int("TRACK_INTERVAL_SECONDS", 2, 1)
DIAGNOSTIC_INTERVAL_SECONDS = _env_int("DIAGNOSTIC_INTERVAL_SECONDS", 1800, 300)
REQUEST_TIMEOUT_SECONDS = _env_float("REQUEST_TIMEOUT_SECONDS", 10.0, 3.0)
API_RETRIES = _env_int("API_RETRIES", 3, 1)
BACKUP_EVERY_PRIMARY = _env_int("BACKUP_EVERY_PRIMARY", 5, 5)
TELEGRAM_RETRIES = _env_int("TELEGRAM_RETRIES", 3, 1)

DB_LOCK = threading.RLock()
RUNTIME_LOCK = threading.RLock()
SCAN_LOCK = threading.Lock()
TRACK_LOCK = threading.Lock()
CACHE_LOCK = threading.RLock()
NOTIFY_LOCK = threading.RLock()

RUNTIME: Dict[str, Any] = {
    "started_at": int(time.time()),
    "scan_count": 0,
    "last_scan": {},
    "last_scan_at": 0,
    "last_track_at": 0,
    "last_diagnostic_at": 0,
    "last_error": "",
    "api_calls": 0,
    "api_errors": 0,
    "telegram_sent": 0,
    "telegram_errors": 0,
    "seed_restore": {},
}

def now_ts() -> int:
    return int(time.time())

def set_runtime_error(message: str) -> None:
    with RUNTIME_LOCK:
        RUNTIME["last_error"] = str(message)[:800]

def runtime_snapshot() -> Dict[str, Any]:
    with RUNTIME_LOCK:
        return json.loads(json.dumps(RUNTIME, ensure_ascii=False, default=str))


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

EPISODE_COLUMNS: Tuple[str, ...] = (
    "id", "episode_key", "cohort_id", "protocol_hash", "created_at",
    "cluster_id", "symbol", "side", "strategy", "tier", "visible_paper", "independent",
    "policy_revision", "policy_hash",
    "quality_score", "paper_reject_reason", "entry_price", "entry_bid",
    "entry_ask", "spread_bps", "depth_usdt", "quote_volume_24h",
    "liquidity_rank", "stop_move", "tp1_move", "tp2_move", "tp3_move",
    "tp4_move", "tp5_move", "features_json", "status", "primary_outcome",
    "primary_pnl_r", "primary_closed_at", "primary_notified", "entry_notified",
    "tp1_hit_at", "tp2_hit_at", "tp3_hit_at", "tp4_hit_at", "tp5_hit_at",
    "sl_hit_at", "max_favorable_move", "max_adverse_move", "last_exit_price",
    "last_seen_at", "data_gap_count", "path_samples", "max_sample_gap_seconds",
    "finalized_at", "final_notified",
)

HORIZON_COLUMNS: Tuple[str, ...] = (
    "episode_id", "horizon_seconds", "observed_at", "exit_price",
    "net_move", "mfe_move", "mae_move", "tp1_hit", "tp2_hit", "tp3_hit",
    "sl_hit", "outcome", "pnl_r", "lateness_seconds",
)

def db_connect() -> sqlite3.Connection:
    path = Path(DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db() -> None:
    with DB_LOCK, db_connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS episodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                episode_key TEXT NOT NULL UNIQUE,
                cohort_id TEXT NOT NULL,
                protocol_hash TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                cluster_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                strategy TEXT NOT NULL,
                tier TEXT NOT NULL,
                visible_paper INTEGER NOT NULL DEFAULT 0,
                independent INTEGER NOT NULL DEFAULT 0,
                policy_revision INTEGER NOT NULL DEFAULT 0,
                policy_hash TEXT NOT NULL DEFAULT '',
                quality_score REAL NOT NULL DEFAULT 0,
                paper_reject_reason TEXT NOT NULL DEFAULT '',
                entry_price REAL NOT NULL,
                entry_bid REAL NOT NULL DEFAULT 0,
                entry_ask REAL NOT NULL DEFAULT 0,
                spread_bps REAL NOT NULL DEFAULT 999,
                depth_usdt REAL NOT NULL DEFAULT 0,
                quote_volume_24h REAL NOT NULL DEFAULT 0,
                liquidity_rank REAL NOT NULL DEFAULT 0,
                stop_move REAL NOT NULL,
                tp1_move REAL NOT NULL,
                tp2_move REAL NOT NULL,
                tp3_move REAL NOT NULL,
                tp4_move REAL NOT NULL,
                tp5_move REAL NOT NULL,
                features_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                primary_outcome TEXT,
                primary_pnl_r REAL,
                primary_closed_at INTEGER,
                primary_notified INTEGER NOT NULL DEFAULT 0,
                entry_notified INTEGER NOT NULL DEFAULT 0,
                tp1_hit_at INTEGER,
                tp2_hit_at INTEGER,
                tp3_hit_at INTEGER,
                tp4_hit_at INTEGER,
                tp5_hit_at INTEGER,
                sl_hit_at INTEGER,
                max_favorable_move REAL NOT NULL DEFAULT 0,
                max_adverse_move REAL NOT NULL DEFAULT 0,
                last_exit_price REAL,
                last_seen_at INTEGER,
                data_gap_count INTEGER NOT NULL DEFAULT 0,
                path_samples INTEGER NOT NULL DEFAULT 0,
                max_sample_gap_seconds INTEGER NOT NULL DEFAULT 0,
                finalized_at INTEGER,
                final_notified INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_episodes_open
            ON episodes(status, created_at);
            CREATE INDEX IF NOT EXISTS idx_episodes_cohort_tier
            ON episodes(cohort_id, tier, primary_closed_at);
            CREATE TABLE IF NOT EXISTS horizon_results (
                episode_id INTEGER NOT NULL,
                horizon_seconds INTEGER NOT NULL,
                observed_at INTEGER NOT NULL,
                exit_price REAL NOT NULL,
                net_move REAL NOT NULL,
                mfe_move REAL NOT NULL,
                mae_move REAL NOT NULL,
                tp1_hit INTEGER NOT NULL,
                tp2_hit INTEGER NOT NULL,
                tp3_hit INTEGER NOT NULL,
                sl_hit INTEGER NOT NULL,
                outcome TEXT NOT NULL,
                pnl_r REAL NOT NULL,
                lateness_seconds INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (episode_id, horizon_seconds),
                FOREIGN KEY (episode_id) REFERENCES episodes(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                payload BLOB NOT NULL,
                filename TEXT,
                caption TEXT,
                created_at INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_retry_at INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        existing = {str(r["name"]) for r in conn.execute("PRAGMA table_info(episodes)")}
        if "strategy" not in existing:
            conn.execute(
                "ALTER TABLE episodes ADD COLUMN strategy TEXT NOT NULL "
                f"DEFAULT '{STRATEGY_MOMENTUM}'"
            )
        if "policy_revision" not in existing:
            conn.execute(
                "ALTER TABLE episodes ADD COLUMN policy_revision INTEGER NOT NULL DEFAULT 0"
            )
        if "policy_hash" not in existing:
            conn.execute(
                "ALTER TABLE episodes ADD COLUMN policy_hash TEXT NOT NULL DEFAULT ''"
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_episodes_symbol_strategy "
            "ON episodes(cohort_id,symbol,side,strategy,tier,created_at)"
        )
        conn.commit()

def meta_get(key: str, default: Any = None) -> Any:
    init_db()
    with DB_LOCK, db_connect() as conn:
        row = conn.execute("SELECT value_json FROM meta WHERE key=?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(row["value_json"])
    except Exception:
        return default

def meta_set(key: str, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    with DB_LOCK, db_connect() as conn:
        conn.execute(
            "INSERT INTO meta(key,value_json) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
            (key, payload),
        )
        conn.commit()

def _nonnegative_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return max(0, int(default))

def _adaptive_level(level: Any) -> int:
    try:
        value = int(level)
    except Exception:
        value = 0
    return min(max(value, 0), len(ADAPTIVE_LEVELS) - 1)

def _lane_policy_hash(strategy: str, lane: Dict[str, Any]) -> str:
    payload = {
        "schema": ADAPTIVE_POLICY_SCHEMA,
        "strategy": strategy,
        "revision": int(lane.get("revision", 0) or 0),
        "level": _adaptive_level(lane.get("level", 0)),
        "min_quality_score": float(lane.get("min_quality_score", 0.0) or 0.0),
        "cooldown_multiplier": float(lane.get("cooldown_multiplier", 1.0) or 1.0),
        "status": "ACTIVE",
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]

def default_adaptive_policy() -> Dict[str, Any]:
    lanes: Dict[str, Any] = {}
    for strategy in STRATEGIES:
        level = ADAPTIVE_LEVELS[0]
        lane = {
            "revision": 0,
            "level": 0,
            "min_quality_score": float(level["min_quality_score"]),
            "cooldown_multiplier": float(level["cooldown_multiplier"]),
            "status": "ACTIVE",
            "last_action": "BASELINE",
            "last_reason": "pre_registered_base_rules",
            "applied_after_paper_count": 0,
            "changed_at": 0,
        }
        lane["policy_hash"] = _lane_policy_hash(strategy, lane)
        lanes[strategy] = lane
    return {
        "schema": ADAPTIVE_POLICY_SCHEMA,
        "global_revision": 0,
        "last_completed_block_end": 0,
        "updated_at": 0,
        "automatic_loosening": False,
        "source_rewriting": False,
        "lanes": lanes,
    }

def normalize_adaptive_policy(raw: Any) -> Dict[str, Any]:
    base = default_adaptive_policy()
    if not isinstance(raw, dict) or raw.get("schema") != ADAPTIVE_POLICY_SCHEMA:
        return base
    base["global_revision"] = _nonnegative_int(raw.get("global_revision", 0))
    base["last_completed_block_end"] = _nonnegative_int(
        raw.get("last_completed_block_end", 0)
    )
    base["updated_at"] = _nonnegative_int(raw.get("updated_at", 0))
    raw_lanes = raw.get("lanes") if isinstance(raw.get("lanes"), dict) else {}
    for strategy in STRATEGIES:
        current = raw_lanes.get(strategy)
        if not isinstance(current, dict):
            continue
        level_index = _adaptive_level(current.get("level", 0))
        level = ADAPTIVE_LEVELS[level_index]
        lane = base["lanes"][strategy]
        lane.update({
            "revision": _nonnegative_int(current.get("revision", 0)),
            "level": level_index,
            # Exact ladder values prevent an exported file from silently
            # loosening or inventing a filter on restart.
            "min_quality_score": float(level["min_quality_score"]),
            "cooldown_multiplier": float(level["cooldown_multiplier"]),
            "status": "ACTIVE",
            "last_action": str(current.get("last_action", "BASELINE"))[:80],
            "last_reason": str(current.get("last_reason", ""))[:500],
            "applied_after_paper_count": _nonnegative_int(
                current.get("applied_after_paper_count", 0)
            ),
            "changed_at": _nonnegative_int(current.get("changed_at", 0)),
        })
        lane["policy_hash"] = _lane_policy_hash(strategy, lane)
    return base

def adaptive_policy_state() -> Dict[str, Any]:
    return normalize_adaptive_policy(meta_get("adaptive_policy", None))

def adaptive_lane_policy(
    strategy: str,
    policy: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    snapshot = normalize_adaptive_policy(policy) if policy is not None else adaptive_policy_state()
    return dict(snapshot["lanes"].get(strategy, snapshot["lanes"][STRATEGY_MOMENTUM]))

def adaptive_policy_summary(policy: Optional[Dict[str, Any]] = None) -> str:
    snapshot = normalize_adaptive_policy(policy) if policy is not None else adaptive_policy_state()
    parts = []
    for strategy in STRATEGIES:
        lane = snapshot["lanes"][strategy]
        short = "MOMENTUM" if strategy == STRATEGY_MOMENTUM else "PULLBACK"
        parts.append(
            f"{short}: rev{int(lane['revision'])}/L{int(lane['level'])}, "
            f"score≥{float(lane['min_quality_score']):.1f}, "
            f"cooldown×{float(lane['cooldown_multiplier']):.1f}"
        )
    return " | ".join(parts)

def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}

def table_count(table: str) -> int:
    if table not in {"episodes", "horizon_results", "outbox"}:
        raise ValueError("unsupported table")
    with DB_LOCK, db_connect() as conn:
        return int(conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])

def primary_count(
    tier: Optional[str] = None,
    independent_only: bool = False,
    strategy: Optional[str] = None,
) -> int:
    q = "SELECT COUNT(*) AS n FROM episodes WHERE cohort_id=? AND primary_outcome IS NOT NULL"
    p: List[Any] = [COHORT_ID]
    if tier:
        q += " AND tier=?"
        p.append(tier)
    if independent_only:
        q += " AND independent=1"
    if strategy:
        q += " AND strategy=?"
        p.append(strategy)
    with DB_LOCK, db_connect() as conn:
        return int(conn.execute(q, p).fetchone()["n"])

def recent_episode_exists(symbol: str, side: str, strategy: str, tier: str, seconds: int) -> bool:
    cutoff = now_ts() - max(0, seconds)
    with DB_LOCK, db_connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM episodes WHERE cohort_id=? AND symbol=? AND side=? "
            "AND strategy=? AND tier=? AND created_at>=? LIMIT 1",
            (COHORT_ID, symbol, side, strategy, tier, cutoff),
        ).fetchone()
    return row is not None

def independent_slot_available(cluster_id: int, side: str, strategy: str) -> bool:
    with DB_LOCK, db_connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM episodes WHERE cohort_id=? AND cluster_id=? "
            "AND side=? AND strategy=? AND tier='paper' AND independent=1 LIMIT 1",
            (COHORT_ID, cluster_id, side, strategy),
        ).fetchone()
    return row is None

def open_episode_count() -> int:
    with DB_LOCK, db_connect() as conn:
        return int(conn.execute(
            "SELECT COUNT(*) AS n FROM episodes WHERE cohort_id=? AND status='open'",
            (COHORT_ID,),
        ).fetchone()["n"])

def insert_episode(candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    created = int(candidate["created_at"])
    key = hashlib.sha256(
        f"{COHORT_ID}:{candidate['symbol']}:{candidate['side']}:{candidate['strategy']}:{candidate['tier']}:{created}:{candidate['entry_price']:.12g}".encode()
    ).hexdigest()
    values = {
        "episode_key": key,
        "cohort_id": COHORT_ID,
        "protocol_hash": PROTOCOL_HASH,
        "created_at": created,
        "cluster_id": int(candidate["cluster_id"]),
        "symbol": candidate["symbol"],
        "side": candidate["side"],
        "strategy": candidate["strategy"],
        "tier": candidate["tier"],
        "visible_paper": int(candidate["tier"] == "paper"),
        "independent": int(bool(candidate.get("independent"))),
        "policy_revision": int(candidate.get("policy_revision", 0) or 0),
        "policy_hash": str(candidate.get("policy_hash", ""))[:64],
        "quality_score": float(candidate.get("quality_score", 0.0)),
        "paper_reject_reason": str(candidate.get("paper_reject_reason", ""))[:500],
        "entry_price": float(candidate["entry_price"]),
        "entry_bid": float(candidate.get("entry_bid", 0.0)),
        "entry_ask": float(candidate.get("entry_ask", 0.0)),
        "spread_bps": float(candidate.get("spread_bps", 999.0)),
        "depth_usdt": float(candidate.get("depth_usdt", 0.0)),
        "quote_volume_24h": float(candidate.get("quote_volume_24h", 0.0)),
        "liquidity_rank": float(candidate.get("liquidity_rank", 0.0)),
        "stop_move": float(candidate["stop_move"]),
        "tp1_move": TP1_MOVE, "tp2_move": TP2_MOVE, "tp3_move": TP3_MOVE,
        "tp4_move": TP4_MOVE, "tp5_move": TP5_MOVE,
        "features_json": json.dumps(candidate["features"], ensure_ascii=False),
        "status": "open",
        "last_seen_at": created,
    }
    cols = tuple(values)
    try:
        with DB_LOCK, db_connect() as conn:
            cur = conn.execute(
                f"INSERT INTO episodes({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
                tuple(values[c] for c in cols),
            )
            eid = int(cur.lastrowid)
            conn.commit()
            row = conn.execute("SELECT * FROM episodes WHERE id=?", (eid,)).fetchone()
        return row_to_dict(row) if row else None
    except sqlite3.IntegrityError:
        return None

def get_open_episodes() -> List[Dict[str, Any]]:
    with DB_LOCK, db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM episodes WHERE cohort_id=? AND status='open' ORDER BY created_at,id",
            (COHORT_ID,),
        ).fetchall()
    return [row_to_dict(x) for x in rows]

def get_episode(eid: int) -> Optional[Dict[str, Any]]:
    with DB_LOCK, db_connect() as conn:
        row = conn.execute("SELECT * FROM episodes WHERE id=?", (eid,)).fetchone()
    return row_to_dict(row) if row else None

def update_episode(eid: int, updates: Dict[str, Any]) -> None:
    allowed = {
        "tp1_hit_at","tp2_hit_at","tp3_hit_at","tp4_hit_at","tp5_hit_at",
        "sl_hit_at","max_favorable_move","max_adverse_move","last_exit_price",
        "last_seen_at","data_gap_count","path_samples","max_sample_gap_seconds",
        "status","primary_outcome","primary_pnl_r","primary_closed_at",
        "primary_notified","entry_notified","finalized_at","final_notified",
    }
    clean = {k:v for k,v in updates.items() if k in allowed}
    if not clean:
        return
    with DB_LOCK, db_connect() as conn:
        conn.execute(
            f"UPDATE episodes SET {','.join(f'{k}=?' for k in clean)} WHERE id=?",
            (*clean.values(), eid),
        )
        conn.commit()

def horizon_exists(eid: int, horizon: int) -> bool:
    with DB_LOCK, db_connect() as conn:
        return conn.execute(
            "SELECT 1 FROM horizon_results WHERE episode_id=? AND horizon_seconds=?",
            (eid, horizon),
        ).fetchone() is not None

def insert_horizon(result: Dict[str, Any]) -> bool:
    cols = HORIZON_COLUMNS
    try:
        with DB_LOCK, db_connect() as conn:
            conn.execute(
                f"INSERT INTO horizon_results({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
                tuple(result[c] for c in cols),
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def primary_rows(
    tier: Optional[str]=None,
    independent_only: bool=False,
    strategy: Optional[str]=None,
) -> List[Dict[str,Any]]:
    q = (
        "SELECT id AS episode_id,symbol,side,strategy,tier,independent,cluster_id,created_at,"
        "primary_outcome AS outcome,primary_pnl_r AS pnl_r,primary_closed_at,data_gap_count,"
        "path_samples,max_sample_gap_seconds,protocol_hash,policy_revision,policy_hash,"
        "quality_score,features_json "
        "FROM episodes WHERE cohort_id=? AND primary_outcome IS NOT NULL"
    )
    p: List[Any] = [COHORT_ID]
    if tier:
        q += " AND tier=?"
        p.append(tier)
    if independent_only:
        q += " AND independent=1"
    if strategy:
        q += " AND strategy=?"
        p.append(strategy)
    q += " ORDER BY created_at,id"
    with DB_LOCK, db_connect() as conn:
        return [row_to_dict(r) for r in conn.execute(q,p).fetchall()]

def horizon_rows(
    horizon: int,
    tier: Optional[str]=None,
    independent_only: bool=False,
    strategy: Optional[str]=None,
) -> List[Dict[str,Any]]:
    q = (
        "SELECT h.*,e.symbol,e.side,e.strategy,e.tier,e.independent,e.cluster_id,e.created_at,"
        "e.protocol_hash,e.policy_revision,e.policy_hash,e.quality_score,"
        "e.data_gap_count,e.path_samples,e.max_sample_gap_seconds "
        "FROM horizon_results h JOIN episodes e ON e.id=h.episode_id "
        "WHERE e.cohort_id=? AND h.horizon_seconds=?"
    )
    p: List[Any] = [COHORT_ID,horizon]
    if tier:
        q += " AND e.tier=?"
        p.append(tier)
    if independent_only:
        q += " AND e.independent=1"
    if strategy:
        q += " AND e.strategy=?"
        p.append(strategy)
    q += " ORDER BY e.created_at,e.id"
    with DB_LOCK, db_connect() as conn:
        return [row_to_dict(r) for r in conn.execute(q,p).fetchall()]

def paper_trade_number(episode_id: int) -> int:
    """Stable visible sequence number for every real PAPER observation."""
    with DB_LOCK, db_connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM episodes "
            "WHERE cohort_id=? AND tier='paper' AND id<=?",
            (COHORT_ID, int(episode_id)),
        ).fetchone()
    return int(row["n"] if row else 0)

def _utc_text(timestamp: Any) -> str:
    try:
        value = int(timestamp or 0)
    except Exception:
        value = 0
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value)) if value else ""

def paper_trade_ledger() -> List[Dict[str, Any]]:
    """Human-readable ledger: open and closed PAPER trades, never OBSERVER."""
    with DB_LOCK, db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM episodes WHERE cohort_id=? AND tier='paper' ORDER BY id",
            (COHORT_ID,),
        ).fetchall()
    ledger: List[Dict[str, Any]] = []
    for number, raw in enumerate(rows, 1):
        row = row_to_dict(raw)
        entry = float(row.get("entry_price", 0.0) or 0.0)
        side = str(row.get("side", ""))
        try:
            features = json.loads(row.get("features_json") or "{}")
        except Exception:
            features = {}
        ledger.append({
            "trade_no": number,
            "episode_id": int(row["id"]),
            "visible_in_telegram": True,
            "hidden_shadow_trade": False,
            "created_at": int(row.get("created_at", 0) or 0),
            "created_at_utc": _utc_text(row.get("created_at")),
            "symbol": row.get("symbol"),
            "side": side,
            "strategy": row.get("strategy"),
            "independent": bool(row.get("independent")),
            "policy_revision": int(row.get("policy_revision", 0) or 0),
            "policy_hash": str(row.get("policy_hash", "")),
            "quality_score": float(row.get("quality_score", 0.0) or 0.0),
            "entry_price": entry,
            "tp1_price": price_target(entry, side, TP1_MOVE),
            "tp2_price": price_target(entry, side, TP2_MOVE),
            "tp3_price": price_target(entry, side, TP3_MOVE),
            "tp4_price": price_target(entry, side, TP4_MOVE),
            "tp5_price": price_target(entry, side, TP5_MOVE),
            "sl_price": stop_price(entry, side, float(row.get("stop_move", 0.0) or 0.0)),
            "spread_bps": float(row.get("spread_bps", 0.0) or 0.0),
            "depth_usdt": float(row.get("depth_usdt", 0.0) or 0.0),
            "status": row.get("status"),
            "primary_outcome": row.get("primary_outcome"),
            "primary_pnl_r": row.get("primary_pnl_r"),
            "primary_closed_at": row.get("primary_closed_at"),
            "primary_closed_at_utc": _utc_text(row.get("primary_closed_at")),
            "mfe_move": float(row.get("max_favorable_move", 0.0) or 0.0),
            "mae_move": float(row.get("max_adverse_move", 0.0) or 0.0),
            "entry_message_sent_or_queued": bool(row.get("entry_notified")),
            "result_message_sent_or_queued": bool(row.get("primary_notified")),
            "final_message_sent_or_queued": bool(row.get("final_notified")),
            "adaptive_level_at_entry": int(features.get("adaptive_level", 0) or 0),
        })
    return ledger


# ---------------------------------------------------------------------------
# Seed/export
# ---------------------------------------------------------------------------

def restore_seed_if_empty() -> Dict[str,Any]:
    init_db()
    if table_count("episodes") > 0:
        report = {"restored":0,"reason":"database_not_empty"}
        RUNTIME["seed_restore"] = report
        return report
    path = Path(SEED_PATH)
    if not path.exists():
        report = {"restored":0,"reason":"seed_not_found","path":str(path)}
        RUNTIME["seed_restore"] = report
        return report
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        report = {"restored":0,"reason":f"seed_invalid:{exc!r}"}
        RUNTIME["seed_restore"] = report
        return report

    # V18.4 restores only the exact same schema/cohort. Old data is audit-only.
    if payload.get("export_schema") == EXPORT_SCHEMA and payload.get("cohort_id") == COHORT_ID:
        episodes = payload.get("episodes") or []
        horizons = payload.get("horizon_results") or []
        ie = ih = 0
        with DB_LOCK, db_connect() as conn:
            for row in episodes:
                clean = {c:row.get(c) for c in EPISODE_COLUMNS if c in row}
                cols = tuple(clean)
                if not cols:
                    continue
                conn.execute(
                    f"INSERT OR IGNORE INTO episodes({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
                    tuple(clean[c] for c in cols),
                )
                ie += int(conn.execute("SELECT changes() n").fetchone()["n"])
            for row in horizons:
                clean = {c:row.get(c) for c in HORIZON_COLUMNS if c in row}
                cols = tuple(clean)
                if not cols:
                    continue
                conn.execute(
                    f"INSERT OR IGNORE INTO horizon_results({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
                    tuple(clean[c] for c in cols),
                )
                ih += int(conn.execute("SELECT changes() n").fetchone()["n"])
            conn.commit()
        state = payload.get("state") if isinstance(payload.get("state"), dict) else {}
        if isinstance(state.get("adaptive_policy"), dict):
            meta_set("adaptive_policy", normalize_adaptive_policy(state["adaptive_policy"]))
        if isinstance(state.get("adaptive_history"), list):
            meta_set("adaptive_history", state["adaptive_history"][-200:])
        restored_paper = primary_count("paper")
        for key in ("last_audit_paper", "last_backup_paper"):
            try:
                value = max(0, min(int(state.get(key, 0) or 0), restored_paper))
            except Exception:
                value = 0
            meta_set(key, value)
        if isinstance(state.get("last_adaptive_review"), dict):
            meta_set("last_adaptive_review", state["last_adaptive_review"])
        if str(state.get("research_leader", "")) in (*STRATEGIES, "INSUFFICIENT_DATA"):
            meta_set("research_leader", state["research_leader"])
        report = {"restored":ie,"horizons":ih,"reason":"v18_4_backup_restored"}
    else:
        # Preserve a compact, verifiable audit note but never train/mix old
        # V17/V18.2 results into this new forward protocol.
        old_rows=list(payload.get("episodes") or payload.get("adaptive_trades") or [])
        old_count=len(old_rows)
        old_outcomes=Counter(
            str(row.get("primary_outcome") or row.get("result") or "unknown")
            for row in old_rows
        )
        old_pnl=[
            float(row.get("primary_pnl_r") if row.get("primary_pnl_r") is not None else row.get("pnl_r") or 0.0)
            for row in old_rows
        ]
        last25=old_rows[-25:]
        last25_outcomes=Counter(
            str(row.get("primary_outcome") or row.get("result") or "unknown")
            for row in last25
        )
        legacy={
            "count":old_count,
            "profit":int(old_outcomes.get("profit",0)),
            "sl":int(old_outcomes.get("sl",0)),
            "expired":int(old_outcomes.get("expired",0)),
            "expectancy_r":statistics.fmean(old_pnl) if old_pnl else 0.0,
            "last25":{
                "n":len(last25),
                "profit":int(last25_outcomes.get("profit",0)),
                "sl":int(last25_outcomes.get("sl",0)),
                "expired":int(last25_outcomes.get("expired",0)),
            },
            "policy":"audit_only_never_training",
        }
        meta_set("legacy_audit",legacy)
        report = {"restored":0,"legacy_audit_count":old_count,"reason":"old_schema_audit_only"}
    RUNTIME["seed_restore"] = report
    return report

def export_payload(
    state_overrides: Optional[Dict[str, Any]] = None,
    audit_context: Optional[Dict[str, Any]] = None,
) -> Dict[str,Any]:
    overrides = state_overrides if isinstance(state_overrides, dict) else {}
    with DB_LOCK, db_connect() as conn:
        episodes = [row_to_dict(r) for r in conn.execute("SELECT * FROM episodes ORDER BY id").fetchall()]
        horizons = [row_to_dict(r) for r in conn.execute(
            "SELECT * FROM horizon_results ORDER BY episode_id,horizon_seconds"
        ).fetchall()]
    return {
        "export_schema":EXPORT_SCHEMA,
        "exported_at":now_ts(),
        "app":APP_NAME,
        "deploy_marker":DEPLOY_MARKER,
        "protocol":PROTOCOL_MANIFEST,
        "protocol_hash":PROTOCOL_HASH,
        "cohort_id":COHORT_ID,
        "legacy_audit":meta_get("legacy_audit",{}),
        "episodes":episodes,
        "horizon_results":horizons,
        "paper_trade_ledger":paper_trade_ledger(),
        "state":{
            "adaptive_policy":adaptive_policy_state(),
            "adaptive_history":meta_get("adaptive_history",[]),
            "last_audit_paper":int(overrides.get(
                "last_audit_paper",meta_get("last_audit_paper",0)
            ) or 0),
            "last_backup_paper":int(overrides.get(
                "last_backup_paper",meta_get("last_backup_paper",0)
            ) or 0),
            "research_leader":meta_get("research_leader","INSUFFICIENT_DATA"),
            "last_adaptive_review":meta_get("last_adaptive_review",{}),
        },
        "runtime":runtime_snapshot(),
        "adaptive_review": champion_challenger_review(),
        "milestone_audit":audit_context or {},
        "warning":"RESEARCH/PAPER only. Rename newest same-schema V18.4 Telegram file to adaptive_seed.json before redeploy without persistent disk.",
    }

def export_bytes(
    state_overrides: Optional[Dict[str, Any]] = None,
    audit_context: Optional[Dict[str, Any]] = None,
) -> bytes:
    return json.dumps(
        export_payload(state_overrides, audit_context),
        ensure_ascii=False,
        indent=2,
        allow_nan=False,
    ).encode()


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def telegram_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

def _tg_text(text: str) -> Tuple[bool,str]:
    if not telegram_configured():
        return False,"Telegram credentials not configured"
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    err="unknown"
    for attempt in range(TELEGRAM_RETRIES):
        try:
            r=requests.post(url,data={
                "chat_id":TELEGRAM_CHAT_ID,
                "text":text,
                "disable_web_page_preview":"true",
            },timeout=REQUEST_TIMEOUT_SECONDS)
            if r.ok:
                RUNTIME["telegram_sent"] += 1
                return True,""
            err=f"HTTP {r.status_code}: {r.text[:180]}"
        except Exception as exc:
            err=repr(exc)
        time.sleep(.35*(2**attempt))
    RUNTIME["telegram_errors"] += 1
    return False,err

def _tg_doc(data: bytes,filename: str,caption: str) -> Tuple[bool,str]:
    if not telegram_configured():
        return False,"Telegram credentials not configured"
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    err="unknown"
    for attempt in range(TELEGRAM_RETRIES):
        try:
            r=requests.post(
                url,
                data={"chat_id":TELEGRAM_CHAT_ID,"caption":caption[:1024]},
                files={"document":(filename,io.BytesIO(data),"application/json")},
                timeout=max(20.0,REQUEST_TIMEOUT_SECONDS*2),
            )
            if r.ok:
                RUNTIME["telegram_sent"] += 1
                return True,""
            err=f"HTTP {r.status_code}: {r.text[:180]}"
        except Exception as exc:
            err=repr(exc)
        time.sleep(.5*(2**attempt))
    RUNTIME["telegram_errors"] += 1
    return False,err

def enqueue_outbox(kind: str,payload: bytes,filename: str="",caption: str="") -> bool:
    with DB_LOCK, db_connect() as conn:
        conn.execute(
            "INSERT INTO outbox(kind,payload,filename,caption,created_at,next_retry_at) VALUES(?,?,?,?,?,?)",
            (kind,sqlite3.Binary(payload),filename,caption,now_ts(),now_ts()+15),
        )
        conn.commit()
    return True

def send_text(text: str,critical: bool=True) -> bool:
    ok,err=_tg_text(text)
    if not ok:
        set_runtime_error(f"Telegram text: {err}")
        if critical and telegram_configured():
            return enqueue_outbox("text",text.encode())
    return ok

def send_document(data: bytes,filename: str,caption: str,critical: bool=True) -> bool:
    ok,err=_tg_doc(data,filename,caption)
    if not ok:
        set_runtime_error(f"Telegram document: {err}")
        if critical and telegram_configured():
            return enqueue_outbox("document",data,filename,caption)
    return ok

def flush_outbox(limit: int=5) -> Dict[str,int]:
    if not telegram_configured():
        return {"sent":0,"remaining":table_count("outbox")}
    with DB_LOCK, db_connect() as conn:
        rows=conn.execute(
            "SELECT * FROM outbox WHERE next_retry_at<=? ORDER BY id LIMIT ?",
            (now_ts(),max(1,limit)),
        ).fetchall()
    sent=0
    for row in rows:
        payload=bytes(row["payload"])
        if row["kind"]=="document":
            ok,err=_tg_doc(payload,str(row["filename"] or "backup.json"),str(row["caption"] or ""))
        else:
            ok,err=_tg_text(payload.decode(errors="replace"))
        with DB_LOCK,db_connect() as conn:
            if ok:
                conn.execute("DELETE FROM outbox WHERE id=?",(row["id"],)); sent+=1
            else:
                attempts=int(row["attempts"] or 0)+1
                delay=min(1800,15*(2**min(attempts,7)))
                conn.execute("UPDATE outbox SET attempts=?,next_retry_at=? WHERE id=?",
                             (attempts,now_ts()+delay,row["id"]))
                set_runtime_error(f"Telegram outbox: {err}")
            conn.commit()
        if not ok:
            break
    return {"sent":sent,"remaining":table_count("outbox")}


# ---------------------------------------------------------------------------
# Public BingX REST market data
# ---------------------------------------------------------------------------

class SlidingRateLimiter:
    def __init__(self,max_calls: int=82,window_seconds: float=10.0)->None:
        self.max_calls=max_calls; self.window=window_seconds
        self.calls: Deque[float]=deque(); self.lock=threading.Lock()
    def acquire(self)->None:
        while True:
            with self.lock:
                now=time.monotonic()
                while self.calls and now-self.calls[0]>=self.window:
                    self.calls.popleft()
                if len(self.calls)<self.max_calls:
                    self.calls.append(now); return
                wait=max(.01,self.window-(now-self.calls[0])+.01)
            time.sleep(min(wait,1.0))

API_LIMITER=SlidingRateLimiter()
KLINE_CACHE: Dict[str,Tuple[float,Optional[List[Dict[str,float]]]]]={}
TICKER_CACHE: Tuple[float,List[Dict[str,Any]]]=(0.0,[])

def api_get(path: str,params: Optional[Dict[str,Any]]=None)->Optional[Dict[str,Any]]:
    err="unknown"
    for attempt in range(API_RETRIES):
        API_LIMITER.acquire()
        try:
            RUNTIME["api_calls"]+=1
            r=requests.get(BINGX_BASE_URL+path,params=params or {},timeout=REQUEST_TIMEOUT_SECONDS)
            if r.status_code in {408,425,429,500,502,503,504}:
                err=f"HTTP {r.status_code} {path}"; time.sleep(.25*(attempt+1)); continue
            r.raise_for_status()
            payload=r.json()
            if isinstance(payload,dict) and payload.get("code") in (None,0,"0"):
                return payload
            err=f"BingX {path}: {str(payload)[:180]}"
        except Exception as exc:
            err=f"{path}: {exc!r}"
            time.sleep(.3*(attempt+1))
    RUNTIME["api_errors"]+=1
    set_runtime_error(err)
    return None

def normalize_symbol(symbol: str)->str:
    s=str(symbol or "").upper().replace("/","-").replace("_","-")
    if "-" not in s and s.endswith("USDT"):
        s=s[:-4]+"-USDT"
    return s

def good_symbol(symbol: str)->bool:
    s=normalize_symbol(symbol)
    if not s.endswith("-USDT"): return False
    base=s.split("-",1)[0]
    if not base or base in {"USDT","USDC","FDUSD","TUSD","DAI"}: return False
    if any(tag in base for tag in ("BULL","BEAR","UP","DOWN")): return False
    return True

def _first_float(item: Dict[str,Any],names: Sequence[str])->float:
    for n in names:
        try:
            v=float(item.get(n) or 0)
            if math.isfinite(v) and v!=0: return v
        except Exception: pass
    return 0.0

def fetch_tickers(force: bool=False)->List[Dict[str,Any]]:
    global TICKER_CACHE
    with CACHE_LOCK:
        if not force and time.time()-TICKER_CACHE[0]<45 and TICKER_CACHE[1]:
            return list(TICKER_CACHE[1])
    payload=api_get("/openApi/swap/v2/quote/ticker")
    data=payload.get("data") if isinstance(payload,dict) else None
    if isinstance(data,dict): data=[data]
    out=[]
    if isinstance(data,list):
        for item in data:
            if not isinstance(item,dict): continue
            symbol=normalize_symbol(str(item.get("symbol") or item.get("s") or ""))
            if not good_symbol(symbol): continue
            last=_first_float(item,("lastPrice","last","price","close"))
            quote=_first_float(item,("quoteVolume","quoteVol","quoteVolume24h","quoteAssetVolume","turnover","amount"))
            basevol=_first_float(item,("volume","vol","volume24h"))
            if quote<=0 and last>0 and basevol>0: quote=last*basevol
            if last<=0 or quote<=0: continue
            out.append({"symbol":symbol,"last":last,"quote_volume_24h":quote})
    out.sort(key=lambda x:x["quote_volume_24h"],reverse=True)
    with CACHE_LOCK:
        if out: TICKER_CACHE=(time.time(),list(out))
    return out

def liquid_universe()->List[Dict[str,Any]]:
    rows=[x for x in fetch_tickers() if x["quote_volume_24h"]>=MIN_24H_QUOTE_VOLUME_USDT][:MAX_UNIVERSE]
    total=max(1,len(rows)-1)
    for i,row in enumerate(rows):
        row["liquidity_rank"]=1.0-i/total
    return rows

def parse_klines(raw: Any)->Optional[List[Dict[str,float]]]:
    if not raw: return None
    out=[]
    for item in raw:
        try:
            if isinstance(item,dict):
                c={
                    "time":int(item.get("time") or item.get("openTime") or item.get("T") or 0),
                    "open":float(item.get("open")),
                    "high":float(item.get("high")),
                    "low":float(item.get("low")),
                    "close":float(item.get("close")),
                    "volume":float(item.get("volume") or item.get("vol") or 0),
                }
            elif isinstance(item,(list,tuple)) and len(item)>=6:
                c={"time":int(item[0]),"open":float(item[1]),"high":float(item[2]),
                   "low":float(item[3]),"close":float(item[4]),"volume":float(item[5])}
            else: continue
            if all(c[k]>0 for k in ("open","high","low","close")): out.append(c)
        except Exception: continue
    out.sort(key=lambda x:x["time"])
    return out if len(out)>=40 else None

def get_klines(symbol: str,limit: int=90,cache_seconds: int=35)->Optional[List[Dict[str,float]]]:
    s=normalize_symbol(symbol); key=f"{s}:1m:{limit}"
    with CACHE_LOCK:
        cached=KLINE_CACHE.get(key)
        if cached and time.time()-cached[0]<cache_seconds: return cached[1]
    for ep in ("/openApi/swap/v3/quote/klines","/openApi/swap/v2/quote/klines"):
        payload=api_get(ep,{"symbol":s,"interval":"1m","limit":limit})
        candles=parse_klines(payload.get("data") if isinstance(payload,dict) else None)
        if candles:
            with CACHE_LOCK: KLINE_CACHE[key]=(time.time(),candles)
            return candles
    with CACHE_LOCK: KLINE_CACHE[key]=(time.time(),None)
    return None

def _book_level(level: Any)->Tuple[float,float]:
    try:
        if isinstance(level,dict):
            return float(level.get("price") or level.get("p") or 0),float(level.get("quantity") or level.get("qty") or level.get("q") or 0)
        if isinstance(level,(list,tuple)) and len(level)>=2:
            return float(level[0]),float(level[1])
    except Exception: pass
    return 0.0,0.0

def get_book(symbol: str,include_depth: bool=False)->Dict[str,Any]:
    s=normalize_symbol(symbol)
    payload=api_get("/openApi/swap/v2/quote/bookTicker",{"symbol":s})
    data=payload.get("data") if isinstance(payload,dict) else None
    if isinstance(data,list): data=data[0] if data else None
    bid=ask=bq=aq=0.0
    if isinstance(data,dict):
        bid=_first_float(data,("bidPrice","bid","b")); ask=_first_float(data,("askPrice","ask","a"))
        bq=_first_float(data,("bidQty","bidQuantity","B")); aq=_first_float(data,("askQty","askQuantity","A"))
    bids=[]; asks=[]
    if include_depth:
        depth=api_get("/openApi/swap/v2/quote/depth",{"symbol":s,"limit":5})
        dd=depth.get("data") if isinstance(depth,dict) else None
        if isinstance(dd,dict):
            bids=list(dd.get("bids") or [])[:5]; asks=list(dd.get("asks") or [])[:5]
            if bids and asks:
                bid,bq=_book_level(bids[0]); ask,aq=_book_level(asks[0])
    if bid<=0 or ask<=bid:
        return {"ok":False,"bid":0.0,"ask":0.0,"spread_bps":999.0,"depth_usdt":0.0}
    mid=(bid+ask)/2
    spread=(ask-bid)/max(mid,1e-12)*10000
    if bids and asks:
        bd=sum(p*q for p,q in map(_book_level,bids)); ad=sum(p*q for p,q in map(_book_level,asks))
    else:
        bd=bid*max(0,bq); ad=ask*max(0,aq)
    return {"ok":True,"bid":bid,"ask":ask,"spread_bps":spread,"depth_usdt":min(bd,ad)}

def get_books(symbols: Sequence[str])->Dict[str,Dict[str,Any]]:
    wanted={normalize_symbol(s) for s in symbols if s}
    if not wanted: return {}
    payload=api_get("/openApi/swap/v2/quote/bookTicker")
    raw=payload.get("data") if isinstance(payload,dict) else None
    if isinstance(raw,dict): raw=[raw]
    books={}
    if isinstance(raw,list):
        for item in raw:
            if not isinstance(item,dict): continue
            s=normalize_symbol(str(item.get("symbol") or item.get("s") or ""))
            if s not in wanted: continue
            bid=_first_float(item,("bidPrice","bid","b")); ask=_first_float(item,("askPrice","ask","a"))
            if bid<=0 or ask<=bid: continue
            bq=_first_float(item,("bidQty","bidQuantity","B")); aq=_first_float(item,("askQty","askQuantity","A"))
            mid=(bid+ask)/2
            books[s]={"ok":True,"bid":bid,"ask":ask,
                      "spread_bps":(ask-bid)/max(mid,1e-12)*10000,
                      "depth_usdt":min(bid*bq,ask*aq)}
    missing=sorted(wanted.difference(books))
    if missing:
        with ThreadPoolExecutor(max_workers=min(4,len(missing))) as pool:
            fs={pool.submit(get_book,s,False):s for s in missing}
            for f in as_completed(fs):
                s=fs[f]
                try: books[s]=f.result()
                except Exception: books[s]={"ok":False}
    return books


# ---------------------------------------------------------------------------
# Features / signal protocol
# ---------------------------------------------------------------------------

def _median(values: Iterable[float],default: float=0.0)->float:
    clean=[]
    for x in values:
        try:
            v=float(x)
            if math.isfinite(v): clean.append(v)
        except Exception: pass
    return statistics.median(clean) if clean else default

def ema(values: Sequence[float],period: int)->float:
    if not values: return 0.0
    a=2/(max(1,period)+1); v=float(values[0])
    for x in values[1:]: v=a*float(x)+(1-a)*v
    return v

def ema_series(values: Sequence[float],period: int)->List[float]:
    if not values:return []
    a=2/(max(1,period)+1);out=[float(values[0])]
    for x in values[1:]:out.append(a*float(x)+(1-a)*out[-1])
    return out

def adx14(candles: Sequence[Dict[str,float]],period: int=14)->float:
    """Wilder ADX on completed candles; returns 0 when history is insufficient."""
    if len(candles)<period*2+2:return 0.0
    tr: List[float]=[];plus_dm: List[float]=[];minus_dm: List[float]=[]
    for i in range(1,len(candles)):
        cur=candles[i];prev=candles[i-1]
        up=cur["high"]-prev["high"];down=prev["low"]-cur["low"]
        plus_dm.append(up if up>down and up>0 else 0.0)
        minus_dm.append(down if down>up and down>0 else 0.0)
        tr.append(max(cur["high"]-cur["low"],abs(cur["high"]-prev["close"]),abs(cur["low"]-prev["close"])))
    sm_tr=sum(tr[:period]);sm_plus=sum(plus_dm[:period]);sm_minus=sum(minus_dm[:period])
    dx: List[float]=[]
    for i in range(period-1,len(tr)):
        if i>=period:
            sm_tr=sm_tr-sm_tr/period+tr[i]
            sm_plus=sm_plus-sm_plus/period+plus_dm[i]
            sm_minus=sm_minus-sm_minus/period+minus_dm[i]
        if sm_tr<=0:dx.append(0.0);continue
        plus_di=100*sm_plus/sm_tr;minus_di=100*sm_minus/sm_tr
        den=plus_di+minus_di
        dx.append(100*abs(plus_di-minus_di)/den if den>0 else 0.0)
    if len(dx)<period:return 0.0
    value=statistics.fmean(dx[:period])
    for x in dx[period:]:value=((period-1)*value+x)/period
    return float(value)

def vwap(candles: Sequence[Dict[str,float]],bars: int=20)->float:
    part=list(candles[-bars:]); vol=sum(max(0,r["volume"]) for r in part)
    if vol<=0: return part[-1]["close"] if part else 0
    return sum(((r["high"]+r["low"]+r["close"])/3)*max(0,r["volume"]) for r in part)/vol

def atr_percent(candles: Sequence[Dict[str,float]],bars: int=14)->float:
    if len(candles)<bars+1:return 0
    rs=[]
    for i in range(len(candles)-bars,len(candles)):
        r=candles[i]; pc=candles[i-1]["close"]
        rs.append(max(r["high"]-r["low"],abs(r["high"]-pc),abs(r["low"]-pc)))
    return statistics.fmean(rs)/max(candles[-1]["close"],1e-12)

def directional_return(candles: Sequence[Dict[str,float]],bars: int)->float:
    if len(candles)<=bars:return 0
    return (candles[-1]["close"]-candles[-1-bars]["close"])/max(candles[-1-bars]["close"],1e-12)

def close_location(c: Dict[str,float])->float:
    return (c["close"]-c["low"])/max(c["high"]-c["low"],1e-12)

def completed_volume_ratio(candles: Sequence[Dict[str,float]])->float:
    if len(candles)<24:return 0
    ref=_median((r["volume"] for r in candles[-22:-2]),0)
    return candles[-2]["volume"]/ref if ref>0 else 0

def completed_range_ratio(candles: Sequence[Dict[str,float]])->float:
    if len(candles)<24:return 0
    ref=_median((r["high"]-r["low"] for r in candles[-22:-2]),0)
    return (candles[-2]["high"]-candles[-2]["low"])/ref if ref>0 else 0

def continuity_features(candles: Sequence[Dict[str,float]])->Tuple[float,float]:
    part=list(candles[-60:])
    if not part:return 0,0
    active=sum(1 for r in part if r["volume"]>0)/len(part)
    unique=len({round(r["close"],10) for r in part})/len(part)
    return active,unique

def btc_context()->Dict[str,Any]:
    c=get_klines("BTC-USDT",90,45)
    if not c:return {"regime":"UNKNOWN","ret15":0.0,"ret60":0.0}
    r15=directional_return(c,15); r60=directional_return(c,60)
    if r15>=.003 and r60>=0: regime="BULL"
    elif r15<=-.003 and r60<=0: regime="BEAR"
    else: regime="RANGE"
    return {"regime":regime,"ret15":r15,"ret60":r60}

def build_broad_candidate(ticker: Dict[str,Any],candles: Sequence[Dict[str,float]],btc: Dict[str,Any])->Optional[Dict[str,Any]]:
    if len(candles)<65:return None
    r1=directional_return(candles,1)
    r3=directional_return(candles,3)
    r15=directional_return(candles,15)
    if r3==0 or r15==0 or r3*r15<=0:return None
    side="LONG" if r3>0 else "SHORT"
    d1=abs(r1); d3=abs(r3); d15=abs(r15)
    if not BROAD_MIN_DIRECTIONAL_3M<=d3<=BROAD_MAX_DIRECTIONAL_3M:return None
    if not BROAD_MIN_DIRECTIONAL_15M<=d15<=BROAD_MAX_DIRECTIONAL_15M:return None
    active,unique=continuity_features(candles)
    if active<MIN_ACTIVE_CANDLE_FRACTION or unique<MIN_UNIQUE_CLOSE_FRACTION:return None

    last=candles[-1]
    completed=candles[-2]
    span=max(completed["high"]-completed["low"],1e-12)
    body=abs(completed["close"]-completed["open"])/span
    loc=close_location(completed)
    e9=ema([r["close"] for r in candles[-30:]],9)
    vw=vwap(candles,20)
    vol1=completed_volume_ratio(candles)
    range1=completed_range_ratio(candles)
    atr=atr_percent(candles)
    stop=min(MAX_STOP_MOVE,max(MIN_STOP_MOVE,atr*ATR_STOP_MULTIPLIER))
    current=last["close"]
    vwap_distance=abs(current-vw)/max(vw,1e-12)
    d3_share=d3/max(d15,1e-12)

    completed_aligned=(side=="LONG" and completed["close"]>completed["open"]) or (side=="SHORT" and completed["close"]<completed["open"])
    current_aligned=(side=="LONG" and r1>0) or (side=="SHORT" and r1<0)
    location_aligned=(side=="LONG" and loc>=PAPER_LONG_MIN_CLOSE_LOCATION) or (side=="SHORT" and loc<=PAPER_SHORT_MAX_CLOSE_LOCATION)
    avg_aligned=(side=="LONG" and current>e9 and current>vw) or (side=="SHORT" and current<e9 and current<vw)
    btc_opposite=(side=="LONG" and btc.get("regime")=="BEAR") or (side=="SHORT" and btc.get("regime")=="BULL")

    return {
        "created_at":now_ts(),
        "cluster_id":now_ts()//INDEPENDENT_CLUSTER_SECONDS,
        "symbol":ticker["symbol"],"side":side,"strategy":STRATEGY_MOMENTUM,
        "last_price":float(current),
        "quote_volume_24h":float(ticker["quote_volume_24h"]),
        "liquidity_rank":float(ticker.get("liquidity_rank",0)),
        "stop_move":stop,
        "features":{
            "directional_1m":d1,"signed_1m":r1,
            "directional_3m":d3,"signed_3m":r3,
            "directional_15m":d15,"signed_15m":r15,
            "directional_30m":abs(directional_return(candles,30)),
            "d3_share_of_d15":d3_share,
            "vol1":vol1,"range1":range1,
            "body_fraction":body,"close_location":loc,
            "ema9":e9,"vwap20":vw,"vwap_distance":vwap_distance,
            "atr_pct":atr,"active_fraction":active,"unique_close_fraction":unique,
            "completed_candle_aligned":completed_aligned,
            "current_1m_aligned":current_aligned,
            "aligned_location":location_aligned,
            "aligned_average":avg_aligned,
            "btc_regime":btc.get("regime","UNKNOWN"),
            "btc_ret15":float(btc.get("ret15",0) or 0),
            "btc_ret60":float(btc.get("ret60",0) or 0),
            "btc_opposite":btc_opposite,
        }
    }

def build_pullback_candidate(
    ticker: Dict[str,Any],
    candles: Sequence[Dict[str,float]],
    btc: Dict[str,Any],
)->Optional[Dict[str,Any]]:
    """Build a fixed, closed-candle trend/pullback/reclaim challenger.

    The last kline can still be forming, so all pattern decisions use
    candles[:-1].  This avoids one of the old bot's main sources of unstable
    signals: treating an unfinished one-minute candle as final evidence.
    """
    if len(candles)<70:return None
    closed=list(candles[:-1])
    if len(closed)<65:return None
    r1=directional_return(closed,1);r3=directional_return(closed,3)
    r15=directional_return(closed,15);r30=directional_return(closed,30)
    if r15==0 or r30==0 or r15*r30<=0:return None
    side="LONG" if r30>0 else "SHORT"
    d1=abs(r1);d3=abs(r3);d15=abs(r15);d30=abs(r30)
    # Broad diagnostic range only. The fixed PAPER rules are applied below.
    if not .0040<=d15<=.0800 or not .0060<=d30<=.1200:return None
    active,unique=continuity_features(closed)
    if active<MIN_ACTIVE_CANDLE_FRACTION or unique<MIN_UNIQUE_CLOSE_FRACTION:return None

    closes=[r["close"] for r in closed]
    e9=ema(closes,9);e20=ema(closes,20);e50=ema(closes,50)
    e20_prev=ema(closes[:-3],20) if len(closes)>3 else e20
    vw=vwap(closed,20);atr=atr_percent(closed);adx=adx14(closed)
    trigger=closed[-1];pull_window=closed[-3:-1]
    trigger_span=max(trigger["high"]-trigger["low"],1e-12)
    trigger_body=abs(trigger["close"]-trigger["open"])/trigger_span
    trigger_loc=close_location(trigger)
    vol1=completed_volume_ratio(candles)
    range1=completed_range_ratio(candles)
    tolerance=max(atr,1e-8)*PULLBACK_EMA20_TOLERANCE_ATR

    anchor=float(closed[-12]["close"])
    if side=="LONG":
        impulse_extreme=max(r["high"] for r in closed[-11:-2])
        impulse_move=max(0.0,(impulse_extreme-anchor)/max(anchor,1e-12))
        pull_extreme=min(r["low"] for r in pull_window)
        retrace=(impulse_extreme-pull_extreme)/max(impulse_extreme-anchor,1e-12)
        trend_stack=e9>e20>e50 and e20>e20_prev
        pullback_present=any(r["close"]<r["open"] for r in pull_window)
        ema_retest=pull_extreme<=e9*(1+tolerance) and min(r["close"] for r in pull_window)>=e20*(1-tolerance)
        reclaim=(trigger["close"]>max(r["high"] for r in pull_window) and trigger["close"]>trigger["open"])
        location_ok=trigger_loc>=PULLBACK_LONG_MIN_CLOSE_LOCATION
        average_ok=trigger["close"]>e9 and trigger["close"]>vw
        btc_opposite=btc.get("regime")=="BEAR"
    else:
        impulse_extreme=min(r["low"] for r in closed[-11:-2])
        impulse_move=max(0.0,(anchor-impulse_extreme)/max(anchor,1e-12))
        pull_extreme=max(r["high"] for r in pull_window)
        retrace=(pull_extreme-impulse_extreme)/max(anchor-impulse_extreme,1e-12)
        trend_stack=e9<e20<e50 and e20<e20_prev
        pullback_present=any(r["close"]>r["open"] for r in pull_window)
        ema_retest=pull_extreme>=e9*(1-tolerance) and max(r["close"] for r in pull_window)<=e20*(1+tolerance)
        reclaim=(trigger["close"]<min(r["low"] for r in pull_window) and trigger["close"]<trigger["open"])
        location_ok=trigger_loc<=PULLBACK_SHORT_MAX_CLOSE_LOCATION
        average_ok=trigger["close"]<e9 and trigger["close"]<vw
        btc_opposite=btc.get("regime")=="BULL"

    stop=min(MAX_STOP_MOVE,max(MIN_STOP_MOVE,atr*ATR_STOP_MULTIPLIER))
    return {
        "created_at":now_ts(),"cluster_id":now_ts()//INDEPENDENT_CLUSTER_SECONDS,
        "symbol":ticker["symbol"],"side":side,"strategy":STRATEGY_PULLBACK,
        "last_price":float(candles[-1]["close"]),
        "quote_volume_24h":float(ticker["quote_volume_24h"]),
        "liquidity_rank":float(ticker.get("liquidity_rank",0)),"stop_move":stop,
        "features":{
            "directional_1m":d1,"signed_1m":r1,
            "directional_3m":d3,"signed_3m":r3,
            "directional_15m":d15,"signed_15m":r15,
            "directional_30m":d30,"signed_30m":r30,
            "vol1":vol1,"range1":range1,"adx14":adx,
            "ema9":e9,"ema20":e20,"ema50":e50,"vwap20":vw,
            "atr_pct":atr,"trigger_close":trigger["close"],
            "trigger_body_fraction":trigger_body,"trigger_close_location":trigger_loc,
            "impulse_move":impulse_move,"pullback_retrace":retrace,
            "trend_stack":trend_stack,"pullback_present":pullback_present,
            "ema_retest":ema_retest,"reclaim_trigger":reclaim,
            "aligned_location":location_ok,"aligned_average":average_ok,
            "active_fraction":active,"unique_close_fraction":unique,
            "btc_regime":btc.get("regime","UNKNOWN"),"btc_opposite":btc_opposite,
        },
    }

def net_payoff_ratio(stop_move: float)->float:
    """Net TP3 winner divided by the absolute net SL loser.

    Entry/exit spread is represented by executable bid/ask prices.  This
    protocol also subtracts two taker fees plus a conservative slippage
    allowance, so the gate cannot advertise a gross RR that disappears after
    execution costs.
    """
    stop=max(float(stop_move),1e-12)
    net_reward=max(0.0,TP3_MOVE-ROUND_TRIP_COST_MOVE)
    net_loss=stop+ROUND_TRIP_COST_MOVE
    return net_reward/max(net_loss,1e-12)

def apply_execution_and_paper_gate(
    candidate: Dict[str,Any],
    book: Dict[str,Any],
    adaptive_policy: Optional[Dict[str,Any]] = None,
)->Dict[str,Any]:
    item=dict(candidate);f=dict(item["features"]);side=item["side"]
    strategy=str(item.get("strategy") or STRATEGY_MOMENTUM)
    ok=bool(book.get("ok")); bid=float(book.get("bid",0) or 0); ask=float(book.get("ask",0) or 0)
    fallback=float(item["last_price"])
    entry=ask if side=="LONG" and ask>0 else bid if side=="SHORT" and bid>0 else fallback
    item.update({
        "entry_price":entry,"entry_bid":bid,"entry_ask":ask,
        "spread_bps":float(book.get("spread_bps",999) or 999),
        "depth_usdt":float(book.get("depth_usdt",0) or 0),
    })
    f.update({"book_ok":ok,"spread_bps":item["spread_bps"],"depth_usdt":item["depth_usdt"],"executable_entry":entry})
    item["features"]=f

    reasons=[]
    d1=float(f["directional_1m"]);d3=float(f["directional_3m"]);d15=float(f["directional_15m"])
    if strategy==STRATEGY_PULLBACK:
        d30=float(f["directional_30m"]);adx=float(f["adx14"])
        retrace=float(f["pullback_retrace"]);atr=max(float(f["atr_pct"]),1e-8)
        trigger_close=max(float(f["trigger_close"]),1e-12)
        chase=((entry-trigger_close)/trigger_close if side=="LONG" else (trigger_close-entry)/trigger_close)
        chase_atr=max(0.0,chase)/atr
        f["entry_chase_atr"]=chase_atr
        if not PULLBACK_MIN_ADX14<=adx<=PULLBACK_MAX_ADX14:reasons.append("adx")
        if not PULLBACK_MIN_DIRECTIONAL_15M<=d15<=PULLBACK_MAX_DIRECTIONAL_15M:reasons.append("directional_15m")
        if not PULLBACK_MIN_DIRECTIONAL_30M<=d30<=PULLBACK_MAX_DIRECTIONAL_30M:reasons.append("directional_30m")
        if not f["trend_stack"]:reasons.append("ema_trend")
        if not f["pullback_present"]:reasons.append("no_pullback")
        if not PULLBACK_MIN_RETRACE<=retrace<=PULLBACK_MAX_RETRACE:reasons.append("retrace")
        if not f["ema_retest"]:reasons.append("ema_retest")
        if not f["reclaim_trigger"]:reasons.append("no_reclaim")
        if not f["aligned_location"]:reasons.append("close_location")
        if not f["aligned_average"]:reasons.append("ema_vwap")
        if float(f["trigger_body_fraction"])<PULLBACK_MIN_TRIGGER_BODY:reasons.append("trigger_body")
        if not PULLBACK_MIN_VOL1<=float(f["vol1"])<=PULLBACK_MAX_VOL1:reasons.append("vol1")
        if not PULLBACK_MIN_RANGE1<=float(f["range1"])<=PULLBACK_MAX_RANGE1:reasons.append("range1")
        if chase_atr>PULLBACK_MAX_ENTRY_CHASE_ATR:reasons.append("entry_chase")
        score=(
            min(1,adx/35)*14+min(1,d15/.025)*14+min(1,d30/.045)*12+
            (12 if f["trend_stack"] else 0)+(12 if f["ema_retest"] else 0)+
            (14 if f["reclaim_trigger"] else 0)+(8 if f["aligned_average"] else 0)+
            min(1,float(item["liquidity_rank"]))*8+max(0,6-abs(retrace-.28)*20)
        )
    else:
        share=float(f["d3_share_of_d15"])
        if not PAPER_MIN_DIRECTIONAL_1M<=d1<=PAPER_MAX_DIRECTIONAL_1M:reasons.append("fresh_1m")
        if not PAPER_MIN_DIRECTIONAL_3M<=d3<=PAPER_MAX_DIRECTIONAL_3M:reasons.append("directional_3m")
        if not PAPER_MIN_DIRECTIONAL_15M<=d15<=PAPER_MAX_DIRECTIONAL_15M:reasons.append("directional_15m")
        if share<PAPER_MIN_ACCELERATION:reasons.append("weak_acceleration")
        if share>PAPER_MAX_D3_SHARE_OF_D15:reasons.append("exhaustion")
        if not PAPER_MIN_VOL1<=float(f["vol1"])<=PAPER_MAX_VOL1:reasons.append("vol1")
        if not PAPER_MIN_RANGE1<=float(f["range1"])<=PAPER_MAX_RANGE1:reasons.append("range1")
        if not f["completed_candle_aligned"]:reasons.append("completed_candle")
        if not f["current_1m_aligned"]:reasons.append("current_1m_reversal")
        if not f["aligned_location"]:reasons.append("close_location")
        if not f["aligned_average"]:reasons.append("ema_vwap")
        if float(f["body_fraction"])<PAPER_MIN_BODY_FRACTION:reasons.append("body")
        if float(f["vwap_distance"])>PAPER_MAX_VWAP_DISTANCE:reasons.append("chase")
        score=(
            min(1,d1/.005)*15+min(1,d3/.015)*20+min(1,d15/.03)*15+
            min(1,float(f["vol1"])/1.5)*12+min(1,float(f["range1"])/2.0)*10+
            (8 if f["current_1m_aligned"] else 0)+(8 if f["aligned_average"] else 0)+
            (6 if not f["btc_opposite"] else 0)+min(1,float(item["liquidity_rank"]))*6
        )
    if f["btc_opposite"]:reasons.append("btc_opposite")
    if float(item["quote_volume_24h"])<PAPER_MIN_24H_QUOTE_VOLUME_USDT: reasons.append("quote_volume")
    if not ok: reasons.append("book")
    if item["spread_bps"]>PAPER_MAX_SPREAD_BPS: reasons.append("spread")
    if item["depth_usdt"]<PAPER_MIN_DEPTH_USDT: reasons.append("depth")
    payoff=net_payoff_ratio(float(item["stop_move"]))
    f["net_tp3_to_sl_payoff_ratio"]=payoff
    if payoff<PAPER_MIN_NET_PAYOFF_RATIO: reasons.append("net_payoff")

    # The base ranking remains fixed and interpretable.  Adaptation is a
    # prospective, pre-registered floor/cooldown policy applied only after a
    # full 25-trade block; it cannot rewrite this score formula.
    policy = (
        normalize_adaptive_policy(adaptive_policy)
        if adaptive_policy is not None else adaptive_policy_state()
    )
    lane = adaptive_lane_policy(strategy, policy)
    score_floor = float(lane["min_quality_score"])
    if score < score_floor:
        reasons.append("adaptive_quality_floor")
    item["policy_revision"] = int(lane["revision"])
    item["policy_hash"] = str(lane["policy_hash"])
    item["adaptive_cooldown_seconds"] = int(
        round(PAPER_SYMBOL_COOLDOWN_SECONDS * float(lane["cooldown_multiplier"]))
    )
    f.update({
        "adaptive_policy_schema": ADAPTIVE_POLICY_SCHEMA,
        "adaptive_global_revision": int(policy["global_revision"]),
        "adaptive_policy_revision": int(lane["revision"]),
        "adaptive_policy_hash": str(lane["policy_hash"]),
        "adaptive_level": int(lane["level"]),
        "adaptive_min_quality_score": score_floor,
        "adaptive_cooldown_multiplier": float(lane["cooldown_multiplier"]),
    })
    item["features"]=f
    item["quality_score"]=round(score,3)
    item["paper_reject_reason"]=",".join(reasons)
    item["paper_gate_pass"]=not reasons
    return item


# ---------------------------------------------------------------------------
# Metrics/readiness
# ---------------------------------------------------------------------------

def metrics(rows: Sequence[Dict[str,Any]])->Dict[str,Any]:
    # DATA_GAP is not silently counted as expired. It is a data-quality failure.
    valid=[r for r in rows if str(r.get("outcome",""))!="data_gap"]
    outcomes=Counter(str(r.get("outcome","")) for r in rows)
    pnl=[float(r.get("pnl_r",0) or 0) for r in valid]
    wins=[x for x in pnl if x>0]; losses=[x for x in pnl if x<0]
    eq=peak=dd=0.0
    for x in pnl:
        eq+=x; peak=max(peak,eq); dd=max(dd,peak-eq)
    n=len(valid); expectancy=statistics.fmean(pnl) if pnl else 0
    std=statistics.stdev(pnl) if len(pnl)>=2 else 0
    lcb=expectancy-1.645*std/math.sqrt(n) if n else -999
    pf=sum(wins)/abs(sum(losses)) if losses else (999 if wins else 0)

    # A fifteen-minute cluster may contain one LONG and one SHORT observation.
    # Treating both as independent understates uncertainty, so readiness also
    # uses an LCB calculated from equal-weight cluster means.
    cluster_values: Dict[int,List[float]]={}
    for row in valid:
        cluster_values.setdefault(int(row.get("cluster_id",0) or 0),[]).append(
            float(row.get("pnl_r",0) or 0)
        )
    cluster_means=[statistics.fmean(values) for values in cluster_values.values()]
    cluster_expectancy=statistics.fmean(cluster_means) if cluster_means else 0.0
    cluster_std=statistics.stdev(cluster_means) if len(cluster_means)>=2 else 0.0
    cluster_lcb=(
        cluster_expectancy-1.645*cluster_std/math.sqrt(len(cluster_means))
        if cluster_means else -999.0
    )

    side_rows={side:[r for r in valid if str(r.get("side","")).upper()==side]
               for side in ("LONG","SHORT")}
    side_counts={side:len(values) for side,values in side_rows.items()}
    side_expectancy={
        side:(statistics.fmean(float(r.get("pnl_r",0) or 0) for r in values) if values else 0.0)
        for side,values in side_rows.items()
    }
    side_tp3={
        side:(sum(str(r.get("outcome",""))=="profit" for r in values)/len(values) if values else 0.0)
        for side,values in side_rows.items()
    }
    side_profit_counts={
        side:sum(str(r.get("outcome",""))=="profit" for r in values)
        for side,values in side_rows.items()
    }

    regimes=set()
    for row in valid:
        raw=row.get("features_json")
        try:
            features=raw if isinstance(raw,dict) else json.loads(raw or "{}")
        except Exception:
            features={}
        regime=str(features.get("btc_regime","UNKNOWN") or "UNKNOWN").upper()
        if regime in {"BULL","BEAR","RANGE"}:
            regimes.add(regime)

    raw_n=len(rows)
    return {
        "n":n,
        "raw_n":raw_n,
        "profit":outcomes["profit"],"sl":outcomes["sl"],"expired":outcomes["expired"],
        "data_gap":outcomes["data_gap"],
        "data_gap_rate":outcomes["data_gap"]/raw_n if raw_n else 0.0,
        "tp3_rate":outcomes["profit"]/n if n else 0,
        "expectancy_r":expectancy,"expectancy_lcb90_r":lcb,
        "cluster_expectancy_r":cluster_expectancy,
        "cluster_expectancy_lcb90_r":cluster_lcb,
        "profit_factor":pf,"max_drawdown_r":dd,
        "unique_symbols":len({str(r.get("symbol","")) for r in valid}),
        "unique_clusters":len({int(r.get("cluster_id",0) or 0) for r in valid}),
        "active_days":len({int(r.get("created_at",0) or 0)//86400 for r in valid}),
        "market_regimes":sorted(regimes),
        "market_regime_count":len(regimes),
        "side_counts":side_counts,
        "side_expectancy_r":side_expectancy,
        "side_tp3_rate":side_tp3,
        "side_profit_counts":side_profit_counts,
        "max_sample_gap_seconds":max([int(r.get("max_sample_gap_seconds",0) or 0) for r in valid] or [0]),
    }

def wilson_interval(wins: int,total: int,z: float=1.96)->Tuple[float,float]:
    if total<=0:return 0,1
    p=wins/total; den=1+z*z/total
    center=(p+z*z/(2*total))/den
    half=z*math.sqrt(p*(1-p)/total+z*z/(4*total*total))/den
    return max(0,center-half),min(1,center+half)

def rows_for_current_policy(strategy: str) -> List[Dict[str, Any]]:
    lane = adaptive_lane_policy(strategy)
    revision = int(lane["revision"])
    policy_hash = str(lane["policy_hash"])
    return [
        row for row in primary_rows("paper", True, strategy)
        if int(row.get("policy_revision", -1) or 0) == revision
        and str(row.get("policy_hash", "")) == policy_hash
    ]

def apply_bounded_adaptation(block_end: int) -> Dict[str, Any]:
    """Apply at most one pre-registered tightening step per lane.

    The decision uses only independent outcomes inside the just-completed,
    non-overlapping block of 25 visible PAPER trades.  Changes are prospective
    and idempotent.  Positive/mixed blocks never loosen an earlier filter.
    """
    block_end = max(0, int(block_end))
    block_start = max(0, block_end - AUDIT_BLOCK_SIZE)
    policy_before = adaptive_policy_state()
    history = meta_get("adaptive_history", [])
    if not isinstance(history, list):
        history = []
    if block_end <= int(policy_before.get("last_completed_block_end", 0) or 0):
        for saved in reversed(history):
            if isinstance(saved, dict) and int(saved.get("block_end", -1)) == block_end:
                result = json.loads(json.dumps(saved, ensure_ascii=False))
                result["idempotent"] = True
                return result
        return {
            "at": now_ts(),
            "block_start": block_start + 1,
            "block_end": block_end,
            "status": "ALREADY_APPLIED",
            "policy_changed": False,
            "changes": [],
            "lanes": {},
            "idempotent": True,
        }

    all_paper = sorted(
        primary_rows("paper"),
        key=lambda row: (
            int(row.get("primary_closed_at", 0) or 0),
            int(row.get("episode_id", 0) or 0),
        ),
    )
    if len(all_paper) < block_end or block_end - block_start != AUDIT_BLOCK_SIZE:
        return {
            "at": now_ts(),
            "block_start": block_start + 1,
            "block_end": block_end,
            "status": "INCOMPLETE_BLOCK",
            "policy_changed": False,
            "changes": [],
            "lanes": {},
            "idempotent": False,
        }

    block = all_paper[block_start:block_end]
    policy_after = json.loads(json.dumps(policy_before, ensure_ascii=False))
    changes: List[Dict[str, Any]] = []
    lane_reports: Dict[str, Any] = {}
    changed_at = now_ts()

    for strategy in STRATEGIES:
        rows = [
            row for row in block
            if str(row.get("strategy", "")) == strategy
            and bool(row.get("independent"))
        ]
        lane_metrics = metrics(rows)
        revisions = sorted({
            int(row.get("policy_revision", 0) or 0) for row in rows
        })
        old_lane = dict(policy_before["lanes"][strategy])
        action = "HOLD"
        reason = "positive_or_mixed_block"

        if lane_metrics["n"] < ADAPTIVE_MIN_INDEPENDENT_PER_LANE_BLOCK:
            action = "HOLD_INSUFFICIENT_INDEPENDENT"
            reason = (
                f"need_{ADAPTIVE_MIN_INDEPENDENT_PER_LANE_BLOCK}_valid_independent_"
                f"got_{lane_metrics['n']}"
            )
        elif len(revisions) != 1 or revisions[0] != int(old_lane["revision"]):
            action = "HOLD_MIXED_POLICY_REVISIONS"
            reason = f"block_policy_revisions_{revisions}_current_{old_lane['revision']}"
        elif lane_metrics["data_gap_rate"] > ADAPTIVE_MAX_DATA_GAP_RATE:
            action = "HOLD_DATA_QUALITY"
            reason = f"data_gap_rate_{lane_metrics['data_gap_rate']:.3f}"
        elif (
            lane_metrics["expectancy_r"] < 0.0
            and lane_metrics["profit"] <= lane_metrics["sl"] + lane_metrics["expired"]
        ):
            old_level = int(old_lane["level"])
            new_level = min(old_level + 1, len(ADAPTIVE_LEVELS) - 1)
            if new_level > old_level:
                level = ADAPTIVE_LEVELS[new_level]
                new_lane = dict(old_lane)
                new_lane.update({
                    "revision": int(old_lane["revision"]) + 1,
                    "level": new_level,
                    "min_quality_score": float(level["min_quality_score"]),
                    "cooldown_multiplier": float(level["cooldown_multiplier"]),
                    "last_action": "TIGHTEN_ONE_LEVEL",
                    "last_reason": (
                        f"block_expectancy_{lane_metrics['expectancy_r']:+.3f}R_"
                        f"tp3_{lane_metrics['profit']}_vs_rest_"
                        f"{lane_metrics['sl'] + lane_metrics['expired']}"
                    ),
                    "applied_after_paper_count": block_end,
                    "changed_at": changed_at,
                })
                new_lane["policy_hash"] = _lane_policy_hash(strategy, new_lane)
                policy_after["lanes"][strategy] = new_lane
                action = "TIGHTEN_ONE_LEVEL"
                reason = str(new_lane["last_reason"])
                changes.append({
                    "strategy": strategy,
                    "action": action,
                    "old_revision": int(old_lane["revision"]),
                    "new_revision": int(new_lane["revision"]),
                    "old_level": old_level,
                    "new_level": new_level,
                    "old_min_quality_score": float(old_lane["min_quality_score"]),
                    "new_min_quality_score": float(new_lane["min_quality_score"]),
                    "old_cooldown_multiplier": float(old_lane["cooldown_multiplier"]),
                    "new_cooldown_multiplier": float(new_lane["cooldown_multiplier"]),
                    "reason": reason,
                })
            else:
                action = "HOLD_MAX_SELECTIVITY"
                reason = "maximum_pre_registered_selectivity_reached"

        lane_reports[strategy] = {
            "action": action,
            "reason": reason,
            "independent_metrics": lane_metrics,
            "policy_revisions_in_block": revisions,
            "policy_before": old_lane,
            "policy_after": dict(policy_after["lanes"][strategy]),
        }

    if changes:
        policy_after["global_revision"] = int(policy_before["global_revision"]) + 1
    policy_after["last_completed_block_end"] = block_end
    policy_after["updated_at"] = changed_at
    policy_after = normalize_adaptive_policy(policy_after)
    event = {
        "at": changed_at,
        "block_start": block_start + 1,
        "block_end": block_end,
        "visible_paper_in_block": len(block),
        "status": "POLICY_TIGHTENED" if changes else "POLICY_HELD",
        "policy_changed": bool(changes),
        "changes": changes,
        "lanes": lane_reports,
        "policy_before_global_revision": int(policy_before["global_revision"]),
        "policy_after_global_revision": int(policy_after["global_revision"]),
        "automatic_loosening": False,
        "real_money_enabled": False,
        "idempotent": False,
    }
    meta_set("adaptive_policy", policy_after)
    meta_set("adaptive_history", (history + [event])[-200:])
    return event

def review_gate(strategy: Optional[str]=None)->Dict[str,Any]:
    if strategy in STRATEGIES:
        rows=rows_for_current_policy(str(strategy))
        lane_policy=adaptive_lane_policy(str(strategy))
    else:
        rows=[]
        lane_policy={"revision":-1,"policy_hash":"per_strategy_required"}
    current=metrics(rows)
    valid_rows=[r for r in rows if str(r.get("outcome",""))!="data_gap"]
    recent=metrics(valid_rows[-REVIEW_RECENT_WINDOW:])
    stability_size=REVIEW_STABILITY_BLOCKS*REVIEW_RECENT_WINDOW
    stability_tail=valid_rows[-stability_size:]
    stability_blocks=(
        [
            metrics(stability_tail[i:i+REVIEW_RECENT_WINDOW])
            for i in range(0,stability_size,REVIEW_RECENT_WINDOW)
        ]
        if len(stability_tail)>=stability_size else []
    )
    stable_blocks_pass=bool(stability_blocks) and all(
        block["profit"]>block["sl"]+block["expired"]
        and block["expectancy_r"]>0.0
        for block in stability_blocks
    )
    lower,upper=wilson_interval(current["profit"],current["n"])
    side_majority=all(
        int(current["side_profit_counts"].get(side,0))
        > int(current["side_counts"].get(side,0))-int(current["side_profit_counts"].get(side,0))
        for side in ("LONG","SHORT")
    )
    side_expectancy_positive=all(
        float(current["side_expectancy_r"].get(side,0.0))>0.0
        for side in ("LONG","SHORT")
    )
    checks={
        "protocol_hash_exact":all(str(r.get("protocol_hash",""))==PROTOCOL_HASH for r in rows),
        "single_current_policy_revision":bool(rows) and all(
            int(r.get("policy_revision",-1))==int(lane_policy["revision"])
            and str(r.get("policy_hash",""))==str(lane_policy["policy_hash"])
            for r in rows
        ),
        "enough_independent_paper":current["n"]>=REVIEW_MIN_INDEPENDENT_PAPER,
        "enough_unique_symbols":current["unique_symbols"]>=REVIEW_MIN_UNIQUE_SYMBOLS,
        "enough_unique_clusters":current["unique_clusters"]>=REVIEW_MIN_UNIQUE_CLUSTERS,
        "enough_active_days":current["active_days"]>=REVIEW_MIN_ACTIVE_DAYS,
        "enough_market_regimes":current["market_regime_count"]>=REVIEW_MIN_MARKET_REGIMES,
        "enough_per_side":min(current["side_counts"].values() or [0])>=REVIEW_MIN_PER_SIDE,
        "each_side_tp3_majority":side_majority,
        "each_side_expectancy_positive":side_expectancy_positive,
        "tp3_gt_sl_plus_expired":current["profit"]>(current["sl"]+current["expired"]),
        "tp3_rate":current["tp3_rate"]>=REVIEW_MIN_TP3_RATE,
        "tp3_wilson_lower_above_50":lower>0.50,
        "expectancy":current["expectancy_r"]>=REVIEW_MIN_EXPECTANCY_R,
        "expectancy_lcb_positive":current["expectancy_lcb90_r"]>0,
        "cluster_expectancy_lcb_positive":current["cluster_expectancy_lcb90_r"]>0,
        "profit_factor":current["profit_factor"]>=REVIEW_MIN_PROFIT_FACTOR,
        "drawdown":current["max_drawdown_r"]<=REVIEW_MAX_DRAWDOWN_R,
        "recent_complete":recent["n"]>=REVIEW_RECENT_WINDOW,
        "recent_tp3_gt_rest":recent["profit"]>(recent["sl"]+recent["expired"]),
        "recent_expectancy_positive":recent["expectancy_r"]>0,
        "three_nonoverlapping_blocks_positive":stable_blocks_pass,
        "path_sampling_ok":current["max_sample_gap_seconds"]<=8,
        "data_gap_rate_ok":current["data_gap_rate"]<=REVIEW_MAX_DATA_GAP_RATE,
    }
    passed=bool(checks) and all(checks.values())
    return {
        "research_pass":passed,
        "micro_live_candidate":passed,
        "real_money_enabled":False,
        "checks":checks,"metrics":current,"recent":recent,
        "stability_blocks":stability_blocks,
        "wilson95":{"lower":lower,"upper":upper},
        "strategy":strategy or "ALL",
        "policy_revision":int(lane_policy["revision"]),
        "policy_hash":str(lane_policy["policy_hash"]),
        "note":"Passing only authorizes a separate reviewed micro-LIVE build. This program never places orders."
    }

def fixed_horizon_review(strategy: str)->Dict[str,Any]:
    """Compare pre-registered holding horizons without rewriting the 6m label."""
    lane=adaptive_lane_policy(strategy)
    by_horizon={
        horizon:metrics([
            row for row in horizon_rows(horizon,"paper",True,strategy)
            if int(row.get("policy_revision",-1))==int(lane["revision"])
            and str(row.get("policy_hash",""))==str(lane["policy_hash"])
        ])
        for horizon in HORIZONS_SECONDS
    }
    eligible=[
        horizon for horizon,item in by_horizon.items()
        if item["n"]>=REVIEW_RECENT_WINDOW
        and item["data_gap_rate"]<=max(REVIEW_MAX_DATA_GAP_RATE,0.05)
    ]
    best=(
        max(
            eligible,
            key=lambda horizon:(
                by_horizon[horizon]["cluster_expectancy_lcb90_r"],
                by_horizon[horizon]["expectancy_r"],
            ),
        )
        if eligible else None
    )
    return {
        "by_horizon":by_horizon,
        "best_research_horizon_seconds":best,
        "primary_horizon_unchanged":PRIMARY_HORIZON_SECONDS,
        "rules_changed":False,
    }

def champion_challenger_review(persist: bool=False)->Dict[str,Any]:
    """Compare lanes under their current prospective policy revisions.

    Core signal/TP/SL rules stay immutable.  A separate bounded adaptation
    function may only tighten selectivity between non-overlapping blocks.
    """
    lanes: Dict[str,Any]={}
    for strategy in STRATEGIES:
        history_rows=primary_rows("paper",True,strategy)
        rows=rows_for_current_policy(strategy)
        lanes[strategy]={
            "all":metrics(rows),
            "all_history":metrics(history_rows),
            "last_25":metrics(rows[-AUDIT_BLOCK_SIZE:]),
            "last_50":metrics(rows[-REVIEW_RECENT_WINDOW:]),
            "gate":review_gate(strategy),
            "horizons":fixed_horizon_review(strategy),
            "policy":adaptive_lane_policy(strategy),
        }
    enough_for_rank=all(lanes[s]["all"]["n"]>=AUDIT_BLOCK_SIZE for s in STRATEGIES)
    enough_for_compare=all(lanes[s]["all"]["n"]>=MIN_LANE_COMPARISON_SAMPLE for s in STRATEGIES)
    ranked=sorted(
        STRATEGIES,
        key=lambda s:(lanes[s]["all"]["expectancy_lcb90_r"],lanes[s]["all"]["expectancy_r"]),
        reverse=True,
    )
    leader=ranked[0] if enough_for_rank else "INSUFFICIENT_DATA"
    runner=ranked[1] if enough_for_rank else "INSUFFICIENT_DATA"
    advantage=(
        lanes[leader]["all"]["expectancy_lcb90_r"]-lanes[runner]["all"]["expectancy_lcb90_r"]
        if enough_for_rank else 0.0
    )
    recommended="NONE"
    status="COLLECTING_FIRST_25_PER_LANE"
    if enough_for_rank:status="AUDITING_FIXED_RULES"
    if enough_for_compare:
        leader_pass=bool(lanes[leader]["gate"]["research_pass"])
        if leader_pass and advantage>=MIN_LCB_ADVANTAGE_R:
            recommended=leader
            status="RESEARCH_WINNER_REQUIRES_EXTERNAL_MICRO_LIVE_REVIEW"
        else:
            status="NO_PROVEN_EDGE_CONTINUE_PAPER"
    review={
        "at":now_ts(),"audit_block":AUDIT_BLOCK_SIZE,
        "leader":leader,"runner_up":runner,"lcb_advantage_r":advantage,
        "recommended_for_external_review":recommended,
        "status":status,"lanes":lanes,
        "base_rules_changed":False,"real_money_enabled":False,
        "bounded_policy":adaptive_policy_state(),
        "explanation":"Every 25 visible closes may tighten the next block by one pre-registered level; automatic loosening and same-block relabelling are forbidden.",
    }
    if persist:
        meta_set("research_leader",leader)
        meta_set("last_adaptive_review",review)
    return review

def metric_line(m: Dict[str,Any])->str:
    pf=float(m.get("profit_factor",0) or 0)
    return (
        f"{int(m.get('profit',0))} TP3+ / {int(m.get('sl',0))} SL / "
        f"{int(m.get('expired',0))} expired / {int(m.get('data_gap',0))} data-gap · "
        f"TP3 {float(m.get('tp3_rate',0))*100:.1f}% · "
        f"{float(m.get('expectancy_r',0)):+.3f}R · PF {'∞' if pf>=900 else f'{pf:.2f}'} · "
        f"cluster-LCB90 {float(m.get('cluster_expectancy_lcb90_r',-999)):+.3f}R · "
        f"DD {float(m.get('max_drawdown_r',0)):.2f}R"
    )


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

def format_price(value: Any)->str:
    try:n=float(value)
    except Exception:return "?"
    if n>=1000:return f"{n:.2f}"
    if n>=1:return f"{n:.5f}"
    if n>=.01:return f"{n:.7f}"
    return f"{n:.10f}".rstrip("0")

def price_target(entry: float,side: str,move: float)->float:
    return entry*(1+move) if side=="LONG" else entry*(1-move)

def stop_price(entry: float,side: str,move: float)->float:
    return entry*(1-move) if side=="LONG" else entry*(1+move)

def entry_message(e: Dict[str,Any])->str:
    f=json.loads(e["features_json"]); entry=float(e["entry_price"]); side=e["side"]
    paper=e["tier"]=="paper"
    icon="📋" if paper else "🔎"
    title="PAPER-КАНДИДАТ" if paper else "OBSERVER — НЕ ПРОШЁЛ PAPER"
    reason="" if paper else f"\nПричина observer: {e['paper_reject_reason'] or 'broad_only'}"
    strategy=str(e.get("strategy") or "?")
    setup=(
        f"ADX14 {float(f.get('adx14',0)):.1f} · retrace {float(f.get('pullback_retrace',0))*100:.1f}% · "
        f"reclaim {'ДА' if f.get('reclaim_trigger') else 'НЕТ'}"
        if strategy==STRATEGY_PULLBACK else
        f"acceleration 3m/15m {float(f.get('d3_share_of_d15',0)):.2f} · fresh continuation"
    )
    trade_no=paper_trade_number(int(e["id"])) if paper else 0
    number_text=f" · СДЕЛКА #{trade_no}" if paper else ""
    return (
        f"{icon} V18.4 {title}{number_text}\n"
        f"{side} {e['symbol']} · score {float(e['quality_score']):.1f}\n"
        f"Стратегия: {strategy}\n"
        f"Независимый: {'ДА' if int(e['independent']) else 'НЕТ'} · protocol {PROTOCOL_HASH}"
        f"{reason}\n\n"
        f"Исполнимый вход: {format_price(entry)}\n"
        f"TP1 {format_price(price_target(entry,side,TP1_MOVE))} · "
        f"TP2 {format_price(price_target(entry,side,TP2_MOVE))}\n"
        f"TP3 {format_price(price_target(entry,side,TP3_MOVE))} ← PROFIT начинается здесь\n"
        f"TP4 {format_price(price_target(entry,side,TP4_MOVE))} · "
        f"TP5 {format_price(price_target(entry,side,TP5_MOVE))}\n"
        f"SL {format_price(stop_price(entry,side,float(e['stop_move'])))} "
        f"({float(e['stop_move'])*100:.2f}%)\n\n"
        f"Net TP3/SL payoff after cost model: "
        f"{float(f.get('net_tp3_to_sl_payoff_ratio',net_payoff_ratio(float(e['stop_move'])))):.2f}\n"
        f"1m {float(f['directional_1m'])*100:.2f}% · "
        f"3m {float(f['directional_3m'])*100:.2f}% · "
        f"15m {float(f['directional_15m'])*100:.2f}% · "
        f"Vol1 x{float(f['vol1']):.2f} · Range1 x{float(f['range1']):.2f}\n"
        f"{setup}\n"
        f"Spread {float(e['spread_bps']):.1f} bps · depth ≈ {float(e['depth_usdt']):.0f} USDT\n"
        f"Policy rev {int(e.get('policy_revision',0) or 0)} · {str(e.get('policy_hash',''))}\n"
        "Основной PAPER-результат — 6 минут. Эта сделка не скрывается и войдёт в файл после блока 25."
    )

def result_message(e: Dict[str,Any])->str:
    labels={"profit":"✅ TP3+","sl":"❌ STOP LOSS","expired":"⏱ EXPIRED 6M","data_gap":"⚠️ DATA GAP"}
    trade_no=paper_trade_number(int(e["id"]))
    return (
        f"📊 V18.4 СДЕЛКА #{trade_no} · {e['tier'].upper()} РЕЗУЛЬТАТ: {labels.get(str(e['primary_outcome']),'?')}\n"
        f"{e['side']} {e['symbol']}\nСтратегия: {e.get('strategy','?')}\n"
        f"Итог после fee+slippage model: {float(e['primary_pnl_r'] or 0):+.3f}R\n"
        f"MFE {float(e['max_favorable_move'])*100:.2f}% · MAE {float(e['max_adverse_move'])*100:.2f}%\n"
        f"Path samples {int(e.get('path_samples',0) or 0)} · max gap {int(e.get('max_sample_gap_seconds',0) or 0)}s\n"
        "TP1/TP2 не считаются положительным исходом."
    )

def final_message(e: Dict[str,Any])->str:
    with DB_LOCK,db_connect() as conn:
        rows=conn.execute("SELECT * FROM horizon_results WHERE episode_id=? ORDER BY horizon_seconds",(e["id"],)).fetchall()
    bits=[]
    for r in rows:
        bits.append(f"• {int(r['horizon_seconds'])//60}м: {r['outcome']} · {float(r['pnl_r']):+.2f}R · late {int(r['lateness_seconds'])}s")
    trade_no=paper_trade_number(int(e["id"]))
    return (
        f"🔬 V18.4 СДЕЛКА #{trade_no} · ПОЛНЫЙ ПУТЬ {e['tier'].upper()}\n{e['side']} {e['symbol']}\nСтратегия: {e.get('strategy','?')}\n"
        +("\n".join(bits) if bits else "нет снимков")
        +"\n6-минутный основной исход не переписывается."
    )

def notify_visible_episode(episode_id: int, stage: str) -> bool:
    """Send or durably queue one visible PAPER notification exactly once."""
    stage_map = {
        "entry": ("entry_notified", entry_message),
        "primary": ("primary_notified", result_message),
        "final": ("final_notified", final_message),
    }
    if stage not in stage_map:
        raise ValueError("unsupported notification stage")
    flag, builder = stage_map[stage]
    with NOTIFY_LOCK:
        episode = get_episode(int(episode_id))
        if not episode or episode.get("tier") != "paper":
            return False
        if int(episode.get(flag, 0) or 0):
            return True
        if stage == "primary" and not episode.get("primary_outcome"):
            return False
        if stage == "final" and episode.get("status") != "closed":
            return False
        if send_text(builder(episode), critical=True):
            update_episode(int(episode_id), {flag: 1})
            return True
        return False

def reconcile_visible_notifications(limit: int = 50) -> Dict[str, int]:
    """Recover notifications missed by a restart or a short network outage."""
    with DB_LOCK, db_connect() as conn:
        rows = conn.execute(
            "SELECT id,entry_notified,primary_outcome,primary_notified,status,final_notified "
            "FROM episodes WHERE cohort_id=? AND tier='paper' AND ("
            "entry_notified=0 OR (primary_outcome IS NOT NULL AND primary_notified=0) OR "
            "(status='closed' AND final_notified=0)) ORDER BY id LIMIT ?",
            (COHORT_ID, max(1, int(limit))),
        ).fetchall()
    sent = failed = 0
    for row in rows:
        stages: List[str] = []
        if not int(row["entry_notified"] or 0):
            stages.append("entry")
        if row["primary_outcome"] is not None and not int(row["primary_notified"] or 0):
            stages.append("primary")
        if row["status"] == "closed" and not int(row["final_notified"] or 0):
            stages.append("final")
        for stage in stages:
            if notify_visible_episode(int(row["id"]), stage):
                sent += 1
            else:
                failed += 1
                return {"sent_or_queued": sent, "failed": failed, "episodes": len(rows)}
    return {"sent_or_queued": sent, "failed": failed, "episodes": len(rows)}


# ---------------------------------------------------------------------------
# Scan and execution path
# ---------------------------------------------------------------------------

def run_scan()->Dict[str,Any]:
    if not SCAN_LOCK.acquire(blocking=False):return {"skipped":"scan_locked"}
    started=time.time()
    try:
        universe=liquid_universe(); btc=btc_context(); candidates=[]; errors=0
        policy=adaptive_policy_state()
        def analyze(ticker):
            try:
                c=get_klines(ticker["symbol"],90,35)
                if not c:return [],None
                broad=[
                    build_broad_candidate(ticker,c,btc),
                    build_pullback_candidate(ticker,c,btc),
                ]
                broad=[x for x in broad if x]
                if not broad:return [],None
                book=get_book(ticker["symbol"],True)
                return [apply_execution_and_paper_gate(x,book,policy) for x in broad],None
            except Exception as exc:return [],repr(exc)
        if universe:
            with ThreadPoolExecutor(max_workers=min(SCAN_WORKERS,len(universe))) as pool:
                fs={pool.submit(analyze,t):t for t in universe}
                for f in as_completed(fs):
                    found,err=f.result()
                    if err:errors+=1
                    candidates.extend(found)
        candidates.sort(key=lambda x:float(x["quality_score"]),reverse=True)

        paper=observer=correlated=skipped=0
        rejects=Counter(); slots=max(0,MAX_OPEN_EPISODES-open_episode_count())
        for c in candidates:
            if slots<=0:break
            symbol,side,strategy=c["symbol"],c["side"],c["strategy"]
            tier="paper" if c["paper_gate_pass"] else "observer"

            adaptive_cooldown=int(c.get("adaptive_cooldown_seconds",PAPER_SYMBOL_COOLDOWN_SECONDS))
            if tier=="paper" and recent_episode_exists(symbol,side,strategy,"paper",adaptive_cooldown):
                # Keep the duplicate for diagnostics, but never call it a new PAPER trade.
                tier="observer"
                c["paper_reject_reason"]=(c["paper_reject_reason"]+",paper_cooldown").strip(",")
            if tier=="observer" and recent_episode_exists(symbol,side,strategy,"observer",OBSERVER_SYMBOL_COOLDOWN_SECONDS):
                skipped+=1; continue

            c["tier"]=tier
            c["independent"]=independent_slot_available(int(c["cluster_id"]),side,strategy) if tier=="paper" else False
            for reason in str(c.get("paper_reject_reason","")).split(","):
                if reason:rejects[reason]+=1
            e=insert_episode(c)
            if not e:continue
            slots-=1
            if tier=="paper":
                paper+=1
                if not int(e["independent"]):correlated+=1
            else: observer+=1

            # Every actual PAPER trade is visible. OBSERVER is a rejected
            # diagnostic candidate, never a hidden/SHADOW trade.
            if tier == "paper":
                notify_visible_episode(int(e["id"]), "entry")

        result={
            "at":now_ts(),"universe":len(universe),"btc":btc,
            "adaptive_policy":policy,
            "broad_candidates":len(candidates),"observer_created":observer,
            "paper_created":paper,"correlated_paper":correlated,
            "cooldown_skipped":skipped,"open":open_episode_count(),
            "errors":errors,"reject_reasons":dict(rejects.most_common(12)),
            "top":[{"symbol":x["symbol"],"side":x["side"],"strategy":x["strategy"],"score":x["quality_score"],
                    "paper":x["paper_gate_pass"],"reason":x["paper_reject_reason"]} for x in candidates[:10]],
            "elapsed":time.time()-started,
        }
        RUNTIME["scan_count"]+=1;RUNTIME["last_scan"]=result;RUNTIME["last_scan_at"]=result["at"]
        return result
    finally:
        SCAN_LOCK.release()

def directional_move(e: Dict[str,Any],exit_price: float)->float:
    entry=float(e["entry_price"])
    return (exit_price-entry)/max(entry,1e-12) if e["side"]=="LONG" else (entry-exit_price)/max(entry,1e-12)

def first_passage_outcome(e: Dict[str,Any],cutoff: int)->str:
    tp3=e.get("tp3_hit_at"); sl=e.get("sl_hit_at")
    t_ok=tp3 is not None and int(tp3)<=cutoff
    s_ok=sl is not None and int(sl)<=cutoff
    # With sampled BBO data the order is unknowable when both barriers first
    # appear in the same sample.  Labelling that event as a winner or a normal
    # expiry would manufacture evidence.  Keep it out of promotion statistics.
    if t_ok and s_ok and int(tp3)==int(sl):return "data_gap"
    if t_ok and (not s_ok or int(tp3)<int(sl)):return "profit"
    if s_ok and (not t_ok or int(sl)<int(tp3)):return "sl"
    return "expired"

def pnl_r(e: Dict[str,Any],outcome: str,move: float)->float:
    risk=max(float(e["stop_move"]),1e-8)
    if outcome=="profit":return (TP3_MOVE-ROUND_TRIP_COST_MOVE)/risk
    if outcome=="sl":return -(risk+ROUND_TRIP_COST_MOVE)/risk
    if outcome=="data_gap":return 0.0
    return (move-ROUND_TRIP_COST_MOVE)/risk

def track_open_episodes(current_time: Optional[int]=None)->Dict[str,int]:
    if not TRACK_LOCK.acquire(blocking=False):return {"skipped":1}
    try:
        current=int(current_time or now_ts())
        episodes=get_open_episodes()
        books=get_books(sorted({e["symbol"] for e in episodes}))
        pclosed=fclosed=gaps=0

        for original in episodes:
            e=dict(original); book=books.get(e["symbol"],{"ok":False})
            if not book.get("ok"):
                gaps+=1
                update_episode(int(e["id"]),{"data_gap_count":int(e["data_gap_count"] or 0)+1})
                continue

            last_seen=int(e.get("last_seen_at") or e["created_at"])
            sample_gap=max(0,current-last_seen)
            exit_price=float(book["bid"] if e["side"]=="LONG" else book["ask"])
            move=directional_move(e,exit_price)
            mfe=max(float(e.get("max_favorable_move",0) or 0),move)
            mae=max(float(e.get("max_adverse_move",0) or 0),-move)

            updates={
                "max_favorable_move":mfe,"max_adverse_move":mae,
                "last_exit_price":exit_price,"last_seen_at":current,
                "path_samples":int(e.get("path_samples",0) or 0)+1,
                "max_sample_gap_seconds":max(int(e.get("max_sample_gap_seconds",0) or 0),sample_gap),
            }
            for i,target in enumerate(TARGET_MOVES,1):
                key=f"tp{i}_hit_at"
                if e.get(key) is None and move>=target:
                    updates[key]=current;e[key]=current
            if e.get("sl_hit_at") is None and move<=-float(e["stop_move"]):
                updates["sl_hit_at"]=current;e["sl_hit_at"]=current
            e.update(updates)

            age=current-int(e["created_at"])
            # Primary: first-passage TP3/SL or 6-minute executable mark.
            if e.get("primary_outcome") is None:
                early=first_passage_outcome(e,int(e["created_at"])+PRIMARY_HORIZON_SECONDS)
                if early in {"profit","sl","data_gap"} or age>=PRIMARY_HORIZON_SECONDS:
                    # If path had a large sampling hole before primary close, do not
                    # pretend the 6m outcome is trustworthy.
                    if early=="data_gap" or int(e.get("max_sample_gap_seconds",0) or 0)>8:
                        out="data_gap"; pr=0.0
                    else:
                        out=early if early in {"profit","sl"} else "expired"
                        pr=pnl_r(e,out,move)
                    updates.update({"primary_outcome":out,"primary_pnl_r":pr,"primary_closed_at":current})
                    e.update(updates);pclosed+=1

            update_episode(int(e["id"]),updates)

            # Immutable horizon snapshots. Late snapshots become DATA_GAP.
            for horizon in HORIZONS_SECONDS:
                if age<horizon or horizon_exists(int(e["id"]),horizon):continue
                lateness=age-horizon
                cutoff=int(e["created_at"])+horizon
                if lateness>MAX_HORIZON_LATENESS_SECONDS:
                    out="data_gap"
                else:
                    out=first_passage_outcome(e,cutoff)
                result={
                    "episode_id":int(e["id"]),"horizon_seconds":horizon,
                    "observed_at":current,"exit_price":exit_price,
                    "net_move":move-ROUND_TRIP_COST_MOVE,
                    "mfe_move":mfe,"mae_move":mae,
                    "tp1_hit":int(e.get("tp1_hit_at") is not None and int(e["tp1_hit_at"])<=cutoff),
                    "tp2_hit":int(e.get("tp2_hit_at") is not None and int(e["tp2_hit_at"])<=cutoff),
                    "tp3_hit":int(e.get("tp3_hit_at") is not None and int(e["tp3_hit_at"])<=cutoff),
                    "sl_hit":int(e.get("sl_hit_at") is not None and int(e["sl_hit_at"])<=cutoff),
                    "outcome":out,"pnl_r":pnl_r(e,out,move),
                    "lateness_seconds":lateness,
                }
                insert_horizon(result)

            refreshed=get_episode(int(e["id"])) or e
            # Telegram result notifications are only for real PAPER trades.
            if (
                refreshed.get("tier") == "paper"
                and refreshed.get("primary_outcome")
                and not int(refreshed.get("primary_notified", 0))
            ):
                notify_visible_episode(int(refreshed["id"]), "primary")

            if age>=FINAL_HORIZON_SECONDS:
                update_episode(int(e["id"]),{"status":"closed","finalized_at":current})
                fclosed+=1
                refreshed=get_episode(int(e["id"])) or e
                if (
                    refreshed.get("tier") == "paper"
                    and not int(refreshed.get("final_notified", 0))
                ):
                    notify_visible_episode(int(refreshed["id"]), "final")

        RUNTIME["last_track_at"]=current
        if pclosed:maybe_send_milestones()
        reconcile_visible_notifications()
        return {"open":len(episodes),"primary_closed":pclosed,"final_closed":fclosed,"gaps":gaps}
    finally:
        TRACK_LOCK.release()


# ---------------------------------------------------------------------------
# Reports/backups
# ---------------------------------------------------------------------------

def build_research_report(
    audit_event: Optional[Dict[str, Any]] = None,
    block_end: Optional[int] = None,
)->str:
    allm=metrics(primary_rows()); obs=metrics(primary_rows("observer"))
    pap=metrics(primary_rows("paper")); ind=metrics(primary_rows("paper",True))
    review=champion_challenger_review()
    lines=[
        "🧠 V18.4 — АУДИТ + БЕЗОПАСНАЯ АВТОАДАПТАЦИЯ",
        f"Protocol {PROTOCOL_HASH}",
        f"Закрыт блок №{max(1,int(block_end or primary_count('paper'))//AUDIT_BLOCK_SIZE)}: "
        f"{int(block_end or primary_count('paper'))} PAPER-исходов накопительно.",
        "Периодический анализ: каждые 25 PAPER; решения строятся только по независимой части и действуют со следующего блока.",
        f"Текущая политика: {adaptive_policy_summary()}",
        "",
        f"ВСЕ: {metric_line(allm)}",
        f"OBSERVER: {metric_line(obs)}",
        f"PAPER все: {metric_line(pap)}",
        f"PAPER независимые: {metric_line(ind)}",
        ""
    ]
    for strategy in STRATEGIES:
        lane=review["lanes"][strategy]
        lane_metrics=lane["all"]
        history_metrics=lane["all_history"]
        policy=lane["policy"]
        best_horizon=lane["horizons"].get("best_research_horizon_seconds")
        lines += [
            f"{strategy}:",
            f"• вся история: {metric_line(history_metrics)}",
            f"• текущая rev{int(policy['revision'])}: {metric_line(lane_metrics)}",
            f"• последние 25 текущей rev: {metric_line(lane['last_25'])}",
            f"• LONG/SHORT: {lane_metrics['side_counts']['LONG']}/"
            f"{lane_metrics['side_counts']['SHORT']} · дней {lane_metrics['active_days']} · "
            f"режимы {','.join(lane_metrics['market_regimes']) or 'нет'}",
            f"• лучший фиксированный research-horizon: "
            f"{int(best_horizon)//60}м" if best_horizon else
            "• лучший фиксированный research-horizon: ещё мало данных",
        ]
    if isinstance(audit_event, dict):
        lines += ["", f"Автоизменение: {audit_event.get('status','?')}"]
        for strategy in STRATEGIES:
            decision=(audit_event.get("lanes") or {}).get(strategy,{})
            if decision:
                lines.append(
                    f"• {strategy}: {decision.get('action','HOLD')} · "
                    f"{decision.get('reason','')}"
                )
        if audit_event.get("changes"):
            for change in audit_event["changes"]:
                lines.append(
                    f"→ {change['strategy']}: level {change['old_level']}→{change['new_level']}, "
                    f"score {change['old_min_quality_score']:.1f}→{change['new_min_quality_score']:.1f}, "
                    f"cooldown ×{change['old_cooldown_multiplier']:.1f}→×{change['new_cooldown_multiplier']:.1f}"
                )
        else:
            lines.append("→ Параметры не изменены: данных для безопасного ужесточения нет или блок не отрицательный.")
    leader=review["leader"]
    lane_gate=(review["lanes"][leader]["gate"] if leader in STRATEGIES else review_gate())
    failed=[k for k,v in lane_gate["checks"].items() if not v]
    ready=review["recommended_for_external_review"]!="NONE"
    lines += [
        "",
        f"Исследовательский лидер: {review['leader']}",
        f"Статус: {review['status']}",
        f"Рекомендован для отдельной micro-LIVE проверки: {review['recommended_for_external_review']}",
        f"Готовность к отдельному micro-LIVE: {ready}",
        "Не пройдены: "+(", ".join(failed) if failed else "нет"),
        "Важно: V18.4 меняет только будущую PAPER-селективность, не переписывает исходы/TP/SL и не может отправлять реальные ордера."
    ]
    return "\n".join(lines)

def diagnostics_text()->str:
    rt=runtime_snapshot();scan=rt.get("last_scan") or {}
    review=champion_challenger_review()
    return (
        "🧪 V18.4 Visible + Bounded Adaptive\n"
        f"Protocol {PROTOCOL_HASH} · scans {rt.get('scan_count',0)}\n"
        f"Policy: {adaptive_policy_summary()}\n"
        f"Universe {scan.get('universe',0)} · broad {scan.get('broad_candidates',0)} · "
        f"PAPER new {scan.get('paper_created',0)} · OBSERVER new {scan.get('observer_created',0)} · "
        f"open {scan.get('open',open_episode_count())} · {scan.get('elapsed',0):.0f}s\n"
        f"OBSERVER: {metric_line(metrics(primary_rows('observer')))}\n"
        f"PAPER: {metric_line(metrics(primary_rows('paper')))}\n"
        f"PAPER independent: {metric_line(metrics(primary_rows('paper',True)))}\n"
        f"CHAMPION: {metric_line(metrics(primary_rows('paper',True,STRATEGY_MOMENTUM)))}\n"
        f"CHALLENGER: {metric_line(metrics(primary_rows('paper',True,STRATEGY_PULLBACK)))}\n"
        f"Research leader: {review['leader']} · {review['status']}\n"
        f"Rejects: {scan.get('reject_reasons',{}) or 'нет'}\n"
        f"API {rt.get('api_calls',0)}/{rt.get('api_errors',0)} · "
        f"Telegram {rt.get('telegram_sent',0)}/{rt.get('telegram_errors',0)} · outbox {table_count('outbox')}\n"
        f"Last error: {rt.get('last_error','')}"
    )

def maybe_send_milestones()->Dict[str,Any]:
    total=primary_count();paper=primary_count("paper");independent=primary_count("paper",True)
    last_audit=int(meta_get("last_audit_paper",0) or 0)
    last_backup=int(meta_get("last_backup_paper",0) or 0)
    rs=bs=False
    audit_events: List[Dict[str,Any]]=[]
    audit_files: List[str]=[]
    # Trigger is what the user can count in Telegram: 25 closed PAPER trades.
    # Promotion metrics still use only the independent subset.
    next_block=last_audit+AUDIT_BLOCK_SIZE
    while paper>=next_block:
        event=apply_bounded_adaptation(next_block)
        if event.get("status")=="INCOMPLETE_BLOCK":
            set_runtime_error(f"milestone incomplete block {next_block}")
            break
        champion_challenger_review(persist=True)
        rs=send_text(build_research_report(event,next_block),True)
        fn=f"adaptive_seed_v18_4_after_{next_block}_closed_paper_{now_ts()}.json"
        data=export_bytes(
            {"last_audit_paper":next_block,"last_backup_paper":next_block},
            event,
        )
        bs=send_document(
            data,fn,
            f"V18.4 · {next_block} закрытых PAPER накопительно · полный журнал + изменения следующего блока · {PROTOCOL_HASH}",
            True,
        )
        if not (rs and bs):
            break
        meta_set("last_audit_paper",next_block)
        meta_set("last_backup_paper",next_block)
        last_audit=last_backup=next_block
        audit_events.append(event)
        audit_files.append(fn)
        next_block+=AUDIT_BLOCK_SIZE

    if not audit_events and paper>=last_backup+BACKUP_EVERY_PRIMARY:
        data=export_bytes();fn=f"v18_4_checkpoint_{paper}_paper_{now_ts()}.json"
        bs=send_document(
            data,fn,
            f"V18.4 checkpoint · {paper} closed PAPER / {independent} independent · {PROTOCOL_HASH}",
            True,
        )
        if bs:meta_set("last_backup_paper",paper)
    return {
        "total":total,"paper":paper,"independent":independent,
        "report_sent":rs,"backup_sent":bs,
        "audit_events":audit_events,"audit_files":audit_files,
    }

def startup_message()->str:
    return (
        f"✅ {APP_NAME} активирован.\n"
        f"Deploy marker: {DEPLOY_MARKER}\nProtocol: {PROTOCOL_HASH}\n\n"
        "Режим: RESEARCH + PAPER/OBSERVER ONLY. Ордеров BingX в коде нет.\n"
        "Каждая PAPER-сделка получает номер и видна в Telegram: вход, основной итог и полный путь. Скрытых SHADOW-сделок нет.\n"
        "OBSERVER — отклонённый диагностический кандидат, а не сделка; он не входит в торговую статистику.\n"
        f"CHAMPION: {STRATEGY_MOMENTUM} — неизменённый liquid momentum.\n"
        f"CHALLENGER: {STRATEGY_PULLBACK} — EMA trend + ADX + controlled pullback + closed-candle reclaim.\n"
        "Обе стратегии проходят одинаковую проверку spread/depth, fee/slippage и получают отдельную статистику.\n"
        f"Tracking executable BBO: каждые ~{TRACK_INTERVAL_SECONDS}s.\n"
        f"Costs in readiness: fee {ROUND_TRIP_FEE_MOVE*100:.2f}% + assumed slippage "
        f"{ASSUMED_SLIPPAGE_MOVE*100:.2f}% = {ROUND_TRIP_COST_MOVE*100:.2f}% round trip.\n"
        f"PAPER liquidity: turnover ≥ {PAPER_MIN_24H_QUOTE_VOLUME_USDT/1e6:.0f}M, "
        f"spread ≤ {PAPER_MAX_SPREAD_BPS:.0f}bps, depth ≥ {PAPER_MIN_DEPTH_USDT:.0f} USDT.\n"
        f"TP: 0.65/1.20/1.85/2.60/3.50%; only TP3+ = profit. SL {MIN_STOP_MOVE*100:.2f}%–{MAX_STOP_MOVE*100:.2f}%. "
        f"Net TP3/SL payoff must be ≥ {PAPER_MIN_NET_PAYOFF_RATIO:.2f}.\n"
        f"Micro-LIVE review only after ≥{REVIEW_MIN_INDEPENDENT_PAPER} independent PAPER, "
        f"TP3+ > SL+expired, TP3 ≥ {REVIEW_MIN_TP3_RATE*100:.0f}%, expectancy ≥ {REVIEW_MIN_EXPECTANCY_R:.2f}R, "
        f"PF ≥ {REVIEW_MIN_PROFIT_FACTOR:.2f}, Wilson95 lower > 50%, cluster-LCB90 > 0.\n"
        f"Regime guard: ≥{REVIEW_MIN_ACTIVE_DAYS} UTC days, ≥{REVIEW_MIN_MARKET_REGIMES} BTC regimes, "
        f"≥{REVIEW_MIN_PER_SIDE} independent outcomes per side, three separate "
        f"{REVIEW_RECENT_WINDOW}-trade blocks positive, data-gap ≤ {REVIEW_MAX_DATA_GAP_RATE*100:.1f}%.\n"
        f"Текущая авто-политика: {adaptive_policy_summary()}.\n"
        f"Безопасный checkpoint: JSON каждые {BACKUP_EVERY_PRIMARY} закрытых PAPER. После каждых "
        f"{AUDIT_BLOCK_SIZE} закрытых PAPER бот отправляет Telegram-файл со всеми PAPER-сделками, "
        "аудитом и точными изменениями для следующего блока.\n"
        "Отрицательный независимый блок может только на один уровень повысить score-floor и cooldown; автоматическое ослабление запрещено. "
        "25 сделок не включают реальные деньги; продвижение считает только текущую неизменную policy revision. "
        f"сравнение допускается после ≥{MIN_LANE_COMPARISON_SAMPLE} на каждую стратегию.\n"
        f"Storage {DB_PATH}. Seed: {runtime_snapshot().get('seed_restore',{})}"
    )


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

async def scan_loop():
    await asyncio.sleep(3)
    while True:
        try: await asyncio.to_thread(run_scan)
        except Exception as exc:
            set_runtime_error(f"scan_loop:{exc!r}")
            send_text(f"⚠️ V18.4 scan error: {exc!r}",True)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

async def track_loop():
    await asyncio.sleep(5)
    while True:
        try:
            await asyncio.to_thread(track_open_episodes)
            await asyncio.to_thread(flush_outbox)
        except Exception as exc:set_runtime_error(f"track_loop:{exc!r}")
        await asyncio.sleep(TRACK_INTERVAL_SECONDS)

async def diagnostic_loop():
    await asyncio.sleep(20)
    while True:
        try:
            send_text(diagnostics_text(),False)
            RUNTIME["last_diagnostic_at"]=now_ts()
        except Exception as exc:set_runtime_error(f"diagnostic_loop:{exc!r}")
        await asyncio.sleep(DIAGNOSTIC_INTERVAL_SECONDS)

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db();restore_seed_if_empty();send_text(startup_message(),True)
    reconcile_visible_notifications()
    maybe_send_milestones()
    tasks=[asyncio.create_task(scan_loop()),asyncio.create_task(track_loop()),asyncio.create_task(diagnostic_loop())]
    try:yield
    finally:
        for t in tasks:t.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)

app=FastAPI(title=APP_NAME,lifespan=lifespan)

def authorized(key: str)->bool:
    return bool(ADMIN_KEY) and secrets.compare_digest(str(key),ADMIN_KEY)

@app.get("/")
def root()->HTMLResponse:
    return HTMLResponse(f"<h3>{APP_NAME}</h3><p>{DEPLOY_MARKER}</p><p>Protocol {PROTOCOL_HASH}. Research/PAPER only.</p>")

@app.get("/health")
def health()->Dict[str,Any]:
    return {"ok":True,"app":APP_NAME,"deploy_marker":DEPLOY_MARKER,"protocol_hash":PROTOCOL_HASH,
            "episodes":table_count("episodes"),"primary_closed":primary_count(),
            "open":open_episode_count(),"last_error":runtime_snapshot().get("last_error","")}

@app.get("/status")
def status()->JSONResponse:
    return JSONResponse({"runtime":runtime_snapshot(),
                         "observer":metrics(primary_rows("observer")),
                         "paper":metrics(primary_rows("paper")),
                         "paper_independent":metrics(primary_rows("paper",True)),
                         "strategies":{
                             s:metrics(primary_rows("paper",True,s)) for s in STRATEGIES
                         },
                         "adaptive_policy":adaptive_policy_state(),
                         "adaptive_review":champion_challenger_review(),
                         "review_gates":{s:review_gate(s) for s in STRATEGIES}})

@app.get("/diagnostic")
def diagnostic()->HTMLResponse:
    return HTMLResponse("<pre>"+diagnostics_text()+"</pre>")

@app.post("/scan")
def manual_scan(key: str=Query(""))->JSONResponse:
    if not authorized(key):return JSONResponse({"ok":False,"error":"unauthorized"},status_code=403)
    return JSONResponse(run_scan())

@app.get("/export")
def export(key: str=Query(""))->Response:
    if not authorized(key):return JSONResponse({"ok":False,"error":"unauthorized"},status_code=403)
    return Response(export_bytes(),media_type="application/json",
                    headers={"Content-Disposition":f"attachment; filename=v18_4_research_{primary_count()}.json"})

@app.get("/trades")
def trades(key: str=Query(""))->JSONResponse:
    if not authorized(key):return JSONResponse({"ok":False,"error":"unauthorized"},status_code=403)
    ledger=paper_trade_ledger()
    return JSONResponse({"count":len(ledger),"paper_trades":ledger})

@app.post("/telegram-backup")
def telegram_backup(key: str=Query(""))->JSONResponse:
    if not authorized(key):return JSONResponse({"ok":False,"error":"unauthorized"},status_code=403)
    count=primary_count();fn=f"v18_4_manual_{count}_{now_ts()}.json"
    sent=send_document(export_bytes(),fn,"Manual V18.4 full trade ledger + adaptive state")
    return JSONResponse({"ok":sent,"filename":fn,"primary_count":count})

if __name__=="__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0",port=PORT,log_level="info")
