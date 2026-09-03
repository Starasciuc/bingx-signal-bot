# VERIFIED GITHUB DEPLOY FILE — V20.3.4 SETUP MASTER · PUMP vs PULLBACK INTELLIGENCE
# Render must start this exact root file with: uvicorn bot:app ...
import os
import time
import json
import random
import asyncio
import io
import secrets
import hashlib
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, Response



# ============================================================
# SAFE ADAPTIVE LEARNING LAYER
# ============================================================

"""
adaptive_learning.py

Safe adaptive layer for a futures signal bot.

What it does:
- Stores every closed signal and its feature snapshot in SQLite.
- Retrains after MIN_TRAIN_TRADES closed trades and then every RETRAIN_EVERY trades.
- Uses a simple logistic model implemented with the Python standard library.
- Uses chronological train, calibration, and independent newest-test blocks.
- Promotes a challenger only if it beats both the baseline and current champion.
- Learns a probability threshold that maximizes an expectancy-style objective.
- Sends a Telegram report for every scheduled training attempt, including failures.
- Audits each promoted model on later closed decisions and compares before/after.
- Automatically rolls back a promoted model only after a guarded live-data failure.
- Never changes leverage, stop-loss safety limits, API permissions, or source code.

This module does NOT guarantee profit and must be used in paper/shadow mode first.
"""

import json
import math
import os
import random
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DB_PATH = os.getenv("ADAPTIVE_DB_PATH", "adaptive_bot.sqlite3")
# Begin the first error-analysis cycle after 50 outcomes. The independent newest
# test still decides whether the challenger is strong enough to affect signals.
MIN_TRAIN_TRADES = max(75, int(os.getenv("ADAPTIVE_MIN_TRAIN_TRADES", "75")))
RETRAIN_EVERY = int(os.getenv("ADAPTIVE_RETRAIN_EVERY", "25"))
MAX_TRAIN_ROWS = int(os.getenv("ADAPTIVE_MAX_TRAIN_ROWS", "1500"))
VALIDATION_FRACTION = float(os.getenv("ADAPTIVE_VALIDATION_FRACTION", "0.20"))
MIN_VALIDATION_ROWS = int(os.getenv("ADAPTIVE_MIN_VALIDATION_ROWS", "10"))
MIN_CALIBRATION_ROWS = int(os.getenv("ADAPTIVE_MIN_CALIBRATION_ROWS", "10"))
MIN_POSITIVE_ROWS = int(os.getenv("ADAPTIVE_MIN_POSITIVE_ROWS", "5"))
MIN_NEGATIVE_ROWS = int(os.getenv("ADAPTIVE_MIN_NEGATIVE_ROWS", "5"))
MIN_MODEL_IMPROVEMENT = float(os.getenv("ADAPTIVE_MIN_MODEL_IMPROVEMENT", "0.010"))
MIN_EXPECTANCY_IMPROVEMENT_R = float(os.getenv("ADAPTIVE_MIN_EXPECTANCY_IMPROVEMENT_R", "0.05"))
MIN_SELECTED_TEST_ROWS = int(os.getenv("ADAPTIVE_MIN_SELECTED_TEST_ROWS", "5"))
MIN_SELECTED_LIVE_TEST_ROWS = int(os.getenv("ADAPTIVE_MIN_SELECTED_LIVE_TEST_ROWS", "5"))
MIN_SIGNAL_PROBABILITY = float(os.getenv("ADAPTIVE_MIN_SIGNAL_PROBABILITY", "0.60"))
MAX_SIGNAL_PROBABILITY = float(os.getenv("ADAPTIVE_MAX_SIGNAL_PROBABILITY", "0.82"))
MIN_VALIDATION_COVERAGE = float(os.getenv("ADAPTIVE_MIN_VALIDATION_COVERAGE", "0.25"))
# The user's explicit target: TP3+ must be a strict majority of all closed
# outcomes selected by a challenger (more than SL and expired combined).
ADAPTIVE_TARGET_SUCCESS_RATE = float(os.getenv("ADAPTIVE_TARGET_SUCCESS_RATE", "0.35"))
ADAPTIVE_CONFIRMATION_PASSES = int(os.getenv("ADAPTIVE_CONFIRMATION_PASSES", "2"))
ADAPTIVE_CONFIRMATION_SELECTED = int(os.getenv("ADAPTIVE_CONFIRMATION_SELECTED", "15"))
ADAPTIVE_INITIAL_LIVE_FRACTION = float(os.getenv("ADAPTIVE_INITIAL_LIVE_FRACTION", "0.50"))
ADAPTIVE_LIVE_FRACTION_STEP = float(os.getenv("ADAPTIVE_LIVE_FRACTION_STEP", "0.25"))
MODEL_ENABLED = True  # V20.3 hard-enabled self-learning; real-money remains separately locked
MODEL_DATA_POLICY = "v20_3_4_setup_master_forward_only_v1"
# Guarded live is the default: the model still cannot block anything until it has
# passed the independent holdout checks below. Set true to observe forever.
SHADOW_ONLY = False  # V20.3: validated champion may gate PAPER canary candidates
ROUND_TRIP_COST_MOVE = float(os.getenv("ROUND_TRIP_COST_MOVE", "0.0012"))

# Post-promotion audit. A "decision" is either a live signal accepted by the
# active model or a model-blocked candidate that is followed in shadow. Accepted
# candidates that were not sent only because all live slots were occupied are
# excluded, so they cannot distort the live comparison.
LIVE_AUDIT_ENABLED = os.getenv("ADAPTIVE_LIVE_AUDIT_ENABLED", "true").lower() == "true"
LIVE_AUDIT_EVERY = int(os.getenv("ADAPTIVE_LIVE_AUDIT_EVERY", "25"))
LIVE_AUDIT_BASELINE_WINDOW = int(os.getenv("ADAPTIVE_LIVE_AUDIT_BASELINE_WINDOW", "50"))
LIVE_AUDIT_MIN_LIVE = int(os.getenv("ADAPTIVE_LIVE_AUDIT_MIN_LIVE", "10"))
LIVE_AUDIT_MIN_BASELINE = int(os.getenv("ADAPTIVE_LIVE_AUDIT_MIN_BASELINE", "15"))
AUTO_ROLLBACK_ENABLED = os.getenv("ADAPTIVE_AUTO_ROLLBACK", "true").lower() == "true"
ROLLBACK_EXPECTANCY_DROP_R = float(os.getenv("ADAPTIVE_ROLLBACK_EXPECTANCY_DROP_R", "0.15"))
ROLLBACK_SUCCESS_DROP = float(os.getenv("ADAPTIVE_ROLLBACK_SUCCESS_DROP", "0.08"))
ROLLBACK_SEVERE_EXPECTANCY_R = float(os.getenv("ADAPTIVE_ROLLBACK_SEVERE_EXPECTANCY_R", "-0.20"))
ROLLBACK_MIN_BLOCKED = int(os.getenv("ADAPTIVE_ROLLBACK_MIN_BLOCKED", "10"))
ROLLBACK_HARMFUL_BLOCK_WIN_RATE = float(
    os.getenv("ADAPTIVE_ROLLBACK_HARMFUL_BLOCK_WIN_RATE", "0.45")
)

# Legacy evidence guard retained for audit compatibility. These defaults come from the user's first 50 closed
# outcomes: every candidate below Score 99 or with raw 1m volume ratio <= 0.80
# was non-profitable. The rule is still audited on NEW outcomes and can disable
# itself if shadow tracking shows that it is rejecting profitable TP3+ trades.
EVIDENCE_GUARD_ENABLED = os.getenv("EVIDENCE_GUARD_ENABLED", "false").lower() == "true"
EVIDENCE_GUARD_VERSION = 1
EVIDENCE_MIN_SCORE = float(os.getenv("EVIDENCE_MIN_SCORE", "99"))
EVIDENCE_MIN_VOL1 = float(os.getenv("EVIDENCE_MIN_VOL1", "0.80"))
EVIDENCE_AUDIT_ENABLED = os.getenv("EVIDENCE_AUDIT_ENABLED", "true").lower() == "true"
EVIDENCE_AUDIT_EVERY = int(os.getenv("EVIDENCE_AUDIT_EVERY", "25"))
EVIDENCE_AUDIT_BASELINE_WINDOW = int(os.getenv("EVIDENCE_AUDIT_BASELINE_WINDOW", "50"))
EVIDENCE_AUDIT_MIN_ACCEPTED = int(os.getenv("EVIDENCE_AUDIT_MIN_ACCEPTED", "8"))
EVIDENCE_AUDIT_MIN_BLOCKED = int(os.getenv("EVIDENCE_AUDIT_MIN_BLOCKED", "5"))
EVIDENCE_ROLLBACK_WINNERS = int(os.getenv("EVIDENCE_ROLLBACK_WINNERS", "2"))
EVIDENCE_ROLLBACK_EXPECTANCY_DROP_R = float(
    os.getenv("EVIDENCE_ROLLBACK_EXPECTANCY_DROP_R", "0.15")
)
EVIDENCE_ROLLBACK_SUCCESS_DROP = float(
    os.getenv("EVIDENCE_ROLLBACK_SUCCESS_DROP", "0.08")
)

ADAPTIVE_SEED_PATH = os.getenv("ADAPTIVE_SEED_PATH", "adaptive_seed.json")
ADAPTIVE_SEED_DISCOVERY_ENABLED = os.getenv(
    "ADAPTIVE_SEED_DISCOVERY_ENABLED", "false"
).lower() == "true"

# Never let learning touch these safety-critical settings.
LOCKED_SAFETY_KEYS = {
    "LEVERAGE",
    "MAX_SL_MOVE",
    "LOCAL_SCALP_MAX_SL_MOVE",
    "MAX_ACTIVE_SIGNALS",
    "MAX_SIGNALS_PER_SCAN",
}

FEATURE_NAMES: Tuple[str, ...] = (
    # V20.3 dedicated SPIKE REGIME feature schema.
    # Keep it compact enough for 50/75-trade chronological validation.
    "score",
    "is_long",
    "sl_price_move",
    "liquidity_rank",
    "book_spread_bps",
    "book_depth_log",
    "spike_impulse_move",
    "spike_atr_mult",
    "spike_volume_pace",
    "spike_body_acceleration",
    "spike_range_acceleration",
    "spike_base_distance",
    "spike_tf_agreement",
    "regime_is_continuation",
    "regime_is_exhaustion",
    "regime_cont_score",
    "regime_fade_score",
    "regime_failed_reclaim",
    "regime_cont_15m_aligned",
    "regime_cont_micro_momentum",
    "regime_micro_overextended",
    "regime_exhaust_micro_reversal",
    "regime_volume_faded",
    "regime_no_new_extreme_ok",
    "regime_spike_context_ok",
    "entry_vol1_raw",
    "entry_mom1",
    "entry_mom3",
    "entry_mom5",
    "entry_mom15",
    "intel_overheat",
    "intel_climax",
    "intel_squeeze",
    "intel_rejection_wick",
    "intel_distance_atr",
    "intel_volume_health",
    "intel_range_expansion",
    "intel_rsi_fast",
    "intel_setup_quality",
    "intel_regime_conflict",
    "intel_compression",
    "intel_structure",
    "intel_false_breakout",
    "intel_continuation_edge",
    "intel_pullback_risk",
)



@dataclass
class ModelState:
    version: int = 0
    active: bool = False
    threshold: float = MIN_SIGNAL_PROBABILITY
    trained_rows: int = 0
    validation_rows: int = 0
    base_logloss: float = 999.0
    model_logloss: float = 999.0
    validation_win_rate: float = 0.0
    validation_coverage: float = 0.0
    validation_selected_wr: float = 0.0
    validation_selected_count: int = 0
    weights: List[float] = None
    mean: List[float] = None
    std: List[float] = None
    last_trained_closed_count: int = 0
    last_attempted_closed_count: int = 0
    validation_expectancy_r: float = 0.0
    validation_baseline_expectancy_r: float = 0.0
    last_candidate_reason: str = "warmup"
    created_at: int = 0
    parent_version: int = 0
    activation_trade_id: int = 0
    activation_closed_count: int = 0
    last_live_audit_decision_count: int = 0
    last_live_audit_at: int = 0
    candidate_pass_streak: int = 0
    candidate_selected_total: int = 0
    deployment_fraction: float = 0.0
    data_policy: str = MODEL_DATA_POLICY

    def __post_init__(self) -> None:
        if self.weights is None:
            self.weights = [0.0] * (len(FEATURE_NAMES) + 1)
        if self.mean is None:
            self.mean = [0.0] * len(FEATURE_NAMES)
        if self.std is None:
            self.std = [1.0] * len(FEATURE_NAMES)


@dataclass
class EvidenceGuardState:
    version: int = EVIDENCE_GUARD_VERSION
    active: bool = EVIDENCE_GUARD_ENABLED
    min_score: float = EVIDENCE_MIN_SCORE
    min_vol1: float = EVIDENCE_MIN_VOL1
    activation_trade_id: int = 0
    activation_closed_count: int = 0
    last_audit_decision_count: int = 0
    last_audit_at: int = 0
    created_at: int = 0
    disabled_reason: str = ""


_LOCK = threading.RLock()


def _connect() -> sqlite3.Connection:
    path = Path(DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_adaptive_db() -> None:
    with _LOCK, _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS adaptive_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT UNIQUE,
                created_at INTEGER NOT NULL,
                closed_at INTEGER NOT NULL,
                source TEXT NOT NULL DEFAULT 'live',
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                strategy TEXT NOT NULL,
                grade TEXT NOT NULL,
                result TEXT NOT NULL,
                label INTEGER NOT NULL,
                pnl_r REAL NOT NULL DEFAULT 0,
                exit_price REAL,
                mfe_r REAL NOT NULL DEFAULT 0,
                mae_r REAL NOT NULL DEFAULT 0,
                duration_minutes REAL NOT NULL DEFAULT 0,
                features_json TEXT NOT NULL,
                model_version INTEGER NOT NULL DEFAULT 0,
                model_probability REAL,
                shadow_accepted INTEGER,
                evidence_guard_version INTEGER NOT NULL DEFAULT 0,
                evidence_guard_accepted INTEGER,
                evidence_guard_reason TEXT,
                decision_reason TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_adaptive_trades_closed
            ON adaptive_trades(closed_at);

            CREATE TABLE IF NOT EXISTS adaptive_model_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                state_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS adaptive_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                message TEXT NOT NULL,
                payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS adaptive_model_versions (
                version INTEGER PRIMARY KEY,
                state_json TEXT NOT NULL,
                status TEXT NOT NULL,
                saved_at INTEGER NOT NULL,
                note TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS adaptive_rule_guard_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                state_json TEXT NOT NULL
            );
            """
        )

        # Migrate an existing V15 database in place. SQLite does not support
        # ADD COLUMN IF NOT EXISTS on all Render Python images.
        existing_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(adaptive_trades)").fetchall()
        }
        migrations = {
            "source": "ALTER TABLE adaptive_trades ADD COLUMN source TEXT NOT NULL DEFAULT 'live'",
            "exit_price": "ALTER TABLE adaptive_trades ADD COLUMN exit_price REAL",
            "mfe_r": "ALTER TABLE adaptive_trades ADD COLUMN mfe_r REAL NOT NULL DEFAULT 0",
            "mae_r": "ALTER TABLE adaptive_trades ADD COLUMN mae_r REAL NOT NULL DEFAULT 0",
            "duration_minutes": "ALTER TABLE adaptive_trades ADD COLUMN duration_minutes REAL NOT NULL DEFAULT 0",
            "evidence_guard_version": "ALTER TABLE adaptive_trades ADD COLUMN evidence_guard_version INTEGER NOT NULL DEFAULT 0",
            "evidence_guard_accepted": "ALTER TABLE adaptive_trades ADD COLUMN evidence_guard_accepted INTEGER",
            "evidence_guard_reason": "ALTER TABLE adaptive_trades ADD COLUMN evidence_guard_reason TEXT",
            "decision_reason": "ALTER TABLE adaptive_trades ADD COLUMN decision_reason TEXT",
        }
        for column, statement in migrations.items():
            if column not in existing_columns:
                conn.execute(statement)

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_adaptive_trades_model_decision "
            "ON adaptive_trades(model_version, source, shadow_accepted, id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_adaptive_trades_evidence_decision "
            "ON adaptive_trades(evidence_guard_version, source, evidence_guard_accepted, id)"
        )

        row = conn.execute("SELECT state_json FROM adaptive_model_state WHERE id = 1").fetchone()
        if row is None:
            state = ModelState(created_at=int(time.time()))
            conn.execute(
                "INSERT INTO adaptive_model_state(id, state_json) VALUES(1, ?)",
                (json.dumps(asdict(state), ensure_ascii=False),),
            )
        else:
            try:
                raw = json.loads(row["state_json"])
                allowed = {field.name for field in fields(ModelState)}
                state = ModelState(**{key: value for key, value in raw.items() if key in allowed})
                if str(raw.get("data_policy", "legacy_all_sources")) != MODEL_DATA_POLICY:
                    eligible_count = int(
                        conn.execute(
                            "SELECT COUNT(*) AS n FROM adaptive_trades "
                            "WHERE COALESCE(decision_reason, '') IN (?,?,?)",
                            (
                                EXHAUSTION_PAPER_REASON,
                                SPIKE_ADAPTIVE_BLOCK_REASON,
                                SPIKE_STRATEGY_GUARD_REASON,
                            ),
                        ).fetchone()["n"]
                        or 0
                    )
                    state.active = False
                    state.trained_rows = eligible_count
                    state.last_trained_closed_count = 0
                    state.last_attempted_closed_count = 0
                    state.last_candidate_reason = "v17_clean_forward_dataset"
                    state.candidate_pass_streak = 0
                    state.candidate_selected_total = 0
                    state.deployment_fraction = 0.0
                    state.data_policy = MODEL_DATA_POLICY
                    state.weights = [0.0] * (len(FEATURE_NAMES) + 1)
                    state.mean = [0.0] * len(FEATURE_NAMES)
                    state.std = [1.0] * len(FEATURE_NAMES)
                    conn.execute(
                        "UPDATE adaptive_model_state SET state_json=? WHERE id=1",
                        (json.dumps(asdict(state), ensure_ascii=False),),
                    )
            except Exception:
                state = ModelState(created_at=int(time.time()))
                conn.execute(
                    "UPDATE adaptive_model_state SET state_json=? WHERE id=1",
                    (json.dumps(asdict(state), ensure_ascii=False),),
                )

        # When upgrading an already-active V16.1 model, begin its live audit
        # after the upgrade instead of incorrectly treating old rows as live
        # post-promotion evidence.
        if state.active and state.activation_trade_id <= 0:
            latest = conn.execute(
                "SELECT COALESCE(MAX(id), 0) AS max_id, COUNT(*) AS n FROM adaptive_trades"
            ).fetchone()
            state.activation_trade_id = int(latest["max_id"] or 0)
            state.activation_closed_count = int(latest["n"] or 0)
            state.last_live_audit_decision_count = 0
            conn.execute(
                "UPDATE adaptive_model_state SET state_json=? WHERE id=1",
                (json.dumps(asdict(state), ensure_ascii=False),),
            )

        initial_status = "active" if state.active else ("baseline" if state.version == 0 else "inactive")
        conn.execute(
            "INSERT OR IGNORE INTO adaptive_model_versions"
            "(version, state_json, status, saved_at, note) VALUES(?,?,?,?,?)",
            (
                int(state.version),
                json.dumps(asdict(state), ensure_ascii=False),
                initial_status,
                int(time.time()),
                "database_initialized",
            ),
        )

        guard_row = conn.execute(
            "SELECT state_json FROM adaptive_rule_guard_state WHERE id = 1"
        ).fetchone()
        latest = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS max_id, COUNT(*) AS n FROM adaptive_trades"
        ).fetchone()
        reset_guard = False
        if guard_row is None:
            guard = EvidenceGuardState(
                activation_trade_id=int(latest["max_id"] or 0),
                activation_closed_count=int(latest["n"] or 0),
                created_at=int(time.time()),
            )
            reset_guard = True
        else:
            try:
                raw_guard = json.loads(guard_row["state_json"])
                allowed_guard = {field.name for field in fields(EvidenceGuardState)}
                guard = EvidenceGuardState(
                    **{key: value for key, value in raw_guard.items() if key in allowed_guard}
                )
            except Exception:
                guard = EvidenceGuardState(
                    activation_trade_id=int(latest["max_id"] or 0),
                    activation_closed_count=int(latest["n"] or 0),
                    created_at=int(time.time()),
                )
                reset_guard = True

        # A threshold/version change is a new rule experiment and therefore
        # receives a fresh before/after activation marker.
        if (
            int(guard.version) != EVIDENCE_GUARD_VERSION
            or abs(float(guard.min_score) - EVIDENCE_MIN_SCORE) > 1e-9
            or abs(float(guard.min_vol1) - EVIDENCE_MIN_VOL1) > 1e-9
        ):
            guard = EvidenceGuardState(
                activation_trade_id=int(latest["max_id"] or 0),
                activation_closed_count=int(latest["n"] or 0),
                created_at=int(time.time()),
            )
            reset_guard = True

        if not EVIDENCE_GUARD_ENABLED and guard.active:
            guard.active = False
            guard.disabled_reason = "disabled_by_environment"
            reset_guard = True

        if reset_guard:
            conn.execute(
                "INSERT INTO adaptive_rule_guard_state(id, state_json) VALUES(1, ?) "
                "ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json",
                (json.dumps(asdict(guard), ensure_ascii=False),),
            )
        conn.commit()


def _clip(value: Any, low: float, high: float, default: float = 0.0) -> float:
    try:
        x = float(value)
        if not math.isfinite(x):
            return default
        return min(high, max(low, x))
    except Exception:
        return default


def build_feature_dict(trade: Dict[str, Any]) -> Dict[str, float]:
    side = str(trade.get("side", "")).upper()
    grade = str(trade.get("grade", "")).upper()
    mode = str(trade.get("setup_mode", "")).upper()
    strategy = str(trade.get("strategy", "")).upper()
    btc_text = str(trade.get("btc_text", "")).upper()
    direction = 1.0 if side == "LONG" else -1.0
    ch3m = _clip(trade.get("ch3m_1m"), -1, 1)
    ch15m = _clip(trade.get("ch15m"), -1, 1)
    ch30m = _clip(trade.get("ch30m"), -1, 1)
    btc_bull = 1.0 if "BTC BULL" in btc_text else 0.0
    btc_bear = 1.0 if "BTC BEAR" in btc_text else 0.0
    btc_alignment = 0.0
    if (side == "LONG" and btc_bull) or (side == "SHORT" and btc_bear):
        btc_alignment = 1.0
    elif (side == "LONG" and btc_bear) or (side == "SHORT" and btc_bull):
        btc_alignment = -1.0

    entry = _clip(trade.get("entry"), 1e-12, 1e18, 1.0)
    sl = _clip(trade.get("sl"), 0.0, 1e18, entry)
    sl_move = abs(entry - sl) / max(entry, 1e-12)

    features = {
        "score": _clip(trade.get("score"), 0, 100) / 100.0,
        "is_long": 1.0 if side == "LONG" else 0.0,
        "is_a_plus": 1.0 if grade == "A+" else 0.0,
        "ch3m_abs": _clip(abs(ch3m), 0, 0.25) / 0.25,
        "ch15m_abs": _clip(abs(ch15m), 0, 0.40) / 0.40,
        "ch30m_abs": _clip(abs(ch30m), 0, 0.60) / 0.60,
        "vol1": _clip(trade.get("vol1"), 0, 8) / 8.0,
        "vol5": _clip(trade.get("volume_ratio", trade.get("vol5", 1.0)), 0, 8) / 8.0,
        "range1": _clip(trade.get("range1"), 0, 8) / 8.0,
        "range5": _clip(trade.get("range_ratio", trade.get("range5", 1.0)), 0, 8) / 8.0,
        "rr_tp1": _clip(trade.get("rr"), 0, 5) / 5.0,
        "ladder_rr": _clip(trade.get("ladder_rr"), 0, 6) / 6.0,
        "final_rr": _clip(trade.get("final_rr"), 0, 8) / 8.0,
        "sl_price_move": _clip(sl_move, 0, 0.08) / 0.08,
        "btc_bull": btc_bull,
        "btc_bear": btc_bear,
        "is_reversal": 1.0 if "REVERSAL" in mode else 0.0,
        "is_dump": 1.0 if "DUMP" in mode or "DUMP" in strategy else 0.0,
        "is_instant": 1.0 if "INSTANT" in mode or "INSTANT" in strategy else 0.0,
        "is_aero": 1.0 if "AERO" in mode or "AERO" in strategy else 0.0,
        # Directional edge is positive when price pressure agrees with the trade.
        "edge_3m": _clip(direction * ch3m, -0.25, 0.25) / 0.25,
        "edge_15m": _clip(direction * ch15m, -0.40, 0.40) / 0.40,
        "edge_30m": _clip(direction * ch30m, -0.60, 0.60) / 0.60,
        "btc_alignment": btc_alignment,
        "symbol_fail_streak": _clip(trade.get("symbol_fail_streak"), 0, 5) / 5.0,
        "is_shadow": 1.0 if str(trade.get("signal_source", "live")).lower() == "shadow" else 0.0,
        "watch_impulse": _clip(trade.get("watch_impulse"), 0.0, 0.25) / 0.25,
        "pullback_depth": _clip(trade.get("pending_retest_depth"), 0.0, 0.05) / 0.05,
        "reclaim_recovery": _clip(trade.get("reclaim_recovery"), 0.0, 1.0),
        "watch_age": _clip(trade.get("pending_confirmation_seconds"), 0.0, 600.0) / 600.0,
        "is_continuation_lane": 1.0 if str(trade.get("paper_setup_lane", "")).upper() == "CONTINUATION" else 0.0,
        "is_sweep_reversal_lane": 1.0 if str(trade.get("paper_setup_lane", "")).upper() == "SWEEP_REVERSAL" else 0.0,
        "liquidity_rank": _clip(trade.get("liquidity_rank_percentile"), 0.0, 1.0, 0.5),
        "liquidity_log_turnover": _clip(
            math.log1p(max(0.0, _clip(trade.get("liquidity_quote_60m"), 0.0, 1e30))),
            0.0,
            30.0,
        ) / 30.0,
        "liquidity_active_fraction": _clip(trade.get("liquidity_active_fraction"), 0.0, 1.0),
        "liquidity_unique_fraction": _clip(trade.get("liquidity_unique_fraction"), 0.0, 1.0),
        "atr1_pct": _clip(trade.get("atr1_pct"), 0.0, 0.03) / 0.03,
        "normalized_directional_move": _clip(trade.get("normalized_directional_move"), 0.0, 6.0) / 6.0,
        "book_spread_bps": _clip(trade.get("book_spread_bps"), 0.0, 100.0) / 100.0,
        "book_depth_log": _clip(
            math.log1p(max(0.0, _clip(trade.get("book_depth_usdt"), 0.0, 1e18))),
            0.0,
            30.0,
        ) / 30.0,

        # V20.1 audit-only spike/regime fields. They are persisted in JSON/SQLite
        # but are not added to FEATURE_NAMES yet, so the adaptive model schema
        # remains frozen while we collect a clean forward cohort.
        "spike_impulse_move": _clip(trade.get("spike_impulse_move"), 0.0, 0.50),
        "spike_atr_mult": _clip(trade.get("spike_atr_mult"), 0.0, 12.0),
        "spike_volume_pace": _clip(trade.get("spike_volume_pace"), 0.0, 20.0),
        "spike_body_acceleration": _clip(trade.get("spike_body_acceleration"), 0.0, 20.0),
        "spike_range_acceleration": _clip(trade.get("spike_range_acceleration"), 0.0, 20.0),
        "spike_base_distance": _clip(trade.get("spike_base_distance"), 0.0, 0.60),
        "spike_tf_agreement": _clip(trade.get("spike_tf_agreement"), 0.0, 4.0),
        "regime_is_continuation": 1.0 if str(trade.get("spike_regime", "")).upper() == "CONTINUATION" else 0.0,
        "regime_is_exhaustion": 1.0 if str(trade.get("spike_regime", "")).upper() == "EXHAUSTION" else 0.0,
        "regime_cont_score": _clip(trade.get("regime_cont_score"), 0.0, 8.0),
        "regime_fade_score": _clip(trade.get("regime_fade_score"), 0.0, 8.0),
        "regime_failed_reclaim": 1.0 if trade.get("regime_failed_reclaim") else 0.0,
        "regime_cont_15m_aligned": 1.0 if trade.get("regime_cont_15m_aligned") else 0.0,
        "regime_cont_micro_momentum": 1.0 if trade.get("regime_cont_micro_momentum") else 0.0,
        "regime_micro_overextended": 1.0 if trade.get("regime_micro_overextended") else 0.0,
        "regime_exhaust_micro_reversal": 1.0 if trade.get("regime_exhaust_micro_reversal") else 0.0,
        "regime_volume_faded": 1.0 if trade.get("regime_volume_faded") else 0.0,
        "regime_no_new_extreme_ok": 1.0 if trade.get("regime_no_new_extreme_ok") else 0.0,
        "regime_spike_context_ok": 1.0 if trade.get("regime_spike_context_ok") else 0.0,
        "entry_vol1_raw": _clip(trade.get("entry_vol1_raw"), 0.0, 12.0),
        "entry_mom1": _clip(trade.get("entry_mom1"), -0.25, 0.25),
        "entry_mom3": _clip(trade.get("entry_mom3"), -0.25, 0.25),
        "entry_mom5": _clip(trade.get("entry_mom5"), -0.40, 0.40),
        "entry_mom15": _clip(trade.get("entry_mom15"), -0.60, 0.60),
        "intel_overheat": _clip(trade.get("intel_overheat"), 0.0, 100.0) / 100.0,
        "intel_climax": _clip(trade.get("intel_climax"), 0.0, 100.0) / 100.0,
        "intel_squeeze": _clip(trade.get("intel_squeeze"), 0.0, 100.0) / 100.0,
        "intel_rejection_wick": _clip(trade.get("intel_rejection_wick"), 0.0, 1.0),
        "intel_distance_atr": _clip(trade.get("intel_distance_atr"), 0.0, 8.0) / 8.0,
        "intel_volume_health": _clip(trade.get("intel_volume_health"), 0.0, 3.0) / 3.0,
        "intel_range_expansion": _clip(trade.get("intel_range_expansion"), 0.0, 6.0) / 6.0,
        "intel_rsi_fast": _clip(trade.get("intel_rsi_fast"), 0.0, 100.0) / 100.0,
        "intel_setup_quality": _clip(trade.get("intel_setup_quality"), 0.0, 100.0) / 100.0,
        "intel_regime_conflict": 1.0 if trade.get("intel_regime_conflict") else 0.0,
        "intel_compression": _clip(trade.get("intel_compression"), 0.0, 100.0) / 100.0,
        "intel_structure": _clip(trade.get("intel_structure"), 0.0, 100.0) / 100.0,
        "intel_false_breakout": _clip(trade.get("intel_false_breakout"), 0.0, 100.0) / 100.0,
        "intel_continuation_edge": _clip(trade.get("intel_continuation_edge"), 0.0, 100.0) / 100.0,
        "intel_pullback_risk": _clip(trade.get("intel_pullback_risk"), 0.0, 100.0) / 100.0,
    }
    raw_score = features["score"] * 100.0
    raw_vol1 = features["vol1"] * 8.0
    features["score_below_guard"] = 1.0 if raw_score < EVIDENCE_MIN_SCORE else 0.0
    features["vol1_below_guard"] = 1.0 if raw_vol1 <= EVIDENCE_MIN_VOL1 else 0.0
    features["evidence_quality_pass"] = (
        1.0
        if raw_score >= EVIDENCE_MIN_SCORE and raw_vol1 > EVIDENCE_MIN_VOL1
        else 0.0
    )
    return features


def _vector_from_dict(features: Dict[str, float]) -> List[float]:
    # V16.2 backups do not contain the four fields below. Derive the three
    # quality markers from the normalized historical score/vol1 values so the
    # first 50 outcomes remain usable after the feature-schema upgrade.
    compatible = dict(features or {})
    raw_score = _clip(compatible.get("score"), 0, 1) * 100.0
    raw_vol1 = _clip(compatible.get("vol1"), 0, 1) * 8.0
    compatible.setdefault(
        "score_below_guard", 1.0 if raw_score < EVIDENCE_MIN_SCORE else 0.0
    )
    compatible.setdefault(
        "vol1_below_guard", 1.0 if raw_vol1 <= EVIDENCE_MIN_VOL1 else 0.0
    )
    compatible.setdefault(
        "evidence_quality_pass",
        1.0 if raw_score >= EVIDENCE_MIN_SCORE and raw_vol1 > EVIDENCE_MIN_VOL1 else 0.0,
    )
    compatible.setdefault("symbol_fail_streak", 0.0)
    compatible.setdefault("is_shadow", 0.0)
    return [float(compatible.get(name, 0.0)) for name in FEATURE_NAMES]


def get_model_state() -> ModelState:
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT state_json FROM adaptive_model_state WHERE id = 1").fetchone()
        if row is None:
            return ModelState()
        raw = json.loads(row["state_json"])
        allowed = {field.name for field in fields(ModelState)}
        state = ModelState(**{key: value for key, value in raw.items() if key in allowed})
        expected_weights = len(FEATURE_NAMES) + 1
        if (
            len(state.weights) != expected_weights
            or len(state.mean) != len(FEATURE_NAMES)
            or len(state.std) != len(FEATURE_NAMES)
        ):
            # Feature schema changed. Old trades remain useful, but old coefficients
            # cannot be applied to the new vector and must return to warm-up.
            state.active = False
            state.weights = [0.0] * expected_weights
            state.mean = [0.0] * len(FEATURE_NAMES)
            state.std = [1.0] * len(FEATURE_NAMES)
            state.last_candidate_reason = "feature_schema_changed"
            conn.execute(
                "UPDATE adaptive_model_state SET state_json=? WHERE id=1",
                (json.dumps(asdict(state), ensure_ascii=False),),
            )
            conn.commit()
        if str(raw.get("data_policy", "legacy_all_sources")) != MODEL_DATA_POLICY:
            eligible_count = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM adaptive_trades "
                    "WHERE COALESCE(decision_reason, '') IN (?,?,?)",
                    (
                        EXHAUSTION_PAPER_REASON,
                        SPIKE_ADAPTIVE_BLOCK_REASON,
                        SPIKE_STRATEGY_GUARD_REASON,
                    ),
                ).fetchone()["n"]
                or 0
            )
            state.active = False
            state.trained_rows = eligible_count
            state.last_trained_closed_count = 0
            state.last_attempted_closed_count = 0
            state.last_candidate_reason = "v17_clean_forward_dataset"
            state.candidate_pass_streak = 0
            state.candidate_selected_total = 0
            state.deployment_fraction = 0.0
            state.data_policy = MODEL_DATA_POLICY
            state.weights = [0.0] * (len(FEATURE_NAMES) + 1)
            state.mean = [0.0] * len(FEATURE_NAMES)
            state.std = [1.0] * len(FEATURE_NAMES)
            conn.execute(
                "UPDATE adaptive_model_state SET state_json=? WHERE id=1",
                (json.dumps(asdict(state), ensure_ascii=False),),
            )
            conn.commit()
        return state


def _save_model_state(state: ModelState) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            "INSERT INTO adaptive_model_state(id, state_json) VALUES(1, ?) "
            "ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json",
            (json.dumps(asdict(state), ensure_ascii=False),),
        )
        conn.commit()


def _model_state_from_json(raw_json: str) -> Optional[ModelState]:
    try:
        raw = json.loads(raw_json)
        allowed = {field.name for field in fields(ModelState)}
        state = ModelState(**{key: value for key, value in raw.items() if key in allowed})
        if (
            len(state.weights) != len(FEATURE_NAMES) + 1
            or len(state.mean) != len(FEATURE_NAMES)
            or len(state.std) != len(FEATURE_NAMES)
        ):
            return None
        return state
    except Exception:
        return None


def _save_model_snapshot(state: ModelState, status: str, note: str = "") -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            "INSERT INTO adaptive_model_versions"
            "(version, state_json, status, saved_at, note) VALUES(?,?,?,?,?) "
            "ON CONFLICT(version) DO UPDATE SET "
            "state_json=excluded.state_json, status=excluded.status, "
            "saved_at=excluded.saved_at, note=excluded.note",
            (
                int(state.version),
                json.dumps(asdict(state), ensure_ascii=False),
                str(status),
                int(time.time()),
                str(note),
            ),
        )
        conn.commit()


def _load_model_snapshot(version: int) -> Optional[ModelState]:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT state_json FROM adaptive_model_versions WHERE version=?",
            (int(version),),
        ).fetchone()
    return _model_state_from_json(row["state_json"]) if row else None


def _next_model_version() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS max_version FROM adaptive_model_versions"
        ).fetchone()
    return int(row["max_version"] or 0) + 1


def _latest_trade_marker() -> Tuple[int, int]:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS max_id, COUNT(*) AS n FROM adaptive_trades"
        ).fetchone()
    return int(row["max_id"] or 0), int(row["n"] or 0)


def get_evidence_guard_state() -> EvidenceGuardState:
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT state_json FROM adaptive_rule_guard_state WHERE id = 1"
        ).fetchone()
    if row is None:
        latest_id, latest_count = _latest_trade_marker()
        state = EvidenceGuardState(
            activation_trade_id=latest_id,
            activation_closed_count=latest_count,
            created_at=int(time.time()),
        )
        _save_evidence_guard_state(state)
        return state
    try:
        raw = json.loads(row["state_json"])
        allowed = {field.name for field in fields(EvidenceGuardState)}
        return EvidenceGuardState(
            **{key: value for key, value in raw.items() if key in allowed}
        )
    except Exception:
        latest_id, latest_count = _latest_trade_marker()
        state = EvidenceGuardState(
            activation_trade_id=latest_id,
            activation_closed_count=latest_count,
            created_at=int(time.time()),
        )
        _save_evidence_guard_state(state)
        return state


def _save_evidence_guard_state(state: EvidenceGuardState) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            "INSERT INTO adaptive_rule_guard_state(id, state_json) VALUES(1, ?) "
            "ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json",
            (json.dumps(asdict(state), ensure_ascii=False),),
        )
        conn.commit()


def evidence_guard(trade: Dict[str, Any]) -> Tuple[bool, str]:
    """Apply the auditable legacy Score/Vol1 rule before a live signal.

    Rejected candidates are not discarded: analyze_symbol sends them to the
    shadow tracker so the bot can measure missed TP3 winners and auto-disable
    this guard if new evidence contradicts the first 50 outcomes.
    """
    state = get_evidence_guard_state()
    score = float(trade.get("score", 0.0) or 0.0)
    vol1 = float(trade.get("vol1", 0.0) or 0.0)
    failures: List[str] = []
    if score < float(state.min_score):
        failures.append(f"Score {score:.0f} < {state.min_score:.0f}")
    if vol1 <= float(state.min_vol1):
        failures.append(f"Vol1 x{vol1:.2f} <= x{state.min_vol1:.2f}")

    accepted = bool(not state.active or not failures)
    if not state.active:
        reason = f"evidence guard v{state.version} inactive ({state.disabled_reason or 'audit rollback'})"
    elif accepted:
        reason = (
            f"evidence guard v{state.version} passed: Score {score:.0f}, "
            f"Vol1 x{vol1:.2f}"
        )
    else:
        reason = f"evidence guard v{state.version} blocked: " + "; ".join(failures)

    trade["evidence_guard_version"] = int(state.version)
    trade["evidence_guard_accepted"] = accepted
    trade["evidence_guard_reason"] = reason
    return accepted, reason


def _event(event_type: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            "INSERT INTO adaptive_events(created_at, event_type, message, payload_json) VALUES(?,?,?,?)",
            (
                int(time.time()),
                event_type,
                message,
                json.dumps(payload or {}, ensure_ascii=False),
            ),
        )
        conn.commit()


def predict_probability(trade: Dict[str, Any]) -> Tuple[Optional[float], ModelState]:
    state = get_model_state()
    if not MODEL_ENABLED or not state.active or state.trained_rows < MIN_TRAIN_TRADES:
        return None, state

    x = _vector_from_dict(build_feature_dict(trade))
    z = state.weights[0]
    for i, value in enumerate(x):
        normalized = (value - state.mean[i]) / max(state.std[i], 1e-8)
        z += state.weights[i + 1] * normalized
    z = max(-35.0, min(35.0, z))
    return 1.0 / (1.0 + math.exp(-z)), state


def adaptive_gate(trade: Dict[str, Any]) -> Tuple[bool, str, Optional[float]]:
    strategy = str(trade.get("strategy", "")).upper()
    if not strategy.startswith("PRO_SPIKE_REGIME_"):
        return True, "adaptive model dedicated to SPIKE REGIME; bypass", None

    probability, state = predict_probability(trade)
    if probability is None:
        return True, f"adaptive warm-up: {state.trained_rows}/{MIN_TRAIN_TRADES}", None

    accepted = probability >= state.threshold
    trade["adaptive_probability"] = probability
    trade["adaptive_shadow_probability"] = probability

    if SHADOW_ONLY:
        trade["adaptive_shadow_accepted"] = accepted
        trade["adaptive_model_version"] = state.version
        return True, (
            f"adaptive shadow v{state.version}: p={probability:.3f}, "
            f"threshold={state.threshold:.3f}, would_accept={accepted}"
        ), probability

    # A newly promoted model begins as a canary. Only a stable fraction of
    # candidates belongs to the model arm; the rest remains an untouched
    # baseline control group. Successful live audits gradually expand it.
    fraction = min(1.0, max(0.0, float(state.deployment_fraction or ADAPTIVE_INITIAL_LIVE_FRACTION)))
    identity = str(
        trade.get("signal_id")
        or f"{trade.get('symbol','?')}:{trade.get('side','?')}:{trade.get('created_at',0)}:{trade.get('strategy','?')}"
    )
    cohort_value = int(hashlib.sha256(identity.encode("utf-8")).hexdigest()[:8], 16) / 0xFFFFFFFF
    in_model_cohort = cohort_value < fraction
    trade["adaptive_canary"] = in_model_cohort
    if not in_model_cohort:
        trade["adaptive_shadow_accepted"] = None
        trade["adaptive_model_version"] = 0
        return True, (
            f"adaptive control group: model v{state.version} observes only; "
            f"live fraction={fraction*100:.0f}%"
        ), probability

    trade["adaptive_shadow_accepted"] = accepted
    trade["adaptive_model_version"] = state.version
    return accepted, (
        f"adaptive v{state.version}: p={probability:.3f}, "
        f"threshold={state.threshold:.3f}, accepted={accepted}, "
        f"live fraction={fraction*100:.0f}%"
    ), probability


def _estimate_pnl_r(signal: Dict[str, Any], result: str) -> float:
    """Estimate the actually executable result in risk units.

    A profit is recorded only at TP3. TP1 and TP2 are intermediate targets and
    do not create a positive label. Expired trades use their observed exit price
    instead of an invented constant value. Round-trip fees/slippage are included
    through ROUND_TRIP_COST_MOVE.
    """
    entry = _clip(signal.get("entry"), 1e-12, 1e18, 1.0)
    sl = _clip(signal.get("sl"), 0.0, 1e18, entry)
    risk = max(abs(entry - sl), entry * 1e-8)
    cost_r = entry * max(0.0, ROUND_TRIP_COST_MOVE) / risk

    if result == "sl":
        return -1.0 - cost_r
    if result == "profit":
        profit_target = _clip(signal.get(PROFIT_TARGET_KEY), 0.0, 1e18, entry)
        return abs(profit_target - entry) / risk - cost_r

    exit_price = _clip(signal.get("_closing_price"), 1e-12, 1e18, entry)
    direction = 1.0 if str(signal.get("side", "")).upper() == "LONG" else -1.0
    return direction * (exit_price - entry) / risk - cost_r


def record_closed_trade(signal: Dict[str, Any], result: str, source: Optional[str] = None) -> Dict[str, Any]:
    """
    Call this exactly once when a signal closes.

    result:
      profit -> label 1
      sl      -> label 0
      expired -> label 0 (conservative)
    """
    if result not in {"profit", "sl", "expired"}:
        raise ValueError(f"Unsupported result: {result}")

    init_adaptive_db()

    created_at = int(signal.get("created_at", int(time.time())))
    closed_at = int(time.time())
    signal_id = str(
        signal.get("signal_id")
        or f"{signal.get('symbol','?')}:{signal.get('side','?')}:{created_at}:{signal.get('strategy','?')}"
    )
    features = build_feature_dict(signal)
    probability = signal.get("adaptive_probability", signal.get("adaptive_shadow_probability"))
    model_version = int(signal.get("adaptive_model_version", 0) or 0)
    shadow_accepted = signal.get("adaptive_shadow_accepted")
    evidence_guard_version = int(signal.get("evidence_guard_version", 0) or 0)
    evidence_guard_accepted = signal.get("evidence_guard_accepted")
    evidence_guard_reason = signal.get("evidence_guard_reason")
    decision_reason = signal.get("shadow_reason") or signal.get("adaptive_reason")
    label = 1 if result == "profit" else 0
    pnl_r = _estimate_pnl_r(signal, result)
    source = str(source or signal.get("signal_source", "live"))
    exit_price = signal.get("_closing_price")
    mfe_r = float(signal.get("mfe_r", 0.0) or 0.0)
    mae_r = float(signal.get("mae_r", 0.0) or 0.0)
    duration_minutes = max(0.0, (closed_at - created_at) / 60.0)

    with _LOCK, _connect() as conn:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO adaptive_trades(
                signal_id, created_at, closed_at, source, symbol, side, strategy, grade,
                result, label, pnl_r, exit_price, mfe_r, mae_r, duration_minutes,
                features_json, model_version,
                model_probability, shadow_accepted,
                evidence_guard_version, evidence_guard_accepted, evidence_guard_reason,
                decision_reason
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                signal_id,
                created_at,
                closed_at,
                source,
                str(signal.get("symbol", "?")),
                str(signal.get("side", "?")),
                str(signal.get("strategy", "?")),
                str(signal.get("grade", "?")),
                result,
                label,
                pnl_r,
                float(exit_price) if exit_price is not None else None,
                mfe_r,
                mae_r,
                duration_minutes,
                json.dumps(features, ensure_ascii=False),
                model_version,
                float(probability) if probability is not None else None,
                int(bool(shadow_accepted)) if shadow_accepted is not None else None,
                evidence_guard_version,
                int(bool(evidence_guard_accepted)) if evidence_guard_accepted is not None else None,
                str(evidence_guard_reason) if evidence_guard_reason is not None else None,
                str(decision_reason) if decision_reason is not None else None,
            ),
        )
        conn.commit()

    if cursor.rowcount == 0:
        return {"trained": False, "reason": "duplicate_signal", "inserted": False}

    # Audit the currently active champion before a scheduled retraining can
    # replace it on the same milestone outcome.
    evidence_guard_audit = maybe_evidence_guard_audit()
    live_audit = maybe_live_audit()
    if live_audit and live_audit.get("action") == "rollback":
        training_report = {
            "attempted": False,
            "trained": False,
            "reason": "rollback_cooldown",
            "closed_count": adaptive_closed_count(),
        }
    else:
        training_report = maybe_retrain()
    return {
        "inserted": True,
        "evidence_guard_audit": evidence_guard_audit,
        "live_audit": live_audit,
        "training_report": training_report,
        **training_report,
    }


def _sigmoid(z: float) -> float:
    z = max(-35.0, min(35.0, z))
    return 1.0 / (1.0 + math.exp(-z))


def _logloss(labels: Sequence[int], probabilities: Sequence[float]) -> float:
    if not labels:
        return 999.0
    total = 0.0
    for y, p in zip(labels, probabilities):
        p = min(1.0 - 1e-7, max(1e-7, p))
        total += -(y * math.log(p) + (1 - y) * math.log(1 - p))
    return total / len(labels)


def _mean_std(rows: Sequence[Sequence[float]]) -> Tuple[List[float], List[float]]:
    n_features = len(FEATURE_NAMES)
    means = [0.0] * n_features
    for row in rows:
        for j, value in enumerate(row):
            means[j] += value
    count = max(len(rows), 1)
    means = [x / count for x in means]

    stds = [0.0] * n_features
    for row in rows:
        for j, value in enumerate(row):
            stds[j] += (value - means[j]) ** 2
    stds = [math.sqrt(x / count) if x > 1e-10 else 1.0 for x in stds]
    return means, stds


def _normalize(rows: Sequence[Sequence[float]], mean: Sequence[float], std: Sequence[float]) -> List[List[float]]:
    return [
        [(value - mean[j]) / max(std[j], 1e-8) for j, value in enumerate(row)]
        for row in rows
    ]


def _train_logistic(
    x_train: Sequence[Sequence[float]],
    y_train: Sequence[int],
    epochs: int = 600,
    learning_rate: float = 0.035,
    l2: float = 0.04,
) -> List[float]:
    random.seed(1337)
    n_features = len(FEATURE_NAMES)
    weights = [0.0] * (n_features + 1)

    positives = sum(y_train)
    negatives = len(y_train) - positives
    pos_weight = len(y_train) / max(2.0 * positives, 1.0)
    neg_weight = len(y_train) / max(2.0 * negatives, 1.0)

    order = list(range(len(x_train)))
    for epoch in range(epochs):
        random.shuffle(order)
        lr = learning_rate / (1.0 + epoch / 300.0)
        grad = [0.0] * len(weights)

        for idx in order:
            x = x_train[idx]
            y = y_train[idx]
            z = weights[0] + sum(weights[j + 1] * x[j] for j in range(n_features))
            p = _sigmoid(z)
            sample_weight = pos_weight if y == 1 else neg_weight
            error = (p - y) * sample_weight
            grad[0] += error
            for j in range(n_features):
                grad[j + 1] += error * x[j]

        n = max(len(x_train), 1)
        weights[0] -= lr * grad[0] / n
        for j in range(1, len(weights)):
            regularized = grad[j] / n + l2 * weights[j]
            weights[j] -= lr * regularized

    return weights


def _predict_matrix(weights: Sequence[float], rows: Sequence[Sequence[float]]) -> List[float]:
    out: List[float] = []
    for row in rows:
        z = weights[0] + sum(weights[j + 1] * row[j] for j in range(len(row)))
        out.append(_sigmoid(z))
    return out


def _choose_threshold(labels: Sequence[int], probabilities: Sequence[float], pnl_r: Sequence[float]) -> Tuple[float, Dict[str, float]]:
    best_threshold = MIN_SIGNAL_PROBABILITY
    best_score = -1e9
    best_metrics: Dict[str, float] = {}

    low = int(MIN_SIGNAL_PROBABILITY * 100)
    high = int(MAX_SIGNAL_PROBABILITY * 100)

    for raw in range(low, high + 1):
        threshold = raw / 100.0
        chosen = [i for i, p in enumerate(probabilities) if p >= threshold]
        coverage = len(chosen) / max(len(labels), 1)
        if len(chosen) < 5 or coverage < MIN_VALIDATION_COVERAGE:
            continue

        wins = sum(labels[i] for i in chosen)
        wr = wins / len(chosen)
        expectancy = sum(pnl_r[i] for i in chosen) / len(chosen)

        # Do not optimize a candidate that cannot meet the requested TP3-majority
        # objective even on calibration data. Coverage still prevents a tiny,
        # cherry-picked handful of signals from winning the search.
        if wr <= ADAPTIVE_TARGET_SUCCESS_RATE:
            continue

        # Prefer expectancy and stability, not win-rate alone.
        score = expectancy * 2.0 + wr * 0.5 + coverage * 0.15
        if score > best_score:
            best_score = score
            best_threshold = threshold
            best_metrics = {
                "coverage": coverage,
                "selected_wr": wr,
                "selected_count": float(len(chosen)),
                "expectancy_r": expectancy,
                "objective": score,
            }

    if not best_metrics:
        best_metrics = {
            "coverage": 1.0,
            "selected_wr": sum(labels) / max(len(labels), 1),
            "selected_count": float(len(labels)),
            "expectancy_r": sum(pnl_r) / max(len(pnl_r), 1),
            "objective": -999.0,
        }

    return best_threshold, best_metrics


def _selection_metrics(
    labels: Sequence[int],
    probabilities: Sequence[float],
    pnl_r: Sequence[float],
    threshold: float,
) -> Dict[str, float]:
    chosen = [i for i, probability in enumerate(probabilities) if probability >= threshold]
    selected_count = len(chosen)
    coverage = selected_count / max(len(labels), 1)
    selected_wr = (
        sum(labels[i] for i in chosen) / selected_count if selected_count else 0.0
    )
    expectancy = (
        sum(pnl_r[i] for i in chosen) / selected_count if selected_count else -999.0
    )
    return {
        "coverage": coverage,
        "selected_wr": selected_wr,
        "selected_count": float(selected_count),
        "expectancy_r": expectancy,
        "baseline_wr": sum(labels) / max(len(labels), 1),
        "baseline_expectancy_r": sum(pnl_r) / max(len(pnl_r), 1),
    }


def _outcome_metrics(rows: Sequence[Any]) -> Dict[str, Any]:
    profit = 0
    sl = 0
    expired = 0
    pnl_values: List[float] = []
    for row in rows:
        result = str(row["result"] or "")
        if result == "profit":
            profit += 1
        elif result == "sl":
            sl += 1
        else:
            expired += 1
        pnl_values.append(float(row["pnl_r"] or 0.0))

    total = len(rows)
    resolved = profit + sl
    return {
        "n": total,
        "profit": profit,
        "sl": sl,
        "expired": expired,
        "success_rate": profit / total if total else 0.0,
        "resolved_wr": profit / resolved if resolved else 0.0,
        "expectancy_r": sum(pnl_values) / total if total else 0.0,
    }


def _metrics_line(metrics: Dict[str, Any]) -> str:
    return (
        f"{int(metrics.get('profit', 0))} TP3+ / "
        f"{int(metrics.get('sl', 0))} SL / "
        f"{int(metrics.get('expired', 0))} expired · "
        f"успех всех {float(metrics.get('success_rate', 0))*100:.1f}% · "
        f"{float(metrics.get('expectancy_r', 0)):+.3f}R"
    )


def _source_breakdown(rows: Sequence[Any]) -> Dict[str, Dict[str, Any]]:
    live_rows = [row for row in rows if str(row["source"] or "live") == "live"]
    shadow_rows = [row for row in rows if str(row["source"] or "live") == "shadow"]
    return {
        "all": _outcome_metrics(rows),
        "live": _outcome_metrics(live_rows),
        "shadow": _outcome_metrics(shadow_rows),
    }


def _collect_evidence_guard_metrics(state: EvidenceGuardState) -> Dict[str, Any]:
    baseline_limit = max(1, EVIDENCE_AUDIT_BASELINE_WINDOW)
    with _LOCK, _connect() as conn:
        baseline_rows = conn.execute(
            """
            SELECT id, result, label, pnl_r, source
            FROM adaptive_trades
            WHERE source='live' AND id <= ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(state.activation_trade_id), baseline_limit),
        ).fetchall()
        decision_rows = conn.execute(
            """
            SELECT id, result, label, pnl_r, source,
                   evidence_guard_accepted, evidence_guard_reason
            FROM adaptive_trades
            WHERE id > ? AND evidence_guard_version = ?
              AND (
                    (source='live' AND evidence_guard_accepted=1)
                    OR (source='shadow' AND evidence_guard_accepted=0)
                  )
            ORDER BY id ASC
            """,
            (int(state.activation_trade_id), int(state.version)),
        ).fetchall()

    accepted_rows = [
        row
        for row in decision_rows
        if str(row["source"]) == "live" and int(row["evidence_guard_accepted"] or 0) == 1
    ]
    blocked_rows = [
        row
        for row in decision_rows
        if str(row["source"]) == "shadow" and int(row["evidence_guard_accepted"] or 0) == 0
    ]
    return {
        "guard_version": int(state.version),
        "decision_count": len(decision_rows),
        "baseline": _outcome_metrics(baseline_rows),
        "accepted": _outcome_metrics(accepted_rows),
        "blocked": _outcome_metrics(blocked_rows),
    }


def maybe_evidence_guard_audit(force: bool = False) -> Optional[Dict[str, Any]]:
    if not EVIDENCE_AUDIT_ENABLED:
        return None
    state = get_evidence_guard_state()
    if not state.active:
        return None

    metrics = _collect_evidence_guard_metrics(state)
    decision_count = int(metrics["decision_count"])
    audit_every = max(1, EVIDENCE_AUDIT_EVERY)
    if (
        not force
        and decision_count - int(state.last_audit_decision_count or 0) < audit_every
    ):
        return None

    baseline = metrics["baseline"]
    accepted = metrics["accepted"]
    blocked = metrics["blocked"]
    enough_accepted = int(accepted["n"]) >= max(1, EVIDENCE_AUDIT_MIN_ACCEPTED)
    enough_blocked = int(blocked["n"]) >= max(1, EVIDENCE_AUDIT_MIN_BLOCKED)
    enough_baseline = int(baseline["n"]) >= 15

    # The most direct proof of harm is a profitable shadow group that the rule
    # refused. A second guarded condition catches broad before/after collapse.
    harmful_blocking = (
        enough_accepted
        and enough_blocked
        and int(blocked["profit"]) >= max(1, EVIDENCE_ROLLBACK_WINNERS)
        and float(blocked["expectancy_r"]) > 0.0
        and float(blocked["success_rate"])
        >= float(accepted["success_rate"]) + 0.10
    )
    material_underperformance = (
        enough_baseline
        and enough_accepted
        and float(baseline["expectancy_r"]) - float(accepted["expectancy_r"])
        >= EVIDENCE_ROLLBACK_EXPECTANCY_DROP_R
        and float(baseline["success_rate"]) - float(accepted["success_rate"])
        >= EVIDENCE_ROLLBACK_SUCCESS_DROP
    )

    if not enough_accepted or not enough_blocked:
        action = "collect_more"
        reason = "evidence_not_enough_comparison_rows"
    elif harmful_blocking:
        action = "rollback"
        reason = "evidence_too_many_profitable_signals_blocked"
    elif material_underperformance:
        action = "rollback"
        reason = "evidence_guard_worse_than_baseline"
    else:
        action = "keep"
        reason = "evidence_guard_passed"

    state.last_audit_decision_count = decision_count
    state.last_audit_at = int(time.time())
    if action == "rollback":
        state.active = False
        state.disabled_reason = reason
    _save_evidence_guard_state(state)

    payload: Dict[str, Any] = {
        **metrics,
        "action": action,
        "reason": reason,
        "enough_baseline": enough_baseline,
        "enough_accepted": enough_accepted,
        "enough_blocked": enough_blocked,
        "guard_active": bool(state.active),
        "min_score": float(state.min_score),
        "min_vol1": float(state.min_vol1),
    }
    _event(
        "evidence_guard_audit",
        "Evidence guard rolled back" if action == "rollback" else "Evidence guard audit completed",
        payload,
    )
    return payload


def _collect_live_audit_metrics(state: ModelState) -> Dict[str, Any]:
    """Audit the adaptive model on PAPER canary cohorts.

    V20.3 does not depend on real-money rows. After model activation, candidates
    are deterministically split into a control cohort (model_version=0) and a
    model cohort. Model-rejected candidates are still tracked in a hidden lane.
    """
    baseline_limit = max(1, LIVE_AUDIT_BASELINE_WINDOW)
    reasons = (
        EXHAUSTION_PAPER_REASON,
        SPIKE_ADAPTIVE_BLOCK_REASON,
        SPIKE_STRATEGY_GUARD_REASON,
    )
    with _LOCK, _connect() as conn:
        post_rows = conn.execute(
            """
            SELECT id, result, label, pnl_r, source, shadow_accepted,
                   model_version, decision_reason
            FROM adaptive_trades
            WHERE id > ?
              AND COALESCE(decision_reason,'') IN (?,?,?)
            ORDER BY id ASC
            """,
            (int(state.activation_trade_id), *reasons),
        ).fetchall()

        pre_rows = conn.execute(
            """
            SELECT id, result, label, pnl_r, source, shadow_accepted,
                   model_version, decision_reason
            FROM adaptive_trades
            WHERE id <= ?
              AND COALESCE(decision_reason,'') IN (?,?,?)
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(state.activation_trade_id), *reasons, baseline_limit),
        ).fetchall()

    control_rows = [
        row for row in post_rows if int(row["model_version"] or 0) == 0
    ]
    if len(control_rows) < max(1, LIVE_AUDIT_MIN_BASELINE):
        baseline_rows = list(reversed(pre_rows))
    else:
        baseline_rows = control_rows[-baseline_limit:]

    model_rows = [
        row for row in post_rows
        if int(row["model_version"] or 0) == int(state.version)
    ]
    accepted_rows = [
        row for row in model_rows if int(row["shadow_accepted"] or 0) == 1
    ]
    blocked_rows = [
        row for row in model_rows if int(row["shadow_accepted"] or 0) == 0
    ]
    return {
        "model_version": int(state.version),
        "decision_count": len(model_rows),
        "baseline": _outcome_metrics(baseline_rows),
        "live": _outcome_metrics(accepted_rows),  # key retained for compatibility
        "blocked": _outcome_metrics(blocked_rows),
        "control": _outcome_metrics(control_rows),
    }


def _rollback_to_parent(state: ModelState, closed_count: int, reason: str) -> Dict[str, Any]:
    failed_version = int(state.version)
    parent_version = int(state.parent_version or 0)
    latest_id, latest_count = _latest_trade_marker()

    state.last_live_audit_at = int(time.time())
    _save_model_snapshot(state, "rolled_back", reason)

    restored = _load_model_snapshot(parent_version)
    if restored is None:
        parent_version = 0
        restored = ModelState(version=0, active=False, created_at=int(time.time()))

    restored.active = bool(parent_version > 0)
    restored.activation_trade_id = latest_id
    restored.activation_closed_count = latest_count
    restored.last_live_audit_decision_count = 0
    restored.last_live_audit_at = int(time.time())
    restored.last_attempted_closed_count = max(
        int(restored.last_attempted_closed_count or 0), int(closed_count)
    )
    restored.last_candidate_reason = f"restored_after_v{failed_version}"
    _save_model_state(restored)
    _save_model_snapshot(
        restored,
        "active" if restored.active else "baseline",
        f"restored_after_v{failed_version}",
    )
    return {
        "from_version": failed_version,
        "to_version": int(restored.version),
        "to_active": bool(restored.active),
    }


def maybe_live_audit(force: bool = False) -> Optional[Dict[str, Any]]:
    if not LIVE_AUDIT_ENABLED or SHADOW_ONLY:
        return None
    state = get_model_state()
    if not state.active or state.version <= 0 or state.activation_trade_id < 0:
        return None

    metrics = _collect_live_audit_metrics(state)
    decision_count = int(metrics["decision_count"])
    audit_every = max(1, LIVE_AUDIT_EVERY)
    if not force and decision_count - int(state.last_live_audit_decision_count or 0) < audit_every:
        return None

    baseline = metrics["baseline"]
    live = metrics["live"]
    blocked = metrics["blocked"]
    enough_baseline = int(baseline["n"]) >= max(1, LIVE_AUDIT_MIN_BASELINE)
    enough_live = int(live["n"]) >= max(1, LIVE_AUDIT_MIN_LIVE)

    expectancy_drop = float(baseline["expectancy_r"]) - float(live["expectancy_r"])
    success_drop = float(baseline["success_rate"]) - float(live["success_rate"])
    material_underperformance = (
        enough_baseline
        and enough_live
        and expectancy_drop >= ROLLBACK_EXPECTANCY_DROP_R
        and success_drop >= ROLLBACK_SUCCESS_DROP
    )
    severe_negative = (
        enough_baseline
        and enough_live
        and float(live["expectancy_r"]) <= ROLLBACK_SEVERE_EXPECTANCY_R
        and float(baseline["expectancy_r"]) >= 0.0
    )
    harmful_blocking = (
        enough_baseline
        and enough_live
        and int(blocked["n"]) >= max(1, ROLLBACK_MIN_BLOCKED)
        and float(blocked["success_rate"]) >= ROLLBACK_HARMFUL_BLOCK_WIN_RATE
        and float(blocked["success_rate"]) >= float(live["success_rate"]) + 0.10
        and float(blocked["expectancy_r"]) > 0.0
    )

    if not enough_baseline or not enough_live:
        action = "collect_more"
        reason = "not_enough_live_comparison_rows"
    elif AUTO_ROLLBACK_ENABLED and (material_underperformance or severe_negative or harmful_blocking):
        action = "rollback"
        if severe_negative:
            reason = "severe_negative_live_expectancy"
        elif harmful_blocking:
            reason = "too_many_profitable_signals_blocked"
        else:
            reason = "live_model_worse_than_baseline"
    else:
        action = "keep"
        reason = "live_model_guard_passed"

    state.last_live_audit_decision_count = decision_count
    state.last_live_audit_at = int(time.time())
    rollback: Optional[Dict[str, Any]] = None
    previous_fraction = float(state.deployment_fraction or ADAPTIVE_INITIAL_LIVE_FRACTION)
    next_fraction = previous_fraction
    if action == "rollback":
        rollback = _rollback_to_parent(state, adaptive_closed_count(), reason)
    else:
        if action == "keep" and enough_live:
            next_fraction = min(1.0, previous_fraction + max(0.0, ADAPTIVE_LIVE_FRACTION_STEP))
            state.deployment_fraction = next_fraction
        _save_model_state(state)
        _save_model_snapshot(state, "active", f"live_audit:{reason}")

    payload: Dict[str, Any] = {
        **metrics,
        "action": action,
        "reason": reason,
        "enough_baseline": enough_baseline,
        "enough_live": enough_live,
        "expectancy_drop_r": expectancy_drop,
        "success_drop": success_drop,
        "rollback": rollback,
        "previous_deployment_fraction": previous_fraction,
        "deployment_fraction": next_fraction,
    }
    _event(
        "live_model_audit",
        "Adaptive model rolled back" if action == "rollback" else "Adaptive live audit completed",
        payload,
    )
    return payload


def _predict_state_on_raw(state: ModelState, rows: Sequence[Sequence[float]]) -> List[float]:
    if (
        not state.active
        or len(state.weights) != len(FEATURE_NAMES) + 1
        or len(state.mean) != len(FEATURE_NAMES)
        or len(state.std) != len(FEATURE_NAMES)
    ):
        return []
    normalized = _normalize(rows, state.mean, state.std)
    return _predict_matrix(state.weights, normalized)


def _training_attempt_failed(
    state: ModelState,
    reason: str,
    closed_count: int,
    rows: Sequence[Any],
    extra: Optional[Dict[str, Any]] = None,
    all_rows: Optional[Sequence[Any]] = None,
) -> Dict[str, Any]:
    state.last_attempted_closed_count = int(closed_count)
    state.last_candidate_reason = str(reason)
    state.candidate_pass_streak = 0
    state.candidate_selected_total = 0
    _save_model_state(state)
    _save_model_snapshot(
        state,
        "active" if state.active else ("baseline" if state.version == 0 else "inactive"),
        f"training_attempt:{reason}",
    )
    payload: Dict[str, Any] = {
        "attempted": True,
        "trained": False,
        "promoted": False,
        "reason": reason,
        "candidate_reason": reason,
        "closed_count": int(closed_count),
        "next_at": int(closed_count) + max(1, RETRAIN_EVERY),
        "active": bool(state.active),
        "version": int(state.version),
        "training_data": _outcome_metrics(rows),
        "source_breakdown": _source_breakdown(rows),
        "model_data_policy": MODEL_DATA_POLICY,
    }
    if all_rows is not None:
        payload["all_closed_count"] = len(all_rows)
        payload["all_data"] = _outcome_metrics(all_rows)
        payload["paper_validation_data"] = _outcome_metrics(
            [
                row for row in all_rows
                if is_adaptive_training_reason(row["decision_reason"])
            ]
        )
    if extra:
        payload.update(extra)
    _event("model_training_attempt", "Adaptive training attempt postponed", payload)
    return payload


def maybe_retrain(force: bool = False) -> Dict[str, Any]:
    init_adaptive_db()
    state = get_model_state()

    with _LOCK, _connect() as conn:
        all_rows = conn.execute(
            """
            SELECT id, result, label, pnl_r, features_json,
                   source, model_version, shadow_accepted, decision_reason,
                   side, strategy
            FROM adaptive_trades
            ORDER BY closed_at ASC, id ASC
            """
        ).fetchall()

    # V17 is a clean forward experiment.  The 400-row archive is retained for
    # audit, but old LIVE/SHADOW outcomes came from changing strategies and are
    # not valid training data for the new entry.  Only a confirmed V17 reclaim
    # PAPER outcome can train this model.
    rows = [
        row for row in all_rows
        if is_adaptive_training_reason(row["decision_reason"])
    ]
    closed_count = len(rows)
    paper_rows = [
        row for row in all_rows
        if is_adaptive_training_reason(row["decision_reason"])
    ]
    if PRO_QUALITY_FORWARD_ENABLED and len(paper_rows) < PAPER_LANE_REQUIRED_OUTCOMES:
        paper_count = len(paper_rows)
        last_forward_report = int(STATE.get("last_forward_report_count", 0) or 0)
        milestone_report = bool(
            paper_count > 0
            and paper_count % max(1, RETRAIN_EVERY) == 0
            and paper_count > last_forward_report
        )
        if milestone_report:
            STATE["last_forward_report_count"] = paper_count
            save_state()
        return {
            "attempted": milestone_report,
            "trained": False,
            "promoted": False,
            "reason": "forward_validation_freeze",
            "candidate_reason": "forward_validation_freeze",
            "closed_count": closed_count,
            "all_closed_count": len(all_rows),
            "needed": PAPER_LANE_REQUIRED_OUTCOMES,
            "paper_collected": paper_count,
            "training_data": _outcome_metrics(rows),
            "all_data": _outcome_metrics(all_rows),
            "paper_validation_data": _outcome_metrics(paper_rows),
            "source_breakdown": _source_breakdown(rows),
            "model_data_policy": MODEL_DATA_POLICY,
            "next_at": closed_count + (PAPER_LANE_REQUIRED_OUTCOMES - paper_count),
        }
    if closed_count < MIN_TRAIN_TRADES:
        return {
            "attempted": bool(force),
            "trained": False,
            "reason": "warmup",
            "closed_count": closed_count,
            "needed": MIN_TRAIN_TRADES,
            "training_data": _outcome_metrics(rows),
            "source_breakdown": _source_breakdown(rows),
            "all_closed_count": len(all_rows),
            "all_data": _outcome_metrics(all_rows),
            "model_data_policy": MODEL_DATA_POLICY,
        }

    last_attempt = max(state.last_attempted_closed_count, state.last_trained_closed_count)
    if last_attempt > closed_count:
        last_attempt = closed_count
    if not force and last_attempt > 0 and closed_count - last_attempt < RETRAIN_EVERY:
        return {
            "attempted": False,
            "trained": False,
            "reason": "waiting_for_more_trades",
            "closed_count": closed_count,
            "next_at": last_attempt + RETRAIN_EVERY,
            "all_closed_count": len(all_rows),
            "model_data_policy": MODEL_DATA_POLICY,
        }

    rows = rows[-MAX_TRAIN_ROWS:]
    labels = [int(row["label"]) for row in rows]
    pnl_r = [float(row["pnl_r"]) for row in rows]
    vectors: List[List[float]] = []
    for row in rows:
        feature_payload = json.loads(row["features_json"])
        # Portable backups before V16.6.5 do not contain is_shadow inside the
        # feature JSON, but the database source column is authoritative.
        feature_payload["is_shadow"] = (
            1.0 if str(row["source"] or "live").lower() == "shadow" else 0.0
        )
        vectors.append(_vector_from_dict(feature_payload))

    positives = sum(labels)
    negatives = len(labels) - positives
    if positives < MIN_POSITIVE_ROWS or negatives < MIN_NEGATIVE_ROWS:
        return _training_attempt_failed(
            state,
            "class_imbalance",
            closed_count,
            rows,
            {"positives": positives, "negatives": negatives},
            all_rows,
        )

    # Three chronological blocks prevent threshold selection from contaminating
    # the final promotion test: train -> calibration -> independent newest test.
    test_size = max(MIN_VALIDATION_ROWS, int(len(rows) * VALIDATION_FRACTION))
    calibration_size = max(MIN_CALIBRATION_ROWS, int(len(rows) * VALIDATION_FRACTION))
    max_holdout = len(rows) // 2
    if test_size + calibration_size > max_holdout:
        test_size = max(MIN_VALIDATION_ROWS, max_holdout // 2)
        calibration_size = max_holdout - test_size
    train_size = len(rows) - calibration_size - test_size

    if train_size < 25 or calibration_size < 10 or test_size < 10:
        return _training_attempt_failed(
            state,
            "split_too_small",
            closed_count,
            rows,
            {
                "train_rows": train_size,
                "calibration_rows": calibration_size,
                "validation_rows": test_size,
            },
            all_rows,
        )

    train_end = train_size
    calibration_end = train_size + calibration_size
    x_train_raw = vectors[:train_end]
    y_train = labels[:train_end]
    x_cal_raw = vectors[train_end:calibration_end]
    y_cal = labels[train_end:calibration_end]
    pnl_cal = pnl_r[train_end:calibration_end]
    x_test_raw = vectors[calibration_end:]
    y_test = labels[calibration_end:]
    pnl_test = pnl_r[calibration_end:]

    if sum(y_train) < 4 or (len(y_train) - sum(y_train)) < 4:
        return _training_attempt_failed(
            state,
            "train_split_too_small",
            closed_count,
            rows,
            {
                "train_positives": int(sum(y_train)),
                "train_negatives": int(len(y_train) - sum(y_train)),
            },
            all_rows,
        )

    mean, std = _mean_std(x_train_raw)
    x_train = _normalize(x_train_raw, mean, std)
    x_cal = _normalize(x_cal_raw, mean, std)
    x_test = _normalize(x_test_raw, mean, std)

    weights = _train_logistic(x_train, y_train)
    cal_prob = _predict_matrix(weights, x_cal)
    test_prob = _predict_matrix(weights, x_test)

    train_base_rate = min(0.95, max(0.05, sum(y_train) / len(y_train)))
    base_prob = [train_base_rate] * len(y_test)
    base_loss = _logloss(y_test, base_prob)
    model_loss = _logloss(y_test, test_prob)
    improvement = base_loss - model_loss

    threshold, calibration_metrics = _choose_threshold(y_cal, cal_prob, pnl_cal)
    test_metrics = _selection_metrics(y_test, test_prob, pnl_test, threshold)
    test_rows = rows[calibration_end:]
    selected_test_indices = [
        i for i, probability in enumerate(test_prob) if probability >= threshold
    ]
    test_baseline_outcomes = _outcome_metrics(test_rows)
    test_candidate_outcomes = _outcome_metrics(
        [test_rows[i] for i in selected_test_indices]
    )
    selected_forward_test_rows = [
        test_rows[i]
        for i in selected_test_indices
        if is_adaptive_training_reason(test_rows[i]["decision_reason"])
    ]
    test_forward_candidate_outcomes = _outcome_metrics(selected_forward_test_rows)
    test_side_breakdown = {
        "long": _outcome_metrics(
            [row for row in test_rows if str(row["side"] or "").upper() == "LONG"]
        ),
        "short": _outcome_metrics(
            [row for row in test_rows if str(row["side"] or "").upper() == "SHORT"]
        ),
    }
    recent_strategy_breakdown = {
        strategy_name: _outcome_metrics(
            [
                row for row in test_rows
                if str(row["strategy"] or "") == strategy_name
            ]
        )
        for strategy_name in sorted(
            {str(row["strategy"] or "?") for row in test_rows}
        )
    }
    forward_validation_ready = (
        int(test_forward_candidate_outcomes["n"])
        >= max(1, MIN_SELECTED_LIVE_TEST_ROWS)
    )
    forward_validation_positive = (
        forward_validation_ready
        and float(test_forward_candidate_outcomes["success_rate"])
        > ADAPTIVE_TARGET_SUCCESS_RATE
        and float(test_forward_candidate_outcomes["expectancy_r"]) > 0
    )

    enough_test_classes = min(sum(y_test), len(y_test) - sum(y_test)) >= 3
    passes_absolute_gate = (
        enough_test_classes
        and improvement >= MIN_MODEL_IMPROVEMENT
        and test_metrics["selected_count"] >= MIN_SELECTED_TEST_ROWS
        and test_metrics["coverage"] >= MIN_VALIDATION_COVERAGE
        and test_metrics["selected_wr"] > ADAPTIVE_TARGET_SUCCESS_RATE
        and test_metrics["expectancy_r"] > 0
        and test_metrics["expectancy_r"]
        >= test_metrics["baseline_expectancy_r"] + MIN_EXPECTANCY_IMPROVEMENT_R
        and test_metrics["selected_wr"] >= test_metrics["baseline_wr"]
        and forward_validation_positive
    )

    champion_expectancy = test_metrics["baseline_expectancy_r"]
    champion_count = len(y_test)
    if state.active:
        champion_prob = _predict_state_on_raw(state, x_test_raw)
        if champion_prob:
            champion_metrics = _selection_metrics(
                y_test, champion_prob, pnl_test, state.threshold
            )
            if champion_metrics["selected_count"] >= MIN_SELECTED_TEST_ROWS:
                champion_expectancy = champion_metrics["expectancy_r"]
                champion_count = int(champion_metrics["selected_count"])

    beats_champion = (
        not state.active
        or test_metrics["expectancy_r"]
        >= champion_expectancy + MIN_EXPECTANCY_IMPROVEMENT_R
    )
    champion_live_audit_ready = True
    champion_decision_count = 0
    if state.active and LIVE_AUDIT_ENABLED and not SHADOW_ONLY:
        champion_live_metrics = _collect_live_audit_metrics(state)
        champion_decision_count = int(champion_live_metrics["decision_count"])
        champion_live_audit_ready = champion_decision_count >= max(1, LIVE_AUDIT_EVERY)
    confirmation_passed = bool(
        passes_absolute_gate and beats_champion and champion_live_audit_ready
    )
    previous_streak = int(state.candidate_pass_streak or 0)
    previous_selected = int(state.candidate_selected_total or 0)
    candidate_pass_streak = previous_streak + 1 if confirmation_passed else 0
    candidate_selected_total = (
        previous_selected + int(test_metrics["selected_count"])
        if confirmation_passed
        else 0
    )
    confirmation_ready = (
        candidate_pass_streak >= max(1, ADAPTIVE_CONFIRMATION_PASSES)
        and candidate_selected_total >= max(1, ADAPTIVE_CONFIRMATION_SELECTED)
    )
    promote = bool(confirmation_passed and confirmation_ready)

    failed_checks: List[str] = []
    if not enough_test_classes:
        failed_checks.append("independent_test_class_imbalance")
    if improvement < MIN_MODEL_IMPROVEMENT:
        failed_checks.append("logloss_not_better")
    if test_metrics["selected_count"] < MIN_SELECTED_TEST_ROWS:
        failed_checks.append("too_few_selected_test_rows")
    if test_metrics["coverage"] < MIN_VALIDATION_COVERAGE:
        failed_checks.append("coverage_too_low")
    if test_metrics["selected_wr"] <= ADAPTIVE_TARGET_SUCCESS_RATE:
        failed_checks.append("tp3_majority_not_reached")
    if test_metrics["expectancy_r"] <= 0:
        failed_checks.append("negative_test_expectancy")
    if test_metrics["expectancy_r"] < test_metrics["baseline_expectancy_r"] + MIN_EXPECTANCY_IMPROVEMENT_R:
        failed_checks.append("expectancy_not_better_than_baseline")
    if not forward_validation_ready:
        failed_checks.append("not_enough_forward_test_rows")
    elif not forward_validation_positive:
        failed_checks.append("selected_forward_test_failed")

    if not enough_test_classes:
        candidate_reason = "independent_test_class_imbalance"
    elif improvement < MIN_MODEL_IMPROVEMENT:
        candidate_reason = "logloss_not_better"
    elif test_metrics["selected_count"] < MIN_SELECTED_TEST_ROWS:
        candidate_reason = "too_few_selected_test_rows"
    elif test_metrics["coverage"] < MIN_VALIDATION_COVERAGE:
        candidate_reason = "coverage_too_low"
    elif test_metrics["selected_wr"] <= ADAPTIVE_TARGET_SUCCESS_RATE:
        candidate_reason = "tp3_majority_not_reached"
    elif test_metrics["expectancy_r"] <= 0:
        candidate_reason = "negative_test_expectancy"
    elif test_metrics["expectancy_r"] < test_metrics["baseline_expectancy_r"] + MIN_EXPECTANCY_IMPROVEMENT_R:
        candidate_reason = "expectancy_not_better_than_baseline"
    elif not forward_validation_ready:
        candidate_reason = "not_enough_forward_test_rows"
    elif not forward_validation_positive:
        candidate_reason = "selected_forward_test_failed"
    elif not beats_champion:
        candidate_reason = "champion_kept"
    elif not champion_live_audit_ready:
        candidate_reason = "champion_live_audit_pending"
    elif not confirmation_ready:
        candidate_reason = "candidate_confirmation_pending"
    else:
        candidate_reason = "promoted"

    if promote:
        parent_version = int(state.version) if state.active else 0
        _save_model_snapshot(
            state,
            "archived" if state.active else "baseline",
            "replaced_by_new_champion",
        )
        activation_trade_id = int(rows[-1]["id"]) if rows else 0
        new_state = ModelState(
            version=_next_model_version(),
            active=True,
            threshold=threshold,
            trained_rows=len(rows),
            validation_rows=len(y_test),
            base_logloss=base_loss,
            model_logloss=model_loss,
            validation_win_rate=sum(y_test) / max(len(y_test), 1),
            validation_coverage=test_metrics["coverage"],
            validation_selected_wr=test_metrics["selected_wr"],
            validation_selected_count=int(test_metrics["selected_count"]),
            weights=list(weights),
            mean=list(mean),
            std=list(std),
            last_trained_closed_count=closed_count,
            last_attempted_closed_count=closed_count,
            validation_expectancy_r=test_metrics["expectancy_r"],
            validation_baseline_expectancy_r=test_metrics["baseline_expectancy_r"],
            last_candidate_reason=candidate_reason,
            created_at=int(time.time()),
            parent_version=parent_version,
            activation_trade_id=activation_trade_id,
            activation_closed_count=closed_count,
            last_live_audit_decision_count=0,
            last_live_audit_at=0,
            candidate_pass_streak=0,
            candidate_selected_total=0,
            deployment_fraction=min(1.0, max(0.05, ADAPTIVE_INITIAL_LIVE_FRACTION)),
        )
    elif state.active:
        # A weaker challenger must never deactivate or overwrite the champion.
        new_state = state
        new_state.last_attempted_closed_count = closed_count
        new_state.last_candidate_reason = candidate_reason
        new_state.candidate_pass_streak = candidate_pass_streak
        new_state.candidate_selected_total = candidate_selected_total
    else:
        # Keep the latest failed candidate metrics visible, but inactive.
        new_state = ModelState(
            version=state.version,
            active=False,
            threshold=threshold,
            trained_rows=len(rows),
            validation_rows=len(y_test),
            base_logloss=base_loss,
            model_logloss=model_loss,
            validation_win_rate=sum(y_test) / max(len(y_test), 1),
            validation_coverage=test_metrics["coverage"],
            validation_selected_wr=test_metrics["selected_wr"],
            validation_selected_count=int(test_metrics["selected_count"]),
            weights=list(weights),
            mean=list(mean),
            std=list(std),
            last_trained_closed_count=0,
            last_attempted_closed_count=closed_count,
            validation_expectancy_r=test_metrics["expectancy_r"],
            validation_baseline_expectancy_r=test_metrics["baseline_expectancy_r"],
            last_candidate_reason=candidate_reason,
            created_at=int(time.time()),
            candidate_pass_streak=candidate_pass_streak,
            candidate_selected_total=candidate_selected_total,
            deployment_fraction=0.0,
        )
    _save_model_state(new_state)
    _save_model_snapshot(
        new_state,
        "active" if new_state.active else ("baseline" if new_state.version == 0 else "inactive"),
        f"training_result:{candidate_reason}",
    )

    payload = {
        "attempted": True,
        "trained": True,
        "version": new_state.version,
        "active": new_state.active,
        "promoted": promote,
        "candidate_reason": candidate_reason,
        "closed_count": closed_count,
        "trained_rows": new_state.trained_rows,
        "calibration_rows": len(y_cal),
        "validation_rows": len(y_test),
        "base_logloss": round(base_loss, 6),
        "model_logloss": round(model_loss, 6),
        "improvement": round(improvement, 6),
        "threshold": round(threshold, 3),
        "calibration_coverage": round(calibration_metrics["coverage"], 3),
        "coverage": round(test_metrics["coverage"], 3),
        "selected_wr": round(test_metrics["selected_wr"], 3),
        "selected_count": int(test_metrics["selected_count"]),
        "expectancy_r": round(test_metrics["expectancy_r"], 4),
        "baseline_expectancy_r": round(test_metrics["baseline_expectancy_r"], 4),
        "champion_expectancy_r": round(champion_expectancy, 4),
        "champion_selected_count": champion_count,
        "champion_live_audit_ready": champion_live_audit_ready,
        "champion_decision_count": champion_decision_count,
        "candidate_pass_streak": candidate_pass_streak,
        "candidate_selected_total": candidate_selected_total,
        "failed_checks": failed_checks,
        "confirmation_passes_required": max(1, ADAPTIVE_CONFIRMATION_PASSES),
        "confirmation_selected_required": max(1, ADAPTIVE_CONFIRMATION_SELECTED),
        "deployment_fraction": float(new_state.deployment_fraction or 0.0),
        "training_data": _outcome_metrics(rows),
        "source_breakdown": _source_breakdown(rows),
        "all_closed_count": len(all_rows),
        "all_data": _outcome_metrics(all_rows),
        "paper_validation_data": _outcome_metrics(
            [
                row for row in all_rows
                if is_adaptive_training_reason(row["decision_reason"])
            ]
        ),
        "model_data_policy": MODEL_DATA_POLICY,
        "test_baseline": test_baseline_outcomes,
        "test_candidate": test_candidate_outcomes,
        "test_forward_candidate": test_forward_candidate_outcomes,
        "test_side_breakdown": test_side_breakdown,
        "recent_strategy_breakdown": recent_strategy_breakdown,
        "min_selected_live_test_rows": max(1, MIN_SELECTED_LIVE_TEST_ROWS),
        "forward_validation_ready": forward_validation_ready,
        "forward_validation_positive": forward_validation_positive,
        "next_at": closed_count + max(1, RETRAIN_EVERY),
        "shadow_only": SHADOW_ONLY,
    }
    _event(
        "model_retrained",
        "Adaptive challenger promoted" if promote else "Adaptive challenger rejected; champion kept",
        payload,
    )
    return {"trained": True, **payload}


ADAPTIVE_REASON_RU: Dict[str, str] = {
    "warmup": "ещё не накоплено 50 результатов",
    "class_imbalance": "слишком мало положительных или отрицательных примеров",
    "split_too_small": "недостаточно данных для трёх независимых частей проверки",
    "train_split_too_small": "в обучающей части недостаточно разных результатов",
    "independent_test_class_imbalance": "в последней независимой проверке мало разных исходов",
    "not_enough_selected_live_test_rows": "кандидат не набрал достаточно реальных LIVE-сделок для допуска",
    "selected_live_test_failed": "отдельная проверка кандидата на реальных LIVE-сделках отрицательная",
    "not_enough_forward_test_rows": "кандидат не набрал достаточно подтверждённых PAPER/LIVE-сделок в новой независимой проверке",
    "selected_forward_test_failed": "подтверждённая PAPER/LIVE-проверка кандидата отрицательная",
    "logloss_not_better": "прогноз новой модели не точнее базового",
    "too_few_selected_test_rows": "новая модель выбрала слишком мало проверочных сигналов",
    "coverage_too_low": "модель блокирует слишком большую часть сигналов",
    "tp3_majority_not_reached": "TP3+ ещё не превышает сумму SL и expired на независимой проверке",
    "negative_test_expectancy": "средний результат новой модели остаётся отрицательным",
    "expectancy_not_better_than_baseline": "средний результат не лучше базовой стратегии",
    "champion_kept": "действующая модель лучше нового кандидата",
    "champion_live_audit_pending": "сначала нужен live-аудит действующей модели",
    "candidate_confirmation_pending": "кандидат прошёл одну проверку, но ещё не набрал два подтверждения и 20 отобранных сигналов",
    "promoted": "новая модель прошла все проверки",
    "not_enough_live_comparison_rows": "для честного сравнения до/после пока мало live-сделок",
    "severe_negative_live_expectancy": "реальный средний результат модели стал сильно отрицательным",
    "too_many_profitable_signals_blocked": "модель заблокировала слишком много прибыльных сигналов",
    "live_model_worse_than_baseline": "реальные результаты модели хуже предыдущей версии",
    "live_model_guard_passed": "live-проверка не выявила ухудшения",
    "feature_schema_changed": "добавлены новые признаки качества; старые исходы сохранены",
    "model_dataset_policy_changed": "модель сброшена безопасно: обычный SHADOW исключён из обучения",
    "forward_validation_freeze": "критерии зафиксированы до 50 независимых V17.3 direct-measured PAPER-исходов",
    "seed_restored_feature_upgrade": "50 исходов восстановлены; следующая проверка продолжится по графику",
    "evidence_not_enough_comparison_rows": "для честной проверки фильтра пока мало отправленных или shadow-исходов",
    "evidence_too_many_profitable_signals_blocked": "фильтр начал пропускать слишком много TP3+ сигналов",
    "evidence_guard_worse_than_baseline": "результаты после фильтра хуже исходной выборки",
    "evidence_guard_passed": "новый защитный фильтр не показал ухудшения",
}


def _adaptive_reason_ru(reason: Any) -> str:
    key = str(reason or "unknown")
    return ADAPTIVE_REASON_RU.get(key, key)


def format_training_attempt_message(report: Dict[str, Any]) -> str:
    promoted = bool(report.get("promoted"))
    trained = bool(report.get("trained"))
    active = bool(report.get("active"))
    version = int(report.get("version", 0) or 0)
    closed_count = int(report.get("closed_count", 0) or 0)
    all_closed_count = int(report.get("all_closed_count", closed_count) or 0)
    reason = str(report.get("candidate_reason", report.get("reason", "unknown")))
    dataset = report.get("training_data") or {}
    all_dataset = report.get("all_data") or {}
    paper_dataset = report.get("paper_validation_data") or {}
    sources = report.get("source_breakdown") or {}

    if promoted:
        title = f"✅ НОВАЯ ADAPTIVE-МОДЕЛЬ V{version} ВКЛЮЧЕНА"
    elif trained:
        title = "🧠 Анализ завершён — кандидат отклонён"
    else:
        title = "🧠 Анализ выполнен — обучение отложено"

    lines = [
        title,
        f"Данных модели (V20.3 SPIKE candidates): {closed_count}",
        f"ВСЕХ наблюдений, включая диагностический SHADOW: {all_closed_count}",
        f"ДАННЫЕ МОДЕЛИ: {_metrics_line(dataset)}",
        f"Причина: {_adaptive_reason_ru(reason)}",
    ]
    if all_dataset:
        lines.append(f"ВСЯ ТЕЛЕМЕТРИЯ: {_metrics_line(all_dataset)}")
    if paper_dataset:
        lines.append(f"SPIKE REGIME training cohort: {_metrics_line(paper_dataset)}")
    if reason == "forward_validation_freeze":
        lines.append(
            f"Зафиксированная проверка: {int(report.get('paper_collected', 0) or 0)}/"
            f"{int(report.get('needed', PAPER_LANE_REQUIRED_OUTCOMES) or PAPER_LANE_REQUIRED_OUTCOMES)} "
            "независимых A/A+ liquidity-first PAPER-исходов. "
            "До завершения границы не меняются."
        )
    if sources:
        lines.extend(
            [
                f"LIVE · реальные сигналы: {_metrics_line(sources.get('live') or {})}",
                f"SHADOW · виртуальная проверка: {_metrics_line(sources.get('shadow') or {})}",
            ]
        )

    baseline = report.get("test_baseline")
    candidate = report.get("test_candidate")
    forward_candidate = report.get("test_forward_candidate")
    if baseline and candidate:
        lines.extend(
            [
                "",
                f"ДО · независимая проверка: {_metrics_line(baseline)}",
                f"КАНДИДАТ · выбранные сигналы: {_metrics_line(candidate)}",
                f"Порог допуска: {float(report.get('threshold', 0)):.3f} · "
                f"покрытие {float(report.get('coverage', 0))*100:.1f}%",
            ]
        )
        if forward_candidate:
            lines.append(
                f"FORWARD КАНДИДАТ · подтверждённые PAPER/LIVE: {_metrics_line(forward_candidate)} · "
                f"нужно минимум {int(report.get('min_selected_live_test_rows', MIN_SELECTED_LIVE_TEST_ROWS))}."
            )

    failed_checks = [
        _adaptive_reason_ru(item) for item in (report.get("failed_checks") or [])
    ]
    if failed_checks:
        lines.append("Не пройдены проверки: " + "; ".join(failed_checks))

    sides = report.get("test_side_breakdown") or {}
    if sides:
        lines.extend(
            [
                "",
                f"LONG · новая проверка: {_metrics_line(sides.get('long') or {})}",
                f"SHORT · новая проверка: {_metrics_line(sides.get('short') or {})}",
            ]
        )
    strategies = report.get("recent_strategy_breakdown") or {}
    if strategies:
        lines.append("Стратегии · новая проверка:")
        for strategy_name, strategy_metrics in strategies.items():
            lines.append(f"• {strategy_name}: {_metrics_line(strategy_metrics or {})}")

    if reason == "candidate_confirmation_pending":
        lines.append(
            f"Подтверждение: {int(report.get('candidate_pass_streak', 0))}/"
            f"{int(report.get('confirmation_passes_required', ADAPTIVE_CONFIRMATION_PASSES))} проверок · "
            f"отобрано суммарно {int(report.get('candidate_selected_total', 0))}/"
            f"{int(report.get('confirmation_selected_required', ADAPTIVE_CONFIRMATION_SELECTED))}."
        )

    if promoted:
        lines.extend(
            [
                "",
                f"Решение: V{version} запущена ограниченно на "
                f"{float(report.get('deployment_fraction', ADAPTIVE_INITIAL_LIVE_FRACTION))*100:.0f}% кандидатов.",
                f"Следующий live-отчёт — после {max(1, LIVE_AUDIT_EVERY)} "
                "закрытых решений этой модели.",
            ]
        )
    elif active:
        lines.append(f"Решение: действующая V{version} сохранена без изменений.")
    else:
        lines.append("Решение: базовая стратегия продолжает работать без adaptive-блокировки.")

    if not trained:
        lines.append(
            f"Следующая автоматическая попытка: на {int(report.get('next_at', closed_count + RETRAIN_EVERY))} результатах."
        )
    return "\n".join(lines)


def format_live_audit_message(report: Dict[str, Any]) -> str:
    version = int(report.get("model_version", 0) or 0)
    decisions = int(report.get("decision_count", 0) or 0)
    baseline = report.get("baseline") or {}
    live = report.get("live") or {}
    blocked = report.get("blocked") or {}
    action = str(report.get("action", "collect_more"))
    reason = str(report.get("reason", "unknown"))
    correctly_blocked = int(blocked.get("sl", 0)) + int(blocked.get("expired", 0))
    missed_winners = int(blocked.get("profit", 0))

    if action == "rollback":
        title = f"↩️ LIVE-АУДИТ V{version}: АВТООТКАТ"
    elif action == "keep":
        title = f"✅ LIVE-АУДИТ V{version}: МОДЕЛЬ ОСТАВЛЕНА"
    else:
        title = f"📊 LIVE-АУДИТ V{version}: НУЖНО БОЛЬШЕ ДАННЫХ"

    lines = [
        title,
        f"Закрытых решений модели: {decisions}",
        "",
        f"ДО · последние {int(baseline.get('n', 0))} live: {_metrics_line(baseline)}",
        f"ПОСЛЕ · отправленные {int(live.get('n', 0))} live: {_metrics_line(live)}",
        f"ЗАБЛОКИРОВАНО · {int(blocked.get('n', 0))}: {_metrics_line(blocked)}",
        f"Правильно заблокировано: {correctly_blocked} · пропущено TP3+: {missed_winners}",
        "",
        f"Причина решения: {_adaptive_reason_ru(reason)}",
    ]

    rollback = report.get("rollback") or {}
    if action == "rollback":
        to_version = int(rollback.get("to_version", 0) or 0)
        if bool(rollback.get("to_active")):
            lines.append(f"Решение: V{version} отключена, восстановлена V{to_version}.")
        else:
            lines.append(f"Решение: V{version} отключена, восстановлена базовая стратегия.")
    elif action == "keep":
        lines.append(
            f"Решение: V{version} оставлена; доля проверки увеличена с "
            f"{float(report.get('previous_deployment_fraction', 0))*100:.0f}% до "
            f"{float(report.get('deployment_fraction', 0))*100:.0f}%."
        )
    else:
        need_live = max(0, max(1, LIVE_AUDIT_MIN_LIVE) - int(live.get("n", 0)))
        need_baseline = max(0, max(1, LIVE_AUDIT_MIN_BASELINE) - int(baseline.get("n", 0)))
        lines.append(
            f"Решение: пока ничего не менять; нужно ещё live {need_live}, baseline {need_baseline}."
        )
    return "\n".join(lines)


def format_evidence_guard_audit_message(report: Dict[str, Any]) -> str:
    version = int(report.get("guard_version", EVIDENCE_GUARD_VERSION) or 0)
    decisions = int(report.get("decision_count", 0) or 0)
    baseline = report.get("baseline") or {}
    accepted = report.get("accepted") or {}
    blocked = report.get("blocked") or {}
    action = str(report.get("action", "collect_more"))
    reason = str(report.get("reason", "unknown"))
    correctly_blocked = int(blocked.get("sl", 0)) + int(blocked.get("expired", 0))
    missed_winners = int(blocked.get("profit", 0))

    if action == "rollback":
        title = f"↩️ АУДИТ V16.6 · RULE {version}: АВТООТКАТ"
    elif action == "keep":
        title = f"✅ АУДИТ V16.6 · RULE {version}: ФИЛЬТР ОСТАВЛЕН"
    else:
        title = f"📊 АУДИТ V16.6 · RULE {version}: НУЖНО БОЛЬШЕ ДАННЫХ"

    lines = [
        title,
        f"Закрытых решений фильтра: {decisions}",
        f"Правило: Score ≥ {float(report.get('min_score', EVIDENCE_MIN_SCORE)):.0f} · "
        f"Vol1 > x{float(report.get('min_vol1', EVIDENCE_MIN_VOL1)):.2f}",
        "",
        f"ДО · последние {int(baseline.get('n', 0))} live: {_metrics_line(baseline)}",
        f"ПОСЛЕ · отправленные {int(accepted.get('n', 0))}: {_metrics_line(accepted)}",
        f"SHADOW-БЛОК · {int(blocked.get('n', 0))}: {_metrics_line(blocked)}",
        f"Правильно заблокировано: {correctly_blocked} · пропущено TP3+: {missed_winners}",
        "",
        f"Причина решения: {_adaptive_reason_ru(reason)}",
    ]
    if action == "rollback":
        lines.append("Решение: фильтр Score/Vol1 отключён; базовая логика продолжает работать.")
    elif action == "keep":
        lines.append("Решение: фильтр Score/Vol1 продолжает отбирать live-сигналы.")
    else:
        lines.append("Решение: пока ничего не менять; бот продолжит собирать shadow-сравнение.")
    return "\n".join(lines)


def adaptive_report() -> str:
    init_adaptive_db()
    state = get_model_state()
    guard_state = get_evidence_guard_state()

    with _LOCK, _connect() as conn:
        total = conn.execute("SELECT COUNT(*) AS n FROM adaptive_trades").fetchone()["n"]
        wins = conn.execute("SELECT COUNT(*) AS n FROM adaptive_trades WHERE label=1").fetchone()["n"]
        losses = total - wins
        by_strategy = conn.execute(
            """
            SELECT strategy,
                   COUNT(*) AS n,
                   SUM(label) AS wins,
                   AVG(pnl_r) AS expectancy_r
            FROM adaptive_trades
            GROUP BY strategy
            HAVING COUNT(*) >= 3
            ORDER BY expectancy_r DESC, n DESC
            LIMIT 10
            """
        ).fetchall()
        shadow = conn.execute(
            """
            SELECT
                COUNT(*) AS n,
                SUM(CASE WHEN shadow_accepted=1 THEN 1 ELSE 0 END) AS accepted,
                SUM(CASE WHEN shadow_accepted=1 AND label=1 THEN 1 ELSE 0 END) AS accepted_wins
            FROM adaptive_trades
            WHERE shadow_accepted IS NOT NULL
            """
        ).fetchone()
        source_rows = conn.execute(
            """
            SELECT source, COUNT(*) AS n, SUM(label) AS wins, AVG(pnl_r) AS expectancy_r
            FROM adaptive_trades
            GROUP BY source
            ORDER BY source
            """
        ).fetchall()

    wr = wins / total * 100 if total else 0.0
    lines = [
        "🧠 ОТЧЁТ ADAPTIVE LEARNING",
        f"Всего обучающих результатов: {total}",
        f"TP3+: {wins} · SL/expired: {losses} · успех всех: {wr:.1f}%",
        f"Model version: {state.version}",
        f"Active: {state.active}",
        f"Shadow only: {SHADOW_ONLY}",
        f"Threshold: {state.threshold:.3f}",
        f"Train rows: {state.trained_rows} · Validation rows: {state.validation_rows}",
        f"Validation logloss: model {state.model_logloss:.4f} vs base {state.base_logloss:.4f}",
        f"Validation selected WR: {state.validation_selected_wr*100:.1f}%",
        f"Validation coverage: {state.validation_coverage*100:.1f}%",
        f"Validation expectancy: {state.validation_expectancy_r:+.3f}R "
        f"vs baseline {state.validation_baseline_expectancy_r:+.3f}R",
        f"Last candidate: {state.last_candidate_reason}",
        f"Adaptive LIVE-доля: {float(state.deployment_fraction or 0)*100:.0f}%",
        "",
        f"Evidence guard v{guard_state.version}: active={guard_state.active} · "
        f"Score ≥ {guard_state.min_score:.0f} · Vol1 > x{guard_state.min_vol1:.2f}",
        f"Evidence guard reason: {guard_state.disabled_reason or 'running'}",
    ]

    if guard_state.active and EVIDENCE_AUDIT_ENABLED:
        guard_preview = _collect_evidence_guard_metrics(guard_state)
        guard_pending = max(
            0,
            int(guard_preview["decision_count"])
            - int(guard_state.last_audit_decision_count or 0),
        )
        lines.extend(
            [
                f"Evidence audit: {guard_pending}/{max(1, EVIDENCE_AUDIT_EVERY)} "
                f"new decisions (total {int(guard_preview['decision_count'])})",
                f"Guard before: {_metrics_line(guard_preview['baseline'])}",
                f"Guard after live: {_metrics_line(guard_preview['accepted'])}",
                f"Guard blocked shadow: {_metrics_line(guard_preview['blocked'])}",
            ]
        )

    if source_rows:
        lines.append("\nИсточники данных (не смешивать с реальными сделками):")
        for row in source_rows:
            n = int(row["n"] or 0)
            w = int(row["wins"] or 0)
            exp_r = float(row["expectancy_r"] or 0.0)
            source_name = "LIVE · реальные сигналы" if str(row["source"]) == "live" else "SHADOW · виртуальные проверки"
            lines.append(
                f"{source_name}: {w}/{n} TP3+ · успех {w/max(n,1)*100:.1f}% · средний {exp_r:+.3f}R"
            )

    if shadow and shadow["n"]:
        accepted = int(shadow["accepted"] or 0)
        accepted_wins = int(shadow["accepted_wins"] or 0)
        shadow_wr = accepted_wins / accepted * 100 if accepted else 0.0
        lines.append(
            f"Shadow decisions: {int(shadow['n'])} · accepted {accepted} · accepted WR {shadow_wr:.1f}%"
        )

    if by_strategy:
        lines.append("\nStrategies:")
        for row in by_strategy:
            n = int(row["n"])
            w = int(row["wins"] or 0)
            exp_r = float(row["expectancy_r"] or 0.0)
            lines.append(
                f"{row['strategy']}: {w}/{n} wins · WR {w/n*100:.1f}% · expectancy {exp_r:+.3f}R"
            )

    guard_statuses = STATE.get("strategy_guard_status", {}) if isinstance(STATE, dict) else {}
    if guard_statuses:
        lines.append("\nStrategy circuit breaker:")
        for strategy, item in sorted(guard_statuses.items()):
            status = "LIVE allowed" if bool(item.get("accepted", True)) else "SHADOW only"
            lines.append(f"{strategy}: {status}")

    if total < MIN_TRAIN_TRADES:
        lines.append(f"\nWarm-up: need {MIN_TRAIN_TRADES-total} more closed trades before first training.")
    elif not state.active:
        lines.append("\nModel is not allowed to block signals because validation improvement is not yet strong enough.")
    elif LIVE_AUDIT_ENABLED and not SHADOW_ONLY:
        live_preview = _collect_live_audit_metrics(state)
        pending_decisions = max(
            0,
            int(live_preview["decision_count"])
            - int(state.last_live_audit_decision_count or 0),
        )
        lines.extend(
            [
                f"\nLive audit V{state.version}: "
                f"{pending_decisions}/{max(1, LIVE_AUDIT_EVERY)} new decisions toward next report "
                f"(total {int(live_preview['decision_count'])})",
                f"Before: {_metrics_line(live_preview['baseline'])}",
                f"After live: {_metrics_line(live_preview['live'])}",
                f"Blocked shadow: {_metrics_line(live_preview['blocked'])}",
            ]
        )

    return "\n".join(lines)


def format_source_audit_message(window: int = 25) -> str:
    """Telegram-friendly proof of what was LIVE and what was only simulated."""
    init_adaptive_db()
    size = max(1, min(int(window), 100))
    with _LOCK, _connect() as conn:
        all_rows = conn.execute(
            "SELECT result, pnl_r, source FROM adaptive_trades ORDER BY id ASC"
        ).fetchall()
        recent_rows = conn.execute(
            "SELECT id, symbol, side, strategy, grade, result, pnl_r, source, decision_reason "
            "FROM adaptive_trades ORDER BY id DESC LIMIT ?",
            (size,),
        ).fetchall()

    recent_rows = list(reversed(recent_rows))
    all_sources = _source_breakdown(all_rows)
    recent_sources = _source_breakdown(recent_rows)
    first_id = int(recent_rows[0]["id"]) if recent_rows else 0
    last_id = int(recent_rows[-1]["id"]) if recent_rows else 0
    lines = [
        "🔎 LIVE / SHADOW — ПРОЗРАЧНЫЙ ОТЧЁТ",
        f"Период: результаты {first_id}–{last_id} ({len(recent_rows)} исходов)",
        "",
        f"LIVE · реальные сигналы: {_metrics_line(recent_sources['live'])}",
        f"SHADOW · виртуальные проверки: {_metrics_line(recent_sources['shadow'])}",
        f"ВСЕГО в периоде: {_metrics_line(recent_sources['all'])}",
        "",
        f"Накоплено LIVE: {int(all_sources['live']['n'])} · "
        f"SHADOW: {int(all_sources['shadow']['n'])} · "
        f"ВСЕГО: {int(all_sources['all']['n'])}",
    ]

    control_metrics = control_validation_metrics()
    reclaim_metrics = paper_validation_metrics()
    lines.extend(
        [
            "",
            f"CONTROL V17.3.1: {_metrics_line(control_metrics)}",
            f"FOLLOW-THROUGH PAPER V17.3.2: {_metrics_line(reclaim_metrics)} · "
            f"собрано {int(reclaim_metrics.get('n', 0))}/"
            f"{paper_progress_target(int(reclaim_metrics.get('n', 0) or 0))}",
            watch_audit_summary(),
        ]
    )

    shadow_rows = [row for row in recent_rows if str(row["source"] or "") == "shadow"][-8:]
    if shadow_rows:
        lines.append("\nПоследние SHADOW (видимые виртуальные наблюдения, не LIVE):")
        for row in shadow_rows:
            reason = str(row["decision_reason"] or "shadow observation")
            if len(reason) > 34:
                reason = reason[:31] + "..."
            lines.append(
                f"• {row['symbol']} {row['side']} · {row['result']} · "
                f"{float(row['pnl_r'] or 0):+.2f}R · {reason}"
            )
    else:
        lines.append("\nВ этом периоде SHADOW-исходов не было.")

    lines.append("\nВажно: улучшение считается реальным только после успешного ограниченного LIVE-аудита.")
    return "\n".join(lines)


def recent_adaptive_events(limit: int = 20) -> List[Dict[str, Any]]:
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """
            SELECT created_at, event_type, message, payload_json
            FROM adaptive_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, min(limit, 100)),),
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "created_at": int(row["created_at"]),
                "event_type": row["event_type"],
                "message": row["message"],
                "payload": json.loads(row["payload_json"] or "{}"),
            }
        )
    return out


def build_export_payload() -> Dict[str, Any]:
    """Create a portable JSON backup without Telegram tokens or environment data."""
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        trade_rows = conn.execute(
            "SELECT * FROM adaptive_trades ORDER BY closed_at ASC, id ASC"
        ).fetchall()
        event_rows = conn.execute(
            "SELECT created_at, event_type, message, payload_json "
            "FROM adaptive_events ORDER BY id DESC LIMIT 250"
        ).fetchall()
        version_rows = conn.execute(
            "SELECT version, state_json, status, saved_at, note "
            "FROM adaptive_model_versions ORDER BY version ASC"
        ).fetchall()

    trades: List[Dict[str, Any]] = []
    for row in trade_rows:
        item = dict(row)
        item["features"] = json.loads(item.pop("features_json") or "{}")
        trades.append(item)

    events: List[Dict[str, Any]] = []
    for row in event_rows:
        item = dict(row)
        item["payload"] = json.loads(item.pop("payload_json") or "{}")
        events.append(item)

    model_versions: List[Dict[str, Any]] = []
    for row in version_rows:
        item = dict(row)
        item["state"] = json.loads(item.pop("state_json") or "{}")
        model_versions.append(item)

    state_snapshot = json.loads(json.dumps(globals().get("STATE", {}), ensure_ascii=False))
    # A backup can be triggered while track_shadow_signals is finishing a close.
    # Never serialize an already-recorded outcome as if it were still active.
    for bucket in ("shadow_signals", "active_signals"):
        state_snapshot[bucket] = [
            item for item in state_snapshot.get(bucket, [])
            if not item.get("learning_recorded") and not item.get("stats_recorded")
        ]
    return {
        "export_schema": 7,
        "exported_at": int(time.time()),
        "app": globals().get("APP_NAME", "adaptive futures bot"),
        "deploy_marker": globals().get("DEPLOY_MARKER", "unknown"),
        "state": state_snapshot,
        "adaptive_model": asdict(get_model_state()),
        "evidence_guard": asdict(get_evidence_guard_state()),
        "adaptive_model_versions": model_versions,
        "adaptive_trades": trades,
        "adaptive_events": events,
    }


def build_export_bytes() -> bytes:
    return json.dumps(
        build_export_payload(), ensure_ascii=False, indent=2
    ).encode("utf-8")


# ============================================================
# V13.28 — MARKET DUMP + AERO STYLE SCALPER
# Professional goal:
# Trade only short-lived market situations with immediate edge.
# No trend prediction, no market phase guessing.
#
# Core idea:
# hot coin -> fresh imbalance -> micro pullback/liquidity grab -> EMA/VWAP reclaim/reject
# -> immediate continuation -> compact 5-target exit.
#
# If the trade does not start paying quickly, it is not the setup and gets expired.
# Important: this bot sends signals/alerts. It does not guarantee profit.
# V13.25 fix: adds a trader-pattern gate based on the user examples.
# The bot should not send weak B-class noise: it needs leader/laggard pressure, real range, and a ladder that can realistically move 3-4%.
# ============================================================

APP_NAME = "Professional Adaptive Futures Bot AUTO V20.3.4 SETUP MASTER · PUMP vs PULLBACK INTELLIGENCE"
DEPLOY_MARKER = "V20_3_4_SETUP_MASTER_PUMP_PULLBACK_INTELLIGENCE_2026_09_03"

app = FastAPI(title=APP_NAME)

BINGX_BASE_URL = "https://open-api.bingx.com"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ADMIN_KEY = os.getenv("ADMIN_KEY", "")

# --- V17.1.1 reliable Telegram transport ---
# Diagnostics are replaceable and are not queued. Trade/WATCH/PAPER/result
# messages are retried immediately and then stored in a small ordered outbox.
# The outbox is persisted in STATE_FILE when persistent storage is available.
TELEGRAM_SEND_ATTEMPTS = max(1, int(os.getenv("TELEGRAM_SEND_ATTEMPTS", "3")))
TELEGRAM_CONNECT_TIMEOUT = max(
    1.0, float(os.getenv("TELEGRAM_CONNECT_TIMEOUT", "5"))
)
TELEGRAM_READ_TIMEOUT = max(
    2.0, float(os.getenv("TELEGRAM_READ_TIMEOUT", "15"))
)
TELEGRAM_RETRY_BASE_SECONDS = max(
    0.1, float(os.getenv("TELEGRAM_RETRY_BASE_SECONDS", "0.8"))
)
TELEGRAM_OUTBOX_ENABLED = os.getenv(
    "TELEGRAM_OUTBOX_ENABLED", "true"
).lower() == "true"
TELEGRAM_OUTBOX_FLUSH_SECONDS = max(
    5, int(os.getenv("TELEGRAM_OUTBOX_FLUSH_SECONDS", "15"))
)
TELEGRAM_OUTBOX_BATCH = max(
    1, int(os.getenv("TELEGRAM_OUTBOX_BATCH", "6"))
)
TELEGRAM_OUTBOX_MAX = max(
    10, int(os.getenv("TELEGRAM_OUTBOX_MAX", "120"))
)
TELEGRAM_OUTBOX_MAX_BACKOFF = max(
    30, int(os.getenv("TELEGRAM_OUTBOX_MAX_BACKOFF", "300"))
)

STATE_FILE = os.getenv("STATE_FILE", "bot_state_v13_29_local_stop_dump_scalper.json")
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"

# --- Scan stability ---
AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() == "true"
AUTO_TRACK_ENABLED = os.getenv("AUTO_TRACK_ENABLED", "true").lower() == "true"
AUTO_SCAN_SECONDS = int(os.getenv("AUTO_SCAN_SECONDS", "15"))
AUTO_TRACK_SECONDS = int(os.getenv("AUTO_TRACK_SECONDS", "3"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
API_RETRIES = int(os.getenv("API_RETRIES", "3"))
API_THROTTLE_SECONDS = float(os.getenv("API_THROTTLE_SECONDS", "0.04"))
MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "450"))
MAX_ANALYZE_SYMBOLS = int(os.getenv("MAX_ANALYZE_SYMBOLS", "220"))
HOT_SYMBOLS_TO_ANALYZE = int(os.getenv("HOT_SYMBOLS_TO_ANALYZE", "80"))
MIN_HOT_CANDIDATES = int(os.getenv("MIN_HOT_CANDIDATES", "70"))
HOT_SCAN_WORKERS = max(1, int(os.getenv("HOT_SCAN_WORKERS", "6")))
DEEP_SCAN_WORKERS = max(1, int(os.getenv("DEEP_SCAN_WORKERS", "6")))
DIAG_SECONDS = int(os.getenv("DIAG_SECONDS", "43200"))  # V18.2.2: automatic diagnostics twice per day

# --- Signal limits ---
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "88"))
B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "80"))
MAX_ACTIVE_SIGNALS = int(os.getenv("MAX_ACTIVE_SIGNALS", "1"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "2"))
PAIR_COOLDOWN_SECONDS = int(os.getenv("PAIR_COOLDOWN_SECONDS", "600"))
STRATEGY_COOLDOWN_SECONDS = int(os.getenv("STRATEGY_COOLDOWN_SECONDS", "90"))
# Zero means unlimited. Quality gates, pair cooldown, strategy protection and
# the maximum number of simultaneously active trades remain in force.
MAX_LIVE_SIGNALS_24H = int(os.getenv("MAX_LIVE_SIGNALS_24H", "3"))
MAX_LIVE_SIGNALS_PER_SIDE_24H = int(os.getenv("MAX_LIVE_SIGNALS_PER_SIDE_24H", "2"))
MIN_LIVE_SIGNAL_SPACING_SECONDS = int(os.getenv("MIN_LIVE_SIGNAL_SPACING_SECONDS", "3600"))
MAX_ADAPTIVE_CANARY_LIVE_24H = int(os.getenv("MAX_ADAPTIVE_CANARY_LIVE_24H", "2"))

# --- V17.2 liquidity-first, dual-setup forward validation ---
# The 400-outcome audit and the failed V17.1 WATCH day show two distinct faults:
# raw hot ranking admits distorted microcaps, while one fixed 1.20% impulse gate
# produces almost no confirmed entries.  V17.2 first ranks tape continuity and
# approximate turnover, then watches either a normalized continuation/reclaim
# or a genuine sweep/reversal.  Both lanes are visible PAPER only.
DATA_ENTRY_GATE_ENABLED = os.getenv("DATA_ENTRY_GATE_ENABLED", "true").lower() == "true"
DATA_MIN_VOL1 = float(os.getenv("DATA_MIN_VOL1", "1.00"))
DATA_MIN_RANGE1 = float(os.getenv("DATA_MIN_RANGE1", "1.50"))
DATA_MIN_DIRECTIONAL_3M = float(os.getenv("DATA_MIN_DIRECTIONAL_3M", "0.0080"))

PAPER_VALIDATION_ENABLED = os.getenv("PAPER_VALIDATION_ENABLED", "true").lower() == "true"
PAPER_PULLBACK_CHALLENGER_ENABLED = os.getenv(
    "PAPER_PULLBACK_CHALLENGER_ENABLED", "true"
).lower() == "true"
PAPER_NOTIFY_RESULTS = os.getenv("PAPER_NOTIFY_RESULTS", "true").lower() == "true"
VISIBLE_SHADOW_NOTIFICATIONS = False  # V18.2.1 hard lock: legacy SHADOW/WATCH never sent to Telegram
PAPER_VALIDATION_REASON = "followthrough_paper_v17_3_2"  # legacy lane; disabled
TRADER_STYLE_PAPER_REASON = "active_mover_paper_v20_3_4"
EXHAUSTION_PAPER_REASON = "spike_regime_paper_v20_3_4"

# Hidden forward-audit lanes. They never appear as user-facing trades and never
# count in clean two-strategy stats, but they are tracked to closure so the
# model/strategy guards can measure whether a blocked candidate would have won.
SPIKE_ADAPTIVE_BLOCK_REASON = "spike_adaptive_block_v20_3_4"
SPIKE_STRATEGY_GUARD_REASON = "spike_strategy_guard_v20_3_4"
ACTIVE_MOVER_GUARD_REASON = "active_mover_strategy_guard_v20_3_4"
SPIKE_SETUP_SELECTIVITY_BLOCK_REASON = "spike_setup_selectivity_block_v20_3_4"
ACTIVE_SETUP_SELECTIVITY_BLOCK_REASON = "active_setup_selectivity_block_v20_3_4"

# V20.3: only these two strategies are user-facing.
# Legacy/control/near-miss rows may remain in historical DB for audit, but they
# are not generated as probes and are never shown as Telegram trades/results.
OFFICIAL_USER_PAPER_REASONS = {
    TRADER_STYLE_PAPER_REASON,
    EXHAUSTION_PAPER_REASON,
}
TRACKABLE_SHADOW_REASONS = OFFICIAL_USER_PAPER_REASONS | {
    SPIKE_ADAPTIVE_BLOCK_REASON,
    SPIKE_STRATEGY_GUARD_REASON,
    ACTIVE_MOVER_GUARD_REASON,
    SPIKE_SETUP_SELECTIVITY_BLOCK_REASON,
    ACTIVE_SETUP_SELECTIVITY_BLOCK_REASON,
}
ADAPTIVE_TRAINING_REASONS = {
    EXHAUSTION_PAPER_REASON,
    SPIKE_ADAPTIVE_BLOCK_REASON,
    SPIKE_STRATEGY_GUARD_REASON,
}

def is_adaptive_training_reason(reason: Any) -> bool:
    return str(reason or "") in ADAPTIVE_TRAINING_REASONS


def is_official_user_paper(signal: Dict[str, Any]) -> bool:
    reason = str(
        signal.get("shadow_reason")
        or signal.get("paper_validation_lane")
        or signal.get("decision_reason")
        or ""
    )
    return reason in OFFICIAL_USER_PAPER_REASONS

PAPER_CONTROL_REASON = "direct_measured_control_v17_3_1"
REAL_MONEY_LIVE_REASON = "followthrough_micro_live_v17_3_2"
LEGACY_V17_3_1_PAPER_VALIDATION_REASON = "direct_measured_paper_v17_2_2"
LEGACY_V17_3_1_CONTROL_REASON = "direct_measured_control_v17_2_2"
LEGACY_V17_2_1_PAPER_VALIDATION_REASON = "measured_edge_paper_v17_2_1"
LEGACY_V17_2_1_CONTROL_REASON = "measured_edge_control_v17_2_1"
LEGACY_V17_1_PAPER_VALIDATION_REASON = "moderate_reclaim_v17_1"
LEGACY_V17_1_CONTROL_REASON = "moderate_control_v17_1"
LEGACY_V17_PAPER_VALIDATION_REASON = "evidence_reclaim_v17"
LEGACY_V17_CONTROL_REASON = "evidence_control_v17"
LEGACY_PAPER_VALIDATION_REASON = "quality_forward_v16_9"
PAPER_INSTANT_STRATEGIES = {
    "LONG": "PRO_INSTANT_EDGE_LONG",
    "SHORT": "PRO_INSTANT_EDGE_SHORT",
}
DIRECT_MEASURED_STRATEGIES = {
    "LONG": "PRO_DIRECT_MEASURED_LONG",
    "SHORT": "PRO_DIRECT_MEASURED_SHORT",
}
PAPER_RECLAIM_STRATEGIES = {
    "LONG": "PRO_LIQUIDITY_RECLAIM_LONG",
    "SHORT": "PRO_LIQUIDITY_RECLAIM_SHORT",
}
PAPER_REVERSAL_STRATEGIES = {
    "LONG": "PRO_LIQUIDITY_SWEEP_REVERSAL_LONG",
    "SHORT": "PRO_LIQUIDITY_SWEEP_REVERSAL_SHORT",
}

# Liquidity is estimated from the already downloaded candles: 60-minute quote
# turnover proxy, active/unique prints and anomaly checks.  The relative rank
# inside each scan is the primary filter, so this works across very different
# token prices without pretending candle volume is identical on every market.
V17_2_LIQUIDITY_FIRST_ENABLED = os.getenv(
    "V17_2_LIQUIDITY_FIRST_ENABLED", "true"
).lower() == "true"
V17_2_LIQUIDITY_KEEP_FRACTION = float(
    os.getenv("V17_2_LIQUIDITY_KEEP_FRACTION", "0.60")
)
V17_2_MIN_ACTIVE_CANDLE_FRACTION = float(
    os.getenv("V17_2_MIN_ACTIVE_CANDLE_FRACTION", "0.82")
)
V17_2_MIN_UNIQUE_CLOSE_FRACTION = float(
    os.getenv("V17_2_MIN_UNIQUE_CLOSE_FRACTION", "0.45")
)
V17_2_MAX_CURRENT_VOL_RATIO = float(
    os.getenv("V17_2_MAX_CURRENT_VOL_RATIO", "12.0")
)
V17_2_MAX_CURRENT_RANGE_RATIO = float(
    os.getenv("V17_2_MAX_CURRENT_RANGE_RATIO", "8.0")
)
V17_2_MAX_ATR1_PCT = float(os.getenv("V17_2_MAX_ATR1_PCT", "0.030"))
V17_2_MIN_ABS_MOVE = float(os.getenv("V17_2_MIN_ABS_MOVE", "0.0045"))
V17_2_MAX_ABS_MOVE = float(os.getenv("V17_2_MAX_ABS_MOVE", "0.0300"))
V17_2_MIN_ATR_MOVE = float(os.getenv("V17_2_MIN_ATR_MOVE", "0.90"))
V17_2_MAX_ATR_MOVE = float(os.getenv("V17_2_MAX_ATR_MOVE", "4.50"))
V17_2_CONT_MIN_15M = float(os.getenv("V17_2_CONT_MIN_15M", "0.0030"))
V17_2_CONT_MAX_15M = float(os.getenv("V17_2_CONT_MAX_15M", "0.0600"))
V17_2_REVERSAL_MIN_STRETCH = float(
    os.getenv("V17_2_REVERSAL_MIN_STRETCH", "0.0120")
)
V17_2_REVERSAL_MIN_COUNTER = float(
    os.getenv("V17_2_REVERSAL_MIN_COUNTER", "0.0035")
)
V17_2_MIN_VOL1 = float(os.getenv("V17_2_MIN_VOL1", "0.55"))
V17_2_MAX_VOL1 = float(os.getenv("V17_2_MAX_VOL1", "5.00"))
V17_2_MIN_RANGE1 = float(os.getenv("V17_2_MIN_RANGE1", "0.80"))
V17_2_MIN_VOL5 = float(os.getenv("V17_2_MIN_VOL5", "0.35"))
V17_2_MIN_RANGE5 = float(os.getenv("V17_2_MIN_RANGE5", "0.65"))
V17_2_A_MIN_SCORE = int(os.getenv("V17_2_A_MIN_SCORE", "82"))
V17_2_A_PLUS_MIN_SCORE = int(os.getenv("V17_2_A_PLUS_MIN_SCORE", "90"))
V17_2_TARGET_PAPER_PER_DAY_MIN = int(
    os.getenv("V17_2_TARGET_PAPER_PER_DAY_MIN", "2")
)
V17_2_TARGET_PAPER_PER_DAY_MAX = int(
    os.getenv("V17_2_TARGET_PAPER_PER_DAY_MAX", "6")
)
V17_2_MAX_NEW_WATCH_PER_SCAN = int(
    os.getenv("V17_2_MAX_NEW_WATCH_PER_SCAN", "3")
)
V17_2_MAX_PENDING_WATCH = int(os.getenv("V17_2_MAX_PENDING_WATCH", "12"))

# V17.3 direct measured cohort.  Unlike V17.2.1, this detector does not call
# the legacy instant_edge_setup first.  The fixed boundaries below were frozen
# after a chronological archive audit: 37 proxy matches in all 500 outcomes
# (21 TP3+, 4 SL, 12 expired), with 18 matches in the newest 150-outcome blind
# block (12 TP3+, 2 SL, 4 expired).  The rows came from older implementations,
# so this is a hypothesis, not proof.  Every new match remains visible PAPER
# until a fresh, independent 50-outcome cohort passes the readiness gate.
MEASURED_MIN_VOL1 = float(os.getenv("MEASURED_MIN_VOL1", "0.70"))
MEASURED_MAX_VOL1 = float(os.getenv("MEASURED_MAX_VOL1", "2.20"))
MEASURED_MIN_RANGE1 = float(os.getenv("MEASURED_MIN_RANGE1", "1.20"))
MEASURED_MAX_RANGE1 = float(os.getenv("MEASURED_MAX_RANGE1", "3.00"))
MEASURED_MIN_DIRECTIONAL_3M = float(
    os.getenv("MEASURED_MIN_DIRECTIONAL_3M", "0.0060")
)
MEASURED_MAX_DIRECTIONAL_3M = float(
    os.getenv("MEASURED_MAX_DIRECTIONAL_3M", "0.0500")
)
MEASURED_MIN_DIRECTIONAL_15M = float(
    os.getenv("MEASURED_MIN_DIRECTIONAL_15M", "0.0060")
)
MEASURED_MAX_DIRECTIONAL_15M = float(
    os.getenv("MEASURED_MAX_DIRECTIONAL_15M", "0.0600")
)
MEASURED_MIN_VOL5 = float(os.getenv("MEASURED_MIN_VOL5", "0.25"))
MEASURED_MAX_VOL5 = float(os.getenv("MEASURED_MAX_VOL5", "2.50"))
MEASURED_MIN_RANGE5 = float(os.getenv("MEASURED_MIN_RANGE5", "0.55"))
MEASURED_MAX_30M_CHASE = float(os.getenv("MEASURED_MAX_30M_CHASE", "0.1000"))
MEASURED_MIN_BODY_FRACTION = float(
    os.getenv("MEASURED_MIN_BODY_FRACTION", "0.25")
)
MEASURED_LONG_MIN_CLOSE_LOCATION = float(
    os.getenv("MEASURED_LONG_MIN_CLOSE_LOCATION", "0.55")
)
MEASURED_SHORT_MAX_CLOSE_LOCATION = float(
    os.getenv("MEASURED_SHORT_MAX_CLOSE_LOCATION", "0.45")
)
MEASURED_MIN_LIQUIDITY_RANK = float(
    os.getenv("MEASURED_MIN_LIQUIDITY_RANK", "0.35")
)
MEASURED_MIN_QUOTE_60M = float(os.getenv("MEASURED_MIN_QUOTE_60M", "50000"))
MEASURED_MAX_BOOK_SPREAD_BPS = float(
    os.getenv("MEASURED_MAX_BOOK_SPREAD_BPS", "15.0")
)
MEASURED_MIN_BOOK_DEPTH_USDT = float(
    os.getenv("MEASURED_MIN_BOOK_DEPTH_USDT", "500")
)

# Real-money alerts are two-key guarded: the environment flag alone is not
# enough.  The new forward cohort must also pass all readiness tests.  This bot
# remains a signal bot and never places an exchange order.
REAL_MONEY_SIGNALS = os.getenv("REAL_MONEY_SIGNALS", "false").lower() == "true"
REAL_MONEY_MIN_FORWARD = int(os.getenv("REAL_MONEY_MIN_FORWARD", "50"))
REAL_MONEY_MIN_UNIQUE_SYMBOLS = int(
    os.getenv("REAL_MONEY_MIN_UNIQUE_SYMBOLS", "25")
)
REAL_MONEY_MIN_SUCCESS_RATE = float(
    os.getenv("REAL_MONEY_MIN_SUCCESS_RATE", "0.50")
)
REAL_MONEY_MIN_EXPECTANCY_R = float(
    os.getenv("REAL_MONEY_MIN_EXPECTANCY_R", "0.20")
)
REAL_MONEY_RECENT_WINDOW = int(os.getenv("REAL_MONEY_RECENT_WINDOW", "25"))
REAL_MONEY_MIN_SIDE_FORWARD = int(
    os.getenv("REAL_MONEY_MIN_SIDE_FORWARD", "15")
)
REAL_MONEY_SIDE_RECENT_WINDOW = int(
    os.getenv("REAL_MONEY_SIDE_RECENT_WINDOW", "10")
)
REAL_MONEY_MIN_SIDE_EXPECTANCY_R = float(
    os.getenv("REAL_MONEY_MIN_SIDE_EXPECTANCY_R", "0.10")
)
MICRO_LIVE_RISK_MULT = float(os.getenv("MICRO_LIVE_RISK_MULT", "0.02"))
MICRO_LIVE_GUARD_MIN_OUTCOMES = int(
    os.getenv("MICRO_LIVE_GUARD_MIN_OUTCOMES", "5")
)
MICRO_LIVE_GUARD_WINDOW = int(os.getenv("MICRO_LIVE_GUARD_WINDOW", "10"))
MICRO_LIVE_MAX_NONPROFIT_STREAK = int(
    os.getenv("MICRO_LIVE_MAX_NONPROFIT_STREAK", "3")
)
MICRO_LIVE_MAX_DRAWDOWN_R = float(
    os.getenv("MICRO_LIVE_MAX_DRAWDOWN_R", "3.0")
)
# The detector still needs a real directional impulse, but entry is no longer
# allowed at the first hot print.  It must survive the retest state machine.
PAPER_EDGE_MIN = float(os.getenv("PAPER_EDGE_MIN", "0.0120"))
PAPER_EDGE_MAX = float(os.getenv("PAPER_EDGE_MAX", "0.0300"))
PAPER_MAX_DIRECTIONAL_15M = float(
    os.getenv("PAPER_MAX_DIRECTIONAL_15M", "0.0400")
)
PAPER_MAX_VOL1 = float(os.getenv("PAPER_MAX_VOL1", "2.50"))
PAPER_LONG_MIN_DIRECTIONAL_15M = float(
    os.getenv("PAPER_LONG_MIN_DIRECTIONAL_15M", "0.0115")
)
PAPER_LONG_MIN_VOL5 = float(os.getenv("PAPER_LONG_MIN_VOL5", "0.40"))
PAPER_LONG_MAX_VOL5 = float(os.getenv("PAPER_LONG_MAX_VOL5", "1.20"))
PAPER_SHORT_MIN_DIRECTIONAL_3M = float(
    os.getenv("PAPER_SHORT_MIN_DIRECTIONAL_3M", "0.0165")
)
PAPER_SHORT_MIN_DIRECTIONAL_15M = float(
    os.getenv("PAPER_SHORT_MIN_DIRECTIONAL_15M", "0.0280")
)
PAPER_PILOT_REQUIRED_OUTCOMES = int(
    os.getenv("PAPER_PILOT_REQUIRED_OUTCOMES", "25")
)
PAPER_REVIEW_REQUIRED_OUTCOMES = int(
    os.getenv("PAPER_REVIEW_REQUIRED_OUTCOMES", "50")
)
# Twenty-five trades are a manual quality review, not enough evidence to let a
# model touch signals.  Promotion stays frozen until at least 50 unchanged,
# registered V17.2 PAPER outcomes and the ordinary independent guards pass.
PAPER_LANE_REQUIRED_OUTCOMES = int(os.getenv("PAPER_LANE_REQUIRED_OUTCOMES", "50"))
PAPER_MIN_UNIQUE_SYMBOLS_PILOT = int(
    os.getenv("PAPER_MIN_UNIQUE_SYMBOLS_PILOT", "7")
)
PAPER_PILOT_MIN_EXPECTANCY_R = float(
    os.getenv("PAPER_PILOT_MIN_EXPECTANCY_R", "0.15")
)
PAPER_SYMBOL_COOLDOWN_SECONDS = int(
    os.getenv("PAPER_SYMBOL_COOLDOWN_SECONDS", "21600")
)
# Pullback/reclaim observation window.  The six-minute trade time-stop begins
# only after the confirmed PAPER entry, not while the setup is on WATCH.
PAPER_RECLAIM_MIN_SECONDS = int(os.getenv("PAPER_RECLAIM_MIN_SECONDS", "20"))
PAPER_RECLAIM_MAX_SECONDS = int(os.getenv("PAPER_RECLAIM_MAX_SECONDS", "300"))
PAPER_RECLAIM_MIN_PULLBACK = float(os.getenv("PAPER_RECLAIM_MIN_PULLBACK", "0.0015"))
PAPER_RECLAIM_MAX_PULLBACK = float(os.getenv("PAPER_RECLAIM_MAX_PULLBACK", "0.0180"))
PAPER_RECLAIM_MIN_IMPULSE_FRACTION = float(
    os.getenv("PAPER_RECLAIM_MIN_IMPULSE_FRACTION", "0.12")
)
PAPER_RECLAIM_MAX_IMPULSE_FRACTION = float(
    os.getenv("PAPER_RECLAIM_MAX_IMPULSE_FRACTION", "0.60")
)
PAPER_RECLAIM_MIN_RECOVERY = float(os.getenv("PAPER_RECLAIM_MIN_RECOVERY", "0.42"))
PAPER_RECLAIM_ENTRY_FLOOR = float(os.getenv("PAPER_RECLAIM_ENTRY_FLOOR", "-0.0005"))
PAPER_RECLAIM_MAX_CHASE = float(os.getenv("PAPER_RECLAIM_MAX_CHASE", "0.0035"))
PAPER_RECLAIM_MIN_3M = float(os.getenv("PAPER_RECLAIM_MIN_3M", "0.0005"))
PAPER_RECLAIM_MIN_VOL1 = float(os.getenv("PAPER_RECLAIM_MIN_VOL1", "0.45"))
PAPER_RECLAIM_MIN_RANGE1 = float(os.getenv("PAPER_RECLAIM_MIN_RANGE1", "0.65"))
PAPER_PENDING_MONITOR_SECONDS = int(
    os.getenv("PAPER_PENDING_MONITOR_SECONDS", "10")
)
PAPER_CONFIRM_LATCH_SECONDS = int(
    os.getenv("PAPER_CONFIRM_LATCH_SECONDS", "60")
)
PAPER_CHECKPOINT_EVERY = int(os.getenv("PAPER_CHECKPOINT_EVERY", "25"))
PAPER_BREAKEVEN_AFTER_TP1 = os.getenv(
    "PAPER_BREAKEVEN_AFTER_TP1", "true"
).lower() == "true"
# Safety invariant for this build.  A Render environment variable cannot turn
# an unvalidated V17.2 setup into a real/LIVE signal by accident.
PRO_QUALITY_FORWARD_ENABLED = False

# Weak strategies are paused only in LIVE. They continue producing SHADOW
# outcomes and automatically recover when the newest rolling evidence improves.
STRATEGY_CIRCUIT_BREAKER_ENABLED = os.getenv("STRATEGY_CIRCUIT_BREAKER_ENABLED", "true").lower() == "true"
STRATEGY_GUARD_WINDOW = int(os.getenv("STRATEGY_GUARD_WINDOW", "10"))
STRATEGY_GUARD_MIN_ROWS = int(os.getenv("STRATEGY_GUARD_MIN_ROWS", "8"))
STRATEGY_GUARD_MAX_EXPECTANCY_R = float(os.getenv("STRATEGY_GUARD_MAX_EXPECTANCY_R", "-0.20"))
STRATEGY_GUARD_MAX_SUCCESS_RATE = float(os.getenv("STRATEGY_GUARD_MAX_SUCCESS_RATE", "0.20"))
STRATEGY_RECOVERY_WINDOW = int(os.getenv("STRATEGY_RECOVERY_WINDOW", "5"))
STRATEGY_RECOVERY_MIN_PROFITS = int(os.getenv("STRATEGY_RECOVERY_MIN_PROFITS", "2"))
STRATEGY_RECOVERY_MIN_EXPECTANCY_R = float(os.getenv("STRATEGY_RECOVERY_MIN_EXPECTANCY_R", "0.05"))
# MARKET_DUMP_SHORT was persistently negative through outcome 150. It remains
# SHADOW until the newest recovery window meets the ordinary recovery rule.
MARKET_DUMP_SHORT_REQUALIFY = os.getenv(
    "MARKET_DUMP_SHORT_REQUALIFY", "true"
).lower() == "true"

# A detected setup is not sent immediately. It must keep its direction for a
# short observation window, remain near the original entry and confirm on 1m.
PRE_LIVE_CONFIRMATION_ENABLED = os.getenv("PRE_LIVE_CONFIRMATION_ENABLED", "true").lower() == "true"
PRE_LIVE_MIN_SECONDS = int(os.getenv("PRE_LIVE_MIN_SECONDS", "10"))
PRE_LIVE_MAX_SECONDS = int(os.getenv("PRE_LIVE_MAX_SECONDS", "120"))
PRE_LIVE_MIN_DIRECTIONAL_MOVE = float(os.getenv("PRE_LIVE_MIN_DIRECTIONAL_MOVE", "0.0008"))
PRE_LIVE_MAX_CHASE_MOVE = float(os.getenv("PRE_LIVE_MAX_CHASE_MOVE", "0.0040"))
PRE_LIVE_MAX_ADVERSE_MOVE = float(os.getenv("PRE_LIVE_MAX_ADVERSE_MOVE", "0.0035"))
PRE_LIVE_MIN_VOL1 = float(os.getenv("PRE_LIVE_MIN_VOL1", "0.80"))
PRE_LIVE_MIN_RANGE1 = float(os.getenv("PRE_LIVE_MIN_RANGE1", "1.00"))
PRE_LIVE_CLOSE_LONG = float(os.getenv("PRE_LIVE_CLOSE_LONG", "0.58"))
PRE_LIVE_CLOSE_SHORT = float(os.getenv("PRE_LIVE_CLOSE_SHORT", "0.42"))
PRE_LIVE_MAX_ACTIVE = int(os.getenv("PRE_LIVE_MAX_ACTIVE", "28"))

# A symbol that produces three consecutive non-profit outcomes is quarantined
# for 12 hours. Shadow observation continues and any TP3+ clears the quarantine.
SYMBOL_QUARANTINE_ENABLED = os.getenv("SYMBOL_QUARANTINE_ENABLED", "true").lower() == "true"
SYMBOL_FAIL_LIMIT = int(os.getenv("SYMBOL_FAIL_LIMIT", "3"))
SYMBOL_QUARANTINE_SECONDS = int(os.getenv("SYMBOL_QUARANTINE_SECONDS", "43200"))

# Shadow candidates are hypothetical trades: they are never sent as live signals,
# but their outcomes let the challenger learn from accepted and rejected examples.
SHADOW_TRACKING_ENABLED = os.getenv("SHADOW_TRACKING_ENABLED", "true").lower() == "true"
SHADOW_MAX_ACTIVE = int(os.getenv("SHADOW_MAX_ACTIVE", "40"))
SHADOW_PAPER_RESERVED_SLOTS = int(os.getenv("SHADOW_PAPER_RESERVED_SLOTS", "6"))
SHADOW_PER_SCAN = int(os.getenv("SHADOW_PER_SCAN", "6"))
SHADOW_COOLDOWN_SECONDS = int(os.getenv("SHADOW_COOLDOWN_SECONDS", "600"))

# V16.7: extra near-miss probes remain disabled. Forward learning now uses only
# explicitly confirmed and visible PAPER candidates plus genuine LIVE results.
# The implementation remains available for controlled experiments via env.
SHADOW_PROBE_ENABLED = False  # V18.2.1 hard lock: no near_miss_probe generation
SHADOW_PROBE_PER_SCAN = int(os.getenv("SHADOW_PROBE_PER_SCAN", "3"))
SHADOW_PROBE_MAX_ACTIVE = int(os.getenv("SHADOW_PROBE_MAX_ACTIVE", "6"))
SHADOW_PROBE_COOLDOWN_SECONDS = int(os.getenv("SHADOW_PROBE_COOLDOWN_SECONDS", "1800"))
SHADOW_PROBE_MIN_DIRECTIONAL_3M = float(os.getenv("SHADOW_PROBE_MIN_DIRECTIONAL_3M", "0.0025"))
SHADOW_PROBE_MIN_VOL1 = float(os.getenv("SHADOW_PROBE_MIN_VOL1", "0.25"))
SHADOW_PROBE_MIN_RANGE1 = float(os.getenv("SHADOW_PROBE_MIN_RANGE1", "0.50"))
SHADOW_PROBE_MIN_EVIDENCE_SCORE = float(os.getenv("SHADOW_PROBE_MIN_EVIDENCE_SCORE", "2.20"))
LADDER_FOLLOWUP_MINUTES = int(os.getenv("LADDER_FOLLOWUP_MINUTES", "90"))
AUTO_TELEGRAM_BACKUP = os.getenv("AUTO_TELEGRAM_BACKUP", "true").lower() == "true"
AUTO_BACKUP_EVERY_CLOSED = int(os.getenv("AUTO_BACKUP_EVERY_CLOSED", "25"))



# --- V18 ACTIVE-MOVER experimental PAPER lane ---
# Visual identity:
# ⚡ = fast follow-through (old V17.3.2 logic)
# 🧲 = active-mover / trader-style (slower realization allowed)
#
# This lane does NOT imitate averaging. It tracks a separate PAPER stop and a
# longer time horizon so slower but valid moves are not automatically classified
# as expired after six minutes.
ACTIVE_MOVER_ENABLED = os.getenv("ACTIVE_MOVER_ENABLED", "true").lower() == "true"
ACTIVE_MOVER_MIN_ABS_3M = float(os.getenv("ACTIVE_MOVER_MIN_ABS_3M", "0.0025"))
ACTIVE_MOVER_MIN_ABS_15M = float(os.getenv("ACTIVE_MOVER_MIN_ABS_15M", "0.0060"))
ACTIVE_MOVER_MIN_RECENT_RANGE = float(os.getenv("ACTIVE_MOVER_MIN_RECENT_RANGE", "0.0180"))
ACTIVE_MOVER_MIN_VOL1 = float(os.getenv("ACTIVE_MOVER_MIN_VOL1", "0.35"))
ACTIVE_MOVER_MIN_RANGE1 = float(os.getenv("ACTIVE_MOVER_MIN_RANGE1", "0.70"))
ACTIVE_MOVER_MIN_VOL5 = float(os.getenv("ACTIVE_MOVER_MIN_VOL5", "0.18"))
ACTIVE_MOVER_MIN_RANGE5 = float(os.getenv("ACTIVE_MOVER_MIN_RANGE5", "0.55"))
ACTIVE_MOVER_MIN_BODY = float(os.getenv("ACTIVE_MOVER_MIN_BODY", "0.28"))
ACTIVE_MOVER_MIN_BOOK_DEPTH_USDT = float(os.getenv("ACTIVE_MOVER_MIN_BOOK_DEPTH_USDT", "500"))
ACTIVE_MOVER_MAX_BOOK_SPREAD_BPS = float(os.getenv("ACTIVE_MOVER_MAX_BOOK_SPREAD_BPS", "18.0"))
ACTIVE_MOVER_MIN_QUOTE_60M = float(os.getenv("ACTIVE_MOVER_MIN_QUOTE_60M", "50000"))
ACTIVE_MOVER_SOFT_EXPIRE_MINUTES = int(os.getenv("ACTIVE_MOVER_SOFT_EXPIRE_MINUTES", "30"))
ACTIVE_MOVER_HARD_EXPIRE_MINUTES = int(os.getenv("ACTIVE_MOVER_HARD_EXPIRE_MINUTES", "120"))
ACTIVE_MOVER_MIN_PROGRESS_AT_SOFT = float(os.getenv("ACTIVE_MOVER_MIN_PROGRESS_AT_SOFT", "0.15"))
ACTIVE_MOVER_MAX_ACTIVE = int(os.getenv("ACTIVE_MOVER_MAX_ACTIVE", "5"))

# V18.1: HOT -> WATCH -> TRIGGER. Detection and entry are deliberately separated.
ACTIVE_WATCH_MIN_SECONDS = int(os.getenv("ACTIVE_WATCH_MIN_SECONDS", "20"))
ACTIVE_WATCH_MAX_SECONDS = int(os.getenv("ACTIVE_WATCH_MAX_SECONDS", "600"))
ACTIVE_WATCH_MAX_CANDIDATES = int(os.getenv("ACTIVE_WATCH_MAX_CANDIDATES", "16"))
ACTIVE_WATCH_MIN_PULLBACK = float(os.getenv("ACTIVE_WATCH_MIN_PULLBACK", "0.0018"))
ACTIVE_WATCH_MAX_PULLBACK = float(os.getenv("ACTIVE_WATCH_MAX_PULLBACK", "0.0120"))
ACTIVE_WATCH_RECLAIM_FRACTION = float(os.getenv("ACTIVE_WATCH_RECLAIM_FRACTION", "0.45"))
ACTIVE_WATCH_MIN_REACCEL_1M = float(os.getenv("ACTIVE_WATCH_MIN_REACCEL_1M", "0.0008"))
ACTIVE_WATCH_MAX_CHASE = float(os.getenv("ACTIVE_WATCH_MAX_CHASE", "0.0060"))
ACTIVE_WATCH_MIN_VOL1 = float(os.getenv("ACTIVE_WATCH_MIN_VOL1", "0.55"))
ACTIVE_WATCH_MIN_RANGE1 = float(os.getenv("ACTIVE_WATCH_MIN_RANGE1", "0.75"))

# Trader-style ladder from the supplied examples: roughly 0.8 / 1.4 / 2 / 3 / 4%.
ACTIVE_TP1_MOVE = float(os.getenv("ACTIVE_TP1_MOVE", "0.0080"))
ACTIVE_TP2_MOVE = float(os.getenv("ACTIVE_TP2_MOVE", "0.0140"))
ACTIVE_TP3_MOVE = float(os.getenv("ACTIVE_TP3_MOVE", "0.0200"))
ACTIVE_TP4_MOVE = float(os.getenv("ACTIVE_TP4_MOVE", "0.0300"))
ACTIVE_TP5_MOVE = float(os.getenv("ACTIVE_TP5_MOVE", "0.0400"))
ACTIVE_MIN_SL_MOVE = float(os.getenv("ACTIVE_MIN_SL_MOVE", "0.0120"))
ACTIVE_MAX_SL_MOVE = float(os.getenv("ACTIVE_MAX_SL_MOVE", "0.0240"))


# --- V20.0 PROFESSIONAL SPIKE REGIME ENGINE ---
# The radar is intentionally stricter than V19.0. It is designed to find a
# visually obvious "stick": abnormal displacement + candle expansion + volume
# acceleration + distance from the recent base. Then it decides whether to
# TRADE WITH the squeeze or FADE an exhausted squeeze.
#
# Four possible official setups:
#   UP squeeze continuation   -> LONG
#   UP squeeze exhaustion     -> SHORT
#   DOWN squeeze continuation -> SHORT
#   DOWN squeeze exhaustion   -> LONG
SQUEEZE_4H_ENABLED = os.getenv("SPIKE_REGIME_ENABLED", "true").lower() == "true"

# Minimum raw displacement by timeframe. Relative/ATR evidence cannot fully
# replace raw displacement anymore; this prevents ordinary noisy candles from
# being called a "spike".
SPIKE_MIN_MOVE_5M = float(os.getenv("SPIKE_MIN_MOVE_5M", "0.018"))
SPIKE_MIN_MOVE_15M = float(os.getenv("SPIKE_MIN_MOVE_15M", "0.032"))
SPIKE_MIN_MOVE_1H = float(os.getenv("SPIKE_MIN_MOVE_1H", "0.050"))
SPIKE_MIN_MOVE_4H = float(os.getenv("SPIKE_MIN_MOVE_4H", "0.075"))

SPIKE_MIN_ATR_MULT = float(os.getenv("SPIKE_MIN_ATR_MULT", "2.20"))
SPIKE_MIN_VOLUME_PACE = float(os.getenv("SPIKE_MIN_VOLUME_PACE", "1.80"))
SPIKE_MIN_BODY_ACCEL = float(os.getenv("SPIKE_MIN_BODY_ACCEL", "2.80"))
SPIKE_MIN_BODY_FRACTION = float(os.getenv("SPIKE_MIN_BODY_FRACTION", "0.52"))
SPIKE_CLOSE_EXTREME = float(os.getenv("SPIKE_CLOSE_EXTREME", "0.68"))
SPIKE_MIN_BASE_DISTANCE = float(os.getenv("SPIKE_MIN_BASE_DISTANCE", "0.018"))
SPIKE_MIN_SCORE = float(os.getenv("SPIKE_MIN_SCORE", "76.0"))
SPIKE_SINGLE_TF_MIN_SCORE = float(os.getenv("SPIKE_SINGLE_TF_MIN_SCORE", "82.0"))
SPIKE_SINGLE_TF_MIN_VOLUME = float(os.getenv("SPIKE_SINGLE_TF_MIN_VOLUME", "1.60"))
SPIKE_SINGLE_TF_MOVE_MULT = float(os.getenv("SPIKE_SINGLE_TF_MOVE_MULT", "1.35"))
SPIKE_ENTRY_MAX_3M_MOVE = float(os.getenv("SPIKE_ENTRY_MAX_3M_MOVE", "0.050"))
SPIKE_ENTRY_MAX_1M_MOVE = float(os.getenv("SPIKE_ENTRY_MAX_1M_MOVE", "0.025"))
SPIKE_CONT_REQUIRE_15M_ALIGNMENT = os.getenv("SPIKE_CONT_REQUIRE_15M_ALIGNMENT", "true").lower() == "true"
SPIKE_CONT_SCORE_LONG = int(os.getenv("SPIKE_CONT_SCORE_LONG", "7"))
SPIKE_CONT_SCORE_SHORT = int(os.getenv("SPIKE_CONT_SCORE_SHORT", "7"))
SPIKE_FADE_SCORE_LONG = int(os.getenv("SPIKE_FADE_SCORE_LONG", "7"))
SPIKE_FADE_SCORE_SHORT = int(os.getenv("SPIKE_FADE_SCORE_SHORT", "6"))

# V20.2 structural fixes from the first 50 V20.1 outcomes.
# These are intentionally interpretable gates, not model-fit coefficients.
SPIKE_REENTRY_COOLDOWN_SECONDS = int(os.getenv("SPIKE_REENTRY_COOLDOWN_SECONDS", "10800"))
SPIKE_EXHAUST_MAX_VOL1 = float(os.getenv("SPIKE_EXHAUST_MAX_VOL1", "0.85"))
SPIKE_EXHAUST_MIN_NO_EXTREME_SECONDS = int(os.getenv("SPIKE_EXHAUST_MIN_NO_EXTREME_SECONDS", "45"))
SPIKE_EXHAUST_MIN_REVERSAL_1M = float(os.getenv("SPIKE_EXHAUST_MIN_REVERSAL_1M", "0.0002"))
SPIKE_EXHAUST_MIN_REVERSAL_3M = float(os.getenv("SPIKE_EXHAUST_MIN_REVERSAL_3M", "0.0015"))
SPIKE_EXHAUST_MIN_TF_AGREEMENT = int(os.getenv("SPIKE_EXHAUST_MIN_TF_AGREEMENT", "2"))
SPIKE_EXTREME_SINGLE_MIN_IMPULSE = float(os.getenv("SPIKE_EXTREME_SINGLE_MIN_IMPULSE", "0.10"))
SPIKE_EXTREME_SINGLE_MIN_BASE_DISTANCE = float(os.getenv("SPIKE_EXTREME_SINGLE_MIN_BASE_DISTANCE", "0.08"))
SPIKE_CONT_MIN_REVERSAL_1M = float(os.getenv("SPIKE_CONT_MIN_REVERSAL_1M", "0.0001"))
SPIKE_CONT_MIN_REVERSAL_3M = float(os.getenv("SPIKE_CONT_MIN_REVERSAL_3M", "0.0010"))

# V20.3.4 professional candle/squeeze/setup intelligence.
PRO_SETUP_MIN_SCORE = float(os.getenv("PRO_SETUP_MIN_SCORE", "72"))
PRO_CONT_MIN_INTEL_SCORE = float(os.getenv("PRO_CONT_MIN_INTEL_SCORE", "58"))
PRO_EXHAUST_MIN_INTEL_SCORE = float(os.getenv("PRO_EXHAUST_MIN_INTEL_SCORE", "58"))
PRO_MAX_CLIMAX_FOR_CONT = float(os.getenv("PRO_MAX_CLIMAX_FOR_CONT", "78"))
PRO_MIN_OVERHEAT_FOR_FADE = float(os.getenv("PRO_MIN_OVERHEAT_FOR_FADE", "52"))
PRO_CONFLICT_MARGIN = float(os.getenv("PRO_CONFLICT_MARGIN", "8"))
PRO_MAX_DISTANCE_ATR_CONT = float(os.getenv("PRO_MAX_DISTANCE_ATR_CONT", "4.5"))

# V20.3.4 SETUP MASTER: interpretable "continuation vs pullback" decision layer.
# These are intentionally selective. Rejected candidates remain hidden audit rows.
SETUP_CONT_EDGE_MIN = float(os.getenv("SETUP_CONT_EDGE_MIN", "66"))
SETUP_FADE_EDGE_MIN = float(os.getenv("SETUP_FADE_EDGE_MIN", "66"))
SETUP_EDGE_MARGIN_MIN = float(os.getenv("SETUP_EDGE_MARGIN_MIN", "10"))
SETUP_FALSE_BREAKOUT_MAX_CONT = float(os.getenv("SETUP_FALSE_BREAKOUT_MAX_CONT", "55"))
SETUP_STRUCTURE_MIN_CONT = float(os.getenv("SETUP_STRUCTURE_MIN_CONT", "48"))
SETUP_MAX_PULLBACK_RISK_CONT = float(os.getenv("SETUP_MAX_PULLBACK_RISK_CONT", "64"))
SETUP_MIN_PULLBACK_RISK_FADE = float(os.getenv("SETUP_MIN_PULLBACK_RISK_FADE", "62"))

SPIKE_WATCH_MAX = int(os.getenv("SPIKE_WATCH_MAX", "24"))
SPIKE_WATCH_SECONDS = int(os.getenv("SPIKE_WATCH_SECONDS", "7200"))

# Backward-compatible aliases used by the state/watch container.
SQUEEZE_4H_WATCH_MAX = SPIKE_WATCH_MAX
SQUEEZE_4H_WATCH_SECONDS = SPIKE_WATCH_SECONDS

# Regime classifier.
SPIKE_MIN_WATCH_SECONDS = int(os.getenv("SPIKE_MIN_WATCH_SECONDS", "20"))
SPIKE_CONT_MIN_PULLBACK = float(os.getenv("SPIKE_CONT_MIN_PULLBACK", "0.0025"))
SPIKE_CONT_MAX_PULLBACK = float(os.getenv("SPIKE_CONT_MAX_PULLBACK", "0.0150"))
SPIKE_CONT_RECOVERY_FRACTION = float(os.getenv("SPIKE_CONT_RECOVERY_FRACTION", "0.55"))
SPIKE_CONT_MIN_SCORE = int(os.getenv("SPIKE_CONT_MIN_SCORE", "6"))
SPIKE_FADE_MIN_RETRACE = float(os.getenv("SPIKE_FADE_MIN_RETRACE", "0.0060"))
SPIKE_FADE_MAX_RETRACE = float(os.getenv("SPIKE_FADE_MAX_RETRACE", "0.0450"))
SPIKE_FADE_MIN_SCORE = int(os.getenv("SPIKE_FADE_MIN_SCORE", "6"))
SPIKE_FAILED_RECLAIM_TOL = float(os.getenv("SPIKE_FAILED_RECLAIM_TOL", "0.0018"))
SPIKE_BREAK_CONFIRM = float(os.getenv("SPIKE_BREAK_CONFIRM", "0.0010"))
SPIKE_MIN_VOL1 = float(os.getenv("SPIKE_MIN_VOL1", "0.35"))

# Legacy names retained because execution/result engine uses them.
EXHAUST_MIN_WATCH_SECONDS = SPIKE_MIN_WATCH_SECONDS
EXHAUST_MIN_RETRACE = SPIKE_FADE_MIN_RETRACE
EXHAUST_MAX_RETRACE_AT_ENTRY = SPIKE_FADE_MAX_RETRACE
EXHAUST_MIN_REVERSAL_1M = float(os.getenv("SPIKE_MIN_REVERSAL_1M", "0.0008"))
EXHAUST_MIN_REVERSAL_3M = float(os.getenv("SPIKE_MIN_REVERSAL_3M", "0.0018"))
EXHAUST_MIN_CONFIRMATIONS = SPIKE_FADE_MIN_SCORE
EXHAUST_RUNAWAY_RECENT_SECONDS = int(os.getenv("SPIKE_RUNAWAY_RECENT_SECONDS", "18"))
EXHAUST_RUNAWAY_MIN_1M = float(os.getenv("SPIKE_RUNAWAY_MIN_1M", "0.0012"))
EXHAUST_MIN_VOL1 = SPIKE_MIN_VOL1

# Execution quality.
EXHAUST_MAX_SPREAD_BPS = float(os.getenv("SPIKE_MAX_SPREAD_BPS", "18.0"))
EXHAUST_MIN_DEPTH_USDT = float(os.getenv("SPIKE_MIN_DEPTH_USDT", "500"))
EXHAUST_MIN_QUOTE_60M = float(os.getenv("SPIKE_MIN_QUOTE_60M", "50000"))
EXHAUST_MIN_LIQUIDITY_RANK = float(os.getenv("SPIKE_MIN_LIQUIDITY_RANK", "0.20"))

# Targets are underlying-price moves. This remains PAPER-only.
EXHAUST_TP1_MOVE = float(os.getenv("SPIKE_TP1_MOVE", "0.0080"))
EXHAUST_TP2_MOVE = float(os.getenv("SPIKE_TP2_MOVE", "0.0140"))
EXHAUST_TP3_MOVE = float(os.getenv("SPIKE_TP3_MOVE", "0.0220"))
EXHAUST_TP4_MOVE = float(os.getenv("SPIKE_TP4_MOVE", "0.0340"))
EXHAUST_TP5_MOVE = float(os.getenv("SPIKE_TP5_MOVE", "0.0500"))
EXHAUST_MIN_SL_MOVE = float(os.getenv("SPIKE_MIN_SL_MOVE", "0.0090"))
EXHAUST_MAX_SL_MOVE = float(os.getenv("SPIKE_MAX_SL_MOVE", "0.0220"))
EXHAUST_SOFT_EXPIRE_MINUTES = int(os.getenv("SPIKE_SOFT_EXPIRE_MINUTES", "40"))
EXHAUST_HARD_EXPIRE_MINUTES = int(os.getenv("SPIKE_HARD_EXPIRE_MINUTES", "180"))
EXHAUST_MIN_PROGRESS_AT_SOFT = float(os.getenv("SPIKE_MIN_PROGRESS_AT_SOFT", "0.12"))

# --- V17.3.2 forward follow-through selector ---
# First unchanged V17.3.1 PAPER cohort: 25 outcomes = 2 TP3+ / 8 SL / 15 expired.
# The new selector keeps the broad V17.3.1 detector as a paired CONTROL and
# admits a much smaller PAPER subset only when the impulse is still accelerating.
# SHORT remains visible CONTROL/SHADOW by default because the first forward
# cohort produced 0 TP3+ / 5 SL / 5 expired on SHORT.
V17_3_2_SELECTOR_ENABLED = os.getenv(
    "V17_3_2_SELECTOR_ENABLED", "true"
).lower() == "true"
V17_3_2_SHORT_PAPER_ENABLED = os.getenv(
    "V17_3_2_SHORT_PAPER_ENABLED", "false"
).lower() == "true"
V17_3_2_MIN_DIRECTIONAL_3M = float(
    os.getenv("V17_3_2_MIN_DIRECTIONAL_3M", "0.0090")
)
V17_3_2_MIN_DIRECTIONAL_15M = float(
    os.getenv("V17_3_2_MIN_DIRECTIONAL_15M", "0.0100")
)
V17_3_2_MIN_DIRECTIONAL_30M = float(
    os.getenv("V17_3_2_MIN_DIRECTIONAL_30M", "-0.0025")
)
V17_3_2_MIN_VOL1 = float(os.getenv("V17_3_2_MIN_VOL1", "0.75"))
V17_3_2_MIN_VOL5 = float(os.getenv("V17_3_2_MIN_VOL5", "0.25"))
V17_3_2_MIN_RANGE1 = float(os.getenv("V17_3_2_MIN_RANGE1", "1.30"))
V17_3_2_MIN_RANGE5 = float(os.getenv("V17_3_2_MIN_RANGE5", "0.65"))
V17_3_2_MIN_PACE_RATIO = float(
    os.getenv("V17_3_2_MIN_PACE_RATIO", "0.90")
)
V17_3_2_EXCEPTIONAL_3M = float(
    os.getenv("V17_3_2_EXCEPTIONAL_3M", "0.0220")
)
V17_3_2_EXCEPTIONAL_15M = float(
    os.getenv("V17_3_2_EXCEPTIONAL_15M", "0.0250")
)
V17_3_2_MIN_SCORE = float(os.getenv("V17_3_2_MIN_SCORE", "72.0"))
V17_3_2_CONTROL_VISIBLE = os.getenv(
    "V17_3_2_CONTROL_VISIBLE", "true"
).lower() == "true"

# --- Fast burst requirements ---
FAST_BURST_ENABLED = os.getenv("FAST_BURST_ENABLED", "true").lower() == "true"
FAST_MIN_15M_MOVE = float(os.getenv("FAST_MIN_15M_MOVE", "0.0045"))        # 1.0% in 15m
FAST_MIN_30M_MOVE = float(os.getenv("FAST_MIN_30M_MOVE", "0.0070"))        # 1.6% in 30m
FAST_MAX_30M_MOVE = float(os.getenv("FAST_MAX_30M_MOVE", "0.090"))        # avoid late vertical chase
FAST_MIN_RANGE_RATIO = float(os.getenv("FAST_MIN_RANGE_RATIO", "0.82"))   # current 5m range expansion
FAST_MIN_VOLUME_RATIO = float(os.getenv("FAST_MIN_VOLUME_RATIO", "0.35")) # current 15m volume expansion
FAST_MIN_1M_CONFIRM = float(os.getenv("FAST_MIN_1M_CONFIRM", "0.00055"))   # 0.15% last 3m direction
# V13.19: fast scalps can be either continuation OR blow-off reversal.
# Example from diagnostics: 30m +16%, last 3m -1% can be a valid SHORT scalp, not a rejection.
REVERSAL_ENABLED = os.getenv("REVERSAL_ENABLED", "true").lower() == "true"
REVERSAL_MIN_30M_MOVE = float(os.getenv("REVERSAL_MIN_30M_MOVE", "0.018"))
REVERSAL_MIN_LIVE_COUNTER_MOVE = float(os.getenv("REVERSAL_MIN_LIVE_COUNTER_MOVE", "0.0012"))
LIVE_BYPASS_VOLUME_MOVE = float(os.getenv("LIVE_BYPASS_VOLUME_MOVE", "0.0035"))
LIVE_BYPASS_RANGE_RATIO = float(os.getenv("LIVE_BYPASS_RANGE_RATIO", "1.35"))
FAST_MAX_SPREAD_PROXY = float(os.getenv("FAST_MAX_SPREAD_PROXY", "0.030"))# current 5m candle too wide/chase block
EDGE_MIN_PRIOR_COMPRESSION = float(os.getenv("EDGE_MIN_PRIOR_COMPRESSION", "99.0")) # prior 5m range should be smaller before expansion
EDGE_MIN_BREAKOUT_DISTANCE = float(os.getenv("EDGE_MIN_BREAKOUT_DISTANCE", "0.00005")) # 0.12% micro break beyond prior 1m structure
EDGE_REQUIRE_MICRO_SWEEP = os.getenv("EDGE_REQUIRE_MICRO_SWEEP", "false").lower() == "true"

# --- Realtime pressure gate ---
# Previous versions expired because they detected a pattern after the flow had already died.
# These filters require live 1m pressure at the exact signal moment.
HOT_MIN_SCORE = float(os.getenv("HOT_MIN_SCORE", "14"))
HOT_MIN_LIVE_MOVE_3M = float(os.getenv("HOT_MIN_LIVE_MOVE_3M", "0.0006"))
HOT_MIN_LIVE_RANGE_OR_VOLUME = float(os.getenv("HOT_MIN_LIVE_RANGE_OR_VOLUME", "0.70"))
HOT_STALE_PENALTY_ENABLED = os.getenv("HOT_STALE_PENALTY_ENABLED", "true").lower() == "true"
REALTIME_MIN_1M_RANGE_RATIO = float(os.getenv("REALTIME_MIN_1M_RANGE_RATIO", "0.45"))
REALTIME_MIN_1M_VOLUME_RATIO = float(os.getenv("REALTIME_MIN_1M_VOLUME_RATIO", "0.20"))
REALTIME_MIN_2M_MOVE = float(os.getenv("REALTIME_MIN_2M_MOVE", "0.00045"))
REALTIME_CLOSE_LOCATION_LONG = float(os.getenv("REALTIME_CLOSE_LOCATION_LONG", "0.57"))
REALTIME_CLOSE_LOCATION_SHORT = float(os.getenv("REALTIME_CLOSE_LOCATION_SHORT", "0.43"))
REALTIME_REQUIRE_TWO_1M_CANDLES = os.getenv("REALTIME_REQUIRE_TWO_1M_CANDLES", "false").lower() == "true"
EDGE_MIN_TP5_FEASIBILITY = float(os.getenv("EDGE_MIN_TP5_FEASIBILITY", "0.50")) # recent 15m move should cover most of TP5 distance

# --- Pullback/retest requirements ---
PULLBACK_MIN = float(os.getenv("PULLBACK_MIN", "0.0015"))                 # 0.25%
PULLBACK_MAX = float(os.getenv("PULLBACK_MAX", "0.0400"))                 # 3.0%
RECLAIM_BUFFER = float(os.getenv("RECLAIM_BUFFER", "0.0005"))
CLOSE_LOCATION_MIN_LONG = float(os.getenv("CLOSE_LOCATION_MIN_LONG", "0.52"))
CLOSE_LOCATION_MAX_SHORT = float(os.getenv("CLOSE_LOCATION_MAX_SHORT", "0.48"))

# --- Compact ladder TPs for fast 10-minute realization style ---
# These are intentionally more compact than slow ladder targets.
# Trader-example ladder: AERO/PORTAL/HOME/VELVET style targets are not tiny 0.3% scalps.
# TP1 should be reachable quickly, but TP5 should represent a real 3-4% move when volatility allows.
TP1_MOVE = float(os.getenv("TP1_MOVE", "0.0065"))
TP2_MOVE = float(os.getenv("TP2_MOVE", "0.0120"))
TP3_MOVE = float(os.getenv("TP3_MOVE", "0.0185"))
TP4_MOVE = float(os.getenv("TP4_MOVE", "0.0260"))
TP5_MOVE = float(os.getenv("TP5_MOVE", "0.0350"))

# User accounting rule: TP1/TP2 are intermediate only. A signal becomes a
# positive trade for statistics and learning only when TP3 is reached.
PROFIT_TARGET_NUMBER = 3
PROFIT_TARGET_KEY = "tp3"

# --- Risk / stop ---
SL_ATR_MULT = float(os.getenv("SL_ATR_MULT", "0.80"))
MIN_SL_MOVE = float(os.getenv("MIN_SL_MOVE", "0.0100"))                  # min 1.0% price risk
MAX_SL_MOVE = float(os.getenv("MAX_SL_MOVE", "0.0260"))                  # technical invalidation cap for example-style ladder

# V13.29: for fast scalps we use a LOCAL execution stop, not the distant invalidation/averaging zone.
# This keeps AERO-style / dump scalps alive while still blocking XMR-style wide-risk trades.
LOCAL_SCALP_STOP_ENABLED = os.getenv("LOCAL_SCALP_STOP_ENABLED", "true").lower() == "true"
LOCAL_SCALP_MAX_SL_MOVE = float(os.getenv("LOCAL_SCALP_MAX_SL_MOVE", "0.0145"))  # 1.45% price risk cap; x20 ≈ 29% ROI
LOCAL_SCALP_MIN_SL_MOVE = float(os.getenv("LOCAL_SCALP_MIN_SL_MOVE", "0.0065"))  # keep stop not too tight
LOCAL_STOP_MODES = {
    "MARKET_DUMP_SHORT", "INSTANT_MOMENTUM_SHORT", "INSTANT_MOMENTUM_LONG",
    "INSTANT_EVIDENCE_SHORT", "INSTANT_EVIDENCE_LONG",
    "INSTANT_SHADOW_OBSERVATION_SHORT", "INSTANT_SHADOW_OBSERVATION_LONG",
    "AERO_STYLE_SHORT", "AERO_STYLE_LONG",
    "INSTANT_PULLBACK_RECLAIM_LONG", "INSTANT_PULLBACK_RECLAIM_SHORT",
    "V17_2_CONTINUATION_LONG", "V17_2_CONTINUATION_SHORT",
    "V17_2_SWEEP_REVERSAL_LONG", "V17_2_SWEEP_REVERSAL_SHORT",
    "V17_2_CONFIRMED_CONTINUATION_LONG", "V17_2_CONFIRMED_CONTINUATION_SHORT",
    "V17_2_CONFIRMED_REVERSAL_LONG", "V17_2_CONFIRMED_REVERSAL_SHORT",
    "V17_2_1_MEASURED_EDGE_LONG", "V17_2_1_MEASURED_EDGE_SHORT",
    "V17_2_2_DIRECT_MEASURED_LONG", "V17_2_2_DIRECT_MEASURED_SHORT",
}
FAST_RISK_MULT = float(os.getenv("FAST_RISK_MULT", "0.08"))
# A+ did not outperform B in the newest 50 outcomes. Grade still ranks a setup,
# but it cannot increase suggested risk until a fresh forward audit proves it.
A_RISK_MULT = float(os.getenv("A_RISK_MULT", "0.08"))

# --- V13.22 professional quality gate ---
# Blocks mathematically bad scalps like: TP1 small, SL huge, weak live volume, poor ladder RR.
MAX_SCALP_SL_ROI = float(os.getenv("MAX_SCALP_SL_ROI", "32.0"))
MIN_TP1_RR = float(os.getenv("MIN_TP1_RR", "0.20"))
MIN_LADDER_RR_HARD = float(os.getenv("MIN_LADDER_RR_HARD", "0.62"))
MIN_FINAL_RR_HARD = float(os.getenv("MIN_FINAL_RR_HARD", "1.15"))
MIN_LIVE_VOL_NORMAL = float(os.getenv("MIN_LIVE_VOL_NORMAL", "0.50"))
MIN_LIVE_VOL_STRONG_PRICE = float(os.getenv("MIN_LIVE_VOL_STRONG_PRICE", "0.30"))
STRONG_1M3_MOVE = float(os.getenv("STRONG_1M3_MOVE", "0.0050"))
STRONG_RANGE1 = float(os.getenv("STRONG_RANGE1", "1.25"))
HEAVY_MIN_FINAL_RR = float(os.getenv("HEAVY_MIN_FINAL_RR", "1.25"))
HEAVY_MAX_SL_ROI = float(os.getenv("HEAVY_MAX_SL_ROI", "13.0"))
HEAVY_MIN_LIVE_VOL = float(os.getenv("HEAVY_MIN_LIVE_VOL", "0.70"))
HEAVY_BASES = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "LINK", "AVAX",
    "DOT", "LTC", "BCH", "XMR", "GMX", "AAVE", "UNI", "ATOM", "ETC", "FIL"
}

# --- V13.24 Instant Edge fallback ---
# This mode catches the examples-style micro-moment when the coin is moving NOW,
# but the older pullback/EMA/VWAP setup is too slow and returns no_fast.
# It is not a loose mode: final RR/SL/live-volume quality gate still applies after trade construction.
INSTANT_EDGE_ENABLED = os.getenv("INSTANT_EDGE_ENABLED", "true").lower() == "true"
INSTANT_MIN_1M3_MOVE = float(os.getenv("INSTANT_MIN_1M3_MOVE", "0.0055"))
INSTANT_MIN_15M_MOVE = float(os.getenv("INSTANT_MIN_15M_MOVE", "0.0040"))
INSTANT_MIN_VOL1 = float(os.getenv("INSTANT_MIN_VOL1", "0.45"))
INSTANT_MIN_RANGE1 = float(os.getenv("INSTANT_MIN_RANGE1", "0.85"))
INSTANT_MIN_VOL5 = float(os.getenv("INSTANT_MIN_VOL5", "0.55"))
INSTANT_MIN_RANGE5 = float(os.getenv("INSTANT_MIN_RANGE5", "0.70"))
INSTANT_CLOSE_LONG = float(os.getenv("INSTANT_CLOSE_LONG", "0.60"))
INSTANT_CLOSE_SHORT = float(os.getenv("INSTANT_CLOSE_SHORT", "0.40"))
INSTANT_MIN_BODY = float(os.getenv("INSTANT_MIN_BODY", "0.34"))
INSTANT_MAX_30M_CHASE = float(os.getenv("INSTANT_MAX_30M_CHASE", "0.065"))
INSTANT_ALLOW_STRONG_1M_EXCEPTION = os.getenv("INSTANT_ALLOW_STRONG_1M_EXCEPTION", "true").lower() == "true"

# --- V13.25 trader-pattern quality gate ---
# Built from the examples: AERO/PORTAL/HOME/WLD/VELVET are not random hot ticks.
# They are either continuation after a controlled pullback/reject, or a leader/laggard relative-strength exception.
TRADER_PATTERN_GATE_ENABLED = os.getenv("TRADER_PATTERN_GATE_ENABLED", "true").lower() == "true"
TRADER_MIN_SCORE = int(os.getenv("TRADER_MIN_SCORE", "82"))
TRADER_ALLOW_B_SCORE = os.getenv("TRADER_ALLOW_B_SCORE", "true").lower() == "true"
TRADER_MIN_ABS_1M3 = float(os.getenv("TRADER_MIN_ABS_1M3", "0.0048"))
TRADER_MIN_ABS_15M = float(os.getenv("TRADER_MIN_ABS_15M", "0.0055"))
TRADER_MIN_ABS_30M = float(os.getenv("TRADER_MIN_ABS_30M", "0.0080"))
TRADER_MIN_VOL1 = float(os.getenv("TRADER_MIN_VOL1", "0.52"))
TRADER_MIN_VOL5 = float(os.getenv("TRADER_MIN_VOL5", "0.52"))
TRADER_MIN_RANGE1 = float(os.getenv("TRADER_MIN_RANGE1", "0.85"))
TRADER_MIN_RANGE5 = float(os.getenv("TRADER_MIN_RANGE5", "0.75"))
TRADER_MIN_TP5_FEASIBILITY = float(os.getenv("TRADER_MIN_TP5_FEASIBILITY", "0.50"))
TRADER_NEED_5M_DIRECTION = os.getenv("TRADER_NEED_5M_DIRECTION", "false").lower() == "true"
TRADER_BLOCK_WEAK_CONTINUATION = os.getenv("TRADER_BLOCK_WEAK_CONTINUATION", "true").lower() == "true"
TRADER_MAX_COUNTER_30M = float(os.getenv("TRADER_MAX_COUNTER_30M", "0.0100"))
TRADER_REQUIRE_MICRO_BREAK = os.getenv("TRADER_REQUIRE_MICRO_BREAK", "true").lower() == "true"
TRADER_CLOSE_LONG = float(os.getenv("TRADER_CLOSE_LONG", "0.57"))
TRADER_CLOSE_SHORT = float(os.getenv("TRADER_CLOSE_SHORT", "0.43"))
TRADER_HEAVY_ONLY_A_PLUS = os.getenv("TRADER_HEAVY_ONLY_A_PLUS", "true").lower() == "true"

# --- V13.27 AERO-style trader gate ---
# This is built from the user's AERO example: short after a controlled upper pullback/reject,
# not a random late short at the bottom. It allows quality B+ trades if the tape shows a true
# pullback -> rejection -> breakdown structure, while keeping RR/SL quality gate active.
AERO_STYLE_GATE_ENABLED = os.getenv("AERO_STYLE_GATE_ENABLED", "true").lower() == "true"
AERO_SHORT_ENABLED = os.getenv("AERO_SHORT_ENABLED", "true").lower() == "true"
AERO_LONG_ENABLED = os.getenv("AERO_LONG_ENABLED", "true").lower() == "true"
AERO_MIN_PULLBACK = float(os.getenv("AERO_MIN_PULLBACK", "0.0045"))       # recent high/low must be away from entry
AERO_MAX_PULLBACK = float(os.getenv("AERO_MAX_PULLBACK", "0.0850"))       # avoid extreme manipulated spikes
AERO_MIN_1M3 = float(os.getenv("AERO_MIN_1M3", "0.0038"))                 # current 3m pressure
AERO_MIN_RECENT_RANGE = float(os.getenv("AERO_MIN_RECENT_RANGE", "0.0140")) # recent 30m range expansion
AERO_MIN_VOL1 = float(os.getenv("AERO_MIN_VOL1", "0.35"))
AERO_MIN_VOL5 = float(os.getenv("AERO_MIN_VOL5", "0.45"))
AERO_MIN_RANGE1 = float(os.getenv("AERO_MIN_RANGE1", "0.60"))
AERO_MIN_RANGE5 = float(os.getenv("AERO_MIN_RANGE5", "0.65"))
AERO_CLOSE_SHORT = float(os.getenv("AERO_CLOSE_SHORT", "0.48"))
AERO_CLOSE_LONG = float(os.getenv("AERO_CLOSE_LONG", "0.52"))
AERO_REQUIRE_EMA_REJECT = os.getenv("AERO_REQUIRE_EMA_REJECT", "true").lower() == "true"
AERO_ALLOW_B_SCORE = os.getenv("AERO_ALLOW_B_SCORE", "true").lower() == "true"

# --- V13.28 Market Dump SHORT fallback ---
# Used when BTC/ETH/alt market is actively selling off and old fast/aero gates are too
# selective. This catches dump continuation, but still avoids shorting a dead bottom:
# it needs live 1m pressure, 5m participation and fresh continuation/reject evidence.
MARKET_DUMP_SHORT_ENABLED = os.getenv("MARKET_DUMP_SHORT_ENABLED", "true").lower() == "true"
DUMP_MIN_1M3 = float(os.getenv("DUMP_MIN_1M3", "0.0048"))
DUMP_MIN_15M = float(os.getenv("DUMP_MIN_15M", "0.0035"))
DUMP_MIN_VOL1 = float(os.getenv("DUMP_MIN_VOL1", "0.35"))
DUMP_MIN_VOL5 = float(os.getenv("DUMP_MIN_VOL5", "0.48"))
DUMP_MIN_RANGE1 = float(os.getenv("DUMP_MIN_RANGE1", "0.40"))
DUMP_MIN_RANGE5 = float(os.getenv("DUMP_MIN_RANGE5", "0.60"))
DUMP_CLOSE_SHORT = float(os.getenv("DUMP_CLOSE_SHORT", "0.58"))
DUMP_MIN_RECENT_RANGE = float(os.getenv("DUMP_MIN_RECENT_RANGE", "0.0100"))
DUMP_MAX_LATE_30M = float(os.getenv("DUMP_MAX_LATE_30M", "0.095"))
DUMP_REQUIRE_REJECT_OR_BREAK = os.getenv("DUMP_REQUIRE_REJECT_OR_BREAK", "false").lower() == "true"


# --- Time stop / no-stall logic ---
FAST_MAX_MINUTES_TO_TP1 = int(os.getenv("FAST_MAX_MINUTES_TO_TP1", "6"))
FAST_HARD_EXPIRE_MINUTES = int(os.getenv("FAST_HARD_EXPIRE_MINUTES", "11"))
FAST_MIN_PROGRESS_TO_KEEP = float(os.getenv("FAST_MIN_PROGRESS_TO_KEEP", "0.25"))
FAST_CANCEL_IF_NO_PROGRESS = os.getenv("FAST_CANCEL_IF_NO_PROGRESS", "true").lower() == "true"

# --- Market shock context ---
# We do not trade market phase/trend. BTC is used only as a shock filter.
BTC_SHOCK_15M_BLOCK = float(os.getenv("BTC_SHOCK_15M_BLOCK", "0.020")) # avoid alt scalp during violent BTC shock

# --- Side control / professional LONG repair ---
# The 125-outcome audit found that evidence-qualified LONGs were the strongest
# family, while both recent SHORT families stayed negative. LONG protection is
# therefore based only on comparable evidence-qualified LIVE history.
ALLOW_LONG = os.getenv("ALLOW_LONG", "true").lower() == "true"
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "true").lower() == "true"
LONG_BLOCK_BTC_BEAR = os.getenv("LONG_BLOCK_BTC_BEAR", "false").lower() == "true"
LONG_MIN_1M_VOLUME_RATIO = float(os.getenv("LONG_MIN_1M_VOLUME_RATIO", "0.75"))
LONG_MIN_1M_RANGE_RATIO = float(os.getenv("LONG_MIN_1M_RANGE_RATIO", "0.80"))
LONG_MIN_3M_CONFIRM = float(os.getenv("LONG_MIN_3M_CONFIRM", "0.0012"))     # 0.12% in 3m
LONG_MIN_CLOSE_LOCATION = float(os.getenv("LONG_MIN_CLOSE_LOCATION", "0.72"))
LONG_MAX_15M_CHASE = float(os.getenv("LONG_MAX_15M_CHASE", "0.040"))        # above this needs pullback/sweep
LONG_MAX_30M_CHASE = float(os.getenv("LONG_MAX_30M_CHASE", "0.070"))
LONG_MIN_PULLBACK_AFTER_PUMP = float(os.getenv("LONG_MIN_PULLBACK_AFTER_PUMP", "0.0055"))
LONG_MAX_PULLBACK_AFTER_PUMP = float(os.getenv("LONG_MAX_PULLBACK_AFTER_PUMP", "0.038"))
LONG_REQUIRE_SWEEP_OR_RECLAIM = os.getenv("LONG_REQUIRE_SWEEP_OR_RECLAIM", "true").lower() == "true"
LONG_REQUIRE_HIGHER_LOW = os.getenv("LONG_REQUIRE_HIGHER_LOW", "true").lower() == "true"
LONG_STATS_PROTECTION = os.getenv("LONG_STATS_PROTECTION", "true").lower() == "true"
LONG_STATS_MIN_CLOSED = int(os.getenv("LONG_STATS_MIN_CLOSED", "4"))
LONG_STATS_MIN_WR = float(os.getenv("LONG_STATS_MIN_WR", "40"))
LONG_STATS_WINDOW = int(os.getenv("LONG_STATS_WINDOW", "20"))
LONG_STATS_MIN_EXPECTANCY_R = float(os.getenv("LONG_STATS_MIN_EXPECTANCY_R", "0.00"))

# --- V13.21 context-adaptive rules ---
# Professional idea: BTC direction is not a simple long/short switch.
# LONG is allowed in a bearish market only if the coin is showing clear relative strength
# and live reclaim pressure. SHORT is prioritized during BTC dump, but not chased without
# a bounce/reject structure.
CONTEXT_ADAPTIVE_ENABLED = os.getenv("CONTEXT_ADAPTIVE_ENABLED", "true").lower() == "true"
BTC_DUMP_SHORT_BIAS_ENABLED = os.getenv("BTC_DUMP_SHORT_BIAS_ENABLED", "true").lower() == "true"
BTC_DUMP_1H = float(os.getenv("BTC_DUMP_1H", "-0.012"))
BTC_DUMP_6H = float(os.getenv("BTC_DUMP_6H", "-0.025"))
LONG_ALLOW_BEAR_RELATIVE_STRENGTH = os.getenv("LONG_ALLOW_BEAR_RELATIVE_STRENGTH", "true").lower() == "true"
LONG_BEAR_MIN_ALT_15M = float(os.getenv("LONG_BEAR_MIN_ALT_15M", "0.0065"))
LONG_BEAR_MIN_ALT_30M = float(os.getenv("LONG_BEAR_MIN_ALT_30M", "0.0100"))
LONG_BEAR_MIN_1M3 = float(os.getenv("LONG_BEAR_MIN_1M3", "0.0020"))
LONG_BEAR_MIN_REL_STRENGTH_1H = float(os.getenv("LONG_BEAR_MIN_REL_STRENGTH_1H", "0.010"))
LONG_BEAR_MIN_VOL1 = float(os.getenv("LONG_BEAR_MIN_VOL1", "0.90"))
LONG_BEAR_MIN_RANGE1 = float(os.getenv("LONG_BEAR_MIN_RANGE1", "0.95"))
LONG_BEAR_MIN_CLOSE_LOCATION = float(os.getenv("LONG_BEAR_MIN_CLOSE_LOCATION", "0.76"))
SHORT_DUMP_ALLOW_EXTENDED_30M = float(os.getenv("SHORT_DUMP_ALLOW_EXTENDED_30M", "0.145"))
SHORT_DUMP_MIN_LIVE_1M3 = float(os.getenv("SHORT_DUMP_MIN_LIVE_1M3", "-0.0014"))
SHORT_DUMP_MIN_BOUNCE = float(os.getenv("SHORT_DUMP_MIN_BOUNCE", "0.0025"))


# --- Ultra-risk blocks ---
ULTRA_RISK_5M_CANDLE = float(os.getenv("ULTRA_RISK_5M_CANDLE", "0.095"))
ULTRA_RISK_15M_CANDLE = float(os.getenv("ULTRA_RISK_15M_CANDLE", "0.140"))

SCALP_STRATEGIES = {"PRO_SCALPING_EDGE_LONG", "PRO_SCALPING_EDGE_SHORT"}

QUALITY_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "LINK", "AVAX", "AAVE", "SUI", "TAO", "NEAR", "INJ",
    "OP", "ARB", "APT", "TIA", "ADA", "DOT", "MATIC", "TON", "LTC", "BCH", "ETC", "FIL", "ATOM",
    "UNI", "RUNE", "SEI", "FET", "WLD", "DOGE", "TRX", "ENA", "JUP", "ORDI",
    "PORTAL", "HOME", "TAC", "VELVET", "BEAT", "BLESS"
}

# Do not include VELVET here; user gave a successful VELVET long example.
ULTRA_RISK_KEYWORDS = {
    "1000", "PEPE", "BONK", "WIF", "MEME", "DOGS", "CATI", "HMSTR", "GOBLIN", "MOG", "TURBO",
    "BOME", "NEIRO", "PNUT", "MOODENG", "ACT", "GOAT", "FIGHT", "BLEND", "MAGMA"
}

FALLBACK_SYMBOLS = [f"{b}-USDT" for b in [
    "BTC", "ETH", "SOL", "BNB", "XRP", "LINK", "AVAX", "AAVE", "SUI", "TAO", "NEAR", "INJ",
    "OP", "ARB", "APT", "TIA", "ADA", "DOT", "LTC", "BCH", "ETC", "FIL", "ATOM", "UNI",
    "RUNE", "SEI", "FET", "WLD", "DOGE", "TRX", "ENA", "JUP", "ORDI", "BEAT", "BLESS",
    "KAITO", "XLM", "WLFI", "PUMP", "PORTAL", "HOME", "TAC", "VELVET"
]]

STATE: Dict[str, Any] = {}
SEED_RESTORE_INFO: Dict[str, Any] = {"restored": 0, "source": "", "reason": "not_checked"}
KLINE_CACHE: Dict[str, Tuple[float, Optional[List[Dict[str, float]]]]] = {}
TICKER_CACHE: Dict[str, Tuple[float, Optional[List[str]]]] = {}
LIQUIDITY_SCAN_CACHE: Dict[str, Dict[str, Any]] = {}
STATE_IO_LOCK = threading.RLock()
SCAN_RUN_LOCK = threading.Lock()
TRACK_RUN_LOCK = threading.Lock()
PENDING_RUN_LOCK = threading.Lock()
TELEGRAM_SEND_LOCK = threading.Lock()
TELEGRAM_OUTBOX_FLUSH_LOCK = threading.Lock()
LONG_STATS_CACHE: Dict[str, Any] = {"ts": 0.0, "value": (True, "not evaluated")}

# ============================================================
# State / utilities
# ============================================================

def now_ts() -> int:
    return int(time.time())


def normalize_symbol(symbol: str) -> str:
    s = symbol.replace("/", "-").upper()
    if s.endswith("USDT") and "-" not in s:
        s = s.replace("USDT", "-USDT")
    return s


def display_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("-", "/")


def base_asset(symbol: str) -> str:
    return normalize_symbol(symbol).split("-")[0]


def default_watch_audit() -> Dict[str, Any]:
    return {
        "version": "V17.3.2",
        "liquidity_checked": 0,
        "liquidity_passed": 0,
        "liquidity_rejected": 0,
        "started": 0,
        "execution_passed": 0,
        "confirmed": 0,
        "rejected": {
            "execution_book": 0,
            "risk_rr": 0,
            "symbol_repeat": 0,
            "other": 0,
        },
        "by_side": {
            "LONG": {"started": 0, "confirmed": 0, "rejected": 0},
            "SHORT": {"started": 0, "confirmed": 0, "rejected": 0},
        },
        "by_lane": {
            "DIRECT_MEASURED": {"started": 0, "confirmed": 0, "rejected": 0},
        },
        "recent": [],
    }


def default_telegram_delivery() -> Dict[str, Any]:
    return {
        "version": "V17.1.1",
        "immediate_sent": 0,
        "queued": 0,
        "delivered_from_queue": 0,
        "failed_attempts": 0,
        "dropped_diagnostics": 0,
        "last_success_at": 0,
        "last_failure_at": 0,
        "last_error": "",
        "last_mode": "not_started",
    }


def default_state() -> Dict[str, Any]:
    return {
        "schema_version": 7,
        "active_signals": [],
        "pending_signals": [],
        "shadow_signals": [],
        "stats": {
            "total": {"profit": 0, "sl": 0, "expired": 0},
            "side": {},
            "grade": {},
            "strategy": {},
            "symbol": {},
            "type": {},
        },
        "pair_cooldown": {},
        "strategy_cooldown": {},
        "shadow_cooldown": {},
        "spike_regime_reentry": {},
        "symbol_outcomes": {},
        "strategy_guard_status": {},
        "live_send_timestamps": [],
        "live_send_history": [],
        "seed_restore": {},
        "last_backup_closed_count": 0,
        "last_official_backup_count": 0,
        "last_source_audit_closed_count": 0,
        "last_forward_report_count": 0,
        "last_paper_checkpoint_count": 0,
        "last_paper_cohort_report_count": 0,
        "watch_audit_v17_2": default_watch_audit(),
        "last_pending_monitor": {},
        "telegram_outbox": [],
        "telegram_delivery": default_telegram_delivery(),
        "last_scan": {},
        "last_diag_ts": 0,
        "last_error": "",
    }


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        base = default_state()
        loaded_schema = 0
        if isinstance(data, dict):
            loaded_schema = int(data.get("schema_version", 0) or 0)
            base.update(data)
        # V20.3 starts a clean transient risk-control cohort. Historical SQLite
        # rows remain for audit, but old re-entry locks/status flags must not
        # block the corrected strategy after deploy.
        if loaded_schema < 7:
            base["spike_regime_reentry"] = {}
            base["strategy_guard_status"] = {}
            base["last_official_backup_count"] = 0
        # Deep defaults keep an older V13/V15 state compatible with V16.
        base.setdefault("active_signals", [])
        base.setdefault("pending_signals", [])
        base.setdefault("shadow_signals", [])
        # V18.2.3 clean cohort: discard stale legacy/control/near-miss positions.
        # Historical SQLite rows stay audit-only, but they cannot close into the
        # new two-strategy statistics or Telegram stream.
        base["shadow_signals"] = [
            item for item in base.get("shadow_signals", [])
            if str(item.get("shadow_reason", "")) in TRACKABLE_SHADOW_REASONS
        ]
        base.setdefault("pair_cooldown", {})
        base.setdefault("strategy_cooldown", {})
        base.setdefault("shadow_cooldown", {})
        base.setdefault("spike_regime_reentry", {})
        base.setdefault("symbol_outcomes", {})
        base.setdefault("strategy_guard_status", {})
        base.setdefault("live_send_timestamps", [])
        base.setdefault("live_send_history", [])
        base.setdefault("seed_restore", {})
        base.setdefault("last_backup_closed_count", 0)
        base.setdefault("last_official_backup_count", 0)
        base.setdefault("last_source_audit_closed_count", 0)
        base.setdefault("last_forward_report_count", 0)
        base.setdefault("last_paper_checkpoint_count", 0)
        base.setdefault("last_paper_cohort_report_count", 0)
        audit = base.setdefault("watch_audit_v17_2", default_watch_audit())
        if not isinstance(audit, dict) or str(audit.get("version", "")) != "V17.3":
            base["watch_audit_v17_2"] = default_watch_audit()
        base.setdefault("last_pending_monitor", {})
        outbox = base.setdefault("telegram_outbox", [])
        if not isinstance(outbox, list):
            base["telegram_outbox"] = []
        delivery = base.setdefault(
            "telegram_delivery", default_telegram_delivery()
        )
        if (
            not isinstance(delivery, dict)
            or str(delivery.get("version", "")) != "V17.1.1"
        ):
            previous_delivery = delivery if isinstance(delivery, dict) else {}
            delivery = default_telegram_delivery()
            for key in delivery:
                if key in previous_delivery and key != "version":
                    delivery[key] = previous_delivery[key]
            base["telegram_delivery"] = delivery
        stats = base.setdefault("stats", {})
        for bucket, value in default_state()["stats"].items():
            stats.setdefault(bucket, value.copy() if isinstance(value, dict) else value)
        base["schema_version"] = 7
        return base
    except Exception:
        return default_state()


def save_state() -> None:
    try:
        path = Path(STATE_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(path.suffix + ".tmp")
        with STATE_IO_LOCK:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(STATE, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, path)
    except Exception as e:
        # Do not recursively call save_state from its own error path.
        STATE["last_error"] = f"state save error: {repr(e)}"


def _adaptive_seed_candidates() -> List[Path]:
    """Return the configured seed first and avoid ambiguous JSON auto-picks.

    V17.0 searched every JSON in the repository and selected the largest one.
    That made duplicate/old backup names capable of restoring the wrong cohort.
    V17.1 loads only ADAPTIVE_SEED_PATH unless discovery is explicitly enabled.
    """
    candidates: List[Path] = []
    configured = Path(ADAPTIVE_SEED_PATH)
    if not configured.is_absolute():
        configured = Path.cwd() / configured
    candidates.append(configured)

    if ADAPTIVE_SEED_DISCOVERY_ENABLED:
        roots = [Path.cwd(), Path(__file__).resolve().parent]
        for root in roots:
            try:
                candidates.extend(sorted(root.glob("*.json")))
            except Exception:
                continue

    state_path = Path(STATE_FILE)
    if not state_path.is_absolute():
        state_path = Path.cwd() / state_path
    state_path = state_path.resolve()

    unique: List[Path] = []
    seen = set()
    for path in candidates:
        try:
            resolved = path.resolve()
        except Exception:
            continue
        marker = str(resolved)
        if marker in seen or resolved == state_path:
            continue
        seen.add(marker)
        unique.append(resolved)
    return unique


def restore_adaptive_seed_if_empty() -> Dict[str, Any]:
    """Restore a portable V16.2+ export only into an empty adaptive DB.

    Active signals and cooldowns are intentionally not restored: stale market
    positions must never come back after a deploy. Historical outcomes, stats
    and the latest training milestone are preserved.
    """
    init_adaptive_db()
    if adaptive_closed_count() > 0:
        return {"restored": 0, "source": "", "reason": "database_not_empty"}

    seed_path: Optional[Path] = None
    payload: Optional[Dict[str, Any]] = None
    best_count = 0
    for candidate in _adaptive_seed_candidates():
        if not candidate.is_file():
            continue
        try:
            with candidate.open("r", encoding="utf-8") as handle:
                possible = json.load(handle)
            trades = possible.get("adaptive_trades") if isinstance(possible, dict) else None
            if isinstance(trades, list) and len(trades) > best_count:
                seed_path = candidate
                payload = possible
                best_count = len(trades)
        except Exception:
            continue

    if seed_path is None or payload is None:
        return {"restored": 0, "source": "", "reason": "seed_not_found"}

    seed_trades = payload.get("adaptive_trades") or []
    inserted = 0
    with _LOCK, _connect() as conn:
        if int(conn.execute("SELECT COUNT(*) AS n FROM adaptive_trades").fetchone()["n"] or 0) > 0:
            return {"restored": 0, "source": "", "reason": "database_not_empty"}

        for index, item in enumerate(seed_trades):
            if not isinstance(item, dict):
                continue
            result = str(item.get("result", "expired"))
            if result not in {"profit", "sl", "expired"}:
                continue
            features_payload = item.get("features")
            if not isinstance(features_payload, dict):
                try:
                    features_payload = json.loads(item.get("features_json") or "{}")
                except Exception:
                    features_payload = {}
            created_at = int(item.get("created_at", int(time.time())) or int(time.time()))
            closed_at = int(item.get("closed_at", created_at) or created_at)
            signal_id = str(item.get("signal_id") or f"seed:{index}:{created_at}")
            evidence_accepted = item.get("evidence_guard_accepted")
            shadow_accepted = item.get("shadow_accepted")
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO adaptive_trades(
                    signal_id, created_at, closed_at, source, symbol, side, strategy, grade,
                    result, label, pnl_r, exit_price, mfe_r, mae_r, duration_minutes,
                    features_json, model_version, model_probability, shadow_accepted,
                    evidence_guard_version, evidence_guard_accepted, evidence_guard_reason,
                    decision_reason
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    signal_id,
                    created_at,
                    closed_at,
                    str(item.get("source", "live")),
                    str(item.get("symbol", "?")),
                    str(item.get("side", "?")),
                    str(item.get("strategy", "?")),
                    str(item.get("grade", "?")),
                    result,
                    1 if result == "profit" else 0,
                    float(item.get("pnl_r", 0.0) or 0.0),
                    float(item["exit_price"]) if item.get("exit_price") is not None else None,
                    float(item.get("mfe_r", 0.0) or 0.0),
                    float(item.get("mae_r", 0.0) or 0.0),
                    float(item.get("duration_minutes", 0.0) or 0.0),
                    json.dumps(features_payload, ensure_ascii=False),
                    int(item.get("model_version", 0) or 0),
                    float(item["model_probability"])
                    if item.get("model_probability") is not None
                    else None,
                    int(bool(shadow_accepted)) if shadow_accepted is not None else None,
                    int(item.get("evidence_guard_version", 0) or 0),
                    int(bool(evidence_accepted)) if evidence_accepted is not None else None,
                    str(item.get("evidence_guard_reason"))
                    if item.get("evidence_guard_reason") is not None
                    else None,
                    str(item.get("decision_reason"))
                    if item.get("decision_reason") is not None
                    else None,
                ),
            )
            inserted += max(0, int(cursor.rowcount or 0))

        for event in payload.get("adaptive_events") or []:
            if not isinstance(event, dict):
                continue
            event_payload = event.get("payload")
            if not isinstance(event_payload, dict):
                event_payload = {}
            conn.execute(
                "INSERT INTO adaptive_events(created_at, event_type, message, payload_json) "
                "VALUES(?,?,?,?)",
                (
                    int(event.get("created_at", int(time.time())) or int(time.time())),
                    str(event.get("event_type", "seed_event")),
                    str(event.get("message", "Imported from adaptive seed")),
                    json.dumps(event_payload, ensure_ascii=False),
                ),
            )
        conn.commit()

    if inserted <= 0:
        return {"restored": 0, "source": seed_path.name, "reason": "no_valid_rows"}

    with _LOCK, _connect() as conn:
        eligible_count = int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM adaptive_trades "
                "WHERE COALESCE(decision_reason, '')=?",
                (PAPER_VALIDATION_REASON,),
            ).fetchone()["n"]
            or 0
        )

    raw_model = payload.get("adaptive_model") or {}
    allowed_model = {field.name for field in fields(ModelState)}
    try:
        restored_model = ModelState(
            **{key: value for key, value in raw_model.items() if key in allowed_model}
        )
    except Exception:
        restored_model = ModelState()
    seed_policy_valid = str(raw_model.get("data_policy", "")) == MODEL_DATA_POLICY
    model_shape_valid = (
        len(restored_model.weights) == len(FEATURE_NAMES) + 1
        and len(restored_model.mean) == len(FEATURE_NAMES)
        and len(restored_model.std) == len(FEATURE_NAMES)
    )
    if not model_shape_valid or not seed_policy_valid:
        restored_model.active = False
        restored_model.weights = [0.0] * (len(FEATURE_NAMES) + 1)
        restored_model.mean = [0.0] * len(FEATURE_NAMES)
        restored_model.std = [1.0] * len(FEATURE_NAMES)
        restored_model.version = 0
        restored_model.last_trained_closed_count = 0
        restored_model.last_attempted_closed_count = 0
        restored_model.candidate_pass_streak = 0
        restored_model.candidate_selected_total = 0
        restored_model.deployment_fraction = 0.0
        restored_model.last_candidate_reason = (
            "feature_schema_changed"
            if not model_shape_valid
            else "v17_2_clean_forward_dataset"
        )
    seed_marker = str(payload.get("deploy_marker", "") or "")
    restored_model.trained_rows = eligible_count
    if not seed_marker.startswith(("V16_8_", "V16_9_", "V17_")):
        # Older versions mixed diagnostic SHADOW rows into model training.
        # Preserve every outcome, but never restore that model as a champion.
        restored_model.active = False
        restored_model.last_trained_closed_count = 0
        restored_model.last_attempted_closed_count = eligible_count
        restored_model.last_candidate_reason = "model_dataset_policy_changed"
        restored_model.candidate_pass_streak = 0
        restored_model.candidate_selected_total = 0
        restored_model.deployment_fraction = 0.0
    else:
        restored_model.last_attempted_closed_count = min(
            eligible_count,
            int(restored_model.last_attempted_closed_count or eligible_count),
        )
    restored_model.data_policy = MODEL_DATA_POLICY
    restored_model.trained_rows = eligible_count
    if restored_model.active and float(restored_model.deployment_fraction or 0) <= 0:
        restored_model.deployment_fraction = min(1.0, max(0.05, ADAPTIVE_INITIAL_LIVE_FRACTION))
    _save_model_state(restored_model)
    _save_model_snapshot(
        restored_model,
        "baseline" if restored_model.version == 0 else "inactive",
        "portable_seed_restored_v16_8",
    )

    latest_id, latest_count = _latest_trade_marker()
    raw_guard = payload.get("evidence_guard") or {}
    allowed_guard = {field.name for field in fields(EvidenceGuardState)}
    try:
        restored_guard = EvidenceGuardState(
            **{key: value for key, value in raw_guard.items() if key in allowed_guard}
        )
    except Exception:
        restored_guard = EvidenceGuardState(
            activation_trade_id=latest_id,
            activation_closed_count=latest_count,
            created_at=int(time.time()),
        )
    # Preserve a proven rollback. Do not silently reactivate a rule that the
    # previous 25-decision audit disabled.
    if int(restored_guard.version) != EVIDENCE_GUARD_VERSION:
        restored_guard = EvidenceGuardState(
            activation_trade_id=latest_id,
            activation_closed_count=latest_count,
            created_at=int(time.time()),
        )
    _save_evidence_guard_state(restored_guard)

    seed_state = payload.get("state") or {}
    seed_stats = seed_state.get("stats") if isinstance(seed_state, dict) else None
    if isinstance(seed_stats, dict):
        STATE["stats"] = seed_stats
    STATE["last_backup_closed_count"] = latest_count
    STATE["last_source_audit_closed_count"] = latest_count
    STATE["seed_restore"] = {
        "restored": inserted,
        "source": seed_path.name,
        "at": now_ts(),
    }
    rebuild_symbol_outcomes_from_adaptive_db()
    save_state()
    _event(
        "portable_seed_restored",
        f"Restored {inserted} adaptive outcomes from portable JSON",
        {"restored": inserted, "source": seed_path.name},
    )
    return {"restored": inserted, "source": seed_path.name, "reason": "restored"}


def inc_stat(bucket: str, key: str, result: str) -> None:
    stats = STATE.setdefault("stats", default_state()["stats"])
    d = stats.setdefault(bucket, {})
    item = d.setdefault(key, {"profit": 0, "sl": 0, "expired": 0})
    item[result] = item.get(result, 0) + 1


def apply_result(signal: Dict[str, Any], result: str) -> bool:
    if result not in ("profit", "sl", "expired"):
        return False
    if signal.get("stats_recorded"):
        return False
    stats = STATE.setdefault("stats", default_state()["stats"])
    stats.setdefault("total", {"profit": 0, "sl": 0, "expired": 0})[result] += 1
    inc_stat("side", signal.get("side", "?"), result)
    inc_stat("grade", signal.get("grade", "?"), result)
    inc_stat("strategy", signal.get("strategy", "?"), result)
    inc_stat("symbol", signal.get("symbol", "?"), result)
    inc_stat("type", signal.get("trade_type", "?"), result)
    signal["stats_recorded"] = result
    signal["closed_at"] = now_ts()
    save_state()
    return True


def wr_text(item: Dict[str, int]) -> str:
    p = int(item.get("profit", 0))
    sl = int(item.get("sl", 0))
    exp = int(item.get("expired", 0))
    resolved = p + sl
    all_outcomes = resolved + exp
    resolved_wr = p / resolved * 100 if resolved else 0.0
    all_success = p / all_outcomes * 100 if all_outcomes else 0.0
    return (
        f"{p} профит / {sl} SL / {exp} time-stop · "
        f"WR TP/SL {resolved_wr:.1f}% · успех всех {all_success:.1f}%"
    )


def build_stats_text() -> str:
    stats = STATE.setdefault("stats", default_state()["stats"])
    lines = ["📊 Статистика", f"Итого: {wr_text(stats.get('total', {}))}"]
    for title, key in [("Стороны", "side"), ("Классы", "grade"), ("Стратегии", "strategy"), ("Типы", "type")]:
        data = stats.get(key, {})
        if data:
            lines.append(f"\n{title}:")
            for k, v in sorted(data.items(), key=lambda kv: -(kv[1].get("profit", 0) + kv[1].get("sl", 0) + kv[1].get("expired", 0)))[:12]:
                lines.append(f"{k}: {wr_text(v)}")
    return "\n".join(lines)

# ============================================================
# Telegram / API — V17.1.1 reliable ordered delivery
# ============================================================

def _telegram_delivery_state() -> Dict[str, Any]:
    delivery = STATE.setdefault("telegram_delivery", default_telegram_delivery())
    if (
        not isinstance(delivery, dict)
        or str(delivery.get("version", "")) != "V17.1.1"
    ):
        delivery = default_telegram_delivery()
        STATE["telegram_delivery"] = delivery
    for key, value in default_telegram_delivery().items():
        delivery.setdefault(key, value)
    return delivery


def telegram_outbox_depth() -> int:
    outbox = STATE.setdefault("telegram_outbox", [])
    return len(outbox) if isinstance(outbox, list) else 0


def _telegram_message_key(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()[:24]


def _telegram_replaceable_message(text: str) -> bool:
    """Diagnostics are superseded by the next scan and must not block trades."""
    return str(text or "").lstrip().startswith("🧪 Диагностика")


def _telegram_clear_stale_main_error() -> None:
    current_error = str(STATE.get("last_error", "") or "")
    if current_error.startswith("Telegram "):
        STATE["last_error"] = ""


def _telegram_record_success(mode: str, attempts_used: int = 1) -> None:
    with STATE_IO_LOCK:
        delivery = _telegram_delivery_state()
        delivery["failed_attempts"] = int(
            delivery.get("failed_attempts", 0) or 0
        ) + max(0, int(attempts_used or 1) - 1)
        if mode == "outbox":
            delivery["delivered_from_queue"] = int(
                delivery.get("delivered_from_queue", 0) or 0
            ) + 1
        else:
            delivery["immediate_sent"] = int(
                delivery.get("immediate_sent", 0) or 0
            ) + 1
        delivery["last_success_at"] = now_ts()
        delivery["last_error"] = ""
        delivery["last_mode"] = mode
        _telegram_clear_stale_main_error()
        save_state()


def _telegram_record_failure(error: str, attempts_used: int, mode: str) -> None:
    with STATE_IO_LOCK:
        delivery = _telegram_delivery_state()
        delivery["failed_attempts"] = int(
            delivery.get("failed_attempts", 0) or 0
        ) + max(1, int(attempts_used or 1))
        delivery["last_failure_at"] = now_ts()
        delivery["last_error"] = str(error or "unknown Telegram failure")[:500]
        delivery["last_mode"] = mode
        STATE["last_error"] = f"Telegram {mode}: {delivery['last_error']}"
        save_state()


def _telegram_post_text(
    text: str,
    attempts_override: Optional[int] = None,
    read_timeout_override: Optional[float] = None,
) -> Tuple[bool, str, int]:
    """Perform bounded immediate retries without mutating the outbox."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return (
            False,
            "env missing: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID",
            1,
        )
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    last_error = "unknown Telegram error"
    attempts_used = 0
    max_attempts = max(
        1,
        int(
            TELEGRAM_SEND_ATTEMPTS
            if attempts_override is None
            else attempts_override
        ),
    )
    read_timeout = max(
        2.0,
        float(
            TELEGRAM_READ_TIMEOUT
            if read_timeout_override is None
            else read_timeout_override
        ),
    )
    with TELEGRAM_SEND_LOCK:
        for attempt in range(max_attempts):
            attempts_used = attempt + 1
            try:
                response = requests.post(
                    url,
                    json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:3900]},
                    timeout=(TELEGRAM_CONNECT_TIMEOUT, read_timeout),
                )
                if response.ok:
                    return True, "", attempts_used
                last_error = (
                    f"HTTP {response.status_code}: "
                    f"{str(getattr(response, 'text', ''))[:250]}"
                )
                retryable = response.status_code in {
                    408, 409, 425, 429, 500, 502, 503, 504
                }
                if not retryable:
                    break
            except Exception as exc:
                last_error = repr(exc)
            if attempt + 1 < max_attempts:
                time.sleep(TELEGRAM_RETRY_BASE_SECONDS * (2 ** attempt))
    return False, last_error, attempts_used


def _queue_telegram_message(text: str, error: str = "") -> bool:
    if not TELEGRAM_OUTBOX_ENABLED or _telegram_replaceable_message(text):
        return False
    message = str(text or "")[:3900]
    message_id = _telegram_message_key(message)
    with STATE_IO_LOCK:
        outbox = STATE.setdefault("telegram_outbox", [])
        if not isinstance(outbox, list):
            outbox = []
            STATE["telegram_outbox"] = outbox
        for item in outbox:
            if str(item.get("id", "")) == message_id:
                item["last_error"] = str(error or item.get("last_error", ""))[:500]
                item["next_retry_at"] = min(
                    int(item.get("next_retry_at", now_ts()) or now_ts()),
                    now_ts() + TELEGRAM_OUTBOX_FLUSH_SECONDS,
                )
                save_state()
                return True
        if len(outbox) >= TELEGRAM_OUTBOX_MAX:
            delivery = _telegram_delivery_state()
            delivery["last_error"] = (
                f"outbox full ({len(outbox)}/{TELEGRAM_OUTBOX_MAX})"
            )
            STATE["last_error"] = f"Telegram outbox: {delivery['last_error']}"
            save_state()
            return False
        outbox.append(
            {
                "id": message_id,
                "text": message,
                "created_at": now_ts(),
                "attempts": 0,
                "next_retry_at": now_ts() + TELEGRAM_OUTBOX_FLUSH_SECONDS,
                "last_error": str(error or "")[:500],
            }
        )
        delivery = _telegram_delivery_state()
        delivery["queued"] = int(delivery.get("queued", 0) or 0) + 1
        delivery["last_mode"] = "queued"
        STATE["last_error"] = (
            f"Telegram queued: critical message saved; outbox={len(outbox)}"
        )
        save_state()
    return True


def send_telegram(text: str) -> bool:
    """Accept a Telegram message for immediate or guaranteed queued delivery.

    True means delivered now OR durably accepted into the ordered outbox. This
    prevents milestone reports from being duplicated while a temporary timeout
    is recovering.
    """
    message = str(text or "")[:3900]
    replaceable = _telegram_replaceable_message(message)

    # Do not let a new result overtake an earlier queued entry. A diagnostic is
    # discarded while the critical outbox is non-empty; the next scan will
    # create a fresh one after entries/results have been delivered.
    if TELEGRAM_OUTBOX_ENABLED and telegram_outbox_depth() > 0:
        if replaceable:
            with STATE_IO_LOCK:
                delivery = _telegram_delivery_state()
                delivery["dropped_diagnostics"] = int(
                    delivery.get("dropped_diagnostics", 0) or 0
                ) + 1
                delivery["last_mode"] = "diagnostic_deferred_for_outbox"
                save_state()
            return False
        return _queue_telegram_message(
            message, "ordered behind an earlier undelivered notification"
        )

    ok, error, attempts_used = _telegram_post_text(
        message,
        attempts_override=1 if replaceable else None,
        read_timeout_override=min(6.0, TELEGRAM_READ_TIMEOUT)
        if replaceable
        else None,
    )
    if ok:
        _telegram_record_success("immediate", attempts_used)
        return True

    _telegram_record_failure(error, attempts_used, "immediate")
    if replaceable:
        with STATE_IO_LOCK:
            delivery = _telegram_delivery_state()
            delivery["dropped_diagnostics"] = int(
                delivery.get("dropped_diagnostics", 0) or 0
            ) + 1
            save_state()
        return False
    return _queue_telegram_message(message, error)


def _format_queued_message(item: Dict[str, Any]) -> str:
    text = str(item.get("text", "") or "")
    created_at = int(item.get("created_at", now_ts()) or now_ts())
    delay = max(0, now_ts() - created_at)
    if delay < 30:
        return text[:3900]
    stamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(created_at))
    prefix = f"⏳ ПОВТОРНАЯ ДОСТАВКА · создано {stamp} · задержка {delay}с\n"
    return (prefix + text)[:3900]


def flush_telegram_outbox() -> Dict[str, Any]:
    if not TELEGRAM_OUTBOX_ENABLED:
        return {"attempted": False, "reason": "disabled", "remaining": 0}
    if not TELEGRAM_OUTBOX_FLUSH_LOCK.acquire(blocking=False):
        return {
            "attempted": False,
            "reason": "already_running",
            "remaining": telegram_outbox_depth(),
        }
    delivered = 0
    try:
        for _ in range(TELEGRAM_OUTBOX_BATCH):
            with STATE_IO_LOCK:
                outbox = STATE.setdefault("telegram_outbox", [])
                if not isinstance(outbox, list) or not outbox:
                    break
                item = dict(outbox[0])
                if int(item.get("next_retry_at", 0) or 0) > now_ts():
                    break

            ok, error, attempts_used = _telegram_post_text(
                _format_queued_message(item)
            )
            message_id = str(item.get("id", ""))
            if ok:
                with STATE_IO_LOCK:
                    current_outbox = STATE.setdefault("telegram_outbox", [])
                    if (
                        isinstance(current_outbox, list)
                        and current_outbox
                        and str(current_outbox[0].get("id", "")) == message_id
                    ):
                        current_outbox.pop(0)
                    _telegram_record_success("outbox", attempts_used)
                delivered += 1
                continue

            with STATE_IO_LOCK:
                current_outbox = STATE.setdefault("telegram_outbox", [])
                if (
                    isinstance(current_outbox, list)
                    and current_outbox
                    and str(current_outbox[0].get("id", "")) == message_id
                ):
                    attempts = int(current_outbox[0].get("attempts", 0) or 0) + 1
                    current_outbox[0]["attempts"] = attempts
                    current_outbox[0]["last_error"] = str(error or "")[:500]
                    backoff = min(
                        TELEGRAM_OUTBOX_MAX_BACKOFF,
                        TELEGRAM_OUTBOX_FLUSH_SECONDS * (2 ** min(attempts, 5)),
                    )
                    current_outbox[0]["next_retry_at"] = now_ts() + backoff
                _telegram_record_failure(error, attempts_used, "outbox")
            break
        return {
            "attempted": True,
            "delivered": delivered,
            "remaining": telegram_outbox_depth(),
        }
    finally:
        TELEGRAM_OUTBOX_FLUSH_LOCK.release()


def telegram_delivery_summary() -> str:
    delivery = _telegram_delivery_state()
    last_success = int(delivery.get("last_success_at", 0) or 0)
    success_age = (
        f"{max(0, now_ts() - last_success)}с назад"
        if last_success
        else "ещё не было"
    )
    return (
        "Telegram delivery: "
        f"outbox={telegram_outbox_depth()} · "
        f"сразу={int(delivery.get('immediate_sent', 0) or 0)} · "
        f"из очереди={int(delivery.get('delivered_from_queue', 0) or 0)} · "
        f"ошибок попыток={int(delivery.get('failed_attempts', 0) or 0)} · "
        f"последняя успешная={success_age}"
    )


def send_telegram_document(data: bytes, filename: str, caption: str = "") -> bool:
    """Retry JSON documents immediately; milestone logic retries them later."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        _telegram_record_failure("env missing for document backup", 1, "document")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    last_error = "unknown Telegram document error"
    attempts_used = 0
    with TELEGRAM_SEND_LOCK:
        for attempt in range(TELEGRAM_SEND_ATTEMPTS):
            attempts_used = attempt + 1
            try:
                files = {
                    "document": (filename, io.BytesIO(data), "application/json"),
                }
                form = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:900]}
                response = requests.post(
                    url,
                    data=form,
                    files=files,
                    timeout=(
                        TELEGRAM_CONNECT_TIMEOUT,
                        max(30.0, TELEGRAM_READ_TIMEOUT),
                    ),
                )
                if response.ok:
                    _telegram_record_success("document", attempts_used)
                    return True
                last_error = (
                    f"HTTP {response.status_code}: "
                    f"{str(getattr(response, 'text', ''))[:250]}"
                )
                retryable = response.status_code in {
                    408, 409, 425, 429, 500, 502, 503, 504
                }
                if not retryable:
                    break
            except Exception as exc:
                last_error = repr(exc)
            if attempt + 1 < TELEGRAM_SEND_ATTEMPTS:
                time.sleep(TELEGRAM_RETRY_BASE_SECONDS * (2 ** attempt))
    _telegram_record_failure(last_error, attempts_used, "document")
    return False


def admin_authorized(key: str) -> bool:
    return bool(ADMIN_KEY) and secrets.compare_digest(str(key or ""), ADMIN_KEY)


def adaptive_closed_count() -> int:
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        return int(conn.execute("SELECT COUNT(*) AS n FROM adaptive_trades").fetchone()["n"])


def adaptive_source_counts() -> Dict[str, int]:
    init_adaptive_db()
    counts = {"live": 0, "shadow": 0, "all": 0}
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT source, COUNT(*) AS n FROM adaptive_trades GROUP BY source"
        ).fetchall()
    for row in rows:
        source = str(row["source"] or "live")
        count = int(row["n"] or 0)
        counts[source] = count
        counts["all"] += count
    return counts


def adaptive_model_data_count() -> int:
    """Rows allowed to train V20.3: current Spike candidates, including hidden guard/model audits."""
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM adaptive_trades "
            "WHERE COALESCE(decision_reason, '') IN (?,?,?)",
            (
                EXHAUSTION_PAPER_REASON,
                SPIKE_ADAPTIVE_BLOCK_REASON,
                SPIKE_STRATEGY_GUARD_REASON,
            ),
        ).fetchone()
    return int(row["n"] or 0)


def paper_validation_metrics() -> Dict[str, Any]:
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT result, pnl_r, symbol FROM adaptive_trades "
            "WHERE COALESCE(decision_reason, '')=? ORDER BY closed_at ASC, id ASC",
            (PAPER_VALIDATION_REASON,),
        ).fetchall()
    metrics = _outcome_metrics(rows)
    metrics["unique_symbols"] = len(
        {normalize_symbol(str(row["symbol"] or "?")) for row in rows}
    )
    return metrics


def paper_lane_metrics() -> Dict[str, Dict[str, Any]]:
    """V17.3 has one frozen direct measured A+ cohort."""
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT result, pnl_r, symbol, strategy FROM adaptive_trades "
            "WHERE COALESCE(decision_reason, '')=? ORDER BY closed_at ASC, id ASC",
            (PAPER_VALIDATION_REASON,),
        ).fetchall()
    return {"FOLLOW_THROUGH": _outcome_metrics(rows)}


def control_validation_metrics() -> Dict[str, Any]:
    """Paired immediate-entry reference; visible but never model-eligible."""
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT result, pnl_r, symbol FROM adaptive_trades "
            "WHERE COALESCE(decision_reason, '')=? ORDER BY closed_at ASC, id ASC",
            (PAPER_CONTROL_REASON,),
        ).fetchall()
    metrics = _outcome_metrics(rows)
    metrics["unique_symbols"] = len(
        {normalize_symbol(str(row["symbol"] or "?")) for row in rows}
    )
    return metrics


def micro_live_circuit_breaker() -> Dict[str, Any]:
    """Stop new real-money alerts after a small, explicit evidence failure.

    This guard is deliberately independent of the adaptive model.  Once it
    blocks, new candidates go back to visible PAPER until the deployment is
    reviewed; changing the model cannot silently reset real-money losses.
    """
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT id, result, pnl_r, symbol, side FROM adaptive_trades "
            "WHERE COALESCE(decision_reason, '')=? "
            "ORDER BY closed_at ASC, id ASC",
            (REAL_MONEY_LIVE_REASON,),
        ).fetchall()

    window_size = max(1, MICRO_LIVE_GUARD_WINDOW)
    recent_rows = rows[-window_size:]
    recent = _outcome_metrics(recent_rows)
    nonprofit_streak = 0
    for row in reversed(rows):
        if str(row["result"] or "") == "profit":
            break
        nonprofit_streak += 1

    equity = peak = 0.0
    max_drawdown = 0.0
    for row in rows:
        equity += float(row["pnl_r"] or 0.0)
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)

    enough_for_window_test = len(recent_rows) >= max(
        1, MICRO_LIVE_GUARD_MIN_OUTCOMES
    )
    checks = {
        "nonprofit_streak_ok": nonprofit_streak
        < max(1, MICRO_LIVE_MAX_NONPROFIT_STREAK),
        "drawdown_ok": max_drawdown <= MICRO_LIVE_MAX_DRAWDOWN_R,
        "rolling_tp3_majority": (
            not enough_for_window_test
            or float(recent.get("success_rate", 0.0) or 0.0) > 0.50
        ),
        "rolling_expectancy_positive": (
            not enough_for_window_test
            or float(recent.get("expectancy_r", 0.0) or 0.0) > 0.0
        ),
    }
    allowed = all(checks.values())
    return {
        "allowed": allowed,
        "checks": checks,
        "closed_micro_live": len(rows),
        "recent": recent,
        "recent_window": window_size,
        "nonprofit_streak": nonprofit_streak,
        "max_drawdown_r": max_drawdown,
    }


def real_money_readiness() -> Dict[str, Any]:
    """Evidence gate for limited LIVE alerts; it never places exchange orders."""
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT id, result, pnl_r, symbol, side FROM adaptive_trades "
            "WHERE COALESCE(decision_reason, '') IN (?,?) "
            "ORDER BY closed_at ASC, id ASC",
            (TRADER_STYLE_PAPER_REASON, EXHAUSTION_PAPER_REASON),
        ).fetchall()

    cumulative = _outcome_metrics(rows)
    recent_window = max(1, REAL_MONEY_RECENT_WINDOW)
    recent_rows = rows[-recent_window:]
    recent = _outcome_metrics(recent_rows)
    unique_symbols = len(
        {normalize_symbol(str(row["symbol"] or "?")) for row in rows}
    )
    equity = peak = 0.0
    max_drawdown = 0.0
    for row in rows:
        equity += float(row["pnl_r"] or 0.0)
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)

    total = int(cumulative.get("n", 0) or 0)
    success = float(cumulative.get("success_rate", 0.0) or 0.0)
    expectancy = float(cumulative.get("expectancy_r", 0.0) or 0.0)
    recent_success = float(recent.get("success_rate", 0.0) or 0.0)
    recent_expectancy = float(recent.get("expectancy_r", 0.0) or 0.0)
    micro_live_guard = micro_live_circuit_breaker()
    by_side: Dict[str, Dict[str, Any]] = {}
    for side in ("LONG", "SHORT"):
        side_rows = [row for row in rows if str(row["side"] or "").upper() == side]
        side_recent_window = max(1, REAL_MONEY_SIDE_RECENT_WINDOW)
        side_recent_rows = side_rows[-side_recent_window:]
        side_metrics = _outcome_metrics(side_rows)
        side_recent = _outcome_metrics(side_recent_rows)
        side_checks = {
            "enough_side_forward": len(side_rows) >= max(1, REAL_MONEY_MIN_SIDE_FORWARD),
            "side_tp3_majority": float(side_metrics.get("success_rate", 0.0) or 0.0)
            > REAL_MONEY_MIN_SUCCESS_RATE,
            "side_expectancy": float(side_metrics.get("expectancy_r", 0.0) or 0.0)
            >= REAL_MONEY_MIN_SIDE_EXPECTANCY_R,
            "side_recent_complete": len(side_recent_rows) >= side_recent_window,
            "side_recent_tp3_majority": float(
                side_recent.get("success_rate", 0.0) or 0.0
            )
            > REAL_MONEY_MIN_SUCCESS_RATE,
            "side_recent_expectancy": float(
                side_recent.get("expectancy_r", 0.0) or 0.0
            )
            > 0.0,
        }
        by_side[side] = {
            "ready": all(side_checks.values()),
            "checks": side_checks,
            "cumulative": side_metrics,
            "recent": side_recent,
            "required": REAL_MONEY_MIN_SIDE_FORWARD,
            "recent_window": side_recent_window,
        }
    checks = {
        "enough_forward": total >= max(1, REAL_MONEY_MIN_FORWARD),
        "enough_unique_symbols": unique_symbols >= max(1, REAL_MONEY_MIN_UNIQUE_SYMBOLS),
        "tp3_majority": success > REAL_MONEY_MIN_SUCCESS_RATE,
        "positive_expectancy": expectancy >= REAL_MONEY_MIN_EXPECTANCY_R,
        "recent_block_complete": len(recent_rows) >= recent_window,
        "recent_tp3_majority": recent_success > REAL_MONEY_MIN_SUCCESS_RATE,
        "recent_positive_expectancy": recent_expectancy > 0.0,
        "drawdown_guard": max_drawdown <= 5.0,
        "micro_live_circuit": bool(micro_live_guard.get("allowed")),
    }
    ready = all(checks.values())
    return {
        "ready": ready,
        "live_flag": REAL_MONEY_SIGNALS,
        "live_enabled": bool(ready and REAL_MONEY_SIGNALS),
        "checks": checks,
        "cumulative": cumulative,
        "recent": recent,
        "unique_symbols": unique_symbols,
        "max_drawdown_r": max_drawdown,
        "required_forward": REAL_MONEY_MIN_FORWARD,
        "required_unique_symbols": REAL_MONEY_MIN_UNIQUE_SYMBOLS,
        "by_side": by_side,
        "micro_live_circuit": micro_live_guard,
    }


def next_model_analysis_target() -> int:
    state = get_model_state()
    normal_target = max(
        MIN_TRAIN_TRADES,
        int(state.last_attempted_closed_count or 0) + max(1, RETRAIN_EVERY),
    )
    if not PRO_QUALITY_FORWARD_ENABLED:
        return normal_target
    paper_count = int(paper_validation_metrics().get("n", 0) or 0)
    remaining = max(0, PAPER_LANE_REQUIRED_OUTCOMES - paper_count)
    return max(normal_target, adaptive_model_data_count() + remaining)


def paper_progress_target(paper_count: Optional[int] = None) -> int:
    count = (
        int(paper_validation_metrics().get("n", 0) or 0)
        if paper_count is None
        else int(paper_count)
    )
    if count < PAPER_PILOT_REQUIRED_OUTCOMES:
        return max(1, PAPER_PILOT_REQUIRED_OUTCOMES)
    if count < PAPER_REVIEW_REQUIRED_OUTCOMES:
        return max(PAPER_PILOT_REQUIRED_OUTCOMES, PAPER_REVIEW_REQUIRED_OUTCOMES)
    return max(PAPER_REVIEW_REQUIRED_OUTCOMES, PAPER_LANE_REQUIRED_OUTCOMES)


def format_paper_cohort_report(window: int = 25) -> str:
    """Fixed-gate forward report. It never changes strategy parameters."""
    size = max(1, int(window))
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        all_rows = conn.execute(
            "SELECT id, result, pnl_r, symbol, side FROM adaptive_trades "
            "WHERE COALESCE(decision_reason, '')=? "
            "ORDER BY closed_at ASC, id ASC",
            (PAPER_VALIDATION_REASON,),
        ).fetchall()
    recent = all_rows[-size:]
    cumulative = _outcome_metrics(all_rows)
    block = _outcome_metrics(recent)
    unique_block = len(
        {normalize_symbol(str(row["symbol"] or "?")) for row in recent}
    )
    unique_total = len(
        {normalize_symbol(str(row["symbol"] or "?")) for row in all_rows}
    )
    n_total = int(cumulative.get("n", 0) or 0)
    first_id = int(recent[0]["id"]) if recent else 0
    last_id = int(recent[-1]["id"]) if recent else 0
    lanes = paper_lane_metrics()

    if n_total == PAPER_PILOT_REQUIRED_OUTCOMES:
        passed = bool(
            int(cumulative.get("profit", 0) or 0)
            > int(cumulative.get("sl", 0) or 0)
            + int(cumulative.get("expired", 0) or 0)
            and float(cumulative.get("expectancy_r", 0.0) or 0.0) > 0.0
            and unique_total >= PAPER_MIN_UNIQUE_SYMBOLS_PILOT
        )
        stage = f"РАННИЙ ЧЕКПОИНТ {n_total}/{PAPER_PILOT_REQUIRED_OUTCOMES}"
        decision = (
            "РАННИЙ ЧЕКПОИНТ ПОЛОЖИТЕЛЬНЫЙ: продолжаем без изменения правил до 50 PAPER."
            if passed
            else "РАННИЙ ЧЕКПОИНТ СЛАБЫЙ: LIVE запрещён; параметры не меняются до ручного разбора."
        )
    elif n_total == PAPER_REVIEW_REQUIRED_OUTCOMES:
        passed = bool(
            int(cumulative.get("profit", 0) or 0)
            > int(cumulative.get("sl", 0) or 0)
            + int(cumulative.get("expired", 0) or 0)
            and float(cumulative.get("expectancy_r", 0.0) or 0.0)
            >= PAPER_PILOT_MIN_EXPECTANCY_R
            and unique_total >= max(12, PAPER_MIN_UNIQUE_SYMBOLS_PILOT)
        )
        stage = f"ПОЛНАЯ FORWARD-ПРОВЕРКА {n_total}/{PAPER_REVIEW_REQUIRED_OUTCOMES}"
        decision = (
            "50 PAPER прошли первичную цель; окончательное решение принимает отдельный readiness gate."
            if passed
            else "50 PAPER не доказали преимущество; реальные деньги запрещены."
        )
    elif n_total >= PAPER_LANE_REQUIRED_OUTCOMES:
        passed = bool(
            int(cumulative.get("profit", 0) or 0)
            > int(cumulative.get("sl", 0) or 0)
            + int(cumulative.get("expired", 0) or 0)
            and float(cumulative.get("expectancy_r", 0.0) or 0.0)
            >= PAPER_PILOT_MIN_EXPECTANCY_R
        )
        stage = f"ПОЛНАЯ ПРОВЕРКА {n_total}/{PAPER_LANE_REQUIRED_OUTCOMES}"
        decision = (
            "PAPER ПРОШЁЛ: проверяем последние 25, разнообразие символов и drawdown перед micro-LIVE."
            if passed
            else "PAPER НЕ ПРОШЁЛ: реальные деньги запрещены; champion не меняется."
        )
    else:
        passed = float(block.get("expectancy_r", 0.0) or 0.0) > 0
        stage = f"ПРОМЕЖУТОЧНЫЙ БЛОК {n_total}/{PAPER_LANE_REQUIRED_OUTCOMES}"
        decision = "Параметры остаются замороженными до полной forward-проверки."

    return (
        "🧾 V17.3.2 — ОТЧЁТ FOLLOW-THROUGH PAPER\n"
        f"Этап: {stage}\n"
        f"Последний блок: ID {first_id}–{last_id} · {_metrics_line(block)} · "
        f"уникальных монет {unique_block}\n"
        f"Накоплено V17.3.2 PAPER: {_metrics_line(cumulative)} · "
        f"уникальных монет {unique_total}\n"
        f"FOLLOW-THROUGH: {_metrics_line(lanes['FOLLOW_THROUGH'])}\n"
        f"{watch_audit_summary()}\n"
        f"Решение: {decision}\n"
        "Важно: отчёт ничего не меняет автоматически и не разрешает реальную торговлю."
    )


def closed_outcome_progress_message(
    signal: Dict[str, Any], result: str, source: Optional[str] = None
) -> str:
    """V18.2.3: post-close stats only for the two official PAPER strategies."""
    active = active_mover_paper_metrics()
    exhaust = squeeze_exhaustion_metrics()
    total = official_two_lane_metrics()
    labels = {"profit": "TP3+", "sl": "SL", "expired": "expired"}
    reason = str(signal.get("shadow_reason", ""))
    lane = (
        "🧲 ACTIVE MOVER"
        if reason == TRADER_STYLE_PAPER_REASON
        else "🚀 SPIKE REGIME"
        if reason == EXHAUSTION_PAPER_REASON
        else "LEGACY/IGNORED"
    )
    official_closed = int(total.get("n", 0) or 0)
    next_checkpoint = (
        ((official_closed // max(1, AUTO_BACKUP_EVERY_CLOSED)) + 1)
        * max(1, AUTO_BACKUP_EVERY_CLOSED)
    )
    return (
        "📊 ДВЕ СТРАТЕГИИ · ЧИСТАЯ СТАТИСТИКА\n"
        f"Последний исход: {labels.get(result, result)} · {lane} · "
        f"{display_symbol(signal.get('symbol', '?'))}\n"
        f"🧲 ACTIVE MOVER: {_metrics_line(active)}\n"
        f"🚀 SPIKE REGIME: {_metrics_line(exhaust)}\n"
        f"🎯 ОБЕ СТРАТЕГИИ: {_metrics_line(total)}\n"
        f"Закрыто в чистом cohort: {official_closed} · следующий checkpoint: {next_checkpoint}."
    )


def maybe_send_auto_backup() -> Dict[str, Any]:
    """Send one JSON backup for every 25 CLOSED user-facing PAPER outcomes.

    V20.3 fixes the old `paper0` bug: milestones are based on the two official
    strategies, not on the disabled V17 Follow-Through cohort or total legacy DB.
    """
    if not AUTO_TELEGRAM_BACKUP or AUTO_BACKUP_EVERY_CLOSED <= 0:
        return {"attempted": False, "sent": False, "reason": "disabled"}
    try:
        total_closed = adaptive_closed_count()
        official = official_two_lane_metrics()
        official_count = int(official.get("n", 0) or 0)
        spike_count = int(squeeze_exhaustion_metrics().get("n", 0) or 0)
        active_count = int(active_mover_paper_metrics().get("n", 0) or 0)
        last_count = int(STATE.get("last_official_backup_count", 0) or 0)

        due = bool(
            official_count >= AUTO_BACKUP_EVERY_CLOSED
            and official_count - last_count >= AUTO_BACKUP_EVERY_CLOSED
        )
        if not due:
            return {
                "attempted": False,
                "sent": False,
                "total_closed": total_closed,
                "official_count": official_count,
                "last_official_backup": last_count,
            }

        filename = (
            f"adaptive_backup_total{total_closed}_clean{official_count}_"
            f"spike{spike_count}_active{active_count}_{int(time.time())}.json"
        )
        caption = (
            f"🧠 V20.3 clean backup · official {official_count} · "
            f"SPIKE {spike_count} · ACTIVE {active_count} · total DB {total_closed}"
        )
        if send_telegram_document(build_export_bytes(), filename, caption):
            STATE["last_official_backup_count"] = official_count
            STATE["last_backup_closed_count"] = total_closed
            save_state()
            return {
                "attempted": True,
                "sent": True,
                "total_closed": total_closed,
                "official_count": official_count,
                "spike_count": spike_count,
                "active_count": active_count,
                "filename": filename,
            }
        return {
            "attempted": True,
            "sent": False,
            "official_count": official_count,
            "error": STATE.get("last_error", "Telegram document send failed"),
        }
    except Exception as e:
        STATE["last_error"] = f"auto backup error: {repr(e)}"
        save_state()
        return {
            "attempted": True,
            "sent": False,
            "error": STATE.get("last_error", repr(e)),
        }


def get_json(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    url = BINGX_BASE_URL + path
    last_err = None
    for attempt in range(API_RETRIES):
        try:
            time.sleep(API_THROTTLE_SECONDS)
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code} {path}"
                time.sleep(0.25 * (attempt + 1))
                continue
            return r.json()
        except Exception as e:
            last_err = f"get_json {path}: {repr(e)}"
            time.sleep(0.35 * (attempt + 1))
    STATE["last_error"] = last_err or "unknown API error"
    save_state()
    return None


def _book_level(level: Any) -> Tuple[float, float]:
    """Read one public order-book level across BingX response variants."""
    try:
        if isinstance(level, dict):
            price = float(level.get("price") or level.get("p") or 0.0)
            quantity = float(
                level.get("quantity") or level.get("qty") or level.get("q") or 0.0
            )
            return price, quantity
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return float(level[0]), float(level[1])
    except Exception:
        pass
    return 0.0, 0.0


def execution_book_snapshot(symbol: str) -> Dict[str, Any]:
    """Best bid/ask and shallow executable depth at the candidate moment.

    The endpoint is called only after the candle setup passes, so it does not
    overload the market-data allowance during the universe scan.  A failed or
    malformed book is a rejection for real-grade PAPER; unknown execution cost
    must never be interpreted as good liquidity.
    """
    normalized = normalize_symbol(symbol)
    bid = ask = bid_qty = ask_qty = 0.0
    response = get_json(
        "/openApi/swap/v2/quote/bookTicker", {"symbol": normalized}
    )
    data: Any = response.get("data") if isinstance(response, dict) else None
    if isinstance(data, list):
        data = data[0] if data else None
    if isinstance(data, dict):
        try:
            bid = float(data.get("bidPrice") or data.get("bid") or 0.0)
            ask = float(data.get("askPrice") or data.get("ask") or 0.0)
            bid_qty = float(
                data.get("bidQty") or data.get("bidQuantity") or 0.0
            )
            ask_qty = float(
                data.get("askQty") or data.get("askQuantity") or 0.0
            )
        except Exception:
            bid = ask = bid_qty = ask_qty = 0.0

    # Fallback also gives several levels for a less fragile depth estimate.
    bids: List[Any] = []
    asks: List[Any] = []
    depth_response: Optional[Dict[str, Any]] = None
    if bid <= 0 or ask <= bid or bid_qty <= 0 or ask_qty <= 0:
        depth_response = get_json(
            "/openApi/swap/v2/quote/depth",
            {"symbol": normalized, "limit": 5},
        )
    depth_data: Any = (
        depth_response.get("data") if isinstance(depth_response, dict) else None
    )
    if isinstance(depth_data, dict):
        bids = list(depth_data.get("bids") or [])[:5]
        asks = list(depth_data.get("asks") or [])[:5]
        if bids and asks:
            bid, bid_qty = _book_level(bids[0])
            ask, ask_qty = _book_level(asks[0])

    if bid <= 0 or ask <= bid:
        return {
            "ok": False,
            "reason": "public BingX bid/ask unavailable",
            "spread_bps": 999.0,
            "depth_usdt": 0.0,
        }

    mid = (bid + ask) / 2.0
    spread_bps = (ask - bid) / max(mid, 1e-12) * 10_000.0
    if bids and asks:
        bid_depth = sum(price * qty for price, qty in map(_book_level, bids))
        ask_depth = sum(price * qty for price, qty in map(_book_level, asks))
    else:
        bid_depth = bid * max(0.0, bid_qty)
        ask_depth = ask * max(0.0, ask_qty)
    depth_usdt = min(bid_depth, ask_depth)
    return {
        "ok": True,
        "reason": "book available",
        "bid": bid,
        "ask": ask,
        "spread_bps": spread_bps,
        "depth_usdt": depth_usdt,
    }


def parse_klines(raw: Any) -> Optional[List[Dict[str, float]]]:
    if not raw:
        return None
    candles: List[Dict[str, float]] = []
    for c in raw:
        try:
            if isinstance(c, dict):
                candles.append({
                    "time": int(c.get("time") or c.get("openTime") or c.get("T") or 0),
                    "open": float(c.get("open")),
                    "high": float(c.get("high")),
                    "low": float(c.get("low")),
                    "close": float(c.get("close")),
                    "volume": float(c.get("volume") or c.get("vol") or 0),
                })
            elif isinstance(c, (list, tuple)) and len(c) >= 6:
                candles.append({
                    "time": int(c[0]),
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                })
        except Exception:
            continue
    candles = [x for x in candles if x["open"] > 0 and x["high"] > 0 and x["low"] > 0 and x["close"] > 0]
    candles.sort(key=lambda x: x["time"])
    return candles if len(candles) >= 30 else None


def get_klines(symbol: str, interval: str, limit: int = 180, cache_seconds: int = 20) -> Optional[List[Dict[str, float]]]:
    symbol = normalize_symbol(symbol)
    key = f"{symbol}:{interval}:{limit}"
    cached = KLINE_CACHE.get(key)
    if cached and time.time() - cached[0] < cache_seconds:
        return cached[1]
    for ep in ["/openApi/swap/v3/quote/klines", "/openApi/swap/v2/quote/klines"]:
        data = get_json(ep, {"symbol": symbol, "interval": interval, "limit": limit})
        if not data:
            continue
        candles = parse_klines(data.get("data"))
        if candles:
            KLINE_CACHE[key] = (time.time(), candles)
            return candles
    KLINE_CACHE[key] = (time.time(), None)
    return None


def is_good_contract_symbol(symbol: str) -> bool:
    s = normalize_symbol(symbol)
    if not s.endswith("-USDT"):
        return False
    b = base_asset(s)
    if any(x in b for x in ["USD", "USDC", "BULL", "BEAR"]):
        return False
    return True


def get_symbols() -> List[str]:
    cached = TICKER_CACHE.get("symbols")
    if cached and time.time() - cached[0] < 600:
        return cached[1] or FALLBACK_SYMBOLS
    data = get_json("/openApi/swap/v2/quote/contracts")
    out: List[str] = []
    if data and isinstance(data.get("data"), list):
        for item in data.get("data", []):
            s = item.get("symbol")
            if s and is_good_contract_symbol(s):
                out.append(normalize_symbol(s))
    if not out:
        out = FALLBACK_SYMBOLS[:]
    # Ensure important user examples are always included if contracts exist/fallback is needed.
    for s in FALLBACK_SYMBOLS:
        if s not in out:
            out.append(s)
    random.shuffle(out)
    quality = [s for s in out if base_asset(s) in QUALITY_BASES]
    rest = [s for s in out if base_asset(s) not in QUALITY_BASES]
    result = (quality + rest)[:MAX_CONTRACTS]
    TICKER_CACHE["symbols"] = (time.time(), result)
    return result

# ============================================================
# Indicators
# ============================================================

def closes(c: List[Dict[str, float]]) -> List[float]:
    return [x["close"] for x in c]


def ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def vwap(candles: List[Dict[str, float]], n: int = 48) -> float:
    part = candles[-n:] if len(candles) >= n else candles
    pv = sum(((x["high"] + x["low"] + x["close"]) / 3) * max(x["volume"], 0) for x in part)
    vv = sum(max(x["volume"], 0) for x in part)
    return pv / vv if vv > 0 else (part[-1]["close"] if part else 0.0)


def atr(candles: List[Dict[str, float]], n: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    part = trs[-n:] if len(trs) >= n else trs
    return sum(part) / len(part) if part else 0.0


def percent_change(candles: List[Dict[str, float]], bars: int) -> float:
    if len(candles) <= bars:
        return 0.0
    a = candles[-bars]["close"]
    b = candles[-1]["close"]
    return (b - a) / a if a else 0.0


def volume_ratio(candles: List[Dict[str, float]], n: int = 30) -> float:
    if len(candles) < n + 2:
        return 1.0
    cur = candles[-1]["volume"]
    avg = sum(x["volume"] for x in candles[-n - 1:-1]) / n
    return cur / avg if avg > 0 else 1.0


def candle_range(c: Dict[str, float]) -> float:
    return max(c["high"] - c["low"], 0.0)


def candle_range_ratio(candles: List[Dict[str, float]], n: int = 20) -> float:
    if len(candles) < n + 2:
        return 1.0
    cur = candle_range(candles[-1])
    avg = sum(candle_range(x) for x in candles[-n - 1:-1]) / n
    return cur / avg if avg > 0 else 1.0


def close_location(c: Dict[str, float]) -> float:
    rng = max(c["high"] - c["low"], 1e-12)
    return (c["close"] - c["low"]) / rng


def prior_compression_ratio(c5: List[Dict[str, float]], n: int = 6) -> float:
    """Lower values mean the market compressed before the impulse.
    A good scalp often comes after short compression then range expansion.
    """
    if len(c5) < n + 8:
        return 1.0
    prior = c5[-n-1:-1]
    older = c5[-n-8:-n-1]
    prior_avg = sum(candle_range(x) for x in prior) / max(len(prior), 1)
    older_avg = sum(candle_range(x) for x in older) / max(len(older), 1)
    return prior_avg / older_avg if older_avg > 0 else 1.0


def micro_structure_break(c1: List[Dict[str, float]], side: str) -> Tuple[bool, str]:
    """Require immediate 1m continuation, not a slow/stuck drift.
    LONG: latest close must break above recent 1m highs.
    SHORT: latest close must break below recent 1m lows.
    """
    if len(c1) < 12:
        return False, "not enough 1m structure"
    last = c1[-1]
    prev_window = c1[-9:-1]
    if side == "LONG":
        ref = max(x["high"] for x in prev_window)
        distance = (last["close"] - ref) / max(ref, 1e-12)
        ok = last["close"] > ref * (1 + EDGE_MIN_BREAKOUT_DISTANCE) and last["close"] > last["open"]
        return ok, f"1m break LONG {distance*100:+.2f}%"
    ref = min(x["low"] for x in prev_window)
    distance = (ref - last["close"]) / max(ref, 1e-12)
    ok = last["close"] < ref * (1 - EDGE_MIN_BREAKOUT_DISTANCE) and last["close"] < last["open"]
    return ok, f"1m break SHORT {distance*100:+.2f}%"


def micro_sweep_reclaim(c1: List[Dict[str, float]], side: str) -> Tuple[bool, str]:
    """Liquidity-grab filter. We want a tiny stop-hunt / failed micro move, then reclaim/reject.
    This is optional but enabled by default because it matches discretionary scalping better.
    """
    if not EDGE_REQUIRE_MICRO_SWEEP:
        return True, "micro sweep disabled"
    if len(c1) < 16:
        return False, "not enough 1m for sweep"
    last = c1[-1]
    recent = c1[-13:-1]
    if side == "LONG":
        swept = min(x["low"] for x in c1[-6:-1]) <= min(x["low"] for x in recent) * 1.001
        reclaimed = last["close"] > last["open"] and close_location(last) >= 0.62
        return swept and reclaimed, "micro sweep/reclaim LONG" if swept and reclaimed else "no micro sweep/reclaim LONG"
    swept = max(x["high"] for x in c1[-6:-1]) >= max(x["high"] for x in recent) * 0.999
    rejected = last["close"] < last["open"] and close_location(last) <= 0.38
    return swept and rejected, "micro sweep/reject SHORT" if swept and rejected else "no micro sweep/reject SHORT"


def tp5_feasible(c5: List[Dict[str, float]], side: str) -> Tuple[bool, str]:
    """If recent velocity cannot realistically cover TP5, skip.
    The examples reached all takes quickly; this blocks slow setups.
    """
    if len(c5) < 8:
        return False, "not enough candles for TP5 feasibility"
    recent_abs_15m = abs(percent_change(c5, 3))
    needed = TP5_MOVE * EDGE_MIN_TP5_FEASIBILITY
    return recent_abs_15m >= needed, f"TP5 feasibility recent15m {recent_abs_15m*100:.2f}% / need {needed*100:.2f}%"


def upper_wick_ratio(c: Dict[str, float]) -> float:
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    rng = max(h - l, 1e-12)
    return (h - max(o, cl)) / rng


def lower_wick_ratio(c: Dict[str, float]) -> float:
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    rng = max(h - l, 1e-12)
    return (min(o, cl) - l) / rng


def trend_state(candles: List[Dict[str, float]]) -> str:
    cs = closes(candles)
    if len(cs) < 60:
        return "UNKNOWN"
    e21 = ema(cs, 21)
    e55 = ema(cs, 55)
    price = cs[-1]
    ch = percent_change(candles, min(20, len(candles) - 1))
    if price > e21 > e55 and ch > 0.003:
        return "UP"
    if price < e21 < e55 and ch < -0.003:
        return "DOWN"
    return "RANGE"


def btc_context() -> Dict[str, Any]:
    c15 = get_klines("BTC-USDT", "15m", 120, cache_seconds=45)
    c1h = get_klines("BTC-USDT", "1h", 120, cache_seconds=120)
    if not c15 or not c1h:
        return {"ok": False, "direction": "UNKNOWN", "text": "BTC data unavailable", "ch1h": 0.0}
    ch1h = percent_change(c15, 4)
    ch6h = percent_change(c15, 24)
    t1h = trend_state(c1h)
    direction = "RANGE"
    if ch1h < -0.004 or ch6h < -0.018 or t1h == "DOWN":
        direction = "BEAR"
    elif ch1h > 0.004 or ch6h > 0.018 or t1h == "UP":
        direction = "BULL"
    return {
        "ok": True,
        "direction": direction,
        "ch1h": ch1h,
        "ch6h": ch6h,
        "t1h": t1h,
        "text": f"BTC {direction}: 1h {ch1h*100:+.2f}%, 6h {ch6h*100:+.2f}%, 1H {t1h}",
    }

# ============================================================
# Hot symbol selection
# ============================================================

def ultra_risk_symbol(symbol: str, c5: List[Dict[str, float]], c15: List[Dict[str, float]]) -> bool:
    b = base_asset(symbol)
    if any(k in b for k in ULTRA_RISK_KEYWORDS):
        return True
    for c in c5[-18:]:
        if (c["high"] - c["low"]) / max(c["open"], 1e-12) > ULTRA_RISK_5M_CANDLE:
            return True
    for c in c15[-10:]:
        if (c["high"] - c["low"]) / max(c["open"], 1e-12) > ULTRA_RISK_15M_CANDLE:
            return True
    return False


def _median(values: Sequence[float]) -> float:
    clean = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not clean:
        return 0.0
    middle = len(clean) // 2
    if len(clean) % 2:
        return clean[middle]
    return (clean[middle - 1] + clean[middle]) / 2.0


def candle_liquidity_snapshot(
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
) -> Dict[str, Any]:
    """Price-agnostic liquidity/print-continuity proxy for V17.2.

    BingX candle volume is used only as an approximate quote-turnover rank
    inside the same scan.  Absolute turnover is deliberately not used as a
    promise of executable size; the hard checks focus on continuous prints and
    reject the volume/range anomalies seen in the user's diagnostics.
    """
    if len(c1) < 35 or len(c5) < 24:
        return {
            "ok": False,
            "reason": "not enough candles for liquidity audit",
            "liquidity_raw": 0.0,
        }
    recent = c1[-60:]
    prices = [max(float(row.get("close", 0.0) or 0.0), 0.0) for row in recent]
    volumes = [max(float(row.get("volume", 0.0) or 0.0), 0.0) for row in recent]
    notionals = [price * volume for price, volume in zip(prices, volumes)]
    quote_60m = sum(notionals)
    median_1m = _median(notionals)
    active_fraction = sum(1 for value in volumes if value > 0.0) / max(1, len(volumes))
    unique_fraction = len({round(value, 12) for value in prices if value > 0.0}) / max(1, len(prices))
    flat_fraction = sum(
        1
        for row in recent
        if (float(row.get("high", 0.0) or 0.0) - float(row.get("low", 0.0) or 0.0))
        / max(float(row.get("close", 0.0) or 0.0), 1e-12)
        <= 1e-7
    ) / max(1, len(recent))
    price = max(prices[-1], 1e-12)
    atr1_pct = atr(c1, 14) / price
    vr1 = volume_ratio(c1, 20)
    rr1 = candle_range_ratio(c1, 20)
    ch3m = abs(percent_change(c1, 3))

    failures: List[str] = []
    if quote_60m <= 0 or median_1m <= 0:
        failures.append("zero turnover proxy")
    if active_fraction < V17_2_MIN_ACTIVE_CANDLE_FRACTION:
        failures.append(f"active candles {active_fraction*100:.0f}%")
    if unique_fraction < V17_2_MIN_UNIQUE_CLOSE_FRACTION:
        failures.append(f"unique closes {unique_fraction*100:.0f}%")
    if flat_fraction > 0.20:
        failures.append(f"flat candles {flat_fraction*100:.0f}%")
    if vr1 > V17_2_MAX_CURRENT_VOL_RATIO:
        failures.append(f"Vol1 anomaly x{vr1:.1f}")
    if rr1 > V17_2_MAX_CURRENT_RANGE_RATIO:
        failures.append(f"Range1 anomaly x{rr1:.1f}")
    if atr1_pct > V17_2_MAX_ATR1_PCT:
        failures.append(f"ATR1 too high {atr1_pct*100:.2f}%")
    if ch3m >= 0.035 and vr1 < 0.25:
        failures.append(f"large move on weak current volume x{vr1:.2f}")

    # Raw score is used only for cross-sectional ranking.  Continuity matters
    # as much as volume, preventing one absurd print from winning the hot list.
    liquidity_raw = (
        math.log1p(max(0.0, quote_60m))
        + 0.35 * math.log1p(max(0.0, median_1m))
        + 3.0 * active_fraction
        + 2.0 * unique_fraction
        - 3.0 * flat_fraction
    )
    return {
        "ok": not failures,
        "reason": "; ".join(failures) if failures else "liquidity continuity passed",
        "liquidity_raw": liquidity_raw,
        "quote_60m": quote_60m,
        "median_quote_1m": median_1m,
        "active_fraction": active_fraction,
        "unique_fraction": unique_fraction,
        "flat_fraction": flat_fraction,
        "atr1_pct": atr1_pct,
        "vol1": vr1,
        "range1": rr1,
    }


def hot_score(symbol: str) -> Tuple[float, str, Dict[str, Any]]:
    """Live-first hot score.
    V13.19 intentionally avoids using 15m candles here to keep scans fast.
    Deep analysis still loads 15m/1h only for selected candidates.
    """
    # Use the same candle depth as deep analysis. The 5m snapshot can be reused
    # there, while 1m is intentionally refreshed before an actual entry.
    c1 = get_klines(symbol, "1m", 120, cache_seconds=8)
    c5 = get_klines(symbol, "5m", 120, cache_seconds=18)
    if not c1 or not c5:
        return 0.0, "no candles", {"ok": False, "reason": "no candles", "liquidity_raw": 0.0}

    liquidity = candle_liquidity_snapshot(symbol, c1, c5)

    ch3m_signed = percent_change(c1, 3)
    ch3m = abs(ch3m_signed)
    ch15m_signed = percent_change(c5, 3)
    ch30m_signed = percent_change(c5, 6)
    ch15m = abs(ch15m_signed)
    ch30m = abs(ch30m_signed)
    vr1 = volume_ratio(c1, 20)
    vr5 = volume_ratio(c5, 20)
    rr1 = candle_range_ratio(c1, 20)
    rr5 = candle_range_ratio(c5, 20)

    # Real-time pressure matters more than old 30m movement.
    live_score = ch3m * 14000 + min(rr1, 5.0) * 14 + min(vr1, 5.0) * 7
    recent_score = ch15m * 700 + ch30m * 320 + min(rr5, 5.0) * 7 + min(vr5, 5.0) * 4

    # Reversal bonus: coin was stretched one way, but 1m flow is now counter-moving.
    reversal_bonus = 0.0
    if REVERSAL_ENABLED:
        if ch30m_signed > REVERSAL_MIN_30M_MOVE and ch3m_signed < -REVERSAL_MIN_LIVE_COUNTER_MOVE:
            reversal_bonus = 35 + abs(ch3m_signed) * 7000
        elif ch30m_signed < -REVERSAL_MIN_30M_MOVE and ch3m_signed > REVERSAL_MIN_LIVE_COUNTER_MOVE:
            reversal_bonus = 35 + abs(ch3m_signed) * 7000

    score = live_score + recent_score + reversal_bonus

    # Penalize coins that moved earlier but are dead right now.
    dead_now = ch3m < HOT_MIN_LIVE_MOVE_3M and rr1 < 0.35 and vr1 < 0.45
    stale = ch30m >= 0.012 and ch3m < HOT_MIN_LIVE_MOVE_3M and rr1 < HOT_MIN_LIVE_RANGE_OR_VOLUME and vr1 < HOT_MIN_LIVE_RANGE_OR_VOLUME
    if HOT_STALE_PENALTY_ENABLED and stale and reversal_bonus <= 0:
        score *= 0.25
    if dead_now and reversal_bonus <= 0:
        score *= 0.12

    # Huge volume without range/movement is absorption, not immediate scalp flow.
    if vr1 > 20 and ch3m < 0.0005 and rr1 < 0.5:
        score *= 0.20

    if base_asset(symbol) in QUALITY_BASES:
        score += 2

    live_tag = "LIVE" if not dead_now and (ch3m >= HOT_MIN_LIVE_MOVE_3M or rr1 >= 0.8 or vr1 >= 0.8 or reversal_bonus > 0) else "STALE"
    mode_tag = "REV" if reversal_bonus > 0 else "MOM"
    note = (
        f"{live_tag}/{mode_tag}: 1m3 {ch3m_signed*100:+.2f}%, "
        f"15m {ch15m_signed*100:+.2f}%, 30m {ch30m_signed*100:+.2f}%, "
        f"vol1 x{vr1:.2f}, vol5 x{vr5:.2f}, range1 x{rr1:.2f}, range5 x{rr5:.2f}"
    )
    if V17_2_LIQUIDITY_FIRST_ENABLED and not bool(liquidity.get("ok")):
        score = 0.0
        note += f" · LIQ REJECT: {liquidity.get('reason', 'unknown')}"
    return score, note, liquidity

def select_hot_symbols(symbols: List[str]) -> Tuple[List[str], List[str]]:
    LIQUIDITY_SCAN_CACHE.clear()
    scored: List[Tuple[float, str, str, Dict[str, Any]]] = []
    notes: List[str] = []
    # Rotate through the complete contract universe instead of repeatedly
    # checking the same cached first 220 names for ten minutes.  Quality bases
    # remain in the universe, while every listed contract gets a timely pass.
    if symbols:
        cursor = int(STATE.get("symbol_scan_cursor", 0) or 0) % len(symbols)
        take = min(len(symbols), max(1, MAX_ANALYZE_SYMBOLS))
        scan_symbols = [symbols[(cursor + offset) % len(symbols)] for offset in range(take)]
        STATE["symbol_scan_cursor"] = (cursor + take) % len(symbols)
    else:
        scan_symbols = []

    # V16.6.5: network-bound candle requests must not run one symbol at a time.
    # The old sequential pass took more than three minutes, so a live impulse
    # often disappeared before deep analysis reached that symbol.
    if HOT_SCAN_WORKERS <= 1 or len(scan_symbols) <= 1:
        results = []
        for sym in scan_symbols:
            try:
                results.append((sym, hot_score(sym), None))
            except Exception as e:
                results.append((sym, None, e))
    else:
        results = []
        with ThreadPoolExecutor(
            max_workers=min(HOT_SCAN_WORKERS, len(scan_symbols)),
            thread_name_prefix="hot-scan",
        ) as pool:
            future_map = {pool.submit(hot_score, sym): sym for sym in scan_symbols}
            for future in as_completed(future_map):
                sym = future_map[future]
                try:
                    results.append((sym, future.result(), None))
                except Exception as e:
                    results.append((sym, None, e))

    for sym, result, error in results:
        if error is not None:
            STATE["last_error"] = f"hot_score {sym}: {repr(error)}"
            continue
        if result is None:
            continue
        sc, note, liquidity = result
        if bool(liquidity.get("ok")):
            scored.append((sc, sym, note, liquidity))

    audit = _watch_audit_state()
    audit["liquidity_checked"] = int(audit.get("liquidity_checked", 0) or 0) + len(results)
    audit["liquidity_rejected"] = int(audit.get("liquidity_rejected", 0) or 0) + max(
        0, len(results) - len(scored)
    )
    if not scored:
        return [], notes

    by_liquidity = sorted(
        scored,
        key=lambda row: float(row[3].get("liquidity_raw", 0.0) or 0.0),
        reverse=True,
    )
    keep_count = max(
        min(HOT_SYMBOLS_TO_ANALYZE, len(by_liquidity)),
        min(
            len(by_liquidity),
            int(math.ceil(len(by_liquidity) * max(0.10, min(1.0, V17_2_LIQUIDITY_KEEP_FRACTION)))),
        ),
    )
    liquid_pool = by_liquidity[:keep_count]
    pool_size = max(1, len(by_liquidity) - 1)
    liquidity_rank = {
        sym: 1.0 - (index / pool_size)
        for index, (_, sym, _, _) in enumerate(by_liquidity)
    }
    adjusted: List[Tuple[float, str, str, Dict[str, Any]]] = []
    for hot, sym, note, liquidity in liquid_pool:
        percentile = float(liquidity_rank.get(sym, 0.0))
        liquidity["rank_percentile"] = percentile
        LIQUIDITY_SCAN_CACHE[normalize_symbol(sym)] = dict(liquidity)
        adjusted_score = float(hot) + 24.0 * percentile
        adjusted.append((adjusted_score, sym, note, liquidity))
    adjusted.sort(reverse=True, key=lambda row: row[0])
    selected = [row[1] for row in adjusted[:HOT_SYMBOLS_TO_ANALYZE]]
    audit["liquidity_passed"] = int(audit.get("liquidity_passed", 0) or 0) + len(selected)

    for adjusted_score, sym, note, liquidity in adjusted[:12]:
        notes.append(
            f"{display_symbol(sym)} quality {adjusted_score:.1f} · "
            f"liq p{float(liquidity.get('rank_percentile', 0.0))*100:.0f} · "
            f"turn60≈{float(liquidity.get('quote_60m', 0.0) or 0.0):.0f}: {note}"
        )

    min_live_candidates = min(HOT_SYMBOLS_TO_ANALYZE, max(1, MIN_HOT_CANDIDATES))
    if len(selected) < min_live_candidates:
        # Use only markets that passed hard continuity/anomaly checks.  The bot
        # may report a smaller pool; it must never fill it with rejected junk.
        seen = set(selected)
        for _, sym, _, _ in by_liquidity:
            if sym not in seen:
                selected.append(sym)
                seen.add(sym)
            if len(selected) >= min_live_candidates:
                break
    return selected[:HOT_SYMBOLS_TO_ANALYZE], notes

# ============================================================
# Setup logic
# ============================================================

def realtime_pressure_ok(c1: List[Dict[str, float]], side: str) -> Tuple[bool, str, Dict[str, float]]:
    """Live 1m pressure gate.
    This is the key V13.18 fix: a signal is allowed only if the coin is moving right now.
    Expired signals usually came from patterns where the flow had already stopped.
    """
    if len(c1) < 30:
        return False, "not enough 1m pressure data", {}

    last = c1[-1]
    prev = c1[-2]
    ch2m = (last["close"] - c1[-3]["close"]) / max(c1[-3]["close"], 1e-12)
    ch3m = percent_change(c1, 3)
    rr1 = candle_range_ratio(c1, 20)
    vr1 = volume_ratio(c1, 20)
    loc = close_location(last)
    body = abs(last["close"] - last["open"]) / max(last["high"] - last["low"], 1e-12)

    same_two_long = last["close"] > last["open"] and prev["close"] >= prev["open"]
    same_two_short = last["close"] < last["open"] and prev["close"] <= prev["open"]

    metrics = {"ch2m": ch2m, "ch3m": ch3m, "range1": rr1, "vol1": vr1, "loc": loc, "body": body}

    if rr1 < REALTIME_MIN_1M_RANGE_RATIO:
        return False, f"1m range not live x{rr1:.2f}", metrics
    if vr1 < REALTIME_MIN_1M_VOLUME_RATIO:
        return False, f"1m volume not live x{vr1:.2f}", metrics
    if body < 0.35:
        return False, f"1m body weak {body:.2f}", metrics

    if side == "LONG":
        if ch2m < REALTIME_MIN_2M_MOVE:
            return False, f"LONG 2m pressure weak {ch2m*100:.2f}%", metrics
        if loc < REALTIME_CLOSE_LOCATION_LONG:
            return False, f"LONG 1m close not near high {loc:.2f}", metrics
        if REALTIME_REQUIRE_TWO_1M_CANDLES and not same_two_long:
            return False, "LONG lacks two 1m bullish candles", metrics
    else:
        if ch2m > -REALTIME_MIN_2M_MOVE:
            return False, f"SHORT 2m pressure weak {ch2m*100:.2f}%", metrics
        if loc > REALTIME_CLOSE_LOCATION_SHORT:
            return False, f"SHORT 1m close not near low {loc:.2f}", metrics
        if REALTIME_REQUIRE_TWO_1M_CANDLES and not same_two_short:
            return False, "SHORT lacks two 1m bearish candles", metrics

    return True, f"live pressure ok: 2m {ch2m*100:+.2f}%, 3m {ch3m*100:+.2f}%, range1 x{rr1:.2f}, vol1 x{vr1:.2f}", metrics


def fast_context_ok(c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], side: str, vol: float) -> Tuple[bool, str, Dict[str, float]]:
    """V13.19 fast context.
    Allows two professional scalp types:
    1) continuation: 15m/30m and 1m pressure agree;
    2) blow-off reversal: 30m is stretched one way, but live 1m pressure flips hard the other way.
    This fixes the prior issue where TIMI-like +16% 30m then -1% 1m dump was rejected as no_fast_short.
    """
    if len(c1) < 20 or len(c5) < 36 or len(c15) < 24:
        return False, "not enough candles", {}

    ch15m = percent_change(c5, 3)
    ch30m = percent_change(c5, 6)
    ch3m_1m = percent_change(c1, 3)
    rr = candle_range_ratio(c5, 20)
    compression = prior_compression_ratio(c5, 6)
    last = c5[-1]
    candle_move = (last["high"] - last["low"]) / max(last["open"], 1e-12)

    metrics = {
        "ch15m": ch15m,
        "ch30m": ch30m,
        "ch3m_1m": ch3m_1m,
        "range_ratio": rr,
        "compression": compression,
        "candle_move": candle_move,
        "vol": vol,
        "setup_mode": "unknown",
    }

    if candle_move > FAST_MAX_SPREAD_PROXY:
        return False, f"last 5m candle too wide/chase risk {candle_move*100:.2f}%", metrics

    if compression > EDGE_MIN_PRIOR_COMPRESSION and rr < 1.75:
        return False, f"no compression-to-expansion edge: compression x{compression:.2f}, range x{rr:.2f}", metrics

    micro_ok, micro_reason = micro_structure_break(c1, side)
    if not micro_ok:
        return False, micro_reason, metrics

    pressure_ok, pressure_reason, pressure_metrics = realtime_pressure_ok(c1, side)
    metrics.update(pressure_metrics)
    if not pressure_ok:
        return False, pressure_reason, metrics

    sweep_ok, sweep_reason = micro_sweep_reclaim(c1, side)
    if not sweep_ok:
        return False, sweep_reason, metrics

    feasible_ok, feasible_reason = tp5_feasible(c5, side)
    if not feasible_ok:
        return False, feasible_reason, metrics

    # continuation vs blow-off reversal classification
    if side == "LONG":
        continuation = ch15m >= FAST_MIN_15M_MOVE and ch30m >= FAST_MIN_30M_MOVE and ch3m_1m >= FAST_MIN_1M_CONFIRM
        reversal = REVERSAL_ENABLED and ch30m <= -REVERSAL_MIN_30M_MOVE and ch3m_1m >= REVERSAL_MIN_LIVE_COUNTER_MOVE
        if not (continuation or reversal):
            return False, f"no LONG edge: 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, 1m3 {ch3m_1m*100:+.2f}%", metrics
        if ch30m > FAST_MAX_30M_MOVE:
            return False, f"late LONG chase 30m {ch30m*100:.2f}%", metrics
        if last["close"] <= last["open"] and not reversal:
            return False, "last 5m not bullish for continuation", metrics
        if close_location(last) < CLOSE_LOCATION_MIN_LONG and not reversal:
            return False, f"LONG close location weak {close_location(last):.2f}", metrics
        metrics["setup_mode"] = "REVERSAL_LONG" if reversal else "CONTINUATION_LONG"
    else:
        btc_dump_context = BTC_DUMP_SHORT_BIAS_ENABLED and ch15m <= -FAST_MIN_15M_MOVE * 0.70 and ch3m_1m <= -FAST_MIN_1M_CONFIRM
        continuation = (ch15m <= -FAST_MIN_15M_MOVE and ch30m <= -FAST_MIN_30M_MOVE and ch3m_1m <= -FAST_MIN_1M_CONFIRM) or btc_dump_context
        reversal = REVERSAL_ENABLED and ch30m >= REVERSAL_MIN_30M_MOVE and ch3m_1m <= -REVERSAL_MIN_LIVE_COUNTER_MOVE
        if not (continuation or reversal):
            return False, f"no SHORT edge: 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, 1m3 {ch3m_1m*100:+.2f}%", metrics
        if ch30m < -FAST_MAX_30M_MOVE:
            # During market-wide dumps many examples realize quickly to the downside.
            # Still avoid blind chasing: require a small bounce/reject structure before continuing.
            recent_low = min(x["low"] for x in c5[-8:])
            bounce = (max(x["high"] for x in c5[-5:]) - recent_low) / max(recent_low, 1e-12)
            if not (BTC_DUMP_SHORT_BIAS_ENABLED and ch30m >= -SHORT_DUMP_ALLOW_EXTENDED_30M and ch3m_1m <= SHORT_DUMP_MIN_LIVE_1M3 and bounce >= SHORT_DUMP_MIN_BOUNCE):
                return False, f"late SHORT chase 30m {ch30m*100:.2f}%", metrics
            metrics["dump_bounce"] = bounce
        if last["close"] >= last["open"] and not reversal:
            return False, "last 5m not bearish for continuation", metrics
        if close_location(last) > CLOSE_LOCATION_MAX_SHORT and not reversal:
            return False, f"SHORT close location weak {close_location(last):.2f}", metrics
        metrics["setup_mode"] = "REVERSAL_SHORT" if reversal else "CONTINUATION_SHORT"

    # For fast scalps, live velocity/range can bypass weak 15m volume.
    live_bypass = abs(ch3m_1m) >= LIVE_BYPASS_VOLUME_MOVE or metrics.get("range1", 1.0) >= LIVE_BYPASS_RANGE_RATIO

    if rr < FAST_MIN_RANGE_RATIO and not live_bypass:
        return False, f"range expansion weak x{rr:.2f}", metrics
    if vol < FAST_MIN_VOLUME_RATIO and not live_bypass:
        return False, f"volume weak x{vol:.2f}", metrics

    return True, (
        f"{metrics['setup_mode']} edge ok: 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, "
        f"1m3 {ch3m_1m*100:+.2f}%, range5 x{rr:.2f}, vol15 x{vol:.2f}, "
        f"range1 x{metrics.get('range1', 1.0):.2f}, vol1 x{metrics.get('vol1', 1.0):.2f}; "
        f"{micro_reason}; {pressure_reason}; {sweep_reason}; {feasible_reason}"
    ), metrics


def long_live_stats_ok() -> Tuple[bool, str]:
    """Evaluate only comparable, evidence-qualified LIVE LONG outcomes.

    Older versions used every historical LONG, including trades created before
    the joint Vol1/Range1/directional gate existed. That legacy mixture could
    keep blocking the improved LONG family even though the 125-row audit showed
    positive expectancy for evidence-qualified LIVE LONGs.
    """
    if not LONG_STATS_PROTECTION:
        return True, "long stats protection disabled"
    current = time.time()
    cached_at = float(LONG_STATS_CACHE.get("ts", 0.0) or 0.0)
    if current - cached_at < 30.0:
        value = LONG_STATS_CACHE.get("value", (True, "cached"))
        return bool(value[0]), str(value[1])

    try:
        init_adaptive_db()
        with _LOCK, _connect() as conn:
            rows = conn.execute(
                "SELECT result, pnl_r, features_json FROM adaptive_trades "
                "WHERE source='live' AND side='LONG' ORDER BY id DESC LIMIT 200"
            ).fetchall()

        comparable: List[Any] = []
        for row in rows:
            features = json.loads(row["features_json"] or "{}")
            directional_3m = float(features.get("edge_3m", 0.0) or 0.0) * 0.25
            vol1 = float(features.get("vol1", 0.0) or 0.0) * 8.0
            range1 = float(features.get("range1", 0.0) or 0.0) * 8.0
            if (
                directional_3m >= DATA_MIN_DIRECTIONAL_3M
                and vol1 >= DATA_MIN_VOL1
                and range1 >= DATA_MIN_RANGE1
            ):
                comparable.append(row)
            if len(comparable) >= max(1, LONG_STATS_WINDOW):
                break

        closed = len(comparable)
        if closed < max(1, LONG_STATS_MIN_CLOSED):
            result = (True, f"not enough comparable LIVE LONG stats: {closed}")
        else:
            profits = sum(1 for row in comparable if str(row["result"]) == "profit")
            wr = profits / max(closed, 1) * 100.0
            expectancy = sum(float(row["pnl_r"] or 0.0) for row in comparable) / max(closed, 1)
            accepted = bool(
                wr >= LONG_STATS_MIN_WR
                and expectancy >= LONG_STATS_MIN_EXPECTANCY_R
            )
            result = (
                accepted,
                f"evidence LIVE LONG: {profits}/{closed} TP3+ · "
                f"success {wr:.1f}% · {expectancy:+.3f}R",
            )
    except Exception as exc:
        result = (True, f"LONG evidence stats unavailable; safety gates remain: {repr(exc)}")

    LONG_STATS_CACHE["ts"] = current
    LONG_STATS_CACHE["value"] = result
    return result


def professional_long_reclaim_gate(
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
    c15: List[Dict[str, float]],
    btc: Dict[str, Any],
    metrics: Dict[str, float],
    setup_mode: str,
    e1: float,
    e5: float,
    vw5: float,
) -> Tuple[bool, str]:
    """Strict LONG-only repair.

    Live results showed LONG was buying weak bounces / late pumps.
    A valid LONG now needs a real reclaim pattern:
    - BTC must not be BEAR by default;
    - strong 1m pressure, close near high, volume/range alive;
    - price must reclaim 1m EMA and be near/above 5m EMA/VWAP;
    - no buying vertical 15m/30m extension unless there was a controlled pullback;
    - prefer liquidity sweep / higher-low reclaim.
    """
    if len(c1) < 24 or len(c5) < 24 or len(c15) < 10:
        return False, "LONG gate: not enough candles"

    btc_dir = str(btc.get("direction", "UNKNOWN"))
    btc_ch1h = float(btc.get("ch1h", 0.0))
    btc_ch6h = float(btc.get("ch6h", 0.0))

    last1 = c1[-1]
    prev1 = c1[-2]
    price = last1["close"]
    ch3m = percent_change(c1, 3)
    ch15m = metrics.get("ch15m", percent_change(c5, 3))
    ch30m = metrics.get("ch30m", percent_change(c5, 6))
    vol1 = metrics.get("vol1", volume_ratio(c1, 20))
    range1 = metrics.get("range1", candle_range_ratio(c1, 20))
    loc1 = close_location(last1)

    # BTC bearish does not automatically forbid LONG. But a LONG against a bearish BTC
    # must be a leader/relative-strength coin, not a weak bounce. This is how coins like
    # VELVET can still be traded LONG while the general market is heavy.
    bear_rs_long = False
    if btc_dir == "BEAR":
        rel_strength_1h = ch15m - btc_ch1h
        bear_rs_long = (
            LONG_ALLOW_BEAR_RELATIVE_STRENGTH
            and ch15m >= LONG_BEAR_MIN_ALT_15M
            and ch30m >= LONG_BEAR_MIN_ALT_30M
            and ch3m >= LONG_BEAR_MIN_1M3
            and rel_strength_1h >= LONG_BEAR_MIN_REL_STRENGTH_1H
            and vol1 >= LONG_BEAR_MIN_VOL1
            and range1 >= LONG_BEAR_MIN_RANGE1
            and loc1 >= LONG_BEAR_MIN_CLOSE_LOCATION
        )
        if LONG_BLOCK_BTC_BEAR and not bear_rs_long:
            return False, (
                f"LONG gate: BTC BEAR and coin has no relative strength: "
                f"alt15m {ch15m*100:+.2f}%, alt30m {ch30m*100:+.2f}%, "
                f"1m3 {ch3m*100:+.2f}%, rel1h {rel_strength_1h*100:+.2f}%"
            )

    if ch3m < LONG_MIN_3M_CONFIRM:
        return False, f"LONG gate: weak 3m confirm {ch3m*100:.2f}%"
    if btc_dir == "BEAR" and LONG_ALLOW_BEAR_RELATIVE_STRENGTH and not bear_rs_long:
        return False, (
            f"LONG gate: BTC BEAR, only relative-strength longs allowed; "
            f"alt15m {ch15m*100:+.2f}%, alt30m {ch30m*100:+.2f}%, 1m3 {ch3m*100:+.2f}%"
        )
    if vol1 < LONG_MIN_1M_VOLUME_RATIO:
        return False, f"LONG gate: weak 1m volume x{vol1:.2f}"
    if range1 < LONG_MIN_1M_RANGE_RATIO:
        return False, f"LONG gate: weak 1m range x{range1:.2f}"
    if loc1 < LONG_MIN_CLOSE_LOCATION:
        return False, f"LONG gate: 1m close not strong {loc1:.2f}"
    if last1["close"] <= last1["open"]:
        return False, "LONG gate: last 1m not bullish"
    if prev1["close"] < prev1["open"] and last1["close"] <= prev1["open"]:
        return False, "LONG gate: did not reclaim prior red candle"

    # Must reclaim micro trend. For continuation LONG, also avoid being below 5m EMA/VWAP.
    if price < e1 * (1 + RECLAIM_BUFFER):
        return False, "LONG gate: no 1m EMA reclaim"
    if setup_mode == "CONTINUATION_LONG" and (price < e5 * (1 + RECLAIM_BUFFER) or price < vw5 * (1 + RECLAIM_BUFFER)):
        return False, "LONG gate: no 5m EMA/VWAP reclaim"

    # Liquidity sweep / higher-low reclaim. This avoids buying a random bounce with no trap.
    recent = c1[-16:-4]
    last_zone = c1[-5:]
    swept_low = min(x["low"] for x in last_zone[:-1]) <= min(x["low"] for x in recent) * 1.0015 if recent else False
    reclaimed = last1["close"] > max(x["close"] for x in c1[-5:-1]) and loc1 >= LONG_MIN_CLOSE_LOCATION
    higher_low = min(x["low"] for x in c1[-4:]) > min(x["low"] for x in c1[-10:-4]) * 0.998 if len(c1) >= 12 else False

    if LONG_REQUIRE_SWEEP_OR_RECLAIM and not (swept_low or reclaimed):
        return False, "LONG gate: no sweep/reclaim trigger"
    if LONG_REQUIRE_HIGHER_LOW and not (higher_low or swept_low):
        return False, "LONG gate: no higher-low/sweep structure"

    # Anti-chase: after a big pump, only buy if there was a real controlled pullback first.
    recent_high = max(x["high"] for x in c5[-18:])
    recent_low = min(x["low"] for x in c5[-10:])
    pullback = (recent_high - recent_low) / max(recent_high, 1e-12)
    if ch15m > LONG_MAX_15M_CHASE or ch30m > LONG_MAX_30M_CHASE:
        if not (LONG_MIN_PULLBACK_AFTER_PUMP <= pullback <= LONG_MAX_PULLBACK_AFTER_PUMP and (swept_low or reclaimed)):
            return False, f"LONG gate: late pump chase blocked 15m {ch15m*100:.2f}%, 30m {ch30m*100:.2f}%, pullback {pullback*100:.2f}%"

    # Avoid buying into a distribution wick.
    last5 = c5[-1]
    if upper_wick_ratio(last5) > 0.48 and close_location(last5) < 0.68:
        return False, "LONG gate: 5m upper wick/distribution"

    return True, (
        f"LONG professional gate ok: BTC {btc_dir}, 3m {ch3m*100:+.2f}%, "
        f"vol1 x{vol1:.2f}, range1 x{range1:.2f}, closeLoc {loc1:.2f}, "
        f"bearRS {bear_rs_long}, sweep {swept_low}, reclaim {reclaimed}, higherLow {higher_low}"
    )

def fast_burst_setup(symbol: str, c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], c1h: List[Dict[str, float]], btc: Dict[str, Any], side: str) -> Optional[Dict[str, Any]]:
    """Scalping Edge setup: no trend prediction.
    We only require a tradable micro-event: fresh imbalance + micro sweep/reclaim + immediate continuation.
    BTC/1H are informational, not directional gates, except violent BTC shock.
    """
    if not FAST_BURST_ENABLED:
        return None
    if len(c1) < 30 or len(c5) < 48 or len(c15) < 40 or len(c1h) < 60:
        return None

    price = c1[-1]["close"]
    e5 = ema(closes(c5), 21)
    e1 = ema(closes(c1), 9)
    vw5 = vwap(c5, 36)
    vol = volume_ratio(c15, 24)
    t1h = trend_state(c1h)

    # Market phase is not traded as a prediction. BTC is a context filter:
    # - during BTC shock down, avoid LONG unless the coin later passes relative-strength LONG gate;
    # - allow SHORT during dump because that is exactly when many alts realize quickly.
    btc_ch1h = float(btc.get("ch1h", 0.0))
    if abs(btc_ch1h) >= BTC_SHOCK_15M_BLOCK and side == "LONG":
        # Do not hard-block here; professional_long_reclaim_gate can still allow an exceptional RS long.
        pass

    fast_ok, fast_reason, metrics = fast_context_ok(c1, c5, c15, side, vol)
    if not fast_ok:
        return None
    setup_mode = str(metrics.get("setup_mode", ""))
    is_reversal = setup_mode.startswith("REVERSAL")

    if side == "LONG":
        long_gate_ok, long_gate_reason = professional_long_reclaim_gate(symbol, c1, c5, c15, btc, metrics, setup_mode, e1, e5, vw5)
        if not long_gate_ok:
            return None
        metrics["long_gate_reason"] = long_gate_reason

    last5 = c5[-1]
    prev5 = c5[-2]

    if side == "LONG":
        recent_high = max(x["high"] for x in c5[-18:])
        pullback_low = min(x["low"] for x in c5[-10:])
        pullback = (recent_high - pullback_low) / max(recent_high, 1e-12)
        if pullback < PULLBACK_MIN or pullback > PULLBACK_MAX:
            return None
        if is_reversal:
            # Blow-off reversal LONG: do not wait for 5m EMA/VWAP reclaim; that is often too late.
            # Require live 1m reclaim only; fast_context already confirmed pressure and micro break.
            if price < e1:
                return None
        else:
            if price < e1 or price < e5 * (1 + RECLAIM_BUFFER) or price < vw5 * (1 + RECLAIM_BUFFER):
                return None
            # Entry must be continuation, not a mid-range hesitation.
            if last5["close"] <= prev5["high"] * 0.999 and last5["close"] <= prev5["close"]:
                return None
            if upper_wick_ratio(last5) > 0.42 and close_location(last5) < 0.72:
                return None
        level = min(pullback_low, min(x["low"] for x in c1[-12:]))
        strategy = "PRO_SCALPING_EDGE_LONG"
        trade_type = "SCALPING EDGE LONG"
        reason = (
            f"SCALPING EDGE LONG: не прогноз рынка, а короткая ситуация. "
            f"Режим {setup_mode}: свежий дисбаланс вверх, микро-откат/перехват {pullback*100:.2f}%, "
            f"live 1m pressure, sweep/reclaim и немедленное продолжение. {fast_reason}. "
            f"{metrics.get('long_gate_reason', '')}."
        )
    else:
        recent_low = min(x["low"] for x in c5[-18:])
        bounce_high = max(x["high"] for x in c5[-10:])
        pullback = (bounce_high - recent_low) / max(recent_low, 1e-12)
        if pullback < PULLBACK_MIN or pullback > PULLBACK_MAX:
            return None
        if is_reversal:
            # Blow-off reversal SHORT: do not wait for 5m EMA/VWAP loss; that is often too late.
            # Require live 1m reject only; fast_context already confirmed pressure and micro break.
            if price > e1:
                return None
        else:
            if price > e1 or price > e5 * (1 - RECLAIM_BUFFER) or price > vw5 * (1 - RECLAIM_BUFFER):
                return None
            if last5["close"] >= prev5["low"] * 1.001 and last5["close"] >= prev5["close"]:
                return None
            if lower_wick_ratio(last5) > 0.42 and close_location(last5) > 0.28:
                return None
        level = max(bounce_high, max(x["high"] for x in c1[-12:]))
        strategy = "PRO_SCALPING_EDGE_SHORT"
        trade_type = "SCALPING EDGE SHORT"
        reason = (
            f"SCALPING EDGE SHORT: не прогноз рынка, а короткая ситуация. "
            f"Режим {setup_mode}: свежий дисбаланс вниз, микро-отскок/перехват {pullback*100:.2f}%, "
            f"live 1m pressure и немедленное продолжение. {fast_reason}."
        )

    strong = vol >= 1.55 and metrics.get("range_ratio", 1.0) >= 1.55 and abs(metrics.get("ch3m_1m", 0)) >= FAST_MIN_1M_CONFIRM * 1.4
    score = 74
    score += min(12, int(abs(metrics.get("ch15m", 0)) * 650))
    score += min(10, int(abs(metrics.get("ch30m", 0)) * 430))
    score += min(8, int((vol - 1.0) * 7))
    score += min(8, int((metrics.get("range_ratio", 1.0) - 1.0) * 7))
    score += min(8, int((metrics.get("vol1", 1.0) - 1.0) * 7))
    score += min(8, int((metrics.get("range1", 1.0) - 1.0) * 7))
    # Market phase does not add or subtract. Only actual speed/liquidity edge matters.
    if strong:
        score += 7
    if base_asset(symbol) in QUALITY_BASES:
        score += 1
    score = max(0, min(100, score))

    return {
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "trade_type": trade_type,
        "score": score,
        "grade": "A+" if score >= A_PLUS_MIN_SCORE and vol >= 1.45 else "B",
        "entry": price,
        "level": level,
        "reason": reason,
        "pullback": pullback,
        "volume_ratio": vol,
        "range_ratio": metrics.get("range_ratio", 1.0),
        "compression": metrics.get("compression", 1.0),
        "ch15m": metrics.get("ch15m", 0.0),
        "ch30m": metrics.get("ch30m", 0.0),
        "ch3m_1m": metrics.get("ch3m_1m", 0.0),
        "vol1": metrics.get("vol1", 1.0),
        "range1": metrics.get("range1", 1.0),
        "ch2m": metrics.get("ch2m", 0.0),
        "setup_mode": setup_mode,
        "t1h": t1h,
        "btc_text": btc.get("text", ""),
    }



def instant_edge_setup(symbol: str, c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], c1h: List[Dict[str, float]], btc: Dict[str, Any], side: str) -> Optional[Dict[str, Any]]:
    """V13.24 fallback: instant momentum/reclaim scalp.

    This is for situations visible in diagnostics such as SYRUP/FOLKS:
    live 1m impulse is present, but the older fast_burst setup rejects the trade because it
    waits for a perfect 5m pullback/reclaim. We still keep strict quality filters after this.
    """
    if not INSTANT_EDGE_ENABLED:
        return None
    if len(c1) < 35 or len(c5) < 36 or len(c15) < 12 or len(c1h) < 40:
        return None

    price = c1[-1]["close"]
    last1 = c1[-1]
    prev1 = c1[-2]
    ch3m = percent_change(c1, 3)
    ch15m = percent_change(c5, 3)
    ch30m = percent_change(c5, 6)
    vol1 = volume_ratio(c1, 20)
    range1 = candle_range_ratio(c1, 20)
    vol5 = volume_ratio(c5, 20)
    range5 = candle_range_ratio(c5, 20)
    loc = close_location(last1)
    body = abs(last1["close"] - last1["open"]) / max(last1["high"] - last1["low"], 1e-12)
    t1h = trend_state(c1h)

    # Live impulse must be real, not a dead hot-list artifact.
    if side == "LONG":
        if ch3m < INSTANT_MIN_1M3_MOVE:
            return None
        if ch15m < INSTANT_MIN_15M_MOVE and not (INSTANT_ALLOW_STRONG_1M_EXCEPTION and ch3m >= INSTANT_MIN_1M3_MOVE * 1.45):
            return None
        if ch30m > INSTANT_MAX_30M_CHASE and ch3m < INSTANT_MIN_1M3_MOVE * 1.35:
            return None
        if loc < INSTANT_CLOSE_LONG or last1["close"] <= last1["open"]:
            return None
        if prev1["close"] < prev1["open"] and last1["close"] <= prev1["open"]:
            return None
        # Avoid buying after multiple vertical green candles without any micro reset.
        had_reset = any(x["close"] < x["open"] for x in c1[-7:-1]) or min(x["low"] for x in c1[-5:]) <= min(x["low"] for x in c1[-14:-5]) * 1.002
        if not had_reset and ch30m > 0.025:
            return None
        level = min(x["low"] for x in c1[-10:])
        strategy = "PRO_INSTANT_EDGE_LONG"
        trade_type = "INSTANT EDGE LONG"
        setup_mode = "INSTANT_MOMENTUM_LONG"
        direction_text = "вверх"
    else:
        if ch3m > -INSTANT_MIN_1M3_MOVE:
            return None
        if ch15m > -INSTANT_MIN_15M_MOVE and not (INSTANT_ALLOW_STRONG_1M_EXCEPTION and abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.45):
            return None
        if ch30m < -INSTANT_MAX_30M_CHASE and abs(ch3m) < INSTANT_MIN_1M3_MOVE * 1.35:
            return None
        if loc > INSTANT_CLOSE_SHORT or last1["close"] >= last1["open"]:
            return None
        if prev1["close"] > prev1["open"] and last1["close"] >= prev1["open"]:
            return None
        had_reset = any(x["close"] > x["open"] for x in c1[-7:-1]) or max(x["high"] for x in c1[-5:]) >= max(x["high"] for x in c1[-14:-5]) * 0.998
        if not had_reset and ch30m < -0.025:
            return None
        level = max(x["high"] for x in c1[-10:])
        strategy = "PRO_INSTANT_EDGE_SHORT"
        trade_type = "INSTANT EDGE SHORT"
        setup_mode = "INSTANT_MOMENTUM_SHORT"
        direction_text = "вниз"

    if body < INSTANT_MIN_BODY:
        return None
    if range1 < INSTANT_MIN_RANGE1:
        return None
    if vol1 < INSTANT_MIN_VOL1 and not (abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.35 and range1 >= 1.15):
        return None
    if range5 < INSTANT_MIN_RANGE5:
        return None
    if vol5 < INSTANT_MIN_VOL5 and not (abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.60):
        return None

    # Keep a micro structure break; this prevents entering the middle of a random candle.
    micro_ok, micro_reason = micro_structure_break(c1, side)
    if not micro_ok:
        return None

    # BTC is context, not a hard phase filter. Against BTC pressure, demand stronger live impulse.
    btc_dir = str(btc.get("direction", "UNKNOWN"))
    if side == "LONG" and btc_dir == "BEAR" and not (ch3m >= INSTANT_MIN_1M3_MOVE * 1.35 and ch15m >= INSTANT_MIN_15M_MOVE * 1.2):
        return None
    if side == "SHORT" and btc_dir == "BULL" and not (abs(ch3m) >= INSTANT_MIN_1M3_MOVE * 1.35 and ch15m <= -INSTANT_MIN_15M_MOVE * 1.2):
        return None

    score = 78
    score += min(10, int(abs(ch3m) * 1000))
    score += min(8, int(abs(ch15m) * 700))
    score += min(6, int(max(0.0, vol1 - 0.8) * 6))
    score += min(6, int(max(0.0, range1 - 1.0) * 6))
    score += min(5, int(max(0.0, range5 - 1.0) * 4))
    score = max(0, min(100, score))

    reason = (
        f"INSTANT EDGE {side}: профессиональный fallback для живого импульса. "
        f"Цена движется {direction_text} сейчас: 1m3 {ch3m*100:+.2f}%, 15m {ch15m*100:+.2f}%, "
        f"30m {ch30m*100:+.2f}%, Vol1 x{vol1:.2f}, Range1 x{range1:.2f}, "
        f"Vol5 x{vol5:.2f}, Range5 x{range5:.2f}, closeLoc {loc:.2f}. "
        f"{micro_reason}. Сделка всё равно проходит RR/SL/live-volume quality gate."
    )

    return {
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "trade_type": trade_type,
        "score": score,
        "grade": "A+" if score >= A_PLUS_MIN_SCORE and vol1 >= 1.20 else "B",
        "entry": price,
        "level": level,
        "reason": reason,
        "pullback": 0.0,
        "volume_ratio": vol5,
        "range_ratio": range5,
        "compression": 1.0,
        "ch15m": ch15m,
        "ch30m": ch30m,
        "ch3m_1m": ch3m,
        "vol1": vol1,
        "range1": range1,
        "ch2m": (c1[-1]["close"] - c1[-3]["close"]) / max(c1[-3]["close"], 1e-12),
        "setup_mode": setup_mode,
        "t1h": t1h,
        "btc_text": btc.get("text", ""),
    }

def market_dump_short_setup(symbol: str, c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], c1h: List[Dict[str, float]], btc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """V13.28 fallback: market-dump continuation SHORT.

    This is for active market selloff sessions when BTC/ETH and many alts are falling.
    The old setup logic often waits for a perfect AERO pullback/reject and returns no_fast,
    while the tape is already giving a clean dump continuation. We still pass the final
    RR/SL/live-volume and trader-pattern gates after this setup is constructed.
    """
    if not MARKET_DUMP_SHORT_ENABLED or not ALLOW_SHORT:
        return None
    if len(c1) < 35 or len(c5) < 36 or len(c15) < 12 or len(c1h) < 40:
        return None

    side = "SHORT"
    price = c1[-1]["close"]
    last1 = c1[-1]
    prev1 = c1[-2]
    ch3m = percent_change(c1, 3)
    ch15m = percent_change(c5, 3)
    ch30m = percent_change(c5, 6)
    vol1 = volume_ratio(c1, 20)
    vol5 = volume_ratio(c5, 20)
    range1 = candle_range_ratio(c1, 20)
    range5 = candle_range_ratio(c5, 20)
    loc = close_location(last1)
    recent_range = (max(x["high"] for x in c5[-6:]) - min(x["low"] for x in c5[-6:])) / max(price, 1e-12)
    btc_1h = float(btc.get("ch1h", 0.0) or 0.0)
    btc_6h = float(btc.get("ch6h", 0.0) or 0.0)

    # Must be real downside pressure now.
    if ch3m > -DUMP_MIN_1M3:
        return None

    # Either the alt itself is already selling on 15m, or BTC has a clear dump context.
    market_dump_context = btc_1h <= -0.0025 or btc_6h <= -0.0100 or str(btc.get("direction", "")) == "BEAR"
    alt_dump_context = ch15m <= -DUMP_MIN_15M or ch30m <= -DUMP_MIN_15M * 1.25
    if not (market_dump_context or alt_dump_context):
        return None

    # Avoid very late shorts after an extreme 30m collapse unless the live tape is still exceptional.
    if ch30m < -DUMP_MAX_LATE_30M and not (abs(ch3m) >= DUMP_MIN_1M3 * 2.0 and range1 >= 1.35):
        return None

    if vol1 < DUMP_MIN_VOL1:
        return None
    if vol5 < DUMP_MIN_VOL5 and not (abs(ch3m) >= DUMP_MIN_1M3 * 1.65):
        return None
    if range1 < DUMP_MIN_RANGE1:
        return None
    if range5 < DUMP_MIN_RANGE5:
        return None
    if recent_range < DUMP_MIN_RECENT_RANGE:
        return None

    # Do not short a weak doji. In a dump, close below mid/low half is enough; exact low-close is too strict.
    if loc > DUMP_CLOSE_SHORT or last1["close"] >= last1["open"]:
        return None

    fresh_low_break = last1["close"] < min(x["low"] for x in c1[-7:-1])
    failed_bounce = any(x["close"] > x["open"] for x in c1[-8:-1]) and last1["close"] < prev1["close"]
    lower_high_reject = max(x["high"] for x in c1[-4:]) < max(x["high"] for x in c1[-14:-4]) and last1["close"] < prev1["close"]
    if DUMP_REQUIRE_REJECT_OR_BREAK and not (fresh_low_break or failed_bounce or lower_high_reject):
        return None

    level = max(x["high"] for x in c1[-10:])
    score = 80
    score += min(10, int(abs(ch3m) * 1100))
    score += min(8, int(abs(min(ch15m, 0.0)) * 700))
    score += min(6, int(max(0.0, vol1 - 0.60) * 6))
    score += min(7, int(max(0.0, range1 - 0.80) * 5))
    score += min(6, int(max(0.0, range5 - 1.00) * 4))
    if market_dump_context:
        score += 3
    if fresh_low_break:
        score += 3
    score = max(0, min(100, score))

    reason = (
        f"MARKET DUMP SHORT: активный рыночный слив, не прогноз, а dump-continuation. "
        f"BTC context {btc.get('text', '')}; alt pressure 1m3 {ch3m*100:+.2f}%, "
        f"15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, Vol1 x{vol1:.2f}, "
        f"Vol5 x{vol5:.2f}, Range1 x{range1:.2f}, Range5 x{range5:.2f}. "
        f"Break/reject: freshLow {fresh_low_break}, failedBounce {failed_bounce}, lowerHighReject {lower_high_reject}. "
        f"Дальше сделка обязана пройти RR/SL/live-volume/trader quality gates."
    )

    return {
        "symbol": symbol,
        "side": side,
        "strategy": "PRO_MARKET_DUMP_SHORT",
        "trade_type": "MARKET DUMP SHORT",
        "score": score,
        "grade": "A+" if score >= A_PLUS_MIN_SCORE and vol1 >= 0.85 and range1 >= 1.15 else "B",
        "entry": price,
        "level": level,
        "reason": reason,
        "pullback": 0.0,
        "volume_ratio": vol5,
        "range_ratio": range5,
        "compression": 1.0,
        "ch15m": ch15m,
        "ch30m": ch30m,
        "ch3m_1m": ch3m,
        "vol1": vol1,
        "range1": range1,
        "ch2m": (c1[-1]["close"] - c1[-3]["close"]) / max(c1[-3]["close"], 1e-12),
        "setup_mode": "MARKET_DUMP_SHORT",
        "t1h": trend_state(c1h),
        "btc_text": btc.get("text", ""),
    }


def evidence_imbalance_setup(
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
    c15: List[Dict[str, float]],
    c1h: List[Dict[str, float]],
    btc: Dict[str, Any],
    side: str,
) -> Optional[Dict[str, Any]]:
    """V16.6.5 direct path for a proven live imbalance.

    Older fast/instant templates can reject a coin before the evidence-backed
    Vol1/Range1/directional rule is evaluated. This constructor does not lower
    that rule. It only creates a candidate when all three requirements already
    pass, the closing candle is directional and a fresh micro break exists.
    The normal RR, trader-pattern, delayed confirmation and adaptive gates still
    run afterwards.
    """
    if len(c1) < 35 or len(c5) < 36 or len(c15) < 12 or len(c1h) < 40:
        return None

    direction = 1.0 if side == "LONG" else -1.0
    price = float(c1[-1]["close"])
    last1 = c1[-1]
    ch3m = percent_change(c1, 3)
    directional_3m = direction * ch3m
    vol1 = volume_ratio(c1, 20)
    range1 = candle_range_ratio(c1, 20)

    # Exact V16.6 evidence gate: this path is an alternative detector, not a
    # relaxation of the quality threshold.
    if directional_3m < DATA_MIN_DIRECTIONAL_3M:
        return None
    if vol1 < DATA_MIN_VOL1 or range1 < DATA_MIN_RANGE1:
        return None

    location = close_location(last1)
    if side == "LONG":
        if last1["close"] <= last1["open"] or location < max(TRADER_CLOSE_LONG, PRE_LIVE_CLOSE_LONG):
            return None
    else:
        if last1["close"] >= last1["open"] or location > min(TRADER_CLOSE_SHORT, PRE_LIVE_CLOSE_SHORT):
            return None

    micro_ok, micro_reason = micro_structure_break(c1, side)
    if not micro_ok:
        return None

    ch15m = percent_change(c5, 3)
    ch30m = percent_change(c5, 6)
    vol5 = volume_ratio(c5, 20)
    range5 = candle_range_ratio(c5, 20)
    level = (
        min(x["low"] for x in c1[-10:])
        if side == "LONG"
        else max(x["high"] for x in c1[-10:])
    )

    score = 82
    score += min(8, int(max(0.0, directional_3m - DATA_MIN_DIRECTIONAL_3M) * 1000))
    score += min(5, int(max(0.0, vol1 - DATA_MIN_VOL1) * 5))
    score += min(5, int(max(0.0, range1 - DATA_MIN_RANGE1) * 4))
    score = max(0, min(100, score))
    setup_mode = f"INSTANT_EVIDENCE_{side}"

    return {
        "symbol": symbol,
        "side": side,
        "strategy": f"PRO_EVIDENCE_IMBALANCE_{side}",
        "trade_type": f"EVIDENCE IMBALANCE {side}",
        "score": score,
        "grade": "A+" if score >= A_PLUS_MIN_SCORE and vol1 >= 1.20 and range1 >= 1.80 else "B",
        "entry": price,
        "level": level,
        "reason": (
            f"EVIDENCE IMBALANCE {side}: прямой путь для подтверждённого живого дисбаланса; "
            f"1m3 {ch3m*100:+.2f}%, Vol1 x{vol1:.2f}, Range1 x{range1:.2f}, "
            f"15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}; {micro_reason}. "
            "Старый fast-шаблон не используется, но все финальные проверки остаются обязательными."
        ),
        "pullback": 0.0,
        "volume_ratio": vol5,
        "range_ratio": range5,
        "compression": 1.0,
        "ch15m": ch15m,
        "ch30m": ch30m,
        "ch3m_1m": ch3m,
        "vol1": vol1,
        "range1": range1,
        "ch2m": (c1[-1]["close"] - c1[-3]["close"]) / max(c1[-3]["close"], 1e-12),
        "setup_mode": setup_mode,
        "t1h": trend_state(c1h),
        "btc_text": btc.get("text", ""),
    }


def near_miss_shadow_setup(
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
    c15: List[Dict[str, float]],
    c1h: List[Dict[str, float]],
    btc: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Build a measurement-only trade for an almost-qualifying imbalance.

    This constructor is deliberately separate from every LIVE path. It gives
    the adaptive layer labelled outcomes during quiet sessions without lowering
    the evidence gate or sending an experimental signal to Telegram.
    """
    if not SHADOW_PROBE_ENABLED or not SHADOW_TRACKING_ENABLED:
        return None
    if len(c1) < 35 or len(c5) < 36 or len(c15) < 12 or len(c1h) < 40:
        return None

    ch3m = percent_change(c1, 3)
    if abs(ch3m) < SHADOW_PROBE_MIN_DIRECTIONAL_3M:
        return None
    side = "LONG" if ch3m > 0 else "SHORT"
    if (side == "LONG" and not ALLOW_LONG) or (side == "SHORT" and not ALLOW_SHORT):
        return None

    vol1 = volume_ratio(c1, 20)
    range1 = candle_range_ratio(c1, 20)
    if vol1 < SHADOW_PROBE_MIN_VOL1 or range1 < SHADOW_PROBE_MIN_RANGE1:
        return None

    directional_3m = abs(ch3m)
    evidence_score = (
        directional_3m / max(DATA_MIN_DIRECTIONAL_3M, 1e-9)
        + vol1 / max(DATA_MIN_VOL1, 1e-9)
        + range1 / max(DATA_MIN_RANGE1, 1e-9)
    )
    if evidence_score < SHADOW_PROBE_MIN_EVIDENCE_SCORE:
        return None

    price = float(c1[-1]["close"])
    ch15m = percent_change(c5, 3)
    ch30m = percent_change(c5, 6)
    vol5 = volume_ratio(c5, 20)
    range5 = candle_range_ratio(c5, 20)
    level = (
        min(x["low"] for x in c1[-10:])
        if side == "LONG"
        else max(x["high"] for x in c1[-10:])
    )
    score = max(0, min(87, 62 + int(min(evidence_score, 5.0) * 5)))
    setup_mode = f"INSTANT_SHADOW_OBSERVATION_{side}"

    live_failures: List[str] = []
    if directional_3m < DATA_MIN_DIRECTIONAL_3M:
        live_failures.append(
            f"move {directional_3m*100:.2f}% < {DATA_MIN_DIRECTIONAL_3M*100:.2f}%"
        )
    if vol1 < DATA_MIN_VOL1:
        live_failures.append(f"Vol1 x{vol1:.2f} < x{DATA_MIN_VOL1:.2f}")
    if range1 < DATA_MIN_RANGE1:
        live_failures.append(f"Range1 x{range1:.2f} < x{DATA_MIN_RANGE1:.2f}")
    live_gap = "; ".join(live_failures) if live_failures else "numeric gate passed; structure rejected"

    return {
        "symbol": symbol,
        "side": side,
        "strategy": f"SHADOW_NEAR_MISS_{side}",
        "trade_type": f"SHADOW NEAR-MISS {side}",
        "score": score,
        "grade": "B",
        "entry": price,
        "level": level,
        "reason": (
            f"SHADOW ONLY: near-miss observation, never a LIVE signal. "
            f"Evidence score {evidence_score:.2f}; 1m3 {ch3m*100:+.2f}%, "
            f"Vol1 x{vol1:.2f}, Range1 x{range1:.2f}; LIVE gap: {live_gap}."
        ),
        "pullback": 0.0,
        "volume_ratio": vol5,
        "range_ratio": range5,
        "compression": 1.0,
        "ch15m": ch15m,
        "ch30m": ch30m,
        "ch3m_1m": ch3m,
        "vol1": vol1,
        "range1": range1,
        "ch2m": (c1[-1]["close"] - c1[-3]["close"]) / max(c1[-3]["close"], 1e-12),
        "setup_mode": setup_mode,
        "t1h": trend_state(c1h),
        "btc_text": btc.get("text", ""),
        "shadow_probe_score": evidence_score,
        "shadow_probe_live_gap": live_gap,
        "shadow_probe_never_live": True,
    }


def v17_2_dual_paper_setup(
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
    c15: List[Dict[str, float]],
    c1h: List[Dict[str, float]],
    btc: Dict[str, Any],
    side: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """Build one liquidity-first WATCH candidate, never a LIVE trade.

    Lane 1 follows a moderate ATR-normalized impulse and waits for a retest.
    Lane 2 follows a genuine sweep/reversal and waits for a second reclaim.
    A detected impulse is not counted as a trade; only the later confirmed
    entry becomes model-eligible PAPER data.
    """
    if len(c1) < 60 or len(c5) < 48 or len(c15) < 24 or len(c1h) < 40:
        return None, "not enough candles"
    liquidity = candle_liquidity_snapshot(symbol, c1, c5)
    cached_liquidity = LIQUIDITY_SCAN_CACHE.get(normalize_symbol(symbol), {})
    if cached_liquidity:
        # Keep fresh hard checks but retain the cross-sectional rank calculated
        # by the immediately preceding universe pass.
        liquidity["rank_percentile"] = float(
            cached_liquidity.get("rank_percentile", 0.50) or 0.50
        )
    if not bool(liquidity.get("ok")):
        return None, f"liquidity rejected: {liquidity.get('reason', 'unknown')}"

    side = str(side).upper()
    direction = 1.0 if side == "LONG" else -1.0
    price = float(c1[-1]["close"])
    last = c1[-1]
    # V20.3: true interval returns. Legacy percent_change is off by one.
    ch3 = spike_bar_return(c1, 3)
    ch15 = spike_bar_return(c5, 3)
    ch30 = spike_bar_return(c5, 6)
    directional_3m = direction * ch3
    directional_15m = direction * ch15
    opposite_stretch = -direction * ch15
    atr1_pct = max(float(liquidity.get("atr1_pct", 0.0) or 0.0), 0.0005)
    normalized_move = directional_3m / atr1_pct
    vol1 = volume_ratio(c1, 20)
    range1 = candle_range_ratio(c1, 20)
    vol5 = volume_ratio(c5, 20)
    range5 = candle_range_ratio(c5, 20)
    location = close_location(last)
    candle_span = max(float(last["high"]) - float(last["low"]), 1e-12)
    body_fraction = abs(float(last["close"]) - float(last["open"])) / candle_span
    ema9 = ema(closes(c1[-40:]), 9)
    ema21 = ema(closes(c1[-50:]), 21)
    vwap30 = vwap(c1, 30)

    if not (
        V17_2_MIN_VOL1 <= vol1 <= V17_2_MAX_VOL1
        and range1 >= V17_2_MIN_RANGE1
        and vol5 >= V17_2_MIN_VOL5
        and range5 >= V17_2_MIN_RANGE5
    ):
        return None, (
            f"flow rejected: Vol1 x{vol1:.2f}, Range1 x{range1:.2f}, "
            f"Vol5 x{vol5:.2f}, Range5 x{range5:.2f}"
        )

    directional_close = bool(
        (side == "LONG" and last["close"] > last["open"] and location >= 0.58)
        or (side == "SHORT" and last["close"] < last["open"] and location <= 0.42)
    )
    aligned = bool(
        (side == "LONG" and price >= ema9 and price >= ema21 and price >= vwap30)
        or (side == "SHORT" and price <= ema9 and price <= ema21 and price <= vwap30)
    )
    min_move = max(V17_2_MIN_ABS_MOVE, V17_2_MIN_ATR_MOVE * atr1_pct)
    max_move = min(V17_2_MAX_ABS_MOVE, V17_2_MAX_ATR_MOVE * atr1_pct)
    continuation = bool(
        min_move <= directional_3m <= max_move
        and V17_2_CONT_MIN_15M <= directional_15m <= V17_2_CONT_MAX_15M
        and directional_close
        and body_fraction >= 0.35
        and aligned
    )

    older = c1[-22:-6]
    recent = c1[-6:-1]
    if side == "LONG":
        swept = min(row["low"] for row in recent) <= min(row["low"] for row in older) * 1.001
        reclaimed = bool(last["close"] > last["open"] and location >= 0.64 and price > ema9)
    else:
        swept = max(row["high"] for row in recent) >= max(row["high"] for row in older) * 0.999
        reclaimed = bool(last["close"] < last["open"] and location <= 0.36 and price < ema9)
    reversal = bool(
        opposite_stretch >= V17_2_REVERSAL_MIN_STRETCH
        and directional_3m >= max(V17_2_REVERSAL_MIN_COUNTER, 0.65 * atr1_pct)
        and directional_3m <= V17_2_MAX_ABS_MOVE
        and swept
        and reclaimed
        and body_fraction >= 0.30
    )

    if continuation:
        lane = "CONTINUATION"
        setup_mode = f"V17_2_CONTINUATION_{side}"
        strategy = f"V17_2_WATCH_CONTINUATION_{side}"
        trade_type = f"LIQUIDITY CONTINUATION WATCH {side}"
        score = 79
        score += min(6, int(max(0.0, normalized_move - V17_2_MIN_ATR_MOVE) * 3.0))
        score += min(5, int(max(0.0, directional_15m - V17_2_CONT_MIN_15M) * 250))
        pullback_fraction_min, pullback_fraction_max = 0.15, 0.60
        pullback_abs_min, pullback_abs_max = 0.0012, 0.0160
        recovery_required = 0.42
        max_watch_seconds = 240
        min_directional_15m = -0.0020
        reason = (
            f"V17.2 CONTINUATION: ликвидный умеренный импульс, нормированный к ATR; "
            f"3m {directional_3m*100:.2f}% = {normalized_move:.2f} ATR1, "
            f"15m {directional_15m*100:.2f}%. Входа сейчас нет: нужен откат и повторный reclaim."
        )
    elif reversal:
        lane = "SWEEP_REVERSAL"
        setup_mode = f"V17_2_SWEEP_REVERSAL_{side}"
        strategy = f"V17_2_WATCH_SWEEP_REVERSAL_{side}"
        trade_type = f"LIQUIDITY SWEEP REVERSAL WATCH {side}"
        score = 81
        score += min(6, int(max(0.0, opposite_stretch - V17_2_REVERSAL_MIN_STRETCH) * 220))
        score += min(5, int(max(0.0, directional_3m - V17_2_REVERSAL_MIN_COUNTER) * 350))
        pullback_fraction_min, pullback_fraction_max = 0.10, 0.55
        pullback_abs_min, pullback_abs_max = 0.0010, 0.0140
        recovery_required = 0.50
        max_watch_seconds = 180
        min_directional_15m = -0.0180
        reason = (
            f"V17.2 SWEEP/REVERSAL: после растяжения против входа "
            f"{opposite_stretch*100:.2f}% произошёл sweep и reclaim; "
            f"контрдвижение 3m {directional_3m*100:.2f}%. Нужен второй retest/reclaim, входа сейчас нет."
        )
    else:
        return None, (
            f"no dual setup: dir3 {directional_3m*100:.2f}%/{normalized_move:.2f}ATR, "
            f"dir15 {directional_15m*100:.2f}%, opposite stretch {opposite_stretch*100:.2f}%"
        )

    rank = float(liquidity.get("rank_percentile", 0.50) or 0.50)
    score += min(6, int(max(0.0, rank) * 6))
    score += min(4, int(max(0.0, vol1 - V17_2_MIN_VOL1) * 2))
    score += min(4, int(max(0.0, range1 - V17_2_MIN_RANGE1) * 2))
    score = max(V17_2_A_MIN_SCORE, min(100, score))
    grade = "A+" if score >= V17_2_A_PLUS_MIN_SCORE else "A"
    level = (
        min(row["low"] for row in c1[-12:])
        if side == "LONG"
        else max(row["high"] for row in c1[-12:])
    )
    return {
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "trade_type": trade_type,
        "score": score,
        "grade": grade,
        "entry": price,
        "level": level,
        "reason": reason,
        "pullback": 0.0,
        "volume_ratio": vol5,
        "range_ratio": range5,
        "compression": prior_compression_ratio(c5),
        "ch15m": ch15,
        "ch30m": ch30,
        "ch3m_1m": ch3,
        "vol1": vol1,
        "range1": range1,
        "ch2m": percent_change(c1, 2),
        "setup_mode": setup_mode,
        "t1h": trend_state(c1h),
        "btc_text": btc.get("text", ""),
        "paper_setup_lane": lane,
        "paper_validation_only": True,
        "paper_validation_immediate": False,
        "paper_validation_lane": PAPER_VALIDATION_REASON,
        "watch_impulse": max(directional_3m, min_move),
        "watch_pullback_min_fraction": pullback_fraction_min,
        "watch_pullback_max_fraction": pullback_fraction_max,
        "watch_pullback_abs_min": pullback_abs_min,
        "watch_pullback_abs_max": pullback_abs_max,
        "watch_recovery_required": recovery_required,
        "watch_min_seconds": 12 if lane == "SWEEP_REVERSAL" else 15,
        "watch_max_seconds": max_watch_seconds,
        "watch_min_directional_3m": 0.0005,
        "watch_min_directional_15m": min_directional_15m,
        "watch_max_directional_15m": 0.0500,
        "watch_max_chase": 0.0045 if lane == "CONTINUATION" else 0.0035,
        "watch_min_vol1": 0.45,
        "watch_max_vol1": V17_2_MAX_VOL1,
        "watch_min_range1": 0.60,
        "watch_min_range5": 0.60,
        "liquidity_quote_60m": float(liquidity.get("quote_60m", 0.0) or 0.0),
        "liquidity_rank_percentile": rank,
        "liquidity_active_fraction": float(liquidity.get("active_fraction", 0.0) or 0.0),
        "liquidity_unique_fraction": float(liquidity.get("unique_fraction", 0.0) or 0.0),
        "atr1_pct": atr1_pct,
        "normalized_directional_move": normalized_move,
        "paper_validation_origin": reason,
    }, "ok"


# V17.3 rationale:
# - The old 500-outcome archive is used as the negative/control history.
# - The user-provided trader examples are used as a target pattern for "active coin + immediate move + TP3+".
# - Because those examples do not include exact timestamps/market snapshots, they are NOT treated as
#   feature-labelled training rows. We therefore keep an archive-derived detector and validate every
#   new candidate in forward PAPER before any live routing.
# - TP3+ remains the minimum profitable outcome.
#

def active_mover_paper_metrics() -> Dict[str, Any]:
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT result, pnl_r, symbol FROM adaptive_trades "
            "WHERE COALESCE(decision_reason, '')=? ORDER BY closed_at ASC, id ASC",
            (TRADER_STYLE_PAPER_REASON,),
        ).fetchall()
    metrics = _outcome_metrics(rows)
    metrics["unique_symbols"] = len(
        {normalize_symbol(str(row["symbol"] or "?")) for row in rows}
    )
    return metrics


def active_mover_setup(
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
    c15: List[Dict[str, float]],
    c1h: List[Dict[str, float]],
    btc: Dict[str, Any],
    side: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """V18.1 stage 1: detect an active coin, but DO NOT enter yet.

    A candidate only enters ACTIVE WATCH. The dedicated 10-second monitor waits
    for a pullback/pause, reclaim/reject and fresh re-acceleration before PAPER.
    """
    if not ACTIVE_MOVER_ENABLED:
        return None, "disabled"
    if len(c1) < 35 or len(c5) < 36 or len(c15) < 12 or len(c1h) < 30:
        return None, "candles"

    side = str(side).upper()
    if side not in {"LONG", "SHORT"}:
        return None, "side"

    direction = 1.0 if side == "LONG" else -1.0
    price = float(c1[-1]["close"])
    last1 = c1[-1]
    # V20.3: true 3m/15m/30m interval returns.
    ch3 = spike_bar_return(c1, 3)
    ch15 = spike_bar_return(c5, 3)
    ch30 = spike_bar_return(c5, 6)
    d3, d15, d30 = direction * ch3, direction * ch15, direction * ch30
    vol1 = volume_ratio(c1, 20)
    range1 = candle_range_ratio(c1, 20)
    vol5 = volume_ratio(c5, 20)
    range5 = candle_range_ratio(c5, 20)
    recent_high = max(float(x["high"]) for x in c5[-12:])
    recent_low = min(float(x["low"]) for x in c5[-12:])
    recent_range = (recent_high - recent_low) / max(price, 1e-12)
    ema9 = ema(closes(c1), 9)
    vw = vwap(c1, 30)

    if recent_range < ACTIVE_MOVER_MIN_RECENT_RANGE:
        return None, "recent_range"
    if vol1 < ACTIVE_MOVER_MIN_VOL1 or range1 < ACTIVE_MOVER_MIN_RANGE1:
        return None, "live_participation"
    if vol5 < ACTIVE_MOVER_MIN_VOL5 or range5 < ACTIVE_MOVER_MIN_RANGE5:
        return None, "5m_participation"

    # Direction is only a WATCH bias. We intentionally do not demand a finished
    # continuation candle here; the trigger monitor decides the actual entry.
    if side == "LONG":
        bias_ok = (d3 >= -0.0015 and (d15 >= 0.0015 or d30 >= 0.0030) and price >= vw * 0.992)
    else:
        bias_ok = (d3 >= -0.0015 and (d15 >= 0.0015 or d30 >= 0.0030) and price <= vw * 1.008)
    if not bias_ok:
        return None, "direction_bias"

    score = 65.0
    score += min(10.0, recent_range * 120.0)
    score += min(8.0, max(0.0, vol1 - 0.35) * 5.0)
    score += min(8.0, max(0.0, range1 - 0.60) * 4.0)
    score += min(6.0, max(0.0, d15) * 250.0)
    score = min(100.0, score)

    reason = (
        f"V18.1 🧲 HOT WATCH {side}: active coin detected; no entry yet. "
        f"range60 {recent_range*100:.2f}%, 3m {ch3*100:+.2f}%, "
        f"15m {ch15*100:+.2f}%, 30m {ch30*100:+.2f}%, "
        f"Vol1 x{vol1:.2f}, Range1 x{range1:.2f}, Vol5 x{vol5:.2f}. "
        f"Waiting pullback/pause → reclaim/reject → re-acceleration."
    )
    return {
        "symbol": normalize_symbol(symbol),
        "side": side,
        "strategy": f"PRO_ACTIVE_MOVER_{side}",
        "trade_type": f"🧲 ACTIVE MOVER {side}",
        "score": int(round(score)),
        "grade": "A+",
        "entry": price,
        "watch_reference": price,
        "reason": reason,
        "volume_ratio": vol5,
        "range_ratio": range5,
        "ch15m": ch15,
        "ch30m": ch30,
        "ch3m_1m": ch3,
        "vol1": vol1,
        "range1": range1,
        "setup_mode": f"V18_1_ACTIVE_WATCH_{side}",
        "t1h": trend_state(c1h),
        "btc_text": btc.get("text", ""),
        "paper_setup_lane": "🧲 ACTIVE MOVER",
        "paper_style": "ACTIVE_MOVER",
        "paper_validation_lane": TRADER_STYLE_PAPER_REASON,
        "paper_validation_origin": reason,
        "created_at": now_ts(),
    }, "active_watch_ok"


def calculate_active_mover_trade(
    setup: Dict[str, Any],
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
) -> Optional[Dict[str, Any]]:
    side = str(setup["side"]).upper()
    entry = float(setup["entry"])
    if entry <= 0:
        return None

    a = atr(c5, 14)
    atr_move = (a / entry) if entry > 0 else 0.0
    technical = abs(entry - float(setup.get("level", entry))) / entry
    sl_move = max(ACTIVE_MIN_SL_MOVE, min(ACTIVE_MAX_SL_MOVE, max(technical * 1.05, atr_move * 1.25)))

    if side == "LONG":
        sl = entry * (1 - sl_move)
        tp1 = entry * (1 + ACTIVE_TP1_MOVE)
        tp2 = entry * (1 + ACTIVE_TP2_MOVE)
        tp3 = entry * (1 + ACTIVE_TP3_MOVE)
        tp4 = entry * (1 + ACTIVE_TP4_MOVE)
        tp5 = entry * (1 + ACTIVE_TP5_MOVE)
    else:
        sl = entry * (1 + sl_move)
        tp1 = entry * (1 - ACTIVE_TP1_MOVE)
        tp2 = entry * (1 - ACTIVE_TP2_MOVE)
        tp3 = entry * (1 - ACTIVE_TP3_MOVE)
        tp4 = entry * (1 - ACTIVE_TP4_MOVE)
        tp5 = entry * (1 - ACTIVE_TP5_MOVE)

    rewards = [abs(tp1-entry), abs(tp2-entry), abs(tp3-entry), abs(tp4-entry), abs(tp5-entry)]
    risk = abs(entry-sl)
    if risk <= 0:
        return None

    trade = dict(setup)
    trade.update({
        "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3, "tp4": tp4, "tp5": tp5,
        "rr": rewards[0]/risk,
        "ladder_rr": (sum(rewards)/len(rewards))/risk,
        "final_rr": rewards[-1]/risk,
        "roi_tp1": ACTIVE_TP1_MOVE * LEVERAGE * 100,
        "roi_sl": sl_move * LEVERAGE * 100,
        "risk_mult": FAST_RISK_MULT,
        "status": "active",
        "tp1_hit": False, "tp2_hit": False, "tp3_hit": False, "tp4_hit": False, "tp5_hit": False,
    })
    return trade


def active_mover_execution_ok(trade: Dict[str, Any]) -> Tuple[bool, str]:
    spread = float(trade.get("book_spread_bps", 0.0) or 0.0)
    depth = float(trade.get("book_depth_usdt", 0.0) or 0.0)
    quote60 = float(trade.get("liquidity_quote_60m", 0.0) or 0.0)
    if spread > ACTIVE_MOVER_MAX_BOOK_SPREAD_BPS:
        return False, f"spread {spread:.1f} bps > {ACTIVE_MOVER_MAX_BOOK_SPREAD_BPS:.1f}"
    if depth < ACTIVE_MOVER_MIN_BOOK_DEPTH_USDT:
        return False, f"depth {depth:.0f} < {ACTIVE_MOVER_MIN_BOOK_DEPTH_USDT:.0f}"
    if quote60 and quote60 < ACTIVE_MOVER_MIN_QUOTE_60M:
        return False, f"turn60 {quote60:.0f} < {ACTIVE_MOVER_MIN_QUOTE_60M:.0f}"
    return True, "execution ok"


def active_watch_key(item: Dict[str, Any]) -> str:
    return f"{normalize_symbol(str(item.get('symbol','?')))}:{str(item.get('side','?')).upper()}"


def add_active_mover_watch(setup: Dict[str, Any]) -> bool:
    """Register stage-1 HOT candidate. No PAPER trade is created here."""
    with STATE_IO_LOCK:
        watches = STATE.setdefault("active_mover_watch", [])
        now = now_ts()
        # Remove stale watches opportunistically.
        watches[:] = [
            x for x in watches
            if now - int(x.get("watch_started_at", now) or now) <= ACTIVE_WATCH_MAX_SECONDS
        ]
        if len(watches) >= max(1, ACTIVE_WATCH_MAX_CANDIDATES):
            return False
        key = active_watch_key(setup)
        if any(active_watch_key(x) == key for x in watches):
            return False
        item = dict(setup)
        ref = float(item.get("entry", 0.0) or 0.0)
        item["watch_started_at"] = now
        item["watch_reference"] = ref
        item["watch_extreme"] = ref
        item["watch_retest"] = ref
        item["watch_pullback_seen"] = False
        item["watch_reclaim_seen"] = False
        item["watch_best_pullback"] = 0.0
        item["watch_stage"] = "WAIT_PULLBACK"
        watches.append(item)
        save_state()
    # V18.1.1 QUIET WATCH:
    # Keep preliminary HOT/WATCH candidates internal. Telegram receives only
    # confirmed PAPER entries and their results, not every candidate being watched.
    return True


def process_active_mover_watches() -> Dict[str, int]:
    """V18.1 stage 2, called by the 10-second monitor.

    Entry requires: a real pullback from a post-detection extreme, recovery of
    part of that pullback, a fresh directional 1m push, EMA9 alignment and
    acceptable public spread/depth. This is intentionally stricter than HOT detection.
    """
    stats = {"checked": 0, "triggered": 0, "expired": 0, "rejected": 0, "selectivity_blocked": 0}
    if not ACTIVE_MOVER_ENABLED:
        return stats

    with STATE_IO_LOCK:
        snapshot = [dict(x) for x in STATE.setdefault("active_mover_watch", [])]

    remaining = []
    now = now_ts()
    active_paper = sum(
        1 for x in STATE.setdefault("shadow_signals", [])
        if str(x.get("shadow_reason", "")) == TRADER_STYLE_PAPER_REASON
    )

    for item in snapshot:
        stats["checked"] += 1
        started = int(item.get("watch_started_at", now) or now)
        age = max(0, now - started)
        if age > ACTIVE_WATCH_MAX_SECONDS:
            stats["expired"] += 1
            continue

        symbol = normalize_symbol(str(item.get("symbol", "")))
        side = str(item.get("side", "")).upper()
        ref = float(item.get("watch_reference", item.get("entry", 0.0)) or 0.0)
        c1 = get_klines(symbol, "1m", 80, cache_seconds=4)
        c5 = get_klines(symbol, "5m", 80, cache_seconds=8)
        c15 = get_klines(symbol, "15m", 80, cache_seconds=12)
        if not c1 or not c5 or not c15 or ref <= 0:
            remaining.append(item)
            continue

        price = float(c1[-1]["close"])
        last = c1[-1]
        prev = c1[-2]
        direction = 1.0 if side == "LONG" else -1.0
        ema9_now = ema(closes(c1[-30:]), 9)
        vw_now = vwap(c1, 30)
        vol1 = volume_ratio(c1, 20)
        range1 = candle_range_ratio(c1, 20)
        raw_one_min = spike_bar_return(c1, 1)
        raw_three_min = spike_bar_return(c1, 3)
        one_min = direction * raw_one_min
        three_min = direction * raw_three_min
        loc = close_location(last)

        extreme = float(item.get("watch_extreme", ref) or ref)
        retest = float(item.get("watch_retest", ref) or ref)
        pullback_seen = bool(item.get("watch_pullback_seen"))

        if side == "LONG":
            if not pullback_seen:
                extreme = max(extreme, price, float(last["high"]))
                retest = min(extreme, price, float(last["low"]))
            else:
                retest = min(retest, price, float(last["low"]))
            pullback = max(0.0, (extreme - retest) / max(extreme, 1e-12))
            recovery = max(0.0, min(1.0, (price - retest) / max(extreme - retest, ref * 1e-8)))
            candle_ok = last["close"] > last["open"] and last["close"] >= prev["close"] and loc >= 0.58
            structure_ok = price > ema9_now and price >= vw_now * 0.998
            chase = (price - ref) / ref
        else:
            if not pullback_seen:
                extreme = min(extreme, price, float(last["low"]))
                retest = max(extreme, price, float(last["high"]))
            else:
                retest = max(retest, price, float(last["high"]))
            pullback = max(0.0, (retest - extreme) / max(extreme, 1e-12))
            recovery = max(0.0, min(1.0, (retest - price) / max(retest - extreme, ref * 1e-8)))
            candle_ok = last["close"] < last["open"] and last["close"] <= prev["close"] and loc <= 0.42
            structure_ok = price < ema9_now and price <= vw_now * 1.002
            chase = (ref - price) / ref

        item["watch_extreme"] = extreme
        item["watch_retest"] = retest
        item["watch_best_pullback"] = max(float(item.get("watch_best_pullback", 0.0) or 0.0), pullback)

        if not pullback_seen and ACTIVE_WATCH_MIN_PULLBACK <= pullback <= ACTIVE_WATCH_MAX_PULLBACK:
            item["watch_pullback_seen"] = True
            item["watch_pullback_seen_at"] = now
            item["watch_stage"] = "WAIT_RECLAIM"
            pullback_seen = True

        if pullback > ACTIVE_WATCH_MAX_PULLBACK:
            stats["rejected"] += 1
            continue

        if not pullback_seen or age < ACTIVE_WATCH_MIN_SECONDS:
            remaining.append(item)
            continue

        reclaim_ok = recovery >= ACTIVE_WATCH_RECLAIM_FRACTION
        reaccel_ok = (
            one_min >= ACTIVE_WATCH_MIN_REACCEL_1M
            and three_min > 0
            and vol1 >= ACTIVE_WATCH_MIN_VOL1
            and range1 >= ACTIVE_WATCH_MIN_RANGE1
            and candle_ok
            and structure_ok
            and chase <= ACTIVE_WATCH_MAX_CHASE
        )
        if not (reclaim_ok and reaccel_ok):
            remaining.append(item)
            continue

        if active_paper >= max(0, ACTIVE_MOVER_MAX_ACTIVE):
            remaining.append(item)
            continue

        setup = dict(item)
        setup["entry"] = price
        setup["created_at"] = now
        setup["ch3m_1m"] = raw_three_min
        setup["ch2m"] = spike_bar_return(c1, 2)
        setup["vol1"] = vol1
        setup["range1"] = range1
        setup["volume_ratio"] = volume_ratio(c5, 20)
        setup["range_ratio"] = candle_range_ratio(c5, 20)
        setup["active_watch_age_seconds"] = age
        setup["active_watch_pullback"] = pullback
        setup["active_watch_recovery"] = recovery
        setup["paper_validation_origin"] = (
            f"V18.1 🧲 TRIGGER {side}: HOT→WATCH {age}s → pullback "
            f"{pullback*100:.2f}% → recovery {recovery*100:.0f}% → "
            f"1m reaccel {one_min*100:+.2f}% · 3m {three_min*100:+.2f}% · "
            f"Vol1 x{vol1:.2f} · Range1 x{range1:.2f}"
        )

        # V18.2.3: ACTIVE MOVER has its own setup + trigger + execution gates.
        # Do NOT pass it through the legacy PRO_DIRECT_MEASURED gate: that gate
        # requires strategy == PRO_DIRECT_MEASURED_* and therefore rejected
        # every PRO_ACTIVE_MOVER_* candidate by name.
        gate_reason = (
            f"ACTIVE_MOVER_NATIVE_GATE: trigger confirmed · pullback {pullback*100:.2f}% · "
            f"recovery {recovery*100:.0f}% · Vol1 x{vol1:.2f} · Range1 x{range1:.2f}"
        )

        active_dir = 1 if side == "LONG" else -1
        active_intel = professional_candle_intelligence(c1, c5, c15, active_dir)
        if (
            float(active_intel.get("climax",0.0) or 0.0) >= 84
            and float(active_intel.get("overheat",0.0) or 0.0) >= 78
        ):
            stats["rejected"] += 1
            continue
        setup["intel_state"] = str(active_intel.get("state","UNKNOWN"))
        setup["intel_overheat"] = float(active_intel.get("overheat",0.0) or 0.0)
        setup["intel_climax"] = float(active_intel.get("climax",0.0) or 0.0)
        setup["intel_squeeze"] = float(active_intel.get("squeeze",0.0) or 0.0)
        setup["intel_rejection_wick"] = float(active_intel.get("rejection_wick",0.0) or 0.0)
        setup["intel_distance_atr"] = float(active_intel.get("distance_atr",0.0) or 0.0)
        setup["intel_volume_health"] = float(active_intel.get("volume_health",0.0) or 0.0)
        setup["intel_range_expansion"] = float(active_intel.get("range_expansion",0.0) or 0.0)
        setup["intel_rsi_fast"] = float(active_intel.get("rsi_fast",50.0) or 50.0)
        setup["intel_setup_quality"] = float(active_intel.get("setup_quality",0.0) or 0.0)
        setup["intel_regime_conflict"] = False
        setup["intel_compression"] = float(active_intel.get("compression",0.0) or 0.0)
        setup["intel_structure"] = float(active_intel.get("structure",0.0) or 0.0)
        setup["intel_false_breakout"] = float(active_intel.get("false_breakout",0.0) or 0.0)
        setup["intel_continuation_edge"] = float(active_intel.get("continuation_edge",0.0) or 0.0)
        setup["intel_pullback_risk"] = float(active_intel.get("pullback_risk",0.0) or 0.0)
        gate_reason += (
            f" · SETUP MASTER {setup['intel_state']} "
            f"CONT={setup['intel_continuation_edge']:.0f} vs PULLBACK={setup['intel_pullback_risk']:.0f} · "
            f"structure={setup['intel_structure']:.0f} compression={setup['intel_compression']:.0f} "
            f"false-break={setup['intel_false_breakout']:.0f}"
        )

        trade = calculate_active_mover_trade(setup, c1, c5)
        if not trade:
            remaining.append(item)
            continue

        cont_edge = float(trade.get("intel_continuation_edge", 0.0) or 0.0)
        pullback_risk = float(trade.get("intel_pullback_risk", 0.0) or 0.0)
        structure_score = float(trade.get("intel_structure", 0.0) or 0.0)
        false_break = float(trade.get("intel_false_breakout", 0.0) or 0.0)
        setup_master_ok = (
            cont_edge >= SETUP_CONT_EDGE_MIN
            and (cont_edge - pullback_risk) >= SETUP_EDGE_MARGIN_MIN
            and pullback_risk <= SETUP_MAX_PULLBACK_RISK_CONT
            and structure_score >= SETUP_STRUCTURE_MIN_CONT
            and false_break <= SETUP_FALSE_BREAKOUT_MAX_CONT
        )
        if not setup_master_ok:
            trade["paper_validation_origin"] += (
                f" · HIDDEN SETUP MASTER BLOCK: CONT={cont_edge:.0f}, "
                f"PULLBACK={pullback_risk:.0f}, structure={structure_score:.0f}, false-break={false_break:.0f}"
            )
            if add_shadow_signal(trade, ACTIVE_SETUP_SELECTIVITY_BLOCK_REASON):
                stats["selectivity_blocked"] += 1
            stats["rejected"] += 1
            continue

        exec_ok, exec_reason = active_mover_execution_ok(trade)
        if not exec_ok:
            remaining.append(item)
            continue

        symbol_ok, symbol_reason = symbol_quarantine_gate(trade)
        if not symbol_ok:
            stats["rejected"] += 1
            continue

        strategy_ok, strategy_reason = strategy_circuit_breaker(trade)
        if not strategy_ok:
            trade["paper_validation_origin"] += (
                f" · {gate_reason} · {exec_reason} · HIDDEN GUARD: {strategy_reason}"
            )
            add_shadow_signal(trade, ACTIVE_MOVER_GUARD_REASON)
            stats["rejected"] += 1
            continue

        trade["paper_validation_origin"] += f" · {gate_reason} · {exec_reason} · {strategy_reason}"
        if add_shadow_signal(trade, TRADER_STYLE_PAPER_REASON):
            send_telegram(build_paper_signal_message(trade))
            stats["triggered"] += 1
            active_paper += 1
            # Do not keep this watch after a confirmed PAPER entry.
            continue
        remaining.append(item)

    with STATE_IO_LOCK:
        STATE["active_mover_watch"] = remaining
        STATE["last_active_watch"] = dict(stats)
        save_state()
    return stats



def official_two_lane_metrics() -> Dict[str, Any]:
    """Only V18.2.3 official PAPER outcomes: Active Mover + 4H Exhaustion."""
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT result, pnl_r, symbol, decision_reason FROM adaptive_trades "
            "WHERE COALESCE(decision_reason, '') IN (?, ?) "
            "ORDER BY closed_at ASC, id ASC",
            (TRADER_STYLE_PAPER_REASON, EXHAUSTION_PAPER_REASON),
        ).fetchall()
    metrics = _outcome_metrics(rows)
    metrics["unique_symbols"] = len(
        {normalize_symbol(str(row["symbol"] or "?")) for row in rows}
    )
    return metrics


def squeeze_exhaustion_metrics() -> Dict[str, Any]:
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT result, pnl_r, symbol FROM adaptive_trades "
            "WHERE COALESCE(decision_reason, '')=? ORDER BY closed_at ASC, id ASC",
            (EXHAUSTION_PAPER_REASON,),
        ).fetchall()
    metrics = _outcome_metrics(rows)
    metrics["unique_symbols"] = len(
        {normalize_symbol(str(row["symbol"] or "?")) for row in rows}
    )
    return metrics


def _median(values: List[float]) -> float:
    vals = sorted(float(x) for x in values if x is not None)
    if not vals:
        return 0.0
    m = len(vals) // 2
    return vals[m] if len(vals) % 2 else (vals[m - 1] + vals[m]) / 2.0


def rsi_context(candles: List[Dict[str, float]], n: int = 14) -> float:
    """RSI is context only; it never creates a reversal trade by itself."""
    if len(candles) < n + 2:
        return 50.0
    vals = closes(candles)
    gains, losses = [], []
    for i in range(max(1, len(vals) - n), len(vals)):
        d = vals[i] - vals[i - 1]
        gains.append(max(d, 0.0)); losses.append(max(-d, 0.0))
    ag = sum(gains) / max(1, len(gains))
    al = sum(losses) / max(1, len(losses))
    if al <= 1e-12:
        return 100.0 if ag > 0 else 50.0
    rs = ag / al
    return 100.0 - 100.0 / (1.0 + rs)


def _candle_shape(c: Dict[str, float], direction: int) -> Dict[str, float]:
    o,h,l,cl = map(float,(c["open"],c["high"],c["low"],c["close"]))
    rng=max(h-l,1e-12)
    body=abs(cl-o)/rng
    upper=max(0.0,h-max(o,cl))/rng
    lower=max(0.0,min(o,cl)-l)/rng
    rejection=upper if direction>0 else lower
    close_extreme=(cl-l)/rng if direction>0 else (h-cl)/rng
    return {"body":body,"upper":upper,"lower":lower,"rejection":rejection,"close_extreme":close_extreme}


def professional_candle_intelligence(
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
    c15: List[Dict[str, float]],
    direction: int,
) -> Dict[str, Any]:
    """Interpret price action as continuation-vs-pullback evidence.

    This is deliberately not a crystal-ball predictor. It scores observable
    evidence: candle anatomy, HH/HL or LH/LL structure, pre-move compression,
    EMA/VWAP/ATR displacement, acceleration, range/volume expansion and false
    breakout/rejection. Without liquidation/OI feeds, `squeeze` is a
    price/volume squeeze proxy rather than proof of forced liquidations.
    """
    neutral = {
        "state": "UNKNOWN", "overheat": 0.0, "climax": 0.0, "squeeze": 0.0,
        "rejection_wick": 0.0, "distance_atr": 0.0, "volume_health": 0.0,
        "range_expansion": 0.0, "rsi_fast": 50.0, "rsi_slow": 50.0,
        "setup_quality": 0.0, "push_volume_ratio": 1.0,
        "compression": 0.0, "structure": 0.0, "false_breakout": 0.0,
        "continuation_edge": 0.0, "pullback_risk": 0.0,
        "acceleration": 0.0,
    }
    if direction not in {-1, 1} or len(c1) < 10 or len(c5) < 20:
        return neutral

    price = float(c1[-1]["close"])
    a5 = max(atr(c5, 14), price * 1e-6)
    e9 = ema(closes(c5[-60:]), 9)
    e21 = ema(closes(c5[-80:]), 21)
    vw = vwap(c5, 36)
    distance_atr = abs(price - e21) / a5
    vwap_distance_atr = abs(price - vw) / a5
    signed_distance = direction * (price - e21) / a5

    shape1 = _candle_shape(c1[-1], direction)
    shape5 = _candle_shape(c5[-1], direction)
    rsi1 = rsi_context(c1, 14)
    rsi5 = rsi_context(c5, 14)

    # ---------- directional structure: HH/HL or LH/LL ----------
    recent = c5[-7:]
    structure_hits = 0
    structure_total = 0
    for prev, cur in zip(recent[:-1], recent[1:]):
        if direction > 0:
            structure_hits += int(float(cur["high"]) >= float(prev["high"]))
            structure_hits += int(float(cur["low"]) >= float(prev["low"]))
        else:
            structure_hits += int(float(cur["high"]) <= float(prev["high"]))
            structure_hits += int(float(cur["low"]) <= float(prev["low"]))
        structure_total += 2
    structure = 100.0 * structure_hits / max(1, structure_total)

    trend_alignment = (
        price > e9 > e21 and price > vw
        if direction > 0
        else price < e9 < e21 and price < vw
    )
    if trend_alignment:
        structure = min(100.0, structure + 12.0)

    # ---------- compression before expansion ----------
    def norm_ranges(items: List[Dict[str, float]]) -> List[float]:
        return [
            candle_range(x) / max(float(x["open"]), 1e-12)
            for x in items
        ]

    if len(c5) >= 36:
        baseline = _median(norm_ranges(c5[-36:-16]))
        pre_move = _median(norm_ranges(c5[-16:-4]))
        if baseline > 1e-9:
            compression_ratio = pre_move / baseline
            compression = max(0.0, min(100.0, (1.20 - compression_ratio) / 0.60 * 100.0))
        else:
            compression = 0.0
    else:
        compression = 25.0

    # ---------- current range/volume expansion ----------
    hist = c5[-26:-1] if len(c5) >= 26 else c5[:-1]
    typical_range = _median(norm_ranges(hist)) if hist else 0.0
    current_range = candle_range(c5[-1]) / max(float(c5[-1]["open"]), 1e-12)
    range_expansion = current_range / max(typical_range, 0.0005)

    med_vol = _median([float(x["volume"]) for x in hist[-20:]]) if hist else 1.0
    volume_health = float(c5[-1]["volume"]) / max(med_vol, 1e-12)
    recent_vol = sum(float(x["volume"]) for x in c5[-2:]) / max(1, min(2, len(c5)))
    prior = c5[-8:-2]
    prior_vol = (
        sum(float(x["volume"]) for x in prior) / len(prior)
        if prior else med_vol
    )
    push_volume_ratio = recent_vol / max(prior_vol, 1e-12)

    # ---------- acceleration / deceleration ----------
    cur3 = direction * spike_bar_return(c1, 3)
    prev3 = 0.0
    if len(c1) >= 7:
        prev_base = float(c1[-7]["close"])
        prev_end = float(c1[-4]["close"])
        prev3 = direction * ((prev_end / max(prev_base, 1e-12)) - 1.0)
    acceleration_raw = cur3 - prev3
    acceleration = max(0.0, min(100.0, 50.0 + acceleration_raw / 0.006 * 50.0))

    mom15 = direction * spike_bar_return(c5, 3)
    mom30 = direction * spike_bar_return(c5, 6)

    # ---------- false breakout / trap ----------
    false_breakout = 0.0
    if len(c5) >= 14:
        prior_window = c5[-14:-2]
        prior_high = max(float(x["high"]) for x in prior_window)
        prior_low = min(float(x["low"]) for x in prior_window)
        last2_high = max(float(x["high"]) for x in c5[-2:])
        last2_low = min(float(x["low"]) for x in c5[-2:])
        close_now = float(c5[-1]["close"])
        if direction > 0 and last2_high > prior_high and close_now < prior_high:
            breach = (last2_high - prior_high) / max(prior_high, 1e-12)
            giveback = (prior_high - close_now) / max(prior_high, 1e-12)
            false_breakout = min(100.0, 35.0 + breach / 0.006 * 30.0 + giveback / 0.006 * 35.0)
        elif direction < 0 and last2_low < prior_low and close_now > prior_low:
            breach = (prior_low - last2_low) / max(prior_low, 1e-12)
            giveback = (close_now - prior_low) / max(prior_low, 1e-12)
            false_breakout = min(100.0, 35.0 + breach / 0.006 * 30.0 + giveback / 0.006 * 35.0)

    # ---------- squeeze continuation proxy ----------
    streak = 0
    for candle in reversed(c5[-8:]):
        with_dir = (
            float(candle["close"]) > float(candle["open"])
            if direction > 0
            else float(candle["close"]) < float(candle["open"])
        )
        if with_dir:
            streak += 1
        else:
            break

    squeeze = 0.0
    squeeze += 15.0 if trend_alignment else 0.0
    squeeze += min(16.0, max(0.0, cur3) / 0.006 * 8.0)
    squeeze += min(14.0, max(0.0, mom15) / 0.020 * 9.0)
    squeeze += 10.0 if shape5["body"] >= 0.55 and shape5["close_extreme"] >= 0.65 else 0.0
    squeeze += min(10.0, max(0.0, volume_health - 0.8) * 7.0)
    squeeze += min(9.0, max(0.0, range_expansion - 1.0) * 4.5)
    squeeze += min(8.0, streak * 2.0)
    squeeze += min(10.0, compression * 0.10)
    squeeze += min(8.0, structure * 0.08)
    squeeze = min(100.0, squeeze)

    # ---------- overheat ----------
    rsi_extreme = (
        max(0.0, (rsi5 - 50.0) / 22.0)
        if direction > 0
        else max(0.0, (50.0 - rsi5) / 22.0)
    )
    overheat = 0.0
    overheat += min(24.0, distance_atr / 4.0 * 24.0)
    overheat += min(16.0, vwap_distance_atr / 4.5 * 16.0)
    overheat += min(18.0, min(1.5, rsi_extreme) * 12.0)
    overheat += min(16.0, streak * 3.0)
    overheat += min(14.0, max(0.0, range_expansion - 1.0) * 7.0)
    overheat += min(12.0, max(0.0, mom30) / 0.05 * 12.0)
    overheat = min(100.0, overheat)

    # ---------- climax / pullback pressure ----------
    rejection = max(shape1["rejection"], shape5["rejection"])
    volume_weakening = max(0.0, min(1.0, (1.0 - push_volume_ratio) / 0.45))
    climax = 0.0
    climax += min(26.0, overheat * 0.26)
    climax += min(24.0, rejection * 55.0)
    climax += 18.0 * volume_weakening
    climax += 12.0 if shape5["body"] < 0.45 else 0.0
    climax += 10.0 if signed_distance > 2.5 and shape5["close_extreme"] < 0.45 else 0.0
    climax += 8.0 if cur3 < 0 else 0.0
    climax += min(10.0, false_breakout * 0.10)
    climax = min(100.0, climax)

    setup_quality = (
        0.34 * squeeze
        + 0.18 * structure
        + 0.12 * compression
        + 0.13 * min(100.0, max(0.0, volume_health) * 50.0)
        + 0.11 * min(100.0, max(0.0, range_expansion) * 35.0)
        + 0.12 * acceleration
    )
    setup_quality = max(0.0, min(100.0, setup_quality))

    # The two final scores answer the actual question the user cares about:
    # "is continuation evidence stronger, or is pullback/exhaustion risk stronger?"
    continuation_edge = (
        0.28 * squeeze
        + 0.21 * structure
        + 0.11 * compression
        + 0.14 * setup_quality
        + 0.10 * acceleration
        + 0.09 * min(100.0, volume_health * 50.0)
        + 0.07 * (100.0 - false_breakout)
    )
    continuation_edge -= 0.12 * climax
    continuation_edge = max(0.0, min(100.0, continuation_edge))

    pullback_risk = (
        0.28 * climax
        + 0.18 * overheat
        + 0.19 * false_breakout
        + 0.12 * (rejection * 100.0)
        + 0.10 * (volume_weakening * 100.0)
        + 0.08 * (100.0 - structure)
        + 0.05 * (100.0 - acceleration)
    )
    pullback_risk = max(0.0, min(100.0, pullback_risk))

    if continuation_edge >= 72 and pullback_risk <= 56:
        state = "CONTINUATION_DOMINANT"
    elif pullback_risk >= 70 and continuation_edge <= 58:
        state = "PULLBACK_RISK_DOMINANT"
    elif squeeze >= 64 and climax < 58:
        state = "SQUEEZE_CONTINUATION"
    elif overheat >= 72 and climax < 60:
        state = "OVERHEATED_TRENDING"
    elif overheat >= 58 and climax >= 58:
        state = "EXHAUSTION_PRESSURE"
    elif squeeze >= 48:
        state = "MOMENTUM"
    else:
        state = "NORMAL"

    return {
        "state": state,
        "overheat": overheat,
        "climax": climax,
        "squeeze": squeeze,
        "rejection_wick": rejection,
        "distance_atr": distance_atr,
        "volume_health": volume_health,
        "push_volume_ratio": push_volume_ratio,
        "range_expansion": range_expansion,
        "rsi_fast": rsi1,
        "rsi_slow": rsi5,
        "setup_quality": setup_quality,
        "trend_alignment": bool(trend_alignment),
        "streak": streak,
        "mom3_with_direction": cur3,
        "mom15_with_direction": mom15,
        "compression": compression,
        "structure": structure,
        "false_breakout": false_breakout,
        "continuation_edge": continuation_edge,
        "pullback_risk": pullback_risk,
        "acceleration": acceleration,
    }


def _spike_tf_metrics(
    candles: List[Dict[str, float]],
    tf_name: str,
    tf_ms: int,
    absolute_min_move: float,
) -> Optional[Dict[str, float]]:
    """Score a visually abnormal price/volume expansion.

    V20 requires BOTH meaningful displacement and abnormality versus the
    instrument's own recent history. This is intentionally much stricter than
    V19's 'absolute OR statistical' detector.
    """
    if not candles or len(candles) < 45:
        return None

    cur = candles[-1]
    hist = candles[-41:-1]
    o, h, l, c = map(float, (cur["open"], cur["high"], cur["low"], cur["close"]))
    if min(o, h, l, c) <= 0:
        return None

    # Multi-bar impulse: a "stick" can be one candle or a fast sequence.
    lookback = 3 if tf_name in {"5m", "15m"} else 2
    anchor_close = float(candles[-(lookback + 1)]["close"])
    recent_move = (c - anchor_close) / max(anchor_close, 1e-12)
    body_move = (c - o) / o
    impulse = recent_move if abs(recent_move) >= abs(body_move) else body_move
    impulse_abs = abs(impulse)
    direction = 1 if impulse > 0 else -1

    # Raw move is mandatory: do not call normal noise a spike only because ATR
    # happens to be tiny.
    if impulse_abs < absolute_min_move:
        return None

    candle_range = (h - l) / o
    body_fraction = abs(c - o) / max(h - l, 1e-12)
    loc = close_location(cur)

    prev_atr = atr(candles[:-1], 14)
    atr_mult = (h - l) / max(prev_atr, o * 1e-6)

    now_ms = int(time.time() * 1000)
    open_ms = int(cur.get("time", 0) or 0)
    elapsed_fraction = (now_ms - open_ms) / float(tf_ms) if open_ms > 0 else 1.0
    elapsed_fraction = max(0.22, min(1.0, elapsed_fraction))
    prev_vol_med = _median([float(x["volume"]) for x in hist[-20:]])
    volume_pace = (float(cur["volume"]) / elapsed_fraction) / max(prev_vol_med, 1e-12)

    hist_bodies = [
        abs(float(x["close"]) - float(x["open"])) / max(float(x["open"]), 1e-12)
        for x in hist[-20:]
    ]
    body_accel = abs(body_move) / max(_median(hist_bodies), 0.0005)

    # Distance from a recent base is the key visual "stick" property missing in V19.
    base_prices = [float(x["close"]) for x in hist[-20:]]
    base_mid = _median(base_prices)
    base_distance = abs(c - base_mid) / max(base_mid, 1e-12)

    # Expansion versus typical range.
    hist_ranges = [
        (float(x["high"]) - float(x["low"])) / max(float(x["open"]), 1e-12)
        for x in hist[-20:]
    ]
    range_accel = candle_range / max(_median(hist_ranges), 0.0005)

    extreme_close = loc >= SPIKE_CLOSE_EXTREME if direction > 0 else loc <= (1.0 - SPIKE_CLOSE_EXTREME)

    # At least two strong abnormality dimensions besides the raw move.
    abnormal = 0
    abnormal += 1 if atr_mult >= SPIKE_MIN_ATR_MULT else 0
    abnormal += 1 if volume_pace >= SPIKE_MIN_VOLUME_PACE else 0
    abnormal += 1 if body_accel >= SPIKE_MIN_BODY_ACCEL else 0
    abnormal += 1 if range_accel >= 2.20 else 0
    abnormal += 1 if base_distance >= SPIKE_MIN_BASE_DISTANCE else 0
    if abnormal < 3:
        return None
    if volume_pace < 1.25:
        return None
    if base_distance < SPIKE_MIN_BASE_DISTANCE:
        return None
    if body_fraction < SPIKE_MIN_BODY_FRACTION and not extreme_close:
        return None

    score = 35.0
    score += min(18.0, (impulse_abs / absolute_min_move) * 8.0)
    score += min(12.0, max(0.0, atr_mult - 1.0) * 4.0)
    score += min(12.0, max(0.0, volume_pace - 1.0) * 4.0)
    score += min(10.0, max(0.0, body_accel - 1.5) * 2.0)
    score += min(9.0, max(0.0, range_accel - 1.5) * 2.0)
    score += min(10.0, (base_distance / max(SPIKE_MIN_BASE_DISTANCE, 1e-9)) * 4.0)
    if extreme_close:
        score += 4.0
    score = min(100.0, score)

    return {
        "tf": tf_name,
        "impulse_move": impulse_abs,
        "signed_impulse": impulse,
        "recent_move": recent_move,
        "body_move": body_move,
        "range_move": candle_range,
        "body_fraction": body_fraction,
        "atr_mult": atr_mult,
        "volume_pace": volume_pace,
        "body_acceleration": body_accel,
        "range_acceleration": range_accel,
        "base_distance": base_distance,
        "score": score,
        "candle_time": int(cur.get("time", 0) or 0),
        "close": c,
        "high": h,
        "low": l,
        "direction": direction,
    }


def detect_mtf_spike_fade(
    symbol: str,
    c5: List[Dict[str, float]],
    c15: List[Dict[str, float]],
    c1h: List[Dict[str, float]],
    c4h: List[Dict[str, float]],
) -> Tuple[Optional[Dict[str, Any]], str]:
    """V20 radar: find the real spike first; direction comes later.

    Unlike V19 this detector does NOT decide SHORT merely because an UP spike
    exists. It creates a neutral regime watch carrying the impulse direction.
    """
    if not SQUEEZE_4H_ENABLED:
        return None, "disabled"

    specs = [
        ("5m", c5, 5 * 60 * 1000, SPIKE_MIN_MOVE_5M),
        ("15m", c15, 15 * 60 * 1000, SPIKE_MIN_MOVE_15M),
        ("1h", c1h, 60 * 60 * 1000, SPIKE_MIN_MOVE_1H),
        ("4h", c4h, 4 * 60 * 60 * 1000, SPIKE_MIN_MOVE_4H),
    ]
    metrics = []
    for tf, candles, tf_ms, threshold in specs:
        m = _spike_tf_metrics(candles, tf, tf_ms, threshold)
        if m:
            metrics.append(m)

    if not metrics:
        return None, "no_real_spike"

    # Strongest event wins; same-direction cross-timeframe confirmation boosts it.
    best = max(metrics, key=lambda x: float(x["score"]))
    direction = int(best["direction"])
    same = [x for x in metrics if int(x["direction"]) == direction]
    opposite = [x for x in metrics if int(x["direction"]) != direction]
    agreement = len(same)
    score = float(best["score"]) + max(0, agreement - 1) * 5.0 - len(opposite) * 6.0
    score = min(100.0, score)
    if score < SPIKE_MIN_SCORE:
        return None, f"quality_{score:.1f}"

    # V20.1 review-50: single-timeframe detections were too easy to admit.
    # Keep them only when the move and volume are unmistakably abnormal.
    if agreement == 1:
        tf_floor = {
            "5m": SPIKE_MIN_MOVE_5M,
            "15m": SPIKE_MIN_MOVE_15M,
            "1h": SPIKE_MIN_MOVE_1H,
            "4h": SPIKE_MIN_MOVE_4H,
        }.get(str(best["tf"]), SPIKE_MIN_MOVE_15M)
        if (
            score < SPIKE_SINGLE_TF_MIN_SCORE
            or float(best["volume_pace"]) < SPIKE_SINGLE_TF_MIN_VOLUME
            or float(best["impulse_move"]) < tf_floor * SPIKE_SINGLE_TF_MOVE_MULT
        ):
            return None, "single_tf_not_extreme_enough"

    tf_summary = ", ".join(
        f"{x['tf']} {x['signed_impulse']*100:+.1f}% "
        f"Vx{x['volume_pace']:.1f} base{x['base_distance']*100:.1f}%"
        for x in same
    )
    direction_name = "UP" if direction > 0 else "DOWN"
    reference = max(float(x["high"]) for x in same) if direction > 0 else min(float(x["low"]) for x in same)

    reason = (
        f"🚀 REAL SPIKE {direction_name}: {best['tf']} impulse {best['signed_impulse']*100:+.1f}%, "
        f"ATR x{best['atr_mult']:.2f}, volume pace x{best['volume_pace']:.2f}, "
        f"body x{best['body_acceleration']:.1f}, range x{best['range_acceleration']:.1f}, "
        f"distance from base {best['base_distance']*100:.1f}%, TF agreement {agreement}. "
        f"Context: {tf_summary}. WATCH ONLY — classify continuation vs exhaustion."
    )

    # 'side' is a placeholder only for the watch key. The actual trade side is
    # chosen later by the regime classifier.
    return {
        "symbol": normalize_symbol(symbol),
        "side": "UP" if direction > 0 else "DOWN",
        "impulse_direction": direction,
        "strategy": "PRO_SPIKE_REGIME_WATCH",
        "trade_type": "🚀 SPIKE REGIME WATCH",
        "paper_style": "SQUEEZE_EXHAUSTION",
        "paper_setup_lane": "🚀 SPIKE REGIME",
        "paper_validation_lane": EXHAUSTION_PAPER_REASON,
        "grade": "A+",
        "score": int(round(score)),
        "reason": reason,
        "paper_validation_origin": reason,
        "spike_primary_tf": str(best["tf"]),
        "spike_impulse_move": float(best["impulse_move"]),
        "spike_signed_impulse": float(best["signed_impulse"]),
        "spike_atr_mult": float(best["atr_mult"]),
        "spike_volume_pace": float(best["volume_pace"]),
        "spike_body_acceleration": float(best["body_acceleration"]),
        "spike_range_acceleration": float(best["range_acceleration"]),
        "spike_base_distance": float(best["base_distance"]),
        "spike_tf_agreement": int(agreement),
        "squeeze_4h_candle_time": int(best["candle_time"]),
        "watch_reference": reference,
        "created_at": now_ts(),
    }, "real_spike_detected"


def squeeze_watch_key(item: Dict[str, Any]) -> str:
    return (
        f"{normalize_symbol(str(item.get('symbol','?')))}:"
        f"{str(item.get('side','?')).upper()}:"
        f"{str(item.get('spike_primary_tf','mtf'))}:"
        f"{int(item.get('squeeze_4h_candle_time',0) or 0)}"
    )


def add_squeeze_watch(setup: Dict[str, Any]) -> bool:
    """Quiet stage-1 watch. Telegram sees only confirmed PAPER entries."""
    with STATE_IO_LOCK:
        watches = STATE.setdefault("squeeze_4h_watch", [])
        now = now_ts()
        watches[:] = [
            x for x in watches
            if now - int(x.get("watch_started_at", now) or now) <= SQUEEZE_4H_WATCH_SECONDS
        ]
        if len(watches) >= max(1, SQUEEZE_4H_WATCH_MAX):
            return False
        key = squeeze_watch_key(setup)
        if any(squeeze_watch_key(x) == key for x in watches):
            return False
        item = dict(setup)
        ref = float(item.get("watch_reference", 0.0) or 0.0)
        item["watch_started_at"] = now
        item["watch_extreme"] = ref
        item["last_extreme_at"] = now
        item["runaway_blocks"] = 0
        item["max_retrace"] = 0.0
        watches.append(item)
        save_state()
    return True


def squeeze_execution_gate(
    setup: Dict[str, Any],
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
) -> Tuple[bool, str]:
    liquidity = candle_liquidity_snapshot(symbol, c1, c5)
    cached = LIQUIDITY_SCAN_CACHE.get(normalize_symbol(symbol), {})
    rank = float(cached.get("rank_percentile", 0.0) or 0.0)
    quote60 = float(liquidity.get("quote_60m", 0.0) or 0.0)
    if not bool(liquidity.get("ok")):
        return False, f"liquidity: {liquidity.get('reason','unknown')}"
    if rank < EXHAUST_MIN_LIQUIDITY_RANK:
        return False, f"liq p{rank*100:.0f} < p{EXHAUST_MIN_LIQUIDITY_RANK*100:.0f}"
    if quote60 < EXHAUST_MIN_QUOTE_60M:
        return False, f"turn60 {quote60:.0f} < {EXHAUST_MIN_QUOTE_60M:.0f}"

    book = execution_book_snapshot(symbol)
    if not bool(book.get("ok")):
        return False, str(book.get("reason", "book unavailable"))
    spread = float(book.get("spread_bps", 999.0) or 999.0)
    depth = float(book.get("depth_usdt", 0.0) or 0.0)
    if spread > EXHAUST_MAX_SPREAD_BPS:
        return False, f"spread {spread:.1f} > {EXHAUST_MAX_SPREAD_BPS:.1f}"
    if depth < EXHAUST_MIN_DEPTH_USDT:
        return False, f"depth {depth:.0f} < {EXHAUST_MIN_DEPTH_USDT:.0f}"

    side = str(setup.get("side", "")).upper()
    executable = float(
        book.get("ask" if side == "LONG" else "bid", setup.get("entry", 0.0))
        or setup.get("entry", 0.0)
    )
    if executable <= 0:
        return False, "bad executable price"
    setup["entry"] = executable
    setup["book_spread_bps"] = spread
    setup["book_depth_usdt"] = depth
    setup["liquidity_quote_60m"] = quote60
    setup["liquidity_rank_percentile"] = rank
    return True, f"book ok: spread {spread:.1f}bps, depth {depth:.0f}, liq p{rank*100:.0f}"


def calculate_exhaustion_trade(setup: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Build either continuation or exhaustion trade from the same real spike."""
    side = str(setup.get("side", "")).upper()
    regime = str(setup.get("spike_regime", "EXHAUSTION")).upper()
    entry = float(setup.get("entry", 0.0) or 0.0)
    extreme = float(setup.get("watch_extreme", entry) or entry)
    if side not in {"LONG", "SHORT"} or entry <= 0:
        return None

    if regime == "CONTINUATION":
        # Continuation invalidation is close to the entry/pullback structure.
        # Do not place the stop all the way at the original spike base.
        pullback = float(setup.get("current_retrace", 0.0) or 0.0)
        sl_move = max(EXHAUST_MIN_SL_MOVE, min(EXHAUST_MAX_SL_MOVE, pullback * 0.70 + 0.0060))
    else:
        # Fade invalidates beyond the spike extreme.
        if extreme <= 0:
            return None
        if side == "SHORT":
            technical_move = max(0.0, (extreme - entry) / entry) + 0.0020
        else:
            technical_move = max(0.0, (entry - extreme) / entry) + 0.0020
        sl_move = max(EXHAUST_MIN_SL_MOVE, technical_move)
        if sl_move > EXHAUST_MAX_SL_MOVE:
            return None

    if side == "SHORT":
        sl = entry * (1 + sl_move)
        tps = [
            entry * (1 - EXHAUST_TP1_MOVE),
            entry * (1 - EXHAUST_TP2_MOVE),
            entry * (1 - EXHAUST_TP3_MOVE),
            entry * (1 - EXHAUST_TP4_MOVE),
            entry * (1 - EXHAUST_TP5_MOVE),
        ]
    else:
        sl = entry * (1 - sl_move)
        tps = [
            entry * (1 + EXHAUST_TP1_MOVE),
            entry * (1 + EXHAUST_TP2_MOVE),
            entry * (1 + EXHAUST_TP3_MOVE),
            entry * (1 + EXHAUST_TP4_MOVE),
            entry * (1 + EXHAUST_TP5_MOVE),
        ]

    rewards = [abs(x - entry) for x in tps]
    risk = abs(sl - entry)
    trade = dict(setup)
    trade.update({
        "sl": sl,
        "tp1": tps[0], "tp2": tps[1], "tp3": tps[2], "tp4": tps[3], "tp5": tps[4],
        "rr": rewards[0] / max(risk, 1e-12),
        "ladder_rr": (sum(rewards) / len(rewards)) / max(risk, 1e-12),
        "final_rr": rewards[-1] / max(risk, 1e-12),
        "risk_mult": FAST_RISK_MULT,
        "roi_tp1": EXHAUST_TP1_MOVE * LEVERAGE * 100,
        "roi_sl": sl_move * LEVERAGE * 100,
        "status": "active",
        "tp1_hit": False, "tp2_hit": False, "tp3_hit": False, "tp4_hit": False, "tp5_hit": False,
    })
    return trade


def spike_bar_return(candles: List[Dict[str, float]], bars: int) -> float:
    """True return across N candle intervals.

    Legacy percent_change(candles, 1) compares the last candle with itself and
    therefore always returns 0. V20.2 keeps the legacy helper untouched for
    old strategies but uses correct indexing inside SPIKE REGIME.
    """
    bars = max(1, int(bars))
    if not candles or len(candles) <= bars:
        return 0.0
    a = float(candles[-bars - 1]["close"])
    b = float(candles[-1]["close"])
    return (b - a) / a if a else 0.0


def spike_regime_reentry_key(setup: Dict[str, Any]) -> str:
    return (
        f"{normalize_symbol(str(setup.get('symbol','?')))}:"
        f"{str(setup.get('strategy','?')).upper()}"
    )


def spike_regime_reentry_allowed(setup: Dict[str, Any]) -> Tuple[bool, str]:
    locks = STATE.setdefault("spike_regime_reentry", {})
    key = spike_regime_reentry_key(setup)
    until = int(locks.get(key, 0) or 0)
    now = now_ts()
    if now < until:
        return False, f"same-regime re-entry locked for {(until-now)/60.0:.0f}m"
    return True, "re-entry ok"


def set_spike_regime_reentry_lock(setup: Dict[str, Any]) -> None:
    with STATE_IO_LOCK:
        locks = STATE.setdefault("spike_regime_reentry", {})
        locks[spike_regime_reentry_key(setup)] = (
            now_ts() + max(0, SPIKE_REENTRY_COOLDOWN_SECONDS)
        )
        # Remove expired locks opportunistically.
        now = now_ts()
        stale = [k for k, v in locks.items() if int(v or 0) <= now]
        for k in stale[:200]:
            locks.pop(k, None)
        save_state()


def process_squeeze_exhaustion_watches() -> Dict[str, int]:
    """V20 professional regime classifier.

    Real spike is detected first. Then:
      UP spike + healthy continuation -> LONG
      UP spike + exhausted/failed reclaim -> SHORT
      DOWN spike + healthy continuation -> SHORT
      DOWN spike + exhausted/failed reclaim -> LONG

    The watch is quiet. Telegram receives only confirmed PAPER entries.
    """
    stats = {
        "checked": 0, "continuation": 0, "exhaustion": 0,
        "runaway_watch": 0, "triggered": 0, "expired": 0,
        "risk_rejected": 0, "execution_rejected": 0,
        "symbol_quarantine_blocked": 0, "reentry_blocked": 0,
        "strategy_guard_blocked": 0, "adaptive_blocked": 0,
        "setup_selectivity_blocked": 0,
        "adaptive_errors": 0, "exhaust_quality_wait": 0,
    }
    if not SQUEEZE_4H_ENABLED:
        return stats

    with STATE_IO_LOCK:
        snapshot = [dict(x) for x in STATE.setdefault("squeeze_4h_watch", [])]

    now = now_ts()
    remaining: List[Dict[str, Any]] = []

    for item in snapshot:
        stats["checked"] += 1
        started = int(item.get("watch_started_at", now) or now)
        age = max(0, now - started)
        if age > SQUEEZE_4H_WATCH_SECONDS:
            stats["expired"] += 1
            continue

        symbol = normalize_symbol(str(item.get("symbol", "")))
        impulse_dir = int(item.get("impulse_direction", 0) or 0)
        if impulse_dir not in {-1, 1}:
            # backward compatibility for stale V19 watch rows
            raw = str(item.get("side", "")).upper()
            impulse_dir = 1 if raw in {"UP", "SHORT"} else -1

        c1 = get_klines(symbol, "1m", 120, cache_seconds=4)
        c5 = get_klines(symbol, "5m", 100, cache_seconds=8)
        c15 = get_klines(symbol, "15m", 80, cache_seconds=15)
        if not c1 or not c5 or not c15:
            remaining.append(item)
            continue

        price = float(c1[-1]["close"])
        last = c1[-1]
        prev3 = c1[-4:-1]
        ema9_now = ema(closes(c1[-50:]), 9)
        ema21_now = ema(closes(c1[-60:]), 21)
        vol1 = volume_ratio(c1, 20)
        range1 = candle_range_ratio(c1, 20)
        loc = close_location(last)

        # V20.3: true 1m/3m/5m/15m returns.
        # Legacy percent_change(..., 1) is always zero, which disabled the
        # V20.1 runaway guard and made continuation/exhaustion timing incomplete.
        mom1 = spike_bar_return(c1, 1)
        mom3 = spike_bar_return(c1, 3)
        mom5 = spike_bar_return(c1, 5)
        mom15 = spike_bar_return(c5, 3)

        # Review-50 anti-chase: losses were concentrated in entries where the
        # micro move was already extremely stretched at the moment of entry.
        micro_overextended = (
            abs(mom3) > SPIKE_ENTRY_MAX_3M_MOVE
            or abs(mom1) > SPIKE_ENTRY_MAX_1M_MOVE
        )

        intel = professional_candle_intelligence(c1, c5, c15, impulse_dir)
        intel_overheat = float(intel.get("overheat", 0.0) or 0.0)
        intel_climax = float(intel.get("climax", 0.0) or 0.0)
        intel_squeeze = float(intel.get("squeeze", 0.0) or 0.0)
        intel_setup_quality = float(intel.get("setup_quality", 0.0) or 0.0)
        intel_distance_atr = float(intel.get("distance_atr", 0.0) or 0.0)
        intel_state = str(intel.get("state", "UNKNOWN"))

        extreme = float(item.get("watch_extreme", price) or price)
        last_extreme_at = int(item.get("last_extreme_at", started) or started)

        # Update the spike extreme and calculate retrace from it.
        if impulse_dir > 0:
            observed = max(extreme, float(last["high"]), price)
            if observed > extreme * 1.0005:
                extreme = observed
                last_extreme_at = now
            retrace = max(0.0, (extreme - price) / max(extreme, 1e-12))
            continuation_momentum = mom1 > 0 and mom3 > 0
            trend_hold = price > ema9_now > ema21_now
            close_with_impulse = loc >= 0.58
            micro_break_against = price < min(float(x["close"]) for x in prev3)
            fade_mom = -mom3
        else:
            observed = min(extreme, float(last["low"]), price)
            if observed < extreme * 0.9995:
                extreme = observed
                last_extreme_at = now
            retrace = max(0.0, (price - extreme) / max(extreme, 1e-12))
            continuation_momentum = mom1 < 0 and mom3 < 0
            trend_hold = price < ema9_now < ema21_now
            close_with_impulse = loc <= 0.42
            micro_break_against = price > max(float(x["close"]) for x in prev3)
            fade_mom = mom3

        item["watch_extreme"] = extreme
        item["last_extreme_at"] = last_extreme_at
        old_max_retrace = float(item.get("max_retrace", 0.0) or 0.0)
        max_retrace = max(old_max_retrace, retrace)
        item["max_retrace"] = max_retrace

        if age < SPIKE_MIN_WATCH_SECONDS:
            remaining.append(item)
            continue

        # ---------- CONTINUATION ----------
        # We do not chase the spike top. Require a real but shallow pullback and
        # then recovery toward the extreme with renewed direction.
        recovery_fraction = 0.0
        if max_retrace > 1e-9:
            recovery_fraction = max(0.0, min(1.0, (max_retrace - retrace) / max_retrace))

        cont_score = 0
        cont_score += 1 if SPIKE_CONT_MIN_PULLBACK <= max_retrace <= SPIKE_CONT_MAX_PULLBACK else 0
        cont_score += 1 if recovery_fraction >= SPIKE_CONT_RECOVERY_FRACTION else 0
        cont_score += 1 if continuation_momentum else 0
        cont_score += 1 if trend_hold else 0
        cont_score += 1 if close_with_impulse else 0
        cont_score += 1 if vol1 >= max(SPIKE_MIN_VOL1, 0.55) else 0
        cont_score += 1 if now - last_extreme_at <= 120 else 0
        cont_score += 1 if range1 >= 0.70 else 0

        cont_15m_aligned = (mom15 > 0) if impulse_dir > 0 else (mom15 < 0)
        cont_micro_momentum = (
            (mom1 >= SPIKE_CONT_MIN_REVERSAL_1M and mom3 >= SPIKE_CONT_MIN_REVERSAL_3M)
            if impulse_dir > 0
            else (mom1 <= -SPIKE_CONT_MIN_REVERSAL_1M and mom3 <= -SPIKE_CONT_MIN_REVERSAL_3M)
        )
        if SPIKE_CONT_REQUIRE_15M_ALIGNMENT and not cont_15m_aligned:
            cont_score = max(0, cont_score - 2)
        if not cont_micro_momentum:
            cont_score = max(0, cont_score - 2)
        cont_score += 1 if intel_squeeze >= 58 else 0
        cont_score += 1 if intel_setup_quality >= PRO_CONT_MIN_INTEL_SCORE else 0
        late_climax_cont = (
            intel_climax >= PRO_MAX_CLIMAX_FOR_CONT
            or (intel_overheat >= 84 and float(intel.get("rejection_wick",0.0) or 0.0) >= 0.30)
            or intel_distance_atr > PRO_MAX_DISTANCE_ATR_CONT
        )

        # ---------- EXHAUSTION ----------
        # First require an actual micro structure break. Then store the broken
        # level and wait for a failed reclaim/retest. This prevents "one red
        # minute candle = fade" behavior.
        if micro_break_against and not item.get("structure_broken"):
            item["structure_broken"] = True
            item["structure_break_at"] = now
            item["break_level"] = ema9_now

        failed_reclaim = False
        if item.get("structure_broken"):
            break_level = float(item.get("break_level", ema9_now) or ema9_now)
            if impulse_dir > 0:
                # For UP spike exhaustion, price is below break level and an
                # attempted rebound cannot reclaim it.
                recent_high = max(float(x["high"]) for x in c1[-3:])
                failed_reclaim = (
                    price < break_level
                    and recent_high >= break_level * (1.0 - SPIKE_FAILED_RECLAIM_TOL)
                    and float(last["close"]) < break_level
                )
            else:
                recent_low = min(float(x["low"]) for x in c1[-3:])
                failed_reclaim = (
                    price > break_level
                    and recent_low <= break_level * (1.0 + SPIKE_FAILED_RECLAIM_TOL)
                    and float(last["close"]) > break_level
                )

        fade_score = 0
        fade_score += 1 if SPIKE_FADE_MIN_RETRACE <= retrace <= SPIKE_FADE_MAX_RETRACE else 0
        fade_score += 1 if bool(item.get("structure_broken")) else 0
        fade_score += 2 if failed_reclaim else 0  # key confirmation, double weight
        fade_score += 1 if fade_mom >= EXHAUST_MIN_REVERSAL_3M else 0
        fade_score += 1 if (price < ema9_now if impulse_dir > 0 else price > ema9_now) else 0
        fade_score += 1 if now - last_extreme_at >= 25 else 0
        fade_score += 1 if vol1 >= SPIKE_MIN_VOL1 else 0

        # V20.2 professional exhaustion confirmation.
        # A fade is not allowed merely because structure once broke.
        if impulse_dir > 0:
            exhaust_micro_reversal = (
                mom1 <= -SPIKE_EXHAUST_MIN_REVERSAL_1M
                and mom3 <= -SPIKE_EXHAUST_MIN_REVERSAL_3M
                and price < ema9_now
            )
        else:
            exhaust_micro_reversal = (
                mom1 >= SPIKE_EXHAUST_MIN_REVERSAL_1M
                and mom3 >= SPIKE_EXHAUST_MIN_REVERSAL_3M
                and price > ema9_now
            )

        volume_faded = vol1 <= SPIKE_EXHAUST_MAX_VOL1
        no_new_extreme_ok = (
            now - last_extreme_at >= SPIKE_EXHAUST_MIN_NO_EXTREME_SECONDS
        )
        tf_agreement = int(item.get("spike_tf_agreement", 0) or 0)
        extreme_single_tf = (
            float(item.get("spike_impulse_move", 0.0) or 0.0)
            >= SPIKE_EXTREME_SINGLE_MIN_IMPULSE
            and float(item.get("spike_base_distance", 0.0) or 0.0)
            >= SPIKE_EXTREME_SINGLE_MIN_BASE_DISTANCE
        )
        spike_context_ok = (
            tf_agreement >= SPIKE_EXHAUST_MIN_TF_AGREEMENT
            or extreme_single_tf
        )
        professional_fade_context = (
            intel_overheat >= PRO_MIN_OVERHEAT_FOR_FADE
            and (
                intel_climax >= PRO_EXHAUST_MIN_INTEL_SCORE
                or float(intel.get("rejection_wick",0.0) or 0.0) >= 0.28
                or float(intel.get("push_volume_ratio",1.0) or 1.0) <= 0.72
            )
        )
        if professional_fade_context:
            fade_score += 1

        # If there is still strong with-spike momentum and no structure break,
        # this is a squeeze-continuation environment, not a fade.
        runaway_like = (
            continuation_momentum
            and trend_hold
            and close_with_impulse
            and not item.get("structure_broken")
            and retrace < SPIKE_CONT_MAX_PULLBACK
        )
        if runaway_like:
            stats["runaway_watch"] += 1

        regime = None
        trade_side = None

        continuation_side = "LONG" if impulse_dir > 0 else "SHORT"
        exhaustion_side = "SHORT" if impulse_dir > 0 else "LONG"

        cont_required = (
            SPIKE_CONT_SCORE_LONG if continuation_side == "LONG"
            else SPIKE_CONT_SCORE_SHORT
        )
        fade_required = (
            SPIKE_FADE_SCORE_LONG if exhaustion_side == "LONG"
            else SPIKE_FADE_SCORE_SHORT
        )

        # Continuation may enter first, but only after pullback/recovery,
        # 15m alignment and without a late/chasing micro impulse.
        if (
            cont_score >= cont_required
            and not failed_reclaim
            and not micro_overextended
            and cont_micro_momentum
            and (cont_15m_aligned or not SPIKE_CONT_REQUIRE_15M_ALIGNMENT)
            and intel_setup_quality >= PRO_CONT_MIN_INTEL_SCORE
            and not late_climax_cont
        ):
            regime = "CONTINUATION"
            trade_side = continuation_side
            stats["continuation"] += 1

        # Exhaustion takes priority once the broken structure has failed reclaim.
        # Exhaustion LONG was low-conversion in the first 50 outcomes, so it
        # deliberately requires one extra point of evidence.
        if (
            failed_reclaim
            and fade_score >= fade_required
            and not micro_overextended
            and exhaust_micro_reversal
            and volume_faded
            and no_new_extreme_ok
            and spike_context_ok
            and professional_fade_context
            and intel_setup_quality >= PRO_SETUP_MIN_SCORE * 0.72
        ):
            regime = "EXHAUSTION"
            trade_side = exhaustion_side
            stats["exhaustion"] += 1

        cont_strength = min(100.0, cont_score / max(1.0,float(cont_required))*70.0 + intel_squeeze*0.30)
        fade_strength = min(100.0, fade_score / max(1.0,float(fade_required))*70.0 + intel_climax*0.30)
        regime_conflict = (
            cont_strength >= 62 and fade_strength >= 62
            and abs(cont_strength-fade_strength) < PRO_CONFLICT_MARGIN
        )
        if regime_conflict:
            regime = None
            trade_side = None

        if not regime or not trade_side:
            if failed_reclaim and fade_score >= fade_required and not (
                exhaust_micro_reversal
                and volume_faded
                and no_new_extreme_ok
                and spike_context_ok
            ):
                stats["exhaust_quality_wait"] += 1
            remaining.append(item)
            continue

        setup = dict(item)
        setup["side"] = trade_side
        setup["strategy"] = f"PRO_SPIKE_REGIME_{regime}_{trade_side}"
        setup["trade_type"] = f"🚀 SPIKE {regime} {trade_side}"
        setup["paper_setup_lane"] = "🚀 SPIKE REGIME"
        setup["spike_regime"] = regime
        setup["entry"] = price
        setup["created_at"] = now
        setup["current_retrace"] = retrace
        setup["vol1"] = vol1
        setup["range1"] = range1
        setup["volume_ratio"] = volume_ratio(c5, 20)
        setup["range_ratio"] = candle_range_ratio(c5, 20)
        setup["ch3m_1m"] = mom3
        setup["ch15m"] = spike_bar_return(c5, 3)
        setup["ch30m"] = spike_bar_return(c5, 6)
        setup["regime_cont_score"] = cont_score
        setup["regime_fade_score"] = fade_score
        setup["regime_failed_reclaim"] = bool(failed_reclaim)
        setup["regime_cont_15m_aligned"] = bool(cont_15m_aligned)
        setup["regime_cont_micro_momentum"] = bool(cont_micro_momentum)
        setup["regime_micro_overextended"] = bool(micro_overextended)
        setup["regime_exhaust_micro_reversal"] = bool(exhaust_micro_reversal)
        setup["regime_volume_faded"] = bool(volume_faded)
        setup["regime_no_new_extreme_ok"] = bool(no_new_extreme_ok)
        setup["regime_spike_context_ok"] = bool(spike_context_ok)
        setup["entry_vol1_raw"] = vol1
        setup["entry_mom1"] = mom1
        setup["entry_mom3"] = mom3
        setup["entry_mom5"] = mom5
        setup["entry_mom15"] = mom15
        setup["intel_state"] = intel_state
        setup["intel_overheat"] = intel_overheat
        setup["intel_climax"] = intel_climax
        setup["intel_squeeze"] = intel_squeeze
        setup["intel_rejection_wick"] = float(intel.get("rejection_wick",0.0) or 0.0)
        setup["intel_distance_atr"] = intel_distance_atr
        setup["intel_volume_health"] = float(intel.get("volume_health",0.0) or 0.0)
        setup["intel_range_expansion"] = float(intel.get("range_expansion",0.0) or 0.0)
        setup["intel_rsi_fast"] = float(intel.get("rsi_fast",50.0) or 50.0)
        setup["intel_setup_quality"] = intel_setup_quality
        setup["intel_regime_conflict"] = bool(regime_conflict)
        setup["intel_compression"] = float(intel.get("compression",0.0) or 0.0)
        setup["intel_structure"] = float(intel.get("structure",0.0) or 0.0)
        setup["intel_false_breakout"] = float(intel.get("false_breakout",0.0) or 0.0)
        setup["intel_continuation_edge"] = float(intel.get("continuation_edge",0.0) or 0.0)
        setup["intel_pullback_risk"] = float(intel.get("pullback_risk",0.0) or 0.0)
        setup["paper_validation_origin"] = (
            f"🚀 V20 {regime} after REAL {('UP' if impulse_dir > 0 else 'DOWN')} SPIKE "
            f"({item.get('spike_primary_tf','MTF')}): retrace {retrace*100:.2f}%, "
            f"max retrace {max_retrace*100:.2f}%, recovery {recovery_fraction*100:.0f}%, "
            f"cont {cont_score}/8(req {cont_required}), fade {fade_score}/8(req {fade_required}), "
            f"15m aligned={cont_15m_aligned}, micro momentum={cont_micro_momentum}, "
            f"overextended={micro_overextended}, failed reclaim={failed_reclaim}, "
            f"fade micro={exhaust_micro_reversal}, vol faded={volume_faded}, "
            f"no-new-extreme={no_new_extreme_ok}, spike context={spike_context_ok}, "
            f"mom1 {mom1*100:+.2f}%, mom3 {mom3*100:+.2f}%, mom15 {mom15*100:+.2f}%, Vol1 x{vol1:.2f}. "
            f"PRO INTEL: state={intel_state} · squeeze={intel_squeeze:.0f} · overheat={intel_overheat:.0f} · "
            f"climax={intel_climax:.0f} · setup={intel_setup_quality:.0f} · ATRdist={intel_distance_atr:.1f} · "
            f"RSIctx={float(intel.get('rsi_fast',50.0)):.0f} · rejection={float(intel.get('rejection_wick',0.0))*100:.0f}% · "
            f"SETUP MASTER: CONT={float(intel.get('continuation_edge',0.0)):.0f} vs "
            f"PULLBACK={float(intel.get('pullback_risk',0.0)):.0f} · structure={float(intel.get('structure',0.0)):.0f} · "
            f"compression={float(intel.get('compression',0.0)):.0f} · false-break={float(intel.get('false_breakout',0.0)):.0f}."
        )

        # The legacy symbol quarantine existed, but the SPIKE REGIME route
        # bypassed it in V20.1. V20.2 enforces it here.
        symbol_ok, symbol_reason = symbol_quarantine_gate(setup)
        setup["symbol_quarantine_reason"] = symbol_reason
        if not symbol_ok:
            stats["symbol_quarantine_blocked"] += 1
            continue

        reentry_ok, reentry_reason = spike_regime_reentry_allowed(setup)
        setup["spike_reentry_reason"] = reentry_reason
        if not reentry_ok:
            stats["reentry_blocked"] += 1
            continue

        exec_ok, exec_reason = squeeze_execution_gate(setup, symbol, c1, c5)
        if not exec_ok:
            stats["execution_rejected"] += 1
            remaining.append(item)
            continue

        trade = calculate_exhaustion_trade(setup)
        if not trade:
            stats["risk_rejected"] += 1
            remaining.append(item)
            continue

        trade["paper_validation_origin"] += f" · {exec_reason}"

        cont_edge = float(trade.get("intel_continuation_edge", 0.0) or 0.0)
        pullback_risk = float(trade.get("intel_pullback_risk", 0.0) or 0.0)
        false_break = float(trade.get("intel_false_breakout", 0.0) or 0.0)
        structure_score = float(trade.get("intel_structure", 0.0) or 0.0)
        regime_name = str(trade.get("spike_regime", "")).upper()

        if regime_name == "CONTINUATION":
            setup_master_ok = (
                cont_edge >= SETUP_CONT_EDGE_MIN
                and (cont_edge - pullback_risk) >= SETUP_EDGE_MARGIN_MIN
                and pullback_risk <= SETUP_MAX_PULLBACK_RISK_CONT
                and structure_score >= SETUP_STRUCTURE_MIN_CONT
                and false_break <= SETUP_FALSE_BREAKOUT_MAX_CONT
            )
        else:
            setup_master_ok = (
                pullback_risk >= SETUP_FADE_EDGE_MIN
                and pullback_risk >= SETUP_MIN_PULLBACK_RISK_FADE
                and (pullback_risk - cont_edge) >= SETUP_EDGE_MARGIN_MIN
            )

        if not setup_master_ok:
            trade["paper_validation_origin"] += (
                f" · HIDDEN SETUP MASTER BLOCK: regime={regime_name} "
                f"CONT={cont_edge:.0f} vs PULLBACK={pullback_risk:.0f}, "
                f"structure={structure_score:.0f}, false-break={false_break:.0f}"
            )
            if add_shadow_signal(trade, SPIKE_SETUP_SELECTIVITY_BLOCK_REASON):
                stats["setup_selectivity_blocked"] += 1
            continue

        # Adaptive risk control #1: recent regime/side performance.
        strategy_ok, strategy_reason = strategy_circuit_breaker(trade)
        if not strategy_ok:
            trade["paper_validation_origin"] += f" · HIDDEN STRATEGY GUARD: {strategy_reason}"
            if add_shadow_signal(trade, SPIKE_STRATEGY_GUARD_REASON):
                stats["strategy_guard_blocked"] += 1
            continue

        # Adaptive risk control #2: champion/challenger model.
        # Warm-up never blocks. After a model passes chronological validation
        # twice, only its canary cohort is gated; rejected candidates remain
        # hidden and are tracked to closure for rollback and unbiased retraining.
        try:
            adaptive_ok, adaptive_reason, adaptive_probability = adaptive_gate(trade)
            trade["adaptive_reason"] = adaptive_reason
            if adaptive_probability is not None:
                trade["adaptive_probability"] = adaptive_probability
        except Exception as exc:
            adaptive_ok = True
            adaptive_reason = f"adaptive fail-open PAPER safety: {repr(exc)}"
            trade["adaptive_reason"] = adaptive_reason
            STATE["last_error"] = adaptive_reason
            stats["adaptive_errors"] += 1

        if not adaptive_ok:
            trade["paper_validation_origin"] += f" · HIDDEN ADAPTIVE BLOCK: {adaptive_reason}"
            if add_shadow_signal(trade, SPIKE_ADAPTIVE_BLOCK_REASON):
                stats["adaptive_blocked"] += 1
            continue

        trade["paper_validation_origin"] += f" · {strategy_reason} · {adaptive_reason}"
        if add_shadow_signal(trade, EXHAUSTION_PAPER_REASON):
            set_spike_regime_reentry_lock(trade)
            send_telegram(build_paper_signal_message(trade))
            stats["triggered"] += 1
            continue

        remaining.append(item)

    with STATE_IO_LOCK:
        STATE["squeeze_4h_watch"] = remaining
        STATE["last_squeeze_watch"] = dict(stats)
        save_state()
    return stats



def direct_measured_setup(
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
    c15: List[Dict[str, float]],
    c1h: List[Dict[str, float]],
    btc: Dict[str, Any],
    side: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """Independent V17.3 entry detector.

    This path deliberately does not call ``instant_edge_setup`` or any legacy
    trader template.  It first applies the one frozen archive hypothesis, then
    demands evidence that the current 1m candle is still closing in the same
    direction around EMA9/VWAP.  Execution liquidity and TP3 risk are checked
    separately after this function returns.
    """
    if len(c1) < 35 or len(c5) < 36 or len(c15) < 12 or len(c1h) < 30:
        return None, "candles"
    side = str(side).upper()
    if side not in DIRECT_MEASURED_STRATEGIES:
        return None, "side"

    direction = 1.0 if side == "LONG" else -1.0
    price = float(c1[-1]["close"])
    last = c1[-1]
    ch3 = percent_change(c1, 3)
    ch15 = percent_change(c5, 3)
    ch30 = percent_change(c5, 6)
    directional_3m = direction * ch3
    directional_15m = direction * ch15
    directional_30m = direction * ch30
    vol1 = volume_ratio(c1, 20)
    range1 = candle_range_ratio(c1, 20)
    vol5 = volume_ratio(c5, 20)
    range5 = candle_range_ratio(c5, 20)

    fixed_band = bool(
        MEASURED_MIN_DIRECTIONAL_3M
        <= directional_3m
        <= MEASURED_MAX_DIRECTIONAL_3M
        and MEASURED_MIN_DIRECTIONAL_15M
        <= directional_15m
        <= MEASURED_MAX_DIRECTIONAL_15M
        and directional_30m <= MEASURED_MAX_30M_CHASE
        and MEASURED_MIN_VOL1 <= vol1 <= MEASURED_MAX_VOL1
        and MEASURED_MIN_RANGE1 <= range1 <= MEASURED_MAX_RANGE1
        and MEASURED_MIN_VOL5 <= vol5 <= MEASURED_MAX_VOL5
        and range5 >= MEASURED_MIN_RANGE5
    )
    if not fixed_band:
        return None, "fixed_band"

    location = close_location(last)
    body_fraction = abs(last["close"] - last["open"]) / max(
        last["high"] - last["low"], 1e-12
    )
    directional_candle = bool(
        (side == "LONG" and last["close"] > last["open"])
        or (side == "SHORT" and last["close"] < last["open"])
    )
    close_quality = bool(
        location >= MEASURED_LONG_MIN_CLOSE_LOCATION
        if side == "LONG"
        else location <= MEASURED_SHORT_MAX_CLOSE_LOCATION
    )
    recent_closes = [float(row["close"]) for row in c1[-4:]]
    directional_steps = sum(
        1
        for previous, current in zip(recent_closes, recent_closes[1:])
        if direction * (current - previous) > 0
    )
    continuing_now = bool(
        directional_steps >= 2
        and direction * (recent_closes[-1] - recent_closes[-2]) > 0
    )
    ema9 = ema(closes(c1), 9)
    current_vwap = vwap(c1, 30)
    aligned = bool(
        (side == "LONG" and price >= ema9 and price >= current_vwap)
        or (side == "SHORT" and price <= ema9 and price <= current_vwap)
    )
    if not (
        directional_candle
        and close_quality
        and body_fraction >= MEASURED_MIN_BODY_FRACTION
        and continuing_now
        and aligned
    ):
        return None, "current_structure"

    score = 90
    score += min(4, int(max(0.0, directional_3m - MEASURED_MIN_DIRECTIONAL_3M) * 250))
    score += min(3, int(max(0.0, directional_15m - MEASURED_MIN_DIRECTIONAL_15M) * 100))
    score += min(2, directional_steps - 1)
    score += 1 if body_fraction >= 0.45 else 0
    score = min(100, max(90, score))
    level = (
        min(row["low"] for row in c1[-10:])
        if side == "LONG"
        else max(row["high"] for row in c1[-10:])
    )
    reason = (
        f"V17.3 ACTIVE TRADER PATTERN {side}: frozen band + live continuation; "
        f"3m {directional_3m*100:.2f}%, 15m {directional_15m*100:.2f}%, "
        f"Vol1 x{vol1:.2f}, Range1 x{range1:.2f}, Vol5 x{vol5:.2f}, "
        f"Range5 x{range5:.2f}, closeLoc {location:.2f}, body {body_fraction:.2f}."
    )
    return {
        "symbol": normalize_symbol(symbol),
        "side": side,
        "strategy": DIRECT_MEASURED_STRATEGIES[side],
        "trade_type": f"DIRECT MEASURED A+ {side}",
        "score": score,
        "grade": "A+",
        "entry": price,
        "level": level,
        "reason": reason,
        "pullback": 0.0,
        "volume_ratio": vol5,
        "range_ratio": range5,
        "compression": prior_compression_ratio(c5),
        "ch15m": ch15,
        "ch30m": ch30,
        "ch3m_1m": ch3,
        "vol1": vol1,
        "range1": range1,
        "ch2m": percent_change(c1, 2),
        "setup_mode": f"V17_2_2_DIRECT_MEASURED_{side}",
        "t1h": trend_state(c1h),
        "btc_text": btc.get("text", ""),
        "paper_setup_lane": "FOLLOW_THROUGH",
        "paper_validation_only": True,
        "paper_validation_immediate": True,
        "paper_validation_lane": PAPER_VALIDATION_REASON,
        "watch_impulse": directional_3m,
        "paper_validation_origin": reason,
    }, "direct_band_and_structure_passed"



def v17_3_2_followthrough_gate(
    trade: Dict[str, Any],
    btc: Dict[str, Any],
) -> Tuple[bool, str, float]:
    """Forward-only selector derived from the failed first 25 V17.3.1 outcomes.

    It does not rewrite the broad detector. Every broad candidate remains a
    paired V17.3.1 CONTROL observation. The selector only decides whether the
    same candidate is strong enough to enter the new V17.3.2 PAPER cohort.
    """
    if not V17_3_2_SELECTOR_ENABLED:
        return True, "selector disabled", 100.0

    side = str(trade.get("side", "")).upper()
    if side not in {"LONG", "SHORT"}:
        return False, "unknown side", 0.0

    if side == "SHORT" and not V17_3_2_SHORT_PAPER_ENABLED:
        return False, "SHORT kept in CONTROL/SHADOW after 0 TP3+ / 10 forward outcomes", 0.0

    direction = 1.0 if side == "LONG" else -1.0
    d3 = direction * float(trade.get("ch3m_1m", 0.0) or 0.0)
    d15 = direction * float(trade.get("ch15m", 0.0) or 0.0)
    d30 = direction * float(trade.get("ch30m", 0.0) or 0.0)
    vol1 = float(trade.get("vol1", 0.0) or 0.0)
    vol5 = float(trade.get("volume_ratio", 0.0) or 0.0)
    range1 = float(trade.get("range1", 0.0) or 0.0)
    range5 = float(trade.get("range_ratio", 0.0) or 0.0)

    # Compare the current 3m velocity with the average 15m velocity.
    # 1.0 means the last 3 minutes are moving at the same per-minute pace
    # as the full 15m window; >1 means acceleration.
    if d15 > 1e-9:
        pace_ratio = (d3 / 3.0) / (d15 / 15.0)
    else:
        pace_ratio = 0.0

    btc_dir = str(btc.get("direction", "UNKNOWN")).upper()
    btc_aligned = (
        (side == "LONG" and btc_dir != "BEAR")
        or (side == "SHORT" and btc_dir != "BULL")
    )
    exceptional = bool(
        d3 >= V17_3_2_EXCEPTIONAL_3M
        and d15 >= V17_3_2_EXCEPTIONAL_15M
        and range1 >= max(V17_3_2_MIN_RANGE1, 1.50)
    )

    hard_failures: List[str] = []
    if d3 < V17_3_2_MIN_DIRECTIONAL_3M:
        hard_failures.append(f"3m {d3*100:.2f}% < {V17_3_2_MIN_DIRECTIONAL_3M*100:.2f}%")
    if d15 < V17_3_2_MIN_DIRECTIONAL_15M:
        hard_failures.append(f"15m {d15*100:.2f}% < {V17_3_2_MIN_DIRECTIONAL_15M*100:.2f}%")
    if d30 < V17_3_2_MIN_DIRECTIONAL_30M:
        hard_failures.append(f"30m {d30*100:.2f}% too counter-directional")
    if vol1 < V17_3_2_MIN_VOL1:
        hard_failures.append(f"Vol1 x{vol1:.2f} < x{V17_3_2_MIN_VOL1:.2f}")
    if vol5 < V17_3_2_MIN_VOL5:
        hard_failures.append(f"Vol5 x{vol5:.2f} < x{V17_3_2_MIN_VOL5:.2f}")
    if range1 < V17_3_2_MIN_RANGE1:
        hard_failures.append(f"Range1 x{range1:.2f} < x{V17_3_2_MIN_RANGE1:.2f}")
    if range5 < V17_3_2_MIN_RANGE5:
        hard_failures.append(f"Range5 x{range5:.2f} < x{V17_3_2_MIN_RANGE5:.2f}")
    if pace_ratio < V17_3_2_MIN_PACE_RATIO and not exceptional:
        hard_failures.append(
            f"follow-through pace x{pace_ratio:.2f} < x{V17_3_2_MIN_PACE_RATIO:.2f}"
        )
    if not btc_aligned and not exceptional:
        hard_failures.append(f"BTC {btc_dir} conflicts with {side}")

    score = 45.0
    score += min(15.0, max(0.0, (d3 - 0.006) / 0.020) * 15.0)
    score += min(12.0, max(0.0, (d15 - 0.006) / 0.035) * 12.0)
    score += min(6.0, max(0.0, (d30 + 0.0025) / 0.040) * 6.0)
    score += min(6.0, max(0.0, (vol1 - 0.70) / 1.30) * 6.0)
    score += min(5.0, max(0.0, (range1 - 1.20) / 1.20) * 5.0)
    score += min(5.0, max(0.0, (range5 - 0.55) / 0.80) * 5.0)
    score += min(4.0, max(0.0, (pace_ratio - 0.80) / 1.20) * 4.0)
    score += 5.0 if btc_aligned else (2.0 if exceptional else 0.0)
    score = max(0.0, min(100.0, score))

    if hard_failures:
        return False, "; ".join(hard_failures[:4]), score
    if score < V17_3_2_MIN_SCORE:
        return False, f"follow-through score {score:.1f} < {V17_3_2_MIN_SCORE:.1f}", score

    return True, (
        f"V17.3.2 follow-through ok: score {score:.1f}; "
        f"3m {d3*100:.2f}%, 15m {d15*100:.2f}%, 30m {d30*100:.2f}%, "
        f"pace x{pace_ratio:.2f}, Vol1 x{vol1:.2f}, Vol5 x{vol5:.2f}, "
        f"Range1 x{range1:.2f}, Range5 x{range5:.2f}, BTC {btc_dir}"
    ), score


def direct_measured_risk_gate(
    trade: Dict[str, Any],
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
) -> Tuple[bool, str]:
    """V17.3 safety check aligned with the TP3 accounting rule."""
    entry = float(trade.get("entry", 0.0) or 0.0)
    sl = float(trade.get("sl", entry) or entry)
    if entry <= 0:
        return False, "invalid entry"
    sl_move = abs(entry - sl) / entry
    tp3_rr = TP3_MOVE / max(sl_move, 1e-12)
    if not LOCAL_SCALP_MIN_SL_MOVE <= sl_move <= LOCAL_SCALP_MAX_SL_MOVE * 1.02:
        return False, f"local SL {sl_move*100:.2f}% outside safety band"
    if tp3_rr < 1.25:
        return False, f"TP3 RR {tp3_rr:.2f} < 1.25"
    if float(trade.get("ladder_rr", 0.0) or 0.0) < 1.20:
        return False, f"ladder RR {float(trade.get('ladder_rr', 0.0) or 0.0):.2f} < 1.20"
    recent_high = max(row["high"] for row in c5[-6:])
    recent_low = min(row["low"] for row in c5[-6:])
    recent_range = (recent_high - recent_low) / entry
    atr_capacity = atr(c5, 14) / entry * 3.0
    feasible_move = max(recent_range, atr_capacity)
    if feasible_move < TP3_MOVE * 0.75:
        return False, (
            f"TP3 capacity {feasible_move*100:.2f}% < "
            f"{TP3_MOVE*0.75*100:.2f}%"
        )
    return True, (
        f"direct TP3 risk passed: SL {sl_move*100:.2f}%, TP3 RR {tp3_rr:.2f}, "
        f"capacity {feasible_move*100:.2f}%, ladder RR "
        f"{float(trade.get('ladder_rr', 0.0) or 0.0):.2f}"
    )


def v17_2_paper_risk_gate(
    trade: Dict[str, Any],
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
) -> Tuple[bool, str]:
    """Risk/feasibility gate aligned to TP3, the actual success definition."""
    entry = float(trade.get("entry", 0.0) or 0.0)
    sl = float(trade.get("sl", entry) or entry)
    if entry <= 0:
        return False, "bad entry"
    sl_move = abs(entry - sl) / entry
    if sl_move > LOCAL_SCALP_MAX_SL_MOVE * 1.10:
        return False, f"local SL too wide {sl_move*100:.2f}%"
    if float(trade.get("ladder_rr", 0.0) or 0.0) < MIN_LADDER_RR_HARD:
        return False, f"ladder RR too low {float(trade.get('ladder_rr', 0.0) or 0.0):.2f}"
    if float(trade.get("final_rr", 0.0) or 0.0) < MIN_FINAL_RR_HARD:
        return False, f"final RR too low {float(trade.get('final_rr', 0.0) or 0.0):.2f}"
    recent_high = max(row["high"] for row in c5[-6:])
    recent_low = min(row["low"] for row in c5[-6:])
    recent_range = (recent_high - recent_low) / entry
    atr_move = atr(c5, 14) / entry
    feasible_move = max(recent_range, atr_move * 3.0)
    if feasible_move < TP3_MOVE * 0.75:
        return False, (
            f"TP3 feasibility weak: recent/ATR capacity {feasible_move*100:.2f}% "
            f"< {TP3_MOVE*0.75*100:.2f}%"
        )
    return True, (
        f"V17.2 risk passed: SL {sl_move*100:.2f}%, capacity {feasible_move*100:.2f}%, "
        f"ladder RR {float(trade.get('ladder_rr', 0.0) or 0.0):.2f}"
    )


def measured_edge_entry_gate(
    setup: Dict[str, Any],
    symbol: str,
    c1: List[Dict[str, float]],
    c5: List[Dict[str, float]],
) -> Tuple[bool, str]:
    """Frozen V17.3 cohort and real execution-cost check.

    The measured candle band comes from the complete archive; the order-book
    check is new forward evidence and is intentionally unavailable to the old
    rows.  A candidate must pass both.  No parameter in this function is
    changed automatically while the 50-outcome cohort is being collected.
    """
    side = str(setup.get("side", "")).upper()
    direction = 1.0 if side == "LONG" else -1.0
    grade = str(setup.get("grade", "B")).upper()
    strategy = str(setup.get("strategy", "")).upper()
    directional_3m = direction * float(setup.get("ch3m_1m", 0.0) or 0.0)
    directional_15m = direction * float(setup.get("ch15m", 0.0) or 0.0)
    vol1 = float(setup.get("vol1", 0.0) or 0.0)
    range1 = float(setup.get("range1", 0.0) or 0.0)
    vol5 = float(setup.get("volume_ratio", 0.0) or 0.0)
    range5 = float(setup.get("range_ratio", 0.0) or 0.0)

    failures: List[str] = []
    if grade != "A+":
        failures.append("grade is not A+")
    if strategy != DIRECT_MEASURED_STRATEGIES.get(side, ""):
        failures.append("not PRO_DIRECT_MEASURED")
    if not MEASURED_MIN_VOL1 <= vol1 <= MEASURED_MAX_VOL1:
        failures.append(
            f"Vol1 x{vol1:.2f} outside x{MEASURED_MIN_VOL1:.2f}–x{MEASURED_MAX_VOL1:.2f}"
        )
    if not MEASURED_MIN_RANGE1 <= range1 <= MEASURED_MAX_RANGE1:
        failures.append(
            f"Range1 x{range1:.2f} outside x{MEASURED_MIN_RANGE1:.2f}–x{MEASURED_MAX_RANGE1:.2f}"
        )
    if not MEASURED_MIN_DIRECTIONAL_3M <= directional_3m <= MEASURED_MAX_DIRECTIONAL_3M:
        failures.append(
            f"directional 3m {directional_3m*100:.2f}% outside "
            f"{MEASURED_MIN_DIRECTIONAL_3M*100:.2f}%–{MEASURED_MAX_DIRECTIONAL_3M*100:.2f}%"
        )
    if not MEASURED_MIN_DIRECTIONAL_15M <= directional_15m <= MEASURED_MAX_DIRECTIONAL_15M:
        failures.append(
            f"directional 15m {directional_15m*100:.2f}% outside "
            f"{MEASURED_MIN_DIRECTIONAL_15M*100:.2f}%–{MEASURED_MAX_DIRECTIONAL_15M*100:.2f}%"
        )
    if not MEASURED_MIN_VOL5 <= vol5 <= MEASURED_MAX_VOL5:
        failures.append(
            f"Vol5 x{vol5:.2f} outside x{MEASURED_MIN_VOL5:.2f}–x{MEASURED_MAX_VOL5:.2f}"
        )
    if range5 < MEASURED_MIN_RANGE5:
        failures.append(f"Range5 x{range5:.2f} < x{MEASURED_MIN_RANGE5:.2f}")
    if failures:
        return False, "; ".join(failures)

    liquidity = candle_liquidity_snapshot(symbol, c1, c5)
    cached = LIQUIDITY_SCAN_CACHE.get(normalize_symbol(symbol), {})
    rank = float(cached.get("rank_percentile", 0.0) or 0.0)
    quote_60m = float(liquidity.get("quote_60m", 0.0) or 0.0)
    if not bool(liquidity.get("ok")):
        return False, f"liquidity continuity rejected: {liquidity.get('reason', 'unknown')}"
    if rank < MEASURED_MIN_LIQUIDITY_RANK:
        return False, f"liquidity rank p{rank*100:.0f} below p{MEASURED_MIN_LIQUIDITY_RANK*100:.0f}"
    if quote_60m < MEASURED_MIN_QUOTE_60M:
        return False, (
            f"60m quote-turnover proxy {quote_60m:.0f} < {MEASURED_MIN_QUOTE_60M:.0f}"
        )

    book = execution_book_snapshot(symbol)
    spread_bps = float(book.get("spread_bps", 999.0) or 999.0)
    depth_usdt = float(book.get("depth_usdt", 0.0) or 0.0)
    if not bool(book.get("ok")):
        return False, str(book.get("reason", "order book unavailable"))
    if spread_bps > MEASURED_MAX_BOOK_SPREAD_BPS:
        return False, (
            f"spread {spread_bps:.1f} bps > {MEASURED_MAX_BOOK_SPREAD_BPS:.1f} bps"
        )
    if depth_usdt < MEASURED_MIN_BOOK_DEPTH_USDT:
        return False, (
            f"visible depth {depth_usdt:.0f} USDT < {MEASURED_MIN_BOOK_DEPTH_USDT:.0f}"
        )

    mark_entry = float(setup.get("entry", 0.0) or 0.0)
    executable_entry = float(
        book.get("ask" if side == "LONG" else "bid", mark_entry) or mark_entry
    )
    if executable_entry <= 0:
        return False, "invalid executable bid/ask"
    setup["execution_mark_entry"] = mark_entry
    setup["entry"] = executable_entry
    setup["book_spread_bps"] = spread_bps
    setup["book_depth_usdt"] = depth_usdt
    setup["liquidity_quote_60m"] = quote_60m
    setup["liquidity_rank_percentile"] = rank
    setup["liquidity_active_fraction"] = float(
        liquidity.get("active_fraction", 0.0) or 0.0
    )
    setup["liquidity_unique_fraction"] = float(
        liquidity.get("unique_fraction", 0.0) or 0.0
    )
    setup["atr1_pct"] = float(liquidity.get("atr1_pct", 0.0) or 0.0)
    setup["normalized_directional_move"] = directional_3m / max(
        float(setup["atr1_pct"] or 0.0), 1e-6
    )
    setup["paper_setup_lane"] = "DIRECT_MEASURED"
    setup["watch_impulse"] = directional_3m
    return True, (
        f"direct measured cohort passed: Vol1 x{vol1:.2f}, Range1 x{range1:.2f}, "
        f"3m {directional_3m*100:.2f}%, 15m {directional_15m*100:.2f}%, "
        f"spread {spread_bps:.1f} bps, depth {depth_usdt:.0f} USDT, liq p{rank*100:.0f}"
    )


def calculate_fast_trade(setup: Dict[str, Any], c1: List[Dict[str, float]], c5: List[Dict[str, float]]) -> Optional[Dict[str, Any]]:
    side = setup["side"]
    entry = setup["entry"]
    level = setup["level"]
    a = atr(c5, 14)
    instant = str(setup.get("setup_mode", "")).startswith("INSTANT")
    buffer = max(entry * (0.0016 if instant else 0.0022), a * (0.55 if instant else SL_ATR_MULT))

    if side == "LONG":
        recent_source = c1[-10:] + (c5[-2:] if instant else c5[-4:])
        recent_low = min(x["low"] for x in recent_source)
        sl = min(level, recent_low) - buffer
        sl = min(sl, entry * (1 - MIN_SL_MOVE))
        tp1 = entry * (1 + TP1_MOVE)
        tp2 = entry * (1 + TP2_MOVE)
        tp3 = entry * (1 + TP3_MOVE)
        tp4 = entry * (1 + TP4_MOVE)
        tp5 = entry * (1 + TP5_MOVE)
    else:
        recent_source = c1[-10:] + (c5[-2:] if instant else c5[-4:])
        recent_high = max(x["high"] for x in recent_source)
        sl = max(level, recent_high) + buffer
        sl = max(sl, entry * (1 + MIN_SL_MOVE))
        tp1 = entry * (1 - TP1_MOVE)
        tp2 = entry * (1 - TP2_MOVE)
        tp3 = entry * (1 - TP3_MOVE)
        tp4 = entry * (1 - TP4_MOVE)
        tp5 = entry * (1 - TP5_MOVE)

    risk = abs(entry - sl)
    risk_move = risk / max(entry, 1e-12)

    # V13.29 professional scalp rule:
    # The public trader examples use a far invalidation/averaging zone, but the signal itself
    # must be managed by a local scalp stop. If the structural stop is too far, compress it
    # to a local stop for fast execution instead of discarding every live dump candidate.
    setup_mode = str(setup.get("setup_mode", ""))
    if LOCAL_SCALP_STOP_ENABLED and risk_move > LOCAL_SCALP_MAX_SL_MOVE and setup_mode in LOCAL_STOP_MODES:
        local_move = max(LOCAL_SCALP_MIN_SL_MOVE, min(LOCAL_SCALP_MAX_SL_MOVE, max(TP1_MOVE * 1.20, abs(float(setup.get("ch3m_1m", 0.0) or 0.0)) * 1.10)))
        if side == "LONG":
            sl = entry * (1 - local_move)
        else:
            sl = entry * (1 + local_move)
        setup["local_stop_used"] = True
        setup["original_sl_move"] = risk_move
        risk = abs(entry - sl)
        risk_move = risk / max(entry, 1e-12)

    if risk_move > MAX_SL_MOVE:
        return None

    rewards = [abs(tp1 - entry), abs(tp2 - entry), abs(tp3 - entry), abs(tp4 - entry), abs(tp5 - entry)]
    rr = rewards[0] / risk if risk > 0 else 0.0
    ladder_rr = (sum(rewards) / len(rewards)) / risk if risk > 0 else 0.0
    final_rr = rewards[-1] / risk if risk > 0 else 0.0
    roi_tp1 = rewards[0] / entry * LEVERAGE * 100
    roi_sl = risk / entry * LEVERAGE * 100

    return {
        **setup,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "tp4": tp4,
        "tp5": tp5,
        "rr": rr,
        "ladder_rr": ladder_rr,
        "final_rr": final_rr,
        "roi_tp1": roi_tp1,
        "roi_sl": roi_sl,
        "risk_mult": A_RISK_MULT if setup["grade"] == "A+" else FAST_RISK_MULT,
        "local_stop_used": bool(setup.get("local_stop_used", False)),
        "original_sl_move": float(setup.get("original_sl_move", 0.0) or 0.0),
        "created_at": now_ts(),
        "status": "active",
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "tp4_hit": False,
        "tp5_hit": False,
    }



def professional_quality_gate(trade: Dict[str, Any], symbol: str) -> Tuple[bool, str, str]:
    """Final professional quality filter.

    This is intentionally hard. A fast scalp is not allowed when:
    - stop risk is much larger than the reward ladder;
    - TP5 does not at least compensate risk;
    - live 1m volume is weak without a strong price/range exception;
    - heavy/slow coins have wide SL and weak RR.
    """
    side = trade.get("side", "?")
    base = base_asset(symbol)
    rr = float(trade.get("rr", 0.0) or 0.0)
    ladder_rr = float(trade.get("ladder_rr", 0.0) or 0.0)
    final_rr = float(trade.get("final_rr", 0.0) or 0.0)
    roi_sl = float(trade.get("roi_sl", 999.0) or 999.0)
    vol1 = float(trade.get("vol1", 1.0) or 1.0)
    range1 = float(trade.get("range1", 1.0) or 1.0)
    ch3m = abs(float(trade.get("ch3m_1m", 0.0) or 0.0))

    # Hard stop check is price-based first, because ROI depends on chosen leverage.
    # At x20 a normal 1.1% local scalp stop looks like 22% ROI, which should not be blocked
    # if final RR and ladder RR are healthy.
    sl_price_move = roi_sl / max(LEVERAGE * 100.0, 1e-12)
    if sl_price_move > LOCAL_SCALP_MAX_SL_MOVE * 1.10:
        return False, "sl_price_too_high_block", f"{display_symbol(symbol)} {side}: SL price risk too high {sl_price_move*100:.2f}%"

    if roi_sl > MAX_SCALP_SL_ROI:
        return False, "sl_roi_too_high_block", f"{display_symbol(symbol)} {side}: SL risk too high {roi_sl:.1f}% ROI"

    if rr < MIN_TP1_RR:
        return False, "tp1_rr_hard_block", f"{display_symbol(symbol)} {side}: TP1 RR too weak {rr:.2f}"

    if ladder_rr < MIN_LADDER_RR_HARD:
        return False, "ladder_rr_hard_block", f"{display_symbol(symbol)} {side}: ladder RR too weak {ladder_rr:.2f}"

    if final_rr < MIN_FINAL_RR_HARD:
        return False, "final_rr_hard_block", f"{display_symbol(symbol)} {side}: final RR too weak {final_rr:.2f}"

    if vol1 < MIN_LIVE_VOL_NORMAL:
        strong_price_exception = (
            vol1 >= MIN_LIVE_VOL_STRONG_PRICE
            and ch3m >= STRONG_1M3_MOVE
            and range1 >= STRONG_RANGE1
        )
        if not strong_price_exception:
            return (
                False,
                "weak_live_volume_block",
                f"{display_symbol(symbol)} {side}: weak live volume x{vol1:.2f}, 1m3 {ch3m*100:.2f}%, range1 x{range1:.2f}"
            )

    if base in HEAVY_BASES:
        if roi_sl > HEAVY_MAX_SL_ROI:
            return False, "heavy_coin_sl_block", f"{display_symbol(symbol)} {side}: heavy coin SL too wide {roi_sl:.1f}% ROI"
        if final_rr < HEAVY_MIN_FINAL_RR:
            return False, "heavy_coin_rr_block", f"{display_symbol(symbol)} {side}: heavy coin final RR too weak {final_rr:.2f}"
        if vol1 < HEAVY_MIN_LIVE_VOL:
            return False, "heavy_coin_volume_block", f"{display_symbol(symbol)} {side}: heavy coin live volume weak x{vol1:.2f}"

    return True, "ok", "quality ok"



def aero_style_gate(trade: Dict[str, Any], symbol: str, c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], btc: Dict[str, Any]) -> Tuple[bool, str, str]:
    """V13.27 AERO/PORTAL-style gate.

    Looks for the specific trader structure:
    - SHORT: recent upper pullback/stop-hunt -> loss of momentum -> 1m breakdown.
    - LONG: recent lower sweep -> reclaim -> 1m breakout.

    This does not replace RR/SL filters. It is a structure-quality exception so the bot
    can catch examples-style trades without accepting random weak B signals.
    """
    if not AERO_STYLE_GATE_ENABLED:
        return False, "aero_disabled", "aero-style disabled"
    if len(c1) < 35 or len(c5) < 20:
        return False, "aero_no_candles", f"{display_symbol(symbol)}: not enough candles for AERO-style gate"

    side = str(trade.get("side", ""))
    if side == "SHORT" and not AERO_SHORT_ENABLED:
        return False, "aero_short_disabled", f"{display_symbol(symbol)} SHORT: AERO short disabled"
    if side == "LONG" and not AERO_LONG_ENABLED:
        return False, "aero_long_disabled", f"{display_symbol(symbol)} LONG: AERO long disabled"

    entry = float(trade.get("entry", 0.0) or 0.0)
    if entry <= 0:
        return False, "aero_bad_entry", f"{display_symbol(symbol)} {side}: bad entry"

    ch3m = float(trade.get("ch3m_1m", 0.0) or 0.0)
    vol1 = float(trade.get("vol1", 1.0) or 1.0)
    vol5 = float(trade.get("volume_ratio", trade.get("vol5", 1.0)) or 1.0)
    range1 = float(trade.get("range1", 1.0) or 1.0)
    range5 = float(trade.get("range_ratio", trade.get("range5", 1.0)) or 1.0)
    loc = close_location(c1[-1])
    e1 = ema([x["close"] for x in c1[-25:]], 9)
    e5 = ema([x["close"] for x in c5[-30:]], 9)
    recent_1m = c1[-18:]
    recent_5m = c5[-8:]
    recent_high = max(x["high"] for x in recent_1m + recent_5m[-3:])
    recent_low = min(x["low"] for x in recent_1m + recent_5m[-3:])
    recent_range = (recent_high - recent_low) / max(entry, 1e-12)

    if recent_range < AERO_MIN_RECENT_RANGE:
        return False, "aero_recent_range_block", f"{display_symbol(symbol)} {side}: recent range too small {recent_range*100:.2f}%"
    if vol1 < AERO_MIN_VOL1:
        return False, "aero_vol1_block", f"{display_symbol(symbol)} {side}: vol1 too weak x{vol1:.2f}"
    if vol5 < AERO_MIN_VOL5:
        return False, "aero_vol5_block", f"{display_symbol(symbol)} {side}: vol5 too weak x{vol5:.2f}"
    if range1 < AERO_MIN_RANGE1:
        return False, "aero_range1_block", f"{display_symbol(symbol)} {side}: range1 too weak x{range1:.2f}"
    if range5 < AERO_MIN_RANGE5:
        return False, "aero_range5_block", f"{display_symbol(symbol)} {side}: range5 too weak x{range5:.2f}"

    if side == "SHORT":
        pullback = (recent_high - entry) / max(entry, 1e-12)
        if pullback < AERO_MIN_PULLBACK:
            return False, "aero_pullback_block", f"{display_symbol(symbol)} SHORT: no upper pullback/reject; pullback {pullback*100:.2f}%"
        if pullback > AERO_MAX_PULLBACK:
            return False, "aero_spike_block", f"{display_symbol(symbol)} SHORT: spike too extreme {pullback*100:.2f}%"
        if ch3m > -AERO_MIN_1M3:
            return False, "aero_pressure_block", f"{display_symbol(symbol)} SHORT: no live breakdown 1m3 {ch3m*100:+.2f}%"
        if loc > AERO_CLOSE_SHORT or c1[-1]["close"] >= c1[-1]["open"]:
            return False, "aero_reject_close_block", f"{display_symbol(symbol)} SHORT: last 1m not rejected near low"
        if c1[-1]["close"] >= min(x["low"] for x in c1[-7:-1]):
            return False, "aero_breakdown_block", f"{display_symbol(symbol)} SHORT: no fresh local breakdown"
        if AERO_REQUIRE_EMA_REJECT and not (c1[-1]["close"] < e1 or c1[-1]["close"] < e5):
            return False, "aero_ema_reject_block", f"{display_symbol(symbol)} SHORT: no EMA/VWAP-style rejection"
        return True, "aero_style_short_ok", (
            f"AERO-style SHORT ok: upper pullback/reject {pullback*100:.2f}%, "
            f"live breakdown 1m3 {ch3m*100:+.2f}%, range {recent_range*100:.2f}%, "
            f"vol1 x{vol1:.2f}, range1 x{range1:.2f}"
        )

    if side == "LONG":
        sweep = (entry - recent_low) / max(entry, 1e-12)
        if sweep < AERO_MIN_PULLBACK:
            return False, "aero_sweep_block", f"{display_symbol(symbol)} LONG: no lower sweep/reclaim; sweep {sweep*100:.2f}%"
        if sweep > AERO_MAX_PULLBACK:
            return False, "aero_spike_block", f"{display_symbol(symbol)} LONG: spike too extreme {sweep*100:.2f}%"
        if ch3m < AERO_MIN_1M3:
            return False, "aero_pressure_block", f"{display_symbol(symbol)} LONG: no live reclaim 1m3 {ch3m*100:+.2f}%"
        if loc < AERO_CLOSE_LONG or c1[-1]["close"] <= c1[-1]["open"]:
            return False, "aero_reclaim_close_block", f"{display_symbol(symbol)} LONG: last 1m not reclaimed near high"
        if c1[-1]["close"] <= max(x["high"] for x in c1[-7:-1]):
            return False, "aero_breakout_block", f"{display_symbol(symbol)} LONG: no fresh local breakout"
        if AERO_REQUIRE_EMA_REJECT and not (c1[-1]["close"] > e1 or c1[-1]["close"] > e5):
            return False, "aero_ema_reclaim_block", f"{display_symbol(symbol)} LONG: no EMA/VWAP-style reclaim"
        return True, "aero_style_long_ok", (
            f"AERO-style LONG ok: lower sweep/reclaim {sweep*100:.2f}%, "
            f"live reclaim 1m3 {ch3m*100:+.2f}%, range {recent_range*100:.2f}%, "
            f"vol1 x{vol1:.2f}, range1 x{range1:.2f}"
        )

    return False, "aero_side_block", f"{display_symbol(symbol)}: unknown side {side}"

def trader_pattern_gate(trade: Dict[str, Any], symbol: str, c1: List[Dict[str, float]], c5: List[Dict[str, float]], c15: List[Dict[str, float]], btc: Dict[str, Any]) -> Tuple[bool, str, str]:
    """Example-style final gate.

    The goal is to block signals that are technically valid but not trader-quality:
    - weak B-class entries with no live volume;
    - tiny or stale continuation;
    - counter-direction entries without true reversal strength;
    - target ladders that require more movement than the recent market has shown;
    - heavy coins unless the setup is genuinely A+.
    """
    if not TRADER_PATTERN_GATE_ENABLED:
        return True, "ok", "trader pattern gate disabled"

    side = str(trade.get("side", ""))
    base = base_asset(symbol)
    score = int(trade.get("score", 0) or 0)
    grade = str(trade.get("grade", "B"))
    setup_mode = str(trade.get("setup_mode", ""))
    ch3m = float(trade.get("ch3m_1m", 0.0) or 0.0)
    ch15m = float(trade.get("ch15m", 0.0) or 0.0)
    ch30m = float(trade.get("ch30m", 0.0) or 0.0)
    vol1 = float(trade.get("vol1", 1.0) or 1.0)
    vol5 = float(trade.get("volume_ratio", trade.get("vol5", 1.0)) or 1.0)
    range1 = float(trade.get("range1", 1.0) or 1.0)
    range5 = float(trade.get("range_ratio", trade.get("range5", 1.0)) or 1.0)
    entry = float(trade.get("entry", 0.0) or 0.0)
    tp5 = float(trade.get("tp5", 0.0) or 0.0)

    aero_ok, aero_block, aero_reason = aero_style_gate(trade, symbol, c1, c5, c15, btc)

    if grade != "A+" and not TRADER_ALLOW_B_SCORE:
        return False, "trader_grade_block", f"{display_symbol(symbol)} {side}: B-class skipped by env; set TRADER_ALLOW_B_SCORE=true to allow B+"

    if score < TRADER_MIN_SCORE:
        if not (aero_ok and AERO_ALLOW_B_SCORE and score >= max(72, TRADER_MIN_SCORE - 10)):
            return False, "trader_score_block", f"{display_symbol(symbol)} {side}: trader score too low {score} < {TRADER_MIN_SCORE}"

    # Balanced B+ mode: B setups are allowed, but only if the current tape is alive.
    # This keeps the bot from going silent while still blocking random weak B entries.
    if grade != "A+":
        if abs(ch3m) < TRADER_MIN_ABS_1M3 * 1.20 and vol1 < TRADER_MIN_VOL1 * 1.20 and range1 < TRADER_MIN_RANGE1 * 1.15:
            if not aero_ok:
                return False, "trader_bplus_quality_block", (
                    f"{display_symbol(symbol)} {side}: B+ not strong enough; 1m3 {ch3m*100:+.2f}%, "
                    f"vol1 x{vol1:.2f}, range1 x{range1:.2f}"
                )

    if base in HEAVY_BASES and TRADER_HEAVY_ONLY_A_PLUS and grade != "A+":
        return False, "trader_heavy_grade_block", f"{display_symbol(symbol)} {side}: heavy coin requires A+"

    # Directional pressure must exist now. Examples are not slow predictions.
    if side == "LONG":
        if ch3m < TRADER_MIN_ABS_1M3:
            return False, "trader_live_pressure_block", f"{display_symbol(symbol)} LONG: weak live pressure 1m3 {ch3m*100:+.2f}%"
        if TRADER_NEED_5M_DIRECTION and c5[-1]["close"] <= c5[-2]["close"]:
            return False, "trader_5m_direction_block", f"{display_symbol(symbol)} LONG: last 5m not confirming up"
        if close_location(c1[-1]) < TRADER_CLOSE_LONG:
            return False, "trader_close_location_block", f"{display_symbol(symbol)} LONG: 1m close not near high"
        if TRADER_REQUIRE_MICRO_BREAK and c1[-1]["close"] <= max(x["high"] for x in c1[-6:-1]):
            return False, "trader_micro_break_block", f"{display_symbol(symbol)} LONG: no fresh 1m high break"
        aligned = ch15m >= TRADER_MIN_ABS_15M and ch30m >= -TRADER_MAX_COUNTER_30M
        reversal_exception = setup_mode.startswith("REVERSAL") and ch3m >= TRADER_MIN_ABS_1M3 * 1.5 and range1 >= TRADER_MIN_RANGE1 * 1.25
    else:
        if ch3m > -TRADER_MIN_ABS_1M3:
            return False, "trader_live_pressure_block", f"{display_symbol(symbol)} SHORT: weak live pressure 1m3 {ch3m*100:+.2f}%"
        if TRADER_NEED_5M_DIRECTION and c5[-1]["close"] >= c5[-2]["close"]:
            return False, "trader_5m_direction_block", f"{display_symbol(symbol)} SHORT: last 5m not confirming down"
        if close_location(c1[-1]) > TRADER_CLOSE_SHORT:
            return False, "trader_close_location_block", f"{display_symbol(symbol)} SHORT: 1m close not near low"
        dump_exception = setup_mode.startswith("MARKET_DUMP") and ch3m <= -TRADER_MIN_ABS_1M3 * 1.10 and range1 >= max(0.45, TRADER_MIN_RANGE1 * 0.60)
        if TRADER_REQUIRE_MICRO_BREAK and c1[-1]["close"] >= min(x["low"] for x in c1[-6:-1]) and not dump_exception:
            return False, "trader_micro_break_block", f"{display_symbol(symbol)} SHORT: no fresh 1m low break"
        aligned = ch15m <= -TRADER_MIN_ABS_15M and ch30m <= TRADER_MAX_COUNTER_30M
        reversal_exception = setup_mode.startswith("REVERSAL") and abs(ch3m) >= TRADER_MIN_ABS_1M3 * 1.5 and range1 >= TRADER_MIN_RANGE1 * 1.25

    if TRADER_BLOCK_WEAK_CONTINUATION and not (aligned or reversal_exception or aero_ok or setup_mode.startswith("MARKET_DUMP")):
        return False, "trader_structure_block", (
            f"{display_symbol(symbol)} {side}: weak structure; 15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, mode {setup_mode}"
        )

    if vol1 < TRADER_MIN_VOL1 and not aero_ok:
        return False, "trader_vol1_block", f"{display_symbol(symbol)} {side}: live vol1 too weak x{vol1:.2f}"
    if vol5 < TRADER_MIN_VOL5 and not aero_ok:
        return False, "trader_vol5_block", f"{display_symbol(symbol)} {side}: vol5 too weak x{vol5:.2f}"
    if range1 < TRADER_MIN_RANGE1 and not aero_ok:
        return False, "trader_range1_block", f"{display_symbol(symbol)} {side}: range1 too weak x{range1:.2f}"
    if range5 < TRADER_MIN_RANGE5 and not aero_ok:
        return False, "trader_range5_block", f"{display_symbol(symbol)} {side}: range5 too weak x{range5:.2f}"

    # TP5 should be plausible from current market expansion, not a fantasy target.
    if entry > 0 and tp5 > 0:
        need = abs(entry - tp5) / entry
        recent_move = max(abs(ch15m), abs(ch30m), abs(percent_change(c5, 6)))
        if recent_move < need * TRADER_MIN_TP5_FEASIBILITY:
            return False, "trader_tp5_feasibility_block", (
                f"{display_symbol(symbol)} {side}: TP5 move {need*100:.2f}% not feasible vs recent {recent_move*100:.2f}%"
            )

    style_note = aero_reason if aero_ok else "standard trader-pattern ok"
    return True, "ok", (
        f"{style_note}; score {score}, grade {grade}, 1m3 {ch3m*100:+.2f}%, "
        f"15m {ch15m*100:+.2f}%, 30m {ch30m*100:+.2f}%, vol1 x{vol1:.2f}, range1 x{range1:.2f}"
    )

def cooldown_ok(symbol: str, strategy: str) -> Tuple[bool, str]:
    t = now_ts()
    if t < STATE.setdefault("pair_cooldown", {}).get(symbol, 0):
        return False, "pair cooldown"
    if t < STATE.setdefault("strategy_cooldown", {}).get(strategy, 0):
        return False, "strategy cooldown"
    return True, "ok"


def _prune_live_send_history(current_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    current = int(current_ts or now_ts())
    cutoff = current - 24 * 60 * 60
    raw_history = STATE.setdefault("live_send_history", [])
    if not raw_history:
        raw_history = [
            {"ts": value, "side": "UNKNOWN", "model_version": 0}
            for value in STATE.setdefault("live_send_timestamps", [])
        ]
    cleaned: List[Dict[str, Any]] = []
    for value in raw_history:
        try:
            if isinstance(value, dict):
                timestamp = int(value.get("ts", 0) or 0)
                side = str(value.get("side", "UNKNOWN")).upper()
                model_version = int(value.get("model_version", 0) or 0)
            else:
                timestamp = int(value)
                side = "UNKNOWN"
                model_version = 0
        except Exception:
            continue
        if timestamp > cutoff:
            cleaned.append(
                {"ts": timestamp, "side": side, "model_version": model_version}
            )
    cleaned.sort(key=lambda item: int(item["ts"]))
    STATE["live_send_history"] = cleaned
    STATE["live_send_timestamps"] = [int(item["ts"]) for item in cleaned]
    return cleaned


def live_signal_budget_24h(current_ts: Optional[int] = None) -> Dict[str, Any]:
    current = int(current_ts or now_ts())
    history = _prune_live_send_history(current)
    side_counts = {"LONG": 0, "SHORT": 0}
    adaptive_count = 0
    for item in history:
        side = str(item.get("side", "UNKNOWN")).upper()
        if side in side_counts:
            side_counts[side] += 1
        if int(item.get("model_version", 0) or 0) > 0:
            adaptive_count += 1
    total = len(history)
    last_ts = int(history[-1]["ts"]) if history else 0
    spacing_left = max(0, MIN_LIVE_SIGNAL_SPACING_SECONDS - (current - last_ts))
    return {
        "total": total,
        "remaining": 10**9 if MAX_LIVE_SIGNALS_24H <= 0 else max(0, MAX_LIVE_SIGNALS_24H - total),
        "long": side_counts["LONG"],
        "short": side_counts["SHORT"],
        "adaptive": adaptive_count,
        "spacing_left": spacing_left,
        "last_ts": last_ts,
    }


def data_entry_quality_gate(trade: Dict[str, Any]) -> Tuple[bool, str]:
    """Joint entry rule validated on chronological slices of the saved 100 outcomes."""
    if not DATA_ENTRY_GATE_ENABLED:
        return True, "data entry gate disabled"
    side = str(trade.get("side", "")).upper()
    direction = 1.0 if side == "LONG" else -1.0
    vol1 = float(trade.get("vol1", 0.0) or 0.0)
    range1 = float(trade.get("range1", 0.0) or 0.0)
    directional_3m = direction * float(trade.get("ch3m_1m", 0.0) or 0.0)
    failures: List[str] = []
    if vol1 < DATA_MIN_VOL1:
        failures.append(f"Vol1 x{vol1:.2f} < x{DATA_MIN_VOL1:.2f}")
    if range1 < DATA_MIN_RANGE1:
        failures.append(f"Range1 x{range1:.2f} < x{DATA_MIN_RANGE1:.2f}")
    if directional_3m < DATA_MIN_DIRECTIONAL_3M:
        failures.append(
            f"directional 3m {directional_3m*100:.2f}% < {DATA_MIN_DIRECTIONAL_3M*100:.2f}%"
        )
    accepted = not failures
    reason = (
        f"data gate passed: Vol1 x{vol1:.2f}, Range1 x{range1:.2f}, "
        f"directional 3m {directional_3m*100:.2f}%"
        if accepted
        else "data gate blocked: " + "; ".join(failures)
    )
    trade["data_entry_gate_accepted"] = accepted
    trade["data_entry_gate_reason"] = reason
    return accepted, reason


def paper_pullback_challenger_eligible(trade: Dict[str, Any]) -> bool:
    """Select the fixed V17.1 moderate-impulse cohort for WATCH, never LIVE."""
    side = str(trade.get("side", "")).upper()
    direction = 1.0 if side == "LONG" else -1.0
    directional_3m = direction * float(trade.get("ch3m_1m", 0.0) or 0.0)
    directional_15m = direction * float(trade.get("ch15m", 0.0) or 0.0)
    vol1 = float(trade.get("vol1", 0.0) or 0.0)
    range1 = float(trade.get("range1", 0.0) or 0.0)
    vol5 = float(trade.get("volume_ratio", trade.get("vol5", 0.0)) or 0.0)
    common = bool(
        PAPER_VALIDATION_ENABLED
        and PAPER_PULLBACK_CHALLENGER_ENABLED
        and side in PAPER_INSTANT_STRATEGIES
        and str(trade.get("strategy", "")).upper() == PAPER_INSTANT_STRATEGIES[side]
        and str(trade.get("grade", "")).upper() == "A+"
        and PAPER_EDGE_MIN <= directional_3m <= PAPER_EDGE_MAX
        and directional_15m <= PAPER_MAX_DIRECTIONAL_15M
        and DATA_MIN_VOL1 <= vol1 <= PAPER_MAX_VOL1
        and range1 >= DATA_MIN_RANGE1
    )
    if not common:
        return False
    if side == "LONG":
        return bool(
            directional_15m >= PAPER_LONG_MIN_DIRECTIONAL_15M
            and PAPER_LONG_MIN_VOL5 <= vol5 <= PAPER_LONG_MAX_VOL5
        )
    return bool(
        directional_3m >= PAPER_SHORT_MIN_DIRECTIONAL_3M
        and directional_15m >= PAPER_SHORT_MIN_DIRECTIONAL_15M
    )


def paper_symbol_independence_gate(trade: Dict[str, Any]) -> Tuple[bool, str]:
    """Prevent one coin/impulse from masquerading as independent evidence."""
    symbol = normalize_symbol(str(trade.get("symbol", "?")))
    for item in STATE.setdefault("pending_signals", []):
        if (
            normalize_symbol(str(item.get("symbol", "?"))) == symbol
            and bool(item.get("paper_validation_only"))
        ):
            return False, "same symbol already awaiting PAPER confirmation"
    for item in STATE.setdefault("shadow_signals", []):
        if (
            normalize_symbol(str(item.get("symbol", "?"))) == symbol
            and str(item.get("shadow_reason", "")) in {
                PAPER_VALIDATION_REASON,
                PAPER_CONTROL_REASON,
            }
        ):
            return False, "same symbol already has an active confirmation/PAPER observation"

    init_adaptive_db()
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT MAX(closed_at) AS last_closed FROM adaptive_trades "
            "WHERE symbol=? AND COALESCE(decision_reason, '') IN (?, ?)",
            (symbol, PAPER_VALIDATION_REASON, PAPER_CONTROL_REASON),
        ).fetchone()
    last_closed = int(row["last_closed"] or 0)
    elapsed = max(0, now_ts() - last_closed) if last_closed else 10**9
    if elapsed < max(0, PAPER_SYMBOL_COOLDOWN_SECONDS):
        hours_left = (PAPER_SYMBOL_COOLDOWN_SECONDS - elapsed) / 3600.0
        return False, f"independence cooldown: same symbol, retry in {hours_left:.1f}h"
    return True, "independent symbol window passed"


def strategy_circuit_breaker(trade: Dict[str, Any]) -> Tuple[bool, str]:
    """Pause a weak strategy in LIVE while continuously rechecking it in SHADOW."""
    strategy = str(trade.get("strategy", "?"))
    if not STRATEGY_CIRCUIT_BREAKER_ENABLED:
        return True, "strategy circuit breaker disabled"
    init_adaptive_db()
    window = max(1, STRATEGY_GUARD_WINDOW)
    with _LOCK, _connect() as conn:
        if strategy.startswith("PRO_SPIKE_REGIME_"):
            rows = conn.execute(
                "SELECT result, pnl_r FROM adaptive_trades "
                "WHERE strategy=? AND COALESCE(decision_reason,'') IN (?,?,?) "
                "ORDER BY id DESC LIMIT ?",
                (
                    strategy,
                    EXHAUSTION_PAPER_REASON,
                    SPIKE_ADAPTIVE_BLOCK_REASON,
                    SPIKE_STRATEGY_GUARD_REASON,
                    window,
                ),
            ).fetchall()
        elif strategy.startswith("PRO_ACTIVE_MOVER_"):
            rows = conn.execute(
                "SELECT result, pnl_r FROM adaptive_trades "
                "WHERE strategy=? AND COALESCE(decision_reason,'') IN (?,?) "
                "ORDER BY id DESC LIMIT ?",
                (
                    strategy,
                    TRADER_STYLE_PAPER_REASON,
                    ACTIVE_MOVER_GUARD_REASON,
                    window,
                ),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT result, pnl_r FROM adaptive_trades WHERE strategy=? "
                "ORDER BY id DESC LIMIT ?",
                (strategy, window),
            ).fetchall()
    ordered = list(reversed(rows))
    metrics = _outcome_metrics(ordered)
    recovery_rows = ordered[-max(1, STRATEGY_RECOVERY_WINDOW):]
    recovery = _outcome_metrics(recovery_rows)
    enough = int(metrics["n"]) >= max(1, STRATEGY_GUARD_MIN_ROWS)
    weak = (
        enough
        and float(metrics["expectancy_r"]) <= STRATEGY_GUARD_MAX_EXPECTANCY_R
        and float(metrics["success_rate"]) <= STRATEGY_GUARD_MAX_SUCCESS_RATE
    )
    recovered = (
        int(recovery["n"]) >= max(1, STRATEGY_RECOVERY_WINDOW)
        and int(recovery["profit"]) >= max(1, STRATEGY_RECOVERY_MIN_PROFITS)
        and float(recovery["expectancy_r"]) >= STRATEGY_RECOVERY_MIN_EXPECTANCY_R
    )
    forced_requalification = bool(
        MARKET_DUMP_SHORT_REQUALIFY
        and strategy == "PRO_MARKET_DUMP_SHORT"
    )
    accepted = bool(recovered if forced_requalification else (not weak or recovered))
    reason = (
        f"strategy guard passed: last {int(metrics['n'])}, {_metrics_line(metrics)}"
        if accepted
        else (
            f"strategy LIVE paused until recovery: last {int(metrics['n'])}, "
            f"{_metrics_line(metrics)}"
            if forced_requalification
            else f"strategy LIVE paused: last {int(metrics['n'])}, {_metrics_line(metrics)}"
        )
    )
    trade["strategy_guard_accepted"] = accepted
    trade["strategy_guard_reason"] = reason

    statuses = STATE.setdefault("strategy_guard_status", {})
    previous = statuses.get(strategy)
    if previous is None or bool(previous.get("accepted", True)) != accepted:
        statuses[strategy] = {
            "accepted": accepted,
            "reason": reason,
            "updated_at": now_ts(),
        }
        save_state()
        if not accepted:
            send_telegram(
                f"⏸ Стратегия временно переведена в SHADOW\n{strategy}\n{reason}\n"
                "Новые исходы продолжают отслеживаться; восстановление автоматическое."
            )
        elif previous is not None:
            send_telegram(
                f"▶️ Стратегия восстановлена в LIVE\n{strategy}\n{reason}"
            )
    return accepted, reason


def rebuild_symbol_outcomes_from_adaptive_db() -> Dict[str, Any]:
    """Rebuild only the lightweight per-symbol streak state from saved rows."""
    if not SYMBOL_QUARANTINE_ENABLED:
        STATE["symbol_outcomes"] = {}
        return {}
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT symbol, result, source, closed_at, decision_reason FROM adaptive_trades "
            "WHERE COALESCE(decision_reason,'') IN (?,?) "
            "ORDER BY closed_at ASC, id ASC",
            (TRADER_STYLE_PAPER_REASON, EXHAUSTION_PAPER_REASON),
        ).fetchall()

    outcomes: Dict[str, Any] = {}
    for row in rows:
        decision_reason = str(row["decision_reason"] or "")
        symbol = normalize_symbol(str(row["symbol"] or "?"))
        item = outcomes.setdefault(
            symbol,
            {
                "fail_streak": 0,
                "quarantine_until": 0,
                "observations": 0,
                "last_result": "",
                "last_source": "",
                "updated_at": 0,
            },
        )
        result = str(row["result"] or "expired")
        closed_at = int(row["closed_at"] or 0)
        item["observations"] = int(item.get("observations", 0) or 0) + 1
        if result == "profit":
            item["fail_streak"] = 0
            item["quarantine_until"] = 0
        else:
            item["fail_streak"] = int(item.get("fail_streak", 0) or 0) + 1
            if item["fail_streak"] >= max(1, SYMBOL_FAIL_LIMIT):
                item["quarantine_until"] = closed_at + max(1, SYMBOL_QUARANTINE_SECONDS)
        item["last_result"] = result
        item["last_source"] = str(row["source"] or "live")
        item["updated_at"] = closed_at
    STATE["symbol_outcomes"] = outcomes
    return outcomes


def symbol_quarantine_gate(trade: Dict[str, Any]) -> Tuple[bool, str]:
    symbol = normalize_symbol(str(trade.get("symbol", "?")))
    item = STATE.setdefault("symbol_outcomes", {}).get(symbol) or {}
    fail_streak = int(item.get("fail_streak", 0) or 0)
    quarantine_until = int(item.get("quarantine_until", 0) or 0)
    trade["symbol_fail_streak"] = fail_streak
    trade["symbol_quarantine_until"] = quarantine_until

    if not SYMBOL_QUARANTINE_ENABLED:
        return True, "symbol quarantine disabled"
    if now_ts() < quarantine_until:
        hours_left = max(0.0, (quarantine_until - now_ts()) / 3600.0)
        return False, (
            f"symbol quarantine: {display_symbol(symbol)} has {fail_streak} consecutive "
            f"non-profit outcomes; retry in {hours_left:.1f}h"
        )
    return True, f"symbol streak {fail_streak}/{max(1, SYMBOL_FAIL_LIMIT)}"


def update_symbol_outcome_guard(
    signal: Dict[str, Any], result: str, source: Optional[str] = None
) -> Dict[str, Any]:
    if not SYMBOL_QUARANTINE_ENABLED:
        return {}
    symbol = normalize_symbol(str(signal.get("symbol", "?")))
    outcomes = STATE.setdefault("symbol_outcomes", {})
    item = outcomes.setdefault(
        symbol,
        {
            "fail_streak": 0,
            "quarantine_until": 0,
            "observations": 0,
            "last_result": "",
            "last_source": "",
            "updated_at": 0,
        },
    )
    item["observations"] = int(item.get("observations", 0) or 0) + 1
    if result == "profit":
        item["fail_streak"] = 0
        item["quarantine_until"] = 0
    else:
        item["fail_streak"] = int(item.get("fail_streak", 0) or 0) + 1
        if item["fail_streak"] >= max(1, SYMBOL_FAIL_LIMIT):
            item["quarantine_until"] = now_ts() + max(1, SYMBOL_QUARANTINE_SECONDS)
    item["last_result"] = result
    item["last_source"] = str(source or signal.get("signal_source", "live"))
    item["updated_at"] = now_ts()
    save_state()
    return dict(item)


def analyze_symbol(
    symbol: str,
    btc: Dict[str, Any],
    blocks: Dict[str, int],
    near_miss: List[str],
    allow_shadow_probe: bool = False,
    shadow_probe_sink: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    symbol = normalize_symbol(symbol)
    # Refresh the execution tape; reuse the slower 5m context collected by the
    # hot scan. This removes one duplicate request per symbol without entering
    # on stale 1m pressure.
    c1 = get_klines(symbol, "1m", 120, cache_seconds=4)
    c5 = get_klines(symbol, "5m", 120, cache_seconds=60)
    c15 = get_klines(symbol, "15m", 120, cache_seconds=30)
    c1h = get_klines(symbol, "1h", 120, cache_seconds=90)
    c4h = get_klines(symbol, "4h", 80, cache_seconds=180)

    if not c1 or not c5 or not c15 or not c1h:
        blocks["no_candles"] = blocks.get("no_candles", 0) + 1
        return None

    # V19.0 lane: multi-timeframe vertical-spike radar.
    # 5m / 15m / 1h / 4h can independently create a quiet watch.
    # Only confirmed spike failure becomes a PAPER trade.
    if SQUEEZE_4H_ENABLED and c4h:
        squeeze_setup, squeeze_reason = detect_mtf_spike_fade(
            symbol, c5, c15, c1h, c4h
        )
        if squeeze_setup:
            if add_squeeze_watch(squeeze_setup):
                blocks["v19_mtf_spike_watch_added"] = blocks.get(
                    "v19_mtf_spike_watch_added", 0
                ) + 1
        else:
            key = f"v19_spike_{squeeze_reason}"
            blocks[key] = blocks.get(key, 0) + 1

    # V18.1 second lane: HOT detection only. Actual PAPER entry is made by
    # process_active_mover_watches() after pullback/reclaim/re-acceleration.
    if ACTIVE_MOVER_ENABLED:
        for active_side in ("LONG", "SHORT"):
            am_setup, am_reason = active_mover_setup(
                symbol, c1, c5, c15, c1h, btc, active_side
            )
            if not am_setup:
                blocks[f"v18_1_watch_{am_reason}_{active_side.lower()}"] = (
                    blocks.get(f"v18_1_watch_{am_reason}_{active_side.lower()}", 0) + 1
                )
                continue
            if add_active_mover_watch(am_setup):
                blocks["v18_1_active_watch_added"] = blocks.get(
                    "v18_1_active_watch_added", 0
                ) + 1

    # Fast ⚡ lane keeps the existing ultra-risk protection.
    if ultra_risk_symbol(symbol, c5, c15):
        blocks["ultra_risk_block"] = blocks.get("ultra_risk_block", 0) + 1
        return None

    # V17.3 evaluates the frozen measured hypothesis directly.  It does not
    # depend on instant_edge_setup or on the older trader-pattern templates,
    # which were the reason V17.2.1 reported tens of thousands of checks but no
    # started PAPER candidates.  New matches remain visible PAPER until the
    # independent readiness gate is satisfied.
    if PRO_QUALITY_FORWARD_ENABLED:
        forward_candidates: List[Dict[str, Any]] = []
        for side in ("LONG", "SHORT"):
            if side == "LONG" and not ALLOW_LONG:
                blocks["long_disabled"] = blocks.get("long_disabled", 0) + 1
                continue
            if side == "SHORT" and not ALLOW_SHORT:
                blocks["short_disabled"] = blocks.get("short_disabled", 0) + 1
                continue
            setup, detector_reason = direct_measured_setup(
                symbol, c1, c5, c15, c1h, btc, side
            )
            if not setup:
                key = f"v17_2_2_{detector_reason}_{side.lower()}"
                blocks[key] = blocks.get(key, 0) + 1
                continue

            audit_item = dict(setup)
            audit_item["pending_started_at"] = now_ts()
            record_watch_audit(audit_item, "started", detector_reason)

            entry_ok, entry_reason = measured_edge_entry_gate(
                setup, symbol, c1, c5
            )
            if not entry_ok:
                blocks["v17_2_2_execution_block"] = blocks.get(
                    "v17_2_2_execution_block", 0
                ) + 1
                record_watch_audit(
                    audit_item, "rejected:execution_book", entry_reason
                )
                if len(near_miss) < 8:
                    near_miss.append(
                        f"{display_symbol(symbol)} {side}: {entry_reason}"
                    )
                continue
            record_watch_audit(setup, "execution_passed", entry_reason)

            trade = calculate_fast_trade(setup, c1, c5)
            if not trade:
                blocks["v17_2_2_sl_structure_block"] = blocks.get(
                    "v17_2_2_sl_structure_block", 0
                ) + 1
                record_watch_audit(
                    setup, "rejected:risk_rr", "trade construction failed"
                )
                continue

            risk_ok, risk_reason = direct_measured_risk_gate(
                trade, symbol, c1, c5
            )
            if not risk_ok:
                blocks["v17_2_2_tp3_risk_block"] = blocks.get(
                    "v17_2_2_tp3_risk_block", 0
                ) + 1
                record_watch_audit(trade, "rejected:risk_rr", risk_reason)
                if len(near_miss) < 8:
                    near_miss.append(
                        f"{display_symbol(symbol)} {side}: {risk_reason}"
                    )
                continue

            independent, independence_reason = paper_symbol_independence_gate(trade)
            if not independent:
                blocks["v17_3_2_correlated_repeat"] = blocks.get(
                    "v17_3_2_correlated_repeat", 0
                ) + 1
                record_watch_audit(
                    trade, "rejected:symbol_repeat", independence_reason
                )
                continue

            trade["strategy"] = DIRECT_MEASURED_STRATEGIES[side]
            trade["trade_type"] = f"FOLLOW-THROUGH A+ {side}"
            trade["setup_mode"] = f"V17_3_2_FOLLOWTHROUGH_{side}"
            trade["paper_setup_lane"] = "FOLLOW_THROUGH"
            trade["paper_validation_lane"] = PAPER_VALIDATION_REASON
            trade["paper_validation_immediate"] = True
            trade["v17_2_risk_reason"] = risk_reason

            # Pair the exact broad V17.3.1 candidate as CONTROL before applying
            # the new selector. This gives a same-market benchmark instead of
            # comparing different days or market regimes.
            control = dict(trade)
            control["trade_type"] = f"V17.3.1 BASELINE CONTROL {side}"
            control["paper_setup_lane"] = "V17_3_1_BASELINE"
            control["paper_validation_only"] = True
            control["paper_validation_immediate"] = False
            control["paper_validation_origin"] = (
                f"V17.3.1 paired baseline · {detector_reason} · "
                f"{entry_reason} · {risk_reason} · {independence_reason}"
            )
            add_shadow_signal(control, PAPER_CONTROL_REASON)

            follow_ok, follow_reason, follow_score = v17_3_2_followthrough_gate(
                trade, btc
            )
            trade["followthrough_score"] = follow_score
            trade["followthrough_reason"] = follow_reason
            if not follow_ok:
                blocks["v17_3_2_followthrough_block"] = blocks.get(
                    "v17_3_2_followthrough_block", 0
                ) + 1
                record_watch_audit(
                    trade, "rejected:followthrough", follow_reason
                )
                if len(near_miss) < 8:
                    near_miss.append(
                        f"{display_symbol(symbol)} {side}: CONTROL only · {follow_reason}"
                    )
                continue

            trade["paper_validation_origin"] = (
                f"V17.3.2 selected from paired V17.3.1 control · "
                f"{follow_reason} · {detector_reason} · {entry_reason} · "
                f"{risk_reason} · {independence_reason}"
            )
            # A promoted model may only influence a stable canary fraction of
            # LIVE alerts.  It never removes a candidate from the visible PAPER
            # baseline, which avoids selection bias in the next audit.
            try:
                adaptive_ok, adaptive_reason, adaptive_probability = adaptive_gate(
                    trade
                )
                trade["adaptive_selection_reason"] = adaptive_reason
                if adaptive_probability is not None:
                    trade["adaptive_probability"] = adaptive_probability
            except Exception as exc:
                adaptive_ok = False
                adaptive_reason = f"adaptive safety fallback to PAPER: {repr(exc)}"
                trade["adaptive_selection_reason"] = adaptive_reason
                STATE["last_error"] = adaptive_reason

            readiness = real_money_readiness()
            side_readiness = (
                readiness.get("by_side", {}).get(side, {})
                if isinstance(readiness.get("by_side", {}), dict)
                else {}
            )
            side_ready = bool(side_readiness.get("ready"))
            live_enabled = bool(
                readiness.get("live_enabled") and side_ready and adaptive_ok
            )
            trade["paper_validation_only"] = not live_enabled
            if live_enabled:
                symbol_ok, symbol_reason = symbol_quarantine_gate(trade)
                if not symbol_ok:
                    blocks["micro_live_symbol_quarantine"] = blocks.get(
                        "micro_live_symbol_quarantine", 0
                    ) + 1
                    live_enabled = False
                    trade["paper_validation_only"] = True
                    trade["adaptive_reason"] = (
                        f"PAPER downgrade: {symbol_reason}"
                    )
                else:
                    cooldown_allowed, cooldown_reason = cooldown_ok(
                        symbol, trade["strategy"]
                    )
                    if not cooldown_allowed:
                        blocks["micro_live_cooldown"] = blocks.get(
                            "micro_live_cooldown", 0
                        ) + 1
                        live_enabled = False
                        trade["paper_validation_only"] = True
                        trade["adaptive_reason"] = (
                            f"PAPER downgrade: {cooldown_reason}"
                        )
                    else:
                        trade["symbol_quarantine_reason"] = symbol_reason
                        trade["adaptive_reason"] = REAL_MONEY_LIVE_REASON
                        trade["risk_mult"] = min(
                            float(
                                trade.get("risk_mult", MICRO_LIVE_RISK_MULT)
                                or MICRO_LIVE_RISK_MULT
                            ),
                            MICRO_LIVE_RISK_MULT,
                        )
            else:
                trade["adaptive_reason"] = (
                    f"PAPER lock: {int(readiness.get('cumulative', {}).get('n', 0) or 0)}/"
                    f"{REAL_MONEY_MIN_FORWARD} new outcomes; live flag={REAL_MONEY_SIGNALS}; "
                    f"side {side} ready={side_ready}; model={adaptive_reason}"
                )
            forward_candidates.append(trade)
            record_watch_audit(trade, "confirmed", risk_reason)
            blocks["v17_2_2_direct_candidate"] = blocks.get(
                "v17_2_2_direct_candidate", 0
            ) + 1
            if len(near_miss) < 8:
                near_miss.append(
                    f"{display_symbol(symbol)} {side}: DIRECT MEASURED A+ "
                    f"{'micro-LIVE' if live_enabled else 'PAPER'} · "
                    f"3m {float(trade.get('ch3m_1m', 0.0) or 0.0)*100:+.2f}% · "
                    f"spread {float(trade.get('book_spread_bps', 0.0) or 0.0):.1f} bps"
                )
        if not forward_candidates:
            return None
        forward_candidates.sort(
            key=lambda row: (
                str(row.get("grade", "A")) == "A+",
                int(row.get("score", 0) or 0),
                float(row.get("ladder_rr", 0.0) or 0.0),
            ),
            reverse=True,
        )
        return forward_candidates[0]

    candidates: List[Dict[str, Any]] = []
    shadow_observed = False
    for side in ("LONG", "SHORT"):
        if side == "LONG" and not ALLOW_LONG:
            blocks["long_disabled"] = blocks.get("long_disabled", 0) + 1
            continue
        if side == "SHORT" and not ALLOW_SHORT:
            blocks["short_disabled"] = blocks.get("short_disabled", 0) + 1
            continue
        setup = fast_burst_setup(symbol, c1, c5, c15, c1h, btc, side)
        if not setup:
            setup = instant_edge_setup(symbol, c1, c5, c15, c1h, btc, side)
            if setup:
                blocks[f"instant_edge_{side.lower()}"] = blocks.get(f"instant_edge_{side.lower()}", 0) + 1
        if not setup and side == "SHORT":
            setup = market_dump_short_setup(symbol, c1, c5, c15, c1h, btc)
            if setup:
                blocks["market_dump_short"] = blocks.get("market_dump_short", 0) + 1
        if not setup:
            setup = evidence_imbalance_setup(symbol, c1, c5, c15, c1h, btc, side)
            if setup:
                key = f"evidence_imbalance_{side.lower()}"
                blocks[key] = blocks.get(key, 0) + 1
        if not setup:
            blocks[f"no_fast_{side.lower()}"] = blocks.get(f"no_fast_{side.lower()}", 0) + 1
            continue

        if side == "LONG":
            long_stats_ok, long_stats_reason = long_live_stats_ok()
            if not long_stats_ok and setup.get("grade") != "A+":
                blocks["long_stats_protection_block"] = blocks.get("long_stats_protection_block", 0) + 1
                if len(near_miss) < 8:
                    near_miss.append(f"{display_symbol(symbol)} LONG: {long_stats_reason}; B-class long skipped")
                continue

        co, reason = cooldown_ok(symbol, setup["strategy"])
        if not co:
            blocks["cooldown_block"] = blocks.get("cooldown_block", 0) + 1
            continue

        trade = calculate_fast_trade(setup, c1, c5)
        if not trade:
            blocks["sl_too_far_block"] = blocks.get("sl_too_far_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: SL too far")
            continue

        if trade["score"] < B_MIN_SCORE:
            blocks["score_block"] = blocks.get("score_block", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: score {trade['score']}, vol x{trade['volume_ratio']:.2f}, range x{trade['range_ratio']:.2f}")
            continue

        # V13.23 balanced professional quality gate.
        # Still blocks XMR-style bad scalps: huge SL, weak RR, weak live volume.
        # But thresholds are not over-tight, so the bot can remain alive during the day.
        q_ok, q_block, q_reason = professional_quality_gate(trade, symbol)
        if not q_ok:
            blocks[q_block] = blocks.get(q_block, 0) + 1
            if len(near_miss) < 8:
                near_miss.append(q_reason)
            continue

        t_ok, t_block, t_reason = trader_pattern_gate(trade, symbol, c1, c5, c15, btc)
        if not t_ok:
            blocks[t_block] = blocks.get(t_block, 0) + 1
            if len(near_miss) < 8:
                near_miss.append(t_reason)
            continue
        trade["trader_pattern_reason"] = t_reason

        # V17.1 fixed forward experiment. B and MARKET_DUMP are diagnostic rejects,
        # not pseudo-trades. An eligible A+ INSTANT setup is first registered
        # as a CONTROL observation, then must form a bounded pullback/reclaim
        # before a separate PAPER entry exists.  Nothing in this block is LIVE.
        if PRO_QUALITY_FORWARD_ENABLED:
            grade = str(trade.get("grade", "B")).upper()
            if grade != "A+":
                blocks["v17_b_excluded_by_400_audit"] = blocks.get(
                    "v17_b_excluded_by_400_audit", 0
                ) + 1
                continue

            if str(trade.get("strategy", "")).upper() == "PRO_MARKET_DUMP_SHORT":
                blocks["v17_market_dump_excluded_by_400_audit"] = blocks.get(
                    "v17_market_dump_excluded_by_400_audit", 0
                ) + 1
                continue

            direction = 1.0 if side == "LONG" else -1.0
            directional_3m = direction * float(trade.get("ch3m_1m", 0.0) or 0.0)
            if paper_pullback_challenger_eligible(trade):
                independent, independence_reason = paper_symbol_independence_gate(trade)
                if independent:
                    trade["paper_validation_only"] = True
                    trade["paper_validation_immediate"] = False
                    trade["paper_validation_lane"] = PAPER_VALIDATION_REASON
                    trade["watch_impulse"] = directional_3m
                    trade["paper_validation_origin"] = (
                        f"registered V17.1 moderate A+ INSTANT {side}: directional 3m "
                        f"{directional_3m*100:+.2f}% · 15m/Vol1 ceilings passed · "
                        "WATCH for dynamic pullback/reclaim · "
                        f"{independence_reason}"
                    )
                    candidates.append(trade)
                    blocks["v17_1_moderate_watch_candidate"] = blocks.get(
                        "v17_1_moderate_watch_candidate", 0
                    ) + 1
                    if len(near_miss) < 8:
                        near_miss.append(
                            f"{display_symbol(symbol)} {side}: A+ WATCH awaiting pullback/reclaim · "
                            f"3m {directional_3m*100:+.2f}%"
                        )
                    continue
                blocks["v17_1_correlated_repeat"] = blocks.get(
                    "v17_1_correlated_repeat", 0
                ) + 1
                trade["paper_validation_origin"] = independence_reason
                continue

            blocks["v17_1_moderate_gate_reject"] = blocks.get(
                "v17_1_moderate_gate_reject", 0
            ) + 1
            trade["paper_validation_origin"] = (
                "A+ did not pass the fixed V17.1 moderate impulse/volume gate"
            )
            continue

        # V16.6: the joint Vol1/Range1/directional-move condition was the only
        # compact entry filter with positive expectancy in both chronological
        # development and holdout slices of the saved 100 outcomes. Rejected
        # setups remain visible in SHADOW, so the rule can be challenged later.
        data_ok, data_reason = data_entry_quality_gate(trade)
        if not data_ok:
            blocks["data_entry_gate_block"] = blocks.get("data_entry_gate_block", 0) + 1
            shadow_observed = add_shadow_signal(trade, "data_entry_gate_block") or shadow_observed
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: {data_reason}")
            continue

        strategy_ok, strategy_reason = strategy_circuit_breaker(trade)
        if not strategy_ok:
            blocks["strategy_circuit_breaker_block"] = blocks.get("strategy_circuit_breaker_block", 0) + 1
            shadow_observed = add_shadow_signal(trade, "strategy_circuit_breaker") or shadow_observed
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: {strategy_reason}")
            continue

        # V16.6: evaluate the transparent evidence rule first. A blocked trade
        # is still followed in shadow, so the bot can measure missed winners and
        # automatically roll the rule back. The symbol gate is evaluated on the
        # same candidate and contributes its failure streak to adaptive features.
        symbol_ok, symbol_reason = symbol_quarantine_gate(trade)
        trade["symbol_quarantine_reason"] = symbol_reason
        try:
            evidence_ok, evidence_reason = evidence_guard(trade)
            if not evidence_ok:
                blocks["evidence_guard_block"] = blocks.get("evidence_guard_block", 0) + 1
                shadow_observed = add_shadow_signal(trade, "evidence_guard_block") or shadow_observed
                if len(near_miss) < 8:
                    near_miss.append(f"{display_symbol(symbol)} {side}: {evidence_reason}")
                continue
        except Exception as e:
            trade["evidence_guard_reason"] = f"evidence guard error bypass: {repr(e)}"
            STATE["last_error"] = trade["evidence_guard_reason"]

        if not symbol_ok:
            blocks["symbol_quarantine_block"] = blocks.get("symbol_quarantine_block", 0) + 1
            shadow_observed = add_shadow_signal(trade, "symbol_quarantine_block") or shadow_observed
            if len(near_miss) < 8:
                near_miss.append(f"{display_symbol(symbol)} {side}: {symbol_reason}")
            continue

        # Adaptive model starts in shadow mode. During warm-up it never blocks signals.
        try:
            adaptive_ok, adaptive_reason, adaptive_probability = adaptive_gate(trade)
            trade["adaptive_reason"] = adaptive_reason
            if adaptive_probability is not None:
                trade["adaptive_probability"] = adaptive_probability
            if not adaptive_ok:
                blocks["adaptive_model_block"] = blocks.get("adaptive_model_block", 0) + 1
                shadow_observed = add_shadow_signal(trade, "adaptive_model_block") or shadow_observed
                if len(near_miss) < 8:
                    near_miss.append(f"{display_symbol(symbol)} {side}: {adaptive_reason}")
                continue
        except Exception as e:
            # Learning failure must never stop the market scanner.
            trade["adaptive_reason"] = f"adaptive error bypass: {repr(e)}"
            STATE["last_error"] = trade["adaptive_reason"]

        candidates.append(trade)

    # Optional measurement path: a strict no-setup scan can produce neither
    # LIVE nor SHADOW rows. Observe only a bounded top-ranked near miss. This
    # branch cannot return a candidate and therefore can never send a signal.
    if not candidates and not shadow_observed and allow_shadow_probe:
        probe_setup = near_miss_shadow_setup(symbol, c1, c5, c15, c1h, btc)
        if probe_setup:
            probe_trade = calculate_fast_trade(probe_setup, c1, c5)
            if probe_trade:
                data_entry_quality_gate(probe_trade)
                if shadow_probe_sink is not None:
                    with STATE_IO_LOCK:
                        shadow_probe_sink.append(probe_trade)
                elif add_shadow_signal(probe_trade, "near_miss_probe"):
                    blocks["near_miss_shadow_added"] = blocks.get("near_miss_shadow_added", 0) + 1
                    if len(near_miss) < 8:
                        near_miss.append(
                            f"{display_symbol(symbol)} {probe_trade['side']}: "
                            f"saved to SHADOW only · {probe_trade['shadow_probe_live_gap']}"
                        )

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x["grade"] == "A+", x["score"], x["ladder_rr"]), reverse=True)
    return candidates[0]

# ============================================================
# Formatting / scanning / tracking
# ============================================================

def format_price(x: Optional[float]) -> str:
    if x is None:
        return "-"
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.5f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def build_signal_message(s: Dict[str, Any]) -> str:
    arrow = "🟢" if s["side"] == "LONG" else "🔴"
    live_header = (
        "🛡 ОГРАНИЧЕННЫЙ MICRO-LIVE СИГНАЛ — forward gate пройден\n"
        if str(s.get("strategy", "")).startswith("PRO_DIRECT_MEASURED_")
        else ""
    )
    return (
        f"{live_header}{arrow} {s['side']} {display_symbol(s['symbol'])}\n"
        f"Класс: {s['grade']} · Score {s['score']} · {s['trade_type']}\n"
        f"Стратегия: {s['strategy']}\n\n"
        f"Вход: {format_price(s['entry'])}\n"
        f"TP1: {format_price(s['tp1'])} · ≈ {s['roi_tp1']:.1f}% ROI x{LEVERAGE}\n"
        f"TP2: {format_price(s['tp2'])}\n"
        f"TP3: {format_price(s['tp3'])}\n"
        f"TP4: {format_price(s['tp4'])}\n"
        f"TP5: {format_price(s['tp5'])}\n"
        f"Учёт результата: TP1/TP2 промежуточные; profit только после TP3.\n"
        f"SL: {format_price(s['sl'])} · риск до SL ≈ {s['roi_sl']:.1f}% ROI x{LEVERAGE}\n"
        f"RR TP1: {s['rr']:.2f} · Ladder RR: {s['ladder_rr']:.2f} · Final RR: {s['final_rr']:.2f}\n"
        f"Риск: multiplier x{s['risk_mult']:.2f}; для первого micro-LIVE рисковать не более 0.10% счёта\n"
        f"Evidence guard: {s.get('evidence_guard_reason', 'not evaluated')}\n"
        f"Symbol guard: {s.get('symbol_quarantine_reason', 'no history')}\n"
        f"Adaptive: {s.get('adaptive_reason', 'warm-up')}\n\n"
        f"📌 Логика:\n{s['reason']}\n"
        f"15m: {s['ch15m']*100:+.2f}% · 30m: {s['ch30m']*100:+.2f}% · 1m3: {s['ch3m_1m']*100:+.2f}%\n"
        f"Volume15 x{s['volume_ratio']:.2f} · Range5 x{s['range_ratio']:.2f} · Vol1 x{s.get('vol1', 1.0):.2f} · Range1 x{s.get('range1', 1.0):.2f}\n"
        f"BTC: {s['btc_text']}\n\n"
        f"⏱ Scalping rule: если за {FAST_MAX_MINUTES_TO_TP1} минут нет движения к TP1 — сигнал expired. Фаза рынка не важна; важна быстрая реализация."
    )


def build_paper_signal_message(s: Dict[str, Any]) -> str:
    is_active = str(s.get("paper_style", "")) == "ACTIVE_MOVER" or str(s.get("paper_validation_lane", "")) == TRADER_STYLE_PAPER_REASON
    is_exhaust = str(s.get("paper_style", "")) == "SQUEEZE_EXHAUSTION" or str(s.get("paper_validation_lane", "")) == EXHAUSTION_PAPER_REASON
    if is_exhaust:
        badge = "🔥🔴 4H SQUEEZE EXHAUSTION"
        horizon = f"до {EXHAUST_HARD_EXPIRE_MINUTES} мин; мягкая проверка после {EXHAUST_SOFT_EXPIRE_MINUTES} мин"
    elif is_active:
        badge = "🧲🟣 ACTIVE MOVER"
        horizon = f"до {ACTIVE_MOVER_HARD_EXPIRE_MINUTES} мин; мягкая проверка после {ACTIVE_MOVER_SOFT_EXPIRE_MINUTES} мин"
    else:
        badge = "⚡🟡 LEGACY FOLLOW-THROUGH"
        horizon = f"быстрый режим: {FAST_MAX_MINUTES_TO_TP1}/{FAST_HARD_EXPIRE_MINUTES} мин"
    return (
        f"📋 PAPER-ВХОД V18 · {badge}\n"
        "🚫 НЕ ВХОДИТЬ РЕАЛЬНЫМИ ДЕНЬГАМИ\n"
        f"{s['side']} {display_symbol(s['symbol'])} · {s['grade']} · Score {s['score']}\n"
        f"Линия: {s.get('paper_setup_lane', '⚡ FOLLOW-THROUGH')}\n"
        f"Стратегия: {s['strategy']}\n"
        f"Горизонт: {horizon}\n"
        "Статус: виртуальная проверка стратегии; только PAPER.\n\n"
        f"Вход наблюдения: {format_price(s['entry'])}\n"
        f"TP1: {format_price(s['tp1'])}\n"
        f"TP2: {format_price(s['tp2'])}\n"
        f"TP3: {format_price(s['tp3'])} · только здесь считается profit\n"
        f"TP4: {format_price(s['tp4'])}\n"
        f"TP5: {format_price(s['tp5'])}\n"
        f"SL: {format_price(s['sl'])}\n\n"
        f"Исполняемый вход: {'ASK' if s.get('side') == 'LONG' else 'BID'} · "
        f"spread {float(s.get('book_spread_bps', 0.0) or 0.0):.1f} bps · "
        f"видимая глубина ≈ {float(s.get('book_depth_usdt', 0.0) or 0.0):.0f} USDT\n"
        f"Vol1 x{float(s.get('vol1', 0.0) or 0.0):.2f} · "
        f"Range1 x{float(s.get('range1', 0.0) or 0.0):.2f} · "
        f"3m {float(s.get('ch3m_1m', 0.0) or 0.0)*100:+.2f}%\n"
        f"Почему PAPER: {s.get('paper_validation_origin', 'forward challenger')}\n"
        f"Следующий отчёт: каждые 25 закрытых исходов сохраняется JSON."
    )


def build_paper_result_message(
    signal: Dict[str, Any], result: str, closing_price: float
) -> str:
    is_active = str(signal.get("paper_style", "")) == "ACTIVE_MOVER" or str(signal.get("shadow_reason", "")) == TRADER_STYLE_PAPER_REASON
    is_exhaust = str(signal.get("paper_style", "")) == "SQUEEZE_EXHAUSTION" or str(signal.get("shadow_reason", "")) == EXHAUSTION_PAPER_REASON
    badge = "🔥🔴 4H SQUEEZE EXHAUSTION" if is_exhaust else ("🧲🟣 ACTIVE MOVER" if is_active else "⚡🟡 LEGACY FOLLOW-THROUGH")
    labels = {
        "profit": "✅ TP3+",
        "sl": "❌ STOP LOSS",
        "expired": "⏱ EXPIRED",
    }
    if bool(signal.get("protected_exit")):
        labels["expired"] = "🛡 ЗАЩИТНЫЙ ВЫХОД ПОСЛЕ TP1"
    pnl_r = _estimate_pnl_r(signal, result)
    age_minutes = max(
        0.0, (now_ts() - int(signal.get("created_at", now_ts()) or now_ts())) / 60.0
    )
    paper_metrics = squeeze_exhaustion_metrics() if is_exhaust else (active_mover_paper_metrics() if is_active else paper_validation_metrics())
    stats_name = "4H SQUEEZE EXHAUSTION V18.2" if is_exhaust else ("ACTIVE MOVER V18.1.1" if is_active else "LEGACY FOLLOW-THROUGH")
    return (
        f"📋 PAPER РЕЗУЛЬТАТ · {badge}: {labels.get(result, result.upper())}\n"
        f"{signal.get('side', '?')} {display_symbol(signal.get('symbol', '?'))}\n"
        f"Стратегия: {signal.get('strategy', '?')}\n"
        f"Вход: {format_price(signal.get('entry'))} · выход: {format_price(closing_price)}\n"
        f"Итог: {pnl_r:+.3f}R · время {age_minutes:.1f} мин.\n"
        "Это результат подтверждённого виртуального входа, не реальная сделка.\n"
        f"{stats_name}: {_metrics_line(paper_metrics)} · "
        f"собрано {int(paper_metrics.get('n', 0))}/"
        f"{paper_progress_target(int(paper_metrics.get('n', 0) or 0))} · "
        f"уникальных монет {int(paper_metrics.get('unique_symbols', 0))}."
    )


def build_control_signal_message(s: Dict[str, Any]) -> str:
    return (
        "🔬 SETUP WATCH V17.2 — ЭТО ЕЩЁ НЕ ВХОД\n"
        f"{s.get('side', '?')} {display_symbol(s.get('symbol', '?'))} · "
        f"{s.get('grade', '?')} · Score {s.get('score', '?')}\n"
        f"Линия: {s.get('paper_setup_lane', 'CONTINUATION')}\n"
        f"Исходный импульс: {s.get('strategy', '?')}\n"
        f"Контрольная цена: {format_price(s.get('entry'))}\n"
        f"3m: {float(s.get('watch_impulse', s.get('ch3m_1m', 0.0)) or 0.0)*100:+.2f}% · "
        f"15m: {float(s.get('ch15m', 0.0) or 0.0)*100:+.2f}% · "
        f"Vol1 x{float(s.get('vol1', 0.0) or 0.0):.2f} · "
        f"Range1 x{float(s.get('range1', 0.0) or 0.0):.2f}\n"
        f"Допустимый откат: "
        f"{float(s.get('pending_min_pullback', 0.0) or 0.0)*100:.2f}%–"
        f"{float(s.get('pending_max_pullback', 0.0) or 0.0)*100:.2f}%\n"
        f"TP3 reference: {format_price(s.get('tp3'))} · SL reference: {format_price(s.get('sl'))}\n"
        "Бот ждёт retest/reclaim. Если подтверждения не будет, PAPER-вход не создаётся. "
        "Контрольная цена видима для аудита, но этот исход не обучает модель."
    )


def build_control_result_message(
    signal: Dict[str, Any], result: str, closing_price: float
) -> str:
    labels = {"profit": "✅ TP3+", "sl": "❌ SL", "expired": "⏱ EXPIRED"}
    return (
        f"🔬 WATCH-КОНТРОЛЬ РЕЗУЛЬТАТ: {labels.get(result, result.upper())}\n"
        f"{signal.get('side', '?')} {display_symbol(signal.get('symbol', '?'))}\n"
        f"Контрольный импульс: {signal.get('strategy', '?')}\n"
        f"Цена наблюдения: {format_price(signal.get('entry'))} · "
        f"выход: {format_price(closing_price)}\n"
        f"Итог: {_estimate_pnl_r(signal, result):+.3f}R.\n"
        "Это парный CONTROL исход V17.3.1 на тех же рыночных условиях; он видим, но не обучает V17.3.2."
    )


def build_shadow_signal_message(s: Dict[str, Any]) -> str:
    if str(s.get("shadow_reason", "")) == PAPER_CONTROL_REASON:
        return build_control_signal_message(s)
    return (
        "👁 SHADOW-ВХОД — ВИДИМОЕ НАБЛЮДЕНИЕ, НЕ LIVE\n"
        f"{s.get('side', '?')} {display_symbol(s.get('symbol', '?'))} · "
        f"{s.get('grade', '?')} · Score {s.get('score', '?')}\n"
        f"Стратегия: {s.get('strategy', '?')}\n"
        f"Причина SHADOW: {s.get('shadow_reason', 'diagnostic observation')}\n\n"
        f"Вход наблюдения: {format_price(s.get('entry'))}\n"
        f"TP1: {format_price(s.get('tp1'))}\n"
        f"TP2: {format_price(s.get('tp2'))}\n"
        f"TP3: {format_price(s.get('tp3'))} · только TP3+ считается profit\n"
        f"TP4: {format_price(s.get('tp4'))}\n"
        f"TP5: {format_price(s.get('tp5'))}\n"
        f"SL: {format_price(s.get('sl'))}\n"
        "Бот сообщит TP3+, SL или expired отдельным уведомлением."
    )


def build_shadow_result_message(
    signal: Dict[str, Any], result: str, closing_price: float
) -> str:
    if str(signal.get("shadow_reason", "")) == PAPER_CONTROL_REASON:
        return build_control_result_message(signal, result, closing_price)
    labels = {
        "profit": "✅ TP3+",
        "sl": "❌ STOP LOSS",
        "expired": "⏱ EXPIRED",
    }
    pnl_r = _estimate_pnl_r(signal, result)
    age_minutes = max(
        0.0, (now_ts() - int(signal.get("created_at", now_ts()) or now_ts())) / 60.0
    )
    return (
        f"👁 SHADOW РЕЗУЛЬТАТ: {labels.get(result, result.upper())}\n"
        f"{signal.get('side', '?')} {display_symbol(signal.get('symbol', '?'))}\n"
        f"Стратегия: {signal.get('strategy', '?')}\n"
        f"Причина SHADOW: {signal.get('shadow_reason', 'diagnostic observation')}\n"
        f"Вход: {format_price(signal.get('entry'))} · "
        f"выход: {format_price(closing_price)}\n"
        f"Итог: {pnl_r:+.3f}R · время {age_minutes:.1f} мин.\n"
        "Это прозрачный виртуальный результат, не реальная сделка."
    )


def _diagnostic_block_label(key: str) -> str:
    """Map legacy internal counter keys to current V20.3 user-facing names."""
    mapping = {
        "v19_spike_no_real_spike": "spike_radar_no_real_spike",
        "v19_spike_single_tf_not_extreme_enough": "spike_radar_single_tf_not_extreme",
        "v18_1_active_watch_added": "active_mover_watch_added",
        "v18_1_watch_recent_range_long": "active_mover_recent_range_long",
        "v18_1_watch_recent_range_short": "active_mover_recent_range_short",
        "v18_1_watch_live_participation_long": "active_mover_participation_long",
        "v18_1_watch_live_participation_short": "active_mover_participation_short",
        "v18_1_watch_direction_bias_long": "active_mover_direction_bias_long",
        "v18_1_watch_direction_bias_short": "active_mover_direction_bias_short",
        "v18_1_watch_5m_participation_long": "active_mover_5m_participation_long",
        "v18_1_watch_5m_participation_short": "active_mover_5m_participation_short",
        "no_fast_long": "legacy_fast_long_disabled/no_setup",
        "no_fast_short": "legacy_fast_short_disabled/no_setup",
    }
    return mapping.get(str(key), str(key))


def build_diagnostic(scan: Dict[str, Any]) -> str:
    blocks = scan.get("blocks", {})
    block_lines = [
        f"{_diagnostic_block_label(k)}: {v}"
        for k, v in sorted(blocks.items(), key=lambda kv: -kv[1])[:12]
    ]
    hot = scan.get("hot_notes", [])[:8]
    near = scan.get("near_miss", [])[:8]

    active_metrics = active_mover_paper_metrics()
    spike_metrics = squeeze_exhaustion_metrics()
    readiness = real_money_readiness()

    spike_watch = (
        STATE.get("last_squeeze_watch", {})
        if isinstance(STATE.get("last_squeeze_watch", {}), dict)
        else {}
    )
    active_watch = (
        STATE.get("last_active_watch", {})
        if isinstance(STATE.get("last_active_watch", {}), dict)
        else {}
    )

    # Current adaptive engine status, not legacy V17 funnel text.
    try:
        model = get_model_state()
        adaptive_rows = adaptive_model_data_count()
        next_train = next_model_analysis_target()
        learning_line = (
            f"🧠 SELF-LEARNING: enabled={MODEL_ENABLED} · active={bool(model.active)} · "
            f"model v{int(model.version)} · rows={adaptive_rows} · "
            f"trained_rows={int(model.trained_rows)} · next checkpoint={next_train} · "
            f"canary={float(model.deployment_fraction)*100:.0f}%"
        )
    except Exception as exc:
        learning_line = f"🧠 SELF-LEARNING: enabled={MODEL_ENABLED} · status error={repr(exc)}"

    return (
        f"🧪 Диагностика V20.3.4 SETUP MASTER · 🧲 ACTIVE MOVER + 🚀 SPIKE REGIME\n"
        f"Проверено: {scan.get('checked', 0)} из universe {scan.get('universe', 0)}\n"
        f"Найдено: {scan.get('candidates', 0)} · pending: {scan.get('pending_active', 0)} · "
        f"подтверждено: {scan.get('confirmed', 0)} · отправлено: {scan.get('sent', 0)} · "
        f"PAPER отправлено: {scan.get('paper_sent', 0)} · "
        f"hidden audit новых: {scan.get('shadow_added', 0)} · "
        f"hidden audit активных: {scan.get('shadow_active', 0)} · "
        f"PAPER активных: {scan.get('paper_active', 0)} · "
        f"время: {scan.get('elapsed', 0):.0f}с\n"
        f"BTC: {scan.get('btc', 'unknown')}\n"
        f"🧲 ACTIVE MOVER V20.3.4: {_metrics_line(active_metrics)} · "
        f"n={int(active_metrics.get('n', 0) or 0)}\n"
        f"🚀 SPIKE REGIME V20.3.4: {_metrics_line(spike_metrics)} · "
        f"n={int(spike_metrics.get('n', 0) or 0)}\n"
        f"WATCH: 🧲 checked={int(active_watch.get('checked',0) or 0)} "
        f"triggered={int(active_watch.get('triggered',0) or 0)} "
        f"setup-blocked={int(active_watch.get('selectivity_blocked',0) or 0)} · "
        f"🚀 checked={int(spike_watch.get('checked',0) or 0)} "
        f"continuation={int(spike_watch.get('continuation',0) or 0)} "
        f"exhaustion={int(spike_watch.get('exhaustion',0) or 0)} "
        f"runaway-watch={int(spike_watch.get('runaway_watch',0) or 0)} "
        f"triggered={int(spike_watch.get('triggered',0) or 0)} "
        f"setup-blocked={int(spike_watch.get('setup_selectivity_blocked',0) or 0)}\n"
        f"{learning_line}\n"
        f"Real-money: ready={readiness.get('ready', False)} · "
        f"env flag={readiness.get('live_flag', False)} · "
        f"enabled={readiness.get('live_enabled', False)}\n\n"
        f"Hot symbols:\n" + ("\n".join(hot) if hot else "нет") +
        f"\n\nГлавные блокировки:\n" + ("\n".join(block_lines) if block_lines else "нет") +
        ("\n\nПочти прошли:\n" + "\n".join(near) if near else "") +
        f"\n\n{telegram_delivery_summary()}" +
        f"\n\nLast error: {STATE.get('last_error', '')}"
    )


def ensure_signal_runtime_fields(signal: Dict[str, Any], source: str) -> None:
    created_at = int(signal.get("created_at", now_ts()) or now_ts())
    signal["created_at"] = created_at
    signal.setdefault(
        "signal_id",
        f"{source}:{signal.get('symbol','?')}:{signal.get('side','?')}:"
        f"{signal.get('strategy','?')}:{created_at}:{time.time_ns() % 1_000_000_000}",
    )
    signal["signal_source"] = source
    signal.setdefault("mfe_r", 0.0)
    signal.setdefault("mae_r", 0.0)
    signal.setdefault("stats_recorded", None)


def update_excursion(signal: Dict[str, Any], price: float) -> None:
    entry = float(signal.get("entry", 0.0) or 0.0)
    sl = float(signal.get("sl", entry) or entry)
    risk = abs(entry - sl)
    if entry <= 0 or risk <= 0:
        return
    direction = 1.0 if signal.get("side") == "LONG" else -1.0
    move_r = direction * (price - entry) / risk
    signal["mfe_r"] = max(float(signal.get("mfe_r", 0.0) or 0.0), move_r)
    signal["mae_r"] = min(float(signal.get("mae_r", 0.0) or 0.0), move_r)
    signal["last_observed_price"] = price
    signal["last_observed_at"] = now_ts()


def shadow_key(signal: Dict[str, Any]) -> str:
    base = (
        f"{signal.get('symbol','?')}:{signal.get('side','?')}:"
        f"{signal.get('strategy','?')}"
    )
    lane = str(
        signal.get("shadow_reason")
        or signal.get("paper_validation_lane")
        or ""
    )
    if lane in {PAPER_CONTROL_REASON, PAPER_VALIDATION_REASON, TRADER_STYLE_PAPER_REASON, EXHAUSTION_PAPER_REASON}:
        return f"{base}:{lane}"
    return base


def remove_matching_shadow(signal: Dict[str, Any]) -> None:
    key = shadow_key(signal)
    STATE["shadow_signals"] = [
        item for item in STATE.setdefault("shadow_signals", [])
        if shadow_key(item) != key
    ]


def add_shadow_signal(signal: Dict[str, Any], reason: str) -> bool:
    # V18.2.3: the internal shadow container is now only a PAPER execution
    # container for the two official strategies. No legacy/control/near-miss
    # candidate can be opened anymore.
    if reason not in TRACKABLE_SHADOW_REASONS:
        return False
    if not SHADOW_TRACKING_ENABLED:
        return False
    with STATE_IO_LOCK:
        shadows = STATE.setdefault("shadow_signals", [])
        protected_lane = reason in TRACKABLE_SHADOW_REASONS
        ordinary_limit = max(0, SHADOW_MAX_ACTIVE - max(0, SHADOW_PAPER_RESERVED_SLOTS))
        if protected_lane and len(shadows) >= SHADOW_MAX_ACTIVE:
            return False
        if not protected_lane and len(shadows) >= ordinary_limit:
            return False
        if reason == "near_miss_probe":
            active_probes = sum(
                1 for item in shadows
                if str(item.get("shadow_reason", "")) == "near_miss_probe"
            )
            if active_probes >= max(0, SHADOW_PROBE_MAX_ACTIVE):
                return False
        shadow = dict(signal)
        shadow["shadow_reason"] = reason
        key = shadow_key(shadow)
        cooldowns = STATE.setdefault("shadow_cooldown", {})
        if now_ts() < int(cooldowns.get(key, 0) or 0):
            return False
        if any(shadow_key(item) == key for item in shadows):
            return False

        ensure_signal_runtime_fields(shadow, "shadow")
        shadow["stats_recorded"] = None
        shadows.append(shadow)
        cooldown_seconds = (
            SHADOW_PROBE_COOLDOWN_SECONDS
            if reason == "near_miss_probe"
            else SHADOW_COOLDOWN_SECONDS
        )
        cooldowns[key] = now_ts() + max(0, cooldown_seconds)
        save_state()
    return True


def set_confirmed_paper_signal(signal: Dict[str, Any]) -> bool:
    """Legacy V17.x route disabled in V18.2.3; only the two official lanes can trade."""
    return False

def pending_key(signal: Dict[str, Any]) -> str:
    return shadow_key(signal)


def _watch_audit_state() -> Dict[str, Any]:
    audit = STATE.setdefault("watch_audit_v17_2", default_watch_audit())
    if not isinstance(audit, dict) or str(audit.get("version", "")) != "V17.3":
        audit = default_watch_audit()
        STATE["watch_audit_v17_2"] = audit
    audit.setdefault("rejected", default_watch_audit()["rejected"].copy())
    audit.setdefault("by_side", default_watch_audit()["by_side"].copy())
    audit.setdefault("by_lane", default_watch_audit()["by_lane"].copy())
    audit.setdefault("recent", [])
    return audit


def record_watch_audit(
    item: Dict[str, Any], event: str, detail: str = ""
) -> None:
    with STATE_IO_LOCK:
        _record_watch_audit_locked(item, event, detail)


def _record_watch_audit_locked(
    item: Dict[str, Any], event: str, detail: str = ""
) -> None:
    """Persist the V17.3 direct-entry funnel for every JSON backup."""
    audit = _watch_audit_state()
    side = str(item.get("side", "UNKNOWN")).upper()
    lane = str(item.get("paper_setup_lane", "DIRECT_MEASURED")).upper()
    side_state = audit.setdefault("by_side", {}).setdefault(
        side, {"started": 0, "confirmed": 0, "rejected": 0}
    )
    lane_state = audit.setdefault("by_lane", {}).setdefault(
        lane, {"started": 0, "confirmed": 0, "rejected": 0}
    )
    rejected = audit.setdefault("rejected", {})
    if event == "started":
        audit["started"] = int(audit.get("started", 0) or 0) + 1
        side_state["started"] = int(side_state.get("started", 0) or 0) + 1
        lane_state["started"] = int(lane_state.get("started", 0) or 0) + 1
    elif event == "execution_passed":
        audit["execution_passed"] = int(audit.get("execution_passed", 0) or 0) + 1
    elif event == "confirmed":
        audit["confirmed"] = int(audit.get("confirmed", 0) or 0) + 1
        side_state["confirmed"] = int(side_state.get("confirmed", 0) or 0) + 1
        lane_state["confirmed"] = int(lane_state.get("confirmed", 0) or 0) + 1
    elif event.startswith("rejected:"):
        reason = event.split(":", 1)[1] or "other"
        rejected[reason] = int(rejected.get(reason, 0) or 0) + 1
        side_state["rejected"] = int(side_state.get("rejected", 0) or 0) + 1
        lane_state["rejected"] = int(lane_state.get("rejected", 0) or 0) + 1

    recent = audit.setdefault("recent", [])
    recent.append(
        {
            "ts": now_ts(),
            "symbol": normalize_symbol(str(item.get("symbol", "?"))),
            "side": side,
            "lane": lane,
            "event": event,
            "detail": str(detail or "")[:280],
            "age_seconds": max(
                0,
                now_ts() - int(item.get("pending_started_at", now_ts()) or now_ts()),
            ),
            "pullback": float(item.get("pending_retest_depth", 0.0) or 0.0),
            "recovery": float(item.get("reclaim_recovery", 0.0) or 0.0),
        }
    )
    if len(recent) > 80:
        del recent[:-80]


def watch_audit_summary() -> str:
    audit = _watch_audit_state()
    rejected = audit.get("rejected", {})
    rejected_total = sum(int(value or 0) for value in rejected.values())
    reasons = ", ".join(
        f"{key}={int(value or 0)}"
        for key, value in sorted(
            rejected.items(), key=lambda item: -int(item[1] or 0)
        )
        if int(value or 0) > 0
    ) or "нет"
    lane_bits = []
    for lane, values in sorted(audit.get("by_lane", {}).items()):
        lane_bits.append(
            f"{lane} {int(values.get('confirmed', 0) or 0)}/"
            f"{int(values.get('started', 0) or 0)}"
        )
    lane_text = ", ".join(lane_bits) or "нет"
    return (
        f"FUNNEL V17.3.2: liquidity={int(audit.get('liquidity_passed', 0) or 0)}/"
        f"{int(audit.get('liquidity_checked', 0) or 0)} · "
        f"detected={int(audit.get('started', 0) or 0)} · "
        f"execution={int(audit.get('execution_passed', 0) or 0)} · "
        f"PAPER={int(audit.get('confirmed', 0) or 0)} · "
        f"rejected={rejected_total} ({reasons}) · lanes: {lane_text}"
    )


def _add_pending_signal_impl(signal: Dict[str, Any]) -> bool:
    if not PRE_LIVE_CONFIRMATION_ENABLED:
        return False
    pending = STATE.setdefault("pending_signals", [])
    if len(pending) >= max(1, min(PRE_LIVE_MAX_ACTIVE, V17_2_MAX_PENDING_WATCH)):
        return False
    key = pending_key(signal)
    if any(pending_key(item) == key for item in pending):
        return False
    item = dict(signal)
    item["pending_started_at"] = now_ts()
    item["pending_reference_entry"] = float(signal.get("entry", 0.0) or 0.0)
    item["pending_status"] = "waiting_confirmation"
    item["pending_stage"] = "seeking_pullback"
    item["pending_retest_seen"] = False
    item["pending_reclaim_seen"] = False
    item["pending_retest_depth"] = 0.0
    reference = float(item.get("pending_reference_entry", 0.0) or 0.0)
    item["pending_extreme_price"] = reference
    item["pending_retest_price"] = reference
    item["pending_last_price"] = reference
    item["pending_last_candle_time"] = 0.0
    impulse = max(0.0025, abs(float(item.get("watch_impulse", 0.0) or 0.0)))
    min_fraction = float(
        item.get("watch_pullback_min_fraction", PAPER_RECLAIM_MIN_IMPULSE_FRACTION)
        or PAPER_RECLAIM_MIN_IMPULSE_FRACTION
    )
    max_fraction = float(
        item.get("watch_pullback_max_fraction", PAPER_RECLAIM_MAX_IMPULSE_FRACTION)
        or PAPER_RECLAIM_MAX_IMPULSE_FRACTION
    )
    abs_min = float(
        item.get("watch_pullback_abs_min", PAPER_RECLAIM_MIN_PULLBACK)
        or PAPER_RECLAIM_MIN_PULLBACK
    )
    abs_max = float(
        item.get("watch_pullback_abs_max", PAPER_RECLAIM_MAX_PULLBACK)
        or PAPER_RECLAIM_MAX_PULLBACK
    )
    item["pending_min_pullback"] = max(
        abs_min,
        min(abs_max, impulse * min_fraction),
    )
    item["pending_max_pullback"] = max(
        float(item["pending_min_pullback"]) + 0.0005,
        min(abs_max, impulse * max_fraction),
    )
    pending.append(item)
    # The immediate impulse is a paired visible CONTROL.  It remains separate
    # from a later reclaim entry and is never eligible for model promotion.
    if not add_shadow_signal(item, PAPER_CONTROL_REASON):
        pending.pop()
        return False
    record_watch_audit(item, "started", str(item.get("paper_validation_origin", "")))
    save_state()
    return True


def add_pending_signal(signal: Dict[str, Any]) -> bool:
    with PENDING_RUN_LOCK:
        return _add_pending_signal_impl(signal)


def send_watch_status(item: Dict[str, Any], status: str, detail: str) -> None:
    if not VISIBLE_SHADOW_NOTIFICATIONS:
        return
    send_telegram(
        "🔬 A/A+ WATCH V17.2 — RETEST/RECLAIM\n"
        f"{item.get('side', '?')} {display_symbol(item.get('symbol', '?'))}\n"
        f"Линия: {item.get('paper_setup_lane', 'CONTINUATION')}\n"
        f"Статус: {status}\n{detail}\n"
        "Это наблюдение перед PAPER-входом; реальной сделки нет."
    )


def _candle_time_seconds(candle: Dict[str, Any]) -> float:
    raw = float(candle.get("time", 0.0) or 0.0)
    return raw / 1000.0 if raw > 10_000_000_000 else raw


def _process_pending_signals_impl(
    blocks: Dict[str, int], near_miss: List[str]
) -> List[Dict[str, Any]]:
    if not PRE_LIVE_CONFIRMATION_ENABLED:
        return []
    pending = STATE.setdefault("pending_signals", [])
    if not pending:
        return []

    confirmed: List[Dict[str, Any]] = []
    remaining: List[Dict[str, Any]] = []
    current_ts = now_ts()
    for item in pending:
        started = int(item.get("pending_started_at", current_ts) or current_ts)
        age = max(0, current_ts - started)
        is_paper = bool(item.get("paper_validation_only"))
        min_seconds = (
            int(item.get("watch_min_seconds", PAPER_RECLAIM_MIN_SECONDS) or PAPER_RECLAIM_MIN_SECONDS)
            if is_paper
            else PRE_LIVE_MIN_SECONDS
        )
        max_seconds = (
            int(item.get("watch_max_seconds", PAPER_RECLAIM_MAX_SECONDS) or PAPER_RECLAIM_MAX_SECONDS)
            if is_paper
            else PRE_LIVE_MAX_SECONDS
        )
        if age > max(min_seconds, max_seconds):
            key = "paper_reclaim_watch_expired" if is_paper else "pre_live_confirmation_expired"
            blocks[key] = blocks.get(key, 0) + 1
            if is_paper:
                if not bool(item.get("pending_retest_seen")):
                    reason = "no_pullback"
                    detail = f"За {max_seconds}с не появился допустимый откат."
                elif not bool(item.get("pending_reclaim_seen")):
                    reason = "no_reclaim"
                    detail = (
                        f"Откат был, но за {max_seconds}с цена не вернула EMA9/"
                        f"{float(item.get('watch_recovery_required', PAPER_RECLAIM_MIN_RECOVERY) or PAPER_RECLAIM_MIN_RECOVERY)*100:.0f}% отката."
                    )
                else:
                    reason = "quality_gate"
                    detail = "Reclaim был, но подтверждение свечи/потока не сложилось в допустимом окне."
                record_watch_audit(item, f"rejected:{reason}", detail)
                send_watch_status(item, "ОТКЛОНЁН", detail)
            continue

        symbol = str(item.get("symbol", ""))
        side = str(item.get("side", "")).upper()
        reference = float(item.get("pending_reference_entry", item.get("entry", 0.0)) or 0.0)
        c1 = get_klines(symbol, "1m", 80, cache_seconds=4)
        c5 = get_klines(symbol, "5m", 80, cache_seconds=8)
        if not c1 or not c5 or reference <= 0:
            remaining.append(item)
            continue

        price = float(c1[-1]["close"])
        direction = 1.0 if side == "LONG" else -1.0
        directional_move = direction * (price - reference) / reference
        last = c1[-1]
        previous = c1[-2]
        location = close_location(last)
        vol1 = volume_ratio(c1, 20)
        range1 = candle_range_ratio(c1, 20)
        vol5_now = volume_ratio(c5, 20)
        range5_now = candle_range_ratio(c5, 20)
        directional_3m = direction * percent_change(c1, 3)
        directional_15m = direction * percent_change(c1, 15)
        ema9 = ema(closes(c1[-30:]), 9)

        if is_paper:
            # V17.1 is a real state machine. Tick samples arrive from the
            # dedicated 10-second monitor. Completed 1m candles that opened
            # after WATCH started provide high/low evidence that a 60-second
            # universe scan used to miss.
            previous_extreme = float(item.get("pending_extreme_price", reference) or reference)
            extreme = previous_extreme
            retest_price = float(item.get("pending_retest_price", extreme) or extreme)
            retest_seen = bool(item.get("pending_retest_seen"))
            last_candle_time = float(item.get("pending_last_candle_time", 0.0) or 0.0)

            # Use only completed candles. If a new extreme formed inside a
            # candle, its close is chronologically after that extreme and is a
            # safe first retest point. This avoids guessing high/low ordering.
            completed_after_watch = []
            for candle in c1[:-1]:
                candle_time = _candle_time_seconds(candle)
                if candle_time < started or candle_time <= last_candle_time:
                    continue
                completed_after_watch.append(candle)
            for candle in completed_after_watch:
                candle_high = float(candle.get("high", candle.get("close", price)) or price)
                candle_low = float(candle.get("low", candle.get("close", price)) or price)
                candle_close = float(candle.get("close", price) or price)
                if side == "LONG":
                    if not retest_seen and candle_high >= extreme:
                        extreme = candle_high
                        retest_price = min(candle_high, candle_close)
                    else:
                        retest_price = min(retest_price, candle_low)
                else:
                    if not retest_seen and candle_low <= extreme:
                        extreme = candle_low
                        retest_price = max(candle_low, candle_close)
                    else:
                        retest_price = max(retest_price, candle_high)
                last_candle_time = max(
                    last_candle_time, _candle_time_seconds(candle)
                )

            # Add the current 10-second price sample. Before the pullback is
            # latched the directional extreme can advance; afterwards the
            # anchor stays fixed and only the deepest retest is updated.
            if side == "LONG":
                if not retest_seen and price >= extreme:
                    extreme = price
                    retest_price = price
                else:
                    retest_price = min(retest_price, price)
                retest_depth = max(
                    0.0, (extreme - retest_price) / max(extreme, 1e-12)
                )
            else:
                if not retest_seen and price <= extreme:
                    extreme = price
                    retest_price = price
                else:
                    retest_price = max(retest_price, price)
                retest_depth = max(
                    0.0, (retest_price - extreme) / max(extreme, 1e-12)
                )

            item["pending_extreme_price"] = extreme
            item["pending_retest_price"] = retest_price
            item["pending_last_price"] = price
            item["pending_last_candle_time"] = last_candle_time
            deepest = max(float(item.get("pending_retest_depth", 0.0) or 0.0), retest_depth)
            item["pending_retest_depth"] = deepest
            min_pullback = float(
                item.get("pending_min_pullback", PAPER_RECLAIM_MIN_PULLBACK)
                or PAPER_RECLAIM_MIN_PULLBACK
            )
            max_pullback = float(
                item.get("pending_max_pullback", PAPER_RECLAIM_MAX_PULLBACK)
                or PAPER_RECLAIM_MAX_PULLBACK
            )
            if min_pullback <= deepest <= max_pullback and not retest_seen:
                item["pending_retest_seen"] = True
                item["pending_retest_seen_at"] = current_ts
                item["pending_stage"] = "seeking_reclaim"
                retest_seen = True
                record_watch_audit(
                    item,
                    "pullback_seen",
                    f"pullback {deepest*100:.2f}% within "
                    f"{min_pullback*100:.2f}%–{max_pullback*100:.2f}%",
                )

            if deepest > max_pullback:
                blocks["paper_reclaim_too_deep"] = blocks.get("paper_reclaim_too_deep", 0) + 1
                detail = (
                    f"Откат {deepest*100:.2f}% глубже разрешённых "
                    f"{max_pullback*100:.2f}% для этого импульса."
                )
                record_watch_audit(item, "rejected:pullback_too_deep", detail)
                send_watch_status(
                    item,
                    "ОТКЛОНЁН",
                    detail,
                )
                continue
            if not bool(item.get("pending_retest_seen")):
                remaining.append(item)
                continue
            if age < max(0, min_seconds):
                remaining.append(item)
                continue

            if side == "LONG":
                candle_ok = (
                    last["close"] > last["open"]
                    and last["close"] > previous["close"]
                    and location >= PRE_LIVE_CLOSE_LONG
                    and price > ema9
                )
            else:
                candle_ok = (
                    last["close"] < last["open"]
                    and last["close"] < previous["close"]
                    and location <= PRE_LIVE_CLOSE_SHORT
                    and price < ema9
                )
            reclaim_span = max(abs(extreme - retest_price), reference * 1e-8)
            recovery = (
                (price - retest_price) / reclaim_span
                if side == "LONG"
                else (retest_price - price) / reclaim_span
            )
            recovery = max(0.0, min(1.0, recovery))
            item["reclaim_recovery"] = recovery
            directional_floor = float(
                item.get("watch_min_directional_15m", -0.0020) or -0.0020
            )
            directional_ceiling = float(
                item.get("watch_max_directional_15m", 0.0500) or 0.0500
            )
            min_directional_3m = float(
                item.get("watch_min_directional_3m", PAPER_RECLAIM_MIN_3M)
                or PAPER_RECLAIM_MIN_3M
            )
            max_chase = float(
                item.get("watch_max_chase", PAPER_RECLAIM_MAX_CHASE)
                or PAPER_RECLAIM_MAX_CHASE
            )
            directional_ok = bool(
                directional_3m >= min_directional_3m
                and directional_15m >= directional_floor
                and directional_15m <= directional_ceiling
                and PAPER_RECLAIM_ENTRY_FLOOR
                <= directional_move
                <= max_chase
            )
            reclaim_core = bool(
                recovery >= float(
                    item.get("watch_recovery_required", PAPER_RECLAIM_MIN_RECOVERY)
                    or PAPER_RECLAIM_MIN_RECOVERY
                )
                and ((side == "LONG" and price > ema9) or (side == "SHORT" and price < ema9))
            )
            flow_ok = bool(
                vol1 >= float(item.get("watch_min_vol1", PAPER_RECLAIM_MIN_VOL1) or PAPER_RECLAIM_MIN_VOL1)
                and vol1 <= float(item.get("watch_max_vol1", PAPER_MAX_VOL1) or PAPER_MAX_VOL1)
                and range1 >= float(item.get("watch_min_range1", PAPER_RECLAIM_MIN_RANGE1) or PAPER_RECLAIM_MIN_RANGE1)
                and range5_now >= float(item.get("watch_min_range5", 0.60) or 0.60)
            )
            if reclaim_core:
                if not bool(item.get("pending_reclaim_seen")):
                    record_watch_audit(
                        item,
                        "reclaim_seen",
                        f"recovery {recovery*100:.0f}% and EMA9 reclaimed",
                    )
                item["pending_reclaim_seen"] = True
                item["pending_reclaim_seen_at"] = current_ts
                item["pending_stage"] = "quality_confirmation"
            if candle_ok:
                item["pending_candle_ok_at"] = current_ts
            if flow_ok:
                item["pending_flow_ok_at"] = current_ts

            def latch_recent(key: str) -> bool:
                timestamp = int(item.get(key, 0) or 0)
                return bool(
                    timestamp > 0
                    and current_ts - timestamp <= max(1, PAPER_CONFIRM_LATCH_SECONDS)
                )

            confirmed_now = bool(
                directional_ok
                and latch_recent("pending_reclaim_seen_at")
                and latch_recent("pending_candle_ok_at")
                and latch_recent("pending_flow_ok_at")
            )
        else:
            if directional_move <= -max(0.0, PRE_LIVE_MAX_ADVERSE_MOVE):
                blocks["pre_live_adverse_reject"] = blocks.get("pre_live_adverse_reject", 0) + 1
                continue
            if directional_move >= max(PRE_LIVE_MIN_DIRECTIONAL_MOVE, PRE_LIVE_MAX_CHASE_MOVE):
                blocks["pre_live_chase_reject"] = blocks.get("pre_live_chase_reject", 0) + 1
                continue
            if age < max(0, min_seconds):
                remaining.append(item)
                continue
            if side == "LONG":
                candle_ok = (
                    last["close"] > last["open"]
                    and last["close"] > previous["close"]
                    and location >= PRE_LIVE_CLOSE_LONG
                    and price > ema9
                )
            else:
                candle_ok = (
                    last["close"] < last["open"]
                    and last["close"] < previous["close"]
                    and location <= PRE_LIVE_CLOSE_SHORT
                    and price < ema9
                )
            confirmed_now = bool(
                directional_move >= PRE_LIVE_MIN_DIRECTIONAL_MOVE
                and directional_3m >= DATA_MIN_DIRECTIONAL_3M * 0.75
                and vol1 >= PRE_LIVE_MIN_VOL1
                and range1 >= PRE_LIVE_MIN_RANGE1
                and candle_ok
            )

        if not confirmed_now:
            remaining.append(item)
            continue

        setup = dict(item)
        setup["entry"] = price
        setup["ch3m_1m"] = direction * directional_3m
        setup["ch2m"] = percent_change(c1, 2)
        setup["vol1"] = vol1
        setup["range1"] = range1
        setup["volume_ratio"] = vol5_now
        setup["range_ratio"] = range5_now
        setup["ch15m"] = direction * directional_15m
        setup["ch30m"] = percent_change(c1, 30)
        setup["pending_confirmation_seconds"] = age
        setup["pending_confirmation_move"] = directional_move
        setup["pre_live_confirmed"] = True
        if is_paper:
            lane = str(item.get("paper_setup_lane", "CONTINUATION")).upper()
            if lane == "SWEEP_REVERSAL":
                setup["strategy"] = PAPER_REVERSAL_STRATEGIES[side]
                setup["trade_type"] = f"LIQUIDITY SWEEP REVERSAL {side}"
                setup["setup_mode"] = f"V17_2_CONFIRMED_REVERSAL_{side}"
            else:
                setup["strategy"] = PAPER_RECLAIM_STRATEGIES[side]
                setup["trade_type"] = f"LIQUIDITY CONTINUATION RECLAIM {side}"
                setup["setup_mode"] = f"V17_2_CONFIRMED_CONTINUATION_{side}"
            setup["paper_validation_lane"] = PAPER_VALIDATION_REASON
            setup["paper_validation_origin"] = (
                f"V17.2 {lane} confirmed {side}: liquidity-first WATCH → pullback "
                f"{float(item.get('pending_retest_depth', 0.0) or 0.0)*100:.2f}% → "
                f"EMA9/price reclaim after {age}s; move vs WATCH {directional_move*100:+.2f}%"
            )
        refreshed = calculate_fast_trade(setup, c1, c5)
        if not refreshed:
            blocks["pre_live_reprice_sl_block"] = blocks.get("pre_live_reprice_sl_block", 0) + 1
            if is_paper:
                detail = "После нового входа защитный SL/RR не прошёл проверку."
                record_watch_audit(item, "rejected:reprice_or_rr", detail)
                send_watch_status(item, "ОТКЛОНЁН", detail)
            continue
        quality_ok, quality_block, quality_reason = professional_quality_gate(refreshed, symbol)
        if not quality_ok:
            blocks[quality_block] = blocks.get(quality_block, 0) + 1
            if len(near_miss) < 8:
                near_miss.append(quality_reason)
            if is_paper:
                record_watch_audit(
                    item, "rejected:quality_gate", quality_reason
                )
                send_watch_status(item, "ОТКЛОНЁН", quality_reason)
            continue
        v17_risk_ok, v17_risk_reason = v17_2_paper_risk_gate(
            refreshed, symbol, c1, c5
        )
        if not v17_risk_ok:
            blocks["v17_2_confirmed_tp3_risk_block"] = blocks.get(
                "v17_2_confirmed_tp3_risk_block", 0
            ) + 1
            if is_paper:
                record_watch_audit(
                    item, "rejected:quality_gate", v17_risk_reason
                )
                send_watch_status(item, "ОТКЛОНЁН", v17_risk_reason)
            continue
        refreshed["v17_2_risk_reason"] = v17_risk_reason
        if refreshed.get("paper_validation_only"):
            refreshed["strategy_guard_reason"] = (
                "PULLBACK PAPER bypasses LIVE strategy gate; never eligible for LIVE"
            )
        else:
            strategy_ok, _ = strategy_circuit_breaker(refreshed)
            if not strategy_ok:
                blocks["strategy_circuit_breaker_block"] = blocks.get("strategy_circuit_breaker_block", 0) + 1
                if is_paper:
                    record_watch_audit(
                        item,
                        "rejected:other",
                        "strategy circuit breaker rejected refreshed setup",
                    )
                continue
        if is_paper:
            record_watch_audit(
                item,
                "confirmed",
                f"entry confirmed after {age}s; pullback "
                f"{float(item.get('pending_retest_depth', 0.0) or 0.0)*100:.2f}%",
            )
        confirmed.append(refreshed)

    STATE["pending_signals"] = remaining
    save_state()
    return confirmed


def process_pending_signals(
    blocks: Dict[str, int], near_miss: List[str]
) -> List[Dict[str, Any]]:
    if not PENDING_RUN_LOCK.acquire(blocking=False):
        return []
    try:
        return _process_pending_signals_impl(blocks, near_miss)
    finally:
        PENDING_RUN_LOCK.release()


def add_active_signal(s: Dict[str, Any]) -> None:
    remove_matching_shadow(s)
    ensure_signal_runtime_fields(s, "live")
    STATE.setdefault("active_signals", []).append(s)
    STATE.setdefault("pair_cooldown", {})[s["symbol"]] = now_ts() + PAIR_COOLDOWN_SECONDS
    STATE.setdefault("strategy_cooldown", {})[s["strategy"]] = now_ts() + STRATEGY_COOLDOWN_SECONDS
    history = _prune_live_send_history()
    history.append(
        {
            "ts": now_ts(),
            "side": str(s.get("side", "UNKNOWN")).upper(),
            "model_version": int(s.get("adaptive_model_version", 0) or 0),
        }
    )
    STATE["live_send_history"] = history
    STATE["live_send_timestamps"] = [int(item["ts"]) for item in history]
    save_state()


def _run_scan_impl(manual: bool = False) -> Dict[str, Any]:
    start = time.time()
    blocks: Dict[str, int] = {}
    near_miss: List[str] = []
    shadow_before_ids = {
        str(item.get("signal_id", ""))
        for item in STATE.setdefault("shadow_signals", [])
        if item.get("signal_id")
    }
    btc = btc_context()
    symbols = get_symbols()
    selected, hot_notes = select_hot_symbols(symbols)

    scan = {
        "checked": 0,
        "universe": len(symbols),
        "candidates": 0,
        "sent": 0,
        "shadow_added": 0,
        "near_miss_added": 0,
        "paper_sent": 0,
        "shadow_active": len(STATE.setdefault("shadow_signals", [])),
        "near_miss_active": sum(
            1 for item in STATE.setdefault("shadow_signals", [])
            if str(item.get("shadow_reason", "")) == "near_miss_probe"
        ),
        "paper_active": sum(
            1 for item in STATE.setdefault("shadow_signals", [])
            if str(item.get("shadow_reason", "")) == PAPER_VALIDATION_REASON
        ),
        "blocks": blocks,
        "near_miss": near_miss,
        "hot_notes": hot_notes,
        "btc": btc.get("text", "BTC unknown"),
        "elapsed": 0,
    }

    if not btc.get("ok"):
        blocks["btc_data_problem"] = 1
        STATE["last_scan"] = scan
        save_state()
        return scan

    # Before scanning, refresh old active signals so expired/TP/SL positions do not block the market scan.
    try:
        track_active_signals()
    except Exception as e:
        STATE["last_error"] = f"pre-scan track_active_signals: {repr(e)}"
        save_state()

    confirmed_candidates = process_pending_signals(blocks, near_miss)

    found: List[Dict[str, Any]] = []
    probe_candidates: List[Dict[str, Any]] = []

    def analyze_one(sym: str) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, int], List[str], Optional[Exception]]:
        local_blocks: Dict[str, int] = {}
        local_near: List[str] = []
        try:
            return (
                sym,
                analyze_symbol(
                    sym,
                    btc,
                    local_blocks,
                    local_near,
                    allow_shadow_probe=SHADOW_PROBE_ENABLED,
                    shadow_probe_sink=probe_candidates,
                ),
                local_blocks,
                local_near,
                None,
            )
        except Exception as exc:
            return sym, None, local_blocks, local_near, exc

    if DEEP_SCAN_WORKERS <= 1 or len(selected) <= 1:
        analysis_results = [analyze_one(sym) for sym in selected]
    else:
        analysis_results = []
        with ThreadPoolExecutor(
            max_workers=min(DEEP_SCAN_WORKERS, len(selected)),
            thread_name_prefix="deep-scan",
        ) as pool:
            future_map = {pool.submit(analyze_one, sym): sym for sym in selected}
            for future in as_completed(future_map):
                analysis_results.append(future.result())

    for sym, candidate, local_blocks, local_near, error in analysis_results:
        if error is not None:
            blocks["analyze_exception"] = blocks.get("analyze_exception", 0) + 1
            STATE["last_error"] = f"analyze {sym}: {repr(error)}"
            continue
        scan["checked"] += 1
        for key, value in local_blocks.items():
            blocks[key] = blocks.get(key, 0) + int(value)
        for item in local_near:
            if len(near_miss) < 8:
                near_miss.append(item)
        if candidate:
            found.append(candidate)

    # Rank all measurement-only candidates after the parallel scan. This keeps
    # the three strongest near misses, rather than whichever worker finishes
    # first or whichever symbol happened to rank first in the hot list.
    probe_candidates.sort(
        key=lambda item: (
            float(item.get("shadow_probe_score", 0.0) or 0.0),
            abs(float(item.get("ch3m_1m", 0.0) or 0.0)),
            float(item.get("vol1", 0.0) or 0.0),
        ),
        reverse=True,
    )
    for probe_trade in probe_candidates[:max(0, SHADOW_PROBE_PER_SCAN)]:
        if add_shadow_signal(probe_trade, "near_miss_probe"):
            blocks["near_miss_shadow_added"] = blocks.get("near_miss_shadow_added", 0) + 1
            if len(near_miss) < 8:
                near_miss.append(
                    f"{display_symbol(probe_trade['symbol'])} {probe_trade['side']}: "
                    f"saved to SHADOW only · {probe_trade['shadow_probe_live_gap']}"
                )

    found.sort(key=lambda x: (x["grade"] == "A+", x["score"], x["ladder_rr"]), reverse=True)
    scan["candidates"] = len(found)
    pending_added = 0
    if PRE_LIVE_CONFIRMATION_ENABLED:
        # V17 has no immediate registered PAPER entry. Every challenger
        # must complete the pullback/reclaim state machine above.
        immediate_paper = [
            candidate for candidate in found
            if bool(candidate.get("paper_validation_immediate"))
        ]
        confirmation_queue = [
            candidate for candidate in found
            if not bool(candidate.get("paper_validation_immediate"))
        ]
        for candidate in confirmation_queue:
            if pending_added >= max(1, V17_2_MAX_NEW_WATCH_PER_SCAN):
                break
            if add_pending_signal(candidate):
                pending_added += 1
        ready_candidates = list(confirmed_candidates) + immediate_paper
    else:
        ready_candidates = found
    ready_candidates.sort(
        key=lambda x: (x["grade"] == "A+", x["score"], x["ladder_rr"]),
        reverse=True,
    )
    paper_ready = [
        item for item in ready_candidates
        if bool(item.get("paper_validation_only"))
    ]
    ready_candidates = [
        item for item in ready_candidates
        if not bool(item.get("paper_validation_only"))
    ]
    paper_sent = 0
    for paper_candidate in paper_ready:
        if set_confirmed_paper_signal(paper_candidate):
            send_telegram(build_paper_signal_message(paper_candidate))
            paper_sent += 1
    scan["paper_sent"] = paper_sent
    scan["pending_added"] = pending_added
    scan["pending_active"] = len(STATE.get("pending_signals", []))
    scan["confirmed"] = len(confirmed_candidates)

    sent = 0
    open_risk_count = sum(
        1 for signal in STATE.get("active_signals", [])
        if not signal.get(PROFIT_TARGET_KEY + "_hit") and not signal.get("stats_recorded")
    )
    free_slots = max(0, MAX_ACTIVE_SIGNALS - open_risk_count)
    budget = live_signal_budget_24h()
    selected_live: List[Dict[str, Any]] = []
    shadow_queue: List[Tuple[Dict[str, Any], str]] = []
    local_total = int(budget["total"])
    local_side = {"LONG": int(budget["long"]), "SHORT": int(budget["short"])}
    local_adaptive = int(budget["adaptive"])
    spacing_open = int(budget["spacing_left"]) <= 0
    scan_limit = min(MAX_SIGNALS_PER_SCAN, free_slots)

    for candidate in ready_candidates:
        side = str(candidate.get("side", "UNKNOWN")).upper()
        model_version = int(candidate.get("adaptive_model_version", 0) or 0)
        reason = ""
        if len(selected_live) >= scan_limit:
            reason = "active_slots_or_scan_limit"
        elif MAX_LIVE_SIGNALS_24H > 0 and local_total >= MAX_LIVE_SIGNALS_24H:
            reason = "daily_live_cap"
        elif side in local_side and MAX_LIVE_SIGNALS_PER_SIDE_24H > 0 and local_side[side] >= MAX_LIVE_SIGNALS_PER_SIDE_24H:
            reason = f"{side.lower()}_daily_reserve"
        elif model_version > 0 and MAX_ADAPTIVE_CANARY_LIVE_24H > 0 and local_adaptive >= MAX_ADAPTIVE_CANARY_LIVE_24H:
            reason = "adaptive_canary_daily_cap"
        elif not spacing_open:
            reason = "live_spacing"

        if reason:
            shadow_queue.append((candidate, reason))
            continue

        selected_live.append(candidate)
        local_total += 1
        if side in local_side:
            local_side[side] += 1
        if model_version > 0:
            local_adaptive += 1
        # With forced spacing disabled, more than one independently confirmed
        # setup may be sent in the same scan (still bounded by active slots).
        if MIN_LIVE_SIGNAL_SPACING_SECONDS > 0:
            spacing_open = False

    if not selected_live and ready_candidates:
        if free_slots <= 0:
            block_key = "active_slots_full_send_block"
            detail = f"found {len(ready_candidates)} confirmed candidate(s), but active slots are full"
        elif int(budget["remaining"]) <= 0:
            block_key = "daily_live_cap_block"
            detail = f"24h live cap reached: {budget['total']}/{MAX_LIVE_SIGNALS_24H}"
        elif int(budget["spacing_left"]) > 0:
            block_key = "live_spacing_block"
            detail = f"next live slot in {math.ceil(int(budget['spacing_left'])/60)} min"
        else:
            block_key = "balanced_side_cap_block"
            detail = (
                f"balanced cap: LONG {budget['long']}/{MAX_LIVE_SIGNALS_PER_SIDE_24H}, "
                f"SHORT {budget['short']}/{MAX_LIVE_SIGNALS_PER_SIDE_24H}"
            )
        blocks[block_key] = blocks.get(block_key, 0) + 1
        if len(near_miss) < 8:
            near_miss.append(detail + "; candidates stay in shadow")

    for s in selected_live:
        add_active_signal(s)
        send_telegram(build_signal_message(s))
        sent += 1

    shadow_added = 0
    near_miss_added = 0
    if SHADOW_TRACKING_ENABLED:
        for candidate, shadow_reason in shadow_queue[:max(0, SHADOW_PER_SCAN)]:
            add_shadow_signal(candidate, shadow_reason)

        # Include shadows created directly by data/model guards and pending
        # confirmation, not only overflow candidates from the send queue.
        new_shadows = [
            item
            for item in STATE.setdefault("shadow_signals", [])
            if item.get("signal_id") and str(item.get("signal_id")) not in shadow_before_ids
        ]
        shadow_added = len(new_shadows)
        near_miss_added = sum(
            1 for item in new_shadows
            if str(item.get("shadow_reason", "")) == "near_miss_probe"
        )
        if VISIBLE_SHADOW_NOTIFICATIONS:
            for item in new_shadows:
                # Registered PAPER entries have their own stronger warning and
                # were already sent above.  Every other tracked shadow becomes
                # visible here instead of existing only inside diagnostics.
                if str(item.get("shadow_reason", "")) == PAPER_VALIDATION_REASON:
                    continue
                send_telegram(build_shadow_signal_message(item))

    scan["sent"] = sent
    scan["shadow_added"] = shadow_added
    scan["near_miss_added"] = near_miss_added
    scan["shadow_active"] = len(STATE.setdefault("shadow_signals", []))
    scan["near_miss_active"] = sum(
        1 for item in STATE.setdefault("shadow_signals", [])
        if str(item.get("shadow_reason", "")) == "near_miss_probe"
    )
    scan["paper_active"] = sum(
        1 for item in STATE.setdefault("shadow_signals", [])
        if str(item.get("shadow_reason", "")) == PAPER_VALIDATION_REASON
    )
    scan["elapsed"] = time.time() - start
    STATE["last_scan"] = scan
    save_state()

    if manual or (sent == 0 and now_ts() - STATE.get("last_diag_ts", 0) >= DIAG_SECONDS):
        send_telegram(build_diagnostic(scan))
        STATE["last_diag_ts"] = now_ts()
        save_state()

    # A temporary Telegram document failure must not postpone the backup until
    # another trade closes.  Every scan retries an overdue milestone until the
    # file is acknowledged and last_backup_closed_count advances.
    maybe_send_auto_backup()

    return scan


def run_scan(manual: bool = False) -> Dict[str, Any]:
    with SCAN_RUN_LOCK:
        return _run_scan_impl(manual=manual)


def current_price(symbol: str) -> Optional[float]:
    c = get_klines(symbol, "1m", 80, cache_seconds=4)
    return c[-1]["close"] if c else None


def target_hit(side: str, price: float, target: float) -> bool:
    return price >= target if side == "LONG" else price <= target


def sl_hit(side: str, price: float, sl: float) -> bool:
    return price <= sl if side == "LONG" else price >= sl


def directional_progress_ratio(s: Dict[str, Any], p: float) -> Tuple[bool, float]:
    entry = s["entry"]
    tp1 = s["tp1"]
    full = abs(tp1 - entry)
    if full <= 0:
        return False, 0.0
    if s["side"] == "LONG":
        directional = p > entry
        progress = max(0.0, p - entry) / full
    else:
        directional = p < entry
        progress = max(0.0, entry - p) / full
    return directional, progress


def safe_record_learning_result(
    signal: Dict[str, Any], result: str, source: Optional[str] = None
) -> None:
    """Persist one outcome and evaluate a challenger without interrupting scans."""
    if signal.get("learning_recorded"):
        return
    try:
        report = record_closed_trade(signal, result, source=source)
        if report.get("inserted"):
            signal["learning_recorded"] = result
            shadow_reason = str(signal.get("shadow_reason", ""))
            if shadow_reason not in {
                "near_miss_probe",
                PAPER_VALIDATION_REASON,
                PAPER_CONTROL_REASON,
                LEGACY_V17_3_1_PAPER_VALIDATION_REASON,
                LEGACY_V17_3_1_CONTROL_REASON,
                LEGACY_V17_2_1_PAPER_VALIDATION_REASON,
                LEGACY_V17_2_1_CONTROL_REASON,
                LEGACY_V17_1_PAPER_VALIDATION_REASON,
                LEGACY_V17_1_CONTROL_REASON,
                LEGACY_V17_PAPER_VALIDATION_REASON,
                LEGACY_V17_CONTROL_REASON,
                LEGACY_PAPER_VALIDATION_REASON,
                SPIKE_ADAPTIVE_BLOCK_REASON,
                SPIKE_STRATEGY_GUARD_REASON,
                ACTIVE_MOVER_GUARD_REASON,
                SPIKE_SETUP_SELECTIVITY_BLOCK_REASON,
                ACTIVE_SETUP_SELECTIVITY_BLOCK_REASON,
            } and not shadow_reason.startswith(("v16_9_", "v16_9_1_")):
                update_symbol_outcome_guard(signal, result, source=source)
            is_official_close = str(signal.get("shadow_reason", "")) in OFFICIAL_USER_PAPER_REASONS
            backup_report = maybe_send_auto_backup() if is_official_close else {"attempted": False, "sent": False}
            if is_official_close:
                send_telegram(closed_outcome_progress_message(signal, result, source=source))
            if is_official_close and backup_report.get("attempted") and not backup_report.get("sent"):
                send_telegram(
                    "⚠️ JSON-backup пока не отправлен; бот повторит попытку на следующем "
                    f"скане. Ошибка: {backup_report.get('error', STATE.get('last_error', 'unknown'))}"
                )
        evidence_audit = report.get("evidence_guard_audit")
        if isinstance(evidence_audit, dict):
            send_telegram(format_evidence_guard_audit_message(evidence_audit))
        live_audit = report.get("live_audit")
        if isinstance(live_audit, dict):
            send_telegram(format_live_audit_message(live_audit))

        training_report = report.get("training_report")
        if not isinstance(training_report, dict):
            training_report = report
        if training_report.get("attempted"):
            send_telegram(format_training_attempt_message(training_report))
    except Exception as e:
        STATE["last_error"] = f"adaptive record error: {repr(e)}"
        save_state()


def track_shadow_signals() -> bool:
    shadows = STATE.setdefault("shadow_signals", [])
    if not shadows:
        return False

    remaining: List[Dict[str, Any]] = []
    changed = False
    for signal in shadows:
        ensure_signal_runtime_fields(signal, "shadow")
        price = current_price(signal["symbol"])
        if price is None:
            remaining.append(signal)
            continue

        update_excursion(signal, price)
        side = signal["side"]
        age_minutes = (now_ts() - int(signal.get("created_at", now_ts()))) / 60.0

        protected_stop = signal.get("protected_sl")
        effective_sl = float(protected_stop if protected_stop is not None else signal["sl"])
        if sl_hit(side, price, effective_sl):
            protected_exit = protected_stop is not None
            close_result = "expired" if protected_exit else "sl"
            signal["protected_exit"] = protected_exit
            signal["_closing_price"] = effective_sl
            safe_record_learning_result(signal, close_result, source="shadow")
            if (
                PAPER_NOTIFY_RESULTS
                and str(signal.get("shadow_reason", "")) in {PAPER_VALIDATION_REASON, TRADER_STYLE_PAPER_REASON, EXHAUSTION_PAPER_REASON}
            ):
                send_telegram(
                    build_paper_result_message(signal, close_result, effective_sl)
                )
            elif VISIBLE_SHADOW_NOTIFICATIONS and is_official_user_paper(signal):
                send_telegram(
                    build_shadow_result_message(signal, close_result, effective_sl)
                )
            changed = True
            continue

        for key in ["tp1", "tp2"]:
            if target_hit(side, price, signal[key]):
                signal[f"{key}_hit"] = True

        if (
            PAPER_BREAKEVEN_AFTER_TP1
            and signal.get("tp1_hit")
            and str(signal.get("shadow_reason", "")) in {PAPER_VALIDATION_REASON, TRADER_STYLE_PAPER_REASON, EXHAUSTION_PAPER_REASON}
            and signal.get("protected_sl") is None
        ):
            entry = float(signal.get("entry", price) or price)
            fee_buffer = max(0.0, ROUND_TRIP_COST_MOVE)
            candidate_stop = (
                entry * (1.0 + fee_buffer)
                if side == "LONG"
                else entry * (1.0 - fee_buffer)
            )
            signal["protected_sl"] = (
                max(float(signal["sl"]), candidate_stop)
                if side == "LONG"
                else min(float(signal["sl"]), candidate_stop)
            )
            signal["tp1_protected"] = True
            changed = True

        # Once TP2 traded, a V17.1 PAPER position may not turn into a full loss.
        # Lock approximately TP1 (after a fee/slippage buffer) while TP3 remains
        # the only positive classification used by the quality target.
        if (
            signal.get("tp2_hit")
            and str(signal.get("shadow_reason", "")) in {PAPER_VALIDATION_REASON, TRADER_STYLE_PAPER_REASON, EXHAUSTION_PAPER_REASON}
            and not signal.get("tp2_protected")
        ):
            fee_buffer = max(0.0, ROUND_TRIP_COST_MOVE)
            tp1 = float(signal.get("tp1", signal.get("entry", price)) or price)
            candidate_stop = (
                tp1 * (1.0 - fee_buffer)
                if side == "LONG"
                else tp1 * (1.0 + fee_buffer)
            )
            current_stop = float(signal.get("protected_sl", signal["sl"]) or signal["sl"])
            signal["protected_sl"] = (
                max(current_stop, candidate_stop)
                if side == "LONG"
                else min(current_stop, candidate_stop)
            )
            signal["tp2_protected"] = True
            changed = True

        if target_hit(side, price, signal[PROFIT_TARGET_KEY]):
            signal[PROFIT_TARGET_KEY + "_hit"] = True
            signal["_closing_price"] = signal[PROFIT_TARGET_KEY]
            safe_record_learning_result(signal, "profit", source="shadow")
            if (
                PAPER_NOTIFY_RESULTS
                and str(signal.get("shadow_reason", "")) in {PAPER_VALIDATION_REASON, TRADER_STYLE_PAPER_REASON, EXHAUSTION_PAPER_REASON}
            ):
                send_telegram(
                    build_paper_result_message(
                        signal, "profit", float(signal[PROFIT_TARGET_KEY])
                    )
                )
            elif VISIBLE_SHADOW_NOTIFICATIONS and is_official_user_paper(signal):
                send_telegram(
                    build_shadow_result_message(
                        signal, "profit", float(signal[PROFIT_TARGET_KEY])
                    )
                )
            changed = True
            continue

        directional, progress = directional_progress_ratio(signal, price)
        is_active_mover = (
            str(signal.get("paper_style", "")) == "ACTIVE_MOVER"
            or str(signal.get("shadow_reason", "")) == TRADER_STYLE_PAPER_REASON
        )
        is_squeeze_exhaustion = (
            str(signal.get("paper_style", "")) == "SQUEEZE_EXHAUSTION"
            or str(signal.get("shadow_reason", "")) == EXHAUSTION_PAPER_REASON
        )
        if is_squeeze_exhaustion:
            fast_stop = (
                age_minutes >= EXHAUST_SOFT_EXPIRE_MINUTES
                and not signal.get("tp1_hit")
                and ((not directional) or progress < EXHAUST_MIN_PROGRESS_AT_SOFT)
            )
            hard_expire_minutes = EXHAUST_HARD_EXPIRE_MINUTES
        elif is_active_mover:
            fast_stop = (
                age_minutes >= ACTIVE_MOVER_SOFT_EXPIRE_MINUTES
                and not signal.get("tp1_hit")
                and ((not directional) or progress < ACTIVE_MOVER_MIN_PROGRESS_AT_SOFT)
            )
            hard_expire_minutes = ACTIVE_MOVER_HARD_EXPIRE_MINUTES
        else:
            fast_stop = (
                FAST_CANCEL_IF_NO_PROGRESS
                and age_minutes >= FAST_MAX_MINUTES_TO_TP1
                and ((not directional) or progress < FAST_MIN_PROGRESS_TO_KEEP)
            )
            hard_expire_minutes = FAST_HARD_EXPIRE_MINUTES
        if signal.get("tp1_hit"):
            fast_stop = False
        if fast_stop or age_minutes >= hard_expire_minutes:
            signal["_closing_price"] = price
            safe_record_learning_result(signal, "expired", source="shadow")
            if (
                PAPER_NOTIFY_RESULTS
                and str(signal.get("shadow_reason", "")) in {PAPER_VALIDATION_REASON, TRADER_STYLE_PAPER_REASON, EXHAUSTION_PAPER_REASON}
            ):
                send_telegram(
                    build_paper_result_message(signal, "expired", float(price))
                )
            elif VISIBLE_SHADOW_NOTIFICATIONS and is_official_user_paper(signal):
                send_telegram(
                    build_shadow_result_message(signal, "expired", float(price))
                )
            changed = True
            continue

        remaining.append(signal)

    if changed:
        STATE["shadow_signals"] = remaining
    return changed


def _track_active_signals_impl() -> None:
    active = STATE.setdefault("active_signals", [])
    remaining: List[Dict[str, Any]] = []
    changed = False

    for signal in active:
        ensure_signal_runtime_fields(signal, "live")
        price = current_price(signal["symbol"])
        if price is None:
            remaining.append(signal)
            continue

        update_excursion(signal, price)
        side = signal["side"]
        age_minutes = (now_ts() - int(signal.get("created_at", now_ts()))) / 60.0

        # Migrate an already-running signal only if TP3 was actually reached.
        if signal.get("tp3_hit") and not signal.get("stats_recorded"):
            signal["_closing_price"] = signal.get("tp3", price)
            if apply_result(signal, "profit"):
                safe_record_learning_result(signal, "profit", source="live")
            changed = True

        # TP1 and TP2 remain intermediate labels: only TP3 is a positive
        # outcome.  Capital protection is separate from labelling, however.
        # After TP1 the stop moves to an execution-cost-adjusted breakeven; after
        # TP2 it locks roughly TP1.  A protected exit is recorded conservatively
        # as ``expired`` with its actual PnL, never as a TP3 profit.
        if not signal.get("tp3_hit"):
            protected_stop = signal.get("protected_sl")
            effective_sl = float(
                protected_stop if protected_stop is not None else signal["sl"]
            )
            if sl_hit(side, price, effective_sl):
                protected_exit = protected_stop is not None
                result = "expired" if protected_exit else "sl"
                signal["protected_exit"] = protected_exit
                signal["_closing_price"] = effective_sl
                result_r = _estimate_pnl_r(signal, result)
                if apply_result(signal, result):
                    safe_record_learning_result(signal, result, source="live")
                if protected_exit:
                    title = "🛡 ЗАЩИТНЫЙ ВЫХОД ПОСЛЕ TP1/TP2"
                    detail = (
                        f"Защитный stop: {format_price(effective_sl)}\n"
                        f"Фактический итог: {result_r:+.2f}R; для TP3-статистики = expired."
                    )
                else:
                    title = "❌ STOP LOSS"
                    detail = (
                        f"SL: {format_price(effective_sl)}\n"
                        f"Фактический итог: {result_r:+.2f}R."
                    )
                send_telegram(
                    f"{title}\n"
                    f"{signal['grade']} · {side} {display_symbol(signal['symbol'])}\n"
                    f"Стратегия: {signal['strategy']}\n"
                    f"Вход: {format_price(signal['entry'])}\n"
                    f"{detail}\n"
                    f"Текущая цена: {format_price(price)}\n\n"
                    f"{build_stats_text()}"
                )
                changed = True
                continue

            for key in ["tp1", "tp2"]:
                if (
                    not signal.get(f"{key}_hit")
                    and target_hit(side, price, signal[key])
                ):
                    signal[f"{key}_hit"] = True
                    changed = True
                    send_telegram(
                        f"🎯 {key.upper()} HIT — промежуточная цель\n"
                        f"{signal['grade']} · {side} {display_symbol(signal['symbol'])}\n"
                        f"Стратегия: {signal['strategy']}\n"
                        f"{key.upper()}: {format_price(signal[key])}\n"
                        f"Текущая цена: {format_price(price)}\n"
                        f"Сделка станет положительной только при TP3."
                    )

            if signal.get("tp1_hit") and signal.get("protected_sl") is None:
                entry = float(signal.get("entry", price) or price)
                fee_buffer = max(0.0, ROUND_TRIP_COST_MOVE)
                candidate_stop = (
                    entry * (1.0 + fee_buffer)
                    if side == "LONG"
                    else entry * (1.0 - fee_buffer)
                )
                signal["protected_sl"] = (
                    max(float(signal["sl"]), candidate_stop)
                    if side == "LONG"
                    else min(float(signal["sl"]), candidate_stop)
                )
                signal["tp1_protected"] = True
                changed = True

            if signal.get("tp2_hit") and not signal.get("tp2_protected"):
                fee_buffer = max(0.0, ROUND_TRIP_COST_MOVE)
                tp1 = float(signal.get("tp1", signal.get("entry", price)) or price)
                candidate_stop = (
                    tp1 * (1.0 - fee_buffer)
                    if side == "LONG"
                    else tp1 * (1.0 + fee_buffer)
                )
                current_stop = float(
                    signal.get("protected_sl", signal["sl"]) or signal["sl"]
                )
                signal["protected_sl"] = (
                    max(current_stop, candidate_stop)
                    if side == "LONG"
                    else min(current_stop, candidate_stop)
                )
                signal["tp2_protected"] = True
                changed = True

            if target_hit(side, price, signal["tp3"]):
                signal["tp3_hit"] = True
                signal["profit_locked_at"] = now_ts()
                signal["_closing_price"] = signal["tp3"]
                if apply_result(signal, "profit"):
                    safe_record_learning_result(signal, "profit", source="live")
                send_telegram(
                    f"✅ TP3 — сделка засчитана в profit\n"
                    f"{signal['grade']} · {side} {display_symbol(signal['symbol'])}\n"
                    f"Стратегия: {signal['strategy']}\n"
                    f"TP3: {format_price(signal['tp3'])}\n"
                    f"Текущая цена: {format_price(price)}\n\n"
                    f"{build_stats_text()}"
                )
                changed = True

            if not signal.get("tp3_hit"):
                directional, progress = directional_progress_ratio(signal, price)
                fast_stop = (
                    FAST_CANCEL_IF_NO_PROGRESS
                    and age_minutes >= FAST_MAX_MINUTES_TO_TP1
                    and not signal.get("tp1_hit")
                    and ((not directional) or progress < FAST_MIN_PROGRESS_TO_KEEP)
                )
                if fast_stop or age_minutes >= FAST_HARD_EXPIRE_MINUTES:
                    signal["_closing_price"] = price
                    result_r = _estimate_pnl_r(signal, "expired")
                    if apply_result(signal, "expired"):
                        safe_record_learning_result(signal, "expired", source="live")
                    label = "FAST TIME-STOP" if fast_stop else "HARD TIME-STOP"
                    stop_reason = (
                        f"TP1 не получил достаточного движения за {age_minutes:.1f} мин.\n"
                        if fast_stop
                        else f"TP3 не достигнут за {age_minutes:.1f} мин.\n"
                    )
                    send_telegram(
                        f"⏱ {label}\n"
                        f"{signal['grade']} · {side} {display_symbol(signal['symbol'])}\n"
                        f"Стратегия: {signal['strategy']}\n"
                        f"{stop_reason}"
                        f"Вход: {format_price(signal['entry'])}\n"
                        f"Цена выхода: {format_price(price)}\n"
                        f"Прогресс к TP1: {progress*100:.1f}% · итог {result_r:+.2f}R\n\n"
                        f"{build_stats_text()}"
                    )
                    changed = True
                    continue

        # After TP3, TP4/TP5 are informational and cannot double-count profit.
        for key in ["tp4"]:
            if (
                signal.get("tp3_hit")
                and signal.get(key)
                and not signal.get(f"{key}_hit")
                and target_hit(side, price, signal[key])
            ):
                signal[f"{key}_hit"] = True
                changed = True
                send_telegram(
                    f"🎯 {key.upper()} HIT\n"
                    f"{signal['grade']} · {side} {display_symbol(signal['symbol'])}\n"
                    f"Стратегия: {signal['strategy']}\n"
                    f"{key.upper()}: {format_price(signal[key])}\n"
                    f"Текущая цена: {format_price(price)}"
                )

        if signal.get("tp3_hit") and target_hit(side, price, signal["tp5"]):
            signal["tp5_hit"] = True
            send_telegram(
                f"✅ FULL LADDER TAKE PROFIT\n"
                f"{signal['grade']} · {side} {display_symbol(signal['symbol'])}\n"
                f"Стратегия: {signal['strategy']}\n"
                f"TP5 достигнут: {format_price(price)}\n"
                f"Время в сделке: {age_minutes:.1f} мин\n\n"
                f"{build_stats_text()}"
            )
            changed = True
            continue

        if signal.get("tp3_hit") and age_minutes >= LADDER_FOLLOWUP_MINUTES:
            changed = True
            continue

        remaining.append(signal)

    if track_shadow_signals():
        changed = True
    if changed:
        STATE["active_signals"] = remaining
        save_state()


def track_active_signals() -> None:
    if not TRACK_RUN_LOCK.acquire(blocking=False):
        return
    try:
        _track_active_signals_impl()
    finally:
        TRACK_RUN_LOCK.release()

# ============================================================
# Background tasks / HTTP endpoints
# ============================================================

async def scan_loop():
    await asyncio.sleep(3)
    guard_state = get_evidence_guard_state()
    model_state = get_model_state()
    source_counts = adaptive_source_counts()
    model_data_count = adaptive_model_data_count()
    paper_metrics = paper_validation_metrics()
    control_metrics = control_validation_metrics()
    readiness = real_money_readiness()
    restored_count = int(SEED_RESTORE_INFO.get("restored", 0) or 0)
    restored_text = (
        f"restored {restored_count} outcomes from {SEED_RESTORE_INFO.get('source', 'JSON')}"
        if restored_count
        else str(SEED_RESTORE_INFO.get("reason", "not needed"))
    )
    state_storage = str(Path(STATE_FILE))
    db_storage = str(Path(DB_PATH))
    storage_warning = (
        "persistent /var/data paths configured"
        if state_storage.startswith("/var/data/") and db_storage.startswith("/var/data/")
        else "WARNING: configure Render Disk /var/data and absolute storage paths"
    )
    send_telegram(
        f"✅ {APP_NAME} активирован.\n"
        f"Deploy marker: {DEPLOY_MARKER}\n\n"
        f"Mode: PAPER RESEARCH · TWO PROFESSIONAL LANES · REAL MONEY LOCKED.\nЛогика V20.3.4: 🧲 ACTIVE MOVER ищет подтверждённый continuation после pullback/reclaim; 🚀 SPIKE REGIME различает CONTINUATION и EXHAUSTION; candle intelligence оценивает squeeze/overheat/climax; при конфликте режима = NO TRADE.\n"
        f"Логика: свежий A+ INSTANT импульс → измеренный диапазон объёма/амплитуды → "
        f"проверка публичного bid/ask и глубины → своевременный PAPER-вход → 5 TP.\n"
        f"Time-stop: если TP1 не двигается за {FAST_MAX_MINUTES_TO_TP1} мин — expired.\n"
        f"Compact targets: {TP1_MOVE*100:.2f}% / {TP2_MOVE*100:.2f}% / {TP3_MOVE*100:.2f}% / {TP4_MOVE*100:.2f}% / {TP5_MOVE*100:.2f}%.\n"
        f"Result rule: TP1/TP2 intermediate; profit starts from TP3.\n"
        f"Risk multiplier: PAPER A+ x{A_RISK_MULT:.2f}; первый micro-LIVE x{MICRO_LIVE_RISK_MULT:.2f} и не более 0.10% счёта на риск.\n"
        f"Opportunity engine: analyze up to {MAX_ANALYZE_SYMBOLS} contracts · "
        f"deep-check up to {HOT_SYMBOLS_TO_ANALYZE} liquidity-qualified names · "
        f"parallel workers {HOT_SCAN_WORKERS}/{DEEP_SCAN_WORKERS}.\n"
        f"Universe rotation: every scan advances through the full contract list; no fixed first-220 blind spot.\n"
        f"Liquidity gate: top {V17_2_LIQUIDITY_KEEP_FRACTION*100:.0f}% by relative turnover/continuity · "
        f"active candles ≥ {V17_2_MIN_ACTIVE_CANDLE_FRACTION*100:.0f}% · "
        f"anomalies Vol1 > x{V17_2_MAX_CURRENT_VOL_RATIO:.0f} or Range1 > x{V17_2_MAX_CURRENT_RANGE_RATIO:.0f} rejected.\n"
        f"Direct measured entry: independent of legacy instant/no_fast templates · Vol1 x{MEASURED_MIN_VOL1:.2f}–x{MEASURED_MAX_VOL1:.2f} · "
        f"Range1 x{MEASURED_MIN_RANGE1:.2f}–x{MEASURED_MAX_RANGE1:.2f} · directional 3m "
        f"{MEASURED_MIN_DIRECTIONAL_3M*100:.2f}%–{MEASURED_MAX_DIRECTIONAL_3M*100:.2f}% · "
        f"15m {MEASURED_MIN_DIRECTIONAL_15M*100:.2f}%–{MEASURED_MAX_DIRECTIONAL_15M*100:.2f}%.\n"
        f"Execution gate: spread ≤ {MEASURED_MAX_BOOK_SPREAD_BPS:.1f} bps · visible depth ≥ "
        f"{MEASURED_MIN_BOOK_DEPTH_USDT:.0f} USDT/side · 60m turnover proxy ≥ {MEASURED_MIN_QUOTE_60M:.0f}.\n"
        f"\n"
        f"Официальные PAPER-линии: 🧲 ACTIVE MOVER + 🚀 SPIKE REGIME V20.3.4. Legacy Follow-Through/SHADOW отключены.\n"
        f"Telegram CLEAN MODE: legacy SHADOW/near-miss/CONTROL уведомления жёстко отключены; hidden adaptive/guard audits не показываются пользователю.\n"
        f"🧠 SELF-LEARNING: HARD ENABLED · fresh V20.3.4 cohort · hard-min warm-up {MIN_TRAIN_TRADES} · retrain каждые {RETRAIN_EVERY} · 2-pass validation · canary {ADAPTIVE_INITIAL_LIVE_FRACTION*100:.0f}%.\n"
        f"🧠 TRADING INTELLIGENCE: candle anatomy + wick rejection + HH/HL or LH/LL structure + pre-move compression + EMA/VWAP/ATR displacement + RSI context + volume/range expansion + squeeze/overheat/climax.\n"
        f"🎯 SETUP MASTER: scores CONTINUATION EDGE vs PULLBACK RISK; weak/ambiguous setups = NO TRADE. Blocked candidates are tracked silently for audit instead of being forgotten.\n"
        f"Diagnostics V20.3.4: current two-lane summary every 12h (2 times/day); legacy V18/V17 labels removed.\n"
        f"🧲 ACTIVE MOVER: HOT → quiet WATCH → pullback/reclaim → re-acceleration → PAPER.\n"        f"🧲 V20.3.4 ACTIVE MOVER: true 1m/3m/15m/30m returns + pullback/reclaim/re-acceleration + professional candle intelligence + liquidity + symbol/strategy guards.\n"
        f"🚀 SPIKE REGIME: сначала ищет только настоящий spike (displacement + expansion + volume + distance from base), затем классифицирует CONTINUATION или EXHAUSTION и выбирает LONG/SHORT.\n"
        f"🚀 Важно: UP spike может дать LONG continuation или SHORT exhaustion; DOWN spike — SHORT continuation или LONG exhaustion. Направление сделки выбирается после классификации режима.\n"        f"V20.3.4 SPIKE REGIME: true 1m/3m/5m/15m returns · RUNAWAY guard · symbol quarantine · 3h re-entry lock · strategy circuit breaker · exhaustion reversal/volume-fade/no-new-extreme · candle/squeeze intelligence · adaptive champion/challenger.\n"
        f"Universe: REAL SPIKE radar работает по ротации полного universe; 5m/15m/1h/4h оцениваются относительно собственной истории каждой монеты.\n"
        f"🧲 horizon: soft {ACTIVE_MOVER_SOFT_EXPIRE_MINUTES}m / hard {ACTIVE_MOVER_HARD_EXPIRE_MINUTES}m · TP {ACTIVE_TP1_MOVE*100:.1f}/{ACTIVE_TP2_MOVE*100:.1f}/{ACTIVE_TP3_MOVE*100:.1f}/{ACTIVE_TP4_MOVE*100:.1f}/{ACTIVE_TP5_MOVE*100:.1f}%.\n"
        f"🔥 horizon: soft {EXHAUST_SOFT_EXPIRE_MINUTES}m / hard {EXHAUST_HARD_EXPIRE_MINUTES}m · TP {EXHAUST_TP1_MOVE*100:.1f}/{EXHAUST_TP2_MOVE*100:.1f}/{EXHAUST_TP3_MOVE*100:.1f}/{EXHAUST_TP4_MOVE*100:.1f}/{EXHAUST_TP5_MOVE*100:.1f}%.\n"
        f"Every official PAPER entry/result is visible in Telegram; legacy SHADOW/near-miss/CONTROL are disabled and excluded from current stats.\n"
        f"Forward PAPER: {PAPER_VALIDATION_ENABLED} · A+ LONG and SHORT · "
        f"one registered outcome per symbol/{PAPER_SYMBOL_COOLDOWN_SECONDS/3600:.0f}h · "
        f"every entry/result visible in Telegram; no forced daily PAPER quota.\n"
        f"Reports: first quality checkpoint at {PAPER_PILOT_REQUIRED_OUTCOMES} · full review at "
        f"{PAPER_REVIEW_REQUIRED_OUTCOMES} · model remains frozen until at least "
        f"{PAPER_LANE_REQUIRED_OUTCOMES} unchanged PAPER outcomes.\n"
        f"Data separation: historical rows remain audit-only; 🧲 and 🔥 PAPER outcomes are tracked separately.\n"
        f"Real-money readiness: ready={readiness.get('ready', False)} · env flag REAL_MONEY_SIGNALS="
        f"{readiness.get('live_flag', False)} · enabled={readiness.get('live_enabled', False)}. "
        f"Both evidence and manual flag are mandatory. This code never places exchange orders.\n"
        f"Side guard: LONG and SHORT are admitted separately after ≥ "
        f"{REAL_MONEY_MIN_SIDE_FORWARD} fresh outcomes/side, TP3+ majority, positive expectancy "
        f"and a positive newest {REAL_MONEY_SIDE_RECENT_WINDOW}-outcome side block.\n"
        f"Micro-LIVE safety when enabled: ≤ {MAX_LIVE_SIGNALS_24H}/24h · ≤ "
        f"{MAX_LIVE_SIGNALS_PER_SIDE_24H}/side · spacing ≥ {MIN_LIVE_SIGNAL_SPACING_SECONDS/60:.0f} min · "
        f"simultaneously active ≤ {MAX_ACTIVE_SIGNALS}.\n"
        f"Micro-LIVE kill switch: after {MICRO_LIVE_MAX_NONPROFIT_STREAK} consecutive "
        f"non-profit outcomes, drawdown > {MICRO_LIVE_MAX_DRAWDOWN_R:.1f}R, or a weak "
        f"rolling {MICRO_LIVE_GUARD_WINDOW}-outcome block, new LIVE alerts stop and return to PAPER.\n"
        f"Reliable Telegram V17.1.1: immediate retries={TELEGRAM_SEND_ATTEMPTS} · "
        f"ordered outbox={TELEGRAM_OUTBOX_ENABLED} · flush every "
        f"{TELEGRAM_OUTBOX_FLUSH_SECONDS}s · diagnostics are replaceable and never block trades.\n"
        f"Learning target: TP3+ > {ADAPTIVE_TARGET_SUCCESS_RATE*100:.0f}% of all closed selected outcomes; "
        f"coverage ≥ {MIN_VALIDATION_COVERAGE*100:.0f}%.\n"
        f"Adaptive model: active={model_state.active} · version={model_state.version} · "
        f"LIVE routing remains locked until readiness passes.\n"
        f"Internal PAPER tracker: two official lanes only · full JSON every "
        f"{AUTO_BACKUP_EVERY_CLOSED} official outcomes · PAPER checkpoint every "
        f"{PAPER_CHECKPOINT_EVERY} confirmed entries.\n"
        f"Seed JSON: {restored_text}.\n"
        f"Restored sources: LIVE={source_counts['live']} · SHADOW={source_counts['shadow']} · "
        f"ALL={source_counts['all']}.\n"
        f"Eligible V17.3 model data={model_data_count} · minimum before analysis "
        f"{PAPER_LANE_REQUIRED_OUTCOMES}.\n"
        f"CONTROL collected: {int(control_metrics.get('n', 0))} · "
        f"{_metrics_line(control_metrics)}.\n"
        f"Direct Measured PAPER V17.3 collected: {int(paper_metrics.get('n', 0))}/"
        f"{paper_progress_target(int(paper_metrics.get('n', 0) or 0))} · "
        f"{_metrics_line(paper_metrics)} · unique symbols="
        f"{int(paper_metrics.get('unique_symbols', 0))}.\n"
        f"{watch_audit_summary()}.\n"
        f"Next JSON backup at {((source_counts['all'] // max(1, AUTO_BACKUP_EVERY_CLOSED)) + 1) * max(1, AUTO_BACKUP_EVERY_CLOSED)} total outcomes.\n"
        f"Storage: STATE_FILE={STATE_FILE} · ADAPTIVE_DB_PATH={DB_PATH} · {storage_warning}."
    )
    try:
        scan = await asyncio.to_thread(run_scan, True)
        # V18.2.2: run_scan(manual=True) already sends the startup diagnostic; do not duplicate it.
    except Exception as e:
        STATE["last_error"] = f"first scan exception: {repr(e)}"
        save_state()
        send_telegram(f"⚠️ Ошибка первого скана: {repr(e)}")

    while True:
        try:
            if AUTO_SCAN_ENABLED:
                await asyncio.to_thread(run_scan, False)
        except Exception as e:
            STATE["last_error"] = f"scan_loop: {repr(e)}"
            save_state()
            send_telegram(f"⚠️ Ошибка auto-scan: {repr(e)}\n{traceback.format_exc()[-2500:]}")
        await asyncio.sleep(AUTO_SCAN_SECONDS)


async def track_loop():
    await asyncio.sleep(8)
    while True:
        try:
            if AUTO_TRACK_ENABLED:
                await asyncio.to_thread(track_active_signals)
        except Exception as e:
            STATE["last_error"] = f"track_loop: {repr(e)}"
            save_state()
        await asyncio.sleep(AUTO_TRACK_SECONDS)


def monitor_pending_and_dispatch() -> Dict[str, Any]:
    """Check WATCH candidates without waiting for the full 80-symbol scan."""
    blocks: Dict[str, int] = {}
    near_miss: List[str] = []
    confirmed = process_pending_signals(blocks, near_miss)
    active_watch_stats = process_active_mover_watches()
    squeeze_watch_stats = process_squeeze_exhaustion_watches()
    paper_sent = 0
    non_paper_confirmed = 0
    for candidate in confirmed:
        if bool(candidate.get("paper_validation_only")):
            if set_confirmed_paper_signal(candidate):
                send_telegram(build_paper_signal_message(candidate))
                paper_sent += 1
        else:
            # PRO_QUALITY_FORWARD_ENABLED means this branch should stay empty.
            # Preserve diagnostics rather than silently turning it into LIVE.
            non_paper_confirmed += 1
    status = {
        "ts": now_ts(),
        "pending_active": len(STATE.get("pending_signals", [])),
        "confirmed": len(confirmed),
        "paper_sent": paper_sent,
        "non_paper_confirmed": non_paper_confirmed,
        "blocks": blocks,
        "near_miss": near_miss[:8],
        "active_watch": active_watch_stats,
        "squeeze_watch": squeeze_watch_stats,
    }
    STATE["last_pending_monitor"] = status
    save_state()
    return status


async def pending_monitor_loop():
    await asyncio.sleep(max(3, PAPER_PENDING_MONITOR_SECONDS))
    while True:
        try:
            if PRE_LIVE_CONFIRMATION_ENABLED:
                await asyncio.to_thread(monitor_pending_and_dispatch)
        except Exception as e:
            STATE["last_error"] = f"pending_monitor_loop: {repr(e)}"
            save_state()
        await asyncio.sleep(max(3, PAPER_PENDING_MONITOR_SECONDS))


async def telegram_outbox_loop():
    await asyncio.sleep(2)
    while True:
        try:
            await asyncio.to_thread(flush_telegram_outbox)
        except Exception as exc:
            STATE["last_error"] = f"telegram_outbox_loop: {repr(exc)}"
            save_state()
        await asyncio.sleep(TELEGRAM_OUTBOX_FLUSH_SECONDS)


BACKGROUND_TASKS: List[asyncio.Task] = []


async def _supervise_background_task(name: str, worker_factory) -> None:
    """Never let a background worker crash the Render web process.

    If a worker exits unexpectedly, record the error and restart it after a
    short delay. This is operational safety only; it does not change strategy.
    """
    while True:
        try:
            await worker_factory()
            # Long-running workers are not expected to return normally.
            STATE["last_error"] = f"{name}: worker returned unexpectedly; restarting"
            save_state()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                STATE["last_error"] = f"{name}: fatal worker error: {repr(exc)}"
                save_state()
            except Exception:
                pass
        await asyncio.sleep(5)


def _launch_background_task(name: str, worker_factory) -> None:
    try:
        task = asyncio.create_task(
            _supervise_background_task(name, worker_factory),
            name=f"render-safe:{name}",
        )
        BACKGROUND_TASKS.append(task)
    except Exception as exc:
        # Startup must remain alive even if task scheduling itself has a problem.
        try:
            STATE["last_error"] = f"task launch {name}: {repr(exc)}"
            save_state()
        except Exception:
            pass


@app.on_event("startup")
async def startup_event():
    """Render-safe startup.

    No recoverable state/DB/background-worker problem is allowed to make the
    ASGI application fail its startup handshake (which is what commonly leads
    to Render/Uvicorn exit status 3).
    """
    global STATE, SEED_RESTORE_INFO

    # 1) State restoration is non-fatal.
    try:
        loaded = load_state()
        STATE = loaded if isinstance(loaded, dict) else default_state()
    except Exception as exc:
        STATE = default_state()
        STATE["last_error"] = f"state restore fallback: {repr(exc)}"

    # 2) DB restoration is non-fatal. The HTTP service must stay available.
    try:
        init_adaptive_db()
        SEED_RESTORE_INFO = restore_adaptive_seed_if_empty()
        if adaptive_closed_count() > 0:
            rebuild_symbol_outcomes_from_adaptive_db()
        save_state()
    except Exception as exc:
        try:
            STATE["last_error"] = f"adaptive DB init error: {repr(exc)}"
            save_state()
        except Exception:
            pass

    # 3) Every long-running worker is supervised and restarted on failure.
    _launch_background_task("telegram_outbox_loop", telegram_outbox_loop)
    _launch_background_task("scan_loop", scan_loop)
    _launch_background_task("track_loop", track_loop)
    _launch_background_task("pending_monitor_loop", pending_monitor_loop)


@app.get("/render-health")
def render_health():
    """Minimal liveness endpoint: intentionally does not query SQLite/exchange."""
    return {
        "ok": True,
        "app": APP_NAME,
        "deploy": DEPLOY_MARKER,
        "state_loaded": isinstance(STATE, dict),
    }


@app.get("/")
def root():
    return HTMLResponse(
        f"<h3>{APP_NAME}</h3>"
        f"<p>{DEPLOY_MARKER}</p>"
        f"<p>Use /health /version /scan /auto-status /readiness /stats /adaptive-report "
        f"/source-audit /watch-audit /adaptive-retrain /adaptive-events "
        f"/export-data /telegram-backup /telegram-status /test-telegram</p>"
    )


@app.get("/health")
def health():
    try:
        readiness = real_money_readiness()
    except Exception as exc:
        readiness = {"ready": False, "live_enabled": False}
        try:
            STATE["last_error"] = f"health readiness error: {repr(exc)}"
        except Exception:
            pass
    return {
        "ok": True,
        "app": APP_NAME,
        "deploy": DEPLOY_MARKER,
        "active": len(STATE.get("active_signals", [])) if isinstance(STATE, dict) else 0,
        "real_money_ready": bool(readiness.get("ready")),
        "real_money_enabled": bool(readiness.get("live_enabled")),
        "last_error": STATE.get("last_error", "") if isinstance(STATE, dict) else "",
    }


@app.get("/version")
def version():
    return {"app": APP_NAME, "deploy_marker": DEPLOY_MARKER}


@app.get("/auto-status")
def auto_status():
    readiness = real_money_readiness()
    return JSONResponse({
        "app": APP_NAME,
        "deploy": DEPLOY_MARKER,
        "active_signals": STATE.get("active_signals", []),
        "pending_signals": STATE.get("pending_signals", []),
        "last_pending_monitor": STATE.get("last_pending_monitor", {}),
        "watch_audit_v17_2": STATE.get("watch_audit_v17_2", {}),
        "telegram_outbox_depth": telegram_outbox_depth(),
        "telegram_delivery": STATE.get("telegram_delivery", {}),
        "paper_metrics_v17_2_2": paper_validation_metrics(),
        "real_money_readiness": readiness,
        "last_scan": STATE.get("last_scan", {}),
        "last_error": STATE.get("last_error", ""),
        "stats": STATE.get("stats", {}),
    })


@app.get("/readiness")
def readiness_endpoint():
    """Human-auditable two-key gate; this endpoint never enables trading."""
    return JSONResponse({
        "app": APP_NAME,
        "deploy": DEPLOY_MARKER,
        "warning": (
            "Signal-only service: no exchange orders are placed. REAL_MONEY_SIGNALS "
            "remains ineffective until every evidence check passes."
        ),
        "readiness": real_money_readiness(),
    })


@app.get("/telegram-status")
def telegram_status_endpoint():
    queued = STATE.get("telegram_outbox", [])
    oldest = queued[0] if isinstance(queued, list) and queued else None
    return JSONResponse({
        "ok": True,
        "app": APP_NAME,
        "deploy": DEPLOY_MARKER,
        "summary": telegram_delivery_summary(),
        "outbox_depth": telegram_outbox_depth(),
        "delivery": STATE.get("telegram_delivery", {}),
        "oldest_queued": (
            {
                "id": oldest.get("id"),
                "created_at": oldest.get("created_at"),
                "attempts": oldest.get("attempts"),
                "next_retry_at": oldest.get("next_retry_at"),
                "last_error": oldest.get("last_error"),
            }
            if isinstance(oldest, dict)
            else None
        ),
    })


@app.get("/scan")
def manual_scan(send: bool = Query(True)):
    scan = run_scan(manual=True)
    # V18.2.2: run_scan(manual=True) already sends exactly one diagnostic.
    return JSONResponse(scan)


@app.get("/stats")
def stats():
    return HTMLResponse("<pre>" + build_stats_text() + "</pre>")


@app.get("/adaptive-report")
def adaptive_report_endpoint():
    try:
        return HTMLResponse("<pre>" + adaptive_report() + "</pre>")
    except Exception as e:
        return HTMLResponse("<pre>Adaptive report error: " + repr(e) + "</pre>", status_code=500)


@app.get("/source-audit")
def source_audit_endpoint(window: int = Query(25, ge=1, le=100)):
    try:
        return HTMLResponse("<pre>" + format_source_audit_message(window) + "</pre>")
    except Exception as e:
        return HTMLResponse("<pre>Source audit error: " + repr(e) + "</pre>", status_code=500)


@app.get("/watch-audit")
def watch_audit_endpoint():
    try:
        return JSONResponse(
            {
                "summary": watch_audit_summary(),
                "audit": STATE.get("watch_audit_v17_2", default_watch_audit()),
                "last_pending_monitor": STATE.get("last_pending_monitor", {}),
            }
        )
    except Exception as e:
        return JSONResponse({"error": repr(e)}, status_code=500)


@app.get("/adaptive-retrain")
def adaptive_retrain_endpoint(key: str = Query("")):
    if not admin_authorized(key):
        return JSONResponse(
            {"ok": False, "error": "Set ADMIN_KEY and provide ?key=..."},
            status_code=403,
        )
    try:
        return JSONResponse(maybe_retrain(force=True))
    except Exception as e:
        return JSONResponse({"trained": False, "error": repr(e)}, status_code=500)


@app.get("/adaptive-events")
def adaptive_events_endpoint(limit: int = Query(20, ge=1, le=100)):
    try:
        return JSONResponse(recent_adaptive_events(limit))
    except Exception as e:
        return JSONResponse({"error": repr(e)}, status_code=500)


@app.get("/export-data")
def export_data_endpoint(key: str = Query("")):
    if not admin_authorized(key):
        return JSONResponse(
            {"ok": False, "error": "Set ADMIN_KEY and provide ?key=..."},
            status_code=403,
        )
    try:
        closed_count = adaptive_closed_count()
        filename = f"adaptive_backup_{closed_count}_{int(time.time())}.json"
        return Response(
            content=build_export_bytes(),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        return JSONResponse({"ok": False, "error": repr(e)}, status_code=500)


@app.get("/telegram-backup")
def telegram_backup_endpoint(key: str = Query("")):
    if not admin_authorized(key):
        return JSONResponse(
            {"ok": False, "error": "Set ADMIN_KEY and provide ?key=..."},
            status_code=403,
        )
    try:
        closed_count = adaptive_closed_count()
        filename = f"adaptive_backup_{closed_count}_{int(time.time())}.json"
        sent = send_telegram_document(
            build_export_bytes(),
            filename,
            f"🧠 Manual adaptive backup · {closed_count} learning outcomes (LIVE + SHADOW)",
        )
        if sent:
            STATE["last_backup_closed_count"] = closed_count
            save_state()
        return {"ok": sent, "closed_count": closed_count, "filename": filename}
    except Exception as e:
        return JSONResponse({"ok": False, "error": repr(e)}, status_code=500)


@app.get("/test-telegram")
def test_telegram():
    accepted = send_telegram(
        f"✅ Test Telegram OK\n{APP_NAME}\n{DEPLOY_MARKER}"
    )
    return {
        "accepted": accepted,
        "outbox_depth": telegram_outbox_depth(),
        "delivery": STATE.get("telegram_delivery", {}),
        "last_error": STATE.get("last_error", ""),
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
