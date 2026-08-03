import os
import time
import json
import random
import asyncio
import io
import secrets
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
MIN_TRAIN_TRADES = int(os.getenv("ADAPTIVE_MIN_TRAIN_TRADES", "50"))
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
MIN_SIGNAL_PROBABILITY = float(os.getenv("ADAPTIVE_MIN_SIGNAL_PROBABILITY", "0.60"))
MAX_SIGNAL_PROBABILITY = float(os.getenv("ADAPTIVE_MAX_SIGNAL_PROBABILITY", "0.82"))
MIN_VALIDATION_COVERAGE = float(os.getenv("ADAPTIVE_MIN_VALIDATION_COVERAGE", "0.20"))
MODEL_ENABLED = os.getenv("ADAPTIVE_MODEL_ENABLED", "true").lower() == "true"
# Guarded live is the default: the model still cannot block anything until it has
# passed the independent holdout checks below. Set true to observe forever.
SHADOW_ONLY = os.getenv("ADAPTIVE_SHADOW_ONLY", "false").lower() == "true"
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

# V16.3 evidence guard. These defaults come from the user's first 50 closed
# outcomes: every candidate below Score 99 or with raw 1m volume ratio <= 0.80
# was non-profitable. The rule is still audited on NEW outcomes and can disable
# itself if shadow tracking shows that it is rejecting profitable TP3+ trades.
EVIDENCE_GUARD_ENABLED = os.getenv("EVIDENCE_GUARD_ENABLED", "true").lower() == "true"
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

# Never let learning touch these safety-critical settings.
LOCKED_SAFETY_KEYS = {
    "LEVERAGE",
    "MAX_SL_MOVE",
    "LOCAL_SCALP_MAX_SL_MOVE",
    "MAX_ACTIVE_SIGNALS",
    "MAX_SIGNALS_PER_SCAN",
}

FEATURE_NAMES: Tuple[str, ...] = (
    "score",
    "is_long",
    "is_a_plus",
    "ch3m_abs",
    "ch15m_abs",
    "ch30m_abs",
    "vol1",
    "vol5",
    "range1",
    "range5",
    "rr_tp1",
    "ladder_rr",
    "final_rr",
    "sl_price_move",
    "btc_bull",
    "btc_bear",
    "is_reversal",
    "is_dump",
    "is_instant",
    "is_aero",
    "edge_3m",
    "edge_15m",
    "edge_30m",
    "btc_alignment",
    # Non-linear quality markers added in V16.3. _vector_from_dict derives
    # them for old V16.2 backups, so all 50 historical rows remain trainable.
    "score_below_guard",
    "vol1_below_guard",
    "evidence_quality_pass",
    "symbol_fail_streak",
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
                evidence_guard_reason TEXT
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
    """Apply the auditable V16.3 Score/Vol1 rule before a live signal.

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
    probability, state = predict_probability(trade)
    if probability is None:
        return True, f"adaptive warm-up: {state.trained_rows}/{MIN_TRAIN_TRADES}", None

    accepted = probability >= state.threshold
    trade["adaptive_probability"] = probability
    trade["adaptive_shadow_probability"] = probability
    trade["adaptive_shadow_accepted"] = accepted
    trade["adaptive_model_version"] = state.version

    if SHADOW_ONLY:
        return True, (
            f"adaptive shadow v{state.version}: p={probability:.3f}, "
            f"threshold={state.threshold:.3f}, would_accept={accepted}"
        ), probability

    return accepted, (
        f"adaptive v{state.version}: p={probability:.3f}, "
        f"threshold={state.threshold:.3f}, accepted={accepted}"
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
                evidence_guard_version, evidence_guard_accepted, evidence_guard_reason
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
    baseline_limit = max(1, LIVE_AUDIT_BASELINE_WINDOW)
    with _LOCK, _connect() as conn:
        baseline_rows = conn.execute(
            """
            SELECT id, result, label, pnl_r, source, shadow_accepted
            FROM adaptive_trades
            WHERE source='live' AND id <= ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(state.activation_trade_id), baseline_limit),
        ).fetchall()
        decision_rows = conn.execute(
            """
            SELECT id, result, label, pnl_r, source, shadow_accepted
            FROM adaptive_trades
            WHERE id > ? AND model_version = ?
              AND (
                    source='live'
                    OR (source='shadow' AND shadow_accepted=0)
                  )
            ORDER BY id ASC
            """,
            (int(state.activation_trade_id), int(state.version)),
        ).fetchall()

    live_rows = [row for row in decision_rows if str(row["source"]) == "live"]
    blocked_rows = [
        row
        for row in decision_rows
        if str(row["source"]) == "shadow" and int(row["shadow_accepted"] or 0) == 0
    ]
    return {
        "model_version": int(state.version),
        "decision_count": len(decision_rows),
        "baseline": _outcome_metrics(baseline_rows),
        "live": _outcome_metrics(live_rows),
        "blocked": _outcome_metrics(blocked_rows),
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
    if action == "rollback":
        rollback = _rollback_to_parent(state, adaptive_closed_count(), reason)
    else:
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
) -> Dict[str, Any]:
    state.last_attempted_closed_count = int(closed_count)
    state.last_candidate_reason = str(reason)
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
    }
    if extra:
        payload.update(extra)
    _event("model_training_attempt", "Adaptive training attempt postponed", payload)
    return payload


def maybe_retrain(force: bool = False) -> Dict[str, Any]:
    init_adaptive_db()
    state = get_model_state()

    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, result, label, pnl_r, features_json,
                   source, model_version, shadow_accepted
            FROM adaptive_trades
            ORDER BY closed_at ASC, id ASC
            """
        ).fetchall()

    closed_count = len(rows)
    if closed_count < MIN_TRAIN_TRADES:
        return {
            "attempted": bool(force),
            "trained": False,
            "reason": "warmup",
            "closed_count": closed_count,
            "needed": MIN_TRAIN_TRADES,
            "training_data": _outcome_metrics(rows),
        }

    last_attempt = max(state.last_attempted_closed_count, state.last_trained_closed_count)
    if not force and last_attempt > 0 and closed_count - last_attempt < RETRAIN_EVERY:
        return {
            "attempted": False,
            "trained": False,
            "reason": "waiting_for_more_trades",
            "closed_count": closed_count,
            "next_at": last_attempt + RETRAIN_EVERY,
        }

    rows = rows[-MAX_TRAIN_ROWS:]
    labels = [int(row["label"]) for row in rows]
    pnl_r = [float(row["pnl_r"]) for row in rows]
    vectors = [_vector_from_dict(json.loads(row["features_json"])) for row in rows]

    positives = sum(labels)
    negatives = len(labels) - positives
    if positives < MIN_POSITIVE_ROWS or negatives < MIN_NEGATIVE_ROWS:
        return _training_attempt_failed(
            state,
            "class_imbalance",
            closed_count,
            rows,
            {"positives": positives, "negatives": negatives},
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

    enough_test_classes = min(sum(y_test), len(y_test) - sum(y_test)) >= 3
    passes_absolute_gate = (
        enough_test_classes
        and improvement >= MIN_MODEL_IMPROVEMENT
        and test_metrics["selected_count"] >= MIN_SELECTED_TEST_ROWS
        and test_metrics["coverage"] >= MIN_VALIDATION_COVERAGE
        and test_metrics["expectancy_r"] > 0
        and test_metrics["expectancy_r"]
        >= test_metrics["baseline_expectancy_r"] + MIN_EXPECTANCY_IMPROVEMENT_R
        and test_metrics["selected_wr"] >= test_metrics["baseline_wr"]
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
    promote = bool(
        passes_absolute_gate and beats_champion and champion_live_audit_ready
    )

    if not enough_test_classes:
        candidate_reason = "independent_test_class_imbalance"
    elif improvement < MIN_MODEL_IMPROVEMENT:
        candidate_reason = "logloss_not_better"
    elif test_metrics["selected_count"] < MIN_SELECTED_TEST_ROWS:
        candidate_reason = "too_few_selected_test_rows"
    elif test_metrics["coverage"] < MIN_VALIDATION_COVERAGE:
        candidate_reason = "coverage_too_low"
    elif test_metrics["expectancy_r"] <= 0:
        candidate_reason = "negative_test_expectancy"
    elif test_metrics["expectancy_r"] < test_metrics["baseline_expectancy_r"] + MIN_EXPECTANCY_IMPROVEMENT_R:
        candidate_reason = "expectancy_not_better_than_baseline"
    elif not beats_champion:
        candidate_reason = "champion_kept"
    elif not champion_live_audit_ready:
        candidate_reason = "champion_live_audit_pending"
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
        )
    elif state.active:
        # A weaker challenger must never deactivate or overwrite the champion.
        new_state = state
        new_state.last_attempted_closed_count = closed_count
        new_state.last_candidate_reason = candidate_reason
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
        "training_data": _outcome_metrics(rows),
        "test_baseline": test_baseline_outcomes,
        "test_candidate": test_candidate_outcomes,
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
    "logloss_not_better": "прогноз новой модели не точнее базового",
    "too_few_selected_test_rows": "новая модель выбрала слишком мало проверочных сигналов",
    "coverage_too_low": "модель блокирует слишком большую часть сигналов",
    "negative_test_expectancy": "средний результат новой модели остаётся отрицательным",
    "expectancy_not_better_than_baseline": "средний результат не лучше базовой стратегии",
    "champion_kept": "действующая модель лучше нового кандидата",
    "champion_live_audit_pending": "сначала нужен live-аудит действующей модели",
    "promoted": "новая модель прошла все проверки",
    "not_enough_live_comparison_rows": "для честного сравнения до/после пока мало live-сделок",
    "severe_negative_live_expectancy": "реальный средний результат модели стал сильно отрицательным",
    "too_many_profitable_signals_blocked": "модель заблокировала слишком много прибыльных сигналов",
    "live_model_worse_than_baseline": "реальные результаты модели хуже предыдущей версии",
    "live_model_guard_passed": "live-проверка не выявила ухудшения",
    "feature_schema_changed": "добавлены новые признаки качества; старые исходы сохранены",
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
    reason = str(report.get("candidate_reason", report.get("reason", "unknown")))
    dataset = report.get("training_data") or {}

    if promoted:
        title = f"✅ НОВАЯ ADAPTIVE-МОДЕЛЬ V{version} ВКЛЮЧЕНА"
    elif trained:
        title = "🧠 Анализ завершён — кандидат отклонён"
    else:
        title = "🧠 Анализ выполнен — обучение отложено"

    lines = [
        title,
        f"Закрытых результатов: {closed_count}",
        f"Все данные: {_metrics_line(dataset)}",
        f"Причина: {_adaptive_reason_ru(reason)}",
    ]

    baseline = report.get("test_baseline")
    candidate = report.get("test_candidate")
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

    if promoted:
        lines.extend(
            [
                "",
                f"Решение: V{version} теперь фильтрует новые сигналы.",
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
        lines.append(f"Решение: V{version} продолжает фильтровать сигналы.")
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
        title = f"↩️ АУДИТ V16.3 · RULE {version}: АВТООТКАТ"
    elif action == "keep":
        title = f"✅ АУДИТ V16.3 · RULE {version}: ФИЛЬТР ОСТАВЛЕН"
    else:
        title = f"📊 АУДИТ V16.3 · RULE {version}: НУЖНО БОЛЬШЕ ДАННЫХ"

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
        "🧠 Adaptive Learning Report",
        f"Closed trades: {total}",
        f"Profit labels: {wins} · Non-profit labels: {losses} · WR: {wr:.1f}%",
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
        lines.append("\nData sources:")
        for row in source_rows:
            n = int(row["n"] or 0)
            w = int(row["wins"] or 0)
            exp_r = float(row["expectancy_r"] or 0.0)
            lines.append(
                f"{row['source']}: {w}/{n} wins · WR {w/max(n,1)*100:.1f}% · expectancy {exp_r:+.3f}R"
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
    return {
        "export_schema": 4,
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

APP_NAME = "Professional Adaptive Futures Bot AUTO V16.3 EVIDENCE GUARD"
DEPLOY_MARKER = "V16_3_EVIDENCE_GUARD_SYMBOL_QUARANTINE_2026_08_02"

app = FastAPI(title=APP_NAME)

BINGX_BASE_URL = "https://open-api.bingx.com"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ADMIN_KEY = os.getenv("ADMIN_KEY", "")

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
MAX_ANALYZE_SYMBOLS = int(os.getenv("MAX_ANALYZE_SYMBOLS", "180"))
HOT_SYMBOLS_TO_ANALYZE = int(os.getenv("HOT_SYMBOLS_TO_ANALYZE", "60"))
DIAG_SECONDS = int(os.getenv("DIAG_SECONDS", "1200"))

# --- Signal limits ---
A_PLUS_MIN_SCORE = int(os.getenv("A_PLUS_MIN_SCORE", "88"))
B_MIN_SCORE = int(os.getenv("B_MIN_SCORE", "80"))
MAX_ACTIVE_SIGNALS = int(os.getenv("MAX_ACTIVE_SIGNALS", "2"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "2"))
PAIR_COOLDOWN_SECONDS = int(os.getenv("PAIR_COOLDOWN_SECONDS", "600"))
STRATEGY_COOLDOWN_SECONDS = int(os.getenv("STRATEGY_COOLDOWN_SECONDS", "90"))
MAX_LIVE_SIGNALS_24H = int(os.getenv("MAX_LIVE_SIGNALS_24H", "8"))

# A symbol that produces three consecutive non-profit outcomes is quarantined
# for 12 hours. Shadow observation continues and any TP3+ clears the quarantine.
SYMBOL_QUARANTINE_ENABLED = os.getenv("SYMBOL_QUARANTINE_ENABLED", "true").lower() == "true"
SYMBOL_FAIL_LIMIT = int(os.getenv("SYMBOL_FAIL_LIMIT", "3"))
SYMBOL_QUARANTINE_SECONDS = int(os.getenv("SYMBOL_QUARANTINE_SECONDS", "43200"))

# Shadow candidates are hypothetical trades: they are never sent as live signals,
# but their outcomes let the challenger learn from accepted and rejected examples.
SHADOW_TRACKING_ENABLED = os.getenv("SHADOW_TRACKING_ENABLED", "true").lower() == "true"
SHADOW_MAX_ACTIVE = int(os.getenv("SHADOW_MAX_ACTIVE", "24"))
SHADOW_PER_SCAN = int(os.getenv("SHADOW_PER_SCAN", "4"))
SHADOW_COOLDOWN_SECONDS = int(os.getenv("SHADOW_COOLDOWN_SECONDS", "600"))
LADDER_FOLLOWUP_MINUTES = int(os.getenv("LADDER_FOLLOWUP_MINUTES", "90"))
AUTO_TELEGRAM_BACKUP = os.getenv("AUTO_TELEGRAM_BACKUP", "true").lower() == "true"
AUTO_BACKUP_EVERY_CLOSED = int(os.getenv("AUTO_BACKUP_EVERY_CLOSED", "25"))

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
LOCAL_STOP_MODES = {"MARKET_DUMP_SHORT", "INSTANT_MOMENTUM_SHORT", "INSTANT_MOMENTUM_LONG", "AERO_STYLE_SHORT", "AERO_STYLE_LONG"}
FAST_RISK_MULT = float(os.getenv("FAST_RISK_MULT", "0.08"))
A_RISK_MULT = float(os.getenv("A_RISK_MULT", "0.14"))

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
# SHORT is already working in live stats. LONG is now stricter and must look like a real reclaim,
# not a late buy at the end of a pump.
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
STATE_IO_LOCK = threading.RLock()
SCAN_RUN_LOCK = threading.Lock()
TRACK_RUN_LOCK = threading.Lock()

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


def default_state() -> Dict[str, Any]:
    return {
        "schema_version": 3,
        "active_signals": [],
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
        "symbol_outcomes": {},
        "live_send_timestamps": [],
        "seed_restore": {},
        "last_backup_closed_count": 0,
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
        if isinstance(data, dict):
            base.update(data)
        # Deep defaults keep an older V13/V15 state compatible with V16.
        base.setdefault("active_signals", [])
        base.setdefault("shadow_signals", [])
        base.setdefault("pair_cooldown", {})
        base.setdefault("strategy_cooldown", {})
        base.setdefault("shadow_cooldown", {})
        base.setdefault("symbol_outcomes", {})
        base.setdefault("live_send_timestamps", [])
        base.setdefault("seed_restore", {})
        base.setdefault("last_backup_closed_count", 0)
        stats = base.setdefault("stats", {})
        for bucket, value in default_state()["stats"].items():
            stats.setdefault(bucket, value.copy() if isinstance(value, dict) else value)
        base["schema_version"] = 3
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
    candidates: List[Path] = []
    configured = Path(ADAPTIVE_SEED_PATH)
    if not configured.is_absolute():
        configured = Path.cwd() / configured
    candidates.append(configured)

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
    """Restore a portable V16.2/V16.3 export only into an empty adaptive DB.

    Active signals and cooldowns are intentionally not restored: stale market
    positions must never come back after a deploy. Historical outcomes, stats
    and the 50-trade training milestone are preserved.
    """
    init_adaptive_db()
    if adaptive_closed_count() > 0:
        return {"restored": 0, "source": "", "reason": "database_not_empty"}

    seed_path: Optional[Path] = None
    payload: Optional[Dict[str, Any]] = None
    for candidate in _adaptive_seed_candidates():
        if not candidate.is_file():
            continue
        try:
            with candidate.open("r", encoding="utf-8") as handle:
                possible = json.load(handle)
            trades = possible.get("adaptive_trades") if isinstance(possible, dict) else None
            if isinstance(trades, list) and trades:
                seed_path = candidate
                payload = possible
                break
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
                    evidence_guard_version, evidence_guard_accepted, evidence_guard_reason
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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

    raw_model = payload.get("adaptive_model") or {}
    allowed_model = {field.name for field in fields(ModelState)}
    try:
        restored_model = ModelState(
            **{key: value for key, value in raw_model.items() if key in allowed_model}
        )
    except Exception:
        restored_model = ModelState()
    restored_model.active = False
    restored_model.weights = [0.0] * (len(FEATURE_NAMES) + 1)
    restored_model.mean = [0.0] * len(FEATURE_NAMES)
    restored_model.std = [1.0] * len(FEATURE_NAMES)
    restored_model.trained_rows = inserted
    restored_model.last_attempted_closed_count = max(
        inserted, int(restored_model.last_attempted_closed_count or 0)
    )
    restored_model.last_candidate_reason = "seed_restored_feature_upgrade"
    restored_model.activation_trade_id = 0
    restored_model.activation_closed_count = 0
    restored_model.last_live_audit_decision_count = 0
    _save_model_state(restored_model)
    _save_model_snapshot(
        restored_model,
        "baseline" if restored_model.version == 0 else "inactive",
        "portable_seed_restored_feature_upgrade",
    )

    latest_id, latest_count = _latest_trade_marker()
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
# Telegram / API
# ============================================================

def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        STATE["last_error"] = "Telegram env missing: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID"
        save_state()
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:3900]}, timeout=10)
        if not r.ok:
            STATE["last_error"] = f"Telegram error {r.status_code}: {r.text[:250]}"
            save_state()
            return False
        return True
    except Exception as e:
        STATE["last_error"] = f"Telegram exception: {repr(e)}"
        save_state()
        return False


def send_telegram_document(data: bytes, filename: str, caption: str = "") -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        STATE["last_error"] = "Telegram env missing for document backup"
        save_state()
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        files = {
            "document": (filename, io.BytesIO(data), "application/json"),
        }
        form = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:900]}
        response = requests.post(url, data=form, files=files, timeout=30)
        if not response.ok:
            STATE["last_error"] = (
                f"Telegram document error {response.status_code}: {response.text[:250]}"
            )
            save_state()
            return False
        return True
    except Exception as e:
        STATE["last_error"] = f"Telegram document exception: {repr(e)}"
        save_state()
        return False


def admin_authorized(key: str) -> bool:
    return bool(ADMIN_KEY) and secrets.compare_digest(str(key or ""), ADMIN_KEY)


def adaptive_closed_count() -> int:
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        return int(conn.execute("SELECT COUNT(*) AS n FROM adaptive_trades").fetchone()["n"])


def maybe_send_auto_backup() -> None:
    if not AUTO_TELEGRAM_BACKUP or AUTO_BACKUP_EVERY_CLOSED <= 0:
        return
    try:
        closed_count = adaptive_closed_count()
        last_count = int(STATE.get("last_backup_closed_count", 0) or 0)
        if closed_count < AUTO_BACKUP_EVERY_CLOSED:
            return
        if closed_count - last_count < AUTO_BACKUP_EVERY_CLOSED:
            return
        filename = f"adaptive_backup_{closed_count}_{int(time.time())}.json"
        if send_telegram_document(
            build_export_bytes(),
            filename,
            f"🧠 Backup adaptive data · {closed_count} closed outcomes",
        ):
            STATE["last_backup_closed_count"] = closed_count
            save_state()
    except Exception as e:
        STATE["last_error"] = f"automatic backup error: {repr(e)}"
        save_state()


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


def hot_score(symbol: str) -> Tuple[float, str]:
    """Live-first hot score.
    V13.19 intentionally avoids using 15m candles here to keep scans fast.
    Deep analysis still loads 15m/1h only for selected candidates.
    """
    c1 = get_klines(symbol, "1m", 60, cache_seconds=8)
    c5 = get_klines(symbol, "5m", 80, cache_seconds=18)
    if not c1 or not c5:
        return 0.0, "no candles"

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
    return score, note

def select_hot_symbols(symbols: List[str]) -> Tuple[List[str], List[str]]:
    scored: List[Tuple[float, str, str]] = []
    notes: List[str] = []
    for sym in symbols[:MAX_ANALYZE_SYMBOLS]:
        try:
            sc, note = hot_score(sym)
            if sc > 0:
                scored.append((sc, sym, note))
        except Exception as e:
            STATE["last_error"] = f"hot_score {sym}: {repr(e)}"
    scored.sort(reverse=True, key=lambda x: x[0])

    for sc, sym, note in scored[:12]:
        notes.append(f"{display_symbol(sym)} hot {sc:.1f}: {note}")

    selected = [sym for sc, sym, _ in scored if sc >= HOT_MIN_SCORE][:HOT_SYMBOLS_TO_ANALYZE]

    # Keep the bot alive: if the market is quiet and strict hot score returns too few,
    # still analyze the best live-ranked names. The deeper fast filters remain in place.
    min_live_candidates = min(HOT_SYMBOLS_TO_ANALYZE, 50)
    if len(selected) < min_live_candidates:
        seen = set(selected)
        for sc, sym, _ in scored:
            if sym not in seen:
                selected.append(sym)
                seen.add(sym)
            if len(selected) >= min_live_candidates:
                break

    return selected[:MAX_ANALYZE_SYMBOLS], notes

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
    """Protect the bot from repeatedly taking bad LONGs while still allowing recovery later.
    If live LONG stats are poor, allow only very high-quality LONGs by blocking B-class setups upstream.
    """
    if not LONG_STATS_PROTECTION:
        return True, "long stats protection disabled"
    stats = STATE.setdefault("stats", default_state()["stats"])
    item = stats.get("side", {}).get("LONG", {})
    closed = int(item.get("profit", 0)) + int(item.get("sl", 0)) + int(item.get("expired", 0))
    if closed < LONG_STATS_MIN_CLOSED:
        return True, "not enough LONG stats"
    wr = int(item.get("profit", 0)) / max(closed, 1) * 100.0
    if wr < LONG_STATS_MIN_WR:
        return False, f"LONG stats weak: WR {wr:.1f}% after {closed}"
    return True, "LONG stats ok"


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


def _prune_live_send_timestamps(current_ts: Optional[int] = None) -> List[int]:
    current = int(current_ts or now_ts())
    cutoff = current - 24 * 60 * 60
    cleaned: List[int] = []
    for value in STATE.setdefault("live_send_timestamps", []):
        try:
            timestamp = int(value)
        except Exception:
            continue
        if timestamp > cutoff:
            cleaned.append(timestamp)
    STATE["live_send_timestamps"] = cleaned
    return cleaned


def live_signal_budget_24h() -> Tuple[int, int]:
    sent = len(_prune_live_send_timestamps())
    if MAX_LIVE_SIGNALS_24H <= 0:
        return sent, 10**9
    return sent, max(0, MAX_LIVE_SIGNALS_24H - sent)


def rebuild_symbol_outcomes_from_adaptive_db() -> Dict[str, Any]:
    """Rebuild only the lightweight per-symbol streak state from saved rows."""
    if not SYMBOL_QUARANTINE_ENABLED:
        STATE["symbol_outcomes"] = {}
        return {}
    init_adaptive_db()
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT symbol, result, source, closed_at FROM adaptive_trades "
            "ORDER BY closed_at ASC, id ASC"
        ).fetchall()

    outcomes: Dict[str, Any] = {}
    for row in rows:
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


def analyze_symbol(symbol: str, btc: Dict[str, Any], blocks: Dict[str, int], near_miss: List[str]) -> Optional[Dict[str, Any]]:
    symbol = normalize_symbol(symbol)
    c1 = get_klines(symbol, "1m", 120, cache_seconds=6)
    c5 = get_klines(symbol, "5m", 120, cache_seconds=15)
    c15 = get_klines(symbol, "15m", 120, cache_seconds=30)
    c1h = get_klines(symbol, "1h", 120, cache_seconds=90)

    if not c1 or not c5 or not c15 or not c1h:
        blocks["no_candles"] = blocks.get("no_candles", 0) + 1
        return None

    if ultra_risk_symbol(symbol, c5, c15):
        blocks["ultra_risk_block"] = blocks.get("ultra_risk_block", 0) + 1
        return None

    candidates: List[Dict[str, Any]] = []
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

        # V16.3: evaluate the transparent evidence rule first. A blocked trade
        # is still followed in shadow, so the bot can measure missed winners and
        # automatically roll the rule back. The symbol gate is evaluated on the
        # same candidate and contributes its failure streak to adaptive features.
        symbol_ok, symbol_reason = symbol_quarantine_gate(trade)
        trade["symbol_quarantine_reason"] = symbol_reason
        try:
            evidence_ok, evidence_reason = evidence_guard(trade)
            if not evidence_ok:
                blocks["evidence_guard_block"] = blocks.get("evidence_guard_block", 0) + 1
                add_shadow_signal(trade, "evidence_guard_block")
                if len(near_miss) < 8:
                    near_miss.append(f"{display_symbol(symbol)} {side}: {evidence_reason}")
                continue
        except Exception as e:
            trade["evidence_guard_reason"] = f"evidence guard error bypass: {repr(e)}"
            STATE["last_error"] = trade["evidence_guard_reason"]

        if not symbol_ok:
            blocks["symbol_quarantine_block"] = blocks.get("symbol_quarantine_block", 0) + 1
            add_shadow_signal(trade, "symbol_quarantine_block")
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
                add_shadow_signal(trade, "adaptive_model_block")
                if len(near_miss) < 8:
                    near_miss.append(f"{display_symbol(symbol)} {side}: {adaptive_reason}")
                continue
        except Exception as e:
            # Learning failure must never stop the market scanner.
            trade["adaptive_reason"] = f"adaptive error bypass: {repr(e)}"
            STATE["last_error"] = trade["adaptive_reason"]

        candidates.append(trade)

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
    return (
        f"{arrow} {s['side']} {display_symbol(s['symbol'])}\n"
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
        f"Риск: multiplier x{s['risk_mult']:.2f}\n"
        f"Evidence guard: {s.get('evidence_guard_reason', 'not evaluated')}\n"
        f"Symbol guard: {s.get('symbol_quarantine_reason', 'no history')}\n"
        f"Adaptive: {s.get('adaptive_reason', 'warm-up')}\n\n"
        f"📌 Логика:\n{s['reason']}\n"
        f"15m: {s['ch15m']*100:+.2f}% · 30m: {s['ch30m']*100:+.2f}% · 1m3: {s['ch3m_1m']*100:+.2f}%\n"
        f"Volume15 x{s['volume_ratio']:.2f} · Range5 x{s['range_ratio']:.2f} · Vol1 x{s.get('vol1', 1.0):.2f} · Range1 x{s.get('range1', 1.0):.2f}\n"
        f"BTC: {s['btc_text']}\n\n"
        f"⏱ Scalping rule: если за {FAST_MAX_MINUTES_TO_TP1} минут нет движения к TP1 — сигнал expired. Фаза рынка не важна; важна быстрая реализация."
    )


def build_diagnostic(scan: Dict[str, Any]) -> str:
    blocks = scan.get("blocks", {})
    block_lines = [f"{k}: {v}" for k, v in sorted(blocks.items(), key=lambda kv: -kv[1])[:12]]
    hot = scan.get("hot_notes", [])[:8]
    near = scan.get("near_miss", [])[:8]
    return (
        f"🧪 Диагностика V16.3 Evidence Guard\n"
        f"Проверено: {scan.get('checked', 0)} из universe {scan.get('universe', 0)}\n"
        f"Кандидатов: {scan.get('candidates', 0)} · отправлено: {scan.get('sent', 0)} · "
        f"shadow: {scan.get('shadow_added', 0)} · время: {scan.get('elapsed', 0):.0f}с\n"
        f"BTC: {scan.get('btc', 'unknown')}\n"
        f"Статистика: {wr_text(STATE.get('stats', {}).get('total', {}))}\n\n"
        f"Hot symbols:\n" + ("\n".join(hot) if hot else "нет") +
        f"\n\nГлавные блокировки:\n" + ("\n".join(block_lines) if block_lines else "нет") +
        ("\n\nПочти прошли:\n" + "\n".join(near) if near else "") +
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
    return (
        f"{signal.get('symbol','?')}:{signal.get('side','?')}:"
        f"{signal.get('strategy','?')}"
    )


def remove_matching_shadow(signal: Dict[str, Any]) -> None:
    key = shadow_key(signal)
    STATE["shadow_signals"] = [
        item for item in STATE.setdefault("shadow_signals", [])
        if shadow_key(item) != key
    ]


def add_shadow_signal(signal: Dict[str, Any], reason: str) -> bool:
    if not SHADOW_TRACKING_ENABLED:
        return False
    shadows = STATE.setdefault("shadow_signals", [])
    if len(shadows) >= SHADOW_MAX_ACTIVE:
        return False
    key = shadow_key(signal)
    cooldowns = STATE.setdefault("shadow_cooldown", {})
    if now_ts() < int(cooldowns.get(key, 0) or 0):
        return False
    if any(shadow_key(item) == key for item in shadows):
        return False

    shadow = dict(signal)
    ensure_signal_runtime_fields(shadow, "shadow")
    shadow["shadow_reason"] = reason
    shadow["stats_recorded"] = None
    shadows.append(shadow)
    cooldowns[key] = now_ts() + SHADOW_COOLDOWN_SECONDS
    save_state()
    return True


def add_active_signal(s: Dict[str, Any]) -> None:
    remove_matching_shadow(s)
    ensure_signal_runtime_fields(s, "live")
    STATE.setdefault("active_signals", []).append(s)
    STATE.setdefault("pair_cooldown", {})[s["symbol"]] = now_ts() + PAIR_COOLDOWN_SECONDS
    STATE.setdefault("strategy_cooldown", {})[s["strategy"]] = now_ts() + STRATEGY_COOLDOWN_SECONDS
    timestamps = _prune_live_send_timestamps()
    timestamps.append(now_ts())
    STATE["live_send_timestamps"] = timestamps
    save_state()


def _run_scan_impl(manual: bool = False) -> Dict[str, Any]:
    start = time.time()
    blocks: Dict[str, int] = {}
    near_miss: List[str] = []
    btc = btc_context()
    symbols = get_symbols()
    selected, hot_notes = select_hot_symbols(symbols)

    scan = {
        "checked": 0,
        "universe": len(symbols),
        "candidates": 0,
        "sent": 0,
        "shadow_added": 0,
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

    found: List[Dict[str, Any]] = []
    for sym in selected:
        try:
            s = analyze_symbol(sym, btc, blocks, near_miss)
            scan["checked"] += 1
            if s:
                found.append(s)
        except Exception as e:
            blocks["analyze_exception"] = blocks.get("analyze_exception", 0) + 1
            STATE["last_error"] = f"analyze {sym}: {repr(e)}"

    found.sort(key=lambda x: (x["grade"] == "A+", x["score"], x["ladder_rr"]), reverse=True)
    scan["candidates"] = len(found)

    sent = 0
    open_risk_count = sum(
        1 for signal in STATE.get("active_signals", [])
        if not signal.get(PROFIT_TARGET_KEY + "_hit") and not signal.get("stats_recorded")
    )
    free_slots = max(0, MAX_ACTIVE_SIGNALS - open_risk_count)
    sent_24h, daily_remaining = live_signal_budget_24h()
    send_limit = min(MAX_SIGNALS_PER_SCAN, free_slots, daily_remaining)
    if send_limit <= 0 and found:
        if daily_remaining <= 0:
            block_key = "daily_live_cap_block"
            detail = (
                f"24h live cap reached: {sent_24h}/{MAX_LIVE_SIGNALS_24H}; "
                "qualified candidates stay in shadow"
            )
        else:
            block_key = "active_slots_full_send_block"
            detail = f"found {len(found)} candidate(s), but active slots are full"
        blocks[block_key] = blocks.get(block_key, 0) + 1
        if len(near_miss) < 8:
            near_miss.append(detail)
    for s in found[:send_limit]:
        add_active_signal(s)
        send_telegram(build_signal_message(s))
        sent += 1

    shadow_added = 0
    if SHADOW_TRACKING_ENABLED:
        for candidate in found[send_limit:send_limit + max(0, SHADOW_PER_SCAN)]:
            shadow_reason = "daily_live_cap" if daily_remaining <= 0 else "qualified_not_sent"
            if add_shadow_signal(candidate, shadow_reason):
                shadow_added += 1

    scan["sent"] = sent
    scan["shadow_added"] = shadow_added
    scan["elapsed"] = time.time() - start
    STATE["last_scan"] = scan
    save_state()

    if manual or (sent == 0 and now_ts() - STATE.get("last_diag_ts", 0) >= DIAG_SECONDS):
        send_telegram(build_diagnostic(scan))
        STATE["last_diag_ts"] = now_ts()
        save_state()

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
            update_symbol_outcome_guard(signal, result, source=source)
            maybe_send_auto_backup()
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

        if sl_hit(side, price, signal["sl"]):
            signal["_closing_price"] = signal["sl"]
            safe_record_learning_result(signal, "sl", source="shadow")
            changed = True
            continue

        for key in ["tp1", "tp2"]:
            if target_hit(side, price, signal[key]):
                signal[f"{key}_hit"] = True

        if target_hit(side, price, signal[PROFIT_TARGET_KEY]):
            signal[PROFIT_TARGET_KEY + "_hit"] = True
            signal["_closing_price"] = signal[PROFIT_TARGET_KEY]
            safe_record_learning_result(signal, "profit", source="shadow")
            changed = True
            continue

        directional, progress = directional_progress_ratio(signal, price)
        fast_stop = (
            FAST_CANCEL_IF_NO_PROGRESS
            and age_minutes >= FAST_MAX_MINUTES_TO_TP1
            and ((not directional) or progress < FAST_MIN_PROGRESS_TO_KEEP)
        )
        if signal.get("tp1_hit"):
            fast_stop = False
        if fast_stop or age_minutes >= FAST_HARD_EXPIRE_MINUTES:
            signal["_closing_price"] = price
            safe_record_learning_result(signal, "expired", source="shadow")
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

        # TP1 and TP2 are intermediate. Full stop risk and the time-stop remain
        # active until TP3. Only TP3 locks a positive statistical outcome.
        if not signal.get("tp3_hit"):
            if sl_hit(side, price, signal["sl"]):
                signal["_closing_price"] = signal["sl"]
                if apply_result(signal, "sl"):
                    safe_record_learning_result(signal, "sl", source="live")
                send_telegram(
                    f"❌ Stop Loss\n"
                    f"{signal['grade']} · {side} {display_symbol(signal['symbol'])}\n"
                    f"Стратегия: {signal['strategy']}\n"
                    f"Вход: {format_price(signal['entry'])}\n"
                    f"SL: {format_price(signal['sl'])}\n"
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
        f"Mode: MARKET DUMP + AERO STYLE + LOCAL STOP SCALPER.\n"
        f"Логика: торгуем не фазу рынка, а только короткий дисбаланс: hot coin → sweep/reclaim → EMA/VWAP → immediate continuation → 5 TP.\n"
        f"Time-stop: если TP1 не двигается за {FAST_MAX_MINUTES_TO_TP1} мин — expired.\n"
        f"Compact targets: {TP1_MOVE*100:.2f}% / {TP2_MOVE*100:.2f}% / {TP3_MOVE*100:.2f}% / {TP4_MOVE*100:.2f}% / {TP5_MOVE*100:.2f}%.\n"
        f"Result rule: TP1/TP2 intermediate; profit starts from TP3.\n"
        f"Risk multiplier: B x{FAST_RISK_MULT:.2f}, A+ x{A_RISK_MULT:.2f}.\n"
        f"Evidence guard v{guard_state.version}: active={guard_state.active} · "
        f"Score ≥ {guard_state.min_score:.0f} · Vol1 > x{guard_state.min_vol1:.2f}; "
        f"audit every {EVIDENCE_AUDIT_EVERY} decisions.\n"
        f"Symbol quarantine: {SYMBOL_QUARANTINE_ENABLED} · after {SYMBOL_FAIL_LIMIT} "
        f"non-profit outcomes for {SYMBOL_QUARANTINE_SECONDS/3600:.0f}h · TP3 resets.\n"
        f"Live frequency cap: {MAX_LIVE_SIGNALS_24H}/24h; extra candidates stay in shadow.\n"
        f"Adaptive: {'permanent shadow' if SHADOW_ONLY else 'guarded live after independent validation'}; "
        f"first training from {MIN_TRAIN_TRADES} outcomes.\n"
        f"Audit: before/after every {LIVE_AUDIT_EVERY} closed model decisions · "
        f"automatic rollback: {AUTO_ROLLBACK_ENABLED}.\n"
        f"Shadow candidates: {SHADOW_TRACKING_ENABLED} · automatic JSON backup every {AUTO_BACKUP_EVERY_CLOSED} outcomes.\n"
        f"Seed JSON: {restored_text}.\n"
        f"Storage: STATE_FILE={STATE_FILE} · ADAPTIVE_DB_PATH={DB_PATH} · {storage_warning}."
    )
    try:
        scan = await asyncio.to_thread(run_scan, True)
        send_telegram(build_diagnostic(scan))
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
            send_telegram(f"⚠️ Ошибка auto-scan: {repr(e)}")
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


@app.on_event("startup")
async def startup_event():
    global STATE, SEED_RESTORE_INFO
    STATE = load_state()
    try:
        init_adaptive_db()
        SEED_RESTORE_INFO = restore_adaptive_seed_if_empty()
        if adaptive_closed_count() > 0:
            rebuild_symbol_outcomes_from_adaptive_db()
            save_state()
    except Exception as e:
        STATE["last_error"] = f"adaptive DB init error: {repr(e)}"
        save_state()
    asyncio.create_task(scan_loop())
    asyncio.create_task(track_loop())


@app.get("/")
def root():
    return HTMLResponse(
        f"<h3>{APP_NAME}</h3>"
        f"<p>{DEPLOY_MARKER}</p>"
        f"<p>Use /health /version /scan /auto-status /stats /adaptive-report "
        f"/adaptive-retrain /adaptive-events /export-data /telegram-backup /test-telegram</p>"
    )


@app.get("/health")
def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "deploy": DEPLOY_MARKER,
        "active": len(STATE.get("active_signals", [])),
        "last_error": STATE.get("last_error", ""),
    }


@app.get("/version")
def version():
    return {"app": APP_NAME, "deploy_marker": DEPLOY_MARKER}


@app.get("/auto-status")
def auto_status():
    return JSONResponse({
        "app": APP_NAME,
        "deploy": DEPLOY_MARKER,
        "active_signals": STATE.get("active_signals", []),
        "last_scan": STATE.get("last_scan", {}),
        "last_error": STATE.get("last_error", ""),
        "stats": STATE.get("stats", {}),
    })


@app.get("/scan")
def manual_scan(send: bool = Query(True)):
    scan = run_scan(manual=True)
    if send:
        send_telegram(build_diagnostic(scan))
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
            f"🧠 Manual adaptive backup · {closed_count} outcomes",
        )
        if sent:
            STATE["last_backup_closed_count"] = closed_count
            save_state()
        return {"ok": sent, "closed_count": closed_count, "filename": filename}
    except Exception as e:
        return JSONResponse({"ok": False, "error": repr(e)}, status_code=500)


@app.get("/test-telegram")
def test_telegram():
    ok = send_telegram(f"✅ Test Telegram OK\n{APP_NAME}\n{DEPLOY_MARKER}")
    return {"sent": ok, "last_error": STATE.get("last_error", "")}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
