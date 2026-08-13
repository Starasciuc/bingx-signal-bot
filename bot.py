"""V18.0 Research Recorder for BingX USDT-M perpetual markets.

This application is deliberately a research and PAPER-observation system.
It contains no authenticated exchange endpoints and cannot place, modify, or
cancel orders.  Its job is to collect a clean, immutable forward cohort before
any separate micro-LIVE implementation is considered.

Primary accounting rule requested by the owner:
    TP1 / TP2 are intermediate; only TP3+ is a profitable outcome.

The protocol records every broad liquid-momentum candidate, including those
that fail the visible PAPER gate, and follows the same executable price path at
1 / 3 / 6 / 10 / 15 / 30 minutes.  The six-minute result is the primary legacy
comparison; longer horizons answer whether the old time-stop is structurally
too short without changing it during the experiment.
"""

from __future__ import annotations

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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, Response


# ---------------------------------------------------------------------------
# Immutable research protocol
# ---------------------------------------------------------------------------

APP_NAME = "Professional Futures Research Bot V18.0 CLEAN FORWARD RECORDER"
DEPLOY_MARKER = "V18_0_CLEAN_FORWARD_RESEARCH_2026_08_13"
EXPORT_SCHEMA = "v18_research_export_v1"
COHORT_ID = "V18_0_LIQUID_MOMENTUM_FIXED_V1"

TARGET_MOVES: Tuple[float, ...] = (0.0065, 0.0120, 0.0185, 0.0260, 0.0350)
TP1_MOVE, TP2_MOVE, TP3_MOVE, TP4_MOVE, TP5_MOVE = TARGET_MOVES
HORIZONS_SECONDS: Tuple[int, ...] = (60, 180, 360, 600, 900, 1800)
PRIMARY_HORIZON_SECONDS = 360
FINAL_HORIZON_SECONDS = 1800

# One broad hypothesis.  These values are not modified by the program.
BROAD_MIN_DIRECTIONAL_3M = 0.0035
BROAD_MAX_DIRECTIONAL_3M = 0.0500
BROAD_MIN_DIRECTIONAL_15M = 0.0050
BROAD_MAX_DIRECTIONAL_15M = 0.0800

PAPER_MIN_DIRECTIONAL_3M = 0.0060
PAPER_MAX_DIRECTIONAL_3M = 0.0350
PAPER_MIN_DIRECTIONAL_15M = 0.0080
PAPER_MAX_DIRECTIONAL_15M = 0.0600
PAPER_MIN_VOL1 = 0.70
PAPER_MAX_VOL1 = 3.00
PAPER_MIN_RANGE1 = 1.10
PAPER_MAX_RANGE1 = 5.00
PAPER_MIN_BODY_FRACTION = 0.25
PAPER_LONG_MIN_CLOSE_LOCATION = 0.58
PAPER_SHORT_MAX_CLOSE_LOCATION = 0.42
PAPER_MAX_SPREAD_BPS = 15.0
PAPER_MIN_DEPTH_USDT = 1_000.0

MIN_24H_QUOTE_VOLUME_USDT = 3_000_000.0
PAPER_MIN_24H_QUOTE_VOLUME_USDT = 5_000_000.0
MIN_ACTIVE_CANDLE_FRACTION = 0.82
MIN_UNIQUE_CLOSE_FRACTION = 0.35
MAX_UNIVERSE = 80
SCAN_WORKERS = 8
MAX_OPEN_EPISODES = 120
BROAD_SYMBOL_COOLDOWN_SECONDS = 60 * 60
PAPER_SYMBOL_COOLDOWN_SECONDS = 6 * 60 * 60
INDEPENDENT_CLUSTER_SECONDS = 15 * 60

MIN_STOP_MOVE = 0.0080
MAX_STOP_MOVE = 0.0140
ATR_STOP_MULTIPLIER = 2.20
TAKER_FEE_PER_SIDE = 0.0005
ROUND_TRIP_FEE_MOVE = TAKER_FEE_PER_SIDE * 2.0

# Research gate.  Passing this does NOT enable real money in this program.
REVIEW_MIN_INDEPENDENT_PAPER = 100
REVIEW_MIN_UNIQUE_SYMBOLS = 30
REVIEW_MIN_TP3_RATE = 0.50
REVIEW_MIN_EXPECTANCY_R = 0.15
REVIEW_MIN_PROFIT_FACTOR = 1.25
REVIEW_MAX_DRAWDOWN_R = 8.0
REVIEW_RECENT_WINDOW = 50

PROTOCOL_MANIFEST = {
    "cohort_id": COHORT_ID,
    "targets": TARGET_MOVES,
    "horizons": HORIZONS_SECONDS,
    "primary_horizon": PRIMARY_HORIZON_SECONDS,
    "broad_3m": [BROAD_MIN_DIRECTIONAL_3M, BROAD_MAX_DIRECTIONAL_3M],
    "broad_15m": [BROAD_MIN_DIRECTIONAL_15M, BROAD_MAX_DIRECTIONAL_15M],
    "paper_3m": [PAPER_MIN_DIRECTIONAL_3M, PAPER_MAX_DIRECTIONAL_3M],
    "paper_15m": [PAPER_MIN_DIRECTIONAL_15M, PAPER_MAX_DIRECTIONAL_15M],
    "paper_vol1": [PAPER_MIN_VOL1, PAPER_MAX_VOL1],
    "paper_range1": [PAPER_MIN_RANGE1, PAPER_MAX_RANGE1],
    "spread_bps": PAPER_MAX_SPREAD_BPS,
    "depth_usdt": PAPER_MIN_DEPTH_USDT,
    "fees_round_trip": ROUND_TRIP_FEE_MOVE,
    "stop_bounds": [MIN_STOP_MOVE, MAX_STOP_MOVE],
}
PROTOCOL_HASH = hashlib.sha256(
    json.dumps(PROTOCOL_MANIFEST, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Environment and runtime
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
DB_PATH = os.getenv("RESEARCH_DB_PATH", "research_v18.sqlite3")
SEED_PATH = os.getenv("RESEARCH_SEED_PATH", "adaptive_seed.json")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
ADMIN_KEY = os.getenv("ADMIN_KEY", "").strip()
PORT = _env_int("PORT", 10000, 1)

SCAN_INTERVAL_SECONDS = _env_int("SCAN_INTERVAL_SECONDS", 60, 30)
TRACK_INTERVAL_SECONDS = _env_int("TRACK_INTERVAL_SECONDS", 15, 8)
DIAGNOSTIC_INTERVAL_SECONDS = _env_int("DIAGNOSTIC_INTERVAL_SECONDS", 1800, 300)
REQUEST_TIMEOUT_SECONDS = _env_float("REQUEST_TIMEOUT_SECONDS", 10.0, 3.0)
API_RETRIES = _env_int("API_RETRIES", 3, 1)
BACKUP_EVERY_PRIMARY = _env_int("BACKUP_EVERY_PRIMARY", 25, 5)
PAPER_CHECKPOINT_EVERY = _env_int("PAPER_CHECKPOINT_EVERY", 5, 1)
TELEGRAM_RETRIES = _env_int("TELEGRAM_RETRIES", 3, 1)


DB_LOCK = threading.RLock()
RUNTIME_LOCK = threading.RLock()
SCAN_LOCK = threading.Lock()
TRACK_LOCK = threading.Lock()
CACHE_LOCK = threading.RLock()

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
# SQLite storage
# ---------------------------------------------------------------------------


EPISODE_COLUMNS: Tuple[str, ...] = (
    "id", "episode_key", "cohort_id", "protocol_hash", "created_at",
    "cluster_id", "symbol", "side", "tier", "visible_paper", "independent",
    "quality_score", "paper_reject_reason", "entry_price", "entry_bid",
    "entry_ask", "spread_bps", "depth_usdt", "quote_volume_24h",
    "liquidity_rank", "stop_move", "tp1_move", "tp2_move", "tp3_move",
    "tp4_move", "tp5_move", "features_json", "status", "primary_outcome",
    "primary_pnl_r", "primary_closed_at", "primary_notified", "tp1_hit_at",
    "tp2_hit_at", "tp3_hit_at", "tp4_hit_at", "tp5_hit_at", "sl_hit_at",
    "max_favorable_move", "max_adverse_move", "last_exit_price",
    "last_seen_at", "data_gap_count", "finalized_at", "final_notified",
)

HORIZON_COLUMNS: Tuple[str, ...] = (
    "episode_id", "horizon_seconds", "observed_at", "exit_price",
    "net_move", "mfe_move", "mae_move", "tp1_hit", "tp2_hit", "tp3_hit",
    "sl_hit", "outcome", "pnl_r",
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
                tier TEXT NOT NULL,
                visible_paper INTEGER NOT NULL DEFAULT 0,
                independent INTEGER NOT NULL DEFAULT 0,
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
                finalized_at INTEGER,
                final_notified INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_episodes_open
            ON episodes(status, created_at);
            CREATE INDEX IF NOT EXISTS idx_episodes_cohort_tier
            ON episodes(cohort_id, tier, primary_closed_at);
            CREATE INDEX IF NOT EXISTS idx_episodes_symbol_side
            ON episodes(cohort_id, symbol, side, tier, created_at);

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
        conn.commit()


def meta_get(key: str, default: Any = None) -> Any:
    init_db()
    with DB_LOCK, db_connect() as conn:
        row = conn.execute("SELECT value_json FROM meta WHERE key=?", (key,)).fetchone()
    if row is None:
        return default
    try:
        return json.loads(row["value_json"])
    except Exception:
        return default


def meta_set(key: str, value: Any) -> None:
    init_db()
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    with DB_LOCK, db_connect() as conn:
        conn.execute(
            "INSERT INTO meta(key,value_json) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
            (key, payload),
        )
        conn.commit()


def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def table_count(table: str) -> int:
    if table not in {"episodes", "horizon_results", "outbox"}:
        raise ValueError("unsupported table")
    init_db()
    with DB_LOCK, db_connect() as conn:
        return int(conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])


def primary_count(tier: Optional[str] = None, independent_only: bool = False) -> int:
    query = (
        "SELECT COUNT(*) AS n FROM episodes WHERE cohort_id=? "
        "AND primary_outcome IS NOT NULL"
    )
    params: List[Any] = [COHORT_ID]
    if tier:
        query += " AND tier=?"
        params.append(tier)
    if independent_only:
        query += " AND independent=1"
    with DB_LOCK, db_connect() as conn:
        return int(conn.execute(query, params).fetchone()["n"])


def recent_episode_exists(symbol: str, side: str, tier: str, seconds: int) -> bool:
    cutoff = now_ts() - max(0, seconds)
    with DB_LOCK, db_connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM episodes WHERE cohort_id=? AND symbol=? AND side=? "
            "AND tier=? AND created_at>=? LIMIT 1",
            (COHORT_ID, symbol, side, tier, cutoff),
        ).fetchone()
    return row is not None


def independent_slot_available(cluster_id: int, side: str) -> bool:
    with DB_LOCK, db_connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM episodes WHERE cohort_id=? AND cluster_id=? "
            "AND side=? AND tier='paper' AND independent=1 LIMIT 1",
            (COHORT_ID, cluster_id, side),
        ).fetchone()
    return row is None


def open_episode_count() -> int:
    with DB_LOCK, db_connect() as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM episodes WHERE cohort_id=? AND status='open'",
                (COHORT_ID,),
            ).fetchone()["n"]
        )


def insert_episode(candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    created = int(candidate["created_at"])
    episode_key = hashlib.sha256(
        (
            f"{COHORT_ID}:{candidate['symbol']}:{candidate['side']}:"
            f"{candidate['tier']}:{created}:{candidate.get('entry_price', 0):.12g}"
        ).encode()
    ).hexdigest()
    values = {
        "episode_key": episode_key,
        "cohort_id": COHORT_ID,
        "protocol_hash": PROTOCOL_HASH,
        "created_at": created,
        "cluster_id": int(candidate["cluster_id"]),
        "symbol": candidate["symbol"],
        "side": candidate["side"],
        "tier": candidate["tier"],
        "visible_paper": 1 if candidate["tier"] == "paper" else 0,
        "independent": int(bool(candidate.get("independent"))),
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
        "tp1_move": TP1_MOVE,
        "tp2_move": TP2_MOVE,
        "tp3_move": TP3_MOVE,
        "tp4_move": TP4_MOVE,
        "tp5_move": TP5_MOVE,
        "features_json": json.dumps(candidate["features"], ensure_ascii=False),
        "status": "open",
        "max_favorable_move": 0.0,
        "max_adverse_move": 0.0,
        "data_gap_count": 0,
    }
    columns = tuple(values.keys())
    placeholders = ",".join("?" for _ in columns)
    try:
        with DB_LOCK, db_connect() as conn:
            cur = conn.execute(
                f"INSERT INTO episodes({','.join(columns)}) VALUES({placeholders})",
                tuple(values[col] for col in columns),
            )
            episode_id = int(cur.lastrowid)
            conn.commit()
            row = conn.execute("SELECT * FROM episodes WHERE id=?", (episode_id,)).fetchone()
        return row_to_dict(row) if row else None
    except sqlite3.IntegrityError:
        return None


def get_open_episodes() -> List[Dict[str, Any]]:
    with DB_LOCK, db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM episodes WHERE cohort_id=? AND status='open' "
            "ORDER BY created_at ASC",
            (COHORT_ID,),
        ).fetchall()
    return [row_to_dict(row) for row in rows]


def get_episode(episode_id: int) -> Optional[Dict[str, Any]]:
    with DB_LOCK, db_connect() as conn:
        row = conn.execute("SELECT * FROM episodes WHERE id=?", (episode_id,)).fetchone()
    return row_to_dict(row) if row else None


def update_episode_path(episode_id: int, updates: Dict[str, Any]) -> None:
    allowed = {
        "tp1_hit_at", "tp2_hit_at", "tp3_hit_at", "tp4_hit_at", "tp5_hit_at",
        "sl_hit_at", "max_favorable_move", "max_adverse_move", "last_exit_price",
        "last_seen_at", "data_gap_count", "status", "primary_outcome",
        "primary_pnl_r", "primary_closed_at", "primary_notified", "finalized_at",
        "final_notified",
    }
    clean = {key: value for key, value in updates.items() if key in allowed}
    if not clean:
        return
    assignments = ",".join(f"{key}=?" for key in clean)
    with DB_LOCK, db_connect() as conn:
        conn.execute(
            f"UPDATE episodes SET {assignments} WHERE id=?",
            (*clean.values(), episode_id),
        )
        conn.commit()


def horizon_exists(episode_id: int, horizon: int) -> bool:
    with DB_LOCK, db_connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM horizon_results WHERE episode_id=? AND horizon_seconds=?",
            (episode_id, horizon),
        ).fetchone()
    return row is not None


def insert_horizon_result(result: Dict[str, Any]) -> bool:
    columns = HORIZON_COLUMNS
    try:
        with DB_LOCK, db_connect() as conn:
            conn.execute(
                f"INSERT INTO horizon_results({','.join(columns)}) VALUES("
                + ",".join("?" for _ in columns)
                + ")",
                tuple(result[col] for col in columns),
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def horizon_rows(
    horizon: int,
    tier: Optional[str] = None,
    independent_only: bool = False,
) -> List[Dict[str, Any]]:
    query = (
        "SELECT h.*, e.symbol, e.side, e.tier, e.independent, e.cluster_id, "
        "e.created_at, e.protocol_hash, e.data_gap_count "
        "FROM horizon_results h JOIN episodes e ON e.id=h.episode_id "
        "WHERE e.cohort_id=? AND h.horizon_seconds=?"
    )
    params: List[Any] = [COHORT_ID, horizon]
    if tier:
        query += " AND e.tier=?"
        params.append(tier)
    if independent_only:
        query += " AND e.independent=1"
    query += " ORDER BY e.created_at ASC, e.id ASC"
    with DB_LOCK, db_connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [row_to_dict(row) for row in rows]


def primary_rows(tier: Optional[str] = None, independent_only: bool = False) -> List[Dict[str, Any]]:
    query = (
        "SELECT id AS episode_id, symbol, side, tier, independent, cluster_id, "
        "created_at, primary_outcome AS outcome, primary_pnl_r AS pnl_r, "
        "data_gap_count, protocol_hash FROM episodes WHERE cohort_id=? "
        "AND primary_outcome IS NOT NULL"
    )
    params: List[Any] = [COHORT_ID]
    if tier:
        query += " AND tier=?"
        params.append(tier)
    if independent_only:
        query += " AND independent=1"
    query += " ORDER BY created_at ASC, id ASC"
    with DB_LOCK, db_connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [row_to_dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Seed restore and portable JSON
# ---------------------------------------------------------------------------


def _legacy_summary(trades: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    results = Counter(str(row.get("result", "")) for row in trades)
    pnl = [float(row.get("pnl_r", 0.0) or 0.0) for row in trades]
    sources = Counter(str(row.get("source", "unknown")) for row in trades)
    return {
        "count": len(trades),
        "tp3": results["profit"],
        "sl": results["sl"],
        "expired": results["expired"],
        "success_rate": results["profit"] / len(trades) if trades else 0.0,
        "expectancy_r": statistics.fmean(pnl) if pnl else 0.0,
        "sources": dict(sources),
        "policy": "audit_only_never_training",
    }


def _insert_export_rows(
    conn: sqlite3.Connection,
    table: str,
    rows: Sequence[Dict[str, Any]],
    columns: Sequence[str],
) -> int:
    inserted = 0
    for row in rows:
        clean = {col: row.get(col) for col in columns if col in row}
        if not clean:
            continue
        cols = tuple(clean.keys())
        try:
            conn.execute(
                f"INSERT OR IGNORE INTO {table}({','.join(cols)}) VALUES("
                + ",".join("?" for _ in cols)
                + ")",
                tuple(clean[col] for col in cols),
            )
            inserted += int(conn.execute("SELECT changes() AS n").fetchone()["n"])
        except Exception:
            continue
    return inserted


def restore_seed_if_empty() -> Dict[str, Any]:
    init_db()
    if table_count("episodes") > 0:
        report = {"restored": 0, "reason": "database_not_empty"}
        with RUNTIME_LOCK:
            RUNTIME["seed_restore"] = report
        return report

    path = Path(SEED_PATH)
    if not path.exists():
        report = {"restored": 0, "reason": "seed_not_found", "path": str(path)}
        with RUNTIME_LOCK:
            RUNTIME["seed_restore"] = report
        return report
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        report = {"restored": 0, "reason": f"seed_invalid: {exc!r}"}
        with RUNTIME_LOCK:
            RUNTIME["seed_restore"] = report
        return report

    if payload.get("export_schema") == EXPORT_SCHEMA:
        episodes = list(payload.get("episodes") or [])
        horizons = list(payload.get("horizon_results") or [])
        with DB_LOCK, db_connect() as conn:
            inserted_episodes = _insert_export_rows(
                conn, "episodes", episodes, EPISODE_COLUMNS
            )
            inserted_horizons = _insert_export_rows(
                conn, "horizon_results", horizons, HORIZON_COLUMNS
            )
            conn.commit()
        legacy = payload.get("legacy_audit") or {}
        if legacy:
            meta_set("legacy_audit", legacy)
        milestones = payload.get("milestones") or {}
        for key, value in milestones.items():
            meta_set(str(key), value)
        report = {
            "restored": inserted_episodes,
            "horizons": inserted_horizons,
            "reason": "v18_backup_restored",
            "source": path.name,
        }
    elif isinstance(payload.get("adaptive_trades"), list):
        legacy = _legacy_summary(payload["adaptive_trades"])
        meta_set("legacy_audit", legacy)
        report = {
            "restored": 0,
            "legacy_restored": legacy["count"],
            "reason": "legacy_500_audit_only",
            "source": path.name,
        }
    else:
        report = {"restored": 0, "reason": "unrecognized_seed_schema"}
    with RUNTIME_LOCK:
        RUNTIME["seed_restore"] = report
    return report


def export_payload() -> Dict[str, Any]:
    init_db()
    with DB_LOCK, db_connect() as conn:
        episodes = [
            row_to_dict(row)
            for row in conn.execute("SELECT * FROM episodes ORDER BY id ASC").fetchall()
        ]
        horizons = [
            row_to_dict(row)
            for row in conn.execute(
                "SELECT * FROM horizon_results ORDER BY episode_id,horizon_seconds"
            ).fetchall()
        ]
    return {
        "export_schema": EXPORT_SCHEMA,
        "exported_at": now_ts(),
        "app": APP_NAME,
        "deploy_marker": DEPLOY_MARKER,
        "protocol": PROTOCOL_MANIFEST,
        "protocol_hash": PROTOCOL_HASH,
        "cohort_id": COHORT_ID,
        "legacy_audit": meta_get("legacy_audit", {}),
        "milestones": {
            "last_backup_primary": meta_get("last_backup_primary", 0),
            "last_report_primary": meta_get("last_report_primary", 0),
            "last_paper_checkpoint": meta_get("last_paper_checkpoint", 0),
        },
        "episodes": episodes,
        "horizon_results": horizons,
        "runtime": runtime_snapshot(),
        "warning": (
            "Research/PAPER data only. No exchange orders. Rename the newest full "
            "backup exactly adaptive_seed.json before a redeploy without Render Disk."
        ),
    }


def export_bytes() -> bytes:
    return json.dumps(
        export_payload(), ensure_ascii=False, indent=2, allow_nan=False
    ).encode("utf-8")


# ---------------------------------------------------------------------------
# Telegram with ordered retry outbox
# ---------------------------------------------------------------------------


def telegram_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def _telegram_post_text(text: str) -> Tuple[bool, str]:
    if not telegram_configured():
        return False, "Telegram credentials are not configured"
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    last_error = "unknown"
    for attempt in range(TELEGRAM_RETRIES):
        try:
            response = requests.post(
                url,
                data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": text,
                    "disable_web_page_preview": "true",
                },
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if response.ok:
                with RUNTIME_LOCK:
                    RUNTIME["telegram_sent"] += 1
                return True, ""
            last_error = f"HTTP {response.status_code}: {response.text[:180]}"
        except Exception as exc:
            last_error = repr(exc)
        time.sleep(0.35 * (2**attempt))
    with RUNTIME_LOCK:
        RUNTIME["telegram_errors"] += 1
    return False, last_error


def _telegram_post_document(data: bytes, filename: str, caption: str) -> Tuple[bool, str]:
    if not telegram_configured():
        return False, "Telegram credentials are not configured"
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    last_error = "unknown"
    for attempt in range(TELEGRAM_RETRIES):
        try:
            response = requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1024]},
                files={"document": (filename, io.BytesIO(data), "application/json")},
                timeout=max(20.0, REQUEST_TIMEOUT_SECONDS * 2),
            )
            if response.ok:
                with RUNTIME_LOCK:
                    RUNTIME["telegram_sent"] += 1
                return True, ""
            last_error = f"HTTP {response.status_code}: {response.text[:180]}"
        except Exception as exc:
            last_error = repr(exc)
        time.sleep(0.5 * (2**attempt))
    with RUNTIME_LOCK:
        RUNTIME["telegram_errors"] += 1
    return False, last_error


def enqueue_outbox(
    kind: str,
    payload: bytes,
    filename: str = "",
    caption: str = "",
) -> bool:
    with DB_LOCK, db_connect() as conn:
        conn.execute(
            "INSERT INTO outbox(kind,payload,filename,caption,created_at,next_retry_at) "
            "VALUES(?,?,?,?,?,?)",
            (kind, sqlite3.Binary(payload), filename, caption, now_ts(), now_ts() + 15),
        )
        conn.commit()
    return True


def send_text(text: str, critical: bool = True) -> bool:
    ok, error = _telegram_post_text(text)
    if not ok:
        set_runtime_error(f"Telegram text: {error}")
        if critical and telegram_configured():
            # "True" means accepted for ordered delivery, not necessarily that
            # Telegram answered on the first attempt.  Callers can safely mark
            # the event as notified, preventing duplicate outbox rows.
            return enqueue_outbox("text", text.encode("utf-8"))
    return ok


def send_document(data: bytes, filename: str, caption: str, critical: bool = True) -> bool:
    ok, error = _telegram_post_document(data, filename, caption)
    if not ok:
        set_runtime_error(f"Telegram document: {error}")
        if critical and telegram_configured():
            return enqueue_outbox("document", data, filename, caption)
    return ok


def flush_outbox(limit: int = 5) -> Dict[str, int]:
    if not telegram_configured():
        return {"sent": 0, "remaining": table_count("outbox")}
    sent = 0
    with DB_LOCK, db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM outbox WHERE next_retry_at<=? ORDER BY id ASC LIMIT ?",
            (now_ts(), max(1, limit)),
        ).fetchall()
    for row in rows:
        payload = bytes(row["payload"])
        if row["kind"] == "document":
            ok, error = _telegram_post_document(
                payload, str(row["filename"] or "backup.json"), str(row["caption"] or "")
            )
        else:
            ok, error = _telegram_post_text(payload.decode("utf-8", errors="replace"))
        with DB_LOCK, db_connect() as conn:
            if ok:
                conn.execute("DELETE FROM outbox WHERE id=?", (row["id"],))
                sent += 1
            else:
                attempts = int(row["attempts"] or 0) + 1
                delay = min(1800, 15 * (2 ** min(attempts, 7)))
                conn.execute(
                    "UPDATE outbox SET attempts=?,next_retry_at=? WHERE id=?",
                    (attempts, now_ts() + delay, row["id"]),
                )
                set_runtime_error(f"Telegram outbox: {error}")
            conn.commit()
        if not ok:
            break
    return {"sent": sent, "remaining": table_count("outbox")}


# ---------------------------------------------------------------------------
# Public BingX market data only
# ---------------------------------------------------------------------------


class SlidingRateLimiter:
    def __init__(self, max_calls: int = 82, window_seconds: float = 10.0) -> None:
        self.max_calls = max_calls
        self.window = window_seconds
        self.calls: Deque[float] = deque()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            wait = 0.0
            with self.lock:
                now = time.monotonic()
                while self.calls and now - self.calls[0] >= self.window:
                    self.calls.popleft()
                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return
                wait = max(0.01, self.window - (now - self.calls[0]) + 0.01)
            time.sleep(min(wait, 1.0))


API_LIMITER = SlidingRateLimiter()
KLINE_CACHE: Dict[str, Tuple[float, Optional[List[Dict[str, float]]]]] = {}
TICKER_CACHE: Tuple[float, List[Dict[str, Any]]] = (0.0, [])


def api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    last_error = "unknown"
    for attempt in range(API_RETRIES):
        API_LIMITER.acquire()
        try:
            with RUNTIME_LOCK:
                RUNTIME["api_calls"] += 1
            response = requests.get(
                BINGX_BASE_URL + path,
                params=params or {},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code in {408, 425, 429, 500, 502, 503, 504}:
                last_error = f"HTTP {response.status_code} {path}"
                time.sleep(0.25 * (attempt + 1))
                continue
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                code = payload.get("code")
                if code in (None, 0, "0"):
                    return payload
                last_error = (
                    f"BingX code={code} {path}: "
                    f"{str(payload.get('msg') or payload.get('message') or '')[:180]}"
                )
                if str(code) == "100410":
                    time.sleep(0.75 * (attempt + 1))
                    continue
                break
            last_error = f"non-object response {path}"
        except Exception as exc:
            last_error = f"{path}: {exc!r}"
            time.sleep(0.30 * (attempt + 1))
    with RUNTIME_LOCK:
        RUNTIME["api_errors"] += 1
    set_runtime_error(last_error)
    return None


def normalize_symbol(symbol: str) -> str:
    value = str(symbol or "").upper().replace("/", "-").replace("_", "-")
    if "-" not in value and value.endswith("USDT"):
        value = value[:-4] + "-USDT"
    return value


def base_asset(symbol: str) -> str:
    return normalize_symbol(symbol).split("-", 1)[0]


def good_symbol(symbol: str) -> bool:
    value = normalize_symbol(symbol)
    if not value.endswith("-USDT"):
        return False
    base = base_asset(value)
    if not base or base in {"USDT", "USDC", "FDUSD", "TUSD", "DAI"}:
        return False
    if any(tag in base for tag in ("BULL", "BEAR", "UP", "DOWN")):
        return False
    return True


def _first_float(item: Dict[str, Any], names: Sequence[str]) -> float:
    for name in names:
        try:
            value = float(item.get(name) or 0.0)
            if math.isfinite(value) and value != 0:
                return value
        except Exception:
            continue
    return 0.0


def fetch_tickers(force: bool = False) -> List[Dict[str, Any]]:
    global TICKER_CACHE
    with CACHE_LOCK:
        if not force and time.time() - TICKER_CACHE[0] < 45 and TICKER_CACHE[1]:
            return list(TICKER_CACHE[1])
    payload = api_get("/openApi/swap/v2/quote/ticker")
    data: Any = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        data = [data]
    out: List[Dict[str, Any]] = []
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            symbol = normalize_symbol(str(item.get("symbol") or item.get("s") or ""))
            if not good_symbol(symbol):
                continue
            last = _first_float(item, ("lastPrice", "last", "price", "close"))
            quote = _first_float(
                item,
                (
                    "quoteVolume", "quoteVol", "quoteVolume24h", "quoteAssetVolume",
                    "turnover", "amount",
                ),
            )
            base_volume = _first_float(item, ("volume", "vol", "volume24h"))
            if quote <= 0 and last > 0 and base_volume > 0:
                quote = last * base_volume
            if last <= 0 or quote <= 0:
                continue
            out.append(
                {
                    "symbol": symbol,
                    "last": last,
                    "quote_volume_24h": quote,
                    "change_24h": _first_float(
                        item, ("priceChangePercent", "priceChangeRate", "change24h")
                    ),
                }
            )
    out.sort(key=lambda row: row["quote_volume_24h"], reverse=True)
    with CACHE_LOCK:
        if out:
            TICKER_CACHE = (time.time(), list(out))
    return out


def liquid_universe() -> List[Dict[str, Any]]:
    rows = [
        row for row in fetch_tickers()
        if float(row["quote_volume_24h"]) >= MIN_24H_QUOTE_VOLUME_USDT
    ]
    rows = rows[:MAX_UNIVERSE]
    total = max(1, len(rows) - 1)
    for index, row in enumerate(rows):
        row["liquidity_rank"] = 1.0 - index / total
    return rows


def parse_klines(raw: Any) -> Optional[List[Dict[str, float]]]:
    if not raw:
        return None
    candles: List[Dict[str, float]] = []
    for item in raw:
        try:
            if isinstance(item, dict):
                candle = {
                    "time": int(item.get("time") or item.get("openTime") or item.get("T") or 0),
                    "open": float(item.get("open")),
                    "high": float(item.get("high")),
                    "low": float(item.get("low")),
                    "close": float(item.get("close")),
                    "volume": float(item.get("volume") or item.get("vol") or 0.0),
                }
            elif isinstance(item, (list, tuple)) and len(item) >= 6:
                candle = {
                    "time": int(item[0]),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                }
            else:
                continue
            if all(candle[key] > 0 for key in ("open", "high", "low", "close")):
                candles.append(candle)
        except Exception:
            continue
    candles.sort(key=lambda candle: candle["time"])
    return candles if len(candles) >= 40 else None


def get_klines(symbol: str, limit: int = 90, cache_seconds: int = 40) -> Optional[List[Dict[str, float]]]:
    normalized = normalize_symbol(symbol)
    key = f"{normalized}:1m:{limit}"
    with CACHE_LOCK:
        cached = KLINE_CACHE.get(key)
        if cached and time.time() - cached[0] < cache_seconds:
            return cached[1]
    for endpoint in ("/openApi/swap/v3/quote/klines", "/openApi/swap/v2/quote/klines"):
        payload = api_get(
            endpoint,
            {"symbol": normalized, "interval": "1m", "limit": limit},
        )
        candles = parse_klines(payload.get("data") if isinstance(payload, dict) else None)
        if candles:
            with CACHE_LOCK:
                KLINE_CACHE[key] = (time.time(), candles)
            return candles
    with CACHE_LOCK:
        KLINE_CACHE[key] = (time.time(), None)
    return None


def _book_level(level: Any) -> Tuple[float, float]:
    try:
        if isinstance(level, dict):
            return (
                float(level.get("price") or level.get("p") or 0.0),
                float(level.get("quantity") or level.get("qty") or level.get("q") or 0.0),
            )
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return float(level[0]), float(level[1])
    except Exception:
        pass
    return 0.0, 0.0


def get_book(symbol: str, include_depth: bool = False) -> Dict[str, Any]:
    normalized = normalize_symbol(symbol)
    payload = api_get("/openApi/swap/v2/quote/bookTicker", {"symbol": normalized})
    data: Any = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, list):
        data = data[0] if data else None
    bid = ask = bid_qty = ask_qty = 0.0
    if isinstance(data, dict):
        bid = _first_float(data, ("bidPrice", "bid", "b"))
        ask = _first_float(data, ("askPrice", "ask", "a"))
        bid_qty = _first_float(data, ("bidQty", "bidQuantity", "B"))
        ask_qty = _first_float(data, ("askQty", "askQuantity", "A"))

    bids: List[Any] = []
    asks: List[Any] = []
    if include_depth:
        depth = api_get(
            "/openApi/swap/v2/quote/depth",
            {"symbol": normalized, "limit": 5},
        )
        depth_data: Any = depth.get("data") if isinstance(depth, dict) else None
        if isinstance(depth_data, dict):
            bids = list(depth_data.get("bids") or [])[:5]
            asks = list(depth_data.get("asks") or [])[:5]
            if bids and asks:
                bid, bid_qty = _book_level(bids[0])
                ask, ask_qty = _book_level(asks[0])

    if bid <= 0 or ask <= bid:
        return {
            "ok": False,
            "bid": 0.0,
            "ask": 0.0,
            "spread_bps": 999.0,
            "depth_usdt": 0.0,
            "reason": "book_unavailable",
        }
    mid = (bid + ask) / 2.0
    spread = (ask - bid) / max(mid, 1e-12) * 10_000.0
    if bids and asks:
        bid_depth = sum(price * qty for price, qty in map(_book_level, bids))
        ask_depth = sum(price * qty for price, qty in map(_book_level, asks))
    else:
        bid_depth = bid * max(0.0, bid_qty)
        ask_depth = ask * max(0.0, ask_qty)
    return {
        "ok": True,
        "bid": bid,
        "ask": ask,
        "spread_bps": spread,
        "depth_usdt": min(bid_depth, ask_depth),
        "reason": "ok",
    }


def get_books(symbols: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch best bid/ask for all tracked contracts in one public request.

    BingX bookTicker supports omitting ``symbol`` to return all contracts.  A
    batch snapshot avoids one REST call per open research episode every 15s.
    Missing rows fall back to the single-symbol endpoint.
    """
    wanted = {normalize_symbol(symbol) for symbol in symbols if symbol}
    if not wanted:
        return {}
    payload = api_get("/openApi/swap/v2/quote/bookTicker")
    raw: Any = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(raw, dict):
        raw = [raw]
    books: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            symbol = normalize_symbol(str(item.get("symbol") or item.get("s") or ""))
            if symbol not in wanted:
                continue
            bid = _first_float(item, ("bidPrice", "bid", "b"))
            ask = _first_float(item, ("askPrice", "ask", "a"))
            if bid <= 0 or ask <= bid:
                continue
            bid_qty = _first_float(item, ("bidQty", "bidQuantity", "B"))
            ask_qty = _first_float(item, ("askQty", "askQuantity", "A"))
            mid = (bid + ask) / 2.0
            books[symbol] = {
                "ok": True,
                "bid": bid,
                "ask": ask,
                "spread_bps": (ask - bid) / max(mid, 1e-12) * 10_000.0,
                "depth_usdt": min(bid * bid_qty, ask * ask_qty),
                "reason": "batch_book_ticker",
            }
    missing = sorted(wanted.difference(books))
    if missing:
        with ThreadPoolExecutor(max_workers=min(4, len(missing))) as pool:
            futures = {pool.submit(get_book, symbol, False): symbol for symbol in missing}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    books[symbol] = future.result()
                except Exception:
                    books[symbol] = {"ok": False, "reason": "book_exception"}
    return books


# ---------------------------------------------------------------------------
# Indicators and fixed candidate protocol
# ---------------------------------------------------------------------------


def _median(values: Iterable[float], default: float = 0.0) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    return statistics.median(clean) if clean else default


def ema(values: Sequence[float], period: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (max(1, period) + 1.0)
    value = float(values[0])
    for item in values[1:]:
        value = alpha * float(item) + (1.0 - alpha) * value
    return value


def vwap(candles: Sequence[Dict[str, float]], bars: int = 20) -> float:
    part = list(candles[-bars:])
    volume = sum(max(0.0, row["volume"]) for row in part)
    if volume <= 0:
        return part[-1]["close"] if part else 0.0
    return sum(
        ((row["high"] + row["low"] + row["close"]) / 3.0)
        * max(0.0, row["volume"])
        for row in part
    ) / volume


def atr_percent(candles: Sequence[Dict[str, float]], bars: int = 14) -> float:
    if len(candles) < bars + 1:
        return 0.0
    ranges = []
    for index in range(len(candles) - bars, len(candles)):
        row = candles[index]
        previous_close = candles[index - 1]["close"]
        ranges.append(
            max(
                row["high"] - row["low"],
                abs(row["high"] - previous_close),
                abs(row["low"] - previous_close),
            )
        )
    return statistics.fmean(ranges) / max(candles[-1]["close"], 1e-12)


def close_location(candle: Dict[str, float]) -> float:
    span = max(candle["high"] - candle["low"], 1e-12)
    return (candle["close"] - candle["low"]) / span


def completed_volume_ratio(candles: Sequence[Dict[str, float]]) -> float:
    if len(candles) < 24:
        return 0.0
    current = candles[-2]["volume"]
    reference = _median((row["volume"] for row in candles[-22:-2]), 0.0)
    return current / reference if reference > 0 else 0.0


def completed_range_ratio(candles: Sequence[Dict[str, float]]) -> float:
    if len(candles) < 24:
        return 0.0
    current = candles[-2]["high"] - candles[-2]["low"]
    reference = _median(
        (row["high"] - row["low"] for row in candles[-22:-2]), 0.0
    )
    return current / reference if reference > 0 else 0.0


def directional_return(candles: Sequence[Dict[str, float]], bars: int) -> float:
    if len(candles) <= bars:
        return 0.0
    current = candles[-1]["close"]
    previous = candles[-1 - bars]["close"]
    return (current - previous) / max(previous, 1e-12)


def continuity_features(candles: Sequence[Dict[str, float]]) -> Tuple[float, float]:
    part = list(candles[-60:])
    if not part:
        return 0.0, 0.0
    active = sum(1 for row in part if row["volume"] > 0) / len(part)
    rounded = {round(row["close"], 10) for row in part}
    unique = len(rounded) / len(part)
    return active, unique


def btc_context() -> Dict[str, Any]:
    candles = get_klines("BTC-USDT", 90, cache_seconds=45)
    if not candles:
        return {"regime": "UNKNOWN", "ret15": 0.0, "ret60": 0.0}
    ret15 = directional_return(candles, 15)
    ret60 = directional_return(candles, 60)
    if ret15 >= 0.003 and ret60 >= 0:
        regime = "BULL"
    elif ret15 <= -0.003 and ret60 <= 0:
        regime = "BEAR"
    else:
        regime = "RANGE"
    return {"regime": regime, "ret15": ret15, "ret60": ret60}


def build_broad_candidate(
    ticker: Dict[str, Any],
    candles: Sequence[Dict[str, float]],
    btc: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if len(candles) < 65:
        return None
    ret3 = directional_return(candles, 3)
    ret15 = directional_return(candles, 15)
    if ret3 == 0 or ret15 == 0 or ret3 * ret15 <= 0:
        return None
    side = "LONG" if ret3 > 0 else "SHORT"
    d3 = abs(ret3)
    d15 = abs(ret15)
    if not (BROAD_MIN_DIRECTIONAL_3M <= d3 <= BROAD_MAX_DIRECTIONAL_3M):
        return None
    if not (BROAD_MIN_DIRECTIONAL_15M <= d15 <= BROAD_MAX_DIRECTIONAL_15M):
        return None

    active_fraction, unique_fraction = continuity_features(candles)
    if active_fraction < MIN_ACTIVE_CANDLE_FRACTION:
        return None
    if unique_fraction < MIN_UNIQUE_CLOSE_FRACTION:
        return None

    last = candles[-1]
    span = max(last["high"] - last["low"], 1e-12)
    body_fraction = abs(last["close"] - last["open"]) / span
    location = close_location(last)
    ema9 = ema([row["close"] for row in candles[-30:]], 9)
    current_vwap = vwap(candles, 20)
    vol1 = completed_volume_ratio(candles)
    range1 = completed_range_ratio(candles)
    atr_pct = atr_percent(candles)
    stop_move = min(MAX_STOP_MOVE, max(MIN_STOP_MOVE, atr_pct * ATR_STOP_MULTIPLIER))
    aligned_candle = (
        side == "LONG" and last["close"] > last["open"]
    ) or (
        side == "SHORT" and last["close"] < last["open"]
    )
    aligned_location = (
        side == "LONG" and location >= PAPER_LONG_MIN_CLOSE_LOCATION
    ) or (
        side == "SHORT" and location <= PAPER_SHORT_MAX_CLOSE_LOCATION
    )
    aligned_average = (
        side == "LONG" and last["close"] > ema9 and last["close"] > current_vwap
    ) or (
        side == "SHORT" and last["close"] < ema9 and last["close"] < current_vwap
    )
    btc_aligned = (
        side == "LONG" and btc.get("regime") == "BULL"
    ) or (
        side == "SHORT" and btc.get("regime") == "BEAR"
    )

    return {
        "created_at": now_ts(),
        "cluster_id": now_ts() // INDEPENDENT_CLUSTER_SECONDS,
        "symbol": ticker["symbol"],
        "side": side,
        "last_price": float(last["close"]),
        "quote_volume_24h": float(ticker["quote_volume_24h"]),
        "liquidity_rank": float(ticker.get("liquidity_rank", 0.0)),
        "stop_move": stop_move,
        "features": {
            "directional_3m": d3,
            "directional_15m": d15,
            "directional_30m": abs(directional_return(candles, 30)),
            "signed_3m": ret3,
            "signed_15m": ret15,
            "vol1": vol1,
            "range1": range1,
            "body_fraction": body_fraction,
            "close_location": location,
            "ema9": ema9,
            "vwap20": current_vwap,
            "atr_pct": atr_pct,
            "active_fraction": active_fraction,
            "unique_close_fraction": unique_fraction,
            "aligned_candle": aligned_candle,
            "aligned_location": aligned_location,
            "aligned_average": aligned_average,
            "btc_regime": btc.get("regime", "UNKNOWN"),
            "btc_ret15": float(btc.get("ret15", 0.0) or 0.0),
            "btc_ret60": float(btc.get("ret60", 0.0) or 0.0),
            "btc_aligned": btc_aligned,
        },
    }


def apply_execution_and_paper_gate(
    candidate: Dict[str, Any],
    book: Dict[str, Any],
) -> Dict[str, Any]:
    item = dict(candidate)
    features = dict(item["features"])
    side = item["side"]
    book_ok = bool(book.get("ok"))
    bid = float(book.get("bid", 0.0) or 0.0)
    ask = float(book.get("ask", 0.0) or 0.0)
    fallback = float(item["last_price"])
    entry = ask if side == "LONG" and ask > 0 else bid if side == "SHORT" and bid > 0 else fallback
    item.update(
        {
            "entry_price": entry,
            "entry_bid": bid,
            "entry_ask": ask,
            "spread_bps": float(book.get("spread_bps", 999.0) or 999.0),
            "depth_usdt": float(book.get("depth_usdt", 0.0) or 0.0),
        }
    )
    features.update(
        {
            "book_ok": book_ok,
            "spread_bps": item["spread_bps"],
            "depth_usdt": item["depth_usdt"],
            "executable_entry": entry,
        }
    )
    item["features"] = features

    reasons: List[str] = []
    d3 = float(features["directional_3m"])
    d15 = float(features["directional_15m"])
    vol1 = float(features["vol1"])
    range1 = float(features["range1"])
    if not (PAPER_MIN_DIRECTIONAL_3M <= d3 <= PAPER_MAX_DIRECTIONAL_3M):
        reasons.append("directional_3m")
    if not (PAPER_MIN_DIRECTIONAL_15M <= d15 <= PAPER_MAX_DIRECTIONAL_15M):
        reasons.append("directional_15m")
    if not (PAPER_MIN_VOL1 <= vol1 <= PAPER_MAX_VOL1):
        reasons.append("vol1")
    if not (PAPER_MIN_RANGE1 <= range1 <= PAPER_MAX_RANGE1):
        reasons.append("range1")
    if not bool(features["aligned_candle"]):
        reasons.append("candle_direction")
    if not bool(features["aligned_location"]):
        reasons.append("close_location")
    if not bool(features["aligned_average"]):
        reasons.append("ema_vwap")
    if float(features["body_fraction"]) < PAPER_MIN_BODY_FRACTION:
        reasons.append("body")
    if float(item["quote_volume_24h"]) < PAPER_MIN_24H_QUOTE_VOLUME_USDT:
        reasons.append("quote_volume")
    if not book_ok:
        reasons.append("book")
    if item["spread_bps"] > PAPER_MAX_SPREAD_BPS:
        reasons.append("spread")
    if item["depth_usdt"] < PAPER_MIN_DEPTH_USDT:
        reasons.append("depth")
    if TP3_MOVE / max(float(item["stop_move"]), 1e-12) < 1.25:
        reasons.append("tp3_rr")

    # Fixed, interpretable ranking only chooses the independent representative
    # when several PAPER candidates occur in one market cluster.  It never
    # changes the pass/fail thresholds above.
    quality_score = (
        min(1.0, d3 / 0.015) * 25.0
        + min(1.0, d15 / 0.030) * 20.0
        + min(1.0, vol1 / 1.5) * 15.0
        + min(1.0, range1 / 2.0) * 15.0
        + (10.0 if features["aligned_average"] else 0.0)
        + (5.0 if features["btc_aligned"] else 0.0)
        + min(1.0, float(item["liquidity_rank"])) * 10.0
    )
    item["quality_score"] = round(quality_score, 3)
    item["paper_reject_reason"] = ",".join(reasons)
    item["paper_gate_pass"] = not reasons
    return item


# ---------------------------------------------------------------------------
# Research statistics and review gate
# ---------------------------------------------------------------------------


def metrics(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    count = len(rows)
    outcomes = Counter(str(row.get("outcome", "")) for row in rows)
    pnl = [float(row.get("pnl_r", 0.0) or 0.0) for row in rows]
    wins = [value for value in pnl if value > 0]
    losses = [value for value in pnl if value < 0]
    equity = peak = max_drawdown = 0.0
    for value in pnl:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    expectancy = statistics.fmean(pnl) if pnl else 0.0
    std = statistics.stdev(pnl) if len(pnl) >= 2 else 0.0
    # One-sided 90% normal approximation.  Used only as a conservative review
    # flag after independent clustered PAPER observations, never as a promise.
    expectancy_lcb = expectancy - 1.645 * std / math.sqrt(count) if count else -999.0
    profit_factor = sum(wins) / abs(sum(losses)) if losses else (999.0 if wins else 0.0)
    unique_symbols = len({str(row.get("symbol", "")) for row in rows})
    unique_clusters = len({int(row.get("cluster_id", 0) or 0) for row in rows})
    data_gaps = sum(int(row.get("data_gap_count", 0) or 0) for row in rows)
    return {
        "n": count,
        "profit": outcomes["profit"],
        "sl": outcomes["sl"],
        "expired": outcomes["expired"],
        "tp3_rate": outcomes["profit"] / count if count else 0.0,
        "expectancy_r": expectancy,
        "expectancy_lcb90_r": expectancy_lcb,
        "profit_factor": profit_factor,
        "max_drawdown_r": max_drawdown,
        "unique_symbols": unique_symbols,
        "unique_clusters": unique_clusters,
        "data_gaps": data_gaps,
    }


def wilson_interval(wins: int, total: int, z: float = 1.96) -> Tuple[float, float]:
    if total <= 0:
        return 0.0, 1.0
    p = wins / total
    denominator = 1.0 + z * z / total
    center = (p + z * z / (2.0 * total)) / denominator
    half = z * math.sqrt(p * (1.0 - p) / total + z * z / (4.0 * total * total)) / denominator
    return max(0.0, center - half), min(1.0, center + half)


def review_gate() -> Dict[str, Any]:
    rows = primary_rows("paper", independent_only=True)
    current = metrics(rows)
    recent = metrics(rows[-REVIEW_RECENT_WINDOW:])
    first_block = metrics(rows[-100:-50]) if len(rows) >= 100 else metrics([])
    second_block = metrics(rows[-50:]) if len(rows) >= 50 else metrics([])
    lower, upper = wilson_interval(current["profit"], current["n"])
    checks = {
        "protocol_hash_exact": all(
            str(row.get("protocol_hash", "")) == PROTOCOL_HASH for row in rows
        ),
        "enough_independent_paper": current["n"] >= REVIEW_MIN_INDEPENDENT_PAPER,
        "enough_unique_symbols": current["unique_symbols"] >= REVIEW_MIN_UNIQUE_SYMBOLS,
        "tp3_majority": current["tp3_rate"] > REVIEW_MIN_TP3_RATE,
        "tp3_wilson_lower_above_50": lower > 0.50,
        "expectancy": current["expectancy_r"] >= REVIEW_MIN_EXPECTANCY_R,
        "expectancy_lcb_positive": current["expectancy_lcb90_r"] > 0.0,
        "profit_factor": current["profit_factor"] >= REVIEW_MIN_PROFIT_FACTOR,
        "drawdown": current["max_drawdown_r"] <= REVIEW_MAX_DRAWDOWN_R,
        "recent_complete": recent["n"] >= REVIEW_RECENT_WINDOW,
        "recent_tp3_majority": recent["tp3_rate"] > REVIEW_MIN_TP3_RATE,
        "recent_expectancy_positive": recent["expectancy_r"] > 0.0,
        "two_positive_50_blocks": (
            first_block["n"] == 50
            and second_block["n"] == 50
            and first_block["tp3_rate"] > 0.50
            and second_block["tp3_rate"] > 0.50
            and first_block["expectancy_r"] > 0.0
            and second_block["expectancy_r"] > 0.0
        ),
    }
    passed = bool(checks) and all(checks.values())
    return {
        "research_pass": passed,
        "micro_live_candidate": passed,
        "real_money_enabled": False,
        "note": (
            "Passing only permits a separately reviewed micro-LIVE build; this "
            "research program never places orders or emits real-money entries."
        ),
        "checks": checks,
        "metrics": current,
        "recent": recent,
        "wilson95": {"lower": lower, "upper": upper},
    }


def metric_line(value: Dict[str, Any]) -> str:
    pf = float(value.get("profit_factor", 0.0) or 0.0)
    pf_text = "∞" if pf >= 900 else f"{pf:.2f}"
    return (
        f"{int(value.get('profit', 0))} TP3+ / {int(value.get('sl', 0))} SL / "
        f"{int(value.get('expired', 0))} expired · "
        f"TP3 {float(value.get('tp3_rate', 0.0))*100:.1f}% · "
        f"{float(value.get('expectancy_r', 0.0)):+.3f}R · PF {pf_text} · "
        f"DD {float(value.get('max_drawdown_r', 0.0)):.2f}R"
    )


# ---------------------------------------------------------------------------
# Scan, executable path tracking, and notifications
# ---------------------------------------------------------------------------


def format_price(value: Any) -> str:
    try:
        number = float(value)
    except Exception:
        return "?"
    if number >= 1000:
        return f"{number:.2f}"
    if number >= 1:
        return f"{number:.5f}"
    if number >= 0.01:
        return f"{number:.7f}"
    return f"{number:.10f}".rstrip("0")


def price_target(entry: float, side: str, move: float) -> float:
    return entry * (1.0 + move) if side == "LONG" else entry * (1.0 - move)


def stop_price(entry: float, side: str, move: float) -> float:
    return entry * (1.0 - move) if side == "LONG" else entry * (1.0 + move)


def paper_entry_message(episode: Dict[str, Any]) -> str:
    features = json.loads(episode["features_json"])
    entry = float(episode["entry_price"])
    side = episode["side"]
    return (
        "📋 V18 PAPER-ВХОД — ИССЛЕДОВАНИЕ, НЕ РЕАЛЬНАЯ СДЕЛКА\n"
        f"{side} {episode['symbol']} · качество {float(episode['quality_score']):.1f}\n"
        f"Независимый для проверки: {'ДА' if int(episode['independent']) else 'НЕТ, correlated'}\n"
        f"Когорта: {COHORT_ID} · protocol {PROTOCOL_HASH}\n\n"
        f"Исполнимый вход: {format_price(entry)}\n"
        f"TP1: {format_price(price_target(entry, side, TP1_MOVE))}\n"
        f"TP2: {format_price(price_target(entry, side, TP2_MOVE))}\n"
        f"TP3: {format_price(price_target(entry, side, TP3_MOVE))} · только TP3+ = profit\n"
        f"TP4: {format_price(price_target(entry, side, TP4_MOVE))}\n"
        f"TP5: {format_price(price_target(entry, side, TP5_MOVE))}\n"
        f"SL: {format_price(stop_price(entry, side, float(episode['stop_move'])))} "
        f"({float(episode['stop_move'])*100:.2f}%)\n\n"
        f"3m {float(features['directional_3m'])*100:.2f}% · "
        f"15m {float(features['directional_15m'])*100:.2f}% · "
        f"Vol1 x{float(features['vol1']):.2f} · Range1 x{float(features['range1']):.2f}\n"
        f"Spread {float(episode['spread_bps']):.1f} bps · depth ≈ "
        f"{float(episode['depth_usdt']):.0f} USDT · 24h turnover ≈ "
        f"{float(episode['quote_volume_24h'])/1_000_000:.1f}M USDT\n"
        "Бот покажет основной результат на 6-й минуте и полный путь 1/3/6/10/15/30 минут."
    )


def primary_result_message(episode: Dict[str, Any]) -> str:
    labels = {"profit": "✅ TP3+", "sl": "❌ STOP LOSS", "expired": "⏱ EXPIRED 6M"}
    age = max(0, int(episode.get("primary_closed_at") or now_ts()) - int(episode["created_at"]))
    return (
        f"📊 V18 PAPER РЕЗУЛЬТАТ: {labels.get(str(episode['primary_outcome']), '?')}\n"
        f"{episode['side']} {episode['symbol']} · {age//60}м {age%60}с\n"
        f"Вход: {format_price(episode['entry_price'])} · последняя исполнимая цена: "
        f"{format_price(episode.get('last_exit_price'))}\n"
        f"Итог после комиссий: {float(episode['primary_pnl_r']):+.3f}R\n"
        f"MFE {float(episode['max_favorable_move'])*100:.2f}% · "
        f"MAE {float(episode['max_adverse_move'])*100:.2f}%\n"
        "TP1/TP2 не считаются положительным исходом. Наблюдение продолжится до 30 минут."
    )


def final_path_message(episode: Dict[str, Any]) -> str:
    with DB_LOCK, db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM horizon_results WHERE episode_id=? ORDER BY horizon_seconds",
            (episode["id"],),
        ).fetchall()
    bits = []
    for row in rows:
        minute = int(row["horizon_seconds"]) // 60
        bits.append(
            f"• {minute:>2}м: {row['outcome']} · {float(row['pnl_r']):+.2f}R · "
            f"MFE {float(row['mfe_move'])*100:.2f}% / MAE {float(row['mae_move'])*100:.2f}%"
        )
    return (
        "🔬 V18 ПОЛНЫЙ ПУТЬ PAPER\n"
        f"{episode['side']} {episode['symbol']}\n"
        + ("\n".join(bits) if bits else "нет снимков")
        + "\nЭто исследование горизонта; основной результат остаётся зафиксированным на 6 минутах."
    )


def run_scan() -> Dict[str, Any]:
    if not SCAN_LOCK.acquire(blocking=False):
        return {"skipped": "scan_locked"}
    started = time.time()
    try:
        universe = liquid_universe()
        btc = btc_context()
        candidates: List[Dict[str, Any]] = []
        errors = 0

        def analyze(ticker: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
            try:
                candles = get_klines(ticker["symbol"], 90, cache_seconds=35)
                if not candles:
                    return None, "no_candles"
                broad = build_broad_candidate(ticker, candles, btc)
                if not broad:
                    return None, None
                # Depth is collected for every broad candidate, not only the
                # accepted PAPER subset.  Rejected alternatives therefore have
                # the same execution evidence and can be compared honestly.
                book = get_book(ticker["symbol"], include_depth=True)
                return apply_execution_and_paper_gate(broad, book), None
            except Exception as exc:
                return None, repr(exc)

        if universe:
            with ThreadPoolExecutor(max_workers=min(SCAN_WORKERS, len(universe))) as pool:
                futures = {pool.submit(analyze, ticker): ticker for ticker in universe}
                for future in as_completed(futures):
                    candidate, error = future.result()
                    if error:
                        errors += 1
                    if candidate:
                        candidates.append(candidate)

        candidates.sort(key=lambda item: float(item["quality_score"]), reverse=True)
        created_observer = created_paper = correlated_paper = skipped_cooldown = 0
        reject_reasons: Counter[str] = Counter()
        open_slots = max(0, MAX_OPEN_EPISODES - open_episode_count())
        for candidate in candidates:
            if open_slots <= 0:
                break
            symbol = candidate["symbol"]
            side = candidate["side"]
            tier = "observer"
            if bool(candidate["paper_gate_pass"]):
                if recent_episode_exists(
                    symbol, side, "paper", PAPER_SYMBOL_COOLDOWN_SECONDS
                ):
                    candidate["paper_reject_reason"] = "paper_symbol_cooldown"
                    skipped_cooldown += 1
                else:
                    tier = "paper"
            for reason in str(candidate.get("paper_reject_reason", "")).split(","):
                if reason:
                    reject_reasons[reason] += 1

            if tier == "observer" and recent_episode_exists(
                symbol, side, "observer", BROAD_SYMBOL_COOLDOWN_SECONDS
            ):
                skipped_cooldown += 1
                continue

            candidate["tier"] = tier
            if tier == "paper":
                candidate["independent"] = independent_slot_available(
                    int(candidate["cluster_id"]), side
                )
            else:
                candidate["independent"] = False
            episode = insert_episode(candidate)
            if not episode:
                continue
            open_slots -= 1
            if tier == "paper":
                created_paper += 1
                if not int(episode["independent"]):
                    correlated_paper += 1
                send_text(paper_entry_message(episode), critical=True)
            else:
                created_observer += 1

        result = {
            "at": now_ts(),
            "universe": len(universe),
            "btc": btc,
            "broad_candidates": len(candidates),
            "observer_created": created_observer,
            "paper_created": created_paper,
            "correlated_paper": correlated_paper,
            "cooldown_skipped": skipped_cooldown,
            "open": open_episode_count(),
            "errors": errors,
            "reject_reasons": dict(reject_reasons.most_common(10)),
            "top": [
                {
                    "symbol": item["symbol"],
                    "side": item["side"],
                    "score": item["quality_score"],
                    "paper": item["paper_gate_pass"],
                    "reason": item["paper_reject_reason"],
                }
                for item in candidates[:8]
            ],
            "elapsed": time.time() - started,
        }
        with RUNTIME_LOCK:
            RUNTIME["scan_count"] += 1
            RUNTIME["last_scan"] = result
            RUNTIME["last_scan_at"] = result["at"]
        return result
    finally:
        SCAN_LOCK.release()


def directional_move(episode: Dict[str, Any], exit_price: float) -> float:
    entry = float(episode["entry_price"])
    if episode["side"] == "LONG":
        return (exit_price - entry) / max(entry, 1e-12)
    return (entry - exit_price) / max(entry, 1e-12)


def first_passage_outcome(episode: Dict[str, Any], cutoff: int) -> str:
    tp3 = episode.get("tp3_hit_at")
    sl = episode.get("sl_hit_at")
    tp3_ok = tp3 is not None and int(tp3) <= cutoff
    sl_ok = sl is not None and int(sl) <= cutoff
    if tp3_ok and (not sl_ok or int(tp3) < int(sl)):
        return "profit"
    if sl_ok and (not tp3_ok or int(sl) < int(tp3)):
        return "sl"
    return "expired"


def outcome_pnl_r(
    episode: Dict[str, Any],
    outcome: str,
    net_directional_move: float,
) -> float:
    risk = max(float(episode["stop_move"]), 1e-8)
    if outcome == "profit":
        return (TP3_MOVE - ROUND_TRIP_FEE_MOVE) / risk
    if outcome == "sl":
        return -(risk + ROUND_TRIP_FEE_MOVE) / risk
    return (net_directional_move - ROUND_TRIP_FEE_MOVE) / risk


def track_open_episodes(current_time: Optional[int] = None) -> Dict[str, int]:
    if not TRACK_LOCK.acquire(blocking=False):
        return {"skipped": 1}
    try:
        current = int(current_time or now_ts())
        episodes = get_open_episodes()
        symbols = sorted({row["symbol"] for row in episodes})
        books = get_books(symbols)

        primary_closed = final_closed = gaps = 0
        for original in episodes:
            episode = dict(original)
            book = books.get(episode["symbol"], {"ok": False})
            if not book.get("ok"):
                gaps += 1
                update_episode_path(
                    int(episode["id"]),
                    {"data_gap_count": int(episode.get("data_gap_count", 0) or 0) + 1},
                )
                continue
            exit_price = (
                float(book["bid"])
                if episode["side"] == "LONG"
                else float(book["ask"])
            )
            move = directional_move(episode, exit_price)
            mfe = max(float(episode.get("max_favorable_move", 0.0) or 0.0), move)
            mae = max(float(episode.get("max_adverse_move", 0.0) or 0.0), -move)
            updates: Dict[str, Any] = {
                "max_favorable_move": mfe,
                "max_adverse_move": mae,
                "last_exit_price": exit_price,
                "last_seen_at": current,
            }
            for index, target in enumerate(TARGET_MOVES, 1):
                key = f"tp{index}_hit_at"
                if episode.get(key) is None and move >= target:
                    updates[key] = current
                    episode[key] = current
            if episode.get("sl_hit_at") is None and move <= -float(episode["stop_move"]):
                updates["sl_hit_at"] = current
                episode["sl_hit_at"] = current
            episode.update(updates)

            # A first TP3 or SL fixes the six-minute primary result immediately.
            # Otherwise the exact current executable price is used at 6 minutes.
            if episode.get("primary_outcome") is None:
                early = first_passage_outcome(
                    episode, int(episode["created_at"]) + PRIMARY_HORIZON_SECONDS
                )
                age = current - int(episode["created_at"])
                if early in {"profit", "sl"} or age >= PRIMARY_HORIZON_SECONDS:
                    primary_outcome = early if early in {"profit", "sl"} else "expired"
                    primary_pnl = outcome_pnl_r(episode, primary_outcome, move)
                    updates.update(
                        {
                            "primary_outcome": primary_outcome,
                            "primary_pnl_r": primary_pnl,
                            "primary_closed_at": current,
                        }
                    )
                    episode.update(updates)
                    primary_closed += 1

            update_episode_path(int(episode["id"]), updates)

            # Each horizon is immutable once written.
            age = current - int(episode["created_at"])
            for horizon in HORIZONS_SECONDS:
                if age < horizon or horizon_exists(int(episode["id"]), horizon):
                    continue
                cutoff = int(episode["created_at"]) + horizon
                outcome = first_passage_outcome(episode, cutoff)
                result = {
                    "episode_id": int(episode["id"]),
                    "horizon_seconds": horizon,
                    "observed_at": current,
                    "exit_price": exit_price,
                    "net_move": move - ROUND_TRIP_FEE_MOVE,
                    "mfe_move": mfe,
                    "mae_move": mae,
                    "tp1_hit": int(
                        episode.get("tp1_hit_at") is not None
                        and int(episode["tp1_hit_at"]) <= cutoff
                    ),
                    "tp2_hit": int(
                        episode.get("tp2_hit_at") is not None
                        and int(episode["tp2_hit_at"]) <= cutoff
                    ),
                    "tp3_hit": int(
                        episode.get("tp3_hit_at") is not None
                        and int(episode["tp3_hit_at"]) <= cutoff
                    ),
                    "sl_hit": int(
                        episode.get("sl_hit_at") is not None
                        and int(episode["sl_hit_at"]) <= cutoff
                    ),
                    "outcome": outcome,
                    "pnl_r": outcome_pnl_r(episode, outcome, move),
                }
                insert_horizon_result(result)

            refreshed = get_episode(int(episode["id"])) or episode
            if (
                int(refreshed.get("visible_paper", 0))
                and refreshed.get("primary_outcome")
                and not int(refreshed.get("primary_notified", 0))
            ):
                if send_text(primary_result_message(refreshed), critical=True):
                    update_episode_path(int(refreshed["id"]), {"primary_notified": 1})

            if age >= FINAL_HORIZON_SECONDS:
                update_episode_path(
                    int(episode["id"]),
                    {"status": "closed", "finalized_at": current},
                )
                final_closed += 1
                refreshed = get_episode(int(episode["id"])) or episode
                if int(refreshed.get("visible_paper", 0)) and not int(
                    refreshed.get("final_notified", 0)
                ):
                    if send_text(final_path_message(refreshed), critical=True):
                        update_episode_path(int(refreshed["id"]), {"final_notified": 1})

        with RUNTIME_LOCK:
            RUNTIME["last_track_at"] = current
        if primary_closed:
            maybe_send_milestones()
        return {
            "open": len(episodes),
            "primary_closed": primary_closed,
            "final_closed": final_closed,
            "gaps": gaps,
        }
    finally:
        TRACK_LOCK.release()


# ---------------------------------------------------------------------------
# Reports, diagnostics, and backups
# ---------------------------------------------------------------------------


def build_research_report() -> str:
    all_primary = metrics(primary_rows())
    observer = metrics(primary_rows("observer"))
    paper_all = metrics(primary_rows("paper"))
    paper_independent = metrics(primary_rows("paper", independent_only=True))
    lines = [
        "🧪 V18 — ЧИСТЫЙ FORWARD-ОТЧЁТ",
        f"Когорта: {COHORT_ID} · protocol {PROTOCOL_HASH}",
        "Параметры не менялись автоматически.",
        "",
        f"ВСЕ широкие кандидаты, 6м: {metric_line(all_primary)}",
        f"OBSERVER, 6м: {metric_line(observer)}",
        f"PAPER все, 6м: {metric_line(paper_all)}",
        f"PAPER независимые, 6м: {metric_line(paper_independent)}",
        "",
        "Сравнение горизонтов PAPER независимых:",
    ]
    for horizon in (360, 600, 900, 1800):
        value = metrics(horizon_rows(horizon, "paper", independent_only=True))
        lines.append(f"• {horizon//60}м: {metric_line(value)}")
    gate = review_gate()
    failed = [key for key, value in gate["checks"].items() if not value]
    lines.extend(
        [
            "",
            f"Готовность к отдельному micro-LIVE проекту: {gate['research_pass']}",
            "Не пройдены: " + (", ".join(failed) if failed else "нет"),
            "Реальные деньги в V18 технически отключены независимо от результата.",
        ]
    )
    return "\n".join(lines)


def diagnostics_text() -> str:
    runtime = runtime_snapshot()
    scan = runtime.get("last_scan") or {}
    paper = metrics(primary_rows("paper"))
    independent = metrics(primary_rows("paper", independent_only=True))
    observer = metrics(primary_rows("observer"))
    legacy = meta_get("legacy_audit", {})
    top = scan.get("top") or []
    top_lines = [
        f"• {row['symbol']} {row['side']} · score {row['score']:.1f} · "
        f"PAPER={row['paper']} · {row['reason'] or 'passed'}"
        for row in top[:8]
    ]
    return (
        "🧪 Диагностика V18.0 Clean Forward Research\n"
        f"Protocol: {PROTOCOL_HASH} · scans {runtime.get('scan_count', 0)}\n"
        f"Universe: {scan.get('universe', 0)} · broad найдено {scan.get('broad_candidates', 0)} · "
        f"observer новых {scan.get('observer_created', 0)} · PAPER новых {scan.get('paper_created', 0)} · "
        f"open {scan.get('open', open_episode_count())} · время {scan.get('elapsed', 0):.0f}с\n"
        f"BTC: {(scan.get('btc') or {}).get('regime', 'UNKNOWN')}\n"
        f"OBSERVER закрыто: {metric_line(observer)}\n"
        f"PAPER все: {metric_line(paper)}\n"
        f"PAPER независимые: {metric_line(independent)}\n"
        f"Legacy audit: {int(legacy.get('count', 0))} старых исходов, никогда не обучают V18.\n"
        f"Rejects: {scan.get('reject_reasons', {}) or 'нет'}\n"
        "Top broad:\n"
        + ("\n".join(top_lines) if top_lines else "нет")
        + f"\nAPI calls/errors: {runtime.get('api_calls', 0)}/{runtime.get('api_errors', 0)} · "
        f"Telegram sent/errors: {runtime.get('telegram_sent', 0)}/{runtime.get('telegram_errors', 0)} · "
        f"outbox {table_count('outbox')}\n"
        f"Last error: {runtime.get('last_error', '')}"
    )


def maybe_send_milestones() -> Dict[str, Any]:
    total = primary_count()
    paper = primary_count("paper")
    last_report = int(meta_get("last_report_primary", 0) or 0)
    last_backup = int(meta_get("last_backup_primary", 0) or 0)
    last_checkpoint = int(meta_get("last_paper_checkpoint", 0) or 0)
    report_sent = backup_sent = checkpoint_sent = False

    if total >= last_report + BACKUP_EVERY_PRIMARY:
        report_sent = send_text(build_research_report(), critical=True)
        if report_sent:
            meta_set("last_report_primary", total)

    if total >= last_backup + BACKUP_EVERY_PRIMARY:
        data = export_bytes()
        filename = f"v18_research_backup_{total}_{now_ts()}.json"
        backup_sent = send_document(
            data,
            filename,
            f"V18 full research backup · {total} primary outcomes · protocol {PROTOCOL_HASH}",
            critical=True,
        )
        if backup_sent:
            meta_set("last_backup_primary", total)

    if paper >= last_checkpoint + PAPER_CHECKPOINT_EVERY:
        data = export_bytes()
        filename = f"v18_paper_checkpoint_{paper}_{now_ts()}.json"
        checkpoint_sent = send_document(
            data,
            filename,
            f"V18 PAPER checkpoint · {paper} closed PAPER · protocol {PROTOCOL_HASH}",
            critical=True,
        )
        if checkpoint_sent:
            meta_set("last_paper_checkpoint", paper)
    return {
        "total": total,
        "paper": paper,
        "report_sent": report_sent,
        "backup_sent": backup_sent,
        "checkpoint_sent": checkpoint_sent,
    }


def startup_message() -> str:
    restore = runtime_snapshot().get("seed_restore") or {}
    legacy = meta_get("legacy_audit", {})
    return (
        f"✅ {APP_NAME} активирован.\n"
        f"Deploy marker: {DEPLOY_MARKER}\n"
        f"Protocol hash: {PROTOCOL_HASH}\n\n"
        "Режим: RESEARCH + VISIBLE PAPER ONLY.\n"
        "В коде нет приватных BingX API, создания, изменения или отмены ордеров.\n"
        "Одна неизменяемая гипотеза: liquid momentum continuation.\n"
        f"Universe: top {MAX_UNIVERSE} по 24h turnover, минимум "
        f"{MIN_24H_QUOTE_VOLUME_USDT/1_000_000:.0f}M USDT.\n"
        f"Broad recorder: 3m {BROAD_MIN_DIRECTIONAL_3M*100:.2f}%–{BROAD_MAX_DIRECTIONAL_3M*100:.2f}% · "
        f"15m {BROAD_MIN_DIRECTIONAL_15M*100:.2f}%–{BROAD_MAX_DIRECTIONAL_15M*100:.2f}%.\n"
        f"Visible PAPER: 3m {PAPER_MIN_DIRECTIONAL_3M*100:.2f}%–{PAPER_MAX_DIRECTIONAL_3M*100:.2f}% · "
        f"15m {PAPER_MIN_DIRECTIONAL_15M*100:.2f}%–{PAPER_MAX_DIRECTIONAL_15M*100:.2f}% · "
        f"Vol1 x{PAPER_MIN_VOL1:.2f}–x{PAPER_MAX_VOL1:.2f} · "
        f"Range1 x{PAPER_MIN_RANGE1:.2f}–x{PAPER_MAX_RANGE1:.2f}.\n"
        f"Execution evidence: ASK/BID + spread ≤ {PAPER_MAX_SPREAD_BPS:.0f} bps + "
        f"depth ≥ {PAPER_MIN_DEPTH_USDT:.0f} USDT.\n"
        f"Targets: {TP1_MOVE*100:.2f}% / {TP2_MOVE*100:.2f}% / "
        f"{TP3_MOVE*100:.2f}% / {TP4_MOVE*100:.2f}% / {TP5_MOVE*100:.2f}%.\n"
        "TP1/TP2 промежуточные; только TP3+ считается profit.\n"
        "Каждый кандидат сопровождается 1/3/6/10/15/30 минут; основной исход — 6 минут.\n"
        f"Независимость: одна основная PAPER-сделка на side в каждом "
        f"{INDEPENDENT_CLUSTER_SECONDS//60}-минутном market cluster. Остальные видимы, но correlated.\n"
        f"Review только после ≥{REVIEW_MIN_INDEPENDENT_PAPER} независимых PAPER и ≥"
        f"{REVIEW_MIN_UNIQUE_SYMBOLS} монет; никакой автоматической смены параметров.\n"
        f"JSON: каждые {BACKUP_EVERY_PRIMARY} закрытых кандидатов; PAPER checkpoint каждые "
        f"{PAPER_CHECKPOINT_EVERY}.\n"
        f"Seed: {restore}.\n"
        f"Legacy audit: {int(legacy.get('count', 0))} исходов · "
        f"{float(legacy.get('expectancy_r', 0.0)):+.3f}R · никогда не обучают V18.\n"
        f"V18 restored episodes: {table_count('episodes')} · primary closed {primary_count()}.\n"
        f"Storage: {DB_PATH}. Без Render Disk сохраняйте Telegram JSON перед каждым deploy."
    )


# ---------------------------------------------------------------------------
# Async service and HTTP status endpoints
# ---------------------------------------------------------------------------


async def scan_loop() -> None:
    await asyncio.sleep(3)
    while True:
        try:
            await asyncio.to_thread(run_scan)
        except Exception as exc:
            set_runtime_error(f"scan_loop: {exc!r}")
            send_text(f"⚠️ V18 scan error: {exc!r}", critical=True)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


async def track_loop() -> None:
    await asyncio.sleep(8)
    while True:
        try:
            await asyncio.to_thread(track_open_episodes)
            await asyncio.to_thread(flush_outbox)
        except Exception as exc:
            set_runtime_error(f"track_loop: {exc!r}")
        await asyncio.sleep(TRACK_INTERVAL_SECONDS)


async def diagnostic_loop() -> None:
    await asyncio.sleep(20)
    while True:
        try:
            send_text(diagnostics_text(), critical=False)
            with RUNTIME_LOCK:
                RUNTIME["last_diagnostic_at"] = now_ts()
        except Exception as exc:
            set_runtime_error(f"diagnostic_loop: {exc!r}")
        await asyncio.sleep(DIAGNOSTIC_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    restore_seed_if_empty()
    send_text(startup_message(), critical=True)
    tasks = [
        asyncio.create_task(scan_loop()),
        asyncio.create_task(track_loop()),
        asyncio.create_task(diagnostic_loop()),
    ]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(title=APP_NAME, lifespan=lifespan)


def authorized(key: str) -> bool:
    return bool(ADMIN_KEY) and secrets.compare_digest(str(key), ADMIN_KEY)


@app.get("/")
def root() -> HTMLResponse:
    return HTMLResponse(
        f"<h3>{APP_NAME}</h3><p>{DEPLOY_MARKER}</p>"
        f"<p>Protocol {PROTOCOL_HASH}. Research/PAPER only. No order endpoints.</p>"
    )


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "app": APP_NAME,
        "deploy_marker": DEPLOY_MARKER,
        "protocol_hash": PROTOCOL_HASH,
        "episodes": table_count("episodes"),
        "primary_closed": primary_count(),
        "open": open_episode_count(),
        "last_error": runtime_snapshot().get("last_error", ""),
    }


@app.get("/status")
def status() -> JSONResponse:
    return JSONResponse(
        {
            "runtime": runtime_snapshot(),
            "legacy_audit": meta_get("legacy_audit", {}),
            "observer": metrics(primary_rows("observer")),
            "paper": metrics(primary_rows("paper")),
            "paper_independent": metrics(primary_rows("paper", independent_only=True)),
            "review_gate": review_gate(),
        }
    )


@app.get("/diagnostic")
def diagnostic() -> HTMLResponse:
    return HTMLResponse("<pre>" + diagnostics_text() + "</pre>")


@app.post("/scan")
def manual_scan(key: str = Query("")) -> JSONResponse:
    if not authorized(key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=403)
    return JSONResponse(run_scan())


@app.get("/export")
def export(key: str = Query("")) -> Response:
    if not authorized(key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=403)
    data = export_bytes()
    return Response(
        data,
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=v18_research_{primary_count()}.json"},
    )


@app.post("/telegram-backup")
def telegram_backup(key: str = Query("")) -> JSONResponse:
    if not authorized(key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=403)
    count = primary_count()
    filename = f"v18_manual_backup_{count}_{now_ts()}.json"
    sent = send_document(export_bytes(), filename, "Manual V18 research backup")
    return JSONResponse({"ok": sent, "filename": filename, "primary_count": count})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
