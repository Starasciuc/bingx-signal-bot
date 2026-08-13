from __future__ import annotations

"""
V18.1 Execution-Accurate Visible Research Recorder
BingX USDT-M perpetual markets.

RESEARCH / PAPER ONLY:
- no authenticated exchange endpoints
- cannot place, modify, or cancel orders
- every newly recorded broad candidate is visible in Telegram
- PAPER is the stricter executable cohort
- OBSERVER is the rejected broad cohort
- only TP3+ is a profitable outcome
- primary legacy result remains 6 minutes
- path is sampled from executable BBO every ~2 seconds
- missed horizons are marked DATA_GAP instead of inventing historical prices

The goal is not to manufacture a high win rate. The goal is to verify whether
a stronger "fresh continuation" gate can produce TP3+ > SL + expired on a
clean forward sample before a separately reviewed micro-LIVE build exists.
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
# Immutable V18.1 protocol
# ---------------------------------------------------------------------------

APP_NAME = "Professional Futures Research Bot V18.1 EXECUTION ACCURATE VISIBLE"
DEPLOY_MARKER = "V18_1_EXECUTION_ACCURATE_VISIBLE_2026_08_13"
EXPORT_SCHEMA = "v18_1_research_export_v1"
COHORT_ID = "V18_1_FRESH_LIQUID_MOMENTUM_FIXED_V1"

TARGET_MOVES: Tuple[float, ...] = (0.0065, 0.0120, 0.0185, 0.0260, 0.0350)
TP1_MOVE, TP2_MOVE, TP3_MOVE, TP4_MOVE, TP5_MOVE = TARGET_MOVES

HORIZONS_SECONDS: Tuple[int, ...] = (60, 180, 360, 600, 900, 1800)
PRIMARY_HORIZON_SECONDS = 360
FINAL_HORIZON_SECONDS = 1800
MAX_HORIZON_LATENESS_SECONDS = 8

# Broad recorder: deliberately permissive. Every created broad episode is
# visible in Telegram, whether it becomes PAPER or OBSERVER.
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
PAPER_MIN_TP3_RR = 1.45

MIN_24H_QUOTE_VOLUME_USDT = 3_000_000.0
MIN_ACTIVE_CANDLE_FRACTION = 0.82
MIN_UNIQUE_CLOSE_FRACTION = 0.35
MAX_UNIVERSE = 100
SCAN_WORKERS = 8
MAX_OPEN_EPISODES = 180

# We still avoid sending the identical symbol/side every scan.
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
REVIEW_MIN_TP3_RATE = 0.52
REVIEW_MIN_EXPECTANCY_R = 0.15
REVIEW_MIN_PROFIT_FACTOR = 1.30
REVIEW_MAX_DRAWDOWN_R = 8.0
REVIEW_RECENT_WINDOW = 50

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
    "fees_round_trip": ROUND_TRIP_FEE_MOVE,
    "assumed_slippage_round_trip": ASSUMED_SLIPPAGE_MOVE,
    "cost_round_trip": ROUND_TRIP_COST_MOVE,
    "stop_bounds": [MIN_STOP_MOVE, MAX_STOP_MOVE],
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
DB_PATH = os.getenv("RESEARCH_DB_PATH", "research_v18_1.sqlite3")
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
# SQLite
# ---------------------------------------------------------------------------

EPISODE_COLUMNS: Tuple[str, ...] = (
    "id", "episode_key", "cohort_id", "protocol_hash", "created_at",
    "cluster_id", "symbol", "side", "tier", "visible_paper", "independent",
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

def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}

def table_count(table: str) -> int:
    if table not in {"episodes", "horizon_results", "outbox"}:
        raise ValueError("unsupported table")
    with DB_LOCK, db_connect() as conn:
        return int(conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])

def primary_count(tier: Optional[str] = None, independent_only: bool = False) -> int:
    q = "SELECT COUNT(*) AS n FROM episodes WHERE cohort_id=? AND primary_outcome IS NOT NULL"
    p: List[Any] = [COHORT_ID]
    if tier:
        q += " AND tier=?"
        p.append(tier)
    if independent_only:
        q += " AND independent=1"
    with DB_LOCK, db_connect() as conn:
        return int(conn.execute(q, p).fetchone()["n"])

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
        return int(conn.execute(
            "SELECT COUNT(*) AS n FROM episodes WHERE cohort_id=? AND status='open'",
            (COHORT_ID,),
        ).fetchone()["n"])

def insert_episode(candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    created = int(candidate["created_at"])
    key = hashlib.sha256(
        f"{COHORT_ID}:{candidate['symbol']}:{candidate['side']}:{candidate['tier']}:{created}:{candidate['entry_price']:.12g}".encode()
    ).hexdigest()
    values = {
        "episode_key": key,
        "cohort_id": COHORT_ID,
        "protocol_hash": PROTOCOL_HASH,
        "created_at": created,
        "cluster_id": int(candidate["cluster_id"]),
        "symbol": candidate["symbol"],
        "side": candidate["side"],
        "tier": candidate["tier"],
        "visible_paper": int(candidate["tier"] == "paper"),
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

def primary_rows(tier: Optional[str]=None, independent_only: bool=False) -> List[Dict[str,Any]]:
    q = (
        "SELECT id AS episode_id,symbol,side,tier,independent,cluster_id,created_at,"
        "primary_outcome AS outcome,primary_pnl_r AS pnl_r,data_gap_count,"
        "path_samples,max_sample_gap_seconds,protocol_hash "
        "FROM episodes WHERE cohort_id=? AND primary_outcome IS NOT NULL"
    )
    p: List[Any] = [COHORT_ID]
    if tier:
        q += " AND tier=?"
        p.append(tier)
    if independent_only:
        q += " AND independent=1"
    q += " ORDER BY created_at,id"
    with DB_LOCK, db_connect() as conn:
        return [row_to_dict(r) for r in conn.execute(q,p).fetchall()]

def horizon_rows(horizon: int, tier: Optional[str]=None, independent_only: bool=False) -> List[Dict[str,Any]]:
    q = (
        "SELECT h.*,e.symbol,e.side,e.tier,e.independent,e.cluster_id,e.created_at,"
        "e.protocol_hash,e.data_gap_count,e.path_samples,e.max_sample_gap_seconds "
        "FROM horizon_results h JOIN episodes e ON e.id=h.episode_id "
        "WHERE e.cohort_id=? AND h.horizon_seconds=?"
    )
    p: List[Any] = [COHORT_ID,horizon]
    if tier:
        q += " AND e.tier=?"
        p.append(tier)
    if independent_only:
        q += " AND e.independent=1"
    q += " ORDER BY e.created_at,e.id"
    with DB_LOCK, db_connect() as conn:
        return [row_to_dict(r) for r in conn.execute(q,p).fetchall()]


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

    # V18.1 restores only the same schema/cohort. Old data is audit-only.
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
        report = {"restored":ie,"horizons":ih,"reason":"v18_1_backup_restored"}
    else:
        # Preserve a compact audit note but do not train/mix old results.
        old_count = len(payload.get("episodes") or payload.get("adaptive_trades") or [])
        meta_set("legacy_audit", {"count":old_count,"policy":"audit_only_never_training"})
        report = {"restored":0,"legacy_audit_count":old_count,"reason":"old_schema_audit_only"}
    RUNTIME["seed_restore"] = report
    return report

def export_payload() -> Dict[str,Any]:
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
        "runtime":runtime_snapshot(),
        "warning":"RESEARCH/PAPER only. Rename newest same-schema backup to adaptive_seed.json before redeploy without persistent disk.",
    }

def export_bytes() -> bytes:
    return json.dumps(export_payload(),ensure_ascii=False,indent=2,allow_nan=False).encode()


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
        "symbol":ticker["symbol"],"side":side,
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

def apply_execution_and_paper_gate(candidate: Dict[str,Any],book: Dict[str,Any])->Dict[str,Any]:
    item=dict(candidate); f=dict(item["features"]); side=item["side"]
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
    d1=float(f["directional_1m"]); d3=float(f["directional_3m"]); d15=float(f["directional_15m"])
    share=float(f["d3_share_of_d15"])
    if not PAPER_MIN_DIRECTIONAL_1M<=d1<=PAPER_MAX_DIRECTIONAL_1M: reasons.append("fresh_1m")
    if not PAPER_MIN_DIRECTIONAL_3M<=d3<=PAPER_MAX_DIRECTIONAL_3M: reasons.append("directional_3m")
    if not PAPER_MIN_DIRECTIONAL_15M<=d15<=PAPER_MAX_DIRECTIONAL_15M: reasons.append("directional_15m")
    if share<PAPER_MIN_ACCELERATION: reasons.append("weak_acceleration")
    if share>PAPER_MAX_D3_SHARE_OF_D15: reasons.append("exhaustion")
    if not PAPER_MIN_VOL1<=float(f["vol1"])<=PAPER_MAX_VOL1: reasons.append("vol1")
    if not PAPER_MIN_RANGE1<=float(f["range1"])<=PAPER_MAX_RANGE1: reasons.append("range1")
    if not f["completed_candle_aligned"]: reasons.append("completed_candle")
    if not f["current_1m_aligned"]: reasons.append("current_1m_reversal")
    if not f["aligned_location"]: reasons.append("close_location")
    if not f["aligned_average"]: reasons.append("ema_vwap")
    if float(f["body_fraction"])<PAPER_MIN_BODY_FRACTION: reasons.append("body")
    if float(f["vwap_distance"])>PAPER_MAX_VWAP_DISTANCE: reasons.append("chase")
    if f["btc_opposite"]: reasons.append("btc_opposite")
    if float(item["quote_volume_24h"])<PAPER_MIN_24H_QUOTE_VOLUME_USDT: reasons.append("quote_volume")
    if not ok: reasons.append("book")
    if item["spread_bps"]>PAPER_MAX_SPREAD_BPS: reasons.append("spread")
    if item["depth_usdt"]<PAPER_MIN_DEPTH_USDT: reasons.append("depth")
    if TP3_MOVE/max(float(item["stop_move"]),1e-12)<PAPER_MIN_TP3_RR: reasons.append("tp3_rr")

    # Ranking is fixed and interpretable, not adaptive.
    score=(
        min(1,d1/.005)*15 +
        min(1,d3/.015)*20 +
        min(1,d15/.03)*15 +
        min(1,float(f["vol1"])/1.5)*12 +
        min(1,float(f["range1"])/2.0)*10 +
        (8 if f["current_1m_aligned"] else 0) +
        (8 if f["aligned_average"] else 0) +
        (6 if not f["btc_opposite"] else 0) +
        min(1,float(item["liquidity_rank"]))*6
    )
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
    return {
        "n":n,
        "raw_n":len(rows),
        "profit":outcomes["profit"],"sl":outcomes["sl"],"expired":outcomes["expired"],
        "data_gap":outcomes["data_gap"],
        "tp3_rate":outcomes["profit"]/n if n else 0,
        "expectancy_r":expectancy,"expectancy_lcb90_r":lcb,
        "profit_factor":pf,"max_drawdown_r":dd,
        "unique_symbols":len({str(r.get("symbol","")) for r in valid}),
        "unique_clusters":len({int(r.get("cluster_id",0) or 0) for r in valid}),
        "max_sample_gap_seconds":max([int(r.get("max_sample_gap_seconds",0) or 0) for r in valid] or [0]),
    }

def wilson_interval(wins: int,total: int,z: float=1.96)->Tuple[float,float]:
    if total<=0:return 0,1
    p=wins/total; den=1+z*z/total
    center=(p+z*z/(2*total))/den
    half=z*math.sqrt(p*(1-p)/total+z*z/(4*total*total))/den
    return max(0,center-half),min(1,center+half)

def review_gate()->Dict[str,Any]:
    rows=primary_rows("paper",True)
    current=metrics(rows); recent=metrics(rows[-REVIEW_RECENT_WINDOW:])
    lower,upper=wilson_interval(current["profit"],current["n"])
    checks={
        "protocol_hash_exact":all(str(r.get("protocol_hash",""))==PROTOCOL_HASH for r in rows),
        "enough_independent_paper":current["n"]>=REVIEW_MIN_INDEPENDENT_PAPER,
        "enough_unique_symbols":current["unique_symbols"]>=REVIEW_MIN_UNIQUE_SYMBOLS,
        "enough_unique_clusters":current["unique_clusters"]>=REVIEW_MIN_UNIQUE_CLUSTERS,
        "tp3_gt_sl_plus_expired":current["profit"]>(current["sl"]+current["expired"]),
        "tp3_rate":current["tp3_rate"]>=REVIEW_MIN_TP3_RATE,
        "tp3_wilson_lower_above_50":lower>0.50,
        "expectancy":current["expectancy_r"]>=REVIEW_MIN_EXPECTANCY_R,
        "expectancy_lcb_positive":current["expectancy_lcb90_r"]>0,
        "profit_factor":current["profit_factor"]>=REVIEW_MIN_PROFIT_FACTOR,
        "drawdown":current["max_drawdown_r"]<=REVIEW_MAX_DRAWDOWN_R,
        "recent_complete":recent["n"]>=REVIEW_RECENT_WINDOW,
        "recent_tp3_gt_rest":recent["profit"]>(recent["sl"]+recent["expired"]),
        "recent_expectancy_positive":recent["expectancy_r"]>0,
        "path_sampling_ok":current["max_sample_gap_seconds"]<=8,
        "no_data_gaps_in_primary":current["data_gap"]==0,
    }
    passed=bool(checks) and all(checks.values())
    return {
        "research_pass":passed,
        "micro_live_candidate":passed,
        "real_money_enabled":False,
        "checks":checks,"metrics":current,"recent":recent,
        "wilson95":{"lower":lower,"upper":upper},
        "note":"Passing only authorizes a separate reviewed micro-LIVE build. This program never places orders."
    }

def metric_line(m: Dict[str,Any])->str:
    pf=float(m.get("profit_factor",0) or 0)
    return (
        f"{int(m.get('profit',0))} TP3+ / {int(m.get('sl',0))} SL / "
        f"{int(m.get('expired',0))} expired / {int(m.get('data_gap',0))} data-gap · "
        f"TP3 {float(m.get('tp3_rate',0))*100:.1f}% · "
        f"{float(m.get('expectancy_r',0)):+.3f}R · PF {'∞' if pf>=900 else f'{pf:.2f}'} · "
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
    return (
        f"{icon} V18.1 {title}\n"
        f"{side} {e['symbol']} · score {float(e['quality_score']):.1f}\n"
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
        f"1m {float(f['directional_1m'])*100:.2f}% · "
        f"3m {float(f['directional_3m'])*100:.2f}% · "
        f"15m {float(f['directional_15m'])*100:.2f}% · "
        f"Vol1 x{float(f['vol1']):.2f} · Range1 x{float(f['range1']):.2f}\n"
        f"Spread {float(e['spread_bps']):.1f} bps · depth ≈ {float(e['depth_usdt']):.0f} USDT\n"
        "Все созданные кандидаты видимы. Основной результат — 6 минут."
    )

def result_message(e: Dict[str,Any])->str:
    labels={"profit":"✅ TP3+","sl":"❌ STOP LOSS","expired":"⏱ EXPIRED 6M","data_gap":"⚠️ DATA GAP"}
    return (
        f"📊 V18.1 {e['tier'].upper()} РЕЗУЛЬТАТ: {labels.get(str(e['primary_outcome']),'?')}\n"
        f"{e['side']} {e['symbol']}\n"
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
    return (
        f"🔬 V18.1 ПОЛНЫЙ ПУТЬ {e['tier'].upper()}\n{e['side']} {e['symbol']}\n"
        +("\n".join(bits) if bits else "нет снимков")
        +"\n6-минутный основной исход не переписывается."
    )


# ---------------------------------------------------------------------------
# Scan and execution path
# ---------------------------------------------------------------------------

def run_scan()->Dict[str,Any]:
    if not SCAN_LOCK.acquire(blocking=False):return {"skipped":"scan_locked"}
    started=time.time()
    try:
        universe=liquid_universe(); btc=btc_context(); candidates=[]; errors=0
        def analyze(ticker):
            try:
                c=get_klines(ticker["symbol"],90,35)
                if not c:return None,None
                broad=build_broad_candidate(ticker,c,btc)
                if not broad:return None,None
                book=get_book(ticker["symbol"],True)
                return apply_execution_and_paper_gate(broad,book),None
            except Exception as exc:return None,repr(exc)
        if universe:
            with ThreadPoolExecutor(max_workers=min(SCAN_WORKERS,len(universe))) as pool:
                fs={pool.submit(analyze,t):t for t in universe}
                for f in as_completed(fs):
                    c,err=f.result()
                    if err:errors+=1
                    if c:candidates.append(c)
        candidates.sort(key=lambda x:float(x["quality_score"]),reverse=True)

        paper=observer=correlated=skipped=0
        rejects=Counter(); slots=max(0,MAX_OPEN_EPISODES-open_episode_count())
        for c in candidates:
            if slots<=0:break
            symbol,side=c["symbol"],c["side"]
            tier="paper" if c["paper_gate_pass"] else "observer"

            if tier=="paper" and recent_episode_exists(symbol,side,"paper",PAPER_SYMBOL_COOLDOWN_SECONDS):
                # The broad candidate is STILL visible: downgrade duplicate PAPER to observer.
                tier="observer"
                c["paper_reject_reason"]=(c["paper_reject_reason"]+",paper_cooldown").strip(",")
            if tier=="observer" and recent_episode_exists(symbol,side,"observer",OBSERVER_SYMBOL_COOLDOWN_SECONDS):
                skipped+=1; continue

            c["tier"]=tier
            c["independent"]=independent_slot_available(int(c["cluster_id"]),side) if tier=="paper" else False
            for reason in str(c.get("paper_reject_reason","")).split(","):
                if reason:rejects[reason]+=1
            e=insert_episode(c)
            if not e:continue
            slots-=1
            if tier=="paper":
                paper+=1
                if not int(e["independent"]):correlated+=1
            else: observer+=1

            # User requirement: every newly recorded candidate is visible.
            if send_text(entry_message(e),critical=True):
                update_episode(int(e["id"]),{"entry_notified":1})

        result={
            "at":now_ts(),"universe":len(universe),"btc":btc,
            "broad_candidates":len(candidates),"observer_created":observer,
            "paper_created":paper,"correlated_paper":correlated,
            "cooldown_skipped":skipped,"open":open_episode_count(),
            "errors":errors,"reject_reasons":dict(rejects.most_common(12)),
            "top":[{"symbol":x["symbol"],"side":x["side"],"score":x["quality_score"],
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
                if early in {"profit","sl"} or age>=PRIMARY_HORIZON_SECONDS:
                    # If path had a large sampling hole before primary close, do not
                    # pretend the 6m outcome is trustworthy.
                    if int(e.get("max_sample_gap_seconds",0) or 0)>8:
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
            # User requirement: results of BOTH PAPER and OBSERVER are visible.
            if refreshed.get("primary_outcome") and not int(refreshed.get("primary_notified",0)):
                if send_text(result_message(refreshed),critical=True):
                    update_episode(int(refreshed["id"]),{"primary_notified":1})

            if age>=FINAL_HORIZON_SECONDS:
                update_episode(int(e["id"]),{"status":"closed","finalized_at":current})
                fclosed+=1
                refreshed=get_episode(int(e["id"])) or e
                if not int(refreshed.get("final_notified",0)):
                    if send_text(final_message(refreshed),critical=True):
                        update_episode(int(refreshed["id"]),{"final_notified":1})

        RUNTIME["last_track_at"]=current
        if pclosed:maybe_send_milestones()
        return {"open":len(episodes),"primary_closed":pclosed,"final_closed":fclosed,"gaps":gaps}
    finally:
        TRACK_LOCK.release()


# ---------------------------------------------------------------------------
# Reports/backups
# ---------------------------------------------------------------------------

def build_research_report()->str:
    allm=metrics(primary_rows()); obs=metrics(primary_rows("observer"))
    pap=metrics(primary_rows("paper")); ind=metrics(primary_rows("paper",True))
    lines=[
        "🧪 V18.1 — EXECUTION-ACCURATE FORWARD ОТЧЁТ",
        f"Protocol {PROTOCOL_HASH}",
        "Все созданные кандидаты видимы; PAPER и OBSERVER считаются отдельно.",
        "",
        f"ВСЕ: {metric_line(allm)}",
        f"OBSERVER: {metric_line(obs)}",
        f"PAPER все: {metric_line(pap)}",
        f"PAPER независимые: {metric_line(ind)}",
        "",
        "Горизонты PAPER независимых:"
    ]
    for h in (360,600,900,1800):
        lines.append(f"• {h//60}м: {metric_line(metrics(horizon_rows(h,'paper',True)))}")
    gate=review_gate(); failed=[k for k,v in gate["checks"].items() if not v]
    lines += [
        "",
        f"TP3+ > SL+expired: {ind['profit']}>{ind['sl']+ind['expired']} = {ind['profit']>ind['sl']+ind['expired']}",
        f"Готовность к отдельному micro-LIVE: {gate['research_pass']}",
        "Не пройдены: "+(", ".join(failed) if failed else "нет"),
        "V18.1 реальные деньги технически не использует."
    ]
    return "\n".join(lines)

def diagnostics_text()->str:
    rt=runtime_snapshot();scan=rt.get("last_scan") or {}
    return (
        "🧪 V18.1 Execution Accurate Visible\n"
        f"Protocol {PROTOCOL_HASH} · scans {rt.get('scan_count',0)}\n"
        f"Universe {scan.get('universe',0)} · broad {scan.get('broad_candidates',0)} · "
        f"PAPER new {scan.get('paper_created',0)} · OBSERVER new {scan.get('observer_created',0)} · "
        f"open {scan.get('open',open_episode_count())} · {scan.get('elapsed',0):.0f}s\n"
        f"OBSERVER: {metric_line(metrics(primary_rows('observer')))}\n"
        f"PAPER: {metric_line(metrics(primary_rows('paper')))}\n"
        f"PAPER independent: {metric_line(metrics(primary_rows('paper',True)))}\n"
        f"Rejects: {scan.get('reject_reasons',{}) or 'нет'}\n"
        f"API {rt.get('api_calls',0)}/{rt.get('api_errors',0)} · "
        f"Telegram {rt.get('telegram_sent',0)}/{rt.get('telegram_errors',0)} · outbox {table_count('outbox')}\n"
        f"Last error: {rt.get('last_error','')}"
    )

def maybe_send_milestones()->Dict[str,Any]:
    total=primary_count();paper=primary_count("paper")
    lr=int(meta_get("last_report_primary",0) or 0)
    lb=int(meta_get("last_backup_primary",0) or 0)
    lp=int(meta_get("last_paper_checkpoint",0) or 0)
    rs=bs=ps=False
    if total>=lr+BACKUP_EVERY_PRIMARY:
        rs=send_text(build_research_report(),True)
        if rs:meta_set("last_report_primary",total)
    if total>=lb+BACKUP_EVERY_PRIMARY:
        data=export_bytes();fn=f"v18_1_backup_{total}_{now_ts()}.json"
        bs=send_document(data,fn,f"V18.1 full backup · {total} outcomes · {PROTOCOL_HASH}",True)
        if bs:meta_set("last_backup_primary",total)
    if paper>=lp+PAPER_CHECKPOINT_EVERY:
        data=export_bytes();fn=f"v18_1_paper_{paper}_{now_ts()}.json"
        ps=send_document(data,fn,f"V18.1 PAPER checkpoint · {paper} closed PAPER",True)
        if ps:meta_set("last_paper_checkpoint",paper)
    return {"total":total,"paper":paper,"report_sent":rs,"backup_sent":bs,"checkpoint_sent":ps}

def startup_message()->str:
    return (
        f"✅ {APP_NAME} активирован.\n"
        f"Deploy marker: {DEPLOY_MARKER}\nProtocol: {PROTOCOL_HASH}\n\n"
        "Режим: RESEARCH + PAPER/OBSERVER ONLY. Ордеров BingX в коде нет.\n"
        "ВСЕ созданные broad-кандидаты и их 6m результаты видимы в Telegram.\n"
        "PAPER усилен против expired: свежий 1m impulse + 3m acceleration + closed-candle continuation + "
        "EMA/VWAP + anti-chase + BTC opposite filter + execution liquidity.\n"
        f"Tracking executable BBO: каждые ~{TRACK_INTERVAL_SECONDS}s.\n"
        f"Costs in readiness: fee {ROUND_TRIP_FEE_MOVE*100:.2f}% + assumed slippage "
        f"{ASSUMED_SLIPPAGE_MOVE*100:.2f}% = {ROUND_TRIP_COST_MOVE*100:.2f}% round trip.\n"
        f"PAPER liquidity: turnover ≥ {PAPER_MIN_24H_QUOTE_VOLUME_USDT/1e6:.0f}M, "
        f"spread ≤ {PAPER_MAX_SPREAD_BPS:.0f}bps, depth ≥ {PAPER_MIN_DEPTH_USDT:.0f} USDT.\n"
        f"TP: 0.65/1.20/1.85/2.60/3.50%; only TP3+ = profit. SL {MIN_STOP_MOVE*100:.2f}%–{MAX_STOP_MOVE*100:.2f}%.\n"
        f"Micro-LIVE review only after ≥{REVIEW_MIN_INDEPENDENT_PAPER} independent PAPER, "
        f"TP3+ > SL+expired, TP3 ≥ {REVIEW_MIN_TP3_RATE*100:.0f}%, expectancy ≥ {REVIEW_MIN_EXPECTANCY_R:.2f}R, "
        f"PF ≥ {REVIEW_MIN_PROFIT_FACTOR:.2f}.\n"
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
            send_text(f"⚠️ V18.1 scan error: {exc!r}",True)
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
                         "review_gate":review_gate()})

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
                    headers={"Content-Disposition":f"attachment; filename=v18_1_research_{primary_count()}.json"})

@app.post("/telegram-backup")
def telegram_backup(key: str=Query(""))->JSONResponse:
    if not authorized(key):return JSONResponse({"ok":False,"error":"unauthorized"},status_code=403)
    count=primary_count();fn=f"v18_1_manual_{count}_{now_ts()}.json"
    sent=send_document(export_bytes(),fn,"Manual V18.1 research backup")
    return JSONResponse({"ok":sent,"filename":fn,"primary_count":count})

if __name__=="__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0",port=PORT,log_level="info")
