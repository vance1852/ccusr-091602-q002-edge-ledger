"""SQLite 持久化层：可信账本的所有表与事务助手。

设计要点：
- raw_messages / deliveries 是只增不改的证据层；同一报文多次投递产生多条 delivery。
- events 是规范事件流，(device_id, point_id, device_epoch, device_seq) 唯一 —— 幂等边界。
- window_revisions 只追加：已封存窗口的任何变化都生成新修订版，旧版本不可改写。
- ledger 表提供全局单调序号，支撑"按版本回放"。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from typing import Any, Iterator, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS point_configs (
  point_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  effective_from_ms INTEGER NOT NULL,
  unit TEXT,
  scale REAL NOT NULL DEFAULT 1.0,
  offset REAL NOT NULL DEFAULT 0.0,
  location TEXT,
  window_seconds INTEGER NOT NULL DEFAULT 3600,
  lateness_seconds INTEGER NOT NULL DEFAULT 600,
  calibration_windows_json TEXT NOT NULL DEFAULT '[]',
  registered_at_ms INTEGER NOT NULL,
  PRIMARY KEY (point_id, version)
);

CREATE TABLE IF NOT EXISTS raw_messages (
  raw_id TEXT PRIMARY KEY,
  first_seen_ms INTEGER NOT NULL,
  device_id TEXT NOT NULL,
  point_id TEXT NOT NULL,
  device_epoch INTEGER NOT NULL,
  device_seq INTEGER NOT NULL,
  collected_at_ms INTEGER,
  gateway_received_at_ms INTEGER,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
  delivery_id TEXT PRIMARY KEY,
  raw_id TEXT NOT NULL REFERENCES raw_messages(raw_id),
  gateway_id TEXT NOT NULL,
  arrived_at_ms INTEGER NOT NULL,
  result TEXT NOT NULL,
  reason TEXT
);
CREATE INDEX IF NOT EXISTS deliveries_raw ON deliveries(raw_id);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  ledger_seq INTEGER NOT NULL,
  device_id TEXT NOT NULL,
  point_id TEXT NOT NULL,
  device_epoch INTEGER NOT NULL,
  device_seq INTEGER NOT NULL,
  event_time_ms INTEGER NOT NULL,
  time_quality TEXT NOT NULL,
  clock_model_version INTEGER,
  offset_applied_ms REAL NOT NULL DEFAULT 0,
  config_version INTEGER NOT NULL,
  raw_value REAL,
  value REAL,
  result TEXT NOT NULL,
  flags_json TEXT NOT NULL DEFAULT '[]',
  raw_id TEXT NOT NULL REFERENCES raw_messages(raw_id),
  ingested_at_ms INTEGER NOT NULL,
  UNIQUE (device_id, point_id, device_epoch, device_seq)
);
CREATE INDEX IF NOT EXISTS events_point_time ON events(point_id, event_time_ms);

CREATE TABLE IF NOT EXISTS clock_observations (
  device_id TEXT NOT NULL,
  observed_at_ms INTEGER NOT NULL,   -- 网关接收时刻（观测锚点）
  collected_at_ms INTEGER NOT NULL,
  offset_ms REAL NOT NULL,           -- 网关时钟 - 设备时钟
  raw_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS clock_obs_dev ON clock_observations(device_id, observed_at_ms);

CREATE TABLE IF NOT EXISTS clock_models (
  device_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  offset_ms REAL NOT NULL,
  sample_count INTEGER NOT NULL,
  method TEXT NOT NULL,
  PRIMARY KEY (device_id, version)
);

CREATE TABLE IF NOT EXISTS stream_frontiers (
  device_id TEXT NOT NULL,
  point_id TEXT NOT NULL,
  device_epoch INTEGER NOT NULL,
  first_seq INTEGER NOT NULL,
  last_contiguous_seq INTEGER NOT NULL,
  last_seen_seq INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (device_id, point_id, device_epoch)
);

CREATE TABLE IF NOT EXISTS gaps (
  gap_id TEXT PRIMARY KEY,
  device_id TEXT NOT NULL,
  point_id TEXT NOT NULL,
  device_epoch INTEGER NOT NULL,
  from_seq INTEGER NOT NULL,
  to_seq INTEGER NOT NULL,
  status TEXT NOT NULL,              -- open | closed
  detected_at_ms INTEGER NOT NULL,
  closed_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS gaps_point ON gaps(point_id, device_epoch, status);

CREATE TABLE IF NOT EXISTS windows (
  window_id TEXT PRIMARY KEY,        -- point_id:start_ms
  point_id TEXT NOT NULL,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER NOT NULL,
  state TEXT NOT NULL,               -- open | sealed | revised
  current_revision INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS windows_point ON windows(point_id, start_ms);

CREATE TABLE IF NOT EXISTS window_revisions (
  window_id TEXT NOT NULL,
  revision_no INTEGER NOT NULL,
  ledger_seq INTEGER NOT NULL,
  metrics_json TEXT NOT NULL,
  affected_metrics_json TEXT NOT NULL DEFAULT '{}',
  caused_by_json TEXT NOT NULL DEFAULT '[]',
  supersedes INTEGER,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (window_id, revision_no)
);
"""


class Store:
    """线程安全的 SQLite 账本。所有写操作在 tx() 事务内完成。"""

    def __init__(self, path: str = ":memory:"):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.execute(
                "INSERT OR IGNORE INTO ledger(id, seq) VALUES (1, 0)"
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """写事务：BEGIN IMMEDIATE 保证并发下序号与唯一约束一致。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            yield self._conn

    # ---------- 全局账本序号 ----------

    @staticmethod
    def next_ledger_seq(conn: sqlite3.Connection) -> int:
        conn.execute("UPDATE ledger SET seq = seq + 1 WHERE id = 1")
        row = conn.execute("SELECT seq FROM ledger WHERE id = 1").fetchone()
        return int(row["seq"])

    def current_ledger_seq(self) -> int:
        with self.read() as conn:
            return int(conn.execute("SELECT seq FROM ledger WHERE id = 1").fetchone()["seq"])

    # ---------- 点位配置 ----------

    def put_config(self, cfg: dict) -> None:
        with self.tx() as conn:
            conn.execute(
                """INSERT INTO point_configs
                   (point_id, version, effective_from_ms, unit, scale, offset,
                    location, window_seconds, lateness_seconds,
                    calibration_windows_json, registered_at_ms)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    cfg["point_id"],
                    cfg["version"],
                    cfg["effective_from_ms"],
                    cfg.get("unit"),
                    cfg.get("scale", 1.0),
                    cfg.get("offset", 0.0),
                    cfg.get("location"),
                    cfg.get("window_seconds", 3600),
                    cfg.get("lateness_seconds", 600),
                    json.dumps(cfg.get("calibration_windows", [])),
                    cfg["registered_at_ms"],
                ),
            )

    def config_at(self, point_id: str, event_time_ms: int) -> Optional[dict]:
        """取事件时间生效的配置版本；事件早于所有版本时回退到最早版本。"""
        with self.read() as conn:
            row = conn.execute(
                """SELECT * FROM point_configs
                   WHERE point_id = ? AND effective_from_ms <= ?
                   ORDER BY effective_from_ms DESC, version DESC LIMIT 1""",
                (point_id, event_time_ms),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    """SELECT * FROM point_configs WHERE point_id = ?
                       ORDER BY effective_from_ms ASC, version ASC LIMIT 1""",
                    (point_id,),
                ).fetchone()
            return _config_row(row) if row else None

    def latest_config(self, point_id: str) -> Optional[dict]:
        with self.read() as conn:
            row = conn.execute(
                """SELECT * FROM point_configs WHERE point_id = ?
                   ORDER BY effective_from_ms DESC, version DESC LIMIT 1""",
                (point_id,),
            ).fetchone()
            return _config_row(row) if row else None

    def list_configs(self, point_id: str) -> list:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT * FROM point_configs WHERE point_id = ? ORDER BY version",
                (point_id,),
            ).fetchall()
            return [_config_row(r) for r in rows]

    # ---------- 原始报文与投递 ----------

    def insert_raw_if_new(self, conn: sqlite3.Connection, raw: dict) -> bool:
        """写入原始报文；已存在则忽略。返回是否为新报文。"""
        cur = conn.execute(
            """INSERT OR IGNORE INTO raw_messages
               (raw_id, first_seen_ms, device_id, point_id, device_epoch,
                device_seq, collected_at_ms, gateway_received_at_ms,
                payload_json, payload_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                raw["raw_id"],
                raw["first_seen_ms"],
                raw["device_id"],
                raw["point_id"],
                raw["device_epoch"],
                raw["device_seq"],
                raw["collected_at_ms"],
                raw["gateway_received_at_ms"],
                raw["payload_json"],
                raw["payload_hash"],
            ),
        )
        return cur.rowcount > 0

    def get_raw(self, raw_id: str) -> Optional[dict]:
        with self.read() as conn:
            row = conn.execute(
                "SELECT * FROM raw_messages WHERE raw_id = ?", (raw_id,)
            ).fetchone()
            return dict(row) if row else None

    def insert_delivery(
        self,
        conn: sqlite3.Connection,
        raw_id: str,
        gateway_id: str,
        arrived_at_ms: int,
        result: str,
        reason: str,
    ) -> str:
        delivery_id = uuid.uuid4().hex
        conn.execute(
            """INSERT INTO deliveries
               (delivery_id, raw_id, gateway_id, arrived_at_ms, result, reason)
               VALUES (?,?,?,?,?,?)""",
            (delivery_id, raw_id, gateway_id, arrived_at_ms, result, reason),
        )
        return delivery_id

    def deliveries_for(self, raw_id: str) -> list:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT * FROM deliveries WHERE raw_id = ? ORDER BY arrived_at_ms",
                (raw_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def count_deliveries(self) -> int:
        with self.read() as conn:
            return int(conn.execute("SELECT COUNT(*) c FROM deliveries").fetchone()["c"])

    # ---------- 规范事件流 ----------

    def canonical_event(
        self, conn: sqlite3.Connection, device_id: str, point_id: str, epoch: int, seq: int
    ) -> Optional[dict]:
        row = conn.execute(
            """SELECT * FROM events WHERE device_id=? AND point_id=?
               AND device_epoch=? AND device_seq=?""",
            (device_id, point_id, epoch, seq),
        ).fetchone()
        return dict(row) if row else None

    def insert_event(self, conn: sqlite3.Connection, ev: dict) -> None:
        conn.execute(
            """INSERT INTO events
               (event_id, ledger_seq, device_id, point_id, device_epoch, device_seq,
                event_time_ms, time_quality, clock_model_version, offset_applied_ms,
                config_version, raw_value, value, result, flags_json, raw_id, ingested_at_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                ev["event_id"],
                ev["ledger_seq"],
                ev["device_id"],
                ev["point_id"],
                ev["device_epoch"],
                ev["device_seq"],
                ev["event_time_ms"],
                ev["time_quality"],
                ev["clock_model_version"],
                ev["offset_applied_ms"],
                ev["config_version"],
                ev["raw_value"],
                ev["value"],
                ev["result"],
                json.dumps(ev.get("flags", [])),
                ev["raw_id"],
                ev["ingested_at_ms"],
            ),
        )

    def get_event(self, event_id: str) -> Optional[dict]:
        with self.read() as conn:
            row = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            return _event_row(row) if row else None

    def events_in_range(
        self,
        point_id: str,
        from_ms: int,
        to_ms: int,
        as_of_seq: Optional[int] = None,
    ) -> list:
        sql = """SELECT * FROM events WHERE point_id=?
                 AND event_time_ms>=? AND event_time_ms<?"""
        args: list = [point_id, from_ms, to_ms]
        if as_of_seq is not None:
            sql += " AND ledger_seq<=?"
            args.append(as_of_seq)
        sql += " ORDER BY event_time_ms, ledger_seq"
        with self.read() as conn:
            return [_event_row(r) for r in conn.execute(sql, args).fetchall()]

    def max_event_time(self, conn: sqlite3.Connection, point_id: str) -> Optional[int]:
        row = conn.execute(
            "SELECT MAX(event_time_ms) m FROM events WHERE point_id=?", (point_id,)
        ).fetchone()
        return row["m"] if row and row["m"] is not None else None

    def seq_exists(
        self,
        conn: sqlite3.Connection,
        device_id: str,
        point_id: str,
        epoch: int,
        seq: int,
    ) -> bool:
        row = conn.execute(
            """SELECT 1 FROM events WHERE device_id=? AND point_id=?
               AND device_epoch=? AND device_seq=?""",
            (device_id, point_id, epoch, seq),
        ).fetchone()
        return row is not None

    def seen_seqs(
        self,
        conn: sqlite3.Connection,
        device_id: str,
        point_id: str,
        epoch: int,
        lo: int,
        hi: int,
    ) -> set:
        rows = conn.execute(
            """SELECT device_seq FROM events WHERE device_id=? AND point_id=?
               AND device_epoch=? AND device_seq>? AND device_seq<=?""",
            (device_id, point_id, epoch, lo, hi),
        ).fetchall()
        return {int(r["device_seq"]) for r in rows}

    def count_events(self) -> int:
        with self.read() as conn:
            return int(conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"])

    # ---------- 时钟模型 ----------

    def insert_observation(
        self,
        conn: sqlite3.Connection,
        device_id: str,
        observed_at_ms: int,
        collected_at_ms: int,
        offset_ms: float,
        raw_id: str,
    ) -> None:
        conn.execute(
            """INSERT INTO clock_observations
               (device_id, observed_at_ms, collected_at_ms, offset_ms, raw_id)
               VALUES (?,?,?,?,?)""",
            (device_id, observed_at_ms, collected_at_ms, offset_ms, raw_id),
        )

    def observations_upto(
        self, conn: sqlite3.Connection, device_id: str, at_ms: int, limit: int = 32
    ) -> list:
        """采集时刻之前（含）的最近 limit 条观测，用于 as-of 修正。"""
        rows = conn.execute(
            """SELECT offset_ms FROM clock_observations
               WHERE device_id=? AND observed_at_ms<=?
               ORDER BY observed_at_ms DESC LIMIT ?""",
            (device_id, at_ms, limit),
        ).fetchall()
        return [float(r["offset_ms"]) for r in rows]

    def current_model(self, conn: sqlite3.Connection, device_id: str) -> Optional[dict]:
        row = conn.execute(
            """SELECT * FROM clock_models WHERE device_id=?
               ORDER BY version DESC LIMIT 1""",
            (device_id,),
        ).fetchone()
        return dict(row) if row else None

    def insert_model(self, conn: sqlite3.Connection, model: dict) -> None:
        conn.execute(
            """INSERT INTO clock_models
               (device_id, version, created_at_ms, offset_ms, sample_count, method)
               VALUES (?,?,?,?,?,?)""",
            (
                model["device_id"],
                model["version"],
                model["created_at_ms"],
                model["offset_ms"],
                model["sample_count"],
                model["method"],
            ),
        )

    # ---------- 序号 frontier 与缺口 ----------

    def get_frontier(
        self, conn: sqlite3.Connection, device_id: str, point_id: str, epoch: int
    ) -> Optional[dict]:
        row = conn.execute(
            """SELECT * FROM stream_frontiers
               WHERE device_id=? AND point_id=? AND device_epoch=?""",
            (device_id, point_id, epoch),
        ).fetchone()
        return dict(row) if row else None

    def list_frontiers(
        self, conn: sqlite3.Connection, device_id: str, point_id: str
    ) -> list:
        rows = conn.execute(
            """SELECT * FROM stream_frontiers WHERE device_id=? AND point_id=?
               ORDER BY device_epoch""",
            (device_id, point_id),
        ).fetchall()
        return [dict(r) for r in rows]

    def upsert_frontier(self, conn: sqlite3.Connection, frontier: dict) -> None:
        conn.execute(
            """INSERT INTO stream_frontiers
               (device_id, point_id, device_epoch, first_seq,
                last_contiguous_seq, last_seen_seq, updated_at_ms)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(device_id, point_id, device_epoch) DO UPDATE SET
                 last_contiguous_seq=excluded.last_contiguous_seq,
                 last_seen_seq=excluded.last_seen_seq,
                 updated_at_ms=excluded.updated_at_ms""",
            (
                frontier["device_id"],
                frontier["point_id"],
                frontier["device_epoch"],
                frontier["first_seq"],
                frontier["last_contiguous_seq"],
                frontier["last_seen_seq"],
                frontier["updated_at_ms"],
            ),
        )

    def open_gaps(
        self, conn: sqlite3.Connection, device_id: str, point_id: str, epoch: int
    ) -> list:
        rows = conn.execute(
            """SELECT * FROM gaps WHERE device_id=? AND point_id=?
               AND device_epoch=? AND status='open' ORDER BY from_seq""",
            (device_id, point_id, epoch),
        ).fetchall()
        return [dict(r) for r in rows]

    def insert_gap(self, conn: sqlite3.Connection, gap: dict) -> None:
        conn.execute(
            """INSERT INTO gaps
               (gap_id, device_id, point_id, device_epoch, from_seq, to_seq,
                status, detected_at_ms, closed_at_ms)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                gap["gap_id"],
                gap["device_id"],
                gap["point_id"],
                gap["device_epoch"],
                gap["from_seq"],
                gap["to_seq"],
                gap["status"],
                gap["detected_at_ms"],
                gap.get("closed_at_ms"),
            ),
        )

    def close_gap(self, conn: sqlite3.Connection, gap_id: str, closed_at_ms: int) -> None:
        conn.execute(
            "UPDATE gaps SET status='closed', closed_at_ms=? WHERE gap_id=?",
            (closed_at_ms, gap_id),
        )

    def gaps_for_point(self, point_id: str) -> list:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT * FROM gaps WHERE point_id=? ORDER BY device_epoch, from_seq",
                (point_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    # ---------- 生产窗口与修订版 ----------

    def get_window(self, conn: sqlite3.Connection, window_id: str) -> Optional[dict]:
        row = conn.execute(
            "SELECT * FROM windows WHERE window_id=?", (window_id,)
        ).fetchone()
        return dict(row) if row else None

    def insert_window(self, conn: sqlite3.Connection, window: dict) -> None:
        conn.execute(
            """INSERT INTO windows (window_id, point_id, start_ms, end_ms, state,
                                    current_revision)
               VALUES (?,?,?,?,?,?)""",
            (
                window["window_id"],
                window["point_id"],
                window["start_ms"],
                window["end_ms"],
                window["state"],
                window["current_revision"],
            ),
        )

    def update_window_state(
        self, conn: sqlite3.Connection, window_id: str, state: str, revision: int
    ) -> None:
        conn.execute(
            "UPDATE windows SET state=?, current_revision=? WHERE window_id=?",
            (state, revision, window_id),
        )

    def open_windows(self, conn: sqlite3.Connection) -> list:
        rows = conn.execute(
            "SELECT * FROM windows WHERE state='open' ORDER BY end_ms"
        ).fetchall()
        return [dict(r) for r in rows]

    def windows_in_range(self, point_id: str, from_ms: int, to_ms: int) -> list:
        with self.read() as conn:
            rows = conn.execute(
                """SELECT * FROM windows WHERE point_id=? AND end_ms>? AND start_ms<?
                   ORDER BY start_ms""",
                (point_id, from_ms, to_ms),
            ).fetchall()
            return [dict(r) for r in rows]

    def insert_revision(self, conn: sqlite3.Connection, rev: dict) -> None:
        conn.execute(
            """INSERT INTO window_revisions
               (window_id, revision_no, ledger_seq, metrics_json,
                affected_metrics_json, caused_by_json, supersedes, created_at_ms)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                rev["window_id"],
                rev["revision_no"],
                rev["ledger_seq"],
                json.dumps(rev["metrics"], sort_keys=True),
                json.dumps(rev.get("affected_metrics", {}), sort_keys=True),
                json.dumps(rev.get("caused_by", [])),
                rev.get("supersedes"),
                rev["created_at_ms"],
            ),
        )

    def revisions(self, window_id: str, as_of_seq: Optional[int] = None) -> list:
        sql = "SELECT * FROM window_revisions WHERE window_id=?"
        args: list = [window_id]
        if as_of_seq is not None:
            sql += " AND ledger_seq<=?"
            args.append(as_of_seq)
        sql += " ORDER BY revision_no"
        with self.read() as conn:
            return [_revision_row(r) for r in conn.execute(sql, args).fetchall()]

    def latest_revision(
        self, conn: sqlite3.Connection, window_id: str, as_of_seq: Optional[int] = None
    ) -> Optional[dict]:
        sql = "SELECT * FROM window_revisions WHERE window_id=?"
        args: list = [window_id]
        if as_of_seq is not None:
            sql += " AND ledger_seq<=?"
            args.append(as_of_seq)
        sql += " ORDER BY revision_no DESC LIMIT 1"
        row = conn.execute(sql, args).fetchone()
        return _revision_row(row) if row else None


def _config_row(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["calibration_windows"] = json.loads(d.pop("calibration_windows_json") or "[]")
    return d


def _event_row(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["flags"] = json.loads(d.pop("flags_json") or "[]")
    return d


def _revision_row(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["metrics"] = json.loads(d.pop("metrics_json"))
    d["affected_metrics"] = json.loads(d.pop("affected_metrics_json"))
    d["caused_by"] = json.loads(d.pop("caused_by_json"))
    return d
