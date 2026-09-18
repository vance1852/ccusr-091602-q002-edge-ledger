"""生产窗口：分配、封存与迟到修订。

不变式：已封存（sealed）窗口的任何修订版都不可改写；迟到数据只追加
新修订版（revision），并记录受影响指标（affected_metrics）与触发事件
（caused_by），形成可回放的血缘链。
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from .models import WindowState
from .store import Store

METRIC_KEYS = ("count", "min", "max", "avg", "sum", "first_event_ms", "last_event_ms")


def window_id_of(point_id: str, start_ms: int) -> str:
    return f"{point_id}:{start_ms}"


def compute_metrics(
    store: Store,
    conn: sqlite3.Connection,
    point_id: str,
    start_ms: int,
    end_ms: int,
    exclude_event_ids: frozenset = frozenset(),
) -> dict:
    """从规范事件流重算窗口指标（确定性的，可随时重放）。"""
    sql = """SELECT COUNT(*) c, MIN(value) mn, MAX(value) mx, AVG(value) av,
                    SUM(value) sm, MIN(event_time_ms) f, MAX(event_time_ms) l
             FROM events
             WHERE point_id=? AND event_time_ms>=? AND event_time_ms<?"""
    args: list = [point_id, start_ms, end_ms]
    if exclude_event_ids:
        placeholders = ",".join("?" for _ in exclude_event_ids)
        sql += f" AND event_id NOT IN ({placeholders})"
        args.extend(sorted(exclude_event_ids))
    row = conn.execute(sql, args).fetchone()
    count = int(row["c"])
    if count == 0:
        return {k: None for k in METRIC_KEYS} | {"count": 0}
    return {
        "count": count,
        "min": row["mn"],
        "max": row["mx"],
        "avg": round(float(row["av"]), 6),
        "sum": round(float(row["sm"]), 6),
        "first_event_ms": row["f"],
        "last_event_ms": row["l"],
    }


def diff_metrics(old: Optional[dict], new: dict) -> dict:
    """计算受影响指标：{指标: {old, new}}。基线版本返回空。"""
    if old is None:
        return {}
    affected = {}
    for key in METRIC_KEYS:
        if old.get(key) != new.get(key):
            affected[key] = {"old": old.get(key), "new": new.get(key)}
    return affected


def ensure_window(
    store: Store, conn: sqlite3.Connection, point_id: str, event_time_ms: int, window_seconds: int
) -> dict:
    """按事件时间定位（必要时创建）窗口。"""
    size_ms = window_seconds * 1000
    start_ms = (event_time_ms // size_ms) * size_ms
    window_id = window_id_of(point_id, start_ms)
    window = store.get_window(conn, window_id)
    if window is None:
        window = {
            "window_id": window_id,
            "point_id": point_id,
            "start_ms": start_ms,
            "end_ms": start_ms + size_ms,
            "state": WindowState.OPEN.value,
            "current_revision": 0,
        }
        store.insert_window(conn, window)
    return window


def seal_window(
    store: Store,
    conn: sqlite3.Connection,
    window: dict,
    now_ms: int,
    baseline_exclude: frozenset = frozenset(),
) -> dict:
    """封存窗口：生成基线修订版（revision 1）。

    baseline_exclude 用于"迟到触发补封"场景：基线不含触发事件本身，
    随后由 revise_window 生成含该事件的 revision 2，血缘完整。
    """
    metrics = compute_metrics(
        store, conn, window["point_id"], window["start_ms"], window["end_ms"],
        exclude_event_ids=baseline_exclude,
    )
    ledger_seq = store.next_ledger_seq(conn)
    revision = {
        "window_id": window["window_id"],
        "revision_no": 1,
        "ledger_seq": ledger_seq,
        "metrics": metrics,
        "affected_metrics": {},
        "caused_by": [],
        "supersedes": None,
        "created_at_ms": now_ms,
    }
    store.insert_revision(conn, revision)
    store.update_window_state(
        conn, window["window_id"], WindowState.SEALED.value, 1
    )
    return revision


def revise_window(
    store: Store,
    conn: sqlite3.Connection,
    window: dict,
    cause_event_id: str,
    now_ms: int,
) -> dict:
    """为已封存窗口追加新修订版（迟到事件触发）。"""
    latest = store.latest_revision(conn, window["window_id"])
    assert latest is not None, "revise_window 要求窗口已有基线修订版"
    metrics = compute_metrics(
        store, conn, window["point_id"], window["start_ms"], window["end_ms"]
    )
    affected = diff_metrics(latest["metrics"], metrics)
    ledger_seq = store.next_ledger_seq(conn)
    revision = {
        "window_id": window["window_id"],
        "revision_no": latest["revision_no"] + 1,
        "ledger_seq": ledger_seq,
        "metrics": metrics,
        "affected_metrics": affected,
        "caused_by": [cause_event_id],
        "supersedes": latest["revision_no"],
        "created_at_ms": now_ms,
    }
    store.insert_revision(conn, revision)
    store.update_window_state(
        conn, window["window_id"], WindowState.REVISED.value, revision["revision_no"]
    )
    return revision


def seal_due_windows(store: Store, now_ms: int) -> list:
    """封存所有超过迟到视野（end + lateness <= now）的开放窗口。"""
    sealed = []
    with store.tx() as conn:
        for window in store.open_windows(conn):
            cfg = store.latest_config(window["point_id"])
            lateness_ms = (cfg["lateness_seconds"] if cfg else 600) * 1000
            if window["end_ms"] + lateness_ms <= now_ms:
                rev = seal_window(store, conn, window, now_ms)
                sealed.append(
                    {"window_id": window["window_id"], "revision": rev["revision_no"]}
                )
    return sealed
