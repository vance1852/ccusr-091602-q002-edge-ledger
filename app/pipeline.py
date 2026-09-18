"""接入管线：幂等判定、时钟修正、缺口跟踪、窗口迟到处理、游标与回放。

每条报文的处理顺序：
1. 原文落账（raw_messages），同一报文重复投递只记一条 raw、多条 delivery；
2. 代际推断（显式 device_epoch 优先，否则按回绕规则推断）；
3. 幂等边界判定：(device, point, epoch, seq) 已入流 → duplicate / sequence_conflict；
4. 校验（点位存在、时间可用、数值合法、非未来时间）→ 失败隔离留证；
5. 时钟观测与事件时间修正（as-of 中位数偏移）；
6. 配置版本解析（按事件时间生效），标定换算 value = raw * scale + offset；
7. 标记乱序 / 补传 / 校准期；
8. 序号 frontier 推进与缺口重算（缺口开闭全程留痕）；
9. 窗口归属：开放窗口 → accepted；已封存或已过迟到视野 → late 并追加修订版。
"""
from __future__ import annotations

import hashlib
import uuid
from typing import Callable, Optional

from .clock import ClockModelManager
from .models import (
    FLAG_BACKFILLED,
    FLAG_CALIBRATION,
    FLAG_OUT_OF_ORDER,
    FUTURE_GUARD_MS,
    WRAP_THRESHOLD,
    EventResult,
    IncomingMessage,
    IngestOutcome,
    WindowState,
    canonical_json,
    raw_message_id,
)
from .store import Store
from . import windows as win


class Pipeline:
    def __init__(
        self,
        store: Store,
        clock: Callable[[], int],
        backfill_threshold_ms: int = 60_000,
    ):
        self.store = store
        self.now_ms = clock
        self.backfill_threshold_ms = backfill_threshold_ms
        self.clocks = ClockModelManager(store)

    # ------------------------------------------------------------------
    # 点位配置
    # ------------------------------------------------------------------

    def register_point_config(self, cfg: dict) -> dict:
        """登记点位配置版本（移机 / 校准参数变更 / 窗口参数均走此入口）。"""
        cfg = dict(cfg)
        cfg.setdefault("registered_at_ms", self.now_ms())
        existing = self.store.list_configs(cfg["point_id"])
        cfg.setdefault("version", len(existing) + 1)
        self.store.put_config(cfg)
        return self.store.latest_config(cfg["point_id"])

    # ------------------------------------------------------------------
    # 报文接入
    # ------------------------------------------------------------------

    def ingest_batch(self, messages: list[IncomingMessage]) -> list[IngestOutcome]:
        return [self.ingest_one(m) for m in messages]

    def ingest_one(self, msg: IncomingMessage) -> IngestOutcome:
        now = self.now_ms()
        with self.store.tx() as conn:
            return self._ingest_in_tx(conn, msg, now)

    def _ingest_in_tx(self, conn, msg: IncomingMessage, now: int) -> IngestOutcome:
        epoch = self._resolve_epoch(conn, msg)
        raw_id = raw_message_id(
            msg.device_id,
            msg.point_id,
            msg.device_seq,
            msg.collected_at_ms,
            msg.gateway_received_at_ms,
            msg.payload,
        )
        payload_json = canonical_json(msg.payload)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

        is_new_raw = self.store.insert_raw_if_new(
            conn,
            {
                "raw_id": raw_id,
                "first_seen_ms": now,
                "device_id": msg.device_id,
                "point_id": msg.point_id,
                "device_epoch": epoch,
                "device_seq": msg.device_seq,
                "collected_at_ms": msg.collected_at_ms,
                "gateway_received_at_ms": msg.gateway_received_at_ms,
                "payload_json": payload_json,
                "payload_hash": payload_hash,
            },
        )

        def finish(result: EventResult, reason: str, event_id=None, ledger_seq=None, flags=None):
            self.store.insert_delivery(
                conn, raw_id, msg.gateway_id, now, result.value, reason
            )
            return IngestOutcome(raw_id, result, reason, event_id, ledger_seq, flags or [])

        # 1) 同一报文的重复投递（含双网关投递同一报文）
        if not is_new_raw:
            existing = self.store.canonical_event(
                conn, msg.device_id, msg.point_id, epoch, msg.device_seq
            )
            return finish(
                EventResult.DUPLICATE,
                "same_message_redelivered",
                event_id=existing["event_id"] if existing else None,
                ledger_seq=existing["ledger_seq"] if existing else None,
            )

        # 2) 幂等边界：同键不同报文
        existing = self.store.canonical_event(
            conn, msg.device_id, msg.point_id, epoch, msg.device_seq
        )
        if existing is not None:
            raw_prev = self.store.get_raw(existing["raw_id"])
            if raw_prev and raw_prev["payload_hash"] == payload_hash:
                return finish(
                    EventResult.DUPLICATE,
                    "same_sequence_same_payload",
                    event_id=existing["event_id"],
                    ledger_seq=existing["ledger_seq"],
                )
            return finish(
                EventResult.SEQUENCE_CONFLICT,
                f"seq {msg.device_seq} epoch {epoch} 已有不同载荷的事件 {existing['event_id']}",
            )

        # 3) 校验：点位必须已登记
        anchor_for_lookup = msg.collected_at_ms
        if anchor_for_lookup is None:
            anchor_for_lookup = msg.gateway_received_at_ms
        if anchor_for_lookup is None:
            anchor_for_lookup = now
        cfg = self.store.config_at(msg.point_id, anchor_for_lookup)
        if cfg is None:
            return finish(EventResult.QUARANTINED, "unknown_point")

        # 4) 时钟观测（修正前先记录本次观测，观测锚点是网关接收时刻）
        if msg.collected_at_ms is not None and msg.gateway_received_at_ms is not None:
            self.clocks.record_observation(
                conn, msg.device_id, msg.collected_at_ms,
                msg.gateway_received_at_ms, raw_id, now,
            )

        # 5) 事件时间解析
        basis = self.clocks.resolve_event_time(
            conn, msg.device_id, msg.collected_at_ms, msg.gateway_received_at_ms
        )
        if basis["event_time_ms"] is None:
            return finish(EventResult.QUARANTINED, "no_usable_timestamp")
        if basis["event_time_ms"] > now + FUTURE_GUARD_MS:
            return finish(EventResult.QUARANTINED, "future_timestamp")
        event_time_ms = basis["event_time_ms"]

        # 6) 数值校验与标定换算（按事件时间生效的配置版本解释）
        cfg = self.store.config_at(msg.point_id, event_time_ms)
        raw_value = msg.payload.get("value")
        if not isinstance(raw_value, (int, float)) or isinstance(raw_value, bool):
            return finish(EventResult.QUARANTINED, "bad_value")
        value = raw_value * cfg["scale"] + cfg["offset"]

        # 7) 标记：乱序 / 补传 / 校准期
        flags = []
        frontier = self._latest_frontier(conn, msg.device_id, msg.point_id)
        if frontier and epoch == frontier["device_epoch"] and msg.device_seq < frontier["last_seen_seq"]:
            flags.append(FLAG_OUT_OF_ORDER)
        max_et = self.store.max_event_time(conn, msg.point_id)
        if max_et is not None and event_time_ms < max_et and FLAG_OUT_OF_ORDER not in flags:
            flags.append(FLAG_OUT_OF_ORDER)
        if now - event_time_ms > self.backfill_threshold_ms:
            flags.append(FLAG_BACKFILLED)
        if self._in_calibration(cfg, event_time_ms):
            flags.append(FLAG_CALIBRATION)

        # 8) 写入规范事件（幂等键唯一约束兜底）
        ledger_seq = self.store.next_ledger_seq(conn)
        event_id = uuid.uuid4().hex
        event = {
            "event_id": event_id,
            "ledger_seq": ledger_seq,
            "device_id": msg.device_id,
            "point_id": msg.point_id,
            "device_epoch": epoch,
            "device_seq": msg.device_seq,
            "event_time_ms": event_time_ms,
            "time_quality": basis["time_quality"],
            "clock_model_version": basis["clock_model_version"],
            "offset_applied_ms": basis["offset_applied_ms"],
            "config_version": cfg["version"],
            "raw_value": raw_value,
            "value": value,
            "result": EventResult.ACCEPTED.value,  # 迟到判定后可能改写
            "flags": flags,
            "raw_id": raw_id,
            "ingested_at_ms": now,
        }

        # 9) 窗口归属与迟到判定
        window = win.ensure_window(
            conn=conn, store=self.store, point_id=msg.point_id,
            event_time_ms=event_time_ms, window_seconds=cfg["window_seconds"],
        )
        lateness_ms = cfg["lateness_seconds"] * 1000
        past_horizon = window["end_ms"] + lateness_ms <= now
        result = EventResult.ACCEPTED
        reason = ""
        if window["state"] != WindowState.OPEN.value:
            # 已封存/已修订：不可改写，追加新修订版
            result = EventResult.LATE
            reason = f"window {window['window_id']} already {window['state']}"
            event["result"] = result.value
            self.store.insert_event(conn, event)
            win.revise_window(self.store, conn, window, event_id, now)
        elif past_horizon:
            # 窗口从未封存但已过迟到视野：先补基线（不含本条），再修订（含本条）
            result = EventResult.LATE
            reason = f"window {window['window_id']} past lateness horizon"
            event["result"] = result.value
            self.store.insert_event(conn, event)
            win.seal_window(self.store, conn, window, now,
                            baseline_exclude=frozenset({event_id}))
            window = self.store.get_window(conn, window["window_id"])
            win.revise_window(self.store, conn, window, event_id, now)
        else:
            self.store.insert_event(conn, event)

        # 10) 序号 frontier 推进与缺口重算
        self._advance_frontier(conn, msg, epoch, now)

        return finish(result, reason, event_id=event_id, ledger_seq=ledger_seq, flags=flags)

    # ------------------------------------------------------------------
    # 代际推断（序号回绕）
    # ------------------------------------------------------------------

    def _resolve_epoch(self, conn, msg: IncomingMessage) -> int:
        if msg.device_epoch is not None:
            return int(msg.device_epoch)
        frontier = self._latest_frontier(conn, msg.device_id, msg.point_id)
        if frontier is None:
            return 0
        latest_epoch = frontier["device_epoch"]
        last_seen = frontier["last_seen_seq"]
        seq = msg.device_seq
        if seq < last_seen - WRAP_THRESHOLD:
            # 序号从接近上限跳回低位：判定为回绕，进入新代际
            return latest_epoch + 1
        if seq > last_seen + WRAP_THRESHOLD and latest_epoch > 0:
            # 序号超出当前代际前沿半个序号空间：不可能是当前代际的前向到达，
            # 判定为回绕前旧代际的迟到/重发报文（重复由幂等键兜底）。
            return latest_epoch - 1
        return latest_epoch

    def _latest_frontier(self, conn, device_id: str, point_id: str) -> Optional[dict]:
        frontiers = self.store.list_frontiers(conn, device_id, point_id)
        return frontiers[-1] if frontiers else None

    # ------------------------------------------------------------------
    # frontier 推进与缺口生命周期
    # ------------------------------------------------------------------

    def _advance_frontier(self, conn, msg: IncomingMessage, epoch: int, now: int) -> None:
        frontier = self.store.get_frontier(conn, msg.device_id, msg.point_id, epoch)
        seq = msg.device_seq
        if frontier is None:
            frontier = {
                "device_id": msg.device_id,
                "point_id": msg.point_id,
                "device_epoch": epoch,
                "first_seq": seq,
                "last_contiguous_seq": seq,
                "last_seen_seq": seq,
                "updated_at_ms": now,
            }
        else:
            if seq > frontier["last_seen_seq"]:
                frontier["last_seen_seq"] = seq
            if seq < frontier["first_seq"]:
                # 代际首个观测并非真实起点（乱序首开）：向下扩展并重算连续水位
                frontier["first_seq"] = seq
                lc = seq
                while self.store.seq_exists(
                    conn, msg.device_id, msg.point_id, epoch, lc + 1
                ):
                    lc += 1
                frontier["last_contiguous_seq"] = lc
            elif seq == frontier["last_contiguous_seq"] + 1:
                lc = seq
                while self.store.seq_exists(
                    conn, msg.device_id, msg.point_id, epoch, lc + 1
                ):
                    lc += 1
                frontier["last_contiguous_seq"] = lc
            frontier["updated_at_ms"] = now
        self.store.upsert_frontier(conn, frontier)
        self._recompute_gaps(conn, msg.device_id, msg.point_id, epoch, frontier, now)

    def _recompute_gaps(self, conn, device_id, point_id, epoch, frontier, now) -> None:
        """由 (last_contiguous, last_seen] 内的缺失序号重算开放缺口并留痕。

        新出现的缺口记 detected_at；不再缺失的缺口记 closed_at —— 断网期间
        开口、补传到达后闭合的完整生命周期可查询。
        """
        lo, hi = frontier["last_contiguous_seq"], frontier["last_seen_seq"]
        expected_open: list[tuple[int, int]] = []
        if hi > lo:
            seen = self.store.seen_seqs(conn, device_id, point_id, epoch, lo, hi)
            missing = [s for s in range(lo + 1, hi + 1) if s not in seen]
            expected_open = _to_ranges(missing)

        stored_open = {
            (g["from_seq"], g["to_seq"]): g
            for g in self.store.open_gaps(conn, device_id, point_id, epoch)
        }
        for rng in expected_open:
            if rng not in stored_open:
                self.store.insert_gap(
                    conn,
                    {
                        "gap_id": uuid.uuid4().hex,
                        "device_id": device_id,
                        "point_id": point_id,
                        "device_epoch": epoch,
                        "from_seq": rng[0],
                        "to_seq": rng[1],
                        "status": "open",
                        "detected_at_ms": now,
                    },
                )
        for rng, gap in stored_open.items():
            if rng not in expected_open:
                self.store.close_gap(conn, gap["gap_id"], now)

    # ------------------------------------------------------------------
    # 查询：游标 / 事件 / 回放 / 血缘
    # ------------------------------------------------------------------

    def resume_cursor(self, device_id: str, point_id: str) -> dict:
        """网关重连续传游标：当前代际、连续水位、已见水位与开放缺口。"""
        with self.store.read() as conn:
            frontiers = self.store.list_frontiers(conn, device_id, point_id)
            if not frontiers:
                return {"device_id": device_id, "point_id": point_id, "known": False}
            latest = frontiers[-1]
            gaps = self.store.open_gaps(conn, device_id, point_id, latest["device_epoch"])
            return {
                "device_id": device_id,
                "point_id": point_id,
                "known": True,
                "device_epoch": latest["device_epoch"],
                "last_contiguous_seq": latest["last_contiguous_seq"],
                "last_seen_seq": latest["last_seen_seq"],
                "open_gaps": [
                    {"from_seq": g["from_seq"], "to_seq": g["to_seq"]} for g in gaps
                ],
                "resume_from_seq": latest["last_seen_seq"] + 1,
                "epochs": [
                    {
                        "device_epoch": f["device_epoch"],
                        "last_contiguous_seq": f["last_contiguous_seq"],
                        "last_seen_seq": f["last_seen_seq"],
                    }
                    for f in frontiers
                ],
            }

    def replay(
        self, point_id: str, from_ms: int, to_ms: int, as_of_seq: Optional[int] = None
    ) -> dict:
        """按事件时间回放指定账本版本：事件流 + 当时可见的窗口修订版。"""
        events = self.store.events_in_range(point_id, from_ms, to_ms, as_of_seq)
        windows = []
        for w in self.store.windows_in_range(point_id, from_ms, to_ms):
            with self.store.read() as conn:
                rev = self.store.latest_revision(conn, w["window_id"], as_of_seq)
            windows.append(
                {
                    "window_id": w["window_id"],
                    "start_ms": w["start_ms"],
                    "end_ms": w["end_ms"],
                    "state": w["state"],
                    "visible_revision": rev,
                }
            )
        return {
            "point_id": point_id,
            "from_ms": from_ms,
            "to_ms": to_ms,
            "as_of_seq": as_of_seq if as_of_seq is not None else self.store.current_ledger_seq(),
            "events": events,
            "windows": windows,
        }

    def lineage(self, point_id: str, window_start_ms: int) -> dict:
        """窗口血缘：修订链 → 触发事件 → 原始报文与投递 → 时钟模型 → 缺口。"""
        window_id = win.window_id_of(point_id, window_start_ms)
        with self.store.read() as conn:
            window = self.store.get_window(conn, window_id)
        if window is None:
            return {"error": f"window {window_id} not found"}
        revisions = self.store.revisions(window_id)
        # 触发事件及其证据链
        caused_events = {}
        for rev in revisions:
            for eid in rev["caused_by"]:
                if eid in caused_events:
                    continue
                ev = self.store.get_event(eid)
                if ev is None:
                    continue
                raw = self.store.get_raw(ev["raw_id"])
                caused_events[eid] = {
                    "event": ev,
                    "raw_message": raw,
                    "deliveries": self.store.deliveries_for(ev["raw_id"]),
                }
        gaps = self.store.gaps_for_point(point_id)
        configs = self.store.list_configs(point_id)
        return {
            "window": window,
            "revisions": revisions,
            "caused_events": caused_events,
            "gaps": gaps,
            "config_history": configs,
        }

    # ------------------------------------------------------------------

    @staticmethod
    def _in_calibration(cfg: dict, event_time_ms: int) -> bool:
        for start, end in cfg.get("calibration_windows", []):
            if start <= event_time_ms < end:
                return True
        return False


def _to_ranges(sorted_nums: list[int]) -> list[tuple[int, int]]:
    """把有序整数列压缩为闭区间列表。"""
    if not sorted_nums:
        return []
    ranges = []
    start = prev = sorted_nums[0]
    for n in sorted_nums[1:]:
        if n == prev + 1:
            prev = n
            continue
        ranges.append((start, prev))
        start = prev = n
    ranges.append((start, prev))
    return ranges
