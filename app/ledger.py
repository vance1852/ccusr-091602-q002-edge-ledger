"""遥测可信账本核心。

判定边界（对应 domain_contract.json）：

* accepted          指纹首次出现、序号落入新槽位（含乱序补入仍开放的窗口）
* duplicate         指纹已存在（双网关冗余转发同一采样也归此）
* late              指纹首次出现，但事件时间落入已封存窗口 -> 接受并出修订版
* sequence_conflict 同设备代际+循环+序号槽位已有不同指纹的采样（双网关碰撞）
* quarantined       报文无法解释（缺字段、越界等），原始载荷留证但不参与统计

不变量：
* 已封存窗口的版本不可变；迟到数据只追加更高版本并标注受影响指标。
* 任何归一化记录都带 clock_model_id、config_version 与原始报文 entry_id。
"""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from typing import Any, Callable, Optional

from .clock import ClockRegistry
from .config import ConfigRegistry
from .model import (
    DuplicateOccurrence, EventResult, Gap, LedgerEntry, NormalizedRecord,
    RawMessage, WindowRevision, message_fingerprint,
)

StreamKey = tuple[str, str]


class TelemetryLedger:
    def __init__(self, modulus: int = 1000, window_seconds: float = 60.0,
                 modulus_by_device: Optional[dict[str, int]] = None,
                 clock: Optional[ClockRegistry] = None,
                 configs: Optional[ConfigRegistry] = None,
                 time_fn: Optional[Callable[[], float]] = None):
        self.modulus = modulus
        self.modulus_by_device = modulus_by_device or {}
        self.window_seconds = window_seconds
        self.clocks = clock or ClockRegistry()
        self.configs = configs or ConfigRegistry()
        self._now = time_fn or _default_clock

        # 证据与判定
        self._entries: dict[str, LedgerEntry] = {}
        self._entry_order: list[str] = []
        self._fp_index: dict[str, str] = {}          # fingerprint -> 首见 entry_id
        self._occurrences: dict[str, list[DuplicateOccurrence]] = defaultdict(list)
        self._events: dict[str, NormalizedRecord] = {}
        self._event_entry: dict[str, str] = {}      # event_id -> 首见 entry_id

        # 每流序号状态
        self._streams: dict[StreamKey, dict] = {}
        # 每流缺口（含已填补闭环的）
        self._gaps: dict[StreamKey, list[Gap]] = defaultdict(list)
        # 槽位冲突台账：slot -> [{"winner","loser",...}]，双方原始报文均可追溯
        self._conflicts: dict[tuple, list[dict]] = defaultdict(list)

        # 窗口：开放工作副本 v0 与不可变历史版本 v1..n
        self._open: dict[str, set[str]] = {}
        self._win_meta: dict[str, tuple[float, float, StreamKey]] = {}
        self._history: dict[str, list[WindowRevision]] = defaultdict(list)

        # 网关续传游标
        self._cursors: dict[str, int] = {}

        self._entry_seq = 0

    # ================================================================ 入账
    def ingest(self, m: RawMessage, at: Optional[float] = None) -> LedgerEntry:
        now = self._now() if at is None else at
        return self._ingest(m, now)

    def _ingest(self, m: RawMessage, now: float) -> LedgerEntry:
        fp = message_fingerprint(m)
        key = (m.device_id, m.metric)

        # 1) 指纹幂等边界
        if fp in self._fp_index:
            first_entry_id = self._fp_index[fp]
            first = self._entries[first_entry_id]
            same_gateway = first.raw.gateway_id == m.gateway_id
            reason = ("同一采样经另一网关冗余转发，指纹一致" if not same_gateway
                      else "同一报文被重复投递")
            entry = self._record_entry(
                EventResult.DUPLICATE, reason, False, m, now,
                duplicate_of=first_entry_id,
                annotations=(("redundant-gateway-forward",)
                             if not same_gateway else ("retransmit",)),
            )
            self._occurrences[first_entry_id].append(self._occ(entry))
            self._advance_cursor(m)
            return entry

        # 2) 序号槽位碰撞（指纹不同、同代际循环槽位已占）
        state = self._streams.get(key)
        cyc, linear, relation = self._linearize(m, state)
        slot_owner = self._slot_owner(key, m.gen, cyc, m.seq)
        if slot_owner is not None:
            owner = self._events.get(slot_owner)
            owner_gw = owner.gateway_id if owner else "?"
            entry = self._record_entry(
                EventResult.SEQUENCE_CONFLICT,
                f"序号槽位 g{m.gen}/c{cyc}/#{m.seq} 已被 {slot_owner}"
                f"（网关 {owner_gw}）占用且载荷指纹不同；无法判定真伪，"
                "两份原始报文均留证，槽位保持先到报文，不静默覆盖",
                False, m, now,
                annotations=("dual-gateway-collision",
                             f"slot=g{m.gen}:c{cyc}:{m.seq}",
                             f"owner={slot_owner}"),
            )
            self._conflicts[(key, m.gen, cyc, m.seq)].append({
                "slot": f"g{m.gen}:c{cyc}:{m.seq}",
                "winner_event_id": slot_owner,
                "winner_entry_id": self._event_entry.get(slot_owner),
                "loser_entry_id": entry.entry_id,
                "at": now,
            })
            self._advance_cursor(m)
            return entry

        # 3) 时钟修正 + 按当时配置解释
        decision = self.clocks.model_for(m.device_id, m.device_ts, m.recv_ts)
        interp = self.configs.interpret(m.device_id, m.metric, m.value,
                                        decision.event_time)
        event_id = f"evt:{m.device_id}:{m.metric}:g{m.gen}c{cyc}:{m.seq}"
        record = NormalizedRecord(
            event_id=event_id, device_id=m.device_id, metric=m.metric,
            seq=m.seq, gen=m.gen, cycle=cyc,
            event_time=decision.event_time,
            event_time_quality=decision.quality,
            clock_model_id=decision.model.model_id,
            config_version=interp.config.version,
            raw_value=m.value, value=interp.value, unit=interp.unit,
            location=interp.location, gateway_id=m.gateway_id,
            recv_ts=m.recv_ts, device_ts=m.device_ts, fingerprint=fp,
            annotations=tuple(decision.annotations),
        )

        # 4) 迟到 / 乱序判定（基于事件时间窗口的封存状态）
        win_id = self._window_id(key, record.event_time)
        sealed = bool(self._history.get(win_id))
        out_of_order = relation == "out-of-order"

        if sealed:
            verdict = EventResult.LATE
            reason = (f"事件时间 {record.event_time:.3f} 属于已封存窗口 {win_id}；"
                      "封存版本保持不变，追加生成修订版并标注受影响指标")
            ann = record.annotations + ("late-arrival-into-sealed-window",)
        else:
            verdict = EventResult.ACCEPTED
            if out_of_order:
                reason = (f"接收顺序颠倒（线性序号 {linear} < 已见最大 "
                          f"{state['max_linear']}），但事件窗口仍开放，按事件时间补入")
                ann = record.annotations + ("arrival-out-of-order",)
            else:
                reason = "首次出现的唯一采样"
                ann = record.annotations

        entry = self._record_entry(verdict, reason, True, m, now,
                                   normalized=record, annotations=ann)
        self._fp_index[fp] = entry.entry_id
        self._events[event_id] = record
        self._event_entry[event_id] = entry.entry_id
        self._advance_cursor(m)

        # 5) 序号状态推进与缺口台账
        self._apply_sequence(state, key, m, cyc, linear, record, now)

        # 6) 入窗口：封存窗 -> 修订版；开放窗 -> 工作副本
        start, end = self._bounds(record.event_time)
        self._win_meta.setdefault(win_id, (start, end, key))
        if sealed:
            self._append_revision(win_id, [record], now, entry.reason)
        else:
            self._open.setdefault(win_id, set()).add(event_id)
        return entry

    def quarantine(self, raw: dict[str, Any], reason: str,
                   gateway_id: str = "?", recv_ts: Optional[float] = None,
                   at: Optional[float] = None) -> LedgerEntry:
        """无法解释的报文：原始载荷原样留证，绝不参与统计。"""
        try:
            placeholder = float(raw.get("value", 0.0) or 0.0)
            if not math.isfinite(placeholder):
                placeholder = 0.0
        except (TypeError, ValueError):
            placeholder = 0.0       # 原值无法解析时只占位，证据在 payload
        m = RawMessage(
            device_id=str(raw.get("device_id", "unknown")),
            seq=self._safe_int(raw.get("seq")),
            metric=str(raw.get("metric", "unknown")),
            value=placeholder,
            device_ts=raw.get("device_ts"),
            gateway_id=gateway_id, recv_ts=recv_ts or (at or self._now()),
            gen=self._safe_int(raw.get("gen", 0)),
            source_cursor=raw.get("source_cursor"),
            payload=raw,
        )
        return self._record_entry(EventResult.QUARANTINED, reason, False, m,
                                  at or self._now(),
                                  annotations=("uninterpretable-payload",))

    @staticmethod
    def _safe_int(v) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            return -1

    # ------------------------------------------------------------- 序号逻辑
    def _mod(self, device_id: str) -> int:
        return self.modulus_by_device.get(device_id, self.modulus)

    def _linearize(self, m: RawMessage, state: Optional[dict]):
        """把设备序号展开为服务端线性序号，处理代际切换与序号回绕。

        回绕不能只看序号大小：断网补传时小序号会晚于大序号到达。
        必须同时满足「已见序号越过半程、当前序号落入前半程、设备时间继续向前」
        才认定回绕；设备时间更早（或落在沉淀期）则按同循环内乱序处理。

        返回 (cycle, linear, relation)。
        """
        M = self._mod(m.device_id)
        if not 0 <= m.seq < M:
            raise ValueError(f"seq {m.seq} 超出代际模数 [0,{M})")
        if state is None:
            return 0, m.seq, "first"
        gen_cycles: dict[int, int] = state["gen_cycles"]
        cycle_max: dict[int, int] = state["cycle_max"]
        time_advances = self._time_advances(m, state)
        if m.gen in gen_cycles:
            cyc = gen_cycles[m.gen]
            cmax_cur = cycle_max.get(cyc, -1)
            cmax_prev = cycle_max.get(cyc - 1, -1) if cyc > 0 else -1
            if m.seq < M / 2 and cmax_cur >= M / 2 and time_advances:
                cyc += 1                       # 首次越过回绕点
                gen_cycles[m.gen] = cyc
                cycle_max.setdefault(cyc, m.seq)
            elif m.seq < M / 2 and cmax_prev >= M / 2 and not time_advances:
                cyc -= 1                       # 新循环已开始，小序号是旧循环迟到
            elif m.seq >= M / 2 and not time_advances and cyc > 0:
                cyc -= 1                       # 旧循环后半段缓冲报文迟到
        else:
            # 新代际：在当前最高循环之后另起循环（代际序号空间互不复用）
            cyc = (max(cycle_max) + 1) if cycle_max else 0
            gen_cycles[m.gen] = cyc
        linear = cyc * M + m.seq
        relation = ("out-of-order" if linear < state["max_linear"] else "in-order")
        return cyc, linear, relation

    def _time_advances(self, m: RawMessage, state: dict) -> bool:
        """设备时间是否继续向前；缺设备时间时以接收时间为准（恒为向前）。"""
        frontier = state.get("max_device_ts")
        if m.device_ts is None or frontier is None:
            return True
        # 重同步可能引入小幅阶跃，容差取实测节奏的一半；无节奏估计时不容忍
        cadence = state.get("cadence")
        tol = cadence * 0.5 if cadence else 0.0
        return m.device_ts >= frontier - tol

    def _slot_owner(self, key: StreamKey, gen: int, cyc: int,
                    seq: int) -> Optional[str]:
        state = self._streams.get(key)
        if not state:
            return None
        return state["slots"].get((gen, cyc, seq))

    def _apply_sequence(self, state: Optional[dict], key: StreamKey,
                        m: RawMessage, cyc: int, linear: int,
                        record: NormalizedRecord, now: float) -> None:
        M = self._mod(m.device_id)
        if state is None:
            self._streams[key] = {
                "gen_cycles": {m.gen: cyc}, "cycle_max": {cyc: m.seq},
                "slots": {(m.gen, cyc, m.seq): record.event_id},
                "max_linear": linear,
                "anchor_linear": linear, "anchor_event_time": record.event_time,
                "max_device_ts": m.device_ts,
                "intervals": [], "cadence": None,
            }
            return

        state["slots"][(m.gen, cyc, m.seq)] = record.event_id
        prev_cmax = state["cycle_max"].get(cyc)
        state["cycle_max"][cyc] = max(m.seq, prev_cmax if prev_cmax is not None else -1)
        gc = state["gen_cycles"].get(m.gen)
        if gc is None or cyc > gc:
            state["gen_cycles"][m.gen] = cyc

        if linear > state["max_linear"]:
            # 节奏估计：仅用顺序推进事件的事件时间差（含时钟修正后）
            step = linear - state["max_linear"]
            dt = record.event_time - state["anchor_event_time"]
            if dt > 0 and step > 0:
                state["intervals"].append(dt / step)
                state["intervals"] = state["intervals"][-20:]
                state["cadence"] = statistics.median(state["intervals"])
            if step > 1:
                self._open_gap(key, state["max_linear"], linear, now)
            state["max_linear"] = linear
            state["anchor_linear"] = linear
            state["anchor_event_time"] = record.event_time
            if m.device_ts is not None and (
                    state.get("max_device_ts") is None
                    or m.device_ts > state["max_device_ts"]):
                state["max_device_ts"] = m.device_ts
        else:
            self._fill_gap(key, linear, record, now)

    def _open_gap(self, key: StreamKey, prev_linear: int, new_linear: int,
                  now: float) -> None:
        state = self._streams[key]
        M = self._mod(key[0])
        cadence = state["cadence"] or self.window_seconds
        missing = list(range(prev_linear + 1, new_linear))
        # 跨越循环边界时按循环拆成多个缺口，保持 (cycle, seq) 可读
        groups: dict[int, list[int]] = defaultdict(list)
        for lin in missing:
            groups[lin // M].append(lin)
        first_cyc = missing[0] // M
        for cyc, lins in sorted(groups.items()):
            gap = Gap(
                gap_id=(f"gap:{key[0]}:{key[1]}:c{cyc}:"
                        f"{lins[0] % M}-{lins[-1] % M}"),
                device_id=key[0], metric=key[1], modulus=M,
                after_linear=prev_linear if cyc == first_cyc else cyc * M - 1,
                before_linear=lins[-1] + 1,
                missing=lins, cadence=cadence,
                anchor_event_time=state["anchor_event_time"],
                first_seen=now, updated_at=now,
            )
            self._gaps[key].append(gap)

    def _fill_gap(self, key: StreamKey, linear: int,
                  record: NormalizedRecord, now: float) -> None:
        for gap in self._gaps[key]:
            if linear in gap.missing:
                gap.missing.remove(linear)
                gap.fillers.append((linear, record.event_id, now))
                gap.updated_at = now
                return

    # ------------------------------------------------------------- 窗口/版本
    def _bounds(self, event_time: float) -> tuple[float, float]:
        W = self.window_seconds
        start = (event_time // W) * W
        return start, start + W

    def _window_id(self, key: StreamKey, event_time: float) -> str:
        start, _ = self._bounds(event_time)
        return f"win:{key[0]}:{key[1]}:{int(start)}"

    def _window_records(self, event_ids) -> list[NormalizedRecord]:
        return sorted((self._events[e] for e in event_ids),
                      key=lambda r: (r.event_time, r.gen, r.cycle, r.seq))

    def _aggregate(self, records: list[NormalizedRecord]) -> dict[str, dict]:
        out: dict[str, dict] = {}
        by_metric: dict[str, list[NormalizedRecord]] = defaultdict(list)
        for r in records:
            by_metric[r.metric].append(r)
        for metric, rs in by_metric.items():
            rs.sort(key=lambda r: (r.event_time, r.gen, r.cycle, r.seq))
            values = [r.value for r in rs]
            # 峰值归属按事件时间定序，不按接收顺序 -> 时钟漂移不再颠倒峰值
            peak = max(rs, key=lambda r: (r.value, -r.event_time, -r.cycle, -r.seq))
            out[metric] = {
                "count": len(rs),
                "min": min(values), "max": max(values),
                "mean": sum(values) / len(values),
                "first_event_time": rs[0].event_time,
                "last_event_time": rs[-1].event_time,
                "peak_event_id": peak.event_id,
                "peak_event_time": peak.event_time,
                "peak_value": peak.value,
            }
        return out

    def _snapshot_gaps(self, key: StreamKey, start: float,
                       end: float) -> tuple[dict, ...]:
        snaps = []
        for gap in self._gaps.get(key, ()):
            lo = gap.approx_time_of(gap.missing[0]) if gap.missing else \
                gap.approx_time_of(gap.after_linear + 1)
            hi = (gap.approx_time_of(gap.missing[-1] + 1) if gap.missing
                  else lo)
            if hi >= start - self.window_seconds and lo < end + self.window_seconds:
                snaps.append(gap.snapshot())
        return tuple(snaps)

    def _make_revision(self, win_id: str, version: int, state: str, now: float,
                       parent: Optional[WindowRevision], event_ids: set[str],
                       introduced: list[str], summary: str) -> WindowRevision:
        start, end, key = self._win_meta[win_id]
        records = self._window_records(event_ids)
        aggs = self._aggregate(records)
        affected: list[str] = []
        if parent is not None:
            for metric, agg in aggs.items():
                old = parent.aggregates.get(metric)
                if old is None or any(old.get(k) != agg.get(k)
                                      for k in ("count", "min", "max", "mean",
                                                "peak_event_id")):
                    affected.append(metric)
        return WindowRevision(
            window_id=win_id, version=version, state=state, created_at=now,
            parent_version=parent.version if parent else None,
            window_start=start, window_end=end,
            event_ids=tuple(r.event_id for r in records),
            introduced_event_ids=tuple(introduced),
            affected_metrics=tuple(sorted(affected)),
            change_summary=summary, aggregates=aggs,
            gaps=self._snapshot_gaps(key, start, end),
            clock_model_ids=tuple(sorted({r.clock_model_id for r in records})),
            config_versions=tuple(sorted({r.config_version for r in records})),
        )

    def seal_window(self, win_id: str,
                    at: Optional[float] = None) -> Optional[WindowRevision]:
        """封存窗口：开放工作副本冻结为不可变 v1。重复封存幂等。"""
        now = self._now() if at is None else at
        history = self._history.get(win_id, [])
        if history:
            return history[-1]
        event_ids = self._open.pop(win_id, None)
        if event_ids is None:
            return None
        rev = self._make_revision(
            win_id, 1, "sealed", now, None, event_ids, [],
            f"窗口封存，冻结 {len(event_ids)} 个唯一事件")
        self._history[win_id].append(rev)
        return rev

    def seal_eligible(self, now: Optional[float] = None) -> list[WindowRevision]:
        """封存所有 window_end <= now 的开放窗口。"""
        now = self._now() if now is None else now
        sealed = []
        for win_id in list(self._open):
            _, end, _ = self._win_meta[win_id]
            if end <= now:
                rev = self.seal_window(win_id, at=now)
                if rev:
                    sealed.append(rev)
        return sealed

    def _append_revision(self, win_id: str, new_records: list[NormalizedRecord],
                         now: float, summary: str) -> WindowRevision:
        parent = self._history[win_id][-1]
        event_ids = set(parent.event_ids) | {r.event_id for r in new_records}
        rev = self._make_revision(
            win_id, parent.version + 1, "revised", now, parent, event_ids,
            [r.event_id for r in new_records], summary)
        self._history[win_id].append(rev)
        return rev

    # ------------------------------------------------------------- 游标
    def _advance_cursor(self, m: RawMessage) -> None:
        if m.source_cursor is None:
            return
        cur = self._cursors.get(m.gateway_id)
        if cur is None or m.source_cursor > cur:
            self._cursors[m.gateway_id] = m.source_cursor

    def resume_token(self, gateway_id: str) -> dict[str, Any]:
        """网关重连时查询续传游标与待补缺口。"""
        open_gaps = []
        for (dev, metric), gaps in self._gaps.items():
            for g in gaps:
                if g.status == "open":
                    open_gaps.append({
                        "device_id": dev, "metric": metric,
                        "gap_id": g.gap_id,
                        "missing": [{"cycle": l // g.modulus, "seq": l % g.modulus}
                                    for l in g.missing],
                        "approx_event_time_range": (
                            [g.approx_time_of(g.missing[0]),
                             g.approx_time_of(g.missing[-1] + 1)]),
                    })
        return {
            "gateway_id": gateway_id,
            "last_cursor": self._cursors.get(gateway_id),
            "resume_from_cursor": self._cursors.get(gateway_id),
            "open_gaps": open_gaps,
            "hint": ("从 last_cursor 之后续传；open_gaps 序号区间请优先重传；"
                     "重复采样会按指纹幂等去重，冲突采样会被隔离留证"),
        }

    # ------------------------------------------------------------- 查询
    def get_event(self, event_id: str) -> Optional[NormalizedRecord]:
        return self._events.get(event_id)

    def get_entry(self, entry_id: str) -> Optional[LedgerEntry]:
        return self._entries.get(entry_id)

    def entries(self) -> list[LedgerEntry]:
        return [self._entries[i] for i in self._entry_order]

    def occurrences_of(self, entry_id: str) -> list[DuplicateOccurrence]:
        return list(self._occurrences.get(entry_id, ()))

    def gaps(self, device_id: str, metric: str) -> list[Gap]:
        return list(self._gaps.get((device_id, metric), ()))

    def conflicts(self, device_id: Optional[str] = None) -> list[dict[str, Any]]:
        out = []
        for (key, gen, cyc, seq), records in self._conflicts.items():
            if device_id is not None and key[0] != device_id:
                continue
            for r in records:
                out.append({"device_id": key[0], "metric": key[1],
                            "gen": gen, "cycle": cyc, "seq": seq, **r})
        return out

    def list_windows(self, device_id: str, metric: str) -> list[str]:
        prefix = f"win:{device_id}:{metric}:"
        ids = [w for w in (set(self._open) | set(self._history))
               if w.startswith(prefix)]
        return sorted(ids, key=lambda w: int(w.rsplit(":", 1)[1]))

    def versions(self, win_id: str) -> list[WindowRevision]:
        return list(self._history.get(win_id, ()))

    def lineage(self, win_id: str) -> list[dict[str, Any]]:
        return [{
            "window_id": rev.window_id, "version": rev.version,
            "state": rev.state, "parent_version": rev.parent_version,
            "created_at": rev.created_at,
            "introduced_event_ids": list(rev.introduced_event_ids),
            "affected_metrics": list(rev.affected_metrics),
            "change_summary": rev.change_summary,
            "aggregates": rev.aggregates,
        } for rev in self._history.get(win_id, ())]

    def replay_window(self, win_id: str, version: Optional[int] = None
                      ) -> Optional[dict[str, Any]]:
        """按版本回放窗口：返回该版本冻结的事件集、时间依据、缺口与血缘。"""
        history = self._history.get(win_id)
        if not history:
            open_ids = self._open.get(win_id)
            if open_ids is None:
                return None
            rev = self._make_revision(
                win_id, 0, "open", self._now(), None, set(open_ids), [],
                "开放窗口工作副本（可变，尚未封存）")
            return self._revision_view(rev, include_lineage=False)
        rev = history[-1] if version is None else next(
            (r for r in history if r.version == version), None)
        if rev is None:
            return None
        return self._revision_view(rev, include_lineage=True)

    def replay_events(self, device_id: str, metric: str,
                      t_start: float, t_end: float,
                      version: Optional[int] = None) -> dict[str, Any]:
        """按事件时间回放 [t_start, t_end)。

        version=None：取各窗口最新版本（含开放窗工作副本）。
        version=n：只回放恰好存在 v<n> 的已封存窗口。
        """
        picked: list[NormalizedRecord] = []
        window_refs = []
        for win_id in self.list_windows(device_id, metric):
            history = self._history.get(win_id, [])
            if history:
                rev = history[-1] if version is None else next(
                    (r for r in history if r.version == version), None)
            elif version is None and self._open.get(win_id) is not None:
                rev = self._make_revision(
                    win_id, 0, "open", self._now(), None,
                    set(self._open[win_id]), [], "开放窗口工作副本")
            else:
                rev = None
            if rev is None or rev.window_end <= t_start or rev.window_start >= t_end:
                continue
            window_refs.append({"window_id": win_id, "version": rev.version,
                                "state": rev.state})
            for eid in rev.event_ids:
                r = self._events[eid]
                if t_start <= r.event_time < t_end:
                    picked.append(r)
        picked.sort(key=lambda r: (r.event_time, r.gen, r.cycle, r.seq))
        return {
            "device_id": device_id, "metric": metric,
            "range": [t_start, t_end], "as_version": version,
            "windows": window_refs,
            "events": [self._event_view(r) for r in picked],
        }

    def explain_event(self, event_id: str) -> Optional[dict[str, Any]]:
        """单个事件的完整可信度证据链：判定、原始报文、时钟依据、配置版本。"""
        r = self._events.get(event_id)
        if r is None:
            return None
        entry = self._entries.get(self._event_entry.get(event_id))
        interp = self.configs.interpret(r.device_id, r.metric, r.raw_value,
                                        r.event_time)
        model = self.clocks.get_model(r.clock_model_id)
        conflicts = [c for c in self.conflicts(r.device_id)
                     if c["winner_event_id"] == event_id]
        return {
            **self._event_view(r),
            "clock_model": model.to_dict() if model else None,
            "config": interp.config.to_dict(),
            "verdict": entry.verdict if entry else None,
            "verdict_reason": entry.reason if entry else None,
            "ingest_ts": entry.ingest_ts if entry else None,
            "raw_payload": entry.raw.to_dict() if entry else None,
            "slot_conflicts": conflicts,
            "duplicate_occurrences": [
                o.to_dict() for o in self._occurrences.get(entry.entry_id, ())
            ] if entry else [],
        }

    # ------------------------------------------------------------- 内部工具
    def _event_view(self, r: NormalizedRecord) -> dict[str, Any]:
        d = r.to_dict()
        model = self.clocks.get_model(r.clock_model_id)
        d["clock_basis"] = model.basis if model else None
        return d

    def _revision_view(self, rev: WindowRevision,
                       include_lineage: bool) -> dict[str, Any]:
        view = rev.to_dict()
        view["events"] = [self._event_view(self._events[e])
                          for e in rev.event_ids]
        if include_lineage:
            view["lineage"] = self.lineage(rev.window_id)
        return view

    def _occ(self, entry: LedgerEntry) -> DuplicateOccurrence:
        return DuplicateOccurrence(
            entry_id=entry.entry_id, gateway_id=entry.raw.gateway_id,
            recv_ts=entry.raw.recv_ts, source_cursor=entry.raw.source_cursor)

    def _record_entry(self, verdict: EventResult, reason: str, accepted: bool,
                      m: RawMessage, now: float, *,
                      normalized: Optional[NormalizedRecord] = None,
                      duplicate_of: Optional[str] = None,
                      annotations: tuple[str, ...] = ()) -> LedgerEntry:
        self._entry_seq += 1
        entry = LedgerEntry(
            entry_id=f"entry:{self._entry_seq:06d}", verdict=verdict.value,
            reason=reason, accepted=accepted, raw=m, ingest_ts=now,
            normalized=normalized, duplicate_of=duplicate_of,
            annotations=annotations,
        )
        self._entries[entry.entry_id] = entry
        self._entry_order.append(entry.entry_id)
        return entry


def _default_clock() -> float:
    import time
    return time.time()
