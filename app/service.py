"""遥测可信服务门面。

面向两类调用方：
* 边缘网关：ingest_packet 投递报文、resume 查询续传游标；
* 数据消费者：replay_window / replay_range 按版本与事件时间回放，
  explain / lineage / gaps / conflicts / audit_trail 调阅证据。
"""
from __future__ import annotations

from typing import Any, Optional

from .model import CalibrationRecord, PointConfig, RawMessage
from .persistence import LedgerStore

_REQUIRED_FIELDS = ("device_id", "seq", "metric", "gateway_id", "recv_ts")


class TrustService:
    def __init__(self, store: LedgerStore):
        self.store = store

    @property
    def ledger(self):
        return self.store.ledger

    # ------------------------------------------------------------ 接入
    def ingest_packet(self, packet: dict[str, Any],
                      at: Optional[float] = None) -> dict[str, Any]:
        """投递一个网关报文。字段不可解释时隔离留证，绝不抛出污染流。"""
        try:
            for f in _REQUIRED_FIELDS:
                if packet.get(f) is None:
                    raise ValueError(f"缺少必填字段 {f}")
            msg = RawMessage.from_dict(packet)
            entry = self.store.ingest(msg, at=at)
        except (TypeError, ValueError, KeyError) as exc:
            entry = self.store.quarantine(
                packet, f"报文校验失败：{exc}",
                gateway_id=str(packet.get("gateway_id", "?")),
                recv_ts=packet.get("recv_ts"), at=at)
        return self.entry_view(entry.entry_id)

    def register_calibration(self, cal: dict[str, Any]) -> None:
        self.store.add_calibration(CalibrationRecord(**cal))

    def register_point(self, cfg: dict[str, Any]) -> None:
        self.store.register_config(PointConfig(**cfg))

    def seal_due(self, now: Optional[float] = None):
        return [r.window_id for r in self.store.seal_eligible(now)]

    def resume(self, gateway_id: str) -> dict[str, Any]:
        return self.ledger.resume_token(gateway_id)

    # ------------------------------------------------------------ 查询
    def entry_view(self, entry_id: str) -> dict[str, Any]:
        e = self.ledger.get_entry(entry_id)
        return e.to_dict() if e else {}

    def audit_trail(self) -> list[dict[str, Any]]:
        """所有入账判定的全量轨迹（含重复、冲突、隔离）。"""
        views = []
        for e in self.ledger.entries():
            d = e.to_dict()
            d["duplicate_occurrences"] = [
                o.to_dict() for o in self.ledger.occurrences_of(e.entry_id)]
            views.append(d)
        return views

    def explain(self, event_id: str) -> Optional[dict[str, Any]]:
        return self.ledger.explain_event(event_id)

    def replay_window(self, win_id: str,
                      version: Optional[int] = None) -> Optional[dict]:
        return self.ledger.replay_window(win_id, version)

    def replay_range(self, device_id: str, metric: str,
                     t_start: float, t_end: float,
                     version: Optional[int] = None) -> dict:
        return self.ledger.replay_events(device_id, metric, t_start, t_end,
                                         version)

    def lineage(self, win_id: str) -> list[dict]:
        return self.ledger.lineage(win_id)

    def gaps(self, device_id: str, metric: str) -> list[dict]:
        return [g.snapshot() for g in self.ledger.gaps(device_id, metric)]

    def conflicts(self, device_id: Optional[str] = None) -> list[dict]:
        return self.ledger.conflicts(device_id)

    def windows(self, device_id: str, metric: str) -> list[str]:
        return self.ledger.list_windows(device_id, metric)
