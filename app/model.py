"""领域值对象。枚举取值严格对应 domain_contract.json。"""
from __future__ import annotations

import enum
import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Any, Optional


class EventResult(str, enum.Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    LATE = "late"
    SEQUENCE_CONFLICT = "sequence_conflict"
    QUARANTINED = "quarantined"


class TimeQuality(str, enum.Enum):
    DEVICE = "device"
    CORRECTED = "corrected"
    ESTIMATED = "estimated"
    UNKNOWN = "unknown"


class WindowState(str, enum.Enum):
    OPEN = "open"
    SEALED = "sealed"
    REVISED = "revised"


# ---------------------------------------------------------------- 原始报文

@dataclass(frozen=True)
class RawMessage:
    """设备侧报文的不可变快照。归一化永远不覆盖它。"""
    device_id: str
    seq: int
    metric: str
    value: float
    device_ts: Optional[float]          # 设备时钟读数（秒），可能缺失
    gateway_id: str
    recv_ts: float                      # 网关/服务端接收时间（秒）
    gen: int = 0                        # 设备代际，序号按代际回绕
    source_cursor: Optional[int] = None # 网关侧投递游标，用于续传
    payload: dict[str, Any] = field(default_factory=dict)  # 原始载荷逐字保留

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RawMessage":
        value = float(d["value"])
        if not math.isfinite(value):
            raise ValueError(f"value 必须是有限数值，收到 {d['value']!r}")
        device_ts = d.get("device_ts")
        if device_ts is not None and not math.isfinite(float(device_ts)):
            raise ValueError("device_ts 必须是有限数值")
        recv_ts = float(d["recv_ts"])
        if not math.isfinite(recv_ts):
            raise ValueError("recv_ts 必须是有限数值")
        return cls(
            device_id=d["device_id"], seq=int(d["seq"]), metric=d["metric"],
            value=value, device_ts=device_ts,
            gateway_id=d["gateway_id"], recv_ts=recv_ts,
            gen=int(d.get("gen", 0)), source_cursor=d.get("source_cursor"),
            payload=d.get("payload", {}),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "device_id": self.device_id, "seq": self.seq, "metric": self.metric,
            "value": self.value, "device_ts": self.device_ts,
            "gateway_id": self.gateway_id, "recv_ts": self.recv_ts,
            "gen": self.gen, "source_cursor": self.source_cursor,
            "payload": self.payload,
        }


def message_fingerprint(m: RawMessage) -> str:
    """同一设备代际+序号+载荷内容的指纹。

    刻意不含 gateway_id：双网关冗余转发同一采样应判为重复，
    只有载荷不同（同序号被两方声称不同内容）才算碰撞。
    """
    basis = [
        m.device_id, m.gen, m.seq, m.metric, round(float(m.value), 9),
        None if m.device_ts is None else round(m.device_ts, 6),
    ]
    blob = json.dumps(basis, separators=(",", ":"), ensure_ascii=False)
    return "fp-" + hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------- 时钟模型

@dataclass(frozen=True)
class ClockModel:
    """一次归一化所采用的时钟模型快照。

    校正公式：event_time = device_ts + applied_offset(device_ts)
    applied_offset(t) = offset_seconds - drift_ppm*1e-6*(t - ref_device_ts)
    （offset_seconds 为校时点 srv-device；drift_ppm 为设备频偏，快为正）
    """
    model_id: str
    source: str                 # uncalibrated | interpolation | extrapolation | server-clock
    offset_seconds: float
    drift_ppm: float
    ref_device_ts: Optional[float]
    basis: str                  # 人类可读的时间修正依据
    created_at: float

    def applied_offset(self, device_ts: float) -> float:
        if self.ref_device_ts is None:
            return self.offset_seconds
        return self.offset_seconds - self.drift_ppm * 1e-6 * (device_ts - self.ref_device_ts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id, "source": self.source,
            "offset_seconds": self.offset_seconds, "drift_ppm": self.drift_ppm,
            "ref_device_ts": self.ref_device_ts, "basis": self.basis,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ClockModel":
        return cls(**d)


@dataclass(frozen=True)
class CalibrationRecord:
    """校时记录。at_device 缺失表示设备在 at_server 被直接对到服务器时钟。"""
    calib_id: str
    device_id: str
    at_server: float
    at_device: Optional[float]
    offset_ms: float            # at_server - at_device
    drift_ppm: float            # 校时后采用的频偏
    source: str                 # ntp | field-console | device-resync
    recv_ts: float
    note: str = ""

    @property
    def offset_seconds(self) -> float:
        return self.offset_ms / 1000.0

    @property
    def anchor_device(self) -> float:
        if self.at_device is not None:
            return self.at_device
        return self.at_server - self.offset_seconds

    def to_dict(self) -> dict[str, Any]:
        return {
            "calib_id": self.calib_id, "device_id": self.device_id,
            "at_server": self.at_server, "at_device": self.at_device,
            "offset_ms": self.offset_ms, "drift_ppm": self.drift_ppm,
            "source": self.source, "recv_ts": self.recv_ts, "note": self.note,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "CalibrationRecord":
        return cls(**d)


# ---------------------------------------------------------------- 测点配置

@dataclass(frozen=True)
class PointConfig:
    """测点解释配置（量程换算、单位、安装位置），按事件时间生效。"""
    device_id: str
    metric: str
    version: str
    effective_event_time: float
    scale: float
    offset: float
    unit: str
    location: str
    note: str
    registered_at: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "device_id": self.device_id, "metric": self.metric, "version": self.version,
            "effective_event_time": self.effective_event_time,
            "scale": self.scale, "offset": self.offset, "unit": self.unit,
            "location": self.location, "note": self.note,
            "registered_at": self.registered_at,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PointConfig":
        return cls(**d)


# ---------------------------------------------------------------- 归一化记录

@dataclass(frozen=True)
class NormalizedRecord:
    event_id: str
    device_id: str
    metric: str
    seq: int
    gen: int
    cycle: int                  # 服务端跟踪的序号循环（代际切换/回绕递增）
    event_time: float           # 校正后事件时间
    event_time_quality: str     # TimeQuality 值
    clock_model_id: str
    config_version: str
    raw_value: float
    value: float                # 按当时配置解释后的工程量
    unit: str
    location: str
    gateway_id: str
    recv_ts: float
    device_ts: Optional[float]
    fingerprint: str
    annotations: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id, "device_id": self.device_id, "metric": self.metric,
            "seq": self.seq, "gen": self.gen, "cycle": self.cycle,
            "event_time": self.event_time, "event_time_quality": self.event_time_quality,
            "clock_model_id": self.clock_model_id, "config_version": self.config_version,
            "raw_value": self.raw_value, "value": self.value, "unit": self.unit,
            "location": self.location, "gateway_id": self.gateway_id,
            "recv_ts": self.recv_ts, "device_ts": self.device_ts,
            "fingerprint": self.fingerprint, "annotations": list(self.annotations),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "NormalizedRecord":
        d = dict(d)
        d["annotations"] = tuple(d.get("annotations", ()))
        return cls(**d)


# ---------------------------------------------------------------- 缺口

@dataclass
class Gap:
    gap_id: str
    device_id: str
    metric: str
    modulus: int
    after_linear: int
    before_linear: int
    missing: list[int]                    # 缺失序号的线性值
    cadence: float
    anchor_event_time: float             # after_linear 对应事件的事件时间
    first_seen: float
    updated_at: float
    fillers: list[tuple[int, str, float]] = field(default_factory=list)

    @property
    def status(self) -> str:
        return "resolved" if not self.missing else "open"

    def approx_time_of(self, linear: int) -> float:
        return self.anchor_event_time + (linear - self.after_linear) * self.cadence

    def _split(self, linear: int) -> tuple[int, int]:
        return linear // self.modulus, linear % self.modulus

    def snapshot(self) -> dict[str, Any]:
        missing_cs = [self._split(l) for l in self.missing]
        c0, s0 = self._split(self.after_linear)
        c1, s1 = self._split(self.before_linear)
        approx_start = (self.approx_time_of(self.missing[0]) if self.missing
                        else self.approx_time_of(self.after_linear + 1))
        approx_end = (self.approx_time_of(self.missing[-1] + 1)
                      if self.missing else approx_start)
        return {
            "gap_id": self.gap_id, "device_id": self.device_id, "metric": self.metric,
            "after": {"cycle": c0, "seq": s0, "linear": self.after_linear},
            "before": {"cycle": c1, "seq": s1, "linear": self.before_linear},
            "missing": [{"cycle": c, "seq": s, "linear": l}
                        for (c, s), l in zip(missing_cs, self.missing)],
            "status": self.status,
            "fillers": [{"linear": l, "cycle": l // self.modulus,
                         "seq": l % self.modulus, "event_id": eid, "at": at}
                        for l, eid, at in self.fillers],
            "approx_event_time_range": [approx_start, approx_end],
            "first_seen": self.first_seen, "updated_at": self.updated_at,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "gap_id": self.gap_id, "device_id": self.device_id,
            "metric": self.metric, "modulus": self.modulus,
            "after_linear": self.after_linear, "before_linear": self.before_linear,
            "missing": list(self.missing), "cadence": self.cadence,
            "anchor_event_time": self.anchor_event_time,
            "first_seen": self.first_seen, "updated_at": self.updated_at,
            "fillers": [list(f) for f in self.fillers],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Gap":
        return cls(
            gap_id=d["gap_id"], device_id=d["device_id"], metric=d["metric"],
            modulus=d["modulus"], after_linear=d["after_linear"],
            before_linear=d["before_linear"],
            missing=list(d["missing"]), cadence=d["cadence"],
            anchor_event_time=d["anchor_event_time"],
            first_seen=d["first_seen"], updated_at=d["updated_at"],
            fillers=[tuple(f) for f in d.get("fillers", ())],
        )


# ---------------------------------------------------------------- 窗口版本

@dataclass(frozen=True)
class WindowRevision:
    window_id: str
    version: int
    state: str                      # sealed | revised
    created_at: float
    parent_version: Optional[int]
    window_start: float
    window_end: float
    event_ids: tuple[str, ...]
    introduced_event_ids: tuple[str, ...]
    affected_metrics: tuple[str, ...]
    change_summary: str
    aggregates: dict[str, dict[str, Any]]
    gaps: tuple[dict[str, Any], ...]
    clock_model_ids: tuple[str, ...]
    config_versions: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_id": self.window_id, "version": self.version, "state": self.state,
            "created_at": self.created_at, "parent_version": self.parent_version,
            "window_start": self.window_start, "window_end": self.window_end,
            "event_ids": list(self.event_ids),
            "introduced_event_ids": list(self.introduced_event_ids),
            "affected_metrics": list(self.affected_metrics),
            "change_summary": self.change_summary, "aggregates": self.aggregates,
            "gaps": list(self.gaps), "clock_model_ids": list(self.clock_model_ids),
            "config_versions": list(self.config_versions),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "WindowRevision":
        d = dict(d)
        d["event_ids"] = tuple(d["event_ids"])
        d["introduced_event_ids"] = tuple(d["introduced_event_ids"])
        d["affected_metrics"] = tuple(d["affected_metrics"])
        d["gaps"] = tuple(d["gaps"])
        d["clock_model_ids"] = tuple(d["clock_model_ids"])
        d["config_versions"] = tuple(d["config_versions"])
        return cls(**d)


# ---------------------------------------------------------------- 入账结果

@dataclass
class DuplicateOccurrence:
    entry_id: str
    gateway_id: str
    recv_ts: float
    source_cursor: Optional[int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id, "gateway_id": self.gateway_id,
            "recv_ts": self.recv_ts, "source_cursor": self.source_cursor,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DuplicateOccurrence":
        return cls(**d)


@dataclass
class LedgerEntry:
    entry_id: str
    verdict: str                    # EventResult 值
    reason: str
    accepted: bool
    raw: RawMessage
    ingest_ts: float
    normalized: Optional[NormalizedRecord] = None
    duplicate_of: Optional[str] = None
    annotations: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id, "verdict": self.verdict, "reason": self.reason,
            "accepted": self.accepted, "raw": self.raw.to_dict(),
            "ingest_ts": self.ingest_ts,
            "normalized": self.normalized.to_dict() if self.normalized else None,
            "duplicate_of": self.duplicate_of,
            "annotations": list(self.annotations),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "LedgerEntry":
        return cls(
            entry_id=d["entry_id"], verdict=d["verdict"], reason=d["reason"],
            accepted=d["accepted"], raw=RawMessage.from_dict(d["raw"]),
            ingest_ts=d["ingest_ts"],
            normalized=NormalizedRecord.from_dict(d["normalized"]) if d.get("normalized") else None,
            duplicate_of=d.get("duplicate_of"),
            annotations=tuple(d.get("annotations", ())),
        )
