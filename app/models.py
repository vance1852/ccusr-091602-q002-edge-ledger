"""领域模型与契约词汇。

所有判定结果、时间质量、窗口状态的取值与 domain_contract.json 对齐。
时间统一用 UTC 毫秒整数存储，API 层用 ISO-8601 字符串。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class EventResult(str, Enum):
    """单条投递的判定结果（契约 event_results）。"""

    ACCEPTED = "accepted"                  # 正常入流
    DUPLICATE = "duplicate"                # 幂等键命中且载荷一致
    LATE = "late"                          # 事件时间落入已封存窗口（仍入流并触发修订）
    SEQUENCE_CONFLICT = "sequence_conflict"  # 幂等键命中但载荷不一致
    QUARANTINED = "quarantined"            # 校验失败，隔离留证


class TimeQuality(str, Enum):
    """事件时间质量（契约 time_quality）。"""

    DEVICE = "device"          # 设备时钟可信，直接使用采集时间
    CORRECTED = "corrected"    # 设备时钟经时钟模型偏移修正
    ESTIMATED = "estimated"    # 无设备时间，用网关接收时间估计
    UNKNOWN = "unknown"        # 无任何可用时间（通常导致隔离）


class WindowState(str, Enum):
    """生产窗口状态（契约 window_states）。"""

    OPEN = "open"
    SEALED = "sealed"
    REVISED = "revised"


# 事件附加标记（存入 events.flags_json）
FLAG_OUT_OF_ORDER = "out_of_order"    # 乱序：事件时间或序号早于已见 frontier
FLAG_BACKFILLED = "backfilled"        # 补传：到达滞后超过阈值
FLAG_CALIBRATION = "calibration"      # 校准期：事件时间落在点位校准窗口内

# uint16 序号空间的回绕判定阈值（序号空间的一半）
SEQ_SPACE = 65536
WRAP_THRESHOLD = SEQ_SPACE // 2

# 设备时间超前服务时间超过该值则隔离（防时钟打飞）
FUTURE_GUARD_MS = 5 * 60 * 1000


def iso_to_ms(text: str) -> int:
    """ISO-8601 字符串转 UTC 毫秒。支持结尾 Z。"""
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def raw_message_id(
    device_id: str,
    point_id: str,
    device_seq: int,
    collected_at_ms: Optional[int],
    gateway_received_at_ms: Optional[int],
    payload: Any,
) -> str:
    """报文身份哈希。

    刻意不含 device_epoch 与 gateway_id：
    - epoch 是推断结果，同一报文重发时推断可能变化，但报文身份不变；
    - 同一报文经主备两个网关投递（双网关碰撞）应识别为同一报文。
    不同代际的同序号报文因采集时间/载荷不同而自然区分。
    """
    material = canonical_json(
        {
            "device_id": device_id,
            "point_id": point_id,
            "device_seq": device_seq,
            "collected_at_ms": collected_at_ms,
            "gateway_received_at_ms": gateway_received_at_ms,
            "payload": payload,
        }
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@dataclass
class IncomingMessage:
    """网关上报的一条遥测报文。"""

    gateway_id: str
    device_id: str
    point_id: str
    device_seq: int
    collected_at_ms: Optional[int]      # 设备采集时刻（设备时钟），可空
    gateway_received_at_ms: Optional[int]  # 网关接收时刻（网关时钟，NTP 可信），可空
    payload: dict                       # 原始载荷原文，至少含数值字段 value
    device_epoch: Optional[int] = None  # 设备代际；缺省时由服务推断（回绕检测）


@dataclass
class IngestOutcome:
    """单条投递的判定输出。"""

    raw_id: str
    result: EventResult
    reason: str = ""
    event_id: Optional[str] = None
    ledger_seq: Optional[int] = None
    flags: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "raw_id": self.raw_id,
            "result": self.result.value,
            "reason": self.reason,
            "event_id": self.event_id,
            "ledger_seq": self.ledger_seq,
            "flags": list(self.flags),
        }
