"""设备时钟模型：观测、偏移估计与事件时间修正。

原理：网关时钟（NTP 同步）视为真值基准。每条同时携带设备采集时刻与
网关接收时刻的报文都是一次时钟观测：offset = 网关时刻 - 设备时刻。

修正策略：
- 报文自带双时间戳时，它自身就是修正依据（自描述）：offset 直接取自
  本条报文 —— 断网补传的旧报文因此携带当时的真实偏移，不会被后来的
  观测污染；
- 缺网关时刻时，退回到设备时钟模型的当前版本（最近一次观测的偏移，
  对漂移响应快）；
- 缺设备时刻时，用网关接收时刻估计；
- 两者皆缺：unknown（由管线隔离）。

模型升版：最新观测偏移与当前模型偏差超过 MODEL_DRIFT_MS 时升版，
每次升版就是一次漂移事件留痕（clock_models 表只增不改）。
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from .models import TimeQuality
from .store import Store

# 偏移小于该值认为设备时钟可信，直接使用设备时间
DEVICE_TRUST_MS = 250
# 最新观测偏移与当前模型偏差超过该值时，时钟模型升版（记录一次漂移事件）
MODEL_DRIFT_MS = 500


class ClockModelManager:
    def __init__(self, store: Store):
        self.store = store

    def record_observation(
        self,
        conn: sqlite3.Connection,
        device_id: str,
        collected_at_ms: int,
        gateway_received_at_ms: int,
        raw_id: str,
        now_ms: int,
    ) -> None:
        """记录一次观测；若偏移显著偏离当前模型则升版（漂移事件留痕）。"""
        offset = float(gateway_received_at_ms - collected_at_ms)
        self.store.insert_observation(
            conn, device_id, gateway_received_at_ms, collected_at_ms, offset, raw_id
        )
        current = self.store.current_model(conn, device_id)
        if current is None or abs(offset - current["offset_ms"]) > MODEL_DRIFT_MS:
            version = 1 if current is None else current["version"] + 1
            self.store.insert_model(
                conn,
                {
                    "device_id": device_id,
                    "version": version,
                    "created_at_ms": now_ms,
                    "offset_ms": offset,
                    "sample_count": 1,
                    "method": "latest-pair",
                },
            )

    def resolve_event_time(
        self,
        conn: sqlite3.Connection,
        device_id: str,
        collected_at_ms: Optional[int],
        gateway_received_at_ms: Optional[int],
    ) -> dict:
        """计算事件时间与修正依据。返回字典含：
        event_time_ms / time_quality / offset_applied_ms / clock_model_version。
        event_time_ms 为 None 表示无法确定（调用方应隔离）。
        """
        current = self.store.current_model(conn, device_id)
        version = current["version"] if current else None

        if collected_at_ms is None and gateway_received_at_ms is None:
            return {
                "event_time_ms": None,
                "time_quality": TimeQuality.UNKNOWN.value,
                "offset_applied_ms": 0.0,
                "clock_model_version": version,
            }

        if collected_at_ms is None:
            # 无设备时间：以网关接收时刻估计
            return {
                "event_time_ms": gateway_received_at_ms,
                "time_quality": TimeQuality.ESTIMATED.value,
                "offset_applied_ms": 0.0,
                "clock_model_version": version,
            }

        if gateway_received_at_ms is not None:
            # 自描述报文：本条 (设备, 网关) 时间对即修正依据
            offset = float(gateway_received_at_ms - collected_at_ms)
        elif current is not None:
            # 缺网关时刻：退回设备时钟模型当前版本
            offset = float(current["offset_ms"])
        else:
            # 尚无任何观测（首条报文）：先信设备时钟
            return {
                "event_time_ms": collected_at_ms,
                "time_quality": TimeQuality.DEVICE.value,
                "offset_applied_ms": 0.0,
                "clock_model_version": version,
            }

        if abs(offset) < DEVICE_TRUST_MS:
            return {
                "event_time_ms": collected_at_ms,
                "time_quality": TimeQuality.DEVICE.value,
                "offset_applied_ms": 0.0,
                "clock_model_version": version,
            }
        return {
            "event_time_ms": int(round(collected_at_ms + offset)),
            "time_quality": TimeQuality.CORRECTED.value,
            "offset_applied_ms": offset,
            "clock_model_version": version,
        }
