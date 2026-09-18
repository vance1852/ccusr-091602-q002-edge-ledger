"""设备时钟模型注册表。

设备漂移时钟的修正依据：

* 两条校时记录之间 -> 分段线性插值（corrected）
* 早于首条校时、晚于末条校时 -> 外推（corrected，但 annotations 标注 extrapolated）
* 校时记录到达时刻的校准沉淀窗口内（settle 窗口）-> estimated，
  因为重同步瞬间设备时钟可能跳变、读数归属不明
* 设备时间完全缺失 -> 用服务器接收时间（unknown 质量），模型 source=server-clock
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass
from typing import Optional

from .model import CalibrationRecord, ClockModel, TimeQuality

_UNCAL = ClockModel(
    model_id="clock:uncalibrated", source="uncalibrated",
    offset_seconds=0.0, drift_ppm=0.0, ref_device_ts=None,
    basis="无校时记录，设备时钟原样采用", created_at=0.0,
)


@dataclass
class ClockDecision:
    event_time: float
    quality: str
    model: ClockModel
    annotations: tuple[str, ...]


class ClockRegistry:
    def __init__(self, settle_seconds: float = 2.0):
        # 每台设备按 at_server 排序的校时记录
        self._cals: dict[str, list[CalibrationRecord]] = {}
        self._models: dict[str, ClockModel] = {_UNCAL.model_id: _UNCAL}
        self.settle_seconds = settle_seconds

    # ------------------------------------------------------------ 登记校时
    def add_calibration(self, cal: CalibrationRecord) -> None:
        lst = self._cals.setdefault(cal.device_id, [])
        pos = bisect.bisect_left([c.at_server for c in lst], cal.at_server)
        lst.insert(pos, cal)

    def calibrations(self, device_id: str) -> list[CalibrationRecord]:
        return list(self._cals.get(device_id, ()))

    # ------------------------------------------------------------ 查询模型
    def model_for(self, device_id: str, device_ts: Optional[float],
                  recv_ts: float) -> ClockDecision:
        if device_ts is None:
            m = ClockModel(
                model_id="clock:server-fallback", source="server-clock",
                offset_seconds=0.0, drift_ppm=0.0, ref_device_ts=None,
                basis="报文缺少设备时间，以网关接收时间作为事件时间（最低可信度）",
                created_at=recv_ts,
            )
            self._models[m.model_id] = m
            return ClockDecision(recv_ts, TimeQuality.UNKNOWN.value, m,
                                 ("event-time=server-recv-time",))

        cals = self._cals.get(device_id, [])
        if not cals:
            return ClockDecision(device_ts, TimeQuality.DEVICE.value, _UNCAL,
                                 ("uncalibrated-device-clock",))

        # 读数在重同步发生后极短时间内到达：设备时钟可能刚跳变，归属存疑
        latest = cals[-1]
        since_resync = recv_ts - latest.at_server
        settle_note: tuple[str, ...] = ()
        if 0 <= since_resync <= self.settle_seconds:
            settle_note = ("clock-resync-settling",)

        # 在设备时间轴上定位相邻校时点（以 anchor_device 为锚）
        anchors = sorted(cals, key=lambda c: c.anchor_device)
        anchor_ts = [c.anchor_device for c in anchors]
        idx = bisect.bisect_left(anchor_ts, device_ts)

        if 0 < idx < len(anchors):               # 两点之间：插值
            c0, c1 = anchors[idx - 1], anchors[idx]
            span = c1.anchor_device - c0.anchor_device
            slope_ppm = 0.0 if span == 0 else (
                (c1.offset_seconds - c0.offset_seconds) / span * 1e6)
            # applied_offset(t) = off - drift*1e-6*(t-ref)
            # 要表达 off(t)=off0+slope*(t-t0)，故 drift 取 -slope
            model = ClockModel(
                model_id=f"clock:{device_id}:interp:{c0.calib_id}:{c1.calib_id}",
                source="interpolation", offset_seconds=c0.offset_seconds,
                drift_ppm=-slope_ppm, ref_device_ts=c0.anchor_device,
                basis=(f"校时点 {c0.calib_id}(偏差{c0.offset_ms:+.1f}ms) 与 "
                       f"{c1.calib_id}(偏差{c1.offset_ms:+.1f}ms) 之间分段线性插值，"
                       f"斜率 {slope_ppm:+.3f}ppm"),
                created_at=c1.at_server,
            )
            quality = TimeQuality.ESTIMATED.value if settle_note else TimeQuality.CORRECTED.value
        elif idx == 0 and anchors:               # 早于最早校时点：向后外推
            c0 = anchors[0]
            model = ClockModel(
                model_id=f"clock:{device_id}:back-extrap:{c0.calib_id}",
                source="extrapolation", offset_seconds=c0.offset_seconds,
                drift_ppm=c0.drift_ppm, ref_device_ts=c0.anchor_device,
                basis=(f"读数早于首条校时记录 {c0.calib_id}，沿用其偏差 "
                       f"{c0.offset_ms:+.1f}ms 与频偏 {c0.drift_ppm:+.2f}ppm 向后外推"),
                created_at=c0.at_server,
            )
            quality = TimeQuality.ESTIMATED.value if settle_note else TimeQuality.CORRECTED.value
            settle_note = settle_note + ("time-correction-extrapolated",)
        else:                                    # 晚于最晚校时点：向前外推
            c1 = anchors[-1]
            model = ClockModel(
                model_id=f"clock:{device_id}:fwd-extrap:{c1.calib_id}",
                source="extrapolation", offset_seconds=c1.offset_seconds,
                drift_ppm=c1.drift_ppm, ref_device_ts=c1.anchor_device,
                basis=(f"读数晚于最近校时记录 {c1.calib_id}，按其偏差 "
                       f"{c1.offset_ms:+.1f}ms 与频偏 {c1.drift_ppm:+.2f}ppm 向前外推"),
                created_at=c1.at_server,
            )
            quality = TimeQuality.ESTIMATED.value if settle_note else TimeQuality.CORRECTED.value
            settle_note = settle_note + ("time-correction-extrapolated",)

        self._models[model.model_id] = model
        event_time = device_ts + model.applied_offset(device_ts)
        return ClockDecision(event_time, quality, model, settle_note)

    def get_model(self, model_id: str) -> Optional[ClockModel]:
        return self._models.get(model_id)

    def all_models(self) -> list[ClockModel]:
        return list(self._models.values())
