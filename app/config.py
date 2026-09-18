"""测点目录注册表。

测点移机、校准参数变更都是「带生效时间的配置版本」。解释历史读数时，
按 *事件时间* 选择当时生效的版本，而不是按注册时间或当前配置。
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass
from typing import Optional

from .model import PointConfig


@dataclass
class Interpretation:
    value: float
    unit: str
    location: str
    config: PointConfig

    def to_dict(self) -> dict:
        return {"value": self.value, "unit": self.unit, "location": self.location,
                "config_version": self.config.version}


_DEFAULT_VERSION = "cfg:default"


class ConfigRegistry:
    def __init__(self):
        # (device_id, metric) -> 按 effective_event_time 排序的版本
        self._versions: dict[tuple[str, str], list[PointConfig]] = {}

    def register(self, cfg: PointConfig) -> None:
        key = (cfg.device_id, cfg.metric)
        lst = self._versions.setdefault(key, [])
        # 同一生效时间再次登记视为覆盖修订
        for i, existing in enumerate(lst):
            if existing.effective_event_time == cfg.effective_event_time:
                lst[i] = cfg
                break
        else:
            bisect.insort(lst, cfg, key=lambda c: c.effective_event_time)

    def effective_at(self, device_id: str, metric: str,
                     event_time: float) -> Optional[PointConfig]:
        lst = self._versions.get((device_id, metric))
        if not lst:
            return None
        times = [c.effective_event_time for c in lst]
        idx = bisect.bisect_right(times, event_time) - 1
        return lst[idx] if idx >= 0 else None

    def interpret(self, device_id: str, metric: str, raw_value: float,
                  event_time: float) -> Interpretation:
        cfg = self.effective_at(device_id, metric, event_time)
        if cfg is None:
            return Interpretation(
                value=float(raw_value), unit="raw", location="unknown",
                config=PointConfig(
                    device_id=device_id, metric=metric, version=_DEFAULT_VERSION,
                    effective_event_time=0.0, scale=1.0, offset=0.0,
                    unit="raw", location="unknown",
                    note="目录中无此测点配置，原值透传并标记 raw/unknown",
                    registered_at=0.0),
            )
        return Interpretation(
            value=raw_value * cfg.scale + cfg.offset,
            unit=cfg.unit, location=cfg.location, config=cfg,
        )

    def versions_of(self, device_id: str, metric: str) -> list[PointConfig]:
        return list(self._versions.get((device_id, metric), ()))
