"""事件溯源持久化。

磁盘上只追加 JSONL WAL，记录五类事实：cal / config / message / quarantine / seal。
服务重启时按原始顺序与逻辑时间戳重放，指纹、线性序号、缺口、时钟模型缓存、
窗口版本与 entry/event 编号全部确定性重建 —— 不需要单独的状态迁移。
"""
from __future__ import annotations

import json
import os
from typing import Any, Optional

from .clock import ClockRegistry
from .config import ConfigRegistry
from .ledger import TelemetryLedger
from .model import CalibrationRecord, PointConfig, RawMessage

WAL_VERSION = 1


class LedgerStore:
    def __init__(self, path: str, *, modulus: int = 1000,
                 window_seconds: float = 60.0,
                 modulus_by_device: Optional[dict[str, int]] = None,
                 rebuild: bool = False):
        self.path = path
        self.ledger = TelemetryLedger(
            modulus=modulus, window_seconds=window_seconds,
            modulus_by_device=modulus_by_device,
            clock=ClockRegistry(), configs=ConfigRegistry())
        self._fh = None
        if rebuild and os.path.exists(path):
            self._replay()
        self._fh = open(path, "a", encoding="utf-8")

    # ------------------------------------------------------------- 追加
    def _append(self, record: dict[str, Any]) -> None:
        self._fh.write(json.dumps(record, ensure_ascii=False,
                                  separators=(",", ":")) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def add_calibration(self, cal: CalibrationRecord) -> None:
        self._append({"t": "cal", "v": WAL_VERSION, "cal": cal.to_dict()})
        self.ledger.clocks.add_calibration(cal)

    def register_config(self, cfg: PointConfig) -> None:
        self._append({"t": "config", "v": WAL_VERSION, "cfg": cfg.to_dict()})
        self.ledger.configs.register(cfg)

    def ingest(self, m: RawMessage, at: Optional[float] = None):
        at = self.ledger._now() if at is None else at
        self._append({"t": "message", "v": WAL_VERSION, "at": at,
                      "msg": m.to_dict()})
        return self.ledger._ingest(m, at)

    def quarantine(self, raw: dict[str, Any], reason: str, *,
                   gateway_id: str = "?", recv_ts: Optional[float] = None,
                   at: Optional[float] = None):
        at = self.ledger._now() if at is None else at
        self._append({"t": "quarantine", "v": WAL_VERSION, "at": at,
                      "raw": raw, "reason": reason,
                      "gateway_id": gateway_id, "recv_ts": recv_ts})
        return self.ledger.quarantine(raw, reason, gateway_id=gateway_id,
                                      recv_ts=recv_ts, at=at)

    def seal_window(self, win_id: str, at: Optional[float] = None):
        at = self.ledger._now() if at is None else at
        self._append({"t": "seal", "v": WAL_VERSION, "win_id": win_id, "at": at})
        return self.ledger.seal_window(win_id, at=at)

    def seal_eligible(self, now: Optional[float] = None):
        sealed = self.ledger.seal_eligible(now)
        for rev in sealed:
            self._append({"t": "seal", "v": WAL_VERSION,
                          "win_id": rev.window_id, "at": rev.created_at})
        return sealed

    def close(self) -> None:
        if self._fh and not self._fh.closed:
            self._fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------- 重放
    def _replay(self) -> None:
        L = self.ledger
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                r = json.loads(line)
                kind = r["t"]
                if kind == "cal":
                    L.clocks.add_calibration(CalibrationRecord.from_dict(r["cal"]))
                elif kind == "config":
                    L.configs.register(PointConfig.from_dict(r["cfg"]))
                elif kind == "message":
                    L._ingest(RawMessage.from_dict(r["msg"]), r["at"])
                elif kind == "quarantine":
                    L.quarantine(r["raw"], r["reason"],
                                 gateway_id=r.get("gateway_id", "?"),
                                 recv_ts=r.get("recv_ts"), at=r["at"])
                elif kind == "seal":
                    L.seal_window(r["win_id"], at=r["at"])
                else:
                    raise ValueError(f"WAL 中出现未知记录类型: {kind}")
