"""测试公共辅助：可控时钟、内存管线、报文构造。"""

from app.models import IncomingMessage, iso_to_ms
from app.pipeline import Pipeline
from app.store import Store

BASE_MS = iso_to_ms("2026-01-01T00:00:00Z")
POINT = "p1"
DEVICE = "dev-1"
GW = "gw-a"


class Clock:
    def __init__(self, start_ms: int = BASE_MS):
        self.now = start_ms

    def __call__(self) -> int:
        return self.now

    def set_ms(self, ms: int) -> None:
        self.now = ms

    def advance_ms(self, ms: int) -> None:
        self.now += ms


def make_pipeline(db_path: str = ":memory:", backfill_threshold_ms: int = 60_000):
    store = Store(db_path)
    clock = Clock()
    pipe = Pipeline(store, clock=clock, backfill_threshold_ms=backfill_threshold_ms)
    return pipe, store, clock


def register_point(pipe: Pipeline, point: str = POINT, **overrides) -> dict:
    cfg = {
        "point_id": point,
        "effective_from_ms": 0,
        "unit": "MPa",
        "scale": 1.0,
        "offset": 0.0,
        "location": "车间A",
        "window_seconds": 60,
        "lateness_seconds": 30,
    }
    cfg.update(overrides)
    return pipe.register_point_config(cfg)


def msg(
    seq: int,
    t_ms: int,
    value: float = 1.0,
    gateway: str = GW,
    device: str = DEVICE,
    point: str = POINT,
    drift_ms: int = 0,
    epoch=None,
    payload=None,
    with_timestamps: bool = True,
) -> IncomingMessage:
    return IncomingMessage(
        gateway_id=gateway,
        device_id=device,
        point_id=point,
        device_seq=seq,
        collected_at_ms=(t_ms + drift_ms) if with_timestamps else None,
        gateway_received_at_ms=(t_ms + 100) if with_timestamps else None,
        payload=payload if payload is not None else {"value": value},
        device_epoch=epoch,
    )
