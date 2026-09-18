"""时钟模型测试：质量分级、漂移修正、模型升版、缺时间戳回退。"""

import unittest

from app.models import TimeQuality
from helpers import BASE_MS, DEVICE, POINT, make_pipeline, msg, register_point


class ClockTest(unittest.TestCase):
    def setUp(self):
        self.pipe, self.store, self.clock = make_pipeline()
        register_point(self.pipe)

    def tearDown(self):
        self.store.close()

    def _only_event(self):
        events = self.store.events_in_range(POINT, 0, BASE_MS + 3_600_000)
        self.assertEqual(len(events), 1)
        return events[0]

    def test_device_quality_when_clock_accurate(self):
        self.pipe.ingest_one(msg(1, BASE_MS))  # 偏移 +100ms < 250ms
        ev = self._only_event()
        self.assertEqual(ev["time_quality"], TimeQuality.DEVICE.value)
        self.assertEqual(ev["offset_applied_ms"], 0.0)
        self.assertEqual(ev["event_time_ms"], BASE_MS)

    def test_drift_corrected_with_basis(self):
        # 设备时钟快 8s：打戳 t+8000，网关 t+100 收到
        self.pipe.ingest_one(msg(1, BASE_MS, drift_ms=8000))
        ev = self._only_event()
        self.assertEqual(ev["time_quality"], TimeQuality.CORRECTED.value)
        self.assertEqual(ev["offset_applied_ms"], -7900.0)
        self.assertEqual(ev["event_time_ms"], BASE_MS + 100)
        self.assertEqual(ev["clock_model_version"], 1)

    def test_model_version_bumps_on_drift(self):
        self.pipe.ingest_one(msg(1, BASE_MS))                 # 偏移 +100 → v1
        self.pipe.ingest_one(msg(2, BASE_MS + 1000, drift_ms=8000))  # 漂移 → v2
        events = self.store.events_in_range(POINT, 0, BASE_MS + 3_600_000)
        self.assertEqual(events[0]["clock_model_version"], 1)
        self.assertEqual(events[1]["clock_model_version"], 2)
        with self.store.read() as conn:
            models = conn.execute(
                "SELECT * FROM clock_models WHERE device_id=? ORDER BY version", (DEVICE,)
            ).fetchall()
        self.assertEqual(len(models), 2)
        self.assertEqual(models[1]["offset_ms"], -7900.0)

    def test_estimated_when_collected_missing(self):
        m = msg(1, BASE_MS)
        m.collected_at_ms = None
        self.pipe.ingest_one(m)
        ev = self._only_event()
        self.assertEqual(ev["time_quality"], TimeQuality.ESTIMATED.value)
        self.assertEqual(ev["event_time_ms"], BASE_MS + 100)  # 用网关接收时刻

    def test_model_fallback_when_gateway_stamp_missing(self):
        # 先建立漂移模型（设备快 8s）
        self.pipe.ingest_one(msg(1, BASE_MS, drift_ms=8000))
        # 后续报文缺网关时刻：按模型当前版本修正
        m = msg(2, BASE_MS + 10_000, drift_ms=8000)
        m.gateway_received_at_ms = None
        self.pipe.ingest_one(m)
        events = self.store.events_in_range(POINT, 0, BASE_MS + 3_600_000)
        ev = events[1]
        self.assertEqual(ev["time_quality"], TimeQuality.CORRECTED.value)
        self.assertEqual(ev["offset_applied_ms"], -7900.0)
        self.assertEqual(ev["event_time_ms"], BASE_MS + 10_000 + 100)

    def test_correction_reproducible_from_observations(self):
        """修正依据可重放：观测表中的 (设备, 网关) 时间对与事件偏移一致。"""
        self.pipe.ingest_one(msg(1, BASE_MS, drift_ms=8000))
        ev = self._only_event()
        with self.store.read() as conn:
            obs = conn.execute(
                "SELECT * FROM clock_observations WHERE device_id=?", (DEVICE,)
            ).fetchall()
        self.assertEqual(len(obs), 1)
        self.assertEqual(obs[0]["offset_ms"], ev["offset_applied_ms"])
        self.assertEqual(obs[0]["raw_id"], ev["raw_id"])


if __name__ == "__main__":
    unittest.main()
