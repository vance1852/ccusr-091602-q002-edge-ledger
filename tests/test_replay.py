"""回放与配置版本测试：按事件时间回放某个账本版本、历史读数按当时配置解释。"""

import unittest

from app.models import FLAG_CALIBRATION
from helpers import BASE_MS, POINT, make_pipeline, msg, register_point


class ReplayTest(unittest.TestCase):
    def setUp(self):
        self.pipe, self.store, self.clock = make_pipeline()
        register_point(self.pipe)

    def tearDown(self):
        self.store.close()

    def _ingest(self, seq, offset_s, value=1.0):
        self.clock.set_ms(BASE_MS + int(offset_s * 1000) + 200)
        return self.pipe.ingest_one(msg(seq, BASE_MS + int(offset_s * 1000), value))

    def test_replay_as_of_excludes_later_events(self):
        self._ingest(1, 0)
        self._ingest(2, 5)
        checkpoint = self.store.current_ledger_seq()
        self._ingest(3, 10)
        replay = self.pipe.replay(POINT, 0, BASE_MS + 60_000, as_of_seq=checkpoint)
        self.assertEqual(len(replay["events"]), 2)
        replay_now = self.pipe.replay(POINT, 0, BASE_MS + 60_000)
        self.assertEqual(len(replay_now["events"]), 3)

    def test_replay_orders_by_event_time_not_arrival(self):
        self._ingest(1, 30)
        self._ingest(2, 10)  # 乱序到达
        self._ingest(3, 20)
        replay = self.pipe.replay(POINT, 0, BASE_MS + 60_000)
        times = [e["event_time_ms"] for e in replay["events"]]
        self.assertEqual(times, sorted(times))
        self.assertEqual([e["device_seq"] for e in replay["events"]], [2, 3, 1])

    def test_replay_window_revision_visible_as_of(self):
        self._ingest(1, 0, 10.0)
        self.clock.set_ms(BASE_MS + 91_000)
        from app.windows import seal_due_windows

        seal_due_windows(self.store, self.clock.now)
        checkpoint = self.store.current_ledger_seq()
        self.clock.set_ms(BASE_MS + 95_000)
        self.pipe.ingest_one(msg(2, BASE_MS + 30_000, 99.0))  # 迟到 → rev2
        replay_old = self.pipe.replay(POINT, 0, BASE_MS + 60_000, as_of_seq=checkpoint)
        replay_now = self.pipe.replay(POINT, 0, BASE_MS + 60_000)
        self.assertEqual(replay_old["windows"][0]["visible_revision"]["revision_no"], 1)
        self.assertEqual(replay_now["windows"][0]["visible_revision"]["revision_no"], 2)

    def test_historical_readings_keep_config_at_the_time(self):
        # v1: scale=1；v2 自 t+120s 起：scale=2, offset=10（校准参数变更）
        self._ingest(1, 60, 5.0)   # v1 时期
        self.pipe.register_point_config(
            {
                "point_id": POINT,
                "effective_from_ms": BASE_MS + 120_000,
                "scale": 2.0,
                "offset": 10.0,
                "location": "车间B",  # 移机
                "window_seconds": 60,
                "lateness_seconds": 30,
            }
        )
        self._ingest(2, 180, 5.0)  # v2 时期
        events = self.store.events_in_range(POINT, 0, BASE_MS + 300_000)
        self.assertEqual(events[0]["config_version"], 1)
        self.assertEqual(events[0]["value"], 5.0)          # 5*1+0
        self.assertEqual(events[1]["config_version"], 2)
        self.assertEqual(events[1]["value"], 20.0)         # 5*2+10
        # 配置历史可查询（移机留痕）
        configs = self.store.list_configs(POINT)
        self.assertEqual(configs[0]["location"], "车间A")
        self.assertEqual(configs[1]["location"], "车间B")

    def test_calibration_window_flagged(self):
        self.pipe.register_point_config(
            {
                "point_id": POINT,
                "effective_from_ms": BASE_MS + 60_000,
                "scale": 1.0,
                "offset": 0.0,
                "window_seconds": 60,
                "lateness_seconds": 30,
                "calibration_windows": [
                    [BASE_MS + 60_000, BASE_MS + 120_000]
                ],
            }
        )
        self._ingest(1, 30)   # 校准期前
        self._ingest(2, 90)   # 校准期内
        self._ingest(3, 150)  # 校准期后
        events = self.store.events_in_range(POINT, 0, BASE_MS + 300_000)
        self.assertNotIn(FLAG_CALIBRATION, events[0]["flags"])
        self.assertIn(FLAG_CALIBRATION, events[1]["flags"])
        self.assertNotIn(FLAG_CALIBRATION, events[2]["flags"])


if __name__ == "__main__":
    unittest.main()
