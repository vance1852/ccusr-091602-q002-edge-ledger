"""消费者查询：按版本回放、事件时间回放、峰值定序、血缘。"""
import unittest

from app.model import CalibrationRecord
from app.ledger import TelemetryLedger
from tests.helpers import make_msg


class QueryTests(unittest.TestCase):
    def setUp(self):
        self.t = 1000.0
        self.L = TelemetryLedger(
            modulus=1000, window_seconds=60.0, time_fn=lambda: self.t)
        # 设备时钟慢 10 秒：device 1000 -> event 1010
        self.L.clocks.add_calibration(CalibrationRecord(
            "cal1", "devA", 1005.0, 995.0, 10000.0, 0.0, "ntp", 1005.0))

    def test_peak_ordered_by_event_time_not_arrival(self):
        # 两个读数：峰值 90 的事件时间更早，但它更晚到达
        self.L.ingest(make_msg(1, value=50.0, device_ts=1002.0,
                               recv_ts=1013.0))
        self.L.ingest(make_msg(2, value=90.0, device_ts=1001.0,
                               recv_ts=1014.0))
        self.L.seal_eligible(now=1080.0)
        win = "win:devA:temp:960"
        agg = self.L.versions(win)[0].aggregates["temp"]
        self.assertEqual(agg["peak_value"], 90.0)
        self.assertEqual(agg["peak_event_id"],
                         "evt:devA:temp:g0c0:2")
        self.assertAlmostEqual(agg["peak_event_time"], 1011.0)

    def test_replay_specific_version_is_immutable(self):
        self.L.ingest(make_msg(1, value=10.0, device_ts=1001.0,
                               recv_ts=1011.0))
        self.L.seal_eligible(now=1080.0)
        win = "win:devA:temp:960"
        self.L.ingest(make_msg(2, value=40.0, device_ts=1002.0,
                               recv_ts=1090.0))   # late -> v2
        v1 = self.L.replay_window(win, version=1)
        v2 = self.L.replay_window(win, version=2)
        self.assertEqual(len(v1["events"]), 1)
        self.assertEqual(v1["state"], "sealed")
        self.assertEqual(len(v2["events"]), 2)
        self.assertEqual(v2["state"], "revised")
        # 血缘链
        self.assertEqual([(n["version"], n["state"]) for n in v2["lineage"]],
                         [(1, "sealed"), (2, "revised")])

    def test_replay_events_by_event_time_range(self):
        for s in [1, 2, 3]:
            self.L.ingest(make_msg(s, value=float(s), device_ts=1000.0 + s,
                                   recv_ts=1010.0 + s))
        out = self.L.replay_events("devA", "temp", 1010.5, 1012.5)
        times = [e["event_time"] for e in out["events"]]
        self.assertEqual(times, [1011.0, 1012.0])
        # 每条回放记录都带时间修正依据
        self.assertIn("校时", out["events"][0]["clock_basis"])

    def test_explain_event_has_full_evidence_chain(self):
        e = self.L.ingest(make_msg(1, value=24.0, device_ts=1001.0,
                                   recv_ts=1011.0))
        view = self.L.explain_event(e.normalized.event_id)
        self.assertEqual(view["raw_payload"]["seq"], 1)
        self.assertIn("offset_seconds", view["clock_model"])
        self.assertEqual(view["config"]["version"], "cfg:default")
        self.assertEqual(view["verdict"], "accepted")
        self.assertIn("duplicate_occurrences", view)


if __name__ == "__main__":
    unittest.main()
