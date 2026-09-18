"""判定边界：重复、乱序、迟到、碰撞、隔离。"""
import unittest

from app.ledger import TelemetryLedger
from tests.helpers import make_msg


class IngestVerdictTests(unittest.TestCase):
    def setUp(self):
        self.t = 1000.0
        self.L = TelemetryLedger(
            modulus=1000, window_seconds=60.0, time_fn=lambda: self.t)

    def test_unique_events_accepted(self):
        e1 = self.L.ingest(make_msg(1))
        e2 = self.L.ingest(make_msg(2))
        self.assertEqual(e1.verdict, "accepted")
        self.assertEqual(e2.verdict, "accepted")
        self.assertTrue(self.L.get_event(e1.normalized.event_id))

    def test_exact_retransmit_is_duplicate(self):
        first = self.L.ingest(make_msg(1))
        again = self.L.ingest(make_msg(1, recv_ts=self.t + 1.5, cursor=101))
        self.assertEqual(again.verdict, "duplicate")
        self.assertEqual(again.duplicate_of, first.entry_id)
        self.assertFalse(again.accepted)
        occ = self.L.occurrences_of(first.entry_id)
        self.assertEqual(len(occ), 1)
        self.assertEqual(occ[0].gateway_id, "gw1")

    def test_redundant_gateway_forward_is_duplicate(self):
        # 同一采样经双网关冗余转发：指纹一致 -> duplicate，且留痕来源网关
        first = self.L.ingest(make_msg(1, gateway="gw1"))
        via2 = self.L.ingest(make_msg(1, gateway="gw2",
                                      recv_ts=self.t + 1.2, cursor=5001))
        self.assertEqual(via2.verdict, "duplicate")
        self.assertIn("redundant-gateway-forward", via2.annotations)
        self.assertEqual(self.L.occurrences_of(first.entry_id)[0].gateway_id, "gw2")
        # 两个网关投递同一份采样，接受的唯一事件恰好一个
        accepted_events = [e.normalized.event_id for e in self.L.entries()
                           if e.accepted and e.normalized]
        self.assertEqual(accepted_events, [first.normalized.event_id])

    def test_dual_gateway_collision_is_sequence_conflict(self):
        self.L.ingest(make_msg(5, gateway="gw1", value=5.0))
        bad = self.L.ingest(make_msg(5, gateway="gwX", value=99.0,
                                     recv_ts=self.t + 5.5, cursor=9005))
        self.assertEqual(bad.verdict, "sequence_conflict")
        self.assertFalse(bad.accepted)
        self.assertIn("dual-gateway-collision", bad.annotations)
        # 原有事件不被覆盖
        owner = self.L.get_event("evt:devA:temp:g0c0:5")
        self.assertEqual(owner.value, 5.0)
        self.assertEqual(owner.gateway_id, "gw1")

    def test_out_of_order_into_open_window_is_accepted(self):
        self.L.ingest(make_msg(1))
        self.L.ingest(make_msg(3))
        backfill = self.L.ingest(make_msg(2, recv_ts=self.t + 3.5, cursor=102))
        self.assertEqual(backfill.verdict, "accepted")
        self.assertIn("arrival-out-of-order", backfill.annotations)

    def test_late_into_sealed_window_creates_revision(self):
        self.L.ingest(make_msg(1, device_ts=1001.0))
        self.L.seal_eligible(now=1070.0)
        win = "win:devA:temp:960"
        self.assertEqual(self.L.versions(win)[0].state, "sealed")

        late = self.L.ingest(make_msg(2, device_ts=1002.0,
                                      recv_ts=1090.0, cursor=202))
        self.assertEqual(late.verdict, "late")
        versions = self.L.versions(win)
        self.assertEqual([v.version for v in versions], [1, 2])
        self.assertEqual(versions[0].state, "sealed")
        self.assertEqual(versions[1].state, "revised")
        self.assertEqual(versions[1].parent_version, 1)
        self.assertIn("evt:devA:temp:g0c0:2", versions[1].introduced_event_ids)

    def test_sealed_version_is_never_mutated(self):
        self.L.ingest(make_msg(1, device_ts=1001.0, value=10.0))
        self.L.seal_eligible(now=1070.0)
        win = "win:devA:temp:960"
        v1 = self.L.versions(win)[0]
        v1_mean = v1.aggregates["temp"]["mean"]
        v1_events = tuple(v1.event_ids)

        self.L.ingest(make_msg(2, device_ts=1002.0, value=90.0,
                               recv_ts=1090.0, cursor=202))
        v1_after = self.L.versions(win)[0]
        self.assertEqual(v1_after.aggregates["temp"]["mean"], v1_mean)
        self.assertEqual(tuple(v1_after.event_ids), v1_events)

    def test_revision_marks_affected_metrics(self):
        self.L.ingest(make_msg(1, device_ts=1001.0, value=10.0))
        self.L.seal_eligible(now=1070.0)
        win = "win:devA:temp:960"
        self.L.ingest(make_msg(2, device_ts=1002.0, value=90.0,
                               recv_ts=1090.0, cursor=202))
        v2 = self.L.versions(win)[1]
        self.assertIn("temp", v2.affected_metrics)

    def test_quarantine_keeps_raw_payload(self):
        raw = {"device_id": "devA", "seq": 7, "metric": "temp",
               "value": "not-a-number", "device_ts": 1007.0,
               "gateway_id": "gw1"}
        e = self.L.quarantine(raw, "value 字段无法解析为浮点数",
                              gateway_id="gw1", recv_ts=1007.0)
        self.assertEqual(e.verdict, "quarantined")
        self.assertFalse(e.accepted)
        self.assertEqual(e.raw.payload["value"], "not-a-number")
        self.assertIsNone(e.normalized)


if __name__ == "__main__":
    unittest.main()
