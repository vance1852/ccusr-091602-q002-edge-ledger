"""序号空间：回绕、代际、缺口。"""
import unittest

from app.ledger import TelemetryLedger
from tests.helpers import make_msg


class SequenceTests(unittest.TestCase):
    def setUp(self):
        self.t = 1000.0
        self.M = 10
        self.L = TelemetryLedger(
            modulus=self.M, window_seconds=60.0, time_fn=lambda: self.t)

    def test_gap_detected_and_filled_on_backfill(self):
        self.L.ingest(make_msg(1))
        self.L.ingest(make_msg(2))
        self.L.ingest(make_msg(5))
        gaps = self.L.gaps("devA", "temp")
        missing = sorted(s for g in gaps for s in g.missing)
        self.assertEqual(missing, [3, 4])

        self.L.ingest(make_msg(3, recv_ts=self.t + 5.5, cursor=103))
        self.L.ingest(make_msg(4, recv_ts=self.t + 5.6, cursor=104))
        self.assertTrue(all(g.status == "resolved"
                            for g in self.L.gaps("devA", "temp")))

    def test_small_seq_after_buffered_backfill_is_not_wrap(self):
        # 关键回归：先收 5，再补传 3，不能把 3 当成回绕后新循环
        self.L.ingest(make_msg(5))
        back = self.L.ingest(make_msg(3, recv_ts=self.t + 5.5, cursor=103))
        self.assertEqual(back.normalized.cycle, 0)
        self.assertEqual(back.verdict, "accepted")
        # 同槽重发仍是重复而不是碰撞
        again = self.L.ingest(make_msg(3, recv_ts=self.t + 5.6, cursor=104))
        self.assertEqual(again.verdict, "duplicate")

    def test_wrap_detected_when_time_advances(self):
        for s in [1, 2, 8, 9]:
            self.L.ingest(make_msg(s))
        wrap = self.L.ingest(make_msg(0, device_ts=self.t + 10,
                                      recv_ts=self.t + 10, cursor=500))
        self.assertEqual(wrap.normalized.cycle, 1)
        nxt = self.L.ingest(make_msg(1, device_ts=self.t + 11,
                                     recv_ts=self.t + 11, cursor=501))
        self.assertEqual(nxt.normalized.cycle, 1)
        self.assertEqual(nxt.normalized.event_id,
                         "evt:devA:temp:g0c1:1")

    def test_late_old_cycle_after_wrap(self):
        for s in [1, 2, 8, 9]:
            self.L.ingest(make_msg(s))
        self.L.ingest(make_msg(0, device_ts=self.t + 10,
                               recv_ts=self.t + 10, cursor=500))
        # 上一循环 seq4 的缓存迟到：设备时间更早 -> 归回旧循环补缺口
        late = self.L.ingest(make_msg(4, device_ts=self.t + 4,
                                      recv_ts=self.t + 10.5, cursor=504))
        self.assertEqual(late.normalized.cycle, 0)
        self.assertEqual(late.verdict, "accepted")

    def test_generation_switch_starts_fresh_cycle_space(self):
        self.L.ingest(make_msg(9))
        g1 = self.L.ingest(make_msg(0, gen=1, device_ts=self.t + 10,
                                    recv_ts=self.t + 10, cursor=600))
        self.assertEqual(g1.normalized.gen, 1)
        # 新代际不复用旧循环序号空间，cycle 在旧 cycle 之后
        self.assertGreaterEqual(g1.normalized.cycle, 1)
        slot_id = g1.normalized.event_id
        # gen=0 seq0 槽位与 gen=1 互不冲突
        g0 = self.L.ingest(make_msg(0, gen=0, device_ts=self.t + 1,
                                    recv_ts=self.t + 11, cursor=700))
        self.assertNotEqual(g0.normalized.event_id, slot_id)

    def test_collision_is_quarantined_in_conflict_ledger(self):
        self.L.ingest(make_msg(1))
        self.L.ingest(make_msg(5))  # opens gap 2,3,4
        self.L.ingest(make_msg(3, recv_ts=self.t + 5.5, cursor=103))  # 真报补缺口
        bad = self.L.ingest(make_msg(3, value=42.0,
                                     recv_ts=self.t + 5.6, cursor=104))
        self.assertEqual(bad.verdict, "sequence_conflict")
        # 槽位保持先到真报
        self.assertEqual(self.L.get_event(
            "evt:devA:temp:g0c0:3").value, 3.0)
        conflicts = self.L.conflicts("devA")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["winner_event_id"],
                         "evt:devA:temp:g0c0:3")
        self.assertEqual(conflicts[0]["loser_entry_id"], bad.entry_id)
        # 败方原始报文仍可逐条调阅
        self.assertEqual(self.L.get_entry(
            bad.entry_id).raw.value, 42.0)

    def test_gap_snapshot_has_linear_and_cycle_coords(self):
        self.L.ingest(make_msg(8))
        self.L.ingest(make_msg(9))
        self.L.ingest(make_msg(1, device_ts=self.t + 11,
                               recv_ts=self.t + 11, cursor=501))
        # 回绕造成 c0 seq? — seq9 -> c1 seq1 之间缺 c1 seq0
        snap = self.L.gaps("devA", "temp")[0].snapshot()
        self.assertEqual(snap["missing"][0]["cycle"], 1)
        self.assertEqual(snap["missing"][0]["seq"], 0)
        self.assertTrue(snap["approx_event_time_range"][0] > self.t)


if __name__ == "__main__":
    unittest.main()
