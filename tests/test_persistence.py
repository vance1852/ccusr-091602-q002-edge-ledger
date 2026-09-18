"""服务重启后的 WAL 重放：判定、序号、缺口、窗口血缘逐位重建。"""
import os
import tempfile
import unittest

from app.model import CalibrationRecord, PointConfig
from app.persistence import LedgerStore
from tests.helpers import make_msg


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "ledger.jsonl")
        self.t = 1000.0

    def tearDown(self):
        self.tmp.cleanup()

    def _build(self):
        s = LedgerStore(self.path, modulus=10, window_seconds=60.0)
        s.add_calibration(CalibrationRecord(
            "cal1", "devA", 1010.0, 1000.0, 10000.0, 0.0, "ntp", 1005.0))
        s.register_config(PointConfig(
            "devA", "temp", "cfg-v1", 0.0, 1.0, 0.0, "C", "A区", "", 900.0))
        for seq in [1, 2, 5]:
            s.ingest(make_msg(seq, device_ts=1000.0 + seq,
                              recv_ts=1005.0 + seq), at=1005.0 + seq)
        s.ingest(make_msg(3, device_ts=1003.0, recv_ts=1005.5,
                          cursor=103), at=1005.5)
        s.ingest(make_msg(1, recv_ts=1006.0, cursor=201), at=1006.0)  # duplicate
        s.seal_eligible(now=1070.0)
        s.ingest(make_msg(4, device_ts=1004.0, recv_ts=1090.0,
                          cursor=104), at=1090.0)                      # late
        s.close()

    def test_replay_rebuilds_verdicts_events_and_lineage(self):
        self._build()
        s = LedgerStore(self.path, modulus=10, window_seconds=60.0,
                        rebuild=True)
        L = s.ledger
        verdicts = [e.verdict for e in L.entries()]
        self.assertEqual(verdicts.count("accepted"), 4)   # 1,2,5,3
        self.assertIn("duplicate", verdicts)
        self.assertIn("late", verdicts)

        win = next(w for w in L.list_windows("devA", "temp"))
        versions = L.versions(win)
        self.assertEqual([v.version for v in versions], [1, 2])
        self.assertEqual(versions[0].state, "sealed")
        self.assertEqual(versions[1].state, "revised")
        self.assertEqual(len(versions[0].event_ids), 4)  # 1,2,3,5
        self.assertEqual(len(versions[1].event_ids), 5)  # +4
        self.assertIn("evt:devA:temp:g0c0:4",
                      versions[1].introduced_event_ids)
        # entry 编号确定性重建
        self.assertEqual(L.entries()[0].entry_id, "entry:000001")
        s.close()

    def test_replay_restores_clock_and_config_basis(self):
        self._build()
        s = LedgerStore(self.path, modulus=10, window_seconds=60.0,
                        rebuild=True)
        L = s.ledger
        r = L.get_event("evt:devA:temp:g0c0:1")
        # 设备 1001 + 10s 偏差 = 1011
        self.assertAlmostEqual(r.event_time, 1011.0)
        self.assertIn("cal1", r.clock_model_id)
        view = L.explain_event(r.event_id)
        self.assertEqual(view["config"]["version"], "cfg-v1")
        self.assertIn("外推", view["clock_model"]["basis"])
        s.close()

    def test_replay_is_idempotent_across_restarts(self):
        self._build()
        s1 = LedgerStore(self.path, modulus=10, window_seconds=60.0,
                         rebuild=True)
        v1 = [r.to_dict() for r in s1.ledger.entries()[0:3]]
        s1.close()
        s2 = LedgerStore(self.path, modulus=10, window_seconds=60.0,
                         rebuild=True)
        v2 = [r.to_dict() for r in s2.ledger.entries()[0:3]]
        self.assertEqual(v1, v2)
        s2.close()

    def test_resume_cursor_survives_restart(self):
        self._build()
        s = LedgerStore(self.path, modulus=10, window_seconds=60.0,
                        rebuild=True)
        token = s.ledger.resume_token("gw1")
        # 最大 cursor 为 201（重复报文也推进游标）
        self.assertEqual(token["last_cursor"], 201)
        s.close()


if __name__ == "__main__":
    unittest.main()
