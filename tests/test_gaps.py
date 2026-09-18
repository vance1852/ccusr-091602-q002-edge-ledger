"""缺口生命周期与续传游标测试，含服务重启后的持久性。"""

import os
import tempfile
import unittest

from app.pipeline import Pipeline
from app.store import Store
from helpers import BASE_MS, DEVICE, POINT, Clock, make_pipeline, msg, register_point


class GapTest(unittest.TestCase):
    def setUp(self):
        self.pipe, self.store, self.clock = make_pipeline()
        register_point(self.pipe)

    def tearDown(self):
        self.store.close()

    def _ingest(self, seq, offset_s):
        self.clock.set_ms(BASE_MS + int(offset_s * 1000) + 200)
        return self.pipe.ingest_one(msg(seq, BASE_MS + int(offset_s * 1000)))

    def test_gap_opens_on_jump_and_closes_on_backfill(self):
        self._ingest(1, 0)
        self._ingest(2, 5)
        self._ingest(4, 15)  # 跳过 seq 3 → 缺口开口
        gaps = self.store.gaps_for_point(POINT)
        self.assertEqual(len(gaps), 1)
        self.assertEqual((gaps[0]["from_seq"], gaps[0]["to_seq"]), (3, 3))
        self.assertEqual(gaps[0]["status"], "open")
        self._ingest(3, 10)  # 补传到达 → 缺口闭合
        gaps = self.store.gaps_for_point(POINT)
        self.assertEqual(gaps[0]["status"], "closed")
        self.assertIsNotNone(gaps[0]["closed_at_ms"])

    def test_cursor_tracks_contiguous_watermark(self):
        self._ingest(1, 0)
        self._ingest(3, 10)  # 缺口 [2,2]
        cursor = self.pipe.resume_cursor(DEVICE, POINT)
        self.assertEqual(cursor["last_contiguous_seq"], 1)
        self.assertEqual(cursor["last_seen_seq"], 3)
        self.assertEqual(cursor["open_gaps"], [{"from_seq": 2, "to_seq": 2}])
        self.assertEqual(cursor["resume_from_seq"], 4)
        self._ingest(2, 5)   # 补传填缺 → 连续水位推进
        cursor = self.pipe.resume_cursor(DEVICE, POINT)
        self.assertEqual(cursor["last_contiguous_seq"], 3)
        self.assertEqual(cursor["open_gaps"], [])

    def test_cursor_unknown_device(self):
        cursor = self.pipe.resume_cursor("no-such", POINT)
        self.assertFalse(cursor["known"])


class RestartDurabilityTest(unittest.TestCase):
    """服务重启（关闭并重开同一账本文件）后游标与事件不丢失。"""

    def test_restart_preserves_cursor_and_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "ledger.db")
            clock = Clock()
            store = Store(db)
            pipe = Pipeline(store, clock=clock)
            register_point(pipe)
            for seq in range(1, 6):
                clock.set_ms(BASE_MS + seq * 1000 + 200)
                pipe.ingest_one(msg(seq, BASE_MS + seq * 1000))
            cursor_pre = pipe.resume_cursor(DEVICE, POINT)
            events_pre = store.events_in_range(POINT, 0, BASE_MS + 60_000)
            store.close()

            # 重启：新 Store / Pipeline，同一文件
            store2 = Store(db)
            pipe2 = Pipeline(store2, clock=clock)
            cursor_post = pipe2.resume_cursor(DEVICE, POINT)
            events_post = store2.events_in_range(POINT, 0, BASE_MS + 60_000)
            self.assertEqual(cursor_pre, cursor_post)
            self.assertEqual(
                [e["event_id"] for e in events_pre],
                [e["event_id"] for e in events_post],
            )
            # 重启后继续接入，序号连续性不丢
            clock.set_ms(BASE_MS + 6_500)
            out = pipe2.ingest_one(msg(6, BASE_MS + 6_000))
            self.assertEqual(out.result.value, "accepted")
            cursor = pipe2.resume_cursor(DEVICE, POINT)
            self.assertEqual(cursor["last_contiguous_seq"], 6)
            store2.close()


if __name__ == "__main__":
    unittest.main()
