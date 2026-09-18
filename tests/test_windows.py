"""生产窗口测试：封存、迟到修订、修订版不可改写、受影响指标。"""

import unittest

from app.models import EventResult, WindowState
from app.windows import seal_due_windows, window_id_of
from helpers import BASE_MS, POINT, make_pipeline, msg, register_point

W0 = window_id_of(POINT, BASE_MS)  # 窗口 [BASE, BASE+60s)


class WindowTest(unittest.TestCase):
    def setUp(self):
        self.pipe, self.store, self.clock = make_pipeline()
        register_point(self.pipe)  # window 60s, lateness 30s

    def tearDown(self):
        self.store.close()

    def _ingest(self, seq, offset_s, value):
        self.clock.set_ms(BASE_MS + int(offset_s * 1000) + 200)
        return self.pipe.ingest_one(msg(seq, BASE_MS + int(offset_s * 1000), value))

    def test_seal_at_horizon_creates_baseline(self):
        self._ingest(1, 0, 10.0)
        self._ingest(2, 10, 20.0)
        self.clock.set_ms(BASE_MS + 91_000)  # end 60s + lateness 30s = 90s
        sealed = seal_due_windows(self.store, self.clock.now)
        self.assertEqual([s["window_id"] for s in sealed], [W0])
        with self.store.read() as conn:
            window = self.store.get_window(conn, W0)
        self.assertEqual(window["state"], WindowState.SEALED.value)
        revs = self.store.revisions(W0)
        self.assertEqual(len(revs), 1)
        self.assertEqual(revs[0]["metrics"]["count"], 2)
        self.assertEqual(revs[0]["metrics"]["max"], 20.0)
        self.assertEqual(revs[0]["affected_metrics"], {})

    def test_late_event_creates_revision_with_affected_metrics(self):
        self._ingest(1, 0, 10.0)
        self._ingest(2, 10, 20.0)
        self.clock.set_ms(BASE_MS + 91_000)
        seal_due_windows(self.store, self.clock.now)
        # 迟到事件落入已封存窗口
        self.clock.set_ms(BASE_MS + 95_000)
        out = self.pipe.ingest_one(msg(3, BASE_MS + 30_000, 99.0))
        self.assertEqual(out.result, EventResult.LATE)
        revs = self.store.revisions(W0)
        self.assertEqual(len(revs), 2)
        rev2 = revs[1]
        self.assertEqual(rev2["supersedes"], 1)
        self.assertEqual(rev2["caused_by"], [out.event_id])
        self.assertEqual(rev2["affected_metrics"]["max"], {"old": 20.0, "new": 99.0})
        self.assertEqual(rev2["affected_metrics"]["count"], {"old": 2, "new": 3})
        with self.store.read() as conn:
            window = self.store.get_window(conn, W0)
        self.assertEqual(window["state"], WindowState.REVISED.value)

    def test_sealed_revision_is_immutable(self):
        self._ingest(1, 0, 10.0)
        self.clock.set_ms(BASE_MS + 91_000)
        seal_due_windows(self.store, self.clock.now)
        self.clock.set_ms(BASE_MS + 95_000)
        self.pipe.ingest_one(msg(2, BASE_MS + 30_000, 99.0))
        self.pipe.ingest_one(msg(3, BASE_MS + 40_000, 55.0))
        revs = self.store.revisions(W0)
        self.assertEqual(len(revs), 3)
        # 基线修订版不被迟到数据改写
        self.assertEqual(revs[0]["metrics"]["count"], 1)
        self.assertEqual(revs[0]["metrics"]["max"], 10.0)
        self.assertEqual(revs[1]["metrics"]["max"], 99.0)
        self.assertEqual(revs[2]["metrics"]["max"], 99.0)
        self.assertEqual(revs[2]["metrics"]["count"], 3)

    def test_past_horizon_open_window_sealed_then_revised(self):
        """窗口从未封存但已过迟到视野：先补基线（不含本条），再修订（含本条）。"""
        self._ingest(1, 0, 10.0)
        # 不调用 seal_due，迟到报文直接到达
        self.clock.set_ms(BASE_MS + 95_000)
        out = self.pipe.ingest_one(msg(2, BASE_MS + 30_000, 99.0))
        self.assertEqual(out.result, EventResult.LATE)
        revs = self.store.revisions(W0)
        self.assertEqual(len(revs), 2)
        self.assertEqual(revs[0]["metrics"]["count"], 1)   # 基线不含迟到事件
        self.assertEqual(revs[1]["metrics"]["count"], 2)   # 修订含迟到事件
        self.assertEqual(revs[1]["caused_by"], [out.event_id])

    def test_open_window_accepts_without_revision(self):
        out = self._ingest(1, 0, 10.0)
        self.assertEqual(out.result, EventResult.ACCEPTED)
        self.assertEqual(self.store.revisions(W0), [])


if __name__ == "__main__":
    unittest.main()
