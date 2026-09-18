"""幂等边界与判定分类测试：重复、冲突、隔离、乱序、补传、代际回绕。"""

import unittest

from app.models import (
    FLAG_BACKFILLED,
    FLAG_OUT_OF_ORDER,
    EventResult,
)
from helpers import BASE_MS, DEVICE, GW, POINT, make_pipeline, msg, register_point


class IngestTest(unittest.TestCase):
    def setUp(self):
        self.pipe, self.store, self.clock = make_pipeline()
        register_point(self.pipe)

    def tearDown(self):
        self.store.close()

    def test_first_delivery_accepted(self):
        out = self.pipe.ingest_one(msg(1, BASE_MS))
        self.assertEqual(out.result, EventResult.ACCEPTED)
        self.assertEqual(self.store.count_events(), 1)

    def test_same_message_redelivered_is_duplicate(self):
        m = msg(1, BASE_MS)
        self.pipe.ingest_one(m)
        out = self.pipe.ingest_one(m)
        self.assertEqual(out.result, EventResult.DUPLICATE)
        self.assertEqual(out.reason, "same_message_redelivered")
        # 幂等：规范事件仍只有一条，投递留痕两条
        self.assertEqual(self.store.count_events(), 1)
        self.assertEqual(self.store.count_deliveries(), 2)

    def test_dual_gateway_same_message_is_duplicate(self):
        m1 = msg(1, BASE_MS, gateway="gw-a")
        m2 = msg(1, BASE_MS, gateway="gw-b")  # 同一报文经备网关再投递
        self.pipe.ingest_one(m1)
        out = self.pipe.ingest_one(m2)
        self.assertEqual(out.result, EventResult.DUPLICATE)
        self.assertEqual(self.store.count_events(), 1)
        # 证据层可见两个网关的投递记录
        deliveries = self.store.deliveries_for(out.raw_id)
        self.assertEqual({d["gateway_id"] for d in deliveries}, {"gw-a", "gw-b"})

    def test_same_sequence_different_payload_is_conflict(self):
        self.pipe.ingest_one(msg(1, BASE_MS, value=1.0))
        out = self.pipe.ingest_one(msg(1, BASE_MS, value=999.0))
        self.assertEqual(out.result, EventResult.SEQUENCE_CONFLICT)
        self.assertEqual(self.store.count_events(), 1)  # 先入者为准，冲突留证不入流

    def test_unknown_point_quarantined(self):
        out = self.pipe.ingest_one(msg(1, BASE_MS, point="ghost"))
        self.assertEqual(out.result, EventResult.QUARANTINED)
        self.assertEqual(out.reason, "unknown_point")
        self.assertEqual(self.store.count_events(), 0)
        self.assertIsNotNone(self.store.get_raw(out.raw_id))  # 原文留证

    def test_bad_value_quarantined(self):
        out = self.pipe.ingest_one(msg(1, BASE_MS, payload={"value": "abc"}))
        self.assertEqual(out.result, EventResult.QUARANTINED)
        self.assertEqual(out.reason, "bad_value")

    def test_no_timestamp_quarantined(self):
        out = self.pipe.ingest_one(msg(1, BASE_MS, with_timestamps=False))
        self.assertEqual(out.result, EventResult.QUARANTINED)
        self.assertEqual(out.reason, "no_usable_timestamp")

    def test_future_timestamp_quarantined(self):
        out = self.pipe.ingest_one(msg(1, BASE_MS + 10 * 60 * 1000))
        self.assertEqual(out.result, EventResult.QUARANTINED)
        self.assertEqual(out.reason, "future_timestamp")

    def test_out_of_order_flag(self):
        self.pipe.ingest_one(msg(5, BASE_MS + 5000))
        out = self.pipe.ingest_one(msg(4, BASE_MS + 4000))
        self.assertEqual(out.result, EventResult.ACCEPTED)
        self.assertIn(FLAG_OUT_OF_ORDER, out.flags)

    def test_backfilled_flag(self):
        self.clock.set_ms(BASE_MS + 10 * 60 * 1000)  # 10 分钟后才到达
        out = self.pipe.ingest_one(msg(1, BASE_MS))
        self.assertIn(FLAG_BACKFILLED, out.flags)

    def test_epoch_wrap_inferred(self):
        self.pipe.ingest_one(msg(65534, BASE_MS))
        self.pipe.ingest_one(msg(65535, BASE_MS + 1000))
        out = self.pipe.ingest_one(msg(0, BASE_MS + 2000))  # 回绕
        self.assertEqual(out.result, EventResult.ACCEPTED)
        events = self.store.events_in_range(POINT, 0, BASE_MS + 60_000)
        self.assertEqual([e["device_epoch"] for e in events], [0, 0, 1])
        # 回绕后同序号不与旧代际冲突
        out = self.pipe.ingest_one(msg(1, BASE_MS + 3000))
        self.assertEqual(out.result, EventResult.ACCEPTED)

    def test_old_epoch_straggler_after_wrap(self):
        self.pipe.ingest_one(msg(65534, BASE_MS))
        self.pipe.ingest_one(msg(65535, BASE_MS + 1000))
        self.pipe.ingest_one(msg(0, BASE_MS + 2000))
        # 旧代际迟到报文（序号接近上限）应归代际 0
        out = self.pipe.ingest_one(msg(65533, BASE_MS - 1000))
        self.assertEqual(out.result, EventResult.ACCEPTED)
        ev = self.store.events_in_range(POINT, 0, BASE_MS + 60_000)
        by_seq = {e["device_seq"]: e for e in ev}
        self.assertEqual(by_seq[65533]["device_epoch"], 0)

    def test_explicit_epoch_respected(self):
        self.pipe.ingest_one(msg(5, BASE_MS, epoch=3))
        ev = self.store.events_in_range(POINT, 0, BASE_MS + 60_000)
        self.assertEqual(ev[0]["device_epoch"], 3)

    def test_raw_payload_preserved_verbatim(self):
        payload = {"value": 3.14, "raw_register": "0x1F", "note": "含中文"}
        out = self.pipe.ingest_one(msg(1, BASE_MS, payload=payload))
        raw = self.store.get_raw(out.raw_id)
        import json

        self.assertEqual(json.loads(raw["payload_json"]), payload)


if __name__ == "__main__":
    unittest.main()
