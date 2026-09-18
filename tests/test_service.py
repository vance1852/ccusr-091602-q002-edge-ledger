"""服务门面：坏报文隔离留证，不污染数据流。"""
import os
import tempfile
import unittest

from app.persistence import LedgerStore
from app.service import TrustService


class TrustServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "svc.jsonl")
        self.svc = TrustService(LedgerStore(self.path, modulus=10))

    def tearDown(self):
        self.svc.store.close()
        self.tmp.cleanup()

    def test_nan_value_is_quarantined_with_raw_text(self):
        r = self.svc.ingest_packet({
            "device_id": "d", "seq": 1, "metric": "x", "value": "NaN",
            "device_ts": 5.0, "gateway_id": "g", "recv_ts": 6.0})
        self.assertEqual(r["verdict"], "quarantined")
        self.assertEqual(r["raw"]["payload"]["value"], "NaN")
        self.assertFalse(r["accepted"])

    def test_missing_required_field_is_quarantined(self):
        r = self.svc.ingest_packet({
            "device_id": "d", "seq": 1, "metric": "x", "value": 1.0,
            "gateway_id": "g"})
        self.assertEqual(r["verdict"], "quarantined")
        self.assertIn("recv_ts", r["reason"])

    def test_good_packet_accepted_and_queryable(self):
        r = self.svc.ingest_packet({
            "device_id": "d", "seq": 1, "metric": "x", "value": 12.0,
            "device_ts": 100.0, "gateway_id": "g", "recv_ts": 101.0})
        self.assertEqual(r["verdict"], "accepted")
        ev = self.svc.explain(r["normalized"]["event_id"])
        self.assertEqual(ev["raw_value"], 12.0)


if __name__ == "__main__":
    unittest.main()
