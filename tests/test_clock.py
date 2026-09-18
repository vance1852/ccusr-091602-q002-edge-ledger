"""时钟修正依据与校准沉淀期。"""
import unittest

from app.clock import ClockRegistry
from app.model import CalibrationRecord, TimeQuality


class ClockModelTests(unittest.TestCase):
    def setUp(self):
        self.reg = ClockRegistry(settle_seconds=2.0)
        # 设备时钟比服务器慢 10 秒
        self.reg.add_calibration(CalibrationRecord(
            "cal1", "devA", at_server=1100.0, at_device=1090.0,
            offset_ms=10000.0, drift_ppm=0.0, source="ntp", recv_ts=1100.0))

    def test_no_calibration_uses_device_quality(self):
        reg = ClockRegistry()
        d = reg.model_for("fresh", device_ts=500.0, recv_ts=501.0)
        self.assertEqual(d.quality, TimeQuality.DEVICE.value)
        self.assertAlmostEqual(d.event_time, 500.0)
        self.assertEqual(d.model.source, "uncalibrated")

    def test_forward_extrapolation_applies_offset(self):
        d = self.reg.model_for("devA", device_ts=1095.0, recv_ts=1105.0)
        self.assertEqual(d.quality, TimeQuality.CORRECTED.value)
        self.assertAlmostEqual(d.event_time, 1105.0)
        self.assertEqual(d.model.source, "extrapolation")
        self.assertIn("向前外推", d.model.basis)

    def test_backward_extrapolation_for_older_reading(self):
        d = self.reg.model_for("devA", device_ts=1000.0, recv_ts=1105.0)
        self.assertAlmostEqual(d.event_time, 1010.0)
        self.assertIn("time-correction-extrapolated", d.annotations)

    def test_settle_window_is_estimated(self):
        d = self.reg.model_for("devA", device_ts=1091.0, recv_ts=1101.0)
        self.assertEqual(d.quality, TimeQuality.ESTIMATED.value)
        self.assertIn("clock-resync-settling", d.annotations)

    def test_interpolation_between_two_calibrations(self):
        # 第二条校时：偏差收窄到 6 秒（设备在追赶/曾被调整）
        self.reg.add_calibration(CalibrationRecord(
            "cal2", "devA", at_server=1200.0, at_device=1194.0,
            offset_ms=6000.0, drift_ppm=0.0, source="ntp", recv_ts=1200.0))
        # 两锚点设备时间 1090(off10) 与 1194(off6)，中点 1142 -> off8
        d = self.reg.model_for("devA", device_ts=1142.0, recv_ts=1201.0)
        self.assertEqual(d.model.source, "interpolation")
        self.assertAlmostEqual(d.event_time, 1150.0, places=3)
        self.assertIn("分段线性插值", d.model.basis)
        # 同一区间不同点复用同一模型 ID，但各自偏差不同
        d2 = self.reg.model_for("devA", device_ts=1168.0, recv_ts=1201.0)
        self.assertEqual(d2.model.model_id, d.model.model_id)
        self.assertAlmostEqual(d2.event_time, 1175.0, places=3)

    def test_missing_device_ts_falls_back_to_server_time(self):
        d = self.reg.model_for("devA", device_ts=None, recv_ts=1300.0)
        self.assertEqual(d.quality, TimeQuality.UNKNOWN.value)
        self.assertEqual(d.event_time, 1300.0)
        self.assertEqual(d.model.source, "server-clock")

    def test_model_is_persistable_snapshot(self):
        d = self.reg.model_for("devA", device_ts=1095.0, recv_ts=1105.0)
        snap = d.model.to_dict()
        self.assertEqual(snap["offset_seconds"], 10.0)
        self.assertIn("basis", snap)


if __name__ == "__main__":
    unittest.main()
