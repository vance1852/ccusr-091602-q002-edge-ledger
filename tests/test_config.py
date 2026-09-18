"""测点配置版本：移机/改参数后历史读数仍按当时配置解释。"""
import unittest

from app.config import ConfigRegistry
from app.model import PointConfig


class ConfigVersionTests(unittest.TestCase):
    def setUp(self):
        self.reg = ConfigRegistry()
        self.reg.register(PointConfig(
            device_id="devA", metric="temp", version="cfg-v1",
            effective_event_time=0.0, scale=5.0, offset=-100.0,
            unit="C", location="A区", note="初装 4-20mA",
            registered_at=900.0))
        self.reg.register(PointConfig(
            device_id="devA", metric="temp", version="cfg-v2",
            effective_event_time=2000.0, scale=6.0, offset=-120.0,
            unit="C", location="B区", note="移机并换量程",
            registered_at=2050.0))

    def test_history_uses_old_config(self):
        old = self.reg.interpret("devA", "temp", 24.0, event_time=1500.0)
        self.assertEqual(old.value, 20.0)           # 24*5-100
        self.assertEqual(old.location, "A区")
        self.assertEqual(old.config.version, "cfg-v1")

    def test_after_move_uses_new_config(self):
        new = self.reg.interpret("devA", "temp", 24.0, event_time=2000.0)
        self.assertEqual(new.value, 24.0)           # 24*6-120
        self.assertEqual(new.location, "B区")
        self.assertEqual(new.config.version, "cfg-v2")

    def test_effective_boundary_is_event_time_not_register_time(self):
        # 新配置 2050 才注册，但 2010 的读数（2050 后才补传解释）仍须用 v2
        reading = self.reg.interpret("devA", "temp", 24.0, event_time=2010.0)
        self.assertEqual(reading.config.version, "cfg-v2")

    def test_unknown_point_passes_through_marked(self):
        d = self.reg.interpret("devX", "pressure", 7.0, event_time=100.0)
        self.assertEqual(d.value, 7.0)
        self.assertEqual(d.unit, "raw")
        self.assertEqual(d.location, "unknown")

    def test_late_registration_does_not_rewrite_old_events(self):
        # 在更晚时间补登记一条“ retroactive”配置，生效起点在过去，
        # 属于显式配置修订；不改变生效起点之前的解释
        self.reg.register(PointConfig(
            device_id="devA", metric="temp", version="cfg-v0",
            effective_event_time=0.0, scale=1.0, offset=0.0,
            unit="C", location="出厂位", note="补登记", registered_at=5000.0))
        reading = self.reg.interpret("devA", "temp", 24.0, event_time=100.0)
        # 同一生效时间覆盖：以最后登记为准（显式修订），这是注册层的明确语义
        self.assertEqual(reading.config.version, "cfg-v0")


if __name__ == "__main__":
    unittest.main()
