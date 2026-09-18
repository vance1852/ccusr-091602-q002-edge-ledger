"""端到端场景测试：断网补传、序号回绕、双网关碰撞、服务重启的不变量。"""

import os
import tempfile
import unittest

from app.demo import run_demo


class DemoScenarioTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.report = run_demo(os.path.join(cls.tmp, "demo.db"))

    @classmethod
    def tearDownClass(cls):
        cls.report["store"].close()

    def test_delivery_classification_counts(self):
        counts = self.report["delivery_result_counts"]
        self.assertEqual(counts.get("duplicate"), 2)            # gw-b 重复投递 + gw-a 重试
        self.assertEqual(counts.get("sequence_conflict"), 1)    # 被篡改的 65532
        self.assertEqual(counts.get("quarantined"), 2)          # 未登记点位 + 无时间戳
        self.assertEqual(counts.get("late"), 7)                 # 6 条补传 + 旧代际迟到 65519

    def test_peak_order_restored_by_clock_correction(self):
        peaks = self.report["peaks"]
        self.assertTrue(peaks["device_stamp_order_reversed"])   # 设备打戳顺序颠倒
        self.assertTrue(peaks["corrected_order_ok"])            # 修正后恢复真实顺序
        self.assertEqual(peaks["peak_a"]["time_quality"], "corrected")
        self.assertEqual(peaks["peak_a"]["offset_applied_ms"], -7900.0)
        self.assertGreaterEqual(len(self.report["clock_models"]), 3)  # 漂移升版留痕

    def test_sealed_window_revised_not_rewritten(self):
        lineage = self.report["lineage_late_window"]
        revisions = lineage["revisions"]
        self.assertEqual(len(revisions), 8)                     # 1 基线 + 7 迟到修订
        baseline = revisions[0]
        self.assertEqual(baseline["metrics"]["count"], 5)       # 基线不被改写
        self.assertEqual(baseline["metrics"]["max"], 10.5)
        # 峰值补传触发的修订标明了受影响指标
        peak_rev = next(r for r in revisions if r["metrics"]["max"] == 45.0)
        self.assertEqual(
            peak_rev["affected_metrics"]["max"], {"old": 10.5, "new": 45.0}
        )
        self.assertEqual(len(peak_rev["caused_by"]), 1)
        # 血缘：触发事件 → 原始报文 → 投递记录
        cause = peak_rev["caused_by"][0]
        bundle = lineage["caused_events"][cause]
        self.assertIn("payload_json", bundle["raw_message"])
        self.assertGreaterEqual(len(bundle["deliveries"]), 1)

    def test_gaps_opened_during_outage_all_closed(self):
        gaps = self.report["gaps"]
        self.assertTrue(gaps)
        self.assertTrue(all(g["status"] == "closed" for g in gaps))
        # 断网期间开口、补传后闭合，开口时间早于闭合时间
        for g in gaps:
            self.assertLessEqual(g["detected_at_ms"], g["closed_at_ms"])

    def test_epoch_wrap_and_straggler(self):
        self.assertEqual(self.report["straggler_result"]["result"], "late")
        cursor = self.report["final_cursor"]
        self.assertEqual(cursor["device_epoch"], 1)             # 回绕进入新代际
        self.assertEqual(cursor["open_gaps"], [])               # 旧代际缺口也被填平

    def test_restart_consistency(self):
        restart = self.report["restart"]
        self.assertTrue(restart["cursor_consistent"])
        self.assertTrue(restart["replay_consistent"])

    def test_replay_versions_differ(self):
        rc = self.report["replay_compare"]
        self.assertEqual(rc["events_visible_before_backfill"], 29)  # 补传前只有正常流
        self.assertGreater(rc["events_visible_now"], rc["events_visible_before_backfill"])

    def test_calibration_and_config_versions(self):
        cal = self.report["calibration_events"]
        self.assertTrue(cal)
        self.assertTrue(all(e["config_version"] == 2 for e in cal))
        self.assertTrue(all("calibration" in e["flags"] for e in cal))
        interp = {c["seq"]: c["config_version"] for c in self.report["config_interpretation"]}
        self.assertEqual(interp[20], 1)   # 配置 v1 时期
        self.assertEqual(interp[30], 2)   # 校准参数变更后
        self.assertEqual(interp[80], 3)   # 移机后


if __name__ == "__main__":
    unittest.main()
