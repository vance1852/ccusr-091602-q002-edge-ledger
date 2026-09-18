"""端到端现场场景：断网缓存补传 → 序号回绕 → 双网关碰撞 → 缺口续传 → 重启。

查询结果必须同时呈现：唯一事件、时间修正依据、数据缺口、版本血缘，
而不是一条被“洗平”的曲线。
"""
import os
import tempfile
import unittest

from app.model import CalibrationRecord, PointConfig, RawMessage
from app.persistence import LedgerStore


def m(seq, *, value, dts, gw, rcv, cursor, gen=0):
    return RawMessage(
        device_id="devA", seq=seq, metric="temp", value=value,
        device_ts=dts, gateway_id=gw, recv_ts=rcv, gen=gen,
        source_cursor=cursor)


class PlantFloorScenarioTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "scene.jsonl")
        self.s = LedgerStore(self.path, modulus=10, window_seconds=60.0)
        self.L = self.s.ledger
        self.L.configs.register(PointConfig(
            "devA", "temp", "cfg-v1", 0.0, 1.0, 0.0, "C", "A区1号线",
            "初装", 900.0))

    def tearDown(self):
        self.s.close()
        self.tmp.cleanup()

    def test_full_incident_and_restart(self):
        L, s = self.L, self.s
        # 校时：设备时钟慢 10 秒
        s.add_calibration(CalibrationRecord(
            "cal1", "devA", 1010.0, 1000.0, 10000.0, 0.0, "ntp", 1010.0))

        # ---- 断网前：seq1,2 实时到达（设备 1001/1002 -> 事件 1011/1012）
        s.ingest(m(1, value=50.0, dts=1001.0, gw="gw-a", rcv=1011.0,
                   cursor=1001), at=1011.0)
        s.ingest(m(2, value=51.0, dts=1002.0, gw="gw-a", rcv=1012.0,
                   cursor=1002), at=1012.0)

        # ---- 厂区网络抖动，数小时缓存压缩成补传突发（recv 集中在 1060 后）
        # seq3 经双网关冗余转发，内容一致 -> duplicate
        s.ingest(m(3, value=52.0, dts=1003.0, gw="gw-a", rcv=1060.1,
                   cursor=3003), at=1060.1)
        dup = s.ingest(m(3, value=52.0, dts=1003.0, gw="gw-b",
                         rcv=1060.2, cursor=8003), at=1060.2)
        self.assertEqual(dup.verdict, "duplicate")
        self.assertIn("redundant-gateway-forward", dup.annotations)

        # seq4 双网关各自声称不同载荷 -> sequence_conflict，双方留证
        win4 = s.ingest(m(4, value=53.0, dts=1004.0, gw="gw-a",
                          rcv=1060.3, cursor=3004), at=1060.3)
        clash = s.ingest(m(4, value=99.9, dts=1004.0, gw="gw-b",
                           rcv=1060.4, cursor=8004), at=1060.4)
        self.assertEqual(clash.verdict, "sequence_conflict")

        # seq5 到达，seq6 在网关侧丢失，seq7..9 继续
        s.ingest(m(5, value=54.0, dts=1005.0, gw="gw-a", rcv=1060.5,
                   cursor=3005), at=1060.5)
        for seq, dts in [(7, 1007.0), (8, 1008.0), (9, 1009.0)]:
            s.ingest(m(seq, value=50.0 + seq, dts=dts, gw="gw-a",
                       rcv=1060.5 + seq * 0.01, cursor=3000 + seq),
                     at=1060.5 + seq * 0.01)

        # 序号回绕：seq0/1/2（cycle 1），设备时间继续向前
        w0 = s.ingest(m(0, value=60.0, dts=1010.0, gw="gw-a",
                        rcv=1061.0, cursor=3100), at=1061.0)
        w1 = s.ingest(m(1, value=61.0, dts=1011.0, gw="gw-a",
                        rcv=1061.1, cursor=3101), at=1061.1)
        w2 = s.ingest(m(2, value=62.0, dts=1012.0, gw="gw-a",
                        rcv=1061.2, cursor=3102), at=1061.2)
        self.assertEqual([w.normalized.cycle for w in (w0, w1, w2)],
                         [1, 1, 1])
        # 回绕后的事件时间：设备 1010..1012 +10s 偏差
        self.assertAlmostEqual(w0.normalized.event_time, 1020.0)

        # ---- 缺口台账：seq6 缺失；重连游标提示续传
        token = L.resume_token("gw-a")
        self.assertTrue(any(
            item["seq"] == 6 and item["cycle"] == 0
            for g in token["open_gaps"] for item in g["missing"]))
        gap6 = next(g for g in L.gaps("devA", "temp")
                    if any(l == 6 for l in g.missing))
        self.assertEqual(gap6.missing, [6])
        self.assertTrue(token["hint"])

        # ---- 封存生产窗口（960-1020 与 1020-1080）
        sealed = s.seal_eligible(now=1080.0)
        sealed_ids = {r.window_id for r in sealed}
        self.assertIn("win:devA:temp:960", sealed_ids)
        self.assertIn("win:devA:temp:1020", sealed_ids)
        v1 = L.versions("win:devA:temp:960")[0]
        self.assertEqual(len(v1.event_ids), 8)   # 1,2,3,4,5,7,8,9

        # ---- 网关于 1090 找回丢失的 seq6 补传：落入已封存窗 -> late -> v2
        late = s.ingest(m(6, value=55.0, dts=1006.0, gw="gw-a",
                          rcv=1090.0, cursor=3206), at=1090.0)
        self.assertEqual(late.verdict, "late")
        versions = L.versions("win:devA:temp:960")
        self.assertEqual([v.version for v in versions], [1, 2])
        self.assertEqual(versions[0].state, "sealed")
        self.assertEqual(versions[1].state, "revised")
        self.assertEqual(len(versions[0].event_ids), 8)   # 封存版不变
        self.assertEqual(len(versions[1].event_ids), 9)
        self.assertEqual(versions[1].affected_metrics, ("temp",))
        self.assertIn("evt:devA:temp:g0c0:6",
                      versions[1].introduced_event_ids)
        self.assertTrue(all(g.status == "resolved"
                            for g in L.gaps("devA", "temp")
                            if 6 in [f[0] for f in g.fillers] or not g.missing))

        # 冲突台账可查，败方原始报文可调阅
        conflicts = L.conflicts("devA")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(L.get_entry(conflicts[0]["loser_entry_id"])
                         .raw.value, 99.9)

        # ---- 按事件时间回放：9 个唯一事件，按修正后事件时间排序，带依据
        replay = L.replay_events("devA", "temp", 1011.0, 1020.0)
        self.assertEqual(len(replay["events"]), 9)
        self.assertEqual([e["event_time"] for e in replay["events"]],
                         [1011.0 + i for i in range(9)])
        for e in replay["events"]:
            self.assertIn("cal1", e["clock_model_id"])
            self.assertTrue(e["clock_basis"])
            self.assertEqual(e["config_version"], "cfg-v1")
            self.assertEqual(e["location"], "A区1号线")

        # ---- 服务重启：WAL 重放后证据全貌逐位一致
        s.close()
        s2 = LedgerStore(self.path, modulus=10, window_seconds=60.0,
                         rebuild=True)
        L2 = s2.ledger
        verdicts = {}
        for e in L2.entries():
            verdicts.setdefault(e.verdict, 0)
            verdicts[e.verdict] += 1
        self.assertEqual(verdicts["accepted"], 11)   # 1,2,3,4,5,7,8,9 + c1:0,1,2
        self.assertEqual(verdicts["duplicate"], 1)
        self.assertEqual(verdicts["sequence_conflict"], 1)
        self.assertEqual(verdicts["late"], 1)

        v_after = L2.versions("win:devA:temp:960")
        self.assertEqual(len(v_after[0].event_ids), 8)
        self.assertEqual(len(v_after[1].event_ids), 9)
        lineage = L2.lineage("win:devA:temp:960")
        self.assertEqual([n["version"] for n in lineage], [1, 2])
        self.assertEqual(lineage[1]["parent_version"], 1)

        # v1 历史版本仍可回放（旧曲线），v2 为修订版
        old = L2.replay_window("win:devA:temp:960", version=1)
        new = L2.replay_window("win:devA:temp:960", version=2)
        self.assertEqual(len(old["events"]), 8)
        self.assertEqual(len(new["events"]), 9)
        self.assertEqual(old["aggregates"]["temp"]["count"], 8)
        self.assertEqual(new["aggregates"]["temp"]["count"], 9)
        s2.close()


if __name__ == "__main__":
    unittest.main()
