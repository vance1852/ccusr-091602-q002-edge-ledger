"""端到端演示：网络抖动后的遥测可信账本。

场景（基准时刻 2026-09-18T10:00:00Z，点位 press-01，设备 plc-7）：

  1. 正常流      seq 65490..65518 实时到达（设备计数器从 65490 起）
  2. 窗口封存    t+181s 封存头两个一分钟窗口
  3. 断网缓存    网关缓存 seq 65520..65535 与回绕后的 0..7（uint16 回绕）
  4. 重连补传    先查询续传游标，再乱序突发上传；夹带双网关碰撞
                 （gw-b 重复投递同一报文 + 一条载荷被篡改的冲突报文）
  5. 旧代际迟到  卡在重试缓冲里的 seq 65519 在回绕之后才到达
  6. 时钟漂移    设备时钟突然 +8s，两个峰值按设备时间顺序颠倒，
                 按时钟模型修正后恢复真实顺序
  7. 校准期      配置 v2 生效：标定参数变更 + 校准窗口，读数打标
  8. 移机        配置 v3 生效：位置变更，历史读数仍按当时配置解释
  9. 服务重启    关闭并重开账本，游标与回放结果不变，继续接入
 10. 终态封存    推进时间封存全部窗口，输出血缘报告

运行：python -m app.demo [--db /tmp/demo.db]
"""
from __future__ import annotations

import argparse
import os
import tempfile

from .models import IncomingMessage, iso_to_ms, ms_to_iso
from .pipeline import Pipeline
from .store import Store
from .windows import seal_due_windows

BASE_MS = iso_to_ms("2026-09-18T10:00:00Z")
POINT = "press-01"
DEVICE = "plc-7"
GW_A = "gw-a"
GW_B = "gw-b"


class ScenarioClock:
    """可推进的场景时钟（毫秒）。"""

    def __init__(self):
        self.now = BASE_MS

    def set_s(self, offset_s: float) -> None:
        self.now = BASE_MS + int(offset_s * 1000)

    def __call__(self) -> int:
        return self.now


def msg(
    seq: int,
    t_s: float,
    value: float,
    gateway: str = GW_A,
    drift_ms: int = 0,
    point: str = POINT,
    with_timestamps: bool = True,
) -> IncomingMessage:
    """构造一条报文：设备采集时刻 t_s（可加漂移），网关 100ms 后收到。"""
    collected = BASE_MS + int(t_s * 1000) + drift_ms if with_timestamps else None
    received = BASE_MS + int(t_s * 1000) + 100 if with_timestamps else None
    return IncomingMessage(
        gateway_id=gateway,
        device_id=DEVICE,
        point_id=point,
        device_seq=seq,
        collected_at_ms=collected,
        gateway_received_at_ms=received,
        payload={"value": value, "unit": "MPa"},
    )


def live_value(seq: int) -> float:
    """正常工况读数（含两个工况峰值在补传段注入）。"""
    return round(10.0 + (seq % 7) * 0.1, 2)


def run_demo(db_path: str) -> dict:
    """执行全部场景，返回结构化报告（测试可直接断言）。"""
    if os.path.exists(db_path):
        os.remove(db_path)
    for suffix in ("-wal", "-shm"):
        if os.path.exists(db_path + suffix):
            os.remove(db_path + suffix)

    clock = ScenarioClock()
    store = Store(db_path)
    pipe = Pipeline(store, clock=clock, backfill_threshold_ms=30_000)
    report: dict = {"phases": []}

    def phase(name: str) -> None:
        report["phases"].append(name)

    # ---- 0. 登记点位配置 v1 --------------------------------------------
    pipe.register_point_config(
        {
            "point_id": POINT,
            "effective_from_ms": BASE_MS - 3600_000,
            "unit": "MPa",
            "scale": 1.0,
            "offset": 0.0,
            "location": "车间A/1号线",
            "window_seconds": 60,
            "lateness_seconds": 30,
        }
    )
    phase("config_v1_registered")

    # ---- 1. 正常流 seq 65490..65518 ------------------------------------
    for i, seq in enumerate(range(65490, 65519)):
        t = i * 5.0
        clock.set_s(t + 0.2)
        pipe.ingest_one(msg(seq, t, live_value(seq)))
    phase("live_stream_65490_65518")

    # ---- 2. 封存头两个窗口 ---------------------------------------------
    clock.set_s(181)
    seal_due_windows(store, clock.now)
    phase("first_windows_sealed")

    # ---- 3+4. 断网缓存 → 重连补传（乱序突发 + 双网关碰撞）--------------
    # 断网期间网关缓存了 seq 65520..65535 与回绕后的 0..7
    cached = []
    for j, seq in enumerate(range(65520, 65536)):
        t = 150.0 + j * 5
        value = live_value(seq)
        if seq == 65523:
            value = 45.0   # 补传段峰值 1（落在已封存窗口）
        if seq == 65525:
            value = 44.0   # 补传段峰值 2
        cached.append(msg(seq, t, value))
    for k, seq in enumerate(range(0, 8)):
        cached.append(msg(seq, 230.0 + k * 5, 11.2 + k * 0.1))

    by_seq = {m.device_seq: m for m in cached}

    def via_gw_b(m: IncomingMessage) -> IncomingMessage:
        """同一报文经备网关 gw-b 再投递一次（内容完全一致 → 同一 raw）。"""
        return IncomingMessage(
            gateway_id=GW_B, device_id=m.device_id, point_id=m.point_id,
            device_seq=m.device_seq, collected_at_ms=m.collected_at_ms,
            gateway_received_at_ms=m.gateway_received_at_ms, payload=m.payload,
        )

    # 乱序突发；夹带：65530 经 gw-b 重复投递、65531 被 gw-a 重试、
    # 65532 经 gw-b 转发但载荷被篡改（序号冲突）
    burst_order = [
        by_seq[65522], by_seq[65535], by_seq[3], by_seq[65520], by_seq[0],
        by_seq[65526], by_seq[65530], via_gw_b(by_seq[65530]),
        by_seq[65531], by_seq[65531],
        by_seq[65532],
        IncomingMessage(  # gw-b 转发的 65532，载荷被篡改 → 序号冲突
            gateway_id=GW_B, device_id=DEVICE, point_id=POINT, device_seq=65532,
            collected_at_ms=by_seq[65532].collected_at_ms,
            gateway_received_at_ms=by_seq[65532].gateway_received_at_ms,
            payload={"value": 999.0, "unit": "MPa"},
        ),
        by_seq[65521], by_seq[65523], by_seq[65524], by_seq[65525],
        by_seq[65527], by_seq[65528], by_seq[65529], by_seq[65533],
        by_seq[65534], by_seq[1], by_seq[2], by_seq[4], by_seq[5],
        by_seq[6], by_seq[7],
        msg(1, 240.0, 9.9, point="ghost-01"),          # 未登记点位 → 隔离
        msg(500, 0, 8.8, with_timestamps=False),        # 无任何时间戳 → 隔离
    ]

    clock.set_s(265)
    cursor_before = pipe.resume_cursor(DEVICE, POINT)   # 网关重连先查游标
    pre_burst_ledger = store.current_ledger_seq()       # 补传前的账本版本
    burst_outcomes = pipe.ingest_batch(burst_order)
    phase("reconnect_backfill_burst")

    # ---- 5. 旧代际迟到：卡在重试缓冲的 65519 ---------------------------
    clock.set_s(266)
    straggler_outcome = pipe.ingest_one(msg(65519, 145.0, 12.0))
    phase("epoch0_straggler_after_wrap")

    # ---- 6. 时钟漂移：峰值顺序颠倒与修正 --------------------------------
    # seq 14 设备时钟 +8s；随后设备 NTP 自校正，seq 15 恢复正常
    live2 = []
    for seq in range(8, 16):
        t = 270.0 + (seq - 8) * 5
        live2.append((seq, t, round(12.0 + (seq % 5) * 0.1, 2), 0))
    live2[6] = (14, 300.0, 55.0, 8000)   # 峰值 A：真实 t+300，设备打戳 t+308
    live2[7] = (15, 305.0, 53.0, 300)    # 峰值 B：真实 t+305，设备打戳 t+305.3
    for seq, t, value, drift in live2:
        clock.set_s(t + 0.2)
        pipe.ingest_one(msg(seq, t, value, drift_ms=drift))
    phase("clock_drift_peak_reversal")

    # ---- 7. 校准期：配置 v2 --------------------------------------------
    clock.set_s(355)
    pipe.register_point_config(
        {
            "point_id": POINT,
            "effective_from_ms": BASE_MS + 360_000,
            "unit": "MPa",
            "scale": 1.02,
            "offset": 0.1,
            "location": "车间A/1号线",
            "window_seconds": 60,
            "lateness_seconds": 30,
            "calibration_windows": [
                [BASE_MS + 360_000, BASE_MS + 420_000]
            ],
        }
    )
    for seq in range(16, 38):
        t = 310.0 + (seq - 16) * 5
        clock.set_s(t + 0.2)
        pipe.ingest_one(msg(seq, t, round(13.0 + (seq % 4) * 0.1, 2)))
    phase("calibration_period_config_v2")

    # ---- 8. 移机：配置 v3 ----------------------------------------------
    for seq in range(38, 48):
        t = 420.0 + (seq - 38) * 5
        clock.set_s(t + 0.2)
        pipe.ingest_one(msg(seq, t, round(12.5 + (seq % 3) * 0.1, 2)))
    clock.set_s(595)
    pipe.register_point_config(
        {
            "point_id": POINT,
            "effective_from_ms": BASE_MS + 600_000,
            "unit": "MPa",
            "scale": 1.0,
            "offset": 0.0,
            "location": "车间B/2号线",
            "window_seconds": 60,
            "lateness_seconds": 30,
        }
    )
    for seq in range(48, 93):
        t = 470.0 + (seq - 48) * 5
        clock.set_s(t + 0.2)
        pipe.ingest_one(msg(seq, t, round(12.0 + (seq % 6) * 0.1, 2)))
    phase("relocation_config_v3")

    # ---- 9. 服务重启：游标与回放一致性 ----------------------------------
    clock.set_s(700)
    cursor_pre = pipe.resume_cursor(DEVICE, POINT)
    replay_pre = pipe.replay(POINT, BASE_MS, BASE_MS + 300_000)
    pre_ids = [e["event_id"] for e in replay_pre["events"]]
    store.close()

    store = Store(db_path)          # 重启：同一账本文件重新打开
    pipe = Pipeline(store, clock=clock, backfill_threshold_ms=30_000)
    cursor_post = pipe.resume_cursor(DEVICE, POINT)
    replay_post = pipe.replay(POINT, BASE_MS, BASE_MS + 300_000)
    post_ids = [e["event_id"] for e in replay_post["events"]]
    report["restart"] = {
        "cursor_consistent": cursor_pre == cursor_post,
        "replay_consistent": pre_ids == post_ids,
        "cursor_after_restart": cursor_post,
    }
    for seq in range(93, 96):       # 重启后继续接入
        t = 700.0 + (seq - 93) * 5
        clock.set_s(t + 0.2)
        pipe.ingest_one(msg(seq, t, round(11.5 + (seq % 3) * 0.1, 2)))
    phase("service_restart_resume")

    # ---- 10. 终态封存 ---------------------------------------------------
    clock.set_s(900)
    seal_due_windows(store, clock.now)
    phase("final_seal")

    # ---- 汇总报告 --------------------------------------------------------
    report.update(
        _build_report(pipe, cursor_before, burst_outcomes, straggler_outcome,
                      pre_burst_ledger)
    )
    report["store"] = store  # 供测试继续断言
    report["pipeline"] = pipe
    return report


def _build_report(pipe, cursor_before, burst_outcomes, straggler_outcome,
                  pre_burst_ledger) -> dict:
    store = pipe.store
    with store.read() as conn:
        result_counts = {
            row["result"]: row["c"]
            for row in conn.execute(
                "SELECT result, COUNT(*) c FROM deliveries GROUP BY result"
            )
        }
        models = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM clock_models WHERE device_id=? ORDER BY version", (DEVICE,)
            )
        ]

    events = store.events_in_range(POINT, BASE_MS, BASE_MS + 1_000_000)
    by_seq = {}
    for e in events:
        by_seq[(e["device_epoch"], e["device_seq"])] = e

    # 峰值顺序：设备打戳 vs 修正后
    peak_a = by_seq[(1, 14)]
    peak_b = by_seq[(1, 15)]
    raw_a = store.get_raw(peak_a["raw_id"])
    raw_b = store.get_raw(peak_b["raw_id"])
    peaks = {
        "device_stamp_order_reversed": raw_b["collected_at_ms"] < raw_a["collected_at_ms"],
        "corrected_order_ok": peak_a["event_time_ms"] < peak_b["event_time_ms"],
        "peak_a": {
            "seq": 14, "value": peak_a["value"],
            "device_stamp": ms_to_iso(raw_a["collected_at_ms"]),
            "corrected": ms_to_iso(peak_a["event_time_ms"]),
            "offset_applied_ms": peak_a["offset_applied_ms"],
            "time_quality": peak_a["time_quality"],
            "clock_model_version": peak_a["clock_model_version"],
        },
        "peak_b": {
            "seq": 15, "value": peak_b["value"],
            "device_stamp": ms_to_iso(raw_b["collected_at_ms"]),
            "corrected": ms_to_iso(peak_b["event_time_ms"]),
            "offset_applied_ms": peak_b["offset_applied_ms"],
            "time_quality": peak_b["time_quality"],
            "clock_model_version": peak_b["clock_model_version"],
        },
    }

    # 回放对比：补传前的账本版本 vs 当前
    replay_before = pipe.replay(POINT, BASE_MS, BASE_MS + 300_000,
                                as_of_seq=pre_burst_ledger)
    replay_now = pipe.replay(POINT, BASE_MS, BASE_MS + 300_000)

    return {
        "cursor_at_reconnect": cursor_before,
        "burst_results": [o.to_dict() for o in burst_outcomes],
        "straggler_result": straggler_outcome.to_dict(),
        "delivery_result_counts": result_counts,
        "unique_events": store.count_events(),
        "clock_models": models,
        "peaks": peaks,
        "gaps": store.gaps_for_point(POINT),
        "lineage_late_window": pipe.lineage(POINT, BASE_MS + 120_000),
        "calibration_events": [
            {"seq": e["device_seq"], "value": e["value"],
             "config_version": e["config_version"], "flags": e["flags"]}
            for e in events if "calibration" in e["flags"]
        ][:5],
        "config_interpretation": [
            {"seq": by_seq[(1, s)]["device_seq"],
             "config_version": by_seq[(1, s)]["config_version"]}
            for s in (20, 30, 80) if (1, s) in by_seq
        ],
        "replay_compare": {
            "events_visible_before_backfill": len(replay_before["events"]),
            "events_visible_now": len(replay_now["events"]),
        },
        "final_cursor": pipe.resume_cursor(DEVICE, POINT),
    }


def print_report(report: dict) -> None:
    p = print
    p("=" * 72)
    p("边缘遥测可信账本 —— 网络抖动场景血缘报告")
    p("=" * 72)

    counts = report["delivery_result_counts"]
    p("\n[1] 投递判定总览（唯一事件 vs 重复/冲突/隔离）")
    p(f"    规范事件（唯一）: {report['unique_events']}")
    for result, c in sorted(counts.items()):
        p(f"    {result:<18} {c}")

    p("\n[2] 重连续传游标（网关重连时查询）")
    c = report["cursor_at_reconnect"]
    p(f"    epoch={c['device_epoch']} 连续水位={c['last_contiguous_seq']} "
      f"已见水位={c['last_seen_seq']} 从 seq={c['resume_from_seq']} 续传")

    p("\n[3] 时钟漂移与峰值顺序（时间修正依据）")
    for key in ("peak_a", "peak_b"):
        pk = report["peaks"][key]
        p(f"    seq={pk['seq']} value={pk['value']} 设备打戳={pk['device_stamp']} "
          f"修正后={pk['corrected']} 偏移={pk['offset_applied_ms']}ms "
          f"质量={pk['time_quality']} 模型v{pk['clock_model_version']}")
    p(f"    设备打戳顺序颠倒: {report['peaks']['device_stamp_order_reversed']}  "
      f"修正后顺序正确: {report['peaks']['corrected_order_ok']}")
    p(f"    时钟模型版本数: {len(report['clock_models'])}（漂移事件已升版留痕）")

    p("\n[4] 数据缺口生命周期（断网开口 → 补传闭合）")
    for g in report["gaps"]:
        closed = ms_to_iso(g["closed_at_ms"]) if g["closed_at_ms"] else "-"
        p(f"    epoch={g['device_epoch']} seq[{g['from_seq']}..{g['to_seq']}] "
          f"{g['status']} 开口={ms_to_iso(g['detected_at_ms'])} 闭合={closed}")

    p("\n[5] 已封存窗口的修订血缘（窗口 [10:02,10:03)）")
    lin = report["lineage_late_window"]
    p(f"    窗口状态: {lin['window']['state']}  当前修订: "
      f"{lin['window']['current_revision']}")
    for rev in lin["revisions"]:
        m = rev["metrics"]
        affected = ",".join(rev["affected_metrics"].keys()) or "(基线)"
        p(f"    rev{rev['revision_no']} ledger={rev['ledger_seq']} "
          f"count={m['count']} max={m['max']} avg={m['avg']} "
          f"受影响指标=[{affected}] 触发={rev['caused_by'] or ['-']}")

    p("\n[6] 校准期与配置版本（历史读数按当时配置解释）")
    for ce in report["calibration_events"]:
        p(f"    校准期读数 seq={ce['seq']} 标定值={ce['value']} "
          f"配置v{ce['config_version']} 标记={ce['flags']}")
    for ci in report["config_interpretation"]:
        p(f"    seq={ci['seq']} 按配置v{ci['config_version']} 解释")

    p("\n[7] 版本回放（同一事件时间范围，不同账本版本）")
    rc = report["replay_compare"]
    p(f"    补传前版本可见事件: {rc['events_visible_before_backfill']}  "
      f"当前版本可见事件: {rc['events_visible_now']}")

    p("\n[8] 服务重启")
    r = report["restart"]
    p(f"    游标一致: {r['cursor_consistent']}  回放一致: {r['replay_consistent']}")
    fc = report["final_cursor"]
    p(f"    终态游标: epoch={fc['device_epoch']} "
      f"连续水位={fc['last_contiguous_seq']} 开放缺口={fc['open_gaps']}")
    p("")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="边缘遥测可信账本演示")
    parser.add_argument(
        "--db",
        default=os.path.join(tempfile.gettempdir(), "edge-telemetry-demo.db"),
    )
    args = parser.parse_args(argv)
    report = run_demo(args.db)
    print_report(report)
    # 释放演示句柄，便于 Windows/重复运行
    report["store"].close()
    print(f"(账本文件: {args.db}，可用 python -m app.service --db {args.db} 继续查询)")


if __name__ == "__main__":
    main()
