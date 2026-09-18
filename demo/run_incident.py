#!/usr/bin/env python3
"""现场事故演示：断网缓存补传 → 序号回绕 → 双网关碰撞 → 服务重启。

用法：
    python3 -m demo.run_incident                # 使用临时 WAL，跑完即弃
    python3 -m demo.run_incident --wal data.log # 保留 WAL，可多次运行观察

报告刻意不输出“一条洗平的曲线”，而是并列呈现：
入账判定轨迹、唯一事件及其时间修正依据、数据缺口、槽位冲突、窗口版本血缘，
以及封存版 v1 与迟到数据产生的修订版 v2 的差异。
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile

from app.model import CalibrationRecord, PointConfig, RawMessage
from app.persistence import LedgerStore
from app.service import TrustService


def line(title: str = "") -> None:
    print("\n" + "=" * 72)
    if title:
        print(title)
        print("-" * 72)


def pkt(seq, value, dts, gw, rcv, cursor, gen=0):
    return {"device_id": "devA", "seq": seq, "metric": "temp",
            "value": value, "device_ts": dts, "gateway_id": gw,
            "recv_ts": rcv, "gen": gen, "source_cursor": cursor}


def build_and_crash(path: str) -> None:
    """阶段一：事故发生，服务在补传与封存后“崩溃”（显式关闭，仅留 WAL）。"""
    store = LedgerStore(path, modulus=10, window_seconds=60.0)
    svc = TrustService(store)

    svc.register_point({"device_id": "devA", "metric": "temp",
        "version": "cfg-v1", "effective_event_time": 0.0,
        "scale": 1.0, "offset": 0.0, "unit": "C",
        "location": "A区1号线", "note": "初装", "registered_at": 900.0})
    # 校准参数后来变更（量程换算 + 移机），1020 起生效
    svc.register_point({"device_id": "devA", "metric": "temp",
        "version": "cfg-v2", "effective_event_time": 1020.0,
        "scale": 2.0, "offset": -20.0, "unit": "C",
        "location": "B区3号线", "note": "移机并更换量程板",
        "registered_at": 1025.0})
    # 设备时钟慢 10 秒
    svc.register_calibration({"calib_id": "cal1", "device_id": "devA",
        "at_server": 1010.0, "at_device": 1000.0, "offset_ms": 10000.0,
        "drift_ppm": 0.0, "source": "ntp", "recv_ts": 1010.0,
        "note": "厂区抖动后首次 NTP 对时"})

    # 断网前实时两条
    svc.ingest_packet(pkt(1, 50.0, 1001.0, "gw-a", 1011.0, 1001), at=1011.0)
    svc.ingest_packet(pkt(2, 51.0, 1002.0, "gw-a", 1012.0, 1002), at=1012.0)

    # 数小时缓存压缩为补传突发：seq3 双网关一致转发；seq4 双网关载荷冲突
    svc.ingest_packet(pkt(3, 52.0, 1003.0, "gw-a", 1060.1, 3003), at=1060.1)
    svc.ingest_packet(pkt(3, 52.0, 1003.0, "gw-b", 1060.2, 8003), at=1060.2)
    svc.ingest_packet(pkt(4, 53.0, 1004.0, "gw-a", 1060.3, 3004), at=1060.3)
    svc.ingest_packet(pkt(4, 99.9, 1004.0, "gw-b", 1060.4, 8004), at=1060.4)

    # seq5 正常，seq6 丢失，7..9 继续
    svc.ingest_packet(pkt(5, 54.0, 1005.0, "gw-a", 1060.5, 3005), at=1060.5)
    for seq, dts in [(7, 1007.0), (8, 1008.0), (9, 1009.0)]:
        svc.ingest_packet(pkt(seq, 50.0 + seq, dts, "gw-a",
                              1060.5 + seq * 0.01, 3000 + seq),
                          at=1060.5 + seq * 0.01)

    # 序号回绕（cycle 1），设备时间继续向前
    for seq, dts in [(0, 1010.0), (1, 1011.0), (2, 1012.0)]:
        svc.ingest_packet(pkt(seq, 60.0 + seq, dts, "gw-a",
                              1061.0 + seq * 0.1, 3100 + seq),
                          at=1061.0 + seq * 0.1)

    # 无法解释的报文 -> 隔离
    bad = pkt(6, "NaN", 1006.0, "gw-a", 1060.6, 3006)
    svc.ingest_packet(bad, at=1060.6)

    # 封存两个生产窗口
    svc.seal_due(now=1080.0)
    store.close()
    print(f"[阶段一] 事故报文已写入 WAL：{path}，服务关闭（模拟崩溃/重启）")


def restart_and_report(path: str) -> None:
    """阶段二：服务重启，仅靠 WAL 重放重建全部状态，再接收迟到补传并查询。"""
    store = LedgerStore(path, modulus=10, window_seconds=60.0, rebuild=True)
    svc = TrustService(store)

    line("1) 重启后入账判定轨迹（证据顺序，未被清洗）")
    for e in svc.audit_trail():
        tag = {"accepted": "✓接受", "duplicate": "＝重复",
               "late": "⏰迟到", "sequence_conflict": "⚔冲突",
               "quarantined": "⛔隔离"}[e["verdict"]]
        r = e.get("raw", {})
        extra = ""
        if e["verdict"] == "duplicate":
            extra = f" -> {e['duplicate_of']}"
        print(f"  {tag}  entry={e['entry_id']} g{r.get('gen')}#{r.get('seq'):<3} "
              f"网关={r.get('gateway_id'):<4} 值={r.get('value')}{extra}")
        print(f"        理由：{e['reason']}")

    line("2) 唯一事件 + 时间修正依据 + 当时配置（按事件时间）")
    replay = svc.replay_range("devA", "temp", 1010.0, 1020.0)
    for ev in replay["events"]:
        print(f"  {ev['event_id']:<28} 事件时间={ev['event_time']:.1f} "
              f"原始设备时间={ev['device_ts']:.1f} "
              f"质量={ev['event_time_quality']:<9} "
              f"值={ev['value']:.1f}{ev['unit']} @{ev['location']} "
              f"配置={ev['config_version']}")
        print(f"        时间依据：{ev['clock_basis']}")

    line("3) 数据缺口台账")
    for g in svc.gaps("devA", "temp"):
        miss = [f"c{x['cycle']}#{x['seq']}" for x in g["missing"]]
        fillers = [f"c{x['cycle']}#{x['seq']}←{x['event_id']}"
                   for x in g["fillers"]]
        print(f"  {g['gap_id']}  状态={g['status']}")
        print(f"      缺失={miss or '无'}  已补={fillers or '无'}")
        print(f"      估计事件时间区间={g['approx_event_time_range']}")

    line("4) 双网关槽位冲突（双方原始报文均可调阅）")
    for c in svc.conflicts("devA"):
        loser = svc.entry_view(c["loser_entry_id"])
        print(f"  槽位 {c['slot']}：胜方 {c['winner_event_id']}；"
              f"败方 entry={c['loser_entry_id']} "
              f"网关={loser['raw']['gateway_id']} 值={loser['raw']['value']}")

    line("5) 网关重连续传游标")
    token = svc.resume("gw-a")
    print(f"  gw-a last_cursor={token['last_cursor']}")
    print(f"  {token['hint']}")
    for g in token["open_gaps"]:
        seqs = [f"c{x['cycle']}#{x['seq']}" for x in g["missing"]]
        print(f"      待补 {g['gap_id']}: {seqs}")

    line("6) 封存窗口 v1 → 迟到补传 → 修订版 v2（血缘，不改写历史）")
    # 丢失的 seq6 在 1090 才找回：事件时间 1006+10=1016 落在已封存窗 960
    late = svc.ingest_packet(pkt(6, 56.0, 1006.0, "gw-a", 1090.0, 3206),
                             at=1090.0)
    print(f"  迟到报文判定：{late['verdict']} — {late['reason']}")
    win = "win:devA:temp:960"
    for node in svc.lineage(win):
        agg = node["aggregates"]["temp"]
        print(f"  v{node['version']} [{node['state']}] "
              f"父版本={node['parent_version']} 事件数={agg['count']} "
              f"均值={agg['mean']:.2f} 峰值={agg['peak_value']:.1f}"
              f"@{agg['peak_event_time']:.1f}")
        print(f"      变更：{node['change_summary']}")
        print(f"      新引入：{list(node['introduced_event_ids'])}")
        print(f"      受影响指标：{list(node['affected_metrics'])}")

    v1 = svc.replay_window(win, version=1)
    v2 = svc.replay_window(win, version=2)
    print(f"\n  按版本回放：v1={len(v1['events'])} 个事件（旧曲线，仍可查），"
          f"v2={len(v2['events'])} 个事件（修订版）")

    line("7) 移机/校准参数变更：新读数按新配置，历史读数锁定旧配置")
    new_reading = svc.replay_range("devA", "temp", 1020.0, 1023.0)["events"]
    for ev in new_reading:
        print(f"  新读数 {ev['event_id']}：原始值={ev['raw_value']:.1f} "
              f"解释后={ev['value']:.1f}{ev['unit']} @{ev['location']} "
              f"配置={ev['config_version']}")
    old = svc.explain("evt:devA:temp:g0c0:1")
    print(f"  历史读数 evt:...g0c0:1：解释后={old['value']:.1f}{old['unit']} "
          f"@{old['location']} 配置={old['config_version']}（不被新配置改写）")

    line("8) 单事件完整证据链示例（峰值归属）")
    peak_id = v2["aggregates"]["temp"]["peak_event_id"]
    ex = svc.explain(peak_id)
    print(json.dumps({
        "event_id": ex["event_id"], "verdict": ex["verdict"],
        "event_time": ex["event_time"],
        "event_time_quality": ex["event_time_quality"],
        "clock_model": ex["clock_model"], "config_version": ex["config_version"],
        "raw_payload": ex["raw_payload"],
        "duplicate_occurrences": ex["duplicate_occurrences"],
        "slot_conflicts": ex["slot_conflicts"],
    }, ensure_ascii=False, indent=2))
    store.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wal", default=None, help="WAL 文件路径（默认临时文件）")
    args = ap.parse_args()
    if args.wal:
        path = args.wal
        if os.path.exists(path):
            os.remove(path)
        cleanup = False
    else:
        fd, path = tempfile.mkstemp(suffix=".jsonl", prefix="telemetry-")
        os.close(fd)
        cleanup = True
    try:
        build_and_crash(path)
        restart_and_report(path)
    finally:
        if cleanup and os.path.exists(path):
            os.remove(path)


if __name__ == "__main__":
    main()
