# 边缘遥测可信账本

本项目处理工业边缘网关在断网、补传和时钟漂移下产生的遥测记录。设备原始序号、网关接收时间和采集时间都属于证据，归一化结果不能替代原始消息。

`domain_contract.json` 约定事件判定、时间质量和生产窗口的外部值。序号允许按设备代际回绕；测点搬迁和校准通过带生效时间的配置版本表达。

仅依赖 Python 3.11 标准库（`sqlite3` / `http.server` / `unittest`），无需安装任何第三方包。

## 设计原则

1. **证据先行**：每条报文原文（`raw_messages`）与每次投递（`deliveries`）只增不改；同一报文经主备两个网关投递只算一条 raw、留下两条投递记录。
2. **幂等边界**：规范事件流以 `(device_id, point_id, device_epoch, device_seq)` 唯一。代际（epoch）支持显式携带或按回绕规则推断（序号从接近 65535 跳回低位 → 新代际；回绕后到达的高位序号 → 归入旧代际）。
3. **判定分类**（契约 `event_results`）：`accepted` / `duplicate`（同键同载荷）/ `late`（落入已封存窗口，仍入流并触发修订）/ `sequence_conflict`（同键不同载荷）/ `quarantined`（点位未登记、数值非法、时间不可用、时间超前）。入流事件另可携带标记：`out_of_order`（乱序）、`backfilled`（补传）、`calibration`（校准期）。
4. **时钟模型**：网关时钟（NTP）为真值基准。自带双时间戳的报文以自身 `(采集, 接收)` 时间对为修正依据；缺网关时刻时退回设备时钟模型当前版本；漂移超阈值时模型升版留痕。每条事件记录 `time_quality`（`device`/`corrected`/`estimated`/`unknown`）、`clock_model_version` 与 `offset_applied_ms`。
5. **窗口不可改写**：生产窗口按事件时间滚动，`open → sealed → revised`。迟到数据不改写已封存修订版，只追加新修订版，记录 `affected_metrics`（受影响指标新旧值）与 `caused_by`（触发事件），形成血缘链。
6. **缺口留痕**：按 `(设备, 点位, 代际)` 跟踪序号 frontier，缺口在断网时开口、补传到达后闭合，开闭时间全程可查。
7. **配置版本**：测点移机、校准参数变更、校准窗口均以带 `effective_from` 的配置版本表达；历史读数按事件时间生效的版本解释（标定换算 `value = raw × scale + offset`）。

## 运行

```bash
# 基础检查与全部测试（43 个）
python -m unittest discover -s tests -v

# 端到端场景演示：断网补传 / 序号回绕 / 双网关碰撞 / 服务重启
python -m app.demo

# 启动 HTTP 服务
python -m app.service --db ledger.db --port 8080
```

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/points` | 登记点位配置版本（移机 / 校准参数 / 校准窗口） |
| GET | `/v1/points/{point_id}` | 点位配置历史 |
| POST | `/v1/ingest` | 批量报文接入，逐条返回判定结果 |
| GET | `/v1/cursor?device_id&point_id` | 网关重连续传游标（代际、连续水位、开放缺口、续传起点） |
| GET | `/v1/events?point_id&from&to` | 规范事件（含时间修正依据 `time_basis`） |
| GET | `/v1/windows?point_id&from&to` | 生产窗口及当前状态 |
| GET | `/v1/windows/{window_id}/revisions` | 窗口修订链（受影响指标、触发事件） |
| GET | `/v1/gaps?point_id` | 缺口开闭全周期 |
| GET | `/v1/replay?point_id&from&to[&as_of]` | 按事件时间回放指定账本版本（`as_of` 为全局账本序号） |
| GET | `/v1/lineage?point_id&window_start` | 窗口血缘：修订链 → 触发事件 → 原始报文与投递 → 配置历史 → 缺口 |
| POST | `/v1/admin/seal` | 推进窗口封存（生产环境由定时器驱动） |

报文格式（`POST /v1/ingest`）：

```json
{
  "messages": [
    {
      "gateway_id": "gw-a",
      "device_id": "plc-7",
      "point_id": "press-01",
      "device_seq": 65520,
      "device_epoch": null,
      "collected_at": "2026-09-18T10:02:30Z",
      "gateway_received_at": "2026-09-18T10:02:30.100Z",
      "payload": {"value": 10.7, "unit": "MPa"}
    }
  ]
}
```

## 代码结构

```
app/
  models.py     契约词汇（判定结果 / 时间质量 / 窗口状态）、报文与判定输出
  store.py      SQLite 账本：证据层、事件流、时钟观测、frontier、缺口、窗口修订
  clock.py      设备时钟模型：观测、漂移升版、事件时间修正
  pipeline.py   接入管线：代际推断、幂等判定、标记、缺口、游标、回放、血缘
  windows.py    窗口分配、封存、迟到修订、指标重算
  service.py    HTTP API（标准库 http.server）
  demo.py       端到端场景演示与血缘报告
tests/          43 个单元与集成测试
```

## 已知边界

- 代际推断是尽力而为：跨代际且序号差恰为半个序号空间的报文存在固有歧义，稳健做法是网关显式携带 `device_epoch`。
- 被隔离（quarantined）的序号不会在缺口中开口；缺口只统计规范事件流的缺失。
- 窗口指标基于全部入流事件（含校准期标记）；控制侧消费时可按 `flags` 过滤。
