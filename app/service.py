"""HTTP API（仅标准库）：遥测接入与可信度查询服务。

运行：python -m app.service --db ledger.db --port 8080

路由：
  POST /v1/points                     登记点位配置版本
  GET  /v1/points/{point_id}          点位配置历史
  POST /v1/ingest                     批量报文接入
  GET  /v1/cursor?device_id&point_id  网关重连续传游标
  GET  /v1/events?point_id&from&to    规范事件（含时间修正依据）
  GET  /v1/windows?point_id&from&to   生产窗口及当前状态
  GET  /v1/windows/{window_id}/revisions  窗口修订链
  GET  /v1/gaps?point_id              缺口（开闭全周期）
  GET  /v1/replay?point_id&from&to[&as_of]  按事件时间回放某个账本版本
  GET  /v1/lineage?point_id&window_start    窗口血缘（修订→事件→原文→时钟模型）
  POST /v1/admin/seal {now}           推进窗口封存（生产环境由定时器驱动）
"""
from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .models import IncomingMessage, iso_to_ms, ms_to_iso
from .pipeline import Pipeline
from .store import Store
from .windows import seal_due_windows


def make_handler(pipeline: Pipeline):
    class Handler(BaseHTTPRequestHandler):
        server_version = "EdgeTelemetryLedger/1.0"

        # ---------- 工具 ----------

        def _json(self, code: int, body: dict) -> None:
            data = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def _query(self) -> dict:
            return {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}

        def log_message(self, fmt, *args):  # 静默访问日志
            pass

        # ---------- 路由 ----------

        def do_POST(self):
            path = urlparse(self.path).path
            try:
                if path == "/v1/points":
                    return self._post_point()
                if path == "/v1/ingest":
                    return self._post_ingest()
                if path == "/v1/admin/seal":
                    return self._post_seal()
                return self._json(404, {"error": "not_found"})
            except (KeyError, ValueError, TypeError) as exc:
                return self._json(400, {"error": "bad_request", "detail": str(exc)})

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                if path.startswith("/v1/points/"):
                    return self._get_point(path.rsplit("/", 1)[-1])
                if path == "/v1/cursor":
                    return self._get_cursor()
                if path == "/v1/events":
                    return self._get_events()
                if path == "/v1/windows":
                    return self._get_windows()
                if path.startswith("/v1/windows/") and path.endswith("/revisions"):
                    return self._get_revisions(path.split("/")[3])
                if path == "/v1/gaps":
                    return self._get_gaps()
                if path == "/v1/replay":
                    return self._get_replay()
                if path == "/v1/lineage":
                    return self._get_lineage()
                return self._json(404, {"error": "not_found"})
            except (KeyError, ValueError, TypeError) as exc:
                return self._json(400, {"error": "bad_request", "detail": str(exc)})

        # ---------- POST ----------

        def _post_point(self):
            body = self._body()
            cfg = {
                "point_id": body["point_id"],
                "effective_from_ms": iso_to_ms(body["effective_from"]),
                "unit": body.get("unit"),
                "scale": body.get("scale", 1.0),
                "offset": body.get("offset", 0.0),
                "location": body.get("location"),
                "window_seconds": body.get("window_seconds", 3600),
                "lateness_seconds": body.get("lateness_seconds", 600),
                "calibration_windows": [
                    [iso_to_ms(s), iso_to_ms(e)]
                    for s, e in body.get("calibration_windows", [])
                ],
            }
            saved = pipeline.register_point_config(cfg)
            return self._json(201, _config_view(saved))

        def _post_ingest(self):
            body = self._body()
            messages = [
                IncomingMessage(
                    gateway_id=m["gateway_id"],
                    device_id=m["device_id"],
                    point_id=m["point_id"],
                    device_seq=int(m["device_seq"]),
                    collected_at_ms=_opt_ms(m.get("collected_at")),
                    gateway_received_at_ms=_opt_ms(m.get("gateway_received_at")),
                    payload=m.get("payload", {}),
                    device_epoch=m.get("device_epoch"),
                )
                for m in body["messages"]
            ]
            outcomes = pipeline.ingest_batch(messages)
            return self._json(200, {"results": [o.to_dict() for o in outcomes]})

        def _post_seal(self):
            body = self._body()
            now = iso_to_ms(body["now"]) if "now" in body else pipeline.now_ms()
            sealed = seal_due_windows(pipeline.store, now)
            return self._json(200, {"sealed": sealed, "now": ms_to_iso(now)})

        # ---------- GET ----------

        def _get_point(self, point_id: str):
            configs = pipeline.store.list_configs(point_id)
            if not configs:
                return self._json(404, {"error": "unknown_point"})
            return self._json(
                200, {"point_id": point_id, "configs": [_config_view(c) for c in configs]}
            )

        def _get_cursor(self):
            q = self._query()
            cursor = pipeline.resume_cursor(q["device_id"], q["point_id"])
            return self._json(200, cursor)

        def _get_events(self):
            q = self._query()
            events = pipeline.store.events_in_range(
                q["point_id"], iso_to_ms(q["from"]), iso_to_ms(q["to"])
            )
            return self._json(200, {"events": [_event_view(e) for e in events]})

        def _get_windows(self):
            q = self._query()
            windows = pipeline.store.windows_in_range(
                q["point_id"], iso_to_ms(q["from"]), iso_to_ms(q["to"])
            )
            return self._json(200, {"windows": windows})

        def _get_revisions(self, window_id: str):
            revisions = pipeline.store.revisions(window_id)
            return self._json(200, {"window_id": window_id, "revisions": revisions})

        def _get_gaps(self):
            q = self._query()
            gaps = pipeline.store.gaps_for_point(q["point_id"])
            return self._json(200, {"gaps": gaps})

        def _get_replay(self):
            q = self._query()
            as_of = int(q["as_of"]) if "as_of" in q else None
            result = pipeline.replay(
                q["point_id"], iso_to_ms(q["from"]), iso_to_ms(q["to"]), as_of
            )
            result["events"] = [_event_view(e) for e in result["events"]]
            return self._json(200, result)

        def _get_lineage(self):
            q = self._query()
            result = pipeline.lineage(q["point_id"], iso_to_ms(q["window_start"]))
            if "error" in result:
                return self._json(404, result)
            for bundle in result["caused_events"].values():
                bundle["event"] = _event_view(bundle["event"])
            return self._json(200, result)

    return Handler


def _opt_ms(text):
    return iso_to_ms(text) if text else None


def _config_view(cfg: dict) -> dict:
    return {
        **cfg,
        "effective_from": ms_to_iso(cfg["effective_from_ms"]),
        "calibration_windows": [
            [ms_to_iso(s), ms_to_iso(e)] for s, e in cfg.get("calibration_windows", [])
        ],
    }


def _event_view(ev: dict) -> dict:
    """事件视图：突出时间修正依据。"""
    return {
        **ev,
        "event_time": ms_to_iso(ev["event_time_ms"]),
        "time_basis": {
            "time_quality": ev["time_quality"],
            "clock_model_version": ev["clock_model_version"],
            "offset_applied_ms": ev["offset_applied_ms"],
        },
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="边缘遥测可信账本服务")
    parser.add_argument("--db", default="ledger.db")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)

    store = Store(args.db)
    pipeline = Pipeline(store, clock=lambda: int(time.time() * 1000))
    server = ThreadingHTTPServer(("0.0.0.0", args.port), make_handler(pipeline))
    print(f"listening on :{args.port}, db={args.db}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        store.close()


if __name__ == "__main__":
    main()
