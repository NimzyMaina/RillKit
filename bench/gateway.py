"""HTTP ingress for the benchmark: one RillKit publish per request (threaded stdlib server)."""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from rillkit import Broker, HashTagKeys, PlainKeys, Settings, TraceContext

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380/0")
CLUSTER = os.environ.get("BENCH_CLUSTER", "")
STREAM = os.environ.get("STREAM", "bench")
TRACE = os.environ.get("GATEWAY_TRACE") == "1"

middlewares = [TraceContext()] if TRACE else []
if CLUSTER:
    host, _, port = CLUSTER.partition(":")
    _settings = Settings(mode="cluster", cluster_nodes=[(host, int(port))], max_connections=1000)
    _keys = HashTagKeys()
else:
    _settings = Settings(url=REDIS_URL, max_connections=1000)
    _keys = PlainKeys()
broker = Broker(settings=_settings, keys=_keys, middlewares=middlewares)
lock = threading.Lock()
counters = {"published": 0, "errors": 0}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args) -> None:  # keep the benchmark output clean
        pass

    def _json(self, status: int, body: dict) -> None:
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._json(200, {"status": "ok"})
        elif self.path == "/stats":
            with lock:
                self._json(200, dict(counters))
        elif self.path == "/reset":
            with lock:
                counters.update(published=0, errors=0)
            self._json(200, {"status": "ok"})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/publish":
            self._json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length))
            broker.publish(body.get("stream") or STREAM,
                           {"seq": body["seq"], "sent_ms": body["sent_ms"],
                            "pad": body.get("pad", "")})
        except Exception as e:  # noqa: BLE001 - the benchmark counts any failure
            with lock:
                counters["errors"] += 1
                n = counters["errors"]
            if n <= 5:
                print(f"publish error {n}: {e!r}", flush=True)
            self._json(500, {"error": str(e)})
            return
        with lock:
            counters["published"] += 1
        self._json(200, {"ok": True})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8090"))
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
