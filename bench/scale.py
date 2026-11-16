"""Autoscaler convergence: lag at a fixed rate, watch the server add and then remove lanes."""

import json
import os
import subprocess
import threading
import time
import urllib.request

import run as h

API = os.environ.get("BENCH_SERVER_URL", "http://localhost:8091") + "/api"
TOKEN = os.environ.get("BENCH_ADMIN_TOKEN", "bench-admin-token-0123456789")
KEY = os.environ.get("BENCH_API_KEY", "rk_bench_local_only_000000000000000000")
HEAD = {"Authorization": f"Bearer {TOKEN}"}


def call(method: str, path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(API + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={**HEAD, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        raw = r.read()
        return json.loads(raw) if raw else {}


def concurrency() -> tuple[int | None, int | None]:
    ov = call("GET", "/tenants/bench/overview")
    for s in ov["streams"]:
        if (s["stream"], s["group"]) == ("bench", "g"):
            return s["concurrency"], s["lag"]
    return None, None


for name in ("bench-server", "bench-pg"):
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
subprocess.run(["docker", "run", "-d", "--rm", "--name", "bench-pg", "-e", "POSTGRES_PASSWORD=postgres",
                "-p", "5440:5432", "postgres:17"], check=True, capture_output=True)
time.sleep(3)
subprocess.run(["docker", "run", "-d", "--rm", "--name", "bench-server", "-p", "8091:8080",
                "-e", "RILLKIT_SERVER_DATABASE_URL=postgresql://postgres:postgres@host.docker.internal:5440/postgres",
                "-e", f"RILLKIT_SERVER_ADMIN_TOKEN={TOKEN}",
                "-e", "RILLKIT_SERVER_PUBLIC_URL=http://localhost:8091",
                "-e", "RILLKIT_SERVER_BOOTSTRAP_TENANT=bench",
                "-e", f"RILLKIT_SERVER_BOOTSTRAP_API_KEY={KEY}",
                "-e", "RILLKIT_SERVER_TELEMETRY_INTERVAL_SEC=1",
                "-e", "RILLKIT_SERVER_AUTOSCALE_INTERVAL_SEC=2",
                os.environ.get("RILLKIT_SERVER_IMAGE", "rillkit-server:latest")], check=True, capture_output=True)
for _ in range(40):
    try:
        urllib.request.urlopen("http://localhost:8091/api/health", timeout=1).read()
        break
    except Exception:
        time.sleep(0.5)

h.start_gateway(trace=False)
h.r.flushall()
h.stop_workers_active()
h.workers_active = h.start_workers(1, tag="auto", control="relay",
                                   control_url="http://localhost:8091", api_key=KEY,
                                   lanes=1, block_ms=200, batch=50, max_retries=0)
call("PUT", "/tenants/bench/policies/bench/g", {
    "enabled": True, "min_workers": 1, "max_workers": 8, "target_lag_per_worker": 300,
    "upscale_cooldown_sec": 5, "downscale_cooldown_sec": 10,
})
time.sleep(3)
samples = []
try:
    k6 = threading.Thread(target=h.run_k6, args=(2000, "45s"), kwargs={"name": "autoscale"}, daemon=True)
    k6.start()
    for _ in range(24):
        time.sleep(2)
        samples.append({"t": len(samples) * 2, **dict(zip(("concurrency", "lag"), concurrency()))})
    print(json.dumps({"phase": "scale-up", "samples": samples}), flush=True)
    time.sleep(12)
    down = []
    for _ in range(30):
        time.sleep(2)
        down.append({"t": len(down) * 2, **dict(zip(("concurrency", "lag"), concurrency()))})
    print(json.dumps({"phase": "scale-down", "samples": down}), flush=True)
finally:
    h.stop_workers_active()
    h.stop_gateway()
    subprocess.run(["docker", "rm", "-f", "bench-server", "bench-pg"], capture_output=True)
