"""Autoscaler leader failover: does a new leader keep the rate estimate?

Three server replicas share one Postgres. The worker connects to `bench-c` (started last, so it
never holds the autoscaler lock). The test loads a stream, kills whichever replica is the
autoscaler leader, and watches the concurrency/lag from the surviving dashboard replica. The new
leader seeds its rate/capacity estimate from the samples table.
"""

import json
import os
import subprocess
import threading
import time
import urllib.request

import run as h

TOKEN = os.environ.get("BENCH_ADMIN_TOKEN", "bench-admin-token-0123456789")
KEY = os.environ.get("BENCH_API_KEY", "rk_bench_local_only_000000000000000000")
HEAD = {"Authorization": f"Bearer {TOKEN}"}
PORTS = {"a": 8091, "b": 8092, "c": 8093}


def get(port: int, path: str) -> dict:
    req = urllib.request.Request(f"http://localhost:{port}{path}", headers=HEAD)
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def health(port: int) -> dict:
    with urllib.request.urlopen(f"http://localhost:{port}/api/health", timeout=3) as r:
        return json.loads(r.read())


def leader() -> tuple[str, int] | None:
    for name, port in PORTS.items():
        try:
            if health(port).get("leader"):
                return name, port
        except Exception:
            pass
    return None


def concurrency() -> tuple[int | None, int | None]:
    ov = get(PORTS["c"], "/api/tenants/bench/overview")
    for s in ov["streams"]:
        if (s["stream"], s["group"]) == ("bench", "g"):
            return s["concurrency"], s["lag"]
    return None, None


def start(name: str, port: int) -> None:
    subprocess.run(["docker", "run", "-d", "--rm", "--name", f"bench-{name}", "-p", f"{port}:8080",
                    "-e", "RILLKIT_SERVER_DATABASE_URL=postgresql://postgres:postgres@"
                          "host.docker.internal:5440/postgres",
                    "-e", f"RILLKIT_SERVER_ADMIN_TOKEN={TOKEN}",
                    "-e", f"RILLKIT_SERVER_PUBLIC_URL=http://localhost:{port}",
                    "-e", "RILLKIT_SERVER_BOOTSTRAP_TENANT=bench",
                    "-e", f"RILLKIT_SERVER_BOOTSTRAP_API_KEY={KEY}",
                    "-e", "RILLKIT_SERVER_TELEMETRY_INTERVAL_SEC=1",
                    "-e", "RILLKIT_SERVER_AUTOSCALE_INTERVAL_SEC=2",
                    "-e", f"RILLKIT_SERVER_REPLICA_ID={name}",
                    "rillkit-server:latest"], check=True, capture_output=True)


for name in ("bench-a", "bench-b", "bench-c", "bench-server"):
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
# a and b race for the advisory lock; c starts last and stays standby, so the worker's
# connection survives whichever of a/b is killed.
for name in ("a", "b"):
    start(name, PORTS[name])
time.sleep(2)
start("c", PORTS["c"])
for _ in range(40):
    try:
        health(PORTS["c"])
        break
    except Exception:
        time.sleep(0.5)

h.start_gateway(trace=False)
h.r.flushall()
h.stop_workers_active()
h.workers_active = h.start_workers(1, tag="failover", control="relay",
                                   control_url=f"http://localhost:{PORTS['c']}", api_key=KEY,
                                   lanes=1, block_ms=200, batch=50, max_retries=0)
# The policy can be created on any replica; it is in Postgres.
req = urllib.request.Request(
    f"http://localhost:{PORTS['c']}/api/tenants/bench/policies/bench/g", method="PUT",
    data=json.dumps({"enabled": True, "min_workers": 1, "max_workers": 8,
                     "target_lag_per_worker": 300, "upscale_cooldown_sec": 2,
                     "downscale_cooldown_sec": 10}).encode(),
    headers={**HEAD, "Content-Type": "application/json"})
urllib.request.urlopen(req, timeout=5).read()
time.sleep(3)

before = leader()
if before is None or before[0] == "c":
    raise SystemExit(f"unexpected leader before load: {before}")
print(json.dumps({"phase": "before", "leader": before[0]}), flush=True)

samples = []
try:
    k6 = threading.Thread(target=h.run_k6, args=(2000, "60s"), kwargs={"name": "failover"},
                          daemon=True)
    k6.start()
    for i in range(12):
        time.sleep(2)
        samples.append({"t": (i + 1) * 2, **dict(zip(("concurrency", "lag"), concurrency()))})
    print(json.dumps({"phase": "pre-kill", "leader": before[0], "samples": samples}), flush=True)

    subprocess.run(["docker", "rm", "-f", f"bench-{before[0]}"], capture_output=True)
    killed_at = time.time()
    after = None
    for _ in range(30):
        time.sleep(0.5)
        after = leader()
        if after is not None:
            break
    print(json.dumps({"phase": "after-kill", "leader": after[0] if after else None,
                      "elected_ms": round((time.time() - killed_at) * 1000)}), flush=True)

    post = []
    for i in range(15):
        time.sleep(2)
        post.append({"t": (i + 1) * 2, **dict(zip(("concurrency", "lag"), concurrency()))})
    print(json.dumps({"phase": "post-kill", "samples": post}), flush=True)
finally:
    h.stop_workers_active()
    h.stop_gateway()
    for name in ("a", "b", "c"):
        subprocess.run(["docker", "rm", "-f", f"bench-{name}"], capture_output=True)
