"""Benchmark orchestrator: separate Redis, gateway, workers and k6 stages (never the demo)."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import redis
import redis.cluster
from rillkit import HashTagKeys, Stream

HERE = Path(__file__).parent
PY = sys.executable
REDIS = os.environ.get("BENCH_REDIS_URL", "redis://localhost:6380/0")
BASE = os.environ.get("BENCH_BASE_URL", "http://localhost:8090")
CLUSTER = os.environ.get("BENCH_CLUSTER", "")
STREAM = "bench"
STREAM_KEY = HashTagKeys().stream(Stream(STREAM)) if CLUSTER else STREAM
SINK = "bench:done"
GROUP = "g"
if CLUSTER:
    _host, _, _port = CLUSTER.partition(":")
    r = redis.cluster.RedisCluster(host=_host, port=int(_port))
else:
    r = redis.Redis.from_url(REDIS)

gateway: subprocess.Popen | None = None
gateway_trace = False


def wait_http(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=1).read()
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"{url} did not come up")


def stats() -> dict:
    return json.loads(urllib.request.urlopen(BASE + "/stats", timeout=5).read())


def reset() -> None:
    urllib.request.urlopen(BASE + "/reset", timeout=5).read()


def start_gateway(trace: bool = False) -> None:
    global gateway, gateway_trace
    if gateway is not None and gateway_trace == trace:
        return
    stop_gateway()
    env = {**os.environ, "REDIS_URL": REDIS, "STREAM": STREAM, "BENCH_CLUSTER": CLUSTER,
           "GATEWAY_TRACE": "1" if trace else "0"}
    gateway = subprocess.Popen([PY, str(HERE / "gateway.py")], env=env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    gateway_trace = trace
    wait_http(BASE + "/health")


def stop_gateway() -> None:
    global gateway
    if gateway is not None:
        gateway.terminate()
        try:
            gateway.wait(10)
        except subprocess.TimeoutExpired:
            gateway.kill()
        gateway = None


def start_workers(n: int, **cfg) -> list[subprocess.Popen]:
    procs = []
    for i in range(1, n + 1):
        env = {
            **os.environ,
            "REDIS_URL": REDIS, "STREAM": cfg.get("stream", STREAM), "GROUP": GROUP, "SINK": SINK,
            "BENCH_CLUSTER": CLUSTER,
            "CONSUMER": (f"{cfg['consumer']}-{i}" if cfg.get("consumer")
                         else f"w{i}-{cfg.get('tag', 'x')}"),
            "NODE": f"bench-{cfg.get('tag', 'x')}-{i}",
            "LANES": str(cfg.get("lanes", 4)), "BATCH": str(cfg.get("batch", 50)),
            "BLOCK_MS": str(cfg.get("block_ms", 1000)),
            "MAX_RETRIES": str(cfg.get("max_retries", 3)),
            "FAIL_RATE": str(cfg.get("fail_rate", 0)),
            "CONTROL": cfg.get("control", "off"),
            "CONTROL_URL": cfg.get("control_url", ""),
            "API_KEY": cfg.get("api_key", ""),
            "TRACE_RECORDS": "1" if cfg.get("trace_records") else "0",
            "CAPTURE_LOGS": "1" if cfg.get("capture_logs") else "0",
            "IDEMPOTENT": "1" if cfg.get("idempotent") else "0",
        }
        procs.append(subprocess.Popen([PY, str(HERE / "worker.py")], env=env,
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    return procs


def stop_workers(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(15)
        except subprocess.TimeoutExpired:
            p.kill()


def lag() -> tuple[int | None, int | None]:
    try:
        for g in r.xinfo_groups(STREAM_KEY):
            if (g.get("name") or b"").decode() == GROUP:
                return g.get("lag"), g.get("pending")
    except Exception:
        pass
    return None, None


def drain(timeout: float = 20.0) -> tuple[int | None, int | None]:
    deadline = time.time() + timeout
    pending = None
    while time.time() < deadline:
        lagged, pending = lag()
        if lagged in (0, None) and pending in (0, None):
            return 0, 0
        time.sleep(0.5)
    return lag()


def run_k6(rate: int, duration: str, pad: int = 1024, name: str = "stage") -> dict:
    out = HERE / f"k6-{name}.json"
    out.unlink(missing_ok=True)
    env = {**os.environ, "RATE": str(rate), "DURATION": duration, "BASE": BASE,
           "PAD_BYTES": str(pad), "PRE_VUS": str(min(200, max(20, rate // 20 + 10))),
           "MAX_VUS": str(min(300, max(50, rate // 10 + 20)))}
    with open(HERE / f"k6-{name}.log", "w") as log:
        subprocess.run(["k6", "run", "--quiet", "--summary-export", str(out),
                        str(HERE / "k6-data.js")], env=env, check=False,
                       stdout=log, stderr=subprocess.STDOUT)
    try:
        return json.loads(out.read_text())
    except Exception:
        return {}


def collect(start_wall_ms: int) -> dict:
    rows = r.lrange(SINK, 0, -1)
    r.delete(SINK)
    seen: dict[str, int] = {}
    lat = []
    dup = 0
    for raw in rows:
        _stream_name, seq, sent, done, _consumer = raw.decode().split("|")
        done_i, sent_i = int(done), int(sent)
        if seq in seen:
            dup += 1
            continue
        seen[seq] = done_i
        if done_i >= start_wall_ms + 3000:
            lat.append(done_i - sent_i)
    lat.sort()
    pct = lambda p: lat[min(len(lat) - 1, int(len(lat) * p))] if lat else None  # noqa: E731
    return {"completed": len(seen), "dup": dup, "p50": pct(0.5), "p95": pct(0.95),
            "p99": pct(0.99), "lat_n": len(lat)}


def k6_values(summary: dict, metric: str) -> dict:
    m = (summary.get("metrics", {}).get(metric, {}) or {})
    return m.get("values", m) or {}


def stage(rate: int, duration: str = "40s", workers: int = 2, pad: int = 1024,
          name: str = "stage", kill_worker_after: float | None = None, **cfg) -> dict:
    global workers_active
    start_gateway(trace=cfg.get("trace_records", False))
    stop_workers_active()
    if CLUSTER:
        r.flushall(target_nodes=list(r.get_primaries()))
    else:
        r.flushall()
    time.sleep(0.3)
    reset()
    procs = start_workers(workers, tag=f"{rate}", **cfg)
    workers_active = procs
    time.sleep(1.5)
    start_ms = int(time.time() * 1000)
    if kill_worker_after is None:
        summary = run_k6(rate, duration, pad, name)
    else:
        env = {**os.environ, "RATE": str(rate), "DURATION": duration, "BASE": BASE,
               "PAD_BYTES": str(pad), "PRE_VUS": str(min(200, max(20, rate // 20 + 10))),
               "MAX_VUS": str(min(300, max(50, rate // 10 + 20)))}
        out = HERE / f"k6-{name}.json"
        out.unlink(missing_ok=True)
        k6p = subprocess.Popen(["k6", "run", "--quiet", "--summary-export", str(out),
                                str(HERE / "k6-data.js")], env=env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(kill_worker_after)
        if len(procs) > 1:
            procs[-1].send_signal(signal.SIGKILL)
        k6p.wait()
        summary = json.loads(out.read_text()) if out.exists() else {}
    lagged, pending = drain(25)
    stop_workers(procs)
    workers_active = []
    elapsed = max(1.0, (int(time.time() * 1000) - start_ms) / 1000)
    pub = stats()
    done = collect(start_ms)
    httpd = k6_values(summary, "http_req_duration")
    reqs = k6_values(summary, "http_reqs")
    result = {
        "stage": name, "rate": rate, "workers": workers, "lanes": cfg.get("lanes", 4),
        "batch": cfg.get("batch", 50), "block": cfg.get("block_ms", 1000),
        "published": pub["published"], "errors": pub["errors"],
        "completed": done["completed"], "dup": done["dup"],
        "lag": lagged, "pending": pending,
        "p50": done["p50"], "p95": done["p95"], "p99": done["p99"],
        "enq_p95": httpd.get("p(95)"), "http_rate": reqs.get("rate"),
        "achieved": round(pub["published"] / elapsed, 1),
        "config": {k: v for k, v in cfg.items() if k not in ("tag",)},
    }
    print(json.dumps(result), flush=True)
    return result


workers_active: list[subprocess.Popen] = []


def stop_workers_active() -> None:
    global workers_active
    if workers_active:
        stop_workers(workers_active)
        workers_active = []


def suite_curve() -> None:
    for rate in (200, 500, 1000, 2000, 4000):
        stage(rate, "40s", 2, name=f"curve-{rate}")


def suite_tune() -> None:
    for lanes, batch, block in ((1, 10, 1000), (4, 50, 1000), (8, 100, 1000), (4, 50, 100)):
        stage(800, "30s", 1, name=f"tune-{lanes}-{batch}-{block}",
              lanes=lanes, batch=batch, block_ms=block)


def suite_features() -> None:
    stage(1500, "30s", 2, name="feat-baseline")
    stage(1500, "30s", 2, name="feat-control", control="valkey")
    stage(1500, "30s", 2, name="feat-traces", control="valkey", trace_records=True)
    stage(1500, "30s", 2, name="feat-logs", capture_logs=True)
    stage(1500, "30s", 2, name="feat-idempotent", idempotent=True)
    stage(1500, "30s", 2, name="feat-baseline2")


def suite_failures() -> None:
    result = stage(800, "40s", 2, name="failures", fail_rate=0.05, max_retries=2)
    dlq = r.xlen(STREAM + "-dlq")
    pending = result["pending"]
    print(json.dumps({"dlq": dlq, "pending": pending}), flush=True)
    # time a full replay of the DLQ through the broker
    code = (
        "import time; from rillkit import Broker, Settings, Stream;"
        f"b=Broker(settings=Settings(url='{REDIS}'));"
        "t=time.perf_counter(); n=b.replay_dlq(Stream('bench', group='g'), count=100000);"
        "print(n, round(time.perf_counter()-t, 3)); b.stop(5)"
    )
    subprocess.run([PY, "-c", code], check=False)


def suite_producer() -> None:
    for rate in (500, 1000, 2000, 4000):
        start_gateway(trace=False)
        stop_workers_active()
        r.flushall()
        reset()
        start_ms = int(time.time() * 1000)
        summary = run_k6(rate, "30s", name=f"producer-{rate}")
        elapsed = max(1.0, (int(time.time() * 1000) - start_ms) / 1000)
        pub = stats()
        httpd = k6_values(summary, "http_req_duration")
        print(json.dumps({"stage": f"producer-{rate}", "rate": rate,
                          "published": pub["published"], "errors": pub["errors"],
                          "achieved": round(pub["published"] / elapsed, 1),
                          "enq_p95": httpd.get("p(95)"),
                          "lag": lag()[0]}), flush=True)


def suite_recovery() -> None:
    stage(800, "50s", 2, name="recovery", kill_worker_after=15)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("suite", choices=["curve", "tune", "features", "failures",
                                          "producer", "recovery"])
    args = parser.parse_args()
    try:
        {"curve": suite_curve, "tune": suite_tune, "features": suite_features,
         "failures": suite_failures, "producer": suite_producer,
         "recovery": suite_recovery}[args.suite]()
    finally:
        stop_workers_active()
        stop_gateway()


if __name__ == "__main__":
    main()
