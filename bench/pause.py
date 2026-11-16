"""Pause / resume / drain convergence under live load, through the valkey control plane."""

import json
import threading
import time
import uuid

import run as h
from rillkit import Broker, Control, Settings, Stream

h.start_gateway(trace=False)
h.r.flushall()
h.stop_workers_active()
h.workers_active = h.start_workers(1, tag="pause", control="valkey", lanes=4)
admin = Broker(settings=Settings(url=h.REDIS), control=Control(mode="valkey", node_id="admin"))

def apply(state: str) -> None:
    ack = admin.control.apply({"type": "DIRECTIVE", "directive_id": f"dir-{uuid.uuid4().hex[:8]}",
                               "cmd": "SET_STATE", "scope": "global",
                               "params": {"state": state}})
    assert ack["status"] == "APPLIED", ack

def wait_stable(seconds: float = 1.0, timeout: float = 10.0) -> tuple[int, float]:
    deadline = time.monotonic() + timeout
    last = h.r.llen(h.SINK)
    changed = time.monotonic()
    while time.monotonic() < deadline:
        time.sleep(0.1)
        now = h.r.llen(h.SINK)
        if now != last:
            last, changed = now, time.monotonic()
        elif time.monotonic() - changed >= seconds:
            break
    return last, time.monotonic()

record: dict = {"rate": 500}
try:
    time.sleep(2)
    k6 = threading.Thread(target=h.run_k6, args=(500, "25s"), kwargs={"name": "pause"}, daemon=True)
    k6.start()
    time.sleep(6)

    before = h.r.llen(h.SINK)
    t0 = time.monotonic()
    apply("PAUSED")
    count, stable_at = wait_stable()
    record["pause_ms"] = round((stable_at - t0) * 1000)
    record["handled_after_pause"] = count - before
    record["lag_paused"] = h.lag()[0]

    time.sleep(2)
    t1 = time.monotonic()
    apply("RUNNING")
    deadline = time.monotonic() + 10
    while h.r.llen(h.SINK) <= count and time.monotonic() < deadline:
        time.sleep(0.05)
    record["resume_ms"] = round((time.monotonic() - t1) * 1000)

    apply("DRAINING")
    time.sleep(3)
    record["consumers_after_drain"] = admin.active_workers(Stream(h.STREAM, group=h.GROUP))
    print(json.dumps(record), flush=True)
finally:
    h.stop_workers_active()
    h.stop_gateway()
    admin.client.close()
