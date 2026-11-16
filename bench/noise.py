"""Two tenants on one Redis: does a hot stream hurt a light one?"""
import json
import threading
import time
import run as h

def read_sink() -> dict:
    rows = h.r.lrange(h.SINK, 0, -1)
    h.r.delete(h.SINK)
    out = {}
    for raw in rows:
        stream, seq, sent, done, _consumer = raw.decode().split("|")
        out.setdefault(stream, []).append(int(done) - int(sent))
    result = {}
    for stream, lat in out.items():
        lat.sort()
        p = lambda q: lat[min(len(lat) - 1, int(len(lat) * q))]
        result[stream] = {"n": len(lat), "p50": p(.5), "p95": p(.95), "p99": p(.99)}
    return result

def k6(hot: int, light: int, duration: str) -> None:
    import os, subprocess
    env = {**os.environ, "BASE": h.BASE, "HOT_RATE": str(hot), "LIGHT_RATE": str(light),
           "DURATION": duration, "PAD_BYTES": "1024"}
    subprocess.run(["k6", "run", "--quiet", "--summary-export", str(h.HERE / "k6-noise.json"),
                    str(h.HERE / "noise.js")], env=env, check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

h.start_gateway(trace=False)
h.r.flushall()
hot = h.start_workers(2, tag="hot", stream="noise.hot", consumer="hot-1", lanes=4)
light = h.start_workers(2, tag="light", stream="noise.light", consumer="light-1", lanes=2)
try:
    time.sleep(2)
    k6(10, 50, "20s")
    time.sleep(2)
    print(json.dumps({"phase": "solo-light", **read_sink()}), flush=True)
    k6(2500, 50, "30s")
    time.sleep(3)
    print(json.dumps({"phase": "noisy", **read_sink()}), flush=True)
finally:
    h.stop_workers(hot)
    h.stop_workers(light)
    h.stop_gateway()
