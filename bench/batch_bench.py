import os
import time
import redis
from rillkit import Broker, Settings, Stream

URL = os.environ.get("BENCH_REDIS_URL", "redis://localhost:6380/0")
r = redis.Redis.from_url(URL)
r.flushdb()
b = Broker(settings=Settings(url=URL, max_connections=200))
body = lambda i: {"seq": i, "pad": "x" * 1024}
N = 1000

t = time.perf_counter()
for i in range(N):
    b.publish("bench-batch", body(i))
single = time.perf_counter() - t
r.delete("bench-batch")

t = time.perf_counter()
ids = b.publish_many("bench-batch", [body(i) for i in range(N)])
batch = time.perf_counter() - t
print({"messages": N, "single_s": round(single, 3), "batch_s": round(batch, 3),
       "single_per_s": round(N / single), "batch_per_s": round(N / batch),
       "ids": len(ids), "xlen": r.xlen("bench-batch")})
b.stop(5)
