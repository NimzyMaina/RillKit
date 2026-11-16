"""One benchmark worker: consumes a stream with N lanes, writes completions to a Redis list."""

import logging
import os
import random
import time

import redis

from rillkit import (
    Broker,
    Control,
    HashTagKeys,
    Idempotent,
    LogCapture,
    Message,
    PlainKeys,
    Settings,
    Stream,
)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380/0")
CLUSTER = os.environ.get("BENCH_CLUSTER", "")
STREAM = os.environ.get("STREAM", "bench")
GROUP = os.environ.get("GROUP", "g")
SINK = os.environ.get("SINK", "bench:done")
CONSUMER = os.environ.get("CONSUMER", "w1")
LANES = int(os.environ.get("LANES", "4"))
BATCH = int(os.environ.get("BATCH", "50"))
BLOCK_MS = int(os.environ.get("BLOCK_MS", "1000"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0"))
CONTROL = os.environ.get("CONTROL", "off")        # off | valkey | relay
CONTROL_URL = os.environ.get("CONTROL_URL")
API_KEY = os.environ.get("API_KEY")
TRACE_RECORDS = os.environ.get("TRACE_RECORDS") == "1"
CAPTURE_LOGS = os.environ.get("CAPTURE_LOGS") == "1"
IDEMPOTENT = os.environ.get("IDEMPOTENT") == "1"
NODE = os.environ.get("NODE", CONSUMER)

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger("bench")

if CLUSTER:
    _host, _, _port = CLUSTER.partition(":")
    sink = redis.RedisCluster(host=_host, port=int(_port))
else:
    sink = redis.Redis.from_url(REDIS_URL)

middlewares = []
if CAPTURE_LOGS:
    middlewares.append(LogCapture(loggers=["bench"]))
if IDEMPOTENT:
    middlewares.append(Idempotent(key=lambda m: str(m.body["seq"]), ttl=3600))

if CONTROL == "relay":
    control = Control(mode="relay", node_id=NODE, trace_records=TRACE_RECORDS,
                      url=CONTROL_URL, api_key=API_KEY)
elif CONTROL == "valkey":
    control = Control(mode="valkey", node_id=NODE, trace_records=TRACE_RECORDS)
else:
    control = Control()
if CLUSTER:
    _host, _, _port = CLUSTER.partition(":")
    _settings = Settings(mode="cluster", cluster_nodes=[(_host, int(_port))], max_connections=200)
    _keys = HashTagKeys()
else:
    _settings = Settings(url=REDIS_URL, max_connections=200)
    _keys = PlainKeys()
broker = Broker(settings=_settings, keys=_keys, control=control, middlewares=middlewares)


@broker.subscribe(
    Stream(STREAM, group=GROUP), consumer=CONSUMER, concurrency=LANES, count=BATCH,
    block_ms=BLOCK_MS, max_retries=MAX_RETRIES, claim_idle_ms=2000,
)
def handle(body: dict, msg: Message) -> None:
    if FAIL_RATE and random.random() < FAIL_RATE:
        raise ValueError("bench failure")
    if CAPTURE_LOGS:
        log.info("handled %s", body["seq"])
    done = int(time.time() * 1000)
    sink.rpush(SINK, f"{msg.stream.name}|{body['seq']}|{body['sent_ms']}|{done}|{msg.consumer}")


if __name__ == "__main__":
    broker.run(handle_signals=True, shutdown_timeout=10)
