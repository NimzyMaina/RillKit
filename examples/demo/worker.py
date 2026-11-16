"""A demo fleet for rillkit-server: workers, a producer whose load rises and falls, and a
two-stream flow for the trace view.

    DEMO_ROLE=worker     consume "orders" and "emails" (the default)
    DEMO_ROLE=producer   publish to both in waves, so lag builds up and drains
    DEMO_ROLE=flow       publish orders.raw -> orders.enriched, with one transient failure
                         in the second stream so the dashboard's trace view shows a retry

Everything else comes from the standard RILLKIT_* variables (see compose.yaml): the Redis or
Valkey connection, and RILLKIT_CONTROL=relay with the control url and API key.
"""

import logging
import math
import os
import random
import time

from rillkit import Broker, LogCapture, Message, Stream

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("demo")

# trimmed to about 50,000 entries each, so a demo left running does not fill the server
ORDERS = Stream("orders", group="billing", maxlen=50_000)
EMAILS = Stream("emails", group="mailer", maxlen=50_000)
RAW = Stream("orders.raw", group="ingest", maxlen=50_000)
ENRICHED = Stream("orders.enriched", group="enrich", maxlen=50_000)


def run_worker() -> None:
    # The dashboard shows each message's logs (RILLKIT_CAPTURE_LOGS). By default only an
    # exception's class is kept, since its text often holds customer data; the demo's data is
    # made up, so keep the full message and traceback too.
    broker = Broker.from_env(middlewares=[LogCapture(exceptions=True)])

    @broker.subscribe(ORDERS, max_retries=2, claim_idle_ms=5_000, block_ms=1_000)
    def charge(order: dict, msg: Message) -> None:
        # the message id is what the dashboard's "Message logs" lookup asks for
        log.info("charging order %s (%s) [message %s]", order["id"], order["amount"], msg.id)
        time.sleep(random.uniform(0.02, 0.12))
        if random.random() < 0.03:
            raise ValueError(f"card declined for order {order['id']}")

    @broker.subscribe(EMAILS, block_ms=1_000)
    def send(email: dict) -> None:
        log.info("sending %s email", email["kind"])
        time.sleep(random.uniform(0.01, 0.05))

    broker.run()


def run_producer() -> None:
    broker = Broker.from_env()
    period = float(os.environ.get("DEMO_WAVE_SEC", "180"))
    peak = float(os.environ.get("DEMO_PEAK_PER_SEC", "60"))
    started, n = time.monotonic(), 0
    while True:
        phase = (time.monotonic() - started) % period / period
        rate = max(2.0, peak * (0.5 - 0.5 * math.cos(2 * math.pi * phase)))  # 2 .. peak per s
        n += 1
        broker.publish(ORDERS, {"id": n, "amount": round(random.uniform(5, 500), 2)})
        if n % 3 == 0:
            broker.publish(EMAILS, {"kind": random.choice(["receipt", "welcome", "reminder"])})
        if n % 200 == 0:
            log.info("published %d orders, now %.0f per second", n, rate)
        time.sleep(1 / rate)


def run_flow() -> None:
    """One order through two streams, every DEMO_FLOW_INTERVAL_SEC seconds (default 120).

    The first stream always succeeds; the second fails its first delivery on purpose and
    succeeds on the retry, so a trace has three steps: ok, error, ok.
    """
    broker = Broker.from_env()
    interval = float(os.environ.get("DEMO_FLOW_INTERVAL_SEC", "120"))

    @broker.subscribe(RAW, max_retries=0, block_ms=1_000)
    def ingest(order: dict, msg: Message) -> None:
        log.info("validating order %s [message %s]", order["id"], msg.id)
        time.sleep(0.06)  # a validating step
        broker.publish(ENRICHED, {"order": order["id"], "checked": True, "retry": True})

    @broker.subscribe(ENRICHED, max_retries=2, claim_idle_ms=150, block_ms=1_000)
    def enrich(body: dict, msg: Message) -> None:
        if body.get("retry") and msg.deliveries == 1:
            log.warning("transient enrichment failure for order %s (delivery 1)", body["order"])
            raise ValueError("transient enrichment failure (demo)")
        log.info("enriching order %s (delivery %s)", body["order"], msg.deliveries)
        time.sleep(0.12)  # an enrichment step

    broker.start()
    try:
        n = 0
        while True:
            broker.publish(RAW, {"id": 4000 + n, "amount": round(random.uniform(5, 500), 2)})
            log.info("flow published order %s", 4000 + n)
            n += 1
            time.sleep(interval)
    finally:
        broker.stop(5)


if __name__ == "__main__":
    role = os.environ.get("DEMO_ROLE")
    if role == "producer":
        run_producer()
    elif role == "flow":
        run_flow()
    else:
        run_worker()
