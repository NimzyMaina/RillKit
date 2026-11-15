import os
import signal
import threading

from helpers import fast, hk, pending, read_count, sk, wait_until
from rillkit import Broker, Message


def _register_hooks(broker: Broker, events: list[str]) -> None:
    @broker.on_startup
    def a():
        events.append("on_startup")

    @broker.after_startup
    def b():
        events.append("after_startup")

    @broker.on_shutdown
    def c():
        events.append("on_shutdown")

    @broker.after_shutdown
    def d():
        events.append("after_shutdown")


EXPECTED = ["on_startup", "after_startup", "on_shutdown", "after_shutdown"]


def test_hooks_order_with_start_and_stop(broker: Broker, name):
    events: list[str] = []
    _register_hooks(broker, events)

    @broker.subscribe(name, group="g", **fast())
    def handle(body): ...

    broker.start()
    assert events == ["on_startup", "after_startup"]
    broker.stop(timeout=5)
    assert events == EXPECTED


def test_hook_state_at_each_stage(broker: Broker, r, name):
    state = {}

    @broker.subscribe(name, group="g", **fast())
    def handle(body): ...

    @broker.on_startup
    def before():
        state["group_exists_in_on_startup"] = bool(r.exists(sk(name)))

    @broker.after_startup
    def after():
        state["group_exists_after_startup"] = bool(r.exists(sk(name)))
        state["alive_after_startup"] = broker.subscribers[0]._thread.is_alive()

    @broker.on_shutdown
    def down():
        state["alive_in_on_shutdown"] = broker.subscribers[0]._thread.is_alive()

    broker.start()
    broker.stop(timeout=5)
    assert state == {
        "group_exists_in_on_startup": False,
        "group_exists_after_startup": True,
        "alive_after_startup": True,
        "alive_in_on_shutdown": False,
    }


def test_hooks_return_the_function(broker: Broker):
    def f(): ...

    assert broker.on_startup(f) is f
    assert broker.after_startup(f) is f
    assert broker.on_shutdown(f) is f
    assert broker.after_shutdown(f) is f


def test_multiple_hooks_run_in_registration_order(broker: Broker):
    events: list[str] = []
    broker.on_startup(lambda: events.append("one"))
    broker.on_startup(lambda: events.append("two"))
    broker.start()
    broker.stop(timeout=5)
    assert events == ["one", "two"]


def test_after_shutdown_runs_even_if_on_shutdown_raises(broker: Broker):
    events: list[str] = []

    @broker.on_shutdown
    def boom():
        raise RuntimeError("x")

    broker.after_shutdown(lambda: events.append("after"))
    broker.start()
    try:
        broker.stop(timeout=5)
    except RuntimeError:
        events.append("raised")
    assert events == ["after", "raised"]


def test_run_in_thread_then_stop(broker: Broker, name):
    events: list[str] = []
    _register_hooks(broker, events)
    got = []

    @broker.subscribe(name, group="g", **fast())
    def handle(body):
        got.append(body)

    t = threading.Thread(target=broker.run, kwargs={"handle_signals": False}, daemon=True)
    t.start()
    assert wait_until(lambda: "after_startup" in events)
    broker.publish(name, {"x": 1})
    assert wait_until(lambda: got == [{"x": 1}])
    broker.stop(timeout=5)
    t.join(10)
    assert not t.is_alive()
    assert events == EXPECTED  # run() must not run the shutdown hooks a second time


def test_run_main_thread_stops_on_sigint(broker: Broker, name):
    events: list[str] = []
    _register_hooks(broker, events)
    previous = signal.getsignal(signal.SIGINT)

    @broker.subscribe(name, group="g", **fast())
    def handle(body): ...

    def send_sigint():
        wait_until(lambda: "after_startup" in events)
        os.kill(os.getpid(), signal.SIGINT)

    t = threading.Thread(target=send_sigint, daemon=True)
    t.start()
    broker.run(shutdown_timeout=5)
    t.join(5)
    assert events == EXPECTED
    assert signal.getsignal(signal.SIGINT) is previous  # handlers restored


def test_graceful_stop_finishes_in_flight_message(broker: Broker, r, name):
    started, release = threading.Event(), threading.Event()
    finished = []

    @broker.subscribe(name, group="g", **fast())
    def handle(body):
        started.set()
        assert release.wait(10)
        finished.append(body["n"])

    for n in range(3):
        broker.publish(name, {"n": n})  # published first so they arrive in one batch
    broker.start()
    assert started.wait(5)

    stopper = threading.Thread(target=broker.stop, kwargs={"timeout": 10})
    stopper.start()
    stopper.join(0.4)
    assert stopper.is_alive()  # waiting for the in-flight message
    assert finished == []

    release.set()
    stopper.join(10)
    assert not stopper.is_alive()
    assert finished == [0]  # the unread rest of the batch is not processed
    assert pending(r, name) == 2  # first one acked, the other two stay pending for a later claim


def test_unprocessed_leftovers_are_picked_up_after_restart(make_broker, r, name):
    first = make_broker()
    started, release = threading.Event(), threading.Event()

    @first.subscribe(name, group="g", **fast())
    def one(body):
        started.set()
        release.wait(10)

    for n in range(3):
        first.publish(name, {"n": n})
    first.start()
    assert started.wait(5)
    stopper = threading.Thread(target=first.stop, kwargs={"timeout": 10})
    stopper.start()
    release.set()
    stopper.join(10)

    second = make_broker()
    got = []

    @second.subscribe(name, group="g", **fast())
    def two(body):
        got.append(body["n"])

    second.start()
    assert wait_until(lambda: sorted(got) == [1, 2] and pending(r, name) == 0)


def test_heartbeat_registered_while_running_and_removed_after_stop(broker: Broker, r, name):
    @broker.subscribe(name, group="g", consumer="worker-1", **fast())
    def handle(body): ...

    assert broker.active_workers(name, "g") == []
    broker.start()
    assert wait_until(lambda: broker.active_workers(name, "g") == ["worker-1"])
    assert broker.active_workers(name, "g") == ["worker-1"]
    key = hk(name, "g", "worker-1")
    assert 0 < r.ttl(key) <= 60
    broker.stop(timeout=5)
    assert not r.exists(key)
    assert Broker(client=r).active_workers(name, "g") == []  # a stopped broker's client is closed


def test_active_workers_uses_stream_group_by_default(broker: Broker, name):
    from rillkit import Stream

    @broker.subscribe(Stream(name, group="g"), consumer="w", **fast())
    def handle(body): ...

    broker.start()
    assert wait_until(lambda: broker.active_workers(Stream(name, group="g")) == ["w"])


def test_heartbeat_ttl_is_configurable(broker: Broker, r, name):
    @broker.subscribe(name, group="g", consumer="w", heartbeat_ttl=5, **fast())
    def handle(body): ...

    broker.start()
    assert wait_until(lambda: r.exists(hk(name, "g", "w")))
    assert r.ttl(hk(name, "g", "w")) <= 5


def test_heartbeat_disabled(broker: Broker, r, name):
    @broker.subscribe(name, group="g", consumer="w", heartbeat=False, **fast())
    def handle(msg: Message): ...

    broker.start()
    broker.publish(name, {})
    assert wait_until(lambda: pending(r, name) == 0 and read_count(r, name) == 1)
    assert broker.active_workers(name, "g") == []


def test_two_consumers_in_one_group_share_work(make_broker, r, name):
    seen = {"a": [], "b": []}
    brokers = []
    for label in ("a", "b"):
        b = make_broker()
        brokers.append(b)

        def make_handler(label):
            def handle(body):
                seen[label].append(body["n"])

            return handle

        b.subscribe(name, group="g", consumer=label, count=1, **fast())(make_handler(label))
        b.start()
    for n in range(20):
        brokers[0].publish(name, {"n": n})
    assert wait_until(lambda: len(seen["a"]) + len(seen["b"]) == 20 and pending(r, name) == 0)
    assert sorted(seen["a"] + seen["b"]) == list(range(20))
    assert wait_until(lambda: brokers[0].active_workers(name, "g") == ["a", "b"])


def _count_closes(broker: Broker) -> list:
    closes: list = []
    real_close = broker.client.close

    def close():
        closes.append(1)
        real_close()

    broker.client.close = close
    return closes


def test_stop_closes_the_client_exactly_once(broker: Broker, name):
    closes = _count_closes(broker)

    @broker.subscribe(name, group="g", **fast())
    def handle(body): ...

    broker.start()
    stoppers = [threading.Thread(target=broker.stop, kwargs={"timeout": 5}) for _ in range(3)]
    for t in stoppers:
        t.start()
    for t in stoppers:
        t.join(10)
    broker.stop(timeout=5)
    assert closes == [1]


def test_client_is_closed_even_when_a_shutdown_hook_raises(broker: Broker):
    closes = _count_closes(broker)

    @broker.on_shutdown
    def boom():
        raise RuntimeError("x")

    broker.start()
    try:
        broker.stop(timeout=5)
    except RuntimeError:
        pass
    assert closes == [1]


def test_injected_client_is_used_as_is(server_settings):
    from rillkit import HashTagKeys, Stream

    client = server_settings().client()
    broker = Broker(client=client, keys=HashTagKeys(prefix="app"))
    try:
        assert broker.client is client
        assert broker.keys.stream(Stream("o")) == "app:{o}:stream"
    finally:
        client.close()


def test_registry_lists_live_consumers_without_scan(broker: Broker, r, name, monkeypatch):
    import time as _time

    from helpers import ck

    @broker.subscribe(name, group="g", consumer="worker-7", heartbeat_ttl=30, **fast())
    def handle(body): ...

    def no_scan(*args, **kwargs):
        raise AssertionError("active_workers must not SCAN")

    monkeypatch.setattr(broker.client, "scan_iter", no_scan)
    broker.start()
    assert wait_until(lambda: broker.active_workers(name, "g") == ["worker-7"])
    expires = r.zscore(ck(name, "g"), "worker-7")
    assert 25_000 < expires - _time.time() * 1000 <= 30_000  # alive until now + heartbeat_ttl
    broker.stop(timeout=5)
    assert r.zscore(ck(name, "g"), "worker-7") is None  # removed on a clean stop
    assert Broker(client=r).active_workers(name, "g") == []


def test_registry_ignores_expired_entries_and_beats_prune_them(broker: Broker, r, name):
    import time as _time

    from helpers import ck

    now_ms = int(_time.time() * 1000)
    r.zadd(ck(name, "g"), {"crashed": now_ms - 1000, "alive-elsewhere": now_ms + 60_000})
    assert broker.active_workers(name, "g") == ["alive-elsewhere"]

    @broker.subscribe(name, group="g", consumer="worker-7", **fast())
    def handle(body): ...

    broker.start()
    assert wait_until(lambda: r.zscore(ck(name, "g"), "crashed") is None)  # pruned by a beat
    assert broker.active_workers(name, "g") == ["alive-elsewhere", "worker-7"]
