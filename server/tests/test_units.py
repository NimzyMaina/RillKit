"""Pure logic: scaling decisions, the overview, HELLO validation, settings."""

from datetime import UTC, datetime, timedelta

import pytest

from rillkit_server.autoscaler import decide
from rillkit_server.config import ServerSettings, SettingsError
from rillkit_server.gateway import Refused, check_hello
from rillkit_server.store import build_overview, most_restrictive

NOW = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
POLICY = {"min_workers": 1, "max_workers": 10, "target_lag_per_worker": 100,
          "upscale_cooldown_sec": 30, "downscale_cooldown_sec": 120, "last_scaled_at": None}


@pytest.mark.parametrize("lag, nodes, concurrency, per_node", [
    (0, 1, 1, None),          # idle at the minimum: nothing to do
    (450, 1, 1, 5),           # ceil(450 / 100) = 5
    (450, 2, 2, 3),           # 5 spread over 2 nodes -> 3 each
    (5000, 1, 1, 10),         # capped at max_workers
    (0, 1, 8, 1),             # back down to the minimum
    (300, 3, 3, None),        # 3 wanted, 1 per node already
])
def test_decide(lag, nodes, concurrency, per_node):
    summary = {"lag": lag, "nodes": nodes, "concurrency": concurrency}
    assert decide(POLICY, summary, NOW).per_node == per_node


def test_decide_respects_cooldowns_per_direction():
    summary = {"lag": 450, "nodes": 1, "concurrency": 1}
    recent = POLICY | {"last_scaled_at": NOW - timedelta(seconds=10)}
    held = decide(recent, summary, NOW)
    assert held.per_node is None and "cooling down for 20 s" in held.text
    assert decide(recent, {"lag": 0, "nodes": 1, "concurrency": 5}, NOW).per_node is None
    later = POLICY | {"last_scaled_at": NOW - timedelta(seconds=31)}
    assert decide(later, summary, NOW).per_node == 5
    assert decide(POLICY, None, NOW).text == "no nodes are consuming this stream"


def test_decide_uses_the_arrival_rate_so_a_healthy_stream_does_not_collapse():
    policy = POLICY | {"max_workers": 8}  # min 1, target lag 100, up to 8
    busy = {"lag": 0, "nodes": 1, "concurrency": 8, "ack_rate_sec": 2000.0}
    # 2000/s at a measured 250/s per worker wants ceil(2000/187.5) = 11 -> capped at 8,
    # which is where it already is: do not scale down just because lag is zero.
    assert decide(policy, busy, NOW, arrival=2000.0, peak_per_worker=250.0).per_node is None
    quiet = {"lag": 0, "nodes": 1, "concurrency": 8, "ack_rate_sec": 500.0}
    # 500/s needs 3 workers, so a downscale is right when the load really drops.
    assert decide(policy, quiet, NOW, arrival=500.0, peak_per_worker=250.0).per_node == 3
    idle = {"lag": 0, "nodes": 1, "concurrency": 8}
    assert decide(policy, idle, NOW, arrival=0.0, peak_per_worker=250.0).per_node == 1
    backlog = {"lag": 5000, "nodes": 1, "concurrency": 2}
    assert decide(policy, backlog, NOW, arrival=500.0, peak_per_worker=250.0).per_node == 8


def test_rate_state_seeds_from_samples():
    from rillkit_server.autoscaler import rate_state

    rows = [
        {"ts": NOW - timedelta(seconds=5), "lag": 0, "ack_rate_sec": 2000.0, "concurrency": 4},
        {"ts": NOW, "lag": 100, "ack_rate_sec": 2000.0, "concurrency": 4},
    ]
    state = rate_state(rows, 300)
    assert state is not None
    assert state["arrival"] == 2020.0  # 2000 finished/s + 100 backlog in 5 s
    assert state["peak"] == 500.0      # 2000/s over 4 workers, while keeping up
    backlog = [{"ts": NOW, "lag": 5000, "ack_rate_sec": 1000.0, "concurrency": 2}]
    state = rate_state(backlog, 300)
    assert state is not None and state["peak"] == 0.0  # capacity is not learned during a backlog
    assert rate_state([], 300) is None


def test_overview_deduplicates_lag_and_sums_the_rest():
    def node(node_id, live, state, lanes, rate, lag):
        return {"node_id": node_id, "connected": live, "last_seen": NOW, "connected_at": NOW,
                "sdk": {"language": "python", "version": "0.4.0"}, "keys": None,
                "telemetry": {"workers": {"state": state, "active": lanes, "in_flight": 1},
                              "streams": [{"stream": "orders", "group": "g", "lag": lag,
                                           "pending": 3, "ack_rate_sec": rate,
                                           "concurrency": lanes, "state": state,
                                           "stream_key": "orders", "dlq_key": "orders-dlq"}]}}

    rows = [node("a", True, "RUNNING", 2, 10.0, 500), node("b", True, "PAUSED", 3, 5.5, 480),
            node("c", False, "RUNNING", 9, 99.0, 1)]
    view = build_overview({"id": "t", "name": "t", "nodes_online": 0}, rows, [],
                          lambda r: r["connected"])
    [s] = view["streams"]
    assert (s["lag"], s["pending"], s["concurrency"], s["nodes"], s["ack_rate_sec"]) == (
        500, 3, 5, 2, 15.5)
    assert s["state"] == "PAUSED" and view["state"] == "PAUSED"
    assert view["totals"] == {"nodes_online": 2, "active_workers": 5, "in_flight": 2,
                              "lag": 500, "pending": 3, "ack_rate_sec": 15.5}
    assert [n["node_id"] for n in view["nodes"]] == ["a", "b", "c"]
    assert view["nodes"][2]["connected"] is False


def test_most_restrictive():
    assert most_restrictive([]) == "RUNNING"
    assert most_restrictive(["PAUSED", None, "DRAINING", "bogus"]) == "DRAINING"


@pytest.mark.parametrize("hello, code", [
    ({"type": "TELEMETRY_REPORT"}, "BAD_FRAME"),
    ({"type": "HELLO", "protocol_version": 0}, "UNSUPPORTED_VERSION"),
    ({"type": "HELLO", "protocol_version": True}, "UNSUPPORTED_VERSION"),
    ({"type": "HELLO", "protocol_version": 1, "node_id": ""}, "BAD_FRAME"),
    ({"type": "HELLO", "protocol_version": 1, "node_id": "n", "keys": {"layout": "x"}},
     "BAD_FRAME"),
])
def test_bad_hellos(hello, code):
    with pytest.raises(Refused) as e:
        check_hello(hello)
    assert e.value.code == code


def test_settings_need_a_database_and_a_strong_token(monkeypatch):
    with pytest.raises(SettingsError, match="DATABASE_URL"):
        ServerSettings(database_url="", admin_token="x" * 16)
    with pytest.raises(SettingsError, match="16 characters"):
        ServerSettings(database_url="postgresql://x", admin_token="short")
    monkeypatch.setenv("RILLKIT_SERVER_DATABASE_URL", "postgresql://db/x")
    monkeypatch.setenv("RILLKIT_SERVER_ADMIN_TOKEN", "s3cret-token-0123456789")
    settings = ServerSettings.from_env()
    assert "s3cret" not in repr(settings) and "db/x" not in repr(settings)


def test_counter_deltas_handle_new_nodes_and_restarts():
    from rillkit_server.autoscaler import add_deltas

    def overview(succeeded, failed=0, timings=True):
        stream = {"stream": "orders", "group": "g", "succeeded_total": succeeded,
                  "failed_total": failed, "dead_lettered_total": 0, "bytes_total": succeeded * 10}
        if timings:
            stream |= {"attempts_total": succeeded, "wait_ms_total": succeeded * 5.0,
                       "work_ms_total": succeeded * 2.0}
        return {"streams": [{"stream": "orders", "group": "g", "nodes": 1}],
                "nodes": [{"node_id": "n", "connected": True, "streams": [stream]}]}

    seen: dict = {}
    first = overview(100)
    add_deltas("t", first, seen)
    assert first["streams"][0]["delta"]["succeeded"] == 0  # no baseline yet
    later = overview(130, failed=2)
    add_deltas("t", later, seen)
    assert later["streams"][0]["delta"] == {"succeeded": 30, "failed": 2, "dead_lettered": 0,
                                            "bytes": 300, "attempts": 30, "wait_ms": 150.0,
                                            "work_ms": 60.0}
    restarted = overview(5)
    add_deltas("t", restarted, seen)
    assert restarted["streams"][0]["delta"]["succeeded"] == 5  # counters reset: all new

    old: dict = {}
    add_deltas("t", overview(10, timings=False), old)  # an older worker without timings
    without = overview(16, timings=False)
    add_deltas("t", without, old)
    assert without["streams"][0]["delta"] == {"succeeded": 6, "failed": 0, "dead_lettered": 0,
                                             "bytes": 60, "attempts": 0, "wait_ms": 0,
                                             "work_ms": 0}


def test_metrics_compare_two_windows():
    from rillkit_server.store import build_metrics

    def row(ago, succeeded, failed, lag, dlq):
        return {"ts": NOW - timedelta(seconds=ago), "lag": lag, "pending": 0, "concurrency": 2,
                "succeeded": succeeded, "failed": failed, "dead_lettered": 0,
                "bytes": succeeded * 100, "dlq_length": dlq, "attempts": 4,
                "wait_ms": 100.0, "work_ms": 40.0}

    rows = [row(500, 50, 50, 900, 4), row(100, 90, 10, 100, 2), row(10, 90, 10, 300, 1)]
    m = build_metrics(rows, 300, NOW)
    assert (m["current"]["succeeded"], m["current"]["failed"]) == (180, 20)
    assert m["current"]["success_rate"] == 0.9 and m["previous"]["success_rate"] == 0.5
    assert m["current"]["per_sec"] == 0.6 and m["current"]["bytes_per_sec"] == 60.0
    assert m["current"]["lag"] == 200.0 and m["current"]["dlq_length"] == 1
    assert len(m["series"]) == 60 and sum(p["succeeded"] for p in m["series"]) == 180
    assert sum(p["ticks"] for p in m["series"]) == 2  # empty buckets say so: they are not zeros
    timed = [p for p in m["series"] if p["attempts"]]
    assert sum(p["attempts"] for p in m["series"]) == 8
    assert all((p["wait_ms"], p["work_ms"]) == (25.0, 10.0) for p in timed)
    assert any(p["wait_ms"] is None and p["work_ms"] is None for p in m["series"])
