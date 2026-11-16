import pytest
from redis.crc import key_slot

from rillkit import ConfigurationError, HashTagKeys, PlainKeys, Stream
from rillkit.keys import glob_escape


def slot(key: str) -> int:
    return key_slot(key.encode())


def test_plain_keys_are_the_original_names():
    keys = PlainKeys()
    s = Stream("orders", group="g")
    assert keys.stream(s) == "orders"
    assert keys.dlq(s) == "orders-dlq"
    assert keys.dlq(Stream("orders", dlq="dead")) == "dead"
    assert keys.heartbeat(s, "g", "w1") == "rillkit:workers:orders:g:w1"


def test_hash_tag_layout():
    keys = HashTagKeys()
    s = Stream("orders")
    assert keys.stream(s) == "rk:{orders}:stream"
    assert keys.dlq(s) == "rk:{orders}:dlq"
    assert keys.dlq(Stream("orders", dlq="dead")) == "rk:{orders}:dlq:dead"
    assert keys.heartbeat(s, "g", "w1") == "rk:{orders}:workers:g:w1"


@pytest.mark.parametrize("name", ["orders", "a}b", "{x}", "a{b", "x:y", "100%", "}"])
def test_stream_dlq_and_heartbeats_share_a_slot(name):
    keys = HashTagKeys(tenant="t-1")
    for s in (Stream(name), Stream(name, dlq="custom}{")):
        slots = {slot(keys.stream(s)), slot(keys.dlq(s)), slot(keys.heartbeat(s, "g", "c{1}"))}
        assert len(slots) == 1, name


def test_idempotency_keys():
    s = Stream("orders")
    assert PlainKeys().idempotency(s, "req-1") == "rillkit:done:orders:req-1"
    assert HashTagKeys().idempotency(s, "req-1") == "rk:{orders}:done:req-1"
    assert (HashTagKeys(prefix="shop", tenant="t-42").idempotency(s, "req-1")
            == "shop:{t-42:orders}:done:req-1")


def test_consumer_registry_keys():
    s = Stream("orders:eu")
    assert PlainKeys().consumers(s, "billing") == "rillkit:consumers:orders%3Aeu:billing"
    assert HashTagKeys().consumers(Stream("orders"), "billing") == "rk:{orders}:consumers:billing"
    assert (HashTagKeys(prefix="shop", tenant="t-42").consumers(Stream("orders"), "billing")
            == "shop:{t-42:orders}:consumers:billing")
    # a stream name with a colon can't run into the group name
    assert (PlainKeys().consumers(Stream("a:b"), "c")
            != PlainKeys().consumers(Stream("a"), "b:c"))


def test_custom_builders_keep_the_original_idempotency_key():
    from rillkit import KeyBuilder

    class Mine(KeyBuilder):
        def stream(self, stream):
            return f"x:{stream.name}"

    assert Mine().idempotency(Stream("orders"), "k") == "rillkit:done:orders:k"


def test_colons_in_stream_names_are_encoded_inside_the_tag():
    assert HashTagKeys().stream(Stream("orders:eu")) == "rk:{orders%3Aeu}:stream"
    assert HashTagKeys(tenant="t-42").stream(Stream("orders:eu")) == "rk:{t-42:orders%3Aeu}:stream"
    assert HashTagKeys().stream(Stream("orders")) == "rk:{orders}:stream"  # unchanged


def test_no_stream_name_can_produce_a_tenant_key():
    # the 0.2.0 collision: no tenant + "t-42:orders" == tenant "t-42" + "orders"
    assert (HashTagKeys().stream(Stream("t-42:orders"))
            != HashTagKeys(tenant="t-42").stream(Stream("orders")))


def test_keys_are_unique_across_tenants_and_names():
    import itertools

    parts = ["a", "b", ":", "a:b", "b:", ":a", "%", "%3A", "{", "}", "{a}", "a}b"]
    tenants = [None, *parts]
    seen = {}
    for tenant, name in itertools.product(tenants, parts):
        keys = HashTagKeys(tenant=tenant)
        s = Stream(name, dlq="d")
        for kind, key in (("stream", keys.stream(s)), ("dlq", keys.dlq(s)),
                          ("hb", keys.heartbeat(s, "g", "c")), ("done", keys.idempotency(s, "k")),
                          ("consumers", keys.consumers(s, "g"))):
            assert seen.setdefault(key, (tenant, name, kind)) == (tenant, name, kind), key
        assert len({slot(keys.stream(s)), slot(keys.dlq(s)), slot(keys.idempotency(s, "k")),
                    slot(keys.consumers(s, "g"))}) == 1


def test_no_stream_tag_can_equal_the_reserved_control_tag():
    # Control-plane keys will live under {<tenant>:} ({:} without a tenant). A stream tag is
    # {name} or {tenant:name} with a non-empty, colon-encoded name, so it never ends with ":".
    import itertools

    parts = ["a", ":", "a:", ":a", "%3A", "{", "}", "%", "t-42"]
    for tenant, name in itertools.product([None, *parts], parts):
        tag = HashTagKeys(tenant=tenant).tag(Stream(name))
        assert not tag.endswith(":}"), (tenant, name, tag)
        assert tag != "{:}"


def test_empty_stream_name_is_refused():
    with pytest.raises(ConfigurationError, match="non-empty stream name"):
        HashTagKeys().stream(Stream(""))


def test_braces_cannot_merge_two_streams():
    keys = HashTagKeys()
    assert keys.stream(Stream("a}b")) != keys.stream(Stream("a"))
    assert keys.tag(Stream("a}b")) == "{a%7Db}"


def test_tenants_never_collide():
    a = HashTagKeys(tenant="t:x").stream(Stream("y"))
    b = HashTagKeys(tenant="t").stream(Stream("x:y"))
    assert a != b
    assert HashTagKeys(tenant="t-42").stream(Stream("orders")) == "rk:{t-42:orders}:stream"


def test_prefix_and_tenant_validation():
    with pytest.raises(ConfigurationError):
        HashTagKeys(prefix="a{b")
    with pytest.raises(ConfigurationError):
        HashTagKeys(tenant="")
    assert HashTagKeys(prefix="").stream(Stream("o")) == "{o}:stream"


def test_glob_escape():
    assert glob_escape("a*b?[c]\\") == "a\\*b\\?\\[c\\]\\\\"


# control-plane keys (spec section 4)


def _control_keys(keys, s, group="billing", node="worker-node-01"):
    return {
        "state_global": keys.state_global(),
        "state_stream": keys.state_stream(s, group),
        "state_node": keys.state_node(node),
        "dedup": keys.dedup("dir_99812"),
        "node": keys.node(node),
        "nodes": keys.nodes(),
        "broadcast": keys.broadcast_channel(),
        "node_channel": keys.node_channel(node),
    }


def test_control_keys_match_the_spec_tables():
    s = Stream("orders")
    plain = PlainKeys()
    assert _control_keys(plain, s) == {
        "state_global": "rillkit:state:global",
        "state_stream": "rillkit:state:stream:orders:billing",
        "state_node": "rillkit:state:node:worker-node-01",
        "dedup": "rillkit:dedup:dir_99812",
        "node": "rillkit:nodes:worker-node-01",
        "nodes": "rillkit:nodes",
        "broadcast": "rillkit:ctl:broadcast",
        "node_channel": "rillkit:ctl:node:worker-node-01",
    }
    assert plain.logs(s, "1794669071538-1") == "rillkit:logs:orders:1794669071538-1"
    assert plain.trace("4bf92f3577b34da6a3ce929d0e0e4736") == (
        "rillkit:trace:4bf92f3577b34da6a3ce929d0e0e4736")
    assert plain.cordon(s) == "rillkit:cordon:orders"
    assert plain.cordon_buffer(s) == "rillkit:cordon:orders:buffer"

    shop = HashTagKeys(prefix="shop", tenant="t-42")
    assert _control_keys(shop, s) == {
        "state_global": "shop:{t-42:}:state:global",
        "state_stream": "shop:{t-42:}:state:stream:orders:billing",
        "state_node": "shop:{t-42:}:state:node:worker-node-01",
        "dedup": "shop:{t-42:}:dedup:dir_99812",
        "node": "shop:{t-42:}:nodes:worker-node-01",
        "nodes": "shop:{t-42:}:nodes",
        "broadcast": "shop:{t-42:}:ctl:broadcast",
        "node_channel": "shop:{t-42:}:ctl:node:worker-node-01",
    }
    assert shop.logs(s, "1794669071538-1") == "shop:{t-42:orders}:logs:1794669071538-1"
    assert shop.trace("4bf92f3577b34da6a3ce929d0e0e4736") == (
        "shop:{t-42:}:trace:4bf92f3577b34da6a3ce929d0e0e4736")
    assert shop.cordon(s) == "shop:{t-42:orders}:cordon"
    assert shop.cordon_buffer(s) == "shop:{t-42:orders}:cordon:buffer"

    assert HashTagKeys().state_global() == "rk:{:}:state:global"
    assert HashTagKeys().node("worker-node-01") == "rk:{:}:nodes:worker-node-01"
    assert HashTagKeys().trace("4bf92f3577b34da6a3ce929d0e0e4736") == (
        "rk:{:}:trace:4bf92f3577b34da6a3ce929d0e0e4736")
    assert HashTagKeys(prefix="").state_global() == "{:}:state:global"


def test_stream_names_are_encoded_in_control_keys():
    assert PlainKeys().state_stream(Stream("a:b"), "c") != PlainKeys().state_stream(
        Stream("a"), "b:c")
    assert PlainKeys().cordon(Stream("a:buffer")) != PlainKeys().cordon_buffer(Stream("a"))
    assert HashTagKeys().state_stream(Stream("a:b"), "c") == "rk:{:}:state:stream:a%3Ab:c"
    with pytest.raises(ConfigurationError):
        HashTagKeys().state_stream(Stream(""), "g")


@pytest.mark.parametrize("tenant", [None, "t-42", "a:b", "{x}"])
def test_control_keys_share_one_slot_per_namespace(tenant):
    keys = HashTagKeys(tenant=tenant)
    names = set(_control_keys(keys, Stream("orders:eu}")).values())
    names |= set(_control_keys(keys, Stream("x"), "g2", "node{7}").values())
    names.add(keys.trace("4bf92f3577b34da6a3ce929d0e0e4736"))
    assert len({slot(k) for k in names}) == 1


@pytest.mark.parametrize("name", ["orders", "a}b", "x:y", "100%"])
def test_logs_and_cordon_keys_share_the_stream_slot(name):
    keys = HashTagKeys(tenant="t-1")
    s = Stream(name)
    assert len({slot(keys.stream(s)), slot(keys.logs(s, "1-0")), slot(keys.cordon(s)),
                slot(keys.cordon_buffer(s))}) == 1


def test_control_and_data_keys_never_collide():
    import itertools

    parts = ["a", ":", "a:b", "%", "{", "}", "state", "nodes", "global"]
    seen: dict = {}
    for prefix, tenant in itertools.product(["rk", "", "x"], [None, "a", "a:b", ":"]):
        keys = HashTagKeys(prefix=prefix, tenant=tenant)
        for name in parts:
            s = Stream(name, dlq="d")
            found = {
                "stream": keys.stream(s), "dlq": keys.dlq(s), "hb": keys.heartbeat(s, "g", "c"),
                "done": keys.idempotency(s, "k"), "consumers": keys.consumers(s, "g"),
                "logs": keys.logs(s, "1-0"), "cordon": keys.cordon(s),
                "buffer": keys.cordon_buffer(s),
                "trace": keys.trace("4bf92f3577b34da6a3ce929d0e0e4736"),
                **{f"ctl-{k}": v for k, v in _control_keys(keys, s, "g", "n").items()},
            }
            for kind, key in found.items():
                owner = (prefix, tenant, None if kind.startswith("ctl-") and kind not in (
                    "ctl-state_stream",) or kind == "trace" else name, kind)
                assert seen.setdefault(key, owner) == owner, key
    plain = PlainKeys()
    names = [plain.stream(Stream(n)) for n in ("orders",)]
    assert not set(names) & set(_control_keys(plain, Stream("orders")).values())


def test_custom_builders_get_plain_control_keys():
    from rillkit import KeyBuilder

    class Mine(KeyBuilder):
        def stream(self, stream):
            return f"x:{stream.name}"

    assert Mine().state_global() == "rillkit:state:global"
    assert Mine().describe() == {"layout": "custom", "prefix": None, "tenant": None}
    assert PlainKeys().describe()["layout"] == "plain"
    assert HashTagKeys(tenant="t").describe() == {
        "layout": "hashtag", "prefix": "rk", "tenant": "t"}
