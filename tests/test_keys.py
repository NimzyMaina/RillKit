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
                          ("hb", keys.heartbeat(s, "g", "c")), ("done", keys.idempotency(s, "k"))):
            assert seen.setdefault(key, (tenant, name, kind)) == (tenant, name, kind), key
        assert len({slot(keys.stream(s)), slot(keys.dlq(s)), slot(keys.idempotency(s, "k"))}) == 1


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
