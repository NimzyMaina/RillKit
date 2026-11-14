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


@pytest.mark.parametrize("name", ["orders", "a}b", "{x}", "a{b", "x:y", "100%", "}", ""])
def test_stream_dlq_and_heartbeats_share_a_slot(name):
    keys = HashTagKeys(tenant="t-1")
    for s in (Stream(name), Stream(name, dlq="custom}{")):
        slots = {slot(keys.stream(s)), slot(keys.dlq(s)), slot(keys.heartbeat(s, "g", "c{1}"))}
        assert len(slots) == 1, name


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
