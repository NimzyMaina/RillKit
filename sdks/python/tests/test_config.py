"""The config file, tenant and prefix settings, and the control-mode switch (spec 2.0)."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from rillkit import Broker, ConfigurationError, Control, HashTagKeys, PlainKeys, Settings, Stream
from rillkit.config import load_config

CONTROL_VARS = ("RILLKIT_CONTROL", "RILLKIT_CONTROL_URL", "RILLKIT_API_KEY", "RILLKIT_NODE_ID",
                "RILLKIT_CONFIG", "RILLKIT_TENANT", "RILLKIT_KEY_PREFIX", "RILLKIT_KEY_LAYOUT",
                "RILLKIT_URL", "REDIS_URL", "RILLKIT_HOST", "RILLKIT_PASSWORD")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for var in CONTROL_VARS:
        monkeypatch.delenv(var, raising=False)


def write(tmp_path: Path, name: str, text: str) -> str:
    path = tmp_path / name
    path.write_text(text)
    return str(path)


TOML = """
[connection]
url = "rediss://cache.internal:6379/0"
backend = "valkey"
mode = "cluster"

[keys]
layout = "hashtag"
prefix = "shop"

[control]
mode = "valkey"
api_key_env = "MY_KEY"
"""


# tenant and prefix


def test_tenant_implies_hash_tagged_keys_with_the_default_prefix():
    keys = Settings(tenant="t-42").key_builder()
    assert isinstance(keys, HashTagKeys)
    assert keys.stream(Stream("orders")) == "rk:{t-42:orders}:stream"
    assert keys.state_global() == "rk:{t-42:}:state:global"


def test_prefix_implies_hash_tagged_keys():
    keys = Settings(key_prefix="billing").key_builder()
    assert keys.stream(Stream("orders")) == "billing:{orders}:stream"
    assert isinstance(Settings().key_builder(), PlainKeys)


@pytest.mark.parametrize("field", ["tenant", "key_prefix"])
def test_tenant_or_prefix_with_plain_keys_is_refused(field):
    with pytest.raises(ConfigurationError, match="key_layout='plain'"):
        Settings(key_layout="plain", **{field: "x"})


def test_invalid_tenant_and_prefix_fail_early():
    with pytest.raises(ConfigurationError):
        Settings(tenant="")
    with pytest.raises(ConfigurationError):
        Settings(key_prefix="a{b")


def test_tenant_and_prefix_from_env(monkeypatch):
    monkeypatch.setenv("RILLKIT_TENANT", "t-42")
    monkeypatch.setenv("RILLKIT_KEY_PREFIX", "shop")
    s = Settings.from_env()
    assert (s.tenant, s.key_prefix) == ("t-42", "shop")
    assert s.key_builder().stream(Stream("o")) == "shop:{t-42:o}:stream"


def test_broker_uses_the_tenant_from_settings():
    broker = Broker(settings=Settings(tenant="t-42"), control=Control())
    try:
        assert broker.keys.stream(Stream("orders")) == "rk:{t-42:orders}:stream"
    finally:
        broker.client.close()


# the config file


def test_toml_file_maps_onto_settings_and_control(tmp_path, monkeypatch):
    monkeypatch.setenv("MY_KEY", "secret-value")
    path = write(tmp_path, "rillkit.toml", TOML)
    assert load_config(path) == {
        "connection": {"url": "rediss://cache.internal:6379/0", "backend": "valkey",
                       "mode": "cluster"},
        "keys": {"key_layout": "hashtag", "key_prefix": "shop"},
        "control": {"mode": "valkey", "api_key": "secret-value"},
    }
    s = Settings.from_env(config=path)
    assert (s.url, s.backend, s.mode, s.key_layout, s.key_prefix) == (
        "rediss://cache.internal:6379/0", "valkey", "cluster", "hashtag", "shop")
    c = Control.from_env(config=path)
    assert (c.mode, c.api_key) == ("valkey", "secret-value")
    assert "secret-value" not in repr(c)


def test_max_connections_from_the_config_file(tmp_path):
    path = write(tmp_path, "rk.toml", "[connection]\nmax_connections = 300\n")
    assert Settings.from_env(config=path).max_connections == 300


def test_json_file_and_rillkit_config_variable(tmp_path, monkeypatch):
    path = write(tmp_path, "rk.json", json.dumps({
        "connection": {"host": "cache", "port": 6380, "socket_timeout": "none", "ssl": True},
        "keys": {"tenant": "t-9"},
    }))
    monkeypatch.setenv("RILLKIT_CONFIG", path)
    s = Settings.from_env()
    assert (s.host, s.port, s.socket_timeout, s.ssl, s.tenant) == ("cache", 6380, None, True,
                                                                   "t-9")


def test_yaml_file(tmp_path):
    pytest.importorskip("yaml")
    path = write(tmp_path, "rk.yaml", "control:\n  mode: valkey\n  node_id: n-1\n")
    assert Control.from_env(config=path).node_id == "n-1"


def test_precedence_is_code_then_env_then_file(tmp_path, monkeypatch):
    path = write(tmp_path, "rk.toml", '[connection]\nhost = "from-file"\nport = 1\n'
                                      '[control]\nmode = "valkey"\nnode_id = "file"\n')
    monkeypatch.setenv("RILLKIT_HOST", "from-env")
    monkeypatch.setenv("RILLKIT_NODE_ID", "env")
    s = Settings.from_env(config=path)
    assert (s.host, s.port) == ("from-env", 1)
    assert Settings.from_env(config=path, host="code").host == "code"
    assert Control.from_env(config=path).node_id == "env"
    assert Control.from_env(config=path, node_id="code").node_id == "code"
    assert Control.from_env(config=path).mode == "valkey"


def test_a_secret_file_reference(tmp_path):
    secret = write(tmp_path, "pw", "hunter2\n")
    path = write(tmp_path, "rk.toml", f'[connection]\npassword_file = "{secret}"\n')
    assert Settings.from_env(config=path).password == "hunter2"


@pytest.mark.parametrize("text, match", [
    ('[nope]\nx = 1\n', "unknown section"),
    ('[connection]\nhots = "x"\n', "unknown field"),
    ('[connection]\npassword = "plain"\n', "must not be stored"),
    ('[control]\napi_key = "plain"\n', "must not be stored"),
    ('[connection]\nurl = "redis://u:pw@h:6379"\n', "carries a password"),
    ('[connection]\nhost_env = "X"\n', "cannot be given by reference"),
    ('[connection]\npassword_env = "RILLKIT_NOT_SET_ANYWHERE"\n', "is not set"),
    ('[connection]\npassword_file = "/nonexistent/pw"\n', "cannot read"),
    ('connection = 1\n', "must be a table"),
    ('[keys\n', "not valid"),
])
def test_bad_config_files_are_refused(tmp_path, text, match):
    path = write(tmp_path, "rk.toml", text)
    with pytest.raises(ConfigurationError, match=match):
        load_config(path)


def test_unknown_extension_and_missing_file(tmp_path):
    with pytest.raises(ConfigurationError, match="unknown extension"):
        load_config(write(tmp_path, "rk.ini", ""))
    with pytest.raises(ConfigurationError, match="cannot read"):
        load_config(str(tmp_path / "missing.toml"))


def test_no_file_is_read_unless_named(tmp_path, monkeypatch):
    write(tmp_path, "rillkit.toml", '[control]\nmode = "valkey"\n')
    monkeypatch.chdir(tmp_path)
    assert Control.from_env().mode == "off"


# the control switch


def test_control_defaults_to_off():
    assert Control().mode == "off"
    assert not Control().enabled
    assert Control.from_env().mode == "off"


def test_control_validation():
    with pytest.raises(ConfigurationError, match="unknown control mode"):
        Control(mode="on")
    with pytest.raises(ConfigurationError, match="needs a url"):
        Control(mode="relay", api_key="k")
    with pytest.raises(ConfigurationError, match="needs an api_key"):
        Control(mode="relay", url="wss://x")
    with pytest.raises(ConfigurationError, match="wss://"):
        Control(mode="relay", url="ws://x", api_key="k")
    assert Control(mode=" VALKEY ").mode == "valkey"


def test_capture_logs_is_off_unless_asked(tmp_path, monkeypatch):
    assert Control(mode="valkey").capture_logs is False
    monkeypatch.setenv("RILLKIT_CAPTURE_LOGS", "true")
    assert Control.from_env().capture_logs is True
    monkeypatch.delenv("RILLKIT_CAPTURE_LOGS")
    path = write(tmp_path, "rk.toml", "[control]\ncapture_logs = true\n")
    assert Control.from_env(config=path).capture_logs is True


def test_trace_records_are_off_unless_asked(tmp_path, monkeypatch):
    assert Control(mode="valkey").trace_records is False
    monkeypatch.setenv("RILLKIT_TRACE_RECORDS", "true")
    assert Control.from_env().trace_records is True
    monkeypatch.delenv("RILLKIT_TRACE_RECORDS")
    path = write(tmp_path, "rk.toml", "[control]\ntrace_records = true\n")
    assert Control.from_env(config=path).trace_records is True
    assert Control(trace_records="1").trace_records is True


def test_control_from_env(monkeypatch):
    monkeypatch.setenv("RILLKIT_CONTROL", "relay")
    monkeypatch.setenv("RILLKIT_CONTROL_URL", "wss://cloud.example")
    monkeypatch.setenv("RILLKIT_API_KEY", "k")
    c = Control.from_env()
    assert (c.mode, c.url, c.api_key) == ("relay", "wss://cloud.example", "k")


def test_broker_reads_the_control_mode_from_the_environment(monkeypatch):
    monkeypatch.setenv("RILLKIT_CONTROL", "valkey")
    broker = Broker()
    try:
        assert broker.control is not None
        assert broker.control_settings.mode == "valkey"
    finally:
        broker.client.close()


def test_off_loads_no_control_plane_code():
    """With control off, nothing of the control plane is imported or installed."""
    code = (
        "import logging, sys, rillkit\n"
        "b = rillkit.Broker('redis://localhost:1/0')\n"
        "assert b.control is None\n"
        "assert 'rillkit.controlplane' not in sys.modules, 'controlplane imported'\n"
        "assert not logging.getLogger().handlers, 'a log handler was installed'\n"
        "assert [type(m).__name__ for m in b.middlewares] == []\n"
        "for name in ('websockets', 'h2', 'httpx', 'aiohttp', 'yaml', 'tomli'):\n"
        "    assert name not in sys.modules, name\n"
    )
    env = {k: v for k, v in os.environ.items() if k not in CONTROL_VARS}
    subprocess.run([sys.executable, "-c", code], check=True, env=env)
