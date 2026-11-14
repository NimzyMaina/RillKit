import pytest
import redis
import valkey

from helpers import BACKEND
from rillkit import Broker, ConfigurationError, HashTagKeys, PlainKeys, Settings

KEYS = ["URL", "HOST", "PORT", "DB", "USERNAME", "PASSWORD", "SSL", "SOCKET_TIMEOUT"]
RILLKIT_KEYS = ["URL", "BACKEND", "MODE", "CLUSTER_NODES", "KEY_LAYOUT"] + KEYS[1:]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for key in KEYS:
        monkeypatch.delenv(f"REDIS_{key}", raising=False)
        monkeypatch.delenv(f"MYAPP_{key}", raising=False)
    for key in RILLKIT_KEYS:
        monkeypatch.delenv(f"RILLKIT_{key}", raising=False)


def kwargs_of(settings: Settings) -> dict:
    client = settings.client()
    try:
        return dict(client.connection_pool.connection_kwargs)
    finally:
        client.close()


# the original REDIS_* configuration, unchanged


def test_defaults():
    s = Settings.from_env()
    assert s == Settings(url=None, host="localhost", port=6379, db=0, password=None, ssl=False)
    assert (s.backend, s.mode, s.key_layout, s.client_kwargs) == ("redis", "standalone", None, {})


def test_url(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://example:1234/3")
    s = Settings.from_env()
    assert s.url == "redis://example:1234/3"
    kwargs = s.client().connection_pool.connection_kwargs
    assert (kwargs["host"], kwargs["port"], kwargs["db"]) == ("example", 1234, 3)


def test_host_port_db_password_ssl(monkeypatch):
    monkeypatch.setenv("REDIS_HOST", "cache.internal")
    monkeypatch.setenv("REDIS_PORT", "6390")
    monkeypatch.setenv("REDIS_DB", "7")
    monkeypatch.setenv("REDIS_PASSWORD", "s3cret")
    monkeypatch.setenv("REDIS_SSL", "true")
    s = Settings.from_env()
    assert s == Settings(
        url=None, host="cache.internal", port=6390, db=7, password="s3cret", ssl=True
    )
    pool = s.client().connection_pool
    assert pool.connection_kwargs["host"] == "cache.internal"
    assert pool.connection_kwargs["password"] == "s3cret"
    assert pool.connection_class is redis.SSLConnection


@pytest.mark.parametrize("value,expected", [("1", True), ("YES", True), ("True", True),
                                            ("0", False), ("false", False), ("", False)])
def test_ssl_parsing(monkeypatch, value, expected):
    monkeypatch.setenv("REDIS_SSL", value)
    assert Settings.from_env().ssl is expected


def test_custom_prefix(monkeypatch):
    monkeypatch.setenv("MYAPP_HOST", "h")
    monkeypatch.setenv("MYAPP_PORT", "1")
    monkeypatch.setenv("REDIS_HOST", "ignored")
    s = Settings.from_env("MYAPP_")
    assert (s.host, s.port) == ("h", 1)


def test_redis_url_ignores_separate_password_like_before(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://example:1234/0")
    monkeypatch.setenv("REDIS_PASSWORD", "stale")
    s = Settings.from_env()
    assert s.password is None
    assert kwargs_of(s).get("password") is None


def test_redis_url_is_the_default_redis_backend(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://example:1234/0")
    client = Settings.from_env().client()
    assert type(client) is redis.Redis
    client.close()


@pytest.mark.standalone_only
def test_broker_from_env_uses_url(monkeypatch, redis_url):
    monkeypatch.setenv("REDIS_URL", redis_url)
    broker = Broker.from_env()
    try:
        assert broker.client.ping()
        assert isinstance(broker.keys, PlainKeys)
    finally:
        broker.client.close()


@pytest.mark.standalone_only
def test_broker_accepts_url_client_and_settings(redis_url):
    client = redis.Redis.from_url(redis_url)
    assert Broker(client=client).client is client
    from_url = Broker(redis_url)
    from_settings = Broker(settings=Settings(url=redis_url))
    try:
        assert from_url.client.ping() and from_settings.client.ping()
    finally:
        from_url.client.close()
        from_settings.client.close()
        client.close()


# the RILLKIT_* variables and precedence


def test_rillkit_url_wins_over_redis_url(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://legacy:1/0")
    monkeypatch.setenv("RILLKIT_URL", "redis://new:2/0")
    assert Settings.from_env().url == "redis://new:2/0"
    monkeypatch.setenv("MYAPP_URL", "redis://custom:3/0")
    assert Settings.from_env("MYAPP_").url == "redis://new:2/0"


def test_rillkit_backend_mode_nodes_layout(monkeypatch):
    monkeypatch.setenv("RILLKIT_BACKEND", "Valkey")
    monkeypatch.setenv("RILLKIT_MODE", "CLUSTER")
    monkeypatch.setenv("RILLKIT_CLUSTER_NODES", "a:7000, b:7001,")
    monkeypatch.setenv("RILLKIT_KEY_LAYOUT", "plain")
    s = Settings.from_env()
    assert (s.backend, s.mode) == ("valkey", "cluster")
    assert s.cluster_nodes == ("a:7000", "b:7001")
    assert isinstance(s.key_builder(), PlainKeys)


def test_keyword_arguments_override_the_environment(monkeypatch):
    monkeypatch.setenv("RILLKIT_BACKEND", "valkey")
    monkeypatch.setenv("REDIS_HOST", "from-env")
    s = Settings.from_env(backend="redis", host="explicit", client_kwargs={"client_name": "x"})
    assert (s.backend, s.host, s.client_kwargs) == ("redis", "explicit", {"client_name": "x"})


def test_rillkit_connection_variables(monkeypatch):
    for key, value in {"HOST": "rk.internal", "PORT": "6390", "DB": "4", "USERNAME": "app",
                       "PASSWORD": "pw", "SSL": "yes", "SOCKET_TIMEOUT": "30"}.items():
        monkeypatch.setenv(f"RILLKIT_{key}", value)
    s = Settings.from_env()
    assert (s.url, s.host, s.port, s.db) == (None, "rk.internal", 6390, 4)
    assert (s.username, s.password, s.ssl, s.socket_timeout) == ("app", "pw", True, 30.0)
    kwargs = kwargs_of(s)
    assert (kwargs["host"], kwargs["port"], kwargs["db"]) == ("rk.internal", 6390, 4)
    assert (kwargs["username"], kwargs["password"]) == ("app", "pw")


@pytest.mark.parametrize("key,rillkit,legacy,field,expected", [
    ("HOST", "new", "old", "host", "new"),
    ("PORT", "7000", "6000", "port", 7000),
    ("DB", "2", "1", "db", 2),
    ("USERNAME", "new", "old", "username", "new"),
    ("PASSWORD", "new", "old", "password", "new"),
    ("SSL", "true", "false", "ssl", True),
    ("SSL", "false", "true", "ssl", False),
    ("SOCKET_TIMEOUT", "none", "30", "socket_timeout", None),
])
def test_rillkit_variable_wins_over_its_redis_name(monkeypatch, key, rillkit, legacy, field,
                                                    expected):
    monkeypatch.setenv(f"RILLKIT_{key}", rillkit)
    monkeypatch.setenv(f"REDIS_{key}", legacy)
    assert getattr(Settings.from_env(), field) == expected
    monkeypatch.setenv(f"MYAPP_{key}", legacy)
    assert getattr(Settings.from_env("MYAPP_"), field) == expected


def test_empty_rillkit_variable_falls_back_to_the_redis_name(monkeypatch):
    monkeypatch.setenv("RILLKIT_HOST", "")
    monkeypatch.setenv("REDIS_HOST", "legacy")
    assert Settings.from_env().host == "legacy"


def test_redis_variables_still_work_without_rillkit_ones(monkeypatch):
    monkeypatch.setenv("REDIS_HOST", "legacy")
    monkeypatch.setenv("REDIS_SOCKET_TIMEOUT", "none")
    s = Settings.from_env()
    assert (s.host, s.socket_timeout) == ("legacy", None)


def test_rillkit_fields_next_to_a_url_are_ignored_with_a_warning(monkeypatch, caplog):
    import logging

    monkeypatch.setenv("REDIS_URL", "redis://from-url:1234/0")
    monkeypatch.setenv("RILLKIT_HOST", "ignored")
    monkeypatch.setenv("RILLKIT_PASSWORD", "ignored-secret")
    monkeypatch.setenv("RILLKIT_SOCKET_TIMEOUT", "30")  # not replaced by a url, so used
    with caplog.at_level(logging.WARNING, logger="rillkit"):
        s = Settings.from_env()
    assert (s.url, s.password, s.socket_timeout) == ("redis://from-url:1234/0", None, 30.0)
    assert kwargs_of(s)["host"] == "from-url"
    [record] = caplog.records
    assert "RILLKIT_HOST" in record.getMessage() and "RILLKIT_PASSWORD" in record.getMessage()
    assert "ignored-secret" not in record.getMessage()


def test_username_from_env_without_url(monkeypatch):
    monkeypatch.setenv("REDIS_USERNAME", "app")
    monkeypatch.setenv("REDIS_PASSWORD", "pw")
    kwargs = kwargs_of(Settings.from_env())
    assert (kwargs["username"], kwargs["password"]) == ("app", "pw")


@pytest.mark.parametrize("field,value", [
    ("backend", "memcached"), ("mode", "sentinel"), ("key_layout", "flat"),
    ("cluster_nodes", ["host:notaport"]),
])
def test_invalid_values_raise_configuration_error(field, value):
    with pytest.raises(ConfigurationError, match=field.split("_")[0]):
        Settings(**{field: value})


def test_configuration_error_is_a_value_error():
    assert issubclass(ConfigurationError, ValueError)


def test_default_key_layout_follows_mode():
    assert isinstance(Settings().key_builder(), PlainKeys)
    assert isinstance(Settings(mode="cluster").key_builder(), HashTagKeys)
    assert isinstance(Settings(mode="cluster", key_layout="plain").key_builder(), PlainKeys)
    assert isinstance(Settings(key_layout="hashtag").key_builder(), HashTagKeys)


# credentials in urls


def test_url_credentials_are_percent_decoded():
    kwargs = kwargs_of(Settings(url="redis://us%40er:p%3Ass%2Fw%25rd@example:6379/2"))
    assert (kwargs["username"], kwargs["password"], kwargs["db"]) == ("us@er", "p:ss/w%rd", 2)


def test_password_only_url():
    kwargs = kwargs_of(Settings(url="redis://:only-pass@example:6379/0"))
    assert kwargs["password"] == "only-pass"
    assert kwargs.get("username") in (None, "")


def test_url_credentials_win_over_fields():
    kwargs = kwargs_of(Settings(url="redis://u:url-pass@h/0", username="f", password="field"))
    assert (kwargs["username"], kwargs["password"]) == ("u", "url-pass")


def test_fields_fill_in_when_the_url_has_no_credentials():
    kwargs = kwargs_of(Settings(url="redis://h/0", username="f", password="field"))
    assert (kwargs["username"], kwargs["password"]) == ("f", "field")


def test_rediss_url_uses_tls():
    client = Settings(url="rediss://h:6380/0").client()
    assert client.connection_pool.connection_class is redis.SSLConnection
    client.close()


def test_repr_never_shows_passwords():
    s = Settings(
        url="rediss://user:hunter2@example.com:6380/0",
        password="hunter3",
        client_kwargs={"ssl_password": "hunter4", "client_name": "visible"},
    )
    text = repr(s)
    for secret in ("hunter2", "hunter3", "hunter4"):
        assert secret not in text
    assert "user:***@example.com:6380" in text and "visible" in text


@pytest.mark.parametrize("url,shown", [
    ("redis://h:6379/0", "redis://h:6379/0"),
    ("redis://:pw@h:6379/0", "redis://:***@h:6379/0"),
    ("redis://tok@h/0", "redis://***@h/0"),
    ("redis://h/0?password=pw&socket_timeout=3", "redis://h/0?password=***&socket_timeout=3"),
    ("rediss://u:pw@[::1]:6380/0", "rediss://u:***@[::1]:6380/0"),
])
def test_redact_url(url, shown):
    from rillkit.settings import redact_url

    assert redact_url(url) == shown


def test_query_string_credentials_count_as_url_credentials():
    kwargs = kwargs_of(Settings(url="redis://h/0?password=from-query", password="field"))
    assert kwargs["password"] == "from-query"


# client_kwargs


def test_client_kwargs_are_passed_to_the_client():
    s = Settings(url="redis://h/0", client_kwargs={
        "socket_connect_timeout": 5, "socket_timeout": 30, "health_check_interval": 30,
        "client_name": "worker", "retry_on_timeout": True,
    })
    kwargs = kwargs_of(s)
    assert kwargs["socket_connect_timeout"] == 5
    assert kwargs["socket_timeout"] == 30  # client_kwargs wins over Settings.socket_timeout
    assert kwargs["health_check_interval"] == 30
    assert kwargs["client_name"] == "worker"


def test_client_kwargs_reach_a_client_built_without_url():
    client = Settings(client_kwargs={"max_connections": 7, "decode_responses": True}).client()
    assert client.connection_pool.max_connections == 7
    assert client.connection_pool.connection_kwargs["decode_responses"] is True
    client.close()


@pytest.mark.parametrize("backend", ["redis", "valkey"])
def test_unknown_client_option_is_refused(backend):
    with pytest.raises(ConfigurationError, match="not_an_option"):
        Settings(backend=backend, client_kwargs={"not_an_option": 1}).client()


@pytest.mark.parametrize("option", ["host", "port", "db", "password", "username", "ssl"])
def test_options_with_their_own_field_are_refused(option):
    with pytest.raises(ConfigurationError, match=f"Settings.{option}"):
        Settings(client_kwargs={option: "x"}).client()


def test_cluster_only_option_is_refused_standalone():
    with pytest.raises(ConfigurationError, match="require_full_coverage"):
        Settings(client_kwargs={"require_full_coverage": False}).client()


@pytest.mark.parametrize("backend", ["redis", "valkey"])
def test_standalone_only_option_is_refused_on_a_cluster(backend):
    # cluster clients would silently drop it, so it never reaches the server
    with pytest.raises(ConfigurationError, match="single_connection_client"):
        Settings(
            backend=backend, mode="cluster", client_kwargs={"single_connection_client": True}
        ).client()


def test_cluster_rejects_a_database():
    with pytest.raises(ConfigurationError, match="database 0"):
        Settings(mode="cluster", db=3).client()
    with pytest.raises(ConfigurationError, match="database 0"):
        Settings(mode="cluster", url="redis://h:7000/3").client()


def test_cluster_nodes_need_cluster_mode():
    with pytest.raises(ConfigurationError, match="cluster_nodes"):
        Settings(cluster_nodes=["h:7000"]).client()


# valkey


def test_valkey_backend_builds_a_valkey_client():
    s = Settings(backend="valkey", url="redis://u:p%40ss@h:6379/4", client_kwargs={
        "socket_connect_timeout": 3,
    })
    client = s.client()
    try:
        assert type(client) is valkey.Valkey
        kwargs = client.connection_pool.connection_kwargs
        assert (kwargs["username"], kwargs["password"], kwargs["db"]) == ("u", "p@ss", 4)
        assert kwargs["socket_connect_timeout"] == 3
        assert kwargs["socket_timeout"] == 15.0
    finally:
        client.close()


def test_valkey_tls_url():
    client = Settings(backend="valkey", url="rediss://h:6380/0").client()
    assert client.connection_pool.connection_class is valkey.SSLConnection
    client.close()


def test_missing_valkey_package_says_how_to_install(monkeypatch):
    import builtins
    import importlib

    from rillkit import clients

    real_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name.split(".")[0] == "valkey":
            raise ImportError("No module named 'valkey'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(clients, "_backends", {})
    monkeypatch.setattr(clients.importlib, "import_module", fake_import)
    assert builtins  # keep the stdlib import used
    with pytest.raises(ConfigurationError, match=r'rillkit\[valkey\]'):
        Settings(backend="valkey").client()


# servers that need a password


@pytest.mark.standalone_only
def test_acl_user_with_reserved_characters_in_the_url(r, redis_url):
    """A real AUTH with a username and a password full of characters that must be encoded."""
    from urllib.parse import quote, urlparse

    user, password = "rk-test-user", "p@ss:w/rd%#?"
    r.acl_setuser(user, enabled=True, passwords=[f"+{password}"], keys=["*"],
                  commands=["+@all"], reset=True)
    try:
        parsed = urlparse(redis_url)
        url = (f"redis://{quote(user, safe='')}:{quote(password, safe='')}"
               f"@{parsed.hostname}:{parsed.port}{parsed.path}")
        client = Settings(backend=BACKEND, url=url).client()
        try:
            assert client.acl_whoami() in (user, user.encode())
        finally:
            client.close()
        wrong = Settings(backend=BACKEND, url=url.replace(quote(password, safe=""), "nope"))
        bad = wrong.client()
        with pytest.raises(Exception, match="(?i)auth|password|WRONGPASS"):
            bad.ping()
        bad.close()
    finally:
        r.acl_deluser(user)
