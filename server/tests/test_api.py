"""The HTTP API without workers: auth, tenants, keys, validation, policies (API.md)."""

import httpx
import psycopg
from fastapi.testclient import TestClient

from rillkit_server.app import create_app
from rillkit_server.config import ServerSettings
from rillkit_server.store import hash_key


def error(response: httpx.Response) -> tuple[int, str]:
    return response.status_code, response.json()["error"]["code"]


def test_health_needs_no_token(server):
    body = httpx.get(f"{server.url}/api/health").json()
    assert body["status"] == "ok"
    assert body["replica_id"] == "replica-a"


def test_everything_else_needs_the_admin_token(server, tenant):
    assert error(httpx.get(f"{server.url}/api/tenants")) == (401, "UNAUTHORIZED")
    bad = {"Authorization": "Bearer wrong-token-wrong-token"}
    assert error(httpx.get(f"{server.url}/api/tenants", headers=bad)) == (401, "UNAUTHORIZED")
    # the admin token is never accepted in the url, where access logs would keep it
    query = httpx.get(f"{server.url}/api/tenants", params={"token": "test-admin-token-0123456789"})
    assert error(query) == (401, "UNAUTHORIZED")


def test_event_tickets_are_single_use_and_bound_to_their_tenant(server, api, tenant):
    def events(t: str, **params: str) -> int:
        with httpx.stream("GET", f"{server.url}/api/tenants/{t}/events", params=params,
                          timeout=10) as response:
            return response.status_code

    assert events(tenant) == 401
    assert events(tenant, token="test-admin-token-0123456789") == 401
    issued = api.post(f"/api/tenants/{tenant}/events/ticket").json()
    assert issued["ticket"].startswith("evt_") and issued["expires_in_sec"] == 60
    assert events(tenant, ticket=issued["ticket"]) == 200
    assert events(tenant, ticket=issued["ticket"]) == 401          # used up
    other = api.post("/api/tenants", json={"id": f"{tenant}-o", "name": "o"}).json()["id"]
    theirs = api.post(f"/api/tenants/{other}/events/ticket").json()["ticket"]
    assert events(tenant, ticket=theirs) == 401                     # another tenant's ticket
    assert error(httpx.post(f"{server.url}/api/tenants/{tenant}/events/ticket")) == (
        401, "UNAUTHORIZED")


def test_expired_tickets_are_refused(server, database_url, tenant):
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute("insert into event_tickets(hash, tenant_id, expires_at) "
                     "values (%s, %s, now() - interval '1 second')", (hash_key("evt_old"), tenant))
    with httpx.stream("GET", f"{server.url}/api/tenants/{tenant}/events",
                      params={"ticket": "evt_old"}, timeout=10) as response:
        assert response.status_code == 401


def test_tenants(api, tenant):
    assert tenant in [t["id"] for t in api.get("/api/tenants").json()]
    got = api.get(f"/api/tenants/{tenant}").json()
    assert (got["id"], got["enforce_keys"], got["nodes_online"]) == (tenant, False, 0)
    assert error(api.post("/api/tenants", json={"id": tenant, "name": "x"})) == (409, "CONFLICT")
    assert error(api.post("/api/tenants", json={"id": "Bad Id", "name": "x"})) == (422, "INVALID")
    assert error(api.post("/api/tenants", json={"id": "t-x", "name": "x", "bogus": 1})) == (
        422, "INVALID")
    assert error(api.get("/api/tenants/t-missing")) == (404, "NOT_FOUND")


def test_api_keys_are_shown_once_and_can_be_revoked(api, tenant):
    created = api.post(f"/api/tenants/{tenant}/api-keys", json={"label": "ci"}).json()
    assert created["key"].startswith("rk_") and len(created["key"]) > 40
    assert created["prefix"] == created["key"][:10]
    listed = api.get(f"/api/tenants/{tenant}/api-keys").json()
    assert [k["id"] for k in listed] == [created["id"]]
    assert "key" not in listed[0]
    revoked = api.delete(f"/api/tenants/{tenant}/api-keys/{created['id']}").json()
    assert revoked["revoked_at"] is not None
    assert error(api.delete(f"/api/tenants/{tenant}/api-keys/key_nope")) == (404, "NOT_FOUND")


def test_connection_snippet(api, tenant, server):
    body = api.get(f"/api/tenants/{tenant}/connection").json()
    assert body["env"] == {"RILLKIT_CONTROL": "relay", "RILLKIT_CONTROL_URL": server.url,
                           "RILLKIT_API_KEY": "<your API key>"}
    api.post("/api/tenants", json={"id": f"{tenant}-c", "name": "cloud", "enforce_keys": True})
    cloud = api.get(f"/api/tenants/{tenant}-c/connection").json()
    assert cloud["env"]["RILLKIT_TENANT"] == f"{tenant}-c"


def test_directives_are_validated_with_the_shared_rules(api, tenant):
    bad = api.post(f"/api/tenants/{tenant}/directives",
                   json={"cmd": "SET_CONCURRENCY", "params": {"concurrency": 2.5}})
    assert error(bad) == (422, "INVALID")
    assert "integer" in bad.json()["error"]["message"]
    assert error(api.post(f"/api/tenants/{tenant}/directives", json={"cmd": "REBOOT"})) == (
        422, "INVALID")


def test_a_directive_needs_a_connected_worker(api, tenant):
    response = api.post(f"/api/tenants/{tenant}/directives",
                        json={"cmd": "SET_STATE", "params": {"state": "PAUSED"}})
    assert error(response) == (409, "NO_NODE")
    assert api.get(f"/api/tenants/{tenant}/directives").json() == {
        "items": [], "total": 0, "limit": 25, "offset": 0}


def test_policies(api, tenant):
    path = f"/api/tenants/{tenant}/policies/orders%3Aeu/billing"
    body = {"min_workers": 1, "max_workers": 8, "target_lag_per_worker": 100}
    saved = api.put(path, json=body).json()
    assert (saved["stream"], saved["group"], saved["enabled"]) == ("orders:eu", "billing", True)
    assert (saved["upscale_cooldown_sec"], saved["downscale_cooldown_sec"]) == (30, 120)
    assert [p["stream"] for p in api.get(f"/api/tenants/{tenant}/policies").json()] == [
        "orders:eu"]
    assert error(api.put(path, json={**body, "max_workers": 0})) == (422, "INVALID")
    assert error(api.put(path, json={**body, "target_lag_per_worker": 0})) == (422, "INVALID")
    assert api.delete(path).status_code == 204
    assert error(api.delete(path)) == (404, "NOT_FOUND")


def test_logs_need_a_connected_worker(api, tenant):
    assert error(api.get(f"/api/tenants/{tenant}/logs/orders/1-0")) == (409, "NO_NODE")


def test_unknown_api_routes_are_json_404s(api):
    assert error(api.get("/api/nope")) == (404, "NOT_FOUND")


def test_prometheus_metrics(api, tenant):
    response = api.get("/api/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    body = response.text
    assert "rillkit_server_up 1" in body
    assert "rillkit_server_autoscaler_leader" in body
    assert f'rillkit_tenant_nodes_online{{tenant="{tenant}"}} 0' in body
    assert f'rillkit_tenant_directives_pending{{tenant="{tenant}"}}' in body


def test_the_dashboard_never_serves_html_for_a_missing_asset(tmp_path):
    (tmp_path / "index.html").write_text("<!doctype html><html><body>app</body></html>")
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "index-abc.js").write_text("console.log('app')")
    settings = ServerSettings(database_url="postgresql://x/y", admin_token="t" * 16,
                              static_dir=str(tmp_path))
    client = TestClient(create_app(settings))

    shell = client.get("/t/demo/streams/orders/billing")
    assert shell.status_code == 200 and "app" in shell.text
    assert shell.headers["cache-control"] == "no-cache"  # a rebuild must reach the next tab

    asset = client.get("/assets/index-abc.js")
    assert asset.status_code == 200 and asset.text == "console.log('app')"
    assert asset.headers["cache-control"] == "public, max-age=31536000, immutable"

    # A tab opened before a rebuild asks for a chunk this build removed. HTML would break it
    # with a MIME error; a 404 lets the tab reload and pick up the new bundle.
    stale = client.get("/assets/index-old.js")
    assert stale.status_code == 404
    assert error(stale) == (404, "NOT_FOUND")
    assert "text/html" not in stale.headers["content-type"]


def test_creating_a_stream_without_workers_records_it(api, tenant):
    created = api.post(f"/api/tenants/{tenant}/streams",
                       json={"stream": "orders:eu", "group": "billing", "start": "0"})
    assert created.status_code == 201
    body = created.json()
    assert body["directive"] is None  # nothing connected to create it in Redis yet
    assert (body["stream"]["stream"], body["stream"]["nodes"]) == ("orders:eu", 0)
    assert body["stream"]["created_at"] is not None
    assert body["stream"]["capabilities"] == {"inspect_messages": False, "capture_logs": False,
                                              "trace_records": False}
    listed = api.get(f"/api/tenants/{tenant}/overview").json()["streams"]
    assert [(s["stream"], s["group"]) for s in listed] == [("orders:eu", "billing")]
    bad = api.post(f"/api/tenants/{tenant}/streams", json={"stream": "x", "group": "g",
                                                          "start": "5"})
    assert error(bad) == (422, "INVALID")


def test_messages_need_a_worker_and_valid_cursors(api, tenant):
    base = f"/api/tenants/{tenant}/streams/orders/billing/messages"
    assert error(api.get(base)) == (409, "NO_NODE")
    assert error(api.get(base, params={"before": "x"})) == (422, "INVALID")
    assert error(api.get(base, params={"before": "1-0", "after": "2-0"})) == (422, "INVALID")
    assert error(api.get(base, params={"count": 500})) == (422, "INVALID")
    assert error(api.get(f"{base}/not-an-id")) == (422, "INVALID")


def test_metrics_of_a_quiet_tenant(api, tenant):
    body = api.get(f"/api/tenants/{tenant}/metrics", params={"window_sec": 900}).json()
    assert body["window_sec"] == 900 and len(body["series"]) == 60
    assert body["current"]["succeeded"] == 0 and body["current"]["success_rate"] is None
    assert error(api.get(f"/api/tenants/{tenant}/metrics", params={"window_sec": 10})) == (
        422, "INVALID")


def test_directive_pages_and_filters(api, tenant):
    page = api.get(f"/api/tenants/{tenant}/directives",
                   params={"limit": 5, "offset": 10, "source": "autoscaler"}).json()
    assert (page["limit"], page["offset"], page["total"]) == (5, 10, 0)
    assert error(api.get(f"/api/tenants/{tenant}/directives", params={"source": "x"})) == (
        422, "INVALID")
