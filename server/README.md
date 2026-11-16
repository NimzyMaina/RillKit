# rillkit-server

The RillKit control plane: workers connect to it over the outbound Relay, and you watch and steer
them from its dashboard. It shows nodes, streams, lag, throughput and success rates live, with
trends; pauses, resumes, drains and scales workers; autoscales on both the observed arrival rate
and the backlog; browses each stream's messages (a snapshot, or live as they arrive) and their
logs; shows queue wait against handler time per stream and each trace across streams; manages
DLQs (replay one, some or all, edit a payload before replaying, delete, purge); and creates
streams. `GET /api/metrics` exposes the same fleet gauges as Prometheus text for a scrape.

What the dashboard may see of messages is decided by the workers: payloads and DLQ error texts
only with `RILLKIT_INSPECT_MESSAGES`, handler logs only with `RILLKIT_CAPTURE_LOGS`, trace
timelines only with `RILLKIT_TRACE_RECORDS`.

Architecture and protocol: [`spec/control-plane.md`](../spec/control-plane.md) (section 5.4 is
the Relay session). HTTP API: [`API.md`](API.md), also served as OpenAPI at `/api/docs`.

## Try it

## Try it

No clone needed:

```bash
curl -fsSLO https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/demo.yaml
docker compose -f demo.yaml up -d
```

Open http://localhost:8080 and sign in with the admin token `rillkit-local-admin-token`. The demo
pulls the published server and demo images and starts Postgres, Valkey, two workers, a producer
and the flow service. You can watch lag build, the autoscaler react (add a policy on the `orders`
stream), and the controls take effect. The `flow` service sends one order through `orders.raw` and
`orders.enriched` every two minutes; the second stream fails its first delivery on purpose, so
**View trace** shows a three-step trace across streams. The demo file is for local use only: its
token and API key are public.

## Running it

```bash
curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/install.sh \
  | bash -s -- --dir rillkit
```

That downloads the Compose file, generates an admin token and database password, writes
`rillkit/.env` with mode 600, and starts `ghcr.io/nimzymaina/rillkit-server` with a local
Postgres. It prints the dashboard URL and token.

```bash
# use your own Postgres, no bundled database
curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/install.sh \
  | bash -s -- --dir rillkit \
      --database-url 'postgresql://user:pass@db:5432/rillkit?sslmode=require' \
      --public-url https://rillkit.example.com

# pin a release
curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/install.sh \
  | bash -s -- --dir rillkit --version v0.4.0
```

The server needs Postgres and nothing else. It does not connect to Redis or Valkey: workers point
`RILLKIT_URL` at their own Redis and the server reaches them through the Relay. See
[`deploy/README.md`](../deploy/README.md) for manual installs, TLS, upgrades, backups and mirrors.

| Variable | Default | |
| --- | --- | --- |
| `RILLKIT_SERVER_DATABASE_URL` (or `DATABASE_URL`) | required | Postgres 13 or newer. Tables are created and migrated at startup. |
| `RILLKIT_SERVER_ADMIN_TOKEN` | required | Bearer token for the API and dashboard, 16+ characters |
| `RILLKIT_SERVER_PUBLIC_URL` | `http://localhost:8080` | How workers reach this server; shown in the connection snippet |
| `RILLKIT_SERVER_HOST`, `RILLKIT_SERVER_PORT` | `0.0.0.0`, `8080` | |
| `RILLKIT_SERVER_STATIC_DIR` | set in the image | The built dashboard |
| `RILLKIT_SERVER_TELEMETRY_INTERVAL_SEC` | `5` | Asked of workers in `WELCOME` |
| `RILLKIT_SERVER_AUTOSCALE_INTERVAL_SEC` | `5` | Autoscaler tick |
| `RILLKIT_SERVER_NODE_TTL_SEC` | `30` | A node without telemetry for this long counts as offline |
| `RILLKIT_SERVER_REPLICA_ID` | hostname, pid, random | Names this replica in logs and `/api/health` |
| `RILLKIT_SERVER_LOG_LEVEL` | `INFO` | |
| `RILLKIT_SERVER_BOOTSTRAP_TENANT`, `..._API_KEY` | none | Create this tenant and key at startup. Demos only. |

**TLS.** Run it behind a reverse proxy or load balancer that terminates TLS, so workers connect
with `wss://` (their default requirement). WebSocket upgrades must pass through to `/relay/v1`,
and the SSE endpoint (`/api/tenants/*/events`) must not be buffered.

**Several replicas.** Run as many as you like against one Postgres. Each holds some of the
Relay connections; directives reach the right replica through Postgres `LISTEN`/`NOTIFY`, and
exactly one replica (holding a Postgres advisory lock) runs the autoscaler. Any replica serves
the dashboard.

## Connecting workers

Create a tenant and an API key in the dashboard (Settings), then give workers:

```bash
pip install "rillkit[control]"
export RILLKIT_URL=rediss://your-redis.example.com:6379/0
export RILLKIT_CONTROL=relay
export RILLKIT_CONTROL_URL=https://rillkit.example.com
export RILLKIT_API_KEY=rk_...          # shown once when you create it
export RILLKIT_TENANT=t-42             # only for tenants with "enforce keys" (Cloud mode)
```

Workers keep consuming when the server is unreachable, and reconnect on their own.

## How it fits together

| Module | |
| --- | --- |
| `gateway.py` | The Relay endpoint: handshake and tenant check, telemetry, acks, log requests; delivering directives |
| `autoscaler.py` | Leader election, expiring undeliverable directives, chart samples, scaling decisions |
| `store.py` | Every query, and the overview built from node telemetry |
| `api.py` | The HTTP API and live events (SSE) |
| `app.py` | Wiring, notifications between replicas, serving the dashboard |
| `migrations/` | The schema, applied in order at startup |

Directive validation uses the SDK's own rules (`rillkit.controlplane.plan_directive`), so the
server and every worker agree on what is valid.

## Development

Needs Docker for Postgres and Redis:

```bash
docker run -d --rm --name rk-postgres -e POSTGRES_PASSWORD=postgres -p 5439:5432 postgres:17
docker run -d --rm --name rk-redis -p 6390:6379 redis:7
cd server
uv sync --group dev
uv run ruff check src tests && uv run mypy src && uv run pytest -q
```

Build the image, including the dashboard, from the repository root:

```bash
docker build -f server/Dockerfile -t rillkit-server:local .
```

Run it against those, with the dashboard's dev server in front:

```bash
RILLKIT_SERVER_DATABASE_URL=postgresql://postgres:postgres@localhost:5439/postgres \
RILLKIT_SERVER_ADMIN_TOKEN=dev-admin-token-0123456789 uv run rillkit-server
cd ../dashboard && npm run dev     # proxies /api to localhost:8080
```

## License

[FSL-1.1-ALv2](LICENSE.md), the Functional Source License with an Apache 2.0 future license. You
may run, self-host and modify `rillkit-server` for your own organization, including in
production; you may not offer it, or something substantially similar, to others as a commercial
product or service. Each version becomes available under the Apache License 2.0 two years after
it is released. The SDK it uses (`sdks/python`) is Apache 2.0.
