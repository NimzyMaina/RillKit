# Running the server

`rillkit-server` is the control plane: the server your workers connect to, the autoscaler, and the [dashboard](/guide/dashboard). It is optional. Without it, RillKit is the same library; with it, you watch and steer every worker from one place.

```text
  workers (your services)                      rillkit-server                       you
  ┌──────────────────────┐   outbound Relay   ┌──────────────────────────────┐   ┌───────────┐
  │ Broker(control=relay)│ ─────────────────▶ │ gateway · autoscaler · API   │ ◀─│ dashboard │
  │  consumes streams    │ ◀── directives ─── │ Postgres for its own state   │   └───────────┘
  └──────────┬───────────┘                    └──────────────────────────────┘
             │ streams, groups, DLQs, control keys
             ▼
       Redis or Valkey
```

- **Workers connect out.** Each worker opens one WebSocket to the server (the Relay), sends telemetry every few seconds and applies the directives it receives. Your workers need no inbound port, and the server does not access your Redis or Valkey.
- **Nothing depends on it at run time.** If the server is down, workers keep consuming under the last state they were given and reconnect on their own.
- **Its own state is in Postgres:** tenants, API keys, directives and their acknowledgements, autoscaling policies, and a day of metrics.

## Try the demo without cloning

```bash
curl -fsSLO https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/demo.yaml
docker compose -f demo.yaml up -d
```

Open `http://localhost:8080` and sign in with the admin token `rillkit-local-admin-token`. The demo pulls prebuilt images: the server with its dashboard, two workers, a producer and a small Valkey and Postgres. The `demo` tenant's load rises and falls every three minutes, so lag builds, the autoscaler has something to do (add a policy on the `orders` stream, under **Configs**), and a few messages fail into the DLQ. A `flow` service sends one order through `orders.raw` and `orders.enriched` every two minutes; the second stream fails its first delivery on purpose, so **View trace** shows a three-step trace (`ok`, `error`, `ok`) across streams.

Pin a release with `RILLKIT_VERSION=v0.4.0` in the environment or a `.env` file. Stop it with `docker compose -f demo.yaml down -v`.

::: warning Local demos only
The demo file's admin token and API key are public, and its workers reach the server over plain `ws://` on the Compose network. Do not expose it.
:::

## Deploy it

```bash
curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/install.sh \
  | bash -s -- --dir rillkit
```

The installer downloads `deploy/compose.yaml`, generates an admin token and a database password, writes `rillkit/.env` with mode 600, and starts `ghcr.io/nimzymaina/rillkit-server` with a local Postgres. It prints the URL and the admin token; sign in, then create an API key under **Settings** for your workers.

```bash
# pin a release instead of edge
curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/install.sh \
  | bash -s -- --dir rillkit --version v0.4.0

# use your own Postgres
curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/install.sh \
  | bash -s -- --dir rillkit \
      --database-url 'postgresql://user:pass@db.example.com:5432/rillkit?sslmode=require' \
      --public-url https://rillkit.example.com
```

Nothing needs a clone or a local build. The image contains the server and the built dashboard, creates and migrates its tables at startup, and is published for amd64 and arm64. See [`deploy/README.md`](https://github.com/NimzyMaina/RillKit/blob/main/deploy/README.md) for manual installs, upgrades, backups and mirrors.

## Your Postgres and Redis

**Postgres is the only dependency.** Point `RILLKIT_SERVER_DATABASE_URL` at a database you already run and remove `COMPOSE_PROFILES` from `.env`, so the bundled one never starts. Postgres 13 or newer; managed databases work, with `?sslmode=require` where they ask for it. The server creates its tables on first start.

**The server does not connect to Redis or Valkey.** That is the workers' data plane, and keeping them separate lets the control plane run somewhere that cannot reach your cluster. Workers keep their own `RILLKIT_URL` and add `RILLKIT_CONTROL=relay` plus `RILLKIT_CONTROL_URL`; on a cluster, point `RILLKIT_URL` at one seed node and the SDK discovers the rest.

| Variable | Default | |
| --- | --- | --- |
| `RILLKIT_SERVER_DATABASE_URL` (or `DATABASE_URL`) | required | Postgres 13 or newer |
| `RILLKIT_SERVER_ADMIN_TOKEN` | required | Bearer token for the dashboard and the API, at least 16 characters |
| `RILLKIT_SERVER_PUBLIC_URL` | `http://localhost:8080` | How workers reach the server; shown in the dashboard's connection snippet |
| `RILLKIT_SERVER_HOST`, `RILLKIT_SERVER_PORT` | `0.0.0.0`, `8080` | |
| `RILLKIT_SERVER_TELEMETRY_INTERVAL_SEC` | `5` | How often workers report |
| `RILLKIT_SERVER_AUTOSCALE_INTERVAL_SEC` | `5` | How often the autoscaler decides |
| `RILLKIT_SERVER_NODE_TTL_SEC` | `30` | A worker silent this long counts as offline |
| `RILLKIT_SERVER_REPLICA_ID` | hostname, pid | Names the replica in logs and `/api/health` |
| `RILLKIT_SERVER_LOG_LEVEL` | `INFO` | |

## TLS

Put the server behind a reverse proxy or load balancer that terminates TLS, so workers connect with `wss://`, which they require by default. Two paths need care in the proxy:

- `/relay/v1` is a WebSocket: pass the upgrade through.
- `/api/tenants/*/events` is a Server-Sent Events stream: turn off response buffering for it.

## Several replicas

Run as many as you like against one Postgres. Each holds some of the worker connections; a directive created on any replica reaches the worker through whichever replica holds it (Postgres `LISTEN`/`NOTIFY`), and exactly one replica, the holder of a Postgres advisory lock, runs the autoscaler. Any replica serves the dashboard. `GET /api/health` says which replica is the autoscaler leader.

## From source

Contributors, or anyone changing the server or dashboard, can still build from a clone:

```bash
docker compose up --build
```

That starts the same demo fleet from local sources.

## Connecting workers

In the dashboard, open **Settings**, create an API key (its secret is shown once), and copy the connection snippet. On the workers:

```bash
pip install "rillkit[control]"
export RILLKIT_URL=rediss://your-redis.example.com:6379/0   # the workers' Redis or Valkey
export RILLKIT_CONTROL=relay
export RILLKIT_CONTROL_URL=https://rillkit.example.com
export RILLKIT_API_KEY=rk_...
```

Nothing else changes in your code. The [control plane guide](/guide/control-plane) covers what workers do in each state, how directives are applied, and the settings that let the dashboard see message payloads and logs.

## Tenants and Cloud mode

The server keeps tenants apart: each API key belongs to one tenant, and a worker only sees and receives its own tenant's directives, messages and metrics.

A tenant created with **enforce keys** (Cloud mode) is for several tenants sharing one Redis or Valkey cluster. Its workers must use hash-tagged keys with the prefix `rk` and the tenant's id, which the server checks when they connect:

```bash
export RILLKIT_TENANT=t-42
```

Pair it with one ACL user per tenant on the shared cluster (see [several tenants on one server](/guide/keys#several-tenants-on-one-server)): key names place each tenant's data, and the ACL user is what isolates it.

## Security

- **The admin token** protects the dashboard and the API. It is only ever sent in a header, never in a URL; the live events stream uses single-use tickets that expire after a minute, so the token never reaches access logs.
- **API keys** are stored only as hashes. Revoking a key in the dashboard disconnects its workers at once; they keep consuming and retry with backoff.
- **Message contents stay with the workers** unless they allow otherwise: payloads and DLQ error texts only with `RILLKIT_INSPECT_MESSAGES`, handler logs only with `RILLKIT_CAPTURE_LOGS`, trace timelines only with `RILLKIT_TRACE_RECORDS`. The server stores none of them; it fetches them from a worker when you open a message or a trace.

## The HTTP API

Everything the dashboard does goes through a documented HTTP API, so you can automate it: [`server/API.md`](https://github.com/NimzyMaina/RillKit/blob/main/server/API.md), also served as OpenAPI at `/api/docs` on your server.

`GET /api/metrics` (admin token) is a Prometheus scrape: replica up/leader, tenants, and per tenant nodes, workers, in-flight, lag, pending, ack rate, streams and pending directives. Per-stream series stay in the dashboard and worker telemetry to keep cardinality bounded.

```bash
curl -H "Authorization: Bearer $TOKEN" https://rillkit.example.com/api/tenants/t-42/overview

curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  https://rillkit.example.com/api/tenants/t-42/directives \
  -d '{"cmd": "SET_STATE", "scope": "stream", "target": {"stream": "orders", "group": "billing"}, "params": {"state": "PAUSED"}}'
```

## License

`rillkit-server` and the dashboard are licensed under [FSL-1.1-ALv2](https://fsl.software), the Functional Source License with an Apache 2.0 future license. You may run, self-host and modify them for your own organization, in production included. You may not offer them, or something substantially similar, to others as a commercial product or service. Each version becomes available under the Apache License 2.0 two years after its release. The SDKs are Apache 2.0.
