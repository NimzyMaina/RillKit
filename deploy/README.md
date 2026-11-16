# Deploying the control plane

`rillkit-server` is a single image with the dashboard built in. It needs Postgres and nothing
else. You do not need this repository to run it.

Requirements: Docker 24 or newer with Compose v2.20 or newer (the Compose file uses an optional
service dependency).

- **Quick start** below: a local Postgres, one command.
- **Your own Postgres**: point `RILLKIT_SERVER_DATABASE_URL` at it and do not start the bundled one.
- **Redis/Valkey** is the workers' data plane, not the server's. Workers set `RILLKIT_URL`; the
  server reaches them through the Relay.

> **TLS:** workers require `wss://` unless they opt into `RILLKIT_CONTROL_INSECURE`. Put the
> server behind a TLS-terminating proxy and set `RILLKIT_SERVER_PUBLIC_URL=https://...`. The
> proxy must pass WebSocket upgrades to `/relay/v1` and disable buffering for
> `/api/tenants/*/events`. Caddy does both out of the box:
>
> ```caddyfile
> rillkit.example.com {
>     reverse_proxy 127.0.0.1:8080
> }
> ```

## Quick start

```bash
curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/install.sh \
  | bash -s -- --dir rillkit
```

The script downloads `compose.yaml`, generates an admin token and database password, writes
`rillkit/.env` with mode 600, and runs `docker compose up -d`. It prints the dashboard URL and
the admin token. Sign in, then create an API key under **Settings** for your workers.

Use `--no-start` to write the files without starting anything, and `--version v0.4.0` to pin a
release instead of `edge`.

To try the dashboard and a demo fleet instead, without any of the above:

```bash
curl -fsSLO https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/demo.yaml
docker compose -f demo.yaml up -d
# http://localhost:8080, token: rillkit-local-admin-token
```

The demo file is for local use only: its token and API key are public.

## Your own Postgres

Use a managed database or one you already run:

```bash
curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/install.sh \
  | bash -s -- --dir rillkit \
      --database-url 'postgresql://user:pass@db.example.com:5432/rillkit?sslmode=require' \
      --public-url https://rillkit.example.com
```

Or edit `.env` after a manual install:

```ini
RILLKIT_SERVER_DATABASE_URL=postgresql://user:pass@db.example.com:5432/rillkit?sslmode=require
# remove COMPOSE_PROFILES: the bundled Postgres is then not started
```

Postgres 13 or newer. The server creates and migrates its tables at startup, so the database and
role need `CREATE` on the schema. Percent-encode special characters in the password.

There is no SQLite or embedded mode: the server uses Postgres advisory locks for leader election
and `LISTEN`/`NOTIFY` across replicas, so a single-node database file is not a drop-in
replacement.

## Redis, Valkey and the Relay

The server **does not connect to Redis or Valkey**. Its only dependency is Postgres. That keeps the
data plane independent of the control plane and lets workers use a cluster or a managed service
the server cannot reach.

| Control mode | What you run |
| --- | --- |
| `relay` | `rillkit-server` (this deployment), plus workers with `RILLKIT_CONTROL=relay`. Workers use their own Redis/Valkey through `RILLKIT_URL`. |
| `off` | The plain SDK. No control plane, no extra keys or connections. |

For a relay worker, the environment is only:

```bash
RILLKIT_URL=rediss://your-redis.example.com:6379/0
RILLKIT_CONTROL=relay
RILLKIT_CONTROL_URL=https://rillkit.example.com
RILLKIT_API_KEY=rk_...
```

On a Redis or Valkey cluster, use one seed node in `RILLKIT_URL`; the SDK discovers the rest.

## Manual install

If you prefer not to run the installer:

```bash
mkdir rillkit && cd rillkit
curl -fsSLO https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/compose.yaml
curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/.env.example -o .env
# edit .env: set the admin token, and either the local Postgres password or your own URL
docker compose up -d
```

`RILLKIT_SERVER_ADMIN_TOKEN` must be at least 16 characters. Generate it with
`openssl rand -hex 24`.

## Without Compose

The image is a normal container. This is the whole deployment for Kubernetes, ECS, Nomad or a
plain `docker run`:

```bash
docker run -d --name rillkit-server -p 8080:8080 \
  -e RILLKIT_SERVER_DATABASE_URL='postgresql://user:pass@db.example.com:5432/rillkit?sslmode=require' \
  -e RILLKIT_SERVER_ADMIN_TOKEN="$(openssl rand -hex 24)" \
  -e RILLKIT_SERVER_PUBLIC_URL=https://rillkit.example.com \
  ghcr.io/nimzymaina/rillkit-server:v0.4.0
```

There is no volume: all state is in Postgres. Serve it behind a TLS proxy and make sure the
proxy passes WebSocket upgrades and does not buffer SSE.

## Upgrades and backups

```bash
# upgrade to another release
sed -i.bak 's/^RILLKIT_VERSION=.*/RILLKIT_VERSION=v0.4.0/' .env
docker compose pull && docker compose up -d

# back up the bundled Postgres
docker compose exec -T postgres pg_dump -U rillkit rillkit > rillkit-$(date +%F).sql
```

Migrations run when the server starts. Back up the database before a version change, as with any
stateful service.

## Several replicas

Point as many server replicas at the same Postgres as you like. One holds a Postgres advisory
lock and runs the autoscaler; each replica serves its own worker connections and any replica can
serve the dashboard. Put them behind a load balancer that passes WebSocket upgrades and does not
buffer SSE. There is no shared volume and no session affinity requirement.

## Image tags and mirrors

The images are published to the GitHub Container Registry:

- `ghcr.io/nimzymaina/rillkit-server`
- `ghcr.io/nimzymaina/rillkit-demo` (the demo fleet)

Use a release tag (`v0.4.0` or `0.4.0`) in production. `edge` follows `main` and is the quick
start's default; `latest` is the newest release. Set `RILLKIT_IMAGE` or `RILLKIT_DEMO_IMAGE` to
install from a mirror.

On a fresh fork, the first publish creates the packages **private**. If you want anonymous pulls,
open the package on GitHub, then **Package settings → Change visibility → Public**. The workflow
uses the repository's `GITHUB_TOKEN`; it needs no extra secret.

## Building from source

Contributors and anyone changing the dashboard or server can still build locally from the
repository root:

```bash
docker compose up --build
```

That starts the demo fleet and builds both images. See the root [compose.yaml](../compose.yaml).
