# RillKit

Simple publish and consume for Redis and Valkey streams, standalone or cluster: consumer groups,
retries with backoff, a dead-letter queue, ack policies, middleware, and an optional control
plane that pauses, drains and scales workers while they run.

**Docs:** https://nimzymaina.github.io/RillKit/

## SDKs

| Language | Package | Source | Status |
| --- | --- | --- | --- |
| Python 3.9+ | [`rillkit`](https://pypi.org/project/rillkit/) | [`sdks/python`](sdks/python) | released |
| TypeScript / Node.js | `rillkit` on npm | `sdks/typescript` | next |

```bash
pip install rillkit
```

TypeScript (Node.js, Bun and Deno) is next; Java and .NET are planned. Every SDK follows the same
[specification](spec), so workers written in different languages can share streams, consumer
groups, DLQs and a control plane.

## The control plane

`rillkit-server` watches and steers a fleet of workers from a dashboard: live nodes, streams,
lag and throughput; pause, resume, drain and scale; DLQ replay; autoscaling on arrival rate and
backlog; each message's logs; and where the bottleneck is, from per-stream queue wait against
handler time and a per-trace timeline across streams. Workers connect out to it, and keep working
if it is unreachable. Try it with a demo fleet, no clone needed:

```bash
curl -fsSLO https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/demo.yaml
docker compose -f demo.yaml up -d      # then open http://localhost:8080, token: rillkit-local-admin-token
```

For a real deployment, the container images are published and [`deploy/`](deploy) has a
no-clone installer. See [`server/README.md`](server/README.md).

## Repository layout

| Path | What it is |
| --- | --- |
| [`spec/`](spec) | The contract every implementation follows: prose rules, JSON Schemas for the control-plane protocol, and conformance test vectors |
| [`sdks/python/`](sdks/python) | The Python SDK, published to PyPI as `rillkit` |
| [`server/`](server) | `rillkit-server`: the Relay gateway, directive router, autoscaler and HTTP API (Python, FastAPI, Postgres) |
| [`deploy/`](deploy) | No-clone deployment: published images, Compose file, installer and external Postgres/Redis notes |
| [`dashboard/`](dashboard) | The dashboard (React, TypeScript, shadcn/ui), built into the server's image |
| [`examples/demo/`](examples/demo) and [`compose.yaml`](compose.yaml) | A local control plane with a demo fleet |
| [`docs/`](docs) | The documentation site (VitePress) |
| [`bench/`](bench) | Reproducible load tests and their results: data plane, Relay fleet, autoscaler, leader failover, SSE fan-out, batching |
| [`scripts/`](scripts) | Shared tooling: a throwaway Redis or Valkey Cluster for tests, the versioned docs build |
| [`.github/workflows/`](.github/workflows) | One workflow per component, triggered by changes under its paths |

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) first. It is written for people and AI coding agents
alike: the rules that keep the SDKs compatible, how to verify a change, and how to make the
common kinds of change.

```bash
cd sdks/python && uv sync --group dev && uv run pytest   # needs Redis or Valkey, see CONTRIBUTING.md
npm install && npm run docs:dev                          # docs, from the repository root
```

## License

Licensed by directory (see [`LICENSE`](LICENSE)):

- **Apache License 2.0** for the SDKs, the spec, the docs, the examples and the scripts: use
  them in any application, open or closed source.
- **FSL-1.1-ALv2** ([Functional Source License](https://fsl.software)) for `server/` and
  `dashboard/`: free to run, self-host and modify for your own organization; not to be offered
  to others as a competing commercial product or service. Each version becomes Apache 2.0 two
  years after its release.
