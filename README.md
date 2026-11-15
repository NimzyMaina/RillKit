# RillKit

Simple publish and consume for Redis and Valkey streams, standalone or cluster: consumer groups,
retries with backoff, a dead-letter queue, ack policies, middleware, and an optional control
plane that pauses, drains and scales workers while they run.

**Docs:** https://nimzymaina.github.io/RillKit/

## SDKs

| Language | Package | Source | Status |
| --- | --- | --- | --- |
| Python 3.9+ | [`rillkit`](https://pypi.org/project/rillkit/) | [`sdks/python`](sdks/python) | released |

```bash
pip install rillkit
```

More languages (TypeScript/Node, Java, .NET) are planned. Every SDK follows the same
[specification](spec), so workers written in different languages can share streams, consumer
groups, DLQs and a control plane.

## Repository layout

| Path | What it is |
| --- | --- |
| [`spec/`](spec) | The contract every implementation follows: prose rules, JSON Schemas for the control-plane protocol, and conformance test vectors |
| [`sdks/python/`](sdks/python) | The Python SDK, published to PyPI as `rillkit` |
| [`docs/`](docs) | The documentation site (VitePress) |
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

MIT licensed.
