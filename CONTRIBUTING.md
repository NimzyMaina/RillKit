# Contributing to RillKit

This guide is for anyone changing this repository, **people and AI coding agents alike**. It does
not depend on any particular AI tool: whatever assistant you use, point it at this file before it
makes changes (for example from `AGENTS.md`, `CLAUDE.md`, `GEMINI.md`,
`.github/copilot-instructions.md` or `.cursor/rules`, with a line such as *"Read and follow
CONTRIBUTING.md"*).

Read sections 1 to 4 before any change. Sections 5 and 6 are recipes; look up the one that
matches your task.

## 1. What RillKit is

RillKit is a library for Redis and Valkey Streams (publish, consumer groups, retries, DLQ, ack
policies, middleware) plus an optional control plane that pauses, drains and scales workers while
they run. It is planned in several languages that must interoperate: a Python producer, Node
consumers and a Java worker can share one stream, one consumer group, one DLQ and one control
plane. **Compatibility between implementations is the main thing to protect.**

| Path | What it is |
| --- | --- |
| `spec/README.md` | How the spec is organised and changed. Read it before touching `spec/`. |
| `spec/control-plane.md` | Architecture, worker states, the keyspace (section 4), tenant isolation (4.4), the wire protocol (5), milestones (6) |
| `spec/data-plane.md` | Entries, publishing, consumer groups, retries, ack policies, DLQ, replay, heartbeats, idempotency, tracing |
| `spec/protocol/v1/` | JSON Schemas of the control-plane messages, plus examples. Types in every language are generated from these. |
| `spec/conformance/` | Test vectors every SDK must pass |
| `sdks/python/` | The Python SDK (`rillkit` on PyPI): `src/rillkit`, `tests`, `pyproject.toml`, `uv.lock` |
| `docs/` | The documentation site (VitePress), currently for the Python SDK |
| `scripts/test-cluster.sh` | Starts a six-node Redis or Valkey Cluster in Docker for tests |
| `scripts/build-docs.mjs` | Builds the versioned docs site (one snapshot per `vX.Y.Z` tag) |
| `.github/workflows/` | `python.yml` (lint, tests, build), `release.yml` (PyPI), `docs.yml` (Pages) |

Python SDK modules (`sdks/python/src/rillkit/`):

| Module | Responsibility |
| --- | --- |
| `broker.py` | `Broker`: publishing, subscriptions, lifecycle, `active_workers`, `replay_dlq` |
| `consumer.py` | `Subscriber`: the consumer loop and lanes, claims, retries, ack policies, the DLQ; the pure data-plane rules (`retry_wait_ms`, `dead_letter_entry`, ...) |
| `keys.py` | Key builders (`PlainKeys`, `HashTagKeys`, `KeyBuilder`): **every** key and channel name |
| `settings.py` | `Settings` and `from_env` (environment variables, config file) |
| `config.py` | The config file loader and `Control` (control-plane mode) |
| `controlplane.py` | `ControlPlane` (`broker.control`): desired state, wakeups, heartbeats, telemetry, directives; the pure rules `resolve_desired` and `plan_directive` |
| `clients.py` | Everything that differs between redis-py and valkey-py, standalone and cluster |
| `middleware.py`, `tracing.py`, `logs.py` | Middleware, including `Idempotent`, `TraceContext` and `LogCapture` |
| `message.py`, `stream.py`, `codec.py`, `ack.py`, `exceptions.py` | Small value types |

## 2. Rules that are never broken

1. **The control plane is opt-in, and off means unchanged.** With control off (the default) no
   control key is read or written, no extra thread or connection is opened, no control module is
   imported and no log handler is installed. `tests/test_config.py::test_off_loads_no_control_plane_code`
   and `tests/test_control.py::test_off_writes_no_control_key_and_starts_no_control_thread`
   check this; they must keep passing.
2. **Every key and channel name comes from the key builder** (`broker.keys.<method>(...)`).
   Never format a key by hand anywhere else, in code, tests or docs examples meant to be copied.
3. **Existing key names never change.** Changing one moves users' data (their streams, DLQs and
   consumer groups appear empty). A new layout is a new key builder, never an edit to an old one.
   Any change to a `"expect"` in `spec/conformance/keys.json` is a breaking change and needs the
   maintainer's explicit approval.
4. **The spec is the contract.** A behaviour change that another SDK would also have to make
   changes, in the same pull request: the prose in `spec/`, the vectors in `spec/conformance/`,
   the schemas in `spec/protocol/` if messages change, and the Python SDK. Vectors are written by
   hand or generated once and reviewed. **Never regenerate or edit a vector to make a failing
   test pass**; decide whether the code or the spec is wrong, and fix that.
5. **Tenant isolation is an allowlist ACL** (`spec/control-plane.md` section 4.4). A change that
   makes RillKit send a Redis command it did not send before must add that command to the rule in
   all three places together: `spec/control-plane.md` (4.4), `docs/guide/keys.md`, and
   `sdks/python/tests/test_acl.py` (which runs a full worker as that user). Never rely on `SCAN`,
   `KEYS`, `RANDOMKEY`, `CLIENT`, `INFO` or `CONFIG`; list things through registries instead.
6. **Atomic where the keys share a slot, safe-ordered where they do not.** Multi-key writes use
   `MULTI`/`EXEC` (or `WATCH` for check-then-write) when the keys share a hash slot, and otherwise
   an order that can repeat work but never lose it (for example add to the DLQ, then ack). Use
   `clients.run_together`, `clients.same_slot` and `clients.node_client_for`; Lua is optional and
   never required.
7. **Processing fails open, security fails closed.** If the control plane is unreachable,
   workers keep consuming under the last stored state. Permission errors are never worked around.
8. **Privacy by default.** Message bodies, handler logs, exception text and credentials are not
   stored or sent anywhere unless the user turns that on (see `LogCapture`). Secrets are never
   logged, printed or written to files: `Settings.__repr__` and `Control.__repr__` redact them,
   and the config file only accepts references (`*_env`, `*_file`).
9. **Both client libraries, both topologies.** Every feature works with redis-py and valkey-py,
   standalone and cluster. Library differences belong in `clients.py`; catch `CLIENT_ERRORS`,
   `RESPONSE_ERRORS`, etc., never one library's exception classes.
10. **Compatibility of the Python SDK:** Python 3.9 to 3.13. `redis` is the only required
    dependency (plus `tomli` on Python below 3.11); everything else is an extra. No new
    dependency, runtime or dev, without the maintainer's approval.

## 3. Git and review rules

These apply to every contributor; AI agents must follow them strictly.

- **Never commit to `main`.** Work on a branch named `feat/<kebab-case>`, `fix/<kebab-case>` or
  `chore/<kebab-case>`. Check with `git branch --show-current` before editing.
- **Agents: ask before every commit, push, merge or pull request.** Run the checks (section 4),
  stage the files, show the branch, the staged files and the proposed message, and wait for an
  explicit "yes". Approval for one commit does not cover the next.
- **Atomic commits**: one logical change each. Do not mix refactoring or reformatting with a fix.
- **Commit messages**: a short imperative subject (what changes, for a reader of `git log`),
  a blank line, then why. **No AI attribution** of any kind: no `Co-authored-by` an AI, no
  "Generated with" lines, no tool names in commits, pull requests or code comments. Use the
  machine's existing git identity; never change `git config user.*`.
- **Never** `git push --force`, `git reset --hard`, `git clean -fd`, or amend pushed commits,
  unless the maintainer asks.
- **Respect work in progress**: before starting, run `git status`. Do not overwrite, revert or
  stage changes you did not make; ask what to do with them.
- **Never modify the release or deploy workflows** (`release.yml`, `docs.yml`) or publish
  anything (PyPI, tags, GitHub releases) without explicit approval.
- **Secrets**: never read, print or commit `.env` files, tokens or keys. If one gets staged,
  stop and tell the maintainer.

## 4. Verifying a change

A change is done when lint, types and the full test suite pass against **Redis and Valkey**, and
against a **cluster** if it touches keys, transactions, Pub/Sub or anything multi-key.

### Test servers

The suite needs a real server; only database 15 of a standalone server is ever flushed.

```bash
docker run -d --rm --name rk-redis  -p 6390:6379 redis:7
docker run -d --rm --name rk-valkey -p 6391:6379 valkey/valkey:8
```

### Commands (from `sdks/python`)

```bash
cd sdks/python
uv sync --group dev
uv run ruff check src tests
uv run mypy src
uv run pytest -q                                                        # Redis, localhost:6390/15
REDIS_URL=redis://localhost:6391/15 RILLKIT_TEST_BACKEND=valkey uv run pytest -q   # Valkey

../../scripts/test-cluster.sh valkey 7101                               # or: redis
RILLKIT_TEST_BACKEND=valkey RILLKIT_TEST_MODE=cluster \
  RILLKIT_TEST_CLUSTER_NODES=127.0.0.1:7101 uv run pytest -q
../../scripts/test-cluster.sh stop valkey
```

`uv run pytest tests/test_conformance.py` alone runs the spec vectors without a server, except
one test that validates real messages.

Docs (from the repository root): `npm install`, then `npx vitepress build docs`. The build fails
on dead links, so run it after any docs change.

### Writing tests

- Every behaviour change or fix comes with tests for the normal case and the edge cases.
- Tests run unchanged on standalone and cluster. Use the fixtures in `tests/conftest.py`
  (`make_broker`, `r`, `name`) and the helpers in `tests/helpers.py` (`sk`, `dk`, ... build
  physical keys with the default layout). Mark tests `standalone_only` or `cluster_only` when
  they cannot run on both.
- A cluster is never flushed, so every test uses unique names (the `name` fixture, uuid node ids
  and directive ids), and control-plane tests use a fresh `HashTagKeys(prefix=...)` per test on
  a cluster (see the `keys` fixture in `tests/test_control.py`).
- **Never sleep to wait for something**; use `wait_until(condition)`. Use `settle()` only to
  prove that something does *not* happen. Wrap idle-consumer tests in `keep_server_awake(r)`.
- Brokers created by `make_broker` are cleaned up for you, including their control plane. A
  broker you create yourself must be stopped in a `finally`.
- Before calling a timing-sensitive test good, run it several times
  (`for i in $(seq 10); do uv run pytest -q tests/test_x.py; done`).

## 5. Recipes: the Python SDK

Each recipe lists every place that must change. Missing one is the most common mistake.

### Add a setting or environment variable

1. A field on `Settings` (connection or keys) or `Control` (control plane), validated in
   `__post_init__`, with secret values redacted in `__repr__`.
2. Read it in `Settings.from_env` / `Control.from_env` (`RILLKIT_<NAME>`; environment over file,
   code over environment).
3. Map it in `config.py` `_FIELDS` so the config file accepts it (secrets: reference only).
4. Tests in `tests/test_config.py` or `tests/test_settings.py`, including precedence.
5. Docs: the table in `docs/guide/configuration.md` and `docs/reference/settings.md`.

### Add a key or channel

1. A method on `KeyBuilder` (a default for custom builders, normally the plain layout) and on
   `HashTagKeys` if its layout differs. Stream-scoped keys go under the stream's tag;
   namespace-wide control keys under the control tag. Stream names in plain keys are
   percent-encoded with `_escape_plain`.
2. Uniqueness and same-slot tests in `tests/test_keys.py`.
3. New cases in `spec/conformance/keys.json` (and the kind in `tests/test_conformance.py`).
4. The tables in `spec/control-plane.md` (4.1, 4.2) and `docs/guide/keys.md`.
5. If it needs a new command: rule 5 in section 2.

### Add or change a directive

1. `plan_directive` in `controlplane.py` (validation and what it writes), `COMMANDS`, and the
   apply path in `ControlPlane.apply` if it is not a plain state write.
2. `spec/protocol/v1/directive.json` (and `directive_ack.json` if acks change).
3. Accepted and rejected cases in `spec/conformance/directives.json`.
4. Integration tests in `tests/test_control.py`.
5. `spec/control-plane.md` (2.2, 5.2) and `docs/guide/control-plane.md`.

Directive parsing is strict on purpose: exact-case names, JSON integers only, everything
validated before the dedup key is recorded.

### Change how messages are consumed, retried or dead-lettered

1. The pure functions at the top of `consumer.py` hold the rules other SDKs must copy; keep
   rules there, not inline in the loop.
2. `spec/data-plane.md` and `spec/conformance/data_plane.json`.
3. Tests in `tests/test_retries.py`, `tests/test_consume.py` or `tests/test_middleware.py`.
4. The matching page under `docs/guide/`.

### Add middleware

Subclass `BaseMiddleware` (`on_receive`, `consume_scope`, `publish_scope`, `after_processed`).
Keep per-message state on the `Message` or in a `contextvars.ContextVar`, never on the
instance. Extra entry fields go in `PublishCommand.headers`; names starting with `x-` are
reserved. Document it in `docs/reference/middleware.md`.

### Release the Python SDK

Only with the maintainer's approval.

1. Bump the version in `sdks/python/pyproject.toml` and `sdks/python/src/rillkit/__init__.py`
   (and run `uv lock`).
2. Add a "From X to Y" section to `docs/guide/upgrading.md`.
3. Merge to `main`; the maintainer tags `vX.Y.Z`. The tag triggers `release.yml` (PyPI) and a
   docs snapshot. Plain `vX.Y.Z` tags belong to the Python SDK; other SDKs use prefixed tags.

## 6. Recipe: a new SDK in another language

1. **Layout:** `sdks/<language>/` with that ecosystem's normal tooling (npm or pnpm, Gradle or
   Maven, dotnet). No monorepo build system is needed.
2. **Order of features:** core first (publish, consumer groups, ack policies, retries, DLQ,
   replay, key builders, heartbeats and the consumer registry), then the managed tier (the
   control plane), then observability (trace context, log capture). Each tier is done when its
   conformance vectors pass.
3. **Conformance runner:** a test that loads every file in `spec/conformance/` and checks each
   case, like `sdks/python/tests/test_conformance.py`, including a check that no vector file is
   left without a runner. The "easy to get wrong" list in `spec/README.md` matters here.
4. **Generated types:** generate the control-plane message types from `spec/protocol/v1/`;
   do not write them by hand. Logic stays hand-written and idiomatic for the language; do not
   generate it or bind to another SDK's implementation.
5. **Interoperability test:** at least one test where the new SDK and the Python SDK share a
   stream and group (one publishes, the other consumes, failures reach the DLQ, replay works).
6. **CI:** `.github/workflows/<language>.yml` triggered by `sdks/<language>/**` and `spec/**`,
   running against Redis and Valkey, standalone and cluster (`scripts/test-cluster.sh`).
7. **Releases:** tags prefixed with the language (`node-v0.1.0`), never plain `vX.Y.Z`.
8. **Docs** for the language, and a row in the SDK table of the root `README.md`.

## 7. Known pitfalls

- **valkey-py 6.2 sharded Pub/Sub:** `get_sharded_message()` without `target_node` ignores its
  timeout and, with `ignore_subscribe_messages=True`, drops every message. Read each shard with
  `target_node=` (see `ControlPlane._next_message`).
- **Cluster transactions:** valkey-py refuses transactional pipelines on a cluster client, so
  `MULTI`/`EXEC` and `WATCH` run on the node that owns the slot (`clients.node_client_for`,
  `clients._node_transaction`).
- **A closed cluster client cannot be reused**, unlike a standalone one. Do everything that needs
  the broker's client before `broker.stop()`.
- **Socket timeouts:** a blocking `XREADGROUP` must be shorter than the client's socket timeout
  (redis-py 8 lowered its default to 5 s). The SDK clamps `block_ms`; keep it that way.
- **Pausing is not instant:** a consumer already blocked in `XREADGROUP` sees a state change when
  that read returns (at most `block_ms`), and handles what the read returned. Tests that check
  "nothing consumed after a pause" must allow for that.
- **String lengths differ by language:** `x-error` is cut to 1000 Unicode code points. JavaScript
  and Java count UTF-16 units; .NET too.
- **The test suite assumes Docker**, for the servers and the cluster.

## 8. Before you finish

- [ ] On a feature branch; nothing you did not create was changed or staged.
- [ ] Lint, types, and the full suite on Redis and Valkey (and a cluster when relevant) pass.
- [ ] Tests cover the change, including edge cases.
- [ ] `spec/` updated if another SDK would need the same change (prose, vectors, schemas).
- [ ] Docs updated (guide, reference, upgrading notes), and the docs build passes.
- [ ] No debug output, scratch files or temporary scripts left behind.
- [ ] A short summary for the maintainer: files changed, commands run and their results, and
      anything left to do or decide.
