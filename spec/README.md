# RillKit specification

The contract that every RillKit implementation (the Python, Node, Java and .NET SDKs, and
`rillkit-server`) must follow, so that workers written in different languages can share streams,
consumer groups and a control plane.

| Path | What it is | How implementations use it |
| --- | --- | --- |
| `protocol/v1/*.json` | JSON Schemas of the messages between workers and the control plane: `TELEMETRY_REPORT`, `DIRECTIVE`, `DIRECTIVE_ACK` | Generate types from them (json-schema-to-typescript, datamodel-code-generator, quicktype, ...) and validate incoming messages |
| `protocol/v1/examples/` | One valid example of each message | Smoke tests for the schemas and for generated types |
| `conformance/*.json` | Test vectors: inputs and the exact expected output | Every SDK's test suite loads them and must pass all of them |

The prose rules (behaviour, atomicity, failure handling):

- [`data-plane.md`](data-plane.md): entries, publishing, consumer groups, retries, ack policies,
  the DLQ and replay, heartbeats, idempotency markers and trace context.
- [`control-plane.md`](control-plane.md): the architecture, the worker state machine, the
  keyspace (section 4), tenant isolation and the wire protocol (section 5), and the milestones.

## Conformance vectors

Each file is `{"description": ..., "cases": [...]}`, except `data_plane.json` (see below).

| File | Covers | Case shape |
| --- | --- | --- |
| `keys.json` | Key and channel names for every layout, prefix and tenant (spec 4.2) | `builder`, `kind`, `args` → `expect` (the key) and `slot` (its cluster hash slot), or `expect_error` |
| `desired_state.json` | Combining the global, stream and node state hashes for one subscription (spec 2.2) | `global`, `stream`, `node` (hashes of strings, or null), `subscription` → `expect`; null fields mean the value given in code |
| `directives.json` | Validating a directive and what applying it writes (spec 2.1, 5.2) | `builder`, `directive` → `expect`: `ACCEPTED` with the state `key`, wakeup `channel` and `fields` (or the `replay` to run), or `REJECTED` |
| `traceparent.json` | Parsing W3C `traceparent` values read from entries | `traceparent` → `expect` (trace id, span id, flags) or null for invalid |
| `data_plane.json` | Retry timing and exhaustion, DLQ entries, replay, lane consumer names ([data-plane.md](data-plane.md)) | Grouped by rule rather than one `cases` list: `retry_wait`, `retries_exhausted`, `dead_letter`, `replay`, `lane_consumer` |

Rules the vectors pin down that are easy to get wrong in a new language:

- Names are compared byte for byte. Percent-encoding (`%25`, `%7B`, `%7D`, `%3A`) is uppercase.
- `slot` is CRC16 of the hash tag modulo 16384, as Redis Cluster computes it. Your client library
  has the function; the vectors check you build the tag the same way.
- Commands, scopes and states are exact-case (`SET_STATE`, `global`, `PAUSED`).
- Directive parameters are JSON integers: `2.5`, `"12"` and `true` are rejected, never converted.
  Values read back from state hashes are strings of ASCII digits: `"03"` is 3, `"+5"`, `" 3"`
  and `"2.5"` are ignored.
- A field that is invalid in a state hash is ignored, so a less specific scope's value applies.
- `reason` in a rejected directive and `expect_error` are explanations, not compared: any
  rejection passes.
- `traceparent` parsing is strict: lowercase hex, no surrounding whitespace.
- `x-error` is cut to 1000 Unicode code points. In JavaScript, `slice` counts UTF-16 units and
  can split an emoji; use `Array.from(error).slice(0, 1000).join("")`.
- Retry waits are floating point (`backoff` may be 1.5); compare with a small tolerance.

A runner is a few lines in each language. The Python one is `sdks/python/tests/test_conformance.py`.

## Changing the spec

1. Change the prose and the vectors in the same pull request. A rule without a vector is a rule
   other SDKs will get wrong.
2. Vectors are written by hand or generated once from a reference and then reviewed. They are
   never regenerated from an implementation to make a failing test pass.
3. Every SDK must pass every vector before it releases.

## Versioning

- The protocol is versioned in the schema path and `$id` (`protocol/v1`). Adding an optional
  field is compatible and stays `v1`. Removing or renaming a field, or changing a meaning, is
  `v2`, and the server must accept both while older workers are around.
- Key names are part of the contract too: changing any vector in `keys.json` moves data for
  every user, so it needs a new key layout rather than an edit.
- The Relay handshake (milestone 2) will carry the protocol version, so the server knows what
  each worker speaks.
