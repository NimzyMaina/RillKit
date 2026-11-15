# RillKit data plane

How every RillKit SDK publishes and consumes, so that a producer in one language and consumers in
others can share a stream, a consumer group and a DLQ. This describes the behaviour of the Python
SDK (rillkit 0.3 plus the control-plane work); the rules marked with a vector are checked by
`conformance/data_plane.json`.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. Key names always come from
the key builder (section 4 of the control-plane specification, checked by
`conformance/keys.json`); this document writes them as `stream key`, `DLQ key` and so on.

## 1. Entries

A message is one stream entry. Its fields:

| Field | Required | Meaning |
| --- | --- | --- |
| `data` | yes | The body, encoded by the stream's codec. The default codec is JSON, UTF-8. |
| `traceparent` | no | W3C trace context of the publishing span (section 9). |
| `x-*` | no | Reserved for RillKit. Producers MUST NOT write them; they carry DLQ context (section 6). |
| anything else | no | Headers added by middleware. Consumers MUST keep them when copying an entry (DLQ, replay). |

- A JSON body MAY be any JSON value. Consumers MUST parse it and MUST NOT depend on its
  formatting (key order, spacing).
- An entry without `data`, or whose `data` does not decode, is a decode error (section 5.3).
- Field names are case-sensitive: only lowercase `x-` is reserved.

## 2. Publishing

```text
XADD <stream key> [MAXLEN ~ <maxlen>] * data <encoded body> [<header> <value> ...]
```

- `MAXLEN` is sent only when a limit is configured for the publish or the stream. Approximate
  trimming (`~`) is the default.
- The returned entry id is the message id.
- A failure to encode or to write MUST surface to the caller as a publish error; it is never
  retried silently.

## 3. Consumer groups and consumers

- Before consuming, a consumer creates its group:
  `XGROUP CREATE <stream key> <group> <start id> MKSTREAM`, with start id `0` by default (the
  group sees every entry already in the stream). `BUSYGROUP` means it exists and is not an error.
- If a read later fails with `NOGROUP` (the stream or group was deleted), the consumer MUST
  create the group again and continue.
- The default consumer name is `<group>-<hostname>-<pid>`. A subscription with concurrency `n`
  runs `n` consumer loops ("lanes") named by `lane_consumer_name` (vector): the name itself for
  lane 0, then `<name>-1`, `<name>-2`, ...

## 4. The consumer loop

Each lane repeats, until it is stopped:

1. Write its heartbeat (section 7).
2. If the desired state (control plane) is not `RUNNING`, wait for a change; go to 1.
3. **Claim** (section 5.1), then handle the claimed messages in order.
4. **Read** new messages: `XREADGROUP GROUP <group> <consumer> COUNT <batch size> BLOCK
   <block ms> STREAMS <stream key> >`, then handle them in order.

- A stop request is checked between messages. Messages already read but not handled stay
  pending and are claimed later (section 5.1); they are never lost.
- The block time MUST stay below the client's socket read timeout: RillKit uses
  `min(block_ms, socket_timeout - 1 s)`, and at least 100 ms.
- A loop-level error (connection lost, unexpected reply) is logged, and the lane waits 5 s and
  continues. It MUST NOT exit the process.

## 5. Delivery, retries and acknowledgement

`deliveries` is how many times the group has delivered the message, counting the current
delivery: 1 for a message from `XREADGROUP`, `times_delivered + 1` for a claimed one.

### 5.1 Claiming

```text
XPENDING <stream key> <group> IDLE <claim idle ms> - + <batch size>
```

A pending message is due when its idle time is at least `retry_wait_ms(claim_idle_ms, backoff,
max_backoff_ms, times_delivered)` (vector):

```text
min(claim_idle_ms * backoff ^ max(times_delivered - 1, 0), max_backoff_ms)
```

Due messages are claimed with `XCLAIM <stream key> <group> <consumer> <claim idle ms> <ids>`.
For each claimed message:

- If the entry is gone (trimmed while pending, `XCLAIM` returns nil), `XACK` it and skip it.
- If `retries_exhausted(max_retries, times_delivered)` (vector), move it to the DLQ with
  `x-error` = `retries exhausted` and `x-deliveries` = `times_delivered`, without running it.
- Otherwise handle it with `deliveries = times_delivered + 1`.

`retries_exhausted(max_retries, deliveries)` is `deliveries > max_retries`; `max_retries` null
means unlimited. Defaults: `max_retries` 3, `claim_idle_ms` 60000, `backoff` 1.0,
`max_backoff_ms` 900000, batch size 10, block 5000 ms.

### 5.2 Ack policies

| Policy | Handler returns | Handler raises |
| --- | --- | --- |
| `NACK_ON_ERROR` (default) | ack | nack: stays pending, retried |
| `ACK_ALWAYS` | ack | ack (logged, never retried) |
| `ACK_FIRST` | acked before the handler ran | acked before the handler ran (at most once) |
| `MANUAL` | the handler's own `ack()`; otherwise stays pending | nack |

To ack is `XACK <stream key> <group> <id>`. To nack is to leave the message pending.

### 5.3 Ending a message early

| Outcome | Effect, under every policy |
| --- | --- |
| Handler signals ack (`AckMessage`) | ack |
| Handler signals nack (`NackMessage`) | nack |
| Handler signals reject (`RejectMessage(reason)`), or calls `reject(reason)` | DLQ now, with `x-error` = reason (or `rejected`) |
| Decode or validation error | DLQ now, never retried |

After a nack, if `retries_exhausted(max_retries, deliveries)`, the message goes to the DLQ at
once with the handler's error as `x-error` (the class name if the error has no text, `nacked`
if there was none), instead of waiting to be claimed again.

## 6. The DLQ

A dead-lettered message is written to the DLQ key as `dead_letter_entry` (vector): every field
of the original entry, plus

| Field | Value |
| --- | --- |
| `x-original-id` | the message id |
| `x-stream` | the logical stream name (not the key) |
| `x-group` | the group |
| `x-error` | the reason, cut to its first 1000 Unicode code points (not bytes or UTF-16 units) |
| `x-deliveries` | `deliveries`, as a decimal string |

overwriting fields of those names. Then the message is acked:

```text
MULTI
XADD <DLQ key> * <fields>
XACK <stream key> <group> <id>
EXEC
```

as one transaction when both keys share a hash slot (always on a single server, and with
hash-tagged keys). Otherwise `XADD` runs first and `XACK` after: a failure in between leaves the
message pending, so it can reach the DLQ twice but is never lost.

### Replay

Replaying moves DLQ entries back, oldest first: each is added to the stream as `replay_entry`
(vector), which drops every `x-` field and keeps the rest (`data`, `traceparent`, headers), then
deleted from the DLQ, in one transaction when the keys share a slot, else add first, then delete.

## 7. Consumer heartbeats and the registry

Before each loop iteration and after each message, a consumer with heartbeats on writes, in one
non-transactional pipeline:

```text
SET <heartbeat key> <unix seconds> EX <ttl>
ZADD <registry key> <expiry ms> <consumer>          expiry ms = (now + ttl) * 1000
ZREMRANGEBYSCORE <registry key> -inf <now ms>
```

`ttl` defaults to 60 s. On a clean stop (and when a lane is retired) the consumer deletes its
heartbeat key and removes itself from the registry. Live consumers are the registry members
whose score is at least now in milliseconds; nothing ever needs `SCAN`.

## 8. Idempotency markers

The `Idempotent` middleware skips messages whose idempotency key was already handled:

1. `EXISTS <idempotency key>`: if it exists, ack the message without running the handler.
2. Run the handler. If it returns without raising,
   `SET <idempotency key> <message id> EX <ttl>` (default 7 days).

## 9. Trace context

When trace propagation is on:

- A consumer parses the entry's `traceparent` (`conformance/traceparent.json`). Valid: the
  handler runs in a child span (same trace id, new span id, same flags). Missing or invalid: a
  new trace.
- Everything published while the handler runs carries a `traceparent` with the handler's trace
  id and a new span id. A publish outside any trace starts a new trace, unless origination is
  turned off.

## 10. Not yet specified

- Producer cordoning during maintenance (milestone 3 of the control-plane plan).
- Codecs other than JSON: a stream's codec must be the same in every language that touches it.
