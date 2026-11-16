# The dashboard

The dashboard comes with [`rillkit-server`](/guide/server): open the server's address in a browser and sign in with its admin token. It updates live, as workers report. Everything it changes, it changes through directives, so every action is applied once by a worker and recorded in **Activity**.

## Overview

![The overview: fleet figures, global controls and the throughput section](/screenshots/dashboard-overview.jpg)

The overview opens with the fleet: nodes online, active workers, messages in flight, total lag and the ack rate. Below them:

- **Controls** for every worker of the tenant: **Pause all**, **Resume**, **Maintenance** and **Drain**. Pause and drain ask first. Paused workers finish the message in hand and stop reading; drained workers also release their consumers. Nothing is lost either way; resume to carry on.
- **Throughput**, over the last 15 minutes, hour or 24 hours, compared with the window before it.
- **Nodes**, with each worker's state, lanes, messages in flight, CPU, memory and SDK. A node's menu pauses, drains or resizes just that node.

### Throughput and trends

![Throughput: six figures with their change, and four charts](/screenshots/dashboard-throughput.jpg)

| Figure | What it counts | Better when |
| --- | --- | --- |
| Messages handled | handler runs that finished and were acknowledged | higher |
| Success rate | succeeded runs out of all runs, failed attempts included | higher |
| Throughput | messages handled per second | higher |
| Data rate | bytes of messages handled per second | neither |
| In the DLQ | dead-lettered messages waiting now | lower |
| Dead-lettered | messages moved to a DLQ in the window | lower |

Each figure shows its change against the previous window of the same length, with an arrow: **green** when it improved, **red** when it got worse, grey when it barely moved or has no direction (data rate). Every chart has one scale and a hover tooltip; a gap in a line means no worker reported in that interval.

## Streams

![The streams list with search and the New stream button](/screenshots/dashboard-streams.jpg)

Every stream and consumer group your workers consume, with its state, lag, pending messages, ack rate, workers, nodes, DLQ size and autoscaling. Search by stream or group name; long lists are paged.

**New stream** creates a stream and its consumer group in Redis or Valkey, through a connected worker. Choose where the group starts: **only new messages** (the default), or **everything already there** if the stream already holds messages you want handled. A stream nobody consumes yet is listed as **No consumers** until a worker subscribes to it.

## A stream

A stream's page has four tabs. The tab is part of the address, so a link opens the same tab.

### Statistics & state

Lag, pending, ack rate, workers, nodes and DLQ size; lag and workers over time, as two aligned charts; and the stream's state controls (**Pause**, **Maintenance**, **Resume**), which apply on every node that consumes it, with each node's state listed.

Below them, **Time per message** shows queue wait and handler time per attempt, as two aligned charts. A queue wait that climbs while handler time stays flat means the stream needs more workers; handler time climbing means the work itself is the bottleneck.

### Messages

![The Messages tab with Live on](/screenshots/dashboard-messages.jpg)

The stream's messages, newest first: time, id, status, consumer, size and a preview of the payload.

- **As of a moment.** The tab opens with the newest 50 messages as they were at the time shown ("As of 14:02:31"), and stays still while you read. Refresh reloads it; **Load older** pages back through the stream.
- **Live.** Turn **Live** on and new messages are added on top as they arrive, with a count of how many came in; the messages you were looking at stay where they are, and their statuses keep updating. Turn it off to freeze the list again.
- **Statuses.** *Waiting*: not delivered to the group yet. *Pending*: a worker has it and has not acknowledged it (being handled, or waiting to be retried; the try number shows from the second delivery). *Acked*: done.
- **Search.** Paste a full message id and **Open message** finds it anywhere in the stream; any other text filters the messages already loaded.

Click a message to expand it **in place**, right under its row (no sidebar): its status, payload, trace id, and what its handler logged, attempt by attempt. **View trace** (with `RILLKIT_TRACE_RECORDS=true` on the workers) expands the whole trace below it.

![A trace waterfall: the end-to-end duration at the top, then each handler offset on a shared time ruler](/screenshots/dashboard-trace.jpg)

The waterfall reads top to bottom: the whole trace on one bar, a time ruler, then one row per handler attempt, offset by when it started (and staggered as the trace moves from stream to stream). Each row shows the stream and group, the message id, the worker and node it ran on, and its wait, handler time and total. Pale is **queue wait**, solid is the **handler**, coloured per stream; every bar aligns with its stream's name. The time between two rows is the hand-off: publish, delivery and backoff. A retry adds another row for the same message, marked `try N` on its message line.

::: info What the dashboard can see
Workers keep message contents to themselves unless you allow otherwise. Without `RILLKIT_INSPECT_MESSAGES=true` the dashboard shows ids, sizes, statuses and trace ids, but no payloads or DLQ error texts; without `RILLKIT_CAPTURE_LOGS=true` there are no logs; without `RILLKIT_TRACE_RECORDS=true` there is no trace waterfall. Attempt timings (the **Time per message** chart) are part of telemetry and need no setting. Where something is hidden, the dashboard says which setting shows it. See [what the dashboard may see](/guide/control-plane#what-the-dashboard-may-see).
:::

### DLQ

![A dead-lettered message open, with the payload editor](/screenshots/dashboard-dlq-edit.jpg)

The stream's dead-letter queue, newest first, 25 to a page: when each message failed, its original id, how many times it was tried, and the reason.

- **Replay** sends messages back onto the stream to be handled again: one from its panel, the ones you tick with **Replay selected**, or everything with **Replay all**.
- **Edit and replay** fixes a message that can never succeed as it is. Open it, change the payload in the editor (it must be valid JSON, which the worker decodes as the message body), and **Replay edited**. Only that message changes.
- **Delete** removes messages for good: one, the selected ones, or the whole DLQ with **Purge**, which asks first.

A message's panel also shows **what happened**: the logs of its original attempts, each with its delivery number, consumer and exception, when the workers capture logs. Replayed messages go through your handlers again, so make handlers idempotent (see [`Idempotent`](/guide/middleware)).

### Configs

- **Workers per node**: how many consumer loops each node runs for this stream. With three nodes, four per node is twelve in total.
- **Poll settings**: how long a read waits for messages (`block_ms`) and how many it takes at once (`batch_size`).
- **Autoscaling**: a minimum and maximum number of workers in total, and a target lag per worker. The autoscaler sizes workers from **both** the observed arrival rate (finished messages per second plus backlog growth, with headroom) and the lag target, keeps the result between the minimum and maximum, and spreads it over the nodes consuming the stream, at least one each. That way a stream that is keeping up at zero lag does not collapse to its minimum and oscillate. Cooldowns stop it changing too often; the last decision is shown, with when it was made. If the leader replica is replaced, the next one seeds its rate and capacity estimate from the latest samples instead of starting from zero.

## Activity

Every directive sent to workers, by you or the autoscaler, newest first and paged: what it was, its target, who sent it, and its status: *pending*, *sent*, then *applied*, or why not. Expand a row for its parameters and the worker's answer. Filter by **User** or **Autoscaler**.

## Settings

- **API keys** for workers. A new key's secret is shown once, so store it right away; afterwards only its first characters are shown. Revoking a key disconnects its workers.
- **Connect a worker**: the environment variables a worker needs, ready to paste into an env file or secret manager.

The dashboard has a light and a dark theme (the moon icon), and signs out from the arrow at the top right.
