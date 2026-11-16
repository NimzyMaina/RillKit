<script setup lang="ts">
import { withBase } from 'vitepress'
import RillMini from './RillMini.vue'

const cards = [
  { t: 'Pause, drain, resume', d: 'One stream, one node or the whole fleet. Paused workers finish the message in hand; drained ones also let go of their consumers.' },
  { t: 'Applied exactly once', d: 'Every change is a directive with an id. Any worker applies it once, answers, and it is listed in Activity with who sent it.' },
  { t: 'Throughput and trends', d: 'Messages handled, success rate, data rate and DLQ size, with the change against the previous window: green when better, red when worse.' },
  { t: 'Traces and wait/work', d: 'Queue wait against handler time per stream, and one trace across every stream a message crossed, so the slow step is obvious. Traces are opt-in metadata, kept for an hour.' },
  { t: 'Logs per message', d: 'What a handler logged for one message, attempt by attempt, with its exception and W3C trace id. Opt in with RILLKIT_CAPTURE_LOGS.' },
  { t: 'Payloads stay put', d: 'The server stores neither payloads, logs nor trace records. It asks a worker when you open a message or a trace, and only if that worker allows it.' },
  { t: 'Lanes you can change', d: 'Raise or lower a stream’s worker lanes per node from the dashboard; workers apply it without a restart, and the autoscaler can do it for you.' },
  { t: 'Several replicas', d: 'Run as many servers as you like on one Postgres. Directives reach a worker through whichever replica holds its connection.' },
  { t: 'Prometheus metrics', d: 'GET /api/metrics exports replica, tenant, node, lane, in-flight, lag, pending and ack-rate gauges for your existing scrape.' },
]
</script>

<template>
  <section id="control-plane" class="rk-band rk-control" aria-labelledby="rk-control-h">
    <div class="rk-wrap">
      <header class="rk-control__head">
        <p class="rk-eyebrow">The control plane · optional</p>
        <h2 id="rk-control-h" class="rk-h2">Watch and steer every worker, <em>from one place.</em></h2>
        <p class="rk-sub">
          Set <code>RILLKIT_CONTROL=relay</code> and each worker reports to <strong>rillkit-server</strong>. Its dashboard
          shows every stream, worker and message, traces each message across streams, and lets you pause, scale and
          replay them. No code changes, and the library works the same without it.
        </p>
        <div class="rk-actions">
          <a class="rk-btn rk-btn--primary" :href="withBase('/guide/server')">Run the server</a>
          <a class="rk-btn rk-btn--ghost" :href="withBase('/guide/dashboard')">Tour the dashboard</a>
        </div>
      </header>

      <figure class="rk-shot rk-shot--hero">
        <div class="rk-shot__bar" aria-hidden="true"><i /><i /><i /><span>localhost:8080/t/demo</span></div>
        <img :src="withBase('/screenshots/dashboard-overview.jpg')" width="1800" height="1125" loading="lazy"
          alt="The dashboard's overview: nodes online, active workers, messages in flight, total lag and ack rate; controls to pause, resume, drain or put every worker in maintenance; and the throughput section." />
      </figure>

      <div class="rk-row">
        <div class="rk-row__text">
          <p class="rk-eyebrow">Workers dial out</p>
          <h3 class="rk-h3">Nothing to open, nothing to install.</h3>
          <p>
            Each worker opens one outbound WebSocket to the server. Telemetry goes up every few seconds; directives come back
            down. The server does not touch your Redis or Valkey.
          </p>
          <ul class="rk-ticks">
            <li><b>No inbound ports</b> on your workers, no agent beside them.</li>
            <li><b>Keeps consuming</b> if the server goes away, under the last state it was given.</li>
            <li><b>One setting</b>: <code>RILLKIT_CONTROL=relay</code>, a URL and an API key.</li>
          </ul>
        </div>
        <RillMini kind="relay" />
      </div>

      <div class="rk-row rk-row--flip">
        <div class="rk-row__text">
          <p class="rk-eyebrow">Autoscale on load</p>
          <h3 class="rk-h3">More lanes when the work demands it.</h3>
          <p>
            Give a stream a minimum, a maximum and a target lag per worker. The autoscaler follows the arrival rate and
            the backlog, adds lanes when work builds and removes them when the rate falls — not just when lag hits zero.
          </p>
          <ul class="rk-ticks">
            <li><b>Cooldowns</b> stop it changing its mind every few seconds.</li>
            <li><b>Safe failover</b>: a replacement leader seeds the rate from the last samples.</li>
            <li><b>Every decision</b> is a directive you can read in Activity.</li>
          </ul>
        </div>
        <RillMini kind="scale" />
      </div>

      <div class="rk-row">
        <div class="rk-row__text">
          <p class="rk-eyebrow">Messages, as they arrive</p>
          <h3 class="rk-h3">See what is in the stream right now.</h3>
          <p>
            Newest first, as of a moment you choose or live as messages land, each with its status: waiting, pending with
            its try number, or acked. Open one in place for its payload, its trace id, its logs, and <b>View trace</b>: every
            stream the trace crossed, with queue wait against handler time.
          </p>
        </div>
        <figure class="rk-shot">
          <div class="rk-shot__bar" aria-hidden="true"><i /><i /><i /><span>orders / billing · Messages</span></div>
          <img :src="withBase('/screenshots/dashboard-messages.jpg')" width="1600" height="1000" loading="lazy"
            alt="A stream's Messages tab with Live on: each message row shows its time, id, status, consumer, size and payload preview; a row is expanded in place." />
        </figure>
      </div>

      <div class="rk-row rk-row--flip">
        <div class="rk-row__text">
          <p class="rk-eyebrow">Find the bottleneck</p>
          <h3 class="rk-h3">A trace, end to end.</h3>
          <p>
            Queue wait against handler time for every stream, and one trace across every stream a message crossed —
            including retries — on one time ruler. The slow step is obvious without opening a tracing stack.
          </p>
        </div>
        <figure class="rk-shot">
          <div class="rk-shot__bar" aria-hidden="true"><i /><i /><i /><span>orders.raw → orders.enriched · Trace</span></div>
          <img :src="withBase('/screenshots/dashboard-trace.jpg')" width="2216" height="1004" loading="lazy"
            alt="A trace waterfall: the end-to-end duration at the top, a time ruler, then each handler attempt offset by its start, with stream, worker, node, queue wait and handler time." />
        </figure>
      </div>

      <div class="rk-row">
        <div class="rk-row__text">
          <p class="rk-eyebrow">Fix and replay the DLQ</p>
          <h3 class="rk-h3">Edit the payload, then send it back.</h3>
          <p>
            Page through dead letters with the reason each one failed and the logs of every attempt. Replay one, a selection
            or all of them, delete them, or correct a payload that could never succeed and replay just that message.
          </p>
        </div>
        <figure class="rk-shot">
          <div class="rk-shot__bar" aria-hidden="true"><i /><i /><i /><span>orders / billing · DLQ</span></div>
          <img :src="withBase('/screenshots/dashboard-dlq-edit.jpg')" width="1600" height="1000" loading="lazy"
            alt="A dead-lettered message expanded under its row: the error, the original message id, the deliveries, and a JSON editor to change the payload before replaying it." />
        </figure>
      </div>

      <ul class="rk-cards">
        <li v-for="c in cards" :key="c.t">
          <h3>{{ c.t }}</h3>
          <p>{{ c.d }}</p>
        </li>
      </ul>

      <div class="rk-control__try">
        <div>
          <h3 class="rk-h3">See it in one command</h3>
          <p>
            One download starts Postgres, Valkey, rillkit-server with its dashboard, two demo workers and a producer
            whose load rises and falls, so lag builds, the autoscaler reacts and failures land in the DLQ. A
            <code>flow</code> service sends one traced order through two streams every two minutes. The images are
            published: no clone, no build.
          </p>
          <p class="rk-control__links">
            <a :href="withBase('/guide/server')">Run the server</a>
            <a :href="withBase('/guide/dashboard')">Tour the dashboard</a>
            <a :href="withBase('/guide/control-plane')">Connect workers</a>
          </p>
          <p class="rk-control__license">
            The SDKs are Apache 2.0. rillkit-server and the dashboard are
            <a href="https://fsl.software" target="_blank" rel="noopener">FSL-1.1-ALv2</a>: free to self-host for your own
            organization, and Apache 2.0 two years after each release.
          </p>
        </div>
        <figure class="rk-term rk-term--plain" aria-label="Commands to start the demo">
          <div class="rk-term__bar"><span class="rk-term__tab">terminal</span></div>
          <pre><code><span class="p">$</span> curl -fsSLO https://raw.githubusercontent.com/\
    NimzyMaina/RillKit/main/deploy/demo.yaml
<span class="p">$</span> docker compose -f demo.yaml up -d

<span class="c"># open http://localhost:8080</span>
<span class="c"># admin token: rillkit-local-admin-token</span></code></pre>
        </figure>
      </div>
    </div>
  </section>
</template>
