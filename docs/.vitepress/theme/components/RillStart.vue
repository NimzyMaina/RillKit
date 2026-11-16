<script setup lang="ts">
import { withBase } from 'vitepress'
import RillArt from './RillArt.vue'
</script>

<template>
  <section class="rk-start" aria-labelledby="rk-start-h">
    <div class="rk-start__rill"><RillArt :seed="11" :trunks="[0.3, 0.55, 0.8]" :max-paths="16" :height="600" /></div>
    <div class="rk-wrap rk-start__grid">
      <div>
        <h2 id="rk-start-h" class="rk-h2">Running in about <em>five minutes.</em></h2>
        <p class="rk-sub">
          You need Python 3.9 or newer and a Redis or Valkey server you can reach. Local Docker is fine. The guide walks through the
          same steps with more explanation, then covers retries, the DLQ and running several workers.
        </p>
        <p class="rk-links">
          <a class="rk-btn rk-btn--primary" :href="withBase('/guide/getting-started')">Read the getting-started guide</a>
          <a :href="withBase('/examples/')">Examples</a>
          <a :href="withBase('/guide/server')">The control plane</a>
          <a :href="withBase('/reference/broker')">API reference</a>
        </p>
      </div>
      <figure class="rk-term rk-term--plain" aria-label="Quick start commands">
        <div class="rk-term__bar"><span class="rk-term__tab">terminal</span></div>
        <pre><code><span class="c"># a server, if you do not have one (or redis:7)</span>
<span class="p">$</span> docker run -d -p 6379:6379 valkey/valkey:8

<span class="p">$</span> pip install rillkit
<span class="p">$</span> export RILLKIT_URL=redis://localhost:6379/0

<span class="p">$</span> python worker.py   <span class="c"># terminal 1</span>
<span class="p">$</span> python publish.py  <span class="c"># terminal 2</span></code></pre>
      </figure>
    </div>
  </section>
</template>
