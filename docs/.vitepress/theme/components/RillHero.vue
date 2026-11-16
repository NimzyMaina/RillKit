<script setup lang="ts">
import { withBase } from 'vitepress'
import CopyInstall from './CopyInstall.vue'
import RillScene from './RillScene.vue'
</script>

<template>
  <section class="rk-hero" aria-labelledby="rk-hero-h">
    <div class="rk-hero__copy">
      <p class="rk-eyebrow">Redis and Valkey streams for Python</p>
      <h1 id="rk-hero-h" class="rk-title">Decorate a function, <em>steer every worker.</em></h1>
      <p class="rk-lede">
        RillKit consumes Redis and Valkey streams for you: consumer groups, retries with backoff, a dead-letter queue and a
        clean shutdown, around a function you write. Turn on the optional control plane and you can watch, pause, scale and
        replay every worker from one dashboard, and see where a message spent its time at every step it crossed.
      </p>
      <div class="rk-actions">
        <a class="rk-btn rk-btn--primary" :href="withBase('/guide/getting-started')">Get started</a>
        <a class="rk-btn rk-btn--ghost" href="#control-plane">See the control plane</a>
      </div>
      <CopyInstall />
      <p class="rk-meta">
        Python 3.9+, TypeScript next <i /> Redis or Valkey, one server or a cluster <i /> Apache 2.0
        <i /> <a href="https://github.com/NimzyMaina/RillKit" target="_blank" rel="noopener">Source on GitHub</a>
      </p>
    </div>
    <RillScene />
  </section>
</template>
