<script setup lang="ts">
// The hero illustration: an isometric line drawing of RillKit at work. Two services publish
// into a stream (the rill, running downhill), a consumer group of three workers takes the
// messages, and each one ends acked, retried or in the DLQ. Above the workers floats the
// optional control plane: telemetry goes up, directives come down.
import { delta, projector, type Pt } from '../iso'

const { P, box, tile, flow, line } = projector(250, 190)

const MONO_W = 6.7
interface Label { x: number; y: number; t: string; tone?: string; tag?: string; anchor?: 'start' | 'middle' }
const pill = (l: Label) => {
  const w = l.t.length * MONO_W + 22
  const tw = l.tag ? l.tag.length * MONO_W + 16 : 0
  const total = w + (tw ? tw + 4 : 0)
  const x0 = l.anchor === 'start' ? l.x : l.x - total / 2
  return { ...l, w, tw, x0 }
}

// --- the publishers -------------------------------------------------------------------
const producers = [-115, 115].map((yc) => ({ yc, b: box(0, yc - 35, 0, 70, 70, 46) }))
const pubLines = producers.map((p) => flow(P(70, p.yc), P(156, p.yc < 0 ? -12 : 12), 0.55))

// --- the stream: a slab with a channel of water on top --------------------------------
const slab = box(150, -26, 0, 300, 52, 16)
const water = tile(160, -15, 16, 280, 30)
const ripples = [
  line(P(172, -6, 16), P(428, -6, 16)),
  line(P(184, 6, 16), P(420, 6, 16)),
]
const CHIP: [number, number, number] = [166, -7, 16]
const chipStart: Pt = P(...CHIP)
const chipEnd: Pt = P(418, CHIP[1], CHIP[2])
const chip = box(...CHIP, 14, 14, 9)
const chipPath = delta(chipStart, chipEnd)
const chips = [
  { tone: 'paper', begin: 0 },
  { tone: 'lav', begin: -1.5 },
  { tone: 'paper', begin: -3 },
  { tone: 'pink', begin: -4.5 },
  { tone: 'paper', begin: -6 },
]

// --- the consumer group ----------------------------------------------------------------
const WX = 560
const workers = [-120, 0, 120].map((yc, i) => ({
  yc,
  i,
  b: box(WX, yc - 40, 0, 80, 80, 56),
  lane: [0, 1, 2].map((k) => tile(WX + 14 + k * 20, yc - 8, 56, 14, 16)),
}))
const toWorkers = workers.map((w) => flow(P(450, w.yc * 0.08), P(WX + 4, w.yc), 0.5))

// --- the three exits -------------------------------------------------------------------
const ack = box(790, -160, 0, 100, 100, 6)
// the tick is drawn flat in the ack plate's own plane: u along x, v along y
const ackO = P(790, -160, 6)
const ackM = `matrix(0.866 0.5 -0.866 0.5 ${ackO[0]} ${ackO[1]})`
const dlq = box(790, 80, 0, 100, 100, 34)
const dlqInner = tile(802, 92, 34, 76, 76)
const toAck = workers.map((w) => flow(P(WX + 80, w.yc), P(796, -110 + w.i * 14), 0.5))
const toDlq = flow(P(WX + 80, 132), P(796, 130), 0.5)
const retry = `M${P(600, 160)[0]} ${P(600, 160)[1]} C${P(540, 236)[0]} ${P(540, 236)[1]} ${P(392, 120)[0]} ${P(392, 120)[1]} ${P(380, 28)[0]} ${P(380, 28)[1]}`

// --- the control plane: a screen floating over the workers ------------------------------
const SX = 590
const screen = box(SX, -170, 236, 10, 340, 150)
const face: Pt = P(SX + 10, 170, 386) // top left corner of the screen's face
const faceM = `matrix(0.866 -0.5 0 1 ${face[0]} ${face[1]})`
const spark = [118, 110, 114, 96, 100, 84, 90, 72, 80, 62, 70, 58, 64, 50]
const sparkD = 'M' + spark.map((v, i) => `${16 + i * 13} ${v}`).join(' L')
const sparkArea = `${sparkD} L${16 + (spark.length - 1) * 13} 132 L16 132 Z`
const rows = [
  { s: 'orders', st: 'running', tone: 'ok' },
  { s: 'payments', st: 'paused', tone: 'warn' },
  { s: 'emails', st: 'running', tone: 'ok' },
]
const wires = workers.map((w) => ({ ...w, d: line(P(SX + 10, w.yc, 236), P(600, w.yc, 60)) }))

// --- labels (flat, like callouts on a drawing) -------------------------------------------
const labels: Label[] = [
  { x: P(35, -115, 46)[0], y: P(35, -115, 46)[1] - 40, t: 'publish()' },
  { x: P(35, 115, 46)[0] - 40, y: P(35, 115, 46)[1] + 96, t: 'publish()' },
  { x: P(300, -26, 16)[0] + 24, y: P(300, -26, 16)[1] - 30, t: 'stream: orders', tone: 'brand' },
  { x: P(600, 160, 0)[0] - 30, y: P(600, 160, 0)[1] + 52, t: 'group: workers', tone: 'brand' },
  { x: P(840, -110, 6)[0] + 36, y: P(840, -110, 6)[1] + 42, t: 'ack', tone: 'ok' },
  { x: P(840, 130, 0)[0] + 10, y: P(840, 180, 0)[1] + 34, t: 'orders-dlq', tone: 'dlq' },
  { x: P(470, 200, 0)[0] - 40, y: P(470, 200, 0)[1] + 26, t: 'retry, after a backoff', tone: 'retry' },
]
const top = P(SX + 10, 0, 386)
const cpLabel = pill({ x: top[0] + 24, y: top[1] - 46, t: 'rillkit-server + dashboard', tag: 'optional' })
const wireLabel = pill({ x: P(600, -120, 150)[0] + 20, y: P(600, -120, 150)[1] + 8, t: 'telemetry ↑  directives ↓', anchor: 'start', tone: 'brand' })
const pills = labels.map(pill)
</script>

<template>
  <div class="rk-scene">
    <svg viewBox="0 0 1260 760" role="img" aria-labelledby="rk-scene-t">
      <title id="rk-scene-t">
        Two services publish messages into the stream "orders". Three workers in the group "workers" take them; each
        message is acked, retried after a backoff, or moved to the dead-letter stream orders-dlq. Above the workers,
        the optional control plane receives their telemetry and sends them directives.
      </title>
      <defs>
        <radialGradient id="rk-ground" cx="50%" cy="50%" r="50%">
          <stop offset="0" class="rk-ground-a" />
          <stop offset="1" class="rk-ground-b" />
        </radialGradient>
      </defs>

      <polygon :points="tile(-60, -250, 0, 1000, 500)" fill="url(#rk-ground)" class="rk-ground" />

      <!-- ground wires, then the messages running along them -->
      <g class="rk-wires" fill="none">
        <path v-for="(d, i) in pubLines" :key="'p' + i" :d="d" />
        <path v-for="(d, i) in toWorkers" :key="'w' + i" :d="d" />
        <path v-for="(d, i) in toAck" :key="'a' + i" :d="d" class="is-ok" />
        <path :d="toDlq" class="is-dlq" />
        <path :d="retry" class="is-retry" />
      </g>
      <g class="rk-mv">
        <circle v-for="(d, i) in pubLines" :key="'pd' + i" r="4.5" class="rk-dot">
          <animateMotion :path="d" dur="2.4s" :begin="`${-i * 1.2}s`" repeatCount="indefinite" />
        </circle>
        <circle v-for="(d, i) in toWorkers" :key="'wd' + i" r="4.5" class="rk-dot">
          <animateMotion :path="d" dur="2.1s" :begin="`${-i * 0.7}s`" repeatCount="indefinite" />
        </circle>
        <circle v-for="(d, i) in toAck" :key="'ad' + i" r="4.5" class="rk-dot is-ok">
          <animateMotion :path="d" dur="1.9s" :begin="`${-i * 0.6 - 0.3}s`" repeatCount="indefinite" />
        </circle>
        <circle r="4.5" class="rk-dot is-retry" opacity="0">
          <animateMotion :path="retry" dur="6s" begin="-1s" repeatCount="indefinite" keyPoints="0;0;1;1" keyTimes="0;0.2;0.62;1" calcMode="linear" />
          <animate attributeName="opacity" values="0;0;1;1;0;0" keyTimes="0;0.19;0.21;0.6;0.63;1" dur="6s" begin="-1s" repeatCount="indefinite" />
        </circle>
        <circle r="4.5" class="rk-dot is-dlq" opacity="0">
          <animateMotion :path="toDlq" dur="9s" begin="-2s" repeatCount="indefinite" keyPoints="0;0;1;1" keyTimes="0;0.7;0.84;1" calcMode="linear" />
          <animate attributeName="opacity" values="0;0;1;1;0;0" keyTimes="0;0.69;0.71;0.83;0.86;1" dur="9s" begin="-2s" repeatCount="indefinite" />
        </circle>
      </g>

      <!-- publishers -->
      <g v-for="p in producers" :key="p.yc" class="rk-box">
        <polygon :points="p.b.left" class="f-l" /><polygon :points="p.b.right" class="f-r" /><polygon :points="p.b.top" class="f-t" />
      </g>

      <!-- the stream -->
      <g class="rk-box rk-slab">
        <polygon :points="slab.left" class="f-l" /><polygon :points="slab.right" class="f-r" /><polygon :points="slab.top" class="f-t" />
        <polygon :points="water" class="rk-water" />
        <path v-for="(d, i) in ripples" :key="i" :d="d" class="rk-ripple" pathLength="100" :style="{ animationDelay: `${-i * 1.7}s` }" />
      </g>
      <g class="rk-mv">
        <g v-for="(c, i) in chips" :key="i" :class="['rk-chip', `is-${c.tone}`]" opacity="0">
          <polygon :points="chip.left" class="f-l" /><polygon :points="chip.right" class="f-r" /><polygon :points="chip.top" class="f-t" />
          <animateMotion :path="chipPath" dur="7.5s" :begin="`${c.begin}s`" repeatCount="indefinite" />
          <animate attributeName="opacity" values="0;1;1;0" keyTimes="0;0.08;0.9;1" dur="7.5s" :begin="`${c.begin}s`" repeatCount="indefinite" />
        </g>
      </g>

      <!-- exits -->
      <g class="rk-box rk-ack">
        <polygon :points="ack.left" class="f-l" /><polygon :points="ack.right" class="f-r" /><polygon :points="ack.top" class="f-t" />
        <path :transform="ackM" d="M30 52 L45 67 L72 34" class="rk-tick" fill="none" />
      </g>
      <g class="rk-box rk-dlq">
        <polygon :points="dlq.left" class="f-l" /><polygon :points="dlq.right" class="f-r" /><polygon :points="dlq.top" class="f-t" />
        <polygon :points="dlqInner" class="rk-dlq__in" />
      </g>

      <!-- workers -->
      <g v-for="w in workers" :key="w.yc" class="rk-box rk-worker">
        <polygon :points="w.b.left" class="f-l" /><polygon :points="w.b.right" class="f-r" /><polygon :points="w.b.top" class="f-t" />
        <polygon v-for="(l, k) in w.lane" :key="k" :points="l" class="rk-lane" :style="{ animationDelay: `${-(w.i * 0.5 + k * 0.35)}s` }" />
      </g>

      <!-- control plane: wires from the screen to every worker -->
      <g class="rk-cp-wires" fill="none">
        <path v-for="w in wires" :key="w.yc" :d="w.d" />
      </g>
      <g class="rk-mv">
        <template v-for="w in wires" :key="w.yc">
          <circle r="3.6" class="rk-dot is-tele">
            <animateMotion :path="w.d" dur="2.6s" :begin="`${-w.i * 0.8}s`" repeatCount="indefinite" keyPoints="1;0" keyTimes="0;1" calcMode="linear" />
          </circle>
          <rect x="-4" y="-4" width="8" height="8" rx="1.5" class="rk-dot is-dir" opacity="0">
            <animateMotion :path="w.d" dur="7s" :begin="`${-w.i * 2.3}s`" repeatCount="indefinite" keyPoints="0;0;1;1" keyTimes="0;0.55;0.8;1" calcMode="linear" />
            <animate attributeName="opacity" values="0;0;1;1;0;0" keyTimes="0;0.54;0.56;0.78;0.81;1" dur="7s" :begin="`${-w.i * 2.3}s`" repeatCount="indefinite" />
          </rect>
        </template>
      </g>

      <g class="rk-float">
        <g class="rk-box rk-screen">
          <polygon :points="screen.left" class="f-l" /><polygon :points="screen.top" class="f-t" /><polygon :points="screen.right" class="f-r" />
          <g :transform="faceM" class="rk-ui">
            <rect x="9" y="9" width="322" height="132" rx="7" class="rk-ui__bg" />
            <circle cx="22" cy="22" r="3.2" class="rk-ui__dot" /><circle cx="33" cy="22" r="3.2" class="rk-ui__dot" /><circle cx="44" cy="22" r="3.2" class="rk-ui__dot" />
            <text x="58" y="26" class="rk-ui__brand">Rill<tspan font-style="italic">Kit</tspan></text>
            <text x="16" y="48" class="rk-ui__k">messages handled</text>
            <text x="16" y="72" class="rk-ui__v">1,284<tspan class="rk-ui__u"> /s</tspan></text>
            <path :d="sparkArea" class="rk-ui__area" />
            <path :d="sparkD" class="rk-ui__line" fill="none" pathLength="100" />
            <g v-for="(r, i) in rows" :key="r.s" :transform="`translate(208 ${46 + i * 24})`">
              <circle cx="4" cy="-3" r="3.4" :class="`rk-ui__st is-${r.tone}`" />
              <text x="13" y="0" class="rk-ui__row">{{ r.s }}</text>
              <text x="114" y="0" text-anchor="end" :class="`rk-ui__state is-${r.tone}`">{{ r.st }}</text>
            </g>
            <g transform="translate(208 118)">
              <rect width="34" height="15" rx="7.5" class="rk-ui__btn" /><text x="17" y="10.5" text-anchor="middle" class="rk-ui__btnt">pause</text>
              <rect x="40" width="34" height="15" rx="7.5" class="rk-ui__btn" /><text x="57" y="10.5" text-anchor="middle" class="rk-ui__btnt">drain</text>
              <rect x="80" width="34" height="15" rx="7.5" class="rk-ui__btn is-on" /><text x="97" y="10.5" text-anchor="middle" class="rk-ui__btnt">scale</text>
            </g>
          </g>
        </g>
        <g class="rk-pill is-cp" :transform="`translate(${cpLabel.x0} ${cpLabel.y})`">
          <rect y="-13" :width="cpLabel.w" height="26" rx="13" />
          <text x="11" y="4.5">{{ cpLabel.t }}</text>
          <g :transform="`translate(${cpLabel.w + 4} 0)`">
            <rect y="-13" :width="cpLabel.tw" height="26" rx="13" class="rk-pill__tag" />
            <text x="8" y="4.5" class="rk-pill__tagt">{{ cpLabel.tag }}</text>
          </g>
        </g>
      </g>

      <g class="rk-pills">
        <g v-for="(l, i) in [...pills, wireLabel]" :key="i" :class="['rk-pill', l.tone && `is-${l.tone}`]" :transform="`translate(${l.x0} ${l.y})`">
          <rect y="-12" :width="l.w" height="24" rx="12" />
          <text x="11" y="4">{{ l.t }}</text>
        </g>
      </g>
    </svg>
  </div>
</template>
