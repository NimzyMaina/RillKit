<script setup lang="ts">
// Small isometric drawings for the control-plane band.
//   relay: workers inside your network dial out to rillkit-server; telemetry goes up, directives come down.
//   scale: a backlog builds on a stream, the autoscaler adds lanes to each node, the backlog drains.
import { computed } from 'vue'
import { projector } from '../iso'

const props = defineProps<{ kind: 'relay' | 'scale' }>()

const MONO_W = 6.7
const pill = (x: number, y: number, t: string, tone = '') => {
  const w = t.length * MONO_W + 22
  return { x0: x - w / 2, y, w, t, tone }
}

/** values/keyTimes for an element that is visible between on and off (fractions of the cycle). */
const span = (on: number, off: number) => ({
  values: '0;0;1;1;0;0',
  keyTimes: `0;${on.toFixed(3)};${(on + 0.01).toFixed(3)};${off.toFixed(3)};${(off + 0.01).toFixed(3)};1`,
})

const relay = computed(() => {
  const { P, box, tile, flow } = projector(205, 130)
  const workers = [-70, 30, 130].map((yc, i) => ({ yc, i, b: box(170, yc - 30, 0, 60, 60, 44) }))
  const db = box(-10, 10, 0, 80, 80, 54)
  const server = box(440, -60, 0, 96, 96, 74)
  const fromDb = workers.map((w) => flow(P(70, 50), P(172, w.yc), 0.5))
  const out = workers.map((w) => flow(P(230, w.yc, 22), P(442, -12 + w.i * 12, 30), 0.5))
  const top = P(440, -60, 74)
  return {
    fence: tile(-40, -120, 0, 300, 290),
    workers, db, server, fromDb, out,
    pills: [
      pill(P(30, 50, 54)[0], P(30, 50, 54)[1] - 30, 'Redis / Valkey'),
      pill(P(260, 170, 0)[0] - 74, P(260, 170, 0)[1] + 30, 'your network: no inbound port', 'quiet'),
      pill(top[0], top[1] - 24, 'rillkit-server', 'cp'),
      pill(470, 384, 'outbound WebSocket'),
    ],
  }
})

const scale = computed(() => {
  const { P, box, tile, flow } = projector(150, 120)
  const slab = box(0, -24, 0, 280, 48, 14)
  const CH: [number, number, number] = [14, -8, 14]
  const chip = box(...CH, 16, 16, 10)
  const N = 8
  const chips = Array.from({ length: N }, (_, k) => ({
    k,
    // each chip sits one place further along the stream
    at: (() => {
      const a = P(...CH)
      const b = P(CH[0] + k * 32, CH[1], CH[2])
      return `translate(${(b[0] - a[0]).toFixed(1)} ${(b[1] - a[1]).toFixed(1)})`
    })(),
    a: span(0.04 + k * 0.04, 0.78 - k * 0.055),
  }))
  const nodes = [-80, 70].map((yc, i) => {
    const x = 380
    const b = box(x, yc - 40, 0, 80, 80, 50)
    const lanes = [0, 1, 2, 3].map((j) => {
      const lx = x + 10 + (j % 2) * 32
      const ly = yc - 30 + Math.floor(j / 2) * 32
      return { j, p: tile(lx, ly, 50, 26, 26), a: j === 0 ? null : span(0.28 + j * 0.06 + i * 0.03, 0.86) }
    })
    return { yc, i, b, lanes }
  })
  const toNodes = nodes.map((n) => flow(P(280, n.yc * 0.1), P(382, n.yc), 0.5))
  const auto = box(380, -180, 190, 80, 110, 12)
  const autoTop = P(420, -125, 202)
  const wires = nodes.map((n) => `M${P(420, n.yc, 190).join(' ')} L${P(420, n.yc, 50).join(' ')}`)
  return {
    slab, chip, chips, nodes, toNodes, auto, wires,
    pills: [
      pill(autoTop[0] + 70, autoTop[1] - 40, 'autoscaler', 'cp'),
      pill(P(140, 24, 0)[0] - 20, P(140, 24, 0)[1] + 40, 'stream: orders, lag rising'),
      pill(P(420, 110, 0)[0] - 10, P(420, 110, 0)[1] + 40, 'lanes per node: 1 → 4'),
    ],
  }
})
</script>

<template>
  <div class="rk-mini">
    <svg v-if="props.kind === 'relay'" viewBox="0 0 760 470" role="img"
      aria-label="Three workers inside your network read from Redis or Valkey and each opens an outbound WebSocket to rillkit-server. Telemetry flows up to the server; directives flow back down. Nothing connects in.">
      <polygon :points="relay.fence" class="rk-fence" />
      <g class="rk-wires" fill="none">
        <path v-for="(d, i) in relay.fromDb" :key="'d' + i" :d="d" />
        <path v-for="(d, i) in relay.out" :key="'o' + i" :d="d" class="is-out" />
      </g>
      <g class="rk-mv">
        <circle v-for="(d, i) in relay.fromDb" :key="'dd' + i" r="4" class="rk-dot">
          <animateMotion :path="d" dur="2s" :begin="`${-i * 0.6}s`" repeatCount="indefinite" />
        </circle>
        <template v-for="(d, i) in relay.out" :key="'od' + i">
          <circle r="4" class="rk-dot">
            <animateMotion :path="d" dur="2.8s" :begin="`${-i * 0.9}s`" repeatCount="indefinite" />
          </circle>
          <rect x="-4.5" y="-4.5" width="9" height="9" rx="1.5" class="rk-dot is-dir" opacity="0">
            <animateMotion :path="d" dur="6s" :begin="`${-i * 2}s`" repeatCount="indefinite" keyPoints="1;1;0;0" keyTimes="0;0.5;0.8;1" calcMode="linear" />
            <animate attributeName="opacity" values="0;0;1;1;0;0" keyTimes="0;0.49;0.51;0.79;0.81;1" dur="6s" :begin="`${-i * 2}s`" repeatCount="indefinite" />
          </rect>
        </template>
      </g>
      <g class="rk-box is-db">
        <polygon :points="relay.db.left" class="f-l" /><polygon :points="relay.db.right" class="f-r" /><polygon :points="relay.db.top" class="f-t" />
      </g>
      <g v-for="w in relay.workers" :key="w.yc" class="rk-box">
        <polygon :points="w.b.left" class="f-l" /><polygon :points="w.b.right" class="f-r" /><polygon :points="w.b.top" class="f-t" />
      </g>
      <g class="rk-box is-cp">
        <polygon :points="relay.server.left" class="f-l" /><polygon :points="relay.server.right" class="f-r" /><polygon :points="relay.server.top" class="f-t" />
      </g>
      <g v-for="(l, i) in relay.pills" :key="i" :class="['rk-pill', l.tone && `is-${l.tone}`]" :transform="`translate(${l.x0} ${l.y})`">
        <rect y="-12" :width="l.w" height="24" rx="12" /><text x="11" y="4">{{ l.t }}</text>
      </g>
    </svg>

    <svg v-else viewBox="0 0 760 500" role="img"
      aria-label="A backlog of messages builds on the stream orders. The autoscaler sees the lag and raises the lanes on each of two nodes from one to four; the backlog drains, and later the lanes go back down.">
      <g class="rk-wires" fill="none">
        <path v-for="(d, i) in scale.toNodes" :key="i" :d="d" />
        <path v-for="(d, i) in scale.wires" :key="'w' + i" :d="d" class="is-out" />
      </g>
      <g class="rk-mv">
        <circle v-for="(d, i) in scale.toNodes" :key="'td' + i" r="4" class="rk-dot">
          <animateMotion :path="d" dur="1.8s" :begin="`${-i * 0.9}s`" repeatCount="indefinite" />
        </circle>
        <rect v-for="(d, i) in scale.wires" :key="'wd' + i" x="-4.5" y="-4.5" width="9" height="9" rx="1.5" class="rk-dot is-dir" opacity="0">
          <animateMotion :path="d" dur="10s" begin="0s" repeatCount="indefinite" keyPoints="0;0;1;1" keyTimes="0;0.26;0.33;1" calcMode="linear" />
          <animate attributeName="opacity" values="0;0;1;1;0;0" keyTimes="0;0.25;0.27;0.33;0.35;1" dur="10s" begin="0s" repeatCount="indefinite" />
        </rect>
      </g>
      <g class="rk-box">
        <polygon :points="scale.slab.left" class="f-l" /><polygon :points="scale.slab.right" class="f-r" /><polygon :points="scale.slab.top" class="f-t" />
      </g>
      <g class="rk-mv">
        <g v-for="c in [...scale.chips].reverse()" :key="c.k" class="rk-chip" opacity="0" :transform="c.at">
          <polygon :points="scale.chip.left" class="f-l" /><polygon :points="scale.chip.right" class="f-r" /><polygon :points="scale.chip.top" class="f-t" />
          <animate attributeName="opacity" :values="c.a.values" :keyTimes="c.a.keyTimes" dur="10s" begin="0s" repeatCount="indefinite" />
        </g>
      </g>
      <g class="rk-still">
        <g v-for="c in scale.chips.slice(0, 4)" :key="c.k" class="rk-chip" :transform="c.at">
          <polygon :points="scale.chip.left" class="f-l" /><polygon :points="scale.chip.right" class="f-r" /><polygon :points="scale.chip.top" class="f-t" />
        </g>
      </g>
      <g v-for="n in scale.nodes" :key="n.yc" class="rk-box">
        <polygon :points="n.b.left" class="f-l" /><polygon :points="n.b.right" class="f-r" /><polygon :points="n.b.top" class="f-t" />
        <template v-for="l in n.lanes" :key="l.j">
          <polygon :points="l.p" class="rk-lane" />
          <polygon v-if="!l.a" :points="l.p" class="rk-lane is-on" />
          <polygon v-else :points="l.p" class="rk-lane is-on rk-mv" opacity="0">
            <animate attributeName="opacity" :values="l.a.values" :keyTimes="l.a.keyTimes" dur="10s" begin="0s" repeatCount="indefinite" />
          </polygon>
          <polygon v-if="l.j === 1" :points="l.p" class="rk-lane is-on rk-still" />
        </template>
      </g>
      <g class="rk-box is-cp">
        <polygon :points="scale.auto.left" class="f-l" /><polygon :points="scale.auto.right" class="f-r" /><polygon :points="scale.auto.top" class="f-t" />
      </g>
      <g v-for="(l, i) in scale.pills" :key="i" :class="['rk-pill', l.tone && `is-${l.tone}`]" :transform="`translate(${l.x0} ${l.y})`">
        <rect y="-12" :width="l.w" height="24" rx="12" /><text x="11" y="4">{{ l.t }}</text>
      </g>
    </svg>
  </div>
</template>
