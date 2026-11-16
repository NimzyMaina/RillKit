// Isometric helpers for the landing page's line-art scenes.
// x runs down and to the right, y down and to the left, z straight up; one unit is one pixel
// of the SVG's user space before projection. Everything is deterministic, so the server and
// the client render identical markup.

export type Pt = [number, number]

const COS = Math.cos(Math.PI / 6)
const SIN = 0.5
const r = (n: number) => Math.round(n * 10) / 10

export function projector(ox: number, oy: number) {
  const P = (x: number, y: number, z = 0): Pt => [r(ox + (x - y) * COS), r(oy + (x + y) * SIN - z)]

  const poly = (...p: Pt[]) => p.map((q) => `${q[0]},${q[1]}`).join(' ')

  /** The three visible faces of a box: its top, its +x face (front right) and its +y face (front left). */
  const box = (x: number, y: number, z: number, w: number, d: number, h: number) => ({
    top: poly(P(x, y, z + h), P(x + w, y, z + h), P(x + w, y + d, z + h), P(x, y + d, z + h)),
    right: poly(P(x + w, y, z), P(x + w, y + d, z), P(x + w, y + d, z + h), P(x + w, y, z + h)),
    left: poly(P(x, y + d, z), P(x + w, y + d, z), P(x + w, y + d, z + h), P(x, y + d, z + h)),
  })

  /** A flat rhombus at height z. */
  const tile = (x: number, y: number, z: number, w: number, d: number) =>
    poly(P(x, y, z), P(x + w, y, z), P(x + w, y + d, z), P(x, y + d, z))

  /** A straight polyline through projected points. */
  const line = (...p: Pt[]) => 'M' + p.map((q) => `${q[0]} ${q[1]}`).join(' L')

  /** A cubic from a to b that leaves and arrives along the x axis (the direction of flow). */
  const flow = (a: Pt, b: Pt, bend = 0.5) => {
    const dx = (b[0] - a[0]) * bend
    const dy = (b[1] - a[1]) * bend
    return `M${a[0]} ${a[1]} C${r(a[0] + dx)} ${r(a[1] + dy * 0.2)} ${r(b[0] - dx)} ${r(b[1] - dy * 0.2)} ${b[0]} ${b[1]}`
  }

  return { P, poly, box, tile, line, flow }
}

/** The offset that carries a shape drawn at a to b, as an animateMotion path. */
export function delta(a: Pt, b: Pt) {
  return `M0 0 L${r(b[0] - a[0])} ${r(b[1] - a[1])}`
}
