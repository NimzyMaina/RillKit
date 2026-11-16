import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    hot: {
      executor: "constant-arrival-rate", rate: Number(__ENV.HOT_RATE || 10),
      timeUnit: "1s", duration: __ENV.DURATION || "20s",
      preAllocatedVUs: 30, maxVUs: 400, exec: "hot",
    },
    light: {
      executor: "constant-arrival-rate", rate: Number(__ENV.LIGHT_RATE || 50),
      timeUnit: "1s", duration: __ENV.DURATION || "20s",
      preAllocatedVUs: 10, maxVUs: 50, exec: "light",
    },
  },
  discardResponseBodies: true,
};
const BASE = __ENV.BASE || "http://localhost:8090";
const PAD = "x".repeat(Number(__ENV.PAD_BYTES || 1024));
function send(stream, prefix, i) {
  const body = `{"stream":"${stream}","seq":"${prefix}-${__VU}-${i}","sent_ms":${Date.now()},"pad":"${PAD}"}`;
  const r = http.post(`${BASE}/publish`, body, { headers: { "Content-Type": "application/json" } });
  check(r, { ok: (x) => x.status === 200 });
}
export function hot() { send("noise.hot", "h", __ITER); }
export function light() { send("noise.light", "l", __ITER); }
