import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    load: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 500),
      timeUnit: "1s",
      duration: __ENV.DURATION || "40s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 50),
      maxVUs: Number(__ENV.MAX_VUS || 600),
      gracefulStop: "5s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.02"],
  },
  discardResponseBodies: true,
};

const BASE = __ENV.BASE || "http://localhost:8090";
const PAD = "x".repeat(Number(__ENV.PAD_BYTES || 1024));

export default function () {
  const body = `{"seq":"${__VU}-${__ITER}","sent_ms":${Date.now()},"pad":"${PAD}"}`;
  const res = http.post(`${BASE}/publish`, body, {
    headers: { "Content-Type": "application/json" },
    tags: { name: "publish" },
  });
  check(res, { ok: (r) => r.status === 200 });
}
