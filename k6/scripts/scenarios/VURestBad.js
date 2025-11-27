import http from "k6/http";
import { sleep, check } from "k6";
import { Trend, Counter, Rate } from 'k6/metrics';

const restBadDuration = new Trend('rest_bad_duration');
const restBadRequests = new Counter('rest_bad_requests');
const restBadSuccessRate = new Rate('rest_bad_success');

export const restBadOptions = {
  executor: "ramping-vus",
  startVUs: 0,
  stages: [
    { duration: "1m", target: 1 },
    { duration: "5m", target: 1 }, // -> p = 1; users = 1;
    { duration: "0s", target: 3 },
    { duration: "1m", target: 3 }, // -> p = 1; users = 3;
    { duration: "0s", target: 1 },
    { duration: "1m", target: 1 },
  ],
};

export function restBadScenario() {
  let pacing = 1000
  const start = Date.now();

  const response = http.post(
    "http://localhost:8080/api/sync",
    "bad",
    {
      headers: { "Content-Type": "text/plain" },
      tags: { request_type: "rest_bad_sync" }
    }
  );

  const ok = check(response, {
    "status is appropriate": (r) => r.status === 400 || r.status === 200,
  });

  restBadDuration.add(Date.now() - start);
  restBadRequests.add(1);
  restBadSuccessRate.add(ok);

  // const elapsed = (Date.now() - start);
  // if (elapsed < pacing) {
  //   sleep((pacing - elapsed) * 1000);
  // }

  // sleep(pacing - ((Date.now() - start)));

  const elapsed = (Date.now() - start);
  sleep(Math.max(pacing - elapsed, 0) / 1000);
}
