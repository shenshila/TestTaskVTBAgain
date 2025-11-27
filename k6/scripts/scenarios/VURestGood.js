import http from "k6/http";
import { sleep, check } from "k6";
import { Trend, Counter, Rate } from 'k6/metrics';

const restGoodDuration = new Trend('rest_good_duration');
const restGoodRequests = new Counter('rest_good_requests');
const restGoodSuccessRate = new Rate('rest_good_success');

export const restGoodOptions = {
  executor: "ramping-vus",
  startVUs: 0,
  stages: [
    { duration: "1m", target: 3 },
    { duration: "5m", target: 3 },
    { duration: "2m", target: 3 },
  ],
};

export function restGoodScenario() {
  const pacing = 1;
  const start = Date.now();

  const response = http.post("http://localhost:8080/api/sync", "good", {
          headers: { 'Content-Type': 'text/plain' },
          tags: { request_type: "rest_good_sync" }
      });
      
      const success = check(response, {
          'status is 200': (r) => r.status === 200,
      });
  
      restGoodDuration.add(Date.now() - start);
      restGoodRequests.add(1);
      restGoodSuccessRate.add(success);

  const elapsed = (Date.now() - start) / 1000;
  sleep(Math.max(pacing - elapsed, 0));
}
