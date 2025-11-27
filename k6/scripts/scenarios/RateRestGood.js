import http from "k6/http";
import { check } from "k6";
import { Trend, Counter, Rate } from 'k6/metrics';

const restGoodDuration = new Trend('rest_good_duration');
const restGoodRequests = new Counter('rest_good_requests');
const restGoodSuccessRate = new Rate('rest_good_success');

export const restGoodOptions = {
    executor: 'constant-arrival-rate',
    rate: 3,
    timeUnit: '1s',
    duration: '8m',
    preAllocatedVUs: 3,
    maxVUs: 10,
};

export function restGoodScenario() {
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
}