import http from "k6/http";
import { check } from "k6";
import { Trend, Counter, Rate } from 'k6/metrics';

const restBadDuration = new Trend('rest_bad_duration');
const restBadRequests = new Counter('rest_bad_requests');
const restBadSuccessRate = new Rate('rest_bad_success');

export const restBadOptions = {
    executor: 'ramping-arrival-rate',
    startRate: 1,
    timeUnit: '1s',
    preAllocatedVUs: 1,
    maxVUs: 10,
    stages: [
        { target: 1, duration: '1m' },
        { target: 1, duration: '5m' },
        { target: 3, duration: '0s' },
        { target: 3, duration: '1m' },
        { target: 1, duration: '0s' },
        { target: 1, duration: '1m' },
    ],
};

export function restBadScenario() {
    const start = Date.now();
    
    const response = http.post("http://localhost:8080/api/sync", "bad", {
        headers: { 'Content-Type': 'text/plain' },
        tags: { request_type: "rest_bad_sync" }
    });
    
    const success = check(response, {
        'status is appropriate': (r) => r.status === 400 || r.status === 200,
    });

    restBadDuration.add(Date.now() - start);
    restBadRequests.add(1);
    restBadSuccessRate.add(success);
}