import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';
import {
  Writer,
  Connection,
  SchemaRegistry,
  SCHEMA_TYPE_STRING,
} from "k6/x/kafka";

// -----------------------------
// Kafka setup
// -----------------------------
const writer = new Writer({
  brokers: ["localhost:29092"],
  topic: "input-topic",
  balancer: "balancer_roundrobin",
  batchSize: 1,
});

const connection = new Connection({
  address: "localhost:29092",
});

const schemaRegistry = new SchemaRegistry();
let kafkaCounter = 0;

function nextPartition() {
  return (kafkaCounter++ % 2 === 0) ? 0 : 1;
}

// -----------------------------
// Metrics
// -----------------------------
const asyncDuration = new Trend('async_request_duration');
const syncGoodDuration = new Trend('sync_good_duration');
const syncBadDuration = new Trend('sync_bad_duration');
const successRate = new Rate('success_rate');
const requestsSent = new Counter('total_requests_sent');

// -----------------------------
// Load profile
// -----------------------------
export const options = {
  stages: [
    { duration: '1m', target: 6 },   // ramp-up
    { duration: '5m', target: 6 },   // stable
    { duration: '1m', target: 8 },   // spike bad
    { duration: '1m', target: 6 },   // back to normal
  ],

  thresholds: {
    http_req_duration: ['p(95)<1000'],
    http_req_failed: ['rate<0.05'],
    checks: ['rate>0.95'],

    'async_request_duration': ['p(95)<500'],
    'sync_good_duration': ['p(95)<300'],
    'sync_bad_duration': ['p(95)<300'],
  },
};

// -----------------------------
// Stage detection
// -----------------------------
function getCurrentStage(time) {
  if (time < 60) return 'ramp_up';
  if (time < 360) return 'stable';
  if (time < 420) return 'spike';
  return 'recovery';
}

// -----------------------------
// Kafka async produce
// -----------------------------
function sendKafkaAsync() {
  writer.produce({
    messages: [
      {
        value: schemaRegistry.serialize({
          data: String(Math.trunc(1 + Math.random() * 10000)),
          schemaType: SCHEMA_TYPE_STRING,
        }),
        partition: nextPartition(),
      },
    ],
  });
}

// -----------------------------
// Main logic
// -----------------------------
export default function () {
  const t = __ITER;  
  const stage = getCurrentStage(t);
  const randomValue = Math.random();  
  const start = Date.now();

  // Наша формула:
  // async = 2/6 на нормальных этапах
  // good/bad меняются в spike

  let type = '';
  let success = false;

  // -----------------------------
  // 1) Kafka async — 2 rps всегда
  // -----------------------------
  if (randomValue < (2/6)) {
    type = 'async';
    sendKafkaAsync();

    asyncDuration.add(Date.now() - start);
    requestsSent.add(1, { type: 'async' });
    successRate.add(true);

    sleep(1);
    return;
  }

  // -----------------------------
  // 2) HTTP sync — good / bad
  // -----------------------------
  let payload, expectedStatus;

  if (stage === 'spike') {
    // На spike — good=3 rps, bad=3 rps из total 8
    if (randomValue < (2/6 + 3/8)) {
      type = 'sync_good';
      payload = JSON.stringify({
        message: `Good msg VU${__VU} Iter${__ITER}`,
        data: "good good good",
      });
      expectedStatus = 200;
    } else {
      type = 'sync_bad';
      payload = JSON.stringify({
        message: `Bad msg VU${__VU} Iter${__ITER}`,
        data: "bad bad bad",
      });
      expectedStatus = 400;
    }
  } else {
    // Нормальные этапы — good=3 rps, bad=1 rps из total 6
    if (randomValue < (2/6 + 3/6)) {
      type = 'sync_good';
      payload = JSON.stringify({
        message: `Good msg VU${__VU} Iter${__ITER}`,
        data: "good good good",
      });
      expectedStatus = 200;
    } else {
      type = 'sync_bad';
      payload = JSON.stringify({
        message: `Bad msg VU${__VU} Iter${__ITER}`,
        data: "bad bad bad",
      });
      expectedStatus = 400;
    }
  }

  const res = http.post(
    "http://localhost:8080/api/sync",
    payload,
    { headers: { "Content-Type": "application/json" } }
  );

  if (type === 'sync_good') {
    success = check(res, {
      'good_status_200': (r) => r.status === 200,
      'good_body_ok': (r) => r.body.includes('ok'),
    });
    syncGoodDuration.add(Date.now() - start);
  } else {
    success = check(res, {
      'bad_status_400': (r) => r.status === 400,
      'bad_body_error': (r) => r.body.includes('bad'),
    });
    syncBadDuration.add(Date.now() - start);
  }

  requestsSent.add(1, { type });
  successRate.add(success);

  sleep(1);
}

// -----------------------------
// Cleanup
// -----------------------------
export function teardown() {
  writer.close();
  connection.close();
}
