import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';
import {
  Writer,
  Connection,
  SchemaRegistry,
  SCHEMA_TYPE_STRING,
} from "k6/x/kafka";

const writer = new Writer({
  brokers: ["localhost:29092"],
  topic: "input-topic",
});

const connection = new Connection({
  address: "localhost:29092",
});

const schemaRegistry = new SchemaRegistry();
let kafkaCounter = 0;

const asyncDuration = new Trend('async_request_duration');
const syncGoodDuration = new Trend('sync_good_duration');
const syncBadDuration = new Trend('sync_bad_duration');
const successRate = new Rate('success_rate');
const requestsSent = new Counter('total_requests_sent');

export const options = {
  stages: [
    { duration: '1m', target: 6 },   // ramp-up
    { duration: '5m', target: 6 },   // stable
    { duration: '1m', target: 8 },   // spike bad
    { duration: '1m', target: 6 },   // back to normal
  ],
};

function getCurrentStage(time) {
  if (time < 60) return 'ramp_up';
  if (time < 360) return 'stable';
  if (time < 420) return 'spike';
  return 'recovery';
}

function sendKafkaAsync() {
  writer.produce({
    messages: [
      {
        value: schemaRegistry.serialize({
          data: String(Math.trunc(1 + Math.random() * 10000)),
          schemaType: SCHEMA_TYPE_STRING,
        }),
      },
    ],
  });
}

export default function () {
  const t = __ITER;  
  const stage = getCurrentStage(t);
  const randomValue = Math.random();  
  const start = Date.now();

  let type = '';
  let success = false;

  if (randomValue < (2/6)) {
    type = 'async';
    sendKafkaAsync();

    asyncDuration.add(Date.now() - start);
    requestsSent.add(1, { type: 'async' });
    successRate.add(true);

    sleep(1);
    return;
  }

  let payload, expectedStatus;

  if (stage === 'spike') {
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

export function teardown() {
  writer.close();
  connection.close();
}
