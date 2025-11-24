import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter, Gauge } from 'k6/metrics';
import { KafkaClient, Producer } from "k6/x/kafka";

// Кастомные метрики для мониторинга
const brokers = ["localhost:9092"];
const topic = "test_topic";

const client = new KafkaClient({ brokers });
const producer = new Producer(client);

const asyncDuration = new Trend('async_request_duration');
const syncGoodDuration = new Trend('sync_good_duration');
const syncBadDuration = new Trend('sync_bad_duration');
const successRate = new Rate('success_rate');
const requestsSent = new Counter('total_requests_sent');

// Глобальные переменные для отслеживания RPS
let currentStage = 'ramp_up';

export const options = {
  stages: [
    // Этап 1: Плавный выход на нагрузку за 1 минуту
    // 2 rps async + 3 rps good + 1 rps bad = 6 rps total
    { duration: '1m', target: 6 },
    
    // Этап 2: Стабильная нагрузка 5 минут
    { duration: '5m', target: 6 },
    
    // Этап 3: Резкий скачок bad запросов до 3 rps на 1 минуту
    // 2 rps async + 3 rps good + 3 rps bad = 8 rps total
    { duration: '1m', target: 8 },
    
    // Этап 4: Возврат к исходной нагрузке
    { duration: '1m', target: 6 }
  ],
  
  thresholds: {
    // Общие thresholds
    http_req_duration: ['p(95)<1000'], // 95% запросов должны быть быстрее 1s
    http_req_failed: ['rate<0.05'],    // Меньше 5% ошибок
    checks: ['rate>0.95'],
    
    // Thresholds по типам запросов
    'async_request_duration': ['p(95)<500'],
    'sync_good_duration': ['p(95)<300'],
    'sync_bad_duration': ['p(95)<300'],
  },
  
  // Настройки для InfluxDB
  ext: {
    influxdb: {
      url: 'http://localhost:8086',
      database: 'k6',
      tags: {
        test_type: 'stub_load_test',
        app_version: '1.0'
      }
    }
  }
};

// Функция для определения текущего этапа теста
function getCurrentStage(executionTime) {
  if (executionTime < 60) return 'ramp_up';
  if (executionTime < 360) return 'stable';
  if (executionTime < 420) return 'spike';
  return 'recovery';
}

export default function () {
  const execTime = __ITER * 1; // Примерное время выполнения
  
  // Определяем тип запроса на основе равномерного распределения
  // и текущего этапа теста
  const randomValue = Math.random();
  const stage = getCurrentStage(execTime);
  
  let requestType = '';
  let success = false;
  const startTime = Date.now();

  // Асинхронные сообщения в Kafka через /produce (2 rps постоянно)
  if (randomValue < 2/6) {
    requestType = 'async';
    
    const asyncPayload = JSON.stringify({
      message: `Async message from VU${__VU} Iter${__ITER}`,
      timestamp: new Date().toISOString(),
      type: 'async',
      data: 'This is async test data'
    });
    
    const asyncResponse = http.post(
      'http://localhost:8080/api/produce',
      asyncPayload,
      {
        headers: {
          'Content-Type': 'application/json',
        },
        tags: { 
          type: 'async',
          stage: stage,
          endpoint: '/api/produce'
        }
      }
    );
    
    success = check(asyncResponse, {
      'async message sent successfully': (r) => r.status === 200,
      'async response contains sent': (r) => r.body.includes('sent'),
      'async request duration acceptable': (r) => r.timings.duration < 1000
    });
    
    asyncDuration.add(Date.now() - startTime);
    requestsSent.add(1, { type: 'async' });
  }
  // Синхронные запросы к /sync
  else {
    let syncPayload = '';
    let expectedStatus = 0;
    
    // Логика распределения запросов в зависимости от этапа
    if (stage === 'spike') {
      // На этапе spike: 3 rps good, 3 rps bad (из 8 total)
      if (randomValue < 5/8) {
        // Good запросы (3 rps)
        requestType = 'sync_good';
        syncPayload = JSON.stringify({
          message: `Good message VU${__VU} Iter${__ITER}`,
          data: 'This is good content with good keyword',
          good: true,
          timestamp: new Date().toISOString()
        });
        expectedStatus = 200;
      } else {
        // Bad запросы (3 rps)
        requestType = 'sync_bad';
        syncPayload = JSON.stringify({
          message: `Bad message VU${__VU} Iter${__ITER}`,
          data: 'This is bad content with bad keyword',
          bad: true,
          timestamp: new Date().toISOString()
        });
        expectedStatus = 400;
      }
    } else {
      // На других этапах: 3 rps good, 1 rps bad (из 6 total)
      if (randomValue < 5/6) {
        // Good запросы (3 rps)
        requestType = 'sync_good';
        syncPayload = JSON.stringify({
          message: `Good message VU${__VU} Iter${__ITER}`,
          data: 'This is good content with good keyword',
          good: true,
          timestamp: new Date().toISOString()
        });
        expectedStatus = 200;
      } else {
        // Bad запросы (1 rps)
        requestType = 'sync_bad';
        syncPayload = JSON.stringify({
          message: `Bad message VU${__VU} Iter${__ITER}`,
          data: 'This is bad content with bad keyword',
          bad: true,
          timestamp: new Date().toISOString()
        });
        expectedStatus = 400;
      }
    }
    
    const syncResponse = http.post(
      'http://localhost:8080/api/sync',
      syncPayload,
      {
        headers: {
          'Content-Type': 'application/json',
        },
        tags: { 
          type: requestType,
          stage: stage,
          endpoint: '/api/sync',
          expected_status: expectedStatus.toString()
        }
      }
    );
    
    if (requestType === 'sync_good') {
      success = check(syncResponse, {
        'good request status is 200': (r) => r.status === 200,
        'good request returns ok': (r) => r.body.includes('ok'),
        'good request duration acceptable': (r) => r.timings.duration < 500
      });
      syncGoodDuration.add(Date.now() - startTime);
    } else {
      success = check(syncResponse, {
        'bad request status is 400': (r) => r.status === 400,
        'bad request returns bad request': (r) => r.body.includes('bad request'),
        'bad request duration acceptable': (r) => r.timings.duration < 500
      });
      syncBadDuration.add(Date.now() - startTime);
    }
    
    requestsSent.add(1, { type: requestType });
  }
  
  successRate.add(success);
  
  // Пауза между запросами для точного контроля RPS
  sleep(1);
}

// Функция для вывода информации о тесте
export function handleSummary(data) {
  console.log('Test completed!');
  console.log('Total duration:', data.state.testDuration);
  console.log('Total iterations:', data.state.iterations);
  console.log('Total requests:', data.metrics.http_reqs.values.count);
  
  return {
    'stdout': 'text summary goes here...',
  };
}

// import http from 'k6/http';
// import { sleep, check } from 'k6';
// import { Trend } from 'k6/metrics';

// export let goodReqTrend = new Trend('good_sync_rtt');
// export let badReqTrend = new Trend('bad_sync_rtt');
// export let asyncReqTrend = new Trend('async_rtt');

// export let options = {
//     discardResponseBodies: true,
//     scenarios: {
//         rampup: {
//             executor: 'ramping-arrival-rate',
//             startRate: 0,
//             timeUnit: '1s',
//             preAllocatedVUs: 50,
//             maxVUs: 200,
//             stages: [
//                 { target: 6, duration: '1m' }, // суммарно 6 rps (3 good +1 bad +2 async), приближение
//             ],
//             tags: { type: 'ramp' }
//         },
//         steady: {
//             executor: 'constant-arrival-rate',
//             rate: 6,
//             timeUnit: '1s',
//             duration: '4m',
//             preAllocatedVUs: 50,
//             maxVUs: 200,
//             startTime: '1m',
//             tags: { type: 'steady' }
//         },
//         spike_bad: {
//             executor: 'constant-arrival-rate',
//             rate: 8, // spike: bad goes to 3 rps, so total becomes 8 (3good? keep good 3)
//             timeUnit: '1s',
//             duration: '1m',
//             preAllocatedVUs: 100,
//             maxVUs: 400,
//             startTime: '5m',
//             tags: { type: 'spike' }
//         },
//         post_spike: {
//             executor: 'constant-arrival-rate',
//             rate: 6,
//             timeUnit: '1s',
//             duration: '1m',
//             preAllocatedVUs: 50,
//             maxVUs: 200,
//             startTime: '6m',
//             tags: { type: 'post' }
//         }
//     },
//     ext: {
//       loadimpact: { projectID: 0 }
//     }
// };

// // helper to pick which request to send by weight
// function sendGood() {
//     let res = http.post('http://localhost:8080/api/sync', 'this is good message');
//     goodReqTrend.add(res.timings.duration);
//     check(res, { 'status 200': (r) => r.status === 200 });
// }

// function sendBad() {
//     let res = http.post('http://localhost:8080/api/sync', 'this is bad message');
//     badReqTrend.add(res.timings.duration);
//     check(res, { 'status 400': (r) => r.status === 400 });
// }

// function sendAsync() {
//     // publish via HTTP endpoint that will produce to Kafka
//     let res = http.post('http://localhost:8080/api/produce', 'async message payload');
//     asyncReqTrend.add(res.timings.duration);
//     check(res, { 'status 200 or 202': (r) => r.status === 200 || r.status === 202 });
// }

// export default function (data) {
//     // distribute calls: weights approximated by simple random, to reach required rps distribution
//     // But we control overall arrival-rates via scenarios. Here we send all three types each iteration to
//     // approximate proportions. Alternative: create separate scenarios per request type.
//     // Simpler and more accurate: create separate scenarios, but for brevity this will mix.
//     sendGood();
//     sendGood();
//     sendGood(); // 3 good
//     sendBad();  // 1 bad (but during spike scenario rate increased)
//     sendAsync();
//     sendAsync(); // 2 async
//     sleep(1);
// }
