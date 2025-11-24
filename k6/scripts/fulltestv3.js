import http from 'k6/http';
import { check } from 'k6';
import { Writer } from "k6/x/kafka";

// Kafka writer
const writer = new Writer({
    brokers: ["localhost:29092"],
    topic: "input-topic",
    autoCreateTopic: true,
});

// ---- SCENARIOS ----
export const options = {
    scenarios: {
        async_kafka: {
            executor: "constant-arrival-rate",
            rate: 2,             // 2 rps
            timeUnit: "1s",
            duration: "7m",      // 1 мин рост + 5 мин основная + 1 мин спад
            preAllocatedVUs: 10,
            exec: "sendAsync",
        },

        http_good: {
            executor: "constant-arrival-rate",
            rate: 3,              // 3 rps good
            timeUnit: "1s",
            duration: "7m",
            preAllocatedVUs: 20,
            exec: "sendGood",
        },

        http_bad: {
            executor: "ramping-arrival-rate",
            startRate: 1,         // 1 rps
            timeUnit: "1s",
            preAllocatedVUs: 10,
            stages: [
                { duration: '1m', target: 1 },   // Плавный рост до базовой нагрузки
                { duration: '5m', target: 1 },   // Основная нагрузка
                { duration: '1m', target: 3 },   // SPIKE: резкий скачок bad до 3 rps
                { duration: '1m', target: 1 },   // Обратно вниз
            ],
            exec: "sendBad",
        },
    }
};

// ---- EXEC FUNCTIONS ----

// 1) ASYNC KAFKA MESSAGES (2 rps)
export function sendAsync() {
    writer.produce({
        messages: [
            {
                key: `async-${__VU}-${__ITER}`,
                value: JSON.stringify({
                    type: "async",
                    timestamp: new Date().toISOString()
                })
            }
        ],
    });
}


// 2) GOOD HTTP REQUESTS (3 rps)
export function sendGood() {
    const payload = JSON.stringify({ message: "good message" });

    const res = http.post("http://localhost:8080/api/sync", payload, {
        headers: { "Content-Type": "application/json" },
        tags: { type: "good" }
    });

    check(res, { "good status 200": (r) => r.status === 200 });
}


// 3) BAD HTTP REQUESTS (1 → 3 → 1 rps)
export function sendBad() {
    const payload = JSON.stringify({ message: "bad message" });

    const res = http.post("http://localhost:8080/api/sync", payload, {
        headers: { "Content-Type": "application/json" },
        tags: { type: "bad" }
    });

    check(res, { "bad status 400": (r) => r.status === 400 });
}

export function teardown() {
    writer.close();
}
