import http from "k6/http";
import { sleep } from "k6";
import {
    Writer,
    SchemaRegistry,
    SCHEMA_TYPE_STRING,
} from "k6/x/kafka";

const writer = new Writer({
    brokers: ["localhost:29092"],
    topic: "input-topic",
});

const schemaRegistry = new SchemaRegistry();
let kafkaCounter = 0;

export const options = {
    scenarios: {
        kafka: {
            executor: "ramping-vus",
            startVUs: 0,
            stages: [
                { duration: "1m", target: 1 },   // ~2 rps (0.5s sleep)
                { duration: "5m", target: 1 },   
                { duration: "1m", target: 1 },
                { duration: "1m", target: 1 },
            ],
            exec: "kafkaScenario"
        },

        rest_good: {
            executor: "ramping-vus",
            startVUs: 0,
            stages: [
                { duration: "1m", target: 1 },   // ~3 rps (0.333s sleep)
                { duration: "5m", target: 1 },
                { duration: "1m", target: 1 },
                { duration: "1m", target: 1 },
            ],
            exec: "restGoodScenario"
        },

        rest_bad: {
            executor: "ramping-vus",
            startVUs: 0,
            stages: [
                { duration: "1m", target: 1 },   // ~1 rps (1s sleep)
                { duration: "5m", target: 1 },
                { duration: "1m", target: 3 },   // скачок: 3 VU → 3 rps
                { duration: "1m", target: 1 },   // откат
            ],
            exec: "restBadScenario"
        },
    },
};

export function kafkaScenario() {
    writer.produce({
        messages: [
            {
                key: schemaRegistry.serialize({
                    data: "Random message",
                    schemaType: SCHEMA_TYPE_STRING,
                }),
                value: schemaRegistry.serialize({
                    data: String(Math.trunc(1 + Math.random() * 10000)),
                    schemaType: SCHEMA_TYPE_STRING,
                }),
                partition: kafkaCounter++ % 2,
            }
        ]
    });
    sleep(0.5); // 2 rps
}

export function restGoodScenario() {
    http.post("http://localhost:8080/api/sync", "good");
    sleep(0.333); // 3 rps
}

export function restBadScenario() {
    http.post("http://localhost:8080/api/sync", "bad");
    sleep(1); // 1 rps (а в фазе скачка VU увеличиваются до 3)
}
