import http from "k6/http";
import { sleep } from "k6";
import {
    Writer,
    Connection,
    SchemaRegistry,
    SCHEMA_TYPE_STRING,
} from "k6/x/kafka";

const KAFKA_BROKER = "localhost:29092";
const TOPIC_IN = "input-topic";

// Kafka writer
const writer = new Writer({
    brokers: [KAFKA_BROKER],
    topic: TOPIC_IN,
    balancer: "balancer_roundrobin",
    batchSize: 1,
});

const schemaRegistry = new SchemaRegistry();
let kafkaCounter = 0;

function sendKafkaMessage() {
    const partition = kafkaCounter++ % 2;
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
                partition: partition,
            },
        ],
    });
}

const URL = "http://localhost:8080/api/sync";

function sendGoodRequest() {
    http.post(URL, "good");
}

function sendBadRequest() {
    http.post(URL, "bad");
}

export const options = {
    scenarios: {
        kafka_async: {
            executor: "ramping-arrival-rate",
            startRate: 0,
            timeUnit: "1s",
            preAllocatedVUs: 20,
            maxVUs: 50,
            stages: [
                { duration: "1m", target: 2 },
                { duration: "5m", target: 2 },
                { duration: "1m", target: 2 },
                { duration: "1m", target: 2 }, 
            ],
            exec: "kafkaScenario"
        },

        rest_good: {
            executor: "ramping-arrival-rate",
            startRate: 0,
            timeUnit: "1s",
            preAllocatedVUs: 20,
            maxVUs: 50,
            stages: [
                { duration: "1m", target: 3 },
                { duration: "5m", target: 3 },
                { duration: "1m", target: 3 },
                { duration: "1m", target: 3 },
            ],
            exec: "restGoodScenario"
        },

        rest_bad: {
            executor: "ramping-arrival-rate",
            startRate: 0,
            timeUnit: "1s",
            preAllocatedVUs: 20,
            maxVUs: 50,
            stages: [
                { duration: "1m", target: 1 },
                { duration: "5m", target: 1 },
                { duration: "1m", target: 3 },
                { duration: "1m", target: 1 },
            ],
            exec: "restBadScenario"
        },
    },
};

export function kafkaScenario() {
    sendKafkaMessage();
}

export function restGoodScenario() {
    sendGoodRequest();
}

export function restBadScenario() {
    sendBadRequest();
}

export function teardown() {
    writer.close();
}
