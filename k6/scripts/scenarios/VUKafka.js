import { sleep } from "k6";
import { Writer, Connection, SchemaRegistry, SCHEMA_TYPE_STRING } from "k6/x/kafka";
import { Trend, Counter, Rate } from 'k6/metrics';

const kafkaDuration = new Trend('kafka_duration');
const kafkaRequests = new Counter('kafka_requests');
const kafkaSuccessRate = new Rate('kafka_success');

const writer = new Writer({
  brokers: ["localhost:29092"],
  topic: "input-topic",
  balancer: "balancer_roundrobin",
  batchSize: 1,
});

const connection = new Connection({
    address: "localhost:29092",
})

const schemaRegistry = new SchemaRegistry();

export const kafkaOptions = {
  executor: "ramping-vus",
  startVUs: 0,
  stages: [
    { duration: "1m", target: 1 },
    { duration: "5m", target: 1 },
    { duration: "2m", target: 1 },
  ],
};

let messageCount = 0;

function getMessagesSendCountThanIncrement() {
    return messageCount++;
}

export function kafkaScenario() {
  const pacing = 500; 
  const start = Date.now();

  try {
    var partitionNum;
  
    if (getMessagesSendCountThanIncrement() % 2 == 0) {
        partitionNum = 0;
    } else {
        partitionNum = 1;
    }
  
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
                partition: partitionNum,
            }
        ]
    });
          
    kafkaSuccessRate.add(1);
    kafkaRequests.add(1);
    kafkaDuration.add(Date.now() - start);
          
  } catch (error) {
        kafkaSuccessRate.add(0);
        kafkaRequests.add(1);
        console.error('Kafka error:', error);
    }

  const elapsed = (Date.now() - start);
  sleep(Math.max(pacing - elapsed, 0) / 1000);

  // sleep(pacing - ((Date.now() - start)));

  // if (elapsed > pacing) {
  //   sleep((pacing - elapsed) * 1000);
  // }
  }

export function teardown() {
    writer.close();
    connection.close();
}
