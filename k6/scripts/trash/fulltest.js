import {
    Writer,
    Connection,
    SchemaRegistry,
    SCHEMA_TYPE_STRING,
} from "k6/x/kafka";

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

let messagesSendCount = 0;

function getMessagesSendCountThanIncrement() {
  return messagesSendCount++;
}

export const options = {
	scenarios: {
        first_stage: {
            executor: 'constant-arrival-rate',
            duration: '5m',
            rate: 5,
            preAllocatedVUs: 1,
            timeUnit: '1s',
			maxVUs: 5,
			gracefulStop: '0s',
        },
		second_stage: {
            executor: 'constant-arrival-rate',
			startTime: '5m',
            duration: '5m',
            rate: 10,
            preAllocatedVUs: 1,
            timeUnit: '1s',
			maxVUs: 10,
			gracefulStop: '0s',
        },
    }
};

export default function() {
 var partitionNumber;
	
 if (getMessagesSendCountThanIncrement() % 2 == 0) {
	partitionNumber = 0;	
 } else {
	partitionNumber = 1;	
 }

 writer.produce({
    messages: [
        {
        value: schemaRegistry.serialize({
            data: String(Math.trunc(1 + Math.random() * 10000)) + "-",
            schemaType: SCHEMA_TYPE_STRING,
        }),
		partition: partitionNumber,
        },
    ],
    });
}

export function teardown(data) {
    writer.close();
    connection.close();
}
