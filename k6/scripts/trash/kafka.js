import {
    Writer,
    Connection,
    SchemaRegistry,
    SCHEMA_TYPE_STRING,
} from "k6/x/kafka";
import { check, sleep } from "k6";

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

export const options = {
    iterations: 10,
}

export function setup() {
    const setupConnection = new Connection({
        address: "localhost:29092",
    });

    const topics = setupConnection.listTopics();
    if (!topics.includes("input-topic")) {
        setupConnection.createTopic({
            topic: "input-topic",
            numPartitions: 10,
            replicationFactor: 1,
        });
        console.log("Topic 'input-topic' created");
    } else {
        console.log("Topic 'input-topic' already exists");
    }

    sleep(2);

    const updatedTopics = setupConnection.listTopics();
    console.log("Topics after creation attempt: ", updatedTopics);

    if (!topics.includes("input-topic")) {
        throw new Error("Topic was not created successfully");
    };

    setupConnection.close();
}

let messagesCount = 0;

function getMessagesSendCountThanIncrement() {
    return messagesCount++;
}

export default function() {
    var partitionNum;

    const topics = connection.listTopics();
    console.log("Available topics: ", topics);

    if (!topics.includes("input-topic")) {
        console.error("Topic 'input-topic' does not exist");
        return;
    }

    if (getMessagesSendCountThanIncrement() % 2 == 0) {
        partitionNum = 0;
    } else {
        partitionNum = 1;
    }

    console.log(`Sending message to partition: ${partitionNum}`);

    try {
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
        console.log("Message sent successfully");
    } catch (error) {
        console.error("Error sending message: ", error);
    }
}

export function teardown() {
    try {
        connection.deleteTopic("input-topic");
        console.log("Topic deleted");
    } catch (error) {
        console.error("Error deleting topic: ", error);
    }

    writer.close();
    connection.close();
}