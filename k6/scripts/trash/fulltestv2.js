import { Writer } from "k6/x/kafka";
import { check, sleep } from 'k6';
import http from 'k6/http';
import { Counter, Rate } from 'k6/metrics';

// Кастомные метрики
const kafkaMessagesSent = new Counter('kafka_messages_sent');
const successRate = new Rate('success_rate');

const writer = new Writer({
    brokers: ["localhost:29092"],
    topic: "input-topic",
    autoCreateTopic: true,
});

export const options = {
    stages: [
        // Плавный выход на нагрузку за 1 минуту: 2 async + 3 good + 1 bad = 6 rps
        { duration: '1m', target: 6 },
        
        // Стабильная нагрузка 5 минут
        { duration: '5m', target: 6 },
        
        // Резкий скачок bad запросов до 3 rps на 1 минуту: 2 async + 3 good + 3 bad = 8 rps
        { duration: '1m', target: 8 },
        
        // Возврат к исходной нагрузке
        { duration: '1m', target: 6 }
    ],
    thresholds: {
        'kafka_messages_sent': ['count>0'],
        'success_rate': ['rate>0.95'],
        'http_req_duration': ['p(95)<500']
    }
};

export default function() {
    const randomValue = Math.random();
    let success = true;
    
    // Асинхронные сообщения в Kafka (2 rps)
    if (randomValue < 2/6) {
        try {
            writer.produce({
                messages: [
                    {
                        key: `async-${__VU}-${__ITER}`,
                        value: JSON.stringify({
                            type: "async",
                            message: "Async test message from k6",
                            timestamp: new Date().toISOString(),
                            virtualUser: __VU,
                            iteration: __ITER
                        })
                    }
                ],
            });
            
            kafkaMessagesSent.add(1);
            console.log(`✅ Async message sent to Kafka`);
        } catch (error) {
            console.error(`❌ Failed to send async message: ${error}`);
            success = false;
        }
    } 
    // Синхронные REST запросы
    else {
        let payload, expectedStatus, requestType;
        
        if (randomValue < 5/6) {
            // Good запросы (3 rps)
            payload = JSON.stringify({ 
                message: 'This is a good request with good content'
            });
            expectedStatus = 200;
            requestType = 'sync_good';
        } else {
            // Bad запросы (1 rps, затем 3 rps на этапе spike)
            payload = JSON.stringify({ 
                message: 'This is a bad request with bad content'
            });
            expectedStatus = 400;
            requestType = 'sync_bad';
        }
        
        const response = http.post(
            'http://localhost:8080/api/sync',
            payload,
            {
                headers: { 'Content-Type': 'application/json' },
                tags: { type: requestType }
            }
        );
        
        const checkResult = check(response, {
            'status correct': (r) => r.status === expectedStatus,
        });
        
        if (checkResult) {
            console.log(`✅ ${requestType} request: ${response.status}`);
        } else {
            console.log(`❌ ${requestType} request failed: ${response.status}`);
            success = false;
        }
    }
    
    successRate.add(success);
    sleep(1);
}

export function teardown() {
    writer.close();
    console.log("Kafka writer closed");
}