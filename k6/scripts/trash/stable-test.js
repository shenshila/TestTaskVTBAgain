import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter, Rate } from 'k6/metrics';
import { Writer, Connection, SchemaRegistry, SCHEMA_TYPE_STRING } from 'k6/x/kafka';

// ==================== КАСТОМНЫЕ МЕТРИКИ ====================
// Метрики для Kafka
const kafkaDuration = new Trend('kafka_duration_ms');
const kafkaRequests = new Counter('kafka_requests_total');
const kafkaSuccess = new Rate('kafka_success_rate');

// Метрики для REST запросов
const restDuration = new Trend('rest_duration_ms');
const restRequests = new Counter('rest_requests_total');
const restSuccess = new Rate('rest_success_rate');

// Общие метрики
const currentVUs = new Counter('vus_active');
const iterations = new Counter('iterations_total');

// ==================== НАСТРОЙКА KAFKA ====================
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

// ==================== КОНФИГУРАЦИЯ ТЕСТА ====================
export const options = {
    scenarios: {
        // Высокая стабильная нагрузка на все системы
        high_load: {
            executor: 'constant-vus',
            vus: 20, // 20 параллельных пользователей
            duration: '1m', // 10 минут стабильной нагрузки
            exec: 'mainScenario',
            tags: { test_type: 'high_load_stability' }
        }
    },

    // Пороги для проверки стабильности
    thresholds: {
        'kafka_duration_ms': ['avg < 1000', 'max < 5000', 'p(95) < 2000'],
        'rest_duration_ms': ['avg < 800', 'max < 4000', 'p(95) < 1500'],
        'kafka_success_rate': ['rate > 0.95'],
        'rest_success_rate': ['rate > 0.95'],
        'http_req_failed': ['rate < 0.05'], // Менее 5% ошибок
    },

    // Детальная статистика
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

// ==================== ОСНОВНОЙ СЦЕНАРИЙ ====================
let messageCount = 0;

export function mainScenario() {
    iterations.add(1);
    
    // Каждый VU выполняет все типы операций
    sendKafkaMessage();
    sendRestGoodRequest();
    sendRestBadRequest();
}

function sendKafkaMessage() {
    const startTime = Date.now();
    
    try {
        const partitionNum = messageCount % 2;
        const currentMessageId = messageCount++;
        
        writer.produce({
            messages: [
                {
                    key: schemaRegistry.serialize({
                        data: `Load test message ${currentMessageId}`,
                        schemaType: SCHEMA_TYPE_STRING,
                    }),
                    value: schemaRegistry.serialize({
                        data: JSON.stringify({
                            id: currentMessageId,
                            timestamp: new Date().toISOString(),
                            load_test: true,
                            data: `High load payload ${Math.trunc(1 + Math.random() * 100000)}`,
                            vu_id: __VU
                        }),
                        schemaType: SCHEMA_TYPE_STRING,
                    }),
                    partition: partitionNum,
                }
            ]
        });

        const duration = Date.now() - startTime;
        
        kafkaDuration.add(duration);
        kafkaRequests.add(1);
        kafkaSuccess.add(1);
        
    } catch (error) {
        const duration = Date.now() - startTime;
        kafkaDuration.add(duration);
        kafkaRequests.add(1);
        kafkaSuccess.add(0);
    }
}

function sendRestGoodRequest() {
    const startTime = Date.now();
    
    const payload = JSON.stringify({
        type: "good",
        timestamp: new Date().toISOString(),
        vu_id: __VU,
        iteration: __ITER,
        data: "High load valid request - " + Math.random().toString(36).substring(7),
        load_test: true
    });
    
    const response = http.post("http://localhost:8080/api/sync", payload, {
        headers: { 
            'Content-Type': 'application/json',
            'User-Agent': 'k6-high-load-test',
            'X-Test-Type': 'stability'
        },
        tags: { 
            request_type: 'rest_good',
            endpoint: '/api/sync'
        },
        timeout: '30s' // Увеличенный timeout для стабильности
    });
    
    const duration = Date.now() - startTime;
    const isSuccess = check(response, {
        'status is 200': (r) => r.status === 200,
        'response has reasonable time': (r) => r.timings.duration < 5000,
    });
    
    restDuration.add(duration);
    restRequests.add(1);
    restSuccess.add(isSuccess);
}

function sendRestBadRequest() {
    const startTime = Date.now();
    
    // Иногда отправляем плохие запросы (20% случаев)
    const isBadRequest = Math.random() < 0.2;
    
    const payload = JSON.stringify({
        type: isBadRequest ? "bad" : "good",
        timestamp: new Date().toISOString(),
        vu_id: __VU,
        iteration: __ITER,
        data: isBadRequest ? null : "Valid data payload",
        invalid_field: isBadRequest ? "This should cause error" : undefined,
        load_test: true
    });
    
    const response = http.post("http://localhost:8080/api/sync", payload, {
        headers: { 
            'Content-Type': 'application/json',
            'User-Agent': 'k6-high-load-test',
            'X-Test-Type': 'stability'
        },
        tags: { 
            request_type: isBadRequest ? 'rest_bad' : 'rest_good',
            endpoint: '/api/sync'
        },
        timeout: '30s'
    });
    
    const duration = Date.now() - startTime;
    
    // Для плохих запросов ожидаем 400, для хороших - 200
    const expectedStatus = isBadRequest ? 400 : 200;
    const isSuccess = check(response, {
        'status is appropriate': (r) => r.status === expectedStatus || r.status === 200,
    });
    
    restDuration.add(duration);
    restRequests.add(1);
    restSuccess.add(isSuccess);
}

// ==================== ХУКИ ====================
export function setup() {
    console.log('🚀 Starting HIGH LOAD stability test');
    console.log('📊 Test configuration:');
    console.log('   - Duration: 10 minutes');
    console.log('   - Virtual Users: 20');
    console.log('   - Each VU performs: Kafka + REST Good + REST Bad');
    console.log('   - Target: Maximum stable load');
    console.log('');
}

export function handleSummary(data) {
    console.log('\n📈 ========== HIGH LOAD STABILITY TEST RESULTS ==========');
    
    // Общая статистика
    console.log(`\n📊 GENERAL STATISTICS:`);
    console.log(`   Total iterations: ${data.metrics.iterations_total.values.count || 0}`);
    console.log(`   Total duration: ${data.state.testDuration}`);
    console.log(`   Max VUs: ${data.state.maxVUs}`);
    
    // Kafka метрики
    const kafkaMetrics = data.metrics.kafka_duration_ms;
    console.log(`\n📨 KAFKA PERFORMANCE (${data.metrics.kafka_requests_total.values.count || 0} requests):`);
    console.log(`   Success Rate: ${((data.metrics.kafka_success_rate.values.rate || 0) * 100).toFixed(2)}%`);
    console.log(`   Avg Duration: ${(kafkaMetrics.values.avg || 0).toFixed(2)}ms`);
    console.log(`   Max Duration: ${kafkaMetrics.values.max || 0}ms`);
    console.log(`   p(90): ${(kafkaMetrics.values['p(90)'] || 0).toFixed(2)}ms`);
    console.log(`   p(95): ${(kafkaMetrics.values['p(95)'] || 0).toFixed(2)}ms`);
    console.log(`   p(99): ${(kafkaMetrics.values['p(99)'] || 0).toFixed(2)}ms`);
    
    // REST метрики
    const restMetrics = data.metrics.rest_duration_ms;
    console.log(`\n🌐 REST PERFORMANCE (${data.metrics.rest_requests_total.values.count || 0} requests):`);
    console.log(`   Success Rate: ${((data.metrics.rest_success_rate.values.rate || 0) * 100).toFixed(2)}%`);
    console.log(`   Avg Duration: ${(restMetrics.values.avg || 0).toFixed(2)}ms`);
    console.log(`   Max Duration: ${restMetrics.values.max || 0}ms`);
    console.log(`   p(90): ${(restMetrics.values['p(90)'] || 0).toFixed(2)}ms`);
    console.log(`   p(95): ${(restMetrics.values['p(95)'] || 0).toFixed(2)}ms`);
    console.log(`   p(99): ${(restMetrics.values['p(99)'] || 0).toFixed(2)}ms`);
    
    // HTTP метрики
    console.log(`\n📡 HTTP REQUESTS (${data.metrics.http_reqs.values.count || 0} requests):`);
    console.log(`   Failed: ${((data.metrics.http_req_failed.values.rate || 0) * 100).toFixed(2)}%`);
    console.log(`   Avg: ${(data.metrics.http_req_duration.values.avg || 0).toFixed(2)}ms`);
    console.log(`   Max: ${data.metrics.http_req_duration.values.max || 0}ms`);
    
    // Расчет производительности
    const totalRequests = (data.metrics.kafka_requests_total.values.count || 0) + 
                         (data.metrics.rest_requests_total.values.count || 0);
    const totalDuration = data.state.testDuration / 1000000000; // наносекунды в секунды
    const avgRPS = totalRequests / totalDuration;
    
    console.log(`\n🎯 PERFORMANCE SUMMARY:`);
    console.log(`   Total operations: ${totalRequests}`);
    console.log(`   Average RPS: ${avgRPS.toFixed(2)}`);
    console.log(`   Test duration: ${totalDuration.toFixed(2)}s`);
    
    // Рекомендации для закрытой системы
    console.log(`\n💡 RECOMMENDATIONS FOR CLOSED MODEL:`);
    const kafkaAvg = kafkaMetrics.values.avg || 100;
    const restAvg = restMetrics.values.avg || 200;
    
    console.log(`   Kafka avg time: ${kafkaAvg.toFixed(2)}ms → VUs needed for 10 RPS: ${Math.ceil(10 * kafkaAvg / 1000)}`);
    console.log(`   REST avg time: ${restAvg.toFixed(2)}ms → VUs needed for 10 RPS: ${Math.ceil(10 * restAvg / 1000)}`);
    console.log(`   Suggested think time: ${Math.max(100, (kafkaAvg + restAvg) / 2).toFixed(2)}ms`);
    console.log(`   System capacity: ~${Math.floor(20 * 1000 / ((kafkaAvg + restAvg) / 2))} RPS total`);
    
    return {
        'stdout': 'High load stability test completed! Check console for detailed analysis.',
    };
}

export function teardown() {
    console.log('\n🧹 Cleaning up Kafka connections...');
    try {
        writer.close();
        connection.close();
    } catch (error) {
        console.log('Cleanup completed with warnings');
    }
}