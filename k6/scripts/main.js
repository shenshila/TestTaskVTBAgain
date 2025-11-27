import { kafkaScenario, kafkaOptions } from "./scenarios/RateKafka.js";
import { restGoodScenario, restGoodOptions } from "./scenarios/RateRestGood.js";
import { restBadScenario, restBadOptions } from "./scenarios/RateRestBad.js";

export const options = {
    scenarios: {
        kafka: { ...kafkaOptions, exec: "kafkaScenario" },
        rest_good: { ...restGoodOptions, exec: "restGoodScenario" },
        rest_bad: { ...restBadOptions, exec: "restBadScenario" },
    },
};

export { kafkaScenario, restGoodScenario, restBadScenario };