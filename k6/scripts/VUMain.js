import { kafkaScenario, kafkaOptions } from "./scenarios/VUKafka.js";
import { restGoodScenario, restGoodOptions } from "./scenarios/VURestGood.js";
import { restBadScenario, restBadOptions } from "./scenarios/VURestBad.js";

export const options = {
    scenarios: {
        kafka: { ...kafkaOptions, exec: "kafkaScenario" },
        rest_good: { ...restGoodOptions, exec: "restGoodScenario" },
        rest_bad: { ...restBadOptions, exec: "restBadScenario" },
    },
};

export { kafkaScenario, restGoodScenario, restBadScenario };