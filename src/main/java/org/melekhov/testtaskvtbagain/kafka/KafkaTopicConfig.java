package org.melekhov.testtaskvtbagain.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {
    @Bean NewTopic inputTopic() {
        return new NewTopic("input-topic", 1, (short) 1);
    }

    @Bean NewTopic outputTopic() {
        return new NewTopic("output-topic", 1, (short) 1);
    }
}
