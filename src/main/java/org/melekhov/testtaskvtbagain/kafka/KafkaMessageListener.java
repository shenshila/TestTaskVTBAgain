package org.melekhov.testtaskvtbagain.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaMessageListener {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = "input-topic", groupId = "stub-consumer-group")
    public void listen(String message, Acknowledgment ack) {
        String msg = "output" + message;
        kafkaTemplate.send("output-topic", msg);
        ack.acknowledge();
    }

}
