package org.melekhov.testtaskvtbagain.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class MessageController {

    @PostMapping("/sync")
    public ResponseEntity<String> sendMessage(@RequestBody String message){
        String lower = message.toLowerCase();
        if (lower.contains("good")){
            return ResponseEntity.ok("ok");
        } else if (lower.contains("bad")){
            return ResponseEntity.status(400).body("bad request");
        } else {
            return ResponseEntity.status(418).body("ignored");
        }
    }

    private final KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/kafka")
    public ResponseEntity<String> sendKafkaMessage(@RequestBody String message){
        kafkaTemplate.send("input-topic", message);
        return ResponseEntity.ok("ok");
    }

}
