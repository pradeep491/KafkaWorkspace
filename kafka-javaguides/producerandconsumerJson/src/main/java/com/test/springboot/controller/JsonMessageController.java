package com.test.springboot.controller;

import com.test.springboot.kafka.JsonKafkaProducer;
import com.test.springboot.model.User;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/kafka")
@AllArgsConstructor
public class JsonMessageController {
    private JsonKafkaProducer producer;

    @PostMapping("/publish")
    public ResponseEntity<String> publish(@RequestBody User user) {
        producer.sendMessage(user);
        return ResponseEntity.ok("Json Message Sent to kafka Topic...!");
    }
}
