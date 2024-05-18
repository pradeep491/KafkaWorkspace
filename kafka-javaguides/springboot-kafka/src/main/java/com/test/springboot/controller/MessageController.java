package com.test.springboot.controller;

import com.test.springboot.kafka.KafkaProducer;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/kafka")
@AllArgsConstructor
public class MessageController {
    private KafkaProducer kafkaProducer;

    //http://localhost:8080/api/v1/kafka/publish?message=Hello World
    @GetMapping("/publish")
    public ResponseEntity<String> publish(@RequestParam("message") String message){
        kafkaProducer.sendMessage(message);
        return ResponseEntity.ok("Message Sent to that topic");
    }

}
