package com.test.controller;

import com.test.avro.Order;
import com.test.service.KafkaMessagePublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class OrderController {
    private KafkaMessagePublisher publisher;

    public OrderController(KafkaMessagePublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping("/publish")
    public ResponseEntity<?> publishMessage(@RequestBody Order order) {
        try {
            publisher.sendMessageToTopic(order);
            return ResponseEntity.ok("message published successfully ..");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }
    //kafka GenericRecord topic example- not working
    @PostMapping("/publishGenericRecord")
    public ResponseEntity<?> publishMessageViaGenericRecord() {
        try {
            publisher.sendGenericRecordToTopic();
            return ResponseEntity.ok("message published successfully ..");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }
}
