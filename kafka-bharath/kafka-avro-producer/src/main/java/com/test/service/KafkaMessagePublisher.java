package com.test.service;

import com.test.avro.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaMessagePublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessageToTopic(Order order) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("bharath-avro", order);
        //future.get() //it will slow down the process

        //callback implementation
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + order.toString() +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        order.toString() + "] due to : " + ex.getMessage());
            }
        });
    }
}
