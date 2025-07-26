package com.test.consumer;

import com.test.avro.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {
    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(topics = "bharath-avro",groupId = "bharath-avro-new")
    public void consumeEvents(Order order) {
        log.info("Order consume the events {} ", order.toString());
    }
}
