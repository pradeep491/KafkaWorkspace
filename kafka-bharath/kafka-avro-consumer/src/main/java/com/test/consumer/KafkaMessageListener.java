package com.test.consumer;

import com.test.avro.Order;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
    //kafka GenericRecord topic example- not working
    @KafkaListener(topics = "bharath-avro",groupId = "bharath-avro-new")
    public void consumeEventsFromGeneric(GenericRecord genericRecord) {
        log.info("Order GenericRecord consume the events: ", genericRecord.toString());
        /*for (ConsumerRecord<String, GenericRecord> record : genericRecord) {
            String customerName = record.key();
            GenericRecord order = record.value();
            System.out.println("Customer name:" + customerName);
            System.out.println("product:" + order.get("product"));
            System.out.println("quantity:" + order.get("quantity"));
            System.out.println("Message Received...!");
        }*/

    }
}
