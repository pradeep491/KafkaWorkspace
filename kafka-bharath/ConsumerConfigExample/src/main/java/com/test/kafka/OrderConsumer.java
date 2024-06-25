package com.test.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup");

        props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"102412323");
        props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,"102412323");
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,"1000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"3000");

        props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,"1MB");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,"OrderConsumer");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderTopic"));
        ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofSeconds(20));
        for (ConsumerRecord<String, Integer> consumerRecord : records) {
            System.out.println(consumerRecord.key());
            System.out.println(consumerRecord.value());
        }
        consumer.close();
    }
}
