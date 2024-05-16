package com.test.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Mac Book Pro", 10);
        try {
            RecordMetadata recordMetadata= producer.send(record).get();
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
            System.out.println("Message Sent successfully");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
