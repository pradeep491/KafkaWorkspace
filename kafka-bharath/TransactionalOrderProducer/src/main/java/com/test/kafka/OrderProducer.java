package com.test.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        //props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        //step-1
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-producer-1");

        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        //step-2
        producer.initTransactions();
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Mac Book Pro", 10);
        ProducerRecord<String, Integer> record1 = new ProducerRecord<>("OrderTopic", "Dell Laptop", 20);
        ProducerRecord<String, Integer> record2 = new ProducerRecord<>("OrderTopic", "Acer Laptop", 30);
        try {
            producer.beginTransaction();
            producer.send(record);
            producer.send(record1);
            producer.send(record2);
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
