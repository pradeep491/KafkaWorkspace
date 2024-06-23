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
        props.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"343434");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"gzip");
        props.setProperty(ProducerConfig.RETRIES_CONFIG,"2");
        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,"500");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"1024");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG,"200");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,"200");


        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Mac Book Pro", 10);
        try {
            producer.send(record);
            System.out.println("Message Sent successfully");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
